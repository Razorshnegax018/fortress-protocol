use std::{collections::HashMap, io::self, thread::JoinHandle, time::Duration};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use arrayvec::ArrayVec;
use bytes::{Buf, Bytes, BytesMut};
use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;

use futures::{SinkExt, StreamExt};
use gdt_cpus::ThreadPriority::Highest;
use tokio::{net::{TcpListener, tcp::{OwnedReadHalf, OwnedWriteHalf}, }, runtime::Handle, sync::mpsc};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use serde::{Deserialize, Serialize};

use crate::{
    infra_peer::ConnectionPacket, utils::utils::*,
};

type SocketFramed = Framed<OwnedWriteHalf, LengthDelimitedCodec>;
type ReadFramed = Framed<OwnedReadHalf, LengthDelimitedCodec>;

type PeerRegistry = [Option<SocketFramed>; 16];

static ADDRESS: &str = "127.0.0.1:8070";
static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";

/** Standard transaction struct that peers append to the block log */
#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    pub seq_counter: u32, pub view_number: u32, pub client_key: Bytes, 
    pub key: Bytes, pub value: Bytes, pub unsigned_msg: Bytes, pub signed_msg: Bytes
}

/** Data that the client sends to a peer when requesting a mutation */
#[derive(Serialize, Deserialize)]
pub struct ClientTransaction<'a> {
    #[serde(borrow)] pub key: &'a [u8], #[serde(borrow)] pub value: &'a [u8], 
    #[serde(borrow)] pub pubkey: &'a [u8], #[serde(borrow)] pub unsigned_tx: &'a [u8],
    #[serde(borrow)] pub signed_tx: &'a [u8]
}

/** Struct that packages required tools for consensus 
 (view number, sequence counter, registry) into single struct */
pub struct ConsensusToolsStruct {
    pub sequence_counter: u32, pub view_number: u32, pub registry: PeerRegistry,
    pub address_list: [SocketAddr; 16], pub len: usize, pub ids: [u8; 16], pub next_id: u8,
    pub active_ids: ArrayVec<u8, 16>, pub active_indexes: ArrayVec<usize, 16>, pub pubkeys: ArrayVec<[u8; 32], 16>,
}

// @impl methods to handle adding, deleting, and looking up sockets
impl ConsensusToolsStruct {

    // use simd to check the entire array at once for the target id
    #[inline(always)]
    pub fn find_index(&self, target_id: u8) -> Option<usize> {
        #[cfg(target_arch = "aarch64")]
        unsafe {
            use std::arch::aarch64::*;
            use std::mem::transmute;

            // 1) load self.ids array into 128-bit vector register, get back a reg ptr
            let ids_reg = vld1q_u8(self.ids.as_ptr());

            // 2) duplicate the target id 16 times and load the array into another 128-bit reg
            let target_id_reg = vdupq_n_u8(target_id);

            // 3) compare the two registers - 0xFF byte on match, 0x00 byte on mismatch
            let cmp = vceqq_u8(ids_reg, target_id_reg);

            // 4) transmute result so we can work with it as an integer
            let mask: u128 = transmute(cmp);

            // if the entire thing is zero there's no match
            if mask != 0 {
                // trailing_zeros() counts the number of bits until the matching byte
                // dividing by 8 gives us the byte index of the matching byte
                Some((mask.trailing_zeros() / 8) as usize)
            } else { None }
        }

        #[cfg(not(target_arch = "aarch64"))]
        { self.ids.iter().position(|&id| id == target_id) }
    }


    /// adds a new socket to the registry
    pub fn insert_socket(&mut self, socket: SocketFramed, addr: SocketAddr, pubkey: [u8; 32], id: u8) -> Result<u8, SocketFramed> {
        // if registry set is full, error out
        if self.len >= 16 { return Err(socket); }

        // Find first empty index
        let idx = self.registry.iter().position(|s| s.is_none()).unwrap();

        // the ids array tells us what index in the registry array a specific socket is
        // so we assign the value of the id array at that index to be the new id
        self.ids[idx] = id;

        // assign the socket to said index, tying the socket, the id, and the index together
        self.registry[idx] = Some(socket);

        // add the id, index, and pubkey to their respective lists
        self.active_ids.push(id);
        self.active_indexes.push(idx);
        self.pubkeys.push(pubkey);

        // increment the registry length and next-id parameter
        self.next_id += 1; self.len += 1; 

        // insert the socket
        self.address_list[idx] = addr; Ok(id)
    }

    /// function to remove a socket from the registry
    pub fn remove(&mut self, target_id: u8) -> Option<(SocketFramed, u8)> {
        // find the socket registry array index
        let index = self.find_index(target_id)?;

        // find the active id array index
        let Some(id_idx) = self.active_indexes.iter().position(|&id| id == target_id as usize) 
            else { eprintln!("Target ID to remove does not exist in list"); return None };

        // remove the socket's id and index from each active array
        self.active_ids.remove(id_idx);
        self.active_indexes.remove(id_idx);

        // remove the socket from the registry and replace it with none
        let socket = self.registry[index].take()?;

        // reset the id decrement the registry length
        self.ids[index] = u8::MAX; self.len -= 1; 

        self.address_list[index] = DEFAULT_ADDR;
        
        Some((socket, target_id))
    }

    // O(1) (due to vectorization) look-up to any given socket via its id
    pub fn get_socket_by_id(&mut self, id: u8) -> Option<&mut SocketFramed> {
        let index = self.find_index(id)?;
        self.registry[index].as_mut()
    }
}

pub type ConsensusTools = ConsensusToolsStruct;

/// @enum The main enum by which all methods - channel and peer through sockets - communicate with any consensus engine actor
/// * @branch ConsensusRequest: requesting the actor to start consensus
///     * @field transaction: The transaction to be commited to the blockchain
/// ---
/// * @branch PeerConsensusRequest: the same thing as consensus request, just for peer, 
/// so that sending a request over the channel doesn't trigger double consensus
///     * @field transaction: The transaction to be commited to the blockchain
/// ---
/// * @branch PeerVote: a vote (cert) from a peer to be sent over the network to a reader task
///     * @field vote type: the stage the peer is voting for (prepare, commit cert)
///     * @field signed_msg: the vote type name signed with the peer's private key 
///         * (since the vote type is signed no need for unsigned_msg)
///     * @field payload: for 
#[derive(Serialize, Deserialize, Debug)]
pub enum ActorRequest {
    ConsensusRequest { transaction: Transaction }, 
    PeerConsensusRequest { transaction: Transaction },
    PeerVote { vote_type: Bytes, signed_msg: Bytes, pubkey: [u8; 32], payload: Bytes }
}

pub struct RegistrationRequest { pub socket: OwnedWriteHalf, pub addr: SocketAddr, pub pubkey: [u8; 32], pub node_id: u8 }

/// Structure that the leader node uses to communicate with the bootnode
#[derive(Serialize, Deserialize)]
pub struct RegistryUpdate<'a> { pub addrs: [SocketAddr; 16], pub ids: &'a [u8], pub pubkeys: ArrayVec<[u8; 32], 16> }

pub static DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

/// @function starts the leader node server. function that handles requests from peer nodes
/// the consensus actor and the netowrk state actor are both started in this fn as long-running tasks
/// * @param network_runtime - the tokio runtime handle for a reader task. can and is cloned per reader
pub async fn start_server() -> io::Result<()> {
    let address_list: [SocketAddr; 16] = [DEFAULT_ADDR; 16]; let ids: [u8; 16] = [u8::MAX; 16];

    let active_ids: ArrayVec<u8, 16> = ArrayVec::new();
    let active_indexes: ArrayVec<usize, 16> = ArrayVec::new();
    let pubkeys: ArrayVec<[u8; 32], 16> = ArrayVec::new();

    let listener = TcpListener::bind(ADDRESS).await?;
    let tools = ConsensusToolsStruct { sequence_counter: 0, view_number: 0, 
        registry: [const { None }; 16], address_list, ids,
        active_ids, active_indexes, len: 0, next_id: 0, pubkeys };

    println!("Server started at {}", ADDRESS);

    // create the manager sender and receiver queue ends
    let (transaction_sender, mut transaction_receiver) = mpsc::channel::<Transaction>(32);

    // primary method of communicating with the consensus actor
    // @returns peer_tx: used by the reader_task so peers can communicate with actor task
    let (peer_tx, peer_receiver) = mpsc::channel::<ActorRequest>(256);

    // channel sender and receiver to add a new peer to the registry
    let (registration_tx, registration_rx) = 
        mpsc::channel::<RegistrationRequest>(32);

    // Start the manager task that controls state and the transaction log
    let _transaction_manager = tokio::task::spawn(async move {
        let mut blockchain_state: HashMap<Bytes, Bytes> = HashMap::new();
        let mut transaction_log: Vec<Transaction> = Vec::with_capacity(1024);

        while let Some(transaction) = transaction_receiver.recv().await {
            blockchain_state.insert(transaction.key.clone(), transaction.value.clone());
            transaction_log.push(transaction);
        }
    });

    let commit_sender = transaction_sender.clone();

    // get a handle to the current runtime so there aren't two reactors competing for I/O control
    let handle = Handle::current();

    // Start the consensus actor thread
    let _consensus_task: JoinHandle<io::Result<()>> = std::thread::spawn(move || {
        // pin the leader node thread to perf core
        let applied = io_err(gdt_cpus::set_thread_priority(Highest))?;
        if applied.effective() != Highest { eprintln!("Failed to pin leader to perf cluster"); }

        // start the task on the thread
        handle.block_on( async move {
            let local = tokio::task::LocalSet::new();

            // run the actor on the local set and then start/await on it
            local.spawn_local(leader_consensus_actor(commit_sender, tools,
                peer_receiver, registration_rx)); local.await;
                
        }); Ok(())
    });

    // create a reusable timout timer
    let sleep = tokio::time::sleep(Duration::from_millis(500));

    tokio::pin!(sleep);

    // create a buffer to store the result from the select
    let mut network_buffer: Option<Result<BytesMut, io::Error>>;
    while let Ok((socket, _)) = listener.accept().await {
        let (read_socket, write_socket) = socket.into_split();

        // prepare the reader socket for reading
        let mut read_framed = make_framed(read_socket, 512);

        // start the timer
        reset_timer(&mut sleep, 750);

        // select between the connected client responding and a timeout
        tokio::select! { value = read_framed.next() => { network_buffer = value } 
            _ = &mut sleep => { eprintln!("from leader - client didn't respond in time, dropping"); continue; } } 

        // on connection, the very first thing the peer should do is send the leader its pubkey
        if let Some(Ok(packet)) = network_buffer {
            // error handling is inside the function. Skip this connection if the peer sends a bad packet
            let Ok(pubkey_packet) = deserialize_packet::<ConnectionPacket>(&packet) 
                else { eprintln!("peer sent a bad packet, dropping connection"); continue; };
            
            // if client sends a packet that isn't listed with 'peer-pubkey', bad packet
            if pubkey_packet.node_type != Bytes::from_static(b"peer-pubkey") 
                { eprintln!("peer didn't send their pubkey, dropping connection"); continue; }

            // pass the read half and the request sender to a new reader task
            let _peer_tx = peer_tx.clone();

            // get the peers' pubkey as a fixed size array
            let Some(stack_pubkey) = pubkey_packet.payload[0..32].as_array::<32>()
                else { eprintln!("failed to get peer's pubkey as fixed array"); continue };

            // spawn the reader task for the peer
            tokio::task::spawn(reader_task(read_framed, _peer_tx, *stack_pubkey));

            // send a registration request to the consensus engine receiver to register the write half
            let addr: SocketAddr = io_err(std::str::from_utf8(&pubkey_packet.address).unwrap().parse())?;
            let request = RegistrationRequest { socket: write_socket, addr, pubkey: *stack_pubkey, node_id: pubkey_packet.node_id };

            io_err(registration_tx.send(request).await)?;

            println!("Peer address {:?} registered", std::str::from_utf8(&pubkey_packet.address));
        }
    }

    Ok(())
}

/// @function consensus actor: actor handler for the consensus engine
///  * @param commit_sender: Sender end of channel engine uses 
///    to send completed transactions back to state manager 
///  * @param tools: critical tools (seq_counter, view_number, peer_socket_registry)
///    required for consensus
///  * @param peer_receiver: entryway for peer sockets to communicate with consensus
pub async fn leader_consensus_actor(
    mut commit_sender: mpsc::Sender<Transaction>, mut tools: ConsensusTools, 
    mut peer_receiver: mpsc::Receiver<ActorRequest>, 
    mut registration_rx: mpsc::Receiver<RegistrationRequest>) {

    // you gotta split the nodes into files/cmdline args 
    let Ok(bootnode_socket) = connect_with_retry(BOOTNODE_ADDRESS, 5).await 
        else { panic!("Cannot connect to bootnode"); };

    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    let mut bootnode_framed = Framed::new(bootnode_socket, socket_codec.clone());
    let mut serialize_pool = BytesMut::with_capacity(1024);

    // generate the leader's pubkey and signing key
    let mut csprng = OsRng;
    let mut keypair: SigningKey = SigningKey::generate(&mut csprng);

    let pubkey = keypair.verifying_key().to_bytes();

    // send the bootnode the leader ack handshake packet (with null ID to be ignored, the bootnode will send the proper one)
    if let Err(_) = send_connection_packet("leader", ADDRESS, Some(&pubkey),
         0, &mut bootnode_framed, &mut serialize_pool).await 
            { eprintln!("Could not register leader address with bootnode") }
    
    // TODO - apparently this value also is length 85 instead of...1.
    //  add a sleep between sending the leader ID and whatever else you send it
    if let Some(Ok(mut value)) = bootnode_framed.next().await {
        // we want to make sure that our ID timeline is in sync with the bootnode
        tools.next_id = value.get_u8();
    }

    loop { tokio::select! {
        biased;

        // wait for transaction request
        Some(request) = peer_receiver.recv() => {
            match request {
                // client/peer node request to commit a transaction
                ActorRequest::ConsensusRequest { transaction } => {
                    // execute consensus
                    match consensus_engine(transaction, &mut tools, &mut peer_receiver, 
                        &mut commit_sender, &mut serialize_pool, &mut keypair).await {
                            Ok(_) => { println!("Consensus has been reached"); },
                            Err(e) => { eprintln!("Consensus from leader failed with error: {}", e); }
                        }
                },
                _ => eprintln!("Invalid message from peer - leader, of type {:?}", request)
            }
        }

        Some(request) = registration_rx.recv() => {
            // onboarding task request to add a new peer to the registry
            let socket_framed = Framed::new(request.socket, socket_codec.clone());

            // add the socket and address to the registry
            if let Err(_) = tools.insert_socket(socket_framed, request.addr, request.pubkey, request.node_id) {
                eprintln!("Could not insert socket for registration"); continue;
            }

            // construct the registry update packet
            let updated_registry = RegistryUpdate { addrs: tools.address_list, pubkeys: tools.pubkeys.clone(), ids: tools.active_ids.as_ref() };

            // serialize the addresses into the heap buffer
            let addr_packet = serialize_into(&mut serialize_pool, &updated_registry);

            // send the entire list as a serialized Vec<UpdatedRegistry>
            let _ = bootnode_framed.send(addr_packet.freeze()).await;
        }
    } }
}

/// @function passed to the multithreaded runtime. 
/// Spanwed as a task-per-connection architecture to route each connection's 
/// network request to the consensus actor
/// * @param read_socket: the read half of the connection's socket, 
/// to receive that connection's requests
/// * @param peer_tx: sender channel to send transaction requests to the consensus actor
pub async fn reader_task(mut read_framed: ReadFramed, peer_tx: mpsc::Sender<ActorRequest>, pubkey: [u8; 32]) -> io::Result<()> {
    // wait for messages from peer
    // framed next for the socket codec - framing entire messages
    while let Some(Ok(read_buffer)) = read_framed.next().await {

        // Step 1 - deserialize client request (change for proper error handling)
        match deserialize_packet::<ActorRequest>(&read_buffer) {

            // Step 2 - send either transaction payload or vote to the consensus actor
            // (all receivers expect payload type ActorRequest
            // so just pass along the deserialized request)
            Ok(request) => { match request {

                // If it's a vote or cert from a peer, verify it before sending to quorum counter
                ActorRequest::PeerVote { ref vote_type, ref signed_msg, pubkey: _, payload: _ } => {
                    // Step 1 - create the verifying key from the pubkey bytes
                    let key_bytes: [u8; 32] = io_err(pubkey[..].try_into())?;
                    let verifying_key = io_err(VerifyingKey::from_bytes(&key_bytes))?;

                    // Step 2 - Create the "signature" (signed msg) from bytes
                    let signed_bytes: [u8; 64] = io_err(signed_msg[..].try_into())?;
                    let signature = Signature::from_bytes(&signed_bytes);

                    // Step 3 - Verify client message/vote and send to engine if valid
                    match verifying_key.verify(&vote_type[..], &signature) {
                        Ok(_) => { io_err(peer_tx.send(request).await)?; },
                        Err(_) => { eprintln!("failed cryptographic verification"); continue;  }
                    }
                },

                // but if it's a client adding a new transaction, send directly to engine
                ActorRequest::ConsensusRequest { transaction: _ } => 
                    { io_err(peer_tx.send(request).await)?; }
                    
            _ => { eprintln!("Reader task got a bad enum variant") },
            } },
            Err(_) => { eprintln!("Deserialization failed"); 
                return Err(io::Error::new(io::ErrorKind::InvalidData, "failed")) }
        }
    }

    Ok(())
}

/// @function CPU bound consensus engine function to be ran per consensus
/// * @param transaction: Transaction to be committed to the network/chain
/// * @param tools: Tools (registry, sequence counter, view number) for consensus
/// * @param vote receiver: the receiving end of the channel all peers send their votes through
/// * @param commit sender: the sending end of the channel to send the final transaction back to the state manager
/// * @param leader socket (optional): An option representing the leader socket that only peers use 
/// * @param signing key: the private key that the current node uses to sign their vote certs
pub async fn consensus_engine(
    transaction: Transaction,
    tools: &mut ConsensusTools,
    vote_receiver: &mut mpsc::Receiver<ActorRequest>,
    commit_sender: &mut mpsc::Sender<Transaction>,
    serialization_pool: &mut BytesMut,
    signing_key: &mut SigningKey) -> io::Result<()> {

    // STEP 0 - calculate f (# of faulty nodes in pbft consensus equation)
    // if N = 3f + 1 holds true, where N = nodes, there can be at most (N - 1) / 3 faulty nodes
    let faulty = (( tools.len ) / 3).max(1) as u32;

    // timers in tokio are separate futures of their own when directly awaited on
    // so create a timer that gets recalculated instead of created and dropped over and over
    let sleep = tokio::time::sleep(Duration::from_millis(1500));

    tokio::pin!(sleep);

    // sequence counter FROM THE GIVEN TRANSACTION - UNVERIFIED
    let seq_counter = transaction.seq_counter;
    let tx_request = ActorRequest::ConsensusRequest { transaction };

    // STEP 1: broadcast the proposed transaction to each peer in the network registry

    // create a transaction actor request to send to peers
    let tx_bytes = serialize_into(serialization_pool, &tx_request);
    let tx_payload = tx_bytes.freeze().clone();

    reset_timer(&mut sleep, 1500);

    // loop through registry and send proposal to all peer nodes
    for index in &tools.active_indexes {
        // set a timout - we don't want to hang sending to nonresponsive peers
        send_with_timeout(&mut tools.registry[*index].as_mut().unwrap(), tx_payload.clone(), &mut sleep).await;

        // the timer needs to be recalculated and reset each time
        // looks inefficent but much faster than registering and dropping a future over and over again
        reset_timer(&mut sleep, 1500);
    }

    // STEP 2: hash the key and value to validate the proposal, and
    // wait for a quorum (2f + 1) of PREPARE votes from other peers

    // given node verifying the transaction themselves
    // CRITICAL - The leader needs to verify the transaction *after* it sends the pre-prepare vote
    // so that it doesn't just "verify" and decide a tx isn't valid and not propagate it (byzantine leader)

    // reset the quorum and timer for the PREPARE vote
    let mut quorum_counter = 0; let sequence_counters = (tools.sequence_counter, seq_counter);

    // verify the transaction - if valid, add this node's count to the quorum
    // if not, don't, but still continue with consensus
    if let ActorRequest::ConsensusRequest { transaction: ref tx } = tx_request {
        // verify the *client* transaction to see if it is valid
        match verify_transaction(&tx.client_key, &tx.unsigned_msg, &tx.signed_msg).await {
            Ok(()) => {
                // add self to quorum counter 
                quorum_counter += 1; 

                // once the transaction has been verified craft the PREPARE vote
                let vote_payload = create_vote("PREPARE", &signing_key, serialization_pool);

                reset_timer(&mut sleep, 1500);

                // loop through registry (active-ids list) and send PREPARE vote to all peer nodes
                for index in &tools.active_indexes {
                    send_with_timeout(&mut tools.registry[*index].as_mut().unwrap(), vote_payload.clone(), &mut sleep).await;

                    reset_timer(&mut sleep, 1500);
                }
            },
            Err(_) => { eprintln!("Transaction verification failed"); }
        }
    } reset_timer(&mut sleep, 3000);

    tokio::select! {
        _ = wait_for_quorum(vote_receiver, sequence_counters, &mut quorum_counter,
                tools.len, faulty, b"PREPARE") => { println!("Prepare quorum has been reached"); }

        _ = &mut sleep => { return return_err("Time limit exceeded, prepare verification failed"); }
    }
    

    // reset the quorum and timer for the COMMIT vote
    quorum_counter = 1; reset_timer(&mut sleep, 1500);

    // STEP 3: Once quorum for prepare has been reached, prepare a commit certificate
    let vote_payload = create_vote("COMMIT", &signing_key, serialization_pool);

    // Broadcast commit message and wait again for commit quorum
    for index in &tools.active_indexes {
        send_with_timeout(&mut tools.registry[*index].as_mut().unwrap(), vote_payload.clone(), &mut sleep).await;

        reset_timer(&mut sleep, 1500);
    }

    tokio::select! {
        _ = wait_for_quorum(vote_receiver, sequence_counters, &mut quorum_counter,
                tools.len, faulty, b"COMMIT") => { println!("Commit quorum has been reached"); }

        _ = &mut sleep => 
            { return return_err("Time limit exceeded, prepare verification failed"); }
    }

    // consensus reached, transaction verified - commit!
    // if state transition fails (split brain) - fail fast and resync after restarting
    if let ActorRequest::ConsensusRequest { transaction } = tx_request {
        if let Err(_) = commit_sender.send(transaction).await {
            eprintln!("Could not commit state transition");
            panic!("Node {ADDRESS} crashed due to state transition failure"); 
        }
    }

    // Update the sequence counter for the next transaction
    // (for peer verification of order and leader identity)
    tools.sequence_counter += 1;

    Ok(())

}