use std::{collections::HashMap, io::self, thread::JoinHandle, time::Duration};

use bytes::{Bytes, BytesMut};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;

use futures::{SinkExt, StreamExt};
use gdt_cpus::ThreadPriority::Highest;
use tokio::{net::{TcpListener, tcp::{OwnedReadHalf, OwnedWriteHalf}, }, runtime::Handle, sync::mpsc};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use serde::{Deserialize, Serialize};

use crate::protocol::{
    infra_peer::ConnectionPacket, utils::utils::{connect_with_retry, deserialize_packet, io_err, make_framed, reset_timer, return_err, send_connection_packet, send_with_timeout, serialize_into, verify_transaction, wait_for_quorum},
};

type SocketFramed = Framed<OwnedWriteHalf, LengthDelimitedCodec>;
type ReadFramed = Framed<OwnedReadHalf, LengthDelimitedCodec>;

type PeerRegistry = Vec<SocketFramed>;

static ADDRESS: &str = "127.0.0.1:8070";
static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";

/** Standard transaction struct that peers append to the block log */
#[derive(Serialize, Deserialize)]
pub struct Transaction {
    pub seq_counter: u32, pub view_number: u32, pub client_key: Bytes, 
    pub key: Bytes, pub value: Bytes, pub unsigned_msg: Bytes, pub signed_msg: Bytes
}

/** Data that the client sends to a peer when requesting a mutation */
#[derive(Serialize, Deserialize)]
pub struct ClientTransaction<'a> {
    #[serde(borrow)] pub key: &'a [u8], #[serde(borrow)] pub value: &'a [u8], 
    #[serde(borrow)] pub pubkey: &'a [u8], #[serde(borrow)] pub signed_tx: &'a [u8]
}

/** Struct that packages required tools for consensus 
 (view number, sequence counter, registry) into one struct
 that can all be unlocked and accessed with a single mutex unlock
*/
pub struct ConsensusToolsStruct {
    pub sequence_counter: u32, pub view_number: u32, pub registry: PeerRegistry,
    pub address_list: Vec<std::net::SocketAddr>
}

pub type ConsensusTools = ConsensusToolsStruct;

/// @enum The main enum by which all methods - channel and peer through sockets - communicate with any consensus engine actor
/// * @branch ConsensusReqeust: requesting the actor to start consensus
///     * @field transaction: The transaction to be commited to the blockchain
/// * @branch PeerVote: a vote (cert) from a peer to be sent over the network to a reader task
///     * @field vote type: the stage the peer is voting for (prepare, commit cert)
///     * @field signed_msg: the vote type name signed with the peer's private key 
///         * (since the vote type is signed no need for unsigned_msg)
#[derive(Serialize, Deserialize)]
pub enum ActorRequest {
    ConsensusRequest { transaction: Transaction }, 
    PeerVote { vote_type: Bytes, signed_msg: Bytes }
}

pub struct RegistrationRequest { pub socket: OwnedWriteHalf, pub addr: std::net::SocketAddr }

/// @function starts the leader node server. function that handles requests from peer nodes
/// the consensus actor and the netowrk state actor are both started in this fn as long running tasks
/// * @param network_runtime - the tokio runtime handle for a reader task. can and is cloned per reader
pub async fn start_server() -> io::Result<()> {
    let registry: PeerRegistry = Vec::with_capacity(10);
    let listener = TcpListener::bind(ADDRESS).await.unwrap();
    let tools = ConsensusToolsStruct { 
        sequence_counter: 0, view_number: 0, registry, 
        address_list: Vec::with_capacity(10) };

    println!("Server started at {}", ADDRESS);

    // create the manager sender and receiver queue ends
    let (transaction_sender, mut transaction_receiver) = mpsc::channel::<Transaction>(32);

    // primary method of communicating with the hot loop
    // @returns peer_tx: used by the reader_task so they can peers can communicate with hot loop task
    let (peer_tx, peer_receiver) = mpsc::channel::<ActorRequest>(512);

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

    let handle = Handle::current();

    // Start the consensus actor thread
    let _consensus_task: JoinHandle<io::Result<()>> = std::thread::spawn(move || {
        // pin the leader node thread to perf core
        let applied = io_err(gdt_cpus::set_thread_priority(Highest))?;
        if applied.effective() != Highest { eprintln!("Failed to pin leader to perf cluster"); }

        // start the task on the thread
        handle.block_on( async move {
            let local = tokio::task::LocalSet::new();

            local.spawn_local(leader_consensus_actor(
                commit_sender, tools, peer_receiver, registration_rx)); 
                
            local.await; }); Ok(())
    });

    // create a reusable timout timer
    let sleep = tokio::time::sleep(Duration::from_millis(500));

    tokio::pin!(sleep);

    // create a buffer to store the result from the select
    let mut network_buffer: Option<Result<BytesMut, std::io::Error>>;
    while let Ok((socket, addr)) = listener.accept().await {
        println!("Peer node {addr} connected to primary");

        let (read_socket, write_socket) = socket.into_split();

        // prepare the reader socket for reading
        let mut read_framed = make_framed(read_socket, 512);

        // start the timer
        reset_timer(&mut sleep, 2000);

        // select between the connceted client responding and a timeout
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
            tokio::task::spawn(reader_task(read_framed, _peer_tx, Box::from(pubkey_packet.payload))); 

            // send a registration request to the consensus enigne receiver to register the write half
            let request = RegistrationRequest { socket: write_socket, addr };

            io_err(registration_tx.send(request).await)?;

            println!("Peer address {:?} registered", std::str::from_utf8(&pubkey_packet.address));
        }
    }

    Ok(())
}

/// @function consensus actor: actor handler for the consensus engine hot loop
///  * @param commit_sender: Sender end of channel engine uses 
///    to send completed transactions back to state manager 
///  * @param tools: criical tools (seq_counter, view_number, peer_socket_registry)
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

    // send the bootnode the leader verification packet
    send_connection_packet("leader", ADDRESS, Some(&pubkey), 
        &mut bootnode_framed, &mut serialize_pool).await;


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
                            Err(e) => { eprintln!("Consensus failed with error: {}", e); }
                        }
                },
                _ => eprintln!("Invalid message from peer")
            }
        }

        Some(request) = registration_rx.recv() => {
            // onboarding task request to add a new peer to the registry
            let socket_framed = Framed::new(request.socket, socket_codec.clone());
            tools.registry.push(socket_framed);

            // add the address to the address list
            tools.address_list.push(request.addr);

            // serialize the addresses into the heap buffer
            let addr_packet = serialize_into(&mut serialize_pool, &tools.address_list);

            // send the entire list as a serialized Vec<SocketAddr>
            let _ = bootnode_framed.send(addr_packet.freeze()).await;
        }
    } }
}

/// @function passed to the multithreaded runtime. 
/// Spanwed as a task-per-connection architecture to route each connection's 
/// network request to the consensus/hot loop actor
/// * @param read_socket: the read half of the connection's socket, 
/// to receive that connection's requests
/// * @param peer_tx: sender channel to send transaction requests to the consensus actor
pub async fn reader_task(mut read_framed: ReadFramed, peer_tx: mpsc::Sender<ActorRequest>, pubkey: Box<[u8]>) -> io::Result<()> {
    // wait for messages from peer
    // framed next for the socket codec - framing entire messages
    while let Some(Ok(read_buffer)) = read_framed.next().await {

        // Step 1 - deserailize client request (change for proper error handling)
        match deserialize_packet::<ActorRequest>(&read_buffer) {

            // Step 2 - send either transaction payload or vote to the consensus actor
            // (all recievers expect payload type ActorRequest 
            // so just pass along the deserialized request)
            Ok(request) => { match request {

                // If it's a vote or cert from a peer, verify it before sending to quorum counter
                ActorRequest::PeerVote { ref vote_type, ref signed_msg } => {
                    // Step 1 - create the verifyng key from the pubkey bytes
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
                    { io_err(peer_tx.send(request).await)?; },
            } },
            Err(_) => { eprintln!("Deserialization failed"); 
                return Err(io::Error::new(io::ErrorKind::InvalidData, "failed")) }
        }
    }

    Ok(())
}

/// @function CPU bound consensus engine function to be ran per consensus
/// * @param transaction: Transaction to be committed to the network/chain
/// * @param tools: Tools (registery, sequence counter, view number) for consensus
/// * @param vote receiver: the recieving end of the channel all peers send their votes through
/// * @param commit sender: the sending end of the channel to send the final transaction back to the state manager
/// * @param leader socket (optional): An option representing the leader socket that only peers use 
/// * @param signing key: the private key that the current node uses to sign their vote certs
pub async fn consensus_engine(
    transaction: Transaction, 
    tools: &mut ConsensusTools,
    vote_reciever: &mut mpsc::Receiver<ActorRequest>, 
    commit_sender: &mut mpsc::Sender<Transaction>, 
    serialization_pool: &mut BytesMut,
    signing_key: &mut SigningKey) -> io::Result<()> {

    // STEP 0 - calculate f (# of faulty nodes in pbft consensus equation)
    // if N = 3f + 1 holds true, where N = nodes, there can be at most (N - 1) / 3 faulty nodes
    let faulty = (( tools.registry.len() - 1 ) / 3).max(1) as u32;

    // timers in tokio are seperate futures of their own when directly awaited on
    // so create a timer that gets recalculated instead of created and dropped over and over
    let sleep = tokio::time::sleep(Duration::from_millis(1500));

    tokio::pin!(sleep);

    let seq_counter = transaction.seq_counter;
    let tx_request = ActorRequest::ConsensusRequest { transaction };

    // STEP 1: broadcast the proposed transaction to each peer in the network registry

    // create a transaction actor request to send to peers
    let tx_bytes = serialize_into(serialization_pool, &tx_request);
    let tx_payload = tx_bytes.freeze().clone(); 

    reset_timer(&mut sleep, 1500);

    // loop through registry and send proposal to all peer nodes
    for socket_frame in &mut tools.registry {
        // set a timout - we don't want to hang sending to nonresponsive peers
        tokio::select! {
            _ = socket_frame.send(tx_payload.clone()) => {}
            _ = &mut sleep => { println!("Peer hanged, skipping"); }
        }

        // the timer needs to be recalculated and reset each time
        // looks inefficent but much faster than registering and dropping a future over and over again
        reset_timer(&mut sleep, 1500);
    }

    // STEP 2: hash the key and value to validate the proposal, and
    // wait for a quorum (2f + 1) of PREPARE votes from other peers

    // given node verifying the transaction themselves
    // CRITICAL - The leader needs to verify the transaction *after* it sends the pre-prepare vote
    // so that it doesn't just "verify" and decide a tx isn't valid and not propagate it (byzantine leader)

    if let ActorRequest::ConsensusRequest { transaction: ref tx } = tx_request {
        // verify the *client* transaction to see if it is valid
        verify_transaction(&tx.client_key, &tx.unsigned_msg, &tx.signed_msg).await?;
    }

    // once the transaction has been verified craft the PREPARE vote
    let signed_prepare_vote = signing_key.sign(b"PREPARE").to_bytes();

    let prepare_vote = ActorRequest::PeerVote { 
        vote_type: Bytes::from_static(b"PREPARE"), 
        signed_msg: Bytes::copy_from_slice(&signed_prepare_vote)
    };

    let vote_bytes = serialize_into(serialization_pool, &prepare_vote);
    let vote_payload = vote_bytes.freeze().clone();

    reset_timer(&mut sleep, 1500);

    // loop through registry and send PREPARE vote to all peer nodes
    for socket_frame in &mut tools.registry {
        send_with_timeout(socket_frame, vote_payload.clone(), &mut sleep).await;

        reset_timer(&mut sleep, 1500);
    }

    // reset the quorum and timer for the COMMIT vote
    let mut quorum_counter = 0; reset_timer(&mut sleep, 3000);
    let sequence_counters = (tools.sequence_counter, seq_counter);

    tokio::select! {
        _ = wait_for_quorum(vote_reciever, sequence_counters, &mut quorum_counter, faulty, b"PREPARE") => { println!("Prepare quorum has been reached"); }

        _ = &mut sleep => { return return_err("Time limit exceeded, prepare verification failed"); }
    }
    

    // clean up the counter and the voter queue in preparation for recieving the commmit votes
    quorum_counter = 0; while let Ok(_) = vote_reciever.try_recv() { /* clear out any PREPARE votes */ }
    reset_timer(&mut sleep, 1500);

    // STEP 3: Once quorum for prepare has been reached, prepare a commit certificate
    let signed_commit_vote = signing_key.sign(b"COMMIT").to_bytes();

    let commit_vote = ActorRequest::PeerVote { 
        vote_type: Bytes::from_static(b"COMMIT"), 
        signed_msg: Bytes::copy_from_slice(&signed_commit_vote)
    };

    let vote_bytes = serialize_into(serialization_pool, &commit_vote);
    let vote_payload = vote_bytes.freeze().clone();

    // Broadcast commit message and wait again for commit quorum
    for socket_frame in &mut tools.registry {
        // set a timout - we don't want to hang sending to nonresponsive peers
        send_with_timeout(socket_frame, vote_payload.clone(), &mut sleep).await;

        // reset the deadline for the next loop
        reset_timer(&mut sleep, 1500);
    }

    tokio::select! {
        _ = wait_for_quorum(vote_reciever, sequence_counters, &mut quorum_counter, faulty, b"COMMIT") => { println!("Commit quorum has been reached"); }

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
