use core::panic;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Relaxed;
use std::thread::JoinHandle;
use std::time::Duration;
use arrayvec::ArrayVec;
use gdt_cpus::ThreadPriority::Highest;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::{io, net::{TcpListener, TcpStream}};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use futures::{SinkExt, StreamExt};

use bytes::{Buf, Bytes, BytesMut};

use serde::{Serialize, Deserialize};

use crate::infra_main::{ConsensusTools, ConsensusToolsStruct, DEFAULT_ADDR, RegistrationRequest, reader_task};
use crate::utils::utils::*;
use crate::{
    infra_main::{ActorRequest, Transaction},
    utils::utils::{deserialize_packet, make_framed, verify_transaction}
};

use rand::rngs::OsRng;
use ed25519_dalek::SigningKey;

static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";

static LEADER_NODE_ID: AtomicU8 = AtomicU8::new(0);

/// Zero copy transport format that contains a node's type, its address, and its pubkey for ID, and the actual node_id
#[derive(Serialize, Deserialize)]
pub struct ConnectionPacket<'a> {
    #[serde(borrow, with = "serde_bytes")] pub address: &'a [u8],
    #[serde(borrow, with = "serde_bytes")] pub payload: &'a [u8],
    #[serde(borrow, with = "serde_bytes")] pub node_type: &'a [u8], 
    pub node_id: u8
}

#[derive(Deserialize)]
pub struct RefusalPacket<'a> { #[serde(borrow)] pub msg: &'a [u8] }

pub type SocketFramed = Framed<OwnedWriteHalf, LengthDelimitedCodec>;

pub async fn discover_network(port: &str) -> io::Result<()> {
    let address_list: [SocketAddr; 16] = [DEFAULT_ADDR; 16]; let ids: [u8; 16] = [u8::MAX; 16];

    let active_ids: ArrayVec<u8, 16> = ArrayVec::new();
    let active_indexes: ArrayVec<usize, 16> = ArrayVec::new();
    let pubkeys: ArrayVec<[u8; 32], 16> = ArrayVec::new();

    // create the struct for tools for consensus
    let mut tools = ConsensusToolsStruct { sequence_counter: 0, view_number: 0, 
        registry: [const { None }; 16], address_list, active_ids,
        ids, active_indexes, pubkeys, len: 0, next_id: 0 };

    // leader socket that may or may not exist
    let mut leader_socket = false;

    // grab the port from the cmdline
    // let Some(port) = std::env::args().nth(1) else { panic!("invalid port") };
    let self_address = port; let mut self_id: u8 = 255;

    // general use serialize pool
    let mut serialize_pool = BytesMut::with_capacity(1024);

    // create a private-public keypair
    let mut csprng = OsRng;
    let keypair: SigningKey = SigningKey::generate(&mut csprng);
    let self_pubkey = keypair.verifying_key().to_bytes();

    // create channel for sending consensus requests
    let (engine_tx, peer_rx) = mpsc::channel::<ActorRequest>(32);

    // the bootnode needs to have connected with the leader before the peer does
    // TODO - these sleeps suck
    // * 1) - Split the three nodes into their own files. Please
    // * 2) - Implement a retry function when connecting to bootnode
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("Starting connection to bootnode...");
    if let Ok(socket) = connect_with_retry(BOOTNODE_ADDRESS, 5).await {
        let mut socket_framed = make_framed(socket, 1024);

        // send a pubkey confirmation packet to the bootnode
        send_connection_packet("peer-pubkey", &self_address, 
            Some(&self_pubkey), 0, &mut socket_framed, &mut serialize_pool).await?;

        // get the given node id from the bootnode
        match socket_framed.next().await {
            Some(Ok(mut value)) => {
                if value.is_empty() { println!("ok dude why is this empty"); return return_err(""); }
                self_id = value.get_u8();
            },

            Some(Err(e)) => { println!("Node ID fetching error: {}", e); }

            None => { println!("socket closed"); }
        }

        if let Some(Ok(value)) = socket_framed.next().await {
            if let Ok(list) = deserialize_packet::<Vec<ConnectionPacket>>(&value) {
                for packet in list {
                    // connect to peer
                    let Ok(peer_addr) = std::str::from_utf8(&packet.address) else { continue; };
                    match TcpStream::connect(peer_addr).await {

                        // we need to spawn reader tasks for each of the sockets the bootnode gave us
                        Ok(stream) => {

                            // 1) split the sockets into read and write and create frameds for them
                            let (reader, writer) = stream.into_split();

                            let read_framed = make_framed(reader, 512);
                            let mut write_framed = make_write_framed(writer, 512);

                            // named "peer-pubkey", even though this is possibly the leader pubkey, to distinguish 
                            // from "self_pubkey", or even worse, the highly descriptive name of just "pubkey"
                            let Some(peer_pubkey) = packet.payload.as_array::<32>()
                                else { eprintln!("failed to get it"); continue; };

                            // 2) send a pubkey confirmation packet to peer
                            if let Err(_) = send_connection_packet("peer-pubkey", &self_address,
                                Some(&self_pubkey), self_id, &mut write_framed, &mut serialize_pool).await {
                                eprintln!("Peer failed to connect to another peer"); todo!()

                                // TODO: Implement retry logic
                            }

                            let addr: SocketAddr = io_err(str::from_utf8(packet.address).unwrap().parse())?;

                            // 3) add the writer into the registry
                            if let Err(_) = tools.insert_socket(write_framed, addr, *peer_pubkey, packet.node_id) {
                                eprintln!("Peer failed to insert socket for new peer"); }

                            // 3b) - if it's the leader node, then store their ID
                            if packet.node_type == b"leader" {
                                LEADER_NODE_ID.store(packet.node_id, Relaxed);
                                leader_socket = true;
                            }

                            // 4) clone the consensus engine sender 
                            let peer_tx = engine_tx.clone();

                            // 4) then, for each reader, spawn a new reader task
                            tokio::task::spawn(reader_task(read_framed, peer_tx, *peer_pubkey));
                        },
                        Err(_) => { eprintln!("Unable to connect to peer w/ addr: {}", self_address); }
                    }
                }
            } else if let Ok(refusal_packet) = deserialize_packet::<RefusalPacket>(&value) {
                let message = std::str::from_utf8(&refusal_packet.msg).unwrap();
                eprintln!("Bootnode refused to send address list with given message: {}", message);
            } else { panic!("Cannot connect to the network: deserialization of peer list from bootnode failed"); }
        }
    } 

    if !leader_socket { panic!("No leader node found from peer w/ ID {self_id}") }

    if let Err(e) = start_server(tools, self_id, keypair, self_address, engine_tx, peer_rx).await
        { eprintln!("Peer node server exited with Error: {}", e); }

    println!("Peer node server closed"); Ok(())
}

async fn start_server(
    tools: ConsensusToolsStruct, self_id: u8,
    signing_key: SigningKey, address: &str,
    engine_tx: mpsc::Sender<ActorRequest>,
    peer_rx: mpsc::Receiver<ActorRequest>) -> io::Result<()> {

    // let Some(port) = std::env::args().nth(1) else { panic!("invalid port") };

    if let Ok(listener) = TcpListener::bind(address).await {
        // create the manager sender and reciever queue ends
        let (commit_sender, mut tx_reciever) = mpsc::channel::<Transaction>(32);

        // Start the manager task that controls state and the transaction log
        let _transaction_manager = tokio::task::spawn(async move {
            let mut blockchain_state: HashMap<Bytes, Bytes> = HashMap::new();
            let mut transaction_log: Vec<Transaction> = Vec::with_capacity(1024);

            while let Some(transaction) = tx_reciever.recv().await {
                blockchain_state.insert(transaction.key.clone(), transaction.value.clone());
                transaction_log.push(transaction);
            }
        });

        // create channel for sending peer joining requests
        let (registration_tx, registration_rx) = mpsc::channel::<RegistrationRequest>(32);

        // we need to spawn reader tasks for all the sockets the bootnode gave us
        // and populate the registry (to be used in tools) with all the writer sockets

        let handle = Handle::current();

        // spawn consensus engine task
        let _consensus_task: JoinHandle<io::Result<()>> = std::thread::spawn(move || {
            // apply high priority to consensus engine thread
            let applied = io_err(gdt_cpus::set_thread_priority(Highest))?;
            if applied.effective() != Highest { eprintln!("Failed to pin peer consensus engine to perf cluster"); }

            // start the task on the thread
            handle.block_on( async move {
                // create the localset, so the task doesn't go into the general queue
                let local = tokio::task::LocalSet::new();

                // run the actor on the localset and start/await on it
                local.spawn_local(peer_consensus_actor(commit_sender, tools, peer_rx,
                   registration_rx, signing_key, self_id)); local.await;
            }); Ok(())
        });

        // whenever a new connection is opened...
        // create a reusable timout timer
        let sleep = tokio::time::sleep(Duration::from_millis(750)); tokio::pin!(sleep); 

        // create a buffer to store the result from the select
        let mut network_buffer: Option<Result<BytesMut, io::Error>>;
        while let Ok((socket, addr)) = listener.accept().await {
            let (read_socket, write_socket) = socket.into_split();

            // prepare the reader socket for reading
            let mut read_framed = make_framed(read_socket, 512);

            // start the timer
            reset_timer(&mut sleep, 750);

            // select between the connected client responding and a timeout
            tokio::select! { value = read_framed.next() => { network_buffer = value } 
                _ = &mut sleep => { eprintln!("from peer - client didn't respond in time, dropping"); continue; } } 

            // on connection, the very first thing the peer should do is send the connected node its pubkey
            if let Some(Ok(packet)) = network_buffer {
                // error handling is inside the function. Skip this connection if the peer sends a bad packet
                let Ok(connection_packet) = deserialize_packet::<ConnectionPacket>(&packet) 
                    else { eprintln!("peer sent a bad packet, dropping connection"); continue; };
                
                // first, decide if the connected machine is client or peer
                match connection_packet.node_type {
                    // on peer connection, take the pubkey (from the payload field) and start reader task
                    b"peer-pubkey" => {
                        // pass the read half and the request sender to a new reader task
                        let _peer_tx = engine_tx.clone();

                        let Some(stack_pubkey) = connection_packet.payload[0..32].as_array::<32>()
                            else { eprintln!("failed to get peer's pubkey as fixed array"); continue };

                        tokio::task::spawn(reader_task(read_framed, _peer_tx, *stack_pubkey));

                        // send a registration request to the consensus engine receiver to register the write half
                        let request = RegistrationRequest { socket: write_socket, addr, pubkey: *stack_pubkey, node_id: connection_packet.node_id };

                        io_err(registration_tx.send(request).await)?;
                    },

                    // on client transaction request, verify and prepare for consensus
                    b"client-transaction" => {
                        // Transaction types are Bytes. ClientTransaction sends &'a [u8]. Not the same type of ptr
                        // so we need to first copy the transaction *once* into a new buffer to stay permanent/reference...
                        let mut client_tx: Bytes = Bytes::copy_from_slice(connection_packet.payload);

                        let slice_out = |buffer: &mut Bytes| -> Bytes {
                            let size = usize::from_le_bytes(buffer.split_to(8)[..8]
                                .try_into().unwrap()); return buffer.split_to(size);
                        };

                        // ...then go in order and slice out/"deserialize" each of the fields one by one
                        let key = slice_out(&mut client_tx);
                        let value = slice_out(&mut client_tx);
                        let client_key = slice_out(&mut client_tx);
                        let unsigned_msg = slice_out(&mut client_tx);
                        let signed_msg = slice_out(&mut client_tx);

                        // we store the sequence counter as '0' here because reading it on this thread can trigger
                        // a race condition, where the sequence counter can get read right before it gets edited in consensus

                        // finally, craft the full tx from each of the fields
                        let request = ActorRequest::PeerConsensusRequest { transaction: Transaction {
                            key, value, client_key, signed_msg, unsigned_msg, seq_counter: 0, view_number: 0
                        }};

                        // Step 3: verify the transaction and send the request to the engine
                        if let ActorRequest::PeerConsensusRequest { transaction: ref tx } = request {
                            // TODO: move into separate function for better error handling
                            verify_transaction(&tx.client_key, &tx.unsigned_msg[..], &tx.signed_msg).await?
                        }

                        // send the request to the engine
                        io_err(engine_tx.send(request).await)?;
                    },

                    _ => { eprintln!("Connection packet with unknown node type"); continue; }
                }
            }
        }
    } else { /* TODO: Implement retry logic */ }

    Ok(())
}


/// @function consensus actor: actor handler for the consensus engine hot loop
///  * @param `commit_sender`: Sender end of channel engine uses 
///    to send completed transactions back to state manager 
///  * @param `tools`: critical tools (seq_counter, view_number, peer_socket_registry)
///    required for consensus
///  * @param `peer_receiver`: entryway for peer sockets to communicate with consensus
///  * @param `registration_rx`: receiver end of the channel the main runtime sends peer registration requests through
async fn peer_consensus_actor(
    mut commit_sender: mpsc::Sender<Transaction>, 
    mut tools: ConsensusTools, 
    mut peer_rx: mpsc::Receiver<ActorRequest>,
    mut registration_rx: mpsc::Receiver<RegistrationRequest>, 
    mut signing_key: SigningKey, self_id: u8) {

    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    // general use serialize pool
    let mut serialize_pool = BytesMut::with_capacity(1024);

    loop { tokio::select! {
        biased;

        // wait for transaction request
        Some(request) = peer_rx.recv() => {
            match request {
                // client/peer node request to commit a transaction
                ActorRequest::ConsensusRequest { transaction } => {
                    // execute consensus
                    match peer_consensus_engine(transaction, &mut tools, &mut peer_rx, 
                        &mut commit_sender, &mut serialize_pool, &mut signing_key, &self_id).await {
                        Ok(_) => {  },
                        Err(e) => { eprintln!("Consensus from peer failed with error: {}", e); }
                    }
                },

                ActorRequest::PeerConsensusRequest { mut transaction } => {
                    // first, place the correct sequence counter at the thread boundary
                    transaction.seq_counter = tools.sequence_counter;

                    // then, craft the actual consensus request (move the tx field, which moving a list of ptrs)
                    let true_request = ActorRequest::ConsensusRequest { transaction };
                    
                    // route the transaction to the leader node
                    if let Err(_) = tools.get_socket_by_id(LEADER_NODE_ID.load(Relaxed)).unwrap()
                        .send(serialize_into(&mut serialize_pool, &true_request).freeze()).await {
                            eprintln!("Failed to send transaction to leader socket");
                    }
                },
                _ => eprintln!("Invalid message from peer - peer, request: {:?}", request)
            }
        }

        // TODO - synchronize the adding of a new peer to the network through epoch lengths

        // wait for join requests from fellow peers
        Some(request) = registration_rx.recv() => {
            let socket_framed = Framed::new(request.socket, socket_codec.clone());

            // add the socket and address to the registry
            if let Err(_) = tools.insert_socket(socket_framed, request.addr, request.pubkey, request.node_id) {
                eprintln!("Could not insert socket for registration"); continue;
            }
        }
    } }
}

/// @function CPU bound consensus engine function to be ran per consensus
/// * @param transaction: Transaction to be committed to the network/chain
/// * @param tools: Tools (registry, sequence counter, view number) for consensus
/// * @param vote receiver: the receiving end of the channel all peers send their votes through
/// * @param commit sender: the sending end of the channel to send the final transaction back to the state manager
/// * @param leader socket (optional): An option representing the leader socket that only peers use 
/// * @param signing key: the private key that the current node uses to sign their vote certs
pub async fn peer_consensus_engine(
    transaction: Transaction,
    tools: &mut ConsensusTools,
    vote_receiver: &mut mpsc::Receiver<ActorRequest>,
    commit_sender: &mut mpsc::Sender<Transaction>,
    serialization_pool: &mut BytesMut,
    signing_key: &mut SigningKey, self_id: &u8) -> io::Result<()> {

    // STEP 0 - calculate f (# of faulty nodes in pbft consensus equation)
    // if N = 3f + 1 holds true, where N = nodes, there can be at most (N - 1) / 3 faulty nodes
    // (for peers omit the - 1 to account for leader node not being in registry)
    let faulty = (( tools.len ) / 3).max(1) as u32;

    // timers in tokio are separate futures of their own when directly awaited on
    // so create a timer that gets recalculated instead of created and dropped over and over
    let sleep = tokio::time::sleep(Duration::from_millis(1500));

    tokio::pin!(sleep);

    // if peer skip straight to step 2

    // STEP 2: hash the key and value to validate the proposal, and
    // wait for a quorum (2f + 1) of PREPARE votes from other peers

    // create a quorum counter
    let mut quorum_counter = 0;

    // given node verifying the transaction themselves, verify the *client* transaction to see if it is valid
    match verify_transaction(&transaction.client_key, &transaction.unsigned_msg, &transaction.signed_msg).await {
        Ok(()) => {
            // add self to counter 
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
        Err(_) => { eprintln!("Transaction verification failed"); } // then skip to waiting for a quorum
    } 

    // reset the timer for the COMMIT vote
    reset_timer(&mut sleep, 3000);

    // get the sequence counters: (known counter, proposed counter)
    let sequence_counters = (tools.sequence_counter, transaction.seq_counter);

    tokio::select! {
        _ = wait_for_quorum(vote_receiver, sequence_counters, &mut quorum_counter, 
            tools.len, faulty, b"PREPARE") => { println!("Prepare quorum has been reached"); }

        _ = &mut sleep => { return return_err("Time limit exceeded, prepare verification failed"); }
    }
    
    // clean up the counter and the voter queue in preparation for receiving the commit votes
    // (reset counter to 1 to include this given node's vote)
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

        _ = &mut sleep => { return return_err("Time limit exceeded, prepare verification failed"); }
    }

    // consensus reached, transaction verified - commit!
    // if state transition fails (split brain) - fail fast and resync after restarting
    if let Err(_) = commit_sender.send(transaction).await {
        eprintln!("Could not commit state transition");
        panic!("Node crashed due to state transition failure"); 
    }

    // Update the sequence counter for the next transaction
    // (for peer verification of order and leader identity)

    // peer needs to use the static thread local cell
    tools.sequence_counter += 1;

    println!("Consensus has been reached");
    

    Ok(())

}