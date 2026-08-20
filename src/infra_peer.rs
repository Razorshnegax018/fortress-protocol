use core::panic;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Mutex, mpsc};
use tokio::{io, net::{TcpListener, TcpStream}};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use futures::{SinkExt, StreamExt};

use bytes::{Bytes, BytesMut};

use serde::{Serialize, Deserialize};

use crate::protocol::infra_main::{ConsensusTools, ConsensusToolsStruct, RegistrationRequest, reader_task};
use crate::protocol::utils::utils::{connect_with_retry, io_err, make_write_framed, reset_timer, return_err, send_connection_packet, send_with_timeout, serialize_into, wait_for_quorum};
use crate::protocol::{
    infra_main::{ActorRequest, ClientTransaction, Transaction},
    utils::utils::{deserialize_packet, make_framed, verify_transaction}
};


use rand::rngs::OsRng;
use ed25519_dalek::{Signer, SigningKey};

/// We use non thread safe Rc because this is a single threaded runtime
/// and async-aware mutex to handle the "refcell async paradox" 
/// (how it's unsafe to hold a refcell over an await in a single threaded localset)
pub type LeaderSocket = Rc<Mutex<SocketFramed>>;

static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";

#[derive(Serialize, Deserialize)]
pub struct ConnectionPacket<'a> {
    #[serde(borrow, with = "serde_bytes")] pub node_type: &'a [u8], 
    #[serde(borrow, with = "serde_bytes")] pub address: &'a [u8], 
    #[serde(borrow, with = "serde_bytes")] pub payload: &'a [u8]
}

type SocketFramed = Framed<OwnedWriteHalf, LengthDelimitedCodec>;

pub async fn discover_network(network_runtime: tokio::runtime::Runtime) {
    // create the registry to be filled with peers alerady connected to the network
    let mut registry: Vec<SocketFramed> = Vec::with_capacity(12);

    // leader socket that may or may not exist
    let mut leader_socket: Option<LeaderSocket> = None;

    // grab the port from the cmdline
    // let Some(port) = std::env::args().nth(1) else { panic!("invalid port") };
    let address = format!("127.0.0.1:{}", "6376");

    // general use serialize pool
    let mut serialize_pool = BytesMut::with_capacity(1024);

    // create a private-public keypair
    let mut csprng = OsRng;
    let keypair: SigningKey = SigningKey::generate(&mut csprng);
    let self_pubkey = keypair.verifying_key().to_bytes();

    let reader_runtime = network_runtime.handle().clone();

    // create channel for sending consensus requests
    let (engine_tx, peer_rx) = mpsc::channel::<ActorRequest>(32);

    // the bootnode needs to have connected with the leader before the peer does
    // TODO - these sleeps suck
    // * 1) - Split the three nodes into their own files. Please
    // * 2) - Implement a retry function when connecting to bootnode
    std::thread::sleep(Duration::from_secs(2));

    println!("Starting connection to bootnode...");
    if let Ok(socket) = connect_with_retry(BOOTNODE_ADDRESS, 5).await {
        let mut socket_framed = make_framed(socket, 1024);

        // send a pubkey confirmation packet to the bootnode
        send_connection_packet("peer-pubkey", &address, 
            Some(&self_pubkey), &mut socket_framed, &mut serialize_pool).await;

        // if deserialization of the connection packet from the bootnode fails,
        // log the error and skip the packet
        if let Some(Ok(value)) = socket_framed.next().await {
            let Ok(list) = deserialize_packet::<Vec<ConnectionPacket>>(&value) else { 
                println!("{:?}", &value[..]);

                // TODO - refactor to not panic
                panic!("Bootnode sent bad packet");
            };

            for packet in list {
                // connect to peer 
                match TcpStream::connect(std::str::from_utf8(&packet.address).unwrap()).await {

                    // we need to spawn reader tasks for each of the sockets the bootnode gave us
                    Ok(stream) => { 
                        // 1) split the sockets into read and write and create frameds for them
                        let (reader, writer) = stream.into_split();

                        let read_framed = make_framed(reader, 512);
                        let mut write_framed = make_write_framed(writer, 512);

                        let peer_pubkey = packet.payload;

                        if packet.node_type == b"leader" { 
                            println!("Peernode connected to leader socket"); 
                        
                            // send a pubkey confirmation packet to leader
                            send_connection_packet("peer-pubkey", &address, 
                            Some(&self_pubkey), &mut write_framed, &mut serialize_pool).await;

                            leader_socket = Some(Rc::new(Mutex::new(write_framed)));

                            // 2) clone the consensus engine sender to hand off to the reader task
                            let peer_tx = engine_tx.clone();

                            // 3) then, for each reader, spawn a new reader task
                            reader_runtime.spawn(reader_task(read_framed, peer_tx, Box::from(peer_pubkey)));
                        } else {
                            // send a pubkey confirmation packet to peer
                            send_connection_packet("peer-pubkey", &address, 
                            Some(&self_pubkey), &mut write_framed, &mut serialize_pool).await;

                            // add the writer into the registry
                            registry.push(write_framed);

                            // 2) clone the consensus engine sender to hand off to the reader task
                            let peer_tx = engine_tx.clone();

                            // 3) then, for each reader, spawn a new reader task
                            reader_runtime.spawn(reader_task(read_framed, peer_tx, Box::from(peer_pubkey)));
                        }
                    },
                    Err(_) => { eprintln!("Unable to connect to peer w/ addr: {}", &*address); }
                }
            }
        }
    } 

    if leader_socket.is_none() { panic!("No leader node found") }

    if let Err(e) = start_server(registry, leader_socket.unwrap(), keypair, address, 
        network_runtime, engine_tx, peer_rx, serialize_pool).await 
            { eprintln!("Peer node server exited with Error: {}", e); } println!("Peer node server closed")
}

async fn start_server(
    registry: Vec<SocketFramed>, 
    leader_socket: LeaderSocket, 
    signing_key: SigningKey, address: String,
    network_runtime: tokio::runtime::Runtime,
    engine_tx: mpsc::Sender<ActorRequest>,
    peer_rx: mpsc::Receiver<ActorRequest>,
    mut serialize_pool: BytesMut) -> io::Result<()> {

    // let Some(port) = std::env::args().nth(1) else { panic!("invalid port") };

    if let Ok(listener) = TcpListener::bind(address).await {
        // create the manager sender and reciever queue ends
        let (commit_sender, mut tx_reciever) = mpsc::channel::<Transaction>(32);

        // Start the manager task that controls state and the transaction log
        let _transaction_manager = tokio::task::spawn_local(async move {
            let mut blockchain_state: HashMap<Bytes, Bytes> = HashMap::new();
            let mut transaction_log: Vec<Transaction> = Vec::with_capacity(1024);

            while let Some(transaction) = tx_reciever.recv().await {
                blockchain_state.insert(transaction.key.clone(), transaction.value.clone());
                transaction_log.push(transaction);
            }
        });

        let reader_runtime = network_runtime.handle().clone();

        // create channel for sending peer joining requests
        let (registration_tx, registration_rx) = local_channel::mpsc::channel::<RegistrationRequest>();

        // we need to spawn reader tasks for all the sockets the bootnode gave us
        // and populate the registry (to be used in tools) with all the writer sockets

        // create the tools for consensus
        let tools = ConsensusToolsStruct { 
            sequence_counter: 0, view_number: 0, registry, 
            address_list: Vec::with_capacity(10) };

        // spawn consensus engine task
        let _consensus_task = tokio::task::spawn_local(
            peer_consensus_actor(commit_sender, tools, peer_rx, 
                leader_socket.clone(), registration_rx, signing_key));

        // whenever a new connection is opened...
        // create a reusable timout timer
        let sleep = tokio::time::sleep(Duration::from_millis(750)); tokio::pin!(sleep); 

        // create a buffer to store the result from the select
        let mut network_buffer: Option<Result<BytesMut, std::io::Error>>;
        while let Ok((socket, addr)) = listener.accept().await {
            println!("Peer node {addr} connected to running peer node");

            let (read_socket, write_socket) = socket.into_split();

            // prepare the reader socket for reading
            let mut read_framed = make_framed(read_socket, 512);

            // start the timer
            reset_timer(&mut sleep, 750);

            // select between the connceted client responding and a timeout
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
                        reader_runtime.spawn(reader_task(read_framed, _peer_tx, Box::from(connection_packet.payload))); 

                        // send a registration request to the consensus enigne receiver to register the write half
                        let request = RegistrationRequest { socket: write_socket, addr };

                        io_err(registration_tx.send(request))?;
                    },

                    // on client transaction request, verify and prepare for consensus
                    b"client-transaction" => {
                        // step 1: verify the client transaction
                        let client_tx: ClientTransaction = deserialize_packet::<ClientTransaction>(&connection_packet.payload)?;

                        // reuse the existing serialize pool to avoid per client request allocation
                        serialize_pool.extend_from_slice(&client_tx.key);
                        serialize_pool.extend_from_slice(&client_tx.value);

                        let unsigned_msg = serialize_pool.split();

                        // craft the full transaction

                        // TODO - This literally defeats the purpose of zero copy. need to find a way to
                        // copy key, client key, and value all in one go, or send the counters individually

                        // Step 2: create a client transaction request
                        let request = ActorRequest::ConsensusRequest { transaction: Transaction {
                            client_key: Bytes::copy_from_slice(client_tx.pubkey),
                            seq_counter: 0, key: Bytes::copy_from_slice(client_tx.key),
                            signed_msg: Bytes::copy_from_slice(client_tx.signed_tx), view_number: 0, 
                            value: Bytes::copy_from_slice(client_tx.value), unsigned_msg: unsigned_msg.freeze(),
                        }};

                        // Step 3: verify the transaction and send the request to the engine
                        if let ActorRequest::ConsensusRequest { transaction: ref tx } = request {
                            // TODO: move into seperate function for better error handling
                            verify_transaction(client_tx.pubkey, &tx.unsigned_msg[..], client_tx.signed_tx).await?
                        }

                        //
                        leader_socket.lock().await
                            .send(serialize_into(&mut serialize_pool, &request).freeze()).await?;
                        
                        // do NOT send the request to the engine once it's verified
                        // the reader task will automatically start consensus
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
///  * @param `leader_socket`: leader write half so peers can send their votes to the leader
///  * @param `registration_rx`: receiver end of the channel the main runtime sends peer registration requestes through
async fn peer_consensus_actor(
    mut commit_sender: mpsc::Sender<Transaction>, mut tools: ConsensusTools, 
    mut peer_rx: mpsc::Receiver<ActorRequest>, leader_socket: LeaderSocket,
    mut registration_rx: local_channel::mpsc::Receiver<RegistrationRequest>,
    mut signing_key: SigningKey) {

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

                    // we hold the lock for the entire function because we don't want client processing tasks
                    // to get scheduled during consensus. it's uncontended from other threads and a quick atomic toggle
                    match peer_consensus_engine(transaction, &mut tools, &mut peer_rx, 
                        &mut commit_sender, &mut serialize_pool, &mut *leader_socket.lock().await, &mut signing_key).await {
                        Ok(_) => { println!("Consensus has been reached"); },
                        Err(e) => { eprintln!("Consensus failed with error: {}", e); }
                    }
                },
                _ => eprintln!("Invalid message from peer")
            }
        }

        // TODO - synchronize the adding of a new peer to the network through epoch lengths

        // wait for join requests from fellow peers
        Some(request) = registration_rx.recv() => {
            // add a new peer to the registry
            let socket_framed = Framed::new(request.socket, socket_codec.clone());
            tools.registry.push(socket_framed);
        }
    } }
}

/// @function CPU bound consensus engine function to be ran per consensus
/// * @param transaction: Transaction to be committed to the network/chain
/// * @param tools: Tools (registery, sequence counter, view number) for consensus
/// * @param vote receiver: the recieving end of the channel all peers send their votes through
/// * @param commit sender: the sending end of the channel to send the final transaction back to the state manager
/// * @param leader socket (optional): An option representing the leader socket that only peers use 
/// * @param signing key: the private key that the current node uses to sign their vote certs
pub async fn peer_consensus_engine(
    transaction: Transaction, 
    tools: &mut ConsensusTools,
    vote_reciever: &mut mpsc::Receiver<ActorRequest>, 
    commit_sender: &mut mpsc::Sender<Transaction>, 
    serialization_pool: &mut BytesMut,
    leader_socket: &mut SocketFramed,
    signing_key: &mut SigningKey) -> io::Result<()> {

    // STEP 0 - calculate f (# of faulty nodes in pbft consensus equation)
    // if N = 3f + 1 holds true, where N = nodes, there can be at most (N - 1) / 3 faulty nodes
    // (for peers omit the - 1 to account for leader node not being in registry)
    let faulty =  (( tools.registry.len() ) / 3).max(1) as u32;

    // timers in tokio are seperate futures of their own when directly awaited on
    // so create a timer that gets recalculated instead of created and dropped over and over
    let sleep = tokio::time::sleep(Duration::from_millis(1500));

    tokio::pin!(sleep);

    // if peer skip straight to step 2

    // STEP 2: hash the key and value to validate the proposal, and
    // wait for a quorum (2f + 1) of PREPARE votes from other peers

    // given node verifying the transaction themselves, verify the *client* transaction to see if it is valid
    verify_transaction(&transaction.client_key, &transaction.unsigned_msg, &transaction.signed_msg).await?;

    // once the transaction has been verified craft the PREPARE vote
    let signed_prepare_vote = signing_key.sign(b"PREPARE").to_bytes();

    let prepare_vote = ActorRequest::PeerVote { 
        vote_type: Bytes::from_static(b"PREPARE"), 
        signed_msg: Bytes::copy_from_slice(&signed_prepare_vote)
    };

    let vote_bytes = serialize_into(serialization_pool, &prepare_vote);
    let vote_payload = vote_bytes.freeze().clone();

    // if it's a peer send the request to the leader first
    send_with_timeout(leader_socket, vote_payload.clone(), &mut sleep).await;

    reset_timer(&mut sleep, 1500);

    // loop through registry and send PREPARE vote to all peer nodes
    for socket_frame in &mut tools.registry {
        send_with_timeout(socket_frame, vote_payload.clone(), &mut sleep).await;

        reset_timer(&mut sleep, 1500);
    }

    // reset the quorum and timer for the COMMIT vote
    let mut quorum_counter = 0; reset_timer(&mut sleep, 3000);
    tokio::select! {
        _ = wait_for_quorum(vote_reciever, &mut quorum_counter, faulty, b"PREPARE") => { println!("Prepare quorum has been reached"); }

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

    send_with_timeout(leader_socket, vote_payload.clone(), &mut sleep).await;
    
    // Broadcast commit message and wait again for commit quorum
    for socket_frame in &mut tools.registry {
        // set a timout - we don't want to hang sending to nonresponsive peers
        send_with_timeout(socket_frame, vote_payload.clone(), &mut sleep).await;

        // reset the deadline for the next loop
        reset_timer(&mut sleep, 1500);
    }

    tokio::select! {
        _ = wait_for_quorum(vote_reciever, &mut quorum_counter, faulty, b"COMMIT") => { println!("Commit quorum has been reached"); }

        _ = &mut sleep => 
            { return return_err("Time limit exceeded, prepare verification failed"); }
    }

    // consensus reached, transaction verified - commit!
    // if state transition fails (split brain) - fail fast and resync after restarting
    if let Err(_) = commit_sender.send(transaction).await {
        eprintln!("Could not commit state transition");
        panic!("Node crashed due to state transition failure"); 
    }

    // Update the sequence counter for the next transaction
    // (for peer verification of order and leader identity)
    tools.sequence_counter += 1;

    Ok(())

}
