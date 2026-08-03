use std::{collections::HashMap, fmt::Write, io::ErrorKind, pin::Pin, time::Duration};

use sha2::digest::typenum::Length;
use tokio::{io::AsyncWriteExt, sync::mpsc, time::{Instant, Sleep}};

use tokio::{io::{self}, net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}};
use tokio_util::{codec::{Framed, LengthDelimitedCodec}};

use futures::{SinkExt, StreamExt};

use bytes::{Bytes, BytesMut};

use serde::{Serialize, Deserialize};

use ed25519_dalek::{Signature, VerifyingKey, Verifier};

use crate::protocol::infra_peer::{ConnectionPacket, deserialize_packet, make_framed, serialize_into};

type SocketFramed = Framed<OwnedWriteHalf, LengthDelimitedCodec>;
type ReadFramed = Framed<OwnedReadHalf, LengthDelimitedCodec>;

type PeerRegistry = Vec<SocketFramed>;

static ADDRESS: &str = "127.0.0.1:8070";
static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";

/** Standard transaction struct that peers append to the block log */
#[derive(Serialize, Deserialize)]
pub struct Transaction {
    pub consensus_stage: String, pub tx_hash: [u8; 32], 
    pub seq_counter: u32, pub view_number: u32, pub client_key: u64,
    pub source: Bytes, pub key: Bytes, pub value: Bytes,
}

/** Data that the client sends to a peer when requesting a mutation */
#[derive(Serialize, Deserialize)]
pub struct ClientTransaction {
    pub key: Bytes, pub value: Bytes, pub pubkey: Bytes, pub signed_tx: Bytes
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

#[derive(Serialize, Deserialize)]
pub enum ActorRequest {
    ConsensusRequest { transaction: Bytes }, 
    PeerVote { vote_type: Bytes, signed_msg: Bytes, unsigned_msg: Bytes }
}

/** @util Takes in any generic result and maps it to type tokio `io::Result` so ? instead of expect can be used */
pub fn io_err<T, E: std::fmt::Display>(result: Result<T, E>) -> io::Result<T> {
    result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

pub struct RegistrationRequest { socket: OwnedWriteHalf, addr: std::net::SocketAddr }

/// @function starts the leader node server. function that handles requests from peer nodes
/// the consensus actor and the netowrk state actor are both started in this fn as long running tasks
/// * @param network_runtime - the tokio runtime handle for a reader task. can and is cloned per reader
pub async fn start_server(network_runtime: tokio::runtime::Runtime) -> io::Result<()> {
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

    /* channel sender and receiver to add a new peer to the registry */
    let (registration_tx, registration_rx) = 
        local_channel::mpsc::channel::<RegistrationRequest>();

    // Start the manager task that controls state and the transaction log
    let _transaction_manager = tokio::task::spawn_local(async move {
        let mut blockchain_state: HashMap<Bytes, Bytes> = HashMap::new();
        let mut transaction_log: Vec<Transaction> = Vec::with_capacity(1024);

        while let Some(transaction) = transaction_receiver.recv().await {
            blockchain_state.insert(transaction.key.clone(), transaction.value.clone());
            transaction_log.push(transaction);
        }
    });

    let commit_sender = transaction_sender.clone();

    let reader_runtime = network_runtime.handle().clone();

    // small wait for testing
    std::thread::sleep(Duration::from_secs(1));

    // Start the consensus actor task
    let _consensus_task = tokio::task::spawn_local(
        consensus_actor(commit_sender, tools, peer_receiver, registration_rx));

    // create a reusable timout timer
    let sleep = tokio::time::sleep(Duration::from_millis(100));

    let deadline = Instant::now() + Duration::from_millis(100);
    tokio::pin!(sleep); sleep.as_mut().reset(deadline.into());

    // create a buffer to store the result from the select
    let mut network_buffer: Option<Result<BytesMut, std::io::Error>>;
    while let Ok((socket, addr)) = listener.accept().await {
        println!("Peer node {addr} connected to primary");

        let (read_socket, write_socket) = socket.into_split();

        // prepare the reader socket for reading
        let mut read_framed = make_framed(read_socket);

        // select between the connceted client responding and a timeout
        tokio::select! { value = read_framed.next() => { network_buffer = value } 
            _ = &mut sleep => { eprintln!("client didn't respond in time, dropping"); continue; } } 

        // on connection, the very first thing the peer should do is send the leader its pubkey
        if let Some(Ok(packet)) = network_buffer {
            // error handling is inside the function. Skip this connection if the peer sends a bad packet
            let Ok(pubkey_packet) = deserialize_packet::<ConnectionPacket>(&packet) 
                else { eprintln!("peer sent a bad packet, dropping connection"); continue; };
            
            // if client sends a packet that isn't listed with 'client-pubkey', bad packet
            if pubkey_packet.node_type != Bytes::from_static(b"client-pubkey") 
                { eprintln!("peer didn't send their pubkey, dropping connection"); continue; }

            // pass the read half and the request sender to a new reader task
            let _peer_tx = peer_tx.clone();
            reader_runtime.spawn(reader_task(read_framed, _peer_tx, pubkey_packet.payload)); 

            // send a registration request to the consensus enigne receiver to register the write half
            let request = RegistrationRequest { socket: write_socket, addr };

            let _ = registration_tx.send(request);
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
pub async fn consensus_actor(
    mut commit_sender: mpsc::Sender<Transaction>, mut tools: ConsensusTools, 
    mut peer_receiver: mpsc::Receiver<ActorRequest>, 
    mut registration_rx: local_channel::mpsc::Receiver<RegistrationRequest>) {


    std::thread::sleep(Duration::from_millis(500));
    let bootnode_socket = TcpStream::connect(BOOTNODE_ADDRESS).await
        .expect("Connection to bootnode failed");

    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    let mut bootnode_framed = Framed::new(bootnode_socket, socket_codec.clone());
    let mut serialize_pool = BytesMut::with_capacity(1024);

    // send the bootnode the leader verification packet
    send_connection_packet("leader", ADDRESS, None, 
        &mut bootnode_framed, &mut serialize_pool).await;

    loop { tokio::select! {
        biased;

        // wait for transaction request
        Some(request) = peer_receiver.recv() => {
            match request {
                // client/peer node request to commit a transaction
                ActorRequest::ConsensusRequest { transaction } => {
                    let tx = bincode::deserialize::<Transaction>(&transaction).unwrap();

                    // execute consensus
                    consensus_engine(tx, &mut tools, &mut peer_receiver, &mut commit_sender).await;
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

            // make a copy of the registry to send
            let addrs = tools.address_list.clone();

            // serialize the addresses into the heap buffer
            let addr_packet = serialize_into(&mut serialize_pool, &addrs);

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
pub async fn reader_task(mut read_framed: ReadFramed, peer_tx: mpsc::Sender<ActorRequest>, pubkey: Bytes) -> io::Result<()> {
    // wait for messages from peer
    // framed next for the socket codec - framing entire messages
    while let Some(Ok(read_buffer)) = read_framed.next().await {

        // Step 1 - deserailize client request (change for proper error handling)
        match bincode::deserialize::<ActorRequest>(&read_buffer) {

            // Step 2 - send either transaction payload or vote to the consensus actor
            // (all recievers expect payload type ActorRequest 
            // so just pass along the deserialized request)
            Ok(request) => { match request {

                // If it's a vote from a peer, verify it before sending
                ActorRequest::PeerVote { vote_type: _, ref signed_msg, ref unsigned_msg } => {
                    // Step 1 - create the verifyng key from the pubkey bytes
                    let key_bytes: [u8; 32] = io_err(pubkey[..].try_into())?;
                    let verifying_key = io_err(VerifyingKey::from_bytes(&key_bytes))?;

                    // Step 2 - Create the "signature" (signed msg) from bytes
                    let signed_bytes: [u8; 64] = io_err(signed_msg[..].try_into())?;
                    let signature = Signature::from_bytes(&signed_bytes);

                    // Step 3 - Verify client message/vote and send to engine if valid
                    match verifying_key.verify(&unsigned_msg[..], &signature) {
                        Ok(_) => { let _ = peer_tx.send(request).await; },
                        Err(_) => { return Err(io::Error::new(io::ErrorKind::InvalidData, 
                            "failed cryptographic verification"))  }
                    }
                },

                // but if it's a client adding a new transaction, send directly to engine
                ActorRequest::ConsensusRequest { transaction: _ } => 
                    {  let _ = peer_tx.send(request).await; },
            } },
            Err(_) => { eprintln!("Deserialization failed"); 
                return Err(io::Error::new(io::ErrorKind::InvalidData, "failed")) }
        }
    }

    Ok(())
}

async fn consensus_engine(
    transaction: Transaction, 
    tools: &mut ConsensusTools, 
    vote_reciever: &mut mpsc::Receiver<ActorRequest>, 
    commit_sender: &mut mpsc::Sender<Transaction>) {

    // STEP 1: broadcast the proposed transaction to each peer in the network registry

    // Encode the transaction and prepare to send it
    let tx_bytes = bincode::serialize(&transaction).unwrap();
    let tx_payload = Bytes::from(tx_bytes);

    let sleep = tokio::time::sleep(Duration::from_millis(100));

    tokio::pin!(sleep);

    let deadline = Instant::now() + Duration::from_millis(100);
    sleep.as_mut().reset(deadline.into());
    
    // loop through registry and send proposal to all peer nodes
    for socket_frame in &mut tools.registry {
        // set a timout - we don't want to hang sending to nonresponsive peers
        tokio::select! {
            _ = socket_frame.send(tx_payload.clone()) => {}
            _ = &mut sleep => { /* peer eviction */ }
        }
    }

    // STEP 2: hash the key and value to validate the proposal, and
    // wait for a quorum (2f + 1) of PREPARE votes from other peers
    let mut quorum_counter = 0; sleep.as_mut().reset(deadline.into());

    // leader verifying the transaction themselves
    // verify_transaction(&transaction, &mut quorum_counter, seq_counter, view_number);

    tokio::select! {
        _ = wait_for_quorum(vote_reciever, &mut quorum_counter, b"PREPARE") => { println!("Quorum has been reached"); }

        _ = &mut sleep => 
            { println!("Time limit for quorum exceeded, 
                prepare verification failed"); return; }
    }
    

    // clean up the counter and the voter queue in preparation for recieving the commmit votes
    quorum_counter = 0; while let Ok(_) = vote_reciever.try_recv() { /* clear */ }
    sleep.as_mut().reset(deadline.into());

    // STEP 3: Broadcast commit message and wait again for commit quorum
    for socket_frame in &mut tools.registry {
        // set a timout - we don't want to hang sending to nonresponsive peers
        tokio::select! {
            _ = socket_frame.send(Bytes::from_static(b"COMMIT")) => {}
            _ = &mut sleep => {}
        }
    }

    tokio::select! {
        _ = wait_for_quorum(vote_reciever, &mut quorum_counter, b"COMMIT") => { println!("Quorum has been reached"); }

        _ = &mut sleep => 
            { println!("Time limit for quorum exceeded, 
                commit verification failed"); return; }
    }

    // consensus reached, transaction verified - commit!
    // if state transition fails (split brain) - fail fast and resync after restarting
    if let Err(_) = commit_sender.send(transaction).await {
        eprintln!("Could not commit state transition");
        panic!("Node {ADDRESS} crashed due to state transition failure"); 
    }

    // Update the sequence counter for the next transaction
    // (for peer verification of order and leader identity)
    tools.sequence_counter += 1;

    // update the leader view number
    if tools.sequence_counter >= 10 {
        tools.sequence_counter = 0; tools.view_number += 1;
    }

}

/// @util cryptographically verifies a given transacton
/// * @params pubkey, unsigned msg, signed msg - all required for signing 
/// * @params request, sender - most transactions are contained inside @param request objects
/// that will be set to a different task using the @param sender
pub async fn verify_transaction(
    pubkey: Bytes, unsigned_msg: Bytes, request: ActorRequest,
    signed_msg: Bytes, sender: mpsc::Sender<ActorRequest>
) -> io::Result<()> {
    // Step 1 - create the verifyng key from the pubkey bytes
    let key_bytes: [u8; 32] = io_err(pubkey[..].try_into())?;
    let verifying_key = io_err(VerifyingKey::from_bytes(&key_bytes))?;

    // Step 2 - Create the "signature" (signed msg) from bytes
    let signed_bytes: [u8; 64] = io_err(signed_msg[..].try_into())?;
    let signature = Signature::from_bytes(&signed_bytes);

    // Step 3 - Verify client message/vote and send to engine if valid
    match verifying_key.verify(&unsigned_msg[..], &signature) {
        Ok(_) => { let _ = sender.send(request).await; },
        Err(_) => { /* err */ }
    } Ok(())
}

fn return_err() -> Result<ConnectionPacket, std::io::Error> { return Err(io::Error::new(ErrorKind::ConnectionRefused, "dropped")) }

/// @util crafts and sends a ConnectionPacket through a given TCP connection
pub async fn send_connection_packet<Writer>(
    node_type: &'static str, addr: &'static str, payload: Option<BytesMut>, 
    socket: &mut Framed<Writer, LengthDelimitedCodec>, pool: &mut BytesMut
) where Writer: AsyncWriteExt + Unpin {

    // craft the connection packet
    let mut packet = ConnectionPacket {
        address: Bytes::from_static(addr.as_bytes()), payload: Bytes::new(),
        node_type: Bytes::from_static(node_type.as_bytes()) };

    // add the payload if it exists
    if let Some(send_payload) = payload { packet.payload = send_payload.freeze(); }

    // serialize into bytes using the pool
    let send_packet = serialize_into(pool, &packet);

    // send the ConnectionPacket
    let _ = socket.send(send_packet.freeze()).await;
}

/// @util takes a tcp connection and tries connecting several times
pub fn connect_retry(addr: &'static str) -> TcpStream { todo!(); }

async fn wait_for_quorum(vote_reciever: &mut mpsc::Receiver<ActorRequest>, 
        quorum_counter: &mut i32, stage: &'static [u8]) {

    while let Some(value) = vote_reciever.recv().await {
        match value {
            ActorRequest::PeerVote { vote_type, signed_msg: _, unsigned_msg: _ } => {
                if vote_type == Bytes::from_static(stage) { 
                    *quorum_counter += 1; 
                } if *quorum_counter >= 3 { break; }
            }, _ => {}
        }
    }
}
