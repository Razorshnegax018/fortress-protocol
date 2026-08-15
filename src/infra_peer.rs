use core::panic;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::{io, net::{TcpListener, TcpStream}};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use futures::{SinkExt, StreamExt};

use bytes::{Bytes, BytesMut};

use serde::{Serialize, Deserialize};

use crate::protocol::infra_main::{ConsensusTools, ConsensusToolsStruct, RegistrationRequest, consensus_engine, reader_task};
use crate::protocol::utils::utils::{make_write_framed, send_connection_packet};
use crate::protocol::{
    infra_main::{ActorRequest, ClientTransaction, Transaction},
    utils::utils::{deserialize_packet, make_framed, verify_transaction}
};


use rand::rngs::OsRng;
use ed25519_dalek::{SigningKey, VerifyingKey};

type LeaderSocket = Framed<TcpStream, LengthDelimitedCodec>;

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
    let mut leader_socket: Option<SocketFramed> = None;

    // grab the port from the cmdline
    // let Some(port) = std::env::args().nth(1) else { panic!("invalid port") };
    let address = format!("127.0.0.1:{}", "6376");

    // general use serialize pool
    let mut serialize_pool = BytesMut::with_capacity(1024);

    // create a private-public keypair
    let mut csprng = OsRng;
    let keypair: SigningKey = SigningKey::generate(&mut csprng);
    let pubkey = keypair.verifying_key().to_bytes();

    let reader_runtime = network_runtime.handle().clone();

    // create channel for sending consensus requests
    let (engine_tx, peer_rx) = mpsc::channel::<ActorRequest>(32);

    // the bootnode needs to have connected with the leader before the peer does
    // TODO - these sleeps suck
    // * 1) - Split the three nodes into their own files. Please
    // * 2) - Implement a retry function when connecting to bootnode
    std::thread::sleep(Duration::from_secs(5));

    println!("Starting connection to bootnode...");
    if let Ok(socket) = TcpStream::connect(BOOTNODE_ADDRESS).await {
        let mut socket_framed = make_framed(socket, 1024);

        // send a pubkey confirmation packet 
        send_connection_packet("peer-pubkey", &address, 
            Some(&pubkey), &mut socket_framed, &mut serialize_pool).await;

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

                        if packet.node_type == b"leader" { 
                            println!("Peernode connected to leader socket"); 
                        
                            // send a pubkey confirmation packet to leader
                            send_connection_packet("peer-pubkey", &address, 
                            Some(&pubkey), &mut write_framed, &mut serialize_pool).await;

                            leader_socket = Some(write_framed);

                            // 2) clone the consensus engine sender to hand off to the reader task
                            let peer_tx = engine_tx.clone();

                            // 3) then, for each reader, spawn a new reader task
                            reader_runtime.spawn(reader_task(read_framed, peer_tx, Box::from(pubkey)));
                        } else {
                            // send a pubkey confirmation packet to peer
                            send_connection_packet("peer-pubkey", &address, 
                            Some(&pubkey), &mut write_framed, &mut serialize_pool).await;

                            // add the writer into the registry
                            registry.push(write_framed);

                            // 2) clone the consensus engine sender to hand off to the reader task
                            let peer_tx = engine_tx.clone();

                            // 3) then, for each reader, spawn a new reader task
                            reader_runtime.spawn(reader_task(read_framed, peer_tx, Box::from(pubkey)));
                        }
                    },
                    Err(_) => { eprintln!("Unable to connect to peer w/ addr: {}", &*address); }
                }
            }
        }
    } 

    if leader_socket.is_none() { panic!("No leader node found") }

    if let Err(e) = start_server(registry, leader_socket.unwrap(), Arc::new(keypair), address, 
        network_runtime, engine_tx, peer_rx).await 
            { eprintln!("Peer node server exited with Error: {}", e); } println!("Peer node server closed")
}

async fn start_server(
    registry: Vec<SocketFramed>, 
    leader_socket: SocketFramed, 
    keypair: Arc<SigningKey>, address: String,
    network_runtime: tokio::runtime::Runtime,
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
            peer_consensus_actor(commit_sender, tools, peer_rx, registration_rx));

        // whenever a new connection is opened...
        // create a reusable timout timer
        let sleep = tokio::time::sleep(Duration::from_millis(100));

        let deadline = Instant::now() + Duration::from_millis(100);
        tokio::pin!(sleep); sleep.as_mut().reset(deadline.into());

        // create a buffer to store the result from the select
        let mut network_buffer: Option<Result<BytesMut, std::io::Error>>;
        while let Ok((socket, addr)) = listener.accept().await {
            println!("Peer node {addr} connected to running peer node");

            let (read_socket, write_socket) = socket.into_split();

            // prepare the reader socket for reading
            let mut read_framed = make_framed(read_socket, 512);

            // select between the connceted client responding and a timeout
            tokio::select! { value = read_framed.next() => { network_buffer = value } 
                _ = &mut sleep => { eprintln!("client didn't respond in time, dropping"); continue; } } 

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

                        let _ = registration_tx.send(request);
                    },

                    // on client transaction request, verify and prepare for consensus
                    b"client-transaction" => {
                        // step 1: verify the client transaction
                        let client_tx: ClientTransaction = deserialize_packet::<ClientTransaction>(&connection_packet.payload)?;
                        let mut unsigned_msg = BytesMut::with_capacity(client_tx.key.len() + client_tx.value.len());

                        unsigned_msg.extend_from_slice(&client_tx.key);
                        unsigned_msg.extend_from_slice(&client_tx.value);

                        // Step 2: create a client transaction request
                        let request = ActorRequest::ConsensusRequest 
                            { transaction: Bytes::copy_from_slice(connection_packet.payload) };

                        // Clone the engine sender
                        let sender = engine_tx.clone();

                        // Step 3: verify the transaction and send the request to the engine
                        verify_transaction(client_tx.pubkey, &unsigned_msg.freeze().clone(), 
                            request, client_tx.signed_tx, sender).await?;
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
///  * @param `registration_rx`: receiver end of the channel the main runtime sends peer registration requestes through
async fn peer_consensus_actor(
    mut commit_sender: mpsc::Sender<Transaction>, mut tools: ConsensusTools, 
    mut peer_rx: mpsc::Receiver<ActorRequest>, 
    mut registration_rx: local_channel::mpsc::Receiver<RegistrationRequest>) {

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
                    let tx = bincode::deserialize::<Transaction>(&transaction).unwrap();

                    // execute consensus
                    consensus_engine(tx, &mut tools, &mut peer_rx, &mut commit_sender, &mut serialize_pool).await;
                },
                _ => eprintln!("Invalid message from peer")
            }
        }

        // TODO - syncrhonize the adding of a new peer to the network through epoch lengths

        // wait for join requests from fellow peers
        Some(request) = registration_rx.recv() => {
            // add a new peer to the registry
            let socket_framed = Framed::new(request.socket, socket_codec.clone());
            tools.registry.push(socket_framed);
        }
    } }
}
