use core::{net, panic};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::{JoinHandle, spawn};
use std::time::Duration;

use rand::Rng;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::{io, net::{TcpListener, TcpStream}};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use futures::{SinkExt, StreamExt};

use bytes::{Bytes, BytesMut};

use serde::{Serialize, Deserialize};

use crate::protocol::infra_main::{ConsensusTools, ConsensusToolsStruct, RegistrationRequest, consensus_engine, reader_task};
use crate::protocol::utils::utils::make_write_framed;
use crate::protocol::{
    infra_main::{ActorRequest, ClientTransaction, Transaction},
    utils::utils::{deserialize_packet, make_framed, verify_transaction}
};


use rand::rngs::OsRng;
use ed25519_dalek::{SigningKey, VerifyingKey};

type PeerRegistry = Arc<Vec<TcpStream>>;
type LeaderSocket = Framed<TcpStream, LengthDelimitedCodec>;

static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";
static LEADER_ADDRESS: &str = "127.0.0.1:8070";

#[derive(Serialize, Deserialize)]
pub struct ConnectionPacket {
    pub node_type: Bytes, pub address: Bytes, pub payload: Bytes
}

pub struct BaseRegistry { sockets: Vec<TcpStream>, pubkeys: Vec<Bytes> }

type SocketFramed = Framed<TcpStream, LengthDelimitedCodec>;

pub async fn start_peer_node(network_runtime: tokio::runtime::Runtime) {
    let mut registry: Vec<(TcpStream, Bytes)> = Vec::with_capacity(12);
        
    let mut _leader_socket: Option<TcpStream> = None;

    if let Ok(socket) = TcpStream::connect(BOOTNODE_ADDRESS).await {
        let mut socket_framed = make_framed(socket, 1024);

        // if deserialization of the connection packet from the bootnode fails,
        // log the error and skip the packet
        while let Some(Ok(value)) = socket_framed.next().await {
            let Ok(packet) = deserialize_packet::<ConnectionPacket>(&value) else { continue; };

            // attempt connection to peer
            let address = String::from_utf8_lossy(&packet.address[..]);

            match TcpStream::connect(&*address).await {
                // TODO - make sure to get the leader's pubkey
                Ok(stream) => { if packet.node_type == "leader" { _leader_socket = Some(stream); } 

                    // TODO - Upon peer connection, 
                    else { 
                        // push the socket-pubkey pair into the list (packet.payload from bootnode should be pubkey)
                        registry.push((stream, packet.payload));
                    }},
                Err(_) => { eprintln!("Unable to connect to peer w/ addr: {}", &*address); }
            }
        }
    } 

    if _leader_socket.is_none() { panic!("No leader node found") }
    let leader_socket = 
        make_framed(_leader_socket.unwrap(), 512);

    // create a private-public keypair
    let mut csprng = OsRng;
    let keypair: SigningKey = SigningKey::generate(&mut csprng);

    start_server(registry, leader_socket, Arc::new(keypair), network_runtime).await;
}

async fn start_server(
    registry: Vec<(TcpStream, Bytes)>, 
    leader_socket: LeaderSocket, 
    keypair: Arc<SigningKey>, 
    network_runtime: tokio::runtime::Runtime) {

    let Some(port) = std::env::args().nth(1) else { panic!("invalid port") };

    if let Ok(listener) = TcpListener::bind(format!("127.0.0.1:{}", port)).await {
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

        // create channel for sending consensus requests
        let (engine_tx, peer_rx) = mpsc::channel::<ActorRequest>(32);

        // create channel for sending peer joining requests
        let (registration_tx, registration_rx) = local_channel::mpsc::channel::<RegistrationRequest>();

        let mut tools_registry: Vec<Framed<OwnedWriteHalf, LengthDelimitedCodec>> = Vec::with_capacity(registry.len());

        // we need to spawn reader tasks for all the sockets the bootnode gave us
        // and populate the registry (to be used in tools) with all the writer sockets
        for (socket, pubkey) in registry {
            // 1) split the sockets into read and write and create frameds for them
            let (reader, writer) = socket.into_split();

            let read_framed = make_framed(reader, 512);
            let write_framed = make_write_framed(writer, 512);

            tools_registry.push(write_framed);

            // 2) clone the consensus engine sender to hand off to the reader task
            let peer_tx = engine_tx.clone();

            // 3) then, for each reader, spawn a new reader task
            reader_runtime.spawn(reader_task(read_framed, peer_tx, pubkey));
        }

        // create the tools for consensus
        let tools = ConsensusToolsStruct { 
            sequence_counter: 0, view_number: 0, registry: tools_registry, 
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
            println!("Peer node {addr} connected to primary");

            let (read_socket, write_socket) = socket.into_split();

            // prepare the reader socket for reading
            let mut read_framed = make_framed(read_socket, 512);

            // select between the connceted client responding and a timeout
            tokio::select! { value = read_framed.next() => { network_buffer = value } 
                _ = &mut sleep => { eprintln!("client didn't respond in time, dropping"); continue; } } 

            // on connection, the very first thing the peer should do is send the connected node its pubkey
            if let Some(Ok(packet)) = network_buffer {
                // error handling is inside the function. Skip this connection if the peer sends a bad packet
                let Ok(pubkey_packet) = deserialize_packet::<ConnectionPacket>(&packet) 
                    else { eprintln!("peer sent a bad packet, dropping connection"); continue; };
                
                // if client sends a packet that isn't listed with 'client-pubkey', bad packet
                if pubkey_packet.node_type != Bytes::from_static(b"client-pubkey") 
                    { eprintln!("peer didn't send their pubkey, dropping connection"); continue; }

                // pass the read half and the request sender to a new reader task
                let _peer_tx = engine_tx.clone();
                reader_runtime.spawn(reader_task(read_framed, _peer_tx, pubkey_packet.payload)); 

                // send a registration request to the consensus enigne receiver to register the write half
                let request = RegistrationRequest { socket: write_socket, addr };

                let _ = registration_tx.send(request);
            }
        }
    } else { /* TODO: Implement retry logic */ }
}

/// TODO - YOU'RE NOT DONE. HANDLE THIS
async fn handle_request(socket: TcpStream, registry: PeerRegistry, engine_tx: mpsc::Sender<ActorRequest>) -> io::Result<()> {
    let mut socket_framed = make_framed(socket, 512);

    // first, decide if the connected machine is client or peer
    if let Some(Ok(value)) = socket_framed.next().await {
        let packet = deserialize_packet::<ConnectionPacket>(&value)?;

        match &packet.node_type[..] {
            b"client" => {
                // step 1: verify the client transaction
                let client_tx: ClientTransaction = deserialize_packet::<ClientTransaction>(&packet.payload)?;
                let mut unsigned_msg = BytesMut::with_capacity(client_tx.key.len() + client_tx.value.len());

                unsigned_msg.extend_from_slice(&client_tx.key);
                unsigned_msg.extend_from_slice(&client_tx.value);

                // Step 2: create a client transaction request
                let request = ActorRequest::ConsensusRequest { transaction: packet.payload };

                // Step 3: verify the transaction and send the request to the engine
                verify_transaction(client_tx.pubkey, unsigned_msg.freeze().clone(), 
                    request, client_tx.signed_tx, engine_tx).await?;
            }
            _ => {}
        }
   }


    // handle requests from clients to the protocol only at this stage
    while let Some(Ok(value)) = socket_framed.next().await {

        // deserialize client transaction
        let Ok(client_tx) = deserialize_packet::<ClientTransaction>(&value) else { continue; };
    }

    Ok(())
}


/// @function consensus actor: actor handler for the consensus engine hot loop
///  * @param `commit_sender`: Sender end of channel engine uses 
///    to send completed transactions back to state manager 
///  * @param `tools`: criical tools (seq_counter, view_number, peer_socket_registry)
///    required for consensus
///  * @param `peer_receiver`: entryway for peer sockets to communicate with consensus
///  * @param `registration_rx`: receiver end of the channel the main runtime sends peer registration requestes through
async fn peer_consensus_actor(
    mut commit_sender: mpsc::Sender<Transaction>, mut tools: ConsensusTools, 
    mut peer_rx: mpsc::Receiver<ActorRequest>, 
    mut registration_rx: local_channel::mpsc::Receiver<RegistrationRequest>) {

    std::thread::sleep(Duration::from_millis(500));

    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    loop { tokio::select! {
        biased;

        // wait for transaction request
        Some(request) = peer_rx.recv() => {
            match request {
                // client/peer node request to commit a transaction
                ActorRequest::ConsensusRequest { transaction } => {
                    let tx = bincode::deserialize::<Transaction>(&transaction).unwrap();

                    // execute consensus
                    consensus_engine(tx, &mut tools, &mut peer_rx, &mut commit_sender).await;
                },
                _ => eprintln!("Invalid message from peer")
            }
        }

        // wait for join requests from fellow peers
        Some(request) = registration_rx.recv() => {
            // add a new peer to the registry
            let socket_framed = Framed::new(request.socket, socket_codec.clone());
            tools.registry.push(socket_framed);
        }
    } }
}
