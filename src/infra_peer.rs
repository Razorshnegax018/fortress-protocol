use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;

use rand::Rng;
use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::{io, net::{TcpListener, TcpStream}};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use futures::{SinkExt, StreamExt};

use bytes::{Bytes, BytesMut, BufMut};

use serde::{Serialize, Deserialize};

use crate::protocol::infra_main::{ActorRequest, ClientTransaction, Transaction, io_err, verify_transaction};

use rand::rngs::OsRng;
use ed25519_dalek::{SigningKey, VerifyingKey};
use ed25519_dalek::Signature;

type PeerRegistry = Arc<Vec<TcpStream>>;
type LeaderSocket = Framed<TcpStream, LengthDelimitedCodec>;

static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";
static LEADER_ADDRESS: &str = "127.0.0.1:8070";

#[derive(Serialize, Deserialize)]
pub struct ConnectionPacket {
    pub node_type: Bytes, pub address: Bytes, pub payload: Bytes
}

type SocketFramed = Framed<TcpStream, LengthDelimitedCodec>;

pub fn make_framed<Reader>(socket: Reader) -> Framed<Reader, LengthDelimitedCodec> 
    where Reader: AsyncReadExt + Unpin {
    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    Framed::new(socket, socket_codec)
}

pub fn deserialize_packet<T: DeserializeOwned>(buffer: &[u8]) -> io::Result<T> {
    match bincode::deserialize::<T>(buffer) {
        Ok(packet) => Ok(packet),
        Err(_) => {
            eprintln!("Bootnode sent bad packet");
            Err(io::Error::new(ErrorKind::InvalidData, ""))
        }
    }
}

async fn start_peer_node() {
    let mut registry: Vec<TcpStream> = Vec::new();
    let mut _leader_socket: Option<TcpStream> = None;

    if let Ok(socket) = TcpStream::connect(BOOTNODE_ADDRESS).await {
        let mut socket_framed = make_framed(socket);

        // if deserialization of the connection packet from the bootnode fails,
        // log the error and skip the packet
        while let Some(Ok(value)) = socket_framed.next().await {
            let Ok(packet) = deserialize_packet::<ConnectionPacket>(&value) else { continue; };

            // attempt connection to peer
            let address = String::from_utf8_lossy(&packet.address[..]);

            match TcpStream::connect(&*address).await {
                Ok(stream) => { if packet.node_type == "leader" 
                    { _leader_socket = Some(stream); } else { registry.push(stream) }},
                Err(_) => { eprintln!("Unable to connect to peer w/ addr: {}", &*address); }
            }
        }
    } 

    if _leader_socket.is_none() { panic!("No leader node found") }
    let leader_socket = 
        make_framed(_leader_socket.unwrap());

    // create a private-public keypair
    let mut csprng = OsRng;
    let keypair: SigningKey = SigningKey::generate(&mut csprng);

    start_server(Arc::new(registry), leader_socket, Arc::new(keypair)).await;
}

async fn start_server(registry: PeerRegistry, leader_socket: LeaderSocket, keypair: Arc<SigningKey>) {
    // loop to try tcp connection again if port is taken up
    loop { let mut rng = rand::thread_rng();
        
        // create a random port number to bind to 
        let port = rng.gen_range(1024..9999);

        // if that port doesn't work, then skip down to the end and loop again to make a new port
        if let Ok(listener) = TcpListener::bind(format!("127.0.0.1:{}", port)).await {
            // create the manager sender and reciever queue ends
            let (sender, mut tx_reciever) = mpsc::channel::<Transaction>(32);

            // Start the manager task that controls state and the transaction log
            let _transaction_manager = tokio::task::spawn(async move {
                let mut blockchain_state: HashMap<Bytes, Bytes> = HashMap::new();
                let mut transaction_log: Vec<Transaction> = Vec::with_capacity(1024);

                while let Some(transaction) = tx_reciever.recv().await {
                    blockchain_state.insert(transaction.key.clone(), transaction.value.clone());
                    transaction_log.push(transaction);
                }
            });

            // connect to leader and make a framed from their socket
            let _socket = TcpStream::connect(LEADER_ADDRESS).await
                .expect("Could not connect to leader node");
            let leader_socket = make_framed(_socket);


            // TODO - CONSENSUS ENGINE TASK
            let (engine_tx, engine_rx) = mpsc::channel::<ActorRequest>(32);

            // whenever a new connection is opened...
            while let Ok((socket, _)) = listener.accept().await {
                let registry_clone = registry.clone();
                let tx_clone = engine_tx.clone();

                // and hand it off to handle_request for later logic
                tokio::task::spawn(async move { let _ = handle_request(socket, registry_clone, tx_clone ).await; });
            }
        } else {} }
}

async fn handle_request(socket: TcpStream, registry: PeerRegistry, engine_tx: mpsc::Sender<ActorRequest>) -> io::Result<()> {
    let mut socket_framed = make_framed(socket);

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

                    // Step 2: Send the client transaction request to the consensus actor
                    let request = ActorRequest::ConsensusRequest { transaction: packet.payload };

                    // (handled inside the function with the var engine_tx)
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

async fn peer_consensus_engine(engine_rx: mpsc::Receiver<ActorRequest>, registry: PeerRegistry) {
    
}

async fn request_client_mutation(transaction: ClientTransaction, leader_socket: &mut LeaderSocket) {

}
