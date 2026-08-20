// mock_client.rs — standalone client transaction tester
//
// Run: cargo run --bin mock_client
// (make sure a peer node is already listening on PEER_ADDRESS below)

use bytes::{BufMut, BytesMut};
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures::SinkExt;

// Address of the running peer node to send the transaction to.
static PEER_ADDRESS: &str = "127.0.0.1:6376";

/// Zero copy transport format that contains a node's type, its address, and it's pubkey for ID
/// (copied from crate)
#[derive(Serialize, Deserialize)]
pub struct ConnectionPacket<'a> {
    #[serde(borrow, with = "serde_bytes")] pub node_type: &'a [u8], 
    #[serde(borrow, with = "serde_bytes")] pub address: &'a [u8], 
    #[serde(borrow, with = "serde_bytes")] pub payload: &'a [u8]
}

/** Data that the client sends to a peer when requesting a mutation (copy of struct from crate) */
#[derive(Serialize, Deserialize)]
pub struct ClientTransaction<'a> {
    #[serde(borrow)] pub key: &'a [u8], #[serde(borrow)] pub value: &'a [u8], 
    #[serde(borrow)] pub pubkey: &'a [u8], #[serde(borrow)] pub signed_tx: &'a [u8]
}

// copy of serialize_into function from main crate
fn serialize_into<T: Serialize>(pool: &mut BytesMut, data: &T) -> BytesMut {
    bincode::serialize_into((&mut *pool).writer(), data).unwrap();
    pool.split()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // generate keypair
    let mut csprng = OsRng;
    let keypair = SigningKey::generate(&mut csprng);
    let pubkey_bytes = keypair.verifying_key().to_bytes();

    // make a test key and value
    let key: &[u8] = b"test-key"; let value: &[u8] = b"test-value";

    // build the unsigned message
    let mut unsigned_msg = Vec::with_capacity(key.len() + value.len());
    unsigned_msg.extend_from_slice(key);
    unsigned_msg.extend_from_slice(value);

    // sign the message
    let signature = keypair.sign(&unsigned_msg).to_bytes();

    let client_tx = ClientTransaction { 
        key, value, pubkey: &pubkey_bytes, signed_tx: &signature };

    let mut pool = BytesMut::with_capacity(1024);
    let client_tx_bytes = serialize_into(&mut pool, &client_tx).freeze();

    // craft connection packet
    let packet = ConnectionPacket {
        node_type: b"client-transaction", address: b"127.0.0.1:0", payload: &client_tx_bytes,
    };

    let payload = serialize_into(&mut pool, &packet).freeze();

    // connect to peer
    let stream = TcpStream::connect(PEER_ADDRESS).await?;

    // make the framed
    let codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();
    let mut framed = Framed::new(stream, codec);

    framed.send(payload).await?;

    // wait few seconds for peer, then drop
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    Ok(())
}
