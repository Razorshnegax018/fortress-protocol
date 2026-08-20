use std::{io::{self, ErrorKind}, pin::Pin, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpStream, tcp::OwnedWriteHalf}, sync::mpsc, time::{Instant, Sleep}};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::protocol::{infra_main::ActorRequest, infra_peer::ConnectionPacket};

/// @util cryptographically verifies a given transacton
/// * @params pubkey, unsigned msg, signed msg - all required for signing 
/// * @params request, sender - most transactions are contained inside @param request objects
/// that will be set to a different task using the @param sender
#[inline]
pub async fn verify_transaction(
    pubkey: &[u8], unsigned_msg: &[u8], signed_msg: &[u8]
) -> io::Result<()> {
    // Step 1 - create the verifyng key from the pubkey bytes
    let key_bytes: [u8; 32] = io_err(pubkey[..].try_into())?;
    let verifying_key = io_err(VerifyingKey::from_bytes(&key_bytes))?;

    // Step 2 - Create the "signature" (signed msg) from bytes
    let signed_bytes: [u8; 64] = io_err(signed_msg[..].try_into())?;
    let signature = Signature::from_bytes(&signed_bytes);

    // Step 3 - Verify client message/vote and send to engine if valid
    io_err(verifying_key.verify(&unsigned_msg[..], &signature))?;

    Ok(())
}

/// @util shorthand for returning `tokio::io::Err(...)`
#[inline(always)]
pub fn return_err<T>(msg: &'static str) -> Result<T, std::io::Error> 
    { return Err(io::Error::new(ErrorKind::Other, msg)) }

/// @util crafts and sends a ConnectionPacket through a given TCP connection
pub async fn send_connection_packet<Writer>(
    node_type: &'static str, addr: &str, payload: Option<&[u8]>, 
    socket: &mut Framed<Writer, LengthDelimitedCodec>, pool: &mut BytesMut
) where Writer: AsyncWriteExt + Unpin {

    // craft the connection packet
	let packet = match payload {
		Some(send_payload) => ConnectionPacket {
        	address: addr.as_bytes(), payload: send_payload,
        	node_type: node_type.as_bytes() },

		None => ConnectionPacket {
        	address: addr.as_bytes(), payload: &[],
        	node_type: node_type.as_bytes() },
	};

    // serialize into bytes using the pool
    let send_packet = serialize_into(pool, &packet);

    // send the ConnectionPacket
    let _ = socket.send(send_packet.freeze()).await;
}

/// @util waits for signed prepare votes and commit certs from reader task until pBFT quorum of 2f + 1
pub async fn wait_for_quorum(
    vote_reciever: &mut mpsc::Receiver<ActorRequest>, 
    quorum_counter: &mut u32, faulty: u32, stage: &'static [u8]) {

    // if F calculates to 1 we most likely don't have enough nodes to do a real quorum calculation
    let quorum = if faulty != 1 { 2 * faulty + 1 } else { 1 };

    while let Some(value) = vote_reciever.recv().await {
        match value {
            ActorRequest::PeerVote { vote_type, signed_msg: _ } => {
                if vote_type == Bytes::from_static(stage) { 
                    *quorum_counter += 1; 
                } if *quorum_counter >= quorum { break; }
            }, _ => {}
        }
    }
}

/// @util takes a AsyncRead type - TcpStream, OwnedReadHalf - and makes a framed from it
pub fn make_framed<Reader>(socket: Reader, size: usize) -> Framed<Reader, LengthDelimitedCodec> 
    where Reader: AsyncReadExt + Unpin {
    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    Framed::with_capacity(socket, socket_codec, size)
}

/// @util takes a AsyncWrite type - TcpStream, OwnedWriteHalf - and makes a framed from it
pub fn make_write_framed<Writer>(socket: Writer, size: usize) -> Framed<Writer, LengthDelimitedCodec> 
    where Writer: AsyncWriteExt + Unpin {
    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    Framed::with_capacity(socket, socket_codec, size)
}

/// @util does an in-place deserialization of a given buffer into type T. Returns io::Result
pub fn deserialize_packet<'a, T>(buffer: &'a [u8]) -> io::Result<T> 
	where T: Deserialize<'a> {
    match bincode::deserialize::<T>(buffer) {
        Ok(packet) => Ok(packet),
        Err(_) => {
            eprintln!("Client sent bad packet");
            Err(io::Error::new(ErrorKind::InvalidData, ""))
        }
    }
}

// @util to serialize data into a reusable buffer and return said data 
pub fn serialize_into<T: Serialize>(mut pool: &mut BytesMut, data: &T) -> BytesMut {
    bincode::serialize_into((&mut pool).writer(), data).unwrap();
    return pool.split();
}

// @util Takes in any generic result and maps it to type tokio `io::Result` so ? can be used 
pub fn io_err<T, E: std::fmt::Display>(result: Result<T, E>) -> io::Result<T> {
    result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

// @util to connect to a given address max_attempts times
pub async fn connect_with_retry(addr: &str, max_attempts: u32) -> io::Result<TcpStream> {
    let mut attempt = 0;
    let mut delay = Duration::from_millis(200);

    loop {
        attempt += 1;
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) if attempt >= max_attempts => return Err(e),
            Err(_) => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        }
    }
}

/// @util takes a reference to a pinned, reusable tokio timer and resets it to start again
#[inline(always)]
pub fn reset_timer(sleep: &mut Pin<&mut Sleep>, time: u64) {
    sleep.as_mut().reset((Instant::now() + Duration::from_millis(time)).into());
}

// @util takes a reference to a framed socket and a pinned tokio timer to select between the two
pub async fn send_with_timeout(
    socket: &mut Framed<OwnedWriteHalf, LengthDelimitedCodec>,
    payload: Bytes, sleep: &mut Pin<&mut Sleep>) {
    tokio::select! {
        _ = socket.send(payload) => {}
        _ = sleep => { println!("Peer hanged, skipping"); }
    }
}
