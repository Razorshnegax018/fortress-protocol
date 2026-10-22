use std::{cell::Cell, io::{self, ErrorKind}, pin::Pin, time::Duration};
use arrayvec::ArrayVec;
use bytes::{BufMut, Bytes, BytesMut};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpStream, tcp::OwnedWriteHalf}, sync::mpsc, time::{Instant, Sleep}};
use tokio_util::{codec::{Framed, LengthDelimitedCodec}};

use crate::{infra_main::{ActorRequest, ConsensusTools}, infra_peer::{ConnectionPacket, SocketFramed}};

use heapless::index_set::FnvIndexSet;

thread_local! {
    /// thread local global static to check if the leader has responded or not
    static LEADER_RESPONSE: Cell<bool> = const { Cell::new(false) };

    /// thread local global static to check if the view change timer is up
    static VIEW_CHANGE_TICK: Cell<bool> = const { Cell::new(false) }
}

/// @util cryptographically verifies a given transaction
/// * @params pubkey, unsigned msg, signed msg - all required for signing 
/// * @params request, sender - most transactions are contained inside @param request objects
/// that will be set to a different task using the @param sender
#[inline]
pub async fn verify_transaction(
    pubkey: &[u8], unsigned_msg: &[u8], signed_msg: &[u8]
) -> io::Result<()> {
    // Step 1 - create the verifying key from the pubkey bytes
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
    node_type: &'static str, addr: &str, payload: Option<&[u8]>, id: u8,
    socket: &mut Framed<Writer, LengthDelimitedCodec>, pool: &mut BytesMut
) -> Result<(), io::Error>

where Writer: AsyncWriteExt + Unpin {

    // craft the connection packet
	let packet = match payload {
		Some(send_payload) => ConnectionPacket {
        	address: addr.as_bytes(), payload: send_payload,
        	node_type: node_type.as_bytes(), node_id: id },

		None => ConnectionPacket {
        	address: addr.as_bytes(), payload: &[],
        	node_type: node_type.as_bytes(), node_id: id },
	};

    // serialize into bytes using the pool
    pool.clear(); let send_packet = serialize_into(pool, &packet);

    // send the ConnectionPacket
    socket.send(send_packet.freeze()).await?; socket.flush().await?;

    Ok(())
}

/// @util waits for signed prepare votes and commit certs from reader task until pBFT quorum of 2f + 1
/// TODO: CHANGE RETURN SIGNATURE TO RESULT
pub async fn wait_for_quorum(
    vote_receiver: &mut mpsc::Receiver<ActorRequest>, sequence_counters: (u32, u32),
    quorum_counter: &mut u32, len: usize, faulty: u32, stage: &'static [u8]) {

    // if F is 1, we most likely don't have enough nodes to do a real quorum calculation
    // so default to a quorum of 1 + peer registry length:
    // - one for the node calling the function + all other nodes in the registry
    let quorum = if faulty == 1 { (len + 1) as u32 } else { 2 * faulty + 1 };

    // create a (stack-based) hashset to guard against vote duplication
    let mut dedup_guard: FnvIndexSet<[u8; 32], 16> = FnvIndexSet::new();

    while let Some(value) = vote_receiver.recv().await {
        match value {
            ActorRequest::PeerVote { vote_type, signed_msg: _, pubkey, payload: _ } => {

                // first check if the vote is for the right stage
                if vote_type == Bytes::from_static(stage)  {

                    // next, check if the vote hash already been counted
                    if dedup_guard.contains(&pubkey) { eprintln!("Duplication attempt detected"); continue; }
                    
                    // then, check if the sequence counters match
                    if sequence_counters.0 == sequence_counters.1 {
                        *quorum_counter += 1; dedup_guard.insert(pubkey).unwrap();
                    }

                    // finally, if quorum has been reached, stop listening for votes
                    if *quorum_counter >= quorum { break; }
                } 
            }, _ => { eprintln!("Wait for quorum got a bad ActorRequest enum type"); }
        }
    }
}


/// @util waits for signed prepare votes and commit certs from reader task until pBFT quorum of 2f + 1
/// TODO - CHANGE RETURN SIGNATURE TO RESULT<T>
pub async fn peer_wait_for_quorum(
    vote_receiver: &mut mpsc::Receiver<ActorRequest>, sequence_counters: (u32, u32),
    quorum_counter: &mut u32, faulty: u32, stage: &'static [u8], tools: &mut ConsensusTools,
    signing_key: &SigningKey, self_id: u8) {

    let mut serialization_pool = BytesMut::with_capacity(512);

    // if F calculates to 1 we most likely don't have enough nodes to do a real quorum calculation
    let quorum = if faulty != 1 { 2 * faulty + 1 } else { 1 };

    // create a hashset to guard against vote deduping
    let mut dedup_guard: FnvIndexSet<[u8; 32], 16> = FnvIndexSet::new();

    // start the view change timer - if the timer gets to the next tick without the leader responding then trigger view change
    let duration = Duration::from_millis(1500);
    let view_change_int = tokio::time::sleep(Duration::from_millis(1500));
    tokio::pin!(view_change_int);

    loop {
        tokio::select! {
            Some(value) = vote_receiver.recv() => {
                match value {
                    ActorRequest::PeerVote { vote_type, signed_msg: _, pubkey, payload } => {

                        // first check if the vote is for the right stage, or if it's a view change
                        if Bytes::from_static(stage) == vote_type {

                            // next, check if the vote hash already been counted
                            if dedup_guard.contains(&pubkey) { eprintln!("Dedup attempt detected"); continue; }

                            // then, check if the sequence counters match
                            if sequence_counters.0 == sequence_counters.1 {
                                *quorum_counter += 1; dedup_guard.insert(pubkey).unwrap();
                            }

                            // check if the leader has responded
                            if &payload[..] == b"LEADER" { LEADER_RESPONSE.set(true) }

                            // finally, if a quorum has been reached,
                            if *quorum_counter >= quorum { break; }
                        }
                    }, _ => { eprintln!("wait for quorum got bad actor request"); }
                }
            }

            // if the view change timer runs out, trigger the view change
            _ = &mut view_change_int => {

                // TODO - first, check the LEADER_RESPONSE cell to see if the timer is up
                let sleep = tokio::time::sleep(Duration::from_millis(200)); tokio::pin!(sleep);

                // create the vote payload and put the new view number and our seq counter in it
                let mut payload = BytesMut::with_capacity(8); tools.view_number += 1;

                payload.put_u32_le(tools.view_number);
                payload.put_u32_le(tools.sequence_counter);

                // craft and serialize the view change vote
                let view_change_vote = ActorRequest::PeerVote {
                    vote_type: Bytes::from_static(b"VIEW-CHANGE"),
                    signed_msg: Bytes::copy_from_slice(&signing_key.sign(b"VIEW-CHANGE").to_bytes()),
                    pubkey: signing_key.verifying_key().to_bytes(), payload: payload.freeze(),
                };

                let vote_bytes = serialize_into(&mut serialization_pool, &view_change_vote).freeze();

                // Broadcast view change message
                for index in &tools.active_indexes {
                    send_with_timeout(&mut tools.registry[*index].as_mut().unwrap(), vote_bytes.clone(), &mut sleep).await;

                    reset_timer(&mut sleep, 1500);
                }

                // the formula for the next leader is (view number + 1) mod N
                let next_leader = tools.view_number as usize % tools.len;

                // if the given node is the new leader, wait for VIEW_CHANGE quorum, but collect each vote for the next stage
                if next_leader == (self_id as usize) {
                    // once quorum has been reached, wait for quorum, but collect each vote
                    // TODO - do something about this outrageous code duplication

                    let quorum = if faulty == 1 { 2 } else { 2 * faulty + 1 };

                    let mut dedup_guard: FnvIndexSet<[u8; 32], 16> = FnvIndexSet::new();
                    let mut peer_votes = ArrayVec::<ActorRequest, 16>::new();

                    while let Some(value) = vote_receiver.recv().await {
                        match value {
                            ActorRequest::PeerVote { ref vote_type, signed_msg: _, pubkey, payload: _ } => {

                                // first check if the vote is for the right stage
                                if *vote_type == Bytes::from_static(b"VIEW-CHANGE")  {

                                    // next, check if the vote hash already been counted
                                    if dedup_guard.contains(&pubkey) { eprintln!("Duplication attempt detected"); continue; }

                                    // then, check if the sequence counters match
                                    if sequence_counters.0 == sequence_counters.1 {
                                        *quorum_counter += 1; dedup_guard.insert(pubkey).unwrap();
                                    }

                                    // add the vote to the list
                                    peer_votes.push(value);

                                    // finally, if quorum has been reached, stop listening for votes
                                    if *quorum_counter >= quorum { break; }
                                }
                            }, _ => { eprintln!("Wait for quorum got a bad ActorRequest enum type"); }
                        }
                    }

                } else {
                    // if the given node is still just a peer, then wait for new_view vote
                    reset_timer(&mut sleep, 2500);

                    tokio::select! {
                        _ = wait_for_quorum(vote_receiver, sequence_counters, quorum_counter,
                            tools.len, faulty, b"VIEW-CHANGE") => { println!("View change has been reached"); }

                        _ = &mut sleep => { return /* return_err("Time limit exceeded, prepare verification failed") */; }
                    }
                }
            }
        }
    }


    while let Some(value) = vote_receiver.recv().await {

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

pub fn create_vote(vote_type: &'static str, signing_key: &SigningKey, serialization_pool: &mut BytesMut) -> Bytes {
    let signed_prepare_vote = signing_key.sign(vote_type.as_bytes()).to_bytes();

    let prepare_vote = ActorRequest::PeerVote { 
        vote_type: Bytes::from_static(vote_type.as_bytes()), 
        signed_msg: Bytes::copy_from_slice(&signed_prepare_vote),
        pubkey: signing_key.verifying_key().to_bytes(), payload: Bytes::new()
    };

    return serialize_into(serialization_pool, &prepare_vote).freeze().clone();
}