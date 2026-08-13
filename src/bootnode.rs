use std::{net::IpAddr, rc::Rc, sync::atomic::{AtomicBool, Ordering}, time::Duration};

use serde::Serialize;
use tokio::{net::{TcpListener, TcpStream}, sync::{Mutex, oneshot}, time::Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use bytes::{BufMut, Bytes, BytesMut};

use futures::{SinkExt, StreamExt};

use crate::protocol::{infra_peer::ConnectionPacket,
	utils::utils::{deserialize_packet, make_framed, serialize_into}};

pub static BOOTNODE_ADDRESS: &'static str = "127.0.0.1:1100";

static UPDATED_LIST: AtomicBool = AtomicBool::new(false);

#[derive(Serialize)]
struct StoragePacket { node_type: &'static str, address: Bytes, payload: Bytes }

/// The list with all the peer addresses, stored as storage packets. 
/// They need to stored as unserialized Storage packets because 
/// bincode cannot mass-deserialize something that was serialized piece by piece
type AddressList = Rc<Mutex<Vec<StoragePacket>>>;

pub async fn start_bootnode() {
	let listener = TcpListener::bind(BOOTNODE_ADDRESS).await
		.expect("Bootnode could not start");

	// List/registry of all connected peers in the network. 
	// we use an uncontended async mutex because that's the tool that allows thread-safe tasks to work cooperatively
	let address_list: AddressList  = Rc::new(Mutex::new(Vec::with_capacity(128)));
	let update_task_list = address_list.clone();

	/* oneshots to send the leader socket from the join task to the listener task */
	let (_leader_tx, leader_rx) = oneshot::channel::<Framed<TcpStream, LengthDelimitedCodec>>();

	// Task 1 - listen for changes to registry from leader node
	let update_listener_task = tokio::task::spawn_local(async move {

		// first, before we can listen, we need to get the leader connection first
		let Ok(mut leader_socket) = leader_rx.await
			else { eprintln!("Leader failed to connect to bootnode"); return; };

		println!("Bootnode waiting for updates from the leader node...");
		while let Some(Ok(addr_list)) = leader_socket.next().await {
			let mut write_lock = update_task_list.lock().await;

			// deserialize addr list and clear out old addresses from write lock
			let Ok(addresses) = deserialize_packet::<Vec<std::net::SocketAddr>>(&addr_list) 
				else { eprintln!("leader sent bad addr list"); continue; }; write_lock.clear();

			// while there are still address bytes in the network buffer
			// copy the entire value from the network into the list buffer
			for addr in addresses {
				// derive the IP from the address
				let mut ip = match addr.ip() { IpAddr::V4(ip) => ip.octets().to_vec(),
					IpAddr::V6(v6) => v6.octets().to_vec() };

				// craft the full address using the ip and port
				ip.extend_from_slice(":".as_bytes()); ip.put_u16(addr.port());

				// make the connection packet for storage
				let packet = StoragePacket {
					node_type: "client", address: Bytes::copy_from_slice(&ip), payload: Bytes::new() };

				// extend the list with the new connection packet
				write_lock.push(packet);
			}

			// updated list - toggle updated list flag
			UPDATED_LIST.store(true, Ordering::Release);
		}

		// DEBUGGING 
		eprintln!("update listener task complete")
	});
	let join_task_list = address_list.clone();

	// Task 2 - listen to connections from new peers
	let join_task = tokio::task::spawn_local(async move {
		// create the payload buffer to store cached address lists
		let mut payload = BytesMut::with_capacity(1024);

		// create a buffer to store the result from the select
    	let mut network_buffer: Option<Result<BytesMut, std::io::Error>>;

		println!("Bootnode waiting for new nodes to join...");

		// create the option wrapper for the leader sender
		let mut leader_tx = Some(_leader_tx);
		while let Ok((socket, addr)) = listener.accept().await {
			println!("New node addr {} joined", addr.to_string());

			// create a reusable timout timer
			let sleep = tokio::time::sleep(Duration::from_millis(100));

			let deadline = Instant::now() + Duration::from_millis(100);
			tokio::pin!(sleep); sleep.as_mut().reset(deadline.into());
			// let list = &mut *join_task_list.lock().await;

			// create a framed for the connected machine - leader or peer
			// TODO - dynamically split and rebuild to change size depending of if it's leader or peer
			let mut socket_framed = make_framed(socket, 1024);

			// select between the connceted client responding and a timeout
        	tokio::select! { value = socket_framed.next() => { network_buffer = value } 
            	_ = &mut sleep => { eprintln!("client didn't respond in time, dropping"); continue; } }

			if let Some(Ok(packet)) = network_buffer {
				// error handling is inside the function. Skip this connection if the peer sends a bad packet
            	let Ok(connected_packet) = deserialize_packet::<ConnectionPacket>(&packet) 
                	else { eprintln!("peer sent a bad packet, dropping connection"); continue; };

				// acquire the lock on the list mutex 
				let list = &mut *join_task_list.lock().await;

				match connected_packet.node_type.as_ref() {
					// if the connection request is from the leader, 
					b"leader" => {
						// send the leader socket to the update listener task
						// so it can connect to the leader and start listening for updates from it
						if let Some(sender) = leader_tx {
							let _ = sender.send(socket_framed); leader_tx = None;
						} else { eprintln!("Leader socket already connected"); continue; }

						// push the connection bytes into the list
						let storage_packet = StoragePacket {
							node_type: "leader", address: Bytes::copy_from_slice(connected_packet.address),
							payload: Bytes::copy_from_slice(connected_packet.payload) };

						list.push(storage_packet); println!("Leader socket connected and verified");
					}, 

					// if the connection request is from a peer,
					b"peer-pubkey" => {
						// if the registry has been updated, rebuild the payload
						if UPDATED_LIST.load(Ordering::Acquire) {
							// clear out the old payload/address list and reserve enough space in it
							payload.clear(); payload.reserve(list.len() * size_of::<StoragePacket>());

							// serialize the address list into the payload
							serialize_into(&mut payload, list);
						} // if not, skip rebuild send the cached payload
						
						// send the full address list to the client
						let _ = socket_framed.send(payload.clone().freeze()).await;

						println!("List sent to addr");
					}, _ => { eprintln!("Bootnode received bad packet"); continue; }
				}
			}
		}

		// DEBUGGING
		eprintln!("Jointask complete ");
	});

	// if either task crashes take down the server
	tokio::select! { _ = update_listener_task => {} _ = join_task => {} }

	println!("Bootnode service closing...");
}
