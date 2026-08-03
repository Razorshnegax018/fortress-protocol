use std::{net::IpAddr, rc::Rc, sync::atomic::{AtomicBool, Ordering}, time::Duration};

use tokio::{net::{TcpListener, TcpStream}, sync::{Mutex, oneshot}, time::Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use bytes::{BufMut, Bytes, BytesMut};

use futures::{SinkExt, StreamExt};

use crate::protocol::infra_peer::{ConnectionPacket, deserialize_packet, make_framed, serialize_into};

pub static BOOTNODE_ADDRESS: &'static str = "127.0.0.1:1100";

static UPDATED_LIST: AtomicBool = AtomicBool::new(false);

type AddressList = Rc<Mutex<Vec<Vec<u8>>>>;

pub async fn start_bootnode() {
	let listener = TcpListener::bind(BOOTNODE_ADDRESS).await
		.expect("Bootnode could not start");

	/* List/registry of all connected peers in the network */
	let address_list: AddressList  = Rc::new(Mutex::new(Vec::with_capacity(128)));
	let update_task_list = address_list.clone();

	/* oneshots to send the leader socket from the join task to the listener task */
	let (_leader_tx, leader_rx) = oneshot::channel::<Framed<TcpStream, LengthDelimitedCodec>>();

	// Task 1 - listen for changes to registry from leader node
	let update_listener_task = tokio::task::spawn_local(async move {

		// first, before we can listen, we need to get the leader connection first
		let Ok(mut leader_socket) = leader_rx.await
			else { eprintln!("Leader failed to connect to bootnode"); return; };
	
		// create a pool for incoming address packets to be serialized into
		let mut serialize_pool = BytesMut::with_capacity(1024);

		println!("Bootnode waiting for updates from the leader node...");
		while let Some(Ok(addr_list)) = leader_socket.next().await {
			let mut write_lock = update_task_list.lock().await;

			// serialize addr list and clear out old addresses from write lock
			let addresses = match bincode::deserialize::<Vec<std::net::SocketAddr>>(&addr_list) {
				Ok(addrs) => addrs, 
				Err(_) => { eprintln!("leader sent bad addr list"); continue; }
			}; write_lock.clear();

			// while there are still address bytes in the network buffer
			// copy the entire value from the network into the list buffer
			for addr in addresses {
				// split off the address and create a packet from it
				let mut ip = match addr.ip() { IpAddr::V4(ip) => ip.octets().to_vec(),
					IpAddr::V6(v6) => v6.octets().to_vec() };

				// craft the full ip address
				ip.extend_from_slice(":".as_bytes()); ip.put_u16(addr.port());

				let packet = ConnectionPacket {
					node_type: Bytes::from_static(b"client"),
					address: Bytes::from(ip), payload: Bytes::new() };

				// serialize the packet
				let data = serialize_into(&mut serialize_pool, &packet);

				// extend the list by splitting off the newly serialized packet
				write_lock.push(data.to_vec());

				// updated list - toggle updated list flag
				UPDATED_LIST.store(true, Ordering::Release);
			}
		}

		// DEBUGGING 
		eprintln!("update listener task complete")
	});
	let join_task_list = address_list.clone();

	// Task 2 - listen to connections from new tasks
	let join_task = tokio::task::spawn_local(async move {
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
			let mut socket_framed = make_framed(socket);

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
						list.push(packet.to_vec()); println!("Leader socket connected and verified");
					}, 

					b"peer" => {
						// if the registry has been updated, rebuild the payload
						if UPDATED_LIST.load(Ordering::Acquire) {
							// create buffer heap space for serialization
							let len: usize = list.iter().map(|v| v.len()).sum();

							// clear out the old payload/address list and reserve enough space in it
							payload.clear(); payload.reserve(len);

							// loop - loop through all addresses and append them to payload
							for value in list {
								payload.extend_from_slice(&value); }
						} // else skip rebuild send the cached payload
						
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
				// updated list - toggle updated list flag
				UPDATED_LIST.store(true, Ordering::Release);
			}
		}
	});

	let task_codec = socket_codec.clone();
	let join_task_list = address_list.clone();

	// Task 2 - listen to connections from new tasks
	let join_task = tokio::task::spawn_local(async move {
		let mut payload = BytesMut::with_capacity(1024);

		while let Ok((socket, _)) = listener.accept().await {
			let list = &mut *join_task_list.lock().await;
			let codec = task_codec.clone();

			let mut socket_framed = 
				Framed::new(socket, codec);

			// construct the connection packet to send to the peer node
			let leader_packet = ConnectionPacket { 
				node_type: Bytes::from_static(b"leader"), 
				address: Bytes::from_static(LEADER_ADDRESS.as_bytes()), payload: Bytes::new() };

			// serialize and send leader connection packet
			match bincode::serialize(&leader_packet) {
				Ok(packet_buffer) => {
					let _ = socket_framed.send(Bytes::from(packet_buffer)).await;
				}, Err(_) => { eprintln!("failure"); }
			}

			// if the registry has not been updated, send it
			if UPDATED_LIST.load(Ordering::Acquire) {
				// create buffer heap space for serialization
				let len: usize = list.iter().map(|v| v.len()).sum();
				payload.clear(); payload.reserve(len);

				// loop - loop through all addresses and append them to payload
				for value in list {
					payload.extend_from_slice(&value); }
			}
			
			// send the full address list to the client
			let _ = socket_framed.send(payload.clone().freeze()).await;
		}
	});

	let _ = tokio::join!(update_listener_task, join_task);

	println!("bootnode at the end of select...");
}
