use std::{thread::JoinHandle, time::Duration};

use crossbeam::{channel::{unbounded}, select};
use gdt_cpus::ThreadPriority::Highest;
use crate::protocol::{bootnode, utils::utils::io_err};

pub mod protocol;
pub mod database;
pub mod file_server;


fn create_bootnode() -> tokio::io::Result<()> {
    std::thread::sleep(Duration::from_secs(1));

    let bootnode_runtime = tokio::runtime::Builder
        ::new_current_thread().enable_all().build()?;

    let bootnode_set = tokio::task::LocalSet::new();

    bootnode_set.block_on(&bootnode_runtime, bootnode::start_bootnode());

    Ok(())
}


fn create_leader_node() -> tokio::io::Result<()> {
    // make the multi-threaded reader runtime
    let network_runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("leader-runtime").enable_all().build()?;

    // start the main server on the multithreaded runtime
    network_runtime.block_on(async move {
        let _ = protocol::infra_main::start_server().await;
    });

    Ok(())
}

fn create_peer_node() -> tokio::io::Result<()> {
    println!("Peernode runtime started");

    // make the multi-threaded reader runtime
    let network_runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("peer-runtime").enable_all().build()?;

    // start the main server on the multithreaded runtime
    network_runtime.block_on(async move {
        let _ = protocol::infra_peer::discover_network().await;
    });
    Ok(())
}

//////////////////////////////////////////// TODO ////////////////////////////////////////////
/// 
/// 
/// SPLIT EACH OF THE THREE NODES INTO THEIR OWN FILES
/// THE AMOUNT OF SLEEPS IN THIS CODE IS RIDICULOUS 
fn main() -> tokio::io::Result<()> {
    let (lead_tx, lead_rx) = unbounded::<&'static str>();
    let (boot_tx, boot_rx) = unbounded::<&'static str>();
    let (peer_tx, peer_rx) = unbounded::<&'static str>();

    // Start the isolated leader node thread (which manages its own I/O pool)
    let _leader_thread: JoinHandle<tokio::io::Result<()>> = std::thread::spawn(move || { 
        let _ = create_leader_node(); lead_tx.send("fin").unwrap();

        Ok(()) }); println!("Leader node thread created");

    // Start the isolated bootnode thread
    let _bootnode_thread: JoinHandle<tokio::io::Result<()>> = std::thread::spawn(move || { 
        let _ = create_bootnode(); boot_tx.send("fin").unwrap();

        Ok(()) }); println!("Bootnode runtime started");

    // create the peernode main multithreaded runtime
    let _peernode_thread: JoinHandle<tokio::io::Result<()>> = std::thread::spawn(move || {
        
        let _ = create_peer_node(); peer_tx.send("fin").unwrap();

        Ok(()) }); 

    // Wait for the threads to prevent main from exiting immediately
    select! {
        recv(lead_rx) -> _ => println!("leader closed"),
        recv(boot_rx) -> _ => println!("bootnode closed"),
        recv(peer_rx) -> _ => println!("peernode closed")
    }

    Ok(())
}
