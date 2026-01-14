//! RDMA Server Demo over Soft-RoCE
//!
//! This example demonstrates the RDMA transport as a server.
//! Run this first, then run the client example.
//!
//! Prerequisites:
//!   1. Set up Soft-RoCE: `sudo rdma link add rxe0 type rxe netdev lo`
//!   2. Verify: `rdma link` should show rxe0
//!
//! Run with:
//!   cargo run --example rdma_server --features rdma
//!
//! Then in another terminal:
//!   cargo run --example rdma_client --features rdma

use bytes::Bytes;
use std::net::SocketAddr;

#[cfg(feature = "rdma")]
use net_rs::drivers::rdma::{cm, get_device_list};
#[cfg(feature = "rdma")]
use net_rs::transport::Transport;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .init();

    #[cfg(feature = "rdma")]
    {
        let devices = get_device_list();
        println!("=== RDMA Server Demo ===");
        println!("Available RDMA devices: {:?}", devices);
        
        if devices.is_empty() {
            eprintln!("\n❌ No RDMA devices found!");
            eprintln!("To set up Soft-RoCE on loopback, run:");
            eprintln!("  sudo modprobe rdma_rxe");
            eprintln!("  sudo rdma link add rxe0 type rxe netdev lo");
            return;
        }

        println!();

        monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_timer()
            .build()
            .expect("Failed to create monoio runtime")
            .block_on(run_server());
    }

    #[cfg(not(feature = "rdma"))]
    {
        eprintln!("This example requires the 'rdma' feature.");
    }
}

#[cfg(feature = "rdma")]
async fn run_server() {
    // Use RDMA_ADDR env var or default to 0.0.0.0:19875
    let addr_str = std::env::var("RDMA_ADDR").unwrap_or_else(|_| "0.0.0.0:19875".to_string());
    let addr: SocketAddr = addr_str.parse().expect("Invalid RDMA_ADDR");
    
    println!("[server] Starting listener on {}...", addr);
    println!("[server] (set RDMA_ADDR env var to change)");
    
    let listener = match cm::listen(addr, 8).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[server] Failed to listen: {}", e);
            return;
        }
    };
    
    println!("[server] ✓ Listening! Waiting for connections...");
    println!("[server] Run `RDMA_ADDR=<your-ip>:19875 cargo run --example rdma_client` in another terminal\n");

    loop {
        match listener.accept().await {
            Ok(transport) => {
                tracing::info!("[server] ✓ Accepted new connection!");
                
                // Handle this connection
                monoio::spawn(async move {
                    handle_connection(transport).await;
                });
            }
            Err(e) => {
                eprintln!("[server] Accept error: {}", e);
                break;
            }
        }
    }
}

#[cfg(feature = "rdma")]
async fn handle_connection(transport: net_rs::drivers::rdma::RdmaTransport) {
    tracing::info!("[server] Handling connection...");
    
    // Echo loop: receive and send back
    loop {
        match transport.recv().await {
            Ok(data) => {
                if data.is_empty() {
                    tracing::info!("[server] Received empty message, closing connection.");
                    break;
                }
                
                let msg = String::from_utf8_lossy(&data);
                tracing::info!("[server] Received ({} bytes): {}", data.len(), msg);
                
                // Check for quit command
                if msg.trim() == "quit" {
                    tracing::info!("[server] Client requested quit.");
                    break;
                }
                
                // Echo back with prefix
                let response = format!("Echo: {}", msg);
                tracing::info!("[server] Sending response: {}", response);
                
                if let Err(e) = transport.send(Bytes::from(response)).await {
                    eprintln!("[server] Send error: {}", e);
                    break;
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    println!("[server] Client disconnected.");
                } else {
                    eprintln!("[server] Recv error: {}", e);
                }
                break;
            }
        }
    }
    
    println!("[server] Connection handler finished.");
}
