//! RDMA Client Demo over Soft-RoCE
//!
//! This example demonstrates the RDMA transport as a client.
//! Run the server example first!
//!
//! Prerequisites:
//!   1. Set up Soft-RoCE: `sudo rdma link add rxe0 type rxe netdev lo`
//!   2. Start server: `cargo run --example rdma_server --features rdma`
//!
//! Run with:
//!   cargo run --example rdma_client --features rdma

use bytes::Bytes;
use std::net::SocketAddr;
use std::time::Duration;

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
        println!("=== RDMA Client Demo ===");
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
            .block_on(run_client());
    }

    #[cfg(not(feature = "rdma"))]
    {
        eprintln!("This example requires the 'rdma' feature.");
    }
}

#[cfg(feature = "rdma")]
async fn run_client() {
    // Use RDMA_ADDR env var or default
    let addr_str = std::env::var("RDMA_ADDR").unwrap_or_else(|_| "127.0.0.1:19875".to_string());
    let addr: SocketAddr = addr_str.parse().expect("Invalid RDMA_ADDR, format: <ip>:<port>");
    
    println!("[client] Connecting to {}...", addr);
    println!("[client] (set RDMA_ADDR env var to change)");
    
    let transport = match cm::connect(addr, Duration::from_secs(10)).await {
        Ok(t) => {
            tracing::info!("[client] ✓ Connected!");
            t
        }
        Err(e) => {
            eprintln!("[client] ✗ Connection failed: {}", e);
            eprintln!("[client] Make sure the server is running:");
            eprintln!("         cargo run --example rdma_server --features rdma");
            return;
        }
    };

    // Test messages
    let messages = [
        "Hello, RDMA!",
        "Testing message 2",
        "こんにちは (Unicode test)",
        "Final message before quit",
        "quit",
    ];

    for msg in messages.iter() {
        tracing::info!("\n[client] Sending: {}", msg);
        
        if let Err(e) = transport.send(Bytes::from(*msg)).await {
            eprintln!("[client] Send error: {}", e);
            break;
        }

        if *msg == "quit" {
            tracing::info!("[client] Sent quit command, exiting.");
            break;
        }

        // Wait for echo
        match transport.recv().await {
            Ok(echo) => {
                let echo_str = String::from_utf8_lossy(&echo);
                tracing::info!("[client] Received: {}", echo_str);
            }
            Err(e) => {
                eprintln!("[client] Recv error: {}", e);
                break;
            }
        }
    }

    println!("\n[client] ✓ Demo complete!");
    
    // Graceful shutdown
    if let Err(e) = transport.shutdown().await {
        eprintln!("[client] Shutdown error: {}", e);
    }
}
