use net_rs::drivers::tcp::transport::TcpTransport;
use net_rs::transport::Transport;
use tokio::net::{TcpListener, TcpStream};
use std::io;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;
    println!("TCP Echo Server listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        tokio::spawn(handle_client(socket, peer));
    }
}

async fn handle_client(socket: TcpStream, peer: SocketAddr) -> io::Result<()> {
    println!("New connection from {}", peer);
    
    let transport = TcpTransport::new(socket);
    
    loop {
        match transport.recv().await {
            Ok(data) => {
                println!("Received {} bytes from {}", data.len(), peer);
                
                // Echo back
                if let Err(e) = transport.send(data).await {
                    eprintln!("Failed to send: {}", e);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Connection error from {}: {}", peer, e);
                break;
            }
        }
    }
    
    println!("Connection closed: {}", peer);
    Ok(())
}
