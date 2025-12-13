use net_rs::drivers::tcp::transport::TcpTransport;
use net_rs::pool::manager::ConnectionManager;
use net_rs::transport::Transport;
use bytes::Bytes;
use tokio::net::TcpStream;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let server_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    
    // Create a connection manager with pooling
    let manager = ConnectionManager::new(
        Duration::from_secs(60), // TTL
        |addr: &SocketAddr| {
            let addr = *addr;
            Box::pin(async move {
                let stream = TcpStream::connect(addr).await?;
                Ok(TcpTransport::new(stream))
            })
        },
        3,                         // max retries
        Duration::from_millis(100), // base backoff
    );

    // Send multiple messages using the same pooled connection
    for i in 0..5 {
        let conn = manager.get_or_connect(server_addr).await?;
        
        let message = format!("Hello, World! Message #{}", i);
        println!("Sending: {}", message);
        
        conn.send(Bytes::from(message)).await?;
        
        let response = conn.recv().await?;
        println!("Received: {}", String::from_utf8_lossy(&response));
        
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    println!("All messages sent successfully!");
    Ok(())
}
