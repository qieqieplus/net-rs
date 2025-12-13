#[cfg(not(feature = "rdma"))]
fn main() {
    eprintln!("This example requires the `rdma` feature.");
    eprintln!("Run: cargo run --example rdma_echo_client --features rdma -- <server_ip:port>");
}

#[cfg(feature = "rdma")]
#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    use bytes::Bytes;
    use net_rs::drivers::rdma::cm;
    use net_rs::transport::Transport;
    use std::env;
    use std::net::SocketAddr;
    use std::time::Duration;

    let server_addr: SocketAddr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:18515".to_string())
        .parse()
        .expect("server addr must be a SocketAddr, e.g. 192.168.1.10:18515");

    let transport = cm::connect(server_addr, Duration::from_secs(2)).await?;

    // RDMA requires posted receives; give the server a moment to enter its recv loop.
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..5u32 {
        let msg = format!("hello #{i}");
        println!("send: {msg}");
        transport.send(Bytes::from(msg.clone())).await?;
        let echo = transport.recv().await?;
        println!("recv: {}", String::from_utf8_lossy(&echo));
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Ok(())
}


