#[cfg(not(feature = "rdma"))]
fn main() {
    eprintln!("This example requires the `rdma` feature.");
    eprintln!("Run: cargo run --example rdma_echo_server --features rdma -- 0.0.0.0:18515");
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

    let bind_addr: SocketAddr = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:18515".to_string())
        .parse()
        .expect("bind addr must be a SocketAddr, e.g. 0.0.0.0:18515");

    let listener = cm::listen(bind_addr, 128).await?;
    println!("RDMA Echo Server listening on {bind_addr} (RDMA-CM)");

    loop {
        let transport = listener.accept().await?;
        tokio::spawn(async move {
            loop {
                let msg = match transport.recv().await {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("recv error: {e}");
                        break;
                    }
                };

                if let Err(e) = transport.send(Bytes::from(msg)).await {
                    eprintln!("send error: {e}");
                    break;
                }
            }
        });
    }
}


