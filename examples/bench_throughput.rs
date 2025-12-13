#[cfg(not(feature = "rdma"))]
fn main() {
    eprintln!("This example requires the `rdma` feature.");
    eprintln!("Run: cargo run --release --example bench_throughput --features rdma -- server 0.0.0.0:18516");
    eprintln!("  or: cargo run --release --example bench_throughput --features rdma -- client <server_ip:port> [seconds] [bytes]");
}

#[cfg(feature = "rdma")]
#[tokio::main]
async fn main() -> std::io::Result<()> {
    use bytes::Bytes;
    use net_rs::drivers::rdma::cm;
    use net_rs::transport::Transport;
    use std::env;
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    tracing_subscriber::fmt::init();

    let mut args = env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "client".to_string());

    if mode == "server" {
        let bind_addr: SocketAddr = args
            .next()
            .unwrap_or_else(|| "0.0.0.0:18516".to_string())
            .parse()
            .expect("bind addr must be SocketAddr");
        let listener = cm::listen(bind_addr, 128).await?;
        println!("bench_throughput server listening on {bind_addr}");

        loop {
            let transport = listener.accept().await?;
            tokio::spawn(async move {
                let mut msgs: u64 = 0;
                let mut bytes: u64 = 0;
                let start = Instant::now();
                loop {
                    let msg = match transport.recv().await {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("recv error: {e}");
                            break;
                        }
                    };
                    msgs += 1;
                    bytes += msg.len() as u64;

                    // Periodic stats
                    if msgs % 100_000 == 0 {
                        let secs = start.elapsed().as_secs_f64().max(1e-9);
                        let mps = msgs as f64 / secs;
                        let gbps = (bytes as f64 / secs) / (1024.0 * 1024.0 * 1024.0);
                        println!("server: msgs={} msg/s={:.2} GB/s={:.2}", msgs, mps, gbps);
                    }
                }
            });
        }
    } else {
        let server_addr: SocketAddr = args
            .next()
            .unwrap_or_else(|| "127.0.0.1:18516".to_string())
            .parse()
            .expect("server addr must be SocketAddr");
        let seconds: u64 = args
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let msg_bytes: usize = args
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024);

        let transport = cm::connect(server_addr, Duration::from_secs(2)).await?;

        let payload = Bytes::from(vec![0u8; msg_bytes]);
        let deadline = Instant::now() + Duration::from_secs(seconds);
        let mut msgs: u64 = 0;
        let mut bytes: u64 = 0;

        while Instant::now() < deadline {
            transport.send(payload.clone()).await?;
            msgs += 1;
            bytes += msg_bytes as u64;
        }

        let secs = seconds as f64;
        let mps = msgs as f64 / secs;
        let gbps = (bytes as f64 / secs) / (1024.0 * 1024.0 * 1024.0);
        println!(
            "bench_throughput client: seconds={} bytes={} msgs={} msg/s={:.2} GB/s={:.2}",
            seconds, msg_bytes, msgs, mps, gbps
        );
        Ok(())
    }
}


