#[cfg(not(feature = "rdma"))]
fn main() {
    eprintln!("This example requires the `rdma` feature.");
    eprintln!("Run: cargo run --release --example bench_latency --features rdma -- server 0.0.0.0:18515");
    eprintln!("  or: cargo run --release --example bench_latency --features rdma -- client <server_ip:port> [iters] [bytes]");
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
            .unwrap_or_else(|| "0.0.0.0:18515".to_string())
            .parse()
            .expect("bind addr must be SocketAddr");
        let listener = cm::listen(bind_addr, 128).await?;
        println!("bench_latency server listening on {bind_addr}");

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
    } else {
        let server_addr: SocketAddr = args
            .next()
            .unwrap_or_else(|| "127.0.0.1:18515".to_string())
            .parse()
            .expect("server addr must be SocketAddr");
        let iters: usize = args
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10_000);
        let msg_bytes: usize = args
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64);

        let transport = cm::connect(server_addr, Duration::from_secs(2)).await?;

        // Warmup to ensure receive queue is posted and caches are hot.
        for _ in 0..100 {
            transport.send(Bytes::from(vec![0u8; msg_bytes])).await?;
            let _ = transport.recv().await?;
        }

        let mut samples_us = Vec::with_capacity(iters);
        for _ in 0..iters {
            let payload = Bytes::from(vec![0u8; msg_bytes]);
            let t0 = Instant::now();
            transport.send(payload).await?;
            let _ = transport.recv().await?;
            let us = t0.elapsed().as_secs_f64() * 1e6;
            samples_us.push(us);
        }

        samples_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = samples_us[(samples_us.len() * 50) / 100];
        let p99 = samples_us[(samples_us.len() * 99) / 100];
        let min = samples_us[0];
        let max = samples_us[samples_us.len() - 1];

        println!(
            "bench_latency client: iters={} bytes={} RTT(us): min={:.2} p50={:.2} p99={:.2} max={:.2}",
            iters, msg_bytes, min, p50, p99, max
        );

        Ok(())
    }
}


