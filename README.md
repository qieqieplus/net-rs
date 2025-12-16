# net-rs

A generic, high-performance async networking library for Rust, designed to support both TCP and RDMA transports with a unified API.

## Features

- **Unified Async Transport**: `Transport` trait for abstracting over TCP and RDMA.
- **Buffer Management**: `BufferPool` trait for efficient memory management (supporting RDMA registered memory).
- **TCP Driver**: Tokio-based implementation with length-delimited framing.
- **RDMA Driver**: High-performance implementation using the `sideway` crate.
  - **Zero-Copy Memory**: Slab allocator with pre-registered memory regions to eliminate registration overhead.
  - **Flow Control**: Credit-based mechanism (`IBV_WR_SEND_WITH_IMM`) to prevent receiver buffer overflow.
  - **Signal Batching**: Intelligent send signaling to reduce completion queue overhead.
  - **One-Sided Ops**: Native support for RDMA READ and WRITE operations (`rdma_read`, `rdma_write`).
  - **Lock-Free Polling**: Optimized CQ poller using `DashMap` and adaptive busy-waiting.
- **Connection Pooling**: Generic `ConnectionPool` with reconnection policies and connection lifecycle management.

## Prerequisites

- **Rust**: Stable toolchain.
- **RDMA Dependencies**:
  - `libibverbs-dev` (or equivalent)
  - `librdmacm-dev` (required for RDMA-CM / `sideway::rdmacm`)
  - `clang` (for bindgen if needed, but `sideway` handles most)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
net-rs = { path = "src/lib/rs/net-rs", features = ["rdma"] }
# Or wherever you locate the crate
```

## Usage

### TCP Example

```rust
use net_rs::transport::Transport;
use net_rs::drivers::tcp::TcpTransport;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let transport = TcpTransport::connect("127.0.0.1:8080").await?;
    let buf = transport.alloc_buf(1024);
    transport.send(buf.freeze()).await?;
    Ok(())
}
```

### RDMA Example

Enable the `rdma` feature.

```rust
use net_rs::drivers::rdma::cm;
use net_rs::transport::Transport;
use net_rs::drivers::rdma::transport::TransportConfig;
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Advanced: Configure transport parameters
    let config = TransportConfig::builder()
        .with_buf_ack_batch(16)
        .with_send_signal_batch(16)
        .with_max_outstanding_sends(64);

    // RDMA-CM connect: creates an RC QP and transitions it to RTS via RDMA CM events.
    let server: SocketAddr = "192.168.1.10:18515".parse().unwrap();
    // Use cm::connect_with_config if custom config is needed (API to be exposed)
    // currently default connect() uses default config.
    let transport = cm::connect(server, Duration::from_secs(2)).await?;

    transport.send(bytes::Bytes::from_static(b"ping")).await?;
    let resp = transport.recv().await?;
    println!("got {} bytes", resp.len());
    Ok(())
}
```

### RDMA Echo Examples

Note: For RDMA-CM, binding to `127.0.0.1` usually fails; use `0.0.0.0` on the server and your real IP on the client.

```bash
# Server
cargo run --example rdma_echo_server --features rdma -- 0.0.0.0:18515

# Client (replace with server's reachable IP)
cargo run --example rdma_echo_client --features rdma -- 192.168.1.10:18515
```

## Architecture

- **`transport`**: Core traits (`Transport`, `BufferPool`) and connection logic.
- **`drivers/tcp`**: Standard socket-based implementation.
- **`drivers/rdma`**:
  - **`context`**: Device context and Protection Domain management.
  - **`slab_allocator`**: Fixed-size slab allocator for zero-copy memory management.
  - **`transport`**:
    - **`mod`**: Core RC Queue Pair logic, initialization, and lifecycle.
    - **`flow_control`**: Semaphore-based credit system.
    - **`rdma_ops`**: One-sided RDMA READ/WRITE operations.
    - **`recv_pool`**: Async receive buffer management.
  - **`poller`**: Background thread for CQ POLLING with fast-path completion delivery.

## License

MIT
