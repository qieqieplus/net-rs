# net-rs

A generic, high-performance async networking library for Rust, designed to support both TCP and RDMA transports with a unified API.

## Features

- **Unified Async Transport**: `Transport` trait for abstracting over TCP and RDMA.
- **Buffer Management**: `BufferPool` trait for efficient memory management (supporting RDMA registered memory).
- **TCP Driver**: Tokio-based implementation with length-delimited framing and max message size enforcement.
- **RDMA Driver**: High-performance implementation using the `sideway` crate.
  - **Zero-Copy Memory**: Slab allocator with pre-registered memory regions to eliminate registration overhead.
  - **Batched Receives**: Maintained depth of 512 pre-posted receive buffers to prevent RNR errors.
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
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // RDMA-CM connect: creates an RC QP and transitions it to RTS via RDMA CM events.
    let server: SocketAddr = "192.168.1.10:18515".parse().unwrap();
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
- **`drivers/tcp`**: Standard socket-based implementation with 4-byte length prefix framing.
- **`drivers/rdma`**:
  - `context.rs`: Device context and Protection Domain management.
  - `slab_allocator.rs`: Fixed-size slab allocator for zero-copy memory management.
  - `recv_pool.rs`: Manager for pre-posted receive buffers and credit replenishment.
  - `buffer.rs`: Memory Region registration and fallback allocator.
  - `transport.rs`: Queue Pair (RC) implementation and batch receive logic.
  - `poller.rs`: Background thread for CQ POLLING with fast-path completion delivery.

## License

MIT
