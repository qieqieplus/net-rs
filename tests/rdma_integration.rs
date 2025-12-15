#![cfg(feature = "rdma")]

use net_rs::drivers::rdma::cm::{listen, connect};
use net_rs::transport::Transport;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
#[ignore] // Requires RDMA hardware or Soft RoCE
async fn test_rdma_loopback() -> std::io::Result<()> {
    // 1. Start Server
    let addr = "127.0.0.1:5000".parse().unwrap();
    let listener = listen(addr, 1).await?;
    let server_handle = tokio::spawn(async move {
        let transport = listener.accept().await.expect("Accept failed");

        // Echo server
        let msg = transport.recv().await.expect("Server recv failed");
        assert_eq!(msg.as_ref(), b"Hello RDMA");
        transport.send(msg).await.expect("Server send failed");
    });

    // 2. Start Client (give server time to listen)
    sleep(Duration::from_millis(100)).await;
    let client = connect(addr, Duration::from_secs(5)).await?;

    // 3. Client Sends
    let data = bytes::Bytes::from_static(b"Hello RDMA");
    client.send(data.clone()).await?;

    // 4. Client Receives Echo
    let response = client.recv().await?;
    assert_eq!(response, data);

    server_handle.await.map_err(|e| std::io::Error::other(e.to_string()))?;

    Ok(())
}
