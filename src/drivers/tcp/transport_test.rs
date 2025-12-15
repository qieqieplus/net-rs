#[cfg(test)]
mod tests {
    use crate::drivers::tcp::transport::TcpTransport;
    use crate::transport::framing;
    use crate::transport::Transport;
    use bytes::Bytes;
    use std::io;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_tcp_transport_max_message_size() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            // Send a "malicious" packet: 1MB + 1 byte
            let bad_len = (framing::DEFAULT_MAX_MESSAGE_SIZE + 1) as u32;
            stream.write_u32(bad_len).await.unwrap();
            // Don't need to send actual data, the reader should fail at reading length
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let transport = TcpTransport::new(stream);

        // This should fail because the length prefix is too large
        let result = transport.recv().await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("exceeds maximum allowed size"));
    }

    #[tokio::test]
    async fn test_tcp_transport_normal_operation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            // Send a valid packet
            let msg = b"hello";
            stream.write_u32(msg.len() as u32).await.unwrap();
            stream.write_all(msg).await.unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let transport = TcpTransport::new(stream);

        let result = transport.recv().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Bytes::from_static(b"hello"));
    }
}
