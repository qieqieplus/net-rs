use crate::pool::ConnectionPool;
use crate::transport::Transport;
use std::hash::Hash;
use std::io;
use std::rc::Rc;
use std::time::Duration;
use monoio::time::sleep;

/// A connection manager that handles automatic reconnection with exponential backoff.
pub struct ConnectionManager<K, T, F>
where
    K: Eq + Hash + Clone + 'static,
    T: Transport,
    F: Fn(&K) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<T>>>> + 'static,
{
    pool: ConnectionPool<K, T>,
    connector: Rc<F>,
    max_retries: usize,
    base_backoff: Duration,
}

impl<K, T, F> ConnectionManager<K, T, F>
where
    K: Eq + Hash + Clone + 'static,
    T: Transport,
    F: Fn(&K) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<T>>>> + 'static,
{
    pub fn new(
        ttl: Duration,
        connector: F,
        max_retries: usize,
        base_backoff: Duration,
    ) -> Self {
        Self {
            pool: ConnectionPool::new(ttl),
            connector: Rc::new(connector),
            max_retries,
            base_backoff,
        }
    }

    /// Get or create a connection with automatic retry.
    pub async fn get_or_connect(&self, key: K) -> io::Result<Rc<T>> {
        // Try to get from pool first
        if let Some(conn) = self.pool.get(&key).await {
            return Ok(conn);
        }

        // Connect with retries
        let mut attempts = 0;
        loop {
            match (self.connector)(&key).await {
                Ok(transport) => {
                    let conn = self.pool.insert(key, transport).await;
                    return Ok(conn);
                }
                Err(e) if attempts < self.max_retries => {
                    attempts += 1;
                    let backoff = self.base_backoff * 2_u32.pow(attempts as u32 - 1);
                    tracing::warn!(
                        "Connection attempt {} failed, retrying in {:?}: {}",
                        attempts,
                        backoff,
                        e
                    );
                    sleep(backoff).await;
                }
                Err(e) => {
                    return Err(io::Error::other(format!(
                        "Failed to connect after {} attempts: {}",
                        attempts + 1,
                        e
                    )));
                }
            }
        }
    }

    /// Remove a connection from the pool (e.g., on error).
    pub async fn invalidate(&self, key: &K) {
        self.pool.remove(key).await;
    }

    /// Clean up expired connections in the background.
    pub async fn cleanup_expired(&self) {
        self.pool.cleanup_expired().await;
    }
}

