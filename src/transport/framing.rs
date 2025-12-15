use std::io;

/// The length of the length header in bytes (u32 = 4 bytes).
pub const HEADER_LEN: usize = 4;

/// Default maximum message size (1 MiB).
/// This limits the size of the payload, excluding the header.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Validates that the frame length does not exceed the maximum allowed size.
pub fn validate_frame_len(len: usize, max_len: usize) -> io::Result<()> {
    if len > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "message length {} exceeds maximum allowed size {}",
                len, max_len
            ),
        ));
    }
    Ok(())
}
