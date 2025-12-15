//! RDMA immediate data protocol constants and encoding/decoding functions.
//!
//! This module defines the wire protocol for control messages sent via RDMA immediate data.

/// Default maximum receive buffer size (1 MiB payload + 4-byte length header).
pub const DEFAULT_MAX_RECV_BYTES: usize = 1024 * 1024;

/// Default receive queue depth.
pub const DEFAULT_RECV_DEPTH: usize = 512;

// Immediate data values for control messages

// Tagged immediate-data protocol:
// - top byte is a magic value (to avoid collisions with arbitrary imm payloads)
// - next byte is a "kind"
// - low 16 bits are a payload (credits, etc)
pub const IMM_MAGIC: u32 = 0xD1_00_00_00;
pub const IMM_MAGIC_MASK: u32 = 0xFF_00_00_00;
pub const IMM_KIND_MASK: u32 = 0x00_FF_00_00;
pub const IMM_PAYLOAD_MASK: u32 = 0x00_00_FF_FF;
pub const IMM_KIND_CLOSE: u32 = IMM_MAGIC | (0x01 << 16);
pub const IMM_KIND_CREDIT: u32 = IMM_MAGIC | (0x02 << 16);

/// Encode a credit count into immediate data format.
#[inline]
pub fn encode_credit_imm(credits: u32) -> u32 {
    // Credits are encoded in the low 16 bits (saturating).
    IMM_KIND_CREDIT | (credits.min(u16::MAX as u32) & IMM_PAYLOAD_MASK)
}

/// Decode a credit count from immediate data format.
/// Returns `None` if the immediate data is not a credit message.
#[inline]
pub fn decode_credit_imm(imm: u32) -> Option<u32> {
    if (imm & IMM_MAGIC_MASK) != IMM_MAGIC {
        return None;
    }
    if (imm & IMM_KIND_MASK) != (IMM_KIND_CREDIT & IMM_KIND_MASK) {
        return None;
    }
    Some(imm & IMM_PAYLOAD_MASK)
}

/// Check if the immediate data indicates a CLOSE message.
#[inline]
pub fn is_close_imm(imm: u32) -> bool {
    (imm & IMM_MAGIC_MASK) == IMM_MAGIC && (imm & IMM_KIND_MASK) == (IMM_KIND_CLOSE & IMM_KIND_MASK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credit_encoding() {
        let encoded = encode_credit_imm(100);
        assert_eq!(decode_credit_imm(encoded), Some(100));
    }

    #[test]
    fn test_credit_saturation() {
        let encoded = encode_credit_imm(u32::MAX);
        assert_eq!(decode_credit_imm(encoded), Some(u16::MAX as u32));
    }

    #[test]
    fn test_close_detection() {
        assert!(is_close_imm(IMM_KIND_CLOSE));
        assert!(!is_close_imm(0));
        assert!(!is_close_imm(encode_credit_imm(100)));
    }
}
