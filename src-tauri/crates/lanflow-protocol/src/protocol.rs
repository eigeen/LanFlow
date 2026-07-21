use prost::Message;

use crate::error::Result;

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/lanflow.v1.rs"));
}

pub const PROTOCOL_MAJOR: u16 = 1;
pub const PROTOCOL_MINOR: u16 = 0;
pub const MAX_FRAME_SIZE: usize = 1024 * 1024;
pub const DEFAULT_DATA_FRAME_SIZE: usize = 256 * 1024;
pub const DEFAULT_CHUNK_SIZE: u32 = 8 * 1024 * 1024;

pub fn encode_envelope(envelope: &wire::Envelope) -> Result<bytes::Bytes> {
    let mut data = bytes::BytesMut::with_capacity(envelope.encoded_len());
    envelope.encode(&mut data)?;
    Ok(data.freeze())
}

pub fn decode_envelope(data: &[u8]) -> Result<wire::Envelope> {
    Ok(wire::Envelope::decode(data)?)
}

pub fn envelope(payload: wire::envelope::Payload) -> wire::Envelope {
    wire::Envelope {
        payload: Some(payload),
    }
}
