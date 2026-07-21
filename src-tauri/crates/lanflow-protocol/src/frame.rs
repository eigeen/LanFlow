use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{ProtocolError, Result};
use crate::protocol::{MAX_FRAME_SIZE, PROTOCOL_MAJOR, PROTOCOL_MINOR};

pub const MAGIC: [u8; 4] = *b"LNF!";
pub const BASE_HEADER_LEN: usize = 32;
pub const V1_HEADER_LEN: usize = 48;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameType {
    Hello,
    VersionReject,
    Control,
    Data,
    Error,
    Cancel,
    Ping,
    Pong,
    /// A length-delimited frame added by a compatible future minor version.
    Unknown(u16),
}

impl FrameType {
    pub const fn code(self) -> u16 {
        match self {
            Self::Hello => 1,
            Self::VersionReject => 2,
            Self::Control => 3,
            Self::Data => 4,
            Self::Error => 5,
            Self::Cancel => 6,
            Self::Ping => 7,
            Self::Pong => 8,
            Self::Unknown(code) => code,
        }
    }
}

impl TryFrom<u16> for FrameType {
    type Error = ProtocolError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::VersionReject),
            3 => Ok(Self::Control),
            4 => Ok(Self::Data),
            5 => Ok(Self::Error),
            6 => Ok(Self::Cancel),
            7 => Ok(Self::Ping),
            8 => Ok(Self::Pong),
            other => Ok(Self::Unknown(other)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub major: u16,
    pub minor: u16,
    pub frame_type: FrameType,
    pub flags: u32,
    pub stream_id: u32,
    pub request_id: u64,
    pub file_offset: u64,
    pub sequence: u32,
}

impl FrameHeader {
    pub fn new(frame_type: FrameType, stream_id: u32, request_id: u64) -> Self {
        Self {
            major: PROTOCOL_MAJOR,
            minor: PROTOCOL_MINOR,
            frame_type,
            flags: 0,
            stream_id,
            request_id,
            file_offset: 0,
            sequence: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Frame {
    pub header: FrameHeader,
    pub body: Bytes,
}

pub async fn write_frame<W>(writer: &mut W, frame: &Frame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if frame.body.len() > MAX_FRAME_SIZE {
        return Err(ProtocolError::Protocol("帧正文超过上限".into()));
    }
    let mut header = BytesMut::with_capacity(V1_HEADER_LEN);
    header.put_slice(&MAGIC);
    header.put_u16(frame.header.major);
    header.put_u16(frame.header.minor);
    header.put_u16(V1_HEADER_LEN as u16);
    header.put_u16(frame.header.frame_type.code());
    header.put_u32(frame.header.flags);
    header.put_u32(frame.body.len() as u32);
    header.put_u32(frame.header.stream_id);
    header.put_u64(frame.header.request_id);
    header.put_u64(frame.header.file_offset);
    header.put_u32(frame.header.sequence);
    header.put_u32(0);
    writer.write_all(&header).await?;
    writer.write_all(&frame.body).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_frame<R>(reader: &mut R) -> Result<Frame>
where
    R: AsyncRead + Unpin,
{
    let mut base = [0u8; BASE_HEADER_LEN];
    reader.read_exact(&mut base).await?;
    let mut base = &base[..];
    let mut magic = [0u8; 4];
    base.copy_to_slice(&mut magic);
    if magic != MAGIC {
        return Err(ProtocolError::Protocol("帧 magic 不匹配".into()));
    }
    let major = base.get_u16();
    let minor = base.get_u16();
    let header_len = base.get_u16() as usize;
    let frame_type = FrameType::try_from(base.get_u16())?;
    let flags = base.get_u32();
    let body_len = base.get_u32() as usize;
    let stream_id = base.get_u32();
    let request_id = base.get_u64();

    if !(BASE_HEADER_LEN..=256).contains(&header_len) {
        return Err(ProtocolError::Protocol(format!(
            "非法帧头长度 {header_len}"
        )));
    }
    if body_len > MAX_FRAME_SIZE {
        return Err(ProtocolError::Protocol(format!(
            "帧正文 {body_len} 超过上限"
        )));
    }
    let extra_len = header_len - BASE_HEADER_LEN;
    let mut extra = vec![0u8; extra_len];
    reader.read_exact(&mut extra).await?;
    let mut extra = &extra[..];
    let (file_offset, sequence) = if extra.remaining() >= 16 {
        let offset = extra.get_u64();
        let sequence = extra.get_u32();
        let _reserved = extra.get_u32();
        (offset, sequence)
    } else {
        (0, 0)
    };
    let mut body = BytesMut::zeroed(body_len);
    reader.read_exact(&mut body).await?;
    Ok(Frame {
        header: FrameHeader {
            major,
            minor,
            frame_type,
            flags,
            stream_id,
            request_id,
            file_offset,
            sequence,
        },
        body: body.freeze(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_roundtrip_and_version_fields() {
        let frame = Frame {
            header: FrameHeader {
                file_offset: 42,
                sequence: 7,
                ..FrameHeader::new(FrameType::Data, 3, 99)
            },
            body: Bytes::from_static(b"hello"),
        };
        let mut output = Vec::new();
        write_frame(&mut output, &frame).await.unwrap();
        let golden: &[u8] = &[
            0x4c, 0x4e, 0x46, 0x21, // magic
            0x00, 0x01, 0x00, 0x00, // major, minor
            0x00, 0x30, 0x00, 0x04, // header length, data frame
            0x00, 0x00, 0x00, 0x00, // flags
            0x00, 0x00, 0x00, 0x05, // body length
            0x00, 0x00, 0x00, 0x03, // stream ID
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, // request ID
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, // file offset
            0x00, 0x00, 0x00, 0x07, // sequence
            0x00, 0x00, 0x00, 0x00, // reserved
            0x68, 0x65, 0x6c, 0x6c, 0x6f, // hello
        ];
        assert_eq!(output, golden);
        let decoded = read_frame(&mut &output[..]).await.unwrap();
        assert_eq!(decoded.header, frame.header);
        assert_eq!(decoded.body, frame.body);
    }

    #[tokio::test]
    async fn rejects_oversized_body_before_allocation() {
        let mut bytes = BytesMut::with_capacity(BASE_HEADER_LEN);
        bytes.put_slice(&MAGIC);
        bytes.put_u16(1);
        bytes.put_u16(0);
        bytes.put_u16(BASE_HEADER_LEN as u16);
        bytes.put_u16(FrameType::Data.code());
        bytes.put_u32(0);
        bytes.put_u32((MAX_FRAME_SIZE + 1) as u32);
        bytes.put_u32(0);
        bytes.put_u64(0);
        let error = read_frame(&mut &bytes[..]).await.unwrap_err();
        assert!(error.to_string().contains("超过上限"));
    }

    #[tokio::test]
    async fn unknown_minor_extension_is_length_delimited() {
        let frame = Frame {
            header: FrameHeader::new(FrameType::Unknown(0x9001), 0, 0),
            body: Bytes::from_static(b"future"),
        };
        let mut output = Vec::new();
        write_frame(&mut output, &frame).await.unwrap();
        let decoded = read_frame(&mut &output[..]).await.unwrap();
        assert_eq!(decoded.header.frame_type, FrameType::Unknown(0x9001));
        assert_eq!(decoded.body, Bytes::from_static(b"future"));
    }

    #[tokio::test]
    async fn truncated_frame_is_rejected() {
        let frame = Frame {
            header: FrameHeader::new(FrameType::Control, 1, 2),
            body: Bytes::from_static(b"control"),
        };
        let mut output = Vec::new();
        write_frame(&mut output, &frame).await.unwrap();
        output.pop();
        assert!(read_frame(&mut &output[..]).await.is_err());
    }
}
