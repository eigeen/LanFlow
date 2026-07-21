use std::io;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("I/O 错误: {0}")]
    Io(#[from] io::Error),
    #[error("协议错误: {0}")]
    Protocol(String),
}

impl From<prost::DecodeError> for ProtocolError {
    fn from(value: prost::DecodeError) -> Self {
        Self::Protocol(value.to_string())
    }
}

impl From<prost::EncodeError> for ProtocolError {
    fn from(value: prost::EncodeError) -> Self {
        Self::Protocol(value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ProtocolError>;
