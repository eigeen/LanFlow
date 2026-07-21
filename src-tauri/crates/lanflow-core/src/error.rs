use std::io;

#[derive(Debug, thiserror::Error)]
pub enum LanFlowError {
    #[error("I/O 错误: {0}")]
    Io(#[from] io::Error),
    #[error("协议错误: {0}")]
    Protocol(String),
    #[error("认证失败: {0}")]
    Auth(String),
    #[error("数据库错误: {0}")]
    Database(String),
    #[error("未找到: {0}")]
    NotFound(String),
    #[error("输入无效: {0}")]
    InvalidInput(String),
    #[error("任务已取消")]
    Cancelled,
    #[error("内部错误: {0}")]
    Internal(String),
}

impl From<prost::DecodeError> for LanFlowError {
    fn from(value: prost::DecodeError) -> Self {
        Self::Protocol(value.to_string())
    }
}

impl From<prost::EncodeError> for LanFlowError {
    fn from(value: prost::EncodeError) -> Self {
        Self::Protocol(value.to_string())
    }
}

impl From<lanflow_protocol::ProtocolError> for LanFlowError {
    fn from(value: lanflow_protocol::ProtocolError) -> Self {
        match value {
            lanflow_protocol::ProtocolError::Io(error) => Self::Io(error),
            lanflow_protocol::ProtocolError::Protocol(message) => Self::Protocol(message),
        }
    }
}

impl From<tokio_rusqlite::Error> for LanFlowError {
    fn from(value: tokio_rusqlite::Error) -> Self {
        Self::Database(value.to_string())
    }
}

impl From<tokio_rusqlite::rusqlite::Error> for LanFlowError {
    fn from(value: tokio_rusqlite::rusqlite::Error) -> Self {
        Self::Database(value.to_string())
    }
}

impl serde::Serialize for LanFlowError {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

pub type Result<T> = std::result::Result<T, LanFlowError>;
