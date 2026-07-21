use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShareDto {
    pub id: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerDto {
    pub id: String,
    pub name: String,
    pub address: String,
    pub port: u16,
    pub online: bool,
    pub manual: bool,
    pub protocol_major: u16,
    pub protocol_minor: u16,
    pub last_seen: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteShareDto {
    pub id: String,
    pub name: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteEntryDto {
    pub id: String,
    pub name: String,
    pub relative_path: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    KeepBoth,
    Overwrite,
    Skip,
}

impl ConflictPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::KeepBoth => "keep_both",
            Self::Overwrite => "overwrite",
            Self::Skip => "skip",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskInput {
    pub peer_id: String,
    pub share_id: String,
    pub remote_paths: Vec<String>,
    pub destination: String,
    pub conflict_policy: ConflictPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDto {
    pub id: String,
    pub peer_id: String,
    pub peer_name: String,
    pub share_id: String,
    pub destination: String,
    pub status: String,
    pub total_bytes: u64,
    pub completed_bytes: u64,
    pub speed_bps: u64,
    pub file_count: u64,
    pub completed_files: u64,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PerformanceSettings {
    pub automatic: bool,
    pub data_connections: u8,
    pub streams_per_connection: u8,
    pub chunk_size_mib: u32,
    /// Zero selects an automatic value based on available parallelism.
    pub hash_workers: u8,
    pub bandwidth_limit_mbps: u32,
    pub listen_port: u16,
    pub autostart: bool,
}

impl Default for PerformanceSettings {
    fn default() -> Self {
        Self {
            automatic: true,
            data_connections: 2,
            streams_per_connection: 4,
            chunk_size_mib: 8,
            hash_workers: 0,
            bandwidth_limit_mbps: 0,
            listen_port: 47_932,
            autostart: false,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppOverview {
    pub device_id: String,
    pub device_name: String,
    pub listen_port: u16,
    pub server_running: bool,
    pub shares: Vec<ShareDto>,
    pub peers: Vec<PeerDto>,
    pub tasks: Vec<TaskDto>,
    pub settings: PerformanceSettings,
}
