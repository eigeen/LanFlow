use std::collections::HashMap;
use std::path::Path;

use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio_rusqlite::{Connection, params};

use crate::error::{LanFlowError, Result};
use crate::models::{PerformanceSettings, ShareDto, TaskDto};
use lanflow_protocol::protocol::wire::{ChunkHash, ManifestFile};

#[derive(Clone, Debug)]
pub struct HashCacheEntry {
    pub size: u64,
    pub modified_ms: i64,
    pub full_hash: Bytes,
    pub chunks: Vec<ChunkHash>,
}

#[derive(Serialize, Deserialize)]
struct StoredChunk {
    index: u32,
    offset: u64,
    length: u32,
    hash: String,
}

#[derive(Clone)]
pub struct Database {
    connection: Connection,
}

impl Database {
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let connection = Connection::open(path).await?;
        let database = Self { connection };
        database.initialize().await?;
        Ok(database)
    }

    async fn initialize(&self) -> Result<()> {
        self.connection
            .call(|conn| {
                conn.execute_batch(
                    "PRAGMA journal_mode=WAL;
                     PRAGMA synchronous=NORMAL;
                     PRAGMA foreign_keys=ON;
                     CREATE TABLE IF NOT EXISTS meta (
                       key TEXT PRIMARY KEY,
                       value BLOB NOT NULL
                     );
                     CREATE TABLE IF NOT EXISTS shares (
                       id TEXT PRIMARY KEY,
                       name TEXT NOT NULL,
                       path TEXT NOT NULL,
                       password_record BLOB NOT NULL,
                       enabled INTEGER NOT NULL DEFAULT 1,
                       created_at INTEGER NOT NULL
                     );
                     CREATE TABLE IF NOT EXISTS tasks (
                       id TEXT PRIMARY KEY,
                       peer_id TEXT NOT NULL,
                       peer_name TEXT NOT NULL,
                       share_id TEXT NOT NULL,
                       destination TEXT NOT NULL,
                       remote_paths TEXT NOT NULL,
                       conflict_policy TEXT NOT NULL,
                       snapshot_json TEXT,
                       status TEXT NOT NULL,
                       total_bytes INTEGER NOT NULL DEFAULT 0,
                       completed_bytes INTEGER NOT NULL DEFAULT 0,
                       speed_bps INTEGER NOT NULL DEFAULT 0,
                       file_count INTEGER NOT NULL DEFAULT 0,
                       completed_files INTEGER NOT NULL DEFAULT 0,
                       error TEXT,
                       created_at INTEGER NOT NULL,
                       updated_at INTEGER NOT NULL
                     );
                     CREATE TABLE IF NOT EXISTS task_files (
                       task_id TEXT NOT NULL,
                       file_id TEXT NOT NULL,
                       relative_path TEXT NOT NULL,
                       size INTEGER NOT NULL,
                       modified_ms INTEGER NOT NULL,
                       full_hash BLOB NOT NULL,
                       chunks_json TEXT NOT NULL,
                       completed_bitmap BLOB NOT NULL,
                       completed INTEGER NOT NULL DEFAULT 0,
                       PRIMARY KEY(task_id, file_id),
                       FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
                     );
                     CREATE TABLE IF NOT EXISTS credentials (
                       peer_id TEXT NOT NULL,
                       share_id TEXT NOT NULL,
                       nonce BLOB NOT NULL,
                       ciphertext BLOB NOT NULL,
                       PRIMARY KEY(peer_id, share_id)
                     );
                     CREATE TABLE IF NOT EXISTS hash_cache (
                       cache_key TEXT PRIMARY KEY,
                       size INTEGER NOT NULL,
                       modified_ms INTEGER NOT NULL,
                       full_hash BLOB NOT NULL,
                       chunks_json TEXT NOT NULL,
                       updated_at INTEGER NOT NULL
                     );",
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let key = key.to_owned();
        Ok(self
            .connection
            .call(move |conn| {
                let mut statement = conn.prepare("SELECT value FROM meta WHERE key=?1")?;
                let mut rows = statement.query([key])?;
                rows.next()?.map(|row| row.get(0)).transpose()
            })
            .await?)
    }

    pub async fn set_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        let key = key.to_owned();
        let value = value.to_vec();
        self.connection
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO meta(key,value) VALUES(?1,?2)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![key, value],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn load_hash_cache(&self) -> Result<HashMap<String, HashCacheEntry>> {
        Ok(self
            .connection
            .call(|conn| {
                let mut statement = conn.prepare(
                    "SELECT cache_key,size,modified_ms,full_hash,chunks_json FROM hash_cache",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?;
                let mut cache = HashMap::new();
                for row in rows {
                    let (key, size, modified_ms, full_hash, chunks_json) = row?;
                    let stored: Vec<StoredChunk> =
                        serde_json::from_str(&chunks_json).map_err(|error| {
                            tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })?;
                    let mut chunks = Vec::with_capacity(stored.len());
                    for chunk in stored {
                        let hash = base64::engine::general_purpose::STANDARD
                            .decode(chunk.hash)
                            .map_err(|error| {
                                tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(
                                    error,
                                ))
                            })?;
                        chunks.push(ChunkHash {
                            index: chunk.index,
                            offset: chunk.offset,
                            length: chunk.length,
                            blake3: Bytes::from(hash),
                        });
                    }
                    cache.insert(
                        key,
                        HashCacheEntry {
                            size,
                            modified_ms,
                            full_hash: Bytes::from(full_hash),
                            chunks,
                        },
                    );
                }
                Ok(cache)
            })
            .await?)
    }

    pub async fn store_hash_cache(&self, entries: Vec<(String, HashCacheEntry)>) -> Result<()> {
        self.connection
            .call(move |conn| {
                let transaction = conn.transaction()?;
                for (key, entry) in entries {
                    let chunks = entry
                        .chunks
                        .iter()
                        .map(|chunk| StoredChunk {
                            index: chunk.index,
                            offset: chunk.offset,
                            length: chunk.length,
                            hash: base64::engine::general_purpose::STANDARD
                                .encode(&chunk.blake3),
                        })
                        .collect::<Vec<_>>();
                    let chunks_json = serde_json::to_string(&chunks).map_err(|error| {
                        tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                    })?;
                    transaction.execute(
                        "INSERT INTO hash_cache(cache_key,size,modified_ms,full_hash,chunks_json,updated_at)
                         VALUES(?1,?2,?3,?4,?5,?6)
                         ON CONFLICT(cache_key) DO UPDATE SET
                           size=excluded.size,modified_ms=excluded.modified_ms,
                           full_hash=excluded.full_hash,chunks_json=excluded.chunks_json,
                           updated_at=excluded.updated_at",
                        params![
                            key,
                            entry.size as i64,
                            entry.modified_ms,
                            entry.full_hash.to_vec(),
                            chunks_json,
                            crate::discovery::now_ms()
                        ],
                    )?;
                }
                transaction.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn list_shares(&self) -> Result<Vec<ShareDto>> {
        Ok(self
            .connection
            .call(|conn| {
                let mut statement = conn.prepare(
                    "SELECT id,name,path,enabled,created_at FROM shares ORDER BY created_at DESC",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(ShareDto {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        path: row.get(2)?,
                        enabled: row.get::<_, i64>(3)? != 0,
                        created_at: row.get(4)?,
                    })
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?)
    }

    pub async fn put_share(&self, share: &ShareDto, password_record: &[u8]) -> Result<()> {
        let share = share.clone();
        let password_record = password_record.to_vec();
        self.connection
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO shares(id,name,path,password_record,enabled,created_at)
                     VALUES(?1,?2,?3,?4,?5,?6)
                     ON CONFLICT(id) DO UPDATE SET
                       name=excluded.name,path=excluded.path,password_record=excluded.password_record,
                       enabled=excluded.enabled",
                    params![
                        share.id,
                        share.name,
                        share.path,
                        password_record,
                        share.enabled as i64,
                        share.created_at
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn set_share_enabled(&self, id: &str, enabled: bool) -> Result<()> {
        let id = id.to_owned();
        self.connection
            .call(move |conn| {
                conn.execute(
                    "UPDATE shares SET enabled=?2 WHERE id=?1",
                    params![id, enabled as i64],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn delete_share(&self, id: &str) -> Result<()> {
        let id = id.to_owned();
        self.connection
            .call(move |conn| {
                conn.execute("DELETE FROM shares WHERE id=?1", [id])?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn share_record(&self, id: &str) -> Result<Option<(ShareDto, Vec<u8>)>> {
        let id = id.to_owned();
        Ok(self
            .connection
            .call(move |conn| {
                let mut statement = conn.prepare(
                    "SELECT id,name,path,enabled,created_at,password_record FROM shares WHERE id=?1",
                )?;
                let mut rows = statement.query([id])?;
                let Some(row) = rows.next()? else { return Ok(None) };
                Ok(Some((
                    ShareDto {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        path: row.get(2)?,
                        enabled: row.get::<_, i64>(3)? != 0,
                        created_at: row.get(4)?,
                    },
                    row.get(5)?,
                )))
            })
            .await?)
    }

    pub async fn settings(&self) -> Result<PerformanceSettings> {
        let Some(value) = self.get_meta("settings").await? else {
            return Ok(PerformanceSettings::default());
        };
        serde_json::from_slice(&value).map_err(|error| LanFlowError::Database(error.to_string()))
    }

    pub async fn save_settings(&self, settings: &PerformanceSettings) -> Result<()> {
        let value = serde_json::to_vec(settings)
            .map_err(|error| LanFlowError::Database(error.to_string()))?;
        self.set_meta("settings", &value).await
    }

    pub async fn insert_task(
        &self,
        task: &TaskDto,
        remote_paths_json: String,
        conflict_policy: String,
        snapshot_json: Option<String>,
    ) -> Result<()> {
        let task = task.clone();
        self.connection
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO tasks(id,peer_id,peer_name,share_id,destination,remote_paths,
                     conflict_policy,snapshot_json,status,total_bytes,completed_bytes,speed_bps,
                     file_count,completed_files,error,created_at,updated_at)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
                    params![
                        task.id,
                        task.peer_id,
                        task.peer_name,
                        task.share_id,
                        task.destination,
                        remote_paths_json,
                        conflict_policy,
                        snapshot_json,
                        task.status,
                        task.total_bytes as i64,
                        task.completed_bytes as i64,
                        task.speed_bps as i64,
                        task.file_count as i64,
                        task.completed_files as i64,
                        task.error,
                        task.created_at,
                        task.updated_at,
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn list_tasks(&self) -> Result<Vec<TaskDto>> {
        Ok(self
            .connection
            .call(|conn| {
                let mut statement = conn.prepare(
                    "SELECT id,peer_id,peer_name,share_id,destination,status,total_bytes,
                     completed_bytes,speed_bps,file_count,completed_files,error,created_at,updated_at
                     FROM tasks ORDER BY created_at DESC",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(TaskDto {
                        id: row.get(0)?,
                        peer_id: row.get(1)?,
                        peer_name: row.get(2)?,
                        share_id: row.get(3)?,
                        destination: row.get(4)?,
                        status: row.get(5)?,
                        total_bytes: row.get::<_, i64>(6)? as u64,
                        completed_bytes: row.get::<_, i64>(7)? as u64,
                        speed_bps: row.get::<_, i64>(8)? as u64,
                        file_count: row.get::<_, i64>(9)? as u64,
                        completed_files: row.get::<_, i64>(10)? as u64,
                        error: row.get(11)?,
                        created_at: row.get(12)?,
                        updated_at: row.get(13)?,
                    })
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_task_state(
        &self,
        id: &str,
        status: &str,
        completed_bytes: u64,
        speed_bps: u64,
        completed_files: u64,
        error: Option<String>,
        updated_at: i64,
    ) -> Result<()> {
        let id = id.to_owned();
        let status = status.to_owned();
        self.connection
            .call(move |conn| {
                conn.execute(
                    "UPDATE tasks SET status=?2,completed_bytes=?3,speed_bps=?4,
                     completed_files=?5,error=?6,updated_at=?7 WHERE id=?1",
                    params![
                        id,
                        status,
                        completed_bytes as i64,
                        speed_bps as i64,
                        completed_files as i64,
                        error,
                        updated_at
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Change lifecycle state without discarding persisted resume progress.
    pub async fn set_task_status(
        &self,
        id: &str,
        status: &str,
        error: Option<String>,
    ) -> Result<()> {
        let id = id.to_owned();
        let status = status.to_owned();
        self.connection
            .call(move |conn| {
                conn.execute(
                    "UPDATE tasks SET status=?2,speed_bps=0,error=?3,updated_at=?4 WHERE id=?1",
                    params![id, status, error, crate::discovery::now_ms()],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn update_task_manifest(
        &self,
        id: &str,
        snapshot_json: String,
        total_bytes: u64,
        file_count: u64,
    ) -> Result<()> {
        let id = id.to_owned();
        self.connection
            .call(move |conn| {
                conn.execute(
                    "UPDATE tasks SET snapshot_json=?2,total_bytes=?3,file_count=?4 WHERE id=?1",
                    params![id, snapshot_json, total_bytes as i64, file_count as i64],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn insert_task_files(&self, task_id: &str, files: &[ManifestFile]) -> Result<()> {
        let task_id = task_id.to_owned();
        let files = files.to_vec();
        self.connection.call(move |conn| {
            let transaction = conn.transaction()?;
            for file in files {
                let chunks_json = serde_json::to_string(&file.chunks.iter().map(|chunk| {
                    serde_json::json!({
                        "index": chunk.index,
                        "offset": chunk.offset,
                        "length": chunk.length,
                        "hash": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &chunk.blake3),
                    })
                }).collect::<Vec<_>>()).unwrap_or_else(|_| "[]".into());
                let bitmap_len = file.chunks.len().div_ceil(8);
                transaction.execute(
                    "INSERT INTO task_files(task_id,file_id,relative_path,size,modified_ms,
                     full_hash,chunks_json,completed_bitmap,completed) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,0)
                     ON CONFLICT(task_id,file_id) DO UPDATE SET
                       relative_path=excluded.relative_path,
                       size=excluded.size,
                       modified_ms=excluded.modified_ms,
                       chunks_json=excluded.chunks_json,
                       completed_bitmap=CASE WHEN task_files.full_hash=excluded.full_hash
                         THEN task_files.completed_bitmap ELSE excluded.completed_bitmap END,
                       completed=CASE WHEN task_files.full_hash=excluded.full_hash
                         THEN task_files.completed ELSE 0 END,
                       full_hash=excluded.full_hash",
                    params![task_id,file.id,file.relative_path,file.size as i64,file.modified_ms,
                        file.blake3.to_vec(),chunks_json,vec![0u8;bitmap_len]],
                )?;
            }
            transaction.commit()?;
            Ok(())
        }).await?;
        Ok(())
    }

    pub async fn chunk_bitmap(&self, task_id: &str, file_id: &str) -> Result<Vec<u8>> {
        let task_id = task_id.to_owned();
        let file_id = file_id.to_owned();
        Ok(self
            .connection
            .call(move |conn| {
                let mut statement = conn.prepare(
                    "SELECT completed_bitmap FROM task_files WHERE task_id=?1 AND file_id=?2",
                )?;
                let mut rows = statement.query(params![task_id, file_id])?;
                Ok(rows
                    .next()?
                    .map(|row| row.get(0))
                    .transpose()?
                    .unwrap_or_default())
            })
            .await?)
    }

    pub async fn mark_chunk_complete(
        &self,
        task_id: &str,
        file_id: &str,
        index: usize,
    ) -> Result<()> {
        let task_id = task_id.to_owned();
        let file_id = file_id.to_owned();
        self.connection
            .call(move |conn| {
                let mut bitmap: Vec<u8> = conn.query_row(
                    "SELECT completed_bitmap FROM task_files WHERE task_id=?1 AND file_id=?2",
                    params![task_id, file_id],
                    |row| row.get(0),
                )?;
                if index / 8 >= bitmap.len() {
                    bitmap.resize(index / 8 + 1, 0);
                }
                bitmap[index / 8] |= 1 << (index % 8);
                conn.execute(
                    "UPDATE task_files SET completed_bitmap=?3 WHERE task_id=?1 AND file_id=?2",
                    params![task_id, file_id, bitmap],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn mark_file_complete(&self, task_id: &str, file_id: &str) -> Result<()> {
        let task_id = task_id.to_owned();
        let file_id = file_id.to_owned();
        self.connection
            .call(move |conn| {
                conn.execute(
                    "UPDATE task_files SET completed=1 WHERE task_id=?1 AND file_id=?2",
                    params![task_id, file_id],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn task_resume_data(
        &self,
        id: &str,
    ) -> Result<Option<(String, String, String, String, String, Option<String>)>> {
        let id = id.to_owned();
        Ok(self.connection.call(move |conn| {
            let mut statement = conn.prepare(
                "SELECT peer_id,share_id,destination,remote_paths,conflict_policy,snapshot_json FROM tasks WHERE id=?1"
            )?;
            let mut rows = statement.query([id])?;
            let Some(row) = rows.next()? else { return Ok(None) };
            Ok(Some((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)))
        }).await?)
    }

    pub async fn save_credential(
        &self,
        peer_id: &str,
        share_id: &str,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> Result<()> {
        let peer_id = peer_id.to_owned();
        let share_id = share_id.to_owned();
        let nonce = nonce.to_vec();
        let ciphertext = ciphertext.to_vec();
        self.connection.call(move |conn| {
            conn.execute(
                "INSERT INTO credentials(peer_id,share_id,nonce,ciphertext) VALUES(?1,?2,?3,?4)
                 ON CONFLICT(peer_id,share_id) DO UPDATE SET nonce=excluded.nonce,ciphertext=excluded.ciphertext",
                params![peer_id,share_id,nonce,ciphertext],
            )?;
            Ok(())
        }).await?;
        Ok(())
    }

    pub async fn load_credential(
        &self,
        peer_id: &str,
        share_id: &str,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let peer_id = peer_id.to_owned();
        let share_id = share_id.to_owned();
        Ok(self
            .connection
            .call(move |conn| {
                let mut statement = conn.prepare(
                    "SELECT nonce,ciphertext FROM credentials WHERE peer_id=?1 AND share_id=?2",
                )?;
                let mut rows = statement.query(params![peer_id, share_id])?;
                let Some(row) = rows.next()? else {
                    return Ok(None);
                };
                Ok(Some((row.get(0)?, row.get(1)?)))
            })
            .await?)
    }

    pub async fn delete_credential(&self, peer_id: &str, share_id: &str) -> Result<()> {
        let peer_id = peer_id.to_owned();
        let share_id = share_id.to_owned();
        self.connection
            .call(move |conn| {
                conn.execute(
                    "DELETE FROM credentials WHERE peer_id=?1 AND share_id=?2",
                    params![peer_id, share_id],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}
