use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use futures_util::{StreamExt, stream::FuturesUnordered};
use prost::Message;
use serde::Serialize;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::client::{MuxConnection, PeerClient};
use crate::db::Database;
use crate::discovery::now_ms;
use crate::error::{LanFlowError, Result};
use crate::fileops::safe_destination_path;
use crate::models::{ConflictPolicy, PerformanceSettings, TaskDto};
use lanflow_protocol::protocol::wire::SnapshotManifest;

#[derive(Clone)]
struct TaskControl {
    cancel: CancellationToken,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskProgressEvent {
    pub task_id: String,
    pub status: String,
    pub completed_bytes: u64,
    pub total_bytes: u64,
    pub speed_bps: u64,
    pub completed_files: u64,
    pub file_count: u64,
    pub current_file: String,
}

pub struct TaskEngine {
    db: Database,
    progress: ProgressCallback,
    active: Mutex<HashMap<String, TaskControl>>,
}

pub type ProgressCallback = Arc<dyn Fn(TaskProgressEvent) + Send + Sync>;

impl TaskEngine {
    pub fn new(db: Database, progress: ProgressCallback) -> Arc<Self> {
        Arc::new(Self {
            db,
            progress,
            active: Mutex::new(HashMap::new()),
        })
    }

    pub async fn start(
        self: &Arc<Self>,
        task: TaskDto,
        client: Arc<PeerClient>,
        manifest: SnapshotManifest,
        policy: ConflictPolicy,
        settings: PerformanceSettings,
    ) -> Result<()> {
        if self.active.lock().await.contains_key(&task.id) {
            return Err(LanFlowError::InvalidInput("任务已在运行".into()));
        }
        let encoded = base64::engine::general_purpose::STANDARD.encode(manifest.encode_to_vec());
        let total_bytes = manifest.files.iter().map(|file| file.size).sum();
        let file_count = manifest.files.iter().filter(|file| !file.is_dir).count() as u64;
        self.db
            .update_task_manifest(&task.id, encoded, total_bytes, file_count)
            .await?;
        self.db.insert_task_files(&task.id, &manifest.files).await?;
        let cancel = CancellationToken::new();
        self.active.lock().await.insert(
            task.id.clone(),
            TaskControl {
                cancel: cancel.clone(),
            },
        );
        let engine = self.clone();
        tokio::spawn(async move {
            let result = engine
                .run_task(
                    task.clone(),
                    client,
                    manifest,
                    policy,
                    settings,
                    cancel.clone(),
                )
                .await;
            if let Err(error) = result
                && !cancel.is_cancelled()
            {
                let _ = engine
                    .db
                    .update_task_state(
                        &task.id,
                        "failed",
                        task.completed_bytes,
                        0,
                        task.completed_files,
                        Some(error.to_string()),
                        now_ms(),
                    )
                    .await;
                (engine.progress)(TaskProgressEvent {
                    task_id: task.id.clone(),
                    status: "failed".into(),
                    completed_bytes: task.completed_bytes,
                    total_bytes: task.total_bytes,
                    speed_bps: 0,
                    completed_files: task.completed_files,
                    file_count: task.file_count,
                    current_file: error.to_string(),
                });
            }
            engine.active.lock().await.remove(&task.id);
        });
        Ok(())
    }

    pub async fn pause(&self, task_id: &str) -> Result<()> {
        if let Some(control) = self.active.lock().await.remove(task_id) {
            control.cancel.cancel();
        }
        self.db.set_task_status(task_id, "paused", None).await
    }

    pub async fn pause_all(&self) -> Result<()> {
        let ids: Vec<String> = self.active.lock().await.keys().cloned().collect();
        for id in ids {
            self.pause(&id).await?;
        }
        Ok(())
    }

    pub async fn cancel(
        &self,
        task_id: &str,
        delete_partial: bool,
        destination: &str,
    ) -> Result<()> {
        if let Some(control) = self.active.lock().await.remove(task_id) {
            control.cancel.cancel();
        }
        if delete_partial {
            let partial = Path::new(destination)
                .join(".lanflow-partials")
                .join(task_id);
            match tokio::fs::remove_dir_all(&partial).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        self.db.set_task_status(task_id, "cancelled", None).await
    }

    async fn run_task(
        &self,
        task: TaskDto,
        client: Arc<PeerClient>,
        manifest: SnapshotManifest,
        policy: ConflictPolicy,
        settings: PerformanceSettings,
        cancel: CancellationToken,
    ) -> Result<()> {
        let total_bytes: u64 = manifest.files.iter().map(|file| file.size).sum();
        let file_count = manifest.files.iter().filter(|file| !file.is_dir).count() as u64;
        let completed_bytes = Arc::new(AtomicU64::new(0));
        let mut completed_files = 0u64;
        let started = Instant::now();
        self.db
            .update_task_state(&task.id, "downloading", 0, 0, 0, None, now_ms())
            .await?;

        let connection_count = settings.data_connections.clamp(1, 8) as usize;
        let mut connections = Vec::<Arc<MuxConnection>>::with_capacity(connection_count);
        for _ in 0..connection_count {
            connections.push(client.data_connection().await?);
        }
        let stream_limit = connection_count * settings.streams_per_connection.clamp(1, 16) as usize;
        let semaphore = Arc::new(Semaphore::new(stream_limit));

        let destination_root = PathBuf::from(&task.destination);
        tokio::fs::create_dir_all(&destination_root).await?;
        let partial_root = destination_root.join(".lanflow-partials").join(&task.id);
        tokio::fs::create_dir_all(&partial_root).await?;

        for file in &manifest.files {
            if cancel.is_cancelled() {
                return Err(LanFlowError::Cancelled);
            }
            let target = safe_destination_path(&destination_root, &file.relative_path)?;
            if file.is_dir {
                tokio::fs::create_dir_all(&target).await?;
                continue;
            }
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if target.exists() && file_hash_matches(target.clone(), file.blake3.clone()).await? {
                completed_bytes.fetch_add(file.size, Ordering::Relaxed);
                completed_files += 1;
                self.db.mark_file_complete(&task.id, &file.id).await?;
                continue;
            }
            if target.exists() && policy == ConflictPolicy::Skip {
                completed_files += 1;
                continue;
            }
            let final_target = if target.exists() && policy == ConflictPolicy::KeepBoth {
                keep_both_path(&target)
            } else {
                target
            };
            let partial = partial_root.join(format!("{}.part", file.id));
            let base_file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .truncate(false)
                .open(&partial)
                .await?;
            base_file.set_len(file.size).await?;
            drop(base_file);

            let bitmap = self.db.chunk_bitmap(&task.id, &file.id).await?;
            let mut futures = FuturesUnordered::new();
            for chunk in &file.chunks {
                if bit_is_set(&bitmap, chunk.index as usize) {
                    completed_bytes.fetch_add(chunk.length as u64, Ordering::Relaxed);
                    continue;
                }
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| LanFlowError::Cancelled)?;
                let initial_connection =
                    connections[chunk.index as usize % connections.len()].clone();
                let client = client.clone();
                let db = self.db.clone();
                let task_id = task.id.clone();
                let snapshot_id = manifest.snapshot_id.clone();
                let file = file.clone();
                let chunk = chunk.clone();
                let partial = partial.clone();
                futures.push(tokio::spawn(async move {
                    let _permit = permit;
                    let mut last_error = None;
                    let mut connection = initial_connection;
                    for attempt in 0..3u32 {
                        let mut handle = tokio::fs::OpenOptions::new()
                            .write(true)
                            .read(true)
                            .open(&partial)
                            .await?;
                        match connection
                            .download_chunk(&snapshot_id, &file, &chunk, &mut handle)
                            .await
                        {
                            Ok(bytes) => {
                                db.mark_chunk_complete(&task_id, &file.id, chunk.index as usize)
                                    .await?;
                                return Result::<u64>::Ok(bytes);
                            }
                            Err(error) => {
                                last_error = Some(error);
                                tokio::time::sleep(Duration::from_millis(250 * 2u64.pow(attempt)))
                                    .await;
                                connection = client.data_connection().await?;
                            }
                        }
                    }
                    Err(last_error.unwrap_or_else(|| LanFlowError::Protocol("分片重试失败".into())))
                }));
            }

            let mut last_emit = Instant::now() - Duration::from_secs(1);
            while !futures.is_empty() {
                let joined = tokio::select! {
                    _ = cancel.cancelled() => return Err(LanFlowError::Cancelled),
                    value = futures.next() => value,
                };
                let Some(joined) = joined else { break };
                let bytes = joined.map_err(|error| LanFlowError::Internal(error.to_string()))??;
                let done = completed_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
                if last_emit.elapsed() >= Duration::from_millis(100) {
                    let speed = (done as f64 / started.elapsed().as_secs_f64().max(0.001)) as u64;
                    self.emit_progress(
                        &task.id,
                        "downloading",
                        done,
                        total_bytes,
                        speed,
                        completed_files,
                        file_count,
                        &file.relative_path,
                    )
                    .await?;
                    last_emit = Instant::now();
                }
            }

            let handle = tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&partial)
                .await?;
            handle.sync_all().await?;
            drop(handle);
            if !file_hash_matches(partial.clone(), file.blake3.clone()).await? {
                return Err(LanFlowError::Protocol(format!(
                    "{} 整文件校验失败",
                    file.relative_path
                )));
            }
            if final_target.exists() && policy == ConflictPolicy::Overwrite {
                tokio::fs::remove_file(&final_target).await?;
            }
            tokio::fs::rename(&partial, &final_target).await?;
            let modified = filetime::FileTime::from_unix_time(
                file.modified_ms.div_euclid(1000),
                (file.modified_ms.rem_euclid(1000) * 1_000_000) as u32,
            );
            let _ = filetime::set_file_mtime(&final_target, modified);
            completed_files += 1;
            self.db.mark_file_complete(&task.id, &file.id).await?;
        }

        let done = completed_bytes.load(Ordering::Relaxed);
        self.db
            .update_task_state(
                &task.id,
                "completed",
                done,
                0,
                completed_files,
                None,
                now_ms(),
            )
            .await?;
        self.emit_progress(
            &task.id,
            "completed",
            done,
            total_bytes,
            0,
            completed_files,
            file_count,
            "",
        )
        .await?;
        let _ = tokio::fs::remove_dir_all(partial_root).await;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_progress(
        &self,
        task_id: &str,
        status: &str,
        completed_bytes: u64,
        total_bytes: u64,
        speed_bps: u64,
        completed_files: u64,
        file_count: u64,
        current_file: &str,
    ) -> Result<()> {
        self.db
            .update_task_state(
                task_id,
                status,
                completed_bytes,
                speed_bps,
                completed_files,
                None,
                now_ms(),
            )
            .await?;
        (self.progress)(TaskProgressEvent {
            task_id: task_id.to_owned(),
            status: status.to_owned(),
            completed_bytes,
            total_bytes,
            speed_bps,
            completed_files,
            file_count,
            current_file: current_file.to_owned(),
        });
        Ok(())
    }
}

fn bit_is_set(bitmap: &[u8], index: usize) -> bool {
    bitmap
        .get(index / 8)
        .map(|byte| byte & (1 << (index % 8)) != 0)
        .unwrap_or(false)
}

async fn file_hash_matches(path: PathBuf, expected: bytes::Bytes) -> Result<bool> {
    if expected.len() != 32 {
        return Ok(false);
    }
    tokio::task::spawn_blocking(move || {
        let file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let mut reader = std::io::BufReader::new(file);
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0u8; 1024 * 1024];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(hasher.finalize().as_bytes() == expected.as_ref())
    })
    .await
    .map_err(|error| LanFlowError::Internal(error.to_string()))?
}

fn keep_both_path(path: &Path) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("file");
    let extension = path.extension().and_then(|value| value.to_str());
    for index in 1..10_000 {
        let name = match extension {
            Some(extension) => format!("{stem} (LanFlow {index}).{extension}"),
            None => format!("{stem} (LanFlow {index})"),
        };
        let candidate = parent.join(name);
        if !candidate.exists() {
            return candidate;
        }
    }
    parent.join(format!("{stem} (LanFlow {})", uuid::Uuid::new_v4()))
}
