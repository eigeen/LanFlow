use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use futures_util::{StreamExt, stream, stream::FuturesUnordered};
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
use lanflow_protocol::protocol::wire::{ManifestFile, SnapshotManifest};

#[derive(Clone)]
struct TaskControl {
    cancel: CancellationToken,
}

struct DownloadedFile {
    id: String,
    relative_path: String,
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
        // Keep network slots busy while other files are being created, verified, or
        // renamed. This is especially important for game projects with many tiny files.
        let file_limit = stream_limit.saturating_mul(2).clamp(4, 64);
        let connection_cursor = Arc::new(AtomicUsize::new(0));

        let destination_root = PathBuf::from(&task.destination);
        tokio::fs::create_dir_all(&destination_root).await?;
        let partial_root = destination_root.join(".lanflow-partials").join(&task.id);
        tokio::fs::create_dir_all(&partial_root).await?;
        let cleanup_partial_root = partial_root.clone();

        let mut files = Vec::new();
        for file in manifest.files {
            let target = safe_destination_path(&destination_root, &file.relative_path)?;
            if file.is_dir {
                tokio::fs::create_dir_all(target).await?;
            } else {
                files.push(file);
            }
        }
        let bitmaps = self.db.task_chunk_bitmaps(&task.id).await?;
        let snapshot_id = manifest.snapshot_id;
        let task_id = task.id.clone();
        let db = self.db.clone();
        let downloads = stream::iter(files.into_iter().map(|file| {
            let client = client.clone();
            let connections = connections.clone();
            let semaphore = semaphore.clone();
            let connection_cursor = connection_cursor.clone();
            let destination_root = destination_root.clone();
            let partial_root = partial_root.clone();
            let snapshot_id = snapshot_id.clone();
            let task_id = task_id.clone();
            let db = db.clone();
            let cancel = cancel.clone();
            let completed_bytes = completed_bytes.clone();
            let bitmap = bitmaps.get(&file.id).cloned().unwrap_or_default();
            let policy = policy.clone();
            async move {
                download_file(
                    file,
                    bitmap,
                    &destination_root,
                    &partial_root,
                    policy,
                    &snapshot_id,
                    &task_id,
                    client,
                    connections,
                    semaphore,
                    connection_cursor,
                    completed_bytes,
                    cancel,
                    db,
                )
                .await
            }
        }))
        .buffer_unordered(file_limit);
        tokio::pin!(downloads);

        let mut pending_complete = Vec::with_capacity(256);
        let mut current_file = String::new();
        let mut progress_tick = tokio::time::interval(Duration::from_millis(100));
        progress_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = progress_tick.tick() => {
                    if !pending_complete.is_empty() {
                        self.db.mark_files_complete(&task.id, std::mem::take(&mut pending_complete)).await?;
                    }
                    let done = completed_bytes.load(Ordering::Relaxed);
                    let speed = (done as f64 / started.elapsed().as_secs_f64().max(0.001)) as u64;
                    self.emit_progress(
                        &task.id,
                        "downloading",
                        done,
                        total_bytes,
                        speed,
                        completed_files,
                        file_count,
                        &current_file,
                    ).await?;
                }
                result = downloads.next() => {
                    let Some(result) = result else { break; };
                    let downloaded = result?;
                    current_file = downloaded.relative_path;
                    completed_files += 1;
                    pending_complete.push(downloaded.id);
                    if pending_complete.len() >= 256 {
                        self.db.mark_files_complete(&task.id, std::mem::take(&mut pending_complete)).await?;
                    }
                }
            }
        }
        self.db
            .mark_files_complete(&task.id, pending_complete)
            .await?;

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
        let _ = tokio::fs::remove_dir_all(cleanup_partial_root).await;
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

#[allow(clippy::too_many_arguments)]
async fn download_file(
    file: ManifestFile,
    mut bitmap: Vec<u8>,
    destination_root: &Path,
    partial_root: &Path,
    policy: ConflictPolicy,
    snapshot_id: &str,
    task_id: &str,
    client: Arc<PeerClient>,
    connections: Vec<Arc<MuxConnection>>,
    semaphore: Arc<Semaphore>,
    connection_cursor: Arc<AtomicUsize>,
    completed_bytes: Arc<AtomicU64>,
    cancel: CancellationToken,
    db: Database,
) -> Result<DownloadedFile> {
    if cancel.is_cancelled() {
        return Err(LanFlowError::Cancelled);
    }
    let target = safe_destination_path(destination_root, &file.relative_path)?;
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if tokio::fs::try_exists(&target).await?
        && file_hash_matches(target.clone(), file.blake3.clone()).await?
    {
        completed_bytes.fetch_add(file.size, Ordering::Relaxed);
        return Ok(DownloadedFile {
            id: file.id,
            relative_path: file.relative_path,
        });
    }
    if tokio::fs::try_exists(&target).await? && policy == ConflictPolicy::Skip {
        completed_bytes.fetch_add(file.size, Ordering::Relaxed);
        return Ok(DownloadedFile {
            id: file.id,
            relative_path: file.relative_path,
        });
    }
    let final_target =
        if tokio::fs::try_exists(&target).await? && policy == ConflictPolicy::KeepBoth {
            keep_both_path(&target)
        } else {
            target
        };
    let partial = partial_root.join(format!("{}.part", file.id));
    let resume_usable = tokio::fs::metadata(&partial)
        .await
        .map(|metadata| metadata.len() == file.size)
        .unwrap_or(false);
    if resume_usable && bitmap.iter().any(|byte| *byte != 0) {
        bitmap = validate_completed_chunks(partial.clone(), file.chunks.clone(), bitmap).await?;
    } else {
        bitmap.fill(0);
    }
    let base_file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(&partial)
        .await?;
    base_file.set_len(file.size).await?;
    drop(base_file);

    let mut missing_chunks = 0usize;
    let mut transfers = FuturesUnordered::new();
    for chunk in &file.chunks {
        if bit_is_set(&bitmap, chunk.index as usize) {
            completed_bytes.fetch_add(chunk.length as u64, Ordering::Relaxed);
            continue;
        }
        missing_chunks += 1;
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| LanFlowError::Cancelled)?;
        let connection_index = connection_cursor.fetch_add(1, Ordering::Relaxed);
        let initial_connection = connections[connection_index % connections.len()].clone();
        let client = client.clone();
        let snapshot_id = snapshot_id.to_owned();
        let file_id = file.id.clone();
        let chunk = chunk.clone();
        let partial = partial.clone();
        let cancel = cancel.clone();
        transfers.push(tokio::spawn(async move {
            let _permit = permit;
            let mut last_error = None;
            let mut connection = initial_connection;
            for attempt in 0..3u32 {
                let mut handle = tokio::fs::OpenOptions::new()
                    .write(true)
                    .read(true)
                    .open(&partial)
                    .await?;
                let result = tokio::select! {
                    _ = cancel.cancelled() => return Err(LanFlowError::Cancelled),
                    result = connection.download_chunk(&snapshot_id, &file_id, &chunk, &mut handle) => result,
                };
                match result {
                    Ok(bytes) => return Ok((bytes, chunk.index as usize)),
                    Err(LanFlowError::Cancelled) => return Err(LanFlowError::Cancelled),
                    Err(error) => {
                        last_error = Some(error);
                        tokio::select! {
                            _ = cancel.cancelled() => return Err(LanFlowError::Cancelled),
                            _ = tokio::time::sleep(Duration::from_millis(250 * 2u64.pow(attempt))) => {}
                        }
                        connection = client.data_connection().await?;
                    }
                }
            }
            Err(last_error.unwrap_or_else(|| LanFlowError::Protocol("分片重试失败".into())))
        }));
    }

    let mut completed_indexes = Vec::with_capacity(missing_chunks);
    let mut first_error = None;
    while let Some(joined) = transfers.next().await {
        match joined.map_err(|error| LanFlowError::Internal(error.to_string()))? {
            Ok((bytes, index)) => {
                completed_bytes.fetch_add(bytes, Ordering::Relaxed);
                completed_indexes.push(index);
            }
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }

    // Small files are crash-safe without a resume checkpoint: either the final
    // rename happened and its hash is detected on restart, or the one chunk is
    // retransmitted. Avoiding fsync + SQLite per tiny file is a major win.
    if file.chunks.len() > 1 && !completed_indexes.is_empty() {
        let handle = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&partial)
            .await?;
        handle.sync_data().await?;
        drop(handle);
        db.mark_chunks_complete(task_id, &file.id, completed_indexes)
            .await?;
    }
    if let Some(error) = first_error {
        return Err(error);
    }

    let verified_by_single_chunk = file.chunks.len() == 1
        && missing_chunks == 1
        && file.chunks[0].offset == 0
        && file.chunks[0].length as u64 == file.size
        && file.chunks[0].blake3 == file.blake3;
    if !verified_by_single_chunk && !file_hash_matches(partial.clone(), file.blake3.clone()).await?
    {
        return Err(LanFlowError::Protocol(format!(
            "{} 整文件校验失败",
            file.relative_path
        )));
    }
    if tokio::fs::try_exists(&final_target).await? && policy == ConflictPolicy::Overwrite {
        tokio::fs::remove_file(&final_target).await?;
    }
    tokio::fs::rename(&partial, &final_target).await?;
    let modified = filetime::FileTime::from_unix_time(
        file.modified_ms.div_euclid(1000),
        (file.modified_ms.rem_euclid(1000) * 1_000_000) as u32,
    );
    let _ = filetime::set_file_mtime(&final_target, modified);
    Ok(DownloadedFile {
        id: file.id,
        relative_path: file.relative_path,
    })
}

async fn validate_completed_chunks(
    path: PathBuf,
    chunks: Vec<lanflow_protocol::protocol::wire::ChunkHash>,
    mut bitmap: Vec<u8>,
) -> Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        use std::io::{Seek, SeekFrom};

        let mut file = std::fs::File::open(path)?;
        let mut buffer = vec![0u8; 1024 * 1024];
        for chunk in chunks {
            let index = chunk.index as usize;
            if !bit_is_set(&bitmap, index) {
                continue;
            }
            file.seek(SeekFrom::Start(chunk.offset))?;
            let mut remaining = chunk.length as usize;
            let mut hasher = blake3::Hasher::new();
            while remaining > 0 {
                let size = remaining.min(buffer.len());
                file.read_exact(&mut buffer[..size])?;
                hasher.update(&buffer[..size]);
                remaining -= size;
            }
            if hasher.finalize().as_bytes() != chunk.blake3.as_ref()
                && let Some(byte) = bitmap.get_mut(index / 8)
            {
                *byte &= !(1 << (index % 8));
            }
        }
        Ok(bitmap)
    })
    .await
    .map_err(|error| LanFlowError::Internal(error.to_string()))?
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use lanflow_protocol::protocol::wire::ChunkHash;

    #[tokio::test]
    async fn resume_validation_clears_only_corrupt_chunks() {
        let path =
            std::env::temp_dir().join(format!("lanflow-resume-bitmap-{}", uuid::Uuid::new_v4()));
        let first = vec![7u8; 1024];
        let second = vec![11u8; 1024];
        let mut contents = first.clone();
        contents.extend_from_slice(&second);
        std::fs::write(&path, &contents).unwrap();
        let chunks = vec![
            ChunkHash {
                index: 0,
                offset: 0,
                length: first.len() as u32,
                blake3: Bytes::copy_from_slice(blake3::hash(&first).as_bytes()),
            },
            ChunkHash {
                index: 1,
                offset: first.len() as u64,
                length: second.len() as u32,
                blake3: Bytes::copy_from_slice(blake3::hash(b"corrupt expected data").as_bytes()),
            },
        ];
        let validated = validate_completed_chunks(path.clone(), chunks, vec![0b11])
            .await
            .unwrap();
        assert!(bit_is_set(&validated, 0));
        assert!(!bit_is_set(&validated, 1));
        std::fs::remove_file(path).unwrap();
    }
}
