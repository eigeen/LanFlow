use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rayon::prelude::*;
use tokio::sync::mpsc::UnboundedSender;

use crate::db::HashCacheEntry;
use crate::error::{LanFlowError, Result};
use lanflow_protocol::protocol::wire::{ChunkHash, Entry, ManifestFile, SnapshotManifest};

#[derive(Clone, Debug)]
pub struct SnapshotFile {
    pub absolute_path: PathBuf,
    pub manifest: ManifestFile,
}

#[derive(Clone, Debug)]
pub struct SnapshotRecord {
    pub id: String,
    pub files: Vec<SnapshotFile>,
    file_index: HashMap<String, usize>,
}

#[derive(Clone, Debug)]
pub struct SnapshotBuildProgress {
    pub snapshot_id: String,
    pub phase: &'static str,
    pub scanned_entries: u64,
    pub total_entries: u64,
    pub prepared_bytes: u64,
    pub total_bytes: u64,
    pub cache_hits: u64,
    pub current_path: String,
    pub hash_workers: u32,
    pub speed_bps: u64,
}

struct HashProgress {
    snapshot_id: String,
    sender: UnboundedSender<SnapshotBuildProgress>,
    prepared_bytes: AtomicU64,
    hashed_bytes: AtomicU64,
    cache_hits: AtomicU64,
    total_entries: u64,
    total_bytes: u64,
    hash_workers: u32,
    started: Instant,
    last_emit: Mutex<Instant>,
}

impl HashProgress {
    fn add(&self, bytes: u64, cache_hit: bool, current_path: &str, force: bool) {
        let prepared_bytes = self.prepared_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let hashed_bytes = if cache_hit {
            self.hashed_bytes.load(Ordering::Relaxed)
        } else {
            self.hashed_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes
        };
        let cache_hits = if cache_hit {
            self.cache_hits.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.cache_hits.load(Ordering::Relaxed)
        };
        let mut last_emit = self
            .last_emit
            .lock()
            .expect("snapshot progress lock poisoned");
        if force || last_emit.elapsed() >= Duration::from_millis(100) {
            *last_emit = Instant::now();
            let _ = self.sender.send(SnapshotBuildProgress {
                snapshot_id: self.snapshot_id.clone(),
                phase: "hashing",
                scanned_entries: self.total_entries,
                total_entries: self.total_entries,
                prepared_bytes,
                total_bytes: self.total_bytes,
                cache_hits,
                current_path: current_path.to_owned(),
                hash_workers: self.hash_workers,
                speed_bps: (hashed_bytes as f64 / self.started.elapsed().as_secs_f64().max(0.001))
                    as u64,
            });
        }
    }
}

impl SnapshotRecord {
    pub fn new(id: String, files: Vec<SnapshotFile>) -> Self {
        let file_index = files
            .iter()
            .enumerate()
            .map(|(index, file)| (file.manifest.id.clone(), index))
            .collect();
        Self {
            id,
            files,
            file_index,
        }
    }

    pub fn wire_manifest(&self) -> SnapshotManifest {
        SnapshotManifest {
            snapshot_id: self.id.clone(),
            files: self
                .files
                .iter()
                .map(|file| file.manifest.clone())
                .collect(),
            error: String::new(),
        }
    }

    pub fn find_file(&self, file_id: &str) -> Option<&SnapshotFile> {
        self.file_index
            .get(file_id)
            .and_then(|index| self.files.get(*index))
    }
}

pub fn validate_relative_path(relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    if path.is_absolute() {
        return Err(LanFlowError::InvalidInput("不允许绝对远端路径".into()));
    }
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => clean.push(value),
            Component::CurDir if clean.as_os_str().is_empty() => {}
            _ => return Err(LanFlowError::InvalidInput("路径包含非法组件".into())),
        }
    }
    Ok(clean)
}

pub fn resolve_shared_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let root = root
        .canonicalize()
        .map_err(|error| LanFlowError::InvalidInput(format!("分享目录不可访问: {error}")))?;
    let clean = validate_relative_path(relative)?;
    let candidate = root.join(clean);
    let metadata = std::fs::symlink_metadata(&candidate)?;
    if metadata.file_type().is_symlink() {
        return Err(LanFlowError::InvalidInput("符号链接不允许访问".into()));
    }
    let canonical = candidate.canonicalize()?;
    if !canonical.starts_with(&root) {
        return Err(LanFlowError::InvalidInput("路径超出分享目录".into()));
    }
    Ok(canonical)
}

pub async fn list_entries(
    root: PathBuf,
    relative: String,
    offset: usize,
    limit: usize,
    query: String,
) -> Result<(Vec<Entry>, bool)> {
    tokio::task::spawn_blocking(move || {
        let directory = resolve_shared_path(&root, &relative)?;
        if !directory.is_dir() {
            return Err(LanFlowError::InvalidInput("远端路径不是目录".into()));
        }
        let mut entries = Vec::new();
        for item in std::fs::read_dir(directory)? {
            let item = item?;
            let metadata = item.metadata()?;
            let file_type = metadata.file_type();
            if file_type.is_symlink() || (!file_type.is_file() && !file_type.is_dir()) {
                continue;
            }
            let name = item.file_name().to_string_lossy().into_owned();
            if !query.is_empty() && !name.to_lowercase().contains(&query.to_lowercase()) {
                continue;
            }
            let relative_path = if relative.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", relative.trim_end_matches('/'), name)
            };
            entries.push(Entry {
                id: blake3::hash(relative_path.as_bytes()).to_hex().to_string(),
                raw_name: bytes::Bytes::copy_from_slice(name.as_bytes()),
                display_name: name,
                relative_path,
                is_dir: file_type.is_dir(),
                size: if file_type.is_file() {
                    metadata.len()
                } else {
                    0
                },
                modified_ms: modified_ms(&metadata),
            });
        }
        entries.sort_by(|left, right| {
            right.is_dir.cmp(&left.is_dir).then_with(|| {
                left.display_name
                    .to_lowercase()
                    .cmp(&right.display_name.to_lowercase())
            })
        });
        let has_more = entries.len() > offset.saturating_add(limit);
        Ok((
            entries.into_iter().skip(offset).take(limit).collect(),
            has_more,
        ))
    })
    .await
    .map_err(|error| LanFlowError::Internal(error.to_string()))?
}

pub async fn create_snapshot(
    root: PathBuf,
    selected: Vec<String>,
    chunk_size: u32,
    requested_hash_workers: u8,
    cache: HashMap<String, HashCacheEntry>,
    progress: UnboundedSender<SnapshotBuildProgress>,
) -> Result<(SnapshotRecord, Vec<(String, HashCacheEntry)>)> {
    tokio::task::spawn_blocking(move || {
        let chunk_size = chunk_size.clamp(1024 * 1024, 64 * 1024 * 1024) as usize;
        let hash_workers = resolve_hash_workers(requested_hash_workers);
        let root = root.canonicalize()?;
        let snapshot_id = uuid::Uuid::new_v4().to_string();
        let mut paths = BTreeMap::<String, PathBuf>::new();
        let mut scanned_entries = 0u64;
        let mut last_scan_emit = Instant::now();
        for relative in selected {
            let absolute = resolve_shared_path(&root, &relative)?;
            if absolute.is_dir() {
                for entry in jwalk::WalkDir::new(&absolute).follow_links(false) {
                    let entry = entry.map_err(|error| LanFlowError::Io(error.into()))?;
                    let file_type = entry.file_type();
                    if file_type.is_symlink() || (!file_type.is_file() && !file_type.is_dir()) {
                        continue;
                    }
                    let child = entry.path();
                    let child_relative = child
                        .strip_prefix(&root)
                        .map_err(|_| LanFlowError::InvalidInput("扫描路径越界".into()))?
                        .to_string_lossy()
                        .replace('\\', "/");
                    paths.insert(child_relative.clone(), child);
                    scanned_entries += 1;
                    if last_scan_emit.elapsed() >= Duration::from_millis(100) {
                        last_scan_emit = Instant::now();
                        let _ = progress.send(SnapshotBuildProgress {
                            snapshot_id: snapshot_id.clone(),
                            phase: "scanning",
                            scanned_entries,
                            total_entries: 0,
                            prepared_bytes: 0,
                            total_bytes: 0,
                            cache_hits: 0,
                            current_path: child_relative,
                            hash_workers: hash_workers as u32,
                            speed_bps: 0,
                        });
                    }
                }
            } else {
                paths.insert(relative.replace('\\', "/"), absolute);
                scanned_entries += 1;
            }
        }
        let mut work = Vec::with_capacity(paths.len());
        let mut total_bytes = 0u64;
        for (relative_path, absolute_path) in paths {
            let metadata = std::fs::symlink_metadata(&absolute_path)?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_file() {
                total_bytes = total_bytes.saturating_add(metadata.len());
            }
            work.push((absolute_path, relative_path, metadata));
        }
        let total_entries = work.len() as u64;
        let _ = progress.send(SnapshotBuildProgress {
            snapshot_id: snapshot_id.clone(),
            phase: "hashing",
            scanned_entries: total_entries,
            total_entries,
            prepared_bytes: 0,
            total_bytes,
            cache_hits: 0,
            current_path: String::new(),
            hash_workers: hash_workers as u32,
            speed_bps: 0,
        });

        let reporter = Arc::new(HashProgress {
            snapshot_id: snapshot_id.clone(),
            sender: progress,
            prepared_bytes: AtomicU64::new(0),
            hashed_bytes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            total_entries,
            total_bytes,
            hash_workers: hash_workers as u32,
            started: Instant::now(),
            last_emit: Mutex::new(Instant::now()),
        });
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(hash_workers)
            .thread_name(|index| format!("lanflow-hash-{index}"))
            .build()
            .map_err(|error| LanFlowError::Internal(error.to_string()))?;
        let built = pool.install(|| {
            work.into_par_iter()
                .map(|(absolute_path, relative_path, metadata)| {
                    // Stable IDs let a newly prepared snapshot reuse an existing task's
                    // verified-chunk bitmap after pause, reconnect, or process restart.
                    let id = blake3::hash(relative_path.as_bytes()).to_hex().to_string();
                    if metadata.is_dir() {
                        return Ok((
                            SnapshotFile {
                                absolute_path,
                                manifest: ManifestFile {
                                    id,
                                    relative_path,
                                    size: 0,
                                    modified_ms: modified_ms(&metadata),
                                    blake3: bytes::Bytes::new(),
                                    chunks: Vec::new(),
                                    is_dir: true,
                                },
                            },
                            None,
                        ));
                    }
                    if !metadata.is_file() {
                        return Err(LanFlowError::InvalidInput(
                            "快照中出现了不支持的文件类型".into(),
                        ));
                    }
                    let size = metadata.len();
                    let mtime = modified_ms(&metadata);
                    let cache_key = hash_cache_key(&root, &relative_path);
                    let cached = cache
                        .get(&cache_key)
                        .filter(|entry| cache_matches(entry, size, mtime, chunk_size));
                    let (full_hash, chunks, update) = if let Some(entry) = cached {
                        reporter.add(size, true, &relative_path, false);
                        (entry.full_hash.clone(), entry.chunks.clone(), None)
                    } else {
                        let reporter_for_file = reporter.clone();
                        let relative_for_progress = relative_path.clone();
                        let (full_hash, chunks) = hash_file(&absolute_path, chunk_size, |bytes| {
                            reporter_for_file.add(bytes, false, &relative_for_progress, false);
                        })?;
                        let current = std::fs::symlink_metadata(&absolute_path)?;
                        if current.len() != size || modified_ms(&current) != mtime {
                            return Err(LanFlowError::InvalidInput(format!(
                                "源文件在准备快照期间发生变化: {relative_path}"
                            )));
                        }
                        let full_hash = bytes::Bytes::copy_from_slice(full_hash.as_bytes());
                        let update = Some((
                            cache_key,
                            HashCacheEntry {
                                size,
                                modified_ms: mtime,
                                full_hash: full_hash.clone(),
                                chunks: chunks.clone(),
                            },
                        ));
                        (full_hash, chunks, update)
                    };
                    Ok((
                        SnapshotFile {
                            absolute_path,
                            manifest: ManifestFile {
                                id,
                                relative_path,
                                size,
                                modified_ms: mtime,
                                blake3: full_hash,
                                chunks,
                                is_dir: false,
                            },
                        },
                        update,
                    ))
                })
                .collect::<Result<Vec<_>>>()
        })?;
        reporter.add(0, false, "", true);
        let mut files = Vec::with_capacity(built.len());
        let mut updates = Vec::new();
        for (file, update) in built {
            files.push(file);
            if let Some(update) = update {
                updates.push(update);
            }
        }
        Ok((SnapshotRecord::new(snapshot_id, files), updates))
    })
    .await
    .map_err(|error| LanFlowError::Internal(error.to_string()))?
}

fn hash_file(
    path: &Path,
    chunk_size: usize,
    mut on_bytes: impl FnMut(u64),
) -> Result<(blake3::Hash, Vec<ChunkHash>)> {
    // The OS already caches file reads. An additional BufReader with another
    // chunk-sized buffer only adds copying for this large sequential workload.
    let mut reader = File::open(path)?;
    let mut full = blake3::Hasher::new();
    let mut chunks = Vec::new();
    let mut offset = 0u64;
    let mut index = 0u32;
    let mut buffer = vec![0u8; chunk_size];
    loop {
        let mut read = 0usize;
        while read < buffer.len() {
            let count = reader.read(&mut buffer[read..])?;
            if count == 0 {
                break;
            }
            read += count;
        }
        if read == 0 {
            break;
        }
        let bytes = &buffer[..read];
        // Whole-file and logical-chunk BLAKE3 are independent. Run them in
        // parallel, and let BLAKE3 split each large input across this snapshot's
        // dedicated Rayon pool. This also accelerates a single huge file.
        let ((), hash) = rayon::join(
            || {
                full.update_rayon(bytes);
            },
            || {
                let mut chunk_hasher = blake3::Hasher::new();
                chunk_hasher.update_rayon(bytes);
                chunk_hasher.finalize()
            },
        );
        chunks.push(ChunkHash {
            index,
            offset,
            length: read as u32,
            blake3: bytes::Bytes::copy_from_slice(hash.as_bytes()),
        });
        on_bytes(read as u64);
        offset += read as u64;
        index += 1;
        if read < buffer.len() {
            break;
        }
    }
    Ok((full.finalize(), chunks))
}

fn resolve_hash_workers(requested: u8) -> usize {
    if requested > 0 {
        return usize::from(requested.clamp(1, 32));
    }
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .clamp(2, 16)
}

fn hash_cache_key(root: &Path, relative_path: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(root.to_string_lossy().as_bytes());
    hasher.update(&[0]);
    hasher.update(relative_path.as_bytes());
    hasher.finalize().to_hex().to_string()
}

fn cache_matches(entry: &HashCacheEntry, size: u64, modified_ms: i64, chunk_size: usize) -> bool {
    if entry.size != size || entry.modified_ms != modified_ms || entry.full_hash.len() != 32 {
        return false;
    }
    let mut expected_offset = 0u64;
    for (index, chunk) in entry.chunks.iter().enumerate() {
        if chunk.index as usize != index
            || chunk.offset != expected_offset
            || chunk.blake3.len() != 32
            || chunk.length == 0
            || (expected_offset + (chunk.length as u64) < size
                && chunk.length as usize != chunk_size)
        {
            return false;
        }
        expected_offset = expected_offset.saturating_add(chunk.length as u64);
    }
    expected_offset == size && (size == 0 || !entry.chunks.is_empty())
}

fn modified_ms(metadata: &std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

pub fn safe_destination_component(component: &str) -> String {
    let upper = component.to_ascii_uppercase();
    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "LPT1", "LPT2", "LPT3", "LPT4",
    ];
    let invalid = component.is_empty()
        || component == "."
        || component == ".."
        || component.starts_with("~lf-")
        || component.ends_with(['.', ' '])
        || component.chars().any(|value| {
            matches!(value, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*') || value == '\0'
        })
        || reserved.contains(&upper.as_str());
    if invalid {
        format!("~lf-{}", hex::encode_upper(component.as_bytes()))
    } else {
        component.to_owned()
    }
}

pub fn safe_destination_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let clean = validate_relative_path(relative)?;
    let mut output = root.to_path_buf();
    for component in clean.components() {
        if let Component::Normal(value) = component {
            output.push(safe_destination_component(&value.to_string_lossy()));
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn rejects_path_traversal() {
        assert!(validate_relative_path("../secret").is_err());
        assert!(validate_relative_path("/etc/passwd").is_err());
    }

    #[test]
    fn escapes_cross_platform_reserved_names() {
        assert!(safe_destination_component("CON").starts_with("~lf-"));
        assert!(safe_destination_component("bad:name").starts_with("~lf-"));
        assert_eq!(safe_destination_component("normal.txt"), "normal.txt");
    }

    #[tokio::test]
    async fn snapshot_reuses_valid_hash_cache() {
        let root =
            std::env::temp_dir().join(format!("lanflow-cache-test-{}", uuid::Uuid::new_v4()));
        let folder = root.join("folder");
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("payload.bin"), vec![7u8; 2 * 1024 * 1024]).unwrap();

        let (progress_sender, mut progress_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (first, updates) = create_snapshot(
            root.clone(),
            vec!["folder".into()],
            1024 * 1024,
            4,
            HashMap::new(),
            progress_sender,
        )
        .await
        .unwrap();
        assert_eq!(
            first
                .files
                .iter()
                .filter(|file| !file.manifest.is_dir)
                .count(),
            1
        );
        assert_eq!(updates.len(), 1);
        while progress_receiver.try_recv().is_ok() {}

        let cache = updates.into_iter().collect::<HashMap<_, _>>();
        let (progress_sender, mut progress_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_, second_updates) = create_snapshot(
            root.clone(),
            vec!["folder".into()],
            1024 * 1024,
            4,
            cache,
            progress_sender,
        )
        .await
        .unwrap();
        assert!(second_updates.is_empty());
        let progress = std::iter::from_fn(|| progress_receiver.try_recv().ok()).collect::<Vec<_>>();
        assert!(progress.iter().any(|event| event.cache_hits == 1));

        std::fs::remove_dir_all(root).unwrap();
    }

    /// Manual local probe: `cargo test -p lanflow-core --release
    /// snapshot_hash_throughput_probe -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn snapshot_hash_throughput_probe() {
        const MIB: usize = 1024 * 1024;
        const TOTAL_MIB: usize = 256;
        let path =
            std::env::temp_dir().join(format!("lanflow-hash-probe-{}", uuid::Uuid::new_v4()));
        let mut file = File::create(&path).unwrap();
        let mut block = vec![0u8; 8 * MIB];
        for (index, byte) in block.iter_mut().enumerate() {
            *byte = (index as u8).wrapping_mul(31).wrapping_add(17);
        }
        for _ in 0..(TOTAL_MIB / 8) {
            file.write_all(&block).unwrap();
        }
        file.sync_all().unwrap();
        drop(file);

        let requested_workers = std::env::var("LANFLOW_HASH_PROBE_WORKERS")
            .ok()
            .and_then(|value| value.parse::<u8>().ok())
            .unwrap_or(0);
        let workers = resolve_hash_workers(requested_workers);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap();
        let started = Instant::now();
        let (_, chunks) = pool.install(|| hash_file(&path, 8 * MIB, |_| {})).unwrap();
        let elapsed = started.elapsed().as_secs_f64();
        eprintln!(
            "LanFlow BLAKE3 probe: {TOTAL_MIB} MiB / {elapsed:.3}s = {:.1} MiB/s ({workers} workers)",
            TOTAL_MIB as f64 / elapsed
        );
        assert_eq!(chunks.len(), TOTAL_MIB / 8);
        std::fs::remove_file(path).unwrap();
    }
}
