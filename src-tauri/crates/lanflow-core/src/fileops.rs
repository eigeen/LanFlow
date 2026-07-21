use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Component, Path, PathBuf};

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
}

impl SnapshotRecord {
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
        self.files.iter().find(|file| file.manifest.id == file_id)
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
) -> Result<SnapshotRecord> {
    tokio::task::spawn_blocking(move || {
        let chunk_size = chunk_size.clamp(1024 * 1024, 64 * 1024 * 1024) as usize;
        let root = root.canonicalize()?;
        let mut paths = Vec::<(PathBuf, String)>::new();
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
                    paths.push((child, child_relative));
                }
            } else {
                paths.push((absolute, relative.replace('\\', "/")));
            }
        }
        paths.sort_by(|left, right| left.1.cmp(&right.1));
        paths.dedup_by(|left, right| left.0 == right.0);

        let mut files = Vec::with_capacity(paths.len());
        for (absolute_path, relative_path) in paths {
            let metadata = std::fs::symlink_metadata(&absolute_path)?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            // Stable IDs let a newly prepared snapshot reuse an existing task's
            // verified-chunk bitmap after pause, reconnect, or process restart.
            let id = blake3::hash(relative_path.as_bytes()).to_hex().to_string();
            if metadata.is_dir() {
                files.push(SnapshotFile {
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
                });
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            let (full_hash, chunks) = hash_file(&absolute_path, chunk_size)?;
            files.push(SnapshotFile {
                absolute_path,
                manifest: ManifestFile {
                    id,
                    relative_path,
                    size: metadata.len(),
                    modified_ms: modified_ms(&metadata),
                    blake3: bytes::Bytes::copy_from_slice(full_hash.as_bytes()),
                    chunks,
                    is_dir: false,
                },
            });
        }
        Ok(SnapshotRecord {
            id: uuid::Uuid::new_v4().to_string(),
            files,
        })
    })
    .await
    .map_err(|error| LanFlowError::Internal(error.to_string()))?
}

fn hash_file(path: &Path, chunk_size: usize) -> Result<(blake3::Hash, Vec<ChunkHash>)> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(chunk_size.min(8 * 1024 * 1024), file);
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
        full.update(bytes);
        let hash = blake3::hash(bytes);
        chunks.push(ChunkHash {
            index,
            offset,
            length: read as u32,
            blake3: bytes::Bytes::copy_from_slice(hash.as_bytes()),
        });
        offset += read as u64;
        index += 1;
        if read < buffer.len() {
            break;
        }
    }
    Ok((full.finalize(), chunks))
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
}
