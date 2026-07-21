use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;

use crate::auth::{LanFlowServerSetup, ServerPendingLogin, begin_server_login, mac, verify_mac};
use crate::db::Database;
use crate::error::{LanFlowError, Result};
use crate::fileops::{SnapshotRecord, create_snapshot, list_entries};
use lanflow_protocol::frame::{FLAG_MORE, Frame, FrameHeader, FrameType, read_frame, write_frame};
use lanflow_protocol::protocol::wire::envelope::Payload;
use lanflow_protocol::protocol::wire::{
    AuthChallenge, AuthResult, ChunkComplete, Error as WireError, Hello, ListEntriesResponse,
    ListSharesResponse, Pong, ShareInfo, SmallFileBatchComplete, SmallFileComplete,
    SnapshotManifestPage, SnapshotProgress,
};
use lanflow_protocol::protocol::{
    DEFAULT_CHUNK_SIZE, DEFAULT_DATA_FRAME_SIZE, MAX_FRAME_SIZE, MAX_SMALL_FILE_BATCH_BYTES,
    MAX_SMALL_FILE_BATCH_COUNT, MAX_SMALL_FILE_SIZE, PROTOCOL_MAJOR, PROTOCOL_MINOR,
    SUPPORTED_FEATURES, decode_envelope, encode_envelope, envelope,
};

struct Session {
    key: Vec<u8>,
    share_id: String,
}

struct PendingLogin {
    share_id: String,
    login: ServerPendingLogin,
}

pub struct ServerContext {
    pub db: Database,
    pub setup: Arc<LanFlowServerSetup>,
    pub device_id: String,
    pub device_name: String,
    snapshots: RwLock<HashMap<String, Arc<SnapshotRecord>>>,
    sessions: RwLock<HashMap<String, Arc<Session>>>,
}

impl ServerContext {
    pub fn new(
        db: Database,
        setup: Arc<LanFlowServerSetup>,
        device_id: String,
        device_name: String,
    ) -> Self {
        Self {
            db,
            setup,
            device_id,
            device_name,
            snapshots: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
        }
    }
}

pub async fn run_server(
    context: Arc<ServerContext>,
    port: u16,
    shutdown: CancellationToken,
) -> Result<u16> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let bound_port = listener.local_addr()?.port();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(bound_port),
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                configure_socket(&stream);
                let context = context.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(context, stream).await {
                        eprintln!("LanFlow connection ended: {error}");
                    }
                });
            }
        }
    }
}

fn configure_socket(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(20))
        .with_interval(Duration::from_secs(5))
        .with_retries(3);
    let _ = SockRef::from(stream).set_tcp_keepalive(&keepalive);
}

async fn handle_connection(context: Arc<ServerContext>, stream: TcpStream) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (sender, mut outgoing) = mpsc::channel::<Frame>(64);
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = outgoing.recv().await {
            write_frame(&mut writer, &frame).await?;
        }
        Result::<()>::Ok(())
    });

    let first = read_frame(&mut reader).await?;
    if first.header.frame_type != FrameType::Hello {
        return Err(LanFlowError::Protocol("连接首帧必须是 Hello".into()));
    }
    let hello = decode_envelope(&first.body)?;
    let Some(Payload::Hello(client_hello)) = hello.payload else {
        return Err(LanFlowError::Protocol("Hello 帧正文无效".into()));
    };
    if client_hello.min_major > PROTOCOL_MAJOR as u32
        || client_hello.max_major < PROTOCOL_MAJOR as u32
    {
        let payload = Payload::VersionReject(lanflow_protocol::protocol::wire::VersionReject {
            min_major: PROTOCOL_MAJOR as u32,
            max_major: PROTOCOL_MAJOR as u32,
            reason: "不支持的协议主版本".into(),
        });
        send_envelope(&sender, first.header, FrameType::VersionReject, payload).await?;
        return Err(LanFlowError::Protocol("协议主版本不兼容".into()));
    }
    let supports_snapshot_pages = client_hello.max_minor >= 1;
    send_envelope(
        &sender,
        first.header,
        FrameType::Hello,
        Payload::Hello(Hello {
            device_id: context.device_id.clone(),
            device_name: context.device_name.clone(),
            min_major: PROTOCOL_MAJOR as u32,
            max_major: PROTOCOL_MAJOR as u32,
            max_minor: PROTOCOL_MINOR as u32,
            features: SUPPORTED_FEATURES,
            max_frame_size: MAX_FRAME_SIZE as u32,
            max_concurrent_streams: 16,
        }),
    )
    .await?;

    let mut pending = HashMap::<Vec<u8>, PendingLogin>::new();
    let mut active_session: Option<Arc<Session>> = None;
    loop {
        let frame = match read_frame(&mut reader).await {
            Ok(frame) => frame,
            Err(lanflow_protocol::ProtocolError::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                break;
            }
            Err(error) => return Err(error.into()),
        };
        if frame.header.major != PROTOCOL_MAJOR {
            send_error(&sender, &frame.header, 1001, "连接期间协议版本改变", false).await?;
            break;
        }
        if matches!(frame.header.frame_type, FrameType::Unknown(_)) {
            continue;
        }
        let message = decode_envelope(&frame.body)?;
        let Some(payload) = message.payload else {
            send_error(&sender, &frame.header, 1002, "空控制消息", false).await?;
            continue;
        };
        match payload {
            Payload::ListSharesRequest(_) => {
                let shares = context
                    .db
                    .list_shares()
                    .await?
                    .into_iter()
                    .filter(|share| share.enabled)
                    .map(|share| ShareInfo {
                        id: share.id,
                        name: share.name,
                        enabled: share.enabled,
                    })
                    .collect();
                send_envelope(
                    &sender,
                    frame.header,
                    FrameType::Control,
                    Payload::ListSharesResponse(ListSharesResponse { shares }),
                )
                .await?;
            }
            Payload::AuthStart(request) => {
                let Some((share, record)) = context.db.share_record(&request.share_id).await?
                else {
                    send_error(&sender, &frame.header, 2001, "分享不存在", false).await?;
                    continue;
                };
                if !share.enabled {
                    send_error(&sender, &frame.header, 2002, "分享已停用", false).await?;
                    continue;
                }
                let (response, login) = begin_server_login(
                    &context.setup,
                    &request.share_id,
                    &record,
                    &request.credential_request,
                )?;
                let challenge = uuid::Uuid::new_v4().as_bytes().to_vec();
                pending.insert(
                    challenge.clone(),
                    PendingLogin {
                        share_id: request.share_id,
                        login,
                    },
                );
                send_envelope(
                    &sender,
                    frame.header,
                    FrameType::Control,
                    Payload::AuthChallenge(AuthChallenge {
                        credential_response: Bytes::from(response),
                        challenge_id: Bytes::copy_from_slice(&challenge),
                    }),
                )
                .await?;
            }
            Payload::AuthFinish(request) => {
                let Some(login) = pending.remove(request.challenge_id.as_ref()) else {
                    send_error(&sender, &frame.header, 2003, "认证挑战已失效", true).await?;
                    continue;
                };
                match login.login.finish(&request.credential_finalization) {
                    Ok(key) => {
                        let session_id = uuid::Uuid::new_v4().to_string();
                        let proof = mac(&key, &[b"lanflow-auth-ok", session_id.as_bytes()])?;
                        let session = Arc::new(Session {
                            key,
                            share_id: login.share_id,
                        });
                        context
                            .sessions
                            .write()
                            .await
                            .insert(session_id.clone(), session.clone());
                        active_session = Some(session);
                        send_envelope(
                            &sender,
                            frame.header,
                            FrameType::Control,
                            Payload::AuthResult(AuthResult {
                                success: true,
                                session_id,
                                server_proof: Bytes::from(proof),
                                error: String::new(),
                            }),
                        )
                        .await?;
                    }
                    Err(_) => {
                        send_envelope(
                            &sender,
                            frame.header,
                            FrameType::Control,
                            Payload::AuthResult(AuthResult {
                                success: false,
                                session_id: String::new(),
                                server_proof: Bytes::new(),
                                error: "密码不正确".into(),
                            }),
                        )
                        .await?;
                    }
                }
            }
            Payload::SessionAttach(request) => {
                let session = context
                    .sessions
                    .read()
                    .await
                    .get(&request.session_id)
                    .cloned();
                let Some(session) = session else {
                    send_error(&sender, &frame.header, 2004, "会话不存在", true).await?;
                    continue;
                };
                verify_mac(
                    &session.key,
                    &[
                        b"lanflow-attach",
                        request.session_id.as_bytes(),
                        &request.nonce,
                    ],
                    &request.proof,
                )?;
                active_session = Some(session);
                send_envelope(
                    &sender,
                    frame.header,
                    FrameType::Control,
                    Payload::AuthResult(AuthResult {
                        success: true,
                        session_id: request.session_id,
                        server_proof: Bytes::new(),
                        error: String::new(),
                    }),
                )
                .await?;
            }
            Payload::ListEntriesRequest(request) => {
                let session = require_session(&active_session, &request.share_id)?;
                let Some((share, _)) = context.db.share_record(&session.share_id).await? else {
                    send_error(&sender, &frame.header, 3001, "分享不存在", false).await?;
                    continue;
                };
                let (entries, has_more) = list_entries(
                    share.path.into(),
                    request.relative_path,
                    request.offset as usize,
                    request.limit.clamp(1, 1000) as usize,
                    request.query,
                )
                .await?;
                let next_offset = request.offset.saturating_add(entries.len() as u32);
                send_envelope(
                    &sender,
                    frame.header,
                    FrameType::Control,
                    Payload::ListEntriesResponse(ListEntriesResponse {
                        entries,
                        has_more,
                        next_offset,
                    }),
                )
                .await?;
            }
            Payload::CreateSnapshotRequest(request) => {
                let session = require_session(&active_session, &request.share_id)?;
                let Some((share, _)) = context.db.share_record(&session.share_id).await? else {
                    send_error(&sender, &frame.header, 3001, "分享不存在", false).await?;
                    continue;
                };
                let hash_workers = context.db.settings().await?.hash_workers;
                let cache = context.db.load_hash_cache().await?;
                let (progress_sender, mut progress_receiver) =
                    tokio::sync::mpsc::unbounded_channel();
                let build = create_snapshot(
                    share.path.into(),
                    request.relative_paths,
                    if request.chunk_size == 0 {
                        DEFAULT_CHUNK_SIZE
                    } else {
                        request.chunk_size
                    },
                    hash_workers,
                    cache,
                    progress_sender,
                );
                tokio::pin!(build);
                let (snapshot, cache_updates) = loop {
                    tokio::select! {
                        Some(progress) = progress_receiver.recv() => {
                            if supports_snapshot_pages {
                                send_envelope_with_flags(
                                    &sender,
                                    frame.header.clone(),
                                    FrameType::Control,
                                    FLAG_MORE,
                                    Payload::SnapshotProgress(SnapshotProgress {
                                        snapshot_id: progress.snapshot_id,
                                        phase: progress.phase.into(),
                                        scanned_entries: progress.scanned_entries,
                                        total_entries: progress.total_entries,
                                        prepared_bytes: progress.prepared_bytes,
                                        total_bytes: progress.total_bytes,
                                        cache_hits: progress.cache_hits,
                                        current_path: progress.current_path,
                                        hash_workers: progress.hash_workers,
                                        speed_bps: progress.speed_bps,
                                    }),
                                ).await?;
                            }
                        }
                        result = &mut build => break result?,
                    }
                };
                context.db.store_hash_cache(cache_updates).await?;
                let snapshot = Arc::new(snapshot);
                context
                    .snapshots
                    .write()
                    .await
                    .insert(snapshot.id.clone(), snapshot.clone());
                if supports_snapshot_pages {
                    send_snapshot_pages(&sender, frame.header, &snapshot).await?;
                } else {
                    send_envelope(
                        &sender,
                        frame.header,
                        FrameType::Control,
                        Payload::SnapshotManifest(snapshot.wire_manifest()),
                    )
                    .await?;
                }
            }
            Payload::SmallFileBatchRequest(request) => {
                let _session = active_session
                    .as_ref()
                    .ok_or_else(|| LanFlowError::Auth("请先认证".into()))?;
                let snapshot = context
                    .snapshots
                    .read()
                    .await
                    .get(&request.snapshot_id)
                    .cloned();
                let Some(snapshot) = snapshot else {
                    send_error(&sender, &frame.header, 4001, "快照不存在或已过期", true).await?;
                    continue;
                };
                let files = match prepare_small_file_batch(&snapshot, &request.file_ids) {
                    Ok(files) => files,
                    Err(error) => {
                        send_error(&sender, &frame.header, 4005, &error.to_string(), false).await?;
                        continue;
                    }
                };
                let sender = sender.clone();
                let header = frame.header;
                tokio::spawn(async move {
                    if let Err(error) =
                        serve_small_file_batch(sender.clone(), header.clone(), files).await
                    {
                        let _ = send_error(&sender, &header, 4006, &error.to_string(), true).await;
                    }
                });
            }
            Payload::ChunkRequest(request) => {
                let _session = active_session
                    .as_ref()
                    .ok_or_else(|| LanFlowError::Auth("请先认证".into()))?;
                let snapshot = context
                    .snapshots
                    .read()
                    .await
                    .get(&request.snapshot_id)
                    .cloned();
                let Some(snapshot) = snapshot else {
                    send_error(&sender, &frame.header, 4001, "快照不存在或已过期", true).await?;
                    continue;
                };
                let Some(file) = snapshot.find_file(&request.file_id).cloned() else {
                    send_error(&sender, &frame.header, 4002, "快照文件不存在", false).await?;
                    continue;
                };
                let Some(chunk) = file
                    .manifest
                    .chunks
                    .get(request.chunk_index as usize)
                    .cloned()
                else {
                    send_error(&sender, &frame.header, 4003, "分片不存在", false).await?;
                    continue;
                };
                let sender = sender.clone();
                let header = frame.header;
                tokio::spawn(async move {
                    if let Err(error) = serve_chunk(
                        sender.clone(),
                        header.clone(),
                        file.absolute_path,
                        request.file_id,
                        chunk,
                    )
                    .await
                    {
                        let _ = send_error(&sender, &header, 4004, &error.to_string(), true).await;
                    }
                });
            }
            Payload::Ping(request) => {
                send_envelope(
                    &sender,
                    frame.header,
                    FrameType::Pong,
                    Payload::Pong(Pong {
                        timestamp_ms: request.timestamp_ms,
                    }),
                )
                .await?;
            }
            Payload::Cancel(_) => {}
            _ => {
                send_error(&sender, &frame.header, 1003, "当前方向不接受此消息", false).await?;
            }
        }
    }
    drop(sender);
    writer_task
        .await
        .map_err(|error| LanFlowError::Internal(error.to_string()))??;
    Ok(())
}

fn prepare_small_file_batch(
    snapshot: &SnapshotRecord,
    file_ids: &[String],
) -> Result<Vec<crate::fileops::SnapshotFile>> {
    if file_ids.is_empty() || file_ids.len() > MAX_SMALL_FILE_BATCH_COUNT {
        return Err(LanFlowError::InvalidInput("小文件批次数量超出上限".into()));
    }
    let mut seen = std::collections::HashSet::with_capacity(file_ids.len());
    let mut files = Vec::with_capacity(file_ids.len());
    let mut total_bytes = 0u64;
    for file_id in file_ids {
        if !seen.insert(file_id) {
            return Err(LanFlowError::InvalidInput("小文件批次包含重复文件".into()));
        }
        let file = snapshot
            .find_file(file_id)
            .cloned()
            .ok_or_else(|| LanFlowError::NotFound("快照文件不存在".into()))?;
        let manifest = &file.manifest;
        let valid_chunks = if manifest.size == 0 {
            manifest.chunks.is_empty()
        } else {
            manifest.chunks.len() == 1
                && manifest.chunks[0].offset == 0
                && manifest.chunks[0].length as u64 == manifest.size
                && manifest.chunks[0].blake3 == manifest.blake3
        };
        if manifest.is_dir || manifest.size > MAX_SMALL_FILE_SIZE || !valid_chunks {
            return Err(LanFlowError::InvalidInput(
                "批次包含非小文件或不兼容分片".into(),
            ));
        }
        total_bytes = total_bytes.saturating_add(manifest.size);
        if total_bytes > MAX_SMALL_FILE_BATCH_BYTES {
            return Err(LanFlowError::InvalidInput(
                "小文件批次字节数超出上限".into(),
            ));
        }
        files.push(file);
    }
    Ok(files)
}

async fn serve_small_file_batch(
    sender: mpsc::Sender<Frame>,
    request_header: FrameHeader,
    files: Vec<crate::fileops::SnapshotFile>,
) -> Result<()> {
    let mut total_bytes = 0u64;
    for snapshot_file in &files {
        let manifest = &snapshot_file.manifest;
        let metadata = tokio::fs::metadata(&snapshot_file.absolute_path).await?;
        if metadata.len() != manifest.size {
            return Err(LanFlowError::Protocol(format!(
                "源文件在传输期间发生变化: {}",
                manifest.relative_path
            )));
        }
        let mut file = tokio::fs::File::open(&snapshot_file.absolute_path).await?;
        let mut remaining = manifest.size as usize;
        let mut offset = 0u64;
        let mut sequence = 0u32;
        while remaining > 0 {
            let size = remaining.min(DEFAULT_DATA_FRAME_SIZE);
            let mut buffer = vec![0u8; size];
            file.read_exact(&mut buffer).await?;
            let mut header = FrameHeader::new(
                FrameType::Data,
                request_header.stream_id,
                request_header.request_id,
            );
            header.file_offset = offset;
            header.sequence = sequence;
            sender
                .send(Frame {
                    header,
                    body: Bytes::from(buffer),
                })
                .await
                .map_err(|_| LanFlowError::Cancelled)?;
            remaining -= size;
            offset += size as u64;
            sequence += 1;
        }
        total_bytes += manifest.size;
        send_envelope_with_flags(
            &sender,
            request_header.clone(),
            FrameType::Control,
            FLAG_MORE,
            Payload::SmallFileComplete(SmallFileComplete {
                file_id: manifest.id.clone(),
                blake3: manifest.blake3.clone(),
                bytes_sent: manifest.size,
            }),
        )
        .await?;
    }
    send_envelope(
        &sender,
        request_header,
        FrameType::Control,
        Payload::SmallFileBatchComplete(SmallFileBatchComplete {
            files_sent: files.len() as u32,
            bytes_sent: total_bytes,
        }),
    )
    .await
}

fn require_session(active: &Option<Arc<Session>>, share_id: &str) -> Result<Arc<Session>> {
    let session = active
        .as_ref()
        .ok_or_else(|| LanFlowError::Auth("请先认证".into()))?;
    if session.share_id != share_id {
        return Err(LanFlowError::Auth("会话无权访问此分享".into()));
    }
    Ok(session.clone())
}

async fn serve_chunk(
    sender: mpsc::Sender<Frame>,
    request_header: FrameHeader,
    path: std::path::PathBuf,
    file_id: String,
    chunk: lanflow_protocol::protocol::wire::ChunkHash,
) -> Result<()> {
    let metadata = tokio::fs::metadata(&path).await?;
    if metadata.len() < chunk.offset.saturating_add(chunk.length as u64) {
        return Err(LanFlowError::Protocol("源文件在传输期间发生变化".into()));
    }
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(SeekFrom::Start(chunk.offset)).await?;
    let mut remaining = chunk.length as usize;
    let mut offset = chunk.offset;
    let mut sequence = 0u32;
    while remaining > 0 {
        let size = remaining.min(DEFAULT_DATA_FRAME_SIZE);
        let mut buffer = vec![0u8; size];
        file.read_exact(&mut buffer).await?;
        let mut header = FrameHeader::new(
            FrameType::Data,
            request_header.stream_id,
            request_header.request_id,
        );
        header.file_offset = offset;
        header.sequence = sequence;
        sender
            .send(Frame {
                header,
                body: Bytes::from(buffer),
            })
            .await
            .map_err(|_| LanFlowError::Cancelled)?;
        remaining -= size;
        offset += size as u64;
        sequence += 1;
    }
    send_envelope(
        &sender,
        request_header,
        FrameType::Control,
        Payload::ChunkComplete(ChunkComplete {
            file_id,
            chunk_index: chunk.index,
            blake3: chunk.blake3,
            bytes_sent: chunk.length as u64,
        }),
    )
    .await
}

async fn send_envelope(
    sender: &mpsc::Sender<Frame>,
    request_header: FrameHeader,
    frame_type: FrameType,
    payload: Payload,
) -> Result<()> {
    send_envelope_with_flags(sender, request_header, frame_type, 0, payload).await
}

async fn send_envelope_with_flags(
    sender: &mpsc::Sender<Frame>,
    request_header: FrameHeader,
    frame_type: FrameType,
    flags: u32,
    payload: Payload,
) -> Result<()> {
    let mut header = FrameHeader::new(
        frame_type,
        request_header.stream_id,
        request_header.request_id,
    );
    header.flags = flags;
    sender
        .send(Frame {
            header,
            body: encode_envelope(&envelope(payload))?,
        })
        .await
        .map_err(|_| LanFlowError::Cancelled)
}

async fn send_snapshot_pages(
    sender: &mpsc::Sender<Frame>,
    request_header: FrameHeader,
    snapshot: &SnapshotRecord,
) -> Result<()> {
    const PAGE_TARGET: usize = 512 * 1024;
    let mut page = Vec::new();
    let mut page_size = snapshot.id.len() + 32;
    for snapshot_file in &snapshot.files {
        let file_size = snapshot_file.manifest.encoded_len() + 8;
        if !page.is_empty() && page_size + file_size > PAGE_TARGET {
            send_envelope_with_flags(
                sender,
                request_header.clone(),
                FrameType::Control,
                FLAG_MORE,
                Payload::SnapshotManifestPage(SnapshotManifestPage {
                    snapshot_id: snapshot.id.clone(),
                    files: std::mem::take(&mut page),
                    done: false,
                    error: String::new(),
                }),
            )
            .await?;
            page_size = snapshot.id.len() + 32;
        }
        if file_size > MAX_FRAME_SIZE / 2 {
            return Err(LanFlowError::Protocol(format!(
                "单文件分片清单过大: {}",
                snapshot_file.manifest.relative_path
            )));
        }
        page_size += file_size;
        page.push(snapshot_file.manifest.clone());
    }
    send_envelope_with_flags(
        sender,
        request_header,
        FrameType::Control,
        0,
        Payload::SnapshotManifestPage(SnapshotManifestPage {
            snapshot_id: snapshot.id.clone(),
            files: page,
            done: true,
            error: String::new(),
        }),
    )
    .await
}

async fn send_error(
    sender: &mpsc::Sender<Frame>,
    request_header: &FrameHeader,
    code: u32,
    message: &str,
    retryable: bool,
) -> Result<()> {
    send_envelope(
        sender,
        request_header.clone(),
        FrameType::Error,
        Payload::Error(WireError {
            code,
            message: message.to_owned(),
            retryable,
        }),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fileops::SnapshotFile;
    use lanflow_protocol::protocol::wire::ManifestFile;

    #[test]
    fn small_file_batch_validation_rejects_duplicates() {
        let data = b"small payload";
        let hash = Bytes::copy_from_slice(blake3::hash(data).as_bytes());
        let file = SnapshotFile {
            absolute_path: "/tmp/small".into(),
            manifest: ManifestFile {
                id: "small".into(),
                relative_path: "small".into(),
                size: data.len() as u64,
                modified_ms: 1,
                blake3: hash.clone(),
                chunks: vec![lanflow_protocol::protocol::wire::ChunkHash {
                    index: 0,
                    offset: 0,
                    length: data.len() as u32,
                    blake3: hash,
                }],
                is_dir: false,
            },
        };
        let snapshot = SnapshotRecord::new("snapshot".into(), vec![file]);
        assert!(prepare_small_file_batch(&snapshot, &["small".into()]).is_ok());
        assert!(prepare_small_file_batch(&snapshot, &["small".into(), "small".into()]).is_err());
    }

    #[tokio::test]
    async fn large_manifest_is_split_into_bounded_pages() {
        let files = (0..8_000)
            .map(|index| SnapshotFile {
                absolute_path: format!("/tmp/file-{index}").into(),
                manifest: ManifestFile {
                    id: format!("{index:064x}"),
                    relative_path: format!("folder/file-{index:05}.txt"),
                    size: 12,
                    modified_ms: 1,
                    blake3: Bytes::from(vec![1; 32]),
                    chunks: Vec::new(),
                    is_dir: false,
                },
            })
            .collect();
        let snapshot = SnapshotRecord::new("snapshot".into(), files);
        let (sender, mut receiver) = mpsc::channel(64);
        send_snapshot_pages(
            &sender,
            FrameHeader::new(FrameType::Control, 3, 9),
            &snapshot,
        )
        .await
        .unwrap();
        drop(sender);

        let mut frames = Vec::new();
        while let Some(frame) = receiver.recv().await {
            assert!(frame.body.len() <= MAX_FRAME_SIZE);
            frames.push(frame);
        }
        assert!(frames.len() > 1);
        assert!(
            frames[..frames.len() - 1]
                .iter()
                .all(|frame| frame.header.flags & FLAG_MORE != 0)
        );
        assert_eq!(frames.last().unwrap().header.flags & FLAG_MORE, 0);
        let count = frames
            .into_iter()
            .map(|frame| decode_envelope(&frame.body).unwrap())
            .map(|message| match message.payload.unwrap() {
                Payload::SnapshotManifestPage(page) => page.files.len(),
                _ => 0,
            })
            .sum::<usize>();
        assert_eq!(count, 8_000);
    }
}
