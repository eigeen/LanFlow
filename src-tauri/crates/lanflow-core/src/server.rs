use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;

use crate::auth::{LanFlowServerSetup, ServerPendingLogin, begin_server_login, mac, verify_mac};
use crate::db::Database;
use crate::error::{LanFlowError, Result};
use crate::fileops::{SnapshotRecord, create_snapshot, list_entries};
use lanflow_protocol::frame::{Frame, FrameHeader, FrameType, read_frame, write_frame};
use lanflow_protocol::protocol::wire::envelope::Payload;
use lanflow_protocol::protocol::wire::{
    AuthChallenge, AuthResult, ChunkComplete, Error as WireError, Hello, ListEntriesResponse,
    ListSharesResponse, Pong, ShareInfo,
};
use lanflow_protocol::protocol::{
    DEFAULT_CHUNK_SIZE, DEFAULT_DATA_FRAME_SIZE, MAX_FRAME_SIZE, PROTOCOL_MAJOR, PROTOCOL_MINOR,
    decode_envelope, encode_envelope, envelope,
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
            features: 0b1111,
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
                let snapshot = create_snapshot(
                    share.path.into(),
                    request.relative_paths,
                    if request.chunk_size == 0 {
                        DEFAULT_CHUNK_SIZE
                    } else {
                        request.chunk_size
                    },
                )
                .await?;
                let manifest = snapshot.wire_manifest();
                context
                    .snapshots
                    .write()
                    .await
                    .insert(snapshot.id.clone(), Arc::new(snapshot));
                send_envelope(
                    &sender,
                    frame.header,
                    FrameType::Control,
                    Payload::SnapshotManifest(manifest),
                )
                .await?;
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
    sender
        .send(Frame {
            header: FrameHeader::new(
                frame_type,
                request_header.stream_id,
                request_header.request_id,
            ),
            body: encode_envelope(&envelope(payload))?,
        })
        .await
        .map_err(|_| LanFlowError::Cancelled)
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
