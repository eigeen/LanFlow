use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use rand::Rng;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::auth::{begin_client_login, mac, verify_mac};
use crate::error::{LanFlowError, Result};
use crate::models::{PeerDto, RemoteEntryDto, RemoteShareDto};
use lanflow_protocol::frame::{FLAG_MORE, Frame, FrameHeader, FrameType, read_frame, write_frame};
use lanflow_protocol::protocol::wire::envelope::Payload;
use lanflow_protocol::protocol::wire::{
    AuthFinish, AuthStart, ChunkRequest, CreateSnapshotRequest, Hello, ListEntriesRequest,
    ListSharesRequest, SessionAttach, SnapshotManifest, SnapshotProgress,
};
use lanflow_protocol::protocol::{
    MAX_FRAME_SIZE, PROTOCOL_MAJOR, PROTOCOL_MINOR, decode_envelope, encode_envelope, envelope,
};

pub struct MuxConnection {
    writer: Mutex<tokio::net::tcp::OwnedWriteHalf>,
    pending: Arc<Mutex<HashMap<u64, mpsc::Sender<Frame>>>>,
    next_request: AtomicU64,
    next_stream: AtomicU32,
    pub remote_hello: Hello,
}

impl MuxConnection {
    pub async fn connect(address: &str, device_id: &str, device_name: &str) -> Result<Arc<Self>> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let (mut reader, mut writer) = stream.into_split();
        let hello = Payload::Hello(Hello {
            device_id: device_id.to_owned(),
            device_name: device_name.to_owned(),
            min_major: PROTOCOL_MAJOR as u32,
            max_major: PROTOCOL_MAJOR as u32,
            max_minor: PROTOCOL_MINOR as u32,
            features: 0b1111,
            max_frame_size: MAX_FRAME_SIZE as u32,
            max_concurrent_streams: 16,
        });
        write_frame(
            &mut writer,
            &Frame {
                header: FrameHeader::new(FrameType::Hello, 0, 1),
                body: encode_envelope(&envelope(hello))?,
            },
        )
        .await?;
        let response = read_frame(&mut reader).await?;
        let response = decode_envelope(&response.body)?;
        let remote_hello = match response.payload {
            Some(Payload::Hello(hello)) => hello,
            Some(Payload::VersionReject(reject)) => {
                return Err(LanFlowError::Protocol(reject.reason));
            }
            _ => return Err(LanFlowError::Protocol("服务端 Hello 响应无效".into())),
        };
        let pending = Arc::new(Mutex::new(HashMap::<u64, mpsc::Sender<Frame>>::new()));
        let pending_reader = pending.clone();
        tokio::spawn(async move {
            loop {
                let frame = match read_frame(&mut reader).await {
                    Ok(frame) => frame,
                    Err(_) => break,
                };
                if matches!(frame.header.frame_type, FrameType::Unknown(_)) {
                    continue;
                }
                let sender = pending_reader
                    .lock()
                    .await
                    .get(&frame.header.request_id)
                    .cloned();
                if let Some(sender) = sender {
                    let final_frame = frame.header.frame_type != FrameType::Data
                        && frame.header.flags & FLAG_MORE == 0;
                    let request_id = frame.header.request_id;
                    let _ = sender.send(frame).await;
                    if final_frame {
                        pending_reader.lock().await.remove(&request_id);
                    }
                }
            }
            pending_reader.lock().await.clear();
        });
        Ok(Arc::new(Self {
            writer: Mutex::new(writer),
            pending,
            next_request: AtomicU64::new(2),
            next_stream: AtomicU32::new(1),
            remote_hello,
        }))
    }

    pub async fn start_request(&self, payload: Payload) -> Result<(u64, mpsc::Receiver<Frame>)> {
        let request_id = self.next_request.fetch_add(1, Ordering::Relaxed);
        let stream_id = self.next_stream.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(16);
        self.pending.lock().await.insert(request_id, sender);
        let frame = Frame {
            header: FrameHeader::new(FrameType::Control, stream_id, request_id),
            body: encode_envelope(&envelope(payload))?,
        };
        if let Err(error) = write_frame(&mut *self.writer.lock().await, &frame).await {
            self.pending.lock().await.remove(&request_id);
            return Err(error.into());
        }
        Ok((request_id, receiver))
    }

    pub async fn unary(&self, payload: Payload) -> Result<Payload> {
        let (request_id, mut receiver) = self.start_request(payload).await?;
        let received = tokio::time::timeout(Duration::from_secs(30), receiver.recv()).await;
        let frame = match received {
            Ok(Some(frame)) => frame,
            Ok(None) => return Err(LanFlowError::Protocol("连接在响应前关闭".into())),
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                return Err(LanFlowError::Protocol("请求响应超时".into()));
            }
        };
        self.pending.lock().await.remove(&request_id);
        let envelope = decode_envelope(&frame.body)?;
        match envelope.payload {
            Some(Payload::Error(error)) => Err(LanFlowError::Protocol(error.message)),
            Some(payload) => Ok(payload),
            None => Err(LanFlowError::Protocol("空响应".into())),
        }
    }

    pub async fn attach_session(&self, session_id: &str, key: &[u8]) -> Result<()> {
        let mut nonce = [0u8; 32];
        rand::rng().fill(&mut nonce);
        let proof = mac(key, &[b"lanflow-attach", session_id.as_bytes(), &nonce])?;
        let response = self
            .unary(Payload::SessionAttach(SessionAttach {
                session_id: session_id.to_owned(),
                nonce: Bytes::copy_from_slice(&nonce),
                proof: Bytes::from(proof),
            }))
            .await?;
        match response {
            Payload::AuthResult(result) if result.success => Ok(()),
            Payload::AuthResult(result) => Err(LanFlowError::Auth(result.error)),
            _ => Err(LanFlowError::Protocol("附加会话响应无效".into())),
        }
    }

    pub async fn download_chunk(
        &self,
        snapshot_id: &str,
        file: &lanflow_protocol::protocol::wire::ManifestFile,
        chunk: &lanflow_protocol::protocol::wire::ChunkHash,
        destination: &mut tokio::fs::File,
    ) -> Result<u64> {
        let (_, mut receiver) = self
            .start_request(Payload::ChunkRequest(ChunkRequest {
                snapshot_id: snapshot_id.to_owned(),
                file_id: file.id.clone(),
                chunk_index: chunk.index,
            }))
            .await?;
        let mut hasher = blake3::Hasher::new();
        let mut received = 0u64;
        while let Some(frame) = receiver.recv().await {
            match frame.header.frame_type {
                FrameType::Data => {
                    destination
                        .seek(SeekFrom::Start(frame.header.file_offset))
                        .await?;
                    destination.write_all(&frame.body).await?;
                    hasher.update(&frame.body);
                    received += frame.body.len() as u64;
                }
                FrameType::Error => {
                    let envelope = decode_envelope(&frame.body)?;
                    if let Some(Payload::Error(error)) = envelope.payload {
                        return Err(LanFlowError::Protocol(error.message));
                    }
                }
                _ => {
                    let envelope = decode_envelope(&frame.body)?;
                    match envelope.payload {
                        Some(Payload::ChunkComplete(complete)) => {
                            if complete.chunk_index != chunk.index || complete.file_id != file.id {
                                return Err(LanFlowError::Protocol("分片完成标记不匹配".into()));
                            }
                            let actual = hasher.finalize();
                            if actual.as_bytes() != chunk.blake3.as_ref()
                                || received != chunk.length as u64
                            {
                                return Err(LanFlowError::Protocol("分片 BLAKE3 校验失败".into()));
                            }
                            return Ok(received);
                        }
                        Some(Payload::Error(error)) => {
                            return Err(LanFlowError::Protocol(error.message));
                        }
                        _ => return Err(LanFlowError::Protocol("分片响应无效".into())),
                    }
                }
            }
        }
        Err(LanFlowError::Protocol("分片传输意外结束".into()))
    }
}

#[derive(Clone)]
pub struct AuthSession {
    pub session_id: String,
    pub key: Vec<u8>,
}

pub struct PeerClient {
    pub peer: PeerDto,
    address: String,
    device_id: String,
    device_name: String,
    control: Arc<MuxConnection>,
    auth: RwLock<Option<AuthSession>>,
}

impl PeerClient {
    pub async fn connect(
        address: String,
        device_id: String,
        device_name: String,
        manual: bool,
    ) -> Result<Arc<Self>> {
        let control = MuxConnection::connect(&address, &device_id, &device_name).await?;
        let socket: std::net::SocketAddr = address
            .parse()
            .map_err(|_| LanFlowError::InvalidInput("地址必须为 IP:端口".into()))?;
        let hello = &control.remote_hello;
        Ok(Arc::new(Self {
            peer: PeerDto {
                id: hello.device_id.clone(),
                name: hello.device_name.clone(),
                address: socket.ip().to_string(),
                port: socket.port(),
                online: true,
                manual,
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: hello.max_minor as u16,
                last_seen: crate::discovery::now_ms(),
            },
            address,
            device_id,
            device_name,
            control,
            auth: RwLock::new(None),
        }))
    }

    pub async fn list_shares(&self) -> Result<Vec<RemoteShareDto>> {
        match self
            .control
            .unary(Payload::ListSharesRequest(ListSharesRequest {}))
            .await?
        {
            Payload::ListSharesResponse(response) => Ok(response
                .shares
                .into_iter()
                .map(|share| RemoteShareDto {
                    id: share.id,
                    name: share.name,
                    enabled: share.enabled,
                })
                .collect()),
            _ => Err(LanFlowError::Protocol("分享列表响应无效".into())),
        }
    }

    pub async fn authenticate(&self, share_id: String, password: String) -> Result<AuthSession> {
        let (request, pending) = begin_client_login(password)?;
        let challenge = match self
            .control
            .unary(Payload::AuthStart(AuthStart {
                share_id: share_id.clone(),
                credential_request: Bytes::from(request),
            }))
            .await?
        {
            Payload::AuthChallenge(challenge) => challenge,
            _ => return Err(LanFlowError::Protocol("认证挑战响应无效".into())),
        };
        let (finalization, key) = pending.finish(&challenge.credential_response)?;
        let result = match self
            .control
            .unary(Payload::AuthFinish(AuthFinish {
                challenge_id: challenge.challenge_id,
                credential_finalization: Bytes::from(finalization),
            }))
            .await?
        {
            Payload::AuthResult(result) => result,
            _ => return Err(LanFlowError::Protocol("认证结果响应无效".into())),
        };
        if !result.success {
            return Err(LanFlowError::Auth(result.error));
        }
        verify_mac(
            &key,
            &[b"lanflow-auth-ok", result.session_id.as_bytes()],
            &result.server_proof,
        )?;
        let session = AuthSession {
            session_id: result.session_id,
            key,
        };
        *self.auth.write().await = Some(session.clone());
        Ok(session)
    }

    pub async fn list_entries(
        &self,
        share_id: String,
        relative_path: String,
        offset: u32,
        limit: u32,
        query: String,
    ) -> Result<Vec<RemoteEntryDto>> {
        match self
            .control
            .unary(Payload::ListEntriesRequest(ListEntriesRequest {
                share_id,
                relative_path,
                offset,
                limit,
                query,
            }))
            .await?
        {
            Payload::ListEntriesResponse(response) => Ok(response
                .entries
                .into_iter()
                .map(|entry| RemoteEntryDto {
                    id: entry.id,
                    name: entry.display_name,
                    relative_path: entry.relative_path,
                    is_dir: entry.is_dir,
                    size: entry.size,
                    modified_ms: entry.modified_ms,
                })
                .collect()),
            _ => Err(LanFlowError::Protocol("目录列表响应无效".into())),
        }
    }

    pub async fn create_snapshot(
        &self,
        share_id: String,
        paths: Vec<String>,
        chunk_size: u32,
        on_progress: impl Fn(SnapshotProgress),
    ) -> Result<SnapshotManifest> {
        let (request_id, mut receiver) = self
            .control
            .start_request(Payload::CreateSnapshotRequest(CreateSnapshotRequest {
                share_id,
                relative_paths: paths,
                chunk_size,
            }))
            .await?;
        let mut snapshot_id = String::new();
        let mut files = Vec::new();
        let progress_timeout = if self.control.remote_hello.max_minor >= 1 {
            Duration::from_secs(5 * 60)
        } else {
            Duration::from_secs(30 * 60)
        };
        loop {
            let received = tokio::time::timeout(progress_timeout, receiver.recv()).await;
            let frame = match received {
                Ok(Some(frame)) => frame,
                Ok(None) => return Err(LanFlowError::Protocol("快照完成前连接关闭".into())),
                Err(_) => {
                    self.control.pending.lock().await.remove(&request_id);
                    return Err(LanFlowError::Protocol("快照准备长时间没有进度".into()));
                }
            };
            let message = decode_envelope(&frame.body)?;
            match message.payload {
                Some(Payload::SnapshotProgress(progress)) => on_progress(progress),
                Some(Payload::SnapshotManifestPage(page)) => {
                    if !page.error.is_empty() {
                        self.control.pending.lock().await.remove(&request_id);
                        return Err(LanFlowError::Protocol(page.error));
                    }
                    if snapshot_id.is_empty() {
                        snapshot_id = page.snapshot_id.clone();
                    } else if snapshot_id != page.snapshot_id {
                        self.control.pending.lock().await.remove(&request_id);
                        return Err(LanFlowError::Protocol("快照分页 ID 不一致".into()));
                    }
                    files.extend(page.files);
                    if page.done {
                        self.control.pending.lock().await.remove(&request_id);
                        return Ok(SnapshotManifest {
                            snapshot_id,
                            files,
                            error: String::new(),
                        });
                    }
                }
                Some(Payload::SnapshotManifest(manifest)) if manifest.error.is_empty() => {
                    return Ok(manifest);
                }
                Some(Payload::SnapshotManifest(manifest)) => {
                    return Err(LanFlowError::Protocol(manifest.error));
                }
                Some(Payload::Error(error)) => return Err(LanFlowError::Protocol(error.message)),
                _ => return Err(LanFlowError::Protocol("快照响应无效".into())),
            }
        }
    }

    pub async fn data_connection(&self) -> Result<Arc<MuxConnection>> {
        let session = self
            .auth
            .read()
            .await
            .clone()
            .ok_or_else(|| LanFlowError::Auth("请先输入分享密码".into()))?;
        let connection =
            MuxConnection::connect(&self.address, &self.device_id, &self.device_name).await?;
        connection
            .attach_session(&session.session_id, &session.key)
            .await?;
        Ok(connection)
    }
}
