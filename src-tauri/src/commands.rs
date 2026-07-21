use std::path::Path;
use std::sync::Arc;

use base64::Engine;
use serde::Serialize;
use tauri::{Emitter, State};
use tauri_plugin_autostart::ManagerExt;

use crate::core::AppCore;
use lanflow_core::auth::{obscure_password, register_password, reveal_password};
use lanflow_core::client::PeerClient;
use lanflow_core::discovery::now_ms;
use lanflow_core::error::{LanFlowError, Result};
use lanflow_core::models::{
    AppOverview, ConflictPolicy, CreateTaskInput, PeerDto, PerformanceSettings, RemoteEntryDto,
    RemoteShareDto, ShareDto, TaskDto,
};
use lanflow_core::tasks::TaskProgressEvent;

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotProgressEvent {
    task_id: String,
    phase: String,
    scanned_entries: u64,
    total_entries: u64,
    prepared_bytes: u64,
    total_bytes: u64,
    cache_hits: u64,
    current_path: String,
    hash_workers: u32,
    speed_bps: u64,
}

#[tauri::command]
pub async fn get_overview(core: State<'_, Arc<AppCore>>) -> Result<AppOverview> {
    core.overview().await
}

#[tauri::command]
pub async fn create_share(
    core: State<'_, Arc<AppCore>>,
    name: String,
    path: String,
    password: String,
) -> Result<ShareDto> {
    if name.trim().is_empty() || password.is_empty() {
        return Err(LanFlowError::InvalidInput("分享名称和密码不能为空".into()));
    }
    let canonical = Path::new(&path).canonicalize()?;
    if !canonical.is_dir() {
        return Err(LanFlowError::InvalidInput("分享路径不是目录".into()));
    }
    let share = ShareDto {
        id: uuid::Uuid::new_v4().to_string(),
        name: name.trim().to_owned(),
        path: canonical.to_string_lossy().into_owned(),
        enabled: true,
        created_at: now_ms(),
    };
    let record = register_password(&core.setup, &share.id, &password)?;
    core.db.put_share(&share, &record).await?;
    Ok(share)
}

#[tauri::command]
pub async fn update_share(
    core: State<'_, Arc<AppCore>>,
    id: String,
    name: String,
    path: String,
    password: Option<String>,
    enabled: bool,
) -> Result<ShareDto> {
    let Some((old, old_record)) = core.db.share_record(&id).await? else {
        return Err(LanFlowError::NotFound("分享不存在".into()));
    };
    let canonical = Path::new(&path).canonicalize()?;
    if !canonical.is_dir() {
        return Err(LanFlowError::InvalidInput("分享路径不是目录".into()));
    }
    let share = ShareDto {
        id: old.id,
        name: name.trim().to_owned(),
        path: canonical.to_string_lossy().into_owned(),
        enabled,
        created_at: old.created_at,
    };
    let record = match password {
        Some(password) if !password.is_empty() => {
            register_password(&core.setup, &share.id, &password)?
        }
        _ => old_record,
    };
    core.db.put_share(&share, &record).await?;
    Ok(share)
}

#[tauri::command]
pub async fn set_share_enabled(
    core: State<'_, Arc<AppCore>>,
    id: String,
    enabled: bool,
) -> Result<()> {
    core.db.set_share_enabled(&id, enabled).await
}

#[tauri::command]
pub async fn delete_share(core: State<'_, Arc<AppCore>>, id: String) -> Result<()> {
    core.db.delete_share(&id).await
}

#[tauri::command]
pub async fn list_peers(core: State<'_, Arc<AppCore>>) -> Result<Vec<PeerDto>> {
    Ok(core
        .peers
        .read()
        .map(|guard| guard.values().cloned().collect())
        .unwrap_or_default())
}

#[tauri::command]
pub async fn connect_peer(core: State<'_, Arc<AppCore>>, address: String) -> Result<PeerDto> {
    let normalized = if address.contains(':') {
        address
    } else {
        format!("{}:{}", address, core.db.settings().await?.listen_port)
    };
    let client = PeerClient::connect(
        normalized,
        core.device_id.clone(),
        core.device_name.clone(),
        true,
    )
    .await?;
    let peer = client.peer.clone();
    core.clients.write().await.insert(peer.id.clone(), client);
    if let Ok(mut peers) = core.peers.write() {
        peers.insert(peer.id.clone(), peer.clone());
    }
    Ok(peer)
}

#[tauri::command]
pub async fn connect_discovered_peer(
    core: State<'_, Arc<AppCore>>,
    peer_id: String,
) -> Result<PeerDto> {
    let peer = core
        .peers
        .read()
        .ok()
        .and_then(|peers| peers.get(&peer_id).cloned())
        .ok_or_else(|| LanFlowError::NotFound("设备已离线".into()))?;
    let address = if peer.address.contains(':') && !peer.address.contains('.') {
        format!("[{}]:{}", peer.address, peer.port)
    } else {
        format!("{}:{}", peer.address, peer.port)
    };
    let client = PeerClient::connect(
        address,
        core.device_id.clone(),
        core.device_name.clone(),
        false,
    )
    .await?;
    let connected = client.peer.clone();
    core.clients
        .write()
        .await
        .insert(connected.id.clone(), client);
    Ok(connected)
}

#[tauri::command]
pub async fn list_remote_shares(
    core: State<'_, Arc<AppCore>>,
    peer_id: String,
) -> Result<Vec<RemoteShareDto>> {
    core.client(&peer_id).await?.list_shares().await
}

#[tauri::command]
pub async fn authenticate_peer(
    core: State<'_, Arc<AppCore>>,
    peer_id: String,
    share_id: String,
    password: String,
    remember: bool,
) -> Result<()> {
    core.client(&peer_id)
        .await?
        .authenticate(share_id.clone(), password.clone())
        .await?;
    if remember {
        let (nonce, ciphertext) = obscure_password(&core.local_credential_key, &password)?;
        core.db
            .save_credential(&peer_id, &share_id, &nonce, &ciphertext)
            .await?;
    }
    Ok(())
}

#[tauri::command]
pub async fn authenticate_with_saved_password(
    core: State<'_, Arc<AppCore>>,
    peer_id: String,
    share_id: String,
) -> Result<()> {
    let Some((nonce, ciphertext)) = core.db.load_credential(&peer_id, &share_id).await? else {
        return Err(LanFlowError::NotFound("没有保存的密码".into()));
    };
    let password = match reveal_password(&core.local_credential_key, &nonce, &ciphertext) {
        Ok(password) => password,
        Err(error) => {
            core.db.delete_credential(&peer_id, &share_id).await?;
            return Err(error);
        }
    };
    core.client(&peer_id)
        .await?
        .authenticate(share_id, password)
        .await?;
    Ok(())
}

#[tauri::command]
pub async fn list_remote_entries(
    core: State<'_, Arc<AppCore>>,
    peer_id: String,
    share_id: String,
    relative_path: String,
    offset: u32,
    query: String,
) -> Result<Vec<RemoteEntryDto>> {
    core.client(&peer_id)
        .await?
        .list_entries(share_id, relative_path, offset, 500, query)
        .await
}

#[tauri::command]
pub async fn create_download_task(
    core: State<'_, Arc<AppCore>>,
    input: CreateTaskInput,
) -> Result<TaskDto> {
    if input.remote_paths.is_empty() {
        return Err(LanFlowError::InvalidInput("至少选择一个文件或目录".into()));
    }
    let destination = Path::new(&input.destination).canonicalize()?;
    if !destination.is_dir() {
        return Err(LanFlowError::InvalidInput("目标路径不是目录".into()));
    }
    let client = core.client(&input.peer_id).await?;
    let settings = core.db.settings().await?;
    let chunk_size = settings.chunk_size_mib.clamp(1, 64) * 1024 * 1024;
    let now = now_ms();
    let mut task = TaskDto {
        id: uuid::Uuid::new_v4().to_string(),
        peer_id: input.peer_id.clone(),
        peer_name: client.peer.name.clone(),
        share_id: input.share_id.clone(),
        destination: destination.to_string_lossy().into_owned(),
        status: "preparing".into(),
        total_bytes: 0,
        completed_bytes: 0,
        speed_bps: 0,
        file_count: 0,
        completed_files: 0,
        error: None,
        created_at: now,
        updated_at: now,
    };
    let remote_paths = serde_json::to_string(&input.remote_paths)
        .map_err(|error| LanFlowError::Internal(error.to_string()))?;
    core.db
        .insert_task(
            &task,
            remote_paths,
            input.conflict_policy.as_str().into(),
            None,
        )
        .await?;

    let app = core.app.clone();
    let task_id = task.id.clone();
    let manifest_result = client
        .create_snapshot(
            input.share_id.clone(),
            input.remote_paths.clone(),
            chunk_size,
            move |progress| {
                let _ = app.emit(
                    "task://progress",
                    TaskProgressEvent {
                        task_id: task_id.clone(),
                        status: "preparing".into(),
                        completed_bytes: progress.prepared_bytes,
                        total_bytes: progress.total_bytes,
                        speed_bps: progress.speed_bps,
                        completed_files: 0,
                        file_count: progress.total_entries,
                        current_file: progress.current_path.clone(),
                    },
                );
                let _ = app.emit(
                    "snapshot://progress",
                    SnapshotProgressEvent {
                        task_id: task_id.clone(),
                        phase: progress.phase,
                        scanned_entries: progress.scanned_entries,
                        total_entries: progress.total_entries,
                        prepared_bytes: progress.prepared_bytes,
                        total_bytes: progress.total_bytes,
                        cache_hits: progress.cache_hits,
                        current_path: progress.current_path,
                        hash_workers: progress.hash_workers,
                        speed_bps: progress.speed_bps,
                    },
                );
            },
        )
        .await;
    let manifest = match manifest_result {
        Ok(manifest) => manifest,
        Err(error) => {
            core.db
                .set_task_status(&task.id, "failed", Some(error.to_string()))
                .await?;
            return Err(error);
        }
    };
    task.total_bytes = manifest.files.iter().map(|file| file.size).sum();
    task.file_count = manifest.files.iter().filter(|file| !file.is_dir).count() as u64;
    task.updated_at = now_ms();
    let snapshot =
        base64::engine::general_purpose::STANDARD.encode(prost::Message::encode_to_vec(&manifest));
    core.db
        .update_task_manifest(&task.id, snapshot, task.total_bytes, task.file_count)
        .await?;
    core.task_engine
        .start(
            task.clone(),
            client,
            manifest,
            input.conflict_policy,
            settings,
        )
        .await?;
    Ok(task)
}

#[tauri::command]
pub async fn list_tasks(core: State<'_, Arc<AppCore>>) -> Result<Vec<TaskDto>> {
    core.db.list_tasks().await
}

#[tauri::command]
pub async fn pause_task(core: State<'_, Arc<AppCore>>, task_id: String) -> Result<()> {
    core.task_engine.pause(&task_id).await
}

#[tauri::command]
pub async fn cancel_task(
    core: State<'_, Arc<AppCore>>,
    task_id: String,
    delete_partial: bool,
) -> Result<()> {
    let destination = core
        .db
        .list_tasks()
        .await?
        .into_iter()
        .find(|task| task.id == task_id)
        .map(|task| task.destination)
        .ok_or_else(|| LanFlowError::NotFound("任务不存在".into()))?;
    core.task_engine
        .cancel(&task_id, delete_partial, &destination)
        .await
}

#[tauri::command]
pub async fn resume_task(core: State<'_, Arc<AppCore>>, task_id: String) -> Result<()> {
    let Some((peer_id, share_id, _, remote_paths_json, policy, _)) =
        core.db.task_resume_data(&task_id).await?
    else {
        return Err(LanFlowError::NotFound("任务不存在".into()));
    };
    let paths: Vec<String> = serde_json::from_str(&remote_paths_json)
        .map_err(|error| LanFlowError::Database(error.to_string()))?;
    let policy = match policy.as_str() {
        "overwrite" => ConflictPolicy::Overwrite,
        "skip" => ConflictPolicy::Skip,
        _ => ConflictPolicy::KeepBoth,
    };
    let client = core.client(&peer_id).await?;
    let settings = core.db.settings().await?;
    let app = core.app.clone();
    let progress_task_id = task_id.clone();
    let manifest_result = client
        .create_snapshot(
            share_id,
            paths,
            settings.chunk_size_mib * 1024 * 1024,
            move |progress| {
                let _ = app.emit(
                    "task://progress",
                    TaskProgressEvent {
                        task_id: progress_task_id.clone(),
                        status: "preparing".into(),
                        completed_bytes: progress.prepared_bytes,
                        total_bytes: progress.total_bytes,
                        speed_bps: progress.speed_bps,
                        completed_files: 0,
                        file_count: progress.total_entries,
                        current_file: progress.current_path.clone(),
                    },
                );
                let _ = app.emit(
                    "snapshot://progress",
                    SnapshotProgressEvent {
                        task_id: progress_task_id.clone(),
                        phase: progress.phase,
                        scanned_entries: progress.scanned_entries,
                        total_entries: progress.total_entries,
                        prepared_bytes: progress.prepared_bytes,
                        total_bytes: progress.total_bytes,
                        cache_hits: progress.cache_hits,
                        current_path: progress.current_path,
                        hash_workers: progress.hash_workers,
                        speed_bps: progress.speed_bps,
                    },
                );
            },
        )
        .await;
    let manifest = match manifest_result {
        Ok(manifest) => manifest,
        Err(error) => {
            core.db
                .set_task_status(&task_id, "failed", Some(error.to_string()))
                .await?;
            return Err(error);
        }
    };
    let task = core
        .db
        .list_tasks()
        .await?
        .into_iter()
        .find(|task| task.id == task_id)
        .ok_or_else(|| LanFlowError::NotFound("任务不存在".into()))?;
    core.task_engine
        .start(task, client, manifest, policy, settings)
        .await
}

#[tauri::command]
pub async fn save_settings(
    core: State<'_, Arc<AppCore>>,
    settings: PerformanceSettings,
) -> Result<()> {
    if !(1..=8).contains(&settings.data_connections)
        || !(1..=16).contains(&settings.streams_per_connection)
        || !(1..=64).contains(&settings.chunk_size_mib)
        || settings.hash_workers > 32
        || settings.memory_buffer_mib > 2048
    {
        return Err(LanFlowError::InvalidInput("性能参数超出允许范围".into()));
    }
    core.db.save_settings(&settings).await?;
    let autostart = core.app.autolaunch();
    if settings.autostart {
        autostart
            .enable()
            .map_err(|error| LanFlowError::Internal(error.to_string()))?;
    } else {
        autostart
            .disable()
            .map_err(|error| LanFlowError::Internal(error.to_string()))?;
    }
    Ok(())
}
