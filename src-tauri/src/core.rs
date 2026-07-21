use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use rand::Rng;
use tauri::{AppHandle, Emitter, Manager};
use tokio::sync::RwLock as AsyncRwLock;
use tokio_util::sync::CancellationToken;

use lanflow_core::auth::{
    LanFlowServerSetup, derive_local_key, deserialize_server_setup, new_server_setup,
    serialize_server_setup,
};
use lanflow_core::client::PeerClient;
use lanflow_core::db::Database;
use lanflow_core::discovery::DiscoveryService;
use lanflow_core::error::{LanFlowError, Result};
use lanflow_core::models::{AppOverview, PeerDto};
use lanflow_core::server::{ServerContext, run_server};
use lanflow_core::tasks::{ProgressCallback, TaskEngine};

pub struct AppCore {
    pub app: AppHandle,
    pub db: Database,
    pub device_id: String,
    pub device_name: String,
    pub setup: Arc<LanFlowServerSetup>,
    pub peers: Arc<RwLock<HashMap<String, PeerDto>>>,
    pub clients: AsyncRwLock<HashMap<String, Arc<PeerClient>>>,
    pub task_engine: Arc<TaskEngine>,
    pub local_credential_key: [u8; 32],
    pub server_running: AtomicBool,
    pub shutdown: CancellationToken,
    discovery: Mutex<Option<DiscoveryService>>,
}

impl AppCore {
    pub async fn initialize(app: AppHandle) -> Result<Arc<Self>> {
        let app_data = app
            .path()
            .app_data_dir()
            .map_err(|error| LanFlowError::Internal(error.to_string()))?;
        tokio::fs::create_dir_all(&app_data).await?;
        let db = Database::open(&app_data.join("lanflow.sqlite3")).await?;

        let device_id = match db.get_meta("device_id").await? {
            Some(value) => String::from_utf8(value)
                .map_err(|_| LanFlowError::Database("设备 ID 编码无效".into()))?,
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                db.set_meta("device_id", id.as_bytes()).await?;
                id
            }
        };
        let stored_device_name = db
            .get_meta("device_name")
            .await?
            .and_then(|value| String::from_utf8(value).ok());
        let device_name = stored_device_name
            .filter(|name| !should_refresh_device_name(name))
            .unwrap_or_else(default_device_name);
        db.set_meta("device_name", device_name.as_bytes()).await?;

        let setup = match db.get_meta("opaque_server_setup").await? {
            Some(value) => deserialize_server_setup(&value)?,
            None => {
                let setup = new_server_setup();
                db.set_meta("opaque_server_setup", &serialize_server_setup(&setup))
                    .await?;
                setup
            }
        };
        let install_salt = match db.get_meta("credential_salt").await? {
            Some(value) if value.len() == 32 => value,
            _ => {
                let mut salt = vec![0u8; 32];
                rand::rng().fill(salt.as_mut_slice());
                db.set_meta("credential_salt", &salt).await?;
                salt
            }
        };
        let settings = db.settings().await?;
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let progress_app = app.clone();
        let progress: ProgressCallback = Arc::new(move |event| {
            let _ = progress_app.emit("task://progress", event);
        });
        let task_engine = TaskEngine::new(db.clone(), progress);
        let core = Arc::new(Self {
            app,
            db: db.clone(),
            device_id: device_id.clone(),
            device_name: device_name.clone(),
            setup: Arc::new(setup),
            peers: peers.clone(),
            clients: AsyncRwLock::new(HashMap::new()),
            task_engine,
            local_credential_key: derive_local_key(&install_salt),
            server_running: AtomicBool::new(false),
            shutdown: CancellationToken::new(),
            discovery: Mutex::new(None),
        });

        let server_context = Arc::new(ServerContext::new(
            db,
            core.setup.clone(),
            device_id.clone(),
            device_name.clone(),
        ));
        let shutdown = core.shutdown.clone();
        let running_core = core.clone();
        let port = settings.listen_port;
        tauri::async_runtime::spawn(async move {
            running_core.server_running.store(true, Ordering::Relaxed);
            if let Err(error) = run_server(server_context, port, shutdown).await {
                eprintln!("LanFlow server failed: {error}");
            }
            running_core.server_running.store(false, Ordering::Relaxed);
        });

        if let Ok(discovery) = DiscoveryService::start(&device_id, &device_name, port, peers) {
            *core.discovery.lock().expect("discovery lock poisoned") = Some(discovery);
        }
        Ok(core)
    }

    pub async fn overview(&self) -> Result<AppOverview> {
        Ok(AppOverview {
            device_id: self.device_id.clone(),
            device_name: self.device_name.clone(),
            listen_port: self.db.settings().await?.listen_port,
            server_running: self.server_running.load(Ordering::Relaxed),
            shares: self.db.list_shares().await?,
            peers: self
                .peers
                .read()
                .map(|guard| guard.values().cloned().collect())
                .unwrap_or_default(),
            tasks: self.db.list_tasks().await?,
            settings: self.db.settings().await?,
        })
    }

    pub async fn client(&self, peer_id: &str) -> Result<Arc<PeerClient>> {
        self.clients
            .read()
            .await
            .get(peer_id)
            .cloned()
            .ok_or_else(|| LanFlowError::NotFound("请先连接设备".into()))
    }
}

impl Drop for AppCore {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn default_device_name() -> String {
    resolve_device_name(
        whoami::devicename().ok(),
        std::env::var("COMPUTERNAME").ok(),
        std::env::var("HOSTNAME").ok(),
        whoami::hostname().ok(),
    )
}

fn resolve_device_name(
    pretty_name: Option<String>,
    computer_name: Option<String>,
    host_env: Option<String>,
    hostname: Option<String>,
) -> String {
    [pretty_name, computer_name, host_env, hostname]
        .into_iter()
        .flatten()
        .find_map(normalize_device_name)
        .unwrap_or_else(|| "LanFlow 设备".into())
}

fn normalize_device_name(value: String) -> Option<String> {
    let value = value.trim().trim_end_matches(".local").trim();
    if value.is_empty()
        || value.eq_ignore_ascii_case("localhost")
        || value.eq_ignore_ascii_case("localhost.localdomain")
    {
        None
    } else {
        Some(value.to_owned())
    }
}

fn should_refresh_device_name(value: &str) -> bool {
    let value = value.trim();
    value.is_empty()
        || value == "LanFlow 设备"
        || value.eq_ignore_ascii_case("lanflow device")
        || value.eq_ignore_ascii_case("localhost")
        || value.eq_ignore_ascii_case("localhost.localdomain")
        || value.to_ascii_lowercase().ends_with(".local")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_name_prefers_pretty_system_name() {
        assert_eq!(
            resolve_device_name(
                Some("Eigeen 的 MacBook Pro".into()),
                None,
                Some("fallback-host".into()),
                None,
            ),
            "Eigeen 的 MacBook Pro"
        );
    }

    #[test]
    fn device_name_cleans_local_suffix_and_rejects_localhost() {
        assert_eq!(
            resolve_device_name(
                Some("localhost".into()),
                None,
                Some("mac-studio.local".into()),
                None,
            ),
            "mac-studio"
        );
    }

    #[test]
    fn placeholder_device_name_is_refreshed() {
        assert!(should_refresh_device_name("LanFlow 设备"));
        assert!(should_refresh_device_name("EigeendeMacBook-Air.local"));
        assert!(!should_refresh_device_name("工作室 Mac"));
    }
}
