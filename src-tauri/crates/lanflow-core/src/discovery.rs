use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};

use crate::error::{LanFlowError, Result};
use crate::models::PeerDto;
use lanflow_protocol::protocol::{PROTOCOL_MAJOR, PROTOCOL_MINOR};

pub const SERVICE_TYPE: &str = "_lanflow._tcp.local.";

pub struct DiscoveryService {
    daemon: ServiceDaemon,
    fullname: String,
}

impl DiscoveryService {
    pub fn start(
        device_id: &str,
        device_name: &str,
        port: u16,
        peers: Arc<RwLock<HashMap<String, PeerDto>>>,
    ) -> Result<Self> {
        let daemon = ServiceDaemon::new()
            .map_err(|error| LanFlowError::Internal(format!("mDNS 启动失败: {error}")))?;
        let hostname = format!("lanflow-{}.local.", &device_id[..device_id.len().min(8)]);
        let properties = [
            ("id", device_id),
            ("name", device_name),
            ("major", &PROTOCOL_MAJOR.to_string()),
            ("minor", &PROTOCOL_MINOR.to_string()),
        ];
        let info = ServiceInfo::new(
            SERVICE_TYPE,
            device_id,
            &hostname,
            "",
            port,
            &properties[..],
        )
        .map_err(|error| LanFlowError::Internal(format!("mDNS 服务无效: {error}")))?
        .enable_addr_auto();
        let fullname = info.get_fullname().to_owned();
        daemon
            .register(info)
            .map_err(|error| LanFlowError::Internal(format!("mDNS 注册失败: {error}")))?;
        let receiver = daemon
            .browse(SERVICE_TYPE)
            .map_err(|error| LanFlowError::Internal(format!("mDNS 浏览失败: {error}")))?;
        let local_id = device_id.to_owned();
        let peers_clone = peers.clone();
        std::thread::Builder::new()
            .name("lanflow-mdns".into())
            .spawn(move || {
                let mut names = HashMap::<String, String>::new();
                while let Ok(event) = receiver.recv() {
                    match event {
                        ServiceEvent::ServiceResolved(info) => {
                            let Some(id) = info.get_property_val_str("id") else {
                                continue;
                            };
                            if id == local_id {
                                continue;
                            }
                            let address = info
                                .get_addresses()
                                .iter()
                                .find(|address| address.is_ipv4())
                                .or_else(|| info.get_addresses().iter().next())
                                .map(ToString::to_string)
                                .unwrap_or_default();
                            if address.is_empty() {
                                continue;
                            }
                            let peer = PeerDto {
                                id: id.to_owned(),
                                name: info.get_property_val_str("name").unwrap_or(id).to_owned(),
                                address,
                                port: info.get_port(),
                                online: true,
                                manual: false,
                                protocol_major: info
                                    .get_property_val_str("major")
                                    .and_then(|value| value.parse().ok())
                                    .unwrap_or(PROTOCOL_MAJOR),
                                protocol_minor: info
                                    .get_property_val_str("minor")
                                    .and_then(|value| value.parse().ok())
                                    .unwrap_or(PROTOCOL_MINOR),
                                last_seen: now_ms(),
                            };
                            names.insert(info.get_fullname().to_owned(), peer.id.clone());
                            if let Ok(mut guard) = peers_clone.write() {
                                guard.insert(peer.id.clone(), peer);
                            }
                        }
                        ServiceEvent::ServiceRemoved(_, fullname) => {
                            if let Some(id) = names.remove(&fullname)
                                && let Ok(mut guard) = peers_clone.write()
                                && let Some(peer) = guard.get_mut(&id)
                            {
                                peer.online = false;
                                peer.last_seen = now_ms();
                            }
                        }
                        _ => {}
                    }
                }
            })
            .map_err(|error| LanFlowError::Internal(error.to_string()))?;
        Ok(Self { daemon, fullname })
    }
}

impl Drop for DiscoveryService {
    fn drop(&mut self) {
        let _ = self.daemon.stop_browse(SERVICE_TYPE);
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
