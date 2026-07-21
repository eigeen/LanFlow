import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { open } from "@tauri-apps/plugin-dialog";
import "./App.css";

type Tab = "devices" | "shares" | "tasks" | "settings";

type Share = { id: string; name: string; path: string; enabled: boolean; createdAt: number };
type Peer = {
  id: string;
  name: string;
  address: string;
  port: number;
  online: boolean;
  manual: boolean;
  protocolMajor: number;
  protocolMinor: number;
  lastSeen: number;
};
type RemoteShare = { id: string; name: string; enabled: boolean };
type RemoteEntry = {
  id: string;
  name: string;
  relativePath: string;
  isDir: boolean;
  size: number;
  modifiedMs: number;
};
type Task = {
  id: string;
  peerId: string;
  peerName: string;
  shareId: string;
  destination: string;
  status: string;
  totalBytes: number;
  completedBytes: number;
  speedBps: number;
  fileCount: number;
  completedFiles: number;
  error?: string;
  createdAt: number;
  updatedAt: number;
};
type Settings = {
  automatic: boolean;
  dataConnections: number;
  streamsPerConnection: number;
  chunkSizeMib: number;
  hashWorkers: number;
  memoryBufferMib: number;
  bandwidthLimitMbps: number;
  listenPort: number;
  autostart: boolean;
};
type Overview = {
  deviceId: string;
  deviceName: string;
  listenPort: number;
  serverRunning: boolean;
  shares: Share[];
  peers: Peer[];
  tasks: Task[];
  settings: Settings;
};
type ProgressEvent = {
  taskId: string;
  status: string;
  completedBytes: number;
  totalBytes: number;
  speedBps: number;
  completedFiles: number;
  fileCount: number;
  currentFile: string;
};
type SnapshotProgress = {
  taskId: string;
  phase: "scanning" | "hashing";
  scannedEntries: number;
  totalEntries: number;
  preparedBytes: number;
  totalBytes: number;
  cacheHits: number;
  currentPath: string;
  hashWorkers: number;
  speedBps: number;
};

const statusText: Record<string, string> = {
  preparing: "正在准备",
  waiting_password: "等待密码",
  downloading: "正在同步",
  paused: "已暂停",
  reconnecting: "正在重连",
  verifying: "正在校验",
  completed: "已完成",
  failed: "失败",
  cancelled: "已取消",
};

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** index).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function shortId(id: string) {
  return id.slice(0, 8).toUpperCase();
}

function App() {
  const [tab, setTab] = useState<Tab>("devices");
  const [overview, setOverview] = useState<Overview | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState("");
  const [address, setAddress] = useState("");
  const [connectedPeer, setConnectedPeer] = useState<Peer | null>(null);
  const [remoteShares, setRemoteShares] = useState<RemoteShare[]>([]);
  const [remoteShare, setRemoteShare] = useState<RemoteShare | null>(null);
  const [password, setPassword] = useState("");
  const [remember, setRemember] = useState(true);
  const [authenticated, setAuthenticated] = useState(false);
  const [entries, setEntries] = useState<RemoteEntry[]>([]);
  const [currentPath, setCurrentPath] = useState("");
  const [selectedPaths, setSelectedPaths] = useState<Set<string>>(new Set());
  const [destination, setDestination] = useState("");
  const [conflictPolicy, setConflictPolicy] = useState("keep_both");
  const [shareForm, setShareForm] = useState({ name: "", path: "", password: "" });
  const [settings, setSettings] = useState<Settings | null>(null);
  const [snapshotProgress, setSnapshotProgress] = useState<SnapshotProgress | null>(null);

  const refresh = useCallback(async () => {
    try {
      const data = await invoke<Overview>("get_overview");
      setOverview(data);
      setSettings((previous) => previous ?? data.settings);
    } catch (reason) {
      setError(String(reason));
    }
  }, []);

  useEffect(() => {
    void refresh();
    const interval = window.setInterval(refresh, 5000);
    const unlistenTask = listen<ProgressEvent>("task://progress", ({ payload }) => {
      setOverview((current) =>
        current
          ? {
              ...current,
              tasks: current.tasks.map((task) =>
                task.id === payload.taskId
                  ? {
                      ...task,
                      status: payload.status,
                      completedBytes: payload.completedBytes,
                      totalBytes: payload.totalBytes,
                      speedBps: payload.speedBps,
                      completedFiles: payload.completedFiles,
                      fileCount: payload.fileCount,
                      error: payload.status === "failed" ? payload.currentFile : undefined,
                    }
                  : task,
              ),
            }
          : current,
      );
    });
    const unlistenSnapshot = listen<SnapshotProgress>("snapshot://progress", ({ payload }) => {
      setSnapshotProgress(payload);
    });
    return () => {
      window.clearInterval(interval);
      void unlistenTask.then((dispose) => dispose());
      void unlistenSnapshot.then((dispose) => dispose());
    };
  }, [refresh]);

  const run = async (label: string, action: () => Promise<void>) => {
    setBusy(label);
    setError("");
    try {
      await action();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy("");
    }
  };

  const connect = async (peer?: Peer) => {
    await run("connect", async () => {
      const connected = peer
        ? await invoke<Peer>("connect_discovered_peer", { peerId: peer.id })
        : await invoke<Peer>("connect_peer", { address: address.trim() });
      const shares = await invoke<RemoteShare[]>("list_remote_shares", { peerId: connected.id });
      setConnectedPeer(connected);
      setRemoteShares(shares);
      setRemoteShare(null);
      setAuthenticated(false);
      setEntries([]);
      setCurrentPath("");
      setSelectedPaths(new Set());
    });
  };

  const authenticate = async (saved = false) => {
    if (!connectedPeer || !remoteShare) return;
    await run("auth", async () => {
      if (saved) {
        await invoke("authenticate_with_saved_password", {
          peerId: connectedPeer.id,
          shareId: remoteShare.id,
        });
      } else {
        await invoke("authenticate_peer", {
          peerId: connectedPeer.id,
          shareId: remoteShare.id,
          password,
          remember,
        });
      }
      setAuthenticated(true);
      setPassword("");
      await loadEntries("");
    });
  };

  const loadEntries = async (path: string) => {
    if (!connectedPeer || !remoteShare) return;
    const rows = await invoke<RemoteEntry[]>("list_remote_entries", {
      peerId: connectedPeer.id,
      shareId: remoteShare.id,
      relativePath: path,
      offset: 0,
      query: "",
    });
    setEntries(rows);
    setCurrentPath(path);
  };

  const createTask = async () => {
    if (!connectedPeer || !remoteShare || selectedPaths.size === 0 || !destination) return;
    setSnapshotProgress(null);
    await run("task", async () => {
      await invoke("create_download_task", {
        input: {
          peerId: connectedPeer.id,
          shareId: remoteShare.id,
          remotePaths: [...selectedPaths],
          destination,
          conflictPolicy,
        },
      });
      await refresh();
      setTab("tasks");
      setSelectedPaths(new Set());
    });
    setSnapshotProgress(null);
  };

  const selectDirectory = async (target: "destination" | "share") => {
    const selected = await open({ directory: true, multiple: false });
    if (typeof selected !== "string") return;
    if (target === "destination") setDestination(selected);
    else setShareForm((form) => ({ ...form, path: selected }));
  };

  const createShare = async (event: FormEvent) => {
    event.preventDefault();
    await run("share", async () => {
      await invoke("create_share", shareForm);
      setShareForm({ name: "", path: "", password: "" });
      await refresh();
    });
  };

  const toggleSelection = (path: string) => {
    setSelectedPaths((current) => {
      const next = new Set(current);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  };

  const breadcrumbs = useMemo(() => {
    const pieces = currentPath ? currentPath.split("/") : [];
    return [
      { name: remoteShare?.name ?? "根目录", path: "" },
      ...pieces.map((piece, index) => ({ name: piece, path: pieces.slice(0, index + 1).join("/") })),
    ];
  }, [currentPath, remoteShare]);

  if (!overview || !settings) {
    return <div className="boot-screen"><span className="spinner" />正在启动 LanFlow…</div>;
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark"><span>↗</span></div>
          <div><strong>LanFlow</strong><small>局域网文件传输</small></div>
        </div>
        <nav>
          <NavButton active={tab === "devices"} icon="⌁" label="附近设备" onClick={() => setTab("devices")} />
          <NavButton active={tab === "shares"} icon="▱" label="我的分享" count={overview.shares.length} onClick={() => setTab("shares")} />
          <NavButton active={tab === "tasks"} icon="⇣" label="同步任务" count={overview.tasks.filter((task) => task.status === "downloading").length} onClick={() => setTab("tasks")} />
          <NavButton active={tab === "settings"} icon="⚙" label="设置" onClick={() => setTab("settings")} />
        </nav>
        <div className="device-card">
          <div className={`status-dot ${overview.serverRunning ? "online" : ""}`} />
          <div><strong>{overview.deviceName}</strong><small>{shortId(overview.deviceId)} · 端口 {overview.listenPort}</small></div>
        </div>
      </aside>

      <main className="main-panel">
        <header className="topbar">
          <div>
            <h1>{tab === "devices" ? "附近设备" : tab === "shares" ? "我的分享" : tab === "tasks" ? "同步任务" : "设置"}</h1>
            <p>{tab === "devices" ? "发现同一局域网中的 LanFlow 设备并开始同步" : tab === "shares" ? "管理其他设备可以访问的目录" : tab === "tasks" ? "查看传输速度、进度与断点状态" : "调整连接并发和应用行为"}</p>
          </div>
          <button className="icon-button" onClick={() => void refresh()} title="刷新">↻</button>
        </header>

        {error && <div className="error-banner"><span>!</span>{error}<button onClick={() => setError("")}>×</button></div>}

        <section className="content">
          {tab === "devices" && (
            <div className="stack">
              <div className="connect-bar panel">
                <div><strong>手动连接</strong><span>mDNS 不可用时输入 IP 地址和端口</span></div>
                <div className="inline-form">
                  <input value={address} onChange={(event) => setAddress(event.target.value)} placeholder={`192.168.1.10:${overview.listenPort}`} />
                  <button className="primary" disabled={!address.trim() || !!busy} onClick={() => void connect()}>{busy === "connect" ? "连接中…" : "连接"}</button>
                </div>
              </div>

              {!connectedPeer ? (
                <div className="device-grid">
                  {overview.peers.map((peer) => (
                    <article className="device-tile panel" key={peer.id}>
                      <div className="device-avatar">{peer.name.slice(0, 1).toUpperCase()}</div>
                      <div className="grow"><h3>{peer.name}</h3><p>{peer.address}:{peer.port}</p><small>协议 v{peer.protocolMajor}.{peer.protocolMinor}</small></div>
                      <span className={`pill ${peer.online ? "success" : "muted"}`}>{peer.online ? "在线" : "离线"}</span>
                      <button className="secondary full" disabled={!peer.online || !!busy} onClick={() => void connect(peer)}>浏览分享</button>
                    </article>
                  ))}
                  {overview.peers.length === 0 && <EmptyState icon="⌁" title="尚未发现设备" text="请确认两台设备位于同一局域网，或使用上方地址直连。" />}
                </div>
              ) : (
                <div className="remote-browser panel">
                  <div className="remote-header">
                    <button className="back-button" onClick={() => { setConnectedPeer(null); setRemoteShare(null); setAuthenticated(false); }}>←</button>
                    <div className="device-avatar small">{connectedPeer.name.slice(0, 1)}</div>
                    <div><strong>{connectedPeer.name}</strong><span>{connectedPeer.address}:{connectedPeer.port}</span></div>
                    <span className="pill success">已连接</span>
                  </div>
                  {!remoteShare ? (
                    <div className="share-picker">
                      <h3>选择分享目录</h3>
                      {remoteShares.map((share) => <button key={share.id} className="share-row" onClick={() => { setRemoteShare(share); setAuthenticated(false); }}><span className="folder-icon">▱</span><strong>{share.name}</strong><span>需要密码</span><b>›</b></button>)}
                      {remoteShares.length === 0 && <EmptyState icon="▱" title="没有可用分享" text="对方尚未启用任何分享目录。" />}
                    </div>
                  ) : !authenticated ? (
                    <div className="auth-card">
                      <div className="lock-icon">◇</div><h2>访问“{remoteShare.name}”</h2><p>密码只用于认证，文件内容将以明文在局域网传输。</p>
                      <input type="password" value={password} onChange={(event) => setPassword(event.target.value)} placeholder="输入访问密码" onKeyDown={(event) => event.key === "Enter" && void authenticate()} />
                      <label className="check-line"><input type="checkbox" checked={remember} onChange={(event) => setRemember(event.target.checked)} />在这台设备上记住密码（仅本地混淆）</label>
                      <button className="primary wide" disabled={!password || !!busy} onClick={() => void authenticate()}>{busy === "auth" ? "正在验证…" : "验证并浏览"}</button>
                      <button className="link-button" disabled={!!busy} onClick={() => void authenticate(true)}>尝试使用已保存密码</button>
                    </div>
                  ) : (
                    <div className="file-browser">
                      <div className="browser-toolbar">
                        <div className="breadcrumbs">{breadcrumbs.map((item, index) => <button key={`${item.path}-${index}`} onClick={() => void loadEntries(item.path)}>{index > 0 && <span>/</span>}{item.name}</button>)}</div>
                        <span>{selectedPaths.size} 项已选择</span>
                      </div>
                      <div className="file-list">
                        {entries.map((entry) => (
                          <div className="file-row" key={entry.id}>
                            <input type="checkbox" checked={selectedPaths.has(entry.relativePath)} onChange={() => toggleSelection(entry.relativePath)} />
                            <button className="file-name" onDoubleClick={() => entry.isDir && void loadEntries(entry.relativePath)} onClick={() => entry.isDir && void loadEntries(entry.relativePath)}>
                              <span className={entry.isDir ? "folder-icon" : "file-icon"}>{entry.isDir ? "▱" : "□"}</span><strong>{entry.name}</strong>
                            </button>
                            <span>{entry.isDir ? "文件夹" : formatBytes(entry.size)}</span>
                            <time>{entry.modifiedMs ? new Date(entry.modifiedMs).toLocaleDateString() : "—"}</time>
                          </div>
                        ))}
                        {entries.length === 0 && <EmptyState icon="□" title="空目录" text="这个目录中没有可传输的普通文件。" />}
                      </div>
                      <div className="download-dock">
                        <button className="path-picker" onClick={() => void selectDirectory("destination")}><span>保存到</span><strong>{destination || "选择本地目录…"}</strong></button>
                        <select value={conflictPolicy} onChange={(event) => setConflictPolicy(event.target.value)}><option value="keep_both">冲突时保留两份</option><option value="overwrite">冲突时覆盖</option><option value="skip">冲突时跳过</option></select>
                        {busy === "task" && snapshotProgress && (
                          <div className="snapshot-progress">
                            <strong>{snapshotProgress.phase === "scanning" ? `已扫描 ${snapshotProgress.scannedEntries} 项` : `准备 ${snapshotProgress.totalBytes > 0 ? Math.min(100, Math.round(snapshotProgress.preparedBytes / snapshotProgress.totalBytes * 100)) : 0}%`}</strong>
                            <span>{snapshotProgress.phase === "hashing" ? `${formatBytes(snapshotProgress.preparedBytes)} / ${formatBytes(snapshotProgress.totalBytes)} · ${formatBytes(snapshotProgress.speedBps)}/s · ${snapshotProgress.hashWorkers} 线程 · 缓存命中 ${snapshotProgress.cacheHits}` : snapshotProgress.currentPath}</span>
                          </div>
                        )}
                        <button className="primary" disabled={!destination || selectedPaths.size === 0 || !!busy} onClick={() => void createTask()}>{busy === "task" ? snapshotProgress?.phase === "scanning" ? `扫描 ${snapshotProgress.scannedEntries} 项…` : snapshotProgress ? `准备 ${snapshotProgress.totalBytes > 0 ? Math.min(100, Math.round(snapshotProgress.preparedBytes / snapshotProgress.totalBytes * 100)) : 0}%…` : "准备快照…" : `开始同步 ${selectedPaths.size} 项`}</button>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {tab === "shares" && (
            <div className="two-column">
              <div className="stack">
                {overview.shares.map((share) => (
                  <article className="share-card panel" key={share.id}>
                    <div className="folder-icon large">▱</div><div className="grow"><h3>{share.name}</h3><p title={share.path}>{share.path}</p><small>分享 ID {shortId(share.id)}</small></div>
                    <label className="switch"><input type="checkbox" checked={share.enabled} onChange={(event) => void run("toggle", async () => { await invoke("set_share_enabled", { id: share.id, enabled: event.target.checked }); await refresh(); })} /><span /></label>
                    <button className="danger-link" onClick={() => void run("delete", async () => { await invoke("delete_share", { id: share.id }); await refresh(); })}>删除</button>
                  </article>
                ))}
                {overview.shares.length === 0 && <EmptyState icon="▱" title="还没有分享目录" text="添加一个目录和访问密码，附近设备即可浏览并下载。" />}
              </div>
              <form className="panel share-form" onSubmit={(event) => void createShare(event)}>
                <span className="eyebrow">新建分享</span><h2>开放一个目录</h2><p>符号链接和特殊文件会自动忽略。</p>
                <label>显示名称<input value={shareForm.name} onChange={(event) => setShareForm({ ...shareForm, name: event.target.value })} placeholder="例如：项目资料" /></label>
                <label>本地目录<div className="field-with-button"><input value={shareForm.path} readOnly placeholder="请选择目录" /><button type="button" onClick={() => void selectDirectory("share")}>选择</button></div></label>
                <label>访问密码<input type="password" value={shareForm.password} onChange={(event) => setShareForm({ ...shareForm, password: event.target.value })} placeholder="不会以明文发送" /></label>
                <button className="primary wide" disabled={!shareForm.name || !shareForm.path || !shareForm.password || !!busy}>{busy === "share" ? "正在创建…" : "创建分享"}</button>
              </form>
            </div>
          )}

          {tab === "tasks" && (
            <div className="stack">
              <div className="stats-row">
                <Stat label="活动任务" value={overview.tasks.filter((task) => task.status === "downloading").length.toString()} />
                <Stat label="当前总速度" value={`${formatBytes(overview.tasks.reduce((sum, task) => sum + task.speedBps, 0))}/s`} />
                <Stat label="本次已传输" value={formatBytes(overview.tasks.reduce((sum, task) => sum + task.completedBytes, 0))} />
              </div>
              {overview.tasks.map((task) => {
                const percent = task.totalBytes ? Math.min(100, (task.completedBytes / task.totalBytes) * 100) : task.status === "completed" ? 100 : 0;
                return <article className="task-card panel" key={task.id}>
                  <div className={`task-state ${task.status}`}><span>{task.status === "completed" ? "✓" : task.status === "failed" ? "!" : "⇣"}</span></div>
                  <div className="grow task-main"><div className="task-title"><h3>{task.peerName}</h3><span className={`pill task-${task.status}`}>{statusText[task.status] ?? task.status}</span></div><p>{task.destination}</p><div className="progress"><i style={{ width: `${percent}%` }} /></div><div className="task-meta"><span>{percent.toFixed(1)}% · {formatBytes(task.completedBytes)} / {formatBytes(task.totalBytes)}</span><span>{task.speedBps ? `${formatBytes(task.speedBps)}/s` : `${task.completedFiles}/${task.fileCount} 个文件`}</span></div>{task.error && <div className="task-error">{task.error}</div>}</div>
                  <div className="task-actions">
                    {task.status === "downloading" && <button onClick={() => void run("pause", async () => { await invoke("pause_task", { taskId: task.id }); await refresh(); })}>暂停</button>}
                    {["paused", "failed"].includes(task.status) && <button onClick={() => void run("resume", async () => { await invoke("resume_task", { taskId: task.id }); await refresh(); })}>继续</button>}
                    {!['completed', 'cancelled'].includes(task.status) && <button className="danger-link" onClick={() => void run("cancel", async () => { await invoke("cancel_task", { taskId: task.id, deletePartial: false }); await refresh(); })}>取消</button>}
                  </div>
                </article>;
              })}
              {overview.tasks.length === 0 && <EmptyState icon="⇣" title="还没有同步任务" text="连接附近设备，选择文件后任务会显示在这里。" />}
            </div>
          )}

          {tab === "settings" && (
            <form className="settings-grid" onSubmit={(event) => { event.preventDefault(); void run("settings", async () => { await invoke("save_settings", { settings }); await refresh(); }); }}>
              <section className="panel settings-section"><span className="eyebrow">传输性能</span><h2>连接与并发</h2><label className="setting-toggle"><div><strong>自动调节</strong><span>根据吞吐和失败率使用安全默认值</span></div><label className="switch"><input type="checkbox" checked={settings.automatic} onChange={(event) => setSettings({ ...settings, automatic: event.target.checked })} /><span /></label></label><div className="form-grid"><label>数据连接数<input type="number" min="1" max="8" value={settings.dataConnections} onChange={(event) => setSettings({ ...settings, dataConnections: Number(event.target.value) })} /></label><label>每连接并发<input type="number" min="1" max="16" value={settings.streamsPerConnection} onChange={(event) => setSettings({ ...settings, streamsPerConnection: Number(event.target.value) })} /></label><label>分片大小（MiB）<input type="number" min="1" max="64" value={settings.chunkSizeMib} onChange={(event) => setSettings({ ...settings, chunkSizeMib: Number(event.target.value) })} /></label><label>快照哈希线程（0 为自动）<input type="number" min="0" max="32" value={settings.hashWorkers} onChange={(event) => setSettings({ ...settings, hashWorkers: Number(event.target.value) })} /></label><label>内存缓冲（MiB，0 为禁用）<input type="number" min="0" max="2048" value={settings.memoryBufferMib} onChange={(event) => setSettings({ ...settings, memoryBufferMib: Number(event.target.value) })} /></label><label>限速（Mbps，0 为不限）<input type="number" min="0" value={settings.bandwidthLimitMbps} onChange={(event) => setSettings({ ...settings, bandwidthLimitMbps: Number(event.target.value) })} /></label></div></section>
              <section className="panel settings-section"><span className="eyebrow">应用行为</span><h2>服务与启动</h2><label>监听端口<input type="number" min="1024" max="65535" value={settings.listenPort} onChange={(event) => setSettings({ ...settings, listenPort: Number(event.target.value) })} /><small>端口修改将在下次启动后生效</small></label><label className="setting-toggle"><div><strong>登录时启动</strong><span>在后台启动分享与未完成任务</span></div><label className="switch"><input type="checkbox" checked={settings.autostart} onChange={(event) => setSettings({ ...settings, autostart: event.target.checked })} /><span /></label></label><div className="notice"><strong>明文传输</strong><p>LanFlow 当前不加密文件名和内容，请只在可信局域网中使用。密码认证和数据校验仍受保护。</p></div></section>
              <div className="settings-footer"><button className="primary" disabled={!!busy}>{busy === "settings" ? "正在保存…" : "保存设置"}</button></div>
            </form>
          )}
        </section>
      </main>
    </div>
  );
}

function NavButton({ active, icon, label, count, onClick }: { active: boolean; icon: string; label: string; count?: number; onClick: () => void }) {
  return <button className={active ? "active" : ""} onClick={onClick}><span className="nav-icon">{icon}</span>{label}{count ? <b>{count}</b> : null}</button>;
}

function EmptyState({ icon, title, text }: { icon: string; title: string; text: string }) {
  return <div className="empty-state"><div>{icon}</div><h3>{title}</h3><p>{text}</p></div>;
}

function Stat({ label, value }: { label: string; value: string }) {
  return <div className="stat panel"><span>{label}</span><strong>{value}</strong></div>;
}

export default App;
