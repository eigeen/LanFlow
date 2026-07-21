# LanFlow

LanFlow 是基于 Tauri 2 的局域网文件拉取工具。一个实例可以同时发布多个带独立密码的分享，并从附近设备选择文件或目录创建一次性同步任务。它不挂载目录、不持续监听，也不做双向同步。

> 当前文件内容与文件名均为明文传输，适用于可信局域网。密码通过 OPAQUE 完成认证，不会明文上网；会话附加使用随机挑战与 HMAC，文件和分片使用 BLAKE3 验证。

## 架构

Rust 是一个 Cargo workspace，核心能力不依赖 WebView：

```text
React / TypeScript
        │ Tauri commands + 限频进度事件
        ▼
lanflow (Tauri 壳层)
        │
        ├── lanflow-core
        │   ├── OPAQUE 认证与本地凭据混淆
        │   ├── mDNS 发现与手动连接
        │   ├── TCP 控制/数据连接池及并发逻辑流
        │   ├── 快照、BLAKE3、路径安全与文件落盘
        │   └── SQLite WAL 任务状态与分片位图
        │
        └── lanflow-protocol
            ├── 固定二进制帧头与版本字段
            ├── 原始数据帧
            └── Prost 控制消息
```

目录：

- `src-tauri/crates/lanflow-protocol`：可独立测试的线协议 crate。
- `src-tauri/crates/lanflow-core`：可独立测试的传输、认证、存储与任务 crate。
- `src-tauri/src`：Tauri 生命周期、托盘、commands 和事件适配。
- `src`：简体中文 React 界面。

## 开发与验证

需要 Rust stable、Node.js 和 pnpm。Protobuf 编译器由 `protoc-bin-vendored` 随构建提供，无需系统安装。

```bash
pnpm install
pnpm build
cargo test --manifest-path src-tauri/Cargo.toml --workspace --all-targets
cargo clippy --manifest-path src-tauri/Cargo.toml --workspace --all-targets --all-features -- -D warnings
pnpm tauri build
```

开发时可使用 `pnpm tauri dev`，但自动化验证只运行会自行退出的 build/test 命令。

可用真实目录执行一次性本机回环端到端基准；测试会启动临时服务端、完成认证、快照、TCP 传输、校验目标文件数与字节数，并自动清理临时副本：

```bash
LANFLOW_E2E_SOURCE=/path/to/tree cargo test \
  --manifest-path src-tauri/Cargo.toml -p lanflow-core --release \
  --test loopback_transfer -- --ignored --nocapture
```

## 传输模型

- 每个对端默认 1 条控制连接、2 条数据连接，每条数据连接 4 个并发逻辑流。
- 下载器使用有界的跨文件并发流水线，小文件会轮询分配到所有数据连接；网络传输、文件创建、校验和重命名可重叠执行。
- 逻辑分片默认 8 MiB，原始数据帧最多 256 KiB；控制消息走 Prost，文件数据不进入 Prost 或 WebView。
- 大目录快照持续报告扫描/哈希速度，并以约 512 KiB 的多帧分页清单返回；协议 v1.1 仍可回退兼容 v1.0 对端。
- 快照使用独立 Rayon 线程池；默认按逻辑核心数自动选择（最多 16），也可在设置中指定 1–32 个 worker。整文件与分片 BLAKE3 会并行计算。
- 分享端使用 `根路径 + 相对路径 + 大小 + mtime + 分片大小` 校验 BLAKE3 缓存，未变化文件无需重复预哈希。
- 多分片文件通过位图和 SQLite WAL 批量持久化；恢复时先重新校验已确认分片，暂停、掉线或应用重启后仅重新请求未确认或校验失败的分片。单分片小文件依靠校验后原子移动和最终文件复检，避免逐文件 `fsync` 与 SQLite 提交。
- 任务结束时校验整文件并原子移动，保留相对目录和 mtime；符号链接及特殊文件不会传输。
- 默认 mDNS 服务类型为 `_lanflow._tcp.local.`，发现失败时可使用 IPv4/IPv6 `host:port` 直连。

## 仓库

[github.com/eigeen/LanFlow](https://github.com/eigeen/LanFlow)
