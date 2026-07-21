use std::sync::Arc;
use std::time::{Duration, Instant};

use lanflow_core::auth::{new_server_setup, register_password};
use lanflow_core::client::PeerClient;
use lanflow_core::db::Database;
use lanflow_core::discovery::now_ms;
use lanflow_core::models::{ConflictPolicy, PerformanceSettings, ShareDto, TaskDto};
use lanflow_core::server::{ServerContext, run_server};
use lanflow_core::tasks::TaskEngine;
use tokio_util::sync::CancellationToken;

/// Real-data loopback probe. It is ignored because the caller supplies a source
/// tree and the run can copy many gigabytes:
///
/// `LANFLOW_E2E_SOURCE=/path/to/tree cargo test -p lanflow-core --release
/// --test loopback_transfer -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn real_tree_loopback_throughput() {
    let source = std::env::var("LANFLOW_E2E_SOURCE")
        .expect("LANFLOW_E2E_SOURCE must point to a representative directory");
    let source = std::fs::canonicalize(source).expect("source directory is not accessible");
    assert!(source.is_dir());

    let scratch = std::env::temp_dir().join(format!("lanflow-e2e-{}", uuid::Uuid::new_v4()));
    let destination = scratch.join("destination");
    std::fs::create_dir_all(&destination).unwrap();
    let server_db = Database::open(&scratch.join("server.sqlite3"))
        .await
        .unwrap();
    let client_db = Database::open(&scratch.join("client.sqlite3"))
        .await
        .unwrap();

    let setup = Arc::new(new_server_setup());
    let share = ShareDto {
        id: "benchmark-share".into(),
        name: "benchmark".into(),
        path: source.to_string_lossy().into_owned(),
        enabled: true,
        created_at: now_ms(),
    };
    let password = "lanflow-benchmark";
    let record = register_password(&setup, &share.id, password).unwrap();
    server_db.put_share(&share, &record).await.unwrap();

    let port = reserve_loopback_port();
    let shutdown = CancellationToken::new();
    let server_context = Arc::new(ServerContext::new(
        server_db,
        setup,
        "benchmark-server".into(),
        "Benchmark Server".into(),
    ));
    let server_shutdown = shutdown.clone();
    let server =
        tokio::spawn(async move { run_server(server_context, port, server_shutdown).await });
    let address = format!("127.0.0.1:{port}");
    let client = connect_with_retry(&address).await;
    client
        .authenticate(share.id.clone(), password.into())
        .await
        .unwrap();

    let snapshot_started = Instant::now();
    let manifest = client
        .create_snapshot(share.id.clone(), vec![".".into()], 8 * 1024 * 1024, |_| {})
        .await
        .unwrap();
    let snapshot_elapsed = snapshot_started.elapsed();
    let total_bytes = manifest.files.iter().map(|file| file.size).sum::<u64>();
    let file_count = manifest.files.iter().filter(|file| !file.is_dir).count() as u64;
    eprintln!(
        "LanFlow E2E snapshot: {file_count} files, {:.1} MiB in {:.3}s ({:.1} MiB/s)",
        total_bytes as f64 / 1_048_576.0,
        snapshot_elapsed.as_secs_f64(),
        total_bytes as f64 / 1_048_576.0 / snapshot_elapsed.as_secs_f64().max(0.001),
    );

    let now = now_ms();
    let task = TaskDto {
        id: uuid::Uuid::new_v4().to_string(),
        peer_id: client.peer.id.clone(),
        peer_name: client.peer.name.clone(),
        share_id: share.id,
        destination: destination.to_string_lossy().into_owned(),
        status: "preparing".into(),
        total_bytes,
        completed_bytes: 0,
        speed_bps: 0,
        file_count,
        completed_files: 0,
        error: None,
        created_at: now,
        updated_at: now,
    };
    client_db
        .insert_task(&task, "[\".\"]".into(), "overwrite".into(), None)
        .await
        .unwrap();
    let engine = TaskEngine::new(client_db.clone(), Arc::new(|_| {}));
    let transfer_started = Instant::now();
    engine
        .start(
            task.clone(),
            client,
            manifest,
            ConflictPolicy::Overwrite,
            PerformanceSettings::default(),
        )
        .await
        .unwrap();
    let task_persistence_elapsed = transfer_started.elapsed();
    let payload_started = Instant::now();

    loop {
        let current = client_db
            .list_tasks()
            .await
            .unwrap()
            .into_iter()
            .find(|item| item.id == task.id)
            .unwrap();
        match current.status.as_str() {
            "completed" => break,
            "failed" => panic!("loopback transfer failed: {:?}", current.error),
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let transfer_elapsed = transfer_started.elapsed();
    let payload_elapsed = payload_started.elapsed();
    eprintln!(
        "LanFlow E2E task persistence: {:.3}s; payload: {:.3}s; total: {:.3}s",
        task_persistence_elapsed.as_secs_f64(),
        payload_elapsed.as_secs_f64(),
        transfer_elapsed.as_secs_f64(),
    );
    eprintln!(
        "LanFlow E2E transfer: {file_count} files, {:.1} MiB at {:.1} MiB/s, {:.0} files/s",
        total_bytes as f64 / 1_048_576.0,
        total_bytes as f64 / 1_048_576.0 / transfer_elapsed.as_secs_f64().max(0.001),
        file_count as f64 / transfer_elapsed.as_secs_f64().max(0.001),
    );
    let (copied_files, copied_bytes) = tree_stats(&destination);
    assert_eq!(copied_files, file_count, "destination file count differs");
    assert_eq!(copied_bytes, total_bytes, "destination byte count differs");

    shutdown.cancel();
    server.await.unwrap().unwrap();
    std::fs::remove_dir_all(scratch).unwrap();
}

fn tree_stats(root: &std::path::Path) -> (u64, u64) {
    let mut files = 0u64;
    let mut bytes = 0u64;
    for entry in jwalk::WalkDir::new(root).follow_links(false) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() {
            files += 1;
            bytes += entry.metadata().unwrap().len();
        }
    }
    (files, bytes)
}

fn reserve_loopback_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn connect_with_retry(address: &str) -> Arc<PeerClient> {
    let mut last_error = None;
    for _ in 0..100 {
        match PeerClient::connect(
            address.into(),
            "benchmark-client".into(),
            "Benchmark Client".into(),
            true,
        )
        .await
        {
            Ok(client) => return client,
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("loopback server did not start: {last_error:?}");
}
