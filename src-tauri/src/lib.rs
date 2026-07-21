mod commands;
mod core;

use std::sync::Arc;

use core::AppCore;
use tauri::menu::{Menu, MenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{Manager, WindowEvent};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let builder = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_window_state::Builder::default().build());

    #[cfg(desktop)]
    let builder = builder.plugin(tauri_plugin_autostart::init(
        tauri_plugin_autostart::MacosLauncher::LaunchAgent,
        None,
    ));

    builder
        .setup(|app| {
            let core = tauri::async_runtime::block_on(AppCore::initialize(app.handle().clone()))?;
            app.manage(core);

            let show = MenuItem::with_id(app, "show", "显示 LanFlow", true, None::<&str>)?;
            let pause = MenuItem::with_id(app, "pause_all", "暂停全部任务", true, None::<&str>)?;
            let quit = MenuItem::with_id(app, "quit", "退出", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&show, &pause, &quit])?;
            let mut tray = TrayIconBuilder::new()
                .menu(&menu)
                .tooltip("LanFlow 局域网文件传输")
                .show_menu_on_left_click(false)
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "show" => {
                        if let Some(window) = app.get_webview_window("main") {
                            let _ = window.show();
                            let _ = window.set_focus();
                        }
                    }
                    "pause_all" => {
                        let core = app.state::<Arc<AppCore>>().inner().clone();
                        tauri::async_runtime::spawn(async move {
                            let _ = core.task_engine.pause_all().await;
                        });
                    }
                    "quit" => app.exit(0),
                    _ => {}
                });
            if let Some(icon) = app.default_window_icon() {
                tray = tray.icon(icon.clone());
            }
            tray.build(app)?;

            if let Some(window) = app.get_webview_window("main") {
                let window_to_hide = window.clone();
                window.on_window_event(move |event| {
                    if let WindowEvent::CloseRequested { api, .. } = event {
                        api.prevent_close();
                        let _ = window_to_hide.hide();
                    }
                });
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_overview,
            commands::create_share,
            commands::update_share,
            commands::set_share_enabled,
            commands::delete_share,
            commands::list_peers,
            commands::connect_peer,
            commands::connect_discovered_peer,
            commands::list_remote_shares,
            commands::authenticate_peer,
            commands::authenticate_with_saved_password,
            commands::list_remote_entries,
            commands::create_download_task,
            commands::list_tasks,
            commands::pause_task,
            commands::resume_task,
            commands::cancel_task,
            commands::save_settings,
        ])
        .run(tauri::generate_context!())
        .expect("error while running LanFlow");
}
