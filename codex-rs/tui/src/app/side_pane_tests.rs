use super::*;
use crate::app::test_support::make_test_app;
use codex_app_server_client::AppServerEvent;
use codex_app_server_protocol::ToolRequestUserInputParams;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn side_pane_ignores_foreign_notifications_and_approvals() -> Result<()> {
    let mut app = make_test_app().await;
    let server = crate::start_embedded_app_server_for_picker(app.chat_widget.config_ref()).await?;
    let parent = ThreadId::new();
    let side = ThreadId::new();
    app.primary_thread_id = Some(parent);
    app.active_thread_id = Some(parent);
    app.side_pane_ignored_threads.insert(side);
    app.handle_app_server_event(
        &server,
        AppServerEvent::ServerRequest(Box::new(ServerRequest::ToolRequestUserInput {
            request_id: codex_app_server_protocol::RequestId::Integer(1),
            params: ToolRequestUserInputParams {
                thread_id: side.to_string(),
                turn_id: "turn".into(),
                item_id: "item".into(),
                questions: Vec::new(),
                is_blocking: true,
                auto_resolution_ms: None,
            },
        })),
    )
    .await;
    app.handle_app_server_event(
        &server,
        AppServerEvent::ServerNotification(Box::new(ServerNotification::ThreadClosed(
            codex_app_server_protocol::ThreadClosedNotification {
                thread_id: side.to_string(),
            },
        ))),
    )
    .await;
    assert_eq!(app.current_displayed_thread_id(), Some(parent));
    assert!(!app.thread_event_channels.contains_key(&side));
    assert!(app.chat_widget.no_modal_or_popup_active());
    Ok(())
}

#[tokio::test]
async fn side_pane_startup_failure_restores_draft_without_switching_parent() -> Result<()> {
    let mut app = make_test_app().await;
    let mut server =
        crate::start_embedded_app_server_for_picker(app.chat_widget.config_ref()).await?;
    let parent = ThreadId::new();
    let launch_id = Uuid::new_v4();
    app.primary_thread_id = Some(parent);
    app.active_thread_id = Some(parent);
    app.side_pane = Some(SidePaneState {
        launch_id,
        launch: None,
        thread_id: None,
        ready: false,
        watcher: None,
        message: Some(crate::chatwidget::UserMessage::from(
            "prepared question".to_string(),
        )),
    });
    app.handle_side_pane_event(
        &mut server,
        launch_id,
        SidePaneEvent::Failed("child failed".into()),
    )
    .await;
    assert_eq!(app.current_displayed_thread_id(), Some(parent));
    assert_eq!(
        app.chat_widget.composer_text_with_pending(),
        "prepared question"
    );
    assert!(app.side_pane.is_none());
    Ok(())
}

#[tokio::test]
async fn side_pane_child_uses_hidden_history_and_exits_instead_of_switching() -> Result<()> {
    let mut app = Box::pin(make_test_app()).await;
    let (widget, events, _event_rx, mut ops) =
        crate::chatwidget::tests::make_chatwidget_manual_with_sender().await;
    app.chat_widget = widget;
    app.app_event_tx = events;
    app.config = app.chat_widget.config_ref().clone();
    app.local_settings = crate::local_settings::LocalSettings::from(&app.config);
    let config = app.config.clone();
    let parent = ThreadId::from_string(
        &app_test_support::create_fake_rollout(
            &config.codex_home,
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Inherited question",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create parent rollout"),
    )?;
    let mut server = Box::pin(crate::start_embedded_app_server_for_picker(&config)).await?;
    server
        .resume_thread(
            &app.local_settings,
            config,
            parent,
            crate::app_server_session::ResumeModelSettings::RestoreFromThread,
        )
        .await?;
    let handoff = SidePaneHandoff::new(
        &AppServerTarget::Remote {
            endpoint: codex_app_server_client::RemoteAppServerEndpoint::WebSocket {
                websocket_url: "ws://127.0.0.1:1".into(),
                auth_token: None,
            },
        },
        parent,
        server.side_fork_params(app.side_fork_config(), parent),
        Some("side question".into()),
    )?;
    let directory = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            directory.path(),
            std::fs::Permissions::from_mode(/*mode*/ 0o700),
        )?;
    }
    let path = directory.path().join("handoff");
    std::fs::write(&path, serde_json::to_vec(&handoff)?)?;
    std::fs::write(directory.path().join("owned"), "null")?;
    let (_, child) = SidePaneChild::read(&path)?;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    Box::pin(app.start_side_pane_child(&mut tui, &mut server, handoff, child)).await?;
    let side = app
        .primary_thread_id
        .expect("side is primary in child frontend");
    assert_ne!(side, parent);
    assert_eq!(app.active_side_parent_thread_id(), Some(parent));
    assert!(app.side_pane_ignores_thread(parent));
    assert!(!app.side_pane_ignores_thread(side));
    assert!(
        app.thread_event_channels
            .get(&side)
            .expect("side channel")
            .store
            .lock()
            .await
            .snapshot()
            .turns
            .is_empty()
    );
    assert!(directory.path().join("ready").exists());
    let mut submitted = Vec::new();
    while let Ok(op) = ops.try_recv() {
        if let crate::app_command::AppCommand::UserTurn { items, .. } = op {
            submitted.push(items);
        }
    }
    assert_eq!(
        submitted,
        vec![vec![codex_app_server_protocol::UserInput::Text {
            text: "side question".into(),
            text_elements: Vec::new(),
        }]]
    );
    Box::pin(app.handle_event(&mut tui, &mut server, AppEvent::NewSession { name: None })).await?;
    assert_eq!(app.primary_thread_id, Some(side));
    assert!(Box::pin(app.maybe_return_from_side(&mut tui, &mut server)).await);
    assert_eq!(app.current_displayed_thread_id(), Some(side));
    assert!(app.begin_reconnect());
    assert!(!app.reconnect.offline);
    app.shutdown_side_threads(&mut server).await;
    Ok(())
}
