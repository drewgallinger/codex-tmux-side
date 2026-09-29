use super::*;
use crate::bottom_pane::LocalImageAttachment;
use crate::bottom_pane::MentionBinding;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn private_handoff_preserves_backend_settings_message_and_ownership() -> Result<()> {
    let directory = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            directory.path(),
            std::fs::Permissions::from_mode(/*mode*/ 0o700),
        )?;
    }
    let parent_thread_id = ThreadId::new();
    let target = AppServerTarget::Remote {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url: "wss://example.com/rpc".into(),
            auth_token: Some("private-token".into()),
        },
    };
    let settings = ThreadForkParams {
        thread_id: parent_thread_id.to_string(),
        model: Some("selected-model".into()),
        ephemeral: true,
        ..Default::default()
    };
    let message = UserMessage {
        text: "Explain @app and this image".into(),
        local_images: vec![LocalImageAttachment {
            placeholder: "[Image #1]".into(),
            path: directory.path().join("image.png"),
        }],
        remote_image_urls: vec!["data:image/png;base64,aGVsbG8=".into()],
        text_elements: vec![],
        mention_bindings: vec![MentionBinding {
            sigil: '@',
            mention: "app".into(),
            path: "app://example".into(),
        }],
    };
    let handoff = SidePaneHandoff::new(
        &target,
        parent_thread_id,
        settings.clone(),
        Some(message.clone()),
    )?;
    write_json(directory.path(), "handoff", &handoff)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(directory.path().join("handoff"))?.mode() & 0o777,
            0o600
        );
    }
    let (received, child) = SidePaneChild::read(&directory.path().join("handoff"))?;
    assert_eq!(
        (received.target(), received.settings, received.message),
        (target, settings, Some(message))
    );
    assert!(!directory.path().join("handoff").exists());
    let thread_id = ThreadId::new();
    child.forked(thread_id)?;
    assert_eq!(
        serde_json::from_slice::<ThreadId>(&std::fs::read(directory.path().join("forked"))?)?,
        thread_id
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(/*millis*/ 1),
            child.wait_for_ownership()
        )
        .await
        .is_err()
    );
    write_json(directory.path(), "owned", &())?;
    child.wait_for_ownership().await?;
    child.ready()?;
    assert!(directory.path().join("ready").exists());
    child.failed("startup failed")?;
    assert_eq!(
        serde_json::from_slice::<String>(&std::fs::read(directory.path().join("failed"))?)?,
        "startup failed"
    );
    let mut invalid = handoff;
    invalid.settings.ephemeral = false;
    write_json(directory.path(), "handoff", &invalid)?;
    assert!(SidePaneChild::read(&directory.path().join("handoff")).is_err());
    directory.close()?;
    assert!(child.wait_for_ownership().await.is_err());
    Ok(())
}

#[test]
fn split_targets_source_on_right_with_local_cwd_and_private_handoff() {
    let mut command = tokio::process::Command::new("tmux");
    split_command(
        &mut command,
        "%12",
        Path::new("/local workspace"),
        Path::new("/bin/codex"),
        Path::new("/private/handoff"),
        Path::new("/local home"),
    );
    assert_eq!(
        command.as_std().get_args().collect::<Vec<_>>(),
        [
            "split-window",
            "-h",
            "-d",
            "-P",
            "-F",
            "#{pane_id}",
            "-t",
            "%12",
            "-c",
            "/local workspace",
            "-e",
            "CODEX_HOME=/local home",
            "--",
            "/bin/codex",
            "--side-handoff",
            "/private/handoff",
        ]
        .map(std::ffi::OsStr::new)
    );
}

#[test]
fn shared_daemon_handoff_never_falls_back_to_embedded() -> Result<()> {
    let target = AppServerTarget::LocalDaemon {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url: "ws://localhost:3000".into(),
            auth_token: None,
        },
        allow_embedded_fallback: true,
    };
    let handoff = SidePaneHandoff::new(
        &target,
        ThreadId::new(),
        ThreadForkParams::default(),
        /*message*/ None,
    )?;
    assert_eq!(
        handoff.target(),
        AppServerTarget::LocalDaemon {
            endpoint: RemoteAppServerEndpoint::WebSocket {
                websocket_url: "ws://localhost:3000".into(),
                auth_token: None
            },
            allow_embedded_fallback: false,
        }
    );
    assert!(
        SidePaneHandoff::new(
            &AppServerTarget::Embedded,
            ThreadId::new(),
            ThreadForkParams::default(),
            /*message*/ None
        )
        .is_err()
    );
    assert!(!valid_pane("-t evil"));
    Ok(())
}
