use anyhow::Result;
use anyhow::bail;
use app_test_support::MockResponsesConfig;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_request_permissions_sse_response;
use app_test_support::to_response;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadSource;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStatus;
use codex_features::Feature;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

use super::connection_handling_websocket::WsClient;
use super::connection_handling_websocket::connect_websocket;
use super::connection_handling_websocket::read_jsonrpc_message;
use super::connection_handling_websocket::read_response_for_id;
use super::connection_handling_websocket::send_initialize_request;
use super::connection_handling_websocket::send_jsonrpc;
use super::connection_handling_websocket::send_request;
use super::connection_handling_websocket::spawn_websocket_server;

#[tokio::test]
async fn side_fork_approval_is_answered_by_child_while_parent_turn_remains_active() -> Result<()> {
    let server = responses::start_mock_server().await;
    let mock = responses::mount_sse_sequence(
        &server,
        vec![
            create_request_permissions_sse_response("parent-permission")?,
            create_request_permissions_sse_response("side-permission")?,
            create_final_assistant_message_sse_response("side done")?,
            create_final_assistant_message_sse_response("parent done")?,
        ],
    )
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_approval_policy("on-request")
        .with_root_config("thread_unload_delay_secs = 0")
        .enable_feature(Feature::RequestPermissionsTool)
        .write(codex_home.path())?;
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut parent = connect_websocket(bind_addr).await?;
    send_initialize_request(&mut parent, /*id*/ 1, "parent").await?;
    read_response_for_id(&mut parent, /*id*/ 1).await?;
    send_request(
        &mut parent,
        "thread/start",
        /*id*/ 2,
        Some(serde_json::to_value(ThreadStartParams::default())?),
    )
    .await?;
    let started: ThreadStartResponse =
        to_response(read_response_for_id(&mut parent, /*id*/ 2).await?)?;
    start_turn(&mut parent, &started.thread.id, "parent question").await?;
    let parent_approval = read_permission_request(&mut parent, &started.thread.id).await?;

    // Connect after the main thread exists: the side frontend does not resume it.
    let mut child = connect_websocket(bind_addr).await?;
    send_initialize_request(&mut child, /*id*/ 1, "side").await?;
    read_response_for_id(&mut child, /*id*/ 1).await?;
    send_request(
        &mut child,
        "thread/fork",
        /*id*/ 2,
        Some(serde_json::to_value(ThreadForkParams {
            thread_id: started.thread.id.clone(),
            ephemeral: true,
            exclude_turns: true,
            thread_source: Some(ThreadSource::Feature("side_conversation".into())),
            ..Default::default()
        })?),
    )
    .await?;
    let fork: ThreadForkResponse = to_response(read_response_for_id(&mut child, /*id*/ 2).await?)?;
    assert_eq!(
        (
            fork.thread.ephemeral,
            fork.thread.path.as_ref(),
            fork.thread.forked_from_id.as_deref(),
            fork.thread.turns.as_slice(),
        ),
        (true, None, Some(started.thread.id.as_str()), [].as_slice()),
    );

    // The side fork subscribes only its creator, so approvals have one owner.
    start_turn(&mut child, &fork.thread.id, "side question").await?;
    let side_approval = read_permission_request(&mut child, &fork.thread.id).await?;
    answer_permission(&mut child, side_approval).await?;
    read_completed_turn(&mut child, &fork.thread.id).await?;
    assert_eq!(mock.requests().len(), 3);

    let side_input = mock.requests()[1].input();
    assert!(side_input.iter().any(|item| item["role"] == "user"
        && item["content"].as_array().is_some_and(|content| {
            content
                .iter()
                .any(|content| content["text"] == "parent question")
        })));
    assert_eq!(
        side_input
            .iter()
            .filter(|item| item["role"] == "user")
            .flat_map(|item| item["content"].as_array().into_iter().flatten())
            .filter(|content| content["text"] == "side question")
            .count(),
        1,
    );
    // Even an abrupt frontend disconnect releases its ephemeral fork without
    // requiring the parent to have learned its ID or explicitly unsubscribed.
    child.close(/*msg*/ None).await?;
    loop {
        match read_jsonrpc_message(&mut parent).await? {
            JSONRPCMessage::Request(request) => {
                bail!("side approval leaked to parent: {request:?}")
            }
            JSONRPCMessage::Notification(notification) => {
                if notification.method == "thread/closed" {
                    assert_eq!(
                        notification.params,
                        Some(json!({ "threadId": fork.thread.id }))
                    );
                    break;
                }
                if notification.method.starts_with("turn/")
                    || notification.method.starts_with("item/")
                {
                    assert_ne!(
                        notification
                            .params
                            .as_ref()
                            .and_then(|params| params.get("threadId")),
                        Some(&json!(fork.thread.id))
                    );
                }
            }
            _ => {}
        }
    }

    // Closing the side leaves the parent's pending approval and active turn intact.
    answer_permission(&mut parent, parent_approval).await?;
    read_completed_turn(&mut parent, &started.thread.id).await?;
    assert_eq!(mock.requests().len(), 4);
    process.kill().await?;
    Ok(())
}

async fn start_turn(client: &mut WsClient, thread_id: &str, question: &str) -> Result<()> {
    send_request(
        client,
        "turn/start",
        /*id*/ 3,
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": question }],
        })),
    )
    .await?;
    read_response_for_id(client, /*id*/ 3).await?;
    Ok(())
}

async fn read_permission_request(client: &mut WsClient, thread_id: &str) -> Result<RequestId> {
    loop {
        if let JSONRPCMessage::Request(request) = read_jsonrpc_message(client).await? {
            let ServerRequest::PermissionsRequestApproval { request_id, params } =
                ServerRequest::try_from(request)?
            else {
                bail!("expected a permission approval");
            };
            assert_eq!(params.thread_id, thread_id);
            return Ok(request_id);
        }
    }
}

async fn answer_permission(client: &mut WsClient, id: RequestId) -> Result<()> {
    send_jsonrpc(
        client,
        JSONRPCMessage::Response(JSONRPCResponse {
            id,
            result: json!({ "permissions": {}, "scope": "turn" }),
        }),
    )
    .await
}

async fn read_completed_turn(client: &mut WsClient, thread_id: &str) -> Result<()> {
    loop {
        match read_jsonrpc_message(client).await? {
            JSONRPCMessage::Notification(notification)
                if notification.method == "turn/completed" =>
            {
                let completed: TurnCompletedNotification =
                    serde_json::from_value(notification.params.unwrap())?;
                if completed.thread_id == thread_id {
                    assert_eq!(completed.turn.status, TurnStatus::Completed);
                    return Ok(());
                }
            }
            JSONRPCMessage::Request(request) => {
                bail!("duplicate or foreign approval: {request:?}");
            }
            _ => {}
        }
    }
}
