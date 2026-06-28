use anyhow::Result;
use codex_features::Feature;
use codex_protocol::config_types::ServiceTier;
use core_test_support::responses::WebSocketConnectionConfig;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::ev_shell_command_call;
use core_test_support::responses::start_websocket_server;
use core_test_support::responses::start_websocket_server_with_headers;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::time::Duration;

const WS_V2_BETA_HEADER_VALUE: &str = "responses_websockets=2026-02-06";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_test_codex_shell_chain() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let call_id = "shell-command-call";
    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_shell_command_call(call_id, "echo websocket"),
            ev_completed("resp-1"),
        ]],
        vec![vec![
            ev_response_created("resp-2"),
            ev_assistant_message("msg-1", "done"),
            ev_completed("resp-2"),
        ]],
    ])
    .await;

    let mut builder = test_codex().with_windows_cmd_shell();

    let test = builder.build_with_websocket_server(&server).await?;
    test.submit_turn_with_policy("run the echo command", test.config.legacy_sandbox_policy())
        .await?;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);

    let first_turn = connections[0]
        .first()
        .expect("missing first turn request")
        .body_json();
    let second_turn = connections[1]
        .first()
        .expect("missing second turn request")
        .body_json();

    assert_eq!(first_turn["type"].as_str(), Some("response.create"));
    assert_eq!(second_turn["type"].as_str(), Some("response.create"));

    let input_items = second_turn
        .get("input")
        .and_then(Value::as_array)
        .expect("second response.create input array");
    assert!(!input_items.is_empty());

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_first_turn_opens_on_create() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_assistant_message("msg-1", "hello"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let mut builder = test_codex();
    let test = builder.build_with_websocket_server(&server).await?;
    test.submit_turn_with_policy("hello", test.config.legacy_sandbox_policy())
        .await?;

    assert_eq!(server.handshakes().len(), 1);
    let connections = server.connections();
    assert_eq!(connections.len(), 1);
    let turn = connections[0]
        .first()
        .expect("missing turn request")
        .body_json();
    assert!(turn.get("client_metadata").is_none());
    assert!(
        turn["tools"]
            .as_array()
            .is_some_and(|tools| !tools.is_empty()),
        "expected request tools to be populated"
    );
    assert_eq!(turn["type"].as_str(), Some("response.create"));

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_first_turn_handles_handshake_delay() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_websocket_server_with_headers(vec![WebSocketConnectionConfig {
        requests: vec![vec![
            ev_response_created("resp-1"),
            ev_assistant_message("msg-1", "hello"),
            ev_completed("resp-1"),
        ]],
        response_headers: Vec::new(),
        // Delay handshake so turn processing must tolerate websocket startup latency.
        accept_delay: Some(Duration::from_millis(150)),
        close_after_requests: true,
    }])
    .await;

    let mut builder = test_codex();
    let test = builder.build_with_websocket_server(&server).await?;
    test.submit_turn_with_policy("hello", test.config.legacy_sandbox_policy())
        .await?;

    assert_eq!(server.handshakes().len(), 1);
    let connections = server.connections();
    assert_eq!(connections.len(), 1);
    let turn = connections[0]
        .first()
        .expect("missing turn request")
        .body_json();
    assert!(
        turn["tools"]
            .as_array()
            .is_some_and(|tools| !tools.is_empty()),
        "expected request tools to be populated"
    );
    assert_eq!(turn["type"].as_str(), Some("response.create"));

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_v2_test_codex_shell_chain() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let call_id = "shell-command-call";
    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_shell_command_call(call_id, "echo websocket"),
            ev_completed("resp-1"),
        ]],
        vec![vec![
            ev_response_created("resp-2"),
            ev_assistant_message("msg-1", "done"),
            ev_completed("resp-2"),
        ]],
    ])
    .await;

    let mut builder = test_codex().with_windows_cmd_shell().with_config(|config| {
        config
            .features
            .enable(Feature::ResponsesWebsocketsV2)
            .expect("test config should allow feature update");
    });

    let test = builder.build_with_websocket_server(&server).await?;
    test.submit_turn_with_policy("run the echo command", test.config.legacy_sandbox_policy())
        .await?;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);

    let first_turn = connections[0]
        .first()
        .expect("missing first turn request")
        .body_json();
    let second_turn = connections[1]
        .first()
        .expect("missing second turn request")
        .body_json();

    assert_eq!(first_turn["type"].as_str(), Some("response.create"));
    assert_eq!(first_turn.get("previous_response_id"), None);
    assert!(
        first_turn
            .get("input")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
    );
    assert_eq!(second_turn["type"].as_str(), Some("response.create"));
    assert_eq!(second_turn.get("previous_response_id"), None);

    let create_items = second_turn
        .get("input")
        .and_then(Value::as_array)
        .expect("response.create input array");
    assert!(!create_items.is_empty());

    let output_item = create_items
        .iter()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("function_call_output"))
        .expect("function_call_output in create");
    assert_eq!(
        output_item.get("call_id").and_then(Value::as_str),
        Some(call_id)
    );

    let handshakes = server.handshakes();
    let handshake = &handshakes[0];
    assert_eq!(
        handshake.header("openai-beta"),
        Some(WS_V2_BETA_HEADER_VALUE.to_string())
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_v2_first_turn_uses_updated_fast_tier() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_assistant_message("msg-1", "fast"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::ResponsesWebsocketsV2)
            .expect("test config should allow feature update");
    });
    let test = builder.build_with_websocket_server(&server).await?;

    test.submit_turn_with_service_tier("hello", Some(ServiceTier::Fast.request_value()))
        .await?;

    assert_eq!(server.handshakes().len(), 1);
    let connections = server.connections();
    assert_eq!(connections.len(), 1);
    let first_turn = connections[0]
        .first()
        .expect("missing first turn request")
        .body_json();

    assert_eq!(first_turn["type"].as_str(), Some("response.create"));
    assert_eq!(first_turn["service_tier"].as_str(), Some("priority"));
    assert_eq!(first_turn.get("previous_response_id"), None);
    assert!(
        first_turn
            .get("input")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_v2_first_turn_drops_fast_tier() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_assistant_message("msg-1", "standard"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::ResponsesWebsocketsV2)
            .expect("test config should allow feature update");
        config.service_tier = Some(ServiceTier::Fast.request_value().to_string());
    });
    let test = builder.build_with_websocket_server(&server).await?;

    test.submit_turn_with_service_tier("hello", /*service_tier*/ None)
        .await?;

    assert_eq!(server.handshakes().len(), 1);
    let connections = server.connections();
    assert_eq!(connections.len(), 1);
    let first_turn = connections[0]
        .first()
        .expect("missing first turn request")
        .body_json();

    assert_eq!(first_turn["type"].as_str(), Some("response.create"));
    assert_eq!(first_turn.get("service_tier"), None);
    assert_eq!(first_turn.get("previous_response_id"), None);
    assert!(
        first_turn
            .get("input")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_v2_next_turn_uses_updated_service_tier() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_assistant_message("msg-1", "fast"),
            ev_completed("resp-1"),
        ]],
        vec![vec![
            ev_response_created("resp-2"),
            ev_assistant_message("msg-2", "standard"),
            ev_completed("resp-2"),
        ]],
    ])
    .await;

    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::ResponsesWebsocketsV2)
            .expect("test config should allow feature update");
    });
    let test = builder.build_with_websocket_server(&server).await?;

    test.submit_turn_with_service_tier("first", Some(ServiceTier::Fast.request_value()))
        .await?;
    test.submit_turn_with_service_tier("second", /*service_tier*/ None)
        .await?;

    assert_eq!(server.handshakes().len(), 2);
    let connections = server.connections();
    assert_eq!(connections.len(), 2);

    let first_turn = connections[0]
        .first()
        .expect("missing first turn request")
        .body_json();
    let second_turn = connections[1]
        .first()
        .expect("missing second turn request")
        .body_json();

    assert_eq!(first_turn["type"].as_str(), Some("response.create"));
    assert_eq!(first_turn["service_tier"].as_str(), Some("priority"));
    assert_eq!(first_turn.get("previous_response_id"), None);
    assert!(
        first_turn
            .get("input")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
    );

    assert_eq!(second_turn["type"].as_str(), Some("response.create"));
    assert_eq!(second_turn.get("service_tier"), None);
    assert_eq!(second_turn.get("previous_response_id"), None);
    assert!(
        second_turn
            .get("input")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
    );

    server.shutdown().await;
    Ok(())
}
