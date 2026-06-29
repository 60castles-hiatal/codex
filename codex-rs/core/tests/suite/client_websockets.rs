#![allow(clippy::expect_used, clippy::unwrap_used)]
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use codex_core::CodexResponsesMetadata;
use codex_core::ModelClient;
use codex_core::ModelClientSession;
use codex_core::Prompt;
use codex_core::ResponseEvent;
use codex_core::X_RESPONSESAPI_INCLUDE_TIMING_METRICS_HEADER;
use codex_features::Feature;
use codex_login::CodexAuth;
use codex_login::REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_otel::MetricsClient;
use codex_otel::MetricsConfig;
use codex_otel::SessionTelemetry;
use codex_otel::TelemetryAuthMode;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::account::PlanType;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ReasoningEffort as ReasoningEffortConfig;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::user_input::UserInput;
use codex_rollout_trace::ConversationPart;
use codex_rollout_trace::InferenceTraceContext;
use codex_rollout_trace::RawTraceEventPayload;
use codex_rollout_trace::TraceWriter;
use codex_rollout_trace::replay_bundle;
use core_test_support::TestCodexResponsesRequestKind;
use core_test_support::load_default_config_for_test;
use core_test_support::responses::WebSocketConnectionConfig;
use core_test_support::responses::WebSocketHandshake;
use core_test_support::responses::WebSocketTestServer;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::start_websocket_server;
use core_test_support::responses::start_websocket_server_with_headers;
use core_test_support::responses_metadata as test_responses_metadata;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use futures::SinkExt;
use futures::StreamExt;
use http::HeaderName;
use http::HeaderValue;
use opentelemetry_sdk::metrics::InMemoryMetricExporter;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use serial_test::serial;
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_tungstenite::accept_hdr_async_with_config;
use tokio_tungstenite::tungstenite::extensions::ExtensionsConfig;
use tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig;
use tokio_tungstenite::tungstenite::handshake::server::Request;
use tokio_tungstenite::tungstenite::handshake::server::Response;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tracing_test::traced_test;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::Respond;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

const MODEL: &str = "gpt-5.3-codex";
const OPENAI_BETA_HEADER: &str = "OpenAI-Beta";
const USER_AGENT_HEADER: &str = "user-agent";
const WS_V2_BETA_HEADER_VALUE: &str = "responses_websockets=2026-02-06";
const CODEX_AUTH_ROTATION_JSON_ENV: &str = "CODEX_AUTH_ROTATION_JSON";
const X_CODEX_TURN_STATE_HEADER: &str = "x-codex-turn-state";
const TEST_INSTALLATION_ID: &str = "11111111-1111-4111-8111-111111111111";
const TEST_WINDOW_ID: &str = "test-thread:0";

fn assert_no_client_metadata(body: &serde_json::Value) {
    assert!(
        body.get("client_metadata").is_none(),
        "client_metadata should not be sent"
    );
}

fn assert_no_handshake_metadata_headers(handshake: &WebSocketHandshake) {
    for header in [
        "session-id",
        "thread-id",
        "x-client-request-id",
        "x-openai-subagent",
        "x-codex-installation-id",
        "x-codex-window-id",
        "x-codex-parent-thread-id",
        "x-codex-turn-metadata",
    ] {
        assert_eq!(handshake.header(header), None, "{header} should be absent");
    }
}

struct WebsocketTestHarness {
    _codex_home: TempDir,
    client: ModelClient,
    session_id: SessionId,
    thread_id: ThreadId,
    model_info: ModelInfo,
    effort: Option<ReasoningEffortConfig>,
    summary: ReasoningSummary,
    session_telemetry: SessionTelemetry,
}

fn responses_metadata(
    harness: &WebsocketTestHarness,
    turn_id: Option<&str>,
    request_kind: TestCodexResponsesRequestKind,
) -> CodexResponsesMetadata {
    test_responses_metadata(
        TEST_INSTALLATION_ID,
        &harness.session_id.to_string(),
        &harness.thread_id.to_string(),
        turn_id,
        TEST_WINDOW_ID.to_string(),
        &SessionSource::Exec,
        /*parent_thread_id*/ None,
        request_kind,
    )
}

fn turn_metadata(harness: &WebsocketTestHarness, turn_id: Option<&str>) -> CodexResponsesMetadata {
    responses_metadata(harness, turn_id, TestCodexResponsesRequestKind::Turn)
}

fn prewarm_metadata(
    harness: &WebsocketTestHarness,
    turn_id: Option<&str>,
) -> CodexResponsesMetadata {
    responses_metadata(harness, turn_id, TestCodexResponsesRequestKind::Prewarm)
}

fn websocket_connection_metadata(harness: &WebsocketTestHarness) -> CodexResponsesMetadata {
    responses_metadata(
        harness,
        /*turn_id*/ None,
        TestCodexResponsesRequestKind::WebsocketConnection,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_streams_request() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let mut prompt = prompt_with_input(vec![message_item("hello")]);
    prompt.input[0].set_id(Some("msg_existing".to_string()));

    stream_until_complete(&mut client_session, &harness, &prompt).await;

    let connection = server.single_connection();
    assert_eq!(connection.len(), 1);
    let body = connection.first().expect("missing request").body_json();

    assert_eq!(body["type"].as_str(), Some("response.create"));
    assert_eq!(body["model"].as_str(), Some(MODEL));
    assert_eq!(body["stream"], serde_json::Value::Bool(true));
    assert_eq!(body["input"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["input"][0].get("id"), None);
    let handshake = server.single_handshake();
    assert_eq!(
        handshake.header(OPENAI_BETA_HEADER),
        Some(WS_V2_BETA_HEADER_VALUE.to_string())
    );
    assert_eq!(
        handshake.header(USER_AGENT_HEADER),
        Some(codex_login::default_client::get_codex_user_agent())
    );
    assert_no_handshake_metadata_headers(&handshake);
    assert_no_client_metadata(&body);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_streams_without_hangup_feature_when_provider_supports_websockets() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ false).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);

    stream_until_complete(&mut client_session, &harness, &prompt).await;

    assert_eq!(server.handshakes().len(), 1);
    assert_eq!(server.single_connection().len(), 1);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_waits_for_completed_without_hangup_feature() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_assistant_message("msg-1", "assistant output"),
    ]]])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);
    let mut stream = client_session
        .stream(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    let mut saw_error_before_completed = false;
    while let Some(event) = stream.next().await {
        match event {
            Ok(ResponseEvent::Completed { .. }) => {
                panic!("websocket_hangup=false should not complete before response.completed");
            }
            Ok(_) => {}
            Err(_) => {
                saw_error_before_completed = true;
                break;
            }
        }
    }

    assert!(saw_error_before_completed);
    assert_eq!(server.handshakes().len(), 1);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_hangup_finishes_before_completed_when_enabled() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_assistant_message("msg-1", "assistant output"),
    ]]])
    .await;

    let harness = websocket_harness_with_hangup(&server, /*runtime_metrics_enabled*/ false).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);
    let mut stream = client_session
        .stream(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    let mut completed_response_id = None;
    while let Some(event) = stream.next().await {
        if let ResponseEvent::Completed { response_id, .. } =
            event.expect("websocket stream should not error")
        {
            completed_response_id = Some(response_id);
            break;
        }
    }

    assert_eq!(completed_response_id.as_deref(), Some(""));
    assert_eq!(server.handshakes().len(), 1);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_reconnects_without_client_metadata_payloads() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness(&server).await;
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![message_item("again")]);

    {
        let mut client_session = harness.client.new_session();
        stream_until_complete(&mut client_session, &harness, &prompt_one).await;
    }

    {
        let mut client_session = harness.client.new_session();
        stream_until_complete(&mut client_session, &harness, &prompt_two).await;
    }

    assert_eq!(server.handshakes().len(), 2);
    assert_eq!(
        server.handshakes()[0].header(USER_AGENT_HEADER),
        Some(codex_login::default_client::get_codex_user_agent())
    );
    let connections = server.connections();
    assert_eq!(connections.len(), 2);

    let first_request = connections[0]
        .first()
        .expect("missing first request")
        .body_json();
    let second_request = connections[1]
        .first()
        .expect("missing second request")
        .body_json();
    assert_no_client_metadata(&first_request);
    assert_no_client_metadata(&second_request);
    assert!(first_request.get("trace").is_none());
    assert!(second_request.get("trace").is_none());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_preconnect_noop_then_stream_omits_client_metadata_payload() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let responses_metadata = websocket_connection_metadata(&harness);
    client_session
        .preconnect_websocket(&harness.session_telemetry, &responses_metadata)
        .await
        .expect("websocket preconnect failed");
    assert!(server.handshakes().is_empty());
    assert!(server.connections().is_empty());

    let prompt = prompt_with_input(vec![message_item("hello")]);

    stream_until_complete(&mut client_session, &harness, &prompt).await;

    assert_eq!(server.handshakes().len(), 1);
    let connection = server.single_connection();
    assert_eq!(connection.len(), 1);
    let request = connection.first().expect("missing request").body_json();
    assert_no_client_metadata(&request);
    assert!(request.get("trace").is_none());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_preconnect_noop_stream_opens_connection() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let responses_metadata = websocket_connection_metadata(&harness);
    client_session
        .preconnect_websocket(&harness.session_telemetry, &responses_metadata)
        .await
        .expect("websocket preconnect failed");
    assert!(server.handshakes().is_empty());
    assert!(server.connections().is_empty());

    let prompt = prompt_with_input(vec![message_item("hello")]);
    stream_until_complete(&mut client_session, &harness, &prompt).await;

    assert_eq!(server.handshakes().len(), 1);
    assert_eq!(
        server.single_handshake().header(USER_AGENT_HEADER),
        Some(codex_login::default_client::get_codex_user_agent())
    );
    let connection = server.single_connection();
    assert_eq!(connection.len(), 1);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_request_prewarm_noop_streams_first_request() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ true).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let responses_metadata = prewarm_metadata(&harness, /*turn_id*/ None);
    client_session
        .prewarm_websocket(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
        )
        .await
        .expect("websocket prewarm failed");
    assert!(server.handshakes().is_empty());
    assert!(server.connections().is_empty());

    stream_until_complete(&mut client_session, &harness, &prompt).await;

    assert_eq!(server.handshakes().len(), 1);
    assert_eq!(
        server.handshakes()[0].header(USER_AGENT_HEADER),
        Some(codex_login::default_client::get_codex_user_agent())
    );
    let connections = server.connections();
    assert_eq!(connections.len(), 1);
    let follow_up = connections[0]
        .first()
        .expect("missing first request")
        .body_json();

    assert_no_client_metadata(&follow_up);
    assert_eq!(follow_up["type"].as_str(), Some("response.create"));
    assert_eq!(follow_up.get("previous_response_id"), None);
    assert_eq!(
        follow_up["input"],
        serde_json::to_value(&prompt.input).expect("serialize full input")
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_request_prewarm_noop_sends_no_request() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("warm-1"),
        ev_completed("warm-1"),
    ]]])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ true).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);
    client_session
        .prewarm_websocket(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
        )
        .await
        .expect("websocket prewarm failed");

    assert!(server.handshakes().is_empty());
    assert!(server.connections().is_empty());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_request_prewarm_noop_traces_real_request() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ true).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let prewarm_responses_metadata = prewarm_metadata(&harness, /*turn_id*/ None);

    client_session
        .prewarm_websocket(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &prewarm_responses_metadata,
        )
        .await
        .expect("websocket prewarm failed");
    assert!(server.handshakes().is_empty());
    assert!(server.connections().is_empty());

    let trace_dir = TempDir::new().expect("trace dir");
    let writer = Arc::new(
        TraceWriter::create(
            trace_dir.path(),
            "trace-1".to_string(),
            harness.session_id.to_string(),
            harness.thread_id.to_string(),
        )
        .expect("trace writer"),
    );
    writer
        .append(RawTraceEventPayload::ThreadStarted {
            thread_id: harness.thread_id.to_string(),
            agent_path: "/root".to_string(),
            metadata_payload: None,
        })
        .expect("thread started");
    writer
        .append(RawTraceEventPayload::CodexTurnStarted {
            codex_turn_id: "turn-1".to_string(),
            thread_id: harness.thread_id.to_string(),
        })
        .expect("turn started");

    let inference_trace = InferenceTraceContext::enabled(
        writer,
        harness.thread_id.to_string(),
        "turn-1".to_string(),
        harness.model_info.slug.clone(),
        "test-provider".to_string(),
    );

    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);
    let mut stream = client_session
        .stream(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &inference_trace,
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    while let Some(event) = stream.next().await {
        if matches!(event, Ok(ResponseEvent::Completed { .. })) {
            break;
        }
    }

    let connections = server.connections();
    let follow_up = connections[0]
        .first()
        .expect("missing first request")
        .body_json();
    assert_eq!(follow_up.get("previous_response_id"), None);
    assert_eq!(
        follow_up["input"],
        serde_json::to_value(&prompt.input).expect("serialize full input")
    );

    let rollout = replay_bundle(trace_dir.path()).expect("replay trace");
    let inference = rollout
        .inference_calls
        .values()
        .next()
        .expect("inference should be present");
    assert_eq!(inference.request_item_ids.len(), 1);
    assert_eq!(
        rollout.conversation_items[&inference.request_item_ids[0]]
            .body
            .parts,
        vec![ConversationPart::Text {
            text: "hello".to_string(),
        }],
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_reconnects_after_session_drop() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness(&server).await;
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![message_item("again")]);

    {
        let mut client_session = harness.client.new_session();
        stream_until_complete(&mut client_session, &harness, &prompt_one).await;
    }

    let mut client_session = harness.client.new_session();
    stream_until_complete(&mut client_session, &harness, &prompt_two).await;

    assert_eq!(server.handshakes().len(), 2);
    assert_eq!(server.connections().iter().map(Vec::len).sum::<usize>(), 2);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_sets_responses_lite_request_shape_without_client_metadata() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("normal-1"),
            ev_completed("normal-1"),
        ]],
        vec![vec![ev_response_created("lite-1"), ev_completed("lite-1")]],
        vec![vec![
            ev_response_created("normal-2"),
            ev_completed("normal-2"),
        ]],
    ])
    .await;

    let harness = websocket_harness(&server).await;
    let mut normal_model_info = harness.model_info.clone();
    normal_model_info.supports_reasoning_summaries = true;
    let mut lite_model_info = normal_model_info.clone();
    lite_model_info.use_responses_lite = true;
    let mut session = harness.client.new_session();

    stream_until_complete_with_model_info(
        &mut session,
        &harness,
        &prompt_with_input(vec![message_item("normal one")]),
        &normal_model_info,
        "normal-1",
    )
    .await;
    stream_until_complete_with_model_info(
        &mut session,
        &harness,
        &prompt_with_input(vec![message_item("lite")]),
        &lite_model_info,
        "lite-1",
    )
    .await;
    stream_until_complete_with_model_info(
        &mut session,
        &harness,
        &prompt_with_input(vec![message_item("normal two")]),
        &normal_model_info,
        "normal-2",
    )
    .await;

    let connection: Vec<_> = server.connections().into_iter().flatten().collect();
    assert_eq!(
        connection
            .iter()
            .map(|request| {
                let body = request.body_json();
                assert_no_client_metadata(&body);
                json!({
                    "reasoning_context": body["reasoning"].get("context"),
                    "parallel_tool_calls": body["parallel_tool_calls"],
                })
            })
            .collect::<Vec<_>>(),
        vec![
            json!({
                "reasoning_context": null,
                "parallel_tool_calls": false,
            }),
            json!({
                "reasoning_context": "all_turns",
                "parallel_tool_calls": false,
            }),
            json!({
                "reasoning_context": null,
                "parallel_tool_calls": false,
            }),
        ]
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_preconnect_noop_streams_even_with_header_changes() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let preconnect_metadata = websocket_connection_metadata(&harness);
    client_session
        .preconnect_websocket(&harness.session_telemetry, &preconnect_metadata)
        .await
        .expect("websocket preconnect failed");
    assert!(server.handshakes().is_empty());
    assert!(server.connections().is_empty());

    let prompt = prompt_with_input(vec![message_item("hello")]);
    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);
    let mut stream = client_session
        .stream(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    while let Some(event) = stream.next().await {
        if matches!(event, Ok(ResponseEvent::Completed { .. })) {
            break;
        }
    }

    assert_eq!(server.handshakes().len(), 1);
    assert_eq!(server.single_connection().len(), 1);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_request_prewarm_noop_streams_even_with_header_changes() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ true).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let prewarm_responses_metadata = prewarm_metadata(&harness, /*turn_id*/ None);
    client_session
        .prewarm_websocket(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &prewarm_responses_metadata,
        )
        .await
        .expect("websocket prewarm failed");
    assert!(server.handshakes().is_empty());
    assert!(server.connections().is_empty());

    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);
    let mut stream = client_session
        .stream(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    while let Some(event) = stream.next().await {
        if matches!(event, Ok(ResponseEvent::Completed { .. })) {
            break;
        }
    }

    assert_eq!(server.handshakes().len(), 1);
    let connections = server.connections();
    assert_eq!(connections.len(), 1);
    let follow_up = connections[0]
        .first()
        .expect("missing first request")
        .body_json();
    assert_eq!(follow_up["type"].as_str(), Some("response.create"));
    assert_eq!(follow_up.get("previous_response_id"), None);
    assert_eq!(
        follow_up["input"],
        serde_json::to_value(&prompt.input).expect("serialize full input")
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_prewarm_noop_first_request_uses_v2() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ false).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let responses_metadata = prewarm_metadata(&harness, /*turn_id*/ None);
    client_session
        .prewarm_websocket(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
        )
        .await
        .expect("websocket prewarm failed");

    assert!(server.handshakes().is_empty());
    assert!(server.connections().is_empty());

    stream_until_complete(&mut client_session, &harness, &prompt).await;
    assert_eq!(server.handshakes().len(), 1);
    let handshake = server.single_handshake();
    let openai_beta_header = handshake
        .header(OPENAI_BETA_HEADER)
        .expect("missing OpenAI-Beta header");
    assert!(
        openai_beta_header
            .split(',')
            .map(str::trim)
            .any(|value| value == WS_V2_BETA_HEADER_VALUE)
    );
    let connections = server.connections();
    assert_eq!(connections.len(), 1);
    let request = connections[0]
        .first()
        .expect("missing turn request")
        .body_json();
    assert_eq!(request["type"].as_str(), Some("response.create"));
    assert_eq!(request.get("previous_response_id"), None);
    assert_eq!(
        request["input"],
        serde_json::to_value(&prompt.input).unwrap()
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_preconnect_noop_when_only_v2_feature_enabled() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ true).await;
    let mut client_session = harness.client.new_session();
    let responses_metadata = websocket_connection_metadata(&harness);
    client_session
        .preconnect_websocket(&harness.session_telemetry, &responses_metadata)
        .await
        .expect("websocket preconnect failed");

    assert!(server.handshakes().is_empty());
    assert!(server.connections().is_empty());

    let prompt = prompt_with_input(vec![message_item("hello")]);
    stream_until_complete(&mut client_session, &harness, &prompt).await;

    assert_eq!(server.handshakes().len(), 1);
    assert_eq!(server.single_connection().len(), 1);

    let handshake = server.single_handshake();
    let openai_beta_header = handshake
        .header(OPENAI_BETA_HEADER)
        .expect("missing OpenAI-Beta header");
    assert!(
        openai_beta_header
            .split(',')
            .map(str::trim)
            .any(|value| value == WS_V2_BETA_HEADER_VALUE)
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_v2_requests_use_v2_when_provider_supports_websockets() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_assistant_message("msg-1", "assistant output"),
            ev_completed("resp-1"),
        ]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ true).await;
    let mut client_session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![
        message_item("hello"),
        assistant_message_item("msg-1", "assistant output"),
        message_item("second"),
    ]);

    stream_until_complete(&mut client_session, &harness, &prompt_one).await;
    stream_until_complete(&mut client_session, &harness, &prompt_two).await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let second = connections[1].first().expect("missing request").body_json();
    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_eq!(second["input"], expected_wire_input(&prompt_two.input));

    let handshakes = server.handshakes();
    let handshake = &handshakes[0];
    let openai_beta_header = handshake
        .header(OPENAI_BETA_HEADER)
        .expect("missing OpenAI-Beta header");
    assert!(
        openai_beta_header
            .split(',')
            .map(str::trim)
            .any(|value| value == WS_V2_BETA_HEADER_VALUE)
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_v2_full_requests_reconnect_across_turns() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_assistant_message("msg-1", "assistant output"),
            ev_completed("resp-1"),
        ]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ false).await;
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![
        message_item("hello"),
        assistant_message_item("msg-1", "assistant output"),
        message_item("second"),
    ]);

    {
        let mut client_session = harness.client.new_session();
        stream_until_complete(&mut client_session, &harness, &prompt_one).await;
    }

    let mut client_session = harness.client.new_session();
    stream_until_complete(&mut client_session, &harness, &prompt_two).await;

    assert_eq!(server.handshakes().len(), 2);
    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let second = connections[1].first().expect("missing request").body_json();
    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_eq!(second["input"], expected_wire_input(&prompt_two.input));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_v2_wins_when_both_features_enabled() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_assistant_message("msg-1", "assistant output"),
            ev_completed("resp-1"),
        ]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness_with_options(&server, /*runtime_metrics_enabled*/ false).await;
    let mut client_session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![
        message_item("hello"),
        assistant_message_item("msg-1", "assistant output"),
        message_item("second"),
    ]);

    stream_until_complete(&mut client_session, &harness, &prompt_one).await;
    stream_until_complete(&mut client_session, &harness, &prompt_two).await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let second = connections[1].first().expect("missing request").body_json();
    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_eq!(second["input"], expected_wire_input(&prompt_two.input));

    let handshakes = server.handshakes();
    let handshake = &handshakes[0];
    let openai_beta_header = handshake
        .header(OPENAI_BETA_HEADER)
        .expect("missing OpenAI-Beta header");
    assert!(
        openai_beta_header
            .split(',')
            .map(str::trim)
            .any(|value| value == WS_V2_BETA_HEADER_VALUE)
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[traced_test]
async fn responses_websocket_emits_websocket_telemetry_events() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness(&server).await;
    harness.session_telemetry.reset_runtime_metrics();
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);

    stream_until_complete(&mut client_session, &harness, &prompt).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    let summary = harness
        .session_telemetry
        .runtime_metrics_summary()
        .expect("runtime metrics summary");
    assert_eq!(summary.api_calls.count, 0);
    assert_eq!(summary.streaming_events.count, 0);
    assert_eq!(summary.websocket_calls.count, 1);
    assert_eq!(summary.websocket_events.count, 2);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_includes_timing_metrics_header_when_runtime_metrics_enabled() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        serde_json::json!({
            "type": "responsesapi.websocket_timing",
            "timing_metrics": {
                "responses_duration_excl_engine_and_client_tool_time_ms": 120,
                "engine_service_total_ms": 450,
                "engine_iapi_ttft_total_ms": 310,
                "engine_service_ttft_total_ms": 340,
                "engine_iapi_tbt_across_engine_calls_ms": 220,
                "engine_service_tbt_across_engine_calls_ms": 260
            }
        }),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness =
        websocket_harness_with_runtime_metrics(&server, /*runtime_metrics_enabled*/ true).await;
    harness.session_telemetry.reset_runtime_metrics();
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);

    stream_until_complete(&mut client_session, &harness, &prompt).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let handshake = server.single_handshake();
    assert_eq!(
        handshake.header(X_RESPONSESAPI_INCLUDE_TIMING_METRICS_HEADER),
        Some("true".to_string())
    );

    let summary = harness
        .session_telemetry
        .runtime_metrics_summary()
        .expect("runtime metrics summary");
    assert_eq!(summary.responses_api_overhead_ms, 120);
    assert_eq!(summary.responses_api_inference_time_ms, 450);
    assert_eq!(summary.responses_api_engine_iapi_ttft_ms, 310);
    assert_eq!(summary.responses_api_engine_service_ttft_ms, 340);
    assert_eq!(summary.responses_api_engine_iapi_tbt_ms, 220);
    assert_eq!(summary.responses_api_engine_service_tbt_ms, 260);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_omits_timing_metrics_header_when_runtime_metrics_disabled() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness =
        websocket_harness_with_runtime_metrics(&server, /*runtime_metrics_enabled*/ false).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);

    stream_until_complete(&mut client_session, &harness, &prompt).await;

    let handshake = server.single_handshake();
    assert_eq!(
        handshake.header(X_RESPONSESAPI_INCLUDE_TIMING_METRICS_HEADER),
        None
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_emits_reasoning_included_event() {
    skip_if_no_network!();

    let server = start_websocket_server_with_headers(vec![WebSocketConnectionConfig {
        requests: vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
        response_headers: vec![("X-Reasoning-Included".to_string(), "true".to_string())],
        accept_delay: None,
        close_after_requests: true,
    }])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);

    let mut stream = client_session
        .stream(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    let mut saw_reasoning_included = false;
    while let Some(event) = stream.next().await {
        match event.expect("event") {
            ResponseEvent::ServerReasoningIncluded(true) => {
                saw_reasoning_included = true;
            }
            ResponseEvent::Completed { .. } => break,
            _ => {}
        }
    }

    assert!(saw_reasoning_included);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_emits_rate_limit_events() {
    skip_if_no_network!();

    let rate_limit_event = json!({
        "type": "codex.rate_limits",
        "plan_type": "plus",
        "rate_limits": {
            "allowed": true,
            "limit_reached": false,
            "primary": {
                "used_percent": 42,
                "window_minutes": 60,
                "reset_at": 1700000000
            },
            "secondary": null
        },
        "code_review_rate_limits": null,
        "credits": {
            "has_credits": true,
            "unlimited": false,
            "balance": "123"
        },
        "promo": null
    });

    let server = start_websocket_server_with_headers(vec![WebSocketConnectionConfig {
        requests: vec![vec![
            rate_limit_event,
            ev_response_created("resp-1"),
            ev_completed("resp-1"),
        ]],
        response_headers: vec![
            ("X-Models-Etag".to_string(), "etag-123".to_string()),
            ("X-Reasoning-Included".to_string(), "true".to_string()),
        ],
        accept_delay: None,
        close_after_requests: true,
    }])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);

    let mut stream = client_session
        .stream(
            &prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    let mut saw_rate_limits = None;
    let mut saw_models_etag = None;
    let mut saw_reasoning_included = false;

    while let Some(event) = stream.next().await {
        match event.expect("event") {
            ResponseEvent::RateLimits(snapshot) => {
                saw_rate_limits = Some(snapshot);
            }
            ResponseEvent::ModelsEtag(etag) => {
                saw_models_etag = Some(etag);
            }
            ResponseEvent::ServerReasoningIncluded(true) => {
                saw_reasoning_included = true;
            }
            ResponseEvent::Completed { .. } => break,
            _ => {}
        }
    }

    let rate_limits = saw_rate_limits.expect("missing rate limits");
    let primary = rate_limits.primary.expect("missing primary window");
    assert_eq!(primary.used_percent, 42.0);
    assert_eq!(primary.window_minutes, Some(60));
    assert_eq!(primary.resets_at, Some(1_700_000_000));
    assert_eq!(rate_limits.plan_type, Some(PlanType::Plus));
    let credits = rate_limits.credits.expect("missing credits");
    assert!(credits.has_credits);
    assert!(!credits.unlimited);
    assert_eq!(credits.balance.as_deref(), Some("123"));
    assert_eq!(saw_models_etag.as_deref(), Some("etag-123"));
    assert!(saw_reasoning_included);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_usage_limit_error_emits_rate_limit_event() {
    skip_if_no_network!();

    let usage_limit_error = json!({
        "type": "error",
        "status": 429,
        "error": {
            "type": "usage_limit_reached",
            "message": "The usage limit has been reached",
            "plan_type": "pro",
            "resets_at": 1704067242,
            "resets_in_seconds": 1234
        },
        "headers": {
            "x-codex-primary-used-percent": "100.0",
            "x-codex-secondary-used-percent": "87.5",
            "x-codex-primary-over-secondary-limit-percent": "95.0",
            "x-codex-primary-window-minutes": "15",
            "x-codex-secondary-window-minutes": "60"
        }
    });

    let server = start_websocket_server(vec![vec![vec![usage_limit_error]]]).await;
    let mut builder = test_codex().with_config(|config| {
        config.model_provider.request_max_retries = Some(0);
        config.model_provider.stream_max_retries = Some(0);
    });
    let test = builder
        .build_with_websocket_server(&server)
        .await
        .expect("build websocket codex");

    let submission_id = test
        .codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "hello".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await
        .expect("submission should succeed while emitting usage limit error events");

    let token_event =
        wait_for_event(&test.codex, |msg| matches!(msg, EventMsg::TokenCount(_))).await;
    let EventMsg::TokenCount(event) = token_event else {
        unreachable!();
    };

    let event_json = serde_json::to_value(&event).expect("serialize token count event");
    pretty_assertions::assert_eq!(
        event_json,
        json!({
            "info": null,
            "rate_limits": {
                "limit_id": "codex",
                "limit_name": null,
                "primary": {
                    "used_percent": 100.0,
                    "window_minutes": 15,
                    "resets_at": null
                },
                "secondary": {
                    "used_percent": 87.5,
                    "window_minutes": 60,
                    "resets_at": null
                },
                "credits": null,
                "individual_limit": null,
                "plan_type": null,
                "rate_limit_reached_type": null
            }
        })
    );

    let error_event = wait_for_event(&test.codex, |msg| matches!(msg, EventMsg::Error(_))).await;
    let EventMsg::Error(error_event) = error_event else {
        unreachable!();
    };
    assert!(
        error_event.message.to_lowercase().contains("usage limit"),
        "unexpected error message for submission {submission_id}: {}",
        error_event.message
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_invalid_request_error_with_status_is_forwarded() {
    skip_if_no_network!();

    let invalid_request_error = json!({
        "type": "error",
        "status": 400,
        "error": {
            "type": "invalid_request_error",
            "message": "Model 'castor-raikou-0205-ev3' does not support image inputs."
        }
    });

    let server = start_websocket_server(vec![vec![vec![invalid_request_error]]]).await;
    let mut builder = test_codex().with_config(|config| {
        config.model_provider.request_max_retries = Some(0);
        config.model_provider.stream_max_retries = Some(0);
    });
    let test = builder
        .build_with_websocket_server(&server)
        .await
        .expect("build websocket codex");

    let submission_id = test
        .codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "hello".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await
        .expect("submission should succeed while emitting invalid request events");

    let error_event = wait_for_event(&test.codex, |msg| matches!(msg, EventMsg::Error(_))).await;
    let EventMsg::Error(error_event) = error_event else {
        unreachable!();
    };
    assert!(
        error_event
            .message
            .to_lowercase()
            .contains("does not support image inputs"),
        "unexpected error message for submission {submission_id}: {}",
        error_event.message
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_connection_limit_error_reconnects_and_completes() {
    skip_if_no_network!();

    let websocket_connection_limit_error = json!({
        "type": "error",
        "status": 400,
        "error": {
            "type": "invalid_request_error",
            "code": "websocket_connection_limit_reached",
            "message": "Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue."
        }
    });

    let server = start_websocket_server(vec![
        vec![vec![websocket_connection_limit_error]],
        vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
    ])
    .await;
    let mut builder = test_codex().with_config(|config| {
        config.model_provider.request_max_retries = Some(0);
        config.model_provider.stream_max_retries = Some(1);
    });
    let test = builder
        .build_with_websocket_server(&server)
        .await
        .expect("build websocket codex");

    test.submit_turn("hello")
        .await
        .expect("submission should reconnect after websocket connection limit error");

    let total_websocket_requests: usize = server.connections().iter().map(Vec::len).sum();
    assert_eq!(total_websocket_requests, 2);
    let handshake_user_agents: Vec<_> = server
        .handshakes()
        .iter()
        .map(|handshake| handshake.header(USER_AGENT_HEADER))
        .collect();
    assert_eq!(
        handshake_user_agents,
        vec![
            Some(codex_login::default_client::get_codex_user_agent()),
            Some(codex_login::default_client::get_codex_user_agent()),
        ]
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_creates_full_request_on_prefix_after_reconnect() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_assistant_message("msg-1", "assistant output"),
            ev_completed("resp-1"),
        ]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![
        message_item("hello"),
        assistant_message_item("msg-1", "assistant output"),
        message_item("second"),
    ]);

    stream_until_complete(&mut client_session, &harness, &prompt_one).await;
    stream_until_complete(&mut client_session, &harness, &prompt_two).await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let first = connections[0].first().expect("missing request").body_json();
    let second = connections[1].first().expect("missing request").body_json();

    assert_eq!(first["type"].as_str(), Some("response.create"));
    assert_eq!(first["model"].as_str(), Some(MODEL));
    assert_eq!(first["stream"], serde_json::Value::Bool(true));
    assert_eq!(first["input"].as_array().map(Vec::len), Some(1));
    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_eq!(second["input"], expected_wire_input(&prompt_two.input));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_omits_client_metadata_on_reconnected_create() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_assistant_message("msg-1", "assistant output"),
            ev_completed("resp-1"),
        ]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![
        message_item("hello"),
        assistant_message_item("msg-1", "assistant output"),
        message_item("second"),
    ]);
    let first_responses_metadata = turn_metadata(&harness, Some("turn-123"));
    let second_responses_metadata = turn_metadata(&harness, Some("turn-456"));

    stream_until_complete_with_metadata(
        &mut client_session,
        &harness,
        &prompt_one,
        /*service_tier*/ None,
        &first_responses_metadata,
    )
    .await;
    stream_until_complete_with_metadata(
        &mut client_session,
        &harness,
        &prompt_two,
        /*service_tier*/ None,
        &second_responses_metadata,
    )
    .await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let first = connections[0].first().expect("missing request").body_json();
    let second = connections[1].first().expect("missing request").body_json();

    assert_eq!(first["type"].as_str(), Some("response.create"));
    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_no_client_metadata(&first);
    assert_no_client_metadata(&second);
    assert_no_handshake_metadata_headers(&server.handshakes()[0]);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_omits_turn_metadata_on_handshake() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);
    let responses_metadata = turn_metadata(&harness, Some("turn-123"));

    stream_until_complete_with_metadata(
        &mut client_session,
        &harness,
        &prompt,
        /*service_tier*/ None,
        &responses_metadata,
    )
    .await;

    let body = server
        .single_connection()
        .first()
        .expect("missing request")
        .body_json();

    assert_eq!(body["type"].as_str(), Some("response.create"));
    assert_no_client_metadata(&body);
    assert_no_handshake_metadata_headers(&server.single_handshake());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_creates_full_request_when_prefix_after_completed() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_assistant_message("msg-1", "assistant output"),
            ev_completed("resp-1"),
        ]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![
        message_item("hello"),
        assistant_message_item("msg-1", "assistant output"),
        message_item("second"),
    ]);

    stream_until_complete(&mut client_session, &harness, &prompt_one).await;
    stream_until_complete(&mut client_session, &harness, &prompt_two).await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let second = connections[1].first().expect("missing request").body_json();

    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_eq!(second["input"], expected_wire_input(&prompt_two.input));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_creates_on_non_prefix() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![message_item("different")]);

    stream_until_complete(&mut client_session, &harness, &prompt_one).await;
    stream_until_complete(&mut client_session, &harness, &prompt_two).await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let second = connections[1].first().expect("missing request").body_json();

    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second["model"].as_str(), Some(MODEL));
    assert_eq!(second["stream"], serde_json::Value::Bool(true));
    assert_eq!(
        second["input"],
        serde_json::to_value(&prompt_two.input).unwrap()
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_creates_when_non_input_request_fields_change() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness(&server).await;
    let mut client_session = harness.client.new_session();
    let prompt_one =
        prompt_with_input_and_instructions(vec![message_item("hello")], "base instructions one");
    let prompt_two = prompt_with_input_and_instructions(
        vec![message_item("hello"), message_item("second")],
        "base instructions two",
    );

    stream_until_complete(&mut client_session, &harness, &prompt_one).await;
    stream_until_complete(&mut client_session, &harness, &prompt_two).await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let second = connections[1].first().expect("missing request").body_json();

    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_eq!(second["input"], expected_wire_input(&prompt_two.input));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_v2_creates_full_request_on_prefix_after_reconnect() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![
            ev_response_created("resp-1"),
            ev_assistant_message("msg-1", "assistant output"),
            ev_completed("resp-1"),
        ]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness_with_v2(&server, /*runtime_metrics_enabled*/ true).await;
    let mut session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![
        message_item("hello"),
        assistant_message_item("msg-1", "assistant output"),
        message_item("second"),
    ]);

    stream_until_complete(&mut session, &harness, &prompt_one).await;
    stream_until_complete(&mut session, &harness, &prompt_two).await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let first = connections[0].first().expect("missing request").body_json();
    let second = connections[1].first().expect("missing request").body_json();

    assert_eq!(first["type"].as_str(), Some("response.create"));
    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_eq!(second["input"], expected_wire_input(&prompt_two.input));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn responses_websocket_auth_rotation_reconnects_to_coordinator_account() {
    skip_if_no_network!();

    let rotation_homes = TempDir::new().expect("create rotation homes");
    let account_a = create_rotation_auth_home(rotation_homes.path(), "account-a", "sk-a");
    let account_b = create_rotation_auth_home(rotation_homes.path(), "account-b", "sk-b");
    let (_coordinator, rotation_state, _env_guard) =
        install_rotation_coordinator_env(&[account_a, account_b]).await;

    let server = ConcurrentWebSocketTestServer::start(vec![
        WebSocketConnectionConfig {
            response_headers: vec![(X_CODEX_TURN_STATE_HEADER.to_string(), "state-a".to_string())],
            close_after_requests: true,
            accept_delay: None,
            requests: vec![vec![
                ev_response_created("resp-1"),
                ev_assistant_message("msg-1", "assistant output one"),
                ev_completed("resp-1"),
            ]],
        },
        WebSocketConnectionConfig {
            response_headers: vec![(X_CODEX_TURN_STATE_HEADER.to_string(), "state-a".to_string())],
            close_after_requests: true,
            accept_delay: None,
            requests: vec![vec![
                ev_response_created("resp-2"),
                ev_assistant_message("msg-2", "assistant output two"),
                ev_completed("resp-2"),
            ]],
        },
        WebSocketConnectionConfig {
            response_headers: Vec::new(),
            close_after_requests: true,
            accept_delay: None,
            requests: vec![vec![ev_response_created("resp-3"), ev_completed("resp-3")]],
        },
    ])
    .await;

    let harness = websocket_harness_with_provider_options(
        websocket_provider_from_uri(server.uri(), /*websocket_connect_timeout_ms*/ None),
        /*runtime_metrics_enabled*/ true,
    )
    .await;
    let mut session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![
        message_item("hello"),
        assistant_message_item("msg-1", "assistant output one"),
        message_item("second"),
    ]);
    let prompt_three = prompt_with_input(vec![
        message_item("hello"),
        assistant_message_item("msg-1", "assistant output one"),
        message_item("second"),
        assistant_message_item("msg-2", "assistant output two"),
        message_item("third"),
    ]);

    stream_until_complete(&mut session, &harness, &prompt_one).await;
    stream_until_complete(&mut session, &harness, &prompt_two).await;

    let handshakes = server.handshakes();
    assert_eq!(handshakes.len(), 2);
    assert_eq!(
        handshakes[0].header("authorization").as_deref(),
        Some("Bearer sk-a")
    );
    assert_eq!(
        handshakes[1].header("authorization").as_deref(),
        Some("Bearer sk-a")
    );
    {
        let mut state = rotation_state.lock().expect("rotation state lock poisoned");
        state.active_index = 1;
        state.generation = 1;
    }
    stream_until_complete(&mut session, &harness, &prompt_three).await;

    assert!(
        server.wait_for_handshakes(3, Duration::from_secs(1)).await,
        "coordinator-selected account should connect"
    );
    let handshakes = server.handshakes();
    assert_eq!(
        handshakes[0].header("authorization").as_deref(),
        Some("Bearer sk-a")
    );
    assert_eq!(
        handshakes[1].header("authorization").as_deref(),
        Some("Bearer sk-a")
    );
    assert_eq!(
        handshakes[2].header("authorization").as_deref(),
        Some("Bearer sk-b")
    );
    assert_eq!(handshakes[2].header(X_CODEX_TURN_STATE_HEADER), None);

    let connections = server.connections();
    assert_eq!(connections.len(), 3);
    assert_eq!(connections[0].len(), 1);
    assert_eq!(connections[1].len(), 1);
    assert_eq!(connections[2].len(), 1);
    let promoted_request = &connections[2][0];
    assert_eq!(promoted_request["type"].as_str(), Some("response.create"));
    assert_eq!(promoted_request.get("previous_response_id"), None);
    assert_eq!(
        promoted_request["input"],
        expected_wire_input(&prompt_three.input)
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn responses_websocket_auth_rotation_fetches_same_account_auth_after_401() {
    skip_if_no_network!();

    let rotation_homes = TempDir::new().expect("create rotation homes");
    let account_a = create_rotation_chatgpt_auth_home(
        rotation_homes.path(),
        "account-a",
        "stale-access-token",
        "stale-refresh-token",
        "acct-rotation-a",
        "2020-01-01T00:00:00Z",
    );
    let account_b = create_rotation_auth_home(rotation_homes.path(), "account-b", "sk-b");
    let (_coordinator, rotation_state, _env_guard) =
        install_rotation_coordinator_env(&[account_a.clone(), account_b]).await;
    {
        let mut state = rotation_state.lock().expect("rotation state lock poisoned");
        state.account_auth_payloads[0] = chatgpt_auth_payload(
            "fresh-access-token",
            "fresh-refresh-token",
            "acct-rotation-a",
            "2099-01-01T00:00:00Z",
        );
    }
    let refresh_authority = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "direct-refresh-access-token",
            "refresh_token": "direct-refresh-token",
        })))
        .expect(0)
        .mount(&refresh_authority)
        .await;
    let refresh_url = format!("{}/oauth/token", refresh_authority.uri());
    let _refresh_env_guard =
        EnvVarGuard::set(REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR, OsStr::new(&refresh_url));

    let server = UnauthorizedThenWebSocketTestServer::start(vec![
        ev_response_created("resp-after-auth-refresh"),
        ev_completed("resp-after-auth-refresh"),
    ])
    .await;
    let harness = websocket_harness_with_provider_options(
        websocket_provider_from_uri(server.uri(), /*websocket_connect_timeout_ms*/ None),
        /*runtime_metrics_enabled*/ true,
    )
    .await;
    let mut session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);

    stream_until_complete(&mut session, &harness, &prompt).await;

    let handshakes = server.handshakes();
    assert_eq!(handshakes.len(), 1);
    assert_eq!(
        handshakes[0].header("authorization").as_deref(),
        Some("Bearer fresh-access-token")
    );
    assert_eq!(server.connections().len(), 1);
    {
        let state = rotation_state.lock().expect("rotation state lock poisoned");
        assert_eq!(state.advance_requests, Vec::<Value>::new());
        assert_eq!(
            state.account_auth_requests,
            vec![json!({
                "account_index": 0,
                "generation": 0,
                "refresh": true,
            })]
        );
    }
    let account_auth_json = std::fs::read_to_string(account_a.join("auth.json"))
        .expect("rotation auth file should remain readable");
    let account_auth: Value =
        serde_json::from_str(&account_auth_json).expect("rotation auth should remain json");
    assert_eq!(
        account_auth["tokens"]["refresh_token"].as_str(),
        Some("stale-refresh-token")
    );
    assert_eq!(account_auth["auth_mode"].as_str(), Some("chatgpt"));

    refresh_authority.verify().await;
    server.shutdown().await;
}

#[test]
#[serial]
fn responses_websocket_auth_rotation_advances_and_retries_after_usage_limit() {
    let handle = std::thread::Builder::new()
        .name("auth-rotation-usage-limit-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread runtime")
                .block_on(
                    responses_websocket_auth_rotation_advances_and_retries_after_usage_limit_inner(
                    ),
                );
        })
        .expect("spawn auth rotation usage-limit test thread");

    if let Err(payload) = handle.join() {
        std::panic::resume_unwind(payload);
    }
}

async fn responses_websocket_auth_rotation_advances_and_retries_after_usage_limit_inner() {
    skip_if_no_network!();

    let rotation_homes = TempDir::new().expect("create rotation homes");
    let account_a = create_rotation_auth_home(rotation_homes.path(), "account-a", "sk-a");
    let account_b = create_rotation_auth_home(rotation_homes.path(), "account-b", "sk-b");
    let (_coordinator, rotation_state, _env_guard) =
        install_rotation_coordinator_env(&[account_a, account_b]).await;

    let usage_limit_error = json!({
        "type": "error",
        "status": 429,
        "error": {
            "type": "usage_limit_reached",
            "message": "The usage limit has been reached"
        }
    });
    let server = start_websocket_server(vec![
        vec![vec![usage_limit_error]],
        vec![vec![
            ev_response_created("resp-after-rotation"),
            ev_completed("resp-after-rotation"),
        ]],
    ])
    .await;
    let mut builder = test_codex().with_config(|config| {
        config.model_provider.request_max_retries = Some(0);
        config.model_provider.stream_max_retries = Some(0);
    });
    let test = builder
        .build_with_websocket_server(&server)
        .await
        .expect("build websocket codex");

    test.submit_turn("hello")
        .await
        .expect("usage-limit rotation retry should complete the turn");

    assert!(
        server.wait_for_handshakes(2, Duration::from_secs(1)).await,
        "usage limit retry should reconnect with the advanced account"
    );
    let handshakes = server.handshakes();
    assert_eq!(
        handshakes[0].header("authorization").as_deref(),
        Some("Bearer sk-a")
    );
    assert_eq!(
        handshakes[1].header("authorization").as_deref(),
        Some("Bearer sk-b")
    );

    {
        let state = rotation_state.lock().expect("rotation state lock poisoned");
        assert_eq!(state.active_index, 1);
        assert_eq!(state.generation, 1);
        assert_eq!(
            state.advance_requests,
            vec![json!({
                "account_index": 0,
                "generation": 0,
                "reason": "usage_limit_reached",
            })]
        );
    }

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    assert_eq!(connections[0].len(), 1);
    assert_eq!(connections[1].len(), 1);
    let retry_request = connections[1][0].body_json();
    assert_eq!(retry_request["type"].as_str(), Some("response.create"));
    assert_eq!(retry_request.get("previous_response_id"), None);
    let retry_input = retry_request["input"]
        .as_array()
        .expect("retry request input should be an array");
    assert_eq!(
        retry_input.last().cloned(),
        Some(json!({
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "hello",
            }],
        }))
    );

    server.shutdown().await;
}

#[test]
#[serial]
fn responses_websocket_auth_rotation_waits_after_full_usage_limited_pass() {
    let handle = std::thread::Builder::new()
        .name("auth-rotation-full-limit-pass-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread runtime")
                .block_on(
                    responses_websocket_auth_rotation_waits_after_full_usage_limited_pass_inner(),
                );
        })
        .expect("spawn full auth rotation usage-limit test thread");

    if let Err(payload) = handle.join() {
        std::panic::resume_unwind(payload);
    }
}

async fn responses_websocket_auth_rotation_waits_after_full_usage_limited_pass_inner() {
    skip_if_no_network!();

    let rotation_homes = TempDir::new().expect("create rotation homes");
    let account_a = create_rotation_auth_home(rotation_homes.path(), "account-a", "sk-a");
    let account_b = create_rotation_auth_home(rotation_homes.path(), "account-b", "sk-b");
    let (_coordinator, rotation_state, _env_guard) =
        install_rotation_coordinator_env(&[account_a, account_b]).await;

    let reset_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_secs() as i64
        + 2;
    let usage_limit_error = |reset_at| {
        json!({
            "type": "error",
            "status": 429,
            "error": {
                "type": "usage_limit_reached",
                "message": "The usage limit has been reached",
                "resets_at": reset_at
            }
        })
    };
    let server = start_websocket_server(vec![
        vec![vec![usage_limit_error(reset_at)]],
        vec![vec![usage_limit_error(reset_at)]],
        vec![vec![
            ev_response_created("resp-after-full-pass-wait"),
            ev_completed("resp-after-full-pass-wait"),
        ]],
    ])
    .await;
    let mut builder = test_codex().with_config(|config| {
        config.model_provider.request_max_retries = Some(0);
        config.model_provider.stream_max_retries = Some(0);
    });
    let test = builder
        .build_with_websocket_server(&server)
        .await
        .expect("build websocket codex");

    let started = std::time::Instant::now();
    test.submit_turn("hello")
        .await
        .expect("usage-limit rotation should wait then retry");

    assert!(
        server.wait_for_handshakes(3, Duration::from_secs(5)).await,
        "usage limit retry should reconnect after waiting"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "full rotation pass should wait for the observed reset"
    );
    let handshakes = server.handshakes();
    assert_eq!(
        handshakes[0].header("authorization").as_deref(),
        Some("Bearer sk-a")
    );
    assert_eq!(
        handshakes[1].header("authorization").as_deref(),
        Some("Bearer sk-b")
    );
    assert_eq!(
        handshakes[2].header("authorization").as_deref(),
        Some("Bearer sk-a")
    );

    {
        let state = rotation_state.lock().expect("rotation state lock poisoned");
        assert_eq!(state.active_index, 0);
        assert_eq!(state.generation, 2);
        assert_eq!(
            state.advance_requests,
            vec![
                json!({
                    "account_index": 0,
                    "generation": 0,
                    "reason": "usage_limit_reached",
                }),
                json!({
                    "account_index": 1,
                    "generation": 1,
                    "reason": "usage_limit_reached",
                }),
            ]
        );
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn responses_websocket_auth_rotation_sends_compacted_history_after_account_swap() {
    skip_if_no_network!();

    let rotation_homes = TempDir::new().expect("create rotation homes");
    let account_a = create_rotation_auth_home(rotation_homes.path(), "account-a", "sk-a");
    let account_b = create_rotation_auth_home(rotation_homes.path(), "account-b", "sk-b");
    let (_coordinator, rotation_state, _env_guard) =
        install_rotation_coordinator_env(&[account_a, account_b]).await;

    let server = ConcurrentWebSocketTestServer::start(vec![
        WebSocketConnectionConfig {
            response_headers: Vec::new(),
            close_after_requests: true,
            accept_delay: None,
            requests: vec![vec![
                ev_response_created("resp-1"),
                ev_assistant_message("msg-1", "assistant output one"),
                ev_completed("resp-1"),
            ]],
        },
        WebSocketConnectionConfig {
            response_headers: Vec::new(),
            close_after_requests: true,
            accept_delay: None,
            requests: vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
        },
        WebSocketConnectionConfig {
            response_headers: Vec::new(),
            close_after_requests: true,
            accept_delay: None,
            requests: vec![vec![ev_response_created("resp-3"), ev_completed("resp-3")]],
        },
    ])
    .await;

    let harness = websocket_harness_with_provider_options(
        websocket_provider_from_uri(server.uri(), /*websocket_connect_timeout_ms*/ None),
        /*runtime_metrics_enabled*/ true,
    )
    .await;
    let mut session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello before compact")]);
    let prompt_two = prompt_with_input(vec![
        message_item("hello before compact"),
        assistant_message_item("msg-1", "assistant output one"),
        message_item("warm standby before compact"),
    ]);
    let compacted_input = vec![
        ResponseItem::Compaction {
            id: None,
            encrypted_content: "encrypted compact summary".to_string(),
            internal_chat_message_metadata_passthrough: None,
        },
        message_item("continue after compact"),
    ];
    let compacted_prompt = prompt_with_input(compacted_input.clone());

    stream_until_complete(&mut session, &harness, &prompt_one).await;
    stream_until_complete(&mut session, &harness, &prompt_two).await;
    {
        let mut state = rotation_state.lock().expect("rotation state lock poisoned");
        state.active_index = 1;
        state.generation = 1;
    }
    stream_until_complete(&mut session, &harness, &compacted_prompt).await;

    assert!(
        server.wait_for_handshakes(3, Duration::from_secs(1)).await,
        "coordinator-selected account should connect before compacted follow-up"
    );

    let handshakes = server.handshakes();
    assert_eq!(
        handshakes[0].header("authorization").as_deref(),
        Some("Bearer sk-a")
    );
    assert_eq!(
        handshakes[1].header("authorization").as_deref(),
        Some("Bearer sk-a")
    );
    assert_eq!(
        handshakes[2].header("authorization").as_deref(),
        Some("Bearer sk-b")
    );

    let connections = server.connections();
    assert_eq!(connections.len(), 3);
    assert_eq!(connections[0].len(), 1);
    assert_eq!(connections[1].len(), 1);
    assert_eq!(connections[2].len(), 1);
    let promoted_request = &connections[2][0];
    assert_eq!(promoted_request["type"].as_str(), Some("response.create"));
    assert_eq!(promoted_request.get("previous_response_id"), None);
    assert_eq!(
        promoted_request["input"],
        serde_json::to_value(&compacted_input).expect("serialize compacted input")
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_v2_creates_without_previous_response_id_when_non_input_fields_change()
{
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
        vec![vec![ev_response_created("resp-2"), ev_completed("resp-2")]],
    ])
    .await;

    let harness = websocket_harness_with_v2(&server, /*runtime_metrics_enabled*/ true).await;
    let mut session = harness.client.new_session();
    let prompt_one =
        prompt_with_input_and_instructions(vec![message_item("hello")], "base instructions one");
    let prompt_two = prompt_with_input_and_instructions(
        vec![message_item("hello"), message_item("second")],
        "base instructions two",
    );

    stream_until_complete(&mut session, &harness, &prompt_one).await;
    stream_until_complete(&mut session, &harness, &prompt_two).await;

    let connections = server.connections();
    assert_eq!(connections.len(), 2);
    let second = connections[1].first().expect("missing request").body_json();

    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_eq!(
        second["input"],
        serde_json::to_value(&prompt_two.input).expect("serialize full input")
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_v2_after_error_uses_full_create_without_previous_response_id() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![
        vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
        vec![vec![json!({
            "type": "response.failed",
            "response": {
                "error": {
                    "code": "invalid_prompt",
                    "message": "synthetic websocket failure"
                }
            }
        })]],
        vec![vec![ev_response_created("resp-3"), ev_completed("resp-3")]],
    ])
    .await;

    let harness = websocket_harness_with_v2(&server, /*runtime_metrics_enabled*/ true).await;
    let mut session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![message_item("hello"), message_item("second")]);
    let prompt_three = prompt_with_input(vec![
        message_item("hello"),
        message_item("second"),
        message_item("third"),
    ]);

    stream_until_complete(&mut session, &harness, &prompt_one).await;

    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);
    let mut second_stream = session
        .stream(
            &prompt_two,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");
    let mut saw_error = false;
    while let Some(event) = second_stream.next().await {
        if event.is_err() {
            saw_error = true;
            break;
        }
    }
    assert!(saw_error, "expected second websocket stream to error");

    stream_until_complete(&mut session, &harness, &prompt_three).await;

    assert_eq!(server.handshakes().len(), 3);

    let connections = server.connections();
    assert_eq!(connections.len(), 3);
    let first = connections[0]
        .first()
        .expect("missing first request")
        .body_json();
    let second = connections[1]
        .first()
        .expect("missing second request")
        .body_json();
    let third = connections[2]
        .first()
        .expect("missing third request")
        .body_json();

    assert_eq!(first["type"].as_str(), Some("response.create"));
    assert_eq!(second["type"].as_str(), Some("response.create"));
    assert_eq!(second.get("previous_response_id"), None);
    assert_eq!(
        second["input"],
        serde_json::to_value(&prompt_two.input).unwrap()
    );
    assert_eq!(third["type"].as_str(), Some("response.create"));
    assert_eq!(third.get("previous_response_id"), None);
    assert_eq!(
        third["input"],
        serde_json::to_value(&prompt_three.input).unwrap()
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_v2_surfaces_terminal_error_without_close_handshake() {
    skip_if_no_network!();

    let server = start_websocket_server_with_headers(vec![
        WebSocketConnectionConfig {
            requests: vec![vec![ev_response_created("resp-1"), ev_completed("resp-1")]],
            response_headers: Vec::new(),
            accept_delay: None,
            close_after_requests: true,
        },
        WebSocketConnectionConfig {
            requests: vec![vec![json!({
                "type": "response.failed",
                "response": {
                    "error": {
                        "code": "invalid_prompt",
                        "message": "synthetic websocket failure"
                    }
                }
            })]],
            response_headers: Vec::new(),
            accept_delay: None,
            close_after_requests: true,
        },
    ])
    .await;

    let harness = websocket_harness_with_v2(&server, /*runtime_metrics_enabled*/ true).await;
    let mut session = harness.client.new_session();
    let prompt_one = prompt_with_input(vec![message_item("hello")]);
    let prompt_two = prompt_with_input(vec![message_item("hello"), message_item("second")]);

    stream_until_complete(&mut session, &harness, &prompt_one).await;

    let responses_metadata = turn_metadata(&harness, /*turn_id*/ None);
    let mut second_stream = session
        .stream(
            &prompt_two,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    let saw_error = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = second_stream.next().await {
            if event.is_err() {
                return true;
            }
        }
        false
    })
    .await
    .expect("timed out waiting for terminal websocket error");

    assert!(saw_error, "expected second websocket stream to error");

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_v2_sets_openai_beta_header() {
    skip_if_no_network!();

    let server = start_websocket_server(vec![vec![vec![
        ev_response_created("resp-1"),
        ev_completed("resp-1"),
    ]]])
    .await;

    let harness = websocket_harness_with_v2(&server, /*runtime_metrics_enabled*/ true).await;
    let mut session = harness.client.new_session();
    let prompt = prompt_with_input(vec![message_item("hello")]);

    stream_until_complete(&mut session, &harness, &prompt).await;

    let handshake = server.single_handshake();
    let openai_beta_header = handshake
        .header(OPENAI_BETA_HEADER)
        .expect("missing OpenAI-Beta header");
    assert!(
        openai_beta_header
            .split(',')
            .map(str::trim)
            .any(|value| value == WS_V2_BETA_HEADER_VALUE)
    );
    server.shutdown().await;
}

fn message_item(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".into(),
        content: vec![ContentItem::InputText { text: text.into() }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn assistant_message_item(id: &str, text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: Some(id.to_string()),
        role: "assistant".into(),
        content: vec![ContentItem::OutputText { text: text.into() }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn expected_wire_input(input: &[ResponseItem]) -> serde_json::Value {
    let mut expected_input = input.to_vec();
    for item in &mut expected_input {
        item.set_id(/*new_id*/ None);
    }
    serde_json::to_value(&expected_input).expect("serialize full input")
}

fn prompt_with_input(input: Vec<ResponseItem>) -> Prompt {
    let mut prompt = Prompt::default();
    prompt.input = input;
    prompt
}

fn prompt_with_input_and_instructions(input: Vec<ResponseItem>, instructions: &str) -> Prompt {
    let mut prompt = prompt_with_input(input);
    prompt.base_instructions = BaseInstructions {
        text: instructions.to_string(),
    };
    prompt
}

fn websocket_provider(server: &WebSocketTestServer) -> ModelProviderInfo {
    websocket_provider_with_connect_timeout(server, /*websocket_connect_timeout_ms*/ None)
}

fn websocket_provider_with_connect_timeout(
    server: &WebSocketTestServer,
    websocket_connect_timeout_ms: Option<u64>,
) -> ModelProviderInfo {
    websocket_provider_from_uri(server.uri(), websocket_connect_timeout_ms)
}

fn websocket_provider_from_uri(
    uri: &str,
    websocket_connect_timeout_ms: Option<u64>,
) -> ModelProviderInfo {
    ModelProviderInfo {
        name: "mock-ws".into(),
        base_url: Some(format!("{uri}/v1")),
        env_key: None,
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Responses,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(0),
        stream_max_retries: Some(0),
        stream_idle_timeout_ms: Some(5_000),
        websocket_connect_timeout_ms,
        requires_openai_auth: false,
        supports_websockets: true,
    }
}

async fn websocket_harness(server: &WebSocketTestServer) -> WebsocketTestHarness {
    websocket_harness_with_runtime_metrics(server, /*runtime_metrics_enabled*/ false).await
}

async fn websocket_harness_with_runtime_metrics(
    server: &WebSocketTestServer,
    runtime_metrics_enabled: bool,
) -> WebsocketTestHarness {
    websocket_harness_with_options(server, runtime_metrics_enabled).await
}

async fn websocket_harness_with_v2(
    server: &WebSocketTestServer,
    runtime_metrics_enabled: bool,
) -> WebsocketTestHarness {
    websocket_harness_with_options(server, runtime_metrics_enabled).await
}

async fn websocket_harness_with_options(
    server: &WebSocketTestServer,
    runtime_metrics_enabled: bool,
) -> WebsocketTestHarness {
    websocket_harness_with_provider_options(websocket_provider(server), runtime_metrics_enabled)
        .await
}

async fn websocket_harness_with_hangup(
    server: &WebSocketTestServer,
    runtime_metrics_enabled: bool,
) -> WebsocketTestHarness {
    websocket_harness_with_provider_options_and_hangup(
        websocket_provider(server),
        runtime_metrics_enabled,
        /*websocket_hangup_enabled*/ true,
    )
    .await
}

async fn websocket_harness_with_provider_options(
    provider: ModelProviderInfo,
    runtime_metrics_enabled: bool,
) -> WebsocketTestHarness {
    websocket_harness_with_provider_options_and_hangup(
        provider,
        runtime_metrics_enabled,
        /*websocket_hangup_enabled*/ false,
    )
    .await
}

async fn websocket_harness_with_provider_options_and_hangup(
    provider: ModelProviderInfo,
    runtime_metrics_enabled: bool,
    websocket_hangup_enabled: bool,
) -> WebsocketTestHarness {
    let codex_home = TempDir::new().unwrap();
    let mut config = load_default_config_for_test(&codex_home).await;
    config.model = Some(MODEL.to_string());
    if websocket_hangup_enabled {
        config
            .features
            .enable(Feature::WebsocketHangup)
            .expect("test config should allow feature update");
    }
    if runtime_metrics_enabled {
        config
            .features
            .enable(Feature::RuntimeMetrics)
            .expect("test config should allow feature update");
    }
    let config = Arc::new(config);
    let model_info = codex_core::test_support::construct_model_info_offline(MODEL, &config);
    let thread_id = ThreadId::new();
    let session_id = SessionId::new();
    let auth_manager =
        codex_core::test_support::auth_manager_from_auth(CodexAuth::from_api_key("Test API Key"));
    let exporter = InMemoryMetricExporter::default();
    let metrics = MetricsClient::new(
        MetricsConfig::in_memory("test", "codex-core", env!("CARGO_PKG_VERSION"), exporter)
            .with_runtime_reader(),
    )
    .expect("in-memory metrics client");
    let session_telemetry = SessionTelemetry::new(
        thread_id,
        MODEL,
        model_info.slug.as_str(),
        /*account_id*/ None,
        Some("test@test.com".to_string()),
        auth_manager.auth_mode().map(TelemetryAuthMode::from),
        "test_originator".to_string(),
        /*log_user_prompts*/ false,
        "test".to_string(),
        SessionSource::Exec,
    )
    .with_metrics(metrics);
    let effort = None;
    let summary = ReasoningSummary::Auto;
    let client = ModelClient::new(
        /*auth_manager*/ None,
        thread_id,
        provider.clone(),
        SessionSource::Exec,
        config.model_verbosity,
        /*enable_request_compression*/ false,
        runtime_metrics_enabled,
        /*websocket_hangup_enabled*/ config.features.enabled(Feature::WebsocketHangup),
        /*beta_features_header*/ None,
        /*item_ids_enabled*/ config.features.enabled(Feature::ItemIds),
        /*attestation_provider*/ None,
    );

    WebsocketTestHarness {
        _codex_home: codex_home,
        client,
        session_id,
        thread_id,
        model_info,
        effort,
        summary,
        session_telemetry,
    }
}

async fn stream_until_complete(
    client_session: &mut ModelClientSession,
    harness: &WebsocketTestHarness,
    prompt: &Prompt,
) {
    stream_until_complete_with_service_tier(
        client_session,
        harness,
        prompt,
        /*service_tier*/ None,
    )
    .await;
}

async fn stream_until_complete_with_model_info(
    client_session: &mut ModelClientSession,
    harness: &WebsocketTestHarness,
    prompt: &Prompt,
    model_info: &ModelInfo,
    expected_response_id: &str,
) {
    let responses_metadata = turn_metadata(harness, /*turn_id*/ None);
    let mut stream = client_session
        .stream(
            prompt,
            model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            /*service_tier*/ None,
            &responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    while let Some(event) = stream.next().await {
        if let ResponseEvent::Completed { response_id, .. } =
            event.expect("websocket stream failed")
        {
            assert_eq!(response_id, expected_response_id);
            return;
        }
    }
    panic!("websocket stream ended before completion");
}

async fn stream_until_complete_with_service_tier(
    client_session: &mut ModelClientSession,
    harness: &WebsocketTestHarness,
    prompt: &Prompt,
    service_tier: Option<ServiceTier>,
) {
    let responses_metadata = turn_metadata(harness, /*turn_id*/ None);
    stream_until_complete_with_metadata(
        client_session,
        harness,
        prompt,
        service_tier,
        &responses_metadata,
    )
    .await;
}

async fn stream_until_complete_with_metadata(
    client_session: &mut ModelClientSession,
    harness: &WebsocketTestHarness,
    prompt: &Prompt,
    service_tier: Option<ServiceTier>,
    responses_metadata: &CodexResponsesMetadata,
) {
    let mut stream = client_session
        .stream(
            prompt,
            &harness.model_info,
            &harness.session_telemetry,
            harness.effort.clone(),
            harness.summary,
            service_tier.map(|service_tier| service_tier.request_value().to_string()),
            responses_metadata,
            &codex_rollout_trace::InferenceTraceContext::disabled(),
            /*context_management*/ None,
        )
        .await
        .expect("websocket stream failed");

    while let Some(event) = stream.next().await {
        if matches!(event, Ok(ResponseEvent::Completed { .. })) {
            break;
        }
    }
}

#[derive(Clone)]
struct ConcurrentWebSocketHandshake {
    headers: Vec<(String, String)>,
}

impl ConcurrentWebSocketHandshake {
    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
    }
}

struct ConcurrentWebSocketTestServer {
    uri: String,
    connections: Arc<Mutex<Vec<Vec<Value>>>>,
    handshakes: Arc<Mutex<Vec<ConcurrentWebSocketHandshake>>>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl ConcurrentWebSocketTestServer {
    async fn start(connections: Vec<WebSocketConnectionConfig>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind concurrent websocket server");
        let addr = listener
            .local_addr()
            .expect("concurrent websocket server address");
        let uri = format!("ws://{addr}");
        let connections_log = Arc::new(Mutex::new(Vec::new()));
        let handshakes_log = Arc::new(Mutex::new(Vec::new()));
        let pending_connections = Arc::new(Mutex::new(VecDeque::from(connections)));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let task = {
            let pending_connections = Arc::clone(&pending_connections);
            let connections_log = Arc::clone(&connections_log);
            let handshakes_log = Arc::clone(&handshakes_log);
            tokio::spawn(async move {
                loop {
                    let accept_result = tokio::select! {
                        _ = shutdown_rx.changed() => return,
                        accept_result = listener.accept() => accept_result,
                    };
                    let (stream, _) = match accept_result {
                        Ok(value) => value,
                        Err(_) => return,
                    };
                    let connection = {
                        let mut pending = pending_connections.lock().unwrap();
                        pending.pop_front()
                    };
                    let Some(connection) = connection else {
                        continue;
                    };

                    let mut handler_shutdown_rx = shutdown_rx.clone();
                    let connections_log = Arc::clone(&connections_log);
                    let handshakes_log = Arc::clone(&handshakes_log);
                    tokio::spawn(async move {
                        if let Some(delay) = connection.accept_delay {
                            tokio::select! {
                                _ = handler_shutdown_rx.changed() => return,
                                _ = tokio::time::sleep(delay) => {}
                            }
                        }

                        let response_headers = connection.response_headers.clone();
                        let handshake_log = Arc::clone(&handshakes_log);
                        let callback = move |req: &Request, mut response: Response| {
                            let headers =
                                req.headers()
                                    .iter()
                                    .filter_map(|(name, value)| {
                                        value.to_str().ok().map(|value| {
                                            (name.as_str().to_string(), value.to_string())
                                        })
                                    })
                                    .collect();
                            handshake_log
                                .lock()
                                .unwrap()
                                .push(ConcurrentWebSocketHandshake { headers });

                            let headers_mut = response.headers_mut();
                            for (name, value) in &response_headers {
                                if let (Ok(name), Ok(value)) = (
                                    HeaderName::from_bytes(name.as_bytes()),
                                    HeaderValue::from_str(value),
                                ) {
                                    headers_mut.insert(name, value);
                                }
                            }

                            Ok(response)
                        };

                        let mut ws_stream = match accept_hdr_async_with_config(
                            stream,
                            callback,
                            Some(test_websocket_accept_config()),
                        )
                        .await
                        {
                            Ok(ws_stream) => ws_stream,
                            Err(_) => return,
                        };
                        let connection_index = {
                            let mut log = connections_log.lock().unwrap();
                            log.push(Vec::new());
                            log.len() - 1
                        };

                        for request_events in connection.requests {
                            let message = tokio::select! {
                                _ = handler_shutdown_rx.changed() => return,
                                message = ws_stream.next() => message,
                            };
                            let Some(Ok(message)) = message else {
                                break;
                            };
                            if let Some(body) = parse_test_ws_request_body(message) {
                                let mut log = connections_log.lock().unwrap();
                                if let Some(connection_log) = log.get_mut(connection_index) {
                                    connection_log.push(body);
                                }
                            }

                            for event in &request_events {
                                let Ok(payload) = serde_json::to_string(event) else {
                                    continue;
                                };
                                if ws_stream.send(Message::Text(payload.into())).await.is_err() {
                                    break;
                                }
                            }
                        }

                        if connection.close_after_requests {
                            let _ = ws_stream.close(None).await;
                        } else {
                            let _ = handler_shutdown_rx.changed().await;
                        }
                    });
                }
            })
        };

        Self {
            uri,
            connections: connections_log,
            handshakes: handshakes_log,
            shutdown: shutdown_tx,
            task,
        }
    }

    fn uri(&self) -> &str {
        &self.uri
    }

    fn connections(&self) -> Vec<Vec<Value>> {
        self.connections.lock().unwrap().clone()
    }

    fn handshakes(&self) -> Vec<ConcurrentWebSocketHandshake> {
        self.handshakes.lock().unwrap().clone()
    }

    async fn wait_for_handshakes(&self, expected: usize, timeout: Duration) -> bool {
        if self.handshakes.lock().unwrap().len() >= expected {
            return true;
        }

        let deadline = tokio::time::Instant::now() + timeout;
        let poll_interval = Duration::from_millis(10);
        loop {
            if self.handshakes.lock().unwrap().len() >= expected {
                return true;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let sleep_for = std::cmp::min(poll_interval, deadline.saturating_duration_since(now));
            tokio::time::sleep(sleep_for).await;
        }
    }

    async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let mut task = self.task;
        if tokio::time::timeout(Duration::from_secs(10), &mut task)
            .await
            .is_err()
        {
            task.abort();
        }
    }
}

struct UnauthorizedThenWebSocketTestServer {
    uri: String,
    connections: Arc<Mutex<Vec<Vec<Value>>>>,
    handshakes: Arc<Mutex<Vec<ConcurrentWebSocketHandshake>>>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl UnauthorizedThenWebSocketTestServer {
    async fn start(response_events: Vec<Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unauthorised websocket server");
        let addr = listener
            .local_addr()
            .expect("unauthorised websocket server address");
        let uri = format!("ws://{addr}");
        let connections_log = Arc::new(Mutex::new(Vec::new()));
        let handshakes_log = Arc::new(Mutex::new(Vec::new()));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let task = {
            let connections_log = Arc::clone(&connections_log);
            let handshakes_log = Arc::clone(&handshakes_log);
            tokio::spawn(async move {
                let Ok((mut rejected_stream, _)) = listener.accept().await else {
                    return;
                };
                let mut ignored_request = [0_u8; 4096];
                let _ = rejected_stream.read(&mut ignored_request).await;
                let _ = rejected_stream
                    .write_all(
                        b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                    )
                    .await;
                let _ = rejected_stream.shutdown().await;

                let accept_result = tokio::select! {
                    _ = shutdown_rx.changed() => return,
                    accept_result = listener.accept() => accept_result,
                };
                let Ok((stream, _)) = accept_result else {
                    return;
                };

                let handshake_log = Arc::clone(&handshakes_log);
                let callback = move |req: &Request, response: Response| {
                    let headers = req
                        .headers()
                        .iter()
                        .filter_map(|(name, value)| {
                            value
                                .to_str()
                                .ok()
                                .map(|value| (name.as_str().to_string(), value.to_string()))
                        })
                        .collect();
                    handshake_log
                        .lock()
                        .unwrap()
                        .push(ConcurrentWebSocketHandshake { headers });

                    Ok(response)
                };

                let mut ws_stream = match accept_hdr_async_with_config(
                    stream,
                    callback,
                    Some(test_websocket_accept_config()),
                )
                .await
                {
                    Ok(ws_stream) => ws_stream,
                    Err(_) => return,
                };

                let message = tokio::select! {
                    _ = shutdown_rx.changed() => return,
                    message = ws_stream.next() => message,
                };
                let mut connection_log = Vec::new();
                if let Some(Ok(message)) = message
                    && let Some(body) = parse_test_ws_request_body(message)
                {
                    connection_log.push(body);
                }
                connections_log.lock().unwrap().push(connection_log);

                for event in &response_events {
                    let Ok(payload) = serde_json::to_string(event) else {
                        continue;
                    };
                    if ws_stream.send(Message::Text(payload.into())).await.is_err() {
                        break;
                    }
                }
                let _ = shutdown_rx.changed().await;
            })
        };

        Self {
            uri,
            connections: connections_log,
            handshakes: handshakes_log,
            shutdown: shutdown_tx,
            task,
        }
    }

    fn uri(&self) -> &str {
        &self.uri
    }

    fn handshakes(&self) -> Vec<ConcurrentWebSocketHandshake> {
        self.handshakes.lock().unwrap().clone()
    }

    fn connections(&self) -> Vec<Vec<Value>> {
        self.connections.lock().unwrap().clone()
    }

    async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let mut task = self.task;
        if tokio::time::timeout(Duration::from_secs(10), &mut task)
            .await
            .is_err()
        {
            task.abort();
        }
    }
}

fn test_websocket_accept_config() -> WebSocketConfig {
    let mut extensions = ExtensionsConfig::default();
    extensions.permessage_deflate = Some(DeflateConfig::default());

    let mut config = WebSocketConfig::default();
    config.extensions = extensions;
    config
}

fn parse_test_ws_request_body(message: Message) -> Option<Value> {
    match message {
        Message::Text(text) => serde_json::from_str(&text).ok(),
        Message::Binary(bytes) => serde_json::from_slice(&bytes).ok(),
        _ => None,
    }
}

fn create_rotation_auth_home(root: &Path, name: &str, api_key: &str) -> std::path::PathBuf {
    let home = root.join(name);
    std::fs::create_dir_all(&home).expect("create rotation account home");
    std::fs::write(
        home.join("auth.json"),
        json!({
            "OPENAI_API_KEY": api_key,
            "tokens": null,
            "last_refresh": null,
        })
        .to_string(),
    )
    .expect("write rotation auth.json");
    home
}

fn create_rotation_chatgpt_auth_home(
    root: &Path,
    name: &str,
    access_token: &str,
    refresh_token: &str,
    account_id: &str,
    last_refresh: &str,
) -> std::path::PathBuf {
    let home = root.join(name);
    std::fs::create_dir_all(&home).expect("create rotation account home");
    std::fs::write(
        home.join("auth.json"),
        chatgpt_auth_payload(access_token, refresh_token, account_id, last_refresh).to_string(),
    )
    .expect("write rotation auth.json");
    home
}

fn chatgpt_auth_payload(
    access_token: &str,
    refresh_token: &str,
    account_id: &str,
    last_refresh: &str,
) -> Value {
    json!({
        "auth_mode": "chatgpt",
        "tokens": {
            "id_token": fake_chatgpt_jwt(account_id),
            "access_token": access_token,
            "refresh_token": refresh_token,
            "account_id": account_id,
        },
        "last_refresh": last_refresh,
    })
}

fn fake_chatgpt_jwt(account_id: &str) -> String {
    let header = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&json!({ "alg": "none", "typ": "JWT" })).expect("serialize jwt header"),
    );
    let payload = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&json!({
            "https://api.openai.com/auth": {
                "chatgpt_account_id": account_id,
                "chatgpt_plan_type": "pro",
            }
        }))
        .expect("serialize jwt payload"),
    );
    let signature = URL_SAFE_NO_PAD.encode(b"signature");
    format!("{header}.{payload}.{signature}")
}

#[derive(Clone, Debug)]
struct TestRotationCoordinatorState {
    rotation_id: String,
    account_count: usize,
    active_index: usize,
    generation: u64,
    advance_requests: Vec<Value>,
    account_auth_requests: Vec<Value>,
    account_auth_payloads: Vec<Value>,
}

#[derive(Clone)]
struct TestRotationStateResponder {
    state: Arc<Mutex<TestRotationCoordinatorState>>,
}

impl Respond for TestRotationStateResponder {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let mut state = self.state.lock().expect("rotation state lock poisoned");
        if request.method == "POST" {
            let body: Value = request.body_json().expect("advance body should be json");
            state.advance_requests.push(body.clone());
            let account_index = body.get("account_index").and_then(Value::as_u64);
            let generation = body.get("generation").and_then(Value::as_u64);
            let reason = body.get("reason").and_then(Value::as_str);
            if account_index == Some(state.active_index as u64)
                && generation == Some(state.generation)
                && reason == Some("usage_limit_reached")
            {
                state.active_index = (state.active_index + 1) % state.account_count;
                state.generation += 1;
            }
        }
        ResponseTemplate::new(200).set_body_json(json!({
            "rotation_id": state.rotation_id,
            "account_count": state.account_count,
            "active_index": state.active_index,
            "generation": state.generation,
        }))
    }
}

#[derive(Clone)]
struct TestRotationAccountAuthResponder {
    state: Arc<Mutex<TestRotationCoordinatorState>>,
}

impl Respond for TestRotationAccountAuthResponder {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let mut state = self.state.lock().expect("rotation state lock poisoned");
        let body: Value = request
            .body_json()
            .expect("account auth body should be json");
        state.account_auth_requests.push(body.clone());
        let account_index = body
            .get("account_index")
            .and_then(Value::as_u64)
            .expect("account_index should be present") as usize;
        let generation = body
            .get("generation")
            .and_then(Value::as_u64)
            .expect("generation should be present");
        let auth = state
            .account_auth_payloads
            .get(account_index)
            .cloned()
            .expect("account auth payload should exist");

        ResponseTemplate::new(200).set_body_json(json!({
            "rotation_id": state.rotation_id,
            "account_count": state.account_count,
            "account_index": account_index,
            "generation": generation,
            "auth": auth,
        }))
    }
}

async fn install_rotation_coordinator_env(
    account_homes: &[std::path::PathBuf],
) -> (
    MockServer,
    Arc<Mutex<TestRotationCoordinatorState>>,
    EnvVarGuard,
) {
    let coordinator = MockServer::start().await;
    let state = Arc::new(Mutex::new(TestRotationCoordinatorState {
        rotation_id: "rotation-test".to_string(),
        account_count: account_homes.len(),
        active_index: 0,
        generation: 0,
        advance_requests: Vec::new(),
        account_auth_requests: Vec::new(),
        account_auth_payloads: account_homes
            .iter()
            .map(|home| {
                let auth_json = std::fs::read_to_string(home.join("auth.json"))
                    .expect("read rotation auth home");
                serde_json::from_str(&auth_json).expect("rotation auth should be json")
            })
            .collect(),
    }));
    Mock::given(method("GET"))
        .and(path("/state"))
        .respond_with(TestRotationStateResponder {
            state: Arc::clone(&state),
        })
        .mount(&coordinator)
        .await;
    Mock::given(method("POST"))
        .and(path("/advance"))
        .respond_with(TestRotationStateResponder {
            state: Arc::clone(&state),
        })
        .mount(&coordinator)
        .await;
    Mock::given(method("POST"))
        .and(path("/account-auth"))
        .respond_with(TestRotationAccountAuthResponder {
            state: Arc::clone(&state),
        })
        .mount(&coordinator)
        .await;
    let rotation_config = json!({
        "account_homes": account_homes
            .iter()
            .map(|path| path.to_string_lossy().to_string())
            .collect::<Vec<_>>(),
        "rotation_id": "rotation-test",
        "coordinator_url": coordinator.uri(),
        "coordinator_token": "token",
    })
    .to_string();
    let env_guard = EnvVarGuard::set(CODEX_AUTH_ROTATION_JSON_ENV, OsStr::new(&rotation_config));
    (coordinator, state, env_guard)
}

struct EnvVarGuard {
    key: &'static str,
    original: Option<OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &OsStr) -> Self {
        let original = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        unsafe {
            match &self.original {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}
