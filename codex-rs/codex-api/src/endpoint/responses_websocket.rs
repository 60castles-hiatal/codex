use crate::auth::SharedAuthProvider;
use crate::common::ResponseEvent;
use crate::common::ResponseStream;
use crate::common::ResponsesWsRequest;
use crate::common::SafetyBufferingTreatment;
use crate::error::ApiError;
use crate::provider::Provider;
use crate::rate_limits::parse_rate_limit_event;
use crate::safety_buffering::treatment_from_headers;
use crate::sse::EarlyFinalAnswerState;
use crate::sse::EarlyToolCallState;
use crate::sse::ResponsesStreamEvent;
use crate::sse::process_responses_event;
use crate::telemetry::WebsocketTelemetry;
use codex_client::TransportError;
use codex_client::maybe_build_rustls_client_config_with_custom_ca;
use codex_protocol::models::ResponseItem;
use codex_utils_rustls_provider::ensure_rustls_crypto_provider;
use futures::SinkExt;
use futures::StreamExt;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use serde_json::map::Map as JsonMap;
use socket2::SockRef;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tracing::Instrument;
use tracing::Span;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::extensions::compression::deflate::DeflateConfig;
use tungstenite::protocol::WebSocketConfig;
use url::Url;

struct WsStream {
    tx_command: mpsc::Sender<WsCommand>,
    rx_message: mpsc::UnboundedReceiver<Result<WsPumpEvent, WsError>>,
    pump_task: tokio::task::JoinHandle<()>,
}

enum WsPumpEvent {
    Message(Message),
    EarlyFinalAnswer(String),
    EarlyToolCall(ResponseItem),
}

enum WsCommand {
    Send {
        message: Message,
        early_final_answer_tool_name: Option<String>,
        early_tool_call_hangup_enabled: bool,
        tx_result: oneshot::Sender<Result<(), WsError>>,
    },
}

fn reset_tcp_on_drop(stream: &WebSocketStream<MaybeTlsStream<TcpStream>>) {
    let tcp_stream = stream.get_ref().get_ref();
    let socket = SockRef::from(tcp_stream);
    if let Err(err) = socket.set_linger(Some(Duration::ZERO)) {
        debug!("failed to configure websocket TCP RST close: {err}");
    }
}

impl WsStream {
    fn new(inner: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        let (tx_command, mut rx_command) = mpsc::channel::<WsCommand>(32);
        let (tx_message, rx_message) = mpsc::unbounded_channel::<Result<WsPumpEvent, WsError>>();

        let pump_task = tokio::spawn(async move {
            let mut inner = inner;
            let mut early_final_answer_state: Option<EarlyFinalAnswerState> = None;
            let mut early_tool_call_state: Option<EarlyToolCallState> = None;
            loop {
                tokio::select! {
                    command = rx_command.recv() => {
                        let Some(command) = command else {
                            break;
                        };
                        match command {
                            WsCommand::Send {
                                message,
                                early_final_answer_tool_name,
                                early_tool_call_hangup_enabled,
                                tx_result,
                            } => {
                                early_final_answer_state = early_final_answer_tool_name
                                    .clone()
                                    .map(EarlyFinalAnswerState::new);
                                early_tool_call_state =
                                    if early_tool_call_hangup_enabled {
                                        early_final_answer_tool_name
                                            .map(EarlyToolCallState::new)
                                    } else {
                                        None
                                    };
                                let result = inner.send(message).await;
                                let should_break = result.is_err();
                                let _ = tx_result.send(result);
                                if should_break {
                                    break;
                                }
                            }
                        }
                    }
                    message = inner.next() => {
                        let Some(message) = message else {
                            break;
                        };
                        match message {
                            Ok(Message::Ping(payload)) => {
                                if let Err(err) = inner.send(Message::Pong(payload)).await {
                                    let _ = tx_message.send(Err(err));
                                    break;
                                }
                            }
                            Ok(Message::Pong(_)) => {}
                            Ok(message @ (Message::Text(_)
                            | Message::Binary(_)
                            | Message::Close(_)
                            | Message::Frame(_))) => {
                                if let Message::Text(text) = &message
                                    && let Some(item) = early_tool_call_state
                                        .as_mut()
                                        .and_then(|state| state.observe_text_frame(text))
                                {
                                    reset_tcp_on_drop(&inner);
                                    drop(inner);
                                    let _ = tx_message
                                        .send(Ok(WsPumpEvent::EarlyToolCall(item)));
                                    return;
                                }
                                if let Message::Text(text) = &message
                                    && let Some(answer) = early_final_answer_state
                                        .as_mut()
                                        .and_then(|state| state.observe_text_frame(text))
                                {
                                    reset_tcp_on_drop(&inner);
                                    drop(inner);
                                    let _ = tx_message
                                        .send(Ok(WsPumpEvent::EarlyFinalAnswer(answer)));
                                    return;
                                }
                                let is_close = matches!(message, Message::Close(_));
                                if tx_message.send(Ok(WsPumpEvent::Message(message))).is_err() {
                                    break;
                                }
                                if is_close {
                                    break;
                                }
                            }
                            Err(err) => {
                                let _ = tx_message.send(Err(err));
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self {
            tx_command,
            rx_message,
            pump_task,
        }
    }

    async fn request(
        &self,
        make_command: impl FnOnce(oneshot::Sender<Result<(), WsError>>) -> WsCommand,
    ) -> Result<(), WsError> {
        let (tx_result, rx_result) = oneshot::channel();
        if self.tx_command.send(make_command(tx_result)).await.is_err() {
            return Err(WsError::ConnectionClosed);
        }
        rx_result.await.unwrap_or(Err(WsError::ConnectionClosed))
    }

    async fn send(
        &self,
        message: Message,
        early_final_answer_tool_name: Option<String>,
        early_tool_call_hangup_enabled: bool,
    ) -> Result<(), WsError> {
        self.request(|tx_result| WsCommand::Send {
            message,
            early_final_answer_tool_name,
            early_tool_call_hangup_enabled,
            tx_result,
        })
        .await
    }

    async fn next(&mut self) -> Option<Result<WsPumpEvent, WsError>> {
        self.rx_message.recv().await
    }
}

impl Drop for WsStream {
    fn drop(&mut self) {
        self.pump_task.abort();
    }
}

const X_CODEX_TURN_STATE_HEADER: &str = "x-codex-turn-state";
const X_MODELS_ETAG_HEADER: &str = "x-models-etag";
const X_REASONING_INCLUDED_HEADER: &str = "x-reasoning-included";
const OPENAI_MODEL_HEADER: &str = "openai-model";
const WEBSOCKET_CONNECTION_LIMIT_REACHED_CODE: &str = "websocket_connection_limit_reached";
const WEBSOCKET_CONNECTION_LIMIT_REACHED_MESSAGE: &str = "Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue.";

pub struct ResponsesWebsocketConnection {
    stream: Arc<Mutex<Option<WsStream>>>,
    // TODO (pakrym): is this the right place for timeout?
    idle_timeout: Duration,
    server_reasoning_included: bool,
    models_etag: Option<String>,
    server_model: Option<String>,
    telemetry: Option<Arc<dyn WebsocketTelemetry>>,
}

impl std::fmt::Debug for ResponsesWebsocketConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponsesWebsocketConnection")
            .field("stream", &"<ws-stream>")
            .field("idle_timeout", &self.idle_timeout)
            .field("server_reasoning_included", &self.server_reasoning_included)
            .field("models_etag", &self.models_etag)
            .field("server_model", &self.server_model)
            .field("telemetry", &self.telemetry.as_ref().map(|_| "<telemetry>"))
            .finish()
    }
}

impl ResponsesWebsocketConnection {
    fn new(
        stream: WsStream,
        idle_timeout: Duration,
        server_reasoning_included: bool,
        models_etag: Option<String>,
        server_model: Option<String>,
        telemetry: Option<Arc<dyn WebsocketTelemetry>>,
    ) -> Self {
        Self {
            stream: Arc::new(Mutex::new(Some(stream))),
            idle_timeout,
            server_reasoning_included,
            models_etag,
            server_model,
            telemetry,
        }
    }

    pub async fn is_closed(&self) -> bool {
        self.stream.lock().await.is_none()
    }

    #[instrument(
        name = "responses_websocket.stream_request",
        level = "info",
        skip_all,
        fields(transport = "responses_websocket", api.path = "responses")
    )]
    pub async fn stream_request(
        &self,
        request: ResponsesWsRequest,
        connection_reused: bool,
        turn_state: Option<Arc<OnceLock<String>>>,
        early_final_answer_tool_name: Option<String>,
    ) -> Result<ResponseStream, ApiError> {
        let (tx_event, rx_event) =
            mpsc::channel::<std::result::Result<ResponseEvent, ApiError>>(1600);
        let stream = Arc::clone(&self.stream);
        let idle_timeout = self.idle_timeout;
        let server_reasoning_included = self.server_reasoning_included;
        let models_etag = self.models_etag.clone();
        let server_model = self.server_model.clone();
        let telemetry = self.telemetry.clone();
        let request_text = serialize_websocket_request(&request)?;

        let current_span = Span::current();
        tokio::spawn(
            #[expect(
                clippy::await_holding_invalid_type,
                reason = "the guard serializes exclusive use of the websocket stream for the lifetime of the response stream"
            )]
            async move {
                if let Some(model) = server_model {
                    let _ = tx_event.send(Ok(ResponseEvent::ServerModel(model))).await;
                }
                if let Some(etag) = models_etag {
                    let _ = tx_event.send(Ok(ResponseEvent::ModelsEtag(etag))).await;
                }
                if server_reasoning_included {
                    let _ = tx_event
                        .send(Ok(ResponseEvent::ServerReasoningIncluded(true)))
                        .await;
                }
                let mut guard = stream.lock().await;
                let result = {
                    let Some(ws_stream) = guard.as_mut() else {
                        let _ = tx_event
                            .send(Err(ApiError::Stream(
                                "websocket connection is closed".to_string(),
                            )))
                            .await;
                        return;
                    };

                    run_websocket_response_stream(
                        request_text,
                        WebsocketResponseStreamOptions {
                            ws_stream,
                            tx_event: tx_event.clone(),
                            idle_timeout,
                            telemetry,
                            connection_reused,
                            turn_state: turn_state.as_deref(),
                            early_final_answer_tool_name,
                        },
                    )
                    .await
                };

                match result {
                    Ok(WebsocketResponseStreamEnd::Completed) => {}
                    Ok(WebsocketResponseStreamEnd::ClosedEarly) => {
                        let closed_stream = guard.take();
                        drop(guard);
                        drop(closed_stream);
                    }
                    Err(err) => {
                        // A terminal stream error should reach the caller immediately. Waiting for a
                        // graceful close handshake here can stall indefinitely and mask the error.
                        let failed_stream = guard.take();
                        drop(guard);
                        drop(failed_stream);
                        let _ = tx_event.send(Err(err)).await;
                    }
                }
            }
            .instrument(current_span),
        );

        Ok(ResponseStream {
            rx_event,
            upstream_request_id: None,
        })
    }
}

/// Client for connecting to the Responses WebSocket endpoint for one provider.
pub struct ResponsesWebsocketClient {
    provider: Provider,
    auth: SharedAuthProvider,
}

/// Close frame information captured by a handshake probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponsesWebsocketClose {
    /// WebSocket close code returned by the server.
    pub code: String,
    /// Human-readable close reason returned by the server.
    pub reason: String,
}

/// Result of a handshake-only Responses WebSocket probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponsesWebsocketProbe {
    /// Redacted by callers before displaying or serializing support reports.
    pub url: String,
    /// HTTP status returned by the successful WebSocket upgrade.
    pub status: StatusCode,
    /// Whether the server reported reasoning support in the upgrade response.
    pub reasoning_included: bool,
    /// Whether the server returned a model catalog ETag in the upgrade response.
    pub models_etag_present: bool,
    /// Whether the server returned a server-selected model in the upgrade response.
    pub server_model_present: bool,
    /// Close frame received immediately after upgrade, when one arrives quickly.
    pub immediate_close: Option<ResponsesWebsocketClose>,
}

impl ResponsesWebsocketClient {
    /// Creates a Responses WebSocket client for an already-resolved provider and auth source.
    pub fn new(provider: Provider, auth: SharedAuthProvider) -> Self {
        Self { provider, auth }
    }

    #[instrument(
        name = "responses_websocket.connect",
        level = "info",
        skip_all,
        fields(transport = "responses_websocket", api.path = "responses")
    )]
    pub async fn connect(
        &self,
        extra_headers: HeaderMap,
        default_headers: HeaderMap,
        turn_state: Option<Arc<OnceLock<String>>>,
        telemetry: Option<Arc<dyn WebsocketTelemetry>>,
    ) -> Result<ResponsesWebsocketConnection, ApiError> {
        let ws_url = self
            .provider
            .websocket_url_for_path("responses")
            .map_err(|err| ApiError::Stream(format!("failed to build websocket URL: {err}")))?;

        let mut headers =
            merge_request_headers(&self.provider.headers, extra_headers, default_headers);
        self.auth.add_auth_headers(&mut headers);

        let (stream, _status, server_reasoning_included, models_etag, server_model) =
            connect_websocket(ws_url, headers, turn_state.clone()).await?;
        Ok(ResponsesWebsocketConnection::new(
            stream,
            self.provider.stream_idle_timeout,
            server_reasoning_included,
            models_etag,
            server_model,
            telemetry,
        ))
    }

    /// Opens a WebSocket connection long enough to validate the upgrade response.
    ///
    /// The probe uses the same URL construction, headers, authentication, TLS,
    /// and custom-CA path as a real Responses WebSocket connection, but it does
    /// not send a request frame. After the HTTP 101 upgrade succeeds, it waits
    /// briefly for an immediate server close frame so diagnostics can distinguish
    /// a usable connection from a policy rejection that closes right away.
    pub async fn probe_handshake(
        &self,
        extra_headers: HeaderMap,
        default_headers: HeaderMap,
        immediate_close_timeout: Duration,
    ) -> Result<ResponsesWebsocketProbe, ApiError> {
        let ws_url = self
            .provider
            .websocket_url_for_path("responses")
            .map_err(|err| ApiError::Stream(format!("failed to build websocket URL: {err}")))?;

        let mut headers =
            merge_request_headers(&self.provider.headers, extra_headers, default_headers);
        self.auth.add_auth_headers(&mut headers);

        let (mut stream, status, reasoning_included, models_etag, server_model) =
            connect_websocket(ws_url.clone(), headers, /*turn_state*/ None).await?;
        let immediate_close = tokio::time::timeout(immediate_close_timeout, stream.next())
            .await
            .ok()
            .flatten()
            .transpose()
            .map_err(|err| {
                ApiError::Stream(format!("failed to read websocket probe event: {err}"))
            })?
            .and_then(|event| match event {
                WsPumpEvent::Message(message) => immediate_close_from_message(message),
                WsPumpEvent::EarlyFinalAnswer(_) | WsPumpEvent::EarlyToolCall(_) => None,
            });

        Ok(ResponsesWebsocketProbe {
            url: ws_url.to_string(),
            status,
            reasoning_included,
            models_etag_present: models_etag.is_some(),
            server_model_present: server_model.is_some(),
            immediate_close,
        })
    }
}

fn immediate_close_from_message(message: Message) -> Option<ResponsesWebsocketClose> {
    let Message::Close(frame) = message else {
        return None;
    };
    frame.map(close_frame_to_probe)
}

fn close_frame_to_probe(frame: CloseFrame) -> ResponsesWebsocketClose {
    ResponsesWebsocketClose {
        code: frame.code.to_string(),
        reason: frame.reason.to_string(),
    }
}

fn merge_request_headers(
    provider_headers: &HeaderMap,
    extra_headers: HeaderMap,
    default_headers: HeaderMap,
) -> HeaderMap {
    let mut headers = provider_headers.clone();
    headers.extend(extra_headers);
    for (name, value) in &default_headers {
        if let http::header::Entry::Vacant(entry) = headers.entry(name) {
            entry.insert(value.clone());
        }
    }
    headers
}

async fn connect_websocket(
    url: Url,
    headers: HeaderMap,
    turn_state: Option<Arc<OnceLock<String>>>,
) -> Result<(WsStream, StatusCode, bool, Option<String>, Option<String>), ApiError> {
    ensure_rustls_crypto_provider();
    info!("connecting to websocket: {url}");

    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|err| ApiError::Stream(format!("failed to build websocket request: {err}")))?;
    request.headers_mut().extend(headers);

    // Secure websocket traffic needs the same custom-CA policy as reqwest-based HTTPS traffic.
    // If a Codex-specific CA bundle is configured, build an explicit rustls connector so this
    // websocket path does not fall back to tungstenite's default native-roots-only behavior.
    let connector = maybe_build_rustls_client_config_with_custom_ca()
        .map_err(|err| ApiError::Stream(format!("failed to configure websocket TLS: {err}")))?
        .map(tokio_tungstenite::Connector::Rustls);

    let response = connect_async_tls_with_config(
        request,
        Some(websocket_config()),
        false, // `false` means "do not disable Nagle", which is tungstenite's recommended default.
        connector,
    )
    .await;

    let (stream, response) = match response {
        Ok((stream, response)) => {
            info!(
                "successfully connected to websocket: {url}, headers: {:?}",
                response.headers()
            );
            (stream, response)
        }
        Err(err) => {
            error!("failed to connect to websocket: {err}, url: {url}");
            return Err(map_ws_error(err, &url));
        }
    };

    let reasoning_included = response.headers().contains_key(X_REASONING_INCLUDED_HEADER);
    let models_etag = response
        .headers()
        .get(X_MODELS_ETAG_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    let server_model = response
        .headers()
        .get(OPENAI_MODEL_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    if let Some(turn_state) = turn_state
        && let Some(header_value) = response
            .headers()
            .get(X_CODEX_TURN_STATE_HEADER)
            .and_then(|value| value.to_str().ok())
    {
        let _ = turn_state.set(header_value.to_string());
    }
    Ok((
        WsStream::new(stream),
        response.status(),
        reasoning_included,
        models_etag,
        server_model,
    ))
}

fn websocket_config() -> WebSocketConfig {
    let mut extensions = ExtensionsConfig::default();
    extensions.permessage_deflate = Some(DeflateConfig::default());

    let mut config = WebSocketConfig::default();
    config.extensions = extensions;
    config
}

fn map_ws_error(err: WsError, url: &Url) -> ApiError {
    match err {
        WsError::Http(response) => {
            let status = response.status();
            let headers = response.headers().clone();
            let body = response
                .body()
                .as_ref()
                .and_then(|bytes| String::from_utf8(bytes.clone()).ok());
            ApiError::Transport(TransportError::Http {
                status,
                url: Some(url.to_string()),
                headers: Some(headers),
                body,
            })
        }
        WsError::ConnectionClosed | WsError::AlreadyClosed => {
            ApiError::Stream("websocket closed".to_string())
        }
        WsError::Io(err) => ApiError::Transport(TransportError::Network(err.to_string())),
        other => ApiError::Transport(TransportError::Network(other.to_string())),
    }
}

#[derive(Debug, Deserialize)]
struct WrappedWebsocketError {
    code: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WrappedWebsocketErrorEvent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(alias = "status_code")]
    status: Option<u16>,
    #[serde(default)]
    error: Option<WrappedWebsocketError>,
    #[serde(default)]
    headers: Option<JsonMap<String, Value>>,
}

fn parse_wrapped_websocket_error_event(payload: &str) -> Option<WrappedWebsocketErrorEvent> {
    let event: WrappedWebsocketErrorEvent = serde_json::from_str(payload).ok()?;
    if event.kind != "error" {
        return None;
    }
    Some(event)
}

fn map_wrapped_websocket_error_event(
    event: WrappedWebsocketErrorEvent,
    original_payload: String,
) -> Option<ApiError> {
    let WrappedWebsocketErrorEvent {
        status,
        error,
        headers,
        ..
    } = event;

    if let Some(error) = error.as_ref()
        && let Some(code) = error.code.as_deref()
        && code == WEBSOCKET_CONNECTION_LIMIT_REACHED_CODE
    {
        return Some(ApiError::Retryable {
            message: error
                .message
                .clone()
                .unwrap_or_else(|| WEBSOCKET_CONNECTION_LIMIT_REACHED_MESSAGE.to_string()),
            delay: None,
        });
    }

    let status = StatusCode::from_u16(status?).ok()?;
    if status.is_success() {
        return None;
    }

    Some(ApiError::Transport(TransportError::Http {
        status,
        url: None,
        headers: headers.as_ref().map(json_headers_to_http_headers),
        body: Some(original_payload),
    }))
}

fn json_headers_to_http_headers(headers: &JsonMap<String, Value>) -> HeaderMap {
    let mut mapped = HeaderMap::new();
    for (name, value) in headers {
        let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Some(header_value) = json_header_value(value) else {
            continue;
        };
        mapped.insert(header_name, header_value);
    }
    mapped
}

fn json_header_value(value: &Value) -> Option<HeaderValue> {
    let value = match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        _ => return None,
    };
    HeaderValue::from_str(&value).ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WebsocketResponseStreamEnd {
    Completed,
    ClosedEarly,
}

struct WebsocketResponseStreamOptions<'a> {
    ws_stream: &'a mut WsStream,
    tx_event: mpsc::Sender<std::result::Result<ResponseEvent, ApiError>>,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn WebsocketTelemetry>>,
    connection_reused: bool,
    turn_state: Option<&'a OnceLock<String>>,
    early_final_answer_tool_name: Option<String>,
}

async fn run_websocket_response_stream(
    request_text: String,
    options: WebsocketResponseStreamOptions<'_>,
) -> Result<WebsocketResponseStreamEnd, ApiError> {
    let WebsocketResponseStreamOptions {
        ws_stream,
        tx_event,
        idle_timeout,
        telemetry,
        connection_reused,
        turn_state,
        early_final_answer_tool_name,
    } = options;
    let mut last_server_model: Option<String> = None;
    let mut safety_buffering_treatment = SafetyBufferingTreatment::default();
    send_websocket_request(
        ws_stream,
        request_text,
        idle_timeout,
        telemetry.as_ref(),
        connection_reused,
        early_final_answer_tool_name,
    )
    .await?;

    loop {
        let poll_start = Instant::now();
        let response = tokio::time::timeout(idle_timeout, ws_stream.next())
            .await
            .map_err(|_| ApiError::Stream("idle timeout waiting for websocket".into()));
        if let Some(t) = telemetry.as_ref() {
            record_ws_pump_telemetry(t, &response, poll_start.elapsed());
        }
        let message = match response {
            Ok(Some(Ok(msg))) => msg,
            Ok(Some(Err(err))) => {
                return Err(ApiError::Stream(err.to_string()));
            }
            Ok(None) => {
                return Err(ApiError::Stream(
                    "stream closed before response.completed".into(),
                ));
            }
            Err(err) => {
                return Err(err);
            }
        };

        match message {
            WsPumpEvent::EarlyFinalAnswer(answer) => {
                let _ = tx_event
                    .send(Ok(ResponseEvent::EarlyFinalAnswer(answer)))
                    .await;
                return Ok(WebsocketResponseStreamEnd::ClosedEarly);
            }
            WsPumpEvent::EarlyToolCall(item) => {
                let _ = tx_event.send(Ok(ResponseEvent::EarlyToolCall(item))).await;
                return Ok(WebsocketResponseStreamEnd::ClosedEarly);
            }
            WsPumpEvent::Message(message) => match message {
                Message::Text(text) => {
                    if let Some(wrapped_error) = parse_wrapped_websocket_error_event(&text)
                        && let Some(error) =
                            map_wrapped_websocket_error_event(wrapped_error, text.to_string())
                    {
                        return Err(error);
                    }

                    let event = match serde_json::from_str::<ResponsesStreamEvent>(&text) {
                        Ok(event) => event,
                        Err(err) => {
                            debug!("failed to parse websocket event: {err}, data: {text}");
                            continue;
                        }
                    };
                    if let Some(response_turn_state) = event.turn_state()
                        && let Some(turn_state) = turn_state
                    {
                        let _ = turn_state.set(response_turn_state);
                    }
                    if let Some(headers) = event.headers.as_ref().and_then(Value::as_object)
                        && let Some(treatment) =
                            treatment_from_headers(&json_headers_to_http_headers(headers))
                    {
                        safety_buffering_treatment = treatment;
                    }
                    let model_verifications = event.model_verifications();
                    let turn_moderation_metadata = event.turn_moderation_metadata();
                    let safety_buffering = event
                        .safety_buffering()
                        .map(|buffering| buffering.with_treatment(&safety_buffering_treatment));
                    if event.kind() == "codex.rate_limits" {
                        if let Some(snapshot) = parse_rate_limit_event(&text) {
                            let _ = tx_event.send(Ok(ResponseEvent::RateLimits(snapshot))).await;
                        }
                        continue;
                    }
                    if let Some(model) = event.response_model()
                        && last_server_model.as_deref() != Some(model.as_str())
                    {
                        let _ = tx_event
                            .send(Ok(ResponseEvent::ServerModel(model.clone())))
                            .await;
                        last_server_model = Some(model);
                    }
                    if let Some(verifications) = model_verifications
                        && tx_event
                            .send(Ok(ResponseEvent::ModelVerifications(verifications)))
                            .await
                            .is_err()
                    {
                        return Err(ApiError::Stream(
                            "response event consumer dropped".to_string(),
                        ));
                    }
                    if let Some(metadata) = turn_moderation_metadata
                        && tx_event
                            .send(Ok(ResponseEvent::TurnModerationMetadata(metadata)))
                            .await
                            .is_err()
                    {
                        return Err(ApiError::Stream(
                            "response event consumer dropped".to_string(),
                        ));
                    }
                    if let Some(buffering) = safety_buffering
                        && tx_event
                            .send(Ok(ResponseEvent::SafetyBuffering(buffering)))
                            .await
                            .is_err()
                    {
                        return Err(ApiError::Stream(
                            "response event consumer dropped".to_string(),
                        ));
                    }
                    match process_responses_event(event) {
                        Ok(Some(event)) => {
                            let is_completed = matches!(event, ResponseEvent::Completed { .. });
                            let is_early_final_answer =
                                matches!(event, ResponseEvent::EarlyFinalAnswer(_));
                            let _ = tx_event.send(Ok(event)).await;
                            if is_completed {
                                return Ok(WebsocketResponseStreamEnd::Completed);
                            }
                            if is_early_final_answer {
                                return Ok(WebsocketResponseStreamEnd::ClosedEarly);
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            return Err(error.into_api_error());
                        }
                    }
                }
                Message::Binary(_) => {
                    return Err(ApiError::Stream("unexpected binary websocket event".into()));
                }
                Message::Close(_) => {
                    return Err(ApiError::Stream(
                        "websocket closed by server before response.completed".into(),
                    ));
                }
                Message::Frame(_) => {}
                Message::Ping(_) | Message::Pong(_) => {}
            },
        }
    }
}

fn record_ws_pump_telemetry(
    telemetry: &Arc<dyn WebsocketTelemetry>,
    response: &Result<Option<Result<WsPumpEvent, WsError>>, ApiError>,
    duration: Duration,
) {
    let message = match response {
        Ok(Some(Ok(WsPumpEvent::Message(message)))) => Ok(Some(Ok(message.clone()))),
        Ok(Some(Ok(WsPumpEvent::EarlyFinalAnswer(answer)))) => {
            let event = serde_json::json!({
                "type": "codex.early_final_answer",
                "answer": answer,
            });
            Ok(Some(Ok(Message::Text(event.to_string().into()))))
        }
        Ok(Some(Ok(WsPumpEvent::EarlyToolCall(item)))) => {
            let event = serde_json::json!({
                "type": "codex.early_tool_call",
                "item": item,
            });
            Ok(Some(Ok(Message::Text(event.to_string().into()))))
        }
        Ok(Some(Err(_))) => Err(ApiError::Stream("websocket error".to_string())),
        Ok(None) => Ok(None),
        Err(err) => Err(ApiError::Stream(err.to_string())),
    };
    telemetry.on_ws_event(&message, duration);
}

async fn send_websocket_request(
    ws_stream: &WsStream,
    request_text: String,
    idle_timeout: Duration,
    telemetry: Option<&Arc<dyn WebsocketTelemetry>>,
    connection_reused: bool,
    early_final_answer_tool_name: Option<String>,
) -> Result<(), ApiError> {
    trace!("websocket request: {request_text}");
    let early_tool_call_hangup_enabled =
        early_final_answer_tool_name.is_some() && request_uses_serial_tool_calls(&request_text);

    let request_start = Instant::now();
    let result = tokio::time::timeout(
        idle_timeout,
        ws_stream.send(
            Message::Text(request_text.into()),
            early_final_answer_tool_name,
            early_tool_call_hangup_enabled,
        ),
    )
    .await
    .map_err(|_| ApiError::Stream("idle timeout sending websocket request".into()))
    .and_then(|result| {
        result.map_err(|err| ApiError::Stream(format!("failed to send websocket request: {err}")))
    });

    if let Some(t) = telemetry.as_ref() {
        t.on_ws_request(
            request_start.elapsed(),
            result.as_ref().err(),
            connection_reused,
        );
    }

    result?;

    Ok(())
}

fn request_uses_serial_tool_calls(request_text: &str) -> bool {
    serde_json::from_str::<Value>(request_text)
        .ok()
        .and_then(|request| request.get("parallel_tool_calls").and_then(Value::as_bool))
        == Some(false)
}

fn serialize_websocket_request(request: &ResponsesWsRequest) -> Result<String, ApiError> {
    serde_json::to_string(request)
        .map_err(|err| ApiError::Stream(format!("failed to encode websocket request: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ContextManagement;
    use crate::common::ResponseCreateWsRequest;
    use codex_protocol::models::ContentItem;
    use codex_protocol::models::ResponseItem;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::collections::HashMap;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async_with_config;

    #[test]
    fn direct_serialization_preserves_websocket_request_payload() {
        let request = ResponsesWsRequest::ResponseCreate(ResponseCreateWsRequest {
            model: "gpt-test".to_string(),
            instructions: "Use the available tools.".to_string(),
            previous_response_id: Some("resp-1".to_string()),
            input: vec![ResponseItem::Message {
                id: Some("msg-1".to_string()),
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "hello".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }],
            tools: vec![json!({
                "type": "function",
                "name": "lookup",
                "parameters": {"type": "object"}
            })],
            tool_choice: "auto".to_string(),
            parallel_tool_calls: true,
            reasoning: None,
            store: false,
            stream: true,
            include: vec!["reasoning.encrypted_content".to_string()],
            service_tier: Some("priority".to_string()),
            prompt_cache_key: Some("cache-key".to_string()),
            text: None,
            context_management: None,
            generate: Some(false),
            client_metadata: Some(HashMap::from([(
                "traceparent".to_string(),
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".to_string(),
            )])),
        });

        let previous_payload = serde_json::to_value(&request).expect("serialize previous payload");
        let request_text =
            serialize_websocket_request(&request).expect("serialize websocket request");
        let wire_payload =
            serde_json::from_str::<Value>(&request_text).expect("parse websocket request");

        assert_eq!(wire_payload, previous_payload);
    }

    #[test]
    fn direct_serialization_includes_context_management() {
        let request = ResponsesWsRequest::ResponseCreate(ResponseCreateWsRequest {
            model: "gpt-test".to_string(),
            instructions: String::new(),
            previous_response_id: None,
            input: Vec::new(),
            tools: Vec::new(),
            tool_choice: "auto".to_string(),
            parallel_tool_calls: false,
            reasoning: None,
            store: false,
            stream: true,
            include: Vec::new(),
            service_tier: None,
            prompt_cache_key: None,
            text: None,
            context_management: Some(vec![ContextManagement::Compaction {
                compact_threshold: 360_000,
            }]),
            generate: None,
            client_metadata: None,
        });

        let payload = serde_json::to_value(&request).expect("serialize websocket payload");
        assert_eq!(
            payload.get("context_management"),
            Some(&json!([{"type": "compaction", "compact_threshold": 360000}]))
        );
    }

    #[test]
    fn websocket_config_enables_permessage_deflate() {
        let config = websocket_config();
        assert!(config.extensions.permessage_deflate.is_some());
    }

    #[test]
    fn request_uses_serial_tool_calls_requires_false_parallel_flag() {
        assert!(request_uses_serial_tool_calls(
            &json!({
                "type": "response.create",
                "parallel_tool_calls": false,
            })
            .to_string()
        ));
        assert!(!request_uses_serial_tool_calls(
            &json!({
                "type": "response.create",
                "parallel_tool_calls": true,
            })
            .to_string()
        ));
        assert!(!request_uses_serial_tool_calls(
            &json!({
                "type": "response.create",
            })
            .to_string()
        ));
    }

    #[tokio::test]
    async fn websocket_early_final_answer_resets_connection_before_completed() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test websocket listener should bind");
        let addr = listener
            .local_addr()
            .expect("test websocket listener should have a local address");

        let server = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("test websocket connection should be accepted");
            let mut ws = accept_async_with_config(stream, Some(websocket_config()))
                .await
                .expect("test websocket handshake should complete");
            let _request = ws
                .next()
                .await
                .expect("request frame should arrive")
                .expect("request frame should be valid");

            ws.send(Message::Text(
                json!({
                    "type": "response.output_item.added",
                    "item": {
                        "type": "function_call",
                        "id": "fc-final",
                        "name": "final_answer",
                        "arguments": "",
                        "call_id": "call-final"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send final_answer added");
            ws.send(Message::Text(
                json!({
                    "type": "response.function_call_arguments.delta",
                    "item_id": "fc-final",
                    "call_id": "call-final",
                    "delta": r#"{"answer":"done\n\nf6d79a07: This is the final answer, there are no more answers after this. All content should be included"#
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send final_answer arguments");

            let close = tokio::time::timeout(Duration::from_secs(1), ws.next())
                .await
                .expect("client should close promptly after final_answer arguments");
            assert!(
                matches!(close, None | Some(Err(_))),
                "client should drop the TCP connection without a websocket close frame: {close:?}"
            );
        });

        let (mut ws_stream, _, _, _, _) = connect_websocket(
            Url::parse(&format!("ws://{addr}/responses")).expect("valid websocket url"),
            HeaderMap::new(),
            /*turn_state*/ None,
        )
        .await
        .expect("connect websocket");
        let (tx_event, mut rx_event) = mpsc::channel(16);

        let result = run_websocket_response_stream(
            json!({"type": "response.create"}).to_string(),
            WebsocketResponseStreamOptions {
                ws_stream: &mut ws_stream,
                tx_event,
                idle_timeout: Duration::from_secs(5),
                telemetry: None,
                connection_reused: false,
                turn_state: None,
                early_final_answer_tool_name: Some("final_answer".to_string()),
            },
        )
        .await
        .expect("websocket response stream should finish early");

        assert_eq!(result, WebsocketResponseStreamEnd::ClosedEarly);
        assert!(matches!(
            rx_event.recv().await.expect("output item added event"),
            Ok(ResponseEvent::OutputItemAdded(_))
        ));
        assert!(matches!(
            rx_event.recv().await.expect("early final answer event"),
            Ok(ResponseEvent::EarlyFinalAnswer(answer)) if answer == "done"
        ));
        assert!(rx_event.try_recv().is_err());
        server.await.expect("server should finish");
    }

    #[tokio::test]
    async fn websocket_early_tool_call_resets_connection_when_arguments_parse() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test websocket listener should bind");
        let addr = listener
            .local_addr()
            .expect("test websocket listener should have a local address");

        let server = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("test websocket connection should be accepted");
            let mut ws = accept_async_with_config(stream, Some(websocket_config()))
                .await
                .expect("test websocket handshake should complete");
            let _request = ws
                .next()
                .await
                .expect("request frame should arrive")
                .expect("request frame should be valid");

            ws.send(Message::Text(
                json!({
                    "type": "response.output_item.added",
                    "item": {
                        "type": "function_call",
                        "id": "fc-real",
                        "name": "update_goal",
                        "arguments": "",
                        "call_id": "call-real"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send tool call added");
            ws.send(Message::Text(
                json!({
                    "type": "response.function_call_arguments.delta",
                    "item_id": "fc-real",
                    "call_id": "call-real",
                    "delta": r#"{"status":"in_progress""#,
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send partial tool call arguments");
            ws.send(Message::Text(
                json!({
                    "type": "response.function_call_arguments.delta",
                    "item_id": "fc-real",
                    "call_id": "call-real",
                    "delta": "}",
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send complete tool call arguments");

            let close = tokio::time::timeout(Duration::from_secs(1), ws.next())
                .await
                .expect("client should close promptly after complete tool call arguments");
            assert!(
                matches!(close, None | Some(Err(_))),
                "client should drop the TCP connection without a websocket close frame: {close:?}"
            );
        });

        let (mut ws_stream, _, _, _, _) = connect_websocket(
            Url::parse(&format!("ws://{addr}/responses")).expect("valid websocket url"),
            HeaderMap::new(),
            /*turn_state*/ None,
        )
        .await
        .expect("connect websocket");
        let (tx_event, mut rx_event) = mpsc::channel(16);

        let result = run_websocket_response_stream(
            json!({
                "type": "response.create",
                "parallel_tool_calls": false,
            })
            .to_string(),
            WebsocketResponseStreamOptions {
                ws_stream: &mut ws_stream,
                tx_event,
                idle_timeout: Duration::from_secs(5),
                telemetry: None,
                connection_reused: false,
                turn_state: None,
                early_final_answer_tool_name: Some("final_answer".to_string()),
            },
        )
        .await
        .expect("websocket response stream should finish early");

        assert_eq!(result, WebsocketResponseStreamEnd::ClosedEarly);
        assert!(matches!(
            rx_event.recv().await.expect("output item added event"),
            Ok(ResponseEvent::OutputItemAdded(ResponseItem::FunctionCall { name, .. }))
                if name == "update_goal"
        ));
        assert!(matches!(
            rx_event.recv().await.expect("partial tool arguments event"),
            Ok(ResponseEvent::ToolCallInputDelta {
                item_id,
                call_id: Some(call_id),
                delta,
            }) if item_id == "fc-real"
                && call_id == "call-real"
                && delta == r#"{"status":"in_progress""#
        ));
        let early_tool_call = rx_event
            .recv()
            .await
            .expect("early tool call event")
            .expect("early tool call should be ok");
        let ResponseEvent::EarlyToolCall(item) = early_tool_call else {
            panic!("expected early tool call event: {early_tool_call:?}");
        };
        assert_eq!(
            item,
            ResponseItem::FunctionCall {
                id: Some("fc-real".to_string()),
                name: "update_goal".to_string(),
                namespace: None,
                arguments: r#"{"status":"in_progress"}"#.to_string(),
                call_id: "call-real".to_string(),
                internal_chat_message_metadata_passthrough: None,
            }
        );
        assert!(rx_event.try_recv().is_err());
        server.await.expect("server should finish");
    }

    #[test]
    fn parse_wrapped_websocket_error_event_maps_to_transport_http() {
        let payload = json!({
            "type": "error",
            "status": 429,
            "error": {
                "type": "usage_limit_reached",
                "message": "The usage limit has been reached",
                "plan_type": "pro",
                "resets_at": 1738888888
            },
            "headers": {
                "x-codex-primary-used-percent": "100.0",
                "x-codex-primary-window-minutes": 15
            }
        })
        .to_string();

        let wrapped_error = parse_wrapped_websocket_error_event(&payload)
            .expect("expected websocket error payload to be parsed");
        let api_error = map_wrapped_websocket_error_event(wrapped_error, payload)
            .expect("expected websocket error payload to map to ApiError");

        let ApiError::Transport(TransportError::Http {
            status,
            headers,
            body,
            ..
        }) = api_error
        else {
            panic!("expected ApiError::Transport(Http)");
        };

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        let headers = headers.expect("expected headers");
        assert_eq!(
            headers
                .get("x-codex-primary-used-percent")
                .and_then(|value| value.to_str().ok()),
            Some("100.0")
        );
        assert_eq!(
            headers
                .get("x-codex-primary-window-minutes")
                .and_then(|value| value.to_str().ok()),
            Some("15")
        );
        let body = body.expect("expected body");
        assert!(body.contains("usage_limit_reached"));
        assert!(body.contains("The usage limit has been reached"));
    }

    #[test]
    fn parse_wrapped_websocket_error_event_ignores_non_error_payloads() {
        let payload = json!({
            "type": "response.created",
            "response": {
                "id": "resp-1"
            }
        })
        .to_string();

        let wrapped_error = parse_wrapped_websocket_error_event(&payload);
        assert!(wrapped_error.is_none());
    }

    #[test]
    fn parse_wrapped_websocket_error_event_with_status_maps_invalid_request() {
        let payload = json!({
            "type": "error",
            "status": 400,
            "error": {
                "type": "invalid_request_error",
                "message": "Model does not support image inputs"
            }
        })
        .to_string();

        let wrapped_error = parse_wrapped_websocket_error_event(&payload)
            .expect("expected websocket error payload to be parsed");
        let api_error = map_wrapped_websocket_error_event(wrapped_error, payload)
            .expect("expected websocket error payload to map to ApiError");
        let ApiError::Transport(TransportError::Http { status, body, .. }) = api_error else {
            panic!("expected ApiError::Transport(Http)");
        };
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body = body.expect("expected body");
        assert!(body.contains("invalid_request_error"));
        assert!(body.contains("Model does not support image inputs"));
    }

    #[test]
    fn parse_wrapped_websocket_error_event_with_connection_limit_maps_retryable() {
        let payload = json!({
            "type": "error",
            "status": 400,
            "error": {
                "type": "invalid_request_error",
                "code": "websocket_connection_limit_reached",
                "message": "Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue."
            }
        })
        .to_string();

        let wrapped_error = parse_wrapped_websocket_error_event(&payload)
            .expect("expected websocket error payload to be parsed");
        let api_error = map_wrapped_websocket_error_event(wrapped_error, payload)
            .expect("expected websocket error payload to map to ApiError");
        let ApiError::Retryable { message, delay } = api_error else {
            panic!("expected ApiError::Retryable");
        };
        assert_eq!(message, WEBSOCKET_CONNECTION_LIMIT_REACHED_MESSAGE);
        assert_eq!(delay, None);
    }

    #[test]
    fn parse_wrapped_websocket_error_event_without_status_is_not_mapped() {
        let payload = json!({
            "type": "error",
            "error": {
                "type": "usage_limit_reached",
                "message": "The usage limit has been reached"
            },
            "headers": {
                "x-codex-primary-used-percent": "100.0",
                "x-codex-primary-window-minutes": 15
            }
        })
        .to_string();

        let wrapped_error = parse_wrapped_websocket_error_event(&payload)
            .expect("expected websocket error payload to be parsed");
        let api_error = map_wrapped_websocket_error_event(wrapped_error, payload);
        assert!(api_error.is_none());
    }

    #[test]
    fn merge_request_headers_matches_http_precedence() {
        let mut provider_headers = HeaderMap::new();
        provider_headers.insert(
            "originator",
            HeaderValue::from_static("provider-originator"),
        );
        provider_headers.insert("x-priority", HeaderValue::from_static("provider"));

        let mut extra_headers = HeaderMap::new();
        extra_headers.insert("x-priority", HeaderValue::from_static("extra"));

        let mut default_headers = HeaderMap::new();
        default_headers.insert("originator", HeaderValue::from_static("default-originator"));
        default_headers.insert("x-priority", HeaderValue::from_static("default"));
        default_headers.insert("x-default-only", HeaderValue::from_static("default-only"));

        let merged = merge_request_headers(&provider_headers, extra_headers, default_headers);

        assert_eq!(
            merged.get("originator"),
            Some(&HeaderValue::from_static("provider-originator"))
        );
        assert_eq!(
            merged.get("x-priority"),
            Some(&HeaderValue::from_static("extra"))
        );
        assert_eq!(
            merged.get("x-default-only"),
            Some(&HeaderValue::from_static("default-only"))
        );
    }
}
