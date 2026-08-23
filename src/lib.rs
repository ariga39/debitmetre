use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::{Stream, StreamExt};
use sha2::{Digest, Sha256};

pub mod config;
pub mod summary;
mod usage;

use usage::{AccountingQuality, AuditRecord, Operation, Outcome, StreamUsageParser};

#[cfg(all(not(test), target_os = "linux"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// SHA-256 digest (hex) -> stable machine id mapping, loaded at construction.
pub type MachineKeys = BTreeMap<String, String>;

/// Fixed production upstream base for the closed route set. Never contacted by
/// tests; supplied only by [`Gateway::production`].
pub const PRODUCTION_UPSTREAM_BASE: &str = "https://chatgpt.com/backend-api/codex/";

/// Central transparent proxy gateway.
///
/// Constructed in-process with the server-side key mapping and the append-only
/// usage file; the returned router exposes the fixed route set. The injectable
/// upstream base is a test seam ([`Gateway::for_tests`]); production uses
/// [`Gateway::production`]. Each accepted request streams its upstream response
/// to the caller unchanged while a bounded side-band parser extracts usage and
/// produces one canonical JSONL record (DESIGN.md §5, §10 audit seam).
#[derive(Clone)]
pub struct Gateway {
    client: reqwest::Client,
    upstream_base: reqwest::Url,
    machine_keys: MachineKeys,
    audit: Arc<usage::AuditWriter>,
}

impl Gateway {
    /// Test seam: injectable upstream base URL pointing at a fake upstream in
    /// tests, and a usage file for the audit seam. This is not a production
    /// configuration option; production is built with [`Gateway::production`].
    /// Opening the usage file fails fast (startup fail-closed).
    pub fn for_tests(
        upstream_base: reqwest::Url,
        machine_keys: MachineKeys,
        usage_file: impl AsRef<Path>,
    ) -> Self {
        let audit = usage::AuditWriter::start(usage_file).expect("open test usage file");
        Self {
            client: build_client(),
            upstream_base,
            machine_keys,
            audit: Arc::new(audit),
        }
    }

    /// Production constructor: fixed upstream base with redirects disabled and
    /// the configured usage file opened fail-closed.
    pub fn production(
        machine_keys: MachineKeys,
        usage_file: impl AsRef<Path>,
    ) -> Result<Self, std::io::Error> {
        let audit = usage::AuditWriter::start(usage_file)?;
        Ok(Self {
            client: build_client(),
            upstream_base: reqwest::Url::parse(PRODUCTION_UPSTREAM_BASE)
                .expect("fixed production upstream base is valid"),
            machine_keys,
            audit: Arc::new(audit),
        })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/v1/responses", post(handle_responses))
            .route("/v1/responses/compact", post(handle_responses_compact))
            .route("/healthz", get(healthz))
            .with_state(self.clone())
    }
}

fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build upstream client")
}

fn digest_hex(key: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key);
    hex::encode(hasher.finalize())
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
}

/// Shared hop-by-hop predicate: a header is per-connection when it is nominated
/// by `Connection` or is a standard hop-by-hop name (including the de-facto
/// `Proxy-Connection`). Used by both request and response policies.
fn hop_by_hop_is_stripped(name: &HeaderName, connection_nominated: &HashSet<HeaderName>) -> bool {
    connection_nominated.contains(name)
        || matches!(
            name.as_str(),
            "connection"
                | "keep-alive"
                | "proxy-connection"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
        )
}

/// A request header must not be forwarded upstream when it is a hop-by-hop
/// header, a privacy/proxy-chain header, or a gateway meter header.
fn request_header_is_stripped(
    name: &HeaderName,
    connection_nominated: &HashSet<HeaderName>,
) -> bool {
    if hop_by_hop_is_stripped(name, connection_nominated) {
        return true;
    }
    match name.as_str() {
        "host" | "cookie" | "forwarded" | "via" | "x-real-ip" => true,
        other => other.starts_with("x-meter-") || other.starts_with("x-forwarded-"),
    }
}

/// A response header is stripped only when it is a hop-by-hop header; every
/// other upstream response header is preserved, even if the request policy
/// would strip it.
fn response_header_is_stripped(
    name: &HeaderName,
    connection_nominated: &HashSet<HeaderName>,
) -> bool {
    hop_by_hop_is_stripped(name, connection_nominated)
}

fn connection_nominated_headers(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter_map(|token| HeaderName::from_bytes(token.as_bytes()).ok())
        .collect()
}

async fn handle_responses(State(gateway): State<Gateway>, req: Request<Body>) -> Response {
    route_responses(gateway, req, "responses").await
}

async fn handle_responses_compact(State(gateway): State<Gateway>, req: Request<Body>) -> Response {
    route_responses(gateway, req, "responses/compact").await
}

/// Minimal health endpoint (see DESIGN.md §2): no authentication required;
/// returns 200 once a valid configuration is loaded and the listener is ready.
async fn healthz() -> StatusCode {
    tracing::debug!("health check");
    StatusCode::OK
}

async fn route_responses(gateway: Gateway, req: Request<Body>, endpoint_suffix: &str) -> Response {
    let mut keys = req.headers().get_all("x-meter-key").iter();
    let (Some(key), None) = (keys.next(), keys.next()) else {
        return unauthorized();
    };
    let Some(key) = key.to_str().ok() else {
        return unauthorized();
    };
    let digest = digest_hex(key.as_bytes());
    let Some(machine_id) = gateway.machine_keys.get(&digest) else {
        tracing::info!(
            route = endpoint_suffix,
            "request rejected: missing or invalid meter key"
        );
        return unauthorized();
    };
    tracing::info!(
        route = endpoint_suffix,
        machine_id = machine_id.as_str(),
        "request accepted"
    );

    let operation = if endpoint_suffix == "responses" {
        Operation::Response
    } else {
        Operation::Compaction
    };

    let base_path = gateway.upstream_base.path().trim_end_matches('/');
    let mut upstream_url = gateway.upstream_base.clone();
    upstream_url.set_path(&format!("{base_path}/{endpoint_suffix}"));
    if let Some(query) = req.uri().query() {
        upstream_url.set_query(Some(query));
    }

    let connection_nominated = connection_nominated_headers(req.headers());

    let mut builder = gateway.client.post(upstream_url);
    for (name, value) in req.headers().iter() {
        if request_header_is_stripped(name, &connection_nominated) {
            continue;
        }
        builder = builder.header(name, value);
    }

    match builder
        .body(reqwest::Body::wrap_stream(
            req.into_body().into_data_stream(),
        ))
        .send()
        .await
    {
        Ok(upstream) => {
            let status = upstream.status();
            let machine_id = machine_id.to_string();
            if status.is_success() {
                tracing::info!(
                    event = "upstream_response",
                    route = endpoint_suffix,
                    machine_id = machine_id.as_str(),
                    status = status.as_u16(),
                    "upstream response"
                );
            } else {
                tracing::warn!(
                    event = "upstream_http_error",
                    route = endpoint_suffix,
                    machine_id = machine_id.as_str(),
                    status = status.as_u16(),
                    "upstream error"
                );
            }
            let connection_nominated = connection_nominated_headers(upstream.headers());
            let mut filtered_headers: Vec<(HeaderName, header::HeaderValue)> = Vec::new();
            for (name, value) in upstream.headers().iter() {
                if response_header_is_stripped(name, &connection_nominated) {
                    continue;
                }
                filtered_headers.push((name.clone(), value.clone()));
            }

            let (tx, rx) =
                tokio::sync::mpsc::channel::<Result<Bytes, std::convert::Infallible>>(16);
            let client_gone = Arc::new(tokio::sync::Notify::new());
            let task_gone = Arc::clone(&client_gone);
            let audit = Arc::clone(&gateway.audit);
            let status_u16 = status.as_u16();
            let is_success = status.is_success();
            tokio::spawn(async move {
                // The parser is independent of the upstream Content-Type: the
                // Codex Responses client feeds the same byte stream to its own
                // SSE parser, and real responses can be SSE-framed even under a
                // JSON Content-Type (issue #20). A body that never frames as
                // SSE is extracted as complete JSON from the same bounded
                // buffer (see `usage::StreamUsageParser`).
                let mut parser = StreamUsageParser::sse();
                let mut outcome = Outcome::Completed;
                let mut stream = upstream.bytes_stream();
                // The response body is wrapped in `DropNotifyStream`, so this
                // notification fires once the caller abandons the response body
                // — even when the upstream is silent (DESIGN.md §5
                // client_cancelled). Racing the pump against it stops upstream
                // pumping immediately on caller disconnect instead of waiting
                // for the next upstream chunk.
                let pump = async {
                    loop {
                        match stream.next().await {
                            Some(Ok(bytes)) => {
                                parser.push(&bytes);
                                if tx.send(Ok(bytes)).await.is_err() {
                                    // Caller dropped the response body: stop
                                    // pumping upstream (DESIGN.md §5
                                    // client_cancelled).
                                    return false;
                                }
                            }
                            Some(Err(_)) => {
                                outcome = Outcome::UpstreamInterrupted;
                                return true;
                            }
                            None => return true,
                        }
                    }
                };
                let completed = tokio::select! {
                    _ = task_gone.notified() => false,
                    done = pump => done,
                };
                if !completed {
                    outcome = Outcome::ClientCancelled;
                }
                record_audit(
                    &audit,
                    &machine_id,
                    operation,
                    status_u16,
                    is_success,
                    outcome,
                    &parser,
                );
            });

            let mut response = Response::builder().status(status);
            for (name, value) in filtered_headers {
                response = response.header(name, value);
            }
            response
                .body(axum::body::Body::from_stream(DropNotifyStream::new(
                    tokio_stream::wrappers::ReceiverStream::new(rx),
                    client_gone,
                )))
                .unwrap_or_else(|_| unauthorized())
        }
        Err(_) => {
            let mut record = AuditRecord::new(machine_id.to_string(), operation);
            record.upstream_status = None;
            record.outcome = Outcome::TransportError;
            gateway.audit.try_record(record);
            tracing::error!(
                event = "upstream_transport_error",
                route = endpoint_suffix,
                machine_id = machine_id.as_str(),
                "upstream transport error"
            );
            (StatusCode::BAD_GATEWAY, "upstream unreachable").into_response()
        }
    }
}

/// Record the canonical audit line for an accepted request at its terminal
/// state (DESIGN.md §5 scenario mapping). Non-2xx responses record
/// `upstream_error` and never meter usage; for 2xx responses the side-band
/// parser supplies model/usage/quality or an explicit metering error.
fn record_audit(
    audit: &usage::AuditWriter,
    machine_id: &str,
    operation: Operation,
    upstream_status: u16,
    is_success: bool,
    outcome: Outcome,
    parser: &StreamUsageParser,
) {
    let mut record = AuditRecord::new(machine_id.to_string(), operation);
    record.upstream_status = Some(upstream_status);
    record.outcome = if !is_success {
        Outcome::UpstreamError
    } else if outcome == Outcome::Completed && parser.incomplete() {
        Outcome::Incomplete
    } else if outcome == Outcome::Completed && !parser.terminal_seen() {
        // A successful stream that ended without observing a terminal event
        // never reached a completed lifecycle (DESIGN.md §4.1, §5).
        Outcome::UpstreamInterrupted
    } else {
        outcome
    };
    if is_success {
        let result = parser.finish();
        record.model = result.model;
        record.usage = result.usage;
        record.accounting_quality = result.quality;
        record.metering_error = result.metering_error;
        // A successful upstream response whose usage could not be extracted is
        // metering failure (DESIGN.md §6): the caller-visible response stays
        // unchanged, and the operator gets one concise sanitized warning. Only
        // the safe enum field is logged — never machine id, path, headers,
        // credentials, body, or raw usage.
        if let Some(metering_error) = result.metering_error {
            tracing::warn!(
                metering_error = ?metering_error,
                "usage metering failed; the upstream response is unaffected"
            );
        }
    } else {
        record.usage = None;
        record.accounting_quality = AccountingQuality::Unavailable;
        record.metering_error = None;
    }
    audit.try_record(record);
}

/// Fires `notify` once when the wrapped stream is dropped — i.e. when the
/// caller abandons the response body, even while the upstream is silent. The
/// response pump races this notification against the upstream stream so that
/// caller disconnect stops upstream pumping without waiting for another body
/// chunk (DESIGN.md §5 client_cancelled).
///
/// Adapted from `src/gateway.rs` in [ariga39/orihsus] (MIT OR Apache-2.0,
/// Copyright (c) 2026 Kagami) at revision
/// `7285dd5c6a7ec5f1c0e521c6ee71f70e659d6220`; see THIRD-PARTY-NOTICES.md.
///
/// [ariga39/orihsus]: https://github.com/ariga39/orihsus
struct DropNotifyStream<S> {
    inner: S,
    notify: Arc<tokio::sync::Notify>,
}

impl<S> DropNotifyStream<S> {
    fn new(inner: S, notify: Arc<tokio::sync::Notify>) -> Self {
        DropNotifyStream { inner, notify }
    }
}

impl<S> Drop for DropNotifyStream<S> {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

impl<S: Stream + Unpin> Stream for DropNotifyStream<S> {
    type Item = S::Item;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<S::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
