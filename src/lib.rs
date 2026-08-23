use std::collections::{BTreeMap, HashSet};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use sha2::{Digest, Sha256};

pub mod config;

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
/// Constructed in-process with the server-side key mapping; the returned router
/// exposes the fixed route set. The injectable upstream base is a test seam
/// ([`Gateway::for_tests`]); production uses [`Gateway::production`].
#[derive(Clone)]
pub struct Gateway {
    client: reqwest::Client,
    upstream_base: reqwest::Url,
    machine_keys: MachineKeys,
}

impl Gateway {
    /// Test seam: injectable upstream base URL pointing at a fake upstream in
    /// tests. This is not a production configuration option; production is
    /// built with [`Gateway::production`].
    pub fn for_tests(upstream_base: reqwest::Url, machine_keys: MachineKeys) -> Self {
        Self {
            client: build_client(),
            upstream_base,
            machine_keys,
        }
    }

    /// Production constructor: fixed upstream base with redirects disabled.
    pub fn production(machine_keys: MachineKeys) -> Self {
        Self {
            client: build_client(),
            upstream_base: reqwest::Url::parse(PRODUCTION_UPSTREAM_BASE)
                .expect("fixed production upstream base is valid"),
            machine_keys,
        }
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
            tracing::info!(
                route = endpoint_suffix,
                machine_id = machine_id.as_str(),
                status = upstream.status().as_u16(),
                "upstream response"
            );
            let connection_nominated = connection_nominated_headers(upstream.headers());
            let mut response = Response::builder().status(upstream.status());
            for (name, value) in upstream.headers().iter() {
                if response_header_is_stripped(name, &connection_nominated) {
                    continue;
                }
                response = response.header(name, value);
            }
            response
                .body(axum::body::Body::from_stream(upstream.bytes_stream()))
                .unwrap_or_else(|_| unauthorized())
        }
        Err(_) => {
            tracing::error!(
                route = endpoint_suffix,
                machine_id = machine_id.as_str(),
                "upstream request failed; returning 502"
            );
            (StatusCode::BAD_GATEWAY, "upstream unreachable").into_response()
        }
    }
}
