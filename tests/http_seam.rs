use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, post};
use axum::Router;
use futures_util::{Stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use debitmetre::Gateway;

/// SHA-256 digest of the synthetic meter key `test-meter-key-machine-a`,
/// independently precomputed with `sha256sum` and OpenSSL `dgst -sha256`
/// (both tools agree); the gateway hashes the presented key at runtime.
const TEST_METER_KEY_DIGEST: &str =
    "82805ec33616c4aa802f141d3703fb17213fd8ced358f3a62348d8cf6e1ce051";

/// A synthetic Responses SSE terminal event with independently known token
/// counts (DESIGN.md §4): input_total=12, cache_read=4, cache_write=2,
/// output_total=5, reasoning=2, total=17, so uncached = 12-4-2 = 6 and the
/// accounting_quality is `complete`.
const STREAMING_RESPONSE_FIXTURE: &str = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-synthetic-01\",\"model\":\"synthetic-model-001\",\"usage\":{\"input_tokens\":12,\"input_tokens_details\":{\"cached_tokens\":4,\"cache_write_tokens\":2},\"output_tokens\":5,\"output_tokens_details\":{\"reasoning_tokens\":2},\"total_tokens\":17}}}\n\n";

/// A synthetic nonterminal SSE event (DESIGN.md §4.1): `response.output_text.delta`
/// is forwarded unchanged and is not a terminal event, so a stream reaching clean
/// EOF with only this event never observed `response.completed` or
/// `response.incomplete`.
const NONTERMINAL_SSE_FIXTURE: &str =
    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic-delta-01\"}\n\n";

/// Parse the current JSONL usage-file content into records (audit seam).
fn read_jsonl(path: &std::path::Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

/// Poll the JSONL usage file until the background writer has flushed a record.
async fn wait_for_jsonl_record(path: &std::path::Path) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(record) = read_jsonl(path).into_iter().next() {
            return record;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for a JSONL record in {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Clone, Default)]
struct FakeUpstream {
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    received_body: Arc<Mutex<Option<Vec<u8>>>>,
    queue: Arc<Mutex<Vec<CannedResponse>>>,
    redirect_hits: Arc<Mutex<usize>>,
    dropped: Arc<Mutex<bool>>,
}

#[derive(Default)]
struct CannedResponse {
    status: StatusCode,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Default)]
struct CapturedRequest {
    method: String,
    path: String,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    query: Option<String>,
}

async fn fake_upstream_handler(
    State(upstream): State<FakeUpstream>,
    req: Request<Body>,
) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(|q| q.to_string());
    let headers = req
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    let bytes = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .expect("fake upstream must read request body");
    upstream.captured.lock().unwrap().push(CapturedRequest {
        method,
        path,
        body: bytes.to_vec(),
        headers,
        query,
    });
    (StatusCode::CREATED, "opaque-upstream-body-01").into_response()
}

async fn spawn(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral listener");
    let addr = listener.local_addr().expect("resolved local address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server runs");
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn responses_route_authenticates_valid_key_and_forwards_to_fake_upstream() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(fake_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response
            .bytes()
            .await
            .expect("gateway response body")
            .as_ref(),
        &b"opaque-upstream-body-01"[..]
    );

    let captured = upstream
        .captured
        .lock()
        .unwrap()
        .pop()
        .expect("fake upstream received one request");
    assert_eq!(captured.method, "POST");
    assert_eq!(captured.path, "/responses");
    assert_eq!(captured.body, b"opaque-request-body-42");
}

#[tokio::test]
async fn accepted_streaming_response_is_forwarded_unchanged_and_records_known_usage() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(canned_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::OK,
        headers: vec![
            ("content-type".to_string(), "text/event-stream".to_string()),
            (
                "x-codex-semantic".to_string(),
                "synthetic-semantic-01".to_string(),
            ),
        ],
        body: STREAMING_RESPONSE_FIXTURE.as_bytes().to_vec(),
    });

    let usage_dir = tempfile::TempDir::new().unwrap();
    let usage_file = usage_dir.path().join("usage.jsonl");
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        &usage_file,
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .header("authorization", "Bearer synthetic-oauth-token-01")
        .header("chatgpt-account-id", "synthetic-account-01")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-codex-semantic")
            .and_then(|v| v.to_str().ok()),
        Some("synthetic-semantic-01"),
        "caller-visible upstream headers preserved"
    );
    assert_eq!(
        response
            .bytes()
            .await
            .expect("gateway response body")
            .as_ref(),
        STREAMING_RESPONSE_FIXTURE.as_bytes(),
        "caller-visible response bytes are byte-for-byte the upstream SSE"
    );

    let record = wait_for_jsonl_record(&usage_file).await;
    assert_eq!(
        read_jsonl(&usage_file).len(),
        1,
        "exactly one audit record per accepted request"
    );
    assert_eq!(record["schema_version"], 1);
    assert_eq!(record["kind"], "request");
    assert!(
        record["event_id"].as_str().is_some_and(|id| !id.is_empty()),
        "a gateway-generated event_id exists"
    );
    assert!(
        record["timestamp"]
            .as_str()
            .is_some_and(|ts| ts.ends_with('Z')),
        "timestamp is a UTC RFC3339 string"
    );
    assert_eq!(record["machine_id"], "machine-a");
    assert_eq!(record["operation"], "response");
    assert_eq!(record["upstream_status"], 200);
    assert_eq!(record["outcome"], "completed");
    assert_eq!(record["model"], "synthetic-model-001");
    assert_eq!(record["accounting_quality"], "complete");
    assert!(record["metering_error"].is_null());

    let usage = &record["usage"];
    assert_eq!(usage["input_total"], 12);
    assert_eq!(usage["uncached"], 6);
    assert_eq!(usage["cache_read"], 4);
    assert_eq!(usage["cache_write"], 2);
    assert_eq!(usage["output_total"], 5);
    assert_eq!(usage["reasoning"], 2);
    assert_eq!(usage["total"], 17);

    let serialized = serde_json::to_string(&record).expect("record serializes");
    for forbidden in [
        "test-meter-key-machine-a",
        "opaque-request-body-42",
        "resp-synthetic-01",
        "synthetic-oauth-token-01",
        "synthetic-account-01",
        "response.completed",
    ] {
        assert!(
            !serialized.contains(forbidden),
            "usage record must never contain {forbidden:?} (privacy allowlist)"
        );
    }
}

/// A synthetic Responses SSE terminal event under a body labeled
/// `application/json` (issue #20): the real Codex Responses client feeds the
/// response byte stream to its SSE parser regardless of the upstream
/// Content-Type, so an SSE-framed `response.completed` can arrive with a JSON
/// Content-Type. Token counts are the known ones from
/// `STREAMING_RESPONSE_FIXTURE`: input_total=12, cache_read=4, cache_write=2,
/// output_total=5, reasoning=2, total=17, so uncached = 12-4-2 = 6 and the
/// accounting_quality is `complete`.
const JSON_LABELED_SSE_RESPONSE_FIXTURE: &str = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-synthetic-json-label\",\"model\":\"synthetic-model-001\",\"usage\":{\"input_tokens\":12,\"input_tokens_details\":{\"cached_tokens\":4,\"cache_write_tokens\":2},\"output_tokens\":5,\"output_tokens_details\":{\"reasoning_tokens\":2},\"total_tokens\":17}}}\n\n";

#[tokio::test]
async fn json_labeled_sse_response_is_forwarded_unchanged_and_records_known_usage() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(canned_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::OK,
        headers: vec![
            ("content-type".to_string(), "application/json".to_string()),
            (
                "x-codex-semantic".to_string(),
                "json-label-semantic-01".to_string(),
            ),
        ],
        body: JSON_LABELED_SSE_RESPONSE_FIXTURE.as_bytes().to_vec(),
    });

    let usage_dir = tempfile::TempDir::new().unwrap();
    let usage_file = usage_dir.path().join("usage.jsonl");
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        &usage_file,
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-codex-semantic")
            .and_then(|v| v.to_str().ok()),
        Some("json-label-semantic-01"),
        "caller-visible upstream headers preserved"
    );
    assert_eq!(
        response
            .bytes()
            .await
            .expect("gateway response body")
            .as_ref(),
        JSON_LABELED_SSE_RESPONSE_FIXTURE.as_bytes(),
        "caller-visible response bytes are byte-for-byte the upstream SSE body"
    );

    let record = wait_for_jsonl_record(&usage_file).await;
    assert_eq!(
        read_jsonl(&usage_file).len(),
        1,
        "exactly one audit record per accepted request"
    );
    assert_eq!(record["kind"], "request");
    assert_eq!(record["operation"], "response");
    assert_eq!(record["machine_id"], "machine-a");
    assert_eq!(record["upstream_status"], 200);
    assert_eq!(record["outcome"], "completed");
    assert_eq!(record["model"], "synthetic-model-001");
    assert_eq!(record["accounting_quality"], "complete");
    assert!(record["metering_error"].is_null());

    let usage = &record["usage"];
    assert_eq!(usage["input_total"], 12);
    assert_eq!(usage["uncached"], 6);
    assert_eq!(usage["cache_read"], 4);
    assert_eq!(usage["cache_write"], 2);
    assert_eq!(usage["output_total"], 5);
    assert_eq!(usage["reasoning"], 2);
    assert_eq!(usage["total"], 17);
}

/// A synthetic non-streaming Responses JSON body that contains a blank line
/// (`\n\n`) as insignificant JSON whitespace between fields (issue #20 PR
/// review): a valid JSON document must not be misread as an SSE stream just
/// because it happens to contain a blank line. Token counts are the known ones
/// from `STREAMING_RESPONSE_FIXTURE`: input_total=12, cache_read=4,
/// cache_write=2, output_total=5, reasoning=2, total=17, so uncached = 6 and
/// the accounting_quality is `complete`.
const BLANKLINE_JSON_RESPONSE_FIXTURE: &str = "{\n  \"id\": \"resp-synthetic-blankline\",\n  \"object\": \"response\",\n  \"model\": \"synthetic-model-001\",\n\n  \"usage\": {\n    \"input_tokens\": 12,\n    \"input_tokens_details\": {\n      \"cached_tokens\": 4,\n      \"cache_write_tokens\": 2\n    },\n    \"output_tokens\": 5,\n    \"output_tokens_details\": {\n      \"reasoning_tokens\": 2\n    },\n    \"total_tokens\": 17\n  }\n}";

#[tokio::test]
async fn genuine_json_with_blank_line_whitespace_is_forwarded_and_records_known_usage() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(canned_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::OK,
        headers: vec![
            ("content-type".to_string(), "application/json".to_string()),
            (
                "x-codex-semantic".to_string(),
                "blankline-json-semantic-01".to_string(),
            ),
        ],
        body: BLANKLINE_JSON_RESPONSE_FIXTURE.as_bytes().to_vec(),
    });

    let usage_dir = tempfile::TempDir::new().unwrap();
    let usage_file = usage_dir.path().join("usage.jsonl");
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        &usage_file,
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-codex-semantic")
            .and_then(|v| v.to_str().ok()),
        Some("blankline-json-semantic-01"),
        "caller-visible upstream headers preserved"
    );
    assert_eq!(
        response
            .bytes()
            .await
            .expect("gateway response body")
            .as_ref(),
        BLANKLINE_JSON_RESPONSE_FIXTURE.as_bytes(),
        "caller-visible response bytes are byte-for-byte the upstream JSON body"
    );

    let record = wait_for_jsonl_record(&usage_file).await;
    assert_eq!(
        read_jsonl(&usage_file).len(),
        1,
        "exactly one audit record per accepted request"
    );
    assert_eq!(record["kind"], "request");
    assert_eq!(record["operation"], "response");
    assert_eq!(record["machine_id"], "machine-a");
    assert_eq!(record["upstream_status"], 200);
    assert_eq!(record["outcome"], "completed");
    assert_eq!(record["model"], "synthetic-model-001");
    assert_eq!(record["accounting_quality"], "complete");
    assert!(record["metering_error"].is_null());

    let usage = &record["usage"];
    assert_eq!(usage["input_total"], 12);
    assert_eq!(usage["uncached"], 6);
    assert_eq!(usage["cache_read"], 4);
    assert_eq!(usage["cache_write"], 2);
    assert_eq!(usage["output_total"], 5);
    assert_eq!(usage["reasoning"], 2);
    assert_eq!(usage["total"], 17);
}

/// A synthetic non-streaming compact response (DESIGN.md §11: the real compact
/// response shape is pending PoC, so this uses a conservative JSON body) with
/// only `input_total` and `output_total` present: the absent counters must stay
/// absent and the accounting quality is `partial`.
const COMPACT_RESPONSE_FIXTURE: &str =
    "{\"id\":\"comp-synthetic-01\",\"object\":\"response\",\"model\":\"synthetic-model-001\",\"output\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}";

/// A synthetic non-streaming Responses JSON body whose top-level `status`
/// (ResponseStatus) is `incomplete`, with `incomplete_details` giving the
/// reason (DESIGN.md §4.1 treats `response.incomplete` as a terminal event; a
/// non-streaming body signals the same lifecycle via the response object's
/// `status` field). `incomplete_details` is context, not lifecycle authority.
/// Token counts are the known ones from `STREAMING_RESPONSE_FIXTURE`:
/// input_total=12, cache_read=4, cache_write=2, output_total=5, reasoning=2,
/// total=17, so uncached = 12-4-2 = 6 and the accounting_quality is `complete`.
const INCOMPLETE_JSON_RESPONSE_FIXTURE: &str = "{\"id\":\"resp-synthetic-incomplete-01\",\"object\":\"response\",\"model\":\"synthetic-model-001\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":12,\"input_tokens_details\":{\"cached_tokens\":4,\"cache_write_tokens\":2},\"output_tokens\":5,\"output_tokens_details\":{\"reasoning_tokens\":2},\"total_tokens\":17}}";

#[tokio::test]
async fn accepted_compact_request_records_its_own_line_with_absent_counters_absent() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses/compact", post(canned_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::OK,
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        body: COMPACT_RESPONSE_FIXTURE.as_bytes().to_vec(),
    });

    let usage_dir = tempfile::TempDir::new().unwrap();
    let usage_file = usage_dir.path().join("usage.jsonl");
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        &usage_file,
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/v1/responses/compact"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-compact-body-07")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .bytes()
            .await
            .expect("gateway response body")
            .as_ref(),
        COMPACT_RESPONSE_FIXTURE.as_bytes(),
        "compact response bytes are forwarded unchanged"
    );

    let record = wait_for_jsonl_record(&usage_file).await;
    assert_eq!(record["kind"], "request");
    assert_eq!(record["operation"], "compaction");
    assert_eq!(record["machine_id"], "machine-a");
    assert_eq!(record["upstream_status"], 200);
    assert_eq!(record["outcome"], "completed");
    assert_eq!(record["model"], "synthetic-model-001");
    assert_eq!(record["accounting_quality"], "partial");
    assert!(record["metering_error"].is_null());

    let usage = &record["usage"];
    assert_eq!(usage["input_total"], 3);
    assert_eq!(usage["output_total"], 1);
    assert!(
        usage["uncached"].is_null()
            && usage["cache_read"].is_null()
            && usage["cache_write"].is_null()
            && usage["reasoning"].is_null()
            && usage["total"].is_null(),
        "absent upstream counters remain null, never guessed or zeroed"
    );
}

#[tokio::test]
async fn successful_json_responses_body_with_incomplete_status_is_forwarded_and_records_incomplete()
{
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(canned_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::OK,
        headers: vec![
            ("content-type".to_string(), "application/json".to_string()),
            (
                "x-codex-semantic".to_string(),
                "incomplete-semantic-01".to_string(),
            ),
        ],
        body: INCOMPLETE_JSON_RESPONSE_FIXTURE.as_bytes().to_vec(),
    });

    let usage_dir = tempfile::TempDir::new().unwrap();
    let usage_file = usage_dir.path().join("usage.jsonl");
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        &usage_file,
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-codex-semantic")
            .and_then(|v| v.to_str().ok()),
        Some("incomplete-semantic-01"),
        "caller-visible upstream headers preserved"
    );
    assert_eq!(
        response
            .bytes()
            .await
            .expect("gateway response body")
            .as_ref(),
        INCOMPLETE_JSON_RESPONSE_FIXTURE.as_bytes(),
        "caller-visible response bytes are byte-for-byte the upstream JSON"
    );

    let record = wait_for_jsonl_record(&usage_file).await;
    assert_eq!(
        read_jsonl(&usage_file).len(),
        1,
        "exactly one audit record per accepted request"
    );
    assert_eq!(record["kind"], "request");
    assert_eq!(record["operation"], "response");
    assert_eq!(record["machine_id"], "machine-a");
    assert_eq!(record["upstream_status"], 200);
    assert_eq!(record["outcome"], "incomplete");
    assert_eq!(record["model"], "synthetic-model-001");
    assert_eq!(record["accounting_quality"], "complete");
    assert!(record["metering_error"].is_null());

    let usage = &record["usage"];
    assert_eq!(usage["input_total"], 12);
    assert_eq!(usage["uncached"], 6);
    assert_eq!(usage["cache_read"], 4);
    assert_eq!(usage["cache_write"], 2);
    assert_eq!(usage["output_total"], 5);
    assert_eq!(usage["reasoning"], 2);
    assert_eq!(usage["total"], 17);
}

#[tokio::test]
async fn successful_stream_with_only_nonterminal_event_records_upstream_interrupted() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(canned_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::OK,
        headers: vec![
            ("content-type".to_string(), "text/event-stream".to_string()),
            (
                "x-codex-semantic".to_string(),
                "nonterminal-semantic-01".to_string(),
            ),
        ],
        body: NONTERMINAL_SSE_FIXTURE.as_bytes().to_vec(),
    });

    let usage_dir = tempfile::TempDir::new().unwrap();
    let usage_file = usage_dir.path().join("usage.jsonl");
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        &usage_file,
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-codex-semantic")
            .and_then(|v| v.to_str().ok()),
        Some("nonterminal-semantic-01"),
        "caller-visible upstream headers preserved"
    );
    assert_eq!(
        response
            .bytes()
            .await
            .expect("gateway response body")
            .as_ref(),
        NONTERMINAL_SSE_FIXTURE.as_bytes(),
        "caller-visible response bytes are byte-for-byte the upstream SSE"
    );

    let record = wait_for_jsonl_record(&usage_file).await;
    assert_eq!(
        read_jsonl(&usage_file).len(),
        1,
        "exactly one audit record per accepted request"
    );
    assert_eq!(record["kind"], "request");
    assert_eq!(record["operation"], "response");
    assert_eq!(record["machine_id"], "machine-a");
    assert_eq!(record["upstream_status"], 200);
    assert_eq!(record["outcome"], "upstream_interrupted");
    assert_eq!(record["model"], serde_json::Value::Null);
    assert_eq!(record["accounting_quality"], "unavailable");
    assert!(record["usage"].is_null());
}

fn raw_request_head(extra_header_bytes: &[u8]) -> Vec<u8> {
    let mut head =
        b"POST /v1/responses HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n".to_vec();
    head.extend_from_slice(extra_header_bytes);
    head.extend_from_slice(b"Content-Length: 100000\r\n\r\n");
    head
}

async fn raw_request_with_incomplete_body(addr: SocketAddr, request_head: Vec<u8>) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect to gateway");
    stream
        .write_all(&request_head)
        .await
        .expect("write request head");
    stream
        .write_all(b"partial-body-")
        .await
        .expect("write partial body");

    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .expect("gateway must answer without the full body")
            .expect("read response byte");
        if n == 0 {
            break;
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n") {
            break;
        }
    }
    buf
}

#[tokio::test]
async fn invalid_x_meter_key_forms_are_rejected_with_uniform_401_before_upstream() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(fake_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;
    let gateway_addr: SocketAddr = gateway_url
        .trim_start_matches("http://")
        .parse()
        .expect("gateway socket addr");

    let client = reqwest::Client::new();

    let mut duplicate_headers = HeaderMap::new();
    duplicate_headers.append(
        "x-meter-key",
        HeaderValue::from_static("test-meter-key-machine-a"),
    );
    duplicate_headers.append(
        "x-meter-key",
        HeaderValue::from_static("intruder-duplicate"),
    );

    let mut malformed_headers = HeaderMap::new();
    malformed_headers.append(
        "x-meter-key",
        HeaderValue::from_bytes(&[0xff, 0xfe]).expect("non-utf8 header value"),
    );

    let mut unknown_headers = HeaderMap::new();
    unknown_headers.append("x-meter-key", HeaderValue::from_static("intruder-unknown"));

    let forms: [(&str, Option<HeaderMap>); 4] = [
        ("missing", None),
        ("duplicate", Some(duplicate_headers)),
        ("malformed", Some(malformed_headers)),
        ("unknown", Some(unknown_headers)),
    ];

    for (name, headers) in forms {
        let mut request = client.post(format!("{gateway_url}/v1/responses"));
        if let Some(headers) = headers {
            request = request.headers(headers);
        }
        let response = request
            .body("opaque-request-body-42")
            .send()
            .await
            .unwrap_or_else(|err| panic!("{name}: caller reaches gateway: {err}"));

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{name} status");
        assert_eq!(
            response.bytes().await.expect("read 401 body").as_ref(),
            &b"unauthorized"[..],
            "{name} body"
        );
    }

    for (name, request_head) in [
        ("missing", raw_request_head(b"")),
        (
            "duplicate",
            raw_request_head(
                b"X-Meter-Key: test-meter-key-machine-a\r\nX-Meter-Key: intruder-duplicate\r\n",
            ),
        ),
        ("malformed", raw_request_head(b"X-Meter-Key: \xff\xfe\r\n")),
        (
            "unknown",
            raw_request_head(b"X-Meter-Key: intruder-unknown\r\n"),
        ),
    ] {
        let status_line = raw_request_with_incomplete_body(gateway_addr, request_head).await;
        assert!(
            status_line.starts_with(b"HTTP/1.1 401"),
            "{name}: uniform 401 without consuming the body, got {status_line:?}"
        );
    }

    assert!(
        upstream.captured.lock().unwrap().is_empty(),
        "fake upstream must observe no request for any invalid form"
    );
}

#[tokio::test]
async fn closed_route_set_reaches_upstream_only_for_known_post_paths() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(fake_upstream_handler))
        .route("/responses/compact", post(fake_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let valid_key = "test-meter-key-machine-a";

    for (path, body) in [
        ("/v1/responses", "opaque-request-body-42"),
        ("/v1/responses/compact", "opaque-compact-body-07"),
    ] {
        let response = client
            .post(format!("{gateway_url}{path}"))
            .header("x-meter-key", valid_key)
            .body(body)
            .send()
            .await
            .unwrap_or_else(|err| panic!("{path}: caller reaches gateway: {err}"));

        assert_eq!(response.status(), StatusCode::CREATED, "{path} status");
        assert_eq!(
            response.bytes().await.expect("read upstream body").as_ref(),
            &b"opaque-upstream-body-01"[..],
            "{path} body"
        );
    }

    {
        let captured = upstream.captured.lock().unwrap();
        assert_eq!(captured.len(), 2, "both known paths reach upstream");
        assert_eq!(captured[0].method, "POST");
        assert_eq!(captured[0].path, "/responses");
        assert_eq!(captured[0].body, b"opaque-request-body-42");
        assert_eq!(captured[1].method, "POST");
        assert_eq!(captured[1].path, "/responses/compact");
        assert_eq!(captured[1].body, b"opaque-compact-body-07");
    }

    for (method, path) in [
        (reqwest::Method::GET, "/v1/responses"),
        (reqwest::Method::DELETE, "/v1/responses/compact"),
    ] {
        let response = client
            .request(method.clone(), format!("{gateway_url}{path}"))
            .header("x-meter-key", valid_key)
            .send()
            .await
            .unwrap_or_else(|err| panic!("{path}: caller reaches gateway: {err}"));

        assert_eq!(
            response.status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "{method} {path} status"
        );
    }

    for path in ["/v1/unknown", "/v1/responses/other", "/nope"] {
        let response = client
            .post(format!("{gateway_url}{path}"))
            .header("x-meter-key", valid_key)
            .body("opaque-request-body-42")
            .send()
            .await
            .unwrap_or_else(|err| panic!("{path}: caller reaches gateway: {err}"));

        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path} status");
    }

    assert_eq!(
        upstream.captured.lock().unwrap().len(),
        2,
        "wrong method and unknown path never reach upstream"
    );
}

async fn raw_post_complete(addr: SocketAddr, request_head: Vec<u8>, body: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect to gateway");
    stream
        .write_all(&request_head)
        .await
        .expect("write request head");
    stream.write_all(body).await.expect("write request body");

    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .expect("read response")
            .expect("read response byte");
        if n == 0 {
            break;
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n") {
            break;
        }
    }
    buf
}

#[tokio::test]
async fn request_header_policy_strips_privacy_and_hop_headers_before_upstream() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(fake_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;
    let gateway_addr: SocketAddr = gateway_url
        .trim_start_matches("http://")
        .parse()
        .expect("gateway socket addr");

    let request_head = b"POST /v1/responses HTTP/1.1\r\n\
Host: caller-host.example\r\n\
Connection: X-Synthetic-Hop\r\n\
X-Synthetic-Hop: hop-value\r\n\
Cookie: session=secret\r\n\
Keep-Alive: timeout=5\r\n\
Forwarded: for=192.0.2.1\r\n\
Via: 1.0 proxy\r\n\
X-Forwarded-For: 192.0.2.1\r\n\
X-Forwarded-Host: example.com\r\n\
X-Forwarded-Proto: https\r\n\
X-Forwarded-Port: 443\r\n\
X-Forwarded-Prefix: /codex\r\n\
X-Forwarded-Client-Cert: synthetic-client-cert\r\n\
X-Real-IP: 192.0.2.1\r\n\
TE: trailers\r\n\
Upgrade: h2c\r\n\
Proxy-Connection: keep-alive\r\n\
Proxy-Authenticate: Basic realm=gw\r\n\
Proxy-Authorization: Basic c3ludGhldGlj\r\n\
X-Meter-Key: test-meter-key-machine-a\r\n\
Authorization: Bearer clearly-invalid-synthetic-token\r\n\
ChatGPT-Account-ID: synthetic-account-01\r\n\
X-Codex-Custom: synthetic-value\r\n\
Content-Length: 8\r\n\
\r\n"
        .to_vec();

    let status_line = raw_post_complete(gateway_addr, request_head, b"opaque-1").await;
    assert!(
        status_line.starts_with(b"HTTP/1.1 201"),
        "valid key request reaches upstream, got {status_line:?}"
    );

    let captured = upstream
        .captured
        .lock()
        .unwrap()
        .pop()
        .expect("fake upstream observed one request");
    let upstream_headers: BTreeMap<String, String> = captured.headers.into_iter().collect();

    assert_eq!(
        upstream_headers.get("authorization").map(String::as_str),
        Some("Bearer clearly-invalid-synthetic-token"),
        "Authorization must reach upstream uninterpreted"
    );
    assert_eq!(
        upstream_headers
            .get("chatgpt-account-id")
            .map(String::as_str),
        Some("synthetic-account-01"),
        "ChatGPT-Account-ID must reach upstream"
    );
    assert_eq!(
        upstream_headers.get("x-codex-custom").map(String::as_str),
        Some("synthetic-value"),
        "unknown Codex header must reach upstream"
    );

    for name in [
        "connection",
        "x-synthetic-hop",
        "cookie",
        "x-meter-key",
        "keep-alive",
        "forwarded",
        "via",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
        "x-forwarded-port",
        "x-forwarded-prefix",
        "x-forwarded-client-cert",
        "x-real-ip",
        "te",
        "upgrade",
        "proxy-connection",
        "proxy-authenticate",
        "proxy-authorization",
    ] {
        assert!(
            !upstream_headers.contains_key(name),
            "{name} must be stripped before upstream"
        );
    }

    assert_ne!(
        upstream_headers.get("host").map(String::as_str),
        Some("caller-host.example"),
        "caller Host must not reach upstream"
    );
}

fn event_for_sentinel(rest: &[u8]) -> Option<(usize, &'static [u8])> {
    let pos = rest.windows(3).position(|w| w == b"|A|" || w == b"|B|")?;
    let event = if &rest[pos..pos + 3] == b"|A|" {
        b"data: A\n\n"
    } else {
        b"data: B\n\n"
    };
    Some((pos, event))
}

async fn streaming_upstream_handler(
    State(upstream): State<FakeUpstream>,
    req: Request<Body>,
) -> Response {
    let mut request_stream = req.into_body().into_data_stream();
    let store = upstream.received_body.clone();
    let (tx, rx) = mpsc::channel::<Result<Bytes, Infallible>>(8);
    tokio::spawn(async move {
        let mut received: Vec<u8> = Vec::new();
        let mut scan = 0usize;
        while let Some(chunk) = request_stream.next().await {
            let Ok(bytes) = chunk else { break };
            received.extend_from_slice(&bytes);
            while let Some((rel, event)) = event_for_sentinel(&received[scan..]) {
                scan += rel + 3;
                if tx.send(Ok(Bytes::from_static(event))).await.is_err() {
                    return;
                }
            }
        }
        let _ = tx.send(Ok(Bytes::from_static(b"data: done\n\n"))).await;
        *store.lock().unwrap() = Some(received);
    });
    Response::new(axum::body::Body::from_stream(ReceiverStream::new(rx)))
}

async fn read_response_until(
    stream: &mut (impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin),
    body: &mut Vec<u8>,
    needle: &[u8],
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !body.windows(needle.len()).any(|w| w == needle) {
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for {needle:?}; response body so far: {:?}",
                String::from_utf8_lossy(body)
            );
        }
        let chunk = tokio::time::timeout(deadline - tokio::time::Instant::now(), stream.next())
            .await
            .expect("read response chunk")
            .expect("response body still open")
            .expect("response body chunk");
        body.extend_from_slice(&chunk);
    }
}

#[tokio::test]
async fn opaque_bidirectional_streaming_relays_progress_in_order() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(streaming_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let (tx, rx) = mpsc::channel::<Result<Bytes, Infallible>>(8);
    let request_body = reqwest::Body::wrap_stream(ReceiverStream::new(rx));

    let response = tokio::time::timeout(
        Duration::from_secs(3),
        client
            .post(format!("{gateway_url}/v1/responses"))
            .header("x-meter-key", "test-meter-key-machine-a")
            .body(request_body)
            .send(),
    )
    .await
    .expect("caller receives gateway response without finishing the request body")
    .expect("caller reaches gateway");
    assert_eq!(response.status(), StatusCode::OK);

    tx.send(Ok(Bytes::from_static(b"part-A|A|")))
        .await
        .expect("caller streams part A");

    let mut body: Vec<u8> = Vec::new();
    let mut response_stream = response.bytes_stream();
    read_response_until(&mut response_stream, &mut body, b"data: A\n\n").await;
    assert_eq!(
        body, b"data: A\n\n",
        "upstream observed request progress and caller observed response progress"
    );

    tx.send(Ok(Bytes::from_static(b"part-B|B|")))
        .await
        .expect("caller streams part B");
    drop(tx);

    read_response_until(&mut response_stream, &mut body, b"data: done\n\n").await;
    assert_eq!(
        body, b"data: A\n\ndata: B\n\ndata: done\n\n",
        "response bytes remain in order"
    );

    let received = upstream
        .received_body
        .lock()
        .unwrap()
        .clone()
        .expect("upstream read the request body");
    assert_eq!(
        received, b"part-A|A|part-B|B|",
        "request bytes remain in order"
    );
}

async fn canned_upstream_handler(
    State(upstream): State<FakeUpstream>,
    req: Request<Body>,
) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(|q| q.to_string());
    let headers = req
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    let bytes = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .expect("fake upstream reads request body");
    upstream.captured.lock().unwrap().push(CapturedRequest {
        method,
        path,
        body: bytes.to_vec(),
        headers,
        query,
    });
    let canned = upstream.queue.lock().unwrap().remove(0);
    let mut builder = Response::builder().status(canned.status);
    for (name, value) in canned.headers {
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from(canned.body))
        .expect("canned response")
}

async fn redirect_target_handler(State(upstream): State<FakeUpstream>) -> Response {
    *upstream.redirect_hits.lock().unwrap() += 1;
    (StatusCode::OK, "redirect-target-hit").into_response()
}

#[tokio::test]
async fn upstream_response_semantics_preserved_without_redirect_following() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(canned_upstream_handler))
        .route("/responses/compact", post(canned_upstream_handler))
        .route("/redirect-target", any(redirect_target_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("caller client");

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::FOUND,
        headers: vec![
            ("location".to_string(), "/redirect-target".to_string()),
            (
                "x-codex-semantic".to_string(),
                "redirect-semantic-01".to_string(),
            ),
            ("content-type".to_string(), "text/plain".to_string()),
            ("connection".to_string(), "X-Synthetic-Hop".to_string()),
            ("x-synthetic-hop".to_string(), "hop-value".to_string()),
            ("keep-alive".to_string(), "timeout=5".to_string()),
        ],
        body: b"redirect-body-01".to_vec(),
    });

    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(
        *upstream.redirect_hits.lock().unwrap(),
        0,
        "redirect target must never be requested"
    );
    assert_eq!(
        response.status(),
        StatusCode::FOUND,
        "3xx status passes through"
    );
    assert_eq!(
        response
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok()),
        Some("/redirect-target"),
        "Location preserved"
    );
    assert_eq!(
        response
            .headers()
            .get("x-codex-semantic")
            .and_then(|v| v.to_str().ok()),
        Some("redirect-semantic-01"),
        "unknown Codex header preserved"
    );
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/plain"),
        "content-type preserved"
    );
    assert!(
        !response.headers().contains_key("connection"),
        "connection stripped"
    );
    assert!(
        !response.headers().contains_key("x-synthetic-hop"),
        "connection-nominated header stripped"
    );
    assert!(
        !response.headers().contains_key("keep-alive"),
        "hop-by-hop keep-alive stripped"
    );
    assert_eq!(
        response.bytes().await.expect("redirect body").as_ref(),
        &b"redirect-body-01"[..],
        "redirect body bytes preserved"
    );

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::TOO_MANY_REQUESTS,
        headers: vec![
            ("retry-after".to_string(), "7".to_string()),
            ("content-type".to_string(), "application/json".to_string()),
            (
                "x-codex-semantic".to_string(),
                "rate-limit-semantic-02".to_string(),
            ),
        ],
        body: b"{\"error\":\"opaque-rate-limit\"}".to_vec(),
    });

    let response = client
        .post(format!("{gateway_url}/v1/responses/compact"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-compact-body-07")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(
        response.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "429 status passes through"
    );
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("7"),
        "Retry-After preserved"
    );
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/json"),
        "content-type preserved on non-2xx"
    );
    assert_eq!(
        response
            .headers()
            .get("x-codex-semantic")
            .and_then(|v| v.to_str().ok()),
        Some("rate-limit-semantic-02"),
        "unknown Codex header preserved on non-2xx"
    );
    assert_eq!(
        response.bytes().await.expect("429 body").as_ref(),
        &b"{\"error\":\"opaque-rate-limit\"}"[..],
        "429 body bytes preserved"
    );
}

#[tokio::test]
async fn response_header_policy_strips_only_hop_by_hop() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(canned_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("caller client");

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::OK,
        headers: vec![
            ("via".to_string(), "1.1 synthetic-via".to_string()),
            ("forwarded".to_string(), "for=192.0.2.1".to_string()),
            (
                "x-meter-upstream".to_string(),
                "upstream-meter-01".to_string(),
            ),
            ("cookie".to_string(), "upstream-cookie=1".to_string()),
            ("set-cookie".to_string(), "session=synthetic".to_string()),
            (
                "x-codex-custom".to_string(),
                "response-custom-01".to_string(),
            ),
            ("connection".to_string(), "X-Response-Hop".to_string()),
            ("x-response-hop".to_string(), "hop-value".to_string()),
            ("keep-alive".to_string(), "timeout=5".to_string()),
            ("proxy-connection".to_string(), "keep-alive".to_string()),
        ],
        body: b"opaque-response-body-09".to_vec(),
    });

    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(response.status(), StatusCode::OK);

    assert_eq!(
        response.headers().get("via").and_then(|v| v.to_str().ok()),
        Some("1.1 synthetic-via"),
        "Via preserved on response (request-only strip name)"
    );
    assert_eq!(
        response
            .headers()
            .get("forwarded")
            .and_then(|v| v.to_str().ok()),
        Some("for=192.0.2.1"),
        "Forwarded preserved on response (request-only strip name)"
    );
    assert_eq!(
        response
            .headers()
            .get("x-meter-upstream")
            .and_then(|v| v.to_str().ok()),
        Some("upstream-meter-01"),
        "X-Meter-* preserved on response (request-only strip name)"
    );
    assert_eq!(
        response
            .headers()
            .get("cookie")
            .and_then(|v| v.to_str().ok()),
        Some("upstream-cookie=1"),
        "Cookie preserved on response (request-only strip name)"
    );
    assert_eq!(
        response
            .headers()
            .get("set-cookie")
            .and_then(|v| v.to_str().ok()),
        Some("session=synthetic"),
        "Set-Cookie preserved"
    );
    assert_eq!(
        response
            .headers()
            .get("x-codex-custom")
            .and_then(|v| v.to_str().ok()),
        Some("response-custom-01"),
        "unknown Codex header preserved"
    );

    assert!(
        !response.headers().contains_key("connection"),
        "connection stripped"
    );
    assert!(
        !response.headers().contains_key("x-response-hop"),
        "Connection-nominated header stripped"
    );
    assert!(
        !response.headers().contains_key("keep-alive"),
        "hop-by-hop keep-alive stripped"
    );
    assert!(
        !response.headers().contains_key("proxy-connection"),
        "hop-by-hop proxy-connection stripped"
    );

    assert_eq!(
        response.bytes().await.expect("response body").as_ref(),
        &b"opaque-response-body-09"[..],
        "response body preserved"
    );
}

#[tokio::test]
async fn missing_authorization_still_reaches_upstream_which_owns_its_401() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(canned_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();

    upstream.queue.lock().unwrap().push(CannedResponse {
        status: StatusCode::UNAUTHORIZED,
        headers: vec![
            (
                "www-authenticate".to_string(),
                "Bearer realm=codex".to_string(),
            ),
            (
                "x-codex-semantic".to_string(),
                "upstream-auth-401".to_string(),
            ),
        ],
        body: b"{\"error\":\"upstream-owned-401\"}".to_vec(),
    });

    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "upstream owns its 401 status"
    );
    assert_eq!(
        response
            .headers()
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok()),
        Some("Bearer realm=codex"),
        "upstream semantic 401 header preserved"
    );
    assert_eq!(
        response.bytes().await.expect("401 body").as_ref(),
        &b"{\"error\":\"upstream-owned-401\"}"[..],
        "upstream owns its 401 body"
    );

    let captured = upstream
        .captured
        .lock()
        .unwrap()
        .pop()
        .expect("fake upstream saw the forwarded request");
    assert_eq!(captured.path, "/responses");
    assert_eq!(captured.body, b"opaque-request-body-42");
    assert!(
        captured
            .headers
            .iter()
            .all(|(name, _)| name != "authorization"),
        "no Authorization header reaches upstream"
    );
}

async fn cancellation_upstream_handler(
    State(upstream): State<FakeUpstream>,
    req: Request<Body>,
) -> Response {
    let mut request_stream = req.into_body().into_data_stream();
    let dropped = upstream.dropped.clone();
    let (tx, rx) = mpsc::channel::<Result<Bytes, Infallible>>(8);
    tokio::spawn(async move {
        let mut sent_first = false;
        loop {
            match request_stream.next().await {
                Some(Ok(bytes)) => {
                    if !sent_first {
                        sent_first = true;
                        if tx
                            .send(Ok(Bytes::from_static(b"data: first\n\n")))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    let _ = bytes;
                }
                _ => {
                    *dropped.lock().unwrap() = true;
                    return;
                }
            }
        }
    });
    Response::new(axum::body::Body::from_stream(ReceiverStream::new(rx)))
}

async fn read_raw_until(stream: &mut TcpStream, acc: &mut Vec<u8>, needle: &[u8]) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !acc.windows(needle.len()).any(|w| w == needle) {
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for {needle:?}; received so far: {:?}",
                String::from_utf8_lossy(acc)
            );
        }
        let mut byte = [0u8; 1];
        let n = tokio::time::timeout(
            deadline - tokio::time::Instant::now(),
            stream.read(&mut byte),
        )
        .await
        .expect("read byte")
        .expect("connection open");
        if n == 0 {
            panic!("connection closed waiting for {needle:?}");
        }
        acc.extend_from_slice(&byte);
    }
}

#[tokio::test]
async fn transport_failure_returns_safe_generic_502() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral listener");
    let dead_addr = listener.local_addr().expect("resolved address");
    drop(listener);

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&format!("http://{dead_addr}")).expect("dead upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway");

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

    let body = response.bytes().await.expect("502 body");
    let body_text = String::from_utf8_lossy(&body);
    assert_eq!(
        body.as_ref(),
        &b"upstream unreachable"[..],
        "generic safe 502 body"
    );
    assert!(
        !body_text.contains(&dead_addr.to_string()),
        "502 must not expose the upstream address"
    );
    assert!(
        !body_text.contains("test-meter-key-machine-a"),
        "502 must not expose credentials"
    );
    assert!(
        !body_text.contains("machine-a"),
        "502 must not expose machine identity"
    );
}

#[tokio::test]
async fn caller_cancellation_stops_upstream_pumping() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/responses", post(cancellation_upstream_handler))
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;
    let gateway_addr: SocketAddr = gateway_url
        .trim_start_matches("http://")
        .parse()
        .expect("gateway socket addr");

    let mut stream = TcpStream::connect(gateway_addr)
        .await
        .expect("connect to gateway");
    stream
        .write_all(
            b"POST /v1/responses HTTP/1.1\r\n\
Host: gateway\r\n\
Transfer-Encoding: chunked\r\n\
X-Meter-Key: test-meter-key-machine-a\r\n\
\r\n",
        )
        .await
        .expect("write request head");
    stream
        .write_all(b"6\r\npart-A\r\n")
        .await
        .expect("write part A");

    let mut raw = Vec::new();
    read_raw_until(&mut stream, &mut raw, b"data: first\n\n").await;
    assert!(
        String::from_utf8_lossy(&raw).contains("data: first\n\n"),
        "streaming has begun before cancellation"
    );

    drop(stream);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if *upstream.dropped.lock().unwrap() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("gateway kept pumping upstream after caller cancelled");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A raw-TCP fake upstream that reads the full request head, answers with a
/// successful `text/event-stream` response head, and then stays quiet forever.
/// It passively observes the gateway's upstream connection and flips `dropped`
/// only when that connection closes — never because the request body ended, so
/// it cannot be fooled by request completion.
async fn spawn_quiet_tcp_upstream(dropped: Arc<Mutex<bool>>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind quiet upstream listener");
    let addr = listener.local_addr().expect("resolved address");
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => return,
            };
            let dropped = dropped.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\n\
                        Content-Type: text/event-stream\r\n\
                        Transfer-Encoding: chunked\r\n\
                        \r\n",
                    )
                    .await
                    .expect("write quiet upstream response head");
                let mut byte = [0u8; 1];
                loop {
                    match stream.read(&mut byte).await {
                        Ok(0) | Err(_) => {
                            *dropped.lock().unwrap() = true;
                            return;
                        }
                        Ok(_) => {}
                    }
                }
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn caller_disconnect_after_response_head_records_client_cancelled_with_quiet_upstream() {
    let dropped = Arc::new(Mutex::new(false));
    let upstream_url = spawn_quiet_tcp_upstream(dropped.clone()).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let usage_file = usage_dir.path().join("usage.jsonl");
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&upstream_url).expect("fake upstream url"),
        machine_keys,
        &usage_file,
    );
    let gateway_url = spawn(gateway.router()).await;
    let gateway_addr: SocketAddr = gateway_url
        .trim_start_matches("http://")
        .parse()
        .expect("gateway socket addr");

    let mut stream = TcpStream::connect(gateway_addr)
        .await
        .expect("connect to gateway");
    stream
        .write_all(
            b"POST /v1/responses HTTP/1.1\r\n\
            Host: gateway\r\n\
            Content-Length: 8\r\n\
            X-Meter-Key: test-meter-key-machine-a\r\n\
            \r\n\
            opaque-1",
        )
        .await
        .expect("write complete request");

    let mut raw = Vec::new();
    read_raw_until(&mut stream, &mut raw, b"\r\n\r\n").await;
    assert!(
        String::from_utf8_lossy(&raw).starts_with("HTTP/1.1 200"),
        "caller receives the upstream response head"
    );

    drop(stream);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if *dropped.lock().unwrap() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "quiet upstream did not observe connection closure after caller disconnect \
                 without another upstream body chunk"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let record = wait_for_jsonl_record(&usage_file).await;
    assert_eq!(
        read_jsonl(&usage_file).len(),
        1,
        "exactly one audit record per accepted request"
    );
    assert_eq!(record["kind"], "request");
    assert_eq!(record["operation"], "response");
    assert_eq!(record["machine_id"], "machine-a");
    assert_eq!(record["upstream_status"], 200);
    assert_eq!(
        record["outcome"], "client_cancelled",
        "caller disconnect must classify as client_cancelled, never upstream_interrupted"
    );
    assert_eq!(record["accounting_quality"], "unavailable");
    assert!(
        record["usage"].is_null(),
        "no terminal usage was received, so usage must stay null without guessed counters"
    );
}

#[tokio::test]
async fn prefixed_upstream_base_preserves_codex_prefix_on_both_routes() {
    let upstream = FakeUpstream::default();
    let fake_app = Router::new()
        .route("/backend-api/codex/responses", post(fake_upstream_handler))
        .route(
            "/backend-api/codex/responses/compact",
            post(fake_upstream_handler),
        )
        .with_state(upstream.clone());
    let upstream_url = spawn(fake_app).await;

    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse(&format!("{upstream_url}/backend-api/codex/"))
            .expect("prefixed fake upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();

    let response = client
        .post(format!("{gateway_url}/v1/responses"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-request-body-42")
        .send()
        .await
        .expect("caller reaches gateway for responses");
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "/v1/responses reaches the prefixed upstream path"
    );

    let response = client
        .post(format!("{gateway_url}/v1/responses/compact?debug=1"))
        .header("x-meter-key", "test-meter-key-machine-a")
        .body("opaque-compact-body-07")
        .send()
        .await
        .expect("caller reaches gateway for compact");
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "/v1/responses/compact reaches the prefixed upstream path"
    );

    {
        let captured = upstream.captured.lock().unwrap();
        assert_eq!(captured.len(), 2, "both routes reach the prefixed upstream");
        assert_eq!(captured[0].method, "POST");
        assert_eq!(captured[0].path, "/backend-api/codex/responses");
        assert_eq!(captured[0].body, b"opaque-request-body-42");
        assert_eq!(captured[0].query, None);
        assert_eq!(captured[1].method, "POST");
        assert_eq!(captured[1].path, "/backend-api/codex/responses/compact");
        assert_eq!(captured[1].query.as_deref(), Some("debug=1"));
        assert_eq!(captured[1].body, b"opaque-compact-body-07");
    }
}

#[tokio::test]
async fn healthz_returns_200_without_authentication_and_rejects_other_methods() {
    let usage_dir = tempfile::TempDir::new().unwrap();
    let machine_keys =
        BTreeMap::from([(TEST_METER_KEY_DIGEST.to_string(), String::from("machine-a"))]);
    let gateway = Gateway::for_tests(
        reqwest::Url::parse("http://127.0.0.1:9").expect("unused upstream url"),
        machine_keys,
        usage_dir.path().join("usage.jsonl"),
    );
    let gateway_url = spawn(gateway.router()).await;

    let client = reqwest::Client::new();

    let response = client
        .get(format!("{gateway_url}/healthz"))
        .send()
        .await
        .expect("healthz is reachable without a meter key");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "healthz returns 200 without authentication"
    );

    let response = client
        .post(format!("{gateway_url}/healthz"))
        .send()
        .await
        .expect("wrong method reaches the gateway");
    assert_eq!(
        response.status(),
        StatusCode::METHOD_NOT_ALLOWED,
        "wrong method on a known path returns 405 and is never forwarded"
    );
}
