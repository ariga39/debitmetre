//! Opt-in `debitmetre smoke` load-smoke command (issue #25).
//!
//! This is an operator acceptance tool, not part of the production proxy. It
//! runs a safe, deterministic mock streaming load through the **real release
//! gateway** (built with the test-only `test-upstream-override` seam) and
//! reconciles the canonical audit records, so it is compiled only under that
//! feature and never in a production build. It contacts no real upstream and
//! consumes zero model tokens.
//!
//! Flow: an external community load generator (`oha`) -> the real gateway ->
//! a loopback-only mock upstream that streams terminal SSE carrying canonical
//! model/usage. The harness reports oha version/workload, completed/success/
//! errors, reference RPS + p50/p95/p99, gateway baseline/peak/end RSS, and
//! canonical audit accepted/metered counts; it fails on load errors, non-2xx
//! responses, or missing/mismatched audit records, and always cleans up its
//! local processes and temporary artifacts.
//!
//! Reuse note: oha (https://github.com/hatoo/oha) is the mature community load
//! generator; this harness only invokes its CLI and parses its JSON output
//! rather than building a bespoke request engine.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use futures_util::stream;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::CliError;

/// Default workload: safe on an ordinary development machine while still
/// producing concurrent live streams.
const DEFAULT_COUNT: u64 = 100;
const DEFAULT_CONCURRENCY: u64 = 10;
const DEFAULT_RESPONSE_BYTES: u64 = 4096;
const DEFAULT_DELAY_MS: u64 = 5;
const DEFAULT_PORT: u16 = 18799;

/// Sanitized static report labels. Only aggregates are printed; never raw
/// records, bodies, keys, or exact token totals.
const REPORT_OHALINE: &str = "oha version/workload";
const REPORT_RPS: &str = "reference rps/p50/p95/p99";
const REPORT_RSS: &str = "gateway rss baseline/peak/end";
const REPORT_AUDIT: &str = "audit accepted/metered";

/// Validated workload and sizing parameters for one smoke run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmokeParams {
    pub count: u64,
    pub concurrency: u64,
    pub response_bytes: u64,
    pub delay_ms: u64,
    /// Explicit oha path (`--oha`); falls back to `OHA_BIN` then `oha` in PATH.
    pub oha: Option<PathBuf>,
    /// Gateway loopback port; 0 means pick a free port.
    pub port: u16,
}

impl Default for SmokeParams {
    fn default() -> Self {
        SmokeParams {
            count: DEFAULT_COUNT,
            concurrency: DEFAULT_CONCURRENCY,
            response_bytes: DEFAULT_RESPONSE_BYTES,
            delay_ms: DEFAULT_DELAY_MS,
            oha: None,
            port: DEFAULT_PORT,
        }
    }
}

/// Static help text for the smoke command.
pub fn usage() -> &'static str {
    "debitmetre smoke — run a safe mock streaming load smoke through the real gateway\n\
     \n\
     USAGE:\n\
     \x20  debitmetre smoke [OPTIONS]\n\
     \n\
     Runs the external oha load generator through the real release gateway into\n\
     a loopback-only mock upstream (terminal SSE with canonical model/usage),\n\
     then reconciles the canonical audit records. Contacts no real upstream and\n\
     consumes zero model tokens.\n\
     \n\
     OPTIONS:\n\
     \x20  --count <N>            total requests (default: 100)\n\
     \x20  --concurrency <C>      concurrent connections (default: 10)\n\
     \x20  --response-bytes <B>   mock response body bytes (default: 4096)\n\
     \x20  --delay-ms <D>         mock stream delay between chunks (default: 5)\n\
     \x20  --oha <PATH>           path to the oha binary (default: OHA_BIN, then oha in PATH)\n\
     \x20  --port <PORT>          gateway loopback port, 0 = free (default: 18799)\n\
     \x20  -h, --help             print this help"
}

/// Parse the smoke subcommand's arguments. Errors are sanitized and name the
/// smoke command; invalid values fail before any traffic or subprocess starts.
pub fn parse_smoke(args: &[String]) -> Result<SmokeParams, CliError> {
    let mut params = SmokeParams::default();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "-h" || arg == "--help" {
            return Err(CliError::SmokeHelp);
        }
        let mut take_value = |name: &str| -> Result<String, CliError> {
            let value = iter
                .next()
                .ok_or_else(|| CliError::Smoke(format!("{name} requires a value")))?;
            if value.starts_with('-') {
                return Err(CliError::Smoke(format!(
                    "{name} requires a value, got option-like '{value}'"
                )));
            }
            Ok(value.clone())
        };
        match arg.as_str() {
            "--count" => params.count = parse_uint("--count", &take_value("--count")?)?,
            "--concurrency" => {
                params.concurrency = parse_uint("--concurrency", &take_value("--concurrency")?)?
            }
            "--response-bytes" => {
                params.response_bytes =
                    parse_uint("--response-bytes", &take_value("--response-bytes")?)?
            }
            "--delay-ms" => params.delay_ms = parse_uint("--delay-ms", &take_value("--delay-ms")?)?,
            "--oha" => params.oha = Some(PathBuf::from(take_value("--oha")?)),
            "--port" => {
                let port = take_value("--port")?;
                params.port = port.parse::<u16>().map_err(|_| {
                    CliError::Smoke(format!(
                        "--port requires an integer port 0..65535, got '{port}'"
                    ))
                })?;
            }
            other => return Err(CliError::Smoke(format!("unknown option '{other}'"))),
        }
    }
    // Zero-size workloads or bodies can never create a concurrent live stream
    // or a meaningful metering record; reject them up front.
    if params.count == 0 {
        return Err(CliError::Smoke("--count must be at least 1".into()));
    }
    if params.concurrency == 0 {
        return Err(CliError::Smoke("--concurrency must be at least 1".into()));
    }
    if params.response_bytes == 0 {
        return Err(CliError::Smoke(
            "--response-bytes must be at least 1".into(),
        ));
    }
    Ok(params)
}

/// Parse a non-negative integer argument with a sanitized, named error.
fn parse_uint(name: &str, value: &str) -> Result<u64, CliError> {
    value.parse::<u64>().map_err(|_| {
        CliError::Smoke(format!(
            "{name} requires a non-negative integer, got '{value}'"
        ))
    })
}

/// Resolve the oha binary: `--oha` wins, then `OHA_BIN`, then `oha` in PATH.
fn resolve_oha(explicit: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(path) = explicit {
        if !path.is_file() {
            return Err(format!("oha not found at '{}'", path.display()));
        }
        return Ok(path.to_path_buf());
    }
    if let Ok(env_path) = std::env::var("OHA_BIN") {
        let path = PathBuf::from(&env_path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "OHA_BIN points to a missing oha: '{}'",
            path.display()
        ));
    }
    if let Some(path) = find_in_path("oha") {
        return Ok(path);
    }
    Err("oha not found; set OHA_BIN or pass --oha".into())
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// A temporary workspace owned by this smoke run; removed on drop so a
/// panicking or erroring run never leaves artifacts behind.
struct WorkDir {
    path: PathBuf,
}

impl WorkDir {
    fn new() -> Result<WorkDir, String> {
        let path = std::env::temp_dir().join(format!(
            "debitmetre-smoke-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path)
            .map_err(|err| format!("cannot create smoke workspace: {err}"))?;
        Ok(WorkDir { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for WorkDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Deterministic synthetic meter key, never committed; only its SHA-256 digest
/// enters the temporary gateway config.
const MOCK_METER_KEY: &str = "dm-smoke-synthetic-meter-key";
const MOCK_MACHINE_ID: &str = "machine-smoke";
const MOCK_MODEL: &str = "mock-model";

fn digest_hex(key: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key);
    hex::encode(hasher.finalize())
}

/// Loopback-only mock upstream: serves the Responses/compact paths (the test
/// seam maps them under the fixed upstream base) with a deterministic terminal
/// SSE response carrying canonical model/usage and a configurable body size and
/// stream delay. Binds to `127.0.0.1:0` so it is always loopback-only.
async fn spawn_mock_upstream(
    response_bytes: u64,
    delay_ms: u64,
) -> Result<(String, tokio::task::JoinHandle<()>), String> {
    let app = Router::new()
        .route(
            "/responses",
            post(move |req| mock_handler(req, response_bytes, delay_ms)),
        )
        .route(
            "/responses/compact",
            post(move |req| mock_handler(req, response_bytes, delay_ms)),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|err| format!("cannot bind mock upstream: {err}"))?;
    let addr = listener
        .local_addr()
        .map_err(|err| format!("cannot resolve mock upstream address: {err}"))?;
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((format!("http://{addr}"), handle))
}

/// Deterministic terminal SSE handler for the mock upstream.
async fn mock_handler(_req: Request<Body>, response_bytes: u64, delay_ms: u64) -> Response {
    let terminal = format!(
        "event: response.completed\ndata: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp_mock\",\"model\":\"{MOCK_MODEL}\",\"usage\":{{\"input_tokens\":20,\"input_tokens_details\":{{\"cached_tokens\":5,\"cache_write_tokens\":5}},\"output_tokens\":15,\"output_tokens_details\":{{\"reasoning_tokens\":5}},\"total_tokens\":35}}}}}}\n\n"
    );
    let terminal_len = terminal.len() as u64;
    let padding = response_bytes.saturating_sub(terminal_len);
    // One delta chunk of the requested padding so response size is meaningful;
    // the terminal event is streamed last so concurrent streams overlap.
    let delay = Duration::from_millis(delay_ms);

    let chunks: Vec<Vec<u8>> = {
        let mut out = Vec::new();
        let mut remaining = padding;
        let mut seq = 0u64;
        while remaining > 0 {
            let this = remaining.min(512);
            remaining -= this;
            let delta = "d".repeat(this as usize);
            out.push(format!(
                "data: {{\"type\":\"response.output_text.delta\",\"seq\":{seq},\"delta\":\"{delta}\"}}\n\n"
            ).into_bytes());
            seq += 1;
        }
        out.push(terminal.clone().into_bytes());
        out
    };

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header("openai-model", MOCK_MODEL);
    let body = stream::unfold(
        (chunks.into_iter(), delay),
        move |(mut iter, delay)| async move {
            match iter.next() {
                Some(chunk) => {
                    if delay > Duration::ZERO {
                        tokio::time::sleep(delay).await;
                    }
                    Some((Ok::<_, std::convert::Infallible>(chunk), (iter, delay)))
                }
                None => None,
            }
        },
    );
    response
        .body(Body::from_stream(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Spawn the real release gateway as a subprocess pointed at the mock upstream
/// via the `test-upstream-override` seam, with a disposable config and usage
/// file. Returns the child, the config, and the usage file path.
fn spawn_gateway(
    current_exe: &Path,
    mock_upstream: &str,
    port: u16,
    workdir: &Path,
) -> Result<(Child, PathBuf, PathBuf), String> {
    let usage_file = workdir.join("usage.jsonl");
    let digest = digest_hex(MOCK_METER_KEY.as_bytes());
    let config_path = workdir.join("gateway.toml");
    let config = format!(
        "listen = \"127.0.0.1:{port}\"\nusage_file = \"{}\"\n\n[machine_keys]\n\"{digest}\" = \"{MOCK_MACHINE_ID}\"\n",
        usage_file.display()
    );
    std::fs::write(&config_path, config)
        .map_err(|err| format!("cannot write smoke gateway config: {err}"))?;
    let child = Command::new(current_exe)
        .arg("--config")
        .arg(&config_path)
        .env("DEBITMETRE_TEST_UPSTREAM", mock_upstream)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| format!("cannot spawn the gateway: {err}"))?;
    Ok((child, config_path, usage_file))
}

/// Pick a free loopback port (bind :0, read the assigned port, drop the
/// listener). Fine for a smoke tool; there is a negligible reuse race.
fn pick_free_port() -> Result<u16, String> {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|err| format!("cannot find a free port: {err}"))?;
    let port = listener
        .local_addr()
        .map_err(|err| format!("cannot resolve a free port: {err}"))?
        .port();
    drop(listener);
    Ok(port)
}

/// Read the VmRSS (kB) of a process from /proc/<pid>/status, if available.
fn process_rss_kb(pid: i32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb);
        }
    }
    None
}

/// Run the full smoke and return a sanitized report. Any load error, non-2xx
/// response, or audit mismatch returns an error so the command fails.
pub async fn run(params: &SmokeParams) -> Result<String, String> {
    let oha_bin = resolve_oha(params.oha.as_deref())?;
    let oha_version = oha_version(&oha_bin)?;

    let workdir = WorkDir::new()?;

    let (mock_upstream, mock_handle) =
        spawn_mock_upstream(params.response_bytes, params.delay_ms).await?;

    let port = if params.port == 0 {
        pick_free_port()?
    } else {
        params.port
    };
    let current_exe =
        std::env::current_exe().map_err(|err| format!("cannot locate the binary: {err}"))?;
    let (mut gateway_child, _config_path, usage_file) =
        spawn_gateway(&current_exe, &mock_upstream, port, workdir.path())?;
    let gateway_pid = gateway_child.id() as i32;

    // Wait until the gateway reports ready via its health endpoint.
    if !wait_ready(port).await {
        let _ = gateway_child.kill();
        return Err("smoke gateway never became ready".into());
    }

    let baseline_rss = process_rss_kb(gateway_pid);

    // Run oha against the gateway while sampling gateway RSS for a peak, then
    // capture the post-load high-water mark (the gateway can briefly rise right
    // after the load as SSE buffers and the audit queue drain).
    let (oha_result, mut peak_rss) =
        run_oha_and_sample(&oha_bin, port, params, gateway_pid, baseline_rss).await?;
    let settle = sample_rss_settle(gateway_pid).await;
    if let (Some(peak), Some(settle)) = (peak_rss, settle) {
        peak_rss = Some(peak.max(settle));
    } else if peak_rss.is_none() {
        peak_rss = settle;
    }
    let end_rss = process_rss_kb(gateway_pid);

    // Let the gateway's background audit writer drain, then reconcile.
    let Some((accepted, metered)) = wait_audit_count(&usage_file, oha_result.completed).await
    else {
        let _ = gateway_child.kill();
        return Err("audit records fell short of completed requests; metering was lost".into());
    };

    // Stop the gateway cleanly so the audit file is fully flushed.
    let _ = gateway_child.kill();
    let _ = gateway_child.wait();
    drop(mock_handle);

    // Reconcile: accepted must equal completed, and every accepted must be metered.
    if accepted != oha_result.completed {
        return Err(format!(
            "audit accepted={accepted} != oha completed={} (mismatch)",
            oha_result.completed
        ));
    }
    if metered != accepted {
        return Err(format!(
            "audit metered={metered} != accepted={accepted} (unexpected metering loss)"
        ));
    }

    Ok(render_report(
        &oha_bin,
        &oha_version,
        params,
        &oha_result,
        baseline_rss,
        peak_rss,
        end_rss,
        accepted,
        metered,
    ))
}

/// Wait for the gateway health endpoint to return 200.
async fn wait_ready(port: u16) -> bool {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/healthz"))
            .send()
            .await
        {
            if response.status() == StatusCode::OK {
                return true;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The reference metrics parsed from oha's JSON output.
#[derive(Debug, Clone, Copy)]
struct OhaResult {
    completed: u64,
    errors: u64,
    rps: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    all_2xx: bool,
}

/// Run oha and sample the gateway's RSS for a peak while it is under load.
/// oha is a blocking subprocess, so it runs on a blocking thread while a tokio
/// task samples the gateway RSS concurrently.
async fn run_oha_and_sample(
    oha_bin: &Path,
    port: u16,
    params: &SmokeParams,
    gateway_pid: i32,
    baseline_rss: Option<u64>,
) -> Result<(OhaResult, Option<u64>), String> {
    let oha_bin = oha_bin.to_path_buf();
    let count = params.count;
    let concurrency = params.concurrency;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(());
    let oha_task =
        tokio::task::spawn_blocking(move || run_oha_process(&oha_bin, port, count, concurrency));
    let sample_task = tokio::spawn(sample_peak_rss(gateway_pid, baseline_rss, stop_rx));

    let oha_json = oha_task
        .await
        .map_err(|err| format!("oha task failed: {err}"))??;
    // Stop RSS sampling now that the load is complete.
    drop(stop_tx);
    let peak_rss = sample_task
        .await
        .map_err(|err| format!("rss sampling task failed: {err}"))?;

    let result = parse_oha(&oha_json)?;

    // Fail on load errors or non-2xx responses before reporting.
    if result.errors > 0 {
        return Err(format!(
            "oha reported {} error(s); load failed",
            result.errors
        ));
    }
    if !result.all_2xx {
        return Err("oha reported non-2xx responses; load failed".into());
    }

    Ok((result, peak_rss))
}

/// Run oha as a blocking subprocess and capture its JSON output.
fn run_oha_process(
    oha_bin: &Path,
    port: u16,
    count: u64,
    concurrency: u64,
) -> Result<String, String> {
    let mut cmd = Command::new(oha_bin);
    cmd.arg("-n")
        .arg(count.to_string())
        .arg("-c")
        .arg(concurrency.to_string())
        .arg("--no-tui")
        .arg("--output-format")
        .arg("json")
        .arg("-m")
        .arg("POST")
        .arg("-H")
        .arg(format!("X-Meter-Key: {MOCK_METER_KEY}"))
        .arg("-d")
        .arg("opaque-smoke-body")
        .arg("-T")
        .arg("application/json")
        .arg(format!("http://127.0.0.1:{port}/v1/responses"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = cmd
        .output()
        .map_err(|err| format!("cannot run oha: {err}"))?;
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    if !output.status.success() {
        return Err(format!(
            "oha exited non-zero ({}): {}",
            output.status, stderr
        ));
    }
    Ok(stdout)
}

/// Poll the gateway's audit file until it records the expected accepted count,
/// so the background writer has drained before shutdown. Returns Some((accepted,
/// metered)) once the target is reached, or None on timeout.
async fn wait_audit_count(usage_file: &Path, expected: u64) -> Option<(u64, u64)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok((accepted, metered)) = count_audit(usage_file) {
            if accepted >= expected {
                return Some((accepted, metered));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Count canonical `kind=request` accepted records and how many are metered.
fn count_audit(usage_file: &Path) -> Result<(u64, u64), String> {
    let content = std::fs::read_to_string(usage_file)
        .map_err(|err| format!("cannot read audit file {}: {err}", usage_file.display()))?;
    let mut accepted = 0u64;
    let mut metered = 0u64;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if record.get("schema_version").and_then(Value::as_u64) != Some(1) {
            continue;
        }
        if record.get("kind").and_then(Value::as_str) != Some("request") {
            continue;
        }
        accepted += 1;
        if record.get("usage").map(|u| !u.is_null()).unwrap_or(false) {
            metered += 1;
        }
    }
    Ok((accepted, metered))
}

/// Parse oha's JSON output into the reference metrics and a 2xx check.
///
/// oha 1.15.0 JSON shape (verified against a loopback probe): the aggregate
/// fields live under `summary` (`successRate` is a 0..1 fraction,
/// `requestsPerSec` a number), `latencyPercentiles.p50/p95/p99` are **seconds**,
/// `statusCodeDistribution` maps status code -> count, and `errorDistribution`
/// maps error label -> count (empty on success). We report latency consistently
/// in milliseconds.
fn parse_oha(json: &str) -> Result<OhaResult, String> {
    let root: Value =
        serde_json::from_str(json).map_err(|err| format!("cannot parse oha JSON output: {err}"))?;

    // Aggregate reference values live under `summary` (oha 1.15.0 JSON).
    let summary = root.get("summary").unwrap_or(&root);
    let rps = get_f64(summary, "requestsPerSec").unwrap_or(0.0);

    let latency = root.get("latencyPercentiles").unwrap_or(&Value::Null);
    let secs = |key: &str| get_ms(latency, key);
    let p50_ms = secs("p50");
    let p95_ms = secs("p95");
    let p99_ms = secs("p99");

    // Request accounting comes from the status/error distributions (the source
    // of truth for success and failure), not an aggregate that varies by output
    // mode: completed = 2xx responses, errors = non-2xx + transport errors.
    let mut completed = 0u64;
    let mut errors = 0u64;
    if let Some(dist) = root
        .get("statusCodeDistribution")
        .and_then(Value::as_object)
    {
        for (key, value) in dist {
            let code: u16 = key.parse().unwrap_or(0);
            let count = value.as_u64().unwrap_or(0);
            if (200..300).contains(&code) {
                completed += count;
            } else {
                errors += count;
            }
        }
    }
    if let Some(errs) = root.get("errorDistribution") {
        if let Some(map) = errs.as_object() {
            for value in map.values() {
                errors += value.as_u64().unwrap_or(0);
            }
        }
    }

    // successRate (fraction 0..1) at top level or under summary; a successful
    // load reports exactly 1.0, so anything below that is a failure.
    let success_rate = success_rate(&root);
    let all_2xx = errors == 0
        && success_rate
            .map(|fraction| (fraction - 1.0).abs() < 1e-9)
            .unwrap_or(false);

    Ok(OhaResult {
        completed,
        errors,
        rps,
        p50_ms,
        p95_ms,
        p99_ms,
        all_2xx,
    })
}

fn get_f64(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64())
}

/// Latency percentiles in oha are reported in seconds; convert to milliseconds
/// for the documented report.
fn get_ms(v: &Value, key: &str) -> f64 {
    match v.get(key) {
        Some(x) => x.as_f64().unwrap_or(0.0) * 1000.0,
        None => 0.0,
    }
}

/// successRate may be a top-level or summary value: a 0..1 fraction (oha 1.15.0)
/// or a "100.00%" string. Returns the fraction.
fn success_rate(root: &Value) -> Option<f64> {
    for candidate in [root, root.get("summary").unwrap_or(&Value::Null)] {
        if let Some(value) = candidate.get("successRate") {
            if let Some(num) = value.as_f64() {
                return Some(num);
            }
            if let Some(s) = value.as_str() {
                if let Ok(num) = s.trim_end_matches('%').parse::<f64>() {
                    return Some(num / 100.0);
                }
            }
        }
    }
    None
}

/// Sample the gateway's RSS continuously until `stop` is dropped (i.e. the load
/// has finished), returning the high-water mark.
async fn sample_peak_rss(
    gateway_pid: i32,
    baseline_rss: Option<u64>,
    mut stop: tokio::sync::watch::Receiver<()>,
) -> Option<u64> {
    let mut peak = baseline_rss.unwrap_or(0);
    loop {
        if let Some(rss) = process_rss_kb(gateway_pid) {
            peak = peak.max(rss);
        }
        // Block until a new value is sent or the sender is dropped (load done);
        // an Err means the sender is gone, so stop sampling.
        if stop.changed().await.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if peak == 0 {
        None
    } else {
        Some(peak)
    }
}

/// Sample the gateway's RSS for a short settle window after the load, so the
/// high-water mark that can appear as SSE buffers and the audit queue drain is
/// captured. Returns the maximum seen, or None if no reading was possible.
async fn sample_rss_settle(gateway_pid: i32) -> Option<u64> {
    let mut peak = 0u64;
    let start = tokio::time::Instant::now();
    while tokio::time::Instant::now().duration_since(start) < Duration::from_millis(300) {
        if let Some(rss) = process_rss_kb(gateway_pid) {
            peak = peak.max(rss);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if peak == 0 {
        None
    } else {
        Some(peak)
    }
}

/// Get the oha version string from `oha --version`.
fn oha_version(oha_bin: &Path) -> Result<String, String> {
    let output = Command::new(oha_bin)
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| format!("cannot run oha --version: {err}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let combined = format!("{stdout}{stderr}");
    let version = combined.split_whitespace().nth(1).unwrap_or("unknown");
    Ok(version.to_string())
}

/// Render the sanitized aggregate report. Only the documented aggregate fields
/// are printed; never raw records, bodies, keys, or exact token totals.
#[allow(clippy::too_many_arguments)]
fn render_report(
    oha_bin: &Path,
    oha_version: &str,
    params: &SmokeParams,
    result: &OhaResult,
    baseline_rss: Option<u64>,
    peak_rss: Option<u64>,
    end_rss: Option<u64>,
    accepted: u64,
    metered: u64,
) -> String {
    let kb = |v: Option<u64>| v.map(|x| x.to_string()).unwrap_or_else(|| "-".into());
    format!(
        "debitmetre smoke: {}: oha={} (bin: {}) count={} concurrency={} response_bytes={} delay_ms={}\n\
         debitmetre smoke: completed={} success={} errors={}\n\
         debitmetre smoke: {}: rps={:.2} p50={:.2}ms p95={:.2}ms p99={:.2}ms\n\
         debitmetre smoke: {}: baseline={}kB peak={}kB end={}kB\n\
         debitmetre smoke: {}: accepted={} metered={}\n\
         debitmetre smoke: PASS",
        REPORT_OHALINE,
        oha_version,
        oha_bin.display(),
        params.count,
        params.concurrency,
        params.response_bytes,
        params.delay_ms,
        result.completed,
        result.completed,
        result.errors,
        REPORT_RPS,
        result.rps,
        result.p50_ms,
        result.p95_ms,
        result.p99_ms,
        REPORT_RSS,
        kb(baseline_rss),
        kb(peak_rss),
        kb(end_rss),
        REPORT_AUDIT,
        accepted,
        metered,
    )
}
