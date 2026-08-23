//! Process-level test of the built `debitmetre` binary (startup seam).
//!
//! Requires the `test-upstream-override` feature so the binary points its fixed
//! upstream at a fake upstream started in this test; see DESIGN.md §8 (the fake
//! upstream is a test adapter, not a product seam) and §10 (startup seam).
#![cfg(feature = "test-upstream-override")]

use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;

/// SHA-256 digest of `test-meter-key-machine-a`, precomputed with sha256sum and
/// OpenSSL `dgst -sha256` (both agree); the config stores the digest, not the key.
const TEST_METER_KEY_DIGEST: &str =
    "82805ec33616c4aa802f141d3703fb17213fd8ced358f3a62348d8cf6e1ce051";
const TEST_METER_KEY: &str = "test-meter-key-machine-a";
const REQUEST_BODY: &str = "opaque-request-body-42";
const UPSTREAM_BODY: &str = "opaque-upstream-body-01";

async fn fake_upstream_handler(_req: Request<Body>) -> Response {
    (StatusCode::CREATED, UPSTREAM_BODY).into_response()
}

async fn spawn_fake_upstream() -> String {
    let app = Router::new()
        .route("/responses", post(fake_upstream_handler))
        .route("/responses/compact", post(fake_upstream_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake upstream");
    let addr = listener.local_addr().expect("fake upstream address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("fake upstream runs");
    });
    format!("http://{addr}")
}

fn write_config(dir: &tempfile::TempDir) -> std::path::PathBuf {
    write_config_with_usage(dir, dir.path().join("usage.jsonl"))
}

fn write_config_with_usage(
    dir: &tempfile::TempDir,
    usage_file: std::path::PathBuf,
) -> std::path::PathBuf {
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        format!(
            "listen = \"127.0.0.1:0\"\nusage_file = \"{}\"\n\n[machine_keys]\n\"{TEST_METER_KEY_DIGEST}\" = \"machine-a\"\n",
            usage_file.display()
        ),
    )
    .expect("write synthetic config");
    path
}

/// A running gateway plus its stderr stream, drained continuously so the pipe
/// never fills and the test can inspect the operational logs live. Dropping the
/// struct kills the child so a panicking test never leaves an orphan process.
struct GatewayProc {
    child: Option<Child>,
    logs: Arc<Mutex<Vec<u8>>>,
    reader: Option<std::thread::JoinHandle<()>>,
}

impl GatewayProc {
    fn spawn(config_path: &Path, upstream: Option<&str>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_debitmetre"));
        command
            .arg("--config")
            .arg(config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(base) = upstream {
            command.env("DEBITMETRE_TEST_UPSTREAM", base);
        }
        let mut child = command.spawn().expect("spawn the debitmetre binary");
        let mut stderr = child.stderr.take().expect("piped stderr");
        let logs = Arc::new(Mutex::new(Vec::new()));
        let drain = logs.clone();
        let reader = std::thread::spawn(move || {
            let mut byte = [0u8; 1024];
            loop {
                match stderr.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => drain.lock().unwrap().extend_from_slice(&byte[..n]),
                }
            }
        });
        Self {
            child: Some(child),
            logs,
            reader: Some(reader),
        }
    }

    fn logs_text(&self) -> String {
        String::from_utf8_lossy(&self.logs.lock().unwrap()).into_owned()
    }

    /// Waits for a line containing `gateway listening` and parses the actual
    /// bound port from its `listen=127.0.0.1:PORT` field.
    fn bound_port(&self) -> u16 {
        let marker = "listen=127.0.0.1:";
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let text = self.logs_text();
            if let Some(pos) = text.find(marker) {
                let rest = &text[pos + marker.len()..];
                let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                if let Ok(port) = digits.parse() {
                    return port;
                }
            }
            assert!(
                Instant::now() < deadline,
                "startup log never appeared; logs so far:\n{text}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_logs(&self, needles: &[&str]) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let text = self.logs_text();
            if needles.iter().all(|needle| text.contains(needle)) {
                return text;
            }
            assert!(
                Instant::now() < deadline,
                "missing log needle(s) {needles:?}; logs so far:\n{text}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Send SIGTERM and wait for the process to drain and exit, then wait for
    /// the stderr reader to reach EOF so no final log line is missed.
    fn stop_gracefully(&mut self) -> ExitStatus {
        let mut child = self.child.take().expect("process still running");
        // SAFETY: child.id() is a valid pid of a child of this process and
        // SIGTERM is exactly the signal we want to test graceful shutdown.
        unsafe {
            libc::kill(child.id() as i32, libc::SIGTERM);
        }
        let status = child.wait().expect("reap the gateway process");
        self.reader
            .take()
            .expect("reader still present")
            .join()
            .expect("stderr reader reaches EOF");
        status
    }
}

impl Drop for GatewayProc {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

async fn wait_ready(port: u16) {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/healthz"))
            .send()
            .await
        {
            if response.status() == StatusCode::OK {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "gateway never became ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn runtime_audit_write_failure_is_fail_open_and_sanitized() {
    let upstream = spawn_fake_upstream().await;
    let dir = tempfile::TempDir::new().expect("temp dir");
    // /dev/full opens successfully (startup passes) but every write fails with
    // ENOSPC: a transient runtime audit write failure must not corrupt the
    // caller-visible upstream response.
    let config_path = write_config_with_usage(&dir, std::path::PathBuf::from("/dev/full"));
    let gateway = GatewayProc::spawn(&config_path, Some(&upstream));

    let port = gateway.bound_port();
    wait_ready(port).await;

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let response = client
        .post(format!("{base}/v1/responses"))
        .header("x-meter-key", TEST_METER_KEY)
        .body(REQUEST_BODY)
        .send()
        .await
        .expect("caller reaches the running gateway");
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "upstream response unchanged despite the audit write failure"
    );
    assert_eq!(
        response.bytes().await.expect("read response").as_ref(),
        UPSTREAM_BODY.as_bytes(),
        "body bytes unchanged despite the audit write failure"
    );

    let logs = gateway.wait_for_logs(&["audit_write_failed"]);
    assert!(
        !logs.contains(TEST_METER_KEY),
        "sanitized log must not leak the meter key"
    );
    assert!(
        !logs.contains(REQUEST_BODY),
        "sanitized log must not leak request bodies"
    );
    assert!(
        !logs.contains(UPSTREAM_BODY),
        "sanitized log must not leak response bodies"
    );
}

#[tokio::test]
async fn built_binary_becomes_ready_and_serves_a_representative_request_lifecycle() {
    let upstream = spawn_fake_upstream().await;
    let dir = tempfile::TempDir::new().expect("temp dir");
    let config_path = write_config(&dir);
    let mut gateway = GatewayProc::spawn(&config_path, Some(&upstream));

    let port = gateway.bound_port();
    wait_ready(port).await;

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let response = client
        .post(format!("{base}/v1/responses"))
        .header("x-meter-key", TEST_METER_KEY)
        .body(REQUEST_BODY)
        .send()
        .await
        .expect("caller reaches the running gateway");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.bytes().await.expect("read response").as_ref(),
        UPSTREAM_BODY.as_bytes(),
        "existing transparent proxy route still forwards byte-for-byte"
    );

    let response = client
        .post(format!("{base}/v1/responses"))
        .header("x-meter-key", "intruder-unknown")
        .body("opaque-request-body-43")
        .send()
        .await
        .expect("caller reaches the running gateway");
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "invalid meter key is rejected before the upstream"
    );

    // The 201 opaque body is not parseable usage: the gateway must emit one
    // sanitized metering warning (DESIGN.md §6) alongside the successful
    // transparent forwarding.
    let logs_at_requests = gateway.wait_for_logs(&[
        "gateway listening",
        "request accepted",
        "upstream response",
        "request rejected",
        "usage metering failed",
    ]);
    assert!(
        !logs_at_requests.contains(TEST_METER_KEY),
        "logs must never print the meter key"
    );
    assert!(
        !logs_at_requests.contains(REQUEST_BODY),
        "logs must never print request bodies"
    );
    assert!(
        !logs_at_requests.contains(UPSTREAM_BODY),
        "logs must never print response bodies"
    );

    let metering_line = logs_at_requests
        .lines()
        .find(|line| line.contains("usage metering failed"))
        .expect("the metering warning line is present");
    assert!(
        !metering_line.contains("machine-a"),
        "metering warning must not leak machine identity"
    );
    assert!(
        !metering_line.contains(TEST_METER_KEY),
        "metering warning must not leak the meter key"
    );
    assert!(
        !metering_line.contains(REQUEST_BODY),
        "metering warning must not leak request bodies"
    );
    assert!(
        !metering_line.contains(UPSTREAM_BODY),
        "metering warning must not leak response bodies"
    );

    let status = gateway.stop_gracefully();
    assert!(status.success(), "graceful shutdown exits zero");

    let final_logs = gateway.logs_text();
    assert!(
        final_logs.contains("shutdown signal received"),
        "shutdown produces a concise operational log"
    );
    assert!(final_logs.contains("gateway stopped"));
    assert!(!final_logs.contains(TEST_METER_KEY));
    assert!(!final_logs.contains(REQUEST_BODY));
}

fn run_binary(args: &[&str]) -> (ExitStatus, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_debitmetre"))
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run the debitmetre binary");
    (
        output.status,
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn invalid_or_unreadable_configuration_exits_fail_closed() {
    let dir = tempfile::TempDir::new().expect("temp dir");

    let missing = dir.path().join("missing.toml");
    let (status, _stdout, stderr) = run_binary(&["--config", missing.to_str().unwrap()]);
    assert!(!status.success(), "unreadable config must fail closed");
    assert!(
        stderr.contains("cannot read config"),
        "useful unreadable-config error, got: {stderr}"
    );

    let malformed = dir.path().join("malformed.toml");
    std::fs::write(&malformed, "listen = 127.0.0.1:8787\n").expect("write malformed config");
    let (status, _stdout, stderr) = run_binary(&["--config", malformed.to_str().unwrap()]);
    assert!(!status.success(), "malformed config must fail closed");
    assert!(
        stderr.contains("invalid TOML"),
        "useful parse error, got: {stderr}"
    );

    let empty = dir.path().join("empty.toml");
    std::fs::write(
        &empty,
        "listen = \"127.0.0.1:8787\"\nusage_file = \"/tmp/usage.jsonl\"\n\n[machine_keys]\n",
    )
    .expect("write empty-keys config");
    let (status, _stdout, stderr) = run_binary(&["--config", empty.to_str().unwrap()]);
    assert!(
        !status.success(),
        "config without machines must fail closed"
    );
    assert!(
        stderr.contains("machine_keys"),
        "useful validation error, got: {stderr}"
    );

    let bad_usage = dir.path().join("bad-usage.toml");
    let missing_parent = dir.path().join("no-such-dir").join("usage.jsonl");
    std::fs::write(
        &bad_usage,
        format!(
            "listen = \"127.0.0.1:8787\"\nusage_file = \"{}\"\n\n[machine_keys]\n\"{TEST_METER_KEY_DIGEST}\" = \"machine-a\"\n",
            missing_parent.display()
        ),
    )
    .expect("write unwritable-usage-file config");
    let (status, _stdout, stderr) = run_binary(&["--config", bad_usage.to_str().unwrap()]);
    assert!(
        !status.success(),
        "unwritable usage-file path must fail closed at startup"
    );
    assert!(
        stderr.contains("usage file") && stderr.contains("cannot open"),
        "useful startup error naming the usage file, got: {stderr}"
    );
}

#[test]
fn help_prints_the_documented_startup_command() {
    let (status, stdout, _stderr) = run_binary(&["--help"]);
    assert!(status.success(), "--help exits zero");
    assert!(
        stdout.contains("debitmetre") && stdout.contains("--config"),
        "help documents the run command, got: {stdout}"
    );
}
