//! CLI-level test (process seam) for the opt-in `debitmetre smoke` load-smoke
//! command (issue #25). The command exists only when built with the
//! `test-upstream-override` feature, because it needs that test-only seam to
//! point the real release gateway at a loopback mock upstream.
//!
//! These tests exercise the public command seam without needing the external
//! `oha` load generator (only parameter parsing and help documentation), so
//! they are hermetic and run under normal `cargo test --all-features`.
#![cfg(feature = "test-upstream-override")]

use std::process::{Command, ExitStatus, Stdio};

fn run_smoke_help() -> (ExitStatus, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_debitmetre"))
        .arg("smoke")
        .arg("--help")
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

fn run_smoke(args: &[&str]) -> (ExitStatus, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_debitmetre"))
        .arg("smoke")
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

/// The documented smoke command is reachable and self-describing through
/// `debitmetre smoke --help` (red evidence: before this issue no such command
/// or documentation existed).
#[test]
fn smoke_command_is_documented_by_help() {
    let (status, stdout, _stderr) = run_smoke_help();
    assert!(status.success(), "smoke --help exits zero");
    assert!(
        stdout.contains("debitmetre") && stdout.contains("smoke"),
        "help documents the smoke command, got: {stdout}"
    );
    assert!(
        stdout.contains("--count") && stdout.contains("--concurrency"),
        "help documents configurable workload, got: {stdout}"
    );
    assert!(
        stdout.contains("--oha"),
        "help documents the oha path option, got: {stdout}"
    );
}

/// Workload and sizing parameters reject non-positive or non-numeric values
/// before any traffic or subprocess is started.
#[test]
fn invalid_workload_parameters_are_rejected() {
    for (label, args) in [
        ("zero count", &["--count", "0"][..]),
        ("negative count", &["--count", "-5"]),
        ("fraction count", &["--count", "1.5"]),
        ("garbage count", &["--count", "abc"]),
        ("zero concurrency", &["--concurrency", "0"]),
        ("fraction concurrency", &["--concurrency", "2.5"]),
        ("zero response bytes", &["--response-bytes", "0"]),
        ("negative delay", &["--delay-ms", "-1"]),
        ("garbage delay", &["--delay-ms", "zz"]),
    ] {
        let (status, _stdout, stderr) = run_smoke(args);
        assert!(!status.success(), "{label}: must fail before running");
        assert!(
            stderr.contains("smoke"),
            "{label}: error names the smoke command, got: {stderr}"
        );
    }
}

/// An explicitly wrong oha path is rejected with a useful error.
#[test]
fn missing_oha_binary_is_rejected() {
    let (status, _stdout, stderr) = run_smoke(&["--oha", "/nonexistent/oha"]);
    assert!(!status.success(), "missing oha must fail");
    assert!(
        stderr.contains("oha"),
        "error names the missing oha binary, got: {stderr}"
    );
}

/// Locate an oha binary for the end-to-end behavior test: the harness accepts
/// `OHA_BIN` or finds `oha` in PATH, mirroring the command itself. Returns None
/// (the test is skipped) when oha is not installed, so CI without oha passes.
fn find_oha() -> Option<std::path::PathBuf> {
    if let Ok(path) = std::env::var("OHA_BIN") {
        let path = std::path::PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join("oha"))
            .find(|candidate| candidate.is_file())
    })
}

/// End-to-end behavior test: the full smoke run drives external oha through the
/// real gateway into the loopback mock upstream, reconciles the canonical audit
/// records, and prints a sanitized report with all documented aggregate fields.
/// Skipped when oha is not installed.
#[test]
fn smoke_run_reconciles_audit_and_prints_sanitized_report() {
    let Some(oha) = find_oha() else {
        eprintln!("skipping smoke end-to-end: oha not found (set OHA_BIN)");
        return;
    };

    let output = Command::new(env!("CARGO_BIN_EXE_debitmetre"))
        .arg("smoke")
        .arg("--count")
        .arg("20")
        .arg("--concurrency")
        .arg("4")
        .arg("--response-bytes")
        .arg("1024")
        .arg("--delay-ms")
        .arg("1")
        .arg("--port")
        .arg("0")
        .arg("--oha")
        .arg(&oha)
        .env("OHA_BIN", &oha)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run the debitmetre binary");
    let status = output.status;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    assert!(
        status.success(),
        "smoke must exit zero; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    // The sanitized report carries every documented aggregate field.
    assert!(
        stdout.contains("oha="),
        "reports oha version, got: {stdout}"
    );
    assert!(
        stdout.contains("count=") && stdout.contains("concurrency="),
        "reports workload, got: {stdout}"
    );
    assert!(
        stdout.contains("completed="),
        "reports completed, got: {stdout}"
    );
    assert!(
        stdout.contains("rps="),
        "reports reference rps, got: {stdout}"
    );
    assert!(
        stdout.contains("p50=") && stdout.contains("p95=") && stdout.contains("p99="),
        "reports latency percentiles, got: {stdout}"
    );
    assert!(
        stdout.contains("baseline=") && stdout.contains("peak=") && stdout.contains("end="),
        "reports gateway RSS, got: {stdout}"
    );
    assert!(
        stdout.contains("accepted=") && stdout.contains("metered="),
        "reports audit accepted/metered, got: {stdout}"
    );
    assert!(stdout.contains("PASS"), "reports PASS, got: {stdout}");

    // No sensitive material appears in the report.
    assert!(
        !stdout.contains("machine-smoke") && !stdout.contains("X-Meter-Key"),
        "report is sanitized, got: {stdout}"
    );
    assert!(
        !stderr.contains("machine-smoke"),
        "stderr is sanitized, got: {stderr}"
    );
    // The report must carry the oha version but never the executable path: the
    // exact path supplied on the command line must not appear anywhere.
    let oha_path = oha.to_str().expect("oha path is utf-8");
    assert!(
        !stdout.contains(oha_path),
        "report must not expose the oha executable path, got: {stdout}"
    );
}

/// Child-process cleanup is exception-safe: a deliberately failing oha must make
/// the smoke command fail, yet the gateway subprocess it spawned must still be
/// terminated and reaped so its loopback port is released and reusable after the
/// command exits. This exercises process cleanup, not the load generator.
#[cfg(unix)]
#[test]
fn failing_oha_still_releases_the_gateway_port() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let fake_oha = dir.path().join("fake-oha");
    std::fs::write(&fake_oha, "#!/bin/sh\nexit 1\n").expect("write fake oha");
    std::fs::set_permissions(&fake_oha, std::fs::Permissions::from_mode(0o755))
        .expect("make fake oha executable");

    // Pick an explicit free loopback port for the smoke gateway.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
        let port = listener.local_addr().expect("probe addr").port();
        drop(listener);
        port
    };

    let output = Command::new(env!("CARGO_BIN_EXE_debitmetre"))
        .arg("smoke")
        .arg("--count")
        .arg("5")
        .arg("--concurrency")
        .arg("2")
        .arg("--port")
        .arg(port.to_string())
        .arg("--oha")
        .arg(&fake_oha)
        .env("OHA_BIN", &fake_oha)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run the debitmetre binary");
    assert!(
        !output.status.success(),
        "failing oha must make the smoke command fail"
    );

    // After the command exits, the gateway must be gone and its port reusable.
    let rebound = std::net::TcpListener::bind(("127.0.0.1", port)).is_ok();
    assert!(
        rebound,
        "gateway loopback port must be released after the smoke command exits"
    );
}

/// A fake oha that returns syntactically valid oha JSON but performs zero
/// requests (successRate 1.0, empty status/error distributions) must fail the
/// smoke command: it must never reach PASS with zero audit records, and the
/// failure must be prompt (validated before waiting on the audit file, not after
/// an audit-drain timeout).
#[cfg(unix)]
#[test]
fn fake_oha_reporting_zero_requests_fails_promptly() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::Instant;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let fake_oha = dir.path().join("fake-oha");
    std::fs::write(
        &fake_oha,
        "#!/bin/sh\n\
         if [ \"$1\" = \"--version\" ]; then\n\
         \x20 echo \"oha 9.9.9\"\n\
         \x20 exit 0\n\
         fi\n\
         cat <<'JSON'\n\
         {\"summary\":{\"successRate\":1.0,\"requestsPerSec\":0.0},\"latencyPercentiles\":{\"p50\":0.0,\"p95\":0.0,\"p99\":0.0},\"statusCodeDistribution\":{},\"errorDistribution\":{}}\n\
         JSON\n\
         exit 0\n",
    )
    .expect("write fake oha");
    std::fs::set_permissions(&fake_oha, std::fs::Permissions::from_mode(0o755))
        .expect("make fake oha executable");

    let start = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_debitmetre"))
        .arg("smoke")
        .arg("--count")
        .arg("5")
        .arg("--concurrency")
        .arg("2")
        .arg("--port")
        .arg("0")
        .arg("--oha")
        .arg(&fake_oha)
        .env("OHA_BIN", &fake_oha)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run the debitmetre binary");
    let elapsed = start.elapsed();

    // A load generator that accounted for zero of the requested requests must
    // not reach PASS.
    assert!(
        !output.status.success(),
        "zero-request oha must make the smoke command fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        stderr.contains("accounting") && stderr.contains("mismatch"),
        "failure names the load accounting mismatch, got: {stderr}"
    );
    // Validated before the audit wait, so it must fail promptly and never spend
    // the audit-drain timeout waiting for records that will never arrive.
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "must fail promptly, took {elapsed:?}"
    );
}
