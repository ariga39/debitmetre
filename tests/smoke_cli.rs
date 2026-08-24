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
}
