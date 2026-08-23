//! CLI-level test (process seam) for the local `debitmetre summary` command
//! (issue #3): runs the built binary against a synthetic config and a small
//! synthetic usage file whose grouped totals are independently calculated by
//! hand, then compares the command output with those known results.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

/// SHA-256 digest of the synthetic meter key, same value as the config example;
/// the summary command only needs a valid gateway config to learn `usage_file`.
const TEST_METER_KEY_DIGEST: &str =
    "82805ec33616c4aa802f141d3703fb17213fd8ced358f3a62348d8cf6e1ce051";

fn write_config(dir: &tempfile::TempDir, usage_file: &Path) -> PathBuf {
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        format!(
            "listen = \"127.0.0.1:8787\"\nusage_file = \"{}\"\n\n[machine_keys]\n\"{TEST_METER_KEY_DIGEST}\" = \"machine-a\"\n",
            usage_file.display()
        ),
    )
    .expect("write synthetic config");
    path
}

/// One canonical request audit line (DESIGN.md §5). `uncached`/`reasoning`
/// accept "null" to exercise the missing-value path.
#[allow(clippy::too_many_arguments)]
fn record(
    event_id: &str,
    machine_id: &str,
    model: &str,
    input: &str,
    uncached: &str,
    cache_read: &str,
    cache_write: &str,
    output: &str,
    reasoning: &str,
    total: &str,
) -> String {
    format!(
        "{{\"schema_version\":1,\"kind\":\"request\",\"event_id\":\"{event_id}\",\"timestamp\":\"2026-08-23T10:00:00Z\",\"machine_id\":\"{machine_id}\",\"operation\":\"response\",\"upstream_status\":200,\"outcome\":\"completed\",\"model\":\"{model}\",\"accounting_quality\":\"complete\",\"metering_error\":null,\"usage\":{{\"input_total\":{input},\"uncached\":{uncached},\"cache_read\":{cache_read},\"cache_write\":{cache_write},\"output_total\":{output},\"reasoning\":{reasoning},\"total\":{total}}}}}"
    )
}

/// Independently known grouped totals, hand-calculated from the fixture above:
///
/// - machine-a / model-m1: 2 records
///   input = 100+200 = 300, uncached = 60+150 = 210, cache_read = 20+30 = 50,
///   cache_write = 20+20 = 40, output = 50+80 = 130, reasoning = 30+40 = 70,
///   total = 150+280 = 430
/// - machine-a / model-m2: 1 record; uncached and reasoning were never
///   recorded, so they must show as missing (`-`), never 0:
///   input = 40, cache_read = 10, cache_write = 10, output = 20, total = 60
/// - machine-b / model-m1: 1 record: input = 10, uncached = 5, cache_read = 3,
///   cache_write = 2, output = 6, reasoning = 2, total = 16
///
/// `tail` optionally appends an unfinished trailing line (no newline) the way a
/// process crash can leave the file (DESIGN.md §7).
fn usage_jsonl(tail: Option<&str>) -> String {
    let mut body = [
        record(
            "evt-a1",
            "machine-a",
            "model-m1",
            "100",
            "60",
            "20",
            "20",
            "50",
            "30",
            "150",
        ),
        record(
            "evt-a2",
            "machine-a",
            "model-m1",
            "200",
            "150",
            "30",
            "20",
            "80",
            "40",
            "280",
        ),
        record(
            "evt-a3",
            "machine-a",
            "model-m2",
            "40",
            "null",
            "10",
            "10",
            "20",
            "null",
            "60",
        ),
        record(
            "evt-b1",
            "machine-b",
            "model-m1",
            "10",
            "5",
            "3",
            "2",
            "6",
            "2",
            "16",
        ),
        record(
            "evt-null-usage",
            "machine-c",
            "model-m1",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
        )
        .replace("\"usage\":{\"input_total\":0", "\"usage\":null"),
    ]
    .join("\n")
        + "\n";
    if let Some(tail) = tail {
        body.push_str(tail);
    }
    body
}

/// Replicates the command's fixed-width table formatting so the test can assert
/// the exact stdout against the hand-calculated totals without counting spaces.
fn expected_row(machine: &str, model: &str, records: u64, counters: [&str; 7]) -> String {
    format!(
        "{machine:<14}{model:<16}{records:>8} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        counters[0], counters[1], counters[2], counters[3], counters[4], counters[5], counters[6],
    )
}

fn expected_stdout() -> String {
    let header = format!(
        "{:<14}{:<16}{:>8} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "machine",
        "model",
        "records",
        "input",
        "uncached",
        "cache_read",
        "cache_write",
        "output",
        "reasoning",
        "total"
    );
    let rows = [
        header,
        expected_row(
            "machine-a",
            "model-m1",
            2,
            ["300", "210", "50", "40", "130", "70", "430"],
        ),
        expected_row(
            "machine-a",
            "model-m2",
            1,
            ["40", "-", "10", "10", "20", "-", "60"],
        ),
        expected_row(
            "machine-b",
            "model-m1",
            1,
            ["10", "5", "3", "2", "6", "2", "16"],
        ),
        "- = not recorded in any record; totals sum only recorded values".to_string(),
    ];
    rows.join("\n") + "\n"
}

fn run_summary(config_path: &Path) -> (ExitStatus, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_debitmetre"))
        .arg("summary")
        .arg("--config")
        .arg(config_path)
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
fn summary_prints_independently_calculated_grouped_totals() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let usage_file = dir.path().join("usage.jsonl");
    std::fs::write(&usage_file, usage_jsonl(None)).expect("write synthetic usage file");
    let config_path = write_config(&dir, &usage_file);

    let (status, stdout, stderr) = run_summary(&config_path);
    assert!(status.success(), "summary exits zero, stderr: {stderr}");
    assert_eq!(stdout, expected_stdout());
    // A record with usage=null is not a token fact: machine-c must not appear,
    // and the command never prints prices.
    assert!(
        !stdout.contains("machine-c"),
        "null-usage records contribute nothing"
    );
    assert!(!stdout.contains('$'), "no prices are invented");
}

#[test]
fn unfinished_trailing_line_is_ignored_with_a_clear_warning() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let usage_file = dir.path().join("usage.jsonl");
    // A crash can leave the file ending mid-record without a final newline.
    let tail = "{\"schema_version\":1,\"kind\":\"request\",\"event_id\":\"evt-crash\",\"timestamp\":\"2026-08-23T10:05:00Z\",\"machine_id\":\"machine-b\",\"operation\":\"response\",\"upstream_status\":200,\"outcome\":\"completed\",\"model\":\"model-m1\",\"accounting_quality\":\"complete\",\"metering_error\":null,\"usage\":{\"input_total\":9999,\"uncached\":";
    std::fs::write(&usage_file, usage_jsonl(Some(tail))).expect("write synthetic usage file");
    let config_path = write_config(&dir, &usage_file);

    let (status, stdout, stderr) = run_summary(&config_path);
    assert!(status.success(), "summary exits zero, stderr: {stderr}");
    // The trailing partial write is ignored; every earlier complete record is
    // still summarized with exactly the independent totals.
    assert_eq!(stdout, expected_stdout());
    assert!(
        stderr.contains("unfinished trailing line"),
        "understandable warning, got: {stderr}"
    );
}
