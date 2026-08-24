//! CLI-level test (process seam) for the local `debitmetre summary` command
//! (issue #3): runs the built binary against a synthetic config and a small
//! synthetic usage file, and compares the command output with a literal
//! expectation whose grouped totals are independently calculated by hand.

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

/// A fully valid canonical record whose `usage` is genuinely `null` (e.g. a
/// transport error): it carries no token facts and must not create a group.
fn null_usage_record() -> String {
    r#"{"schema_version":1,"kind":"request","event_id":"evt-null-usage","timestamp":"2026-08-23T10:00:00Z","machine_id":"machine-c","operation":"response","upstream_status":200,"outcome":"completed","model":"model-m1","accounting_quality":"complete","metering_error":null,"usage":null}"#
        .to_string()
}

/// The base fixture: four complete newline-terminated records plus a null-usage
/// record. The grouped totals are the hand-calculated constants documented on
/// [`EXPECTED_BASE_STDOUT`].
fn base_fixture() -> String {
    [
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
        null_usage_record(),
    ]
    .join("\n")
        + "\n"
}

/// Complete expected stdout for the base fixture, as a literal — not a
/// reconstruction of the renderer. The token totals are independently
/// hand-calculated from the fixture:
///
/// - machine-a / model-m1 (2 records):
///   input = 100+200 = 300, uncached = 60+150 = 210, cache_read = 20+30 = 50,
///   cache_write = 20+20 = 40, output = 50+80 = 130, reasoning = 30+40 = 70,
///   total = 150+280 = 430
/// - machine-a / model-m2 (1 record; uncached and reasoning were never recorded
///   and must show as `-`, never as 0):
///   input = 40, cache_read = 10, cache_write = 10, output = 20, total = 60
/// - machine-b / model-m1 (1 record): input = 10, uncached = 5, cache_read = 3,
///   cache_write = 2, output = 6, reasoning = 2, total = 16
const EXPECTED_BASE_STDOUT: &str = r#"machine       model            records        input     uncached   cache_read  cache_write       output    reasoning        total
machine-a     model-m1               2          300          210           50           40          130           70          430
machine-a     model-m2               1           40            -           10           10           20            -           60
machine-b     model-m1               1           10            5            3            2            6            2           16
- = not recorded in any record; totals sum only recorded values
coverage: accepted=5 metered=4 unmetered=1 (80.0%)
"#;

/// Complete expected stdout when the final record (machine-b / model-m2) is
/// also summarized: input = 22, uncached = 11, cache_read = 5, cache_write = 6,
/// output = 12, reasoning = 4, total = 34, 1 record.
const EXPECTED_STDOUT_WITH_FINAL_RECORD: &str = r#"machine       model            records        input     uncached   cache_read  cache_write       output    reasoning        total
machine-a     model-m1               2          300          210           50           40          130           70          430
machine-a     model-m2               1           40            -           10           10           20            -           60
machine-b     model-m1               1           10            5            3            2            6            2           16
machine-b     model-m2               1           22           11            5            6           12            4           34
- = not recorded in any record; totals sum only recorded values
coverage: accepted=6 metered=5 unmetered=1 (83.3%)
"#;

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

/// Two serde-deserializable records that are NOT valid canonical `kind=request`
/// records: one carries a wrong `schema_version`, one a wrong `kind`. Their
/// usage is conspicuous so any accidental inclusion would be plainly visible in
/// the literal expected output. Neither may enter the coverage counts or any
/// token group (issue #23 spec blocker).
fn non_canonical_records() -> String {
    [
        r#"{"schema_version":2,"kind":"request","event_id":"evt-bad-version","timestamp":"2026-08-23T10:00:00Z","machine_id":"machine-bad","operation":"response","upstream_status":200,"outcome":"completed","model":"model-bad","accounting_quality":"complete","metering_error":null,"usage":{"input_total":9999,"uncached":0,"cache_read":0,"cache_write":0,"output_total":999,"reasoning":0,"total":10998}}"#,
        r#"{"schema_version":1,"kind":"meter_snapshot","event_id":"evt-bad-kind","timestamp":"2026-08-23T10:00:00Z","machine_id":"machine-bad","operation":"response","upstream_status":200,"outcome":"completed","model":"model-bad","accounting_quality":"complete","metering_error":null,"usage":{"input_total":8888,"uncached":0,"cache_read":0,"cache_write":0,"output_total":888,"reasoning":0,"total":9776}}"#,
    ]
    .join("\n")
}

/// The base fixture (see [`base_fixture`]) contains 5 valid canonical
/// `kind=request` lifecycles: 4 carry a non-null `usage` object (evt-a1, evt-a2,
/// evt-a3, evt-b1, including the partial evt-a3) and 1 carries `usage: null`
/// (evt-null-usage). So accepted=5, metered=4, unmetered=1, coverage=80.0%.
/// These counts are independently known from the fixture itself, not derived
/// from the renderer. `EXPECTED_BASE_STDOUT` includes the coverage line.
#[test]
fn non_canonical_records_do_not_enter_coverage_or_token_totals() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let usage_file = dir.path().join("usage.jsonl");
    // Base fixture plus two wrong-version/wrong-kind records with conspicuous
    // usage. If either slipped through, the counts, coverage, and token rows
    // would all deviate from the literal expected output below.
    let mut contents = base_fixture();
    contents.push_str(&non_canonical_records());
    contents.push('\n');
    std::fs::write(&usage_file, contents).expect("write synthetic usage file");
    let config_path = write_config(&dir, &usage_file);

    let (status, stdout, stderr) = run_summary(&config_path);
    assert!(status.success(), "summary exits zero, stderr: {stderr}");
    // The per-machine/per-model token rows and the coverage line are exactly
    // the base fixture's literal output: both non-canonical records are absent
    // from accepted/metered/unmetered counts and from every token group.
    assert_eq!(stdout, EXPECTED_BASE_STDOUT);
    assert!(
        !stdout.contains("machine-bad"),
        "non-canonical records create no token group"
    );
    assert!(
        !stderr.contains("unfinished trailing line"),
        "no coverage warnings expected, got: {stderr}"
    );
}

#[test]
fn summary_prints_independently_calculated_grouped_totals() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let usage_file = dir.path().join("usage.jsonl");
    std::fs::write(&usage_file, base_fixture()).expect("write synthetic usage file");
    let config_path = write_config(&dir, &usage_file);

    let (status, stdout, stderr) = run_summary(&config_path);
    assert!(status.success(), "summary exits zero, stderr: {stderr}");
    assert_eq!(stdout, EXPECTED_BASE_STDOUT);
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
    // A crash can leave the file ending mid-record without a final newline;
    // this tail is genuinely incomplete (not parseable JSON).
    let tail = "{\"schema_version\":1,\"kind\":\"request\",\"event_id\":\"evt-crash\",\"timestamp\":\"2026-08-23T10:05:00Z\",\"machine_id\":\"machine-b\",\"operation\":\"response\",\"upstream_status\":200,\"outcome\":\"completed\",\"model\":\"model-m1\",\"accounting_quality\":\"complete\",\"metering_error\":null,\"usage\":{\"input_total\":9999,\"uncached\":";
    let mut contents = base_fixture();
    contents.push_str(tail);
    std::fs::write(&usage_file, contents).expect("write synthetic usage file");
    let config_path = write_config(&dir, &usage_file);

    let (status, stdout, stderr) = run_summary(&config_path);
    assert!(status.success(), "summary exits zero, stderr: {stderr}");
    // The trailing partial write is ignored; every earlier complete record is
    // still summarized with exactly the independent totals.
    assert_eq!(stdout, EXPECTED_BASE_STDOUT);
    assert!(
        stderr.contains("unfinished trailing line"),
        "understandable warning, got: {stderr}"
    );
}

#[test]
fn complete_final_record_without_newline_is_still_summarized() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let usage_file = dir.path().join("usage.jsonl");
    // The last line is a fully valid canonical record whose terminating newline
    // was lost; it must be summarized, not mistaken for a crash partial write.
    let final_record = r#"{"schema_version":1,"kind":"request","event_id":"evt-b2","timestamp":"2026-08-23T10:00:00Z","machine_id":"machine-b","operation":"response","upstream_status":200,"outcome":"completed","model":"model-m2","accounting_quality":"complete","metering_error":null,"usage":{"input_total":22,"uncached":11,"cache_read":5,"cache_write":6,"output_total":12,"reasoning":4,"total":34}}"#;
    let mut contents = base_fixture();
    contents.push_str(final_record);
    std::fs::write(&usage_file, contents).expect("write synthetic usage file");
    let config_path = write_config(&dir, &usage_file);

    let (status, stdout, stderr) = run_summary(&config_path);
    assert!(status.success(), "summary exits zero, stderr: {stderr}");
    assert_eq!(stdout, EXPECTED_STDOUT_WITH_FINAL_RECORD);
    assert!(
        !stderr.contains("unfinished trailing line"),
        "a complete final record is not an unfinished tail, got: {stderr}"
    );
    assert!(
        !stderr.contains("unparseable"),
        "a complete final record is not skipped, got: {stderr}"
    );
}
