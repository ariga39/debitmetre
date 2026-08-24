//! Local summary command (issue #3): reads the configured JSONL usage file and
//! prints accumulated recorded token facts grouped by machine and model.
//!
//! Reuse note: orihsus contains no offline reader/aggregation slice (its audits
//! are only ever written; `src/usage.rs` there is OpenCode Go quota polling),
//! and NerfTrack's collector targets the Codex client's *own* local JSONL with
//! byte-offset checkpoints, event fingerprints, and SQLite aggregation — a
//! different schema and data model whose adaptation would carry disproportionate
//! baggage. Only the generic "ignore an unfinished trailing line" idiom applies,
//! which is a few `read_until` lines below, not a port. The reader therefore
//! reuses the gateway's own canonical serde types ([`AuditRecord`], [`Usage`])
//! instead of adding a framework.

use std::collections::BTreeMap;
use std::io::{self, BufRead, Write};
use std::path::Path;

use crate::usage::{AuditRecord, Usage};

/// Aggregation key: stable machine id plus the recorded model. A record whose
/// model is missing still forms a group under a `-` model label; a model is
/// never invented.
type GroupKey = (String, Option<String>);

/// Accumulated token counters for one (machine, model) group. Each sum covers
/// only the values actually recorded: a counter never present in the group
/// stays missing and renders as `-`, never as an invented 0.
#[derive(Debug, Default)]
struct GroupTotals {
    records: u64,
    input_total: Option<u64>,
    uncached: Option<u64>,
    cache_read: Option<u64>,
    cache_write: Option<u64>,
    output_total: Option<u64>,
    reasoning: Option<u64>,
    total: Option<u64>,
}

/// Warnings collected while reading, kept off stdout so the table stays clean.
#[derive(Debug, Default)]
struct ReadWarnings {
    partial_tail: bool,
    unparseable: u64,
}

/// Overall metering coverage over accepted request lifecycles (issue #23).
/// Every valid canonical `kind=request` record is one accepted lifecycle
/// (including records whose `usage` is null); a lifecycle is metered exactly
/// when its record carries a non-null `usage` object (partial usage still
/// counts). The percentage is `metered / accepted`, defined and printed as
/// `(percentage)%`. Malformed complete lines and unfinished trailing lines are
/// warned about and never enter any of these counts.
#[derive(Debug, Clone, Copy, Default)]
struct Coverage {
    accepted: u64,
    metered: u64,
}

impl Coverage {
    fn unmetered(self) -> u64 {
        self.accepted - self.metered
    }

    /// Coverage as a percentage of accepted lifecycles, or `None` when there is
    /// no accepted lifecycle to divide by (the division is never performed on 0).
    fn percentage(self) -> Option<f64> {
        if self.accepted == 0 {
            None
        } else {
            Some(self.metered as f64 * 100.0 / self.accepted as f64)
        }
    }
}

/// Read the usage file and print the grouped summary to `out`. Warnings (a
/// skipped unfinished trailing line, unparseable records) go to stderr.
pub fn print_summary(path: &Path, out: &mut dyn Write) -> Result<(), String> {
    let (groups, coverage, warnings) = read_usage_file(path)?;
    render(out, &groups, &coverage).map_err(|err| format!("cannot write summary: {err}"))?;
    print_warnings(&warnings);
    Ok(())
}

fn read_usage_file(
    path: &Path,
) -> Result<(BTreeMap<GroupKey, GroupTotals>, Coverage, ReadWarnings), String> {
    let file = std::fs::File::open(path)
        .map_err(|err| format!("cannot read usage file {}: {err}", path.display()))?;
    let mut reader = io::BufReader::new(file);
    let mut groups: BTreeMap<GroupKey, GroupTotals> = BTreeMap::new();
    let mut coverage = Coverage::default();
    let mut warnings = ReadWarnings::default();
    let mut line: Vec<u8> = Vec::new();
    loop {
        line.clear();
        let bytes = reader
            .read_until(b'\n', &mut line)
            .map_err(|err| format!("cannot read usage file {}: {err}", path.display()))?;
        if bytes == 0 {
            break;
        }
        let terminated = line.ends_with(b"\n");
        let mut record = line.as_slice();
        if terminated {
            record = &record[..record.len() - 1];
        }
        if let Some(stripped) = record.strip_suffix(b"\r") {
            record = stripped;
        }
        match serde_json::from_slice::<AuditRecord>(record) {
            Ok(record) => {
                // Every valid canonical record is one accepted lifecycle, with
                // or without usage; a non-null usage object is metered.
                coverage.accepted += 1;
                if record.usage.is_some() {
                    coverage.metered += 1;
                }
                if let Some(usage) = record.usage {
                    let key = (record.machine_id, record.model);
                    accumulate_usage(groups.entry(key).or_default(), &usage);
                }
            }
            Err(_) if terminated => warnings.unparseable += 1,
            Err(_) => {
                // A crash can leave a trailing unfinished line (DESIGN.md §7).
                // Only a genuinely incomplete or invalid tail is ignored; a
                // complete final record without a terminating newline above is
                // already counted.
                if !record.iter().all(u8::is_ascii_whitespace) {
                    warnings.partial_tail = true;
                }
            }
        }
        if !terminated {
            // The final line had no newline; there is nothing after it.
            break;
        }
    }
    Ok((groups, coverage, warnings))
}

fn accumulate_usage(totals: &mut GroupTotals, usage: &Usage) {
    totals.records += 1;
    add(&mut totals.input_total, usage.input_total);
    add(&mut totals.uncached, usage.uncached);
    add(&mut totals.cache_read, usage.cache_read);
    add(&mut totals.cache_write, usage.cache_write);
    add(&mut totals.output_total, usage.output_total);
    add(&mut totals.reasoning, usage.reasoning);
    add(&mut totals.total, usage.total);
}

fn add(sum: &mut Option<u64>, value: Option<u64>) {
    if let Some(v) = value {
        *sum = Some(sum.unwrap_or(0).saturating_add(v));
    }
}

const MACHINE_W: usize = 14;
const MODEL_W: usize = 16;
const RECORDS_W: usize = 8;
const COUNTER_W: usize = 12;

fn render(
    out: &mut dyn Write,
    groups: &BTreeMap<GroupKey, GroupTotals>,
    coverage: &Coverage,
) -> io::Result<()> {
    writeln!(
        out,
        "{:<MACHINE_W$}{:<MODEL_W$}{:>RECORDS_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$}",
        "machine",
        "model",
        "records",
        "input",
        "uncached",
        "cache_read",
        "cache_write",
        "output",
        "reasoning",
        "total",
    )?;
    for ((machine, model), totals) in groups {
        writeln!(
            out,
            "{machine:<MACHINE_W$}{:<MODEL_W$}{:>RECORDS_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$} {:>COUNTER_W$}",
            model.as_deref().unwrap_or("-"),
            totals.records,
            cell(totals.input_total),
            cell(totals.uncached),
            cell(totals.cache_read),
            cell(totals.cache_write),
            cell(totals.output_total),
            cell(totals.reasoning),
            cell(totals.total),
        )?;
    }
    writeln!(
        out,
        "- = not recorded in any record; totals sum only recorded values"
    )?;
    let percentage = match coverage.percentage() {
        Some(pct) => format!("({pct:.1}%)"),
        None => "(no accepted lifecycles)".to_string(),
    };
    writeln!(
        out,
        "coverage: accepted={} metered={} unmetered={} {}",
        coverage.accepted,
        coverage.metered,
        coverage.unmetered(),
        percentage
    )
}

fn cell(value: Option<u64>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "-".to_string(),
    }
}

fn print_warnings(warnings: &ReadWarnings) {
    if warnings.partial_tail {
        eprintln!(
            "debitmetre: summary: ignored 1 unfinished trailing line (possible crash partial write)"
        );
    }
    if warnings.unparseable > 0 {
        eprintln!(
            "debitmetre: summary: skipped {} unparseable record(s); only complete canonical records are counted",
            warnings.unparseable
        );
    }
}
