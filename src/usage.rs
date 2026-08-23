//! Canonical JSONL usage audit: the record schema (DESIGN.md §5), bounded
//! SSE/JSON usage extraction (§4), and a bounded fail-open JSONL writer (§7).
//!
//! The `StreamUsageParser`/`JsonUsageParser`/`SseUsageParser` side-band parsers
//! are adapted from `src/gateway.rs` and the bounded single-writer JSONL audit
//! writer from `src/audit.rs` in [ariga39/orihsus] (MIT OR Apache-2.0,
//! Copyright (c) 2026 Kagami) at revision
//! `7285dd5c6a7ec5f1c0e521c6ee71f70e659d6220`; see THIRD-PARTY-NOTICES.md.
//! orihsus's key-pool/retry/quota/product semantics are not imported. The
//! token-accounting basis (mutually exclusive input buckets, derived `uncached`,
//! non-negativity) follows DESIGN.md §4. The present-but-invalid counter
//! distinction (`pick_counter`) is bespoke: orihsus reads counters with
//! `.as_u64()` only, which collapses a present-invalid value into absent for
//! optional counters and into whole-usage failure for required ones.
//!
//! [ariga39/orihsus]: https://github.com/ariga39/orihsus

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Bounded audit queue capacity (DESIGN.md §8). At most this many records wait
/// in memory for the single background writer; further records are dropped
/// (fail-open) instead of ever blocking the proxy.
const AUDIT_QUEUE_CAPACITY: usize = 2048;

/// Maximum bytes of one SSE event buffered for usage extraction (DESIGN.md
/// §4.1). A larger event is still forwarded in full but records
/// `metering_error=event_too_large`; the limit is an implementation parameter.
const SSE_EVENT_CAP: usize = 256 * 1024;

/// Fixed small cap on the buffered bytes of a non-streaming JSON response body
/// used to extract usage. A body larger than this (or one that never reaches
/// EOF) is forwarded untouched and records null usage — the gateway never
/// accumulates a whole body.
const JSON_USAGE_CAP: usize = 64 * 1024;

/// `kind` is fixed by the canonical schema (DESIGN.md §5).
const KIND: &str = "request";

/// `schema_version` is fixed by the canonical schema (DESIGN.md §5).
const SCHEMA_VERSION: u8 = 1;

/// Static, sanitized one-shot warning printed to stderr on the first audit
/// write failure. No path, machine, request, or credential detail is echoed.
const AUDIT_WRITE_FAILED: &str = "debitmetre: audit_write_failed: usage record write failed; \
the usage file may be incomplete (further failures are only counted)";

/// Static, sanitized one-shot warning printed to stderr on the first dropped
/// audit record. No request detail or value is ever echoed.
const AUDIT_DROPPED: &str = "debitmetre: audit_dropped: usage record dropped; the audit queue is \
full (further drops are only counted)";

/// Operation category of an accepted request (DESIGN.md §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Operation {
    Response,
    Compaction,
}

/// Terminal-state classification of a request lifecycle (DESIGN.md §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    Completed,
    Incomplete,
    UpstreamError,
    TransportError,
    UpstreamInterrupted,
    ClientCancelled,
}

/// Marker of how trustworthy a usage datum is (DESIGN.md §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AccountingQuality {
    Complete,
    Partial,
    Inconsistent,
    #[default]
    Unavailable,
}

/// Why usage could not be recorded (DESIGN.md §5). `null` when usage is
/// recorded normally or when a non-success response made metering inapplicable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MeteringError {
    MissingUsage,
    MalformedUsage,
    EventTooLarge,
}

/// Canonical token counters (DESIGN.md §4). Missing fields stay missing (JSON
/// `null`); a missing value is never disguised as 0.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Usage {
    pub(crate) input_total: Option<u64>,
    pub(crate) uncached: Option<u64>,
    pub(crate) cache_read: Option<u64>,
    pub(crate) cache_write: Option<u64>,
    pub(crate) output_total: Option<u64>,
    pub(crate) reasoning: Option<u64>,
    pub(crate) total: Option<u64>,
}

/// One canonical JSONL audit line (DESIGN.md §5). Only allowlisted fields;
/// bodies, credentials, meter keys, raw headers, and raw usage JSON never enter
/// a record. Also deserializable so the local `summary` command can read the
/// recorded facts back from the usage file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AuditRecord {
    pub(crate) schema_version: u8,
    pub(crate) kind: String,
    pub(crate) event_id: String,
    pub(crate) timestamp: String,
    pub(crate) machine_id: String,
    pub(crate) operation: Operation,
    pub(crate) upstream_status: Option<u16>,
    pub(crate) outcome: Outcome,
    pub(crate) model: Option<String>,
    pub(crate) accounting_quality: AccountingQuality,
    pub(crate) metering_error: Option<MeteringError>,
    pub(crate) usage: Option<Usage>,
}

impl AuditRecord {
    /// Start a record for an accepted request with the gateway-generated
    /// identity and timestamp anchored to the moment the record is produced
    /// (end of the request lifecycle). Callers fill in the terminal fields.
    pub(crate) fn new(machine_id: String, operation: Operation) -> Self {
        AuditRecord {
            schema_version: SCHEMA_VERSION,
            kind: KIND.to_string(),
            event_id: Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            machine_id,
            operation,
            upstream_status: None,
            outcome: Outcome::Completed,
            model: None,
            accounting_quality: AccountingQuality::Unavailable,
            metering_error: None,
            usage: None,
        }
    }
}

/// Result of extracting usage from a completed stream.
#[derive(Debug, Clone, Default)]
pub(crate) struct AuditResult {
    pub(crate) model: Option<String>,
    pub(crate) usage: Option<Usage>,
    pub(crate) quality: AccountingQuality,
    pub(crate) metering_error: Option<MeteringError>,
}

/// Side-band response-body usage parser selected by the response Content-Type
/// (DESIGN.md §2): SSE for `text/event-stream`, bounded JSON otherwise.
pub(crate) enum StreamUsageParser {
    Sse(SseUsageParser),
    Json(JsonUsageParser),
}

impl StreamUsageParser {
    pub(crate) fn sse() -> Self {
        StreamUsageParser::Sse(SseUsageParser::new(SSE_EVENT_CAP))
    }

    pub(crate) fn json() -> Self {
        StreamUsageParser::Json(JsonUsageParser::new(JSON_USAGE_CAP))
    }

    pub(crate) fn push(&mut self, chunk: &[u8]) {
        match self {
            StreamUsageParser::Sse(p) => p.push(chunk),
            StreamUsageParser::Json(p) => p.push(chunk),
        }
    }

    /// Finalize parsing after the stream reaches its terminal state.
    pub(crate) fn finish(&self) -> AuditResult {
        match self {
            StreamUsageParser::Sse(p) => p.finish(),
            StreamUsageParser::Json(p) => p.finish(),
        }
    }

    /// Whether the stream carried a terminal `response.incomplete` event (SSE)
    /// or the non-streaming body carries the `incomplete` marker.
    pub(crate) fn incomplete(&self) -> bool {
        match self {
            StreamUsageParser::Sse(p) => p.incomplete(),
            StreamUsageParser::Json(p) => p.incomplete(),
        }
    }

    /// Whether the parser observed a terminal event: for SSE, a
    /// `response.completed` or `response.incomplete` event (DESIGN.md §4.1); a
    /// non-streaming body is inherently terminal once fully received.
    pub(crate) fn terminal_seen(&self) -> bool {
        match self {
            StreamUsageParser::Sse(p) => p.terminal_seen(),
            StreamUsageParser::Json(_) => true,
        }
    }
}

/// Bounded incremental SSE parser: extracts usage and model from the terminal
/// `response.completed` / `response.incomplete` events without accumulating the
/// whole stream (DESIGN.md §4.1). Handles LF (`\n\n`) and CRLF (`\r\n\r\n`)
/// delimiters split across arbitrary chunk boundaries. An event larger than
/// `event_cap` is marked for discard until its delimiter arrives, so a
/// truncated event is never mis-parsed; such an event still records
/// `metering_error=event_too_large` when no valid usage was seen.
pub(crate) struct SseUsageParser {
    event_buf: Vec<u8>,
    event_cap: usize,
    tail: [u8; 4],
    tail_len: usize,
    discarding: bool,
    oversized: bool,
    terminal_incomplete: bool,
    terminal_seen: bool,
    usage_malformed: bool,
    malformed_counter: bool,
    model: Option<String>,
    usage: Option<(Usage, AccountingQuality)>,
}

impl SseUsageParser {
    fn new(event_cap: usize) -> Self {
        SseUsageParser {
            event_buf: Vec::with_capacity(event_cap.min(4096)),
            event_cap,
            tail: [0; 4],
            tail_len: 0,
            discarding: false,
            oversized: false,
            terminal_incomplete: false,
            terminal_seen: false,
            usage_malformed: false,
            malformed_counter: false,
            model: None,
            usage: None,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        for &b in chunk {
            if self.tail_len == 4 {
                self.tail.copy_within(1..4, 0);
                self.tail[3] = b;
            } else {
                self.tail[self.tail_len] = b;
                self.tail_len += 1;
            }
            let tail = &self.tail[..self.tail_len];
            if tail.ends_with(b"\n\n") || tail.ends_with(b"\r\n\r\n") {
                let event = std::mem::take(&mut self.event_buf);
                if !self.discarding {
                    self.consume_event(&event);
                }
                self.discarding = false;
                self.tail_len = 0;
            } else if !self.discarding {
                if self.event_buf.len() < self.event_cap {
                    self.event_buf.push(b);
                } else {
                    self.discarding = true;
                    self.oversized = true;
                }
            }
        }
    }

    fn consume_event(&mut self, event: &[u8]) {
        let text = String::from_utf8_lossy(event);
        let data: Vec<&str> = text
            .lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(str::trim)
            .collect();
        if data.is_empty() {
            return;
        }
        let payload = data.join("\n");
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&payload) else {
            return;
        };
        // Only the documented terminal events carry the final usage (DESIGN.md
        // §4.1); `response.done` is not treated as terminal until a manual
        // protocol PoC proves it exists. All other SSE events are ignored.
        let kind = value.get("type").and_then(serde_json::Value::as_str);
        if !matches!(kind, Some("response.completed" | "response.incomplete")) {
            return;
        }
        self.terminal_seen = true;
        if kind == Some("response.incomplete") {
            self.terminal_incomplete = true;
        }
        let Some(response) = value.get("response").and_then(serde_json::Value::as_object) else {
            return;
        };
        if let Some(model) = response.get("model").and_then(serde_json::Value::as_str) {
            self.model = Some(model.to_string());
        }
        match response.get("usage") {
            None | Some(serde_json::Value::Null) => {}
            Some(usage) if usage.is_object() => {
                let (raw, malformed) = extract_raw_usage(usage);
                self.malformed_counter |= malformed;
                self.usage = Some(canonicalize(raw, malformed));
            }
            Some(_) => self.usage_malformed = true,
        }
    }

    fn incomplete(&self) -> bool {
        self.terminal_incomplete
    }

    fn terminal_seen(&self) -> bool {
        self.terminal_seen
    }

    fn finish(&self) -> AuditResult {
        AuditResult {
            model: self.model.clone(),
            usage: self.usage.map(|(usage, _)| usage),
            quality: self
                .usage
                .map(|(_, quality)| quality)
                .unwrap_or(AccountingQuality::Unavailable),
            metering_error: match &self.usage {
                Some(_) if self.malformed_counter => Some(MeteringError::MalformedUsage),
                Some(_) => None,
                None if self.oversized => Some(MeteringError::EventTooLarge),
                None if self.usage_malformed => Some(MeteringError::MalformedUsage),
                None => Some(MeteringError::MissingUsage),
            },
        }
    }
}

/// Bounded side-band parser for a non-streaming (`application/json`) response
/// body. Buffers at most `cap` bytes; once exceeded the body is marked
/// overflowed and never accumulated. `finish()` returns the token counts only
/// when the buffered body is complete JSON within the cap.
pub(crate) struct JsonUsageParser {
    buf: Vec<u8>,
    cap: usize,
    overflowed: bool,
}

impl JsonUsageParser {
    fn new(cap: usize) -> Self {
        JsonUsageParser {
            buf: Vec::with_capacity(cap.min(4096)),
            cap,
            overflowed: false,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        if self.overflowed {
            return;
        }
        let remaining = self.cap - self.buf.len();
        if chunk.len() <= remaining {
            self.buf.extend_from_slice(chunk);
        } else {
            self.buf.extend_from_slice(&chunk[..remaining]);
            self.buf.clear();
            self.overflowed = true;
        }
    }

    /// Whether the buffered non-streaming body carries the `incomplete` lifecycle
    /// marker: the top-level Responses `status` (ResponseStatus) is exactly
    /// `incomplete`. A separate `incomplete_details` object is context, not
    /// lifecycle authority. A body that overflowed or is not complete JSON
    /// within the cap is not marked incomplete.
    fn incomplete(&self) -> bool {
        if self.overflowed {
            return false;
        }
        serde_json::from_slice::<serde_json::Value>(&self.buf)
            .ok()
            .and_then(|root| {
                root.as_object().and_then(|obj| {
                    obj.get("status")
                        .and_then(serde_json::Value::as_str)
                        .map(|status| status == "incomplete")
                })
            })
            .unwrap_or(false)
    }

    fn finish(&self) -> AuditResult {
        if self.overflowed {
            return AuditResult {
                metering_error: Some(MeteringError::MissingUsage),
                quality: AccountingQuality::Unavailable,
                ..AuditResult::default()
            };
        }
        let Ok(root) = serde_json::from_slice::<serde_json::Value>(&self.buf) else {
            return AuditResult {
                metering_error: Some(MeteringError::MissingUsage),
                quality: AccountingQuality::Unavailable,
                ..AuditResult::default()
            };
        };
        let Some(root) = root.as_object() else {
            return AuditResult {
                metering_error: Some(MeteringError::MissingUsage),
                quality: AccountingQuality::Unavailable,
                ..AuditResult::default()
            };
        };
        let model = root
            .get("model")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        match root.get("usage") {
            None | Some(serde_json::Value::Null) => AuditResult {
                model,
                metering_error: Some(MeteringError::MissingUsage),
                quality: AccountingQuality::Unavailable,
                ..AuditResult::default()
            },
            Some(usage) if usage.is_object() => {
                let (raw, malformed) = extract_raw_usage(usage);
                let (usage, quality) = canonicalize(raw, malformed);
                AuditResult {
                    model,
                    usage: Some(usage),
                    quality,
                    metering_error: malformed.then_some(MeteringError::MalformedUsage),
                }
            }
            Some(_) => AuditResult {
                model,
                metering_error: Some(MeteringError::MalformedUsage),
                quality: AccountingQuality::Unavailable,
                ..AuditResult::default()
            },
        }
    }
}

/// Raw counters exactly as reported upstream; missing fields stay missing.
#[derive(Debug, Clone, Copy, Default)]
struct RawUsage {
    input_total: Option<u64>,
    cache_read: Option<u64>,
    cache_write: Option<u64>,
    output_total: Option<u64>,
    reasoning: Option<u64>,
    total: Option<u64>,
}

/// Read one token counter from the first *present* candidate path (in priority
/// order). Distinguishes a genuinely absent counter (`Ok(None)`, also for an
/// explicit `null`) from a counter that is present but not a valid non-negative
/// integer (`Err(())`). A higher-priority candidate that is present but invalid
/// is malformed — it is not silently skipped in favor of a lower-priority
/// fallback. This is the basis of `metering_error=malformed_usage` being
/// distinct from a missing counter (DESIGN.md §5).
fn pick_counter(usage: &serde_json::Value, paths: &[&[&str]]) -> Result<Option<u64>, ()> {
    for &path in paths {
        let mut cur = usage;
        let mut present = true;
        for key in path {
            match cur.get(key) {
                Some(value) => cur = value,
                None => {
                    present = false;
                    break;
                }
            }
        }
        if !present || cur.is_null() {
            continue;
        }
        return match cur.as_u64() {
            Some(count) => Ok(Some(count)),
            None => Err(()),
        };
    }
    Ok(None)
}

/// Read the token counters from a Responses terminal `usage` object. Candidate
/// field names cover the Responses API shape observed in the OpenAI Codex
/// source and the community chat-completion legacy names; absent fields stay
/// absent (never defaulted to 0). Returns the counters and whether any
/// recognized counter was present but not a valid non-negative integer.
fn extract_raw_usage(usage: &serde_json::Value) -> (RawUsage, bool) {
    let input_total = pick_counter(usage, &[&["input_tokens"], &["prompt_tokens"]]);
    let cache_read = pick_counter(
        usage,
        &[
            &["input_tokens_details", "cached_tokens"],
            &["prompt_cache_hit_tokens"],
            &["cache_read_input_tokens"],
        ],
    );
    let cache_write = pick_counter(
        usage,
        &[
            &["input_tokens_details", "cache_write_tokens"],
            &["cache_write_input_tokens"],
            &["cache_write_tokens"],
            &["cache_creation_input_tokens"],
            &["prompt_tokens_details", "cache_write_tokens"],
        ],
    );
    let output_total = pick_counter(usage, &[&["output_tokens"], &["completion_tokens"]]);
    let reasoning = pick_counter(
        usage,
        &[
            &["output_tokens_details", "reasoning_tokens"],
            &["completion_tokens_details", "reasoning_tokens"],
            &["reasoning_tokens"],
        ],
    );
    let total = pick_counter(usage, &[&["total_tokens"]]);
    let malformed = matches!(input_total, Err(()))
        || matches!(cache_read, Err(()))
        || matches!(cache_write, Err(()))
        || matches!(output_total, Err(()))
        || matches!(reasoning, Err(()))
        || matches!(total, Err(()));
    (
        RawUsage {
            input_total: input_total.unwrap_or(None),
            cache_read: cache_read.unwrap_or(None),
            cache_write: cache_write.unwrap_or(None),
            output_total: output_total.unwrap_or(None),
            reasoning: reasoning.unwrap_or(None),
            total: total.unwrap_or(None),
        },
        malformed,
    )
}

/// Canonicalize upstream counters into the DESIGN.md §4 accounting basis:
/// `input_total = uncached + cache_read + cache_write` with mutually exclusive
/// buckets. `uncached` is derived only when all required input details are
/// present and satisfy the non-negativity invariant; otherwise it is null.
/// Contradictory data keeps the upstream value and is marked `inconsistent`. A
/// counter that is present but invalid keeps the valid counters, records the
/// invalid counter as null, and downgrades the quality to `partial`.
fn canonicalize(raw: RawUsage, malformed: bool) -> (Usage, AccountingQuality) {
    let (uncached, quality) = match (raw.input_total, raw.cache_read, raw.cache_write) {
        (Some(input), Some(read), Some(write)) => {
            if input >= read.saturating_add(write) {
                (Some(input - read - write), AccountingQuality::Complete)
            } else {
                (None, AccountingQuality::Inconsistent)
            }
        }
        _ => (None, AccountingQuality::Partial),
    };
    (
        Usage {
            input_total: raw.input_total,
            uncached,
            cache_read: raw.cache_read,
            cache_write: raw.cache_write,
            output_total: raw.output_total,
            reasoning: raw.reasoning,
            total: raw.total,
        },
        if malformed {
            AccountingQuality::Partial
        } else {
            quality
        },
    )
}

/// Handle to an asynchronous append-only JSONL usage writer.
///
/// [`AuditWriter::start`] opens the file synchronously at startup: an invalid
/// or unwritable path fails here (startup fail-closed, DESIGN.md §6).
/// `try_record` never blocks on file I/O; accepted records are appended by a
/// single background thread, and a transient write failure only warns once
/// (runtime fail-open) without affecting the proxy. Dropping the writer
/// detaches the thread best-effort, so a stuck filesystem never hangs shutdown.
pub(crate) struct AuditWriter {
    tx: Option<SyncSender<Box<AuditRecord>>>,
    drop_warned: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl AuditWriter {
    /// Open `path` for appending (creating it if needed) and start the bounded
    /// background writer. Failure to open returns an `io::Error` so startup can
    /// fail closed.
    pub(crate) fn start(path: impl AsRef<Path>) -> std::io::Result<AuditWriter> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(spawn(file))
    }

    /// Offer a record to the writer. Never blocks; a full or gone writer drops
    /// the record and warns once (fail-open).
    pub(crate) fn try_record(&self, record: AuditRecord) {
        let Some(tx) = &self.tx else {
            warn_once(&self.drop_warned, AUDIT_DROPPED);
            return;
        };
        if let Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) =
            tx.try_send(Box::new(record))
        {
            warn_once(&self.drop_warned, AUDIT_DROPPED);
        }
    }
}

impl Drop for AuditWriter {
    fn drop(&mut self) {
        // Never join the writer thread here: a writer stuck on I/O must not
        // hang whatever drops the last handle. Drop the command sender so the
        // writer drains what it has and exits on its own, and detach.
        if let Some(tx) = self.tx.take() {
            drop(tx);
        }
        self.handle.take();
    }
}

fn spawn(file: std::fs::File) -> AuditWriter {
    let (tx, rx) = sync_channel(AUDIT_QUEUE_CAPACITY);
    let drop_warned = Arc::new(AtomicBool::new(false));
    let write_warned = Arc::new(AtomicBool::new(false));
    let ww = Arc::clone(&write_warned);
    let handle = thread::spawn(move || write_loop(file, rx, ww));
    AuditWriter {
        tx: Some(tx),
        drop_warned,
        handle: Some(handle),
    }
}

fn write_loop(
    mut file: std::fs::File,
    rx: Receiver<Box<AuditRecord>>,
    write_warned: Arc<AtomicBool>,
) {
    while let Ok(record) = rx.recv() {
        let Ok(mut line) = serde_json::to_vec(&record) else {
            continue;
        };
        line.push(b'\n');
        if file.write_all(&line).is_err() {
            warn_once(&write_warned, AUDIT_WRITE_FAILED);
        }
    }
}

/// One-shot stderr warning guard: prints `message` on the first call, stays
/// silent afterwards so a failing writer never floods stderr.
fn warn_once(flag: &AtomicBool, message: &str) -> bool {
    if flag.swap(true, Ordering::Relaxed) {
        return false;
    }
    eprintln!("{message}");
    true
}
