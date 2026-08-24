//! Canonical JSONL usage audit: the record schema (DESIGN.md §5), the
//! library-based SSE/JSON usage observer (§4), and a bounded fail-open JSONL
//! writer (§7).
//!
//! The response-body observer mirrors every forwarded byte and parses the mirror
//! once at lifecycle finalization: the whole mirrored body is framed as SSE with
//! `eventsource-stream` 0.2.3 — the same mature crate used by the pinned
//! openai/codex 0.149.0 client, which drives `stream.eventsource()` regardless
//! of Content-Type — and a supported terminal `response.completed` /
//! `response.incomplete` is selected from the event-data JSON. A body that never
//! framed as SSE is parsed as one complete JSON document. The bounded
//! single-writer JSONL audit writer is adapted from `src/audit.rs` in
//! [ariga39/orihsus] (MIT OR Apache-2.0, Copyright (c) 2026 Kagami) at revision
//! `7285dd5c6a7ec5f1c0e521c6ee71f70e659d6220`; see THIRD-PARTY-NOTICES.md.
//! orihsus's key-pool/retry/quota/product semantics are not imported. The
//! token-accounting basis (mutually exclusive input buckets, derived `uncached`,
//! non-negativity) follows DESIGN.md §4.
//!
//! [ariga39/orihsus]: https://github.com/ariga39/orihsus

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use eventsource_stream::{Event, Eventsource};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Bounded audit queue capacity (DESIGN.md §8). At most this many records wait
/// in memory for the single background writer; further records are dropped
/// (fail-open) instead of ever blocking the proxy.
const AUDIT_QUEUE_CAPACITY: usize = 2048;

/// `kind` is fixed by the canonical schema (DESIGN.md §5).
pub(crate) const KIND: &str = "request";

/// `schema_version` is fixed by the canonical schema (DESIGN.md §5).
pub(crate) const SCHEMA_VERSION: u8 = 1;

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
/// `event_too_large` remains a valid schema value for records written before the
/// library-based observer replaced the per-event-capped parser; the current
/// observer never produces it.
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

/// Terminal-state facts derived from one mirrored response body.
#[derive(Debug, Clone)]
struct ParsedBody {
    audit: AuditResult,
    terminal_seen: bool,
    incomplete: bool,
}

/// Library-based, functional-first response-body usage observer.
///
/// Mirrors every forwarded byte and parses the mirror once at lifecycle
/// finalization (DESIGN.md §4, §5): the whole mirror is framed as SSE with
/// `eventsource-stream` 0.2.3 regardless of Content-Type (the pinned Codex
/// client feeds the same bytes to its own SSE parser), and a supported terminal
/// `response.completed` / `response.incomplete` is selected from the event-data
/// JSON. A body that never framed as SSE (the library emitted no SSE data event)
/// is parsed as one complete JSON document. The valid `openai-model` response
/// header is the primary server-reported model, mirroring the pinned Codex
/// client's `ServerModel` event; the terminal-body model is only the fallback.
/// The observer never delays or alters forwarding: it only reads the mirror at
/// finalization, and no per-event cap or oversized-event recovery is applied
/// (memory is not optimized before measurement).
pub(crate) struct StreamUsageParser {
    header_model: Option<String>,
    mirror: Vec<u8>,
    parsed: Option<ParsedBody>,
}

impl StreamUsageParser {
    pub(crate) fn new(header_model: Option<String>) -> Self {
        StreamUsageParser {
            header_model,
            mirror: Vec::new(),
            parsed: None,
        }
    }

    /// Mirror one forwarded chunk; the pump keeps forwarding bytes unchanged.
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.mirror.extend_from_slice(chunk);
    }

    /// Parse the mirrored body once the lifecycle reaches its terminal state.
    pub(crate) async fn finalize(&mut self) {
        if self.parsed.is_none() {
            let parsed = parse_mirror(&self.mirror, &self.header_model).await;
            self.parsed = Some(parsed);
        }
    }

    /// Whether the mirrored body signaled an incomplete lifecycle: a terminal
    /// `response.incomplete` SSE event, or a non-streaming JSON body whose
    /// top-level `status` is `incomplete`.
    pub(crate) fn incomplete(&self) -> bool {
        self.parsed.as_ref().is_some_and(|parsed| parsed.incomplete)
    }

    /// Whether the mirrored body reached a grounded terminal state: a supported
    /// terminal SSE event, or a body that never framed as SSE and reached clean
    /// EOF (a completed non-streaming body, regardless of whether its metering
    /// parse succeeded).
    pub(crate) fn terminal_seen(&self) -> bool {
        self.parsed
            .as_ref()
            .is_some_and(|parsed| parsed.terminal_seen)
    }

    pub(crate) fn finish(&self) -> AuditResult {
        self.parsed
            .as_ref()
            .map(|parsed| parsed.audit.clone())
            .unwrap_or_default()
    }
}

/// Frame the whole mirror as SSE with `eventsource-stream` 0.2.3 and return
/// every event it produced. The crate emits only events whose `data` field is
/// non-empty (an event with empty data is dropped at dispatch), so a
/// non-streaming JSON body — including one with blank-line whitespace — yields
/// no events here, which is how a terminal-less SSE EOF is told apart from a
/// completed non-SSE body.
async fn sse_events(mirror: &[u8]) -> Vec<Event> {
    let stream = futures_util::stream::iter(std::iter::once(
        Ok::<&[u8], std::convert::Infallible>(mirror),
    ));
    let mut eventsource = stream.eventsource();
    let mut events = Vec::new();
    while let Some(event) = eventsource.next().await {
        let Ok(event) = event else { break };
        events.push(event);
    }
    events
}

async fn parse_mirror(mirror: &[u8], header_model: &Option<String>) -> ParsedBody {
    let mut terminal_seen = false;
    let mut terminal_incomplete = false;
    let mut body_model: Option<String> = None;
    let mut usage: Option<(Usage, AccountingQuality)> = None;
    let mut usage_malformed = false;

    let events = sse_events(mirror).await;
    for event in &events {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&event.data) else {
            continue;
        };
        let Some(kind) = value.get("type").and_then(serde_json::Value::as_str) else {
            continue;
        };
        // Only the documented terminal events carry the final usage (DESIGN.md
        // §4.1); `response.done` is not treated as terminal until a manual
        // protocol PoC proves it exists. All other SSE events are forwarded
        // unchanged and ignored here.
        if !matches!(kind, "response.completed" | "response.incomplete") {
            continue;
        }
        terminal_seen = true;
        if kind == "response.incomplete" {
            terminal_incomplete = true;
        }
        let Some(response) = value.get("response").and_then(serde_json::Value::as_object) else {
            continue;
        };
        if let Some(model) = response.get("model").and_then(serde_json::Value::as_str) {
            body_model = Some(model.to_string());
        }
        match response.get("usage") {
            None | Some(serde_json::Value::Null) => {}
            Some(usage_value) if usage_value.is_object() => {
                usage = Some(canonicalize(extract_raw_usage(usage_value)));
            }
            Some(_) => usage_malformed = true,
        }
    }

    if terminal_seen {
        return ParsedBody {
            audit: AuditResult {
                model: effective_model(header_model, &body_model),
                usage: usage.map(|(value, _)| value),
                quality: usage
                    .map(|(_, quality)| quality)
                    .unwrap_or(AccountingQuality::Unavailable),
                metering_error: match usage {
                    Some(_) => None,
                    None if usage_malformed => Some(MeteringError::MalformedUsage),
                    None => Some(MeteringError::MissingUsage),
                },
            },
            terminal_seen: true,
            incomplete: terminal_incomplete,
        };
    }

    // A body that framed as SSE but ended without a supported terminal event
    // never reached a completed lifecycle (DESIGN.md §4.1, §5): the caller
    // classifies the outcome as `upstream_interrupted`, and no terminal usage
    // exists to record.
    if !events.is_empty() {
        return ParsedBody {
            audit: AuditResult {
                model: header_model.clone(),
                quality: AccountingQuality::Unavailable,
                metering_error: Some(MeteringError::MissingUsage),
                ..AuditResult::default()
            },
            terminal_seen: false,
            incomplete: false,
        };
    }

    // No SSE data event was observed: the body never framed as SSE, so it is
    // read as one complete non-streaming JSON document.
    let (audit, incomplete) = parse_json_body(mirror, header_model);
    ParsedBody {
        audit,
        terminal_seen: true,
        incomplete,
    }
}

/// Parse the mirrored body as one complete non-streaming JSON document. The
/// lifecycle is incomplete when the top-level Responses `status` is exactly
/// `incomplete`; a separate `incomplete_details` object is context, not
/// lifecycle authority. A body that is not complete JSON records null usage.
fn parse_json_body(mirror: &[u8], header_model: &Option<String>) -> (AuditResult, bool) {
    let missing = AuditResult {
        quality: AccountingQuality::Unavailable,
        metering_error: Some(MeteringError::MissingUsage),
        ..AuditResult::default()
    };
    let Ok(root) = serde_json::from_slice::<serde_json::Value>(mirror) else {
        return (missing, false);
    };
    let Some(root) = root.as_object() else {
        return (missing, false);
    };
    let incomplete = root.get("status").and_then(serde_json::Value::as_str) == Some("incomplete");
    let body_model = root
        .get("model")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let model = effective_model(header_model, &body_model);
    let audit = match root.get("usage") {
        None | Some(serde_json::Value::Null) => AuditResult {
            model,
            quality: AccountingQuality::Unavailable,
            metering_error: Some(MeteringError::MissingUsage),
            ..AuditResult::default()
        },
        Some(usage) if usage.is_object() => {
            let (usage, quality) = canonicalize(extract_raw_usage(usage));
            AuditResult {
                model,
                usage: Some(usage),
                quality,
                metering_error: None,
            }
        }
        Some(_) => AuditResult {
            model,
            quality: AccountingQuality::Unavailable,
            metering_error: Some(MeteringError::MalformedUsage),
            ..AuditResult::default()
        },
    };
    (audit, incomplete)
}

/// Primary server-reported model with deterministic precedence: a valid
/// `openai-model` response header wins (the pinned Codex client emits it as its
/// `ServerModel` event), and the terminal-body model is only the fallback. A
/// conflicting pair resolves to the header, with a sanitized diagnostic that
/// never echoes values.
fn effective_model(header_model: &Option<String>, body_model: &Option<String>) -> Option<String> {
    match (header_model, body_model) {
        (Some(header), Some(body)) => {
            if header != body {
                tracing::trace!("openai-model response header overrides the terminal-body model");
            }
            Some(header.clone())
        }
        (Some(header), None) => Some(header.clone()),
        (None, body) => body.clone(),
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

/// Read the token counters from a Responses terminal `usage` object. Candidate
/// field names cover the Responses API shape observed in the OpenAI Codex
/// source and the community chat-completion legacy names; absent fields stay
/// absent (never defaulted to 0).
fn extract_raw_usage(usage: &serde_json::Value) -> RawUsage {
    let nested = |path: &[&str]| {
        let mut cur = usage;
        for key in path {
            cur = cur.get(key)?;
        }
        cur.as_u64()
    };
    RawUsage {
        input_total: usage
            .get("input_tokens")
            .or_else(|| usage.get("prompt_tokens"))
            .and_then(serde_json::Value::as_u64),
        cache_read: nested(&["input_tokens_details", "cached_tokens"])
            .or_else(|| {
                usage
                    .get("prompt_cache_hit_tokens")
                    .and_then(serde_json::Value::as_u64)
            })
            .or_else(|| {
                usage
                    .get("cache_read_input_tokens")
                    .and_then(serde_json::Value::as_u64)
            }),
        cache_write: nested(&["input_tokens_details", "cache_write_tokens"])
            .or_else(|| {
                usage
                    .get("cache_write_input_tokens")
                    .and_then(serde_json::Value::as_u64)
            })
            .or_else(|| {
                usage
                    .get("cache_write_tokens")
                    .and_then(serde_json::Value::as_u64)
            })
            .or_else(|| {
                usage
                    .get("cache_creation_input_tokens")
                    .and_then(serde_json::Value::as_u64)
            })
            .or_else(|| nested(&["prompt_tokens_details", "cache_write_tokens"])),
        output_total: usage
            .get("output_tokens")
            .or_else(|| usage.get("completion_tokens"))
            .and_then(serde_json::Value::as_u64),
        reasoning: nested(&["output_tokens_details", "reasoning_tokens"])
            .or_else(|| nested(&["completion_tokens_details", "reasoning_tokens"]))
            .or_else(|| {
                usage
                    .get("reasoning_tokens")
                    .and_then(serde_json::Value::as_u64)
            }),
        total: usage
            .get("total_tokens")
            .and_then(serde_json::Value::as_u64),
    }
}

/// Canonicalize upstream counters into the DESIGN.md §4 accounting basis:
/// `input_total = uncached + cache_read + cache_write` with mutually exclusive
/// buckets. `uncached` is derived only when all required input details are
/// present and satisfy the non-negativity invariant; otherwise it is null.
/// Contradictory data keeps the upstream value and is marked `inconsistent`.
fn canonicalize(raw: RawUsage) -> (Usage, AccountingQuality) {
    let (uncached, quality) = match (raw.input_total, raw.cache_read, raw.cache_write) {
        (Some(input), Some(read), Some(write)) => {
            match read
                .checked_add(write)
                .and_then(|sum| input.checked_sub(sum))
            {
                Some(uncached) => (Some(uncached), AccountingQuality::Complete),
                None => (None, AccountingQuality::Inconsistent),
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
        quality,
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
