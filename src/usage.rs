//! Canonical JSONL usage audit: the record schema (DESIGN.md §5), bounded
//! SSE/JSON usage extraction (§4), and a bounded fail-open JSONL writer (§7).
//!
//! The `SseUsageParser` and `JsonUsageParser` side-band observers and the
//! `StreamUsageParser` container are adapted from `src/gateway.rs` and the
//! bounded single-writer JSONL audit writer from `src/audit.rs` in
//! [ariga39/orihsus] (MIT OR Apache-2.0, Copyright (c) 2026 Kagami) at
//! revision `7285dd5c6a7ec5f1c0e521c6ee71f70e659d6220`; see
//! THIRD-PARTY-NOTICES.md. orihsus's key-pool/retry/quota/product semantics are
//! not imported. The token-accounting basis (mutually exclusive input buckets,
//! derived `uncached`, non-negativity) follows DESIGN.md §4.
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

/// Side-band response-body usage parser.
///
/// Runs two bounded observers over the response byte stream — the SSE observer
/// ([`SseUsageParser`]) and the non-streaming JSON observer
/// ([`JsonUsageParser`]) — and selects the grounded terminal result. Content-Type
/// alone is not authoritative for Responses framing: the Codex Responses client
/// feeds the same bytes to its own SSE parser regardless of Content-Type
/// (pinned codex-cli 0.149.0, `codex-rs/codex-api/src/sse/responses.rs`), so a
/// JSON-labeled body can still be SSE-framed. A body is metered as SSE only when
/// a terminal `response.completed` / `response.incomplete` event is actually
/// observed (including one recognized from its line-start `event:` field at the
/// completed delimiter when oversized); otherwise the complete non-streaming JSON
/// body owns the result. The lifecycle
/// is terminal when the SSE observer saw a supported terminal event or the body
/// never framed as SSE and reached clean EOF — a completed non-streaming body
/// whose bounded metering parse failed stays `completed`, never
/// `upstream_interrupted`. Both observers are bounded ([`SSE_EVENT_CAP`] per SSE
/// event, [`JSON_USAGE_CAP`] per JSON body); the whole body is never accumulated.
pub(crate) struct StreamUsageParser {
    sse: SseUsageParser,
    json: JsonUsageParser,
}

impl StreamUsageParser {
    pub(crate) fn new() -> Self {
        StreamUsageParser {
            sse: SseUsageParser::new(SSE_EVENT_CAP),
            json: JsonUsageParser::new(JSON_USAGE_CAP),
        }
    }

    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.sse.push(chunk);
        self.json.push(chunk);
    }

    /// Finalize parsing after the stream reaches its terminal state: the SSE
    /// observer owns the result when it observed a supported terminal event
    /// (including an oversized one recognized from its line-start `event:` field
    /// at the completed delimiter); otherwise the non-streaming JSON body owns
    /// it.
    pub(crate) fn finish(&self) -> AuditResult {
        if self.sse.terminal_seen() || self.sse.oversized_terminal_seen() {
            self.sse.finish()
        } else {
            self.json.finish()
        }
    }

    /// Whether the stream carried a terminal `response.incomplete` event (SSE)
    /// or the non-streaming JSON body carries the top-level `incomplete` marker.
    pub(crate) fn incomplete(&self) -> bool {
        if self.sse.terminal_seen() || self.sse.oversized_terminal_seen() {
            self.sse.incomplete()
        } else {
            self.json.incomplete()
        }
    }

    /// Whether the parser observed a grounded terminal state: a supported
    /// terminal SSE event (DESIGN.md §4.1), or a body that never framed as SSE
    /// and reached clean EOF (a completed non-streaming body, regardless of
    /// whether its bounded metering parse succeeded).
    pub(crate) fn terminal_seen(&self) -> bool {
        self.sse.terminal_seen() || self.sse.oversized_terminal_seen() || !self.sse.saw_event()
    }
}

/// Literal prefix of an SSE `event:` field line.
const EVENT_FIELD: &[u8] = b"event:";

/// Maximum bytes of an SSE event name retained by the field scanner; larger
/// than any supported terminal name (`response.incomplete` is 19 bytes), so the
/// scanner state stays constant-size.
const EVENT_NAME_CAP: usize = 24;

/// Progressive state for scanning a single SSE `event:` field line.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FieldScan {
    /// Not inside an `event:` field.
    Idle,
    /// Matching the literal `event:` prefix (bytes matched so far).
    Prefix(usize),
    /// The byte immediately after the colon: this is the only position where the
    /// single optional leading U+0020 may be removed.
    AfterColon,
    /// Reading the event name value into the bounded name buffer; every byte,
    /// including a later or trailing space, is retained.
    Name,
}

/// A supported terminal SSE event kind named by the last `event:` field of a
/// discarded oversized event completed at its delimiter.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OversizedTerminalKind {
    Completed,
    Incomplete,
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
    /// Whether at least one SSE data event was actually observed (a real SSE
    /// stream), as opposed to a body that never framed as SSE.
    saw_event: bool,
    /// Whether a discarded oversized event completed at its delimiter with a
    /// line-start `event:` field naming `response.completed`.
    oversized_terminal_completed: bool,
    /// Whether a discarded oversized event completed at its delimiter with a
    /// line-start `event:` field naming `response.incomplete`.
    oversized_terminal_incomplete: bool,
    /// True at a line start (start of stream or after a newline), where an
    /// `event:` field may begin. Constant-size field scanner state.
    line_start: bool,
    /// Progressive scanner state for the current `event:` field line.
    field: FieldScan,
    /// Bounded name buffer for the current `event:` field.
    name: [u8; EVENT_NAME_CAP],
    name_len: usize,
    /// The supported terminal kind named by the last `event:` field completed
    /// in the current in-flight event; `None` for an unsupported or too-long
    /// name. Reset only at a completed event delimiter; a partial event at EOF
    /// is never finalized.
    last_terminal: Option<OversizedTerminalKind>,
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
            saw_event: false,
            oversized_terminal_completed: false,
            oversized_terminal_incomplete: false,
            line_start: true,
            field: FieldScan::Idle,
            name: [0; EVENT_NAME_CAP],
            name_len: 0,
            last_terminal: None,
            model: None,
            usage: None,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        for &b in chunk {
            // The constant-size field scanner is fed every byte of the event,
            // including bytes after `event_cap` while discarding, so `event:`
            // fields after the retained head participate and the last field
            // wins.
            self.scan_event_field(b);
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
                } else {
                    // The field scanner saw every byte of the oversized event,
                    // so the final `event:` field (last field wins) decides the
                    // terminal kind at the completed delimiter. A partial event
                    // is never a delivered terminal (DESIGN.md §4.1).
                    if prefix_has_sse_framing(&event) {
                        self.saw_event = true;
                    }
                    match self.last_terminal {
                        Some(OversizedTerminalKind::Completed) => {
                            self.oversized_terminal_completed = true;
                        }
                        Some(OversizedTerminalKind::Incomplete) => {
                            self.oversized_terminal_incomplete = true;
                        }
                        None => {}
                    }
                }
                self.discarding = false;
                self.tail_len = 0;
                self.reset_event_field_scan();
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
        self.saw_event = true;
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
                self.usage = Some(canonicalize(extract_raw_usage(usage)));
            }
            Some(_) => self.usage_malformed = true,
        }
    }

    /// Constant-size scan of one byte for a supported terminal `event:` name at
    /// a line start. Retains at most [`EVENT_NAME_CAP`] bytes of the name and
    /// tracks the last completed `event:` field (SSE last-field-wins). The
    /// scanner is reused/adapted from a prior constant-size streaming
    /// implementation.
    fn scan_event_field(&mut self, b: u8) {
        if b == b'\n' {
            self.line_start = true;
        }
        match self.field {
            FieldScan::Idle => {
                if self.line_start {
                    if b == b'e' {
                        self.field = FieldScan::Prefix(1);
                        self.line_start = false;
                    } else if b != b'\n' {
                        self.line_start = false;
                    }
                }
            }
            FieldScan::Prefix(matched) => {
                if b == EVENT_FIELD[matched] {
                    let next = matched + 1;
                    if next == EVENT_FIELD.len() {
                        self.field = FieldScan::AfterColon;
                    } else {
                        self.field = FieldScan::Prefix(next);
                    }
                } else {
                    self.field = FieldScan::Idle;
                }
            }
            FieldScan::AfterColon => {
                if b == b' ' {
                    // Remove at most one leading U+0020, only here at the first
                    // byte after the colon.
                    self.field = FieldScan::Name;
                } else if b == b'\n' || b == b'\r' {
                    self.finish_event_name();
                    self.field = FieldScan::Idle;
                } else {
                    self.name[self.name_len] = b;
                    self.name_len += 1;
                    self.field = FieldScan::Name;
                }
            }
            FieldScan::Name => {
                if b == b'\n' || b == b'\r' {
                    self.finish_event_name();
                    self.field = FieldScan::Idle;
                } else if self.name_len < EVENT_NAME_CAP {
                    // Every byte is retained verbatim, including a later or
                    // trailing space, tab, or second leading space.
                    self.name[self.name_len] = b;
                    self.name_len += 1;
                } else {
                    // A name longer than any supported terminal name makes the
                    // current field unsupported, overwriting an earlier value.
                    self.last_terminal = None;
                    self.field = FieldScan::Idle;
                    self.name_len = 0;
                }
            }
        }
    }

    fn finish_event_name(&mut self) {
        // Exact, whitespace-preserving comparison: only the single optional
        // leading U+0020 was removed by the scanner, so the name must match a
        // supported terminal exactly (no tab, extra-space, or trailing-space
        // trimming).
        self.last_terminal = match &self.name[..self.name_len] {
            b"response.completed" => Some(OversizedTerminalKind::Completed),
            b"response.incomplete" => Some(OversizedTerminalKind::Incomplete),
            _ => None,
        };
        self.name_len = 0;
    }

    /// Reset the field scanner at a completed event delimiter. A partial event
    /// at EOF is never finalized and never resets here.
    fn reset_event_field_scan(&mut self) {
        self.line_start = true;
        self.field = FieldScan::Idle;
        self.name_len = 0;
        self.last_terminal = None;
    }

    fn incomplete(&self) -> bool {
        self.terminal_incomplete || self.oversized_terminal_incomplete()
    }

    fn terminal_seen(&self) -> bool {
        self.terminal_seen
    }

    /// Whether a discarded oversized event completed at its delimiter as a
    /// supported terminal event, recognized from a line-start `event:` field in
    /// its bounded prefix. A partial event truncated at EOF before the delimiter
    /// is not delivered and is never reported here. Bounded: scans at most
    /// `event_cap` bytes, never re-buffering a whole event.
    fn oversized_terminal_seen(&self) -> bool {
        self.oversized_terminal_completed || self.oversized_terminal_incomplete
    }

    fn oversized_terminal_incomplete(&self) -> bool {
        self.oversized_terminal_incomplete
    }

    /// Whether the body framed as an SSE stream: at least one SSE data event was
    /// consumed, or an oversized (discarded) event's bounded prefix carries SSE
    /// field lines. A body that never frames as SSE (a non-streaming JSON body)
    /// reports false.
    fn saw_event(&self) -> bool {
        self.saw_event || (self.discarding && prefix_has_sse_framing(&self.event_buf))
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
                Some(_) => None,
                // `event_too_large` is reserved for a terminal event exceeding
                // the bounded parse limit (DESIGN.md §4.1); an oversized
                // non-terminal event that never produced a terminal is simply
                // missing usage.
                None if self.oversized_terminal_seen() => Some(MeteringError::EventTooLarge),
                None if self.oversized => Some(MeteringError::MissingUsage),
                None if self.usage_malformed => Some(MeteringError::MalformedUsage),
                None => Some(MeteringError::MissingUsage),
            },
        }
    }
}

/// Whether `bytes` (a bounded SSE-event prefix) starts a line with an SSE field
/// name (`event:`, `data:`, `id:`, `retry:`). A non-streaming JSON body never
/// contains such lines, so this distinguishes a discarded oversized SSE event
/// from a huge JSON body.
fn prefix_has_sse_framing(bytes: &[u8]) -> bool {
    bytes.split(|&b| b == b'\n').any(|line| {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        line.starts_with(b"event:")
            || line.starts_with(b"data:")
            || line.starts_with(b"id:")
            || line.starts_with(b"retry:")
    })
}

/// Bounded side-band observer for a non-streaming (`application/json`) response
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
