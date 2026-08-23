# DESIGN-REVIEW — batch design grill record

> Date: 2026-08. This file records the batch grill conclusions on the early draft
> (2026-08-19 metered-codex-gateway design draft) before issue #1.
> It is a **process record**, not a specification; the current authoritative specification is [DESIGN.md](DESIGN.md).

## Process

Independent questioners produced batches of questions per domain (protocol compatibility, accounting correctness, authentication and machine identity, privacy, audit semantics,
deployment failure modes, scope contradictions); the coordinator ruled centrally. The following are the adopted rulings,
already merged into DESIGN.md; only the key points and the superseded draft statements are kept here.

## Key rulings summary

| Domain | Ruling |
|---|---|
| Routing | Fixed `/v1/responses`, `/v1/responses/compact`, `/healthz`; unknown path 404, wrong method 405, never forward |
| Transparency | HTTP semantics + body byte-order transparency; no promise of identical chunk boundaries and per-header bytes; reframing allowed |
| Identity | Remove `X-Meter-Device`/`openai-machine-id` self-reporting schemes; X-Meter-Key server-side SHA-256 digest → machine_id mapping; uniform 401 on auth failure without touching the upstream |
| OAuth | The gateway neither understands nor validates Authorization; pass through if present, still forward if absent; 401 decided by the upstream |
| Token accounting | Deprecate `uncached = input − cached`; adopt mutually exclusive buckets + four-state accounting_quality; missing ≠ 0 |
| Audit line count | Deprecate the narrow "one line per successful request" definition; each accepted request lifecycle attempts to produce at most one record containing an outcome and optional usage; compact gets its own line |
| Privacy | Audit is allowlist-based: no session/request id, account id, OAuth, key, IP, body, raw headers, raw usage JSON; correlation only via event_id |
| Failure semantics | Startup fail-closed (exit if configuration/audit file unavailable); runtime fail-open (audit failures do not change proxy results); transport failure → safe 502 |
| Cancellation | client_cancelled stops pumping to the upstream; terminal usage received before cancellation is recorded, no estimation |
| Storage | Best-effort JSONL: crash may drop a trailing line, reader ignores unfinished lines; no fsync-per-record, rotation, SQLite |
| Performance boundary | Keep only correctness constraints (SSE correctness, opaque streaming, bounded parse buffers and audit queue); body budget/MemoryMax/stream slots/benchmark metrics deferred to measurement-driven future work |
| quota | rate-limit/quota snapshot is not part of the gateway MVP; if added in the future it must be a separate kind |
| Dedup | MVP does no retry dedup — Codex retries are real upstream usage, dedup by request id would undercount; event_id only for import idempotency |
| TDD seams | HTTP seam / audit seam / startup seam: three public observation points pre-confirmed; fake upstream is a test adapter |

## Draft statements overturned by rulings (do not treat as open questions)

1. "Standalone component vs orihsus extension", "language TBD" → settled: independent Rust project (ADR-0002).
2. `X-Meter-Device` client self-reported machine identity → replaced by server-side key mapping.
3. The `uncached_input = input_tokens − cached_tokens` accounting basis → replaced by mutually exclusive buckets.
4. The single merged audit-line assumption of "one line per successful request" → changed to the per-request lifecycle record model.
5. The research suggestions of global body budget, MemoryMax/stream slots sizing, quota snapshot,
   daily reports/reconciliation → conflict with "no premature performance design" and the MVP scope,
   moved entirely into future work; the research document has been annotated with the authority boundary note.

## Remaining manual PoC questions

See [DESIGN.md](DESIGN.md) §11: content-encoding distribution, `model` stability,
`response.done` existence, terminal-event size distribution, compact response shape.
