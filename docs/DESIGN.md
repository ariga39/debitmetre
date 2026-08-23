# debitmetre — Codex usage-metering gateway · current specification

> Status: **accepted**. This document is the only authoritative current design specification in the repository.
> In case of conflict with other documents (including [docs/research/community-codex-proxies.md](research/community-codex-proxies.md)),
> this document prevails; research documents are evidence input only, not specification.
> Basis: issue #1; the centralized rulings of the 2026-08 batch design grill (see
> [DESIGN-REVIEW.md](DESIGN-REVIEW.md)). Terminology: see [CONTEXT.md](../CONTEXT.md).

## 1. Product positioning

debitmetre is a central transparent proxy sitting between multiple Codex clients and the single OpenAI Codex upstream.
For each request it records per-model, per-machine token usage (canonical request audit), for later offline cost estimation.
The gateway MVP records token facts only; "cost" refers solely to the equivalent estimate computed later against public API prices,
not an actual bill or subscription-credit consumption.

## 2. Routing and protocol behavior

Fixed external route set (closed route set):

| Route | Method | Behavior |
|---|---|---|
| `/v1/responses` | POST | Fixed mapping to the corresponding path under `chatgpt.com/backend-api/codex` |
| `/v1/responses/compact` | POST | Same as above, for compact requests |
| `/healthz` | GET | Minimal health check; contract below |

`/healthz` accepted contract: no authentication required; returns a minimal 200 once a valid configuration is loaded and the listener is ready.
It does not probe OpenAI, does not leak any configuration content, and does not represent runtime audit writability or any runtime health state.

- Unknown paths return 404; wrong methods on known paths return 405; neither is ever forwarded to the upstream.
- The production upstream is fixed in code and cannot be changed via configuration (SSRF prevention); following upstream redirects is forbidden.
- Tests may inject a fake upstream at process/module construction time; no such option exists in production configuration.
- Both SSE streaming and non-streaming JSON responses are transparently supported. Content-Type alone is not authoritative for Responses framing: a body labeled `application/json` can still be SSE-framed (the Codex client feeds the response bytes to its own SSE parser regardless of Content-Type). The extractor runs two bounded observers over the same stream — an SSE observer and a non-streaming JSON observer — and selects the grounded terminal result: a terminal `response.completed` / `response.incomplete` SSE event wins, otherwise the complete non-streaming JSON body wins. Both observers stay bounded; the whole body is never accumulated.

### 2.1 Semantic transparency boundary

Transparency promise = **HTTP semantics + response body bytes and ordering unchanged**.
No promise of identical TCP/HTTP chunk boundaries, and no promise of byte-identical headers:
normal HTTP reframing is allowed; `Host`, `Content-Length`, and `Transfer-Encoding` may be rebuilt by the HTTP stack.

Request headers follow an explicit stripping policy that prioritizes compatibility:

- Stripped: hop-by-hop headers, `Host`, proxy-chain headers, `Cookie`, all `X-Meter-*`.
- Preserved and passed through: `Authorization`, `ChatGPT-Account-ID`, and unknown Codex-specific headers.
- Responses: hop-by-hop headers are stripped; everything else is passed through unchanged.

## 3. Authentication and machine identity

- `X-Meter-Key` is the **only** source of identity; no client-reported identity exists or is trusted
  (`X-Meter-Device`, `openai-machine-id`, and similar schemes are deprecated).
- The gateway does not validate OAuth: it does not check whether `Authorization` is present or valid;
  if present it is passed through per the forwarding rules; if absent the request still proceeds to the upstream, and 401 is decided by the upstream.
- One active key per machine; the server stores the key's SHA-256 digest → stable `machine_id` mapping.
- Rotation = replacing the digest and restarting the process; no hot reload, no dual-key overlap window, and no revoke state.
- A missing, duplicated, malformed, or unknown key always returns the same 401 with a generic body **before**
  reading the body or connecting to the upstream, without distinguishing the cause.
- The key→machine mapping lives in a startup configuration file outside the repository, readable only by the deployment user;
  loaded once at startup; an invalid configuration exits immediately (fail-closed). No DB.
- No per-key quota, rate limit, or concurrency control; public-internet volumetric DoS protection is not part of the MVP.
- Multiple machine keys carry only identity and attribution; they introduce no user, role, or tenant semantics;
  multi-user is an explicit non-goal; a future change would require redesign.

## 4. Metering and token accounting

- The request body is forwarded directly as an opaque stream: no decompression, no parsing, no caching.
- `model` is taken from the response terminal object; if it cannot be obtained, record null, which does not affect forwarding.
- Input tokens use a mutually exclusive accounting basis:

  ```text
  input_total = uncached + cache_read + cache_write
  ```

- `output_total` includes reasoning; reasoning is only a detail field and must not be added again.
- `total` keeps the upstream's original value and is not recomputed or overwritten by us.
- Counter fields that exist upstream are stored under the canonical fields; missing fields are null,
  and a missing value is never disguised as 0. `uncached` is derived only when all required input details
  are present and satisfy the non-negativity invariant; otherwise `uncached = null`.
- On contradictory data, keep the upstream-reported value and mark quality as `inconsistent`; never let the proxy fail.
- usage is recorded only when genuinely received; never backfilled with zeros.
- **Sole authoritative metering source**: the upstream response usage observed by the gateway is the only authoritative source for MVP metering;
  local Codex JSONL and reconciliation do not participate in MVP metering.

### 4.1 SSE terminal events

- Only `response.completed` and `response.incomplete` are treated as terminal events;
  `response.done` is added to compatibility only after a manual protocol PoC proves it actually exists.
- All unknown SSE events are forwarded unchanged.
- When a terminal event exceeds the internal bounded parse limit, it is still forwarded to the client in full;
  that request's audit records `usage=null`, `accounting_quality=unavailable`, and
  `metering_error=event_too_large`. The limit value is chosen after PoC observation
  and is an implementation parameter, not a product promise.
- Implementation note (parser reuse): the bounded SSE observer does not adopt `eventsource-stream` 0.2.3
  (the crate used by the pinned Codex client). That crate has no per-event size cap, and observing through
  a transparent tee would require either blocking, dropping, or unbounded buffering of the side channel,
  plus separate cancellation coordination between the response pump and the observer. The pinned
  orihsus bounded SSE parser (`src/gateway.rs`) is adapted instead, with a tiny bounded terminal-name
  observation: an oversized discarded event's already-buffered prefix is scanned at its completed delimiter
  for a line-start `event:` field (last field wins), so an oversized terminal event keeps its terminal
  outcome while a partial event truncated before the delimiter is never delivered.

## 5. Audit (canonical request audit)

At the end of its lifecycle, each accepted request **attempts to produce at most one** `kind=request` record.
compact is an independent HTTP request and produces its own audit line (same schema,
`operation=compaction`); it is not merged into the `/responses` requests before or after it.

**No retry/request-id deduplication**: every HTTP request that actually arrives and becomes an accepted request
independently attempts to produce a request audit. Codex client retries can genuinely produce additional upstream usage;
merging by request id would undercount. `event_id` is used only for import idempotency by a future importer
and does not mean the gateway merges any requests.

Schema (canonical, allowlist-based):

| Field | Type / enum |
|---|---|
| `schema_version` | Required, fixed `1` |
| `kind` | Required, fixed `request` |
| `event_id` | Random, globally unique (for import idempotency) |
| `timestamp` | UTC RFC3339, anchored to the end of the request lifecycle (the moment outcome is determined) |
| `machine_id` | Stable identifier derived from the key mapping |
| `operation` | `response \| compaction` |
| `upstream_status` | `null \| integer` |
| `outcome` | `completed \| incomplete \| upstream_error \| transport_error \| upstream_interrupted \| client_cancelled` |
| `model` | `null \| string` |
| `accounting_quality` | `complete \| partial \| inconsistent \| unavailable` |
| `metering_error` | `null \| missing_usage \| malformed_usage \| event_too_large` |
| `usage` | `null` or object `{input_total, uncached, cache_read, cache_write, output_total, reasoning, total}`, each number may be null |

Scenario mapping:

- The upstream returns any HTTP response (including 4xx/5xx): pass it through unchanged and record `upstream_status` with the corresponding outcome
  (non-2xx records `upstream_error`).
- Connection/DNS/TLS and other transport failures without an HTTP response: the gateway generates a safe 502
  and records `transport_error` (`upstream_status=null` in this case).
- Stream interrupted mid-flight: record `upstream_interrupted`.
- Client cancellation: stop pumping further to the upstream and record `client_cancelled`;
  usage is included only if terminal usage was already received before cancellation, otherwise `usage=null`, no estimation.

Privacy red line — the audit absolutely never contains: session_id, OpenAI request_id, ChatGPT-Account-ID,
OAuth/token, X-Meter-Key, IP, request/response bodies, raw headers, raw usage JSON, or complete terminal SSE.
Only the allowlisted canonical fields are stored (schema/kind identifiers, event id, timestamp, machine and model identifiers,
terminal enumerations, and token counts); raw usage JSON or complete terminal SSE is never included; `schema_version` is required.
Correlation uses only the `event_id` generated by the gateway itself.

## 6. fail-open / fail-closed

- **Startup fail-closed**: invalid configuration, or the audit file cannot be created/opened → exit immediately,
  to avoid starting when metering is known to be impossible.
- **Runtime fail-open**: transient audit write failures after a successful startup do not block the proxy.
  Acceptance invariant: compared to when auditing works normally, the upstream status, semantic headers, and
  body bytes and ordering received by the client are completely unchanged; audit records may be lost.
- Metering parse failures, malformed data, and audit failures must never change the already-received upstream
  status/header/body, and must never proactively cancel a request that could otherwise continue.
- Runtime diagnostics: emit sanitized events to stderr without machine/request/credential information
  (`audit_write_failed` / `audit_dropped`); the MVP has no metrics system, dashboard, or detailed labels.

## 7. Storage semantics

- JSONL append-only, best-effort: at most one attempt per accepted request; no exactly-once promise.
- A process crash may lose the last record or leave a trailing partial line; a future reader must ignore unfinished trailing lines.
- No fsync-per-record, no built-in rotation, no SQLite. In the low-traffic phase, operations observe the file directly;
  rotation will be designed separately later.
- Runtime JSONL and aggregate reports are private machine-local data by default and are never automatically published or committed;
  the repository contains only the schema, synthetic examples, and test fixtures.

## 8. Deployment boundary and resource constraints

- Form: a single Rust binary listening on loopback HTTP/1.1; nginx terminates TLS/HTTP2 at the edge.
- nginx/systemd configuration belongs to deployment documentation and deployment acceptance, and does not enter the core proxy's behavior tests.
- jemalloc is the default allocator for Linux release builds, with no parameters tuned.
- The following are **correctness constraints** (in-spec): SSE streaming response protocol correctness, opaque request streaming,
  bounded bypass parsing buffers, and bounded audit queues — these fulfill the promise that "parsing/audit must not drag the proxy down".
- Deferred to future work: global body budget, admission queue, per-client/global concurrency control,
  systemd MemoryMax/stream slots tuning, and performance-benchmark metrics. Performance optimization is measurement-driven.
- Fallback to direct connection: a purely operational action (switching the Codex provider back to direct connection); automatic failover is not implemented.

## 9. MVP scope and non-goals

**Included**

1. Fixed-route transparent proxy (Responses/compact, SSE+JSON).
2. Per-machine X-Meter-Key authentication with a server-side machine_id mapping.
3. Responses/compact usage extraction with canonical best-effort JSONL audit.
4. Minimal health endpoint.
5. Linux deployment examples (synthetic nginx/systemd/config example).

**Excluded (explicit non-goals)**

Price/cost calculation, daily reports, SQLite/DB, UI, local-log importer, reconciliation,
quota snapshot/dashboard, automatic failover, user/tenant/quota/concurrency control, and a metrics system.
All of the above are future independent outcomes and need a new design and issue before they enter.
In particular, if a quota/rate-limit snapshot is added in the future, it must use a record with a separate kind
and must not be packed into a `kind=request` usage line.

## 10. TDD seams (pre-confirmed)

Behavior tests observe only the following three public seams:

1. **HTTP seam**: a Codex-like caller through the gateway to a fake upstream,
   observing auth, status, semantic headers, body bytes and ordering, cancellation, and failure paths.
2. **audit seam**: after a request completes, observe the canonical JSONL content or explicit fail-open behavior.
3. **startup seam**: given a synthetic configuration, observe startup success or fail-closed exit.

The fake upstream is a test adapter, not an additional product seam.
Real OpenAI/Codex upstreams are used only for manual protocol PoCs and do not enter normal CI;
real captures must not be committed; only minimal, de-identified, synthetic SSE/JSON fixtures constructed by hand from them.

## 11. Questions pending manual PoC ruling

The following issues do not block specification or test writing, but they do block confirmation of production usability:

- [ ] Content-encoding distribution of real Codex requests.
- [ ] Whether the response terminal object stably carries `model`.
- [ ] Whether the `response.done` event actually exists (determines whether it is included for compatibility).
- [ ] Typical size of real terminal SSE events, used to choose the bounded parse limit value.
- [ ] Confirmation of the real response shape of the compact route.

## 12. Initial outcome issues

1. A Codex caller holding a valid machine key can get semantically transparent responses through the fixed Responses/compact routes,
   and unauthorized callers are rejected before touching the upstream.
2. After each accepted request completes, the operator gets one canonical JSONL request record
   with no sensitive data and explicit semantics for completed/incomplete/error/cancel.
3. When metering parsing or audit storage fails, the upstream response available to the Codex caller stays unchanged,
   and the operator gets a safe diagnostic.
4. The operator can start the gateway on a Linux VPS using the published synthetic nginx/systemd/config examples,
   check health, and manually switch back to direct connection.

Within each issue, work in vertical red→green slices per behavior (see [AGENTS.md](../AGENTS.md));
do not split tasks by file or layer.
