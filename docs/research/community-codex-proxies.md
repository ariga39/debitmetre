# Survey of Codex community proxies and usage-metering implementations

> **Status: historical evidence — dated 2026-08-23, non-authoritative, and not the current implementation plan.**
> This document is a snapshot of community research from the date above. It was written before much of the
> current behavior shipped, so its suggestions must not be read as current requirements or as the current
> roadmap. Some suggestions were superseded or rejected by later rulings; treat this file as provenance only.
>
> The only authoritative current design is [../DESIGN.md](../DESIGN.md) (per issue #1 and the batch grill rulings,
> process in [../DESIGN-REVIEW.md](../DESIGN-REVIEW.md)).
> Wherever this document conflicts with DESIGN.md it has been superseded by the official design, in particular:
>
> - quota/rate-limit snapshot collection, global body budget, MemoryMax/stream slots sizing,
>   benchmark metrics → moved into future work; the MVP keeps only correctness constraints such as bounded parse buffers and bounded audit queues.
> - daily reports/reconciliation/importer → excluded from the gateway MVP, as future independent outcomes.
> - Token accounting follows the mutually exclusive buckets + accounting_quality of DESIGN.md §4;
>   the audit line model follows the per-request lifecycle record of §5.

Date: 2026-08-23

## Conclusion

The project should keep the central gateway and implement an independent small transparent proxy in Rust. The central deployment is not about introducing a complex control plane, but because the same Codex account issues requests from multiple local machines and VPSs: recording events at the single upstream egress naturally yields a unified, real-time ledger that needs no synchronization. If instead each machine read local logs, deployment, resumable transfer, rotation, duplicate events, machine offline, and cross-machine aggregation would all have to be solved additionally.

Reuse the design experience of community projects rather than directly depending on or forking any full product:

- From orihsus, borrow the bounded resource model, streaming forwarding, audit writer, and nginx/systemd deployment approach.
- From NerfTrack, borrow cumulative-count-to-incremental conversion, event fingerprints, deduplication, quota snapshot, and price-catalog versioning.
- From CLIProxyAPI, borrow normalized token buckets and data-quality markers, but do not introduce a multi-provider protocol translation layer.
- codex2api and LudwigAJ/codex-proxy can serve as references for the Responses/compact route and streaming transfer, but they rewrite or buffer requests and are not suitable as the direct basis for a transparent metering gateway.

## Why the central gateway is the correct boundary

This system's deployment topology is multi-source, single-upstream: Codex on several local machines and VPSs all ultimately hit the same OpenAI Codex backend. Placing metering at the convergence point has several direct benefits:

1. Each upstream response is observed exactly once; local JSONL does not need to be uploaded and then deduplicated.
2. Machine identity is mapped server-side from the independent gateway key; clients cannot arbitrarily fill in a display name.
3. All machines use the same event schema, clock, and price catalog, so daily reports use a consistent accounting basis.
4. Local machines going offline does not affect already-generated events being recorded; adding a machine only requires configuring the provider and key.
5. quota/rate-limit information comes from real response headers or SSE events, so the central gateway can also produce a global quota snapshot.

The cost is that the gateway becomes a concentration point for availability and memory pressure. The proxy must therefore bypass metering failures, bound resources, and keep a configuration to quickly switch back to direct connection; it cannot rely on the assumption that "Rust does not easily OOM".

## orihsus: the parts most worth reusing for a small VPS

orihsus uses Rust, Axum, Reqwest, and Tokio. Its key value is not the language but the explicit resource caps:

- Beyond a per-request body cap, there is a global body byte budget; a request acquires its budget before reading the body, avoiding memory spikes from concurrency multiplied by maximum body size.
- Successful responses are forwarded as a byte stream, with backpressure applied through a bounded channel, and downstream cancellation and write timeouts are handled.
- SSE bypass parsing keeps only a single event of bounded size; the audit uses a bounded queue and a single writer, dropping audits when the queue is full or on disk failure rather than blocking the proxy.
- nginx handles TLS/HTTP2 at the edge; the application listens only on loopback HTTP/1.1, avoiding unnecessary protocol and connection complexity in the application.
- The project's load tests show that the system allocator can retain RSS even after application objects are freed; it eventually adopted jemalloc. This shows that either Go or Rust can exhibit OOM/RSS problems due to large bodies, concurrent copying, or allocator retention.

orihsus's default budgets are tuned for its own environment and cannot be used as-is for a small VPS. This gateway should derive stream slots, body budget, and systemd MemoryHigh/MemoryMax backwards from VPS memory.

Note: the `response.completed` event of Responses may contain the complete response; the fixed 256 KiB event cap must be validated by a real-traffic PoC. When the cap is exceeded, the bypass parser must give up metering that item rather than growing into an unbounded cache or affecting forwarding.

## NerfTrack: suitable for reusing the data model, not for replacing the central gateway

NerfTrack is a local-first Rust/Tauri usage application. Its local JSONL collector handles byte-offset checkpoints, file truncation/rotation, partial-line recovery, and event deduplication well, and converts a session's cumulative tokens into increments. Its SQLite model also distinguishes usage event, quota snapshot, and pricing snapshot, and can recompute historical costs after price-catalog changes.

These capabilities are suitable as a reference for the gateway audit format and the offline reporter. But in a multi-site, multi-machine scenario, deploying a NerfTrack-style collector to every machine reintroduces synchronization and deduplication problems. A better division of labor is: the central gateway produces canonical events; the offline reporter adopts NerfTrack's event fingerprints, price versioning, and recomputation approach. If disaster recovery is needed in the future, local Codex JSONL serves only as a reconciliation data source, not the primary metering source.

## Token accounting correction

The draft's `uncached_input = input_tokens - cached_tokens` is not rigorous enough when cache write exists. Input tokens should use mutually exclusive buckets:

```text
input_total = uncached + cache_read + cache_write
```

The event also stores `accounting_quality = complete | inconsistent | partial`. When details are missing, cost is not guessed; only the upstream raw counts and quality state are stored. Cost calculation belongs to the offline cold path and handles cache read, cache write, service tier, and possibly long-context pricing per the versioned price catalog.

The official OpenAI Codex source already parses cache-write tokens, as well as `x-codex-primary-*`, `x-codex-secondary-*`, and `codex.rate_limits`; therefore the audit model should split token usage and quota snapshot into two kinds of events.

## Suggested implementation slices (historical, superseded as a plan)

These slices were a research-time proposal for how the MVP might be built, written before the current
implementation. They are **not** the current plan and some were later rejected or superseded (for example,
bounded global body budget and offline reporting were moved out of MVP scope). Current scope and TDD seams
are defined only by [../DESIGN.md](../DESIGN.md) §9–§10.

1. Protocol PoC: confirm the actual routes, request content-encoding, compact response, terminal SSE event size, and quota fields.
2. Fixed routes and machine authentication: allow only Responses/compact; keep the OAuth Authorization header, strip gateway-owned headers.
3. Bounded requests: raw body cap, global byte budget; if the bypass decompresses zstd, do so only under strict limits.
4. Transparent response pump: byte-by-byte forwarding, fixed-size chunks, bounded byte queue, cancellation and slow-client tests.
5. Bypass parsing: usage and rate-limit; parse failures and over-limit conditions do not affect the response.
6. Bounded JSONL writer: single writer, rotation, drop/error metrics; recording request/response bodies and credentials is forbidden.
7. Offline reporting: schema/version, event deduplication, price snapshot, and aggregation by machine/model.
8. Small-VPS load testing: verify large bodies, multiple SSE, disconnects, and disk failures under systemd memory limits.

Each slice proceeds in red—green—refactor; real upstreams are used only for protocol verification, and core tests use a controllable fake upstream.

## References

- [orihsus](https://github.com/ariga39/orihsus)
- [NerfTrack](https://github.com/NerfTrack/NerfTrack)
- [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI)
- [codex2api](https://github.com/Arocial/codex2api)
- [LudwigAJ/codex-proxy](https://github.com/LudwigAJ/codex-proxy)
- [Codex configuration reference](https://developers.openai.com/codex/config-reference)
- [OpenAI Codex Responses SSE parser](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/sse/responses.rs)
- [OpenAI Codex rate-limit parser](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/rate_limits.rs)
