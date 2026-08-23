---
status: accepted
date: 2026-08
issue: 1
---

# ADR-0001: Metering boundary at the central gateway

## Background

Requests from the same Codex account originate on multiple local machines and VPSs and ultimately converge on the single upstream.
Metering could be collected locally on each machine, or placed at the single convergence point of all traffic.

## Decision

Adopt the central gateway: each upstream response is observed exactly once, machine identity is determined server-side by the gateway,
and all machines share the same event schema, clock, and accounting basis.

## Consequences

- A unified, real-time ledger that needs no cross-machine synchronization or deduplication arises naturally; adding a machine only requires configuring the provider and key.
- The gateway becomes a single point of availability, so metering must fail-open, and the operational path of manually switching back to direct connection is kept.
- Local Codex JSONL is only a potential reconciliation reference source, not the primary metering source.

Per-machine local collection (NerfTrack-style) was rejected because it reintroduces deployment, resumable transfer, rotation, duplicate events, and
offline aggregation problems.
