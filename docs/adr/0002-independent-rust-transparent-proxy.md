---
status: accepted
date: 2026-08
issue: 1
---

# ADR-0002: Independent Rust transparent proxy, rather than extending an existing project

## Background

The community already has proxy implementations such as orihsus (multi-key-pool rotating gateway), litellm, and codex2api.
This project's need is single-account OAuth pass-through plus usage recording.

## Decision

Build an independent single-binary Rust transparent proxy. Do not fork or modify any existing product.

## Consequences

- The requirement surface (no key pool, no rotation cooldown, no multi-provider protocol translation) does not match the core complexity
  of orihsus/litellm, so an independent component can keep the simplicity of security constraints such as a fixed upstream and closed routes.
- Protocol compatibility tracking and manual protocol PoCs must be maintained ourselves.
- jemalloc as the default allocator for Linux release builds is an accepted low-cost baseline and is not a point of contention in this decision.

Extending orihsus (key-pool/quota-cooldown semantics are orthogonal to this scenario) and adopting litellm
(the subscription side strips usage fields and introduces a multi-provider translation layer) were both rejected.

## Amendment (2026-08, issue #2)

The original wording treated community implementations strictly as "design evidence only". Per AGENTS.md
(Community reuse) and the issue #2 ruling, that restriction is superseded for compatible building blocks:
the gateway deliberately reuses or selectively ports compatible stream pump, SSE/JSON usage parsing, and
JSONL writer code from a pinned `ariga39/orihsus` revision rather than rebuilding them, while never
importing orihsus's key-pool/retry/quota/product semantics. "Independent component" refers to the product
shape (a standalone binary), not to requiring independently reimplemented plumbing.
