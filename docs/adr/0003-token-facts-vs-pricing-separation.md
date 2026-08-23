---
status: accepted
date: 2026-08
issue: 1
---

# ADR-0003: The gateway records raw token facts only; pricing and reporting stay separate offline

## Background

The goal is to convert subscription usage into an equivalent cost. Price catalogs change, while token facts are the only thing
the gateway can reliably observe at request time.

## Decision

The gateway MVP records only canonical, quality-marked token facts (raw token facts);
cost conversion, price-catalog versioning, daily reports, and aggregation are all future independent offline outcomes,
not gateway behavior.

## Consequences

- Historical cost can be recomputed at any time from the same token facts after a price-catalog update, without the gateway needing to know any prices.
- The gateway keeps a minimal attack surface and privacy surface: the audit contains only allowlisted canonical fields
  (schema/kind identifiers, event id, timestamp, machine and model identifiers, terminal enumerations, and token counts),
  which naturally satisfies the privacy boundary between the public repository and runtime data.
- The cost is that "viewing cost" needs one extra hop (fetch the JSONL first, then compute offline),
  and in the MVP phase the operator can only see token counts rather than amounts — this is accepted deliberately.

Embedded pricing/daily reports inside the gateway were rejected because they let an external variable (prices) invade the hot-path audit semantics
and force the audit schema to carry mutable snapshot data.
