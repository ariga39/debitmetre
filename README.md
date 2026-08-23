# debitmetre

A central transparent proxy gateway: it sits between multiple Codex clients and the single OpenAI Codex upstream,
and records per-model, per-machine token usage (raw token facts) for offline cost estimation.
The MVP does not compute prices or produce reports; it only emits best-effort canonical JSONL audits.

## Documentation navigation

New contributors can read in this order to obtain the complete current design, without any external material:

1. [CONTEXT.md](CONTEXT.md) — project-specific glossary of ubiquitous-language terms.
2. [docs/DESIGN.md](docs/DESIGN.md) — **the only authoritative current design specification**:
   fixed routing and protocol behavior, X-Meter-Key authentication and machine mapping, token accounting basis,
   canonical audit schema, fail-open/fail-closed semantics, deployment boundary,
   MVP scope and non-goals, the three TDD seams, issues pending manual PoC, and the initial outcome issues.
3. [docs/adr/](docs/adr/) — architecture decision records (hard-to-reverse trade-offs):
   central gateway boundary (0001), independent Rust transparent proxy (0002),
   token facts vs. pricing/reporting separation (0003).
4. [docs/DESIGN-REVIEW.md](docs/DESIGN-REVIEW.md) — design-grill process record and superseded draft statements.
5. [docs/research/community-codex-proxies.md](docs/research/community-codex-proxies.md) —
   community-implementation research; evidence input only, DESIGN.md wins in case of conflict.

## Collaboration rules

[AGENTS.md](AGENTS.md): behavior-based TDD, GitHub issue → PR → acceptance → merge,
public-repo privacy red lines, scope discipline. Confirm the issue and the test seam before starting implementation.
