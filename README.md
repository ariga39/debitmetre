# debitmetre

A central transparent proxy gateway that sits between multiple Codex clients and the single OpenAI Codex
upstream. For each accepted request lifecycle it best-effort attempts one canonical audit record — raw
token facts per machine and model — in an append-only JSONL file; usage or model may be absent per the
contract. It can also summarize what it recorded.

It does **not** compute prices, equivalent cost, or daily billing reports — those are future offline outcomes.

## Build, configure, run (local first run)

```sh
cargo build --release
cp config.example.toml ./debitmetre.toml
sed -i 's|/var/lib/debitmetre/usage.jsonl|./usage.jsonl|' ./debitmetre.toml
target/release/debitmetre --config ./debitmetre.toml
```

This first run needs no privileges: it binds the loopback listener and creates `./usage.jsonl` in the
current working directory. Relative paths in the config (like `usage_file`) resolve from the process
working directory. `--config` defaults to `/etc/debitmetre/config.toml`; use `debitmetre --help` for the
full usage text. Operational logs go to stderr (suitable for a terminal, journald, or an operator-selected
supervisor) and never print meter keys or request/response bodies.

## View accumulated usage

```sh
target/release/debitmetre summary --config ./debitmetre.toml
```

The summary aggregates the recorded token facts by machine and model and reports the overall metering
coverage of accepted request lifecycles. It does not calculate prices, equivalent cost, or daily billing.
For a system-wide install under an operator-selected supervisor, see [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Documentation

- **[docs/OPERATIONS.md](docs/OPERATIONS.md)** — configure, run, and operate the gateway: configuration and
  failure semantics, readiness and routing, the summary command, and the deployment boundary.
- **[docs/VERIFICATION.md](docs/VERIFICATION.md)** — verify a build: the opt-in mock load smoke, the opt-in
  real-Codex loopback self-test, prerequisites, and the process-level test.
- **[docs/DESIGN.md](docs/DESIGN.md)** — the authoritative current product specification (routing, auth,
  token accounting, audit schema, fail-open/fail-closed, scope and non-goals, TDD seams, pending PoCs).
- **[docs/adr/](docs/adr/)** — architecture decision records (hard-to-reverse trade-offs).
- **[docs/DESIGN-REVIEW.md](docs/DESIGN-REVIEW.md)** — design-grill process record and superseded draft statements.
- **[docs/research/community-codex-proxies.md](docs/research/community-codex-proxies.md)** — dated community
  research; historical evidence input only, not current requirements.
- **[CONTEXT.md](CONTEXT.md)** — project-specific glossary of ubiquitous-language terms.
- **[AGENTS.md](AGENTS.md)** — collaboration rules (delivery-first behavior-based TDD, issue→PR→merge,
  public-repo privacy red lines, community reuse).
