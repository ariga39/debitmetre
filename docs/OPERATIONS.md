# debitmetre — operator guide

This document is the operator-oriented companion to [DESIGN.md](DESIGN.md): it explains how to configure,
run, and operate the gateway. DESIGN.md remains the authoritative product specification; where behavior is
specified there, this guide focuses on how to use it.

## Build and run

Build the single binary and run it against a local copy of the example configuration:

```sh
cargo build --release
cp config.example.toml ./debitmetre.toml
sed -i 's|/var/lib/debitmetre/usage.jsonl|./usage.jsonl|' ./debitmetre.toml
target/release/debitmetre --config ./debitmetre.toml
```

This local run needs no privileges: it binds the configured loopback `listen` address and creates
`./usage.jsonl` in the current working directory. Relative paths in the config (like `usage_file`) resolve
from the process working directory. `--config` defaults to `/etc/debitmetre/config.toml`; use
`debitmetre --help` for the full usage text. Operational logs go to stderr (suitable for a terminal,
journald, or an operator-selected supervisor).

### Optional system-wide install

A system-wide install is optional and is left to the operator. Optionally install the single binary onto the
system, then arrange the config and data locations (create them and set their ownership) for whichever
runtime user and supervisor the operator chooses. The repository ships only the TOML example; it does not
ship or prescribe a supervisor, service unit, service user, or one universal account.

```sh
sudo install -m 0755 target/release/debitmetre /usr/local/bin/debitmetre
```

The operator must then create the configuration and usage-file directories, set their ownership to the
chosen runtime user, and place `config.example.toml` (with `usage_file` pointing at a path that user can
write) where the binary can read it as that user. For example, with a runtime user and paths of the
operator's choosing, `usage_file` could be `/var/lib/debitmetre/usage.jsonl`.

## Configuration

The `machine_keys` table maps the **SHA-256 digest** of each machine's meter key to a stable machine id.
Compute the digest for a meter key once, then paste the lowercase hex output:

```sh
printf '%s' 'test-meter-key-machine-a' | sha256sum
```

```toml
# config.example.toml — synthetic example; do not commit real meter keys
listen = "127.0.0.1:8787"

# Append-only JSONL usage file; opened at startup (fail-closed on bad path).
usage_file = "/var/lib/debitmetre/usage.jsonl"

[machine_keys]
"82805ec33616c4aa802f141d3703fb17213fd8ced358f3a62348d8cf6e1ce051" = "machine-a"
```

## Failure semantics

The gateway binds the configured `listen` address and refuses to start on invalid or unreadable
configuration (startup fail-closed): a missing file, malformed TOML, unknown fields, an invalid listen
address, a blank `usage_file`, an empty `machine_keys` table, a non-64-character lowercase-hex digest, or a
blank machine id all exit non-zero with a useful error. The configured `usage_file` is opened at startup the
same way: an invalid or unwritable path prevents startup.

After a successful startup, a transient audit write failure is **fail-open**: the caller-visible upstream
response stays unchanged and a sanitized `audit_write_failed` diagnostic is emitted to stderr. Request
lifecycle diagnostics may include the allowlisted `machine_id`, route/operation, `outcome`, and upstream
`status`; other diagnostics may include necessary non-secret operational context. None ever include
credentials, bodies, raw headers, account or upstream request identifiers, or client network metadata. See
DESIGN.md §6–§7 for the full contract.

## Logging and privacy

Operational logs go to stderr. Request-lifecycle diagnostics may include the allowlisted stable
`machine_id`, the request `operation` (route), the `outcome`, and the upstream `status`. Other diagnostics
(startup, configuration, and shutdown) may include necessary non-secret operational context such as event
names, the listen address, machine count, reason, and the config path. The following are **forbidden** from
any log or diagnostic: meter keys/credentials, `Authorization`/OAuth material, request or response bodies,
ChatGPT account or upstream request identifiers, raw headers, and client network metadata (for example IP
addresses). The audit record itself is governed by the stricter allowlist in DESIGN.md §5.

## Readiness and routing

```sh
curl -fsS http://127.0.0.1:8787/healthz    # 200 once configured and listening
```

Codex clients reach the gateway through the authenticated transparent proxy routes: `POST /v1/responses`
and `POST /v1/responses/compact` with an `X-Meter-Key` header. The upstream is fixed in code and redirects
are never followed. Unknown paths return 404 and wrong methods return 405; neither is forwarded.

## View accumulated usage

Run one local command with the same configuration to see the recorded token facts accumulated in the
configured usage file, grouped by machine and model:

```sh
target/release/debitmetre summary --config ./debitmetre.toml
```

The output is a fixed-width table (machine, model, record count, then input, uncached, cache-read,
cache-write, output, reasoning, and total tokens). Totals sum only the values actually recorded; a counter
that was never recorded shows `-` and is never invented as 0, and no prices are computed. A final coverage
line reports the overall metering coverage of accepted request lifecycles: every valid canonical record
contributes one accepted lifecycle, and one whose record carries a non-null usage object counts as metered
(partial usage still counts). An unfinished trailing line left by a process crash is ignored with a warning
while earlier complete records are still summarized. Warnings go to stderr.

The summary is a local read of the audit file; it does not calculate prices, equivalent cost, or daily
billing reports.

## Deployment boundary

The gateway is a single binary that binds the configured `listen` address and writes to stderr logs suitable
for an operator-selected supervisor. Deployment-manager and edge-proxy (for example nginx/systemd)
configuration are outside this repository; the repository ships only the TOML example. See DESIGN.md §8.
