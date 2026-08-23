# CONTEXT.md — project glossary

Only debitmetre-specific domain terminology is defined here, to unify team language.
Behavioral specification and implementation details: see [docs/DESIGN.md](docs/DESIGN.md).

**Machine**: a device running a Codex client and accessing the upstream through this gateway. Each machine has a stable `machine_id` in the ledger.

**Meter key**: the secret credential held by each machine; it is the sole basis on which the gateway determines which machine a request comes from. Machine identity is determined on the gateway side; the client cannot self-report it.

**Metering**: the gateway's observation and retention of the tokens consumed by a request. Metering only preserves facts; it does not guess or backfill numbers, and it does not represent an actual bill or subscription-credit consumption.

**Usage quality**: a marker of how trustworthy a usage datum is, reflecting whether the upstream detail is complete, consistent, or observable. It describes data quality, not amounts.

**Request audit record**: a record in the ledger corresponding to the terminal state of one accepted request's full lifecycle, optionally carrying the usage observed for that request. At most one record per request.

**Event id**: the unique identifier the gateway generates for each record; it is the only way records are correlated. Other identifiers, such as session or request headers, do not enter the ledger.

**Outcome**: the terminal-state classification of a request's lifecycle — normal completion, abnormal completion, various failures, or abandonment by the caller.

**Semantic transparency**: the gateway's promise to Codex clients: compared with connecting to the upstream directly, a request through the gateway receives a semantically equivalent response. It has an explicit boundary, and the boundary itself is specification content.

**Equivalent cost estimate**: a future monetary reference computed offline from token facts against the public API price catalog. It is an estimate, not a bill issued by OpenAI.
