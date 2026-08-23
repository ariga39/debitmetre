# Codex Proxy project collaboration rules

This file records only the rules that apply to the project itself and can be shared publicly with all contributors. Machine-local orchestration tooling and credential-operations conventions are not repository content.

Functional delivery comes first: process, documentation, review, and compliance checks must stay proportional to actual risk and must not dominate a small personal project.

## Behavior-based TDD

- Development follows behavior-based TDD. Tests describe behavior observable through the public interface; they do not test private methods, internal call counts, or implementation structure.
- Record the expected behavior and acceptance criteria in the issue before implementing; no approval is required before writing tests.
- Use small vertical loops: for one behavior, first write a failing test (red), then the minimal implementation that just satisfies that behavior (green), then move on to the next behavior.
- Do not break an issue into many implementation details or restrict the specific implementation approach; as long as all expected behaviors, constraints, and quality standards are met, that is enough.
- Do not use horizontal slicing that "writes all tests first, then all implementation". Refactoring belongs in the review stage after a behavior passes, keeping external behavior unchanged.
- Test expectations must come from the specification, protocol fixtures, or independent known results; they must not re-implement the same algorithm and form tautological tests.

## GitHub issue → PR → merge

- Split issues and tasks by observable behavior, not by files, layers, functions, or exhaustive edge-case matrices.
- Deliver the smallest useful behavior with behavior-based TDD and no speculative scope.
- Link the implementation branch and PR to the corresponding issue, and keep issue and PR descriptions useful but lightweight.
- Once the required basic behavior works and its focused tests pass, commit and open the PR promptly. Do not hold the PR for exhaustive hardening, refactoring, compliance work, or imagined edge cases.
- Missing required functionality, real regressions, or exposed secrets are blockers. Non-critical bugs, enhancements, extra edge cases, cleanup, and process improvements become explicit follow-up issues rather than silently expanding or indefinitely delaying the current PR.
- Merge after the basic required behavior and relevant tests pass; do not require ceremony for its own sake.

## Public repository and privacy

- The repository is public by default; any commit is immediately public. Never commit real secrets, tokens, OAuth credentials, certificate keys, or any recoverable form of them, or machine-specific or personal data (real usernames, home directories, hostnames, IPs, machine IDs, account IDs, or identifiable logs or fixtures).
- Use synthetic placeholders such as `gateway.example.com` or `machine-a` when example values are needed.

## Scope discipline

- Do not implement functionality beyond the issue specification in advance.
- Performance optimization is driven by actual measurement. Currently do not add per-client quotas, concurrency policies, admission queues, or complex memory scheduling in advance.
- jemalloc is an accepted low-cost runtime baseline and is not regarded as premature optimization requiring re-argument.

## Community reuse

- An independent gateway is a product/deployment shape, not a requirement to implement everything independently.
- Before building protocol forwarding, streaming pumps, SSE/usage parsing, audit writing, or similar infrastructure, first look for compatible mature community libraries and reusable source. `ariga39/orihsus` is a preferred concrete reference and source of compatible code.
- Reuse or selectively port compatible code instead of reinventing it. Write bespoke code only when available implementations conflict with required behavior or impose clearly disproportionate baggage; record the short technical reason.
- When source is copied or adapted, pin its revision and preserve the required license/notice. Treat this as a small necessary step, not the center of the task.
