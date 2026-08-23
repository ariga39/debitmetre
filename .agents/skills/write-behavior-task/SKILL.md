---
name: write-behavior-task
description: Write or revise a GitHub Issue, delegated task, acceptance contract, or implementation brief around one observable outcome. Use when selecting work, handing an outcome to another agent, or correcting a task that prescribes files, functions, layers, or implementation steps before the behavior is owned.
---

# Write a behavior task

Frame one coherent result that an owner can deliver without returning for routine implementation choices.

1. **Outcome:** name the actor or caller, the behavior that changes, and the observable result. Explain why this is the highest eligible work when an active falsifier or accepted-outcome blocker exists.
2. **Authority:** link the Issue or user decision, record the exact base when relevant, and name the behavior-cluster owner. State permitted external effects and decisions that still require new authority.
3. **Current contract:** preserve applicable product invariants, compatibility promises, and known failure evidence. Link canonical sources instead of copying their history.
4. **Non-goals:** exclude independent outcomes and unauthorized effects. Do not use non-goals to freeze poor internals or prevent the owner from touching a layer required for coherence.
5. **Evidence:** require the smallest shipped entry path or owning-interface observation that could disprove the outcome, plus applicable project checks and independent review. Do not substitute file count, test count, a commit, or agent completion prose.
6. **Checkpoints:** define the first checkable draft and the final stopping condition. Checkpoints report progress; they do not request routine user supervision.

Give the outcome owner freedom to change every internal layer needed to deliver the behavior. Limit files only for a real permission, security, concurrent-ownership, destructive-action, or external-authority boundary. If a path list is useful for navigation, label it as expected evidence rather than writable scope.

Reject a task that:

- decomposes one behavior into database, service, repository, UI, test, function, or file tasks;
- prescribes an interface or abstraction before a current caller proves the seam;
- requires preserving obsolete wrappers or compatibility with no active promise;
- treats planning, scaffolding, local green tests, or PR creation as the outcome;
- asks the user to approve reversible implementation details or send “continue”;
- hides a direction decision in a temporary task file.

Before dispatch, verify that one agent can state the outcome without reciting the proposed implementation, can revise all necessary internal layers, and knows exactly what evidence ends the task.
