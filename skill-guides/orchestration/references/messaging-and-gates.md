# Messaging and gates

Handle a whole `check` batch before the next `check`.

Groups (`@all`, `@idle`, `@codex`, ...) reach the live workers of your Run;
`@worktree:<id>` also reaches coordinators in that workspace. Use groups only
for deliberate fan-out.

To reach an agent on a paired server, use `send --on <server>`; a reply to mail
from another server needs no flag. See
[Placement and remote execution](placement-and-remote.md).

## Selective acknowledgment

After inspecting mail with `check --peek` or `inbox --full`, acknowledge only
explicitly selected messages with:

```text
ORCA orchestration check --terminal <handle> --ack-ids "<msg_id>,<msg_id>" --json
```

This never takes another batch. `markedIds` lists newly handled messages;
`alreadyReadIds` lists selected messages already handled, making retries safe.
Unselected mail, including questions arriving after inspection, stays unread.
Every ID must exist in the caller's current mailbox (direct, bound Run or
Dispatch); an unknown, foreign or legacy ID rejects the entire selection.
An implicitly bound child Run can also acknowledge pre-bind Dispatch mail while
it remains that Dispatch's current consumer, even with an outstanding Run delivery.
A bound Run selection never includes the caller's own direct mail. To select
only unbound direct mail, specify `--run run_unbound`.
If any selected ID is a lifecycle report (`worker_done` or `heartbeat`), or any
selected unread message is held by a structured chat, the entire selection is
refused. Use a normal check for that mail.
The CLI supplies the required request ID. `--retry-request` only replays for the
original process or concrete live chat lease and mailbox generation. If either
changed, inspect the current mailbox again.
Released agent sessions cannot acknowledge wake-up mail until they are running
again. Do not use this to discard mail the agent has not processed.

Do not combine `--ack-ids` with other read, acknowledgment, formatting or wait
options. Both the CLI and the execution host need selective-ack support; older
hosts and the legacy SSH bridge are refused before consumption.
Selective acknowledgment uses the same mailbox authorization as a normal
consuming `check`. Peer-Orca federation cannot bind a receipt on the execution
host and is refused.
`request-show` reports selective request status only; to recover its outcome,
run `check --ack-ids` with the same selection and `--retry-request` as the
original caller. A refused retry did not mark mail on that attempt; its error
does not tell you whether the original request completed.
