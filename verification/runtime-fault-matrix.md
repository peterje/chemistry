# Durable runtime fault matrix

The deterministic store adapter can fail immediately before a transaction commit or immediately after the commit while reporting failure to the caller. The turn adapter can pause after execution starts and before its first event, allowing a fault to be placed at append boundaries without sleeps.

| Transition                   | Before-commit evidence                                                           | After-commit evidence                                                       | Convergence invariant                                                                          |
| ---------------------------- | -------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Admission                    | `admission converges…` leaves no runtime record                                  | Same test persists one operation but reports failure                        | Retrying the same submission ID returns the existing operation; queue length remains one       |
| FIFO claim                   | `claim is retryable…` leaves operation queued                                    | Same test leaves claimed operation/generation durable                       | Retrying claims the same operation under a fresh fenced generation and completes once          |
| Lost queued runner           | `restores the oldest durably admitted turn…` drops ephemeral runners             | N/A—admission is already durable                                            | Wake exposes the FIFO head; alarm restoration executes queued prompts in order                 |
| Stream append                | `append never publishes…` retains no event                                       | Same test retains sequence 0 although producer saw failure                  | Uncommitted event is never emitted; committed/unacknowledged event is replayable               |
| Terminal commit              | Covered by ordinary failure/retry paths                                          | `terminal committed before acknowledgement…` reports failure after commit   | Re-running returns the one retained stream/terminal and does not duplicate operation or stream |
| Wake/incident creation       | Retrying a failed wake reclassifies the orphan                                   | `wake incident creation…` retains one incident despite reported failure     | Retry preserves incident identity, operation identity, and attempt zero                        |
| Replay-to-live registration  | `replay-handoff.test.ts` registers/parks before backlog drain                    | Same suite injects overlap and out-of-order live events                     | One strictly increasing sequence with overlap deduplication; overflow forces reconnect         |
| Recovery partial persistence | `durable-execution.test.ts` reconstructs and records partial before continuation | Transcript-before-runtime-terminal test simulates the opposite commit order | Deterministic assistant ID makes both windows converge without duplicate user/model turns      |
| Generation fence             | Stale producer test changes generation before second append                      | N/A—fencing is checked at mutation commit                                   | Only sequence 0 remains; stale sequence 1 is rejected                                          |
| Recovery terminalization     | Attempt/work/no-progress/OOM tests cross each finite budget                      | Repeated exhaustion call checks generation/terminal stay unchanged          | One durable failed terminal and no additional model execution                                  |
| Cleanup                      | Expired and over-count fixtures                                                  | N/A                                                                         | Active/interrupted streams remain; terminal streams are bounded and expired records disappear  |

Focused command:

```bash
bun test test/runtime-fault-matrix.test.ts test/durable-execution.test.ts test/replay-handoff.test.ts
```

Expected current result: 25 passing tests, 0 failures.
