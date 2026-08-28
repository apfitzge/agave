# Block verification stage

This note describes the design introduced on this branch after its fork point
(`6bb264b372`). The stage moves replay verification and transaction execution
behind one scheduler thread, `solBlkVerif`.

## Communication and threads

ReplayStage (`solReplayStage` and its optional `solReplayForkNN` pool) talks to
the scheduler over two shared-memory SPSC queues. Replay sends `BEGIN`, entry
headers and serialized transactions, followed by `COMPLETE` or `ABORT`. The
scheduler returns one terminal slot status: success, invalid entry hash,
invalid transaction, or aborted.

The scheduler communicates with four worker groups:

- Entry-hash workers (`solEntryHashNN`) use bounded job/result channels to
  verify each entry from the slot's rolling last-entry hash.

- Signature workers (`solBvSigvrNN`) use shared MPMC queues to verify every
  transaction's signatures and forward verified simple votes.

- Check workers (`solBvCheckNN`) use shared MPMC queues. They look up the
  slot's bank, parse and sanitize each transaction, resolve address lookup
  tables and account metadata, and estimate cost.

- Execution workers (`solBvCoWorkerNN`) each have their own SPSC request/result
  queues. They look up the slot's bank, execute and commit replay transactions,
  and report whether each transaction was included.

Transaction data and responses live in a common shared-memory allocator; queue
messages transfer offsets and ownership rather than copying between stage
threads. Workers read the appropriate `Bank` from the shared `BankForks` by
slot. They also retain the existing transaction-status and replay-vote channel
side effects. An optional shared-memory broadcast carries tracing events but is
not part of the control path.

## Verification pipeline

`solBlkVerif` is an orchestrator. For every entry it submits hash-chain work to
an entry-hash pool. For every transaction it independently submits signature
verification and a transaction check. Checked transactions become executable
in ledger order; non-conflicting transactions may execute concurrently. The
scheduler uses per-slot account locks and chooses an execution worker using
account affinity, estimated compute cost, lock count, and worker backlog.

There are eight signature workers and eight check workers. The entry-hash and
execution pools each contain `--replay-transactions-threads` workers. Entry-hash
verification falls back to the scheduler thread only if its bounded job queue
cannot accept work. ReplayStage still performs tick verification before it
submits entries; fork choice is also outside this stage.

A slot is successful only after `COMPLETE` and after all entry-hash, signature,
check, and execution work has drained. Any invalid hash, signature, transaction
check, or non-included execution marks the slot failed, but already-dispatched
work is still drained before the final status is returned.

## Fork management

The scheduler does not model fork ancestry or choose a fork. `BEGIN` creates an
independent `SchedulingState` keyed by slot and records its bank ID; several
slot states can be in flight, and their work is serviced in ingress order.
Account locks are per slot, so separate forks do not conflict with each other.
The replay fork threads share the replay-side session behind a mutex, which
serializes queue submission and status polling while the scheduler continues
processing all submitted slots asynchronously.

ReplayStage owns rooting, repair, purge, and fork removal. Before removing a
bank from `BankForks`, it asks block verification to finish that slot. An
in-progress slot is sent `ABORT`; the scheduler stops accepting or dispatching
new work, waits for all outstanding worker references to return, frees the
slot's shared allocations, sends the aborted status, and only then allows the
bank to be removed.
