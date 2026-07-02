# Event Consumer

Small standalone consumer for Agave broadcast event queues.

It joins a shaq queue as an untyped `SliceConsumer`, reads the matching `.frs`
flatrecord schema at startup, decodes queue payloads through
`flatrecord::DynamicRecord`, and prints one JSON object per event to stdout.
Event schemas are not compiled into this app.

Run against the validator default events directory:

```bash
cargo run -- --ledger-dir <LEDGER_DIR>
```

Or point directly at an events directory:

```bash
cargo run -- --events-dir <LEDGER_DIR>/events --from-backlog
```

The default queue name is `bank_events`, so this reads
`<events-dir>/bank_events` and `<events-dir>/bank_events.frs`. Use
`--queue-name <NAME>` for another queue. Use `--object <RECORD_NAME>` to print
only matching flatrecord record variants.

Each line is a JSON object keyed by record name, for example
`{"FrozenBankEvent":{"timestamp":25324,"slot":42,"bank_hash":[...]}}`.

`SIGINT` and `SIGTERM` request a graceful shutdown. The app exits its read loop
and drops the `SliceConsumer`, which releases the shaq consumer slot.
