# Event Consumer

Small standalone consumer for Agave broadcast event queues.

It joins a shaq queue as an untyped `SliceConsumer`, reads the matching `.bfbs`
schema at startup, decodes length-prefixed FlatBuffer table payloads through
`flatbuffers-reflection`, and prints one JSON object per event to stdout. Event
schemas are not compiled into this app.

Run against the validator default events directory:

```bash
cargo run -- --ledger-dir <LEDGER_DIR>
```

Or point directly at an events directory:

```bash
cargo run -- --events-dir <LEDGER_DIR>/events --from-backlog
```

The default queue name is `bank_events`, so this reads
`<events-dir>/bank_events` and `<events-dir>/bank_events.bfbs`. Use
`--queue-name <NAME>` for another queue. Use `--object <TABLE_NAME>` to decode a
specific reflected table instead of the schema root table.

`SIGINT` and `SIGTERM` request a graceful shutdown. The app exits its read loop
and drops the `SliceConsumer`, which releases the shaq consumer slot.
