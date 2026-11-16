google.json and postgres.json contain unmodified raw rows captured by
tests/spanner_stream.rs on 2026-11-15, run s1-4fc694885d68, from the harness's
pinned emulator (compose.yaml). Each keeps all data rows and the first heartbeat
and child-partition row. GoogleSQL STRUCTs are expanded using SDK metadata;
PostgreSQL JSONB stays a JSON string inside its result row.

multi-partition.json is fault injection, based on a captured GoogleSQL data row.
The transaction ID, tag, record count (2) and partition count (2) were explicitly
set. It is one record of a hypothetical two-partition transaction, not evidence
that the emulator supports multiple partitions per transaction or preserves tags.
The real emulator test exercises the same decoding path alongside this fixture.
