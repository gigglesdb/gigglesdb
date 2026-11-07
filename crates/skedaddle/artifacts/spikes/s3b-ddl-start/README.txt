GD-0049 / Q14 / S3b: transactional ddl_command_start + ddl_command_end.
Run from K:\Projects\gigglesdb\crates\skedaddle:
  python -B artifacts/spikes/s3b-ddl-start/campaign.py <fresh-run-id>
  python -B artifacts/spikes/s3b-ddl-start/analyze.py <fresh-run-id>
No host deletion, external writes, commits/pushes or subagents. Archdocs is unused;
any future write MUST explicitly bind/verify space 5714972c-8f62-4f17-8ebc-b62fbf4420b9.
S3's decoder/framing/SCRAM is reused verbatim; added query logging records every
protocol SQL batch in each version's commands.jsonl. Actual messages and complete
Begin/Commit envelopes are in events.jsonl. Not a production applier/parser.
S4 guard/up/export/down/check only, six services, sequential PG16/17/18, all own
resources labeled skedaddle.test=<run-id>-pg<major>. No cross-run sweeper.
Cargo locked/offline; jobs=2; target=target/s3; cargo-home=S1's; TEMP/TMP=target/s3.
All source writes here; all evidence under artifacts/s3b-<run-id>/pg<major>.
S4 up creates the per-version evidence directory before save() is first invoked.
Each split test holds a reader/writer through the first catalog transition and a
decoded barrier; catalog xmin is equated to start's Begin/Commit XID. The observer
WAL LSN is an upper bound on that commit, not substituted for its decoded LSN.
Unique failure: commit a concurrently inserted duplicate while CIC waits before
its first scan. It then fails with 23505, after committing its invalid index.
Cancellation: pg_cancel_backend while DETACH waits; then recover via FINALIZE.
Session set_config(...,false) carries pid:sequence id across internal commits.
Only top-level commands tested; nested DDL needs a backend statement-token stack,
not this single-GUC prototype. Include installation/database epoch in real IDs.
Strict mode deliberately rejects ALL ALTER TABLE/CREATE INDEX tags, independent
of raw SQL. It is a coarse fail-closed proof, including published target/part;
not a selective mapped-table implementation. No regex guard claimed safe.
At start, capture actual pg_event_trigger_ddl_commands() row count/error. An empty
result is not assumed to be an error, despite the documented end-only contract.
All retained source/evidence/build files are intentionally preserved.
