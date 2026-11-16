`multi-partition-set.json` is hand-built fault injection derived from the
recorded GoogleSQL row used by `multi-partition.json`. It contains two records
for the same transaction, with sequences `00000000` and `00000001`, on explicit
partition labels `fault-partition-a` and `fault-partition-b`. Both declared
transaction counts are 2. Each record is marked last in its own partition;
the second has one mod with key `parent_id=103`. Other scalar metadata and row
shapes come from the original capture. The outer partition/record objects are
a test-only envelope; each `record` uses the production decoder.

The emulator always reports one partition per transaction. This fixture proves
multi-partition assembler faults, not multi-partition emulator behavior.
`recorded_real_dialects_assemble_completely` exercises the assembler using the
unmodified `google.json` and `postgres.json` captures. Live Capture adapter and
restart qualification remain required before landing S5.
