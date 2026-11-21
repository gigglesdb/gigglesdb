# Native multi-row statements

Native INSERT MANY and DELETE MANY now run as one engine statement. Their
foreign-key behavior matches the equivalent SQL multi-row statement, including
NO ACTION checks at statement end and one statement snapshot under READ COMMITTED.

These valid end states now succeed:
- Insert self-referencing rows (3 -> 4), (4, NULL) together.
- Delete both self-referencing rows (1, NULL), (2 -> 1) together.
- Delete parents 1 and 2 when child 10 references parent 1 with CASCADE and
  grandchild 100 references child 10 with NO ACTION and parent 2 with CASCADE.
  The three tables end empty.

Native admission checks, canonical primary-key RETURN order, expected row
versions and sequence durability remain in effect. UPDATE MANY keeps its
existing row-at-a-time execution.

Single-row native DELETE uses the same statement path. All target versions,
including missing targets, are checked before any writes; a stale target returns
SQLSTATE 40001 (the SQL error variant) and wins over other rows' constraint errors.
The affected-count Conflict check remains a defensive backstop.

Under READ COMMITTED, eligible inserts retain row locks even above 200 rows
(unlike SQL VALUES, which falls back above 200). Batches remain subject to the
normal lock/write budgets. Eligible native deletes with incoming foreign keys also
retain parent row locks above 200 targets, matching the previous per-row native
path; SQL deletes keep their 200-target limit. Cascaded children retain the existing
row locking and reread protocol. Retention-budget overflow and unsupported FK
graphs still use the conservative table lock. Native cascade deletes read parent
candidates by canonical primary key rather than scanning the parent table.

A DELETE through SnapshotContext now returns native Unsupported; previously it
returned the Core Unsupported variant. Snapshot contexts remain read-only.
