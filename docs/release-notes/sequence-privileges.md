# Sequence privileges are enforced when sequence calls run (V2)

**Security fix.** Before this change, the V2 engine checked sequence privileges only for sequence names it could see
when planning a statement. A sequence named at run time escaped the check: a parameter (`nextval($1)`), a text
expression (`nextval('s' || '')`), a call nested inside a function in a column default, or the column default of
a native INSERT (which had no sequence check at all). So a role without
privileges on a sequence could advance it with `nextval`, set it with `setval`, or read it with `currval`. The Legacy
engine and COPY already enforced the check.

## What changes for you

- **Sequence calls without privileges return SQLSTATE 42501.** This covers `nextval`, `setval` and `currval` on V2,
  whether the name is written in the SQL, passed as a parameter, built by an expression or reached through a column
  default. The check runs when the call runs, on the sequence that was actually found. It reads the latest committed
  roles and ownership, so a sequence dropped and recreated under the same name is judged on its new owner, and
  revokes, membership changes, owner changes and `ALTER SEQUENCE ... OWNED BY` transfers committed by another session
  apply from the next statement. This holds inside READ COMMITTED and REPEATABLE READ transactions too.
  In ordinary local V2 PostgreSQL sessions with no private catalog changes, column defaults are checked once per
  statement, so a revoke committed while one statement draws several rows (a multi-row INSERT, or a COPY of an
  `INSERT ... RETURNING` to the client) applies from the next statement, while explicit
  sequence calls are checked on every call. A refused
  call leaves the sequence unchanged. The transaction's own uncommitted DDL (a rename of the owning table, a new
  sequence) is judged as the transaction sees it. Over the native API, typed sequence calls return 42501, and so
  does a native INSERT whose column default draws a sequence the caller may not use: it returns the native SQL error
  class with SQLSTATE 42501 over HTTP and gRPC (earlier builds reported the core error class,
  `NATIVE_ERROR_CLASS_CORE`, with no SQLSTATE).
- **Native Application identities and application sessions are now restricted when sequences are drawn.** Named native sequence calls were
  already refused outside an Application's tables, but defaults drawn by its native INSERTs were not checked. It is
  now limited to the sequences owned by the tables it was granted. Sequence calls and serial/identity defaults whose
  sequence is owned by one of its tables keep working; any other sequence, including a standalone sequence named in
  a default of its own table, is refused.
- **Foreign-key actions run as the referencing table's owner.** As in PostgreSQL, an `ON DELETE` or `ON UPDATE SET
  DEFAULT` action that evaluates a sequence default is checked against the owner of the child table, not the role
  that deleted or updated the parent row. Two outcomes differ from earlier releases:
  - If the child table's owner has no privilege on that sequence, the action is refused with 42501 (earlier releases
    allowed it).
  - If the child table has no owner record (for example, it was created by an Application session), the action is
    checked against the role running the statement. A role that could not call the sequence directly now gets 42501.

## Changing or dropping a sequence follows its current owner

- **`ALTER SEQUENCE`, `DROP SEQUENCE` and relinking a sequence with `OWNED BY` are authorized against the sequence's
  current owner.** For a sequence linked to a table (`OWNED BY t.c`, including serial and identity sequences), that
  is the owner of the owning table, found by the table's stable id, so a table rename does not change it. For a
  standalone sequence it is the sequence's own owner. This covers every `ALTER SEQUENCE` form and the native
  DropSequence call. Earlier builds checked the sequence's own owner record even after the sequence was linked to
  another role's table, so the role that created the sequence kept the right to alter, drop or relink it. That role
  now gets 42501.
- **`OWNED BY` records that owner.** Linking a sequence to a table makes the table's owner the sequence's owner, in the
  same transaction as the DDL. `OWNED BY NONE` keeps the current owner, as in PostgreSQL 18.6, so unlinking no longer
  hands the sequence back to the role that first created it.
- GigglesDB still allows a superuser, or a member of both roles, to link a sequence to a table with a different owner;
  PostgreSQL refuses that with 55000. After such a link the table's owner owns the sequence.
- A sequence linked to another role's table before this release keeps its old owner record until it is next
  relinked. Authorization ignores that record and uses the owning table's owner, but the owner shown in the catalog
  and the `DROP ROLE` dependency check still read it.

## Who holds privileges on a sequence

GigglesDB has no `GRANT ... ON SEQUENCE`. A role may use a sequence when it is a superuser, or when it is the owner,
or an inheriting member of the owner, of the sequence's owning table (or of the sequence itself, for a standalone
sequence). Grants on the owning table (SELECT, INSERT, UPDATE) do not give access to its sequence. A `NOINHERIT`
member gains access after `SET ROLE` to the owner role.

PostgreSQL 18.6 checks USAGE or UPDATE for `nextval`, UPDATE for `setval`, and USAGE or SELECT for `currval` and
`lastval`. Under GigglesDB's ownership rule, a role either holds all three or none. `lastval()` is not supported.

## Not changed

- The Legacy engine and COPY keep their existing checks.
- Statement authorization and its order are unchanged. There is no new SQLSTATE and no on-disk format change.
- Superusers keep access to every sequence for their own calls. A foreign-key `SET DEFAULT` action fired by a
  superuser is still judged on the referencing table's owner, so it is refused when that owner lacks access. The
  database owner role has no special standing: unless it is a superuser, the ownership rule above applies to it.
