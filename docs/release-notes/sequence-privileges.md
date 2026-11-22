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
  apply from the next statement. This holds inside READ COMMITTED and REPEATABLE READ transactions too. A refused
  call leaves the sequence unchanged. The transaction's own uncommitted DDL (a rename of the owning table, a new
  sequence) is judged as the transaction sees it. Over the native API, typed sequence calls return 42501; a native
  INSERT whose default is refused reports the core error class (`NATIVE_ERROR_CLASS_CORE`) without a SQLSTATE.
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
