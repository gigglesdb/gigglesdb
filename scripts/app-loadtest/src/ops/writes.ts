import { assert } from "../db.js";
import type { User } from "../rows.js";
import type { Ctx, Op } from "./types.js";
export async function signup(c: Ctx, n = c.n) {
  const token = c.uniq();
  const values = Array.from({ length: n }, (_, i) => [
    `${token}-${i}@example.com`,
    "Owned",
  ]).flat();
  const r = await c.q(
    "INSERT INTO {s}users(email,name) VALUES " +
      Array.from({ length: n }, (_, i) => `($${2 * i + 1},$${2 * i + 2})`).join(
        ",",
      ) +
      " RETURNING *",
    values,
  );
  assert(r.rowCount === n);
  c.counters.created += n;
  return r.rows as User[];
}
export const writes: Op[] = [
  {
    name: "insert_returning",
    kind: "write",
    doc: "Signup: create a user and return its generated id",
    async run(c) {
      assert(Number((await signup(c, 1))[0].id) > c.rows);
    },
  },
  {
    name: "multi_insert",
    kind: "write",
    batched: true,
    doc: "Import: sign up a batch in one multi row insert",
    async run(c) {
      await signup(c);
    },
  },
  {
    name: "update",
    kind: "write",
    doc: "Profile: update an owned user and return the new name",
    async run(c) {
      const u = (await signup(c, 1))[0];
      const r = await c.q(
        "UPDATE {s}users SET name=$1 WHERE id=$2 RETURNING name",
        ["Updated", u.id],
      );
      assert(r.rows[0].name === "Updated");
    },
  },
  {
    name: "optimistic",
    kind: "write",
    doc: "Profile: reject a stale version on the second update",
    async run(c) {
      const u = (await signup(c, 1))[0];
      const sql =
        "UPDATE {s}users SET version=version+1 WHERE id=$1 AND version=0";
      assert((await c.q(sql, [u.id])).rowCount === 1);
      assert((await c.q(sql, [u.id])).rowCount === 0);
    },
  },
  {
    name: "upsert",
    kind: "write",
    batched: true,
    doc: "Import: update existing signups and ignore duplicate conflicts",
    async run(c) {
      const users = await signup(c);
      const values = users.flatMap((u) => [u.email, "Upserted"]);
      const sql =
        "INSERT INTO {s}users(email,name) VALUES " +
        users.map((_, i) => `($${2 * i + 1},$${2 * i + 2})`).join(",");
      const r = await c.q(
        sql +
          " ON CONFLICT(email) DO UPDATE SET name=excluded.name RETURNING name",
        values,
      );
      assert(r.rowCount === c.n && r.rows.every((u) => u.name === "Upserted"));
      assert(
        (await c.q(sql + " ON CONFLICT DO NOTHING", values)).rowCount === 0,
      );
    },
  },
  {
    name: "roundtrip",
    kind: "write",
    doc: "Form: preserve NULL timestamp exact numeric and Unicode quoted text",
    async run(c) {
      const text = "\u96ea O'Brien \u00e9\u00e8 \u{1F600} NUL-free";
      const email = c.uniq() + "@example.com";
      const date = new Date("2020-01-02T03:04:05.000Z");
      const r = await c.q(
        "INSERT INTO {s}users(email,name,created_at,active) VALUES($1,$2,$3,NULL) RETURNING *,length(name) chars,octet_length(name) bytes",
        [email, text, date],
      );
      c.counters.created++;
      const u = r.rows[0] as User;
      assert(
        u.email === email &&
          u.name === text &&
          Number(r.rows[0].chars) === Array.from(text).length &&
          Number(r.rows[0].bytes) === Buffer.byteLength(text) &&
          u.created_at.toISOString() === date.toISOString() &&
          u.active === null,
      );
      const n = await c.q(
        "SELECT 0.10::numeric(12,2)+0.20::numeric(12,2) v,now() IS NOT NULL t",
      );
      assert(n.rows[0].v === "0.30" && n.rows[0].t);
    },
  },
  {
    name: "bulk_delete",
    kind: "write",
    batched: true,
    doc: "Cleanup: delete only users created by this call",
    async run(c) {
      const users = await signup(c);
      const r = await c.q(
        "DELETE FROM {s}users WHERE id IN (" +
          users.map((_, i) => "$" + (i + 1)).join(",") +
          ") RETURNING id",
        users.map((u) => u.id),
      );
      assert(r.rowCount === c.n);
      c.counters.deleted += c.n;
    },
  },
  {
    name: "ddl_multi_statement_batch",
    kind: "write",
    matrixOnly: true,
    doc: "Migration: execute related table and index DDL as one batch",
    async run(c) {
      await c.q(
        "CREATE TABLE {s}batch_a(id integer PRIMARY KEY); CREATE TABLE {s}batch_b(id integer REFERENCES {s}batch_a(id)); CREATE INDEX ON {s}batch_b(id)",
      );
      assert(
        Number(
          (
            await c.q(
              "SELECT count(*) n FROM information_schema.tables WHERE table_schema=$1 AND table_name IN ('batch_a','batch_b')",
              [c.q.schema],
            )
          ).rows[0].n,
        ) === 2,
      );
      await c.q("DROP TABLE {s}batch_b");
      await c.q("DROP TABLE {s}batch_a");
    },
  },
];
