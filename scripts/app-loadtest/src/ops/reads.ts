import { assert } from "../db.js";
import type { User, Order } from "../rows.js";
import { id, type Op } from "./types.js";
export const reads: Op[] = [
  {
    name: "pk",
    kind: "read",
    doc: "Profile: fetch a user by primary key",
    async run(c) {
      const i = id(c);
      const r = await c.q("SELECT * FROM {s}users WHERE id=$1", [i]);
      assert((r.rows[0] as User).name === `User ${i}`);
    },
    native: {
      text: ["{s}users where id=$id"],
      async run(c, [t]) {
        const i = id(c);
        const r = await c.nq(t, { id: i });
        assert(r.rows[0].name === `User ${i}`);
      },
    },
  },
  {
    name: "email",
    kind: "read",
    doc: "Login: fetch a user by unique email",
    async run(c) {
      const i = id(c);
      const r = await c.q("SELECT * FROM {s}users WHERE email=$1", [
        `user${i}@example.com`,
      ]);
      assert(Number((r.rows[0] as User).id) === i);
    },
    native: {
      text: ["{s}users where email=$email"],
      async run(c, [t]) {
        const i = id(c);
        const r = await c.nq(t, { email: `user${i}@example.com` });
        assert(Number(r.rows[0].id) === i);
      },
    },
  },
  {
    name: "pagination",
    kind: "read",
    doc: "Directory: read an ordered offset page",
    async run(c) {
      const offset = Math.floor(c.rng() * (c.rows - 5));
      const r = await c.q(
        "SELECT id FROM {s}users WHERE id<=$1 ORDER BY id LIMIT 5 OFFSET $2",
        [c.rows, offset],
      );
      assert(r.rowCount === 5);
      r.rows.forEach((u, i) =>
        assert(Number((u as User).id) === offset + i + 1),
      );
    },
    native: {
      unsupported:
        "native reads have no OFFSET; ordered paging is keyset only (see keyset)",
    },
  },
  {
    name: "keyset",
    kind: "read",
    doc: "Feed: read the next ordered keyset page",
    async run(c) {
      const i = Math.max(1, id(c) - 5);
      const r = await c.q(
        "SELECT id FROM {s}users WHERE id>$1 AND id<=$2 ORDER BY id LIMIT 5",
        [i, c.rows],
      );
      assert(r.rowCount === Math.min(5, c.rows - i));
      r.rows.forEach((u, k) => assert(Number((u as User).id) === i + k + 1));
    },
    native: {
      text: [
        "{s}users where id>$after and id<=$max order by id limit 5 select id",
      ],
      async run(c, [t]) {
        const i = Math.max(1, id(c) - 5);
        const r = await c.nq(t, { after: i, max: c.rows });
        assert(r.rowCount === Math.min(5, c.rows - i));
        r.rows.forEach((u, k) => assert(Number(u.id) === i + k + 1));
      },
    },
  },
  {
    name: "search",
    kind: "read",
    doc: "Directory: search names by case insensitive prefix",
    async run(c) {
      const r = await c.q("SELECT name FROM {s}users WHERE name ILIKE $1", [
        "user 1%",
      ]);
      assert(r.rows.length > 0);
      assert(r.rows.every((u) => (u as User).name?.startsWith("User 1")));
    },
    native: {
      unsupported:
        "no ILIKE/LIKE pattern match and no LOWER/UPPER in native text",
    },
  },
  {
    name: "range_in",
    kind: "read",
    doc: "Filters: combine an id range and membership list",
    async run(c) {
      const i = id(c);
      const r = await c.q(
        "SELECT id FROM {s}users WHERE id BETWEEN $1 AND $2 AND id IN ($1,$2)",
        [i, Math.min(c.rows, i + 1)],
      );
      assert(r.rowCount === (i === c.rows ? 1 : 2));
    },
    native: {
      // Native has no BETWEEN or IN-list syntax: spelled as comparisons.
      text: [
        "{s}users where id>=$lo and id<=$hi and (id=$lo or id=$hi) select id",
      ],
      async run(c, [t]) {
        const i = id(c);
        const r = await c.nq(t, { lo: i, hi: Math.min(c.rows, i + 1) });
        assert(r.rowCount === (i === c.rows ? 1 : 2));
      },
    },
  },
  {
    name: "count_exists",
    kind: "read",
    doc: "Dashboard: count baseline users and check existence",
    async run(c) {
      const r = await c.q(
        "SELECT count(*) n,EXISTS(SELECT 1 FROM {s}users WHERE id=1) e FROM {s}users WHERE id<=$1",
        [c.rows],
      );
      assert(Number(r.rows[0].n) === c.rows && r.rows[0].e);
    },
    native: {
      unsupported:
        "0.19 aggregate/EXISTS typed queries are not lowered by the server text arm (lower_text returns Unsupported for typed queries: HTTP 400)",
    },
  },
  {
    name: "join_sum",
    kind: "read",
    doc: "Orders: verify joined item subtotals against order totals",
    async run(c) {
      const r = await c.q(
        "SELECT o.id,o.total,sum(i.qty*p.price) calculated FROM {s}orders o JOIN {s}order_items i ON i.order_id=o.id JOIN {s}products p ON p.id=i.product_id GROUP BY o.id,o.total",
      );
      assert(r.rows.length >= c.rows);
      assert(
        r.rows.every(
          (u) => Number((u as Order).total) === Number((u as Order).calculated),
        ),
      );
    },
    native: {
      unsupported:
        "joins with GROUP BY/sum are 0.19 typed queries, which the server text arm does not lower (HTTP 400)",
    },
  },
  {
    name: "cte",
    kind: "read",
    doc: "Analytics: select top customers using a common table expression",
    async run(c) {
      const r = await c.q(
        "WITH totals AS (SELECT user_id,sum(total) total FROM {s}orders GROUP BY user_id) SELECT user_id,total FROM totals ORDER BY total DESC,user_id LIMIT 5",
      );
      assert(r.rowCount === 5);
      assert(
        r.rows.every(
          (u, i) => i === 0 || Number(r.rows[i - 1].total) >= Number(u.total),
        ),
      );
    },
    native: { unsupported: "no common table expressions in native text" },
  },
  {
    name: "window",
    kind: "read",
    doc: "History: rank orders within each customer",
    async run(c) {
      const r = await c.q(
        "SELECT id,user_id,row_number() OVER (PARTITION BY user_id ORDER BY id) rank FROM {s}orders ORDER BY user_id,id",
      );
      assert(r.rows.length >= c.rows);
      let previous = "";
      let rank = 0;
      for (const u of r.rows) {
        const user = String(u.user_id);
        rank = user === previous ? rank + 1 : 1;
        assert(Number((u as Order).rank) === rank);
        previous = user;
      }
    },
    native: { unsupported: "no window functions in native text" },
  },
  {
    name: "large_in",
    kind: "read",
    doc: "Batch lookup: resolve at least one thousand bound ids",
    async run(c) {
      const ids = Array.from({ length: 1000 }, (_, i) => (i % c.rows) + 1);
      const r = await c.q(
        "SELECT id FROM {s}users WHERE id IN (" +
          ids.map((_, i) => "$" + (i + 1)).join(",") +
          ")",
        ids,
      );
      assert(r.rowCount === Math.min(1000, c.rows));
    },
    native: {
      unsupported:
        "no IN-list syntax, and 1000 bound ids exceed the 256-binding limit",
    },
  },
];
