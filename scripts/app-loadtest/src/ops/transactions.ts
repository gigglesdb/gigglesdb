import { assert } from "../db.js";
import type { Account } from "../rows.js";
import { id, type Ctx, type Op } from "./types.js";
import { Numeric } from "../native.js";
/** Exact numeric(12,2) text <-> integer cents; native text has no decimal arithmetic. */
export const cents = (v: unknown) => {
  const [w, f = ""] = String(v).split(".");
  const neg = w.startsWith("-");
  const c = BigInt(w.replace("-", "")) * 100n + BigInt((f + "00").slice(0, 2));
  return neg ? -c : c;
};
export const decimal = (c: bigint) => {
  const m = c < 0n ? -c : c;
  return new Numeric(
    (c < 0n ? "-" : "") + m / 100n + "." + String(m % 100n).padStart(2, "0"),
  );
};
async function place(c: Ctx) {
  const user = id(c);
  const r = await c.q(
    "INSERT INTO {s}orders(user_id,total) VALUES($1,0) RETURNING id",
    [user],
  );
  const order = r.rows[0].id as string;
  const product =
    1 + Math.floor(c.rng() * Math.max(10, Math.floor(c.rows / 10)));
  const qty = 1 + Math.floor(c.rng() * 3);
  await c.q("INSERT INTO {s}order_items VALUES($1,$2,$3)", [
    order,
    product,
    qty,
  ]);
  const result = await c.q(
    "UPDATE {s}orders SET total=(SELECT price*$2 FROM {s}products WHERE id=$3) WHERE id=$1 RETURNING total",
    [order, qty, product],
  );
  assert(Number(result.rows[0].total) === (product + 0.25) * qty);
  return order;
}
export const transactions: Op[] = [
  {
    name: "transfer",
    kind: "tx",
    ownsTx: true,
    doc: "Banking: lock two accounts in id order and transfer available funds",
    async run(c) {
      const a = id(c);
      let b = id(c);
      if (a === b) b = (a % c.rows) + 1;
      await c.q("BEGIN");
      const r = await c.q(
        "SELECT id,balance FROM {s}accounts WHERE id IN ($1,$2) ORDER BY id FOR UPDATE",
        [a, b],
      );
      assert(r.rowCount === 2);
      const balance = Number(
        (r.rows as Account[]).find((u) => Number(u.id) === a)!.balance,
      );
      const amount = Math.min(balance, 1 + Math.floor(c.rng() * 10));
      await c.q("UPDATE {s}accounts SET balance=balance-$1 WHERE id=$2", [
        amount,
        a,
      ]);
      await c.q("UPDATE {s}accounts SET balance=balance+$1 WHERE id=$2", [
        amount,
        b,
      ]);
      await c.q("COMMIT");
    },
    native: {
      // No FOR UPDATE and no decimal arithmetic: read both balances inside the
      // transaction, compute exactly, assign. Concurrent writers to the same rows
      // fail with a conflict (mapped to 40001 and retried like SQL).
      text: [
        "BEGIN (typed control)",
        "{s}accounts where id=$a or id=$b order by id select id,balance",
        "update {s}accounts where id=$id set balance=$balance",
        "update {s}accounts where id=$id set balance=$balance",
        "COMMIT (typed control)",
      ],
      async run(c, [, read, debit, credit]) {
        const a = id(c);
        let b = id(c);
        if (a === b) b = (a % c.rows) + 1;
        await c.nq.begin();
        const r = await c.nq(read, { a, b });
        assert(r.rowCount === 2);
        const from = cents(r.rows.find((u) => Number(u.id) === a)!.balance);
        const to = cents(r.rows.find((u) => Number(u.id) === b)!.balance);
        const amount =
          BigInt(Math.min(Number(from / 100n), 1 + Math.floor(c.rng() * 10))) *
          100n;
        await c.nq(debit, { id: a, balance: decimal(from - amount) });
        await c.nq(credit, { id: b, balance: decimal(to + amount) });
        await c.nq.commit();
      },
    },
  },
  {
    name: "place_order",
    kind: "tx",
    ownsTx: true,
    doc: "Checkout: atomically create an order items and exact total",
    async run(c) {
      await c.q("BEGIN");
      await place(c);
      await c.q("COMMIT");
    },
    native: {
      unsupported:
        "order_items has no primary key; native reads and writes refuse keyless tables (NATIVE_ERROR_CLASS_UNSUPPORTED)",
    },
  },
  {
    name: "cancel_order",
    kind: "tx",
    ownsTx: true,
    doc: "Cancellation: delete an owned order and cascade its items",
    async run(c) {
      await c.q("BEGIN");
      const order = await place(c);
      assert(
        (await c.q("DELETE FROM {s}orders WHERE id=$1 RETURNING id", [order]))
          .rowCount === 1,
      );
      assert(
        (
          await c.q("SELECT order_id FROM {s}order_items WHERE order_id=$1", [
            order,
          ])
        ).rowCount === 0,
      );
      await c.q("COMMIT");
    },
    native: {
      unsupported:
        "order_items has no primary key; native reads and writes refuse keyless tables (NATIVE_ERROR_CLASS_UNSUPPORTED)",
    },
  },
  {
    name: "rollback",
    kind: "tx",
    ownsTx: true,
    doc: "Abandoned signup: roll back and verify no user remains",
    async run(c) {
      const email = c.uniq() + "@example.com";
      await c.q("BEGIN");
      await c.q("INSERT INTO {s}users(email,name) VALUES($1,'Abandoned')", [
        email,
      ]);
      await c.q("ROLLBACK");
      assert(
        (await c.q("SELECT id FROM {s}users WHERE email=$1", [email]))
          .rowCount === 0,
      );
    },
    native: {
      text: [
        "BEGIN (typed control)",
        'insert into {s}users(email,name,created_at) values($email,"Abandoned",$now)',
        "ROLLBACK (typed control)",
        "{s}users where email=$email select id",
      ],
      async run(c, [, insert, , read]) {
        const email = c.uniq() + "@example.com";
        await c.nq.begin();
        await c.nq(insert, { email, now: new Date() });
        await c.nq.rollback();
        assert((await c.nq(read, { email })).rowCount === 0);
      },
    },
  },
  {
    name: "savepoint",
    kind: "tx",
    ownsTx: true,
    doc: "Optional signup: roll back a savepoint and keep the session usable",
    async run(c) {
      const email = c.uniq() + "@example.com";
      await c.q("BEGIN");
      await c.q("SAVEPOINT optional");
      await c.q("INSERT INTO {s}users(email,name) VALUES($1,'Optional')", [
        email,
      ]);
      await c.q("ROLLBACK TO optional");
      assert(
        (await c.q("SELECT id FROM {s}users WHERE email=$1", [email]))
          .rowCount === 0,
      );
      await c.q("COMMIT");
    },
    native: {
      text: [
        "BEGIN (typed control)",
        "SAVEPOINT optional (typed control)",
        'insert into {s}users(email,name,created_at) values($email,"Optional",$now)',
        "ROLLBACK TO optional (typed control)",
        "{s}users where email=$email select id",
        "COMMIT (typed control)",
      ],
      async run(c, [, , insert, , read]) {
        const email = c.uniq() + "@example.com";
        await c.nq.begin();
        await c.nq.savepoint("optional");
        await c.nq(insert, { email, now: new Date() });
        await c.nq.rollbackTo("optional");
        assert((await c.nq(read, { email })).rowCount === 0);
        await c.nq.commit();
      },
    },
  },
];
