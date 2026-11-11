import { assert } from "../db.js";
import type { Account } from "../rows.js";
import { id, type Ctx, type Op } from "./types.js";
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
  },
];
