import { assert } from "../db.js";
import type { NativeCtx, Op } from "./types.js";
import { Numeric, type Binding } from "../native.js";
// SELECT 1 equivalent: a table-free read-only block emitting one scalar.
const PROBE = "transaction read only { emit 1 }";
export const errors: Op[] = [
  {
    name: "unique",
    kind: "error",
    ownsTx: true,
    doc: "Signup validation: reject a duplicate email and recover",
    run: async (c) =>
      expect(
        c,
        "INSERT INTO {s}users(email) VALUES ($1)",
        ["user1@example.com"],
        "23505",
      ),
    native: {
      text: nativeText(
        "insert into {s}users(email,created_at) values($email,$now)",
      ),
      run: (c, t) =>
        nativeExpect(
          c,
          t,
          { email: "user1@example.com", now: new Date() },
          "23505",
        ),
    },
  },
  {
    name: "fk",
    kind: "error",
    ownsTx: true,
    doc: "Order validation: reject an unknown customer and recover",
    run: async (c) =>
      expect(
        c,
        "INSERT INTO {s}orders(user_id,total) VALUES ($1,0)",
        [-1],
        "23503",
      ),
    native: {
      text: nativeText(
        "insert into {s}orders(user_id,total) values($user,$zero)",
      ),
      run: (c, t) =>
        nativeExpect(c, t, { user: -1, zero: new Numeric("0") }, "23503"),
    },
  },
  {
    name: "check",
    kind: "error",
    ownsTx: true,
    doc: "Bank validation: reject a negative balance and recover",
    run: async (c) =>
      expect(c, "UPDATE {s}accounts SET balance=-1 WHERE id=$1", [1], "23514"),
    native: {
      text: nativeText("update {s}accounts where id=$id set balance=$balance"),
      run: (c, t) =>
        nativeExpect(c, t, { id: 1, balance: new Numeric("-1") }, "23514"),
    },
  },
];
async function expect(
  c: import("./types.js").Ctx,
  sql: string,
  values: unknown[],
  code: string,
) {
  await c.q("BEGIN");
  await c.q("SAVEPOINT expected");
  let got = "";
  try {
    await c.q(sql, values);
  } catch (e) {
    got = (e as { code: string }).code;
  }
  await c.q("ROLLBACK TO expected");
  assert(got === code, `expected ${code}, got ${got}`);
  assert((await c.q("SELECT 1 n")).rows[0].n === 1);
  await c.q("COMMIT");
}
function nativeText(statement: string) {
  return [
    "BEGIN (typed control)",
    "SAVEPOINT expected (typed control)",
    statement,
    "ROLLBACK TO expected (typed control)",
    PROBE,
    "COMMIT (typed control)",
  ];
}
async function nativeExpect(
  c: NativeCtx,
  [, , statement]: string[],
  bindings: Record<string, Binding>,
  code: string,
) {
  await c.nq.begin();
  await c.nq.savepoint("expected");
  let got = "";
  try {
    await c.nq(statement, bindings);
  } catch (e) {
    got = (e as { code: string }).code;
  }
  await c.nq.rollbackTo("expected");
  assert(got === code, `expected ${code}, got ${got}`);
  const probe = await c.nq(PROBE);
  assert(JSON.stringify(probe.emitted).includes('"1"'), "probe emit");
  await c.nq.commit();
}
