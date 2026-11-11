import { assert } from "../db.js";
import type { Op } from "./types.js";
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
  },
  {
    name: "check",
    kind: "error",
    ownsTx: true,
    doc: "Bank validation: reject a negative balance and recover",
    run: async (c) =>
      expect(c, "UPDATE {s}accounts SET balance=-1 WHERE id=$1", [1], "23514"),
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
