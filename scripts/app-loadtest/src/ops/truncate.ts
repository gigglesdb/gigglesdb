import { assert } from "../db.js";
import { seed } from "../schema.js";
import type { Op } from "./types.js";
export const truncates: Op[] = [
  "restart_identity_cascade",
  "cascade_from_parent",
  "restrict_fk",
  "rollback",
].map((mode) => ({
  name: "truncate_" + mode,
  kind: "write",
  matrixOnly: true,
  ownsTx: true,
  doc: mode,
  native: {
    unsupported:
      "no TRUNCATE in native text; the server text arm refuses catalog text",
  },
  async run({ q }) {
    if (mode === "restrict_fk") {
      let code = "";
      try {
        await q("TRUNCATE {s}users");
      } catch (e) {
        code = (e as { code: string }).code;
      }
      assert(code === "0A000", "expected 0A000 got " + code);
    } else if (mode === "rollback") {
      await q("BEGIN");
      await q("TRUNCATE {s}users CASCADE");
      await q("ROLLBACK");
      assert(
        Number((await q("SELECT count(*) n FROM {s}users")).rows[0].n) === 20,
      );
    } else {
      await q(
        mode === "cascade_from_parent"
          ? "TRUNCATE {s}users CASCADE"
          : "TRUNCATE {s}users,{s}accounts,{s}products,{s}orders,{s}order_items RESTART IDENTITY CASCADE",
      );
      for (const t of mode === "cascade_from_parent"
        ? ["users", "accounts", "orders", "order_items"]
        : ["users", "accounts", "products", "orders", "order_items"])
        assert(
          Number((await q("SELECT count(*) n FROM {s}" + t)).rows[0].n) === 0,
        );
      if (mode === "restart_identity_cascade")
        assert(
          Number(
            (
              await q(
                "INSERT INTO {s}users(email) VALUES ('reset@example.com') RETURNING id",
              )
            ).rows[0].id,
          ) === 1,
        );
    }
    await seed(q, 20);
  },
}));
