import { assert } from "../db.js";
import type { Op } from "./types.js";
export const gaps: Op[] = [
  ["alias_without_as", "read", "SELECT count(*) n FROM {s}users"],
  ["generate_series_from", "read", "SELECT i FROM generate_series(1,3) i"],
  [
    "update_table_alias",
    "write",
    "UPDATE {s}users u SET name = u.name WHERE u.id = $1",
  ],
  ["pg_get_serial_sequence", "read", "SELECT pg_get_serial_sequence($1,'id')"],
  ["regclass_cast", "read", "SELECT $1::regclass"],
].map(([name, kind, sql]) => ({
  name,
  kind: kind as "read" | "write",
  doc: name,
  matrixOnly: true,
  native: {
    unsupported:
      "SQL syntax/catalog-function probe with no native-language counterpart",
  },
  async run({ q }) {
    await q(
      sql,
      sql.includes("$1")
        ? [name === "update_table_alias" ? 1 : q.schema + ".users"]
        : [],
    );
  },
}));
gaps.push({
  name: "update_correlated_subquery",
  kind: "write",
  doc: "Correlated total update",
  matrixOnly: true,
  native: {
    unsupported:
      "no correlated subquery or join aggregate in native SET expressions",
  },
  async run({ q }) {
    const before = (await q("SELECT total FROM {s}orders WHERE id=$1", [1]))
      .rows[0].total;
    await q(
      "UPDATE {s}orders SET total = (SELECT sum(i.qty*p.price) FROM {s}order_items i JOIN {s}products p ON p.id=i.product_id WHERE i.order_id = {s}orders.id) WHERE id = $1",
      [1],
    );
    assert(
      String(
        (await q("SELECT total FROM {s}orders WHERE id=$1", [1])).rows[0].total,
      ) === String(before),
    );
  },
});
