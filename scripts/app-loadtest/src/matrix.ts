import { random } from "./ops/types.js";
import { query, failure } from "./db.js";
import { seed } from "./schema.js";
import { ops } from "./ops/index.js";
import type pg from "pg";
export function selected(s: string, f: string) {
  return (
    !f ||
    f.split(",").some((x) =>
      new RegExp(
        "^" +
          x
            .split("*")
            .map((y) => y.replace(/[.+?^${}()|[\]\\]/g, "\\$&"))
            .join(".*") +
          "$",
      ).test(s),
    )
  );
}
export async function matrix(
  c: pg.Client,
  schema: string,
  opt: (k: string, d?: string) => string,
) {
  const results = [];
  let seq = 0;
  for (const op of ops.filter((o) => selected(o.name, opt("ops"))))
    for (const protocol of ["simple", "extended", "prepared"])
      for (const tx of ["auto", "tx"])
        for (const n of op.batched ? [1, 10, 100] : [1]) {
          const variant = `${protocol}-${tx}-${n}`;
          if (!selected(variant, opt("variants"))) continue;
          if (op.ownsTx && tx === "tx") {
            results.push({ op: op.name, variant, status: "n/a", ms: 0 });
            continue;
          }
          const q = query(c, schema, protocol);
          const start = performance.now();
          let result;
          try {
            await seed(q, 20);
            if (tx === "tx") await q("BEGIN");
            await op.run({
              q,
              rng: random(1),
              n,
              rows: 20,
              uniq: () => `${schema}-matrix-${++seq}`,
              counters: { created: 0, deleted: 0 },
            });
            if (tx === "tx") await q("COMMIT");
            result = {
              op: op.name,
              variant: op.ownsTx ? `${protocol}-n/a-${n}` : variant,
              status: "PASS",
              ms: performance.now() - start,
            };
          } catch (e) {
            result = {
              op: op.name,
              variant,
              status: "FAIL",
              ms: performance.now() - start,
              ...failure(e, q),
            };
            await q("ROLLBACK").catch(() => {});
          }
          results.push(result);
          console.log(JSON.stringify(result));
        }
  return { results, failed: results.some((r) => r.status === "FAIL") };
}
