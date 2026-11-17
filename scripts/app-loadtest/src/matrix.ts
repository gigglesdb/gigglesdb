import { random } from "./ops/types.js";
import { query, failure } from "./db.js";
import { seed } from "./schema.js";
import { ops } from "./ops/index.js";
import { NativeSession, type NativeOptions } from "./native.js";
import { nativeQ, type Frontend } from "./frontend.js";
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
  frontend: Frontend = "pg",
  nativeOptions?: NativeOptions,
) {
  const results = [];
  let seq = 0;
  // Native: one persistent API session; the protocol axis is the single "native" value.
  const session =
    frontend === "native" ? new NativeSession(nativeOptions!) : null;
  await session?.open();
  const nq = session ? nativeQ(session, schema) : null;
  const protocols = nq ? ["native"] : ["simple", "extended", "prepared"];
  try {
    for (const op of ops.filter((o) => selected(o.name, opt("ops")))) {
      if (nq && "unsupported" in op.native) {
        const result = {
          op: op.name,
          variant: "native",
          status: "n/a",
          ms: 0,
          message: op.native.unsupported,
        };
        results.push(result);
        console.log(JSON.stringify(result));
        continue;
      }
      for (const protocol of protocols)
        for (const tx of ["auto", "tx"])
          for (const n of op.batched ? [1, 10, 100] : [1]) {
            const variant = `${protocol}-${tx}-${n}`;
            if (!selected(variant, opt("variants"))) continue;
            if (op.ownsTx && tx === "tx") {
              results.push({ op: op.name, variant, status: "n/a", ms: 0 });
              continue;
            }
            // Seeding stays SQL (setup, not measured); in native mode the op itself
            // goes only through native text and typed controls.
            const q = query(c, schema, nq ? "extended" : protocol);
            const start = performance.now();
            let result;
            const ctx = {
              rng: random(1),
              n,
              rows: 20,
              uniq: () => `${schema}-matrix-${++seq}`,
              counters: { created: 0, deleted: 0 },
            };
            try {
              await seed(q, 20);
              if (nq && "text" in op.native) {
                if (tx === "tx") await nq.begin();
                await op.native.run({ ...ctx, nq }, op.native.text);
                if (tx === "tx") await nq.commit();
              } else {
                if (tx === "tx") await q("BEGIN");
                await op.run({ ...ctx, q });
                if (tx === "tx") await q("COMMIT");
              }
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
                // Assertion failures carry no statement: name the last native one.
                ...(nq && !(e as { statement?: string }).statement
                  ? { sql: nq.lastSQL }
                  : {}),
              };
              if (nq) await nq.rollback().catch(() => {});
              else await q("ROLLBACK").catch(() => {});
            }
            results.push(result);
            console.log(JSON.stringify(result));
          }
    }
  } finally {
    await session?.close();
  }
  return {
    frontend,
    results,
    failed: results.some((r) => r.status === "FAIL"),
  };
}
