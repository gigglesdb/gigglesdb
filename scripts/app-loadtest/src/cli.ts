import { ops } from "./ops/index.js";
import pg from "pg";
import { query, failure, assert } from "./db.js";
import { setup, seed } from "./schema.js";
import { matrix } from "./matrix.js";
import { load } from "./load.js";
import { save } from "./report.js";
const args = process.argv.slice(2);
const mode = args.shift();
function opt(k: string, d = "") {
  const i = args.indexOf("--" + k);
  return i < 0 ? d : args[i + 1];
}
if (args.includes("--help") || mode === "--help") {
  console.log(
    "Usage: npm run matrix|load -- --url URL [--ops GLOB] [--variants GLOB] [--protocols simple,extended,prepared] [--mix read=60,write=25,tx=10,error=5] [--rows N] [--seed N] [--clients N] [--duration 30s|--iterations N] [--max-error-rate 0] [--report FILE] [--keep]",
  );
  process.exit(0);
}
const flags = new Set(
  "url ops variants protocols mix rows seed clients duration iterations max-error-rate report keep".split(
    " ",
  ),
);
for (let i = 0; i < args.length; i++) {
  const key = args[i].slice(2);
  if (!args[i].startsWith("--") || !flags.has(key))
    throw Error("unknown flag: " + args[i]);
  if (key !== "keep") {
    if (!args[i + 1] || args[i + 1].startsWith("--"))
      throw Error("missing value: " + key);
    i++;
  }
}
if (mode === "ops") {
  console.log(
    "| Name | Kind | App behavior |\n| --- | --- | --- |\n" +
      ops.map((o) => `| ${o.name} | ${o.kind} | ${o.doc} |`).join("\n"),
  );
  process.exit(0);
}
const url = opt("url", process.env.DATABASE_URL);
if (!url) throw Error("--url or DATABASE_URL required");
const schema =
  "app_lt_" + Date.now() + "_" + Math.random().toString(16).slice(2, 8);
const c = new pg.Client({ connectionString: url });
const q = query(c, schema);
let report: Record<string, unknown> = { schema, mode };
try {
  await c.connect();
  await setup(q);
  report = {
    ...report,
    ...(opt("ops") === "none"
      ? await (async () => {
          await seed(q, 20);
          const balances = (
            await q(
              "SELECT sum(balance) AS total,min(balance) AS minimum FROM {s}accounts",
            )
          ).rows[0];
          assert(
            Number(balances.total) === 2000 && Number(balances.minimum) >= 0,
            "balance invariant",
          );
          assert(
            (
              await q(
                "SELECT o.id FROM {s}orders o LEFT JOIN {s}order_items i ON i.order_id=o.id LEFT JOIN {s}products p ON p.id=i.product_id GROUP BY o.id,o.total HAVING o.total<>coalesce(sum(i.qty*p.price),0)",
              )
            ).rowCount === 0,
            "total invariant",
          );
          assert(
            (
              await q(
                "SELECT i.order_id FROM {s}order_items i LEFT JOIN {s}orders o ON o.id=i.order_id WHERE o.id IS NULL",
              )
            ).rowCount === 0,
            "orphan invariant",
          );
          assert(
            Number(
              (await q("SELECT count(*) AS n FROM {s}users WHERE id>$1", [20]))
                .rows[0].n,
            ) === 0,
            "user invariant",
          );
          return { smoke: true };
        })()
      : mode === "matrix"
        ? await matrix(c, schema, opt)
        : mode === "load"
          ? await load(c, url, schema, opt)
          : (() => {
              throw Error("expected matrix or load");
            })()),
  };
} catch (e) {
  report = { ...report, failed: true, blocker: failure(e, q) };
} finally {
  if (!args.includes("--keep"))
    await q("DROP SCHEMA IF EXISTS " + schema + " CASCADE").catch(() => {});
  await c.end();
  save(opt("report"), report);
  console.log(JSON.stringify(report));
  process.exitCode = report.failed ? 1 : 0;
}
