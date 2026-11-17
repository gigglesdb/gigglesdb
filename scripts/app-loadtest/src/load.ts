import pg from "pg";
import { query, assert, failure } from "./db.js";
import { seed } from "./schema.js";
import { stats, LatencyRecorder } from "./stats.js";
import { ops } from "./ops/index.js";
import { random, type Kind } from "./ops/types.js";
import { selected } from "./matrix.js";
import { NativeSession, type NativeOptions } from "./native.js";
import { nativeQ, type Frontend } from "./frontend.js";
export async function load(
  owner: pg.Client,
  url: string,
  schema: string,
  opt: (k: string, d?: string) => string,
  frontend: Frontend = "pg",
  nativeOptions?: NativeOptions,
) {
  const root = query(owner, schema),
    rows = Number(opt("rows", "1000")),
    clients = Number(opt("clients", "8"));
  assert(Number.isInteger(rows) && rows >= 10, "rows must be integer >= 10");
  assert(
    Number.isInteger(clients) && clients > 0,
    "clients must be positive integer",
  );
  assert(
    frontend === "pg" || !opt("protocols"),
    "--protocols applies only to --frontend pg",
  );
  const protocols =
    frontend === "native"
      ? ["native"]
      : opt("protocols", "simple,extended,prepared").split(",");
  assert(
    frontend === "native" ||
      (protocols.length > 0 &&
        new Set(protocols).size === protocols.length &&
        protocols.every((p) => ["simple", "extended", "prepared"].includes(p))),
    "invalid protocols",
  );
  const weights: Record<Kind, number> = { read: 0, write: 0, tx: 0, error: 0 };
  const entries = opt("mix", "read=60,write=25,tx=10,error=5").split(",");
  const seen = new Set<string>();
  for (const entry of entries) {
    const [k, v] = entry.split("=");
    assert(
      k in weights &&
        !seen.has(k) &&
        v !== undefined &&
        Number.isFinite(Number(v)) &&
        Number(v) >= 0,
      "invalid mix",
    );
    seen.add(k);
    weights[k as Kind] = Number(v);
  }
  const eligible = ops.filter(
    (o) => !o.matrixOnly && selected(o.name, opt("ops")),
  );
  // Ops with no native equivalent are excluded and listed as n/a, never run as SQL.
  const notApplicable = Object.fromEntries(
    frontend === "native"
      ? eligible.flatMap((o) =>
          "unsupported" in o.native ? [[o.name, o.native.unsupported]] : [],
        )
      : [],
  );
  const catalog = eligible.filter((o) => !(o.name in notApplicable));
  const groups = Object.fromEntries(
    Object.keys(weights).map((k) => [k, catalog.filter((o) => o.kind === k)]),
  ) as Record<Kind, typeof ops>;
  const kinds = (Object.keys(weights) as Kind[]).filter((k) => weights[k] > 0);
  assert(
    kinds.length > 0 && kinds.every((k) => groups[k].length > 0),
    "mix selects an empty operation kind",
  );
  const total = kinds.reduce((a, k) => a + weights[k], 0);
  const duration = Number.parseFloat(opt("duration", "30s")) * 1000,
    iterations = Number(opt("iterations", "0"));
  assert(
    Number.isFinite(duration) &&
      duration > 0 &&
      Number.isInteger(iterations) &&
      iterations >= 0,
    "invalid duration or iterations",
  );
  await seed(root, rows);
  const perOp: Record<string, number[]> = {},
    perProtocol: Record<string, number[]> = {},
    perOpBatch: Record<string, number[]> = {},
    perStatement: Record<string, number[]> = {},
    statementsPerOp: Record<string, number> = {},
    errors: Record<string, number> = {},
    details: unknown[] = [];
  const opLatency = new LatencyRecorder(),
    protocolLatency = new LatencyRecorder();
  const counters = { created: 0, deleted: 0 };
  let retries = 0,
    count = 0;
  for (const op of catalog) {
    perOp[op.name] = [];
    for (const p of protocols) perProtocol[op.name + "/" + p] = [];
  }
  const start = performance.now();
  const progress = setInterval(
    () => console.log(`completed=${count} retries=${retries}`),
    2000,
  );
  try {
    await Promise.all(
      Array.from({ length: clients }, async (_, client) => {
        const c =
          frontend === "pg" ? new pg.Client({ connectionString: url }) : null;
        const session =
          frontend === "native" ? new NativeSession(nativeOptions!) : null;
        await c?.connect();
        await session?.open();
        const rng = random(Number(opt("seed", "1")) + client * 1009);
        const queries = Object.fromEntries(
          c ? protocols.map((p) => [p, query(c, schema, p)]) : [],
        );
        let seq = 0;
        try {
          while (
            iterations
              ? count < iterations
              : performance.now() - start < duration
          ) {
            count++;
            let roll = rng() * total;
            const kind =
              kinds.find((k) => (roll -= weights[k]) < 0) ??
              kinds[kinds.length - 1];
            const group = groups[kind];
            const op = group[Math.floor(rng() * group.length)];
            const protocol = protocols[Math.floor(rng() * protocols.length)];
            const timed = (label: string) => {
              const started = performance.now();
              statementsPerOp[op.name] = (statementsPerOp[op.name] ?? 0) + 1;
              return () =>
                (perStatement[op.name + "/" + label] ??= []).push(
                  performance.now() - started,
                );
            };
            const keyword = (text: string) =>
              text
                .match(/^\s*([A-Za-z]+(?:\s+[A-Za-z]+)?)/)?.[1]
                .toUpperCase() ?? "SQL";
            const base = queries[protocol];
            const q = Object.assign(
              async (sql: string, values?: unknown[]) => {
                const done = timed(keyword(sql));
                try {
                  return await base(sql, values);
                } finally {
                  q.lastSQL = base.lastSQL;
                  done();
                }
              },
              { schema, lastSQL: "" },
            );
            const nq = session
              ? nativeQ(session, schema, (text) => timed(keyword(text)))
              : null;
            const rollback = () => (nq ? nq.rollback() : q("ROLLBACK"));
            const n = op.batched ? [1, 10, 100][Math.floor(rng() * 3)] : 1;
            let failed = false;
            const t = performance.now();
            for (let attempt = 0; ; attempt++) {
              try {
                const ctx = {
                  rng,
                  n,
                  rows,
                  uniq: () => `${schema}-${client}-${++seq}`,
                  counters,
                };
                if (nq && "text" in op.native)
                  await op.native.run({ ...ctx, nq }, op.native.text);
                else await op.run({ ...ctx, q });
                break;
              } catch (e) {
                const f = failure(e, q);
                if (nq && !(e as { statement?: string }).statement)
                  f.sql = nq.lastSQL;
                await rollback().catch(() => {});
                if (["40001", "40P01"].includes(f.code) && attempt < 5) {
                  retries++;
                  continue;
                }
                failed = true;
                const key = op.name + ":" + f.code;
                errors[key] = (errors[key] ?? 0) + 1;
                if (details.length < 100)
                  details.push({ op: op.name, protocol, ...f });
                break;
              }
            }
            const ms = performance.now() - t;
            opLatency.record(op.name, ms, failed);
            protocolLatency.record(protocol, ms, failed);
            perOp[op.name].push(ms);
            (perOpBatch[op.name + "/" + n] ??= []).push(ms);
            perProtocol[op.name + "/" + protocol].push(ms);
          }
        } finally {
          await c?.end();
          await session?.close();
        }
      }),
    );
  } finally {
    clearInterval(progress);
  }
  const elapsed = (performance.now() - start) / 1000;
  const balances = (
    await root(
      "SELECT sum(balance) AS total,min(balance) AS minimum FROM {s}accounts",
    )
  ).rows[0];
  const invariants = {
    balances: Number(balances.total) === 100 * rows,
    nonnegative: Number(balances.minimum) >= 0,
    totals:
      (
        await root(
          "SELECT o.id FROM {s}orders o LEFT JOIN {s}order_items i ON i.order_id=o.id LEFT JOIN {s}products p ON p.id=i.product_id GROUP BY o.id,o.total HAVING o.total<>coalesce(sum(i.qty*p.price),0)",
        )
      ).rowCount === 0,
    orphans:
      (
        await root(
          "SELECT i.order_id FROM {s}order_items i LEFT JOIN {s}orders o ON o.id=i.order_id WHERE o.id IS NULL",
        )
      ).rowCount === 0,
    users:
      Number(
        (await root("SELECT count(*) AS n FROM {s}users WHERE id>$1", [rows]))
          .rows[0].n,
      ) ===
      counters.created - counters.deleted,
  };
  for (const [label, values] of Object.entries({
    op: perOp,
    "op/n": perOpBatch,
    statement: perStatement,
  }))
    console.log(
      `Top 10 ${label} by p99:`,
      Object.entries(values)
        .map(([key, samples]) => ({ key, ...stats(samples) }))
        .sort((a, b) => b.p99 - a.p99)
        .slice(0, 10),
    );
  return {
    frontend,
    ops: opLatency.summary(),
    protocols: protocolLatency.summary(),
    notApplicable,
    perOpBatch: Object.fromEntries(
      Object.entries(perOpBatch).map(([k, v]) => [k, stats(v)]),
    ),
    perStatement: Object.fromEntries(
      Object.entries(perStatement).map(([k, v]) => [k, stats(v)]),
    ),
    statementsPerOp,
    count,
    elapsed,
    throughput: count / elapsed,
    latency: stats(Object.values(perOp).flat()),
    perOp: Object.fromEntries(
      Object.entries(perOp).map(([k, v]) => [k, stats(v)]),
    ),
    perProtocol: Object.fromEntries(
      Object.entries(perProtocol).map(([k, v]) => [k, stats(v)]),
    ),
    errors,
    details,
    retries,
    counters,
    invariants,
    failed:
      Object.values(invariants).some((x) => !x) ||
      Object.values(errors).reduce((a, b) => a + b, 0) / count >
        Number(opt("max-error-rate", "0")),
  };
}
