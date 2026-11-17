import { test } from "node:test";
import { strict as assert } from "node:assert";
import { query, failure } from "./db.js";
import { setup } from "./schema.js";
import type pg from "pg";
test("setup failure records exact SQL across Q instances", async () => {
  const c = {
    query: async () => {
      throw Error("deliberate");
    },
  } as unknown as pg.Client;
  const old = query(c, "old");
  try {
    await setup(query(c, "new"));
  } catch (e) {
    assert.equal(failure(e, old).sql, "CREATE SCHEMA new");
  }
});

import { LatencyRecorder } from "./stats.js";
test("latency aggregation by operation includes failed operations", () => {
  const ops = new LatencyRecorder();
  ops.record("read", 9);
  ops.record("write", 20, true);
  ops.record("read", 1);
  assert.deepEqual(ops.summary(), {
    read: { count: 2, p50_ms: 9, p95_ms: 9, p99_ms: 9, max_ms: 9, errors: 0 },
    write: {
      count: 1,
      p50_ms: 20,
      p95_ms: 20,
      p99_ms: 20,
      max_ms: 20,
      errors: 1,
    },
  });
});
test("protocol aggregation combines operations and isolates warmup", () => {
  const warmup = new LatencyRecorder(),
    measured = new LatencyRecorder();
  warmup.record("simple", 999, true);
  for (const p of ["simple", "extended", "prepared", "native"]) {
    measured.record(p, 2);
    measured.record(p, 4, true);
    assert.deepEqual(measured.summary()[p], {
      count: 2,
      p50_ms: 4,
      p95_ms: 4,
      p99_ms: 4,
      max_ms: 4,
      errors: 1,
    });
  }
  assert.deepEqual(new LatencyRecorder().summary(), {});
});
