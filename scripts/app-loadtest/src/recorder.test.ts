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
