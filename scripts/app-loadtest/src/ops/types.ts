import type { Q } from "../db.js";
export type Kind = "read" | "write" | "tx" | "error";
export interface Ctx {
  q: Q;
  rng: () => number;
  n: number;
  rows: number;
  uniq: () => string;
  counters: { created: number; deleted: number };
}
export interface Op {
  name: string;
  kind: Kind;
  doc: string;
  batched?: boolean;
  ownsTx?: boolean;
  matrixOnly?: boolean;
  run(ctx: Ctx): Promise<void>;
}
export const id = (c: Ctx) => 1 + Math.floor(c.rng() * c.rows);
export function random(seed: number) {
  let state = seed;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 4294967296;
  };
}
