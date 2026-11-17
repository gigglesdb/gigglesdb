import type { Q } from "../db.js";
import type { Binding, NativeResult } from "../native.js";
export type Kind = "read" | "write" | "tx" | "error";
export interface Ctx {
  q: Q;
  rng: () => number;
  n: number;
  rows: number;
  uniq: () => string;
  counters: { created: number; deleted: number };
}
/** Native-frontend handle: language text and typed transaction controls only, no SQL. */
export interface NativeQ {
  (text: string, bindings?: Record<string, Binding>): Promise<NativeResult>;
  begin(): Promise<unknown>;
  commit(): Promise<unknown>;
  rollback(): Promise<unknown>;
  savepoint(name: string): Promise<unknown>;
  rollbackTo(name: string): Promise<unknown>;
  lastSQL: string;
}
export interface NativeCtx extends Omit<Ctx, "q"> {
  nq: NativeQ;
}
/**
 * The op's native-language equivalent. `text` lists every native statement the
 * op sends ({s} is the run schema prefix) and is passed to `run` in order, so
 * the catalog shows exactly what is measured. Ops without an equivalent carry
 * `unsupported` and report n/a under the native frontend.
 */
export type Native =
  | { text: string[]; run(c: NativeCtx, text: string[]): Promise<void> }
  | { unsupported: string };
export interface Op {
  name: string;
  kind: Kind;
  doc: string;
  batched?: boolean;
  ownsTx?: boolean;
  matrixOnly?: boolean;
  run(ctx: Ctx): Promise<void>;
  native: Native;
}
export const id = (c: { rng: () => number; rows: number }) =>
  1 + Math.floor(c.rng() * c.rows);
export function random(seed: number) {
  let state = seed;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 4294967296;
  };
}
/** Balanced `(f=$p0 or f=$p1 ...)`; keeps the native expression depth logarithmic. */
export function anyOf(field: string, params: string[]): string {
  if (params.length === 1) return `${field}=$${params[0]}`;
  const mid = params.length >> 1;
  return `(${anyOf(field, params.slice(0, mid))} or ${anyOf(field, params.slice(mid))})`;
}
