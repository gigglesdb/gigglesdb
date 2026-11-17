import https from "node:https";
import { readFileSync } from "node:fs";

/** Newest native language profile the giggles-server text arm accepts. */
export const LANGUAGE_VERSION = "0.21";

export interface NativeOptions {
  url: string;
  ca?: string;
  cert?: string;
  key?: string;
}

/** A typed numeric parameter (exact decimal text), bound as `numericValue`. */
export class Numeric {
  constructor(readonly text: string) {}
}
export type Binding =
  | null
  | boolean
  | number
  | bigint
  | string
  | Date
  | Numeric
  | Uint8Array;
export type Row = Record<string, unknown>;
export interface NativeResult {
  rows: Row[];
  rowCount: number;
  matched: number;
  affected: number;
  changed: number;
  emitted?: unknown[];
}

/** Engine failure reported in an HTTP 200 reply (`code` != "ok"). */
export class NativeError extends Error {
  constructor(
    /** SQLSTATE when the server supplies one, else the reply code / error class. */
    readonly code: string,
    readonly kind: string | undefined,
    message: string,
    readonly statement: string,
  ) {
    super(message);
  }
}

// PostgreSQL binary temporal epoch, 2000-01-01T00:00:00Z.
const PG_EPOCH_MS = 946684800000;

function encode(v: Binding): Record<string, unknown> {
  if (v === null) return { nullValue: "SQL_NULL" };
  if (typeof v === "boolean") return { boolValue: v };
  if (typeof v === "number") {
    if (!Number.isSafeInteger(v)) throw Error("native integer binding: " + v);
    return { intValue: String(v) };
  }
  if (typeof v === "bigint") return { intValue: v.toString() };
  if (typeof v === "string") return { textValue: v };
  if (v instanceof Numeric) return { numericValue: v.text };
  if (v instanceof Date) {
    const b = Buffer.alloc(8);
    b.writeBigInt64BE(BigInt(v.getTime() - PG_EPOCH_MS) * 1000n);
    return { timestamptzBinary: b.toString("base64") };
  }
  return { bytesValue: Buffer.from(v).toString("base64") };
}

/** Decode a wire value; integers become numbers (strings beyond 2^53). */
export function decode(v: Record<string, unknown>): unknown {
  const [[tag, x]] = Object.entries(v);
  switch (tag) {
    case "nullValue":
      return null;
    case "boolValue":
      return x as boolean;
    case "intValue": {
      const n = Number(x);
      return Number.isSafeInteger(n) ? n : String(x);
    }
    case "textValue":
    case "numericValue":
      return x as string;
    case "bytesValue":
      return Buffer.from(x as string, "base64");
    case "timestamptzBinary":
    case "timestampBinary": {
      const us = Buffer.from(x as string, "base64").readBigInt64BE();
      return new Date(PG_EPOCH_MS + Number(us / 1000n));
    }
    default:
      return { [tag]: x };
  }
}

function rows(outcome: {
  columns?: string[];
  rows?: { values?: Record<string, unknown>[] }[];
}): Row[] {
  const columns = outcome.columns ?? [];
  return (outcome.rows ?? []).map((r) =>
    Object.fromEntries(
      (r.values ?? []).map((v, i) => [columns[i] ?? String(i), decode(v)]),
    ),
  );
}

/**
 * One persistent giggles-server HTTP API session over mTLS (HTTP/1.1 keep-alive).
 * Not safe for concurrent calls: the server allows one Execute per session.
 */
export class NativeSession {
  lastText = "";
  private agent: https.Agent;
  private base: URL;
  private handle = "";
  constructor(opts: NativeOptions) {
    this.base = new URL(opts.url);
    const read = (p?: string) => (p ? readFileSync(p) : undefined);
    this.agent = new https.Agent({
      keepAlive: true,
      maxSockets: 1,
      ca: read(opts.ca),
      cert: read(opts.cert),
      key: read(opts.key),
    });
  }
  private call(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<{ status: number; body: string }> {
    const data = body === undefined ? undefined : JSON.stringify(body);
    return new Promise((resolve, reject) => {
      const req = https.request(
        new URL(path, this.base),
        {
          method,
          agent: this.agent,
          headers: {
            ...(data !== undefined
              ? {
                  "content-type": "application/json",
                  "content-length": Buffer.byteLength(data),
                }
              : { "content-length": 0 }),
            ...(this.handle ? { "x-giggles-session": this.handle } : {}),
          },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (c: Buffer) => chunks.push(c));
          res.on("end", () =>
            resolve({
              status: res.statusCode ?? 0,
              body: Buffer.concat(chunks).toString("utf8"),
            }),
          );
          res.on("error", reject);
        },
      );
      req.on("error", reject);
      req.end(data);
    });
  }
  async open() {
    const r = await this.call("POST", "/v1/sessions", {});
    if (r.status !== 201)
      throw Error(`open session: HTTP ${r.status} ${r.body}`);
    this.handle = JSON.parse(r.body).sessionId;
  }
  async close() {
    if (!this.handle) return;
    await this.call("DELETE", "/v1/session").catch(() => {});
    this.handle = "";
    this.agent.destroy();
  }
  /** Execute one request body; throws NativeError unless the reply code is ok. */
  async execute(body: Record<string, unknown>, label: string) {
    this.lastText = label;
    const r = await this.call("POST", "/v1/session/execute", body);
    if (r.status !== 200)
      throw new NativeError(
        "HTTP" + r.status,
        undefined,
        `HTTP ${r.status} ${r.body}`,
        label,
      );
    const reply = JSON.parse(r.body);
    if (reply.code !== "ok") {
      const e = reply.nativeError;
      throw new NativeError(
        e?.sqlstate ?? (e?.kind ? kindCode(e.kind) : reply.code),
        e?.kind,
        `${reply.code}${e?.kind ? " " + e.kind : ""}${e?.sqlstate ? " " + e.sqlstate : ""}`,
        label,
      );
    }
    return reply;
  }
  /** Native language text with named bindings ($name). */
  async text(
    text: string,
    bindings: Record<string, Binding> = {},
  ): Promise<NativeResult> {
    const reply = await this.execute(
      {
        text: {
          text,
          bindings: Object.entries(bindings).map(([name, v]) => ({
            name,
            value: encode(v),
          })),
          languageVersion: LANGUAGE_VERSION,
        },
      },
      text,
    );
    if (reply.transactionOutcome) {
      const emitted = (reply.transactionOutcome.emitted ??
        reply.transactionOutcome.values ??
        []) as unknown[];
      return {
        rows: [],
        rowCount: 0,
        matched: 0,
        affected: 0,
        changed: 0,
        emitted,
      };
    }
    const o = reply.operationOutcome ?? {};
    const r = rows(o);
    return {
      rows: r,
      rowCount: r.length,
      matched: Number(o.matched ?? 0),
      affected: Number(o.affected ?? 0),
      changed: Number(o.changed ?? 0),
    };
  }
  begin() {
    return this.execute({ begin: {} }, "BEGIN");
  }
  commit() {
    return this.execute({ commit: {} }, "COMMIT");
  }
  rollback() {
    return this.execute({ rollback: {} }, "ROLLBACK");
  }
  savepoint(name: string) {
    return this.execute({ savepoint: { name } }, "SAVEPOINT " + name);
  }
  rollbackTo(name: string) {
    return this.execute({ rollbackTo: { name } }, "ROLLBACK TO " + name);
  }
}

/**
 * Error classes without a SQLSTATE. A commit/write conflict is the native
 * serialization failure, so it maps to 40001 and gets the same capped retry.
 */
function kindCode(kind: string) {
  const k = kind.replace("NATIVE_ERROR_CLASS_", "");
  return k === "CONFLICT" ? "40001" : "NATIVE_" + k;
}
