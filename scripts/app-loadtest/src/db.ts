import pg from "pg";
import { createHash } from "node:crypto";
export function assert(v: unknown, m = "incorrect result"): asserts v {
  if (!v) throw Error(m);
}
export type Q = ((
  sql: string,
  values?: unknown[],
) => Promise<pg.QueryResult>) & {
  schema: string;
  lastSQL: string;
};
/** Record the exact statement sent, including setup and transaction control. */
export const recorder = { lastSQL: "" };
export function query(c: pg.Client, schema: string, variant = "extended"): Q {
  const q = async (
    sql: string,
    values: unknown[] = [],
  ): Promise<pg.QueryResult> => {
    sql = sql.replaceAll("{s}", '"' + schema + '".');
    if (variant.startsWith("simple"))
      sql = sql.replace(/\$(\d+)/g, (_, n) => {
        const x = values[Number(n) - 1];
        if (x === null) return "NULL";
        if (typeof x === "number" || typeof x === "bigint") return String(x);
        if (typeof x === "boolean") return x ? "TRUE" : "FALSE";
        const value =
          x instanceof Date
            ? x.toISOString()
            : typeof x === "object"
              ? JSON.stringify(x)
              : String(x);
        return "'" + value.replaceAll("'", "''") + "'";
      });
    (q as Q).lastSQL = sql;
    recorder.lastSQL = sql;
    try {
      return await (variant.startsWith("simple") || sql.includes(";")
        ? c.query(sql)
        : c.query({
            text: sql,
            values,
            name: variant.startsWith("prepared")
              ? "s_" +
                createHash("sha256").update(sql).digest("hex").slice(0, 40)
              : undefined,
          }));
    } catch (e) {
      Object.assign(e as object, { statement: sql });
      throw e;
    }
  };
  return Object.assign(q, { schema, lastSQL: "" });
}
export function failure(e: unknown, q: Q) {
  const x = e as {
    code?: string;
    message: string;
  };
  return {
    code: x.code ?? "ASSERT",
    message: x.message,
    sql: (e as { statement?: string }).statement ?? recorder.lastSQL,
  };
}
