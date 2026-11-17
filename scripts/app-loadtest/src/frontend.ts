import type { NativeSession, Binding } from "./native.js";
import type { NativeQ } from "./ops/types.js";
export type Frontend = "pg" | "native";
/**
 * Bind a native session to the run schema. `{s}` becomes `schema.` (native 0.18+
 * qualified names). `onStatement` is called before each request and returns the
 * completion callback, so load can time native statements like SQL ones.
 */
export function nativeQ(
  session: NativeSession,
  schema: string,
  onStatement: (label: string) => () => void = () => () => {},
): NativeQ {
  const timed = async <T>(label: string, f: () => Promise<T>) => {
    const done = onStatement(label);
    try {
      return await f();
    } finally {
      nq.lastSQL = session.lastText;
      done();
    }
  };
  const nq: NativeQ = Object.assign(
    (text: string, bindings: Record<string, Binding> = {}) => {
      const t = text.replaceAll("{s}", schema + ".");
      return timed(t, () => session.text(t, bindings));
    },
    {
      begin: () => timed("BEGIN", () => session.begin()),
      commit: () => timed("COMMIT", () => session.commit()),
      rollback: () => timed("ROLLBACK", () => session.rollback()),
      savepoint: (name: string) =>
        timed("SAVEPOINT " + name, () => session.savepoint(name)),
      rollbackTo: (name: string) =>
        timed("ROLLBACK TO " + name, () => session.rollbackTo(name)),
      lastSQL: "",
    },
  );
  return nq;
}
