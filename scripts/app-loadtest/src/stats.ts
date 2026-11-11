/** Exact sorted samples, reported in milliseconds. */
export function stats(a: number[]) {
  a.sort((x, y) => x - y);
  const at = (p: number) =>
    a[Math.min(a.length - 1, Math.floor(a.length * p))] ?? 0;
  return {
    count: a.length,
    p50: at(0.5),
    p95: at(0.95),
    p99: at(0.99),
    max: at(1),
  };
}
