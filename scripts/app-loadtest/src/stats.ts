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

/** One recorder per load invocation; warmup uses a separate invocation. */
export class LatencyRecorder {
  private samples = new Map<string, number[]>();
  private errors = new Map<string, number>();

  record(key: string, ms: number, failed = false) {
    const samples = this.samples.get(key) ?? [];
    samples.push(ms);
    this.samples.set(key, samples);
    if (failed) this.errors.set(key, (this.errors.get(key) ?? 0) + 1);
  }

  summary() {
    return Object.fromEntries(
      [...this.samples].map(([key, samples]) => {
        const s = stats(samples);
        return [
          key,
          {
            count: s.count,
            p50_ms: s.p50,
            p95_ms: s.p95,
            p99_ms: s.p99,
            max_ms: s.max,
            errors: this.errors.get(key) ?? 0,
          },
        ];
      }),
    );
  }
}
