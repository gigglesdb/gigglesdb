import { writeFileSync } from "node:fs";
export function save(path: string, value: unknown) {
  if (path) writeFileSync(path, JSON.stringify(value, null, 2) + "\n");
}
