import { type Q } from "./db.js";
export async function setup(q: Q) {
  await q("CREATE SCHEMA " + q.schema);
  for (const sql of `CREATE TABLE {s}users(id bigserial PRIMARY KEY,email text UNIQUE NOT NULL,name text,created_at timestamptz DEFAULT now(),active boolean DEFAULT true,version integer DEFAULT 0); CREATE TABLE {s}accounts(id bigint PRIMARY KEY,user_id bigint REFERENCES {s}users(id) ON DELETE CASCADE,balance numeric(12,2) CHECK(balance>=0)); CREATE TABLE {s}products(id bigint PRIMARY KEY,sku text UNIQUE,price numeric(12,2),tags text,attributes jsonb); CREATE TABLE {s}orders(id bigserial PRIMARY KEY,user_id bigint REFERENCES {s}users(id) ON DELETE CASCADE,total numeric(12,2)); CREATE TABLE {s}order_items(order_id bigint REFERENCES {s}orders(id) ON DELETE CASCADE,product_id bigint REFERENCES {s}products(id),qty integer); CREATE INDEX ON {s}orders(user_id); CREATE INDEX ON {s}order_items(order_id); CREATE INDEX ON {s}accounts(user_id)`
    .split(";")
    .map((x) => x.trim())
    .filter(Boolean))
    await q(sql);
}
export async function seed(q: Q, rows: number) {
  for (const table of [
    "order_items",
    "orders",
    "accounts",
    "products",
    "users",
  ])
    await q("DELETE FROM {s}" + table);
  for (const table of ["users", "orders"])
    await q("ALTER SEQUENCE {s}" + table + "_id_seq RESTART");
  async function insert(table: string, cols: string, data: unknown[][]) {
    for (let start = 0; start < data.length; start += 20) {
      const batch = data.slice(start, start + 20);
      const values = batch.flat();
      let n = 0;
      await q(
        "INSERT INTO {s}" +
          table +
          "(" +
          cols +
          ") VALUES " +
          batch
            .map((row) => "(" + row.map(() => "$" + ++n).join(",") + ")")
            .join(","),
        values,
      );
    }
  }
  const products = Math.max(10, Math.floor(rows / 10));
  const users: unknown[][] = [],
    accounts: unknown[][] = [],
    orders: unknown[][] = [],
    items: unknown[][] = [];
  for (let i = 1; i <= rows; i++) {
    users.push([i, `user${i}@example.com`, `User ${i}`]);
    accounts.push([i, i, 100]);
    let total = 0;
    for (let k = 1; k <= 1 + (i % 3); k++) {
      const product = 1 + ((i + k - 2) % products);
      items.push([i, product, k]);
      total += k * (product + 0.25);
    }
    orders.push([i, i, total]);
  }
  await insert("users", "id,email,name", users);
  await insert("accounts", "id,user_id,balance", accounts);
  await insert(
    "products",
    "id,sku,price,tags,attributes",
    Array.from({ length: products }, (_, j) => [
      j + 1,
      `SKU-${j + 1}`,
      j + 1.25,
      "tag",
      "{}",
    ]),
  );
  await insert("orders", "id,user_id,total", orders);
  await insert("order_items", "order_id,product_id,qty", items);
  for (const table of ["users", "orders"])
    await q("SELECT setval($1,$2::bigint)", [
      q.schema + "." + table + "_id_seq",
      rows,
    ]);
}
