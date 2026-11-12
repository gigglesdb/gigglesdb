const {spawnSync} = require('node:child_process');
const assert = require('node:assert/strict');
process.env.DATABASE_URL = `postgresql://postgres:${encodeURIComponent(process.env.PGPASSWORD)}@${process.env.PGHOST}:${process.env.PGPORT}/postgres?schema=public`;
const {PrismaClient} = require('@prisma/client');
const db = new PrismaClient({log: [{emit: 'event', level: 'query'}, {emit: 'stdout', level: 'error'}]});
const rows = [], sql = [];
db.$on('query', e => { sql.push({sql: e.query, parameters: e.params}); console.log('ORM_SQL=' + JSON.stringify(e)); });
async function step(name, action, blocked = false) {
  sql.length = 0;
  const row = {step: name, status: 'FAIL', classification: blocked ? 'blocked' : null};
  try {
    if (blocked) throw Error('Blocked by forward migration failure; no workaround schema installed.');
    await action(); row.status = 'PASS';
  } catch (e) {
    const state = e.meta?.code || e.message.match(/(?:database error code|SQLSTATE|code):?\s*["']?([0-9A-Z]{5})/i)?.[1];
    Object.assign(row, {error: e.message, exception: e.constructor.name, code: e.code, sqlstate: state || null,
      failures: [...sql], classification: blocked ? 'blocked' : (state || e.code ? 'product-gap' : 'needs-review')});
  }
  rows.push(row); return row.status === 'PASS';
}
async function main() {
  const ok = await step('migrate_deploy', async () => {
    const p = spawnSync('/app/node_modules/.bin/prisma', ['migrate', 'deploy'], {encoding: 'utf8', timeout: 90000,
      env: {...process.env, RUST_LOG: 'quaint=trace,schema_core=debug,sql_schema_connector=debug', RUST_LOG_FORMAT: 'json'}});
    const output = (p.stdout || '') + (p.stderr || ''); console.log(output);
    sql.push({sql: 'Prisma migrate deploy: see engine trace in stdout', output});
    if (p.status !== 0) throw Error(output || String(p.error));
  });
  await step('create_read', async () => {
    await db.user.create({data: {id: 1, email: 'ada@example.test'}});
    await db.order.create({data: {id: 10, user_id: 1, status: 'new', total: '12.50', created_at: new Date('2026-01-02T00:00:00Z')}});
    await db.orderItem.create({data: {order_id: 10, sku: 'book', quantity: 2}});
    const o = await db.order.findUniqueOrThrow({where: {id: 10}});
    assert.equal(o.total.toFixed(2), '12.50'); assert.equal(o.created_at.toISOString(), '2026-01-02T00:00:00.000Z');
  }, !ok);
  await step('relations', async () => {
    const o = await db.order.findUniqueOrThrow({where: {id: 10}, include: {user: true, items: true}});
    assert.equal(o.user.email, 'ada@example.test'); assert.equal(o.items[0].sku, 'book');
  }, !ok);
  await step('update', async () => { await db.order.update({where: {id: 10}, data: {status: 'paid'}});
    assert.equal((await db.order.findUniqueOrThrow({where: {id: 10}})).status, 'paid'); }, !ok);
  await step('transaction', async () => {
    await db.$transaction(async tx => { await tx.user.create({data: {id: 3, email: 'outer@example.test'}}); });
    await assert.rejects(db.$transaction(async tx => {
      await tx.user.create({data: {id: 4, email: 'inner@example.test'}}); throw Error('intentional rollback');
    }), /intentional rollback/);
    assert.equal(await db.user.count({where: {id: 3}}), 1); assert.equal(await db.user.count({where: {id: 4}}), 0);
  }, !ok);
  await step('unique_integrity', async () => {
    await assert.rejects(db.user.create({data: {id: 2, email: 'ada@example.test'}}), e => e.code === 'P2002');
  }, !ok);
  await step('filter_order_page', async () => assert.deepEqual((await db.user.findMany({where: {id: {gte: 1}},
    orderBy: {id: 'asc'}, skip: 1, take: 1})).map(u => u.id), [3]), !ok);
  await step('aggregate_count', async () => assert.equal(await db.order.count(), 1), !ok);
  await step('delete', async () => { await db.orderItem.delete({where: {order_id_sku: {order_id: 10, sku: 'book'}}});
    assert.equal(await db.orderItem.count(), 0); }, !ok);
  await step('down', async () => {
    for (const table of ['order_items', 'orders', 'users', '_prisma_migrations'])
      await db.$executeRawUnsafe(`DROP TABLE IF EXISTS "${table}" CASCADE`);
    assert.equal((await db.$queryRawUnsafe("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('users','orders','order_items','_prisma_migrations')")).length, 0);
  });
}
main().catch(e => rows.push({step: 'initialization', status: 'FAIL', classification: 'harness-issue', error: e.message}))
  .finally(async () => { await db.$disconnect(); console.log('ORM_RESULT=' + JSON.stringify(rows)); });
