# App SQL tester

Run asserted app operations through simple queries with safely inlined literals, unnamed extended queries, and reused named prepared statements. Each invocation creates a unique schema and drops it unless `--keep` is supplied. Node 22 is required; install with `npm install`.

```sh
npm run matrix -- --url postgres://postgres:PASSWORD@127.0.0.1:5432/postgres --report J:/gigglesdb-work/dg-refresh/r5/matrix.json
npm run load -- --url postgres://postgres:PASSWORD@127.0.0.1:5432/postgres --clients 8 --duration 30s --rows 2000
npm run load -- --url postgres://postgres:PASSWORD@127.0.0.1:5432/postgres --protocols simple --mix read=0,write=0,tx=100,error=0
```

On Windows PowerShell use `npm.cmd` so npm receives the option names unchanged. `DATABASE_URL` can replace `--url`. Connect to the [GigglesDB dev server](../../crates/giggles-server/README.md) with its owner password, user/database `postgres`, and TLS disabled.

| Flag             | Default / purpose                                                                 |
| ---------------- | --------------------------------------------------------------------------------- |
| --url            | DATABASE_URL                                                                      |
| --ops            | Comma separated names or star globs, in either mode                               |
| --variants       | Matrix variant name globs, e.g. prepared-tx-\*                                    |
| --protocols      | Load: simple,extended,prepared; uniform per call                                  |
| --mix            | Load kind weights: read=60,write=25,tx=10,error=5; omitted kinds have weight zero |
| --rows           | 1000 baseline users/accounts/orders; at least 10                                  |
| --seed           | 1; deterministic independent RNG per client                                       |
| --clients        | 8                                                                                 |
| --duration       | 30s                                                                               |
| --iterations     | Global call limit, replaces duration                                              |
| --max-error-rate | 0; unexpected errors / attempted calls                                            |
| --report         | JSON output path                                                                  |
| --keep           | Retain the run schema                                                             |
| --help           | Usage                                                                             |

Matrix reseeds 20 baseline users per case. Only batched operations vary n over 1, 10, 100; others run n=1. Transaction-owning operations run once per protocol, report n/a for the outer transaction axis, and record the redundant outer-tx case as n/a. The migration DDL batch is matrix-only and uses PostgreSQL's simple multi-statement query path. Every other catalog operation runs in both modes. Run `npm run ops` to regenerate this table:

| Name                      | Kind  | App behavior                                                        |
| ------------------------- | ----- | ------------------------------------------------------------------- |
| pk                        | read  | Profile: fetch a user by primary key                                |
| email                     | read  | Login: fetch a user by unique email                                 |
| pagination                | read  | Directory: read an ordered offset page                              |
| keyset                    | read  | Feed: read the next ordered keyset page                             |
| search                    | read  | Directory: search names by case insensitive prefix                  |
| range_in                  | read  | Filters: combine an id range and membership list                    |
| count_exists              | read  | Dashboard: count baseline users and check existence                 |
| join_sum                  | read  | Orders: verify joined item subtotals against order totals           |
| cte                       | read  | Analytics: select top customers using a common table expression     |
| window                    | read  | History: rank orders within each customer                           |
| large_in                  | read  | Batch lookup: resolve at least one thousand bound ids               |
| insert_returning          | write | Signup: create a user and return its generated id                   |
| multi_insert              | write | Import: sign up a batch in one multi row insert                     |
| update                    | write | Profile: update an owned user and return the new name               |
| optimistic                | write | Profile: reject a stale version on the second update                |
| upsert                    | write | Import: update existing signups and ignore duplicate conflicts      |
| roundtrip                 | write | Form: preserve NULL timestamp exact numeric and Unicode quoted text |
| bulk_delete               | write | Cleanup: delete only users created by this call                     |
| ddl_multi_statement_batch | write | Migration: execute related table and index DDL as one batch         |
| transfer                  | tx    | Banking: lock two accounts in id order and transfer available funds |
| place_order               | tx    | Checkout: atomically create an order items and exact total          |
| cancel_order              | tx    | Cancellation: delete an owned order and cascade its items           |
| rollback                  | tx    | Abandoned signup: roll back and verify no user remains              |
| savepoint                 | tx    | Optional signup: roll back a savepoint and keep the session usable  |
| unique                    | error | Signup validation: reject a duplicate email and recover             |
| fk                        | error | Order validation: reject an unknown customer and recover            |
| check                     | error | Bank validation: reject a negative balance and recover              |

Load chooses a kind by weight, an operation uniformly within that kind, and a protocol uniformly from `--protocols`; batched operations choose n uniformly from 1/10/100. Writers use unique run/client/sequence tokens and only mutate their own users. Transfers lock accounts in id order; order creation/deletion is atomic. Expected constraint failures verify SQLSTATE and session recovery and do not count as errors. SQLSTATE 40001/40P01 retries are capped at five and counted separately.

Exit 0 means all assertions and invariants pass within the configured error threshold. Exit 1 means matrix failure, invariant failure, excess unexpected errors, setup failure, or invalid arguments. Matrix emits one JSON result per executed case followed by a complete report. Load emits progress every two seconds and a final report, for example:

```text
completed=1871 retries=0
{"count":27679,"throughput":922.09,"errors":{},"retries":0,"invariants":{"balances":true,"nonnegative":true,"totals":true,"orphans":true,"users":true},"failed":false,...}
```

Matrix JSON contains `{schema,mode,results:[{op,variant,status,ms,code?,message?,sql?}],failed}`. Load JSON includes `count`, `elapsed`, `throughput`, `latency`, `perOp`, `perProtocol` (keys `op/protocol`), `errors`, `details`, `retries`, `counters`, `invariants`, and `failed`. Latency objects contain count/p50/p95/p99/max in milliseconds, including failed calls and retry time. Error keys are `op:SQLSTATE`; details include protocol and exact SQL. End checks cover conserved account balances, nonnegative balances, every order total against item prices, orphan items, and persisted run-created users against successful creation/deletion counters. Baseline order item quantities and product ids derive deterministically from the order id, and prices use exact quarter units.

Operation latency includes all statements, retries and commits. perOpBatch groups op/n, perStatement groups op/SQL keywords, and statementsPerOp counts statements. Text summaries print the top ten p99 for each granularity.
