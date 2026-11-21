# GigglesDB compatibility report

Run: r0-20261121T111418Z-303688
Started: 2026-11-21T11:14:18.442081Z
Finished: 2026-11-21T13:01:19.728418Z

Commit: 895147dd696978d40539d20abaf1f0553c5997eb (labelled-by-operator)
Server binary SHA-256: 30ee9f85dc89c993bb1e6ec70016a3a329c2642f84b9f5e75b644f606fc3e506
Oracle: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2); digest: sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42; collation: C.UTF-8; TZ: UTC
Clients: pg_dump -> pg_dump (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2); digest: sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42; psql -> psql (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2); digest: sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42; psycopg -> 3.2.9; digest: sha256:31324d1c4c6cf65dba2a3db8a1c745ef56a357bece09738c766bc26a6585eac6; node-pg -> 8.16.3; digest: sha256:5ac0faf6d1349a9c48c444c671f7df4215da0288547c4ba84ca845b70324cfb5; pgx -> 5.7.6; digest: sha256:283855a569dca65709462dd02244eeda75093dec91d7866eb95501f2f88f11a8; npgsql -> 9.0.4; digest: sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f; npgsql-notypes -> 9.0.4; digest: sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f; tokio-postgres -> 0.7.15; digest: sha256:f8df3216debd1932b6cdbe5211fda75b1792f52365ad5041fffea60e3b0eefc5; jdbc -> 42.7.3; digest: sha256:31cb4f391f3434796242cc0c12cba70d5a23882d1c640d76e32fc213c4e527e1

Identity:
```json
{
  "harness": {
    "commit": "895147dd696978d40539d20abaf1f0553c5997eb",
    "dirty": false
  },
  "build": {
    "commit": "895147dd696978d40539d20abaf1f0553c5997eb",
    "build_identity": "labelled-by-operator",
    "server_mode": "single-node-dev",
    "binary_sha256": "30ee9f85dc89c993bb1e6ec70016a3a329c2642f84b9f5e75b644f606fc3e506",
    "binary_size": 99315200,
    "stdout_banner": [
      "postgresql=127.0.0.1:63296 preview=single-host",
      "[REDACTED_URL]"
    ],
    "exit": 0
  },
  "oracle": {
    "image": "postgres:18.6",
    "digest": "sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42",
    "initdb_args": "--locale=C.UTF-8 --encoding=UTF8",
    "tz": "UTC",
    "server_version": "18.6 (Debian 18.6-1.pgdg13+2)",
    "collation": "C.UTF-8",
    "timezone": "UTC",
    "encoding": "UTF8",
    "oracle_log": "oracle.log"
  },
  "clients": {
    "pg_dump": {
      "image": "postgres:18.6",
      "digest": "sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42",
      "available": true,
      "version": "pg_dump (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2)"
    },
    "psql": {
      "image": "postgres:18.6",
      "digest": "sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42",
      "available": true,
      "version": "psql (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2)"
    },
    "psycopg": {
      "available": true,
      "tag": "gd-r0-probe-psycopg:38a03c6417ed",
      "reason": "",
      "image_id": "sha256:31324d1c4c6cf65dba2a3db8a1c745ef56a357bece09738c766bc26a6585eac6",
      "digest": "sha256:31324d1c4c6cf65dba2a3db8a1c745ef56a357bece09738c766bc26a6585eac6",
      "repo_digests": [
        "gd-r0-probe-psycopg@sha256:31324d1c4c6cf65dba2a3db8a1c745ef56a357bece09738c766bc26a6585eac6"
      ],
      "expected_version": "3.2.9",
      "image_build_cache": "hit",
      "image_build_duration_s": 0,
      "image": "gd-r0-probe-psycopg:38a03c6417ed",
      "version": "3.2.9",
      "libpq_version": 170005
    },
    "node-pg": {
      "available": true,
      "tag": "gd-r0-probe-node-pg:4b0907d98610",
      "reason": "",
      "image_id": "sha256:5ac0faf6d1349a9c48c444c671f7df4215da0288547c4ba84ca845b70324cfb5",
      "digest": "sha256:5ac0faf6d1349a9c48c444c671f7df4215da0288547c4ba84ca845b70324cfb5",
      "repo_digests": [
        "gd-r0-probe-node-pg@sha256:5ac0faf6d1349a9c48c444c671f7df4215da0288547c4ba84ca845b70324cfb5"
      ],
      "expected_version": "8.16.3",
      "image_build_cache": "hit",
      "image_build_duration_s": 0,
      "image": "gd-r0-probe-node-pg:4b0907d98610",
      "version": "8.16.3"
    },
    "pgx": {
      "available": true,
      "tag": "gd-r0-probe-pgx:f7449e4a2911",
      "reason": "",
      "image_id": "sha256:283855a569dca65709462dd02244eeda75093dec91d7866eb95501f2f88f11a8",
      "digest": "sha256:283855a569dca65709462dd02244eeda75093dec91d7866eb95501f2f88f11a8",
      "repo_digests": [
        "gd-r0-probe-pgx@sha256:283855a569dca65709462dd02244eeda75093dec91d7866eb95501f2f88f11a8"
      ],
      "expected_version": "5.7.6",
      "image_build_cache": "hit",
      "image_build_duration_s": 0,
      "image": "gd-r0-probe-pgx:f7449e4a2911",
      "version": "5.7.6"
    },
    "npgsql": {
      "available": true,
      "tag": "gd-r0-probe-npgsql:4c5d54a1c85a",
      "reason": "",
      "image_id": "sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f",
      "digest": "sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f",
      "repo_digests": [
        "gd-r0-probe-npgsql@sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f"
      ],
      "expected_version": "9.0.4",
      "image_build_cache": "hit",
      "image_build_duration_s": 0,
      "image": "gd-r0-probe-npgsql:4c5d54a1c85a",
      "version": "9.0.4"
    },
    "npgsql-notypes": {
      "available": true,
      "tag": "gd-r0-probe-npgsql:4c5d54a1c85a",
      "reason": "",
      "image_id": "sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f",
      "digest": "sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f",
      "repo_digests": [
        "gd-r0-probe-npgsql@sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f"
      ],
      "expected_version": "9.0.4",
      "image_build_cache": "hit",
      "image_build_duration_s": 0,
      "image": "gd-r0-probe-npgsql:4c5d54a1c85a",
      "version": "9.0.4"
    },
    "tokio-postgres": {
      "available": true,
      "tag": "gd-r0-probe-tokio-postgres:a23fb03a969e",
      "reason": "",
      "image_id": "sha256:f8df3216debd1932b6cdbe5211fda75b1792f52365ad5041fffea60e3b0eefc5",
      "digest": "sha256:f8df3216debd1932b6cdbe5211fda75b1792f52365ad5041fffea60e3b0eefc5",
      "repo_digests": [
        "gd-r0-probe-tokio-postgres@sha256:f8df3216debd1932b6cdbe5211fda75b1792f52365ad5041fffea60e3b0eefc5"
      ],
      "expected_version": "0.7.15",
      "image_build_cache": "hit",
      "image_build_duration_s": 0,
      "image": "gd-r0-probe-tokio-postgres:a23fb03a969e",
      "version": "0.7.15"
    },
    "jdbc": {
      "available": true,
      "tag": "gd-r0-probe-jdbc:e821a8a43f6a",
      "reason": "",
      "image_id": "sha256:31cb4f391f3434796242cc0c12cba70d5a23882d1c640d76e32fc213c4e527e1",
      "digest": "sha256:31cb4f391f3434796242cc0c12cba70d5a23882d1c640d76e32fc213c4e527e1",
      "repo_digests": [
        "gd-r0-probe-jdbc@sha256:31cb4f391f3434796242cc0c12cba70d5a23882d1c640d76e32fc213c4e527e1"
      ],
      "expected_version": "42.7.3",
      "image_build_cache": "hit",
      "image_build_duration_s": 0,
      "image": "gd-r0-probe-jdbc:e821a8a43f6a",
      "version": "42.7.3"
    }
  },
  "networking": {
    "hostname": "host.docker.internal",
    "add_host": "host.docker.internal:host-gateway",
    "listener": "127.0.0.1",
    "status": "reached-server"
  },
  "normalization_rules": [
    "crlf",
    "trailing-ws"
  ]
}
```

| Client | PASS | FAIL | UNTESTED | HARNESS_ERROR | Pass rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| pg_dump | 7 | 0 | 0 | 0 | 100.0% |
| psql | 16 | 4 | 0 | 0 | 80.0% |
| psycopg | 19 | 0 | 0 | 0 | 100.0% |
| node-pg | 18 | 0 | 0 | 0 | 100.0% |
| pgx | 19 | 0 | 0 | 0 | 100.0% |
| npgsql | 19 | 0 | 0 | 0 | 100.0% |
| npgsql-notypes | 19 | 0 | 0 | 0 | 100.0% |
| tokio-postgres | 18 | 0 | 0 | 0 | 100.0% |
| jdbc | 19 | 0 | 0 | 0 | 100.0% |

| Client | Check | Owner | Outcome |
| --- | --- | --- | --- |
| pg_dump | fixture_apply | E1 | PASS |
| pg_dump | dump_schema_runs | G | PASS |
| pg_dump | dump_schema_restores_into_pg | G | PASS |
| pg_dump | dump_schema_equivalent | G | PASS |
| pg_dump | dump_data_runs | G | PASS |
| pg_dump | dump_full_restores_into_pg | G | PASS |
| pg_dump | pg_dump_restores_into_giggles | E1 | PASS |
| psql | connect | A | PASS |
| psql | create_table | E1 | PASS |
| psql | delete | E1 | PASS |
| psql | drop_table | E1 | PASS |
| psql | insert | E1 | PASS |
| psql | insert_multirow | E1 | PASS |
| psql | meta_d | G | PASS |
| psql | meta_d_table | G | PASS |
| psql | meta_dt | G | PASS |
| psql | pgt_startup_appname | D | PASS |
| psql | pgt_startup_datestyle | D | FAIL |
| psql | pgt_startup_encoding | D | FAIL |
| psql | pgt_startup_options | D | FAIL |
| psql | pgt_startup_timezone | D | FAIL |
| psql | select | E3 | PASS |
| psql | startup_timezone | A | PASS |
| psql | transaction_commit | D | PASS |
| psql | transaction_error_recovery | D | PASS |
| psql | transaction_rollback | D | PASS |
| psql | update | E1 | PASS |
| psycopg | connect | A | PASS |
| psycopg | keepalive | D | PASS |
| psycopg | ddl_create | E1 | PASS |
| psycopg | prepared_insert | E1 | PASS |
| psycopg | prepared_point_select | E3 | PASS |
| psycopg | prepared_repeat | D | PASS |
| psycopg | select_ordered | E3 | PASS |
| psycopg | update_delete_params | E3 | PASS |
| psycopg | tx_commit | D | PASS |
| psycopg | tx_rollback | D | PASS |
| psycopg | tx_error_recovery | D | PASS |
| psycopg | savepoint | unowned | PASS |
| psycopg | types_roundtrip | E2 | PASS |
| psycopg | param_bytea_position | E2 | PASS |
| psycopg | param_no_from_aggregate | DD-0068 | PASS |
| psycopg | copy_from_stdin | unowned | PASS |
| psycopg | client_cursor_simple_protocol | D | PASS |
| psycopg | introspection_information_schema | G | PASS |
| psycopg | server_side_cursor | D | PASS |
| node-pg | connect | A | PASS |
| node-pg | keepalive | D | PASS |
| node-pg | ddl_create | E1 | PASS |
| node-pg | prepared_insert | E1 | PASS |
| node-pg | prepared_point_select | E3 | PASS |
| node-pg | prepared_repeat | D | PASS |
| node-pg | select_ordered | E3 | PASS |
| node-pg | update_delete_params | E3 | PASS |
| node-pg | tx_commit | D | PASS |
| node-pg | tx_rollback | D | PASS |
| node-pg | tx_error_recovery | D | PASS |
| node-pg | savepoint | unowned | PASS |
| node-pg | types_roundtrip | E2 | PASS |
| node-pg | param_bytea_position | E2 | PASS |
| node-pg | param_no_from_aggregate | DD-0068 | PASS |
| node-pg | multi_statement_simple_query | D | PASS |
| node-pg | rowmode_array | E3 | PASS |
| node-pg | introspection_pg_catalog | G | PASS |
| pgx | connect | A | PASS |
| pgx | keepalive | D | PASS |
| pgx | ddl_create | E1 | PASS |
| pgx | prepared_insert | E1 | PASS |
| pgx | prepared_point_select | E3 | PASS |
| pgx | prepared_repeat | D | PASS |
| pgx | select_ordered | E3 | PASS |
| pgx | update_delete_params | E3 | PASS |
| pgx | tx_commit | D | PASS |
| pgx | tx_rollback | D | PASS |
| pgx | tx_error_recovery | D | PASS |
| pgx | savepoint | unowned | PASS |
| pgx | types_roundtrip | E2 | PASS |
| pgx | param_bytea_position | E2 | PASS |
| pgx | param_no_from_aggregate | DD-0068 | PASS |
| pgx | exec_mode_simple | D | PASS |
| pgx | batch | D | PASS |
| pgx | copy_from | unowned | PASS |
| pgx | field_descriptions | G | PASS |
| npgsql | connect | A | PASS |
| npgsql | keepalive | D | PASS |
| npgsql | ddl_create | E1 | PASS |
| npgsql | prepared_insert | E1 | PASS |
| npgsql | prepared_point_select | E3 | PASS |
| npgsql | prepared_repeat | D | PASS |
| npgsql | select_ordered | E3 | PASS |
| npgsql | update_delete_params | E3 | PASS |
| npgsql | tx_commit | D | PASS |
| npgsql | tx_rollback | D | PASS |
| npgsql | tx_error_recovery | D | PASS |
| npgsql | savepoint | unowned | PASS |
| npgsql | types_roundtrip | E2 | PASS |
| npgsql | param_bytea_position | E2 | PASS |
| npgsql | param_no_from_aggregate | DD-0068 | PASS |
| npgsql | get_schema_tables | G | PASS |
| npgsql | get_schema_columns | G | PASS |
| npgsql | prepare_explicit | D | PASS |
| npgsql | binary_import | unowned | PASS |
| npgsql-notypes | connect | A | PASS |
| npgsql-notypes | keepalive | D | PASS |
| npgsql-notypes | ddl_create | E1 | PASS |
| npgsql-notypes | prepared_insert | E1 | PASS |
| npgsql-notypes | prepared_point_select | E3 | PASS |
| npgsql-notypes | prepared_repeat | D | PASS |
| npgsql-notypes | select_ordered | E3 | PASS |
| npgsql-notypes | update_delete_params | E3 | PASS |
| npgsql-notypes | tx_commit | D | PASS |
| npgsql-notypes | tx_rollback | D | PASS |
| npgsql-notypes | tx_error_recovery | D | PASS |
| npgsql-notypes | savepoint | unowned | PASS |
| npgsql-notypes | types_roundtrip | E2 | PASS |
| npgsql-notypes | param_bytea_position | E2 | PASS |
| npgsql-notypes | param_no_from_aggregate | DD-0068 | PASS |
| npgsql-notypes | get_schema_tables | G | PASS |
| npgsql-notypes | get_schema_columns | G | PASS |
| npgsql-notypes | prepare_explicit | D | PASS |
| npgsql-notypes | binary_import | unowned | PASS |
| tokio-postgres | connect | A | PASS |
| tokio-postgres | keepalive | D | PASS |
| tokio-postgres | ddl_create | E1 | PASS |
| tokio-postgres | prepared_insert | E1 | PASS |
| tokio-postgres | prepared_point_select | E3 | PASS |
| tokio-postgres | prepared_repeat | D | PASS |
| tokio-postgres | select_ordered | E3 | PASS |
| tokio-postgres | update_delete_params | E3 | PASS |
| tokio-postgres | tx_commit | D | PASS |
| tokio-postgres | tx_rollback | D | PASS |
| tokio-postgres | tx_error_recovery | D | PASS |
| tokio-postgres | savepoint | unowned | PASS |
| tokio-postgres | types_roundtrip | E2 | PASS |
| tokio-postgres | param_bytea_position | E2 | PASS |
| tokio-postgres | param_no_from_aggregate | DD-0068 | PASS |
| tokio-postgres | simple_query | D | PASS |
| tokio-postgres | statement_types | G | PASS |
| tokio-postgres | typed_prepare | E2 | PASS |
| jdbc | connect | A | PASS |
| jdbc | keepalive | D | PASS |
| jdbc | ddl_create | E1 | PASS |
| jdbc | prepared_insert | E1 | PASS |
| jdbc | prepared_point_select | E3 | PASS |
| jdbc | prepared_repeat | D | PASS |
| jdbc | select_ordered | E3 | PASS |
| jdbc | update_delete_params | E3 | PASS |
| jdbc | tx_commit | D | PASS |
| jdbc | tx_rollback | D | PASS |
| jdbc | tx_error_recovery | D | PASS |
| jdbc | savepoint | unowned | PASS |
| jdbc | metadata_tables | G | PASS |
| jdbc | metadata_columns | G | PASS |
| jdbc | metadata_pk | G | PASS |
| jdbc | metadata_typeinfo | G | PASS |
| jdbc | param_bytea_position | E2 | PASS |
| jdbc | param_no_from_aggregate | DD-0068 | PASS |
| jdbc | types_roundtrip | E2 | PASS |

## Failures by owning track

### D
psql/pgt_startup_datestyle, psql/pgt_startup_encoding, psql/pgt_startup_options, psql/pgt_startup_timezone

psql/pgt_startup_datestyle
```diff
=== stdout ===
--- pg
+++ giggles
@@ -1,9 +0,0 @@
--- id: pgt_startup_datestyle
--- title: Startup pgt_startup_datestyle
--- owner: D
--- env: PGDATESTYLE=SQL, DMY
-SELECT current_setting('DateStyle'), DATE '2024-01-02';
- current_setting |    date
------------------+------------
- SQL, DMY        | 02/01/2024
-(1 row)
```

First GigglesDB error:
```text
psql: error: connection to server at "host.docker.internal" (192.168.65.254), port 63296 failed: ERROR:  startup DateStyle must be ISO
```

psql/pgt_startup_encoding
```diff
=== stdout ===
--- pg
+++ giggles
@@ -1,9 +0,0 @@
--- id: pgt_startup_encoding
--- title: Startup pgt_startup_encoding
--- owner: D
--- env: PGCLIENTENCODING=LATIN1
-SELECT current_setting('client_encoding');
- current_setting
------------------
- LATIN1
-(1 row)
```

First GigglesDB error:
```text
psql: error: connection to server at "host.docker.internal" (192.168.65.254), port 63296 failed: ERROR:  startup client_encoding must be UTF8
```

psql/pgt_startup_options
```diff
=== stdout ===
--- pg
+++ giggles
@@ -1,9 +0,0 @@
--- id: pgt_startup_options
--- title: Startup pgt_startup_options
--- owner: D
--- env: PGOPTIONS=-c work_mem=16MB -c default_transaction_isolation=repeatable\ read
-SELECT current_setting('work_mem'), current_setting('default_transaction_isolation');
- current_setting | current_setting
------------------+-----------------
- 16MB            | repeatable read
-(1 row)
```

First GigglesDB error:
```text
psql: error: connection to server at "host.docker.internal" (192.168.65.254), port 63296 failed: ERROR:  unrecognized configuration parameter
```

psql/pgt_startup_timezone
```diff
=== stdout ===
--- pg
+++ giggles
@@ -3,7 +3,3 @@
 -- owner: D
 -- env: PGTZ=Asia/Kolkata
 SELECT current_setting('TimeZone'), TIMESTAMPTZ '2024-01-02 00:00:00+00';
- current_setting |        timestamptz
------------------+---------------------------
- Asia/Kolkata    | 2024-01-02 05:30:00+05:30
-(1 row)
```

First GigglesDB error:
```text
psql:<stdin>:5: ERROR:  unsupported SQL feature or parameter type in this server profile
```

## Differential SQL

| Area | Cases | COMPATIBLE | UNSUPPORTED | DIVERGENT | INTENTIONAL | UNTESTED | HARNESS_ERROR | Pass rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Overall | 991 | 488 | 413 | 90 | 0 | 0 | 0 | 49.2% |
| aggregates | 9 | 9 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| alter_table | 8 | 8 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| constraints | 8 | 8 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| datagrip | 36 | 36 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| defaults | 8 | 8 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| dml_predicates | 9 | 9 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| identifiers | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| insert | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| joins | 6 | 6 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| known_gaps | 26 | 23 | 1 | 2 | 0 | 0 | 0 | 88.5% |
| on_conflict | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| pg_ddl | 168 | 19 | 139 | 10 | 0 | 0 | 0 | 11.3% |
| pg_functions | 62 | 11 | 43 | 8 | 0 | 0 | 0 | 17.7% |
| pg_limits | 12 | 6 | 4 | 2 | 0 | 0 | 0 | 50.0% |
| pg_queries | 225 | 218 | 7 | 0 | 0 | 0 | 0 | 96.9% |
| pg_roles | 25 | 9 | 15 | 1 | 0 | 0 | 0 | 36.0% |
| pg_routines | 91 | 5 | 84 | 2 | 0 | 0 | 0 | 5.5% |
| pg_silent | 31 | 31 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| pg_txn | 35 | 12 | 18 | 5 | 0 | 0 | 0 | 34.3% |
| pg_types | 205 | 43 | 102 | 60 | 0 | 0 | 0 | 21.0% |
| returning | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| seed | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| session | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| transactions | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 100.0% |

## Differential failures by owning track

| Case | Owner | Comparison | Outcome | Row counts (informational) |
| --- | --- | --- | --- | --- |
| avg_numeric_and_float | E3 | compare: full | COMPATIBLE | PG [0, 4, 1, 1]; Giggles [0, 4, 1, 1] |
| count_distinct | E3 | compare: full | COMPATIBLE | PG [0, 5, 1, 2]; Giggles [0, 5, 1, 2] |
| empty_group_and_global | E3 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 0]; Giggles [0, 0, 1, 1, 0] |
| group_by_count | E3 | compare: full | COMPATIBLE | PG [0, 5, 3]; Giggles [0, 5, 3] |
| group_by_expression | E3 | compare: full | COMPATIBLE | PG [0, 4, 3, 3]; Giggles [0, 4, 3, 3] |
| group_by_having | E3 | compare: full | COMPATIBLE | PG [0, 5, 2, 2]; Giggles [0, 5, 2, 2] |
| group_by_ordinal_alias | E3 | compare: full | COMPATIBLE | PG [0, 3, 2, 2]; Giggles [0, 3, 2, 2] |
| group_by_pk_dependency_django | E3 | compare: full | COMPATIBLE | PG [0, 0, 3, 3, 3]; Giggles [0, 0, 3, 3, 3] |
| sum_avg_distinct_grouped | E3 | compare: full | COMPATIBLE | PG [0, 5, 2, 2, 1]; Giggles [0, 5, 2, 2, 1] |
| add_column | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| add_column_with_default | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1, 2]; Giggles [0, 1, 0, 1, 2] |
| add_constraint_unique | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 0, 1]; Giggles [0, 1, 0, 0, 1] |
| alter_column_drop_not_null_and_set_not_null | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 1, 0, 0, 1, 1]; Giggles [0, 0, 1, 1, 1, 0, 0, 1, 1] |
| alter_column_set_default | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1, 2]; Giggles [0, 1, 0, 1, 2] |
| drop_column | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| rename_column | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| rename_table | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| check_violation | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 1]; Giggles [0, 0, 0, 1, 1] |
| composite_primary_key | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 0, 2]; Giggles [0, 1, 1, 0, 2] |
| fk_delete_restrict | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 0, 1, 1]; Giggles [0, 0, 1, 1, 0, 1, 1] |
| fk_on_delete_cascade | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 1, 0, 0]; Giggles [0, 0, 1, 1, 1, 0, 0] |
| fk_violation | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1]; Giggles [0, 0, 0, 1] |
| not_null_violation | E1 | compare: full | COMPATIBLE | PG [0, 0, 1]; Giggles [0, 0, 1] |
| unique_multi_column | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 0, 2]; Giggles [0, 1, 1, 0, 2] |
| unique_violation | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| datagrip_catalog_acls | G | compare: header | COMPATIBLE | PG [0, 0, 9]; Giggles [0, 0, 4] |
| datagrip_databases | G | compare: header | COMPATIBLE | PG [0, 0, 9]; Giggles [0, 0, 1] |
| datagrip_datestyle | G | compare: header | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| datagrip_event_triggers | G | compare: header | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| datagrip_existent_languages | G | compare: header | COMPATIBLE | PG [0, 0, 4]; Giggles [0, 0, 0] |
| datagrip_flat_comments | G | compare: header | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| datagrip_full_schema_object_acls | G | compare: header | COMPATIBLE | PG [0, 0, 323]; Giggles [0, 0, 260] |
| datagrip_languages | G | compare: header | COMPATIBLE | PG [0, 0, 4]; Giggles [0, 0, 0] |
| datagrip_listcasts | G | compare: header | COMPATIBLE | PG [0, 0, 235]; Giggles [0, 0, 26] |
| datagrip_retrieveaccessmethods | G | compare: header | COMPATIBLE | PG [0, 0, 7]; Giggles [0, 0, 0] |
| datagrip_retrievecolumns | G | compare: header | COMPATIBLE | PG [0, 0, 178]; Giggles [0, 0, 193] |
| datagrip_retrievecolumns_dceb66d8c64d | G | compare: header | COMPATIBLE | PG [0, 0, 178]; Giggles [0, 0, 193] |
| datagrip_retrieveconstraints | G | compare: header | COMPATIBLE | PG [0, 0, 82]; Giggles [0, 0, 89] |
| datagrip_retrieveconstraints_9df0a4e5953e | G | compare: header | COMPATIBLE | PG [0, 0, 82]; Giggles [0, 0, 89] |
| datagrip_retrieveexistentconstrain_644b4beeaf5a | G | compare: header | COMPATIBLE | PG [0, 0, 165]; Giggles [0, 0, 178] |
| datagrip_retrieveexistentconstraints | G | compare: header | COMPATIBLE | PG [0, 0, 165]; Giggles [0, 0, 178] |
| datagrip_retrieveexistenttables | G | compare: header | COMPATIBLE | PG [0, 0, 80]; Giggles [0, 0, 85] |
| datagrip_retrieveexistenttables_38c1901282c3 | G | compare: header | COMPATIBLE | PG [0, 0, 80]; Giggles [0, 0, 85] |
| datagrip_retrieveextensionmembers | G | compare: header | COMPATIBLE | PG [0, 0, 4]; Giggles [0, 0, 0] |
| datagrip_retrieveextensions | G | compare: header | COMPATIBLE | PG [0, 0, 1]; Giggles [0, 0, 1] |
| datagrip_retrieveforeigndatawrappers | G | compare: header | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| datagrip_retrieveforeignservers | G | compare: header | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| datagrip_retrieveindexcolumns | G | compare: header | COMPATIBLE | PG [0, 0, 83]; Giggles [0, 0, 90] |
| datagrip_retrieveindexcolumns_48727062bd45 | G | compare: header | COMPATIBLE | PG [0, 0, 83]; Giggles [0, 0, 90] |
| datagrip_retrieveindices | G | compare: header | COMPATIBLE | PG [0, 0, 83]; Giggles [0, 0, 90] |
| datagrip_retrieveindices_8e659477d09d | G | compare: header | COMPATIBLE | PG [0, 0, 83]; Giggles [0, 0, 90] |
| datagrip_retrieveroles | G | compare: header | COMPATIBLE | PG [0, 0, 17]; Giggles [0, 0, 18] |
| datagrip_retrievetables | G | compare: header | COMPATIBLE | PG [0, 0, 80]; Giggles [0, 0, 85] |
| datagrip_retrievetables_c222cc0a5577 | G | compare: header | COMPATIBLE | PG [0, 0, 80]; Giggles [0, 0, 85] |
| datagrip_retrievetablespaces | G | compare: header | COMPATIBLE | PG [0, 0, 2]; Giggles [0, 0, 2] |
| datagrip_retrievetimezones | G | compare: header | COMPATIBLE | PG [0, 0, 680]; Giggles [0, 0, 680] |
| datagrip_retrieveusermappings | G | compare: header | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| datagrip_role_memberships | G | compare: header | COMPATIBLE | PG [0, 0, 3]; Giggles [0, 0, 3] |
| datagrip_schemas | G | compare: header | COMPATIBLE | PG [0, 0, 5]; Giggles [0, 0, 4] |
| datagrip_shared_acls | G | compare: header | COMPATIBLE | PG [0, 0, 11]; Giggles [0, 0, 3] |
| datagrip_superuser | G | compare: header | COMPATIBLE | PG [0, 0, 1]; Giggles [0, 0, 1] |
| bigserial_column | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 2]; Giggles [0, 1, 1, 2] |
| create_table_without_primary_key | F | compare: full | COMPATIBLE | PG [0, 1, 1, 2]; Giggles [0, 1, 1, 2] |
| default_keyword_in_values | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 2]; Giggles [0, 1, 1, 2] |
| default_literal | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| identity_always | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1, 2]; Giggles [0, 1, 0, 1, 2] |
| identity_by_default | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 2]; Giggles [0, 1, 1, 2] |
| sequence_basic | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 1]; Giggles [0, 1, 1, 1, 1] |
| serial_column | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 2]; Giggles [0, 1, 1, 2] |
| delete_with_in_subquery | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 1, 1, 1]; Giggles [0, 0, 1, 1, 1, 1, 1] |
| update_multiple_columns | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| update_set_expression | E3 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 1]; Giggles [0, 1, 1, 1, 1] |
| update_where_between | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 2, 3]; Giggles [0, 1, 1, 1, 2, 3] |
| where_in_list | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 2]; Giggles [0, 1, 1, 1, 2] |
| where_is_null_and_is_not_null | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 1]; Giggles [0, 1, 1, 1, 1] |
| where_like_and_ilike | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 1, 2]; Giggles [0, 1, 1, 1, 1, 2] |
| where_not_equal | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 1]; Giggles [0, 1, 1, 1, 1] |
| where_or | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 2]; Giggles [0, 1, 1, 1, 2] |
| quoted_identifier_with_space | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| quoted_mixed_case_table | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| quoted_reserved_word_columns | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| quoted_vs_unquoted_folding | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 0, 1, 1, 0]; Giggles [0, 1, 1, 0, 1, 1, 0] |
| insert_column_subset_nulls | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 2]; Giggles [0, 1, 1, 2] |
| insert_select | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 1, 2, 2]; Giggles [0, 0, 1, 1, 1, 2, 2] |
| insert_select_returning | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 1, 1]; Giggles [0, 0, 1, 1, 1, 1] |
| multi_row_values | E1 | compare: full | COMPATIBLE | PG [0, 3, 3]; Giggles [0, 3, 3] |
| inner_join_multi_condition | E3 | compare: full | COMPATIBLE | PG [0, 0, 3, 3, 1]; Giggles [0, 0, 3, 3, 1] |
| inner_join_non_equi | E3 | compare: full | COMPATIBLE | PG [0, 0, 3, 3, 2]; Giggles [0, 0, 3, 3, 2] |
| left_join_basic | E3 | compare: full | COMPATIBLE | PG [0, 0, 3, 3, 4]; Giggles [0, 0, 3, 3, 4] |
| left_join_chain | E3 | compare: full | COMPATIBLE | PG [0, 0, 3, 3, 4]; Giggles [0, 0, 3, 3, 4] |
| left_join_fk_count_grouped | E3 | compare: full | COMPATIBLE | PG [0, 0, 3, 3, 3]; Giggles [0, 0, 3, 3, 3] |
| left_join_on_vs_where | E3 | compare: full | COMPATIBLE | PG [0, 0, 3, 3, 3, 1]; Giggles [0, 0, 3, 3, 3, 1] |
| aggregate_filter | E3 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| aggregate_over_app_table | E3 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| array_column | E2 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| composite_column | E2 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 1, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| composite_type | E2 | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| copy_to_csv | E2 | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| domain_cast | E2 | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| domain_type | E2 | compare: full | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| drop_type_cascade | E2 | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| enum_type | E2 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 2]; Giggles [0, 0, 1, 1, 2] |
| explicit_bytea_cast | E2 | compare: full | COMPATIBLE | PG [1]; Giggles [1] |
| global_type_name | E2 | compare: full | DIVERGENT | PG [0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0] |
| immutable_overflow_error | E2 | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| information_schema_columns | G | compare: full | COMPATIBLE | PG [0, 2]; Giggles [0, 2] |
| jsonb_column | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| lock_mode | E2 | compare: full | COMPATIBLE | PG [0, 0, 0, 0]; Giggles [0, 0, 0, 0] |
| mixed_catalog_app_query | G | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| multidimensional_array | E2 | compare: full | DIVERGENT | PG [0, 1, 1, 1, 2]; Giggles [0, 0, 0, 0, 0] |
| no_from_aggregate | DD-0068 | compare: full | COMPATIBLE | PG [1, 1]; Giggles [1, 1] |
| numeric_precision | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 2]; Giggles [0, 1, 1, 2] |
| pg_get_userbyid_owner | G | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| public_schema_routine | E2 | compare: full | COMPATIBLE | PG [1, 0]; Giggles [1, 0] |
| search_path_custom | D | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 1, 0]; Giggles [0, 0, 0, 1, 1, 0] |
| table_free_scalars | E2 | compare: full | COMPATIBLE | PG [1]; Giggles [1] |
| timestamptz_roundtrip | E2 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| uuid_column | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| on_conflict_do_nothing | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 0, 1]; Giggles [0, 1, 0, 0, 1] |
| on_conflict_do_update_excluded | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| on_conflict_do_update_where | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1, 1, 1]; Giggles [0, 1, 0, 1, 1, 1] |
| on_conflict_on_constraint | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgd_alter_access | E1 | compare: full | UNSUPPORTED | PG [0, 1, 0, 0, 1]; Giggles [0, 1, 0, 0, 1] |
| pgd_alter_basics | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0] |
| pgd_alter_column_options | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 1, 0] |
| pgd_alter_constraint | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_alter_expression | E1 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1, 0, 1, 1]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgd_alter_fk_validate | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0, 1, 0] |
| pgd_alter_identity | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0, 1, 3, 0]; Giggles [0, 0, 0, 0, 1, 0, 0, 1, 0] |
| pgd_alter_if_exists | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 1]; Giggles [0, 0, 0, 0, 1, 0, 1] |
| pgd_alter_not_null | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1, 0, 0, 1]; Giggles [0, 1, 0, 1, 0, 0, 1] |
| pgd_alter_options | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1]; Giggles [0, 0, 1, 0, 1] |
| pgd_alter_owner_schema | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_alter_rename_constraint | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 3]; Giggles [0, 0, 0, 0, 3] |
| pgd_alter_replica | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0] |
| pgd_alter_rls | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 0, 1]; Giggles [0, 0, 0, 1, 0, 0, 1] |
| pgd_alter_system | G | compare: full | UNSUPPORTED | PG [0, 0, 1]; Giggles [0, 0, 0] |
| pgd_alter_trigger | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 4, 0, 0]; Giggles [0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 4, 0, 0] |
| pgd_alter_type_narrow | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgd_alter_type_using | E1 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgd_alter_type_widen | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgd_alter_typed | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_alter_using_index | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 0, 1]; Giggles [0, 0, 0, 1, 0, 0, 1] |
| pgd_alter_validate | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 0, 0, 1, 0, 1, 0]; Giggles [0, 1, 0, 0, 0, 1, 0, 1, 0] |
| pgd_cluster | E1 | compare: full | UNSUPPORTED | PG [0, 0, 2, 0, 0, 0, 2, 0, 0]; Giggles [0, 0, 2, 0, 0, 0, 2, 0, 0] |
| pgd_comment_access_method | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_aggregate | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_comment_cast | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0, 0]; Giggles [0, 0, 0, 1, 0, 1, 0, 0] |
| pgd_comment_collation | G | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_column | G | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 1]; Giggles [0, 0, 1, 0, 1] |
| pgd_comment_constraint | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1]; Giggles [0, 0, 0, 1, 0, 1] |
| pgd_comment_conversion | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_domain | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_event_trigger | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0, 0]; Giggles [0, 0, 0, 1, 0, 1, 0, 0] |
| pgd_comment_fdw | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_foreign_table | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_comment_function | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_comment_index | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgd_comment_language | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_large_object | G | compare: full | UNSUPPORTED | PG [1, 0, 1, 0, 1, 1]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_long | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 0]; Giggles [0, 0, 0, 0, 0] |
| pgd_comment_materialized_view | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgd_comment_opclass | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0, 0]; Giggles [0, 0, 1, 0, 1, 0, 0] |
| pgd_comment_operator | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 1, 0, 1, 0, 0, 0] |
| pgd_comment_opfamily | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_policy | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 1, 0, 1, 0] |
| pgd_comment_procedure | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_comment_publication | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 1, 0, 1, 0] |
| pgd_comment_role | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_routine | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_comment_rule | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 1, 0, 1, 0] |
| pgd_comment_schema | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_sequence | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_comment_server | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0, 0]; Giggles [0, 0, 0, 1, 0, 1, 0, 0] |
| pgd_comment_statistics | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 1, 0, 1, 0] |
| pgd_comment_subscription | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_table | G | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 1]; Giggles [0, 0, 1, 0, 1] |
| pgd_comment_trigger | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 1, 0, 1, 0, 0] |
| pgd_comment_ts_config | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_ts_dict | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_ts_parser | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_ts_template | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_type | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_comment_unavailable | G | compare: full | DIVERGENT | PG [0, 0, 0, 0, 1]; Giggles [0, 0, 0, 0, 1] |
| pgd_comment_view | G | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 1, 0, 1, 0] |
| pgd_ctas | E1 | compare: full | DIVERGENT | PG [0, 1, 1, 0, 1, 0]; Giggles [0, 1, 0, 0, 1, 0] |
| pgd_ctas_no_data | E1 | compare: full | DIVERGENT | PG [0, 1, 0, 0, 0, 0]; Giggles [0, 1, 0, 0, 0, 0] |
| pgd_database | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgd_default_arithmetic | E1 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgd_default_function | E1 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgd_default_user | E1 | compare: full | DIVERGENT | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgd_default_uuid | E1 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgd_drop_atomic | unowned | compare: full | COMPATIBLE | PG [0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0]; Giggles [0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0] |
| pgd_exclude | E1 | compare: full | UNSUPPORTED | PG [0, 0, 2, 0, 2]; Giggles [0, 0, 0, 0, 0] |
| pgd_fk_match_full | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 2, 0, 2, 0]; Giggles [0, 0, 1, 0, 0, 0, 0] |
| pgd_generated_stored | E1 | compare: full | UNSUPPORTED | PG [0, 1, 1, 1, 0]; Giggles [0, 0, 0, 0, 0] |
| pgd_generated_virtual | E1 | compare: full | UNSUPPORTED | PG [0, 1, 1, 1, 0]; Giggles [0, 0, 0, 0, 0] |
| pgd_idx_alter | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgd_idx_array | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_array_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_boolean | E1 | compare: full | COMPATIBLE | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_boolean_unique | E1 | compare: full | COMPATIBLE | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 0, 1, 0] |
| pgd_idx_brin | G | compare: full | UNSUPPORTED | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 2, 0, 0, 2, 0] |
| pgd_idx_bytea | E1 | compare: full | COMPATIBLE | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_bytea_unique | E1 | compare: full | COMPATIBLE | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 0, 1, 0] |
| pgd_idx_char | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 0, 0] |
| pgd_idx_char_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 0, 1, 1, 0] |
| pgd_idx_concurrent | E1 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1, 0]; Giggles [0, 1, 0, 1, 0] |
| pgd_idx_date | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_date_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_descending | G | compare: full | UNSUPPORTED | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 2, 0, 0, 2, 0] |
| pgd_idx_double | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_double_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_expression | G | compare: full | UNSUPPORTED | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 2, 0, 0, 2, 0] |
| pgd_idx_gin_array | G | compare: full | UNSUPPORTED | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 2, 0, 0, 2, 0] |
| pgd_idx_gin_jsonb | G | compare: full | UNSUPPORTED | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 2, 0, 0, 2, 0] |
| pgd_idx_gist | G | compare: full | UNSUPPORTED | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_idx_hash | G | compare: full | UNSUPPORTED | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 2, 0, 0, 2, 0] |
| pgd_idx_if_exists | G | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgd_idx_include | G | compare: full | UNSUPPORTED | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 2, 0, 0, 2, 0] |
| pgd_idx_interval | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_interval_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_jsonb | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_jsonb_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_multi | G | compare: full | COMPATIBLE | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 2, 0, 1, 2, 0] |
| pgd_idx_nine | G | compare: full | DIVERGENT | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_idx_numeric | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_numeric_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_partial | G | compare: full | UNSUPPORTED | PG [0, 2, 0, 1, 2, 0]; Giggles [0, 2, 0, 0, 2, 0] |
| pgd_idx_real | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_real_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_reindex | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0, 0, 1, 0]; Giggles [0, 0, 1, 0, 0, 0, 1, 0] |
| pgd_idx_time | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_time_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_timestamp | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_timestamp_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_timestamptz | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_timestamptz_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_idx_uuid | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0]; Giggles [0, 2, 0, 2, 0] |
| pgd_idx_uuid_unique | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2, 0, 1, 0]; Giggles [0, 2, 0, 2, 1, 1, 0] |
| pgd_inheritance | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 2, 1, 0, 1, 0, 0]; Giggles [0, 0, 1, 0, 1, 0, 0, 1, 0, 0] |
| pgd_large_object | E1 | compare: full | UNSUPPORTED | PG [1, 0, 0, 1, 1, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_like_all | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_maintenance | E1 | compare: full | UNSUPPORTED | PG [0, 2, 1, 0, 0, 0, 1]; Giggles [0, 2, 1, 0, 0, 0, 1] |
| pgd_matview | E1 | compare: full | UNSUPPORTED | PG [0, 1, 1, 1, 0, 2, 0, 0, 0, 0]; Giggles [0, 1, 0, 1, 0, 0, 0, 0, 0, 0] |
| pgd_matview_concurrent | E1 | compare: full | UNSUPPORTED | PG [0, 1, 1, 0, 1, 0, 2, 0, 0, 0, 0]; Giggles [0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0] |
| pgd_migration | E1 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1, 0, 0, 0, 1, 0]; Giggles [0, 1, 0, 0, 0, 0, 0, 1, 0] |
| pgd_partition_attach | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0, 1, 1, 0, 0]; Giggles [0, 0, 1, 0, 0, 0, 0, 1, 0, 0] |
| pgd_partition_hash | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 3, 3, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgd_partition_list | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 3, 3, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_partition_range | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 3, 3, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_pk_char | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2]; Giggles [0, 0, 0, 0] |
| pgd_pk_date | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2]; Giggles [0, 0, 0, 0] |
| pgd_pk_interval | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2]; Giggles [0, 0, 0, 0] |
| pgd_pk_time | E1 | compare: full | UNSUPPORTED | PG [0, 2, 0, 2]; Giggles [0, 0, 0, 0] |
| pgd_publication | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_rule | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 1, 0, 1, 2, 1, 0]; Giggles [0, 0, 0, 0, 1, 0, 0, 1, 2, 0, 0] |
| pgd_schema | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_security_label | E1 | compare: full | DIVERGENT | PG [0, 0, 1]; Giggles [0, 0, 1] |
| pgd_select_into | E1 | compare: full | DIVERGENT | PG [0, 1, 1, 0, 1, 0]; Giggles [0, 1, 0, 0, 1, 0] |
| pgd_sequence | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0, 0, 0, 1, 0, 0]; Giggles [0, 0, 1, 0, 0, 0, 0, 0, 0, 0] |
| pgd_sequence_temp | E1 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0] |
| pgd_statistics | G | compare: full | UNSUPPORTED | PG [0, 0, 20, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgd_storage_fillfactor | G | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1]; Giggles [0, 0, 0, 0, 0] |
| pgd_subscription | G | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgd_tablespace | G | compare: full | DIVERGENT | PG [0, 0, 0, 0, 1]; Giggles [0, 0, 0, 0, 1] |
| pgd_temp_delete | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgd_temp_drop | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 0, 1]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_temp_preserve | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_unique_deferred | E1 | compare: full | UNSUPPORTED | PG [0, 0, 2, 0, 0, 1, 1, 0, 0, 2, 0, 1, 0, 2]; Giggles [0, 0, 2, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0, 3] |
| pgd_unique_nulls | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1]; Giggles [0, 0, 1, 1, 2] |
| pgd_unlogged | E1 | compare: full | UNSUPPORTED | PG [0, 1, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgd_view_aggregate | E1 | compare: full | UNSUPPORTED | PG [0, 3, 0, 2, 0]; Giggles [0, 3, 0, 0, 0] |
| pgd_view_check | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgd_view_cte | E1 | compare: full | UNSUPPORTED | PG [0, 3, 0, 1, 0]; Giggles [0, 3, 0, 0, 0] |
| pgd_view_delete | E1 | compare: full | UNSUPPORTED | PG [0, 0, 2, 1, 1, 0]; Giggles [0, 0, 2, 0, 2, 0] |
| pgd_view_distinct | E1 | compare: full | UNSUPPORTED | PG [0, 3, 0, 2, 0]; Giggles [0, 3, 0, 0, 0] |
| pgd_view_expression | E1 | compare: full | UNSUPPORTED | PG [0, 3, 0, 3, 0]; Giggles [0, 3, 0, 0, 0] |
| pgd_view_insert | E1 | compare: full | DIVERGENT | PG [0, 0, 1, 1, 0]; Giggles [0, 0, 0, 0, 0] |
| pgd_view_join | E1 | compare: full | UNSUPPORTED | PG [0, 3, 0, 3, 0]; Giggles [0, 3, 0, 0, 0] |
| pgd_view_limit | E1 | compare: full | UNSUPPORTED | PG [0, 3, 0, 1, 0]; Giggles [0, 3, 0, 0, 0] |
| pgd_view_order | E1 | compare: full | UNSUPPORTED | PG [0, 3, 0, 3, 0]; Giggles [0, 3, 0, 0, 0] |
| pgd_view_replace | E1 | compare: full | UNSUPPORTED | PG [0, 1, 0, 0, 1, 0, 0, 0, 1, 0]; Giggles [0, 1, 0, 0, 1, 0, 0, 0, 0, 0] |
| pgd_view_security_barrier | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 0, 0]; Giggles [0, 0, 1, 0, 0, 0] |
| pgd_view_security_invoker | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 0, 0]; Giggles [0, 0, 1, 0, 0, 0] |
| pgd_view_union | E1 | compare: full | UNSUPPORTED | PG [0, 3, 0, 2, 0]; Giggles [0, 3, 0, 0, 0] |
| pgd_view_update | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 1, 0]; Giggles [0, 0, 1, 0, 1, 0] |
| pgd_view_where | E1 | compare: full | COMPATIBLE | PG [0, 3, 0, 2, 0]; Giggles [0, 3, 0, 2, 0] |
| pgd_wide_columns | E1 | compare: full | DIVERGENT | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgd_wide_row | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgf_advisory | DD-0068 | compare: full | UNSUPPORTED | PG [0, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 0]; Giggles [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0] |
| pgf_aggregate_general | DD-0068 | compare: full | UNSUPPORTED | PG [0, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0]; Giggles [0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_aggregate_json | DD-0068 | compare: full | UNSUPPORTED | PG [0, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0]; Giggles [0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_aggregate_ordered | DD-0068 | compare: full | UNSUPPORTED | PG [0, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0]; Giggles [0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_aggregate_regression | DD-0068 | compare: full | UNSUPPORTED | PG [0, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0]; Giggles [0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_aggregate_statistics | DD-0068 | compare: full | UNSUPPORTED | PG [0, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0]; Giggles [0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_array_dimensions | DD-0068 | compare: full | DIVERGENT | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_array_edit | DD-0068 | compare: full | DIVERGENT | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 3, 0] |
| pgf_array_unnest | DD-0068 | compare: full | DIVERGENT | PG [0, 3, 3, 2, 3, 1, 0]; Giggles [0, 0, 0, 0, 3, 0, 0] |
| pgf_casts_numeric | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 0] |
| pgf_casts_text | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 3, 0, 3, 3, 0] |
| pgf_comparison | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0] |
| pgf_conditional | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 3, 3, 3, 3, 3, 0, 3, 3, 3, 0] |
| pgf_datetime_arithmetic | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 0] |
| pgf_datetime_clock_fields | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_datetime_construct | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 0, 3, 3, 3, 3, 0, 0] |
| pgf_datetime_current | DD-0068 | compare: full | UNSUPPORTED | PG [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0]; Giggles [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0] |
| pgf_datetime_fields | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_datetime_timezone | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 3, 0, 0, 0] |
| pgf_datetime_truncate | DD-0068 | compare: full | DIVERGENT | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 0, 3, 3, 3, 0, 3, 3, 3, 0, 0] |
| pgf_enum | DD-0068 | compare: full | UNSUPPORTED | PG [0, 0, 4, 4, 4, 4, 4, 4, 1, 1, 4, 4, 0, 0]; Giggles [0, 0, 4, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0] |
| pgf_errors | DD-0068 | compare: full | DIVERGENT | PG [0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_format_datetime | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_format_numbers | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_geometric | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_json_access | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0] |
| pgf_json_build | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_json_expand | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 2, 2, 2, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0] |
| pgf_json_path | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_json_path_rows | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 2, 0]; Giggles [0, 3, 0, 0, 0] |
| pgf_json_sql | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_json_table | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 2, 3, 0]; Giggles [0, 3, 0, 0, 0] |
| pgf_math_bitwise | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_math_buckets | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0] |
| pgf_math_hyperbolic | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_math_operators | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 3, 0, 0, 0, 0, 3, 3, 0] |
| pgf_math_random | DD-0068 | compare: full | UNSUPPORTED | PG [1, 0, 3, 3, 3, 3, 3, 1, 1, 1, 1, 0]; Giggles [0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_math_rounding | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_math_transcendental | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_math_trig | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_math_trig_degrees | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_network | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_range | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 1, 1, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_sequence | DD-0068 | compare: full | UNSUPPORTED | PG [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0]; Giggles [0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0] |
| pgf_set_series | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 4, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 0, 0, 0, 3, 0] |
| pgf_set_subscripts | DD-0068 | compare: full | DIVERGENT | PG [0, 3, 3, 3, 3, 2, 3, 2, 3, 0]; Giggles [0, 0, 0, 0, 0, 0, 3, 0, 0, 0] |
| pgf_string_arrays | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_string_binary | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_string_binary_edit | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_string_char | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_string_combine | DD-0068 | compare: full | COMPATIBLE | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_string_edit | DD-0068 | compare: full | DIVERGENT | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0] |
| pgf_string_lengths | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 3, 3, 3, 0] |
| pgf_string_patterns | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 0, 0, 0, 0, 0, 3, 0] |
| pgf_string_regex_scalar | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_string_regex_sets | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 2, 4, 2, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0] |
| pgf_string_substrings | DD-0068 | compare: full | DIVERGENT | PG [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0]; Giggles [0, 3, 3, 3, 3, 3, 3, 3, 0, 0, 3, 3, 0] |
| pgf_system_catalog | DD-0068 | compare: full | UNSUPPORTED | PG [0, 2, 0, 0, 2, 2, 1, 1, 1, 1, 1, 1, 2, 1, 2, 1, 0]; Giggles [0, 2, 0, 0, 2, 0, 1, 1, 0, 0, 1, 1, 2, 1, 2, 1, 0] |
| pgf_system_session | DD-0068 | compare: full | UNSUPPORTED | PG [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]; Giggles [0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 1] |
| pgf_system_view | DD-0068 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 1, 0, 0]; Giggles [0, 0, 1, 1, 1, 0, 0] |
| pgf_text_search | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 3, 3, 3, 1, 1, 1, 3, 3, 3, 0]; Giggles [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgf_uuid | DD-0068 | compare: full | UNSUPPORTED | PG [0, 3, 3, 3, 3, 1, 1, 1, 1, 1, 1, 1, 0]; Giggles [0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgl_drop_recreate | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1000, 0, 0, 0, 0, 1, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 1, 1, 0] |
| pgl_large_aggregates | E3 | compare: full | UNSUPPORTED | PG [0, 100000, 1, 0]; Giggles [0, 0, 1, 0] |
| pgl_large_returning | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 0]; Giggles [0, 1, 1, 0] |
| pgl_large_row | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 1, 0]; Giggles [0, 1, 1, 1, 1, 0] |
| pgl_large_update | E1 | compare: full | UNSUPPORTED | PG [0, 50000, 50000, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgl_long_comment | E1 | compare: full | COMPATIBLE | PG [1]; Giggles [1] |
| pgl_long_in_list | E1 | compare: full | COMPATIBLE | PG [0, 3, 2, 0]; Giggles [0, 3, 2, 0] |
| pgl_long_literal | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 0]; Giggles [0, 1, 1, 0] |
| pgl_maintenance | E1 | compare: full | UNSUPPORTED | PG [0, 1, 1, 0, 0, 0, 0, 1, 0]; Giggles [0, 1, 1, 0, 0, 0, 0, 1, 0] |
| pgl_twelve_indexes | E1 | compare: full | DIVERGENT | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0] |
| pgl_twelve_join | E3 | compare: full | COMPATIBLE | PG [0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgl_wide_table | E1 | compare: full | DIVERGENT | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgq_all_array | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_all_subquery | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_any_array | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_any_subquery | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_copy_csv | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_default_marker | E1 | compare: full | COMPATIBLE | PG [0, 2, 2]; Giggles [0, 2, 2] |
| pgq_copy_default_no_default | E1 | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| pgq_copy_delimiter | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_encoding_names | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgq_copy_encoding_roundtrip | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgq_copy_escape | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_force_quote | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_freeze | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1]; Giggles [0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 1] |
| pgq_copy_freeze_errors | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgq_copy_from_file | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_from_header | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_from_program | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_from_quote | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_header | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_on_error_constraint | E1 | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| pgq_copy_on_error_ignore | E1 | compare: full | COMPATIBLE | PG [0, 2, 2]; Giggles [0, 2, 2] |
| pgq_copy_options_errors | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgq_copy_options_escape | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgq_copy_options_force_all | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgq_copy_options_force_null | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgq_copy_options_header_count | E1 | compare: full | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| pgq_copy_options_header_match | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgq_copy_options_header_mismatch | E1 | compare: full | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| pgq_copy_options_header_null | E1 | compare: full | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| pgq_copy_options_header_true | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgq_copy_options_legacy | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgq_copy_options_quote | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgq_copy_program_roundtrip | E1 | compare: full | COMPATIBLE | PG [0, 3, 0, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 0, 0]; Giggles [0, 3, 0, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 0, 0] |
| pgq_copy_quote | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_quote_data | E1 | compare: full | COMPATIBLE | PG [0, 4, 1, 0]; Giggles [0, 4, 1, 0] |
| pgq_copy_reject_limit | E1 | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| pgq_copy_server_file_errors | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 0, 0, 0, 0, 0, 0, 1]; Giggles [0, 1, 0, 0, 0, 0, 0, 0, 0, 1] |
| pgq_copy_server_program_errors | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 0, 0, 1]; Giggles [0, 1, 0, 0, 0, 1] |
| pgq_copy_server_roles | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgq_copy_server_unprivileged | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgq_copy_table | E1 | compare: full | COMPATIBLE | PG [0, 4, 3, 0]; Giggles [0, 4, 3, 0] |
| pgq_copy_text | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_to_file | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_to_program | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_copy_unparenthesized | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_correlated_in_null | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_correlated_not_in_null | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_cte_cycle | E3 | compare: full | COMPATIBLE | PG [0, 4, 1, 5]; Giggles [0, 4, 1, 5] |
| pgq_cte_materialized | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_cte_multiple | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_cte_not_materialized | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_cte_plain | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_cte_recursive_series | E3 | compare: full | COMPATIBLE | PG [5]; Giggles [5] |
| pgq_cte_recursive_tree | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_cte_reused | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_cte_search_breadth | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_cte_search_depth | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_cube | E3 | compare: full | COMPATIBLE | PG [0, 4, 10]; Giggles [0, 4, 10] |
| pgq_delete_current_of | E1 | compare: full | UNSUPPORTED | PG [0, 4, 0, 0, 1, 1, 0, 0]; Giggles [0, 4, 0, 0, 0, 0, 0, 0] |
| pgq_delete_old_new | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_delete_returning | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_delete_using | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 2]; Giggles [0, 4, 2, 2] |
| pgq_distinct_on | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_exists | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_explain_analyze | E1 | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| pgq_explain_analyze_update | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_explain_delete | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_explain_insert | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_explain_json | E1 | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| pgq_explain_merge | E1 | compare: full | UNSUPPORTED | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_explain_select | E1 | compare: full | UNSUPPORTED | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_explain_update | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_explain_verbose | E1 | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| pgq_fetch_first | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_fetch_ties | E3 | compare: full | COMPATIBLE | PG [0, 4, 3]; Giggles [0, 4, 3] |
| pgq_grouping_sets | E3 | compare: full | COMPATIBLE | PG [0, 4, 6]; Giggles [0, 4, 6] |
| pgq_having_filter | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_insert_current_date | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 2]; Giggles [0, 4, 2, 2] |
| pgq_insert_default_values | E1 | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| pgq_insert_identity_error | E1 | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| pgq_insert_multi_cast | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 2]; Giggles [0, 4, 2, 2] |
| pgq_insert_multi_concat | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 2]; Giggles [0, 4, 2, 2] |
| pgq_insert_multi_expressions | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 2]; Giggles [0, 4, 2, 2] |
| pgq_insert_multi_function | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 2]; Giggles [0, 4, 2, 2] |
| pgq_insert_old_new | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_insert_overriding_system | E1 | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| pgq_insert_overriding_user | E1 | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| pgq_insert_returning | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_insert_select | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 2]; Giggles [0, 4, 2, 2] |
| pgq_insert_trailing | E1 | compare: full | COMPATIBLE | PG [0, 4, 1, 1]; Giggles [0, 4, 1, 1] |
| pgq_insert_values_default | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 2]; Giggles [0, 4, 2, 2] |
| pgq_isnull | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_join_comma | E3 | compare: full | COMPATIBLE | PG [0, 4, 3]; Giggles [0, 4, 3] |
| pgq_join_cross | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_join_derived_alias | E3 | compare: full | COMPATIBLE | PG [0, 4, 3]; Giggles [0, 4, 3] |
| pgq_join_full | E3 | compare: full | COMPATIBLE | PG [0, 4, 6]; Giggles [0, 4, 6] |
| pgq_join_natural | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_join_nine | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_join_right | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_join_self | E3 | compare: full | COMPATIBLE | PG [0, 4, 3]; Giggles [0, 4, 3] |
| pgq_join_using | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_lateral_srf | E3 | compare: full | COMPATIBLE | PG [0, 4, 10]; Giggles [0, 4, 10] |
| pgq_lateral_subquery | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_limit_all | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_limit_expression | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_literal_bit | E3 | compare: full | UNSUPPORTED | PG [0, 4, 4]; Giggles [0, 4, 0] |
| pgq_literal_decimal_no_from | E3 | compare: full | COMPATIBLE | PG [1]; Giggles [1] |
| pgq_literal_dollar | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_literal_dollar_no_from | E3 | compare: full | COMPATIBLE | PG [1]; Giggles [1] |
| pgq_literal_escape | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_literal_hex | E3 | compare: full | UNSUPPORTED | PG [0, 4, 4]; Giggles [0, 4, 0] |
| pgq_literal_leading_dot | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_literal_unicode | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_lock_key_share | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_key_share_nowait | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_key_share_skip_locked | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_no_key_update | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_no_key_update_nowait | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_no_key_update_skip_locked | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_of_table | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 3, 0]; Giggles [0, 4, 0, 3, 0] |
| pgq_lock_share | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_share_nowait | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_share_skip_locked | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_update | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_update_nowait | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_lock_update_skip_locked | E3 | compare: full | COMPATIBLE | PG [0, 4, 0, 2, 0]; Giggles [0, 4, 0, 2, 0] |
| pgq_merge_by_source_delete | E1 | compare: full | COMPATIBLE | PG [0, 4, 3, 1]; Giggles [0, 4, 3, 1] |
| pgq_merge_by_source_nothing | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 4]; Giggles [0, 4, 0, 4] |
| pgq_merge_by_source_update | E1 | compare: full | COMPATIBLE | PG [0, 4, 3, 4]; Giggles [0, 4, 3, 4] |
| pgq_merge_by_target_nothing | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 4]; Giggles [0, 4, 0, 4] |
| pgq_merge_conditional | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 5]; Giggles [0, 4, 2, 5] |
| pgq_merge_default_values | E1 | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| pgq_merge_insert_overriding | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 1]; Giggles [0, 4, 0, 1] |
| pgq_merge_matched_delete | E1 | compare: full | COMPATIBLE | PG [0, 4, 1, 3]; Giggles [0, 4, 1, 3] |
| pgq_merge_matched_nothing | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 4]; Giggles [0, 4, 0, 4] |
| pgq_merge_matched_update | E1 | compare: full | COMPATIBLE | PG [0, 4, 1, 4]; Giggles [0, 4, 1, 4] |
| pgq_merge_multiple_match_error | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_merge_not_matched_insert | E1 | compare: full | COMPATIBLE | PG [0, 4, 1, 5]; Giggles [0, 4, 1, 5] |
| pgq_merge_not_matched_nothing | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 4]; Giggles [0, 4, 0, 4] |
| pgq_merge_not_matched_target | E1 | compare: full | COMPATIBLE | PG [0, 4, 1, 5]; Giggles [0, 4, 1, 5] |
| pgq_merge_old_new | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_merge_returning | E1 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_not_exists | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_notnull | E3 | compare: full | COMPATIBLE | PG [0, 4, 3]; Giggles [0, 4, 3] |
| pgq_order_nulls_first | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_order_nulls_last | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_order_using | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_pagination | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_queue_claim | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 1, 0]; Giggles [0, 4, 0, 1, 0] |
| pgq_rollup | E3 | compare: full | COMPATIBLE | PG [0, 4, 7]; Giggles [0, 4, 7] |
| pgq_scalar_correlated | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_scalar_correlated_where | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_scalar_empty | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_scalar_many_error | E3 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_scalar_select | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_scalar_where | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_select_all | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_select_distinct | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_select_group_distinct | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_select_tablesample | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_set_except | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_set_except_all | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_set_except_all_parenthesized | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_set_except_parenthesized | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_set_intersect | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_set_intersect_all | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_set_intersect_all_parenthesized | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_set_intersect_parenthesized | E3 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_set_union | E3 | compare: full | COMPATIBLE | PG [0, 4, 3]; Giggles [0, 4, 3] |
| pgq_set_union_all | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_set_union_all_parenthesized | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_set_union_parenthesized | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_some_array | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_some_subquery | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_srf_from | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_srf_ordinality | E3 | compare: full | COMPATIBLE | PG [0, 4, 8]; Giggles [0, 4, 8] |
| pgq_subquery_not_in_empty | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_table_query | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_truncate_basic | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 1]; Giggles [0, 4, 0, 1] |
| pgq_truncate_cascade | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 1, 0, 1]; Giggles [0, 4, 0, 1, 0, 1] |
| pgq_truncate_continue_identity | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgq_truncate_multiple | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 1, 0, 1]; Giggles [0, 4, 0, 1, 0, 1] |
| pgq_truncate_only | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 1]; Giggles [0, 4, 0, 1] |
| pgq_truncate_restart_identity | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgq_truncate_restrict_error | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 0]; Giggles [0, 4, 0, 0] |
| pgq_update_current_of | E1 | compare: full | UNSUPPORTED | PG [0, 4, 0, 0, 1, 1, 0, 0]; Giggles [0, 4, 0, 0, 0, 0, 0, 0] |
| pgq_update_default | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_update_from | E1 | compare: full | COMPATIBLE | PG [0, 4, 2, 4]; Giggles [0, 4, 2, 4] |
| pgq_update_old_new | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_update_row_many_error | E1 | compare: full | COMPATIBLE | PG [0, 4, 0]; Giggles [0, 4, 0] |
| pgq_update_row_subquery | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_update_row_subquery_empty | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_update_row_values | E1 | compare: full | COMPATIBLE | PG [0, 4, 1]; Giggles [0, 4, 1] |
| pgq_upsert_no_target | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 1]; Giggles [0, 4, 0, 1] |
| pgq_upsert_partial | E1 | compare: full | UNSUPPORTED | PG [0, 4, 0, 1, 0]; Giggles [0, 4, 0, 0, 0] |
| pgq_upsert_returning | E1 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_values_query | E3 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_values_standalone | E3 | compare: full | COMPATIBLE | PG [2]; Giggles [2] |
| pgq_window_avg | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_count | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_dense_rank | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_exclude_current_row | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_exclude_group | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_exclude_no_others | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_exclude_ties | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_filter | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_first_value | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_groups | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_groups_exclude | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_lag | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_last_value | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_lead | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_named | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_nth_value | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_ntile | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_range | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_range_exclude | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_rank | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_row_number | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_rows | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_window_sum | E3 | compare: full | COMPATIBLE | PG [0, 4, 4]; Giggles [0, 4, 4] |
| pgq_with_delete_returning | E1 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_with_insert_returning | E1 | compare: full | COMPATIBLE | PG [0, 4, 2]; Giggles [0, 4, 2] |
| pgq_with_move_rows | E1 | compare: full | COMPATIBLE | PG [0, 4, 0, 2]; Giggles [0, 4, 0, 2] |
| pgq_with_update_returning | E1 | compare: full | COMPATIBLE | PG [0, 4, 3]; Giggles [0, 4, 3] |
| pgr_all_functions | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgr_all_sequences | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgr_all_tables | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgr_attributes | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgr_columns | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0]; Giggles [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgr_default_privileges | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgr_grant_atomic | unowned | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0] |
| pgr_has_role | unowned | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0] |
| pgr_has_role_tables | unowned | compare: full | DIVERGENT | PG [0, 0, 0, 0, 4, 0, 4, 2, 4, 1, 1, 3, 3, 3, 3, 3, 0, 4, 0, 2, 0, 0, 1, 1, 5, 0, 5, 0, 0, 0]; Giggles [0, 0, 0, 0, 4, 0, 0, 0, 0, 1, 1, 0, 0, 0, 3, 0, 0, 0, 0, 2, 0, 0, 1, 1, 5, 0, 5, 0, 0, 0] |
| pgr_membership | unowned | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0] |
| pgr_membership_options | unowned | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0] |
| pgr_missing_drop | unowned | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgr_owned | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0] |
| pgr_privilege_catalog | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 13, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0] |
| pgr_public | unowned | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0] |
| pgr_rls_delete | unowned | compare: full | UNSUPPORTED | PG [0, 0, 2, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0]; Giggles [0, 0, 2, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0] |
| pgr_rls_force | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 2, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0] |
| pgr_rls_insert | unowned | compare: full | UNSUPPORTED | PG [0, 0, 2, 0, 0, 0, 0, 1, 0, 0, 3, 0, 0, 0]; Giggles [0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 3, 0, 0, 0] |
| pgr_rls_restrictive | unowned | compare: full | UNSUPPORTED | PG [0, 0, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0]; Giggles [0, 0, 3, 0, 0, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 0, 0] |
| pgr_rls_select | unowned | compare: full | UNSUPPORTED | PG [0, 0, 2, 0, 0, 0, 0, 1, 0, 2, 0, 0, 0]; Giggles [0, 0, 2, 0, 0, 0, 0, 2, 0, 2, 0, 0, 0] |
| pgr_rls_update | unowned | compare: full | UNSUPPORTED | PG [0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0]; Giggles [0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0, 0, 0, 0] |
| pgr_schema | unowned | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgr_session_auth | unowned | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 0, 1, 0]; Giggles [0, 0, 1, 0, 0, 1, 0] |
| pgr_view_create | unowned | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0] |
| pgr_view_grant | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_access_method | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgp_aggregate | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgp_alter_access_method | unowned | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| pgp_alter_cast | unowned | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| pgp_alter_transform | unowned | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| pgp_available_extensions | unowned | compare: full | DIVERGENT | PG [16]; Giggles [1] |
| pgp_cast | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgp_create_routine | unowned | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| pgp_do_block | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_event_trigger | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_ext_btree_gist | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgp_ext_citext | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_ext_crosstab | unowned | compare: full | UNSUPPORTED | PG [0, 2, 0]; Giggles [0, 0, 0] |
| pgp_ext_crypt | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_ext_cube | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_ext_digest | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_ext_earth | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0] |
| pgp_ext_fuzzy | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_ext_hstore | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_ext_intarray | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_ext_ltree | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_ext_statements | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0]; Giggles [0, 0, 0] |
| pgp_ext_trgm | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_ext_unaccent | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_ext_uuid | unowned | compare: full | COMPATIBLE | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgp_extension_lifecycle | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_fdw | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_fdw_file | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_fdw_import | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_fdw_postgres | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_fts_config | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0] |
| pgp_fts_dictionary | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgp_fts_parser | unowned | compare: full | UNSUPPORTED | PG [0, 3, 0, 0]; Giggles [0, 0, 0, 0] |
| pgp_fts_template | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgp_function_alter | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_function_catalog | unowned | compare: full | UNSUPPORTED | PG [0, 0, 2, 0, 0]; Giggles [0, 0, 0, 0, 0] |
| pgp_function_def | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_function_drop | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgp_function_replace | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgp_language | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgp_opclass | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 2, 2, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 2, 2, 0, 0, 0] |
| pgp_operator | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_opfamily | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgp_pl_cursor | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_pl_diagnostics | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_pl_dynamic | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_pl_exception | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_pl_for | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_pl_foreach | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_pl_if_case | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 1, 0]; Giggles [0, 0, 0, 0, 0] |
| pgp_pl_notice | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_pl_raise_error | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0]; Giggles [0, 0, 0] |
| pgp_pl_return_next | unowned | compare: full | UNSUPPORTED | PG [0, 2, 0]; Giggles [0, 0, 0] |
| pgp_pl_return_query | unowned | compare: full | UNSUPPORTED | PG [0, 3, 0]; Giggles [0, 0, 0] |
| pgp_pl_rowtype | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0, 1, 0, 0]; Giggles [0, 1, 0, 0, 0, 0] |
| pgp_pl_vars | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_pl_while | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_procedure_inout | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0]; Giggles [0, 0, 0, 0] |
| pgp_procedure_txn | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0] |
| pgp_routine_alter | unowned | compare: full | UNSUPPORTED | PG [0, 0, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_sql_defaults | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_sql_definer | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_sql_immutable | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_sql_named | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_sql_out | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_sql_poly | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_sql_scalar | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_sql_search_path | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgp_sql_set | unowned | compare: full | UNSUPPORTED | PG [0, 3, 0]; Giggles [0, 0, 0] |
| pgp_sql_stable | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_sql_strict | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_sql_table | unowned | compare: full | UNSUPPORTED | PG [0, 2, 0]; Giggles [0, 0, 0] |
| pgp_sql_variadic | unowned | compare: full | UNSUPPORTED | PG [0, 1, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_transform | unowned | compare: full | DIVERGENT | PG [0, 0, 1, 0]; Giggles [0, 0, 0, 0] |
| pgp_trigger_after_delete | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0] |
| pgp_trigger_after_insert | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 1, 0, 0, 0, 0, 0] |
| pgp_trigger_after_update | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0] |
| pgp_trigger_before_delete | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 1, 1, 0, 0, 0, 0] |
| pgp_trigger_before_insert | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 1, 0, 0, 0]; Giggles [0, 0, 0, 1, 1, 0, 0, 0] |
| pgp_trigger_before_update | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 1, 1, 0, 0, 0]; Giggles [0, 0, 0, 1, 1, 1, 0, 0, 0] |
| pgp_trigger_constraint | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0] |
| pgp_trigger_instead | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgp_trigger_notify | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 1, 0, 0, 0]; Giggles [0, 0, 0, 1, 1, 0, 0, 0] |
| pgp_trigger_replica | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0]; Giggles [0, 0, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0] |
| pgp_trigger_statement | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 2, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 2, 0, 0, 0, 0, 0] |
| pgp_trigger_timestamp | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0, 0, 1, 1, 0, 0, 0]; Giggles [0, 1, 0, 0, 1, 1, 0, 0, 0] |
| pgp_trigger_toggle | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 0, 1, 2, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 1, 0, 1, 2, 0, 0, 0] |
| pgp_trigger_transition | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 2, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 2, 0, 0, 0, 0, 0] |
| pgp_trigger_truncate | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0] |
| pgp_trigger_update_of | unowned | compare: full | UNSUPPORTED | PG [0, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0]; Giggles [0, 1, 0, 0, 1, 1, 1, 1, 0, 0, 0] |
| pgp_trigger_when | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 2, 2, 0, 0, 0]; Giggles [0, 0, 0, 2, 2, 0, 0, 0] |
| pgs_array_element | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgs_array_literal | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgs_array_typed | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgs_collation | E2 | compare: full | COMPATIBLE | PG [0, 5, 5, 1]; Giggles [0, 5, 5, 1] |
| pgs_enum_compare | E2 | compare: full | COMPATIBLE | PG [0, 0, 3, 3, 0, 0]; Giggles [0, 0, 3, 3, 0, 0] |
| pgs_enum_minmax | E2 | compare: full | COMPATIBLE | PG [0, 0, 3, 1, 0, 0]; Giggles [0, 0, 3, 1, 0, 0] |
| pgs_explain | E3 | compare: full | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| pgs_float_acosh | DD-0068 | compare: full | COMPATIBLE | PG [0, 6, 6]; Giggles [0, 6, 6] |
| pgs_float_atanh | DD-0068 | compare: full | COMPATIBLE | PG [0, 7, 7]; Giggles [0, 7, 7] |
| pgs_float_cbrt | DD-0068 | compare: full | COMPATIBLE | PG [0, 9, 9]; Giggles [0, 9, 9] |
| pgs_float_cosh | DD-0068 | compare: full | COMPATIBLE | PG [0, 7, 7]; Giggles [0, 7, 7] |
| pgs_float_other | DD-0068 | compare: full | COMPATIBLE | PG [0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7]; Giggles [0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7] |
| pgs_float_sin | DD-0068 | compare: full | COMPATIBLE | PG [0, 9, 9]; Giggles [0, 9, 9] |
| pgs_isolation | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgs_jsonb_literal | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgs_jsonb_typed | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgs_julian | DD-0068 | compare: full | COMPATIBLE | PG [1]; Giggles [1] |
| pgs_lock | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgs_schemas | G | compare: full | COMPATIBLE | PG [1, 0, 0, 1, 0, 0]; Giggles [1, 0, 0, 1, 0, 0] |
| pgs_timezone_interval | D | compare: full | COMPATIBLE | PG [0, 0, 1, 0]; Giggles [0, 0, 1, 0] |
| pgs_timezone_names | G | compare: full | COMPATIBLE | PG [2, 2]; Giggles [2, 2] |
| pgs_timezone_numeric | D | compare: full | COMPATIBLE | PG [0, 0, 1, 0]; Giggles [0, 0, 1, 0] |
| pgs_timezone_offset | D | compare: full | COMPATIBLE | PG [0, 0, 1, 0]; Giggles [0, 0, 1, 0] |
| pgs_timezone_posix | D | compare: full | COMPATIBLE | PG [0, 0, 1, 0]; Giggles [0, 0, 1, 0] |
| pgs_timezone_unknown | D | compare: full | COMPATIBLE | PG [0, 0, 1]; Giggles [0, 0, 1] |
| pgs_underscores | DD-0068 | compare: full | COMPATIBLE | PG [1, 1, 1]; Giggles [1, 1, 1] |
| pgs_uuid_max | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgs_uuid_min | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgs_version | G | compare: full | COMPATIBLE | PG [1, 1]; Giggles [1, 1] |
| pgs_viewdef | G | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 0, 0, 1, 0]; Giggles [0, 0, 1, 1, 0, 0, 1, 0] |
| pgs_viewdef_pretty | G | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 0, 0, 1, 0]; Giggles [0, 0, 1, 1, 0, 0, 1, 0] |
| pgt_advisory_shared | E1 | compare: full | UNSUPPORTED | PG [1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_boundaries | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1]; Giggles [0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1] |
| pgt_catalogs | G | compare: full | UNSUPPORTED | PG [0, 3, 1]; Giggles [0, 0, 0] |
| pgt_chain | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_constraints | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 2]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_cursor_binary | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0] |
| pgt_cursor_hold | E1 | compare: full | DIVERGENT | PG [0, 0, 1, 0, 2, 0]; Giggles [0, 0, 1, 0, 2, 0] |
| pgt_cursor_scroll | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 1, 1, 1, 2, 2, 3, 4, 1, 1, 1, 1, 0, 4, 2, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_deferrable | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_discard_all | D | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1]; Giggles [0, 0, 0, 0, 0] |
| pgt_discard_plans | D | compare: full | DIVERGENT | PG [0, 1, 0, 1, 0]; Giggles [0, 0, 0, 0, 0] |
| pgt_discard_sequences | D | compare: full | UNSUPPORTED | PG [0, 1, 0, 0, 0]; Giggles [0, 1, 0, 1, 0] |
| pgt_discard_temp | D | compare: full | UNSUPPORTED | PG [0, 1, 0, 0]; Giggles [0, 0, 0, 0] |
| pgt_encoding | D | compare: full | DIVERGENT | PG [0, 0, 1, 0, 0]; Giggles [0, 0, 1, 0, 0] |
| pgt_isolation_read_committed | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_isolation_read_uncommitted | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_isolation_repeatable_read | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_isolation_serializable | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_load | unowned | compare: full | DIVERGENT | PG [0]; Giggles [0] |
| pgt_lock_access_exclusive | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgt_lock_access_share | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgt_lock_exclusive | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgt_lock_row_exclusive | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgt_lock_row_share | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgt_lock_share | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgt_lock_share_row_exclusive | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgt_lock_share_update_exclusive | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 0]; Giggles [0, 0, 0, 1, 0] |
| pgt_notify | unowned | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_prepared | E1 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_read_only | E1 | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1] |
| pgt_roles | unowned | compare: full | COMPATIBLE | PG [0, 1, 0, 0, 1, 0, 0]; Giggles [0, 1, 0, 0, 1, 0, 0] |
| pgt_savepoints | E1 | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1]; Giggles [0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1] |
| pgt_settings | D | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgt_snapshot | E1 | compare: full | DIVERGENT | PG [0, 0, 0, 1]; Giggles [0, 0, 0, 0] |
| pgt_two_phase | E1 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0] |
| pgy_alter_column_aliases | E2 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 0, 1, 1]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_array_boolean | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_array_bounds | E2 | compare: full | DIVERGENT | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_array_bytea | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_array_constructors | E2 | compare: full | UNSUPPORTED | PG [1]; Giggles [0] |
| pgy_array_date | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_array_jsonb | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_array_multidimensional | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_array_numeric | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_array_operations | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1, 3, 1, 0]; Giggles [0, 1, 0, 3, 0, 0] |
| pgy_array_real | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_array_slices | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1, 1]; Giggles [0, 1, 0, 0] |
| pgy_array_smallint | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_array_text | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 2, 0] |
| pgy_array_timestamptz | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_array_uuid | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_array_varchar | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_boolean_input | E2 | compare: full | COMPATIBLE | PG [1, 1, 0, 1]; Giggles [1, 1, 0, 1] |
| pgy_bytea_escape | E2 | compare: full | DIVERGENT | PG [0, 0, 3, 3, 0]; Giggles [0, 0, 3, 3, 0] |
| pgy_bytea_invalid | E2 | compare: full | COMPATIBLE | PG [0, 0, 0, 1]; Giggles [0, 0, 0, 1] |
| pgy_cast_date_to_interval | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_date_to_time | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_date_to_timestamp | E2 | compare: full | DIVERGENT | PG [0, 1, 1, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_date_to_timestamptz | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgy_cast_date_to_timetz | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_interval_to_date | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_interval_to_time | E2 | compare: full | DIVERGENT | PG [0, 1, 1, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_interval_to_timestamp | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_interval_to_timestamptz | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_interval_to_timetz | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_time_to_date | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_time_to_interval | E2 | compare: full | DIVERGENT | PG [0, 1, 1, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_time_to_timestamp | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_time_to_timestamptz | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_time_to_timetz | E2 | compare: full | DIVERGENT | PG [0, 1, 1, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timestamp_to_date | E2 | compare: full | DIVERGENT | PG [0, 1, 1, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timestamp_to_interval | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timestamp_to_time | E2 | compare: full | DIVERGENT | PG [0, 1, 1, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timestamp_to_timestamptz | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgy_cast_timestamp_to_timetz | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timestamptz_to_date | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgy_cast_timestamptz_to_interval | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timestamptz_to_time | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgy_cast_timestamptz_to_timestamp | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgy_cast_timestamptz_to_timetz | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| pgy_cast_timetz_to_date | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timetz_to_interval | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timetz_to_time | E2 | compare: full | DIVERGENT | PG [0, 1, 1, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timetz_to_timestamp | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_cast_timetz_to_timestamptz | E2 | compare: full | DIVERGENT | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_char_functions | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 1, 0] |
| pgy_char_index | E2 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 1]; Giggles [0, 0, 0, 0, 0] |
| pgy_char_numeric_cast | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 1, 0] |
| pgy_char_padding | E2 | compare: full | UNSUPPORTED | PG [0, 3, 3, 1]; Giggles [0, 3, 0, 0] |
| pgy_collate_c | E2 | compare: full | COMPATIBLE | PG [0, 3, 3]; Giggles [0, 3, 3] |
| pgy_collate_posix | E2 | compare: full | COMPATIBLE | PG [0, 3, 3]; Giggles [0, 3, 3] |
| pgy_collate_unicode | E2 | compare: full | COMPATIBLE | PG [0, 3, 3]; Giggles [0, 3, 3] |
| pgy_collation_icu | E2 | compare: full | COMPATIBLE | PG [0, 0, 2, 1, 0, 0]; Giggles [0, 0, 2, 1, 0, 0] |
| pgy_collation_lifecycle | E2 | compare: full | COMPATIBLE | PG [0, 0, 0, 0, 0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 1, 0, 0] |
| pgy_column_bit | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_box | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_cidr | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_circle | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_datemultirange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_daterange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_float | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_float4 | E2 | compare: full | COMPATIBLE | PG [0, 2, 2]; Giggles [0, 2, 2] |
| pgy_column_float8 | E2 | compare: full | COMPATIBLE | PG [0, 2, 2]; Giggles [0, 2, 2] |
| pgy_column_float_p24 | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_float_p53 | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_inet | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_int2 | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_int4 | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_int4multirange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_int4range | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_int8 | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_int8multirange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_int8range | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_json | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_line | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_lseg | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_macaddr | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_macaddr8 | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_money | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_name | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_nummultirange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_numrange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_oid | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_path | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_pg_lsn | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_point | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_polygon | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_regclass | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_tsmultirange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_tsquery | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_tsrange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_tstzmultirange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_tstzrange | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_tsvector | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_varbit | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_column_xml | E2 | compare: full | UNSUPPORTED | PG [0, 2, 2]; Giggles [0, 0, 0] |
| pgy_composite_array_attribute | E2 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0] |
| pgy_composite_enum_attribute | E2 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0] |
| pgy_composite_nested | E2 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_conversion_default | E2 | compare: full | UNSUPPORTED | PG [0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0] |
| pgy_conversion_lifecycle | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1, 0, 0, 0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_date_dmy_compact | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_dmy_epoch | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_dmy_infinity | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_dmy_julian | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_dmy_month | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_dmy_slash | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_dmy_yesterday | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_german_compact | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_german_epoch | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_german_infinity | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_german_julian | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_german_month | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_german_slash | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_german_yesterday | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_iso_mdy_compact | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_iso_mdy_epoch | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_iso_mdy_infinity | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_iso_mdy_julian | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_iso_mdy_month | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_iso_mdy_slash | DD-0068 | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_iso_mdy_yesterday | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_postgres_compact | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_postgres_epoch | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_postgres_infinity | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_postgres_julian | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_postgres_month | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_postgres_slash | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_postgres_yesterday | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_sql_dmy_compact | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_sql_dmy_epoch | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_sql_dmy_infinity | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_date_sql_dmy_julian | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_sql_dmy_month | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_sql_dmy_slash | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_date_sql_dmy_yesterday | DD-0068 | compare: full | DIVERGENT | PG [0, 1, 0]; Giggles [0, 1, 0] |
| pgy_datetime_special | DD-0068 | compare: full | COMPATIBLE | PG [1, 1, 0]; Giggles [1, 1, 0] |
| pgy_domain_alter | E2 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_domain_nested | E2 | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_enum_alter | E2 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 0, 4, 4, 1, 16, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_enum_cast | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0]; Giggles [0, 0, 0] |
| pgy_extension_citext | E2 | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_extension_hstore | E2 | compare: full | UNSUPPORTED | PG [0, 0, 0, 1, 0, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_float_special | E2 | compare: full | COMPATIBLE | PG [1, 0, 1, 0]; Giggles [1, 0, 1, 0] |
| pgy_integer_input | E2 | compare: full | COMPATIBLE | PG [1, 0, 0, 0, 0]; Giggles [1, 0, 0, 0, 0] |
| pgy_interval_day | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_day_second | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_hour | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_hour_minute | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_minute | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_minute_second | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_month | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_precision | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_second | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_styles | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 0, 1, 0, 1, 0]; Giggles [0, 1, 0, 1, 0, 1, 0] |
| pgy_interval_year | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_interval_year_month | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1]; Giggles [0, 0, 0] |
| pgy_jsonb_operators | E2 | compare: full | UNSUPPORTED | PG [0, 1, 1, 1, 0]; Giggles [0, 1, 0, 0, 0] |
| pgy_key_index_array | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_key_index_date | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 0, 0, 0] |
| pgy_key_index_double | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_key_index_interval | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 0, 0, 0] |
| pgy_key_index_jsonb | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_key_index_numeric | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_key_index_real | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_key_index_time | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 0, 0, 0] |
| pgy_key_index_timestamp | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 0, 0, 0] |
| pgy_key_index_timestamptz | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 0, 0, 0] |
| pgy_key_index_timetz | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 0, 0, 0] |
| pgy_key_index_uuid | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| pgy_numeric_modifiers | E2 | compare: full | COMPATIBLE | PG [0, 2, 2, 0, 0, 1]; Giggles [0, 2, 2, 0, 0, 1] |
| pgy_numeric_special | E2 | compare: full | COMPATIBLE | PG [0, 4, 4, 1]; Giggles [0, 4, 4, 1] |
| pgy_text_edges | E2 | compare: full | COMPATIBLE | PG [1, 0]; Giggles [1, 0] |
| pgy_time_precision_0 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_time_precision_3 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_time_precision_6 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_time_precision_7 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timestamp_precision_0 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timestamp_precision_3 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timestamp_precision_6 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timestamp_precision_7 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timestamptz_precision_0 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timestamptz_precision_3 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timestamptz_precision_6 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timestamptz_precision_7 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timetz_precision_0 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timetz_precision_3 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timetz_precision_6 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timetz_precision_7 | DD-0068 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| pgy_timezone_input_est5edt | DD-0068 | compare: full | DIVERGENT | PG [1]; Giggles [0] |
| pgy_timezone_input_pst | DD-0068 | compare: full | DIVERGENT | PG [1]; Giggles [0] |
| pgy_timezone_operations | DD-0068 | compare: full | UNSUPPORTED | PG [0, 1, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0]; Giggles [0, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0] |
| pgy_type_alter_attributes | E2 | compare: full | UNSUPPORTED | PG [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_type_cascade_pk | E2 | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 1, 1, 0]; Giggles [0, 0, 1, 0, 1, 1, 0] |
| pgy_type_modifiers | E2 | compare: full | UNSUPPORTED | PG [0, 8, 8]; Giggles [0, 0, 0] |
| pgy_type_range_create | E2 | compare: full | UNSUPPORTED | PG [0, 0, 1, 1, 0, 1, 0, 0, 1]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 1] |
| pgy_type_schema_names | E2 | compare: full | DIVERGENT | PG [0, 0, 0, 0, 1, 0, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_type_shell | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 0, 0, 1]; Giggles [0, 0, 0, 0, 0, 1] |
| pgy_uuid_input | E2 | compare: full | UNSUPPORTED | PG [0, 1, 0, 1, 1, 0, 0, 0]; Giggles [0, 0, 0, 0, 0, 0, 0, 0] |
| pgy_varchar_length | E2 | compare: full | COMPATIBLE | PG [0, 2, 0, 2, 1]; Giggles [0, 2, 0, 2, 1] |
| delete_returning | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 1]; Giggles [0, 1, 1, 1, 1] |
| insert_returning_cols | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| insert_returning_default_id | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| insert_returning_star | E1 | compare: full | COMPATIBLE | PG [0, 1, 1]; Giggles [0, 1, 1] |
| update_returning | E1 | compare: full | COMPATIBLE | PG [0, 1, 1, 1, 2]; Giggles [0, 1, 1, 1, 2] |
| division_by_zero | E2 | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| error_sqlstate | E1 | compare: full | COMPATIBLE | PG [0, 1, 0, 1]; Giggles [0, 1, 0, 1] |
| select_literals | E3 | compare: full | COMPATIBLE | PG [1, 1]; Giggles [1, 1] |
| reset_all | D | compare: full | COMPATIBLE | PG [0, 0, 0, 0]; Giggles [0, 0, 0, 0] |
| set_application_name | D | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| set_show_search_path | D | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| set_show_timezone | D | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| savepoint_release | unowned | compare: full | COMPATIBLE | PG [0, 0, 0, 1, 1, 0, 0, 2]; Giggles [0, 0, 0, 1, 1, 0, 0, 2] |
| savepoint_rollback_to | unowned | compare: full | COMPATIBLE | PG [0, 0, 1, 0, 1, 0, 0, 1]; Giggles [0, 0, 1, 0, 1, 0, 0, 1] |
| set_transaction_isolation | D | compare: full | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |

### D

- pgt_discard_all: UNSUPPORTED; SQL: `SET work_mem = '16MB';`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgt_discard_plans: DIVERGENT; SQL: `PREPARE r0c_pgt_discard_plans AS SELECT 42 AS answer;`; PostgreSQL 00000 vs GigglesDB 42501.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42501

- pgt_discard_sequences: UNSUPPORTED; SQL: `DISCARD SEQUENCES;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgt_discard_temp: UNSUPPORTED; SQL: `CREATE TEMP TABLE r0c_pgt_discard_temp (id int PRIMARY KEY);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgt_encoding: DIVERGENT; SQL: `SET client_encoding TO 'LATIN1';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgt_settings: UNSUPPORTED; SQL: `SHOW work_mem;`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

### DD-0068

- pgf_advisory: UNSUPPORTED; SQL: `SELECT pg_try_advisory_lock_shared(k) FROM r0c_pgf_advisory ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_aggregate_general: UNSUPPORTED; SQL: `SELECT string_agg(s,',' ORDER BY id) FROM r0c_pgf_aggregate_general;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_aggregate_json: UNSUPPORTED; SQL: `SELECT json_agg(x ORDER BY id) FROM r0c_pgf_aggregate_json;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgf_aggregate_ordered: UNSUPPORTED; SQL: `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_aggregate_regression: UNSUPPORTED; SQL: `SELECT regr_count(y,x) FROM r0c_pgf_aggregate_regression;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_aggregate_statistics: UNSUPPORTED; SQL: `SELECT stddev(x) FROM r0c_pgf_aggregate_statistics;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_array_dimensions: DIVERGENT; SQL: `INSERT INTO r0c_pgf_array_dimensions VALUES (1,'{1,2,3}'),(2,'[0:1]={4,5}'),(3,NULL);`; PostgreSQL 00000 vs GigglesDB 22P02.
  First GigglesDB error: psql:<stdin>:9: ERROR:  22P02

- pgf_array_edit: DIVERGENT; SQL: `SELECT id, ARRAY[]::integer[] FROM r0c_pgf_array_edit ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42804

- pgf_array_unnest: DIVERGENT; SQL: `INSERT INTO r0c_pgf_array_unnest VALUES (1,ARRAY[1,2],ARRAY['a','b','c']),(2,ARRAY[]::integer[],ARRAY[]::text[]),(3,NULL,NULL);`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42804

- pgf_casts_numeric: UNSUPPORTED; SQL: `SELECT id, '255'::oid FROM r0c_pgf_casts_numeric ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:45: ERROR:  0A000

- pgf_casts_text: UNSUPPORTED; SQL: `SELECT id, '{"a":1}'::json FROM r0c_pgf_casts_text ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:36: ERROR:  0A000

- pgf_comparison: UNSUPPORTED; SQL: `SELECT id, x BETWEEN SYMMETRIC 3 AND 1 FROM r0c_pgf_comparison ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_conditional: UNSUPPORTED; SQL: `SELECT id, nullif(x,0) FROM r0c_pgf_conditional ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_datetime_arithmetic: UNSUPPORTED; SQL: `SELECT id, (t,t+i) OVERLAPS (timestamp '2024-02-28',timestamp '2024-03-02') FROM r0c_pgf_datetime_arithmetic ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:51: ERROR:  42601

- pgf_datetime_construct: UNSUPPORTED; SQL: `SELECT id, make_interval(days => 3, hours => 4) FROM r0c_pgf_datetime_construct ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:27: ERROR:  0A000

- pgf_datetime_current: UNSUPPORTED; SQL: `SELECT pg_typeof(timestamp with time zone '2024-01-01 00:00+00');`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:48: ERROR:  0A000

- pgf_datetime_timezone: UNSUPPORTED; SQL: `SELECT id, t AT TIME ZONE 'UTC' FROM r0c_pgf_datetime_timezone ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_datetime_truncate: DIVERGENT; SQL: `SELECT id, date_trunc('week',t::date) FROM r0c_pgf_datetime_truncate ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42804

- pgf_enum: UNSUPPORTED; SQL: `SELECT id, e, enum_first(e) FROM r0c_pgf_enum ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgf_errors: DIVERGENT; SQL: `SELECT width_bucket(1,0,10,0);`; PostgreSQL 2201G vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgf_format_datetime: UNSUPPORTED; SQL: `SELECT id, to_char(t,'YYYY-MM-DD HH24:MI:SS.US') FROM r0c_pgf_format_datetime ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_format_numbers: UNSUPPORTED; SQL: `SELECT id, to_char(x,'FM999,999.00') FROM r0c_pgf_format_numbers ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_geometric: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgf_geometric (id integer PRIMARY KEY, p point, b box);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgf_json_access: UNSUPPORTED; SQL: `SELECT id, j #> '{a,0}' FROM r0c_pgf_json_access ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:18: ERROR:  0A000

- pgf_json_build: UNSUPPORTED; SQL: `SELECT id, jsonb_build_object('id',id,'value',j) FROM r0c_pgf_json_build ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_json_expand: UNSUPPORTED; SQL: `SELECT id,v,n FROM r0c_pgf_json_expand CROSS JOIN LATERAL jsonb_array_elements(a) WITH ORDINALITY AS e(v,n) ORDER BY id,n;`; PostgreSQL 00000 vs GigglesDB 42883.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42883

- pgf_json_path: UNSUPPORTED; SQL: `SELECT id, j @? '$.a[*] ? (@ > 1)' FROM r0c_pgf_json_path ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_json_path_rows: UNSUPPORTED; SQL: `SELECT id,v,n FROM r0c_pgf_json_path_rows CROSS JOIN LATERAL jsonb_path_query(j,'$.a[*]') WITH ORDINALITY AS q(v,n) ORDER BY id,n;`; PostgreSQL 00000 vs GigglesDB 42883.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42883

- pgf_json_sql: UNSUPPORTED; SQL: `SELECT id, JSON_VALUE(j,'$.s') FROM r0c_pgf_json_sql ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_json_table: UNSUPPORTED; SQL: `SELECT t.id,q.n,q.name,q.qty,q.has_qty FROM r0c_pgf_json_table AS t CROSS JOIN JSON_TABLE(t.j,'$[*]' COLUMNS(n FOR ORDINALITY,name text PATH '$.name',qty intege`; PostgreSQL 00000 vs GigglesDB 42883.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42883

- pgf_math_bitwise: UNSUPPORTED; SQL: `SELECT id, n & 3 FROM r0c_pgf_math_bitwise ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_math_buckets: UNSUPPORTED; SQL: `SELECT id, width_bucket(x,0,10,5) FROM r0c_pgf_math_buckets ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_math_operators: UNSUPPORTED; SQL: `SELECT id, x ^ 2 FROM r0c_pgf_math_operators ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_math_random: UNSUPPORTED; SQL: `SELECT setseed(0.125) IS NULL;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgf_network: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgf_network (id integer PRIMARY KEY, a inet);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgf_range: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgf_range (id integer PRIMARY KEY, r int4range);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgf_sequence: UNSUPPORTED; SQL: `SELECT lastval();`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgf_set_series: UNSUPPORTED; SQL: `SELECT v FROM generate_series(0.5::numeric,2::numeric,0.5::numeric) AS g(v) ORDER BY v;`; PostgreSQL 00000 vs GigglesDB 42883.
  First GigglesDB error: psql:<stdin>:21: ERROR:  42883

- pgf_set_subscripts: DIVERGENT; SQL: `INSERT INTO r0c_pgf_set_subscripts VALUES (1,ARRAY[3,2,1]),(2,ARRAY[]::integer[]),(3,NULL);`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42804

- pgf_string_arrays: UNSUPPORTED; SQL: `SELECT id, string_to_array(s,',') FROM r0c_pgf_string_arrays ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_string_char: UNSUPPORTED; SQL: `SELECT id, length(s) FROM r0c_pgf_string_char ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_string_edit: DIVERGENT; SQL: `SELECT id, initcap(s) FROM r0c_pgf_string_edit ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 00000.

- pgf_string_lengths: UNSUPPORTED; SQL: `SELECT id, s ^@ 'a' FROM r0c_pgf_string_lengths ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:33: ERROR:  0A000

- pgf_string_patterns: UNSUPPORTED; SQL: `SELECT id, s SIMILAR TO '(Abc|a)%' FROM r0c_pgf_string_patterns ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:21: ERROR:  0A000

- pgf_string_regex_scalar: UNSUPPORTED; SQL: `SELECT id, regexp_match(s,'([0-9]+)') FROM r0c_pgf_string_regex_scalar ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_string_regex_sets: UNSUPPORTED; SQL: `SELECT id,m,ord FROM r0c_pgf_string_regex_sets CROSS JOIN LATERAL regexp_matches(s,'([0-9]+)','g') WITH ORDINALITY AS r(m,ord) ORDER BY id,ord;`; PostgreSQL 00000 vs GigglesDB 42883.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42883

- pgf_string_substrings: DIVERGENT; SQL: `SELECT id, substring(s FROM '[a-z]+') FROM r0c_pgf_string_substrings ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 22P02.
  First GigglesDB error: psql:<stdin>:30: ERROR:  22P02

- pgf_system_catalog: UNSUPPORTED; SQL: `SELECT id, pg_column_size(s) FROM r0c_pgf_system_catalog ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:21: ERROR:  0A000

- pgf_system_session: UNSUPPORTED; SQL: `SELECT pg_typeof(1);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgf_text_search: UNSUPPORTED; SQL: `SELECT id, to_tsvector('english',s) FROM r0c_pgf_text_search ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgf_uuid: UNSUPPORTED; SQL: `SELECT id, uuid_extract_version(u) FROM r0c_pgf_uuid ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_date_dmy_compact: DIVERGENT; SQL: `SET datestyle = 'DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_dmy_epoch: DIVERGENT; SQL: `SET datestyle = 'DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_dmy_infinity: DIVERGENT; SQL: `SET datestyle = 'DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_dmy_julian: DIVERGENT; SQL: `SET datestyle = 'DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_dmy_month: DIVERGENT; SQL: `SET datestyle = 'DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_dmy_slash: DIVERGENT; SQL: `SET datestyle = 'DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_dmy_yesterday: DIVERGENT; SQL: `SET datestyle = 'DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_german_compact: DIVERGENT; SQL: `SET datestyle = 'German';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_german_epoch: DIVERGENT; SQL: `SET datestyle = 'German';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_german_infinity: DIVERGENT; SQL: `SET datestyle = 'German';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_german_julian: DIVERGENT; SQL: `SET datestyle = 'German';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_german_month: DIVERGENT; SQL: `SET datestyle = 'German';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_german_slash: DIVERGENT; SQL: `SET datestyle = 'German';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_german_yesterday: DIVERGENT; SQL: `SET datestyle = 'German';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_iso_mdy_compact: DIVERGENT; SQL: `SELECT '20240101'::date;`; PostgreSQL 00000 vs GigglesDB 22007.
  First GigglesDB error: psql:<stdin>:9: ERROR:  22007

- pgy_date_iso_mdy_julian: DIVERGENT; SQL: `SELECT 'J2451187'::date;`; PostgreSQL 00000 vs GigglesDB 22007.
  First GigglesDB error: psql:<stdin>:9: ERROR:  22007

- pgy_date_iso_mdy_month: DIVERGENT; SQL: `SELECT 'Jan 2 2024'::date;`; PostgreSQL 00000 vs GigglesDB 22007.
  First GigglesDB error: psql:<stdin>:9: ERROR:  22007

- pgy_date_iso_mdy_slash: UNSUPPORTED; SQL: `SELECT '01/02/2024'::date;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgy_date_postgres_compact: DIVERGENT; SQL: `SET datestyle = 'Postgres';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_postgres_epoch: DIVERGENT; SQL: `SET datestyle = 'Postgres';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_postgres_infinity: DIVERGENT; SQL: `SET datestyle = 'Postgres';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_postgres_julian: DIVERGENT; SQL: `SET datestyle = 'Postgres';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_postgres_month: DIVERGENT; SQL: `SET datestyle = 'Postgres';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_postgres_slash: DIVERGENT; SQL: `SET datestyle = 'Postgres';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_postgres_yesterday: DIVERGENT; SQL: `SET datestyle = 'Postgres';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_sql_dmy_compact: DIVERGENT; SQL: `SET datestyle = 'SQL, DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_sql_dmy_epoch: DIVERGENT; SQL: `SET datestyle = 'SQL, DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_sql_dmy_infinity: DIVERGENT; SQL: `SET datestyle = 'SQL, DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_sql_dmy_julian: DIVERGENT; SQL: `SET datestyle = 'SQL, DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_sql_dmy_month: DIVERGENT; SQL: `SET datestyle = 'SQL, DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_sql_dmy_slash: DIVERGENT; SQL: `SET datestyle = 'SQL, DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_date_sql_dmy_yesterday: DIVERGENT; SQL: `SET datestyle = 'SQL, DMY';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_timezone_input_est5edt: DIVERGENT; SQL: `SELECT '2024-01-02 03:04:05 EST5EDT'::timestamptz, '2024-07-02 03:04:05 EST5EDT'::timestamptz;`; PostgreSQL 00000 vs GigglesDB 22007.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22007

- pgy_timezone_input_pst: DIVERGENT; SQL: `SELECT '2024-01-02 03:04:05 PST'::timestamptz, '2024-07-02 03:04:05 PST'::timestamptz;`; PostgreSQL 00000 vs GigglesDB 22007.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22007

- pgy_timezone_operations: UNSUPPORTED; SQL: `SELECT '2024-07-01 12:00'::timestamptz AT TIME ZONE 'UTC';`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

### E1

- pgd_alter_access: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_access SET ACCESS METHOD heap;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_alter_constraint: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_constraint_c ALTER CONSTRAINT r0c_pgd_alter_constraint_c_pid_fkey DEFERRABLE INITIALLY DEFERRED;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgd_alter_expression: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_alter_expression (id int PRIMARY KEY,v int,total int GENERATED ALWAYS AS (v*2) STORED);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_alter_identity: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_identity ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (START WITH 10);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_alter_if_exists: UNSUPPORTED; SQL: `ALTER TABLE IF EXISTS r0c_pgd_alter_if_exists_missing ADD COLUMN v int;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_alter_owner_schema: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_owner_schema OWNER TO postgres;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgd_alter_trigger: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgd_alter_trigger_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v:=NEW.v+1; RETURN NEW; END$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_alter_type_using: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_type_using ALTER COLUMN v TYPE int USING v::int;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgd_alter_typed: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_alter_typed OF r0c_pgd_alter_typed_type (PRIMARY KEY(id));`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_alter_using_index: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_using_index ADD CONSTRAINT r0c_pgd_alter_using_index_uq UNIQUE USING INDEX r0c_pgd_alter_using_index_u;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgd_cluster: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_cluster CLUSTER ON r0c_pgd_cluster_i;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42601

- pgd_ctas: DIVERGENT; SQL: `CREATE TABLE r0c_pgd_ctas_copy /* PRIMARY KEY added immediately after CTAS */ AS SELECT * FROM r0c_pgd_ctas;`; PostgreSQL 00000 vs GigglesDB 00000.

- pgd_ctas_no_data: DIVERGENT; SQL: `CREATE TABLE r0c_pgd_ctas_no_data_copy /* PRIMARY KEY added immediately after CTAS */ AS SELECT * FROM r0c_pgd_ctas_no_data WITH NO DATA;`; PostgreSQL 00000 vs GigglesDB 00000.

- pgd_default_arithmetic: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_default_arithmetic (id int PRIMARY KEY, v int DEFAULT (6*7));`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_default_function: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_default_function (id int PRIMARY KEY, v text DEFAULT upper('pending'));`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_default_user: DIVERGENT; SQL: `CREATE TABLE r0c_pgd_default_user (id int PRIMARY KEY, v text DEFAULT CURRENT_USER);`; PostgreSQL 00000 vs GigglesDB 42501.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42501

- pgd_default_uuid: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_default_uuid (id int PRIMARY KEY, v uuid DEFAULT gen_random_uuid());`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_exclude: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_exclude (id int PRIMARY KEY, v int4range);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgd_fk_match_full: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_fk_match_full_child(id int PRIMARY KEY,a int,b int,FOREIGN KEY(a,b) REFERENCES r0c_pgd_fk_match_full(a,b) MATCH FULL);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_generated_stored: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_generated_stored (id int PRIMARY KEY, price int, qty int, total int GENERATED ALWAYS AS (price*qty) STORED);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_generated_virtual: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_generated_virtual (id int PRIMARY KEY, price int, qty int, total int GENERATED ALWAYS AS (price*qty) VIRTUAL);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_idx_array: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_array_i ON r0c_pgd_idx_array (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_array_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_array_unique_i ON r0c_pgd_idx_array_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_char: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_char_i ON r0c_pgd_idx_char (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_char_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_char_unique_i ON r0c_pgd_idx_char_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_concurrent: UNSUPPORTED; SQL: `CREATE INDEX CONCURRENTLY r0c_pgd_idx_concurrent_i ON r0c_pgd_idx_concurrent (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_date: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_date_i ON r0c_pgd_idx_date (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_date_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_date_unique_i ON r0c_pgd_idx_date_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_double: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_double_i ON r0c_pgd_idx_double (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_double_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_double_unique_i ON r0c_pgd_idx_double_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_interval: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_interval_i ON r0c_pgd_idx_interval (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_interval_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_interval_unique_i ON r0c_pgd_idx_interval_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_jsonb: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_jsonb_i ON r0c_pgd_idx_jsonb (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_jsonb_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_jsonb_unique_i ON r0c_pgd_idx_jsonb_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_numeric: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_numeric_i ON r0c_pgd_idx_numeric (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_numeric_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_numeric_unique_i ON r0c_pgd_idx_numeric_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_real: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_real_i ON r0c_pgd_idx_real (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_real_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_real_unique_i ON r0c_pgd_idx_real_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_reindex: UNSUPPORTED; SQL: `REINDEX INDEX r0c_pgd_idx_reindex_i;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgd_idx_time: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_time_i ON r0c_pgd_idx_time (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_time_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_time_unique_i ON r0c_pgd_idx_time_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_timestamp: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_timestamp_i ON r0c_pgd_idx_timestamp (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_timestamp_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_timestamp_unique_i ON r0c_pgd_idx_timestamp_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_timestamptz: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_timestamptz_i ON r0c_pgd_idx_timestamptz (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_timestamptz_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_timestamptz_unique_i ON r0c_pgd_idx_timestamptz_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_uuid: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_uuid_i ON r0c_pgd_idx_uuid (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_uuid_unique: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgd_idx_uuid_unique_i ON r0c_pgd_idx_uuid_unique (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_inheritance: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_inheritance_child (PRIMARY KEY(id),extra int) INHERITS(r0c_pgd_inheritance);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_large_object: UNSUPPORTED; SQL: `SELECT lo_from_bytea(1900040001,decode('abcd','hex')) = 1900040001 AS created;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_like_all: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_like_all_copy (LIKE r0c_pgd_like_all INCLUDING ALL) /* PRIMARY KEY copied by INCLUDING ALL */;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_maintenance: UNSUPPORTED; SQL: `VACUUM (ANALYZE) r0c_pgd_maintenance;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgd_matview: UNSUPPORTED; SQL: `CREATE MATERIALIZED VIEW r0c_pgd_matview_v AS SELECT id,v*2 AS v FROM r0c_pgd_matview;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgd_matview_concurrent: UNSUPPORTED; SQL: `CREATE MATERIALIZED VIEW r0c_pgd_matview_concurrent_v AS SELECT id,v*2 AS v FROM r0c_pgd_matview_concurrent;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgd_migration: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_migration ADD COLUMN slug text DEFAULT lower('PENDING');`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_partition_attach: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_partition_attach (id int PRIMARY KEY,v text) PARTITION BY RANGE(id);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_partition_hash: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_partition_hash (id int PRIMARY KEY,v text) PARTITION BY HASH(id);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_partition_list: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_partition_list (id int PRIMARY KEY,v text) PARTITION BY LIST(id);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_partition_range: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_partition_range (id int PRIMARY KEY,v text) PARTITION BY RANGE(id);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_pk_char: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_pk_char (id char(4) PRIMARY KEY, v text);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_pk_date: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_pk_date (id date PRIMARY KEY, v text);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_pk_interval: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_pk_interval (id interval PRIMARY KEY, v text);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_pk_time: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_pk_time (id time PRIMARY KEY, v text);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_rule: UNSUPPORTED; SQL: `CREATE RULE r0c_pgd_rule_rule AS ON INSERT TO r0c_pgd_rule DO ALSO INSERT INTO r0c_pgd_rule_audit VALUES(NEW.id,NEW.v);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgd_schema: UNSUPPORTED; SQL: `ALTER SCHEMA r0c_pgd_schema RENAME TO r0c_pgd_schema_new;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_security_label: DIVERGENT; SQL: `SECURITY LABEL ON TABLE r0c_pgd_security_label IS 'confidential';`; PostgreSQL 22023 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_select_into: DIVERGENT; SQL: `SELECT * INTO r0c_pgd_select_into_copy FROM r0c_pgd_select_into;`; PostgreSQL 00000 vs GigglesDB 00000.

- pgd_sequence: UNSUPPORTED; SQL: `ALTER SEQUENCE r0c_pgd_sequence_s RENAME TO r0c_pgd_sequence_new;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgd_sequence_temp: UNSUPPORTED; SQL: `CREATE TEMPORARY SEQUENCE r0c_pgd_sequence_temp START WITH 4;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_temp_delete: UNSUPPORTED; SQL: `CREATE TEMPORARY TABLE r0c_pgd_temp_delete (id int PRIMARY KEY, v int) ON COMMIT DELETE ROWS;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_temp_drop: UNSUPPORTED; SQL: `CREATE TEMPORARY TABLE r0c_pgd_temp_drop (id int PRIMARY KEY, v int) ON COMMIT DROP;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_temp_preserve: UNSUPPORTED; SQL: `CREATE TEMP TABLE r0c_pgd_temp_preserve (id int PRIMARY KEY,v text) ON COMMIT PRESERVE ROWS;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_unique_deferred: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_unique_deferred ADD CONSTRAINT r0c_pgd_unique_deferred_uq UNIQUE(v) DEFERRABLE INITIALLY DEFERRED;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_unique_nulls: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_unique_nulls ADD CONSTRAINT r0c_pgd_unique_nulls_uq UNIQUE NULLS NOT DISTINCT(v);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_unlogged: UNSUPPORTED; SQL: `CREATE UNLOGGED TABLE r0c_pgd_unlogged (id int PRIMARY KEY, v text);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_view_aggregate: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_aggregate_v AS SELECT v,count(*) AS n FROM r0c_pgd_view_aggregate GROUP BY v;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_view_check: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_check_v AS SELECT * FROM r0c_pgd_view_check WHERE v>10 WITH CASCADED CHECK OPTION;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_view_cte: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_cte_v AS WITH c AS (SELECT id,v FROM r0c_pgd_view_cte WHERE id=1) SELECT * FROM c;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_view_delete: UNSUPPORTED; SQL: `DELETE FROM r0c_pgd_view_delete_v WHERE id=1;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgd_view_distinct: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_distinct_v AS SELECT DISTINCT v FROM r0c_pgd_view_distinct;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_view_expression: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_expression_v AS SELECT id,v*2 AS doubled FROM r0c_pgd_view_expression;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_view_insert: DIVERGENT; SQL: `INSERT INTO r0c_pgd_view_insert_v VALUES (1,12);`; PostgreSQL 00000 vs GigglesDB 42P01.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42P01

- pgd_view_join: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_join_v AS SELECT a.id,a.v+b.v AS total FROM r0c_pgd_view_join a JOIN r0c_pgd_view_join b ON a.id=b.id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_view_limit: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_limit_v AS SELECT id,v FROM r0c_pgd_view_limit ORDER BY id LIMIT 1;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_view_order: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_order_v AS SELECT id,v FROM r0c_pgd_view_order ORDER BY v DESC,id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_view_replace: UNSUPPORTED; SQL: `CREATE OR REPLACE VIEW r0c_pgd_view_replace_v (key,value) AS SELECT id,v*3 FROM r0c_pgd_view_replace;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgd_view_security_barrier: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_security_barrier_v WITH (security_barrier=true) AS SELECT * FROM r0c_pgd_view_security_barrier WHERE v>0;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_view_security_invoker: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_security_invoker_v WITH (security_invoker=true) AS SELECT * FROM r0c_pgd_view_security_invoker WHERE v>0;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_view_union: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgd_view_union_v AS SELECT id,v FROM r0c_pgd_view_union WHERE id=1 UNION SELECT id,v FROM r0c_pgd_view_union WHERE id=2;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_view_update: UNSUPPORTED; SQL: `UPDATE r0c_pgd_view_update_v SET v=25 WHERE id=1;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgd_wide_columns: DIVERGENT; SQL: `CREATE TABLE r0c_pgd_wide_columns (id int PRIMARY KEY, c1 int DEFAULT 1, c2 int DEFAULT 2, c3 int DEFAULT 3, c4 int DEFAULT 4, c5 int DEFAULT 5, c6 int DEFAULT `; PostgreSQL 00000 vs GigglesDB 54000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  54000

- pgl_drop_recreate: UNSUPPORTED; SQL: `INSERT INTO r0c_pgl_drop_recreate SELECT g, repeat('x', 1000) FROM generate_series(1, 1000) AS g;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgl_large_update: UNSUPPORTED; SQL: `INSERT INTO r0c_pgl_large_update SELECT g, 0 FROM generate_series(1, 50000) AS g;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgl_maintenance: UNSUPPORTED; SQL: `VACUUM r0c_pgl_maintenance;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgl_twelve_indexes: DIVERGENT; SQL: `CREATE INDEX r0c_pgl_twelve_indexes_i9 ON r0c_pgl_twelve_indexes (value);`; PostgreSQL 00000 vs GigglesDB 54000.
  First GigglesDB error: psql:<stdin>:33: ERROR:  54000

- pgl_wide_table: DIVERGENT; SQL: `CREATE TABLE r0c_pgl_wide_table (c1 integer PRIMARY KEY, c2 integer, c3 integer, c4 integer, c5 integer, c6 integer, c7 integer, c8 integer, c9 integer, c10 int`; PostgreSQL 00000 vs GigglesDB 54000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  54000

- pgq_delete_current_of: UNSUPPORTED; SQL: `DECLARE r0c_pgq_delete_current_of_c CURSOR FOR SELECT * FROM r0c_pgq_delete_current_of ORDER BY id FOR UPDATE;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgq_explain_merge: UNSUPPORTED; SQL: `EXPLAIN (COSTS OFF) MERGE INTO r0c_pgq_explain_merge t USING (VALUES (5)) s(id) ON t.id=s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgq_explain_select: UNSUPPORTED; SQL: `EXPLAIN (COSTS OFF) SELECT * FROM r0c_pgq_explain_select ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgq_update_current_of: UNSUPPORTED; SQL: `DECLARE r0c_pgq_update_current_of_c CURSOR FOR SELECT * FROM r0c_pgq_update_current_of ORDER BY id FOR UPDATE;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgq_upsert_partial: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgq_upsert_partial_u ON r0c_pgq_upsert_partial (dept) WHERE parent IS NULL;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgt_advisory_shared: UNSUPPORTED; SQL: `SELECT pg_advisory_lock_shared(918274661::bigint);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgt_chain: UNSUPPORTED; SQL: `COMMIT AND CHAIN;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgt_constraints: UNSUPPORTED; SQL: `SET CONSTRAINTS r0c_pgt_constraints_fk DEFERRED;`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42704

- pgt_cursor_binary: UNSUPPORTED; SQL: `DECLARE r0c_pgt_cursor_binary BINARY NO SCROLL CURSOR FOR SELECT 42::int4 AS id;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgt_cursor_hold: DIVERGENT; SQL: `CLOSE ALL;`; PostgreSQL 00000 vs GigglesDB 00000.

- pgt_cursor_scroll: UNSUPPORTED; SQL: `DECLARE r0c_pgt_cursor_scroll SCROLL CURSOR FOR SELECT n FROM (VALUES (1),(2),(3),(4)) AS x(n) ORDER BY n;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgt_deferrable: UNSUPPORTED; SQL: `SHOW transaction_deferrable;`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42704

- pgt_isolation_read_committed: UNSUPPORTED; SQL: `SHOW transaction_deferrable;`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42704

- pgt_isolation_read_uncommitted: UNSUPPORTED; SQL: `SHOW transaction_deferrable;`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42704

- pgt_isolation_repeatable_read: UNSUPPORTED; SQL: `SHOW transaction_deferrable;`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42704

- pgt_isolation_serializable: UNSUPPORTED; SQL: `SHOW transaction_deferrable;`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42704

- pgt_prepared: UNSUPPORTED; SQL: `PREPARE r0c_pgt_prepared_insert (int, text) AS INSERT INTO r0c_pgt_prepared VALUES ($1,$2) RETURNING id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgt_snapshot: DIVERGENT; SQL: `SET TRANSACTION SNAPSHOT '00000000-00000000-0';`; PostgreSQL 42704 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgt_two_phase: UNSUPPORTED; SQL: `SHOW max_prepared_transactions;`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

### E2

- composite_column: UNSUPPORTED; SQL: `CREATE TABLE r0c_composite_column (id int PRIMARY KEY, v r0c_composite_column_type);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- global_type_name: DIVERGENT; SQL: `CREATE TYPE r0c_global_type_name_b.r0c_global_type_name AS ENUM ('b');`; PostgreSQL 00000 vs GigglesDB 42710.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42710

- multidimensional_array: DIVERGENT; SQL: `INSERT INTO r0c_multidimensional_array VALUES (1, '{{1,2},{3,4}}');`; PostgreSQL 00000 vs GigglesDB 22P02.
  First GigglesDB error: psql:<stdin>:9: ERROR:  22P02

- pgy_alter_column_aliases: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgy_alter_column_aliases ADD COLUMN d float;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:18: ERROR:  0A000

- pgy_array_boolean: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_boolean (id int PRIMARY KEY, v boolean[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_bounds: DIVERGENT; SQL: `INSERT INTO r0c_pgy_array_bounds VALUES (1,'[0:1]={1,2}');`; PostgreSQL 00000 vs GigglesDB 22P02.
  First GigglesDB error: psql:<stdin>:9: ERROR:  22P02

- pgy_array_bytea: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_bytea (id int PRIMARY KEY, v bytea[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_constructors: UNSUPPORTED; SQL: `SELECT ARRAY[1,2,NULL], ARRAY[ARRAY[1,2],ARRAY[3,4]], ARRAY[]::int[], ARRAY['a','b']::varchar[];`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_date: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_date (id int PRIMARY KEY, v date[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_jsonb: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_jsonb (id int PRIMARY KEY, v jsonb[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_multidimensional: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_multidimensional (id int PRIMARY KEY, v int[][]);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_array_numeric: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_numeric (id int PRIMARY KEY, v numeric[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_operations: UNSUPPORTED; SQL: `SELECT 2 = ANY(v), cardinality(v), v @> ARRAY[2], tags || '{c}', tags || '{c}'::text[], array_position(v,2) FROM r0c_pgy_array_operations ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_array_real: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_real (id int PRIMARY KEY, v real[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_slices: UNSUPPORTED; SQL: `UPDATE r0c_pgy_array_slices SET v[2:3] = ARRAY[8,9] WHERE id=1;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgy_array_smallint: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_smallint (id int PRIMARY KEY, v smallint[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_text: UNSUPPORTED; SQL: `SELECT id, v, v = v, v @> v FROM r0c_pgy_array_text ORDER BY v, id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_array_timestamptz: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_timestamptz (id int PRIMARY KEY, v timestamptz[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_uuid: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_uuid (id int PRIMARY KEY, v uuid[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_array_varchar: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_array_varchar (id int PRIMARY KEY, v varchar[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_bytea_escape: DIVERGENT; SQL: `SET bytea_output='escape';`; PostgreSQL 00000 vs GigglesDB 22023.
  First GigglesDB error: psql:<stdin>:6: ERROR:  22023

- pgy_cast_date_to_interval: DIVERGENT; SQL: `SELECT v::interval FROM r0c_pgy_cast_date_to_interval ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_date_to_time: DIVERGENT; SQL: `SELECT v::time FROM r0c_pgy_cast_date_to_time ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_date_to_timestamp: DIVERGENT; SQL: `SELECT v::timestamp FROM r0c_pgy_cast_date_to_timestamp ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_date_to_timetz: DIVERGENT; SQL: `SELECT v::timetz FROM r0c_pgy_cast_date_to_timetz ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_interval_to_date: DIVERGENT; SQL: `SELECT v::date FROM r0c_pgy_cast_interval_to_date ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_interval_to_time: DIVERGENT; SQL: `SELECT v::time FROM r0c_pgy_cast_interval_to_time ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_interval_to_timestamp: DIVERGENT; SQL: `SELECT v::timestamp FROM r0c_pgy_cast_interval_to_timestamp ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_interval_to_timestamptz: DIVERGENT; SQL: `SELECT v::timestamptz FROM r0c_pgy_cast_interval_to_timestamptz ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_interval_to_timetz: DIVERGENT; SQL: `SELECT v::timetz FROM r0c_pgy_cast_interval_to_timetz ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_time_to_date: DIVERGENT; SQL: `SELECT v::date FROM r0c_pgy_cast_time_to_date ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_time_to_interval: DIVERGENT; SQL: `SELECT v::interval FROM r0c_pgy_cast_time_to_interval ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_time_to_timestamp: DIVERGENT; SQL: `SELECT v::timestamp FROM r0c_pgy_cast_time_to_timestamp ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_time_to_timestamptz: DIVERGENT; SQL: `SELECT v::timestamptz FROM r0c_pgy_cast_time_to_timestamptz ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_time_to_timetz: DIVERGENT; SQL: `SELECT v::timetz FROM r0c_pgy_cast_time_to_timetz ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timestamp_to_date: DIVERGENT; SQL: `SELECT v::date FROM r0c_pgy_cast_timestamp_to_date ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timestamp_to_interval: DIVERGENT; SQL: `SELECT v::interval FROM r0c_pgy_cast_timestamp_to_interval ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timestamp_to_time: DIVERGENT; SQL: `SELECT v::time FROM r0c_pgy_cast_timestamp_to_time ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timestamp_to_timetz: DIVERGENT; SQL: `SELECT v::timetz FROM r0c_pgy_cast_timestamp_to_timetz ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timestamptz_to_interval: DIVERGENT; SQL: `SELECT v::interval FROM r0c_pgy_cast_timestamptz_to_interval ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timetz_to_date: DIVERGENT; SQL: `SELECT v::date FROM r0c_pgy_cast_timetz_to_date ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timetz_to_interval: DIVERGENT; SQL: `SELECT v::interval FROM r0c_pgy_cast_timetz_to_interval ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timetz_to_time: DIVERGENT; SQL: `SELECT v::time FROM r0c_pgy_cast_timetz_to_time ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timetz_to_timestamp: DIVERGENT; SQL: `SELECT v::timestamp FROM r0c_pgy_cast_timetz_to_timestamp ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_cast_timetz_to_timestamptz: DIVERGENT; SQL: `SELECT v::timestamptz FROM r0c_pgy_cast_timetz_to_timestamptz ORDER BY id;`; PostgreSQL 42846 vs GigglesDB 42804.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42804

- pgy_char_functions: UNSUPPORTED; SQL: `SELECT v,length(v),octet_length(v),lower(v),upper(v),substring(v FROM 1 FOR 2),trim(v),v || 'x',replace(v,'a','z') FROM r0c_pgy_char_functions ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_char_index: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_char_index (id int PRIMARY KEY,v char(3) UNIQUE);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_char_numeric_cast: UNSUPPORTED; SQL: `SELECT v::integer,v::numeric,v::varchar,v::text FROM r0c_pgy_char_numeric_cast ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_char_padding: UNSUPPORTED; SQL: `SELECT id,v,v='a',v='a ',v::text,octet_length(v),v LIKE 'a' FROM r0c_pgy_char_padding ORDER BY r0c_pgy_char_padding.v,id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_column_bit: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_bit (id int PRIMARY KEY, v bit(4));`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_column_box: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_box (id int PRIMARY KEY, v box);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_cidr: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_cidr (id int PRIMARY KEY, v cidr);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_circle: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_circle (id int PRIMARY KEY, v circle);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_datemultirange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_datemultirange (id int PRIMARY KEY, v datemultirange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_daterange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_daterange (id int PRIMARY KEY, v daterange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_float: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_float (id int PRIMARY KEY, v float);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_float_p24: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_float_p24 (id int PRIMARY KEY, v float(24));`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_column_float_p53: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_float_p53 (id int PRIMARY KEY, v float(53));`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_column_inet: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_inet (id int PRIMARY KEY, v inet);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_int2: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_int2 (id int PRIMARY KEY, v int2);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_int4: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_int4 (id int PRIMARY KEY, v int4);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_int4multirange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_int4multirange (id int PRIMARY KEY, v int4multirange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_int4range: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_int4range (id int PRIMARY KEY, v int4range);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_int8: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_int8 (id int PRIMARY KEY, v int8);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_int8multirange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_int8multirange (id int PRIMARY KEY, v int8multirange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_int8range: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_int8range (id int PRIMARY KEY, v int8range);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_json: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_json (id int PRIMARY KEY, v json);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_line: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_line (id int PRIMARY KEY, v line);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_lseg: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_lseg (id int PRIMARY KEY, v lseg);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_macaddr: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_macaddr (id int PRIMARY KEY, v macaddr);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_macaddr8: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_macaddr8 (id int PRIMARY KEY, v macaddr8);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_money: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_money (id int PRIMARY KEY, v money);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_name: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_name (id int PRIMARY KEY, v name);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_nummultirange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_nummultirange (id int PRIMARY KEY, v nummultirange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_numrange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_numrange (id int PRIMARY KEY, v numrange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_oid: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_oid (id int PRIMARY KEY, v oid);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_path: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_path (id int PRIMARY KEY, v path);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_pg_lsn: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_pg_lsn (id int PRIMARY KEY, v pg_lsn);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_column_point: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_point (id int PRIMARY KEY, v point);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_polygon: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_polygon (id int PRIMARY KEY, v polygon);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_regclass: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_regclass (id int PRIMARY KEY, v regclass);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_tsmultirange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_tsmultirange (id int PRIMARY KEY, v tsmultirange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_tsquery: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_tsquery (id int PRIMARY KEY, v tsquery);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_tsrange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_tsrange (id int PRIMARY KEY, v tsrange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_tstzmultirange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_tstzmultirange (id int PRIMARY KEY, v tstzmultirange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_tstzrange: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_tstzrange (id int PRIMARY KEY, v tstzrange);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_tsvector: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_tsvector (id int PRIMARY KEY, v tsvector);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_varbit: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_varbit (id int PRIMARY KEY, v varbit);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_column_xml: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_column_xml (id int PRIMARY KEY, v xml);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgy_composite_array_attribute: UNSUPPORTED; SQL: `CREATE TYPE r0c_pgy_composite_array_attribute_outer AS (items r0c_pgy_composite_array_attribute_pair[]);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgy_composite_enum_attribute: UNSUPPORTED; SQL: `CREATE TYPE r0c_pgy_composite_enum_attribute_c AS (mood r0c_pgy_composite_enum_attribute_e);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgy_composite_nested: UNSUPPORTED; SQL: `CREATE TYPE r0c_pgy_composite_nested_outer AS (pair r0c_pgy_composite_nested_pair, items int[], checked r0c_pgy_composite_nested_d);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_conversion_default: UNSUPPORTED; SQL: `CREATE DEFAULT CONVERSION r0c_pgy_conversion_default_s.r0c_pgy_conversion_default_c FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgy_conversion_lifecycle: UNSUPPORTED; SQL: `CREATE CONVERSION r0c_pgy_conversion_lifecycle_c FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_domain_alter: UNSUPPORTED; SQL: `CREATE DOMAIN r0c_pgy_domain_alter_d AS int DEFAULT 2;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_domain_nested: UNSUPPORTED; SQL: `CREATE DOMAIN r0c_pgy_domain_nested_child AS r0c_pgy_domain_nested_base NOT NULL CHECK (VALUE < 100);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgy_enum_alter: UNSUPPORTED; SQL: `ALTER TYPE r0c_pgy_enum_alter_e ADD VALUE 'ok' BEFORE 'happy';`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgy_enum_cast: UNSUPPORTED; SQL: `SELECT 'z'::r0c_pgy_enum_cast_e, 'z'::r0c_pgy_enum_cast_e < 'a'::r0c_pgy_enum_cast_e, ARRAY['z','a']::r0c_pgy_enum_cast_e[];`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgy_extension_citext: UNSUPPORTED; SQL: `CREATE EXTENSION citext WITH SCHEMA r0c_pgy_extension_citext_s;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgy_extension_hstore: UNSUPPORTED; SQL: `CREATE EXTENSION hstore WITH SCHEMA r0c_pgy_extension_hstore_s;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgy_interval_day: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_day (id int PRIMARY KEY,v interval day);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_day_second: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_day_second (id int PRIMARY KEY,v interval day to second(2));`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_hour: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_hour (id int PRIMARY KEY,v interval hour);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_hour_minute: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_hour_minute (id int PRIMARY KEY,v interval hour to minute);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_minute: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_minute (id int PRIMARY KEY,v interval minute);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_minute_second: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_minute_second (id int PRIMARY KEY,v interval minute to second(3));`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_month: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_month (id int PRIMARY KEY,v interval month);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_precision: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_precision (id int PRIMARY KEY,v interval(3));`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_second: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_second (id int PRIMARY KEY,v interval second(3));`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_year: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_year (id int PRIMARY KEY,v interval year);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_interval_year_month: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_interval_year_month (id int PRIMARY KEY,v interval year to month);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_jsonb_operators: UNSUPPORTED; SQL: `SELECT v#>'{a,b}',v#>>'{a,b,1}',v @> '{"c":"x"}',v <@ v,v ? 'c',v ?| ARRAY['c','d'],v ?& ARRAY['a','c'],v-'c',v||'{"d":1}',v||'{"d":1}'::jsonb FROM r0c_pgy_json`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_key_index_array: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgy_key_index_array_idx ON r0c_pgy_key_index_array(v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_key_index_date: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_key_index_date (v date PRIMARY KEY);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_key_index_double: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgy_key_index_double_idx ON r0c_pgy_key_index_double(v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_key_index_interval: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_key_index_interval (v interval PRIMARY KEY);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_key_index_jsonb: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgy_key_index_jsonb_idx ON r0c_pgy_key_index_jsonb(v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_key_index_numeric: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgy_key_index_numeric_idx ON r0c_pgy_key_index_numeric(v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_key_index_real: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgy_key_index_real_idx ON r0c_pgy_key_index_real(v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_key_index_time: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_key_index_time (v time PRIMARY KEY);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_key_index_timestamp: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_key_index_timestamp (v timestamp PRIMARY KEY);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_key_index_timestamptz: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_key_index_timestamptz (v timestamptz PRIMARY KEY);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_key_index_timetz: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_key_index_timetz (v timetz PRIMARY KEY);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgy_key_index_uuid: UNSUPPORTED; SQL: `CREATE UNIQUE INDEX r0c_pgy_key_index_uuid_idx ON r0c_pgy_key_index_uuid(v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgy_type_alter_attributes: UNSUPPORTED; SQL: `ALTER TYPE r0c_pgy_type_alter_attributes_c ADD ATTRIBUTE b text CASCADE;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgy_type_modifiers: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_type_modifiers (id int PRIMARY KEY,a varchar(12),b char(5),c numeric(10,3),d timestamp(3),e time(2),f interval second(3),g bit(7));`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_type_range_create: UNSUPPORTED; SQL: `CREATE TYPE r0c_pgy_type_range_create_r AS RANGE (subtype=int4,subtype_diff=int4range_subdiff,multirange_type_name=r0c_pgy_type_range_create_m);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_type_schema_names: DIVERGENT; SQL: `CREATE TYPE r0c_pgy_type_schema_names_b.r0c_pgy_type_schema_names_e AS ENUM ('b');`; PostgreSQL 00000 vs GigglesDB 42710.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42710

- pgy_type_shell: UNSUPPORTED; SQL: `CREATE TYPE r0c_pgy_type_shell_shell;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgy_uuid_input: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgy_uuid_input (id uuid PRIMARY KEY, v uuid UNIQUE);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

### E3

- pgl_large_aggregates: UNSUPPORTED; SQL: `INSERT INTO r0c_pgl_large_aggregates SELECT g, g::bigint, g::numeric FROM generate_series(1, 100000) AS g;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgq_literal_bit: UNSUPPORTED; SQL: `SELECT id,B'1010' FROM r0c_pgq_literal_bit ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgq_literal_hex: UNSUPPORTED; SQL: `SELECT id,X'AF' FROM r0c_pgq_literal_hex ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

### G

- pgd_alter_column_options: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_column_options ALTER COLUMN v SET STATISTICS 200;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_alter_options: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_options SET (autovacuum_enabled=false);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_alter_rename_constraint: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_rename_constraint RENAME CONSTRAINT r0c_pgd_alter_rename_constraint_positive TO r0c_pgd_alter_rename_constraint_check;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgd_alter_replica: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_replica REPLICA IDENTITY FULL;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_alter_rls: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgd_alter_rls ENABLE ROW LEVEL SECURITY;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_alter_system: UNSUPPORTED; SQL: `ALTER SYSTEM SET work_mem='8MB';`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_access_method: UNSUPPORTED; SQL: `CREATE ACCESS METHOD r0c_pgd_comment_access_method TYPE TABLE HANDLER heap_tableam_handler;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_aggregate: UNSUPPORTED; SQL: `CREATE AGGREGATE r0c_pgd_comment_aggregate(int) (SFUNC=int4pl,STYPE=int,INITCOND='0');`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_cast: UNSUPPORTED; SQL: `CREATE CAST (r0c_pgd_comment_cast AS text) WITH INOUT;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_comment_constraint: UNSUPPORTED; SQL: `COMMENT ON CONSTRAINT r0c_pgd_comment_constraint_ck ON r0c_pgd_comment_constraint IS 'migration note';`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_comment_conversion: UNSUPPORTED; SQL: `CREATE CONVERSION r0c_pgd_comment_conversion FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_domain: UNSUPPORTED; SQL: `COMMENT ON DOMAIN r0c_pgd_comment_domain IS 'migration note';`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_comment_event_trigger: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgd_comment_event_trigger_f() RETURNS event_trigger LANGUAGE plpgsql AS $$BEGIN RETURN; END$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_fdw: UNSUPPORTED; SQL: `CREATE FOREIGN DATA WRAPPER r0c_pgd_comment_fdw;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_foreign_table: UNSUPPORTED; SQL: `CREATE FOREIGN DATA WRAPPER r0c_pgd_comment_foreign_table_fdw;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_function: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgd_comment_function() RETURNS int LANGUAGE sql IMMUTABLE AS $$SELECT 1$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_index: UNSUPPORTED; SQL: `COMMENT ON INDEX r0c_pgd_comment_index_i IS 'migration note';`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_comment_language: UNSUPPORTED; SQL: `CREATE LANGUAGE r0c_pgd_comment_language HANDLER plpgsql_call_handler INLINE plpgsql_inline_handler VALIDATOR plpgsql_validator;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_large_object: UNSUPPORTED; SQL: `SELECT lo_from_bytea(1900040002,decode('ab','hex'))=1900040002 AS created;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_comment_long: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgd_comment_long_f() RETURNS void LANGUAGE plpgsql AS $$BEGIN EXECUTE format('COMMENT ON TABLE r0c_pgd_comment_long IS %L',repeat('x',70000)`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_comment_materialized_view: UNSUPPORTED; SQL: `CREATE MATERIALIZED VIEW r0c_pgd_comment_materialized_view_v AS SELECT * FROM r0c_pgd_comment_materialized_view;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_comment_opclass: UNSUPPORTED; SQL: `CREATE OPERATOR CLASS r0c_pgd_comment_opclass FOR TYPE int USING btree AS OPERATOR 1 < (int,int),OPERATOR 2 <= (int,int),OPERATOR 3 = (int,int),OPERATOR 4 >= (i`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_operator: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgd_comment_operator_eq(int,int) RETURNS bool LANGUAGE sql IMMUTABLE AS $$SELECT $1=$2$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_comment_opfamily: UNSUPPORTED; SQL: `CREATE OPERATOR FAMILY r0c_pgd_comment_opfamily USING btree;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_policy: UNSUPPORTED; SQL: `CREATE POLICY r0c_pgd_comment_policy_p ON r0c_pgd_comment_policy USING (true);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_comment_procedure: UNSUPPORTED; SQL: `CREATE PROCEDURE r0c_pgd_comment_procedure() LANGUAGE sql AS $$SELECT 1$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_publication: UNSUPPORTED; SQL: `CREATE PUBLICATION r0c_pgd_comment_publication_p FOR TABLE r0c_pgd_comment_publication;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_comment_role: UNSUPPORTED; SQL: `COMMENT ON ROLE r0c_pgd_comment_role IS 'migration note';`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_comment_routine: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgd_comment_routine() RETURNS int LANGUAGE sql AS $$SELECT 3$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_rule: UNSUPPORTED; SQL: `CREATE RULE r0c_pgd_comment_rule_r AS ON INSERT TO r0c_pgd_comment_rule DO ALSO NOTHING;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_comment_schema: UNSUPPORTED; SQL: `COMMENT ON SCHEMA r0c_pgd_comment_schema IS 'migration note';`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_comment_sequence: UNSUPPORTED; SQL: `COMMENT ON SEQUENCE r0c_pgd_comment_sequence IS 'migration note';`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_comment_server: UNSUPPORTED; SQL: `CREATE FOREIGN DATA WRAPPER r0c_pgd_comment_server_fdw;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_statistics: UNSUPPORTED; SQL: `CREATE STATISTICS r0c_pgd_comment_statistics_s ON a,b FROM r0c_pgd_comment_statistics;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_comment_subscription: UNSUPPORTED; SQL: `CREATE SUBSCRIPTION r0c_pgd_comment_subscription CONNECTION 'host=127.0.0.1 port=1 dbname=postgres' PUBLICATION r0c_pgd_comment_subscription_p WITH(connect=fals`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_trigger: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgd_comment_trigger_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_comment_ts_config: UNSUPPORTED; SQL: `CREATE TEXT SEARCH CONFIGURATION r0c_pgd_comment_ts_config (COPY=pg_catalog.simple);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_ts_dict: UNSUPPORTED; SQL: `CREATE TEXT SEARCH DICTIONARY r0c_pgd_comment_ts_dict (TEMPLATE=pg_catalog.simple);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_ts_parser: UNSUPPORTED; SQL: `CREATE TEXT SEARCH PARSER r0c_pgd_comment_ts_parser (START=prsd_start,GETTOKEN=prsd_nexttoken,END=prsd_end,LEXTYPES=prsd_lextype,HEADLINE=prsd_headline);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_ts_template: UNSUPPORTED; SQL: `CREATE TEXT SEARCH TEMPLATE r0c_pgd_comment_ts_template (INIT=dsimple_init,LEXIZE=dsimple_lexize);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_comment_type: UNSUPPORTED; SQL: `COMMENT ON TYPE r0c_pgd_comment_type IS 'migration note';`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgd_comment_unavailable: DIVERGENT; SQL: `COMMENT ON TABLESPACE r0c_pgd_comment_unavailable IS 'missing tablespace';`; PostgreSQL 42704 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_database: UNSUPPORTED; SQL: `CREATE DATABASE r0c_pgd_database TEMPLATE template0;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_idx_alter: UNSUPPORTED; SQL: `ALTER INDEX r0c_pgd_idx_alter_i RENAME TO r0c_pgd_idx_alter_renamed;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgd_idx_brin: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_brin_i ON r0c_pgd_idx_brin USING brin (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_descending: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_descending_i ON r0c_pgd_idx_descending (v DESC NULLS FIRST);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_expression: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_expression_i ON r0c_pgd_idx_expression (lower(v));`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_gin_array: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_gin_array_i ON r0c_pgd_idx_gin_array USING gin (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_gin_jsonb: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_gin_jsonb_i ON r0c_pgd_idx_gin_jsonb USING gin (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_gist: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_idx_gist (id int PRIMARY KEY, v int4range);`; PostgreSQL 00000 vs GigglesDB 42704.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42704

- pgd_idx_hash: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_hash_i ON r0c_pgd_idx_hash USING hash (v);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_include: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_include_i ON r0c_pgd_idx_include (v) INCLUDE (id);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_idx_nine: DIVERGENT; SQL: `CREATE INDEX r0c_pgd_idx_nine_i9 ON r0c_pgd_idx_nine (v);`; PostgreSQL 00000 vs GigglesDB 54000.
  First GigglesDB error: psql:<stdin>:33: ERROR:  54000

- pgd_idx_partial: UNSUPPORTED; SQL: `CREATE INDEX r0c_pgd_idx_partial_i ON r0c_pgd_idx_partial (v) WHERE v > 1;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgd_publication: UNSUPPORTED; SQL: `CREATE PUBLICATION r0c_pgd_publication_p FOR TABLE r0c_pgd_publication;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_statistics: UNSUPPORTED; SQL: `CREATE STATISTICS r0c_pgd_statistics_s (dependencies,ndistinct,mcv) ON a,b FROM r0c_pgd_statistics;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgd_storage_fillfactor: UNSUPPORTED; SQL: `CREATE TABLE r0c_pgd_storage_fillfactor (id int PRIMARY KEY,v text) WITH(fillfactor=70);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgd_subscription: UNSUPPORTED; SQL: `CREATE SUBSCRIPTION r0c_pgd_subscription CONNECTION 'host=127.0.0.1 port=1 dbname=postgres' PUBLICATION r0c_pgd_subscription_pub WITH(connect=false);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgd_tablespace: DIVERGENT; SQL: `CREATE TABLESPACE r0c_pgd_tablespace LOCATION '/r0c_pgd_tablespace_missing';`; PostgreSQL 58P01 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgt_catalogs: UNSUPPORTED; SQL: `DO $$ BEGIN EXECUTE 'SHOW ALL'; END $$;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

### Unowned (needs an owner)

- pgr_all_functions: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgr_all_functions.r0c_pgr_all_functions_f(integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT $1 + 1 $$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42601

- pgr_all_sequences: UNSUPPORTED; SQL: `GRANT USAGE ON ALL SEQUENCES IN SCHEMA r0c_pgr_all_sequences TO r0c_pgr_all_sequences_u;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:27: ERROR:  42601

- pgr_all_tables: UNSUPPORTED; SQL: `GRANT SELECT ON ALL TABLES IN SCHEMA r0c_pgr_all_tables TO r0c_pgr_all_tables_u;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:30: ERROR:  42601

- pgr_attributes: UNSUPPORTED; SQL: `CREATE ROLE r0c_pgr_attributes_u LOGIN CREATEDB CREATEROLE NOINHERIT CONNECTION LIMIT 3 VALID UNTIL '2099-01-01 00:00:00+00';`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgr_columns: UNSUPPORTED; SQL: `GRANT SELECT (id, allowed), UPDATE (allowed) ON r0c_pgr_columns TO r0c_pgr_columns_u;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:15: ERROR:  0A000

- pgr_default_privileges: UNSUPPORTED; SQL: `ALTER DEFAULT PRIVILEGES FOR ROLE r0c_pgr_default_privileges_owner IN SCHEMA r0c_pgr_default_privileges GRANT SELECT ON TABLES TO r0c_pgr_default_privileges_u;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:27: ERROR:  42601

- pgr_has_role_tables: DIVERGENT; SQL: `SELECT id, pg_has_role(who, 'r0c_pgr_has_role_tables_g', priv) FROM r0c_pgr_has_role_tables_t ORDER BY id;`; PostgreSQL 00000 vs GigglesDB 55000.
  First GigglesDB error: psql:<stdin>:24: ERROR:  55000

- pgr_owned: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgr_owned OWNER TO r0c_pgr_owned_old;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42601

- pgr_privilege_catalog: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:18: ERROR:  42601

- pgr_rls_delete: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgr_rls_delete ENABLE ROW LEVEL SECURITY;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:18: ERROR:  42601

- pgr_rls_force: UNSUPPORTED; SQL: `CREATE ROLE r0c_pgr_rls_force_bypass BYPASSRLS;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgr_rls_insert: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgr_rls_insert ENABLE ROW LEVEL SECURITY;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:18: ERROR:  42601

- pgr_rls_restrictive: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgr_rls_restrictive ENABLE ROW LEVEL SECURITY;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:18: ERROR:  42601

- pgr_rls_select: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgr_rls_select ENABLE ROW LEVEL SECURITY;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:18: ERROR:  42601

- pgr_rls_update: UNSUPPORTED; SQL: `ALTER TABLE r0c_pgr_rls_update ENABLE ROW LEVEL SECURITY;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:18: ERROR:  42601

- pgr_view_grant: UNSUPPORTED; SQL: `CREATE VIEW r0c_pgr_view_grant.r0c_pgr_view_grant_v WITH (security_invoker = false) AS SELECT id, value FROM r0c_pgr_view_grant.r0c_pgr_view_grant_t;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:27: ERROR:  42601

- pgp_access_method: UNSUPPORTED; SQL: `CREATE ACCESS METHOD r0c_pgp_access_method TYPE TABLE HANDLER heap_tableam_handler;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_aggregate: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_aggregate_step(integer,integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT $1+$2$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_available_extensions: DIVERGENT; SQL: `SELECT name,installed_version IS NULL AS uninstalled FROM pg_available_extensions WHERE name IN ('pgcrypto','citext','hstore','pg_trgm','uuid-ossp','btree_gist'`; PostgreSQL 00000 vs GigglesDB 00000.

- pgp_cast: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_cast_f(r0c_pgp_cast) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'value=' || ($1).v$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_do_block: UNSUPPORTED; SQL: `DO $$BEGIN INSERT INTO r0c_pgp_do_block VALUES(1); END;$$ LANGUAGE plpgsql;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- pgp_event_trigger: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_event_trigger_f() RETURNS event_trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_event_trigger_log(tag,kind) SELECT command_tag,o`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_ext_btree_gist: UNSUPPORTED; SQL: `CREATE EXTENSION "btree_gist";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_citext: UNSUPPORTED; SQL: `CREATE EXTENSION "citext";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_crosstab: UNSUPPORTED; SQL: `CREATE EXTENSION "tablefunc";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_crypt: UNSUPPORTED; SQL: `CREATE EXTENSION "pgcrypto";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_cube: UNSUPPORTED; SQL: `CREATE EXTENSION "cube";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_digest: UNSUPPORTED; SQL: `CREATE EXTENSION "pgcrypto";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_earth: UNSUPPORTED; SQL: `CREATE EXTENSION cube;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_fuzzy: UNSUPPORTED; SQL: `CREATE EXTENSION "fuzzystrmatch";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_hstore: UNSUPPORTED; SQL: `CREATE EXTENSION "hstore";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_intarray: UNSUPPORTED; SQL: `CREATE EXTENSION "intarray";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_ltree: UNSUPPORTED; SQL: `CREATE EXTENSION "ltree";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_statements: UNSUPPORTED; SQL: `CREATE EXTENSION pg_stat_statements;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_trgm: UNSUPPORTED; SQL: `CREATE EXTENSION "pg_trgm";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_ext_unaccent: UNSUPPORTED; SQL: `CREATE EXTENSION "unaccent";`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_extension_lifecycle: UNSUPPORTED; SQL: `CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA r0c_pgp_extension_lifecycle;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:12: ERROR:  0A000

- pgp_fdw: UNSUPPORTED; SQL: `CREATE EXTENSION postgres_fdw;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_fdw_file: UNSUPPORTED; SQL: `CREATE EXTENSION file_fdw;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_fdw_import: UNSUPPORTED; SQL: `CREATE EXTENSION postgres_fdw;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_fdw_postgres: UNSUPPORTED; SQL: `CREATE EXTENSION postgres_fdw;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgp_fts_config: UNSUPPORTED; SQL: `CREATE TEXT SEARCH CONFIGURATION r0c_pgp_fts_config (COPY=pg_catalog.simple);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_fts_dictionary: UNSUPPORTED; SQL: `CREATE TEXT SEARCH DICTIONARY r0c_pgp_fts_dictionary (TEMPLATE=pg_catalog.simple,STOPWORDS=english);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_fts_parser: UNSUPPORTED; SQL: `CREATE TEXT SEARCH PARSER r0c_pgp_fts_parser (START=pg_catalog.prsd_start,GETTOKEN=pg_catalog.prsd_nexttoken,END=pg_catalog.prsd_end,HEADLINE=pg_catalog.prsd_he`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_fts_template: UNSUPPORTED; SQL: `CREATE TEXT SEARCH TEMPLATE r0c_pgp_fts_template (INIT=pg_catalog.dsimple_init,LEXIZE=pg_catalog.dsimple_lexize);`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_function_alter: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_function_alter(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_function_catalog: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_function_catalog(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_function_def: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_function_def(integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT $1 + 1$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_function_drop: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_function_drop(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_function_replace: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_function_replace(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1+1$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_language: UNSUPPORTED; SQL: `CREATE TRUSTED LANGUAGE r0c_pgp_language HANDLER plpgsql_call_handler INLINE plpgsql_inline_handler VALIDATOR plpgsql_validator;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_opclass: UNSUPPORTED; SQL: `CREATE OPERATOR FAMILY r0c_pgp_opclass_fam USING btree;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_operator: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_operator_f(integer,integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT $1*10+$2$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_opfamily: UNSUPPORTED; SQL: `CREATE OPERATOR FAMILY r0c_pgp_opfamily USING btree;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_cursor: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_cursor() RETURNS integer LANGUAGE plpgsql AS $f$DECLARE c CURSOR FOR SELECT 7; x integer; BEGIN OPEN c; FETCH c INTO x; CLOSE c; RETU`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_diagnostics: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_diagnostics() RETURNS bigint LANGUAGE plpgsql AS $f$DECLARE n bigint; BEGIN PERFORM g FROM generate_series(1,4) g; GET DIAGNOSTICS n `; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_dynamic: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_dynamic(x integer) RETURNS integer LANGUAGE plpgsql AS $f$DECLARE y integer; BEGIN EXECUTE 'SELECT $1 + 7' INTO y USING x; RETURN y; `; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_exception: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_exception() RETURNS text LANGUAGE plpgsql AS $f$BEGIN RAISE EXCEPTION USING ERRCODE='22012', MESSAGE='application failure'; EXCEPTION`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_for: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_for() RETURNS integer LANGUAGE plpgsql AS $f$DECLARE s integer := 0; i integer; BEGIN FOR i IN 1..3 LOOP s := s+i; END LOOP; RETURN s`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_foreach: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_foreach() RETURNS integer LANGUAGE plpgsql AS $f$DECLARE s integer := 0; i integer; BEGIN FOREACH i IN ARRAY ARRAY[2,4,6] LOOP s := s`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_if_case: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_if_case(x integer) RETURNS text LANGUAGE plpgsql AS $f$BEGIN IF x < 0 THEN RETURN 'negative'; END IF; CASE x WHEN 0 THEN RETURN 'zero`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_notice: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_notice() RETURNS integer LANGUAGE plpgsql AS $f$BEGIN RAISE NOTICE 'application notice'; RETURN 7; END;$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_raise_error: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_raise_error() RETURNS integer LANGUAGE plpgsql AS $f$BEGIN RAISE EXCEPTION USING ERRCODE='P0001', MESSAGE='rejected'; END;$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_return_next: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_return_next() RETURNS SETOF integer LANGUAGE plpgsql AS $f$BEGIN RETURN NEXT 2; RETURN NEXT 1; RETURN; END;$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_return_query: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_return_query() RETURNS TABLE(id integer) LANGUAGE plpgsql AS $f$BEGIN RETURN QUERY SELECT generate_series(1,3); END;$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_rowtype: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_rowtype_f() RETURNS text LANGUAGE plpgsql AS $$DECLARE r r0c_pgp_pl_rowtype%ROWTYPE; k r0c_pgp_pl_rowtype.id%TYPE := 1; BEGIN SELECT `; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_pl_vars: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_vars(x integer) RETURNS integer LANGUAGE plpgsql AS $f$DECLARE y integer := x+3; BEGIN y := y*2; RETURN y; END;$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_pl_while: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_pl_while() RETURNS integer LANGUAGE plpgsql AS $f$DECLARE s integer := 0; i integer; BEGIN WHILE s < 3 LOOP s := s+1; END LOOP; RETURN s`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_procedure_inout: UNSUPPORTED; SQL: `CREATE PROCEDURE r0c_pgp_procedure_inout(INOUT x integer) LANGUAGE plpgsql AS $$BEGIN x := x+10; END;$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_procedure_txn: UNSUPPORTED; SQL: `CREATE PROCEDURE r0c_pgp_procedure_txn_p() LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_procedure_txn VALUES(1); COMMIT; INSERT INTO r0c_pgp_procedure_txn VA`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_routine_alter: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_routine_alter(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_defaults: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_defaults(x integer, y integer DEFAULT 10) RETURNS integer LANGUAGE sql AS $f$SELECT x+y$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_definer: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_definer(x integer) RETURNS integer LANGUAGE sql SECURITY DEFINER AS $f$SELECT x+1$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_immutable: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_immutable(x integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $f$SELECT coalesce(x,99)$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_named: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_named(x integer, y integer) RETURNS integer LANGUAGE sql AS $f$SELECT x-y$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_out: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_out(IN x integer, OUT doubled integer, OUT label text) LANGUAGE sql AS $$SELECT x*2, 'ok'::text$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_poly: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_poly(x anyelement) RETURNS anyelement LANGUAGE sql AS $f$SELECT x$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_scalar: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_scalar(x integer) RETURNS integer LANGUAGE sql AS $f$SELECT x * 2$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_search_path: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_search_path() RETURNS text LANGUAGE sql SET search_path = pg_catalog AS $f$SELECT current_setting('search_path')$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_set: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_set(n integer) RETURNS SETOF integer LANGUAGE sql AS $f$SELECT generate_series(1,n)$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_stable: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_stable(x integer) RETURNS integer LANGUAGE sql STABLE AS $f$SELECT coalesce(x,99)$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_strict: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_strict(x integer) RETURNS integer LANGUAGE sql STRICT AS $f$SELECT coalesce(x,99)$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_table: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_table(n integer) RETURNS TABLE(id integer, label text) LANGUAGE sql AS $f$SELECT g, 'item-' || g FROM generate_series(1,n) AS g$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_sql_variadic: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_sql_variadic(VARIADIC xs integer[]) RETURNS integer LANGUAGE sql AS $f$SELECT sum(v)::integer FROM unnest(xs) v$f$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_transform: DIVERGENT; SQL: `CREATE TRANSFORM FOR integer LANGUAGE plpgsql (FROM SQL WITH FUNCTION int4send(integer));`; PostgreSQL 42P17 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:6: ERROR:  42601

- pgp_trigger_after_delete: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_after_delete_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_after_delete_log(op,level) VALUES(TG_OP`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_after_insert: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_after_insert_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_after_insert_log(op,level) VALUES(TG_OP`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_after_update: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_after_update_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_after_update_log(op,level) VALUES(TG_OP`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_before_delete: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_before_delete_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF OLD.v=2 THEN RETURN NULL; END IF; RETURN OLD; END;$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_trigger_before_insert: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_before_insert_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := NEW.v+10; RETURN NEW; END;$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_trigger_before_update: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_before_update_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := OLD.v+NEW.v; RETURN NEW; END;$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_trigger_constraint: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_constraint_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_constraint_log VALUES(NEW.id); RETURN NUL`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_instead: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_instead_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_instead VALUES(NEW.id,NEW.v+1); RETURN NEW; `; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_notify: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_notify_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_notify('r0c_pgp_trigger_notify_channel',NEW.id::text); RETURN `; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_trigger_replica: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_replica_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := NEW.v+1; RETURN NEW; END;$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_trigger_statement: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_statement_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_statement_log(op,level) VALUES(TG_OP,TG_LE`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_timestamp: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_timestamp_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.updated_at := statement_timestamp(); RETURN NEW; END;$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_toggle: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_toggle_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := NEW.v+1; RETURN NEW; END;$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgp_trigger_transition: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_transition_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_transition_log SELECT 1,sum(v) FROM new_r`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_truncate: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_truncate_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_truncate_log(op,level) VALUES(TG_OP,TG_LEVE`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_update_of: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_update_of_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.w := OLD.w+10; RETURN NEW; END;$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:12: ERROR:  42601

- pgp_trigger_when: UNSUPPORTED; SQL: `CREATE FUNCTION r0c_pgp_trigger_when_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := NEW.v+100; RETURN NEW; END;$$;`; PostgreSQL 00000 vs GigglesDB 42601.
  First GigglesDB error: psql:<stdin>:9: ERROR:  42601

- pgt_load: DIVERGENT; SQL: `LOAD 'r0c_pgt_load_missing_library';`; PostgreSQL 58P01 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

- pgt_notify: UNSUPPORTED; SQL: `LISTEN r0c_pgt_notify;`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:6: ERROR:  0A000

### Differential warnings
- pgq_copy_freeze_errors: statement 5: CREATE TABLE t_pgq_copy_freeze_parent does not contain case id

Scratch removed: bin, data, tmp, target, cwd
Scratch bytes freed: 102172916
