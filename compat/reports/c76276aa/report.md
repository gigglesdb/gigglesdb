# GigglesDB compatibility report

Run: r0-20261112T072939Z-f1e6d1
Started: 2026-11-12T07:29:39.576891Z
Finished: 2026-11-12T07:48:47.740160Z

Commit: unknown (unverified-binary)
Server binary SHA-256: 4e1bea2dcea45e15dee52576ba9f93d073808de5b2b5ec4fe2acda825c615c3e
Oracle: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2); digest: sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42; collation: C.UTF-8; TZ: UTC
Clients: pg_dump -> pg_dump (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2); digest: sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42; psql -> psql (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2); digest: sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42; psycopg -> 3.2.9; digest: sha256:31324d1c4c6cf65dba2a3db8a1c745ef56a357bece09738c766bc26a6585eac6; node-pg -> 8.16.3; digest: sha256:5ac0faf6d1349a9c48c444c671f7df4215da0288547c4ba84ca845b70324cfb5; pgx -> 5.7.6; digest: sha256:283855a569dca65709462dd02244eeda75093dec91d7866eb95501f2f88f11a8; npgsql -> 9.0.4; digest: sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f; npgsql-notypes -> 9.0.4; digest: sha256:73d6399cbc165b44d620c5cf708c70a36acb0f59f1fb582c08f1b6bf94118b3f; tokio-postgres -> 0.7.15; digest: sha256:7a09bf2b9a6ffec311b6d011cc64c9a0dd9641ae3a62fb93b35d1a46409fad17; jdbc -> 42.7.3; digest: sha256:31cb4f391f3434796242cc0c12cba70d5a23882d1c640d76e32fc213c4e527e1

Identity:
```json
{
  "harness": {
    "commit": "c76276aa391002cfb64104fb191e6ddd81bb996d",
    "dirty": true
  },
  "build": {
    "commit": null,
    "build_identity": "unverified-binary",
    "binary_sha256": "4e1bea2dcea45e15dee52576ba9f93d073808de5b2b5ec4fe2acda825c615c3e",
    "binary_size": 59454976,
    "stdout_banner": [
      "postgresql=127.0.0.1:56336 preview=single-host",
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
      "tag": "gd-r0-probe-tokio-postgres:54d0b3e60783",
      "reason": "",
      "image_id": "sha256:7a09bf2b9a6ffec311b6d011cc64c9a0dd9641ae3a62fb93b35d1a46409fad17",
      "digest": "sha256:7a09bf2b9a6ffec311b6d011cc64c9a0dd9641ae3a62fb93b35d1a46409fad17",
      "repo_digests": [
        "gd-r0-probe-tokio-postgres@sha256:7a09bf2b9a6ffec311b6d011cc64c9a0dd9641ae3a62fb93b35d1a46409fad17"
      ],
      "expected_version": "0.7.15",
      "image_build_cache": "hit",
      "image_build_duration_s": 0,
      "image": "gd-r0-probe-tokio-postgres:54d0b3e60783",
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
| psql | 15 | 0 | 0 | 0 | 100.0% |
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

## Differential SQL

| Area | Cases | COMPATIBLE | UNSUPPORTED | DIVERGENT | INTENTIONAL | UNTESTED | HARNESS_ERROR | Pass rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Overall | 137 | 134 | 0 | 0 | 3 | 0 | 0 | 100.0% |
| aggregates | 9 | 9 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| alter_table | 8 | 8 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| constraints | 8 | 8 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| datagrip | 36 | 36 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| defaults | 8 | 8 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| dml_predicates | 9 | 9 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| identifiers | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| insert | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| joins | 6 | 6 | 0 | 0 | 0 | 0 | 0 | 100.0% |
| known_gaps | 26 | 23 | 0 | 0 | 3 | 0 | 0 | 100.0% |
| on_conflict | 4 | 4 | 0 | 0 | 0 | 0 | 0 | 100.0% |
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
| datagrip_full_schema_object_acls | G | compare: header | COMPATIBLE | PG [0, 0, 314]; Giggles [0, 0, 246] |
| datagrip_languages | G | compare: header | COMPATIBLE | PG [0, 0, 4]; Giggles [0, 0, 0] |
| datagrip_listcasts | G | compare: header | COMPATIBLE | PG [0, 0, 235]; Giggles [0, 0, 0] |
| datagrip_retrieveaccessmethods | G | compare: header | COMPATIBLE | PG [0, 0, 7]; Giggles [0, 0, 0] |
| datagrip_retrievecolumns | G | compare: header | COMPATIBLE | PG [0, 0, 173]; Giggles [0, 0, 183] |
| datagrip_retrievecolumns_dceb66d8c64d | G | compare: header | COMPATIBLE | PG [0, 0, 173]; Giggles [0, 0, 183] |
| datagrip_retrieveconstraints | G | compare: header | COMPATIBLE | PG [0, 0, 78]; Giggles [0, 0, 81] |
| datagrip_retrieveconstraints_9df0a4e5953e | G | compare: header | COMPATIBLE | PG [0, 0, 78]; Giggles [0, 0, 81] |
| datagrip_retrieveexistentconstrain_644b4beeaf5a | G | compare: header | COMPATIBLE | PG [0, 0, 159]; Giggles [0, 0, 166] |
| datagrip_retrieveexistentconstraints | G | compare: header | COMPATIBLE | PG [0, 0, 159]; Giggles [0, 0, 166] |
| datagrip_retrieveexistenttables | G | compare: header | COMPATIBLE | PG [0, 0, 78]; Giggles [0, 0, 81] |
| datagrip_retrieveexistenttables_38c1901282c3 | G | compare: header | COMPATIBLE | PG [0, 0, 78]; Giggles [0, 0, 81] |
| datagrip_retrieveextensionmembers | G | compare: header | COMPATIBLE | PG [0, 0, 4]; Giggles [0, 0, 0] |
| datagrip_retrieveextensions | G | compare: header | COMPATIBLE | PG [0, 0, 1]; Giggles [0, 0, 0] |
| datagrip_retrieveforeigndatawrappers | G | compare: header | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| datagrip_retrieveforeignservers | G | compare: header | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| datagrip_retrieveindexcolumns | G | compare: header | COMPATIBLE | PG [0, 0, 80]; Giggles [0, 0, 84] |
| datagrip_retrieveindexcolumns_48727062bd45 | G | compare: header | COMPATIBLE | PG [0, 0, 80]; Giggles [0, 0, 84] |
| datagrip_retrieveindices | G | compare: header | COMPATIBLE | PG [0, 0, 80]; Giggles [0, 0, 84] |
| datagrip_retrieveindices_8e659477d09d | G | compare: header | COMPATIBLE | PG [0, 0, 80]; Giggles [0, 0, 84] |
| datagrip_retrieveroles | G | compare: header | COMPATIBLE | PG [0, 0, 17]; Giggles [0, 0, 18] |
| datagrip_retrievetables | G | compare: header | COMPATIBLE | PG [0, 0, 78]; Giggles [0, 0, 81] |
| datagrip_retrievetables_c222cc0a5577 | G | compare: header | COMPATIBLE | PG [0, 0, 78]; Giggles [0, 0, 81] |
| datagrip_retrievetablespaces | G | compare: header | COMPATIBLE | PG [0, 0, 2]; Giggles [0, 0, 2] |
| datagrip_retrievetimezones | G | compare: header | COMPATIBLE | PG [0, 0, 680]; Giggles [0, 0, 0] |
| datagrip_retrieveusermappings | G | compare: header | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| datagrip_role_memberships | G | compare: header | COMPATIBLE | PG [0, 0, 3]; Giggles [0, 0, 0] |
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
| composite_column | E2 | compare: full | INTENTIONAL | PG [0, 0]; Giggles [0, 0] |
| composite_type | E2 | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| copy_to_csv | E2 | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| domain_cast | E2 | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| domain_type | E2 | compare: full | COMPATIBLE | PG [0, 0, 0]; Giggles [0, 0, 0] |
| drop_type_cascade | E2 | compare: full | COMPATIBLE | PG [0, 0]; Giggles [0, 0] |
| enum_type | E2 | compare: full | COMPATIBLE | PG [0, 0, 1, 1, 2]; Giggles [0, 0, 1, 1, 2] |
| explicit_bytea_cast | E2 | compare: full | COMPATIBLE | PG [1]; Giggles [1] |
| global_type_name | E2 | compare: full | INTENTIONAL | PG [0, 0, 0, 0]; Giggles [0, 0, 0, 0] |
| immutable_overflow_error | E2 | compare: full | COMPATIBLE | PG [0]; Giggles [0] |
| information_schema_columns | G | compare: full | COMPATIBLE | PG [0, 2]; Giggles [0, 2] |
| jsonb_column | E2 | compare: full | COMPATIBLE | PG [0, 1, 1, 1]; Giggles [0, 1, 1, 1] |
| lock_mode | E2 | compare: full | COMPATIBLE | PG [0, 0, 0, 0]; Giggles [0, 0, 0, 0] |
| mixed_catalog_app_query | G | compare: full | COMPATIBLE | PG [0, 1]; Giggles [0, 1] |
| multidimensional_array | E2 | compare: full | INTENTIONAL | PG [0, 1]; Giggles [0, 0] |
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

### E2

- composite_column: INTENTIONAL; SQL: `CREATE TABLE r0c_composite_column (id int PRIMARY KEY, v r0c_composite_column_type);`; PostgreSQL 00000 vs GigglesDB 0A000.
  First GigglesDB error: psql:<stdin>:9: ERROR:  0A000

- global_type_name: INTENTIONAL; SQL: `CREATE TYPE r0c_global_type_name_b.r0c_global_type_name AS ENUM ('b');`; PostgreSQL 00000 vs GigglesDB 42710.
  First GigglesDB error: psql:<stdin>:15: ERROR:  42710

- multidimensional_array: INTENTIONAL; SQL: `INSERT INTO r0c_multidimensional_array VALUES (1, '{{1,2},{3,4}}');`; PostgreSQL 00000 vs GigglesDB 22P02.
  First GigglesDB error: psql:<stdin>:9: ERROR:  22P02

Scratch removed: bin, data, tmp, target, cwd
Scratch bytes freed: 59817276
