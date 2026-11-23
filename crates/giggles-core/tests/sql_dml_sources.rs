//! Migration-shaped SQL, checked again after closing and reopening the database.
mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{
        SqlResult, StatementClock, Value, take_sql_diagnostic_hint, take_sql_error_message,
    },
    sqlstate,
};

const CLOCK: StatementClock = StatementClock {
    unavailable: None,
    transaction_us: 850_000_000_000_000,
    statement_us: 850_000_000_000_000,
    zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
    wall: None,
};

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute_at(sql, &[], None, &CLOCK)
        .unwrap_or_else(|error| panic!("{sql}: {error:?} ({:?})", take_sql_error_message()))
}

fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    run(session, sql)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn t(value: &str) -> Value {
    Value::Text(value.into())
}

fn reject(session: &mut SqlSession, sql: &str, code: &str, message: &str, hint: Option<&str>) {
    let error = session.execute_at(sql, &[], None, &CLOCK)
        .expect_err(sql);
    assert_eq!(sqlstate(&error), code, "{sql}");
    assert_eq!(take_sql_error_message().as_deref(), Some(message), "{sql}");
    assert_eq!(take_sql_diagnostic_hint().as_deref(), hint, "{sql}");
}

fn migrate(engine: Engine) {
    let path = common::fresh("sql-dml-sources");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in [
            r#"CREATE TABLE "Organization" (id int PRIMARY KEY, name text)"#,
            r#"INSERT INTO "Organization" VALUES (1,'o1'),(2,'o2')"#,
            r#"CREATE TABLE "Role" (id serial PRIMARY KEY, "organizationId" int, name text, key text, description text, "isSystem" bool, "createdAt" timestamp(3), "updatedAt" timestamp(3))"#,
            r#"CREATE UNIQUE INDEX role_org_key ON "Role" ("organizationId", key)"#,
            r#"CREATE TABLE "RolePermission" (id serial PRIMARY KEY, "roleId" int, permission text)"#,
            r#"CREATE UNIQUE INDEX role_perm_key ON "RolePermission" ("roleId", permission)"#,
            r#"CREATE TABLE "Folder" (id uuid PRIMARY KEY, name text, "createdById" uuid)"#,
            r#"CREATE TABLE "ArchitectureDocs" (id uuid PRIMARY KEY, "folderId" uuid, "createdById" uuid, "createdAt" timestamp(3))"#,
            r#"INSERT INTO "Folder" VALUES ('00000000-0000-0000-0000-000000000001','f1',NULL),('00000000-0000-0000-0000-000000000002','f2',NULL)"#,
            r#"INSERT INTO "ArchitectureDocs" VALUES
                ('00000000-0000-0000-0000-000000000012','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-0000000000b2','2026-01-01'),
                ('00000000-0000-0000-0000-000000000011','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-0000000000b1','2026-01-01')"#,
            r#"CREATE TABLE "DesignDocVariationType" (id int PRIMARY KEY, "presetKey" text, "briefFields" jsonb)"#,
            r#"INSERT INTO "DesignDocVariationType" VALUES (1,'spec','[{"key":"problem"},{"key":"openQuestions","computed":false},{"key":"z"}]'),(2,'plan','[{"key":"openQuestions","computed":false}]')"#,
            r#"CREATE TABLE "LinearOutbox" (id int PRIMARY KEY, "installationId" int, "dedupeKey" text, status text, "createdAt" timestamp(3), "lockedUntil" timestamp(3), "lastError" text, "updatedAt" timestamp(3))"#,
            r#"INSERT INTO "LinearOutbox" VALUES (1,1,'k','PENDING','2026-01-01','2026-05-01',NULL,'2026-01-01'),(2,1,'k','PENDING','2026-01-02','2026-05-01',NULL,'2026-01-01'),(3,2,'k','PENDING','2026-01-01',NULL,NULL,'2026-01-01')"#,
        ] {
            run(&mut s, sql);
        }
        for (values, affected) in [
            (
                "('owner','Owner','Full access'),('viewer','Viewer','Read-only access')",
                4,
            ),
            ("('owner','Owner','x'),('extra','Extra','y')", 2),
        ] {
            assert_eq!(run(&mut s, &format!(r#"
                INSERT INTO "Role" ("organizationId", name, key, description, "isSystem", "createdAt", "updatedAt")
                SELECT o.id, v.name, v.key, v.description, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                FROM "Organization" o CROSS JOIN (VALUES {values}) AS v(key,name,description)
                ON CONFLICT ("organizationId", key) DO NOTHING"#)).affected, affected);
        }
        for values in [
            "('org:manage'),('members:read')",
            "('org:manage'),('audit:read')",
        ] {
            run(
                &mut s,
                &format!(
                    r#"
                INSERT INTO "RolePermission" ("roleId", permission)
                SELECT r.id, p.permission FROM "Role" r CROSS JOIN (VALUES {values}) AS p(permission)
                WHERE r."isSystem" = true AND r.key = 'owner'
                ON CONFLICT ("roleId", permission) DO NOTHING"#
                ),
            );
        }
        assert_eq!(
            run(
                &mut s,
                r#"
            UPDATE "Folder" f SET "createdById" = (
                SELECT d."createdById" FROM "ArchitectureDocs" d WHERE d."folderId" = f.id
                ORDER BY d."createdAt", d.id LIMIT 1
            ) WHERE f."createdById" IS NULL"#
            )
            .affected,
            2
        );
        assert_eq!(run(&mut s, r#"
            UPDATE "DesignDocVariationType" AS t SET "briefFields" = (
                SELECT jsonb_agg(CASE WHEN f = '{"key":"openQuestions","computed":false}'::jsonb
                    THEN jsonb_set(f, '{computed}', 'true'::jsonb) ELSE f END ORDER BY ordinal)
                FROM jsonb_array_elements(t."briefFields") WITH ORDINALITY AS fields(f, ordinal)
            ) WHERE "presetKey" = 'spec' AND "briefFields" @> '[{"key":"openQuestions","computed":false}]'::jsonb
        "#).affected, 1);
        assert_eq!(run(&mut s, r#"
            WITH duplicates AS (
                SELECT id, row_number() OVER (PARTITION BY "installationId", "dedupeKey" ORDER BY "createdAt", id) AS n
                FROM "LinearOutbox" WHERE status = 'PENDING' AND "dedupeKey" IS NOT NULL
            )
            UPDATE "LinearOutbox" SET status = 'DONE', "lockedUntil" = NULL,
                "lastError" = 'Duplicate pending link job', "updatedAt" = now()
            WHERE id IN (SELECT id FROM duplicates WHERE n > 1)
        "#).affected, 1);
    }
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT p.permission,count(*) FROM "RolePermission" p
        JOIN "Role" r ON p."roleId"=r.id GROUP BY p.permission ORDER BY 1"#
        ),
        ["audit:read", "members:read", "org:manage"]
            .into_iter()
            .map(|p| vec![t(p), Value::Int(2)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT name,"createdById" FROM "Folder" ORDER BY name"#
        ),
        vec![
            vec![t("f1"), t("00000000-0000-0000-0000-0000000000b1")],
            vec![t("f2"), Value::Null]
        ]
    );
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT "presetKey","briefFields" FROM "DesignDocVariationType" ORDER BY 1"#
        ),
        vec![
            vec![
                t("plan"),
                t(r#"[{"key": "openQuestions", "computed": false}]"#)
            ],
            vec![
                t("spec"),
                t(
                    r#"[{"key": "problem"}, {"key": "openQuestions", "computed": true}, {"key": "z"}]"#
                )
            ],
        ]
    );
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT id,status,"lockedUntil" IS NULL,"lastError" FROM "LinearOutbox" ORDER BY id"#
        ),
        vec![
            vec![Value::Int(1), t("PENDING"), Value::Bool(false), Value::Null],
            vec![
                Value::Int(2),
                t("DONE"),
                Value::Bool(true),
                t("Duplicate pending link job")
            ],
            vec![Value::Int(3), t("PENDING"), Value::Bool(true), Value::Null],
        ]
    );
    drop(s);
    drop(admin);
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

fn archdocs_role_seed(engine: Engine) {
    let path = common::fresh("sql-dml-sources-archdocs-role-seed");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in [
            r#"CREATE TABLE "Organization" ("id" UUID PRIMARY KEY DEFAULT gen_random_uuid(), "ownerId" UUID, "name" TEXT);"#,
            r#"CREATE TABLE "Role" ("id" UUID NOT NULL DEFAULT gen_random_uuid(), "organizationId" UUID NOT NULL, "name" TEXT NOT NULL, "key" TEXT NOT NULL, "description" TEXT, "isSystem" BOOLEAN NOT NULL DEFAULT false, "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP, "updatedAt" TIMESTAMP(3) NOT NULL, CONSTRAINT "Role_pkey" PRIMARY KEY ("id"));"#,
            r#"CREATE UNIQUE INDEX "Role_organizationId_key_key" ON "Role"("organizationId", "key");"#,
            r#"CREATE TABLE "RolePermission" ("id" UUID NOT NULL DEFAULT gen_random_uuid(), "roleId" UUID NOT NULL, "permission" TEXT NOT NULL, CONSTRAINT "RolePermission_pkey" PRIMARY KEY ("id"));"#,
            r#"CREATE UNIQUE INDEX "RolePermission_roleId_permission_key" ON "RolePermission"("roleId", "permission");"#,
            r#"INSERT INTO "Organization" ("id","ownerId","name") VALUES ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-0000000000a1','o1'),('00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-0000000000a2','o2');"#,
        ] {
            run(&mut s, sql);
        }
        // The five INSERT statements from migration 20260530000002_member_role_required.
        let seed = [
            r#"INSERT INTO "Role" ("id", "organizationId", "name", "key", "description", "isSystem", "createdAt", "updatedAt")
SELECT gen_random_uuid(), o."id", v.name, v.key, v.description, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM "Organization" o
CROSS JOIN (VALUES
  ('owner', 'Owner', 'Full access including billing, deletion, and transfer'),
  ('admin', 'Admin', 'Manage everything except organization deletion/transfer'),
  ('editor', 'Editor', 'Read everything; create and edit docs, spaces, icons, and objects'),
  ('viewer', 'Viewer', 'Read-only access')
) AS v(key, name, description)
ON CONFLICT ("organizationId", "key") DO NOTHING;"#,
            r#"INSERT INTO "RolePermission" ("id", "roleId", "permission")
SELECT gen_random_uuid(), r."id", p.permission
FROM "Role" r
CROSS JOIN (VALUES
  ('org:manage'),('org:delete'),('org:transfer'),('members:read'),('members:manage'),
  ('roles:read'),('roles:manage'),('billing:read'),('billing:manage'),('spaces:read'),
  ('spaces:manage'),('teams:read'),('teams:manage'),('docs:read'),('docs:write'),
  ('docs:delete'),('icons:read'),('icons:manage'),('objects:read'),('objects:manage'),
  ('cloud:read'),('cloud:manage'),('audit:read')
) AS p(permission)
WHERE r."isSystem" = true AND r."key" = 'owner'
ON CONFLICT ("roleId", "permission") DO NOTHING;"#,
            r#"INSERT INTO "RolePermission" ("id", "roleId", "permission")
SELECT gen_random_uuid(), r."id", p.permission
FROM "Role" r
CROSS JOIN (VALUES
  ('org:manage'),('members:read'),('members:manage'),('roles:read'),('roles:manage'),
  ('billing:read'),('billing:manage'),('spaces:read'),('spaces:manage'),('teams:read'),
  ('teams:manage'),('docs:read'),('docs:write'),('docs:delete'),('icons:read'),
  ('icons:manage'),('objects:read'),('objects:manage'),('cloud:read'),('cloud:manage'),
  ('audit:read')
) AS p(permission)
WHERE r."isSystem" = true AND r."key" = 'admin'
ON CONFLICT ("roleId", "permission") DO NOTHING;"#,
            r#"INSERT INTO "RolePermission" ("id", "roleId", "permission")
SELECT gen_random_uuid(), r."id", p.permission
FROM "Role" r
CROSS JOIN (VALUES
  ('members:read'),('roles:read'),('billing:read'),('spaces:read'),('teams:read'),
  ('docs:read'),('icons:read'),('objects:read'),('cloud:read'),('audit:read'),
  ('docs:write'),('docs:delete'),('spaces:manage'),('icons:manage'),('objects:manage'),
  ('cloud:manage')
) AS p(permission)
WHERE r."isSystem" = true AND r."key" = 'editor'
ON CONFLICT ("roleId", "permission") DO NOTHING;"#,
            r#"INSERT INTO "RolePermission" ("id", "roleId", "permission")
SELECT gen_random_uuid(), r."id", p.permission
FROM "Role" r
CROSS JOIN (VALUES
  ('members:read'),('roles:read'),('billing:read'),('spaces:read'),('teams:read'),
  ('docs:read'),('icons:read'),('objects:read'),('cloud:read'),('audit:read')
) AS p(permission)
WHERE r."isSystem" = true AND r."key" = 'viewer'
ON CONFLICT ("roleId", "permission") DO NOTHING;"#,
        ];
        for expected in [[8, 46, 42, 32, 20], [0; 5]] {
            for (sql, affected) in seed.into_iter().zip(expected) {
                assert_eq!(run(&mut s, sql).affected, affected, "{sql}");
            }
        }
    }
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();

    // PostgreSQL 18.6 readback for the two organizations in oracle/p1.sql.
    let roles = [
        ("admin", "Admin", "Manage everything except organization deletion/transfer"),
        ("editor", "Editor", "Read everything; create and edit docs, spaces, icons, and objects"),
        ("owner", "Owner", "Full access including billing, deletion, and transfer"),
        ("viewer", "Viewer", "Read-only access"),
    ];
    let permissions = [
        ("admin", "audit:read billing:manage billing:read cloud:manage cloud:read docs:delete \
            docs:read docs:write icons:manage icons:read members:manage members:read \
            objects:manage objects:read org:manage roles:manage roles:read spaces:manage \
            spaces:read teams:manage teams:read"),
        ("editor", "audit:read billing:read cloud:manage cloud:read docs:delete docs:read docs:write \
            icons:manage icons:read members:read objects:manage objects:read roles:read \
            spaces:manage spaces:read teams:read"),
        ("owner", "audit:read billing:manage billing:read cloud:manage cloud:read docs:delete \
            docs:read docs:write icons:manage icons:read members:manage members:read \
            objects:manage objects:read org:delete org:manage org:transfer roles:manage \
            roles:read spaces:manage spaces:read teams:manage teams:read"),
        ("viewer", "audit:read billing:read cloud:read docs:read icons:read members:read objects:read \
            roles:read spaces:read teams:read"),
    ];
    let organizations = ["o1", "o2"];
    assert_eq!(
        rows(&mut s, r#"SELECT o.name, r.key, r.name, r.description, r."isSystem"
            FROM "Role" r JOIN "Organization" o ON o.id = r."organizationId" ORDER BY 1, 2"#),
        organizations.into_iter().flat_map(|organization| {
            roles.map(|(key, name, description)| {
                vec![t(organization), t(key), t(name), t(description), Value::Bool(true)]
            })
        }).collect::<Vec<_>>()
    );
    assert_eq!(
        rows(&mut s, r#"SELECT o.name, r.key, p.permission
            FROM "RolePermission" p JOIN "Role" r ON r.id = p."roleId"
            JOIN "Organization" o ON o.id = r."organizationId" ORDER BY 1, 2, 3"#),
        organizations.into_iter().flat_map(|organization| {
            permissions.into_iter().flat_map(move |(key, permissions)| {
                permissions.split_ascii_whitespace().map(move |permission| {
                    vec![t(organization), t(key), t(permission)]
                })
            })
        }).collect::<Vec<_>>()
    );
    drop(s);
    drop(admin);
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn archdocs_role_seed_legacy() {
    archdocs_role_seed(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn archdocs_role_seed_v2() {
    archdocs_role_seed(Engine::V2);
}

fn expressions(engine: Engine) {
    let path = common::fresh("sql-dml-source-expressions");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    run(
        &mut s,
        "CREATE TABLE sink (id int PRIMARY KEY, label text, amount numeric)",
    );
    assert_eq!(
        rows(
            &mut s,
            "INSERT INTO sink SELECT v.i,v.column2,v.column3 FROM (VALUES (1,'a',1),(2,'b',2.5)) v(i) RETURNING label,amount::text"
        ),
        vec![vec![t("a"), t("1")], vec![t("b"), t("2.5")]]
    );
    assert_eq!(
        rows(
            &mut s,
            "INSERT INTO sink (id,label) SELECT 3,column1 FROM (VALUES (NULL)) v RETURNING label"
        ),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        rows(
            &mut s,
            "UPDATE sink SET label = (SELECT s.label FROM sink s WHERE s.id < sink.id ORDER BY s.id DESC LIMIT 1 OFFSET 1) WHERE id = (SELECT max(id) FROM sink) RETURNING label"
        ),
        vec![vec![t("a")]]
    );
    assert_eq!(
        run(
            &mut s,
            "WITH ignored AS (SELECT 1 / 0) UPDATE sink SET label=label WHERE false"
        )
        .affected,
        0
    );
    assert_eq!(
        rows(
            &mut s,
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) UPDATE sink SET label='changed' WHERE id IN (SELECT n FROM r WHERE n=2) RETURNING id,label"
        ),
        vec![vec![Value::Int(2), t("changed")]]
    );
    assert_eq!(
        rows(
            &mut s,
            "WITH d AS NOT MATERIALIZED (SELECT 4 AS id) INSERT INTO sink(id,label) SELECT id,'X' || id FROM d RETURNING label"
        ),
        vec![vec![t("X4")]]
    );
    assert_eq!(
        rows(
            &mut s,
            "WITH d AS MATERIALIZED (SELECT id FROM sink WHERE label='changed') DELETE FROM sink WHERE id IN (SELECT id FROM d) RETURNING id"
        ),
        vec![vec![Value::Int(2)]]
    );
    reject(
        &mut s,
        "WITH d AS (SELECT id, row_number() OVER (ORDER BY id) AS n FROM sink) UPDATE sink SET label=label || n WHERE id IN (SELECT id FROM d WHERE n > 2)",
        "42703",
        r#"column "n" does not exist"#,
        None,
    );
    reject(
        &mut s,
        "UPDATE sink SET label = (SELECT label FROM sink)",
        "21000",
        "more than one row returned by a subquery used as an expression",
        None,
    );
    reject(
        &mut s,
        "SELECT * FROM (VALUES (1,2), (3)) AS v(a,b)",
        "42601",
        "VALUES lists must all be the same length",
        None,
    );
    reject(
        &mut s,
        "SELECT * FROM (VALUES (1), ('x')) AS v(n)",
        "22P02",
        r#"invalid input syntax for type integer: "x""#,
        None,
    );
    reject(
        &mut s,
        "SELECT * FROM (VALUES (1,2)) AS v(a,b,c)",
        "42P10",
        r#"table "v" has 2 columns available but 3 columns specified"#,
        None,
    );
    reject(
        &mut s,
        "UPDATE sink s SET s.label='x' WHERE false",
        "42703",
        r#"column "s" of relation "sink" does not exist"#,
        Some("SET target columns cannot be qualified with the relation name."),
    );
    reject(
        &mut s,
        "UPDATE sink s SET label=sink.label WHERE false",
        "42P01",
        r#"invalid reference to FROM-clause entry for table "sink""#,
        Some(r#"Perhaps you meant to reference the table alias "s"."#),
    );

    assert_eq!(
        rows(
            &mut s,
            "SELECT json_agg(DISTINCT x ORDER BY x DESC) FILTER (WHERE x > 1), jsonb_agg(x ORDER BY x DESC) FROM (VALUES (1),(2),(3),(3),(NULL)) v(x)"
        ),
        vec![vec![t("[3, 2]"), t("[null, 3, 3, 2, 1]")]]
    );
    drop(s);
    drop(admin);
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn legacy() {
    migrate(Engine::Legacy);
    expressions(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2() {
    migrate(Engine::V2);
    expressions(Engine::V2);
}

fn review_json(session: &mut SqlSession) {
    assert_eq!(rows(session,
        "SELECT pg_typeof(json_agg(1)), pg_typeof((SELECT json_agg(1))), pg_typeof(jsonb_agg(1))"),
        [vec![t("json"), t("json"), t("jsonb")]]);
    assert_eq!(rows(session,
        "SELECT pg_typeof(CASE WHEN true THEN json_agg(1) ELSE NULL END),
                pg_typeof(COALESCE(json_agg(1),NULL))"),
        [vec![t("json"), t("json")]]);
    assert_eq!(rows(session, "SELECT json_agg(1) UNION ALL SELECT json_agg(2)"), [vec![t("[1]")], vec![t("[2]")]]);
    reject(session, "SELECT json_agg(1) UNION SELECT json_agg(2)", "42883",
        "could not identify an equality operator for type json", None);
    let sql = "SELECT json_agg(x ORDER BY id), jsonb_agg(x ORDER BY id)
               FROM (VALUES (1,ARRAY[1,2]),(2,NULL),(3,'{}'::int[])) v(id,x)";
    assert_eq!(rows(session, sql), [vec![t("[[1,2], null, \n []]"), t("[[1, 2], null, []]")]]);
    assert_eq!(
        rows(session, r#"SELECT json_agg(DISTINCT t), jsonb_agg(DISTINCT j)
            FROM (VALUES ('c', '{"k":1}'::jsonb), ('a', 'null'::jsonb), (NULL, NULL), ('c', NULL)) v(t,j)"#),
        [vec![t(r#"["a", "c", null]"#), t(r#"[null, {"k": 1}, null]"#)]]
    );
    assert_eq!(
        rows(session, "SELECT json_agg(x), jsonb_agg(x)
            FROM (VALUES ('2026-01-02 03:04:05.5'::timestamp)) v(x)"),
        [vec![t(r#"["2026-01-02T03:04:05.5"]"#); 2]]
    );
    let clock = StatementClock {
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(19_800)),
        ..CLOCK
    };
    let result = session.execute_at(
        "SELECT json_agg('2026-01-02 03:04:05.123456+05:30'::timestamptz)", &[], None, &clock,
    ).unwrap();
    assert_eq!(result.rows[0].values, [t(r#"["2026-01-02T03:04:05.123456+05:30"]"#)]);
    assert_eq!(
        rows(session, "SELECT json_object_agg(k,v ORDER BY v), jsonb_object_agg(k,v ORDER BY v)
            FROM (VALUES ('a',1),('a',2),('bbb',NULL),('b',3)) s(k,v)"),
        [vec![t(r#"{ "a" : 1, "a" : 2, "b" : 3, "bbb" : null }"#), t(r#"{"a": 2, "b": 3, "bbb": null}"#)]]
    );
    assert_eq!(
        rows(session, "SELECT json_object_agg(DISTINCT k,v ORDER BY v,k) FILTER (WHERE v > 1),
            jsonb_object_agg(DISTINCT k,v ORDER BY v,k)
            FROM (VALUES ('b',1),('a',2),('a',2)) s(k,v)"),
        [vec![t(r#"{ "a" : 2 }"#), t(r#"{"a": 2, "b": 1}"#)]]
    );
    assert_eq!(rows(session,
        "SELECT json_object_agg(DISTINCT k,v ORDER BY k) FROM (VALUES ('a',3),('a',1),('a',3)) s(k,v)"),
        [vec![t(r#"{ "a" : 1, "a" : 3 }"#)]]);
    reject(session,
        "SELECT json_agg(DISTINCT x ORDER BY y) FROM (VALUES (1,2)) v(x,y)",
        "42P10", "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list", None);
    reject(session, "SELECT json_object_agg(NULL::integer,1)", "22004", "null value not allowed for object key", None);
    reject(session, "SELECT jsonb_object_agg(NULL::integer,1)", "22023", "field name must not be null", None);
    assert_eq!(
        rows(session, r#"SELECT jsonb_set('[1,2]', '{" 1"}', '9'), jsonb_set('[1,2]', '{+1}', '9')"#),
        [vec![t("[1, 9]"); 2]]
    );
    for (sql, columns) in [
        ("SELECT * FROM jsonb_array_elements('[1]')", &["value"][..]),
        ("SELECT e.value FROM jsonb_array_elements_text('[1]') e", &["value"][..]),
        ("SELECT * FROM jsonb_array_elements('[1]') WITH ORDINALITY AS e(x)", &["x", "ordinality"][..]),
        ("SELECT * FROM jsonb_each('{\"a\":1}') WITH ORDINALITY AS e", &["key", "value", "ordinality"][..]),
        ("SELECT * FROM jsonb_each_text('{\"a\":1}') AS e(k)", &["k", "value"][..]),
        ("SELECT * FROM jsonb_object_keys('{\"a\":1}')", &["jsonb_object_keys"][..]),
        ("SELECT * FROM jsonb_object_keys('{\"a\":1}') WITH ORDINALITY AS e", &["e", "ordinality"][..]),
    ] {
        assert_eq!(run(session, sql).columns, columns, "{sql}");
    }
}

fn review_subqueries(session: &mut SqlSession) {
    for sql in [
        "CREATE TABLE reviewed (id int PRIMARY KEY, label text, j jsonb)",
        r#"INSERT INTO reviewed VALUES (1,'ok','[1,2,3]'),(2,'obj','{}'),(3,'ok','[4]')"#,
        "CREATE TABLE matches (id int PRIMARY KEY, fk int, n int)",
        "CREATE INDEX matches_fk ON matches(fk)",
        "INSERT INTO matches VALUES (1,1,9),(2,1,8),(3,3,7)",
    ] {
        run(session, sql);
    }
    assert_eq!(rows(session,
        "SELECT r.id, (SELECT count(*) FROM jsonb_array_elements(r.j))
         FROM reviewed r WHERE r.label='ok' ORDER BY r.id"),
        [vec![Value::Int(1), Value::Int(3)], vec![Value::Int(3), Value::Int(1)]]);
    assert_eq!(rows(session,
        "SELECT r.id, (SELECT count(*) FROM jsonb_array_elements(r.j))
         FROM reviewed r WHERE jsonb_typeof(r.j)='array' ORDER BY r.id"),
        [vec![Value::Int(1), Value::Int(3)], vec![Value::Int(3), Value::Int(1)]]);
    assert_eq!(rows(session,
        "SELECT r.id, CASE WHEN r.label='ok' THEN (SELECT 10/(r.id-2)) END
         FROM reviewed r ORDER BY r.id"),
        [vec![Value::Int(1), Value::Int(-10)], vec![Value::Int(2), Value::Null],
         vec![Value::Int(3), Value::Int(10)]]);
    assert_eq!(rows(session,
        "UPDATE reviewed r SET label='big'
         WHERE (SELECT count(*) FROM jsonb_array_elements(r.j))>2 AND r.label='ok' RETURNING id"),
        [vec![Value::Int(1)]]);
    assert_eq!(rows(session,
        "SELECT r.id,(SELECT n FROM matches m WHERE r.id=m.fk ORDER BY n LIMIT 1)
         FROM reviewed r ORDER BY r.id"),
        [vec![Value::Int(1), Value::Int(8)], vec![Value::Int(2), Value::Null],
         vec![Value::Int(3), Value::Int(7)]]);
    assert_eq!(rows(session,
        "UPDATE reviewed r SET label=(SELECT n FROM matches m WHERE m.fk=r.id ORDER BY n LIMIT 1)
         WHERE (SELECT count(*) FROM matches m WHERE m.fk=r.id) > 0 RETURNING id,label"),
        [vec![Value::Int(1), t("8")], vec![Value::Int(3), t("7")]]);
    assert_eq!(rows(session,
        "UPDATE reviewed SET (label)=(SELECT (SELECT 'ignored') FROM reviewed WHERE false)
         WHERE id=2 RETURNING label"),
        [vec![Value::Null]]);
    reject(session, "UPDATE reviewed SET label.x='a'", "42804",
        r#"cannot assign to field "x" of column "label" because its type text is not a composite type"#, None);
    assert_eq!(run(session, "UPDATE reviewed r SET label='hit'
        WHERE id IN (SELECT reviewed.id FROM reviewed)").affected, 3);
    for source in ["VALUES (1,'x'),(1,'y')", "SELECT 1,'x' UNION ALL SELECT 1,'y'"] {
        reject(session, &format!("INSERT INTO reviewed(id,label) {source}
                ON CONFLICT (id) DO UPDATE SET label=excluded.label"), "21000",
            "ON CONFLICT DO UPDATE command cannot affect row a second time",
            Some("Ensure that no rows proposed for insertion within the same command have duplicate constrained values."));
    }
}

fn review_bulk_update(session: &mut SqlSession) {
    run(session, "CREATE TABLE bulk_target (id int PRIMARY KEY, n int)");
    run(session, "CREATE TABLE bulk_values (id int PRIMARY KEY, n int)");
    run(session, "INSERT INTO bulk_values VALUES (1,9),(2,8)");
    for range in [1..601, 601..1026] {
        let values = range.map(|id| format!("({id},NULL)")).collect::<Vec<_>>().join(",");
        run(session, &format!("INSERT INTO bulk_target VALUES {values}"));
    }
    assert_eq!(run(session, "UPDATE bulk_target t SET n=
        (SELECT n FROM bulk_values v WHERE v.id=t.id)").affected, 1025);
    assert_eq!(rows(session, "SELECT count(*),sum(n) FROM bulk_target"),
        [vec![Value::Int(1025), Value::Int(17)]]);
}

fn review_fixes(engine: Engine) {
    let path = common::fresh("sql-dml-review-fixes");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    review_json(&mut session);
    review_subqueries(&mut session);
    review_bulk_update(&mut session);
    drop(session);
    drop(admin);
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn f1_legacy() {
    review_fixes(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn f1_v2() {
    review_fixes(Engine::V2);
}

fn unique_cte_mutations(engine: Engine) {
    use giggles_core::relational::take_sql_error_detail;

    let path = common::fresh("cte-unique-mutations");
    let expected = [
        vec![t("a"), t("xx#1")],
        vec![t("a"), t("y")],
        vec![t("b"), t("xx#1")],
    ];
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for (table, key_type, keys) in [
            ("indexed_int", "int", ["1", "2", "3"]),
            (
                "indexed_uuid",
                "uuid",
                [
                    "'00000000-0000-0000-0000-000000000001'",
                    "'00000000-0000-0000-0000-000000000002'",
                    "'00000000-0000-0000-0000-000000000003'",
                ],
            ),
        ] {
            let [first, second, third] = keys;
            run(
                &mut s,
                &format!(
                    "CREATE TABLE {table}(id {key_type} PRIMARY KEY, grp text NOT NULL, k text NOT NULL)"
                ),
            );
            let index = format!("{table}_gk");
            run(
                &mut s,
                &format!("CREATE UNIQUE INDEX {index} ON {table}(grp,k)"),
            );
            run(
                &mut s,
                &format!(
                    "INSERT INTO {table} VALUES ({first},'a','x'),({second},'b','x'),({third},'a','y')"
                ),
            );
            assert_eq!(
                run(
                    &mut s,
                    &format!(
                        "WITH d AS (SELECT id FROM {table} WHERE k='x')
                     UPDATE {table} SET k='xx' WHERE id IN (SELECT id FROM d)"
                    )
                )
                .affected,
                2
            );
            let updated = rows(&mut s, &format!(
                "WITH d AS (SELECT id,row_number() OVER (PARTITION BY grp ORDER BY k) AS n FROM {table})
                 UPDATE {table} t SET k=t.k || '#' || d.n FROM d
                 WHERE d.id=t.id AND d.n=1 RETURNING t.grp,t.k"
            ));
            assert_eq!(updated.len(), 2);
            assert!(updated.contains(&expected[0]));
            assert!(updated.contains(&expected[2]));

            for (sql, duplicate) in [
                (
                    format!(
                        "WITH d AS (SELECT id FROM {table} WHERE grp='a' AND k='xx#1')
                             UPDATE {table} SET k='y' WHERE id IN (SELECT id FROM d)"
                    ),
                    Some("y"),
                ),
                (
                    format!(
                        "WITH d AS (SELECT id,k FROM {table} WHERE grp='a')
                             UPDATE {table} t SET k=CASE WHEN d.k='y' THEN 'xx#1' ELSE 'y' END
                             FROM d WHERE t.id=d.id"
                    ),
                    None,
                ),
            ] {
                let error = s.execute(&sql, &[], None).expect_err(&sql);
                assert_eq!(sqlstate(&error), "23505", "{sql}");
                let message = take_sql_error_message();
                let detail = take_sql_error_detail();
                if key_type == "uuid"
                    && let Some(duplicate) = duplicate
                {
                    assert_eq!(
                        message.unwrap(),
                        format!("duplicate key value violates unique constraint \"{index}\"")
                    );
                    assert_eq!(
                        detail.unwrap(),
                        format!("Key (grp, k)=(a, {duplicate}) already exists.")
                    );
                }
            }
            assert_eq!(
                rows(&mut s, &format!("SELECT grp,k FROM {table} ORDER BY 1,2")),
                expected
            );
            assert_eq!(
                rows(
                    &mut s,
                    &format!(
                        "WITH d AS (SELECT id FROM {table} WHERE k='y')
                     DELETE FROM {table} WHERE id IN (SELECT id FROM d) RETURNING grp,k"
                    )
                ),
                [expected[1].clone()]
            );
            assert_eq!(
                rows(
                    &mut s,
                    &format!(
                        "WITH d AS (SELECT grp FROM {table} WHERE grp='a' AND k='xx#1')
                     INSERT INTO {table} SELECT {third},d.grp,'y' FROM d RETURNING grp,k"
                    )
                ),
                [expected[1].clone()]
            );
        }
    }
    {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        for table in ["indexed_int", "indexed_uuid"] {
            assert_eq!(
                rows(&mut s, &format!("SELECT grp,k FROM {table} ORDER BY 1,2")),
                expected
            );
            assert_eq!(
                rows(
                    &mut s,
                    &format!("SELECT grp,k FROM {table} WHERE grp='a' AND k='xx#1'")
                ),
                [expected[0].clone()]
            );
        }
    }
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn f2_legacy() {
    unique_cte_mutations(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn f2_v2() {
    unique_cte_mutations(Engine::V2);
}

// These pairs exercise the first conjunct of a correlated scan. They also
// cover representations whose equality cannot be used as a lookup key.
const CORRELATION_CASTS: &[(&str, &str, &str, &str)] = &[
    ("smallint", "smallint", "1", "' 1 '::smallint"),
    ("integer", "int", "1", "1.4::int"),
    ("bigint", "bigint", "1", "1.0::bigint"),
    ("boolean", "bool", "true", "'yes'::boolean"),
    ("numeric", "numeric", "1", "'1.00'::numeric"),
    ("real", "real", "'-0'", "'0'::real"),
    (
        "double",
        "double precision",
        "'NaN'",
        "'NaN'::double precision",
    ),
    ("text_c", r#"text COLLATE "C""#, "'AbC'", "'AbC'::text"),
    ("text_icu", "text COLLATE f3_ci", "'AbC'", "'abc'::text"),
    ("varchar", "varchar(8)", "'check'", "'checked'::varchar(5)"),
    ("character", "char(5)", "'AB'", "'AB'::char(2)"),
    ("date", "date", "'2026-01-02'", "'20260102'::date"),
    ("time", "time", "'03:04:05'", "'03:04:05.000'::time"),
    (
        "timetz",
        "timetz",
        "'03:04:05+02'",
        "'03:04:05+02:00'::timetz",
    ),
    (
        "timestamp",
        "timestamp",
        "'2026-01-02 03:04:05'",
        "'2026-01-02 03:04:05+05'::timestamp",
    ),
    (
        "timestamptz",
        "timestamptz",
        "'2026-01-02 03:04:05+00'",
        "'2026-01-01 22:04:05-05'::timestamptz",
    ),
    ("interval", "interval", "'1 day'", "'24 hours'::interval"),
    ("bytea", "bytea", r"'\x4100'", r"'A\000'::bytea"),
    (
        "jsonb",
        "jsonb",
        r#"'{"a":1,"b":2}'"#,
        r#"'{"b":2, "a":1}'::jsonb"#,
    ),
    ("int_array", "int[]", "'{1,2}'", "'{01, 2}'::int[]"),
    ("bigint_array", "bigint[]", "'{1,2}'", "'{01, 2}'::bigint[]"),
    ("text_array", "text[]", "'{a,b}'", r#"'{"a", b}'::text[]"#),
    (
        "uuid_array",
        "uuid[]",
        "'{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'",
        "'{A0EEBC999C0B4EF8BB6D6BB9BD380A11}'::uuid[]",
    ),
];

fn correlation_casts(engine: Engine) {
    let path = common::fresh("correlation-literal-casts");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        run(
            &mut s,
            "CREATE TABLE f3_o (id int PRIMARY KEY, k int, n int)",
        );
        run(&mut s, "INSERT INTO f3_o VALUES (1,1,0),(2,2,0)");
        run(
            &mut s,
            "CREATE TABLE f3_u (id int PRIMARY KEY, f int, u uuid)",
        );
        run(
            &mut s,
            "INSERT INTO f3_u VALUES
            (1,1,'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
            (2,2,'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
            (3,1,'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')",
        );
        let expected = [
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(1)],
        ];
        for indexed in [false, true] {
            if indexed {
                run(&mut s, "CREATE INDEX f3_u_uuid ON f3_u (u)");
            }
            for literal in [
                "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
                "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
                "a0eebc999c0b4ef8bb6d6bb9bd380a11",
            ] {
                let subquery = format!(
                    "(SELECT count(*) FROM f3_u i WHERE i.u = '{literal}'::uuid AND i.f = o.k)"
                );
                let select = format!("SELECT o.id, {subquery} FROM f3_o o ORDER BY o.id");
                assert_eq!(
                    rows(&mut s, &select),
                    expected,
                    "{select} (index={indexed})"
                );
                // Clear the result so a missed update cannot pass on the previous case's rows.
                run(&mut s, "UPDATE f3_o SET n = 0");
                let update = format!("UPDATE f3_o o SET n = {subquery}");
                assert_eq!(run(&mut s, &update).affected, 2);
                assert_eq!(
                    rows(&mut s, "SELECT id,n FROM f3_o ORDER BY id"),
                    expected,
                    "{update} (index={indexed})"
                );
            }
        }

        run(
            &mut s,
            "CREATE COLLATION f3_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        );
        for &(name, ty, stored, literal) in CORRELATION_CASTS {
            run(
                &mut s,
                &format!("CREATE TABLE f3_{name} (id int PRIMARY KEY, f int, v {ty})"),
            );
            run(
                &mut s,
                &format!("INSERT INTO f3_{name} VALUES (1,1,{stored}),(2,2,NULL)"),
            );
            let select = format!(
                "SELECT o.id, (SELECT count(*) FROM f3_{name} i WHERE i.v = {literal} AND i.f = o.k) FROM f3_o o ORDER BY o.id"
            );
            if name == "character" {
                // Both engines currently reject direct bpchar comparisons.
                let error = s.execute_at(&select, &[], None, &CLOCK).expect_err(&select);
                assert_eq!(sqlstate(&error), "0A000", "{select}");
                continue;
            }
            assert_eq!(
                rows(&mut s, &select),
                [
                    vec![Value::Int(1), Value::Int(1)],
                    vec![Value::Int(2), Value::Int(0)]
                ],
                "{select}"
            );
        }
        assert_eq!(
            rows(
                &mut s,
                "SELECT o.id, (SELECT count(*) FROM f3_u i WHERE 'int'::regtype = 'integer'::regtype AND i.f = o.k) FROM f3_o o ORDER BY o.id"
            ),
            [
                vec![Value::Int(1), Value::Int(2)],
                vec![Value::Int(2), Value::Int(1)]
            ]
        );
    }
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn f3_correlation_legacy() {
    correlation_casts(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn f3_correlation_v2() {
    correlation_casts(Engine::V2);
}

fn json_assignments_and_conditionals(engine: Engine) {
    let path = common::fresh("json-assignment-and-conditionals");
    {
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        for sql in [
            "CREATE TABLE f3_j_src (id int PRIMARY KEY, g int, t text)",
            "INSERT INTO f3_j_src VALUES (1,1,'a'), (2,1,'b'), (3,2,'c')",
            "CREATE TABLE f3_j_dst (id int PRIMARY KEY, jb jsonb, txt text, n int)",
            "INSERT INTO f3_j_dst (id) VALUES (1),(2),(3)",
            "UPDATE f3_j_dst d SET jb = (SELECT json_agg(s.t ORDER BY s.id) FROM f3_j_src s WHERE s.g = d.id),
                txt = (SELECT json_agg(s.t ORDER BY s.id) FROM f3_j_src s WHERE s.g = d.id)",
            "INSERT INTO f3_j_dst (id,jb,txt) SELECT 4,json_agg(t ORDER BY id),json_agg(t ORDER BY id) FROM f3_j_src",
            "INSERT INTO f3_j_dst (id,jb) SELECT 5,json_agg(v.t) FROM (VALUES ('z')) v(t)",
        ] {
            run(&mut s, sql);
        }
        assert_eq!(
            rows(&mut s, "SELECT id,jb,txt FROM f3_j_dst ORDER BY id"),
            [
                vec![Value::Int(1), t(r#"["a", "b"]"#), t(r#"["a", "b"]"#)],
                vec![Value::Int(2), t(r#"["c"]"#), t(r#"["c"]"#)],
                vec![Value::Int(3), Value::Null, Value::Null],
                vec![
                    Value::Int(4),
                    t(r#"["a", "b", "c"]"#),
                    t(r#"["a", "b", "c"]"#)
                ],
                vec![Value::Int(5), t(r#"["z"]"#), Value::Null],
            ]
        );
        for (sql, expected) in [
            (
                "SELECT COALESCE(json_agg(id), ' [ ] '), COALESCE(jsonb_agg(id), ' [ ] ') FROM f3_j_src WHERE id > 10",
                vec![t(" [ ] "), t("[]")],
            ),
            (
                "SELECT pg_typeof(COALESCE('[]', json_agg(id))), pg_typeof(COALESCE(jsonb_agg(id), '[]')) FROM f3_j_src",
                vec![t("json"), t("jsonb")],
            ),
            (
                "SELECT CASE WHEN false THEN json_agg(id) ELSE ' [ ] ' END, CASE WHEN false THEN jsonb_agg(id) ELSE ' [ ] ' END FROM f3_j_src",
                vec![t(" [ ] "), t("[]")],
            ),
            (
                "SELECT pg_typeof(CASE WHEN false THEN json_agg(id) ELSE '[]' END), pg_typeof(CASE WHEN false THEN jsonb_agg(id) ELSE '[]' END) FROM f3_j_src",
                vec![t("json"), t("jsonb")],
            ),
            (
                "SELECT COALESCE(json_agg(id), '[]'::json) FROM f3_j_src WHERE false",
                vec![t("[]")],
            ),
            (
                "SELECT NULLIF(jsonb_agg(id ORDER BY id), '[1,2,3]'), NULLIF(jsonb_agg(id ORDER BY id), '[]'), NULLIF('[1.00,2,3]', jsonb_agg(id ORDER BY id)) FROM f3_j_src",
                vec![Value::Null, t("[1, 2, 3]"), Value::Null],
            ),
        ] {
            assert_eq!(rows(&mut s, sql), [expected], "{sql}");
        }
        for sql in [
            "INSERT INTO f3_j_dst (id,jb) SELECT 6,'[1]'::text",
            "INSERT INTO f3_j_dst (id,n) SELECT 6,json_agg(t) FROM f3_j_src",
            "UPDATE f3_j_dst d SET n = (SELECT json_agg(t) FROM f3_j_src WHERE g = d.id)",
        ] {
            assert_eq!(
                sqlstate(&s.execute_at(sql, &[], None, &CLOCK).expect_err(sql)),
                "42804",
                "{sql}"
            );
        }
        for sql in [
            "SELECT COALESCE(json_agg(id), 'not json') FROM f3_j_src",
            "SELECT CASE WHEN true THEN json_agg(id) ELSE 'not json' END FROM f3_j_src",
        ] {
            reject(
                &mut s,
                sql,
                "22P02",
                "invalid input syntax for type json",
                None,
            );
        }
        for (sql, code, message) in [
            (
                "SELECT COALESCE(json_agg(id), '[]'::text) FROM f3_j_src",
                "42804",
                "COALESCE types json and text cannot be matched",
            ),
            (
                "SELECT COALESCE(jsonb_agg(id), json_agg(id)) FROM f3_j_src",
                "42846",
                "COALESCE could not convert type json to jsonb",
            ),
            (
                "SELECT CASE WHEN true THEN '[]'::text ELSE json_agg(id) END FROM f3_j_src",
                "42804",
                "CASE types json and text cannot be matched",
            ),
        ] {
            reject(&mut s, sql, code, message, None);
        }
        for (args, message) in [
            (
                "json_agg(id), '[]'",
                "operator does not exist: json = unknown",
            ),
            (
                "'[]', json_agg(id)",
                "operator does not exist: unknown = json",
            ),
            (
                "json_agg(id), NULL",
                "operator does not exist: json = unknown",
            ),
            (
                "jsonb_agg(id), json_agg(id)",
                "operator does not exist: jsonb = json",
            ),
        ] {
            reject(
                &mut s,
                &format!("SELECT NULLIF({args}) FROM f3_j_src"),
                "42883",
                message,
                Some(
                    "No operator matches the given name and argument types. You might need to add explicit type casts.",
                ),
            );
        }
    }
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

#[test]
fn f3_json_legacy() {
    json_assignments_and_conditionals(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn f3_json_v2() {
    json_assignments_and_conditionals(Engine::V2);
}
