//! Task-session statement class of a prepared statement (GD-0061 R3-202, design section 2.4(a)).
//!
//! The match is exhaustive with no wildcard arm: a new `Statement` variant fails to compile
//! until someone classifies it.

use super::schema_binding::split_key;
use super::{Prepared, Statement};
use crate::task::columns::StatementShape;
use crate::task::StatementClass;

/// A relation of the system catalogs: `pg_catalog`, `information_schema` or any `pg_*`
/// schema. Such names are reserved for user tables, so they only ever name a catalog.
fn is_catalog_relation(key: &str) -> bool {
    let (schema, name) = split_key(key);
    schema == "pg_catalog"
        || schema == "information_schema"
        || schema.starts_with("pg_")
        || (schema == "public" && name.starts_with("pg_"))
}

impl Prepared {
    /// The class of `sql` from its text alone (GD-0061 R3-206). It parses without binding, so
    /// it reads no catalog, sequence or table: a task session admits the class before any of
    /// those reads happen. A statement that does not parse returns the parse error.
    pub fn task_class_of_sql(sql: &str) -> Result<StatementClass, crate::CoreError> {
        if sql.len() > super::MAX_STATEMENT_SQL {
            return Err(crate::CoreError::Sql(super::SqlError::Limit));
        }
        Prepared::parse(sql, 0, 0)
            .map(|p| p.task_class())
            .map_err(super::routines::into_core)
    }

    /// The class `task::admit::admit` sees for this statement.
    ///
    /// Table 4(a), plus one rule: a read that names a catalog relation anywhere (including a
    /// subquery) is `Catalog`, because catalog queries are refused or filtered as a class
    /// (ruling D11), never released through the column rules.
    pub fn task_class(&self) -> StatementClass {
        match self.statement.get() {
            Statement::Select { name, .. } if is_catalog_relation(name) => StatementClass::Catalog,
            Statement::Select { .. } => StatementClass::Select,
            Statement::Query(q) if q.row_lock_command().is_some() => StatementClass::Other,
            Statement::Query(q) => {
                let mut shape = StatementShape::new(StatementClass::Select);
                q.task_refs(&mut shape, true);
                if shape.tables.iter().any(|t| is_catalog_relation(t)) {
                    StatementClass::Catalog
                } else if q.explain {
                    StatementClass::Explain
                } else if q.task_has_join() {
                    // Table 4(a) refuses every join, including a self-join.
                    StatementClass::Other
                } else {
                    StatementClass::Select
                }
            }
            // Fail closed: every GD-0079 planner statement is `Other`, so a task session refuses
            // it, even when it names a catalog relation. Task-session column rules, catalog
            // filtering (R3-312) and row scope and masking hook the older execution paths, not
            // the planner, so a planner shape that mixes a catalog relation with a user table
            // (UNION, VALUES, WITH, WITH RECURSIVE, data-modifying WITH) would otherwise run
            // unfiltered once `Catalog` is admitted (R3-315). Non-task sessions never read this.
            Statement::Plan(_) => StatementClass::Other,
            Statement::CompositeSelect(_) => StatementClass::Other,
            Statement::Update { .. } | Statement::UpdateExpressions { .. } => {
                StatementClass::Update
            }
            Statement::Insert { .. }
            | Statement::InsertExpressions(_)
            | Statement::InsertMulti(_) => StatementClass::Insert,
            Statement::Delete { .. } | Statement::Truncate { .. } => StatementClass::Delete,
            Statement::Begin
            | Statement::Commit
            | Statement::Rollback
            | Statement::SetConstraints(..) => StatementClass::TransactionControl,
            Statement::Savepoint(_) | Statement::Release(_) | Statement::RollbackTo(_) => {
                StatementClass::Savepoint
            }
            Statement::Extension(_) | Statement::Schema(_)
            | Statement::TypeDdl(_) | Statement::CollationDdl(_)
            | Statement::Sequence(_)
            | Statement::Create { .. }
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::Comment(_)
            | Statement::AdminUtility(_)
            | Statement::SecurityLabel(_)
            | Statement::View(_)
            | Statement::Routine(_) | Statement::Trigger(_)
            | Statement::DropTable { .. } => StatementClass::Ddl,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    // `deliveries` is reserved (relational/ident.rs); the worked example's columns are used
    // under `shipments(id, merchant_id, status, failed_at, attempt_count, customer_email,
    // card_token)`. Classification is syntactic, so no table needs to exist.
    fn prepared(sql: &str) -> Prepared {
        Prepared::parse(sql, 1, 0).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    }

    #[test]
    fn task_class_covers_every_statement() {
        let cases = [
            ("SELECT id FROM shipments WHERE id = 1", StatementClass::Select),
            ("SELECT * FROM s.shipments", StatementClass::Select),
            ("EXPLAIN SELECT id FROM shipments", StatementClass::Explain),
            ("SELECT 1", StatementClass::Select),
            ("SELECT now()", StatementClass::Select),
            (
                "SELECT a.id FROM shipments a JOIN others b ON a.id = b.id",
                StatementClass::Other,
            ),
            ("SELECT s.status FROM shipments s JOIN shipments t ON s.status = t.status", StatementClass::Other),
            ("SELECT ROW(1, 'a')::pair", StatementClass::Other),
            ("SELECT id FROM shipments UNION SELECT id FROM others", StatementClass::Other),
            ("WITH q AS (SELECT id FROM shipments) SELECT id FROM q", StatementClass::Other),
            ("VALUES (1)", StatementClass::Other),
            (
                // A planner statement naming a catalog relation is still `Other` (fail closed).
                "SELECT id FROM shipments UNION SELECT table_name FROM information_schema.tables",
                StatementClass::Other,
            ),
            (
                "WITH c AS (SELECT table_name FROM information_schema.tables) SELECT * FROM c",
                StatementClass::Other,
            ),
            (
                "WITH d AS (DELETE FROM shipments WHERE id = 1 RETURNING id) SELECT id FROM d",
                StatementClass::Other,
            ),
            ("SELECT * FROM information_schema.tables", StatementClass::Catalog),
            (
                "SELECT id FROM shipments WHERE status IN (SELECT table_name FROM information_schema.tables)",
                StatementClass::Catalog,
            ),
            ("UPDATE shipments SET status = 'x' WHERE id = 1", StatementClass::Update),
            (
                "UPDATE shipments SET attempt_count = attempt_count + 1 WHERE id = 1",
                StatementClass::Update,
            ),
            ("INSERT INTO shipments (id) VALUES (1)", StatementClass::Insert),
            ("INSERT INTO shipments (id) VALUES (1), (2)", StatementClass::Insert),
            ("INSERT INTO shipments (id) VALUES (1 + 1)", StatementClass::Insert),
            ("DELETE FROM shipments WHERE id = 1", StatementClass::Delete),
            ("TRUNCATE shipments", StatementClass::Delete),
            ("BEGIN", StatementClass::TransactionControl),
            ("COMMIT", StatementClass::TransactionControl),
            ("ROLLBACK", StatementClass::TransactionControl),
            ("SET CONSTRAINTS ALL DEFERRED", StatementClass::TransactionControl),
            ("SAVEPOINT a", StatementClass::Savepoint),
            ("RELEASE SAVEPOINT a", StatementClass::Savepoint),
            ("ROLLBACK TO SAVEPOINT a", StatementClass::Savepoint),
            ("CREATE TABLE t (id INT PRIMARY KEY)", StatementClass::Ddl),
            ("CREATE INDEX i ON shipments (status)", StatementClass::Ddl),
            ("DROP INDEX i", StatementClass::Ddl),
            ("ALTER TABLE shipments ADD COLUMN x INT", StatementClass::Ddl),
            ("COMMENT ON TABLE shipments IS 'x'", StatementClass::Ddl),
            (
                "SECURITY LABEL ON COLUMN shipments.status IS 'untrusted_text'",
                StatementClass::Ddl,
            ),
            ("CREATE VIEW v AS SELECT id FROM shipments", StatementClass::Ddl),
            ("DROP VIEW v", StatementClass::Ddl),
            ("DROP TABLE shipments", StatementClass::Ddl),
            ("CREATE SCHEMA s", StatementClass::Ddl),
            ("DROP SCHEMA s", StatementClass::Ddl),
            ("CREATE SEQUENCE q", StatementClass::Ddl),
            ("ALTER SEQUENCE q RESTART", StatementClass::Ddl),
            ("DROP SEQUENCE q", StatementClass::Ddl),
            ("CREATE TYPE mood AS ENUM ('a')", StatementClass::Ddl),
        ];
        let mut commands = BTreeSet::new();
        for (sql, want) in cases {
            let p = prepared(sql);
            assert_eq!(p.task_class(), want, "{sql}");
            commands.insert(p.command());
            // The column collector reports the same class.
            assert_eq!(p.task_shape().unwrap().class, want, "{sql}");
        }

        // The legacy single-table `Select` has no SQL spelling of its own.
        let mut p = prepared("SELECT id FROM shipments");
        *p.statement.get_mut() = Statement::Select {
            name: "shipments".into(),
            projection: vec!["id".into()],
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
        };
        assert_eq!(p.task_class(), StatementClass::Select);
        if let Statement::Select { name, .. } = p.statement.get_mut() {
            *name = "pg_catalog\0pg_class".into();
        }
        assert_eq!(p.task_class(), StatementClass::Catalog);

        // One case per `command()` kind the parser produces.
        for command in [
            "SELECT", "EXPLAIN", "UPDATE", "INSERT", "DELETE", "TRUNCATE TABLE", "BEGIN",
            "COMMIT", "ROLLBACK", "SET CONSTRAINTS", "SAVEPOINT", "RELEASE", "CREATE TABLE",
            "CREATE INDEX", "DROP INDEX", "ALTER TABLE", "COMMENT", "SECURITY LABEL", "CREATE VIEW", "DROP VIEW",
            "DROP TABLE", "CREATE SCHEMA", "DROP SCHEMA", "CREATE SEQUENCE", "ALTER SEQUENCE",
            "DROP SEQUENCE",
        ] {
            assert!(commands.contains(command), "{command} not covered: {commands:?}");
        }
    }

    /// Fail closed (GD-0079 x R3-315): every planner statement is `Other` in a task session,
    /// whether it names a catalog relation, the task table, both or no table at all.
    #[test]
    fn planner_statements_are_other_even_with_catalog_relations() {
        // `pg_class` and other `pg_*` relations do not parse here (`Reserved`, or `Unsupported`
        // inside WITH): the server answers them on its catalog path. `information_schema`
        // relations do reach the planner.
        for sql in [
            "SELECT table_name FROM information_schema.tables UNION ALL SELECT status FROM shipments",
            "WITH c AS (SELECT table_name FROM information_schema.tables) SELECT * FROM c",
            "WITH t AS (SELECT * FROM shipments) SELECT * FROM t",
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3)              SELECT n FROM r",
            "WITH d AS (DELETE FROM shipments WHERE id = 1 RETURNING id) SELECT id FROM d",
            "VALUES (1)",
        ] {
            let p = prepared(sql);
            assert!(p.is_planner_statement(), "{sql}: not a planner statement");
            assert_eq!(p.task_class(), StatementClass::Other, "{sql}");
            assert_eq!(Prepared::task_class_of_sql(sql).unwrap(), StatementClass::Other, "{sql}");
        }
    }

    #[test]
    fn task_class_of_sql_matches_prepared_class_without_binding() {
        for sql in [
            "SELECT id FROM shipments WHERE id = $1",
            "UPDATE shipments SET status = $1 WHERE id = $2",
            "BEGIN",
            "DROP INDEX i",
        ] {
            assert_eq!(
                Prepared::task_class_of_sql(sql).unwrap(),
                prepared(sql).task_class(),
                "{sql}"
            );
        }
        assert!(Prepared::task_class_of_sql("UPDATE").is_err());
    }

    #[test]
    fn catalog_relation_names() {
        assert!(is_catalog_relation("pg_catalog\0pg_class"));
        assert!(is_catalog_relation("information_schema\0tables"));
        assert!(is_catalog_relation("pg_toast\0x"));
        assert!(is_catalog_relation("pg_class"));
        assert!(!is_catalog_relation("shipments"));
        assert!(!is_catalog_relation("s\0shipments"));
    }
}
