//! Data-modifying CTEs reuse ordinary mutations against one statement snapshot.
use super::*;
use bind::Meta;
use exec::Relation;
use super::super::super::{row_sink, spill};
use crate::CoreError;

#[derive(Debug, Clone)]
pub(super) struct MainInsert {
    pub prepared: Prepared,
    pub parameters: BTreeSet<usize>,
}

pub(super) fn parameters(tokens: &[Token]) -> BTreeSet<usize> {
    tokens.iter().filter_map(|token| match token {
        Token::Param(n) => Some(*n),
        _ => None,
    }).collect()
}

pub(super) fn parse(tokens: &[Token], owner: &mut Parser) -> R<Prepared> {
    let mut p = parse::parser(tokens);
    let statement = match p.parse("") {
        Ok(statement) => statement,
        Err(_)
            if tokens.first().is_some_and(|t| parse::word(t, "insert"))
                && !sql_insert::claims(tokens) =>
        {
            p.at = 0;
            p.params.clear();
            p.relations.clear();
            Statement::InsertExpressions(InsertExpressions::parse(&mut p)?)
        }
        Err(_) if mutation::claims(tokens) || merge::claims(tokens) => {
            p = parse::parser(tokens);
            Statement::Plan(Plan::parse(&mut p).map_err(|e| match e { ParseError::Definite(e)=>e, ParseError::Decline=>SqlError::Unsupported })?)
        }
        Err(error) => return Err(error),
    };
    let mut prepared = Prepared::engine(statement, 0, 0);
    prepared.params = parameters(tokens).last().copied().unwrap_or(0);
    prepared.relations = p.relations;
    owner.relations.extend(prepared.relations.clone());
    Ok(prepared)
}

pub(super) fn references(prepared: &Prepared, name: &str) -> bool {
    match prepared.statement.get() {
        Statement::Plan(plan) => cte::references(plan, name) > 0,
        Statement::InsertMulti(insert) if let Some(sql_insert::Source::Plan(plan, _)) = &insert.source => {
            cte::references(plan, name) > 0
        }
        _ => prepared.read_tables().iter().any(|table| table == name),
    }
}

pub(super) fn remap(p: &mut Prepared, alias: &str, name: &str) {
    if let Statement::Plan(plan) = p.statement.get_mut() { cte::remap(plan, alias, name); }
    if let Statement::InsertMulti(insert) = p.statement.get_mut()
        && let Some(source) = &mut insert.source
    {
        match source {
            sql_insert::Source::Query(query) => cte::remap_query(query, alias, name),
            sql_insert::Source::Plan(plan, _) => cte::remap(plan, alias, name),
        }
    }
    if let Statement::Update { search, .. }
    | Statement::UpdateExpressions { search, .. }
    | Statement::Delete { search, .. } = p.statement.get_mut()
        && let Some(query) = search
    {
        // The root scan is the real mutation target, even if a CTE shadows it.
        let sources = query.sources.clone();
        cte::remap_query(query, alias, name);
        query.sources = sources;
    }
    // Only read references can denote a CTE; mutation targets are real tables.
    let target = p.table().map(str::to_owned);
    p.relations
        .retain(|r| r.schema.is_some() || r.name != alias || target.as_deref() == Some(alias));
}

pub(super) fn resolve(
    p: &mut Prepared,
    state: &Relational,
    parameters: &mut [Option<DataType>],
    used: &BTreeSet<usize>,
) -> R<Meta> {
    p.epoch = state.epoch;
    p.params = parameters.len();
    p.parameter_types = parameters.to_vec();
    for (i, ty) in p.parameter_types.iter_mut().enumerate() {
        if !used.contains(&(i + 1)) {
            ty.get_or_insert(DataType::Text);
        }
    }
    let description = p.describe(state)?;
    for n in used {
        parameters[*n - 1] = Some(description.parameters[*n - 1].clone());
    }
    Ok(Meta {
        unknown: vec![None; description.columns.len()],
        columns: description.columns,
        typmods: description.typmods,
        type_oids: description.type_oids,
        collations: vec![],
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "shared statement snapshot and mutation context"
)]
pub(super) fn execute<'e>(
    p: &Prepared,
    snapshot: &Relational,
    catalog: &Relational,
    source: &dyn scan::RowSource,
    target: &mut Relational,
    events: &mut Vec<crate::relational::constraints::Event>,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &'e Execution<'_>,
) -> R<Relation<'e>> {
    if execution.cancelled() {
        return Err(SqlError::Aborted);
    }
    let mut candidate = snapshot.clone();
    candidate.next_version = target.next_version;
    if !std::sync::Arc::ptr_eq(&candidate.ext, &target.ext) && candidate.ext.sequences != target.ext.sequences { std::sync::Arc::make_mut(&mut candidate.ext).sequences = target.ext.sequences.clone(); }
    for (name, table) in catalog.tables.iter() {
        if name.starts_with("\0plan_") {
            std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
            candidate.rows.insert(table.id, BTreeMap::new());
        }
    }
    if !std::sync::Arc::ptr_eq(&candidate.ext, &catalog.ext) && candidate.ext.columns != catalog.ext.columns { std::sync::Arc::make_mut(&mut candidate.ext).columns = catalog.ext.columns.clone(); }
    // INSERT and MERGE occupancy includes earlier CTE writes, while their source
    // still reads the statement snapshot through the injected RowSource.
    if matches!(p.command(), "INSERT" | "MERGE")
        && let Some(table) = p.table().and_then(|n| candidate.tables.get(n))
    {
        candidate
            .rows
            .insert(table.id, target.rows[&table.id].clone());
        candidate.secondary = candidate
            .secondary_build_complete_engine(target.secondary_descriptors_engine(), || {
                execution.cancelled()
            })?;
    }
    let mut prepared = p.clone();
    prepared.epoch = candidate.epoch;
    prepared.params = params.len();
    prepared.parameter_types = parameters.to_vec();
    let description = prepared.describe(&candidate)?;
    let injected_source = if matches!(
        prepared.statement.get(),
        Statement::Insert { .. } | Statement::InsertExpressions(_)
    ) {
        // Point INSERT uses its source for occupancy, not a query scan.
        None
    } else {
        Some(source)
    };
    let output = candidate.execute_statement_inner(
        &prepared,
        params,
        None,
        || execution.cancelled(),
        clock,
        injected_source,
        events,
    )?;
    // Merge only this substatement's changes. Reads of base tables never use
    // this accumulating write image; RETURNING is the communication channel.
    for table in snapshot.tables.values() {
        let old =
            if matches!(p.command(), "INSERT" | "MERGE") && p.table() == Some(snapshot.table_key(table).as_str()) {
                &target.rows[&table.id]
            } else {
                &snapshot.rows[&table.id]
            };
        let new = &candidate.rows[&table.id];
        let removed: Vec<_> = old
            .keys()
            .filter(|k| !new.contains_key(*k))
            .cloned()
            .collect();
        let changed: Vec<_> = new
            .iter()
            .filter(|(k, r)| old.get(*k) != Some(*r))
            .map(|(k, r)| (k.clone(), r.clone()))
            .collect();
        let rows = target
            .rows
            .get_mut(&table.id)
            .ok_or(SqlError::UnknownTable)?;
        for key in removed {
            rows.remove(&key);
        }
        rows.extend(changed);
    }
    target.next_version = candidate.next_version;
    if !std::sync::Arc::ptr_eq(&target.ext, &candidate.ext) && target.ext.sequences != candidate.ext.sequences { std::sync::Arc::make_mut(&mut target.ext).sequences = candidate.ext.sequences.clone(); }
    let descriptors = target.secondary_descriptors_engine().to_vec();
    target.secondary =
        target.secondary_build_complete_engine(&descriptors, || execution.cancelled())?;
    let mut relation = Relation::new(description.columns, description.typmods, execution);
    relation.affected = output.affected;
    for row in output.rows {
        relation.push(row)?;
    }
    Ok(relation)
}

impl Plan {
    pub(in crate::relational) fn has_dml_ctes(&self) -> bool {
        !self.ctes.is_empty() && !self.write_tables().is_empty()
    }

    #[expect(clippy::too_many_arguments, reason = "paged writable CTE statement context")]
    pub(in crate::relational) fn paged_dml_ctes(
        &self, state: &mut Relational, prepared: &Prepared, params: &[Value],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        sink: &mut impl row_sink::RowSink, version_end: u64,
        deferred: Option<&mut super::super::super::DeferralState>,
    ) -> Result<SqlResult, CoreError> {
        scan::with_cursor_limit(scan::WRITE_SOURCE_BYTES, || {
            let bound = self.bind(state, prepared.params, &prepared.parameter_types).map_err(CoreError::Sql)?;
            let parameters: Vec<_> = bound.description.parameters.into_iter().map(Some).collect();
            let mut snapshot = state.clone();
            let mut cancel = || crate::cancellation::check().is_err();
            let execution = Execution::new(&mut cancel, &parameters);
            let retained = RetentionScope::new(&execution);
            row_sink::freeze_tables(&mut snapshot, bound.plan.read_tables(), sink,
                &mut |bytes| retained.borrow().charge(bytes))?;
            let first_version = state.next_version;
            let mut work = row_sink::paged_dml::ActionWork::new(deferred.as_deref().cloned().unwrap_or_default())?;
            work.track_inserts();
            let mut failure = None;
            let mut publish = |p: &Prepared, catalog: &Relational, source: &dyn scan::RowSource| -> R<Relation<'_>> {
                let mut candidate = state.clone();
                for (name, table) in catalog.tables.iter().filter(|(name, _)| name.starts_with("\0plan_")) {
                    std::sync::Arc::make_mut(&mut candidate.tables).insert(name.clone(), table.clone());
                    candidate.rows.insert(table.id, BTreeMap::new());
                }
                if candidate.ext.columns != catalog.ext.columns {
                    std::sync::Arc::make_mut(&mut candidate.ext).columns = catalog.ext.columns.clone();
                }
                let mut sub = p.clone();
                sub.epoch = candidate.epoch;
                sub.params = params.len();
                sub.parameter_types.clone_from(&parameters);
                sub.search_path.clone_from(&prepared.search_path);
                let description = sub.describe(&candidate)?;
                let (result, spilled) = spill::capture_returning(|| {
                    row_sink::publish_cte(&mut candidate, &sub, params, clock, sink, version_end,
                        source, first_version, &mut work)
                });
                let result = result.map_err(|error| {
                    failure = Some(error);
                    SqlError::Aborted
                })?;
                state.next_version = candidate.next_version;
                let mut rows = Relation::new(description.columns, description.typmods, &execution);
                rows.affected = result.affected;
                rows.spilled = spilled;
                for row in result.rows {
                    rows.push(row)?;
                }
                Ok(rows)
            };
            let relation = cte::run_inner(&bound.plan, &snapshot, &scan::LegacyRows(&snapshot),
                params, &parameters, clock, &execution, Some(&mut publish), true);
            if let Some(error) = failure { return Err(error); }
            let relation = relation.map_err(CoreError::Sql)?;
            work.complete(state, sink, clock, version_end, deferred)?;
            let mut output = SqlResult {
                columns: relation.columns.into_iter().map(|column| column.name).collect(),
                rows: Vec::new(),
                affected: relation.affected,
            };
            if let Some(spilled) = relation.spilled {
                spilled.publish(&mut output)?;
            } else {
                spill::RowBuffer::new(relation.rows)?.finish(&mut output)?;
            }
            Ok(output)
        })
    }

    #[expect(clippy::too_many_arguments, reason = "shared statement snapshot and optional injected source")]
    pub(in crate::relational) fn execute_dml(
        &self,
        state: &mut Relational,
        prepared: &Prepared,
        params: &[Value],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        cancel: &mut dyn FnMut() -> bool,
        events: &mut Vec<crate::relational::constraints::Event>,
        source: Option<&dyn scan::RowSource>,
    ) -> R<SqlResult> {
        if self.ctes.is_empty() && let Some(m) = &self.mutation { return m.execute(state, prepared, params, clock, cancel, events, source); }
        if self.ctes.is_empty() && let Some(m) = &self.merge { return m.execute(state, prepared, params, clock, cancel, events, source); }
        let bound = self.bind(state, prepared.params, &prepared.parameter_types)?;
        let parameters: Vec<_> = bound.description.parameters.into_iter().map(Some).collect();
        let snapshot = state.clone();
        let execution = Execution::new(cancel, &prepared.parameter_types);
        let mut execute = |prepared: &Prepared, catalog: &Relational, source: &dyn scan::RowSource| {
            dml::execute(prepared, &snapshot, catalog, source, state, events, params, &parameters,
                clock, &execution)
        };
        let relation = cte::run_inner(
            &bound.plan,
            &snapshot,
            source.unwrap_or(&scan::LegacyRows(&snapshot)),
            params,
            &parameters,
            clock,
            &execution,
            Some(&mut execute),
            false,
        )?;
        let mut bytes = 0;
        for value in relation.rows.iter().flat_map(|r| &r.values) {
            charge(&mut bytes, value)?;
        }
        Ok(SqlResult {
            columns: relation.columns.into_iter().map(|c| c.name).collect(),
            rows: relation.rows,
            affected: relation.affected,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "engine-rocksdb")]
    use crate::{Admin, Engine as StorageEngine, Limits, SqlSession};

    #[cfg(feature = "engine-rocksdb")]
    struct Fixture {
        admin: Option<Admin>,
        path: std::path::PathBuf,
    }
    #[cfg(feature = "engine-rocksdb")]
    impl Fixture {
        fn new() -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            let path = std::env::temp_dir()
                .join(format!("plan-with-dml-{:032x}", u128::from_be_bytes(nonce)));
            Self {
                admin: Some(
                    Admin::create_engine(&path, Limits::default(), StorageEngine::V2).unwrap(),
                ),
                path,
            }
        }
        fn session(&self) -> SqlSession {
            self.admin.as_ref().unwrap().sql_session()
        }
    }
    #[cfg(feature = "engine-rocksdb")]
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.admin.take();
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }
    #[cfg(feature = "engine-rocksdb")]
    fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
        s.execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    }
    #[cfg(feature = "engine-rocksdb")]
    fn values(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
        run(s, sql).rows.into_iter().map(|r| r.values).collect()
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn unreferenced_dml_executes_once_and_classifies_as_write() {
        let f = Fixture::new();
        let mut s = f.session();
        run(
            &mut s,
            "CREATE TABLE w(id int PRIMARY KEY, n int DEFAULT 10)",
        );
        let sql = "WITH d AS (INSERT INTO w(id) VALUES(1)) SELECT 42";
        let p = Prepared::parse(sql, 1, 0).unwrap();
        assert_eq!(p.v2_class(), V2Class::Dml);
        assert!(!p.is_read());
        assert_eq!(p.command(), "SELECT");
        assert_eq!(p.write_tables(), vec![("w".into(), "INSERT")]);
        assert!(p.authorization_tables().unwrap().is_empty());
        assert_eq!(values(&mut s, sql), vec![vec![Value::Int(42)]]);
        assert_eq!(
            values(&mut s, "SELECT * FROM w"),
            vec![vec![Value::Int(1), Value::Int(10)]]
        );
        assert_eq!(
            values(
                &mut s,
                "WITH d AS (UPDATE w SET n=n+1 RETURNING id,n) SELECT a.n,b.n FROM d a JOIN d b ON a.id=b.id"
            ),
            vec![vec![Value::Int(11), Value::Int(11)]]
        );
        assert_eq!(
            values(&mut s, "SELECT n FROM w"),
            vec![vec![Value::Int(11)]]
        );
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn main_and_sibling_queries_read_the_statement_snapshot() {
        let f = Fixture::new();
        let mut s = f.session();
        run(&mut s, "CREATE TABLE w(id int PRIMARY KEY)");
        run(&mut s, "CREATE TABLE archive(id int PRIMARY KEY)");
        run(&mut s, "INSERT INTO w VALUES(1),(2)");
        assert_eq!(
            values(
                &mut s,
                "WITH d AS (DELETE FROM w WHERE id=1 RETURNING id), a AS (INSERT INTO archive SELECT id FROM w RETURNING id) SELECT count(*) FROM w"
            ),
            vec![vec![Value::Int(2)]]
        );
        assert_eq!(
            values(&mut s, "SELECT * FROM archive ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
        assert_eq!(values(&mut s, "SELECT * FROM w"), vec![vec![Value::Int(2)]]);
        assert_eq!(
            values(
                &mut s,
                "WITH a AS (INSERT INTO w VALUES(3) RETURNING id) SELECT count(*) FROM w"
            ),
            vec![vec![Value::Int(1)]]
        );
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn move_rows_passes_returning_to_later_mutation() {
        let f = Fixture::new();
        let mut s = f.session();
        run(
            &mut s,
            "CREATE TABLE w(id int PRIMARY KEY, label text DEFAULT 'new')",
        );
        run(
            &mut s,
            "CREATE TABLE archive(id int PRIMARY KEY, label text)",
        );
        run(&mut s, "INSERT INTO w(id) VALUES(1),(2)");
        assert_eq!(
            values(
                &mut s,
                "WITH d AS (DELETE FROM w WHERE id=2 RETURNING id,label), a AS (INSERT INTO archive SELECT * FROM d RETURNING id,label) SELECT * FROM a"
            ),
            vec![vec![Value::Int(2), Value::Text("new".into())]]
        );
        assert_eq!(
            values(&mut s, "SELECT id FROM w"),
            vec![vec![Value::Int(1)]]
        );
        assert_eq!(
            values(&mut s, "SELECT id FROM archive"),
            vec![vec![Value::Int(2)]]
        );
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn parameters_aliases_defaults_and_returning_expressions() {
        let f = Fixture::new();
        let mut s = f.session();
        run(
            &mut s,
            "CREATE TABLE w(id serial PRIMARY KEY, n int DEFAULT 5)",
        );
        let clock = crate::relational::StatementClock {
            unavailable: None,
            transaction_us: 1,
            statement_us: 1,
            zone: None,
            wall: None,
        };
        let result = s.execute_at(
            "WITH a(k,v) AS (INSERT INTO w(n) VALUES($2) RETURNING id,n+1), b AS (INSERT INTO w(n) VALUES($1) RETURNING id) SELECT * FROM a",
            &[Value::Int(10), Value::Int(20)], None, &clock).unwrap();
        assert_eq!(result.columns, vec!["k", "v"]);
        assert_eq!(result.rows[0].values, vec![Value::Int(1), Value::Int(21)]);
        assert_eq!(
            values(&mut s, "SELECT * FROM w ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(20)],
                vec![Value::Int(2), Value::Int(10)]
            ]
        );
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn later_update_can_read_an_earlier_returning_relation() {
        let f = Fixture::new();
        let mut s = f.session();
        run(&mut s, "CREATE TABLE w(id int PRIMARY KEY, n int)");
        run(&mut s, "CREATE TABLE other(id int PRIMARY KEY)");
        run(&mut s, "INSERT INTO w VALUES(1,10),(2,20)");
        run(&mut s, "INSERT INTO other VALUES(2)");
        assert_eq!(
            values(
                &mut s,
                "WITH a AS (DELETE FROM other RETURNING id), b AS (UPDATE w SET n=n+1 WHERE id IN (SELECT id FROM a) RETURNING id,n) SELECT * FROM b"
            ),
            vec![vec![Value::Int(2), Value::Int(21)]]
        );
    }
    #[test]
    fn no_returning_reference_and_nested_modification_are_unsupported() {
        for sql in [
            "WITH d AS (DELETE FROM w) SELECT * FROM d",
            "WITH d AS (DELETE FROM w), a AS (INSERT INTO archive SELECT * FROM d RETURNING id) SELECT * FROM a",
            "SELECT * FROM (WITH d AS (DELETE FROM w RETURNING id) SELECT * FROM d) q",
            "WITH q AS (WITH d AS (DELETE FROM w RETURNING id) SELECT * FROM d) SELECT * FROM q",
        ] {
            let e = Prepared::parse(sql, 1, 0).unwrap_err();
            assert_eq!(crate::sqlstate(&crate::CoreError::Sql(e)), "0A000", "{sql}");
        }
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn rollback_and_statement_failure_leave_no_mutations_on_v2() {
        let f = Fixture::new();
        let mut s = f.session();
        run(
            &mut s,
            "CREATE TABLE w(id int PRIMARY KEY, n int CHECK(n>0))",
        );
        run(&mut s, "INSERT INTO w VALUES(1,10)");
        run(&mut s, "BEGIN");
        run(
            &mut s,
            "WITH d AS (DELETE FROM w RETURNING id) SELECT * FROM d",
        );
        assert!(values(&mut s, "SELECT * FROM w").is_empty());
        run(&mut s, "ROLLBACK");
        assert_eq!(
            values(&mut s, "SELECT * FROM w"),
            vec![vec![Value::Int(1), Value::Int(10)]]
        );
        let e = s.execute("WITH d AS (DELETE FROM w RETURNING id), a AS (INSERT INTO w VALUES(2,-1) RETURNING id) SELECT * FROM a", &[], None).unwrap_err();
        assert_eq!(crate::sqlstate(&e), "23514");
        assert_eq!(
            values(&mut s, "SELECT * FROM w"),
            vec![vec![Value::Int(1), Value::Int(10)]]
        );
    }
}
