//! Query-owned children. Modes describe evaluation, never grammar admission.
use super::*;
pub(super) const MAX_DEPTH: usize = 32;
const MAX_SUBPLANS: usize = MAX_WORK;
#[allow(dead_code)] // EXISTS/scalar admission belongs to later slices.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SubplanMode {
    In,
    Exists,
    Scalar,
}
struct Subplan<'a> {
    key: usize,
    mode: SubplanMode,
    query: BoundQuery<'a>,
}
#[derive(Default)]
pub(super) struct Subplans<'a> {
    entries: Vec<Subplan<'a>>,
    count: usize,
    depth: usize,
}

impl<'a> Subplans<'a> {
    pub(super) fn correlated(&self, depth: usize) -> bool {
        self.entries.iter().any(|entry| entry.query.correlated(depth))
    }
    pub(super) fn register(
        &mut self,
        key: usize,
        mode: SubplanMode,
        query: BoundQuery<'a>,
    ) -> R<()> {
        let count = self
            .count
            .checked_add(1 + query.subqueries.count)
            .ok_or(SqlError::Limit)?;
        let depth = self.depth.max(1 + query.subqueries.depth);
        if count > MAX_SUBPLANS || depth > MAX_DEPTH {
            return Err(SqlError::Limit);
        }
        if self.entries.iter().any(|entry| entry.key == key) {
            return Err(SqlError::Unsupported);
        }
        self.entries.push(Subplan { key, mode, query });
        self.count = count;
        self.depth = depth;
        Ok(())
    }
    pub(super) fn get(&self, key: usize, mode: SubplanMode) -> R<&BoundQuery<'a>> {
        self.entries
            .iter()
            .find(|entry| entry.key == key && entry.mode == mode)
            .map(|entry| &entry.query)
            .ok_or(SqlError::Unsupported)
    }
    pub(super) fn attach_clock(&mut self, clock: Option<&super::super::clock::ExecutionClock>) {
        for entry in &mut self.entries {
            entry.query.attach_execution_clock(clock);
        }
    }
    #[allow(dead_code)] // Authorization consumes this visitor in C8.
    pub(super) fn visit(
        &self,
        visit: &mut impl FnMut(SubplanMode, &BoundQuery<'a>) -> R<()>,
    ) -> R<()> {
        for entry in &self.entries {
            visit(entry.mode, &entry.query)?;
            entry.query.subqueries.visit(visit)?;
        }
        Ok(())
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
pub(super) struct Dependencies {
    pub(super) relations: BTreeSet<String>,
    pub(super) routines: BTreeSet<String>,
}
impl Query {
    /// Syntactic dependencies, including unused child projections.
    pub(super) fn dependencies(&self) -> R<Dependencies> {
        let mut out = Dependencies::default();
        self.collect_dependencies(&mut out)?;
        Ok(out)
    }
    fn collect_dependencies(&self, out: &mut Dependencies) -> R<()> {
        out.relations.extend(self.read_tables());
        self.walk_scalars(&mut |scalar| {
            let name = match scalar {
                Scalar::Call(name, _) => match Aggregate::from_name(name) {
                    Some(aggregate) => aggregate.name(),
                    None if name == aggregate_filter::COUNT_STAR => "count",
                    None if !name.starts_with('\0') => name,
                    None => return Ok(()),
                },
                Scalar::SubstringOrder(_) => "substring",
                _ => return Ok(()),
            };
            out.routines.insert(name.into());
            Ok(())
        })?;
        for (item, _) in &self.items {
            if let Item::Aggregate(aggregate, _) = item {
                out.routines.insert(aggregate.name().into());
            }
        }
        if let Some(filter) = &self.filter {
            filter.visit_subqueries(&mut |_, child| child.collect_dependencies(out))?;
        }
        Ok(())
    }
}
impl BoundQuery<'_> {
    #[allow(dead_code)] // C8 scaffold, including synthetic children.
    pub(super) fn dependencies(&self) -> R<Dependencies> {
        let mut out = self.query.dependencies()?;
        out.relations.extend(self.outer_slots.iter().map(|s| self.tables[s.slot].name.clone()));
        self.subqueries.visit(&mut |_, child| {
            out.relations.extend(child.outer_slots.iter().map(|s| child.tables[s.slot].name.clone()));
            child.query.collect_dependencies(&mut out)
        })?;
        Ok(out)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn query(sql: &str) -> Query {
        Query::parse(&mut Parser {
            tokens: lex(sql).unwrap(),
            at: 0,
            params: BTreeSet::new(),
            relations: vec![],
        })
        .unwrap()
    }

    #[test]
    fn in_dependencies_include_nested_relations_and_projection_routines() {
        let q = query(
            "SELECT abs(id) FROM a WHERE id IN (SELECT length(v) FROM b WHERE id NOT IN (SELECT count(*) FILTER (WHERE lower(v) = 'x') FROM c WHERE substring(v FOR 1 FROM 1) = 'x')) ORDER BY abs(id)",
        );
        let deps = q.dependencies().unwrap();
        assert_eq!(deps.relations, ["a", "b", "c"].map(String::from).into());
        assert_eq!(
            deps.routines,
            ["abs", "length", "count", "lower", "substring"]
                .map(String::from)
                .into()
        );
    }

    #[test]
    fn synthetic_modes_are_visited_recursively_without_enabling_grammar() {
        let mut state = Relational::default();
        let create = Prepared::parse(
            "CREATE TABLE child_relation(id int PRIMARY KEY)",
            1,
            state.epoch,
        )
        .unwrap();
        state
            .execute_with_cancel(&create, &[], None, || false)
            .unwrap();
        let root = query("SELECT abs(1)");
        let child = query("SELECT lower('x')");
        let leaf = query("SELECT length('x') FROM child_relation");
        let mut bound = root.bind(&state, 0, &[]).unwrap();
        let mut nested = child.bind(&state, 0, &[]).unwrap();
        nested
            .subqueries
            .register(2, SubplanMode::Scalar, leaf.bind(&state, 0, &[]).unwrap())
            .unwrap();
        bound
            .subqueries
            .register(1, SubplanMode::Exists, nested)
            .unwrap();
        assert!(bound.subqueries.get(1, SubplanMode::In).is_err());
        let deps = bound.dependencies().unwrap();
        assert_eq!(deps.relations, ["child_relation".into()].into());
        assert_eq!(
            deps.routines,
            ["abs", "lower", "length"].map(String::from).into()
        );
    }

    #[test]
    fn registry_limits_are_inclusive_and_rejection_preserves_registry() {
        let state = Relational::default();
        let q = query("SELECT 1");
        let bind = || q.bind(&state, 0, &[]).unwrap();
        let mut registry = Subplans::default();
        for key in 0..MAX_SUBPLANS {
            registry.register(key, SubplanMode::In, bind()).unwrap();
        }
        assert!(matches!(
            registry.register(MAX_SUBPLANS, SubplanMode::In, bind()),
            Err(SqlError::Limit)
        ));
        assert_eq!(registry.entries.len(), MAX_SUBPLANS);
        let mut nested = bind();
        for depth in 1..=MAX_DEPTH {
            let mut parent = bind();
            parent
                .subqueries
                .register(depth, SubplanMode::In, nested)
                .unwrap();
            nested = parent;
        }
        assert_eq!(nested.subqueries.depth, MAX_DEPTH);
        let mut parent = Subplans::default();
        assert!(matches!(
            parent.register(0, SubplanMode::In, nested),
            Err(SqlError::Limit)
        ));
        assert!(parent.entries.is_empty());
    }
    #[test]
    fn lexical_outer_bindings_and_synthetic_exists() {
        let mut state = Relational::default();
        for sql in ["CREATE TABLE a(id int PRIMARY KEY, v text)",
            "CREATE TABLE b(id bigint PRIMARY KEY)", "CREATE TABLE c(other int PRIMARY KEY)"] {
            let create = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute_with_cancel(&create, &[], None, || false).unwrap();
        }
        let root = query("SELECT id FROM a o");
        let middle = query("SELECT id FROM b m");
        let child = query("SELECT abs(id) FROM c WHERE o.id = $1");
        let bound = child.bind_seeded_inner(&state, vec![None], false,
            &[&middle.sources, &root.sources]).unwrap();
        assert_eq!(bound.columns[0].data_type, DataType::BigInt);
        assert_eq!(bound.parameters, vec![DataType::Integer]);
        assert!(bound.outer_slots.iter().any(|s| s.depth == 1 && s.source == 0
            && s.column == 0 && s.data_type == DataType::BigInt && s.slot == 1));
        assert!(bound.outer_slots.iter().any(|s| s.depth == 2 && s.data_type == DataType::Integer));
        let mut parent = root.bind(&state, 1, &[Some(DataType::Integer)]).unwrap();
        parent.subqueries.register(9, SubplanMode::Exists, bound).unwrap();
        let deps = parent.dependencies().unwrap();
        assert_eq!(deps.relations, ["a", "b", "c"].map(String::from).into());
        assert_eq!(deps.routines, ["abs".into()].into());
        let declared = query("SELECT $2 FROM c WHERE o.id = $1");
        let bound = declared.bind_seeded_inner(&state,
            vec![Some(DataType::Integer), Some(DataType::Text)], false, &[&root.sources]).unwrap();
        assert_eq!(bound.parameters, vec![DataType::Integer, DataType::Text]);
        assert_eq!(bound.columns[0].data_type, DataType::Text);
        for (sql, error) in [
            ("SELECT id FROM a x JOIN b y ON x.id=y.id", SqlError::AmbiguousColumn),
            ("SELECT id FROM c", SqlError::AmbiguousColumn),
            ("SELECT o.id FROM c o", SqlError::UnknownColumn),
            ("SELECT absent FROM c", SqlError::UnknownColumn),
            ("SELECT absent.id FROM c", SqlError::UnknownTable),
        ] {
            let q = query(sql);
            let ambiguous = query("SELECT x.id FROM a x JOIN b y ON x.id=y.id");
            let scopes = if sql == "SELECT id FROM c" { &ambiguous.sources } else { &root.sources };
            assert!(matches!(q.bind_seeded_inner(&state, vec![], false, &[scopes]),
                Err(actual) if actual == error), "{sql}");
        }
        for sql in ["SELECT o.id, count(*) FROM c", "SELECT o.id FROM c GROUP BY o.id"] {
            let q = query(sql);
            let bound = q.bind_seeded_inner(&state, vec![], false, &[&root.sources]).unwrap();
            assert_eq!(bound.outer_slots.len(), 1);
            assert_eq!(bound.columns[0].data_type, DataType::Integer);
        }
        let shadow = query("SELECT o.id FROM b o");
        let bound = shadow.bind_seeded_inner(&state, vec![], false, &[&root.sources]).unwrap();
        assert!(bound.outer_slots.is_empty());
        assert_eq!(bound.columns[0].data_type, DataType::BigInt);
        let q = query("SELECT o.id");
        assert!(q.bind_seeded_inner(&state, vec![], false,
            &vec![root.sources.as_slice(); MAX_DEPTH]).is_ok());
        assert!(matches!(q.bind_seeded_inner(&state, vec![], false,
            &vec![root.sources.as_slice(); MAX_DEPTH + 1]), Err(SqlError::Limit)));
        for sql in ["SELECT id FROM a WHERE id IN (SELECT 1)",
            "SELECT id FROM a WHERE id IN (SELECT count(*))"] {
            let q = query(sql);
            q.bind(&state, 0, &[]).unwrap().subqueries.visit(&mut |_, child| {
                assert!(child.outer_slots.is_empty());
                assert!(child.tables.is_empty());
                Ok(())
            }).unwrap();
        }
        let sql = "SELECT id FROM a o WHERE id IN (SELECT o.id FROM c)";
        let q = query(sql);
        assert_eq!(q.bind(&state, 0, &[]).unwrap().subqueries
            .get(match q.where_filter().unwrap() { Filter::InSubquery { query, .. } => query.as_ref() as *const Query as usize,
                _ => panic!("IN child") }, SubplanMode::In).unwrap().outer_slots.len(), 1);
        let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
        assert!(matches!(state.execute_with_cancel(&prepared, &[], None, || false), Err(SqlError::Unsupported)));
    }

    #[test]
    fn exists_parser_shapes_and_recursive_dependencies() {
        for (term, negated) in [("EXISTS (SELECT NULL)", false), ("NOT EXISTS (SELECT NULL)", true)] {
            let q = query(&format!("SELECT 1 WHERE {term}"));
            assert!(matches!(q.where_filter(), Some(Filter::Exists { negated: n, .. }) if *n == negated));
        }
        let q = query("SELECT 1 WHERE NOT (EXISTS (SELECT NULL))");
        assert!(matches!(q.where_filter(), Some(Filter::Not(f)) if matches!(f.as_ref(), Filter::Exists { negated: false, .. })));
        let q = query("SELECT 1 WHERE false OR (EXISTS (SELECT NULL) AND NOT EXISTS (SELECT NULL))");
        assert!(matches!(q.where_filter(), Some(Filter::Logic(_, false, _))));
        let q = query("SELECT id FROM a WHERE EXISTS (SELECT abs(id) FROM b WHERE id IN (SELECT length(v) FROM c WHERE EXISTS (SELECT count(*) FROM d WHERE lower(v) = 'x'))) OR id IN (SELECT id FROM e WHERE NOT EXISTS (SELECT NULL FROM f WHERE nextval('seq') > 0))");
        let deps = q.dependencies().unwrap();
        assert_eq!(deps.relations, ["a", "b", "c", "d", "e", "f"].map(String::from).into());
        assert_eq!(deps.routines, ["abs", "length", "count", "lower", "nextval"].map(String::from).into());
        assert_eq!(q.sequence_dependencies(), vec![Some("seq".into())]);
        assert_eq!(q.read_tables().into_iter().collect::<BTreeSet<_>>(), deps.relations);
    }

    #[test]
    fn exists_binds_nullable_multi_column_children_and_lexical_outer_slots() {
        let mut state = Relational::default();
        for sql in ["CREATE TABLE a(id int PRIMARY KEY)", "CREATE TABLE b(id int PRIMARY KEY)"] {
            let create = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute_with_cancel(&create, &[], None, || false).unwrap();
        }
        let q = query("SELECT id FROM a o WHERE EXISTS (SELECT NULL, abs(o.id) FROM b WHERE id = $1 AND NOT EXISTS (SELECT NULL WHERE o.id = $1))");
        let bound = q.bind(&state, 1, &[]).unwrap();
        assert_eq!(bound.parameters, vec![DataType::Integer]);
        let child = &bound.subqueries.entries[0];
        assert_eq!(child.mode, SubplanMode::Exists);
        assert_eq!(child.query.columns.len(), 2);
        assert_eq!(child.query.outer_slots[0].depth, 1);
        let nested = &child.query.subqueries.entries[0];
        assert_eq!(nested.mode, SubplanMode::Exists);
        assert_eq!(nested.query.outer_slots[0].depth, 2);
        assert_eq!(bound.dependencies().unwrap(), q.dependencies().unwrap());
        let q = query("SELECT 1 WHERE EXISTS (SELECT NULL)");
        q.bind(&state, 0, &[]).unwrap();
    }

}
