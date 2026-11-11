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
    #[allow(dead_code)] // C8 will authorize this complete set before classification.
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
        self.subqueries
            .visit(&mut |_, child| child.query.collect_dependencies(&mut out))?;
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
}
