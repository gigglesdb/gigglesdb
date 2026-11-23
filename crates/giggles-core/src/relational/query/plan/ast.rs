use super::*;

/// A query expression owns its pagination boundary; nested operands keep theirs.
#[derive(Debug, Clone)]
pub(in crate::relational) struct Plan {
    pub(super) body: Body,
    pub(super) mutation: Option<Box<mutation::Mutation>>,
    pub(super) merge: Option<Box<merge::Merge>>,
    pub(super) insert: Option<Box<dml::MainInsert>>,
    pub(super) ctes: Vec<cte::Cte>,
    pub(super) order: Vec<(Scalar, bool, bool)>,
    pub(super) limit: Option<Scalar>,
    pub(super) offset: Option<Scalar>,
    pub(super) decorations: Box<paging::Options>,
}
#[derive(Debug, Clone)]
pub(super) enum Body {
    Select(Box<Select>),
    Values(Vec<Vec<Scalar>>),
    Set {
        op: SetOp,
        all: bool,
        left: Box<Plan>,
        right: Box<Plan>,
    },
}
#[derive(Debug, Clone, Copy)]
pub(super) enum SetOp {
    Union,
    Intersect,
    Except,
}
#[derive(Debug, Clone)]
pub(super) struct Select {
    pub applies: Vec<subquery::Apply>,
    pub windows: Vec<window::Call>,
    pub query: Query,
    pub parameters: BTreeSet<usize>,
    // A materialized input exposes operator outputs as ordinary scalar columns.
    // Later operators (windows, apply, CTEs) can use the same boundary.
    pub input: Option<Input>,
    pub from: Option<join::From>,
}
#[derive(Debug, Clone)]
pub(super) struct Input {
    pub query: Box<Plan>,
    pub name: String,
    pub aliases: Vec<String>,
}
impl Plan {
    pub(super) fn new(body: Body) -> Self {
        Self {
            body,
            mutation: None,
            merge: None,
            insert: None,
            ctes: vec![],
            order: vec![],
            limit: None,
            offset: None,
            decorations: Box::default(),
        }
    }
    pub(in crate::relational) fn paged_mutation(&self) -> Option<&mutation::Mutation> {
        self.ctes.is_empty().then_some(self.mutation.as_deref()).flatten()
    }
    pub(in crate::relational) fn read_tables(&self) -> Vec<String> {
        let mut out = self.mutation.as_ref().map_or_else(Vec::new, |m| m.read_tables());
        if let Some(m) = &self.merge { out.extend(m.read_tables()); }
        if let Some(insert) = &self.insert {
            out.extend(insert.prepared.read_tables().into_iter().filter(|name| !name.starts_with("\0plan_")));
            if let Some(target) = insert.prepared.table() && !out.iter().any(|name| name == target) {
                out.push(target.into());
            }
        }
        self.visit_queries(&mut |q| {
            for name in q.read_tables() {
                if !name.starts_with("\0plan_") && !out.contains(&name) {
                    out.push(name);
                }
            }
        });
        for cte in &self.ctes {
            if let Some(dml) = &cte.dml {
                for name in dml
                    .read_tables()
                    .into_iter()
                    .chain(dml.table().map(str::to_owned))
                {
                    if !name.starts_with("\0plan_") && !out.contains(&name) {
                        out.push(name);
                    }
                }
            }
            for name in cte.query.read_tables() {
                if !out.contains(&name) {
                    out.push(name);
                }
            }
        }
        out
    }
    pub(in crate::relational) fn row_lock_tables(&self) -> Vec<String> {
        let mut out = vec![];
        self.visit_queries(&mut |q| out.extend(q.row_lock_tables()));
        out.sort(); out.dedup(); out
    }
    pub(in crate::relational) fn row_lock_command(&self) -> Option<&'static str> {
        let mut out = None;
        self.visit_queries(&mut |q| { if out.is_none() { out = q.row_lock_command(); } });
        out
    }
    pub(in crate::relational) fn write_tables(&self) -> Vec<(String, &'static str)> {
        let mut tables: Vec<_> = self.ctes
            .iter()
            .flat_map(|cte| {
                cte.dml
                    .as_ref()
                    .map_or_else(|| cte.query.write_tables(), |p| p.write_tables())
            })
            .collect();
        if let Some(m) = &self.mutation { tables.push((m.target.clone(),m.command())); }
        if let Some(m) = &self.merge { tables.extend(m.write_tables()); }
        if let Some(insert) = &self.insert { tables.extend(insert.prepared.write_tables()); }
        tables
    }
    pub(in crate::relational) fn authorization_tables(&self) -> R<Vec<String>> {
        let mut out = self.mutation.as_ref().map_or_else(Vec::new, |m| m.read_tables());
        if let Some(m) = &self.merge { out.extend(m.read_tables()); }
        if let Some(insert) = &self.insert {
            out.extend(insert.prepared.authorization_tables()?.into_iter().filter(|name| !name.starts_with("\0plan_")));
        }
        let mut error = None;
        self.visit_queries(&mut |q| match q.authorization_tables() {
            Ok(names) => {
                for name in names {
                    if !name.starts_with("\0plan_") && !out.contains(&name) {
                        out.push(name);
                    }
                }
            }
            Err(e) => error = Some(e),
        });
        for cte in &self.ctes {
            let names = if let Some(dml) = &cte.dml {
                dml.authorization_tables()?
            } else {
                cte.query.authorization_tables()?
            };
            for name in names {
                if !name.starts_with("\0plan_") && !out.contains(&name) {
                    out.push(name);
                }
            }
        }
        error.map_or(Ok(out), Err)
    }
    pub(in crate::relational) fn sequences(&self) -> Vec<SequenceDependency> {
        let mut out = self.mutation.as_ref().map_or_else(Vec::new, |m| m.sequences());
        if let Some(m) = &self.merge { out.extend(m.sequences()); }
        if let Some(insert) = &self.insert {
            out.extend(insert.prepared.sequence_calls());
        }
        for cte in &self.ctes {
            if let Some(dml) = &cte.dml {
                out.extend(dml.sequence_calls());
            }
        }
        self.visit_queries(&mut |q| out.extend(q.sequence_dependencies()));
        self.visit_scalars(&mut |s| s.sequence_dependencies(&mut out));
        out
    }
    pub(super) fn visit_queries(&self, f: &mut impl FnMut(&Query)) {
        for cte in &self.ctes {
            cte.query.visit_queries(f);
        }
        match &self.body {
            Body::Select(s) => {
                f(&s.query);
                for apply in &s.applies {
                    apply.visit_queries(f);
                }
                if let Some(i) = &s.input {
                    i.query.visit_queries(f);
                }
                if let Some(from) = &s.from {
                    from.visit_queries(f);
                }
            }
            Body::Set { left, right, .. } => {
                left.visit_queries(f);
                right.visit_queries(f);
            }
            _ => {}
        }
    }
    pub(super) fn visit_scalars(&self, f: &mut impl FnMut(&Scalar)) {
        for cte in &self.ctes {
            cte.query.visit_scalars(f);
        }
        self.order.iter().for_each(|(s, _, _)| f(s));
        self.limit.iter().chain(&self.offset).for_each(&mut *f);
        if let Some(sample) = &self.decorations.sample { f(&sample.percent); if let Some(seed) = &sample.seed { f(seed); } }
        match &self.body {
            Body::Values(rows) => rows.iter().flatten().for_each(f),
            Body::Select(s) => {
                for apply in &s.applies {
                    apply.visit_scalars(f);
                }
                for window in &s.windows {
                    window.visit_scalars(f);
                }
                if let Some(i) = &s.input {
                    i.query.visit_scalars(f);
                }
                if let Some(from) = &s.from {
                    from.visit_scalars(f);
                }
            }
            Body::Set { left, right, .. } => {
                left.visit_scalars(f);
                right.visit_scalars(f);
            }
        }
    }
    pub(in crate::relational) fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) -> R<()> {
        if let Some(m) = &mut self.mutation { m.bind_schema_names(bind, state, path)?; }
        if let Some(m) = &mut self.merge { m.bind_schema_names(bind, state, path)?; }
        if let Some(insert) = &mut self.insert {
            insert.prepared.search_path = path.to_vec();
            insert.prepared = insert.prepared.bind_schemas(state)?;
        }
        for cte in &mut self.ctes {
            if let Some(dml) = &mut cte.dml {
                dml.search_path = path.to_vec();
                **dml = dml.bind_schemas(state)?;
            } else {
                cte.query.bind_schema_names(bind, state, path)?;
            }
        }
        for (s, _, _) in &mut self.order {
            schema_names::scalar(s, bind, state, path);
        }
        for s in self.limit.iter_mut().chain(&mut self.offset) {
            schema_names::scalar(s, bind, state, path);
        }
        if let Some(sample) = &mut self.decorations.sample {
            schema_names::scalar(&mut sample.percent, bind, state, path);
            if let Some(seed) = &mut sample.seed { schema_names::scalar(seed, bind, state, path); }
        }
        match &mut self.body {
            Body::Values(rows) => {
                for s in rows.iter_mut().flatten() {
                    schema_names::scalar(s, bind, state, path);
                }
            }
            Body::Select(s) => {
                for apply in &mut s.applies {
                    apply.bind_schema_names(bind, state, path)?;
                }
                for window in &mut s.windows {
                    window.map_scalars(&mut |s| {
                        schema_names::scalar(s, bind, state, path);
                        Ok(())
                    })?;
                }
                s.query.bind_schema_names(
                    &|name| {
                        if !name.starts_with("\0plan_") {
                            bind(name);
                        }
                    },
                    state,
                    path,
                );
                if let Some(i) = &mut s.input {
                    i.query.bind_schema_names(bind, state, path)?;
                }
                if let Some(from) = &mut s.from {
                    from.bind_schema_names(bind, state, path)?;
                }
            }
            Body::Set { left, right, .. } => {
                left.bind_schema_names(bind, state, path)?;
                right.bind_schema_names(bind, state, path)?;
            }
        }
        Ok(())
    }
}

impl Plan {
    pub(in crate::relational) fn has_privilege_inquiry(&self) -> bool {
        let mut found = false;
        self.visit_queries(&mut |q| found |= q.has_privilege_inquiry());
        self.visit_scalars(&mut |s| found |= s.has_privilege_inquiry());
        found || self.insert.as_ref().is_some_and(|p| p.prepared.has_privilege_inquiry())
            || self.mutation.as_ref().is_some_and(|m| m.has_privilege_inquiry())
            || self.merge.as_ref().is_some_and(|m| m.has_privilege_inquiry())
            || self.ctes.iter().any(|c| c.dml.as_ref().is_some_and(|p| p.has_privilege_inquiry()))
    }
}
