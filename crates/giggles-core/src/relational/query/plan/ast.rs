use super::*;

/// A query expression owns its pagination boundary; nested operands keep theirs.
#[derive(Debug, Clone)]
pub(in crate::relational) struct Plan {
    pub(super) body: Body,
    pub(super) order: Vec<(Scalar, bool, bool)>,
    pub(super) limit: Option<Scalar>,
    pub(super) offset: Option<Scalar>,
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
    pub query: Query,
    pub parameters: BTreeSet<usize>,
    // A materialized input exposes operator outputs as ordinary scalar columns.
    // Later operators (windows, apply, CTEs) can use the same boundary.
    pub input: Option<Input>,
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
            order: vec![],
            limit: None,
            offset: None,
        }
    }
    pub(in crate::relational) fn read_tables(&self) -> Vec<String> {
        let mut out = vec![];
        self.visit_queries(&mut |q| {
            for name in q.read_tables() {
                if !name.starts_with("\0plan_") && !out.contains(&name) {
                    out.push(name);
                }
            }
        });
        out
    }
    pub(in crate::relational) fn authorization_tables(&self) -> R<Vec<String>> {
        let mut out = vec![];
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
        error.map_or(Ok(out), Err)
    }
    pub(in crate::relational) fn sequences(&self) -> Vec<Option<String>> {
        let mut out = vec![];
        self.visit_queries(&mut |q| out.extend(q.sequence_dependencies()));
        self.visit_scalars(&mut |s| s.sequence_dependencies(&mut out));
        out
    }
    fn visit_queries(&self, f: &mut impl FnMut(&Query)) {
        match &self.body {
            Body::Select(s) => {
                f(&s.query);
                if let Some(i) = &s.input {
                    i.query.visit_queries(f);
                }
            }
            Body::Set { left, right, .. } => {
                left.visit_queries(f);
                right.visit_queries(f);
            }
            _ => {}
        }
    }
    fn visit_scalars(&self, f: &mut impl FnMut(&Scalar)) {
        self.order.iter().for_each(|(s, _, _)| f(s));
        self.limit.iter().chain(&self.offset).for_each(&mut *f);
        match &self.body {
            Body::Values(rows) => rows.iter().flatten().for_each(f),
            Body::Select(s) => {
                if let Some(i) = &s.input {
                    i.query.visit_scalars(f);
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
    ) {
        for (s, _, _) in &mut self.order {
            schema_names::scalar(s, bind, state, path);
        }
        for s in self.limit.iter_mut().chain(&mut self.offset) {
            schema_names::scalar(s, bind, state, path);
        }
        match &mut self.body {
            Body::Values(rows) => {
                for s in rows.iter_mut().flatten() {
                    schema_names::scalar(s, bind, state, path);
                }
            }
            Body::Select(s) => {
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
                    i.query.bind_schema_names(bind, state, path);
                }
            }
            Body::Set { left, right, .. } => {
                left.bind_schema_names(bind, state, path);
                right.bind_schema_names(bind, state, path);
            }
        }
    }
}
