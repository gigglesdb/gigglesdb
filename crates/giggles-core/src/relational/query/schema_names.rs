use super::*;

impl Query {
    pub(in crate::relational) fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) {
        if let Some(Filter::Clauses(c)) = &mut self.filter
            && let Some(q) = &mut c.derived { q.bind_schema_names(bind, state, path); }
        for source in &mut self.sources {
            bind(&mut source.name);
        }
        for join in &mut self.joins {
            join.on.bind_schema_names(bind, state, path);
        }
        for (item, _) in &mut self.items {
            match item {
                Item::Scalar(s) | Item::Aggregate(_, Some(s)) => scalar(s, bind, state, path),
                _ => {}
            }
        }
        if let Some(filter) = &mut self.filter {
            filter.bind_schema_names(bind, state, path);
        }
        for (s, _, _) in &mut self.order {
            scalar(s, bind, state, path);
        }
    }
}

pub(super) fn scalar(
    s: &mut Scalar,
    bind: &impl Fn(&mut String),
    state: &Relational,
    path: &[String],
) {
    if let Scalar::Domain(_, name, target) = s {
        *target = state.resolve_type(name, path).and_then(|ty| {
            ty.map(|(_, ty)| ty.clone()).ok_or_else(|| {
                target
                    .as_ref()
                    .err()
                    .cloned()
                    .unwrap_or(SqlError::UndefinedObject)
            })
        });
    }
    match s {
        Scalar::Predicate(f) => f.bind_schema_names(bind, state, path),
        Scalar::Case(c) => {
            for (f, s) in &mut c.arms {
                f.bind_schema_names(bind, state, path);
                scalar(s, bind, state, path);
            }
            if let Some(s) = &mut c.otherwise {
                scalar(s, bind, state, path);
            }
        }
        Scalar::Unary(_, s) | Scalar::Cast(s, _) | Scalar::Domain(s, ..) => {
            scalar(s, bind, state, path)
        }
        Scalar::Binary(_, a, b) => {
            scalar(a, bind, state, path);
            scalar(b, bind, state, path);
        }
        Scalar::Call(_, args) => {
            for s in args {
                scalar(s, bind, state, path);
            }
        }
        Scalar::SubstringOrder(args) => {
            for s in args {
                scalar(s, bind, state, path);
            }
        }
        _ => {}
    }
}

impl InsertExpressions {
    pub(in crate::relational) fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) {
        bind(&mut self.name);
        self.bind_value_schema_names(bind, state, path);
    }
}

impl Filter {
    fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) {
        match self {
            Self::Clauses(c) => {
                if let Some(f) = &mut c.filter {
                    f.bind_schema_names(bind, state, path);
                }
                if let Some(f) = &mut c.having {
                    f.bind_schema_names(bind, state, path);
                }
                for s in &mut c.group {
                    scalar(s, bind, state, path);
                }
            }
            Self::Scalar(s) | Self::Null(s, _) => scalar(s, bind, state, path),
            Self::Compare(a, _, b) => {
                scalar(a, bind, state, path);
                scalar(b, bind, state, path);
            }
            Self::Exists { query, .. } => query.bind_schema_names(bind, state, path),
            Self::InSubquery { value, query, .. } => {
                scalar(value, bind, state, path);
                query.bind_schema_names(bind, state, path);
            }
            Self::Like {
                value,
                pattern,
                escape,
                ..
            } => {
                scalar(value, bind, state, path);
                scalar(pattern, bind, state, path);
                if let Some(s) = escape {
                    scalar(s, bind, state, path);
                }
            }
            Self::Not(f) | Self::Is(f, _, _) => f.bind_schema_names(bind, state, path),
            Self::Logic(a, _, b) => {
                a.bind_schema_names(bind, state, path);
                b.bind_schema_names(bind, state, path);
            }
        }
    }
}
