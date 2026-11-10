use super::*;

impl Query {
    pub(in crate::relational) fn bind_schema_names(&mut self, bind: &impl Fn(&mut String)) {
        for source in &mut self.sources {
            bind(&mut source.name);
        }
        for join in &mut self.joins {
            join.on.bind_schema_names(bind);
        }
        for (item, _) in &mut self.items {
            match item {
                Item::Scalar(s) | Item::Aggregate(_, Some(s)) => scalar(s, bind),
                _ => {}
            }
        }
        if let Some(filter) = &mut self.filter {
            filter.bind_schema_names(bind);
        }
        for (s, _, _) in &mut self.order {
            scalar(s, bind);
        }
    }
}

pub(super) fn scalar(s: &mut Scalar, bind: &impl Fn(&mut String)) {
    match s {
        Scalar::Predicate(f) => f.bind_schema_names(bind),
        Scalar::Case(c) => {
            for (f, s) in &mut c.arms {
                f.bind_schema_names(bind);
                scalar(s, bind);
            }
            if let Some(s) = &mut c.otherwise {
                scalar(s, bind);
            }
        }
        Scalar::Unary(_, s) | Scalar::Cast(s, _) => scalar(s, bind),
        Scalar::Binary(_, a, b) => {
            scalar(a, bind);
            scalar(b, bind);
        }
        Scalar::Call(_, args) => {
            for s in args {
                scalar(s, bind);
            }
        }
        Scalar::SubstringOrder(args) => {
            for s in args {
                scalar(s, bind);
            }
        }
        _ => {}
    }
}

impl InsertExpressions {
    pub(in crate::relational) fn bind_schema_names(&mut self, bind: &impl Fn(&mut String)) {
        bind(&mut self.name);
        self.bind_value_schema_names(bind);
    }
}

impl Filter {
    fn bind_schema_names(&mut self, bind: &impl Fn(&mut String)) {
        match self {
            Self::Clauses(c) => {
                if let Some(f) = &mut c.filter {
                    f.bind_schema_names(bind);
                }
                if let Some(f) = &mut c.having {
                    f.bind_schema_names(bind);
                }
                for s in &mut c.group {
                    scalar(s, bind);
                }
            }
            Self::Scalar(s) | Self::Null(s, _) => scalar(s, bind),
            Self::Compare(a, _, b) => {
                scalar(a, bind);
                scalar(b, bind);
            }
            Self::InSubquery { value, query, .. } => {
                scalar(value, bind);
                query.bind_schema_names(bind);
            }
            Self::Like {
                value,
                pattern,
                escape,
                ..
            } => {
                scalar(value, bind);
                scalar(pattern, bind);
                if let Some(s) = escape {
                    scalar(s, bind);
                }
            }
            Self::Not(f) | Self::Is(f, _, _) => f.bind_schema_names(bind),
            Self::Logic(a, _, b) => {
                a.bind_schema_names(bind);
                b.bind_schema_names(bind);
            }
        }
    }
}
