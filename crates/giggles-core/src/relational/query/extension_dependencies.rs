//! Inspect parsed expressions, resolving extension function names in their schema.
use super::*;
pub(in crate::relational) fn extension_call(
    sql: &str,
    state: &Relational,
    path: &[String],
) -> R<Option<String>> {
    let mut p = Parser { privilege_seen: false, privilege_casts: false,
        tokens: lex(sql)?,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let s = Scalar::parse(&mut p, 0)?;
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(scalar(&s, state, path))
}
fn scalar(s: &Scalar, state: &Relational, path: &[String]) -> Option<String> {
    let mut found = None;
    walk_scalar(s, &mut |s| {
        call(s, state, path, &mut found);
        Ok(())
    })
    .expect("infallible expression visitor");
    found
}
fn call(s: &Scalar, state: &Relational, path: &[String], found: &mut Option<String>) {
    if found.is_none()
        && let Scalar::Call(n, _) = s
    {
        let mut n = n.to_string();
        functions::uuid_ossp::resolve_name(&mut n, state, path);
        if functions::uuid_ossp::known(&n) {
            *found = Some(n);
        }
    }
}
pub(in crate::relational) fn view_call(
    sql: &str,
    state: &Relational,
    path: &[String],
) -> R<Option<String>> {
    let mut p = Parser { privilege_seen: false, privilege_casts: false,
        tokens: lex(sql)?,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let q = Query::parse(&mut p)?;
    let mut found = None;
    q.walk_scalars(&mut |s| {
        call(s, state, path, &mut found);
        Ok(())
    })?;
    Ok(found)
}

impl Scalar {
    pub(in crate::relational::query) fn resolve_extension_default(&mut self) {
        match self {
            Self::Call(n, args) => {
                if let Some(raw) = n.strip_prefix("\0uuid-ossp:") {
                    *n = raw.rsplit('\0').next().unwrap_or(raw).into();
                }
                for a in args {
                    a.resolve_extension_default();
                }
            }
            Self::Cast(a, _) | Self::Domain(a, ..) | Self::Unary(_, a) => {
                a.resolve_extension_default()
            }
            _ => {}
        }
    }
}
