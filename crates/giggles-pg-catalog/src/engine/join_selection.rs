use super::*;

/// Only replace a root join's literal TRUE test. In particular, NULL ON must
/// never acquire an AND RHS: it would evaluate formerly discarded predicates.
pub(super) fn place(query: &mut Query, parameters: &[PgType]) {
    let Source::Join(_, _, on, false, _, false) = &mut query.source else {
        return;
    };
    if *on == truth() && eligible(&query.filter, query.source_width, parameters) {
        *on = std::mem::replace(&mut query.filter, truth());
    }
}

fn primitive(ty: PgType) -> bool {
    matches!(
        ty,
        PgType::Bool
            | PgType::Int2
            | PgType::Int4
            | PgType::Int8
            | PgType::Oid
            | PgType::Text
            | PgType::Name
            | PgType::Char
    )
}

fn operand(e: &E, width: usize, parameters: &[PgType]) -> bool {
    primitive(e.ty)
        && match &e.kind {
            K::Literal(Value::Null | Value::Bool(_) | Value::Int(_) | Value::Text(_)) => true,
            K::Column(i) => *i < width,
            // A later use can promote the global parameter type. Keep the old
            // placement if this leaf would still cast to a different type.
            K::Param(i) => parameters.get(*i) == Some(&e.ty),
            _ => false,
        }
}

fn eligible(e: &E, width: usize, parameters: &[PgType]) -> bool {
    match &e.kind {
        K::Binary(l, Op::And, r) => {
            eligible(l, width, parameters) && eligible(r, width, parameters)
        }
        K::Binary(l, Op::Eq, r) => operand(l, width, parameters) && operand(r, width, parameters),
        _ => false,
    }
}
