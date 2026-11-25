//! Stable collation dependencies. Values and keys retain their existing encoding.
use super::*;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Reference {
    Builtin(u32),
    User(u64),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Binding {
    Column(Reference),
    Index(#[serde(deserialize_with = "index_references")] Vec<Option<Reference>>),
    /// CASCADE may remove the final logical column. Keep an explicit feature
    /// witness for that dependency outcome while preserving the heap row codec.
    EmptyTable,
}
fn index_references<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<Option<Reference>>, D::Error> {
    struct V;
    impl<'de> serde::de::Visitor<'de> for V {
        type Value = Vec<Option<Reference>>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("bounded index collation references")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            if seq.size_hint().is_some_and(|n| n > MAX_COLUMNS) {
                return Err(serde::de::Error::custom("too many index collations"));
            }
            let mut out = Vec::new();
            while let Some(r) = seq.next_element()? {
                if out.len() == MAX_COLUMNS {
                    return Err(serde::de::Error::custom("too many index collations"));
                }
                out.push(r);
            }
            Ok(out)
        }
    }
    d.deserialize_seq(V)
}
const DATA: &str = include_str!("../../../giggles-icu/src/pg_collation.txt");
pub(super) fn parse_name(p: &mut Parser) -> R<String> {
    user_types::parse_name(p)
}
pub(crate) fn builtin(oid: u32) -> Option<String> {
    DATA.lines().find_map(|line| {
        let mut f = line.split('|');
        (f.next()?.parse::<u32>().ok()? == oid).then(|| f.next().unwrap().to_owned())
    })
}
pub(crate) fn resolve(state: &Relational, n: &str, path: &[String]) -> R<Reference> {
    if let Some((id, _)) = collations::resolve(state, n, path) {
        return Ok(Reference::User(id));
    }
    let n = n.strip_prefix("pg_catalog.").unwrap_or(n);
    if let Some(oid) = DATA.lines().find_map(|l| {
        let mut f = l.split('|');
        let oid = f.next()?;
        (f.next()? == n).then(|| oid.parse().unwrap())
    }) {
        return Ok(Reference::Builtin(oid));
    }
    user_types::view_replace_error(format!(
        "collation \"{n}\" for encoding \"UTF8\" does not exist"
    ));
    Err(SqlError::UndefinedObject)
}
pub(super) fn resolve_column(
    state: &Relational,
    c: &ColumnDef,
    type_id: Option<u64>,
    n: Option<&str>,
    path: &[String],
) -> R<Option<Reference>> {
    let Some(n) = n else {
        return Ok(None);
    };
    let reference = resolve(state, n, path)?;
    if !matches!(
        c.data_type,
        DataType::Text | DataType::Varchar | DataType::Bpchar
    ) || type_id
        .and_then(|id| state.ext.types.get(&id))
        .is_some_and(|t| matches!(t, TypeDef::Enum { .. }))
    {
        user_types::view_replace_error(format!(
            "collations are not supported by type {}",
            match c.data_type {
                DataType::Integer => "integer",
                DataType::BigInt => "bigint",
                DataType::Boolean => "boolean",
                _ => "non-collatable",
            }
        ));
        return Err(SqlError::Type);
    }
    // Explicit default is represented by absence, preserving the old bytes.
    Ok((reference != Reference::Builtin(100)).then_some(reference))
}
pub(crate) fn validate_binding(b: &Binding) -> R<()> {
    let valid = |r: &Reference| match r {
        Reference::Builtin(oid) => builtin(*oid).is_some(),
        Reference::User(id) => *id != 0,
    };
    match b {
        Binding::EmptyTable=>Ok(()),
        Binding::Column(r) if valid(r) && *r != Reference::Builtin(100) => Ok(()),
        Binding::Index(rs)
            if !rs.is_empty() && rs.len() <= MAX_COLUMNS && rs.iter().flatten().all(valid) =>
        {
            Ok(())
        }
        _ => Err(SqlError::Type),
    }
}
pub(crate) fn validate_bindings(
    state: &Relational,
    ext: &CatalogExt,
    excluded: Option<u64>,
) -> R<()> {
    for table in state.tables.values().filter(|t|Some(t.id)!=excluded) {
        if table.logical_columns().next().is_none() && !matches!(ext.column_collations.get(&table.id),Some(Binding::EmptyTable)) {return Err(SqlError::Type);}
    }
    for (id, b) in &ext.column_collations {
        validate_binding(b)?;
        let refs: Vec<_> = match b {
            Binding::EmptyTable=>{
                let t=state.tables.values().find(|t|t.id==*id && Some(t.id)!=excluded).ok_or(SqlError::Type)?;
                if t.hidden_rowid().is_none() || t.logical_columns().next().is_some() {return Err(SqlError::Type);}
                vec![]
            },
            Binding::Column(r) => {
                let c = state
                    .tables
                    .values()
                    .filter(|t| Some(t.id) != excluded)
                    .flat_map(|t| &t.columns)
                    .find(|c| c.id == *id)
                    .ok_or(SqlError::Type)?;
                if !matches!(
                    c.data_type,
                    DataType::Text | DataType::Varchar | DataType::Bpchar
                ) || ext
                    .columns
                    .get(id)
                    .and_then(|c| c.type_id)
                    .and_then(|id| ext.types.get(&id))
                    .is_some_and(|t| matches!(t, TypeDef::Enum { .. }))
                {
                    return Err(SqlError::Type);
                }
                vec![r]
            }
            Binding::Index(rs) => {
                if let Some(d) = state
                    .secondary
                    .descriptors()
                    .iter()
                    .find(|d| d.id == *id && Some(d.table) != excluded)
                    && d.columns.len() != rs.len() {
                    return Err(SqlError::Type);
                }
                rs.iter().flatten().collect()
            }
        };
        if refs
            .iter()
            .any(|r| matches!(r,Reference::User(id) if !ext.collations.contains_key(id)))
        {
            return Err(SqlError::Type);
        }
    }
    Ok(())
}
pub(crate) fn validate_index_bindings(
    ext: &CatalogExt,
    indexes: &[secondary_index::Descriptor],
) -> R<()> {
    for (id, b) in &ext.column_collations {
        if let Binding::Index(rs) = b {
            let d = indexes.iter().find(|d| d.id == *id).ok_or(SqlError::Type)?;
            if d.columns.len() != rs.len() {
                return Err(SqlError::Type);
            }
        }
    }
    Ok(())
}
impl Relational {
    pub(crate) fn column_collation(&self, id: u64) -> Option<&Reference> {
        match self.ext.column_collations.get(&id) {
            Some(Binding::Column(r)) => Some(r),
            _ => None,
        }
    }
    pub(crate) fn collation_name(&self, r: &Reference) -> R<String> {
        match r {
            Reference::Builtin(oid) => builtin(*oid).ok_or(SqlError::Type),
            Reference::User(id) => {
                let c = self.ext.collations.get(id).ok_or(SqlError::Type)?;
                Ok(format!(
                    "{}.{}\0{}\0{}\0{}\0{}",
                    c.schema,
                    c.name,
                    c.provider,
                    c.deterministic,
                    c.locale.as_deref().or(c.collate.as_deref()).unwrap_or(""),
                    c.rules.as_deref().unwrap_or("")
                ))
            }
        }
    }
    pub(crate) fn byte_ordered(&self, id: u64) -> bool {
        self.column_collation(id)
            .is_none_or(|r| self.reference_byte_ordered(r))
    }
    pub(crate) fn reference_byte_ordered(&self, r: &Reference) -> bool {
        let Ok(n) = self.collation_name(r) else {
            return false;
        };
        matches!(
            n.as_str(),
            "C" | "POSIX" | "C.utf8" | "default" | "ucs_basic" | "pg_c_utf8" | "pg_unicode_fast"
        )
    }
    pub(crate) fn deterministic_column(&self, id: u64) -> bool {
        !matches!(self.column_collation(id),Some(Reference::User(id)) if self.ext.collations.get(id).is_some_and(|c|!c.deterministic))
    }
    pub(super) fn byte_key_predicate(&self, column: &ColumnDef, op: &str) -> bool {
        self.deterministic_column(column.id)
            && (op == "=" || !column.data_type.is_array() && self.byte_ordered(column.id))
    }
    /// Catalog OIDs for both columns and explicitly collated index attributes.
    pub fn collation_oids(&self) -> R<BTreeMap<u64, Vec<u32>>> {
        let oid = |r: &Reference| match r {
            Reference::Builtin(o) => Ok(*o),
            Reference::User(id) => user_types::type_oid(*id),
        };
        self.ext
            .column_collations
            .iter()
            .map(|(id, b)| {
                Ok((
                    *id,
                    match b {
                        Binding::EmptyTable=>vec![],
                    Binding::Column(r) => vec![oid(r)?],
                        Binding::Index(rs) => rs
                            .iter()
                            .map(|r| r.as_ref().map(oid).transpose().map(|r| r.unwrap_or(0)))
                            .collect::<R<_>>()?,
                    },
                ))
            })
            .collect()
    }
}

pub(super) fn rewrite_into(mut tokens: Vec<Token>) -> R<Vec<Token>> {
    if !matches!(tokens.first(),Some(Token::Word(w)) if w=="select") {
        return Ok(tokens);
    }
    let mut depth = 0;
    let at = tokens.iter().position(|t| {
        match t {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth -= 1,
            _ => {}
        };
        depth == 0 && matches!(t,Token::Word(w) if w=="into")
    });
    let Some(at) = at else { return Ok(tokens) };
    let mut end = at + 1;
    if matches!(tokens.get(end),Some(Token::Word(w)) if w=="table") {
        end += 1;
    }
    let start = end;
    if !matches!(
        tokens.get(end),
        Some(Token::Word(_) | Token::QuotedIdent(_))
    ) {
        return Err(SqlError::Syntax);
    }
    end += 1;
    if tokens.get(end) == Some(&Token::Symbol('.')) {
        end += 2;
    }
    let mut result = vec![Token::Word("create".into()), Token::Word("table".into())];
    result.extend_from_slice(tokens.get(start..end).ok_or(SqlError::Syntax)?);
    result.push(Token::Word("as".into()));
    tokens.drain(at..end);
    result.extend(tokens);
    Ok(result)
}
