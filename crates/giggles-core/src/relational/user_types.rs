//! User types retain the existing row storage representation.
pub use super::catalog_ext::TypeDef;
use super::*;

thread_local! {
    static SQL_ERROR_MESSAGE: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}
/// Consume the diagnostic immediately after a synchronous SQL call on this thread.
/// SQLSTATE remains the stable, field-less SqlError contract.
pub fn take_sql_error_message() -> Option<String> {
    SQL_ERROR_MESSAGE.with(|message| message.borrow_mut().take())
}
pub(super) fn clear_sql_error_message() {
    take_sql_error_message();
}

/// Global catalog IDs give disjoint relation/type slots (65536 + id * 4).
pub fn type_oid(id: u64) -> R<u32> {
    u32::try_from(
        id.checked_mul(4)
            .and_then(|n| n.checked_add(65536))
            .ok_or(SqlError::Limit)?,
    )
    .ok()
    .filter(|v| *v <= u32::MAX - 3)
    .ok_or(SqlError::Limit)
}
#[derive(Debug, Clone)]
pub(super) enum Ddl {
    Create(TypeDef),
    Drop {
        name: String,
        domain: bool,
        optional: bool,
    },
}
impl Ddl {
    pub(super) fn command(&self) -> &'static str {
        match self {
            Self::Create(TypeDef::Domain { .. }) => "CREATE DOMAIN",
            Self::Create(_) => "CREATE TYPE",
            Self::Drop { domain: true, .. } => "DROP DOMAIN",
            Self::Drop { .. } => "DROP TYPE",
        }
    }
}
fn builtin(p: &mut Parser) -> R<(DataType, Option<i32>)> {
    let parsed = ddl_type::parse(p, ddl_type::Site::Create)?;
    if let Some(e) = parsed.semantic_error {
        return Err(e);
    }
    Ok((parsed.data_type, parsed.typmod))
}
fn end(p: &mut Parser) -> R<()> {
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Unsupported);
    }
    Ok(())
}
// Keep type references out of the table-reference authority/schema list.
pub(super) fn parse_name(p: &mut Parser) -> R<String> {
    let first = p.name()?;
    if p.sym('.') {
        Ok(format!("{first}.{}", p.name()?))
    } else {
        Ok(first)
    }
}
pub(super) fn claims(p: &Parser) -> bool {
    matches!(p.tokens.first(), Some(Token::Word(w)) if w == "create" || w == "drop")
        && matches!(p.tokens.get(1), Some(Token::Word(w)) if w == "type" || w == "domain")
}
pub(super) fn parse(p: &mut Parser) -> R<Ddl> {
    let create = p.word("create");
    if !create {
        p.need("drop")?;
    }
    let domain = p.word("domain");
    if !domain {
        p.need("type")?;
    }
    let optional = !create && p.word("if");
    if optional {
        p.need("exists")?;
    }
    let spelling = parse_name(p)?;
    let (schema, name) = spelling
        .split_once('.')
        .map_or((String::new(), spelling.clone()), |(s, n)| {
            (s.into(), n.into())
        });
    if !create {
        if p.word("cascade") {
            return Err(SqlError::Unsupported);
        }
        p.word("restrict");
        end(p)?;
        return Ok(Ddl::Drop {
            name: spelling,
            domain,
            optional,
        });
    }
    let ty = if domain {
        p.word("as");
        let (base, typmod) = builtin(p)?;
        let mut not_null = false;
        let mut check = None;
        let mut prefix = name.clone();
        while prefix.len() > 57 {
            prefix.pop();
        }
        let mut constraint = format!("{prefix}_check");
        loop {
            if p.word("not") {
                p.need("null")?;
                not_null = true;
            } else if p.word("null") {
            } else {
                let named = if p.word("constraint") {
                    Some(p.name()?)
                } else {
                    None
                };
                if !p.word("check") {
                    if named.is_some() {
                        return Err(SqlError::Unsupported);
                    }
                    break;
                }
                if check.is_some() {
                    return Err(SqlError::Unsupported);
                }
                if let Some(n) = named {
                    constraint = n;
                }
                p.punct('(')?;
                let start = p.at;
                let mut depth = 1;
                while depth > 0 {
                    match p.tokens.get(p.at) {
                        Some(Token::Symbol('(')) => depth += 1,
                        Some(Token::Symbol(')')) => depth -= 1,
                        None => return Err(SqlError::Syntax),
                        _ => (),
                    }
                    p.at += 1;
                }
                let expression = p.tokens[start..p.at - 1].to_vec();
                check = Some(query::constraint_deparse(&expression)?);
            }
        }
        TypeDef::Domain {
            name,
            schema,
            base,
            typmod,
            not_null,
            check,
            constraint,
        }
    } else {
        p.need("as")?;
        if p.word("enum") {
            p.punct('(')?;
            let mut labels = Vec::new();
            if !p.sym(')') {
                loop {
                    let Some(Token::String(label)) = p.tokens.get(p.at) else {
                        return Err(SqlError::Syntax);
                    };
                    labels.push(label.clone());
                    p.at += 1;
                    if !p.sym(',') {
                        p.punct(')')?;
                        break;
                    }
                }
            }
            TypeDef::Enum {
                name,
                schema,
                labels,
            }
        } else {
            p.punct('(')?;
            let mut fields = Vec::new();
            if !p.sym(')') {
                loop {
                    let name = p.name()?;
                    let (base, typmod) = builtin(p)?;
                    fields.push((name, base, typmod));
                    if !p.sym(',') {
                        p.punct(')')?;
                        break;
                    }
                }
            }
            TypeDef::Composite {
                name,
                schema,
                fields,
            }
        }
    };
    end(p)?;
    validate_type(&ty)?;
    Ok(Ddl::Create(ty))
}
fn domain_state(base: &DataType, typmod: Option<i32>) -> (Relational, TableDef) {
    let table = TableDef {
        id: 1,
        name: "domain_value".into(),
        columns: vec![ColumnDef {
            id: 2,
            name: "value".into(),
            data_type: base.clone(),
            not_null: false,
        }],
        primary_key: vec![],
    };
    let mut state = Relational::default();
    state.tables.insert(table.name.clone(), table.clone());
    if typmod.is_some() {
        state.ext.columns.entry(2).or_default().typmod = typmod;
    }
    (state, table)
}
pub(super) fn validate_type(ty: &TypeDef) -> R<()> {
    ident(ty.name())?;
    match ty {
        TypeDef::Enum { labels, .. } => {
            if labels.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            if labels.iter().collect::<BTreeSet<_>>().len() != labels.len() {
                return Err(SqlError::DuplicateObject);
            }
            if labels.iter().any(|s| s.len() > 63 || s.contains('\0')) {
                return Err(SqlError::InvalidParameterValue);
            }
        }
        TypeDef::Domain {
            base,
            typmod,
            check,
            constraint,
            ..
        } => {
            ident(constraint)?;
            if let Some(t) = typmod {
                validate_typmod(base, *t)?;
            }
            if let Some(expr) = check {
                if expr.len() > MAX_SQL {
                    return Err(SqlError::Limit);
                }
                let (state, table) = domain_state(base, *typmod);
                query::constraint_validate(&state, &table, expr)?;
            }
        }
        TypeDef::Composite { fields, .. } => {
            if fields.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            unique(fields.iter().map(|f| &f.0))?;
            for (name, ty, typmod) in fields {
                ident(name)?;
                if let Some(t) = typmod {
                    validate_typmod(ty, *t)?;
                }
            }
        }
    }
    Ok(())
}
pub(super) fn execute(state: &mut Relational, ddl: &Ddl, prepared: &Prepared) -> R<()> {
    match ddl {
        Ddl::Create(ty) => {
            let mut ty = ty.clone();
            let schema = if ty.schema().is_empty() {
                prepared.creation_schema(state)?
            } else {
                ty.schema().into()
            };
            if schema != "public" && !state.ext.schemas.contains_key(&schema) {
                return Err(SqlError::InvalidSchema);
            }
            ty.set_schema(schema);
            let ty = &ty;
            if state.ext.types.values().any(|t| t.name() == ty.name())
                || state.relation_name_taken(ty.name())
            {
                return Err(SqlError::DuplicateObject);
            }
            validate_type(ty)?;
            state.mutate_ext(|ext, next| {
                let id = allocate(next)?;
                type_oid(id)?;
                if let TypeDef::Enum { labels, .. } = ty {
                    for _ in labels {
                        type_oid(allocate(next)?)?;
                    }
                }
                ext.types.insert(id, ty.clone());
                Ok(())
            })?;
        }
        Ddl::Drop {
            name,
            domain,
            optional,
        } => {
            let Some((id, ty)) = state.resolve_type(name, &prepared.search_path)? else {
                return if *optional {
                    Ok(())
                } else {
                    Err(SqlError::UndefinedObject)
                };
            };
            if *domain && !matches!(ty, TypeDef::Domain { .. }) {
                return Err(SqlError::WrongObjectType);
            }
            if state.ext.columns.values().any(|c| c.type_id == Some(id)) {
                return Err(SqlError::DependentObjects);
            }
            state.mutate_ext(|ext, _| {
                ext.types.remove(&id);
                Ok(())
            })?;
        }
    }
    state.epoch = state.epoch.checked_add(1).ok_or(SqlError::Limit)?;
    Ok(())
}
impl Relational {
    pub(super) fn type_by_name(&self, name: &str) -> Option<(u64, &TypeDef)> {
        self.ext
            .types
            .iter()
            .find(|(_, ty)| ty.name() == name)
            .map(|(id, ty)| (*id, ty))
    }
    pub(super) fn resolve_type(&self, name: &str, path: &[String]) -> R<Option<(u64, &TypeDef)>> {
        let (schema, name) = name
            .split_once('.')
            .map_or((None, name), |(s, n)| (Some(s), n));
        if let Some(schema) = schema
            && schema != "public"
            && !self.ext.schemas.contains_key(schema)
        {
            return Err(SqlError::InvalidSchema);
        }
        Ok(self.type_by_name(name).filter(|(_, ty)| {
            schema.map_or_else(
                || path.iter().any(|s| s == ty.schema()),
                |s| s == ty.schema(),
            )
        }))
    }
    pub(super) fn column_type(&self, id: u64) -> Option<&TypeDef> {
        self.ext
            .columns
            .get(&id)
            .and_then(|c| c.type_id)
            .and_then(|id| self.ext.types.get(&id))
    }
}
pub(super) fn coerce(state: &Relational, column: &ColumnDef, value: &Value) -> R<()> {
    match state.column_type(column.id) {
        Some(TypeDef::Enum { labels, .. }) if *value != Value::Null => {
            if !matches!(value, Value::Text(v) if labels.contains(v)) {
                return Err(SqlError::InvalidTextRepresentation);
            }
        }
        Some(TypeDef::Domain {
            name,
            base,
            typmod,
            not_null,
            check,
            constraint,
            ..
        }) => {
            if *not_null && *value == Value::Null {
                return Err(SqlError::Null);
            }
            if let Some(expr) = check {
                let (state, table) = domain_state(base, *typmod);
                query::constraint_check(
                    &state,
                    &table,
                    expr,
                    &SqlRow {
                        values: vec![value.clone()],
                        version: 0,
                    },
                )
                .map_err(|e| {
                    if e == SqlError::CheckViolation {
                        {
                            SQL_ERROR_MESSAGE.with(|message| *message.borrow_mut() = Some(format!(
                                "value for domain {name} violates check constraint \"{constraint}\"")));
                            SqlError::CheckViolation
                        }
                    } else {
                        e
                    }
                })?;
            }
        }
        _ => (),
    }
    Ok(())
}

/// Deliberately bounded subset: standalone SELECT lists of ROW(literal/parameter,...)::type.
#[derive(Debug, Clone)]
pub(super) struct CompositeSelect(Vec<(Vec<Expr>, String, String)>);
impl CompositeSelect {
    pub(super) fn claims(p: &Parser) -> bool {
        matches!(p.tokens.get(1), Some(Token::Word(w)) if w == "row")
    }
    pub(super) fn parse(p: &mut Parser) -> R<Self> {
        p.need("select")?;
        let mut items = Vec::new();
        loop {
            p.need("row")?;
            p.punct('(')?;
            let mut values = Vec::new();
            if !p.sym(')') {
                loop {
                    values.push(p.expr()?);
                    if !p.sym(',') {
                        p.punct(')')?;
                        break;
                    }
                }
            }
            if !p.sym(':') {
                return Err(SqlError::Unsupported);
            }
            p.punct(':')?;
            let ty = parse_name(p)?;
            let alias = if p.word("as") {
                p.name()?
            } else {
                "row".into()
            };
            items.push((values, ty, alias));
            if !p.sym(',') {
                break;
            }
        }
        end(p)?;
        Ok(Self(items))
    }
    pub(super) fn describe(&self, state: &Relational, n: usize, path: &[String]) -> R<Description> {
        let mut parameters = vec![None; n];
        let mut columns = Vec::new();
        let mut type_oids = Vec::new();
        for (values, name, alias) in &self.0 {
            let (id, ty) = state
                .resolve_type(name, path)?
                .ok_or(SqlError::UndefinedObject)?;
            let TypeDef::Composite { fields, .. } = ty else {
                return Err(SqlError::CannotCoerce);
            };
            if values.len() != fields.len() {
                return Err(SqlError::CannotCoerce);
            }
            for (expr, (_, ty, _)) in values.iter().zip(fields) {
                if let Expr::Param(n) = expr {
                    let slot = &mut parameters[n - 1];
                    if slot.as_ref().is_some_and(|old| old != ty) {
                        return Err(SqlError::AmbiguousParameter);
                    }
                    *slot = Some(ty.clone());
                }
            }
            columns.push(ColumnDef {
                id: 0,
                name: alias.clone(),
                data_type: DataType::Text,
                not_null: false,
            });
            type_oids.push(type_oid(id)?);
        }
        Ok(Description {
            parameters: parameters
                .into_iter()
                .map(|p| p.ok_or(SqlError::Parameters))
                .collect::<R<_>>()?,
            typmods: vec![-1; columns.len()],
            columns,
            type_oids,
        })
    }
    pub(super) fn execute(
        &self,
        state: &Relational,
        params: &[Value],
        path: &[String],
    ) -> R<SqlResult> {
        self.describe(state, params.len(), path)?;
        let mut output = Vec::new();
        for (values, name, _) in &self.0 {
            let (_, TypeDef::Composite { fields, .. }) = state
                .resolve_type(name, path)?
                .ok_or(SqlError::UndefinedObject)?
            else {
                return Err(SqlError::CannotCoerce);
            };
            let mut parts = Vec::new();
            for (expr, (_, ty, typmod)) in values.iter().zip(fields) {
                let (domain_state, table) = domain_state(ty, *typmod);
                let value = query::assignment::literal(expr.value(params)?, ty, None)?;
                let value = coerce_to_column(&domain_state, &table.columns[0], value)?;
                let text = match value {
                    Value::Null => {
                        parts.push(String::new());
                        continue;
                    }
                    Value::Text(s) => s,
                    Value::Int(n) => n.to_string(),
                    Value::Bool(b) => if b { "t" } else { "f" }.into(),
                    _ => return Err(SqlError::Unsupported),
                };
                parts.push(
                    if text.is_empty()
                        || text
                            .chars()
                            .any(|c| c.is_whitespace() || matches!(c, ',' | '(' | ')' | '"' | '\\'))
                    {
                        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\"\""))
                    } else {
                        text
                    },
                );
            }
            output.push(Value::Text(format!("({})", parts.join(","))));
        }
        Ok(SqlResult {
            columns: self.0.iter().map(|i| i.2.clone()).collect(),
            rows: vec![SqlRow {
                values: output,
                version: 0,
            }],
            affected: 0,
        })
    }
}

/// Catalog type metadata from one SQL snapshot.
#[derive(Debug, Clone)]
pub struct UserTypeCatalog {
    pub epoch: u64,
    pub types: BTreeMap<u64, TypeDef>,
    pub column_types: BTreeMap<u64, u64>,
}
impl Relational {
    pub(crate) fn user_type_catalog(&self) -> UserTypeCatalog {
        UserTypeCatalog {
            epoch: self.epoch,
            types: self.ext.types.clone(),
            column_types: self
                .ext
                .columns
                .iter()
                .filter_map(|(id, c)| c.type_id.map(|ty| (*id, ty)))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::Options;
    #[test]
    fn catalog_ext_six_type_definition_scalar_tags_preserve_logical_kinds() {
        let codec = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .reject_trailing_bytes();
        for base in [
            DataType::Varchar,
            DataType::Bpchar,
            DataType::Uuid,
            DataType::Jsonb,
            DataType::IntArray,
            DataType::BigIntArray,
            DataType::TextArray,
        ] {
            let mut ext = CatalogExt::default();
            ext.types.insert(
                1,
                TypeDef::Domain {
                    name: "d".into(),
                    schema: "public".into(),
                    base: base.clone(),
                    typmod: None,
                    not_null: false,
                    check: None,
                    constraint: "d_check".into(),
                },
            );
            ext.types.insert(
                2,
                TypeDef::Composite {
                    name: "c".into(),
                    schema: "public".into(),
                    fields: vec![("v".into(), base, None)],
                },
            );
            ext.logical_kinds.insert(7, LogicalKind::Uuid);
            Relational::normalize_ext(&mut ext);
            let bytes = codec.serialize(&ext).unwrap();
            assert_eq!(codec.deserialize::<CatalogExt>(&bytes).unwrap(), ext);
            assert_eq!(&bytes[..4], &6u32.to_be_bytes());
        }
    }
    #[test]
    fn catalog_ext_six_enum_golden_and_legacy_column_layout() {
        let mut ext = CatalogExt::default();
        ext.types.insert(
            1,
            TypeDef::Enum {
                name: "e".into(),
                schema: "public".into(),
                labels: vec!["a".into(), "b".into()],
            },
        );
        Relational::normalize_ext(&mut ext);
        assert_eq!(ext.ext_version, 6);
        let codec = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .reject_trailing_bytes();
        let mut golden = 6u32.to_be_bytes().to_vec();
        for n in [0u64, 0, 0, 1, 1] {
            golden.extend(n.to_be_bytes());
        }
        golden.extend(0u32.to_be_bytes()); // Enum discriminant
        golden.extend(1u64.to_be_bytes());
        golden.push(b'e');
        golden.extend(6u64.to_be_bytes());
        golden.extend(b"public");
        golden.extend(2u64.to_be_bytes());
        for label in *b"ab" {
            golden.extend(1u64.to_be_bytes());
            golden.push(label);
        }
        golden.extend(0u64.to_be_bytes()); // v4 logical_kinds
        golden.extend(0u64.to_be_bytes()); // v5 schemas
        golden.extend(0u64.to_be_bytes()); // v5 table_schemas
        assert_eq!(codec.serialize(&ext).unwrap(), golden);
        assert_eq!(codec.deserialize::<CatalogExt>(&golden).unwrap(), ext);
        let mut invalid = golden;
        // A legacy version never accepts a nonempty type map.
        invalid[..4].copy_from_slice(&1u32.to_be_bytes());
        assert!(
            codec
                .deserialize::<CatalogExt>(&invalid)
                .unwrap_err()
                .to_string()
                .contains(UNSUPPORTED_CATALOG_EXT)
        );
        ext.columns.insert(
            7,
            ColumnExt {
                typmod: Some(9),
                type_id: Some(1),
                ..ColumnExt::default()
            },
        );
        let bytes = codec.serialize(&ext).unwrap();
        assert_eq!(codec.deserialize::<CatalogExt>(&bytes).unwrap(), ext);
    }
}
