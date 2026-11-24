//! User types retain the existing row storage representation.
pub use super::catalog_ext::TypeDef;
use super::*;

/// Key values are supplied to a trusted consumer, which must authorize their disclosure.
pub struct DuplicateKeyDetail {
    pub table_id: u64,
    pub schema: String,
    pub table: String,
    pub constraint: String,
    pub values: String,
}

pub enum SqlErrorDetail {
    Text(String),
    DuplicateKey(DuplicateKeyDetail),
    ForeignKey { key: DuplicateKeyDetail, redacted: String },
}

type SqlNotice = (String, Option<String>, Option<&'static str>);
type SqlErrorMessage = (String, Option<String>);
thread_local! {
    static SQL_NOTICES: std::cell::RefCell<Vec<SqlNotice>> = const { std::cell::RefCell::new(Vec::new()) };
    static SQL_ERROR_HINT: std::cell::RefCell<Option<std::borrow::Cow<'static, str>>> = const { std::cell::RefCell::new(None) };
    static SQL_ERROR_DETAIL: std::cell::RefCell<Option<SqlErrorDetail>> = const { std::cell::RefCell::new(None) };
    static SQL_ERROR_MESSAGE: std::cell::RefCell<Option<SqlErrorMessage>> = const { std::cell::RefCell::new(None) };
}
/// Consume the diagnostic immediately after a synchronous SQL call on this thread.
/// SQLSTATE remains the stable, field-less SqlError contract.
pub fn take_sql_error_hint() -> Option<&'static str> {
    match take_sql_diagnostic_hint()? {
        std::borrow::Cow::Borrowed(hint) => Some(hint),
        std::borrow::Cow::Owned(_) => None,
    }
}
pub fn take_sql_diagnostic_hint() -> Option<std::borrow::Cow<'static, str>> {
    SQL_ERROR_HINT.with(|v| v.borrow_mut().take())
}
pub(super) fn sql_error_hint(hint: &'static str) {
    SQL_ERROR_HINT.with(|v| *v.borrow_mut() = Some(hint.into()));
}
pub(super) fn sql_error_hint_owned(hint: String) {
    SQL_ERROR_HINT.with(|v| *v.borrow_mut() = Some(hint.into()));
}
pub fn take_sql_error_detail() -> Option<String> {
    take_sql_diagnostic_detail().map(|detail| match detail {
        SqlErrorDetail::Text(text) => text,
        SqlErrorDetail::DuplicateKey(detail) | SqlErrorDetail::ForeignKey { key: detail, .. } => detail.values,
    })
}
pub fn take_sql_diagnostic_detail() -> Option<SqlErrorDetail> {
    SQL_ERROR_DETAIL.with(|v| v.borrow_mut().take())
}
/// Keep ordinary details and discard key values when the caller cannot read them.
pub fn retain_sql_duplicate_key_detail(allowed: impl FnOnce(&DuplicateKeyDetail) -> bool) {
    let detail = take_sql_diagnostic_detail();
    let key = match &detail {
        Some(SqlErrorDetail::DuplicateKey(key) | SqlErrorDetail::ForeignKey { key, .. }) => Some(key),
        _ => None,
    };
    let detail = if key.is_none_or(allowed) {
        detail
    } else if let Some(SqlErrorDetail::ForeignKey { redacted, .. }) = detail {
        Some(SqlErrorDetail::Text(redacted))
    } else {
        None
    };
    SQL_ERROR_DETAIL.with(|v| *v.borrow_mut() = detail);
}

pub(super) fn sql_duplicate_key_detail(detail: DuplicateKeyDetail) {
    SQL_ERROR_DETAIL.with(|v| *v.borrow_mut() = Some(SqlErrorDetail::DuplicateKey(detail)));
}
pub(super) fn sql_foreign_key_detail(key: DuplicateKeyDetail, redacted: String) {
    SQL_ERROR_DETAIL.with(|v| *v.borrow_mut() = Some(SqlErrorDetail::ForeignKey { key, redacted }));
}
pub(super) fn sql_error_detail(detail: String) {
    SQL_ERROR_DETAIL.with(|v| *v.borrow_mut() = Some(SqlErrorDetail::Text(detail)));
}
pub(super) fn type_name_syntax_error(near: &str, input: &str) {
    let message = format!(r#"syntax error at or near "{near}""#);
    let input = input.replace('\0', "\\0");
    let context = format!(r#"invalid type name "{input}""#);
    SQL_ERROR_MESSAGE.with(|pending| *pending.borrow_mut() = Some((message, Some(context))));
}
pub fn take_sql_error_message() -> Option<String> {
    take_sql_error_message_with_context().0
}
/// Consume an error's primary message and optional PostgreSQL context together.
pub fn take_sql_error_message_with_context() -> (Option<String>, Option<String>) {
    SQL_ERROR_MESSAGE.with(|pending| {
        pending.borrow_mut().take().map_or((None, None), |(message, context)| {
            (Some(message), context)
        })
    })
}
/// Keep intentionally swallowed failures from replacing the caller's diagnostic.
pub(super) fn probe_preserving_error_message<T, E>(probe: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
    let saved = SQL_ERROR_MESSAGE.with(|message| message.borrow().clone());
    let result = probe();
    if result.is_err() {
        SQL_ERROR_MESSAGE.with(|message| *message.borrow_mut() = saved);
    }
    result
}
pub(super) fn invalid_type_modifier_error() {
    view_replace_error("invalid type modifier".into());
}
pub fn take_sql_notices() -> Vec<SqlNotice> {
    SQL_NOTICES.with(|notices| std::mem::take(&mut *notices.borrow_mut()))
}
pub(super) fn view_recreate_error() {
    view_replace_error("view projection bindings unavailable; recreate the view".into());
}
pub(super) fn sql_notice_detail(message: String, detail: String, code: &'static str) {
    SQL_NOTICES.with(|notices| notices.borrow_mut().push((message, Some(detail), Some(code))));
}
pub(super) fn sql_notice(message: String, code: &'static str) {
    SQL_NOTICES.with(|notices| notices.borrow_mut().push((message, None, Some(code))));
}
pub(super) fn missing_object_notice(kind: &str, name: &str) {
    let name = name.trim_start_matches('\0');
    SQL_NOTICES.with(|notices| notices.borrow_mut().push((format!("{kind} \"{name}\" does not exist, skipping"), None, None)));
}
pub(super) fn view_missing_notice(name: &str) {
    let name = name.trim_start_matches('\0');
    SQL_NOTICES.with(|notices| notices.borrow_mut().push((format!("view \"{name}\" does not exist, skipping"), None, None)));
}
pub(super) fn duplicate_column_notice(column: &str, table: &str) {
    SQL_NOTICES.with(|notices| notices.borrow_mut().push((
        format!("column \"{column}\" of relation \"{table}\" already exists, skipping"),
        None,
        Some("42701"),
    )));
}
pub(super) fn view_replace_error(message: String) {
    SQL_ERROR_MESSAGE.with(|pending| *pending.borrow_mut() = Some((message, None)));
}
pub(super) fn truncate_reference_error() {
    view_replace_error("cannot truncate a table referenced in a foreign key constraint".into());
}
pub(super) fn clear_sql_error_message() {
    take_sql_notices();
    take_sql_error_hint();
    take_sql_diagnostic_detail();
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
    Alter(enum_alter::Alter),
    Drop {
        names: Vec<String>,
        cascade: bool,
        domain: bool,
        optional: bool,
    },
}
impl Ddl {
    pub(super) fn command(&self) -> &'static str {
        match self {
            Self::Create(TypeDef::Domain { .. }) => "CREATE DOMAIN",
            Self::Create(_) => "CREATE TYPE",
            Self::Alter(_) => "ALTER TYPE",
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
fn parse_catalog_name(p: &mut Parser) -> R<String> {
    let first = p.name()?;
    if p.sym('.') {
        Ok(format!("{first}\0{}", p.name()?))
    } else {
        Ok(first)
    }
}

#[derive(Debug, Clone)]
pub(super) struct TypeReference {
    pub name: String,
    pub array: bool,
}

impl TypeReference {
    pub(super) fn parse(p: &mut Parser) -> R<Self> {
        let name = parse_catalog_name(p)?;
        let mut array = false;
        while p.sym('[') {
            p.punct(']')?;
            array = true;
        }
        Ok(Self { name, array })
    }

    pub(super) fn sql(&self) -> String {
        let name = self.name.split('\0').map(quote_type_ident).collect::<Vec<_>>().join(".");
        if self.array { format!("{name}[]") } else { name }
    }

    pub(super) fn resolve<'a>(
        &self,
        state: &'a Relational,
        path: &[String],
    ) -> R<(u64, &'a TypeDef)> {
        state.resolve_type(&self.name, path)?.ok_or_else(|| {
            view_replace_error(format!("type \"{}\" does not exist", self.name.replace('\0', ".")));
            SqlError::UndefinedObject
        })
    }
}

pub(super) fn quote_type_ident(name: &str) -> String {
    query::quote_identifier(name)
}

pub(super) fn storage_type(ty: &TypeDef, array: bool) -> R<DataType> {
    if array {
        if matches!(ty, TypeDef::Enum { .. }) {
            Ok(DataType::TextArray)
        } else {
            Err(SqlError::Unsupported)
        }
    } else {
        ty.storage_type()
    }
}

pub(super) fn enum_input_error(name: &str, label: &str) -> SqlError {
    view_replace_error(format!(
        "invalid input value for enum {}: \"{label}\"",
        quote_type_ident(name)
    ));
    SqlError::InvalidTextRepresentation
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
    let spelling = parse_catalog_name(p)?;
    let (schema, name) = spelling
        .split_once('\0')
        .map_or((String::new(), spelling.clone()), |(s, n)| {
            (s.into(), n.into())
        });
    if !create {
        let mut names = vec![spelling];
        while p.sym(',') {
            names.push(parse_catalog_name(p)?);
        }
        let cascade = p.word("cascade");
        if !cascade {
            p.word("restrict");
        }
        end(p)?;
        return Ok(Ddl::Drop {
            names,
            cascade,
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
    std::sync::Arc::make_mut(&mut state.tables).insert(table.name.clone(), table.clone());
    if typmod.is_some() {
        std::sync::Arc::make_mut(&mut state.ext).columns.entry(2).or_default().typmod = typmod;
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
pub(super) fn execute(
    state: &mut Relational,
    ddl: &Ddl,
    prepared: &Prepared,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    match ddl {
        Ddl::Alter(ddl) => return enum_alter::execute(state, ddl, &prepared.search_path, cancel),
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
                || state.tables.values().any(|table| table.name == ty.name())
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
            enum_alter::created(ty);
        }
        Ddl::Drop {
            names,
            cascade,
            domain,
            optional,
        } => {
            let mut ids = BTreeSet::new();
            let mut notices = Vec::new();
            for name in names {
                let Some((id, ty)) = state.resolve_type(name, &prepared.search_path)? else {
                    if !optional {
                        return Err(SqlError::UndefinedObject);
                    }
                    notices.push((format!("type \"{name}\" does not exist, skipping"), None, None));
                    continue;
                };
                if *domain && !matches!(ty, TypeDef::Domain { .. }) {
                    return Err(SqlError::WrongObjectType);
                }
                ids.insert(id);
            }
            let columns: BTreeSet<_> = state
                .ext
                .columns
                .iter()
                .filter(|(_, c)| c.type_id.is_some_and(|id| ids.contains(&id)))
                .map(|(id, _)| *id)
                .collect();
            if !cascade && !columns.is_empty() {
                return Err(SqlError::DependentObjects);
            }
            let mut candidate = state.clone();
            let mut detail = Vec::new();
            let descriptors: Vec<_> = state
                .secondary_descriptors_engine()
                .iter()
                .filter(|d| !d.columns.iter().any(|id| columns.contains(id)))
                .cloned()
                .collect();
            for (key, original) in state.tables.iter() {
                if !original.columns.iter().any(|c| columns.contains(&c.id)) {
                    continue;
                }
                let mut table = original.clone();
                // The current table validator requires at least one logical column.
                if table.logical_columns().all(|c| columns.contains(&c.id)) {
                    view_replace_error("DROP TYPE CASCADE cannot leave a zero-column table".into());
                    return Err(SqlError::Unsupported);
                }
                let heap = table
                    .primary_key
                    .iter()
                    .any(|at| columns.contains(&table.columns[*at].id));
                let mut rows = candidate.rows.remove(&table.id).ok_or(SqlError::Type)?;
                for at in (0..table.columns.len()).rev() {
                    let column = &table.columns[at];
                    if !columns.contains(&column.id) {
                        continue;
                    }
                    detail.push(format!(
                        "drop cascades to column {} of table {}",
                        column.name,
                        if state.table_schema(table.id) == "public" {
                            table.name.clone()
                        } else {
                            format!("{}.{}", state.table_schema(table.id), table.name)
                        }
                    ));
                    candidate.drop_column_ext(column.id)?;
                    table.columns.remove(at);
                    table.primary_key.retain(|key| *key != at);
                    for key in &mut table.primary_key {
                        if *key > at {
                            *key -= 1;
                        }
                    }
                    for row in rows.values_mut() {
                        row.values.remove(at);
                    }
                }
                if heap {
                    table.primary_key = vec![table.columns.len()];
                    table.columns.push(ColumnDef {
                        id: allocate(&mut candidate.next_id)?,
                        name: HIDDEN_ROWID.into(),
                        data_type: DataType::BigInt,
                        not_null: true,
                    });
                    let mut rebuilt = BTreeMap::new();
                    for (n, mut row) in rows.into_values().enumerate() {
                        let id = Value::Int(i64::try_from(n + 1).map_err(|_| SqlError::Limit)?);
                        row.values.push(id.clone());
                        rebuilt.insert(encode_key(&[id])?, row);
                    }
                    rows = rebuilt;
                }
                std::sync::Arc::make_mut(&mut candidate.tables).insert(key.clone(), table.clone());
                let constraints = candidate
                    .ext
                    .constraints
                    .get(&table.id)
                    .cloned()
                    .unwrap_or_default();
                let retained = constraints
                    .into_iter()
                    .filter(|c| match &c.kind {
                        ConstraintKind::Check { expr, .. } => {
                            query::constraint_validate(&candidate, &table, expr).is_ok()
                        }
                        ConstraintKind::PrimaryKey => !heap,
                        _ => true,
                    })
                    .collect();
                std::sync::Arc::make_mut(&mut candidate.ext).constraints.insert(table.id, retained);
                candidate.rows.insert(table.id, rows);
            }
            for constraints in std::sync::Arc::make_mut(&mut candidate.ext).constraints.values_mut() {
                constraints.retain(|c| !matches!(&c.kind, ConstraintKind::ForeignKey { columns: local, ref_columns, .. } if local.iter().chain(ref_columns).any(|id| columns.contains(id))));
            }
            std::sync::Arc::make_mut(&mut candidate.ext).types.retain(|id, _| !ids.contains(id));
            Relational::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));
            candidate.secondary =
                candidate.secondary_build_complete_engine(&descriptors, &mut || false)?;
            if detail.len() == 1 {
                notices.push((detail[0].clone(), None, None));
            } else if !detail.is_empty() {
                notices.push((
                    format!("drop cascades to {} other objects", detail.len()),
                    Some(detail.join("\n")),
                    None,
                ));
            }
            *state = candidate;
            SQL_NOTICES.with(|pending| pending.borrow_mut().extend(notices));
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
            .split_once('\0')
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
        Some(ty) => coerce_user_type(ty, &column.data_type, value),
        None => Ok(()),
    }
}
pub(super) fn coerce_user_type(ty: &TypeDef, storage: &DataType, value: &Value) -> R<()> {
    match ty {
        TypeDef::Enum { labels, .. } if *value != Value::Null => {
            let Value::Text(text) = value else { return Err(SqlError::Type); };
            let validate = |label: &str| super::enum_alter::check_value(ty, labels, label);
            if *storage == DataType::TextArray {
                for label in logical_types::array_values(text)?.iter().flatten() {
                    validate(label)?;
                }
            } else {
                validate(text)?;
            }
        }
        TypeDef::Domain {
            name,
            base,
            typmod,
            not_null,
            check,
            constraint,
            ..
        } => {
            if *not_null && *value == Value::Null {
                view_replace_error(format!("domain {name} does not allow null values"));
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
                            view_replace_error(format!(
                                "value for domain {name} violates check constraint \"{constraint}\""
                            ));
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
            let ty = parse_catalog_name(p)?;
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
        clock: Option<&super::clock::ExecutionClock>,
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
                let value = query::assignment::literal(expr.value(params)?, ty, clock)?;
                let value = coerce_to_column(&domain_state, &table.columns[0], value)?;
                let text = query::output_text_with_clock(&value, clock)?;
                parts.push(text.map_or_else(String::new, |text| record_field(&text)));
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

fn record_field(text: &str) -> String {
    if text.is_empty()
        || text.chars().any(|c| c.is_whitespace() || matches!(c, ',' | '(' | ')' | '"' | '\\'))
    {
        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\"\""))
    } else {
        text.into()
    }
}

/// Catalog type metadata from one SQL snapshot.
#[derive(Debug, Clone)]
pub struct UserTypeCatalog {
    pub extensions: BTreeMap<String, super::extensions::InstalledExtension>,
    pub epoch: u64,
    pub types: BTreeMap<u64, TypeDef>,
    pub column_types: BTreeMap<u64, u64>,
    pub enum_array_columns: BTreeSet<u64>,
}
impl Relational {
    pub(crate) fn user_type_catalog(&self) -> UserTypeCatalog {
        UserTypeCatalog {
            extensions: self.ext.extensions.clone(),
            epoch: self.epoch,
            types: self.ext.types.clone(),
            enum_array_columns: self.ext.logical_kinds.iter().filter_map(|(&id, kind)| {
                (*kind == catalog_ext::LogicalKind::TextArray
                    && matches!(self.column_type(id), Some(TypeDef::Enum { .. }))).then_some(id)
            }).collect(),
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
    fn enum_array_round_trip_preserves_legacy_catalog_bytes() {
        fn run(state: &mut Relational, sql: &str) -> R<SqlResult> {
            state.execute(&Prepared::parse(sql, 1, state.epoch)?, &[], None)
        }

        let mut state = Relational::default();
        run(&mut state, r#"CREATE TYPE "Size" AS ENUM ('SMALL','LARGE')"#).unwrap();
        run(&mut state, r#"CREATE TABLE sizes (id int PRIMARY KEY, size "Size", tags text[])"#).unwrap();
        let existing_id = state.tables["sizes"].columns[2].id;
        // Old text-backed array defaults were serialized with a text cast.
        state.set_column_default(existing_id, Some("'{}'::text".into())).unwrap();
        let legacy = bincode::serialize(&state).unwrap();
        let mut reopened: Relational = bincode::deserialize(&legacy).unwrap();
        assert_eq!(bincode::serialize(&reopened).unwrap(), legacy);
        assert_eq!(reopened.ext.ext_version, 6);
        assert!(reopened.user_type_catalog().enum_array_columns.is_empty());
        run(&mut reopened, r#"INSERT INTO sizes (id, size) VALUES (1,'SMALL')"#).unwrap();

        run(&mut reopened, r#"ALTER TABLE sizes ADD COLUMN sizes "Size"[] DEFAULT ARRAY['LARGE']::"Size"[]"#).unwrap();
        let column_id = reopened.tables["sizes"].columns[3].id;
        let type_id = reopened.type_by_name("Size").unwrap().0;
        assert_eq!(reopened.ext.columns[&column_id].type_id, Some(type_id));
        assert_eq!(reopened.ext.logical_kinds[&column_id], catalog_ext::LogicalKind::TextArray);
        assert_eq!(reopened.ext.ext_version, 6);
        let new = bincode::serialize(&reopened).unwrap();
        let mut reopen_array: Relational = bincode::deserialize(&new).unwrap();
        assert_eq!(bincode::serialize(&reopen_array).unwrap(), new);
        assert_eq!(reopen_array.user_type_catalog().enum_array_columns, BTreeSet::from([column_id]));
        let rows = run(&mut reopen_array, "SELECT tags, sizes FROM sizes").unwrap().rows;
        assert_eq!(rows[0].values, vec![Value::Text("{}".into()), Value::Text("{LARGE}".into())]);
        run(&mut reopen_array, "ALTER TABLE sizes DROP COLUMN sizes").unwrap();
        assert!(reopen_array.user_type_catalog().enum_array_columns.is_empty());
    }

    #[test]
    fn swallowed_probe_restores_prior_message() {
        for saved in [None, Some("division by zero")] {
            SQL_ERROR_MESSAGE.with(|message| {
                *message.borrow_mut() = saved.map(|text| (text.into(), None));
            });
            let result: R<()> = probe_preserving_error_message(|| {
                super::super::logical_types::array_values("{a,b")?;
                Ok(())
            });
            assert_eq!(result, Err(SqlError::InvalidTextRepresentation));
            assert_eq!(take_sql_error_message().as_deref(), saved);
        }
        let result: R<()> = probe_preserving_error_message(|| {
            view_replace_error("division by zero".into());
            Ok(())
        });
        assert!(result.is_ok());
        assert_eq!(take_sql_error_message().as_deref(), Some("division by zero"));

        view_replace_error("division by zero".into());
        let result: R<()> = probe_preserving_error_message(|| {
            type_name_syntax_error("-", "timestamp(-1)");
            Err(SqlError::Syntax)
        });
        assert_eq!(result, Err(SqlError::Syntax));
        assert_eq!(take_sql_error_message_with_context(), (Some("division by zero".into()), None));
    }
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
