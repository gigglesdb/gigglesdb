//! Bounded default syntax, assignment validation, and PostgreSQL-style stored text.
use super::*;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

thread_local! {
    // Only syntax is cached: catalog lookup and evaluation stay with the caller.
    static DEPENDENCIES: RefCell<HashMap<String, Rc<[String]>>> = RefCell::new(HashMap::new());
    static QUALIFIED_DEFAULTS: RefCell<HashMap<String, Rc<str>>> = RefCell::new(HashMap::new());
}

fn cached_syntax<T: Clone>(
    cache: &'static std::thread::LocalKey<RefCell<HashMap<String, T>>>,
    sql: &str,
    parse: impl FnOnce() -> R<T>,
) -> R<T> {
    if let Some(value) = cache.with(|cache| cache.borrow().get(sql).cloned()) {
        return Ok(value);
    }
    let value = parse()?;
    cache.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= 512 {
            cache.clear();
        }
        cache.insert(sql.into(), value.clone());
    });
    Ok(value)
}

#[derive(Debug, Clone)]
pub(crate) enum SequenceDdl {
    Create {
        name: String,
        optional: bool,
        spec: SequenceSpec,
        resolution_error: Option<SqlError>,
    },
    Alter {
        name: String,
        optional: bool,
        spec: SequenceSpec,
        resolution_error: Option<SqlError>,
    },
    Drop {
        names: Vec<String>,
        optional: bool,
        cascade: bool,
        resolution_error: Option<SqlError>,
    },
}
impl SequenceDdl {
    pub(crate) fn create_native(name: String) -> Self {
        Self::Create { name, optional: false, spec: SequenceSpec::default(), resolution_error: None }
    }
    pub(crate) fn drop_native(name: String, cascade: bool) -> Self {
        Self::Drop { names: vec![name], optional: false, cascade, resolution_error: None }
    }
    pub(super) fn bind_schema_names(
        &mut self,
        read: &impl Fn(&mut String),
        create: &impl Fn(&str) -> R<String>,
    ) -> R<()> {
        match self {
            Self::Create { name, spec, .. } => {
                *name = create(name)?;
                if let Some(Some((table, _))) = &mut spec.owned_by {
                    read(table);
                }
            }
            Self::Alter { name, spec, .. } => {
                read(name);
                if let Some(Some((table, _))) = &mut spec.owned_by {
                    read(table);
                }
            }
            Self::Drop { names, .. } => {
                for name in names {
                    read(name);
                }
            }
        }
        Ok(())
    }

    pub(super) fn copies_counter(&self) -> bool {
        matches!(self, Self::Alter { spec, .. } if spec.restart.is_none() && spec.rewrites_counter())
    }
    pub(super) fn names(&self) -> Vec<&str> {
        match self {
            Self::Create { name, .. } | Self::Alter { name, .. } => vec![name],
            Self::Drop { names, .. } => names.iter().map(String::as_str).collect(),
        }
    }
    pub(super) fn owned_by_table(&self) -> Option<&str> {
        match self {
            Self::Create { spec, .. } | Self::Alter { spec, .. } => spec
                .owned_by
                .as_ref()
                .and_then(|owner| owner.as_ref())
                .map(|(table, _)| table.as_str()),
            _ => None,
        }
    }
}

pub(super) fn sequence_ddl(p: &mut Parser) -> R<SequenceDdl> {
    let create = p.word("create");
    let alter = !create && p.word("alter");
    if !create && !alter {
        p.need("drop")?;
    }
    p.need("sequence")?;
    let optional = if p.word("if") {
        if create {
            p.need("not")?;
        }
        p.need("exists")?;
        true
    } else {
        false
    };
    // Preserve the raw target for authorization before reporting schema policy.
    let (name, mut resolution_error) = sequence_name(p, create)?;
    let result = if create || alter {
        let mut spec = SequenceSpec::default();
        options(p, &mut spec, false, alter)?;
        if create {
            SequenceDdl::Create {
                name,
                optional,
                spec,
                resolution_error,
            }
        } else {
            SequenceDdl::Alter {
                name,
                optional,
                spec,
                resolution_error,
            }
        }
    } else {
        let mut names = vec![name];
        while p.sym(',') {
            let (name, error) = sequence_name(p, false)?;
            names.push(name);
            resolution_error = resolution_error.or(error);
        }
        let cascade = p.word("cascade");
        if !cascade {
            p.word("restrict");
        }
        SequenceDdl::Drop {
            names,
            optional,
            cascade,
            resolution_error,
        }
    };
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Unsupported);
    }
    Ok(result)
}
fn sequence_name(p: &mut Parser, create: bool) -> R<(String, Option<SqlError>)> {
    let first = alter_table::raw_name(p)?;
    let (schema, name) = if p.sym('.') {
        (Some(first), alter_table::raw_name(p)?)
    } else {
        (None, first)
    };
    let error = ident::ident(&name).err();
    p.relations.push(schemas::Reference {
        name: name.clone(),
        schema: schema.clone(),
        create,
    });
    Ok((
        schema.map_or(name.clone(), |s| format!("{s}\0{name}")),
        error,
    ))
}

/// The same option parser is used by identity and standalone sequence DDL.
fn options(p: &mut Parser, spec: &mut SequenceSpec, parenthesized: bool, alter: bool) -> R<()> {
    let mut seen = BTreeSet::new();
    loop {
        if parenthesized && p.sym(')') {
            break;
        }
        if !parenthesized
            && (p.at == p.tokens.len() || matches!(p.tokens.get(p.at), Some(Token::Symbol(';'))))
        {
            break;
        }
        let key = if p.word("start") {
            p.word("with");
            spec.start = Some(integer(p)?);
            "start"
        } else if p.word("increment") {
            p.word("by");
            spec.increment = Some(integer(p)?);
            "increment"
        } else if p.word("minvalue") {
            spec.min = Some(integer(p)?);
            "min"
        } else if p.word("maxvalue") {
            spec.max = Some(integer(p)?);
            "max"
        } else if p.word("cache") {
            spec.cache = Some(integer(p)?);
            "cache"
        } else if p.word("cycle") {
            spec.cycle = Some(true);
            "cycle"
        } else if p.word("no") {
            if p.word("cycle") {
                spec.cycle = Some(false);
                "cycle"
            } else if p.word("minvalue") {
                spec.no_min = true;
                "min"
            } else if p.word("maxvalue") {
                spec.no_max = true;
                "max"
            } else {
                return Err(SqlError::Unsupported);
            }
        } else if !parenthesized && p.word("as") {
            spec.data_type = Some(if p.word("smallint") {
                DataType::SmallInt
            } else if p.word("integer") || p.word("int") {
                DataType::Integer
            } else if p.word("bigint") {
                DataType::BigInt
            } else {
                return Err(SqlError::InvalidParameterValue);
            });
            "as"
        } else if alter && p.word("restart") {
            let with = p.word("with");
            let value =
                if with || matches!(p.tokens.get(p.at), Some(Token::Number(_) | Token::Op(_))) {
                    Some(integer(p)?)
                } else {
                    None
                };
            spec.restart = Some(value);
            "restart"
        } else if !parenthesized && p.word("owned") {
            p.need("by")?;
            spec.owned_by = Some(if p.word("none") {
                None
            } else {
                let first = alter_table::raw_name(p)?;
                p.punct('.')?;
                let second = alter_table::raw_name(p)?;
                if p.sym('.') {
                    p.relations.push(schemas::Reference::read(
                        second.clone(),
                        Some(first.clone()),
                    ));
                    let column = alter_table::raw_name(p)?;
                    spec.owned_by_error = spec
                        .owned_by_error
                        .take()
                        .or_else(|| ident::ident(&second).err())
                        .or_else(|| column_ident(&column).err());
                    Some((format!("{first}\0{second}"), column))
                } else {
                    p.relations
                        .push(schemas::Reference::read(first.clone(), None));
                    spec.owned_by_error = ident::ident(&first)
                        .err()
                        .or_else(|| column_ident(&second).err());
                    Some((first, second))
                }
            });
            "owned"
        } else {
            return Err(SqlError::Unsupported);
        };
        if !seen.insert(key) {
            return Err(SqlError::Syntax);
        }
    }
    if alter && seen.is_empty() {
        return Err(SqlError::Syntax);
    }
    Ok(())
}

fn type_bounds(ty: &DataType) -> R<(i64, i64)> {
    match ty {
        DataType::SmallInt => Ok((i64::from(i16::MIN), i64::from(i16::MAX))),
        DataType::Integer => Ok((i64::from(i32::MIN), i64::from(i32::MAX))),
        DataType::BigInt => Ok((i64::MIN, i64::MAX)),
        _ => Err(SqlError::InvalidParameterValue),
    }
}
fn validate_sequence(
    ty: &DataType,
    increment: i64,
    min: i64,
    max: i64,
    start: i64,
    cache: i64,
) -> R<()> {
    let (lo, hi) = type_bounds(ty)?;
    if increment == 0 {
        return Err(SqlError::InvalidParameterValue);
    }
    if min < lo || min > hi {
        return Err(SqlError::InvalidParameterValue);
    }
    if max < lo || max > hi {
        return Err(SqlError::InvalidParameterValue);
    }
    if min >= max {
        return Err(SqlError::InvalidParameterValue);
    }
    if start < min {
        return Err(SqlError::InvalidParameterValue);
    }
    if start > max {
        return Err(SqlError::InvalidParameterValue);
    }
    if cache <= 0 {
        return Err(SqlError::InvalidParameterValue);
    }
    Ok(())
}

fn resolve_owner(state: &Relational, owner: &Option<(String, String)>) -> R<Option<(u64, u64)>> {
    let Some((table_name, column_name)) = owner else {
        return Ok(None);
    };
    let Some(table) = state.tables.get(table_name) else {
        return Err(SqlError::UnknownTable);
    };
    let Some(column) = table
        .columns
        .iter()
        .find(|column| column.name == *column_name)
    else {
        return Err(SqlError::UnknownColumn);
    };
    Ok(Some((table.id, column.id)))
}
fn restart_bounds(value: i64, min: i64, max: i64) -> R<()> {
    if value < min {
        return Err(SqlError::InvalidParameterValue);
    }
    if value > max {
        return Err(SqlError::InvalidParameterValue);
    }
    Ok(())
}
impl SequenceDdl {
    pub(super) fn restart_seed(&self, state: &Relational) -> Option<(u64, i64)> {
        let Self::Alter { name, spec, .. } = self else {
            return None;
        };
        let restart = spec.restart?;
        state
            .ext
            .sequences
            .iter()
            .find(|(_, seq)| seq.name == *name)
            .map(|(&id, seq)| (id, restart.unwrap_or(seq.start)))
    }
    pub(super) fn validate_current(
        &self,
        state: &Relational,
        current: impl Fn(u64) -> Option<i64>,
    ) -> R<()> {
        if let Self::Alter { name, spec, .. } = self
            && spec.restart.is_none()
            && let Some((&id, seq)) = state
                .ext
                .sequences
                .iter()
                .find(|(_, seq)| seq.name == *name)
        {
            restart_bounds(current(id).unwrap_or(seq.start), seq.min, seq.max)?;
        }
        Ok(())
    }
}
pub(super) fn execute_sequence(state: &mut Relational, ddl: &SequenceDdl) -> R<u64> {
    let resolution_error = match ddl {
        SequenceDdl::Create {
            resolution_error, ..
        }
        | SequenceDdl::Alter {
            resolution_error, ..
        }
        | SequenceDdl::Drop {
            resolution_error, ..
        } => resolution_error,
    };
    if let Some(error) = resolution_error {
        return Err(error.clone());
    }
    let mut candidate = state.clone();
    let mut changed = false;
    match ddl {
        SequenceDdl::Create {
            name,
            optional,
            spec,
            ..
        } => {
            if candidate.relation_name_taken(name) {
                if *optional {
                    return Ok(0);
                }
                return Err(SqlError::DuplicateTable);
            }
            if let Some(error) = &spec.owned_by_error {
                return Err(error.clone());
            }
            let owner = resolve_owner(&candidate, spec.owned_by.as_ref().unwrap_or(&None))?;
            let definition = sequence_definition(
                spec,
                spec.data_type.as_ref().unwrap_or(&DataType::BigInt),
                name.clone(),
                owner,
            )?;
            candidate.add_sequence(definition)?;
            changed = true;
        }
        SequenceDdl::Alter {
            name,
            optional,
            spec,
            ..
        } => {
            let Some((&id, original)) = candidate
                .ext
                .sequences
                .iter()
                .find(|(_, seq)| seq.name == *name)
            else {
                if candidate.relation_name_taken(name) {
                    return Err(SqlError::WrongObjectType);
                }

                if *optional {
                    return Ok(0);
                }
                return Err(SqlError::UnknownTable);
            };
            let original = original.clone();
            let ty = spec.data_type.as_ref().unwrap_or(&original.data_type);
            let (lo, hi) = type_bounds(ty)?;
            let increment = spec.increment.unwrap_or(original.increment);
            let old_bounds = type_bounds(&original.data_type)?;
            let changed_type = ty != &original.data_type;
            let min = if spec.no_min {
                None
            } else {
                Some(
                    spec.min
                        .unwrap_or(if changed_type && original.min == old_bounds.0 {
                            lo
                        } else {
                            original.min
                        }),
                )
            };
            let max = if spec.no_max {
                None
            } else {
                Some(
                    spec.max
                        .unwrap_or(if changed_type && original.max == old_bounds.1 {
                            hi
                        } else {
                            original.max
                        }),
                )
            };
            let effective = SequenceSpec {
                identity: if original.identity { Some(false) } else { None },
                start: Some(spec.start.unwrap_or(original.start)),
                increment: Some(increment),
                min,
                max,
                cache: Some(spec.cache.unwrap_or(original.cache)),
                cycle: Some(spec.cycle.unwrap_or(original.cycle)),
                ..Default::default()
            };
            if original.identity && spec.owned_by.is_some() {
                return Err(SqlError::Unsupported);
            }
            if let Some(error) = &spec.owned_by_error {
                return Err(error.clone());
            }
            let owner = if let Some(owner) = &spec.owned_by {
                resolve_owner(&candidate, owner)?
            } else {
                original.owned_by
            };
            let mut definition = sequence_definition(&effective, ty, name.clone(), owner)?;
            if let Some(restart) = spec.restart {
                restart_bounds(
                    restart.unwrap_or(definition.start),
                    definition.min,
                    definition.max,
                )?;
            } else if !spec.rewrites_counter() {
                definition.incarnation = original.incarnation;
            }
            candidate.mutate_ext(|ext, _| {
                ext.sequences.insert(id, definition);
                Ok(())
            })?;
            changed = true;
        }
        SequenceDdl::Drop {
            names,
            optional,
            cascade,
            ..
        } => {
            // Resolve the whole list against a private candidate: failure is atomic.
            for name in names {
                if let Err(error) = candidate.require_relation_schema(name) {
                    if *optional { user_types::missing_object_notice("schema", split_key(name).0); continue; }
                    return Err(error);
                }
                let Some((&id, seq)) = candidate
                    .ext
                    .sequences
                    .iter()
                    .find(|(_, seq)| seq.name == *name)
                else {
                    if candidate.relation_name_taken(name) {
                        return Err(SqlError::WrongObjectType);
                    }

                    if *optional {
                        user_types::missing_object_notice("sequence", split_key(name).1);
                        continue;
                    }
                    user_types::view_replace_error(format!("sequence \"{}\" does not exist", split_key(name).1.trim_start_matches('\0')));
                    return Err(SqlError::UnknownTable);
                };
                if seq.identity {
                    return Err(SqlError::DependentObjects);
                }
                let mut dependent = vec![];
                for table in candidate.tables.values() {
                    for column in &table.columns {
                        if let Some(sql) = candidate.column_default(column.id)
                            && dependencies(sql)?.iter().any(|dependency| {
                                let Ok(tokens) = lex(dependency) else {
                                    return false;
                                };
                                let mut parser = Parser { privilege_seen: false, privilege_casts: false,
                                    tokens,
                                    at: 0,
                                    params: BTreeSet::new(),
                                    relations: Vec::new(),
                                };
                                parser.table_name().is_ok_and(|dependency| {
                                    relation_key(split_key(&dependency).0, split_key(&dependency).1)
                                        == *name
                                        && parser.at == parser.tokens.len()
                                })
                            })
                        {
                            dependent.push(column.id);
                        }
                    }
                }
                if !*cascade && !dependent.is_empty() {
                    return Err(SqlError::DependentObjects);
                }
                for column_id in dependent {
                    candidate.set_column_default(column_id, None)?;
                }
                candidate.remove_sequence(id)?;
                changed = true;
            }
        }
    }
    if changed {
        candidate.epoch = candidate.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
        *state = candidate;
    }
    Ok(u64::from(changed))
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SequenceSpec {
    pub identity: Option<bool>,
    data_type: Option<DataType>,
    start: Option<i64>,
    increment: Option<i64>,
    min: Option<i64>,
    max: Option<i64>,
    cache: Option<i64>,
    cycle: Option<bool>,
    no_min: bool,
    no_max: bool,
    restart: Option<Option<i64>>,
    owned_by: Option<Option<(String, String)>>,
    owned_by_error: Option<SqlError>,
}
impl SequenceSpec {
    pub(crate) fn serial() -> Self { Self::default() }
    pub(crate) fn identity_default(always: bool) -> Self {
        Self { identity: Some(always), ..Self::default() }
    }

    fn rewrites_counter(&self) -> bool {
        self.data_type.is_some()
            || self.start.is_some()
            || self.increment.is_some()
            || self.min.is_some()
            || self.max.is_some()
            || self.no_min
            || self.no_max
            || self.cache.is_some()
            || self.cycle.is_some()
            || self.restart.is_some()
    }
}
fn integer(p: &mut Parser) -> R<i64> {
    match p.expr()? {
        Expr::Literal(Value::Int(n)) => Ok(n),
        _ => Err(SqlError::InvalidParameterValue),
    }
}
pub(super) fn identity(p: &mut Parser) -> R<SequenceSpec> {
    let always = if p.word("always") {
        true
    } else {
        p.need("by")?;
        p.need("default")?;
        false
    };
    p.need("as")?;
    p.need("identity")?;
    let mut spec = SequenceSpec {
        identity: Some(always),
        ..Default::default()
    };
    if p.sym('(') {
        options(p, &mut spec, true, false)?;
    }
    Ok(spec)
}
fn sequence_definition(
    spec: &SequenceSpec,
    ty: &DataType,
    name: String,
    owner: Option<(u64, u64)>,
) -> R<catalog_ext::SequenceDef> {
    let (lo, hi) = match ty {
        DataType::SmallInt => (i64::from(i16::MIN), i64::from(i16::MAX)),
        DataType::Integer => (i64::from(i32::MIN), i64::from(i32::MAX)),
        DataType::BigInt => (i64::MIN, i64::MAX),
        _ => return Err(SqlError::InvalidParameterValue),
    };
    let increment = spec.increment.unwrap_or(1);
    let min = spec.min.unwrap_or(if increment < 0 { lo } else { 1 });
    let max = spec.max.unwrap_or(if increment < 0 { -1 } else { hi });
    let start = spec.start.unwrap_or(if increment < 0 { max } else { min });
    validate_sequence(ty, increment, min, max, start, spec.cache.unwrap_or(1))?;
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| SqlError::Unsupported)?;
    Ok(catalog_ext::SequenceDef {
        name,
        data_type: ty.clone(),
        start,
        increment,
        min,
        max,
        cache: spec.cache.unwrap_or(1),
        cycle: spec.cycle.unwrap_or(false),
        owned_by: owner,
        identity: spec.identity.is_some(),
        incarnation: u128::from_le_bytes(bytes),
    })
}

pub(super) fn install_create(
    state: &mut Relational,
    table: TableDef,
    defaults: &[Option<String>],
    sequences: &[Option<SequenceSpec>],
    (typmods, type_ids): (&[Option<i32>], &[Option<u64>]),
    version: (u64, u64),
    context: (&str, &[String]),
) -> R<()> {
    let (schema, path) = context;
    let (next_id, epoch) = version;
    let mut candidate = state.clone();
    candidate.next_id = next_id;
    candidate.epoch = epoch;
    candidate.rows.insert(table.id, BTreeMap::new());
    if schema != "public" {
        std::sync::Arc::make_mut(&mut candidate.ext).table_schemas.insert(table.id, schema.into());
        Relational::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));
    }
    std::sync::Arc::make_mut(&mut candidate.tables).insert(relation_key(schema, &table.name), table.clone());
    // Sequence mutators validate the complete catalog, including later columns.
    for ((column, typmod), type_id) in table.columns.iter().zip(typmods).zip(type_ids) {
        if let Some(id) = type_id {
            std::sync::Arc::make_mut(&mut candidate.ext).columns.entry(column.id).or_default().type_id = Some(*id);
        }
        let logical = match column.data_type {
            DataType::Uuid => Some(catalog_ext::LogicalKind::Uuid),
            DataType::Jsonb => Some(catalog_ext::LogicalKind::Jsonb),
            DataType::IntArray => Some(catalog_ext::LogicalKind::IntArray),
            DataType::BigIntArray => Some(catalog_ext::LogicalKind::BigIntArray),
            DataType::TextArray => Some(catalog_ext::LogicalKind::TextArray),
            DataType::UuidArray => Some(catalog_ext::LogicalKind::UuidArray),
            _ => None,
        };
        if let Some(kind) = logical {
            std::sync::Arc::make_mut(&mut candidate.ext).logical_kinds.insert(column.id, kind);
            Relational::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));
        }
        let kind = match column.data_type {
            DataType::Varchar => Some(catalog_ext::TextKind::Varchar),
            DataType::Bpchar => Some(catalog_ext::TextKind::Bpchar),

            _ => None,
        };
        if let Some(kind) = kind {
            std::sync::Arc::make_mut(&mut candidate.ext)
                .columns.entry(column.id)
                .or_default()
                .text_kind = Some(kind);
            // ext_version follows content (v3 iff any text_kind); validate_ext checks it.
            Relational::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));
        }
        if let Some(typmod) = typmod {
            std::sync::Arc::make_mut(&mut candidate.ext).columns.entry(column.id).or_default().typmod = Some(*typmod);
        }
    }
    Relational::normalize_ext(std::sync::Arc::make_mut(&mut candidate.ext));
    for (column, spec) in table.columns.iter().zip(sequences) {
        if let Some(spec) = spec {
            let name = candidate.choose_sequence_name(&candidate.table_key(&table), &column.name);
            let definition = sequence_definition(
                spec,
                &column.data_type,
                name.clone(),
                Some((table.id, column.id)),
            )?;
            let id = candidate.add_sequence(definition)?;
            if let Some(always) = spec.identity {
                candidate.set_column_identity(
                    column.id,
                    Some(catalog_ext::Identity {
                        always,
                        sequence_id: id,
                    }),
                )?;
            } else {
                candidate.set_column_default(
                    column.id,
                    Some(format!(
                        "nextval({}::regclass)",
                        quoted(&sequence_reference(&name))
                    )),
                )?;
            }
        }
    }
    for ((column, default), spec) in table.columns.iter().zip(defaults).zip(sequences) {
        if spec.is_none()
            && let Some(default) = default
            && default != "NULL"
        {
            let mut parser = Parser { privilege_seen: false, privilege_casts: false,
                tokens: lex(&format!("{default},"))?,
                at: 0,
                params: BTreeSet::new(),
                relations: Vec::new(),
            };
            let mut value = expression(&mut parser, 0)?;
            let sql = column_default(&candidate, column, &mut value, path)?;
            candidate.set_column_default(column.id, Some(sql))?;
        }
    }
    candidate.validate_ext()?;
    *state = candidate;
    Ok(())
}

// Decode-only: R1 stored globally bound bare sequence names in default SQL.
// Preserve that binding when moving an owned sequence into its owner's schema.
pub(super) fn normalize_r1_sequence_defaults(
    ext: &mut catalog_ext::CatalogExt,
    renamed: &BTreeMap<String, String>,
) -> R<()> {
    fn rewrite(value: &mut Expression, renamed: &BTreeMap<String, String>) -> R<bool> {
        match value {
            Expression::Sequence(name) => {
                let key = crate::storage::sequences::sequence_name(name)?;
                if let Some(key) = renamed.get(&key) {
                    *name = sequence_reference(key);
                    return Ok(true);
                }
                Ok(false)
            }
            Expression::Cast(value, _) | Expression::Negative(value) => rewrite(value, renamed),
            _ => Ok(false),
        }
    }
    if renamed.is_empty() {
        return Ok(());
    }
    for column in ext.columns.values_mut() {
        if let Some(sql) = &mut column.default {
            let mut parser = Parser { privilege_seen: false, privilege_casts: false,
                tokens: lex(&format!("{sql},"))?,
                at: 0,
                params: BTreeSet::new(),
                relations: Vec::new(),
            };
            let mut value = expression(&mut parser, 0)?;
            if parser.at + 1 != parser.tokens.len()
                || parser.tokens.get(parser.at) != Some(&Token::Symbol(','))
            {
                return Err(SqlError::Unsupported);
            }
            if rewrite(&mut value, renamed)? {
                *sql = value.sql();
            }
        }
    }
    Ok(())
}

// Deparse sequence references for the reader without changing stored defaults.
pub(super) fn present_default(state: &Relational, sql: &str, path: &[String]) -> String {
    fn present(state: &Relational, value: &mut Expression, path: &[String]) -> R<bool> {
        match value {
            Expression::Sequence(name) => {
                let key = crate::storage::sequences::sequence_name(name)?;
                let (schema, local) = split_key(&key);
                let local_ref = sequence_reference(local);
                *name = if state.resolve_relation_key(local, path) == key {
                    local_ref
                } else {
                    format!("{}.{}", sequence_reference(schema), local_ref)
                };
                Ok(true)
            }
            Expression::Cast(value, _) | Expression::Negative(value) => present(state, value, path),
            _ => Ok(false),
        }
    }
    let Ok(tokens) = lex(&format!("{sql},")) else { return sql.to_owned() };
    let mut parser = Parser { privilege_seen: false, privilege_casts: false, tokens, at: 0,
        params: BTreeSet::new(), relations: Vec::new() };
    let Ok(mut value) = expression(&mut parser, 0) else { return sql.to_owned() };
    if parser.at + 1 != parser.tokens.len() || parser.tokens.get(parser.at) != Some(&Token::Symbol(',')) {
        return sql.to_owned();
    }
    if matches!(present(state, &mut value, path), Ok(true)) { value.sql() } else { sql.to_owned() }
}

pub(super) fn default_sql(state: &Relational, column: &ColumnDef) -> Option<String> {
    if let Some(identity) = state.column_identity(column.id) {
        state.ext.sequences.get(&identity.sequence_id).map(|seq| {
            format!(
                "nextval({}::regclass)",
                quoted(&sequence_reference(&seq.name))
            )
        })
    } else {
        state.column_default(column.id).map(str::to_owned)
    }
}
pub(super) fn check_explicit(state: &Relational, column: &ColumnDef) -> R<()> {
    if state.column_identity(column.id).is_some_and(|i| i.always) {
        Err(SqlError::GeneratedAlways)
    } else {
        Ok(())
    }
}
pub(super) fn dependencies(sql: &str) -> R<Rc<[String]>> {
    cached_syntax(&DEPENDENCIES, sql, || parse_dependencies(sql).map(Rc::from))
}

fn parse_dependencies(sql: &str) -> R<Vec<String>> {
    let mut parser = Parser { privilege_seen: false, privilege_casts: false,
        tokens: lex(&format!("{sql},"))?,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let value = expression(&mut parser, 0)?;
    if parser.at + 1 != parser.tokens.len() {
        return Err(SqlError::Unsupported);
    }
    let mut values = vec![];
    fn collect(value: &Expression, values: &mut Vec<String>) {
        match value {
            Expression::Sequence(name) => values.push(name.clone()),
            Expression::Cast(value, _) | Expression::Negative(value) => collect(value, values),
            _ => {}
        }
    }
    collect(&value, &mut values);
    Ok(values)
}

pub(super) fn type_name(ty: &DataType) -> &'static str {
    match ty {
        DataType::SmallInt => "smallint",
        DataType::Integer => "integer",
        DataType::BigInt => "bigint",
        DataType::Boolean => "boolean",
        DataType::Numeric => "numeric",
        DataType::Real => "real",
        DataType::DoublePrecision => "double precision",
        DataType::Text => "text",
        DataType::RegType => "regtype",
        DataType::Varchar => "varchar",
        DataType::Bpchar => "bpchar",
        DataType::Uuid => "uuid",
        DataType::Jsonb => "jsonb",
        DataType::IntArray => "integer[]",
        DataType::BigIntArray => "bigint[]",
        DataType::TextArray => "text[]",
        DataType::UuidArray => "uuid[]",

        DataType::Bytes => "bytea",
        DataType::Date => "date",
        DataType::Time => "time without time zone",
        DataType::TimeTz => "time with time zone",
        DataType::Timestamp => "timestamp without time zone",
        DataType::TimestampTz => "timestamp with time zone",
        DataType::Interval => "interval",
    }
}
pub(super) fn quoted(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
fn sequence_reference(name: &str) -> String {
    if name.contains('\0') {
        let (schema, name) = split_key(name);
        return format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\"")
        );
    }
    if name
        .bytes()
        .enumerate()
        .all(|(i, b)| b == b'_' || b.is_ascii_lowercase() || i > 0 && b.is_ascii_digit())
    {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[derive(Clone)]
enum Expression {
    Extension(String, Vec<Expression>),
    Sequence(String),
    Number(String),
    String(String),
    Boolean(bool),
    Null,
    Clock(String, DataType),
    Builtin(String, DataType),
    Negative(Box<Expression>),
    Cast(Box<Expression>, DataType),
    NamedCast(Box<Expression>, user_types::TypeReference),
    Array(Vec<Expression>),
}
impl Expression {
    fn qualify_stored_sequences(&mut self) -> R<()> {
        match self {
            Self::Sequence(name) => {
                let normalized = crate::storage::sequences::sequence_name(name)?;
                if !normalized.contains('\0') {
                    *name = format!("\"public\".{}", sequence_reference(&normalized));
                }
            }
            Self::Cast(value, _) | Self::Negative(value) => value.qualify_stored_sequences()?,
            _ => {}
        }
        Ok(())
    }
    fn numeric_literal(&self) -> bool {
        match self {
            Self::Number(_) => true,
            Self::Cast(value, ty) => {
                (value.numeric_literal()
                    || matches!(value.as_ref(),Self::String(s) if Numeric::parse(s).is_ok()))
                    && matches!(
                        ty,
                        DataType::SmallInt
                            | DataType::Integer
                            | DataType::BigInt
                            | DataType::Numeric
                            | DataType::Real
                            | DataType::DoublePrecision
                    )
            }
            _ => false,
        }
    }
    fn resolve(&mut self, state: &Relational, path: &[String]) -> R<()> {
        match self {
            Self::Extension(name,args) => {
                let installed=state.ext.extensions.get("uuid-ossp").ok_or(SqlError::UndefinedFunction)?;
                let (schema,routine)=name.split_once('\0').map_or((None,name.as_str()),|(s,n)|(Some(s),n));
                if schema.is_some_and(|s|s != installed.schema) || schema.is_none() && installed.schema != "pg_catalog" && !path.contains(&installed.schema) {return Err(SqlError::UndefinedFunction);}
                let routine=routine.to_owned();
                *name=format!("{}\0{}",installed.schema,routine);
                for a in args {a.resolve(state,path)?;}
            }
            Self::Sequence(name) => {
                let mut p = Parser { privilege_seen: false, privilege_casts: false,
                    tokens: lex(name)?,
                    at: 0,
                    params: BTreeSet::new(),
                    relations: Vec::new(),
                };
                let normalized = p.table_name().map_err(|_| SqlError::UnknownTable)?;
                let normalized = state.resolve_relation_key(&normalized, path);
                if p.at != p.tokens.len() {
                    return Err(SqlError::UnknownTable);
                }
                let (_, sequence) = state
                    .sequence_by_name(&normalized)
                    .ok_or(SqlError::UnknownTable)?;
                *name = sequence_reference(&sequence.name);
            }
            Self::Cast(value, _) | Self::NamedCast(value, _) | Self::Negative(value) => value.resolve(state, path)?,
            Self::Array(values) => {
                for value in values {
                    value.resolve(state, path)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn ty(&self) -> DataType {
        match self {
            Self::Extension(..) => DataType::Uuid,
            Self::Sequence(_) => DataType::BigInt,
            Self::Number(n) => number_type(n),
            Self::String(_) | Self::Null => DataType::Text,
            Self::Boolean(_) => DataType::Boolean,
            Self::Clock(_, ty) | Self::Builtin(_, ty) | Self::Cast(_, ty) => ty.clone(),
            Self::Negative(value) => value.ty(),
            Self::NamedCast(_, name) if name.array => DataType::TextArray,
            Self::NamedCast(..) => DataType::Text,
            Self::Array(_) => DataType::TextArray,
        }
    }
    fn sql(&self) -> String {
        self.render(false)
    }
    fn render(&self, physical: bool) -> String {
        match self {
            Self::Extension(name,args) => format!("{}({})",name.split('\0').map(|n|format!("\"{}\"",n.replace('"',"\"\""))).collect::<Vec<_>>().join("."),args.iter().map(Self::sql).collect::<Vec<_>>().join(",")),
            Self::Sequence(name) => format!("nextval({}::regclass)", quoted(name)),
            Self::Number(n) => n.clone(),
            Self::String(s) => quoted(s),
            Self::Boolean(b) => b.to_string(),
            Self::Null => "NULL".into(),
            Self::Clock(s, _) => s.clone(),
            Self::Builtin(sql, _) => sql.clone(),
            Self::Negative(value) => format!("-({})", value.render(physical)),
            Self::Cast(value, ty) => format!("({})::{}", value.render(physical), type_name(ty)),
            Self::NamedCast(value, name) => format!("({})::{}", value.render(physical),
                if physical { type_name(&self.ty()).into() } else { name.sql() }),
            Self::Array(values) => format!("ARRAY[{}]",
                values.iter().map(|v| v.render(physical)).collect::<Vec<_>>().join(", ")),
        }
    }

    fn canonical(&self, target: &DataType) -> R<String> {
        match self {
            Self::Extension(..) => Ok(self.sql()),
            Self::Sequence(name) => Ok(format!("nextval({}::regclass)", quoted(name))),
            Self::Number(n) => number_text(n),
            Self::String(_) => constant_text(&self.sql(), target),
            Self::Boolean(b) => Ok(b.to_string()),
            Self::Null => Ok("NULL".into()),
            Self::Clock(s, _) => Ok(s.clone()),
            Self::Builtin(..) => Ok(self.sql()),
            Self::Negative(value) => {
                if let Self::Number(n) = value.as_ref() {
                    number_text(&format!("-{n}"))
                } else {
                    Ok(format!("(- {})", value.canonical(&value.ty())?))
                }
            }
            Self::Array(values) => array_text(values, target, None),
            Self::NamedCast(..) => Err(SqlError::Unsupported),
            Self::Cast(value, ty) => {
                if let Self::Array(values) = value.as_ref() {
                    return array_text(values, ty, None);
                }
                if matches!(value.as_ref(), Self::String(_) | Self::Null) {
                    constant_text(&self.sql(), ty)
                } else if value.ty() == *ty {
                    value.canonical(ty)
                } else {
                    Ok(format!(
                        "({})::{}",
                        value.canonical(&value.ty())?,
                        type_name(ty)
                    ))
                }
            }
        }
    }
}
fn number_type(n: &str) -> DataType {
    if n.parse::<i32>().is_ok() {
        DataType::Integer
    } else if n.parse::<i64>().is_ok() {
        DataType::BigInt
    } else {
        DataType::Numeric
    }
}
fn number_text(n: &str) -> R<String> {
    let ty = number_type(n);
    let display = match ty {
        DataType::Integer | DataType::BigInt => {
            n.parse::<i64>().map_err(|_| SqlError::Type)?.to_string()
        }
        _ => Numeric::parse(n).map_err(|_| SqlError::Type)?.to_string(),
    };
    Ok(match ty {
        DataType::Integer if !display.starts_with('-') => display,
        DataType::Numeric if !display.starts_with('-') && display.contains('.') => display,
        _ => format!("{}::{}", quoted(&display), type_name(&ty)),
    })
}
fn constant_text(sql: &str, ty: &DataType) -> R<String> {
    let value =
        query::InsertExpressions::default_value(sql, ty, None, true)?.ok_or(SqlError::Type)?;
    Ok(match value {
        Value::Int(n) if *ty == DataType::Integer && n >= 0 => n.to_string(),
        Value::Int(n) => format!("{}::{}", quoted(&n.to_string()), type_name(ty)),
        Value::Numeric(n) => format!("{}::numeric", quoted(&n.to_string())),
        Value::Bool(b) => b.to_string(),
        Value::Text(s) => format!("{}::{}", quoted(&s),
            if ty.is_extended_text() { type_name(ty) } else { "text" }),
        Value::Float4(v) => format!("{}::real", quoted(&v.to_text())),
        Value::Float8(v) => format!("{}::double precision", quoted(&v.to_text())),
        Value::Bytes(bytes) => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut text = String::with_capacity(2 + bytes.len() * 2);
            text.push_str("\\x");
            for byte in bytes {
                text.push(HEX[usize::from(byte >> 4)] as char);
                text.push(HEX[usize::from(byte & 15)] as char);
            }
            format!("{}::bytea", quoted(&text))
        }
        Value::Null => "NULL".into(),
        Value::Date(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::Time(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::TimeTz(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::Timestamp(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::TimestampTz(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
        Value::Interval(v) => format!("{}::{}", quoted(&v.to_string()), type_name(ty)),
    })
}
pub(super) fn cast_type(p: &mut Parser) -> R<DataType> {
    let Some(Token::Word(word)) = p.tokens.get(p.at).cloned() else {
        return Err(SqlError::Unsupported);
    };
    p.at += 1;
    let mut ty = match word.as_str() {
        "int" | "int4" | "integer" => DataType::Integer,
        "int2" | "smallint" => DataType::SmallInt,
        "int8" | "bigint" => DataType::BigInt,
        "bool" | "boolean" => DataType::Boolean,
        "numeric" | "decimal" => DataType::Numeric,
        "real" | "float4" => DataType::Real,
        "float8" => DataType::DoublePrecision,
        "double" => {
            p.need("precision")?;
            DataType::DoublePrecision
        }
        "text" => DataType::Text,
        "uuid" => DataType::Uuid,
        "jsonb" => DataType::Jsonb,
        "bytea" => DataType::Bytes,
        "date" => DataType::Date,
        "time" | "timestamp" => {
            let with = p.word("with");
            if with || p.word("without") {
                p.need("time")?;
                p.need("zone")?;
            }
            match (word.as_str(), with) {
                ("time", false) => DataType::Time,
                ("time", true) => DataType::TimeTz,
                (_, false) => DataType::Timestamp,
                (_, true) => DataType::TimestampTz,
            }
        }
        "timetz" => DataType::TimeTz,
        "timestamptz" => DataType::TimestampTz,
        "interval" => DataType::Interval,
        _ => return Err(SqlError::Unsupported),
    };
    if p.sym('[') {
        p.punct(']')?;
        ty = match ty {
            DataType::Text => DataType::TextArray,
            DataType::Integer => DataType::IntArray,
            DataType::BigInt => DataType::BigIntArray,
            DataType::Uuid => DataType::UuidArray,
            _ => return Err(SqlError::Unsupported),
        };
    }
    Ok(ty)
}
fn expression(p: &mut Parser, depth: usize) -> R<Expression> {
    if depth > 16 {
        return Err(SqlError::Limit);
    }
    if matches!(p.tokens.get(p.at), Some(Token::Op(s)) if s == "-") {
        p.at += 1;
        let value = expression(p, depth + 1)?;
        if !value.numeric_literal() {
            return Err(SqlError::Unsupported);
        }
        return Ok(Expression::Negative(Box::new(value)));
    }
    let start=p.at;
    let mut call = None;
    if matches!(p.tokens.get(p.at),Some(Token::Word(_) | Token::QuotedIdent(_))) {
        let first=alter_table::raw_name(p)?;
        let (schema,routine)=if p.sym('.') {(Some(first),alter_table::raw_name(p)?)} else {(None,first)};
        if query::random_uuid::default_candidate(schema.as_deref(), &routine) && p.sym('(') {
            let (sql, ty) = query::random_uuid::parse_default(p, schema.as_deref(), &routine, depth + 1)?;
            call = Some(Expression::Builtin(sql, ty));
        } else if super::extensions::BUILTINS.iter().flat_map(|b|b.functions).any(|f|f.name==routine) && p.sym('(') {
            let mut args=Vec::new();
            if !p.sym(')') { loop {args.push(expression(p,depth+1)?);if p.sym(')'){break;} p.punct(',')?;} }
            call=Some(Expression::Extension(schema.map_or_else(||routine.clone(),|s|format!("{s}\0{routine}")),args));
        } else {p.at=start;}
    }
    let mut value = if let Some(call) = call {
        call
    } else if p.sym('(') {
        let value = expression(p, depth + 1)?;
        p.punct(')')?;
        value
    } else {
        let token = p.tokens.get(p.at).cloned().ok_or(SqlError::Syntax)?;
        p.at += 1;
        match token {
            Token::String(s) => Expression::String(s),
            Token::Number(n) => Expression::Number(n),
            Token::Word(w) => match w.as_str() {
                "nextval" => {
                    p.punct('(')?;
                    let Some(Token::String(name)) = p.tokens.get(p.at).cloned() else {
                        return Err(SqlError::Unsupported);
                    };
                    p.at += 1;
                    if p.sym(':') {
                        p.punct(':')?;
                        p.need("regclass")?;
                    }
                    p.punct(')')?;
                    Expression::Sequence(name)
                }
                "array" => {
                    p.punct('[')?;
                    let mut values = vec![];
                    if !p.sym(']') {
                        loop {
                            let value = expression(p, depth + 1)?;
                            if !matches!(&value,
                                Expression::String(_) | Expression::Number(_) | Expression::Null
                                | Expression::Boolean(_) | Expression::Cast(..) | Expression::NamedCast(..))
                                || values.len() >= MAX_COLUMNS
                            {
                                return Err(SqlError::Unsupported);
                            }
                            values.push(value);
                            if p.sym(']') {
                                break;
                            }
                            p.punct(',')?;
                        }
                    }
                    Expression::Array(values)
                }
                "null" => Expression::Null,
                "true" | "false" => Expression::Boolean(w == "true"),
                "now" | "transaction_timestamp" | "statement_timestamp" | "clock_timestamp" => {
                    p.punct('(')?;
                    p.punct(')')?;
                    Expression::Clock(format!("{w}()"), DataType::TimestampTz)
                }
                "current_date" | "current_time" | "current_timestamp" | "localtime"
                | "localtimestamp" => {
                    let mut sql = w.to_uppercase();
                    if w != "current_date" && p.sym('(') {
                        if matches!(p.tokens.get(p.at), Some(Token::Op(sign)) if sign == "-") {
                            return Err(SqlError::Syntax);
                        }
                        let Some(Token::Number(n)) = p.tokens.get(p.at) else {
                            return Err(SqlError::Unsupported);
                        };
                        let precision = n.parse::<u32>().map_err(|_| SqlError::Unsupported)?.min(6);
                        sql.push_str(&format!("({precision})"));
                        p.at += 1;
                        p.punct(')')?;
                    }
                    let ty = match w.as_str() {
                        "current_date" => DataType::Date,
                        "current_time" => DataType::TimeTz,
                        "localtime" => DataType::Time,
                        "localtimestamp" => DataType::Timestamp,
                        _ => DataType::TimestampTz,
                    };
                    Expression::Clock(sql, ty)
                }
                _ => return Err(SqlError::Unsupported),
            },
            _ => return Err(SqlError::Unsupported),
        }
    };
    if p.tokens.get(p.at) == Some(&Token::Symbol(':'))
        && p.tokens.get(p.at + 1) == Some(&Token::Symbol(':'))
    {
        p.at += 2;
        let start = p.at;
        value = match cast_type(p) {
            Ok(ty) if p.tokens.get(p.at) != Some(&Token::Symbol('.')) => Expression::Cast(Box::new(value), ty),
            Err(SqlError::Unsupported) | Ok(_) if p.at <= start + 1 => {
                p.at = start;
                Expression::NamedCast(Box::new(value), user_types::TypeReference::parse(p)?)
            }
            Err(error) => return Err(error),
            Ok(_) => return Err(SqlError::Unsupported),
        };
    }
    Ok(value)
}

/// Lower a typed native default through the SQL default validator and canonicalizer.
pub(crate) fn native_default(default: &crate::operations::ColumnDefault, target: &DataType) -> R<String> {
    use crate::operations::{ColumnDefault, ClockDefault};
    let number = |text: String| {
        if let Some(positive) = text.strip_prefix('-') {
            Expression::Negative(Box::new(Expression::Number(positive.into())))
        } else { Expression::Number(text) }
    };
    let cast = |text: String, ty| Expression::Cast(Box::new(Expression::String(text)), ty);
    let value = match default {
        ColumnDefault::Nextval(name) => Expression::Sequence(name.clone()),
        ColumnDefault::Value(value) => match value {
            Value::Null => Expression::Null,
            Value::Bool(b) => Expression::Boolean(*b),
            Value::Int(n) => number(n.to_string()),
            Value::Numeric(n) => {
                let text = n.to_string();
                if matches!(text.as_str(), "NaN" | "Infinity" | "-Infinity") { cast(text, DataType::Numeric) }
                else { number(text) }
            }
            Value::Text(v) => cast(v.clone(), DataType::Text),
            Value::Bytes(v) => {
                let mut text = String::from("\\x");
                for byte in v { text.push_str(&format!("{byte:02x}")); }
                cast(text, DataType::Bytes)
            }
            Value::Float4(v) => cast(v.to_text(), DataType::Real),
            Value::Float8(v) => cast(v.to_text(), DataType::DoublePrecision),
            Value::Date(v) => cast(v.to_string(), DataType::Date),
            Value::Time(v) => cast(v.to_string(), DataType::Time),
            Value::TimeTz(v) => cast(v.to_string(), DataType::TimeTz),
            Value::Timestamp(v) => cast(v.to_string(), DataType::Timestamp),
            Value::TimestampTz(v) => cast(v.to_string(), DataType::TimestampTz),
            Value::Interval(v) => cast(v.to_string(), DataType::Interval),
        },
        ColumnDefault::Clock(clock) => {
            let (name, ty, precision) = match clock {
                ClockDefault::Now => ("now()", DataType::TimestampTz, None),
                ClockDefault::TransactionTimestamp => ("transaction_timestamp()", DataType::TimestampTz, None),
                ClockDefault::StatementTimestamp => ("statement_timestamp()", DataType::TimestampTz, None),
                ClockDefault::ClockTimestamp => ("clock_timestamp()", DataType::TimestampTz, None),
                ClockDefault::CurrentDate => ("CURRENT_DATE", DataType::Date, None),
                ClockDefault::CurrentTime(p) => ("CURRENT_TIME", DataType::TimeTz, *p),
                ClockDefault::CurrentTimestamp(p) => ("CURRENT_TIMESTAMP", DataType::TimestampTz, *p),
                ClockDefault::LocalTime(p) => ("LOCALTIME", DataType::Time, *p),
                ClockDefault::LocalTimestamp(p) => ("LOCALTIMESTAMP", DataType::Timestamp, *p),
            };
            Expression::Clock(precision.map_or_else(|| name.into(), |p| format!("{name}({})", p.min(6))), ty)
        }
    };
    query::InsertExpressions::default_value(&value.sql(), target, None, false)?;
    value.canonical(target)
}

pub(super) fn parse(p: &mut Parser, target: &DataType, named_type: bool) -> R<String> {
    let value = expression(p, 0)?;
    if !matches!(p.tokens.get(p.at), Some(Token::Symbol(',' | ')')))
        && !matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "not" || w == "null" || w == "primary" || w == "references" || w == "check" || w == "constraint" || w == "unique")
    {
        return Err(SqlError::Unsupported);
    }
    // UUID array default diagnostics need the column name at installation.
    if named_type || *target == DataType::UuidArray {
        return Ok(value.sql());
    }
    query::InsertExpressions::default_value(&value.sql(), target, None, false)?;
    value.canonical(target)
}

fn array_text(
    values: &[Expression],
    target: &DataType,
    enum_name: Option<&str>,
) -> R<String> {
    let element = match target {
        DataType::TextArray => DataType::Text,
        DataType::IntArray => DataType::Integer,
        DataType::BigIntArray => DataType::BigInt,
        DataType::UuidArray => DataType::Uuid,
        _ => return Err(SqlError::Type),
    };
    let name = enum_name.unwrap_or(type_name(&element));
    if values.is_empty() {
        return Ok(format!("ARRAY[]::{name}[]"));
    }
    let items = values.iter().map(|v| {
        let value = query::InsertExpressions::default_value(&v.render(true), &element, None, false)?
            .ok_or(SqlError::Unsupported)?;
        Ok(match value {
            Value::Text(text) => format!("{}::{name}", quoted(&text)),
            Value::Null => format!("NULL::{name}"),
            Value::Int(n) if element == DataType::Integer => n.to_string(),
            Value::Int(n) => format!("{}::{name}", quoted(&n.to_string())),
            _ => return Err(SqlError::Type),
        })
    }).collect::<R<Vec<_>>>()?;
    Ok(format!("ARRAY[{}]", items.join(", ")))
}

fn column_default(
    state: &Relational,
    column: &ColumnDef,
    expression: &mut Expression,
    path: &[String],
) -> R<String> {
    expression.resolve(state, path)?;
    let target = &column.data_type;
    if let Some(ty @ TypeDef::Enum { .. }) = state.column_type(column.id) {
        let is_array = *target == DataType::TextArray;
        enum_default_casts(expression, state, path, ty, is_array)?;
        let value = query::InsertExpressions::default_value(
            &expression.render(true), target, None, false,
        )?.ok_or(SqlError::Unsupported)?;
        user_types::coerce_user_type(ty, target, &value)?;
        let name = user_types::quote_type_ident(ty.name());
        let name = if ty.schema() == "public" { name } else {
            format!("{}.{name}", user_types::quote_type_ident(ty.schema()))
        };
        let input = match &*expression {
            Expression::NamedCast(inner, _) => inner.as_ref(),
            other => other,
        };
        if let Expression::Array(values) = input {
            return array_text(values, target, Some(&name));
        }
        return match value {
            Value::Text(text) => Ok(format!("{}::{name}{}", quoted(&text), if is_array { "[]" } else { "" })),
            Value::Null => Ok("NULL".into()),
            _ => Err(SqlError::Type),
        };
    }
    let sql = expression.sql();
    if *target == DataType::UuidArray {
        query::InsertExpressions::validate_uuid_array_default(&sql, &column.name)?;
    } else {
        query::InsertExpressions::default_value(&sql, target, None, false)?;
    }
    expression.canonical(target)
}

fn enum_default_casts(
    value: &Expression,
    state: &Relational,
    path: &[String],
    ty: &TypeDef,
    array: bool,
) -> R<()> {
    match value {
        Expression::NamedCast(inner, name) => {
            let (_, cast_type) = name.resolve(state, path)?;
            if cast_type != ty || name.array != array {
                return Err(SqlError::Type);
            }
            enum_default_casts(inner, state, path, ty, array)
        }
        Expression::Array(values) if array => {
            for value in values {
                enum_default_casts(value, state, path, ty, false)?;
            }
            Ok(())
        }
        Expression::String(_) | Expression::Null => Ok(()),
        _ => Err(SqlError::Type),
    }
}

pub(super) fn evaluate_sql(
    sql: &str,
    target: &DataType,
    clock: Option<&clock::ExecutionClock>,
) -> R<Value> {
    let qualified = cached_syntax(&QUALIFIED_DEFAULTS, sql, || qualify_default(sql))?;
    let legacy_text = target.is_extended_text() && qualified.ends_with("::text");
    let qualified = if legacy_text {
        qualified.strip_suffix("::text").expect("text suffix")
    } else {
        &qualified
    };
    // Character defaults are stored as text constants. Evaluate that physical
    // value before the caller applies the destination column's character rules;
    // relabeling a text cast as varchar/bpchar would change its scalar cast type.
    let evaluation_target = match target {
        DataType::Varchar | DataType::Bpchar => &DataType::Text,
        other => other,
    };
    let clock = clock.map(clock::ExecutionClock::for_column_default);
    query::InsertExpressions::default_value(qualified, evaluation_target, clock.as_ref(), true)?
        .ok_or(SqlError::Type)
}
fn qualify_default(sql: &str) -> R<Rc<str>> {
    let mut parser = Parser { privilege_seen: false, privilege_casts: false,
        tokens: lex(&format!("{sql},"))?,
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let mut value = expression(&mut parser, 0)?;
    value.qualify_stored_sequences()?;
    if parser.at + 1 != parser.tokens.len()
        || parser.tokens.get(parser.at) != Some(&Token::Symbol(','))
    {
        return Err(SqlError::Unsupported);
    }
    Ok(Rc::from(value.render(true)))
}

pub(super) fn evaluate(
    state: &Relational,
    column: &ColumnDef,
    clock: Option<&clock::ExecutionClock>,
) -> R<Value> {
    let value = match default_sql(state, column) {
        Some(sql) => evaluate_sql(&sql, &column.data_type, clock)?,
        None => Value::Null,
    };
    let value = coerce_to_column(state, column, value)?;
    apply_typmod(value, state.column_typmod(column.id))
}
pub(super) fn apply_typmod(value: Value, modifier: i32) -> R<Value> {
    if modifier == -1 {
        return Ok(value);
    }
    let temporal = |_| SqlError::DatetimeFieldOverflow;
    Ok(match value {
        Value::Numeric(v) => Value::Numeric(
            v.apply_typmod(
                giggles_pg_types::typmod::NumericTypmod::from_typmod(modifier)
                    .ok_or(SqlError::Type)?,
            )
            .map_err(|_| SqlError::Overflow)?,
        ),
        Value::Time(v) => Value::Time(v.with_precision(modifier as u32).map_err(temporal)?),
        Value::TimeTz(v) => Value::TimeTz(v.with_precision(modifier as u32).map_err(temporal)?),
        Value::Timestamp(v) => {
            Value::Timestamp(v.with_precision(modifier as u32).map_err(temporal)?)
        }
        Value::TimestampTz(v) => {
            Value::TimestampTz(v.with_precision(modifier as u32).map_err(temporal)?)
        }
        Value::Interval(v) => {
            use giggles_pg_types::temporal::IntervalField;
            let mask = modifier >> 16;
            let last = if mask & 4096 != 0 {
                IntervalField::Second
            } else if mask & 2048 != 0 {
                IntervalField::Minute
            } else if mask & 1024 != 0 {
                IntervalField::Hour
            } else if mask & 8 != 0 {
                IntervalField::Day
            } else if mask & 2 != 0 {
                IntervalField::Month
            } else {
                IntervalField::Year
            };
            let digits = modifier & 0xffff;
            Value::Interval(
                v.with_fields(last, if digits == 65535 { 6 } else { digits as u32 })
                    .map_err(temporal)?,
            )
        }
        v => v,
    })
}
pub(super) fn row(
    state: &Relational,
    table: &TableDef,
    supplied: &[usize],
    clock: Option<&clock::ExecutionClock>,
) -> R<Vec<Value>> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if supplied.contains(&index) || column.name == HIDDEN_ROWID {
                Ok(Value::Null)
            } else {
                evaluate(state, column, clock)
            }
        })
        .collect()
}

pub(super) fn install_alter(
    state: &mut Relational,
    column: &ColumnDef,
    sql: Option<&str>,
    path: &[String],
) -> R<()> {
    enum_alter::validate_default(state.column_type(column.id), sql)?;
    let value = if let Some(sql) = sql {
        let mut parser = Parser { privilege_seen: false, privilege_casts: false,
            tokens: lex(&format!("{sql},"))?,
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        };
        let mut value = expression(&mut parser, 0)?;
        Some(column_default(state, column, &mut value, path)?)
    } else {
        None
    };
    state.set_column_default(column.id, value)
}

pub(super) fn parse_alter_untyped(p: &mut Parser) -> R<String> {
    Ok(expression(p, 0)?.sql())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uuid_array_default_reuses_existing_catalog_bytes() {
        fn run(state: &mut Relational, sql: &str) {
            let statement = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute(&statement, &[], None).unwrap();
        }

        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE t (id int PRIMARY KEY, u uuid[], a text[] DEFAULT '{}')");
        let before = bincode::serialize(&state).unwrap();
        let before_ext = bincode::serialize(state.ext.as_ref()).unwrap();
        let mut reopened: Relational = bincode::deserialize(&before).unwrap();
        assert_eq!(bincode::serialize(&reopened).unwrap(), before);

        let version = reopened.ext.wire_version();
        let column = reopened.tables["t"].columns[1].id;
        let mut expected = reopened.ext.as_ref().clone();
        expected.columns.entry(column).or_default().default = Some("ARRAY[]::uuid[]".into());

        run(&mut reopened, "ALTER TABLE t ALTER COLUMN u SET DEFAULT ARRAY[]::uuid[]");
        assert_eq!(reopened.ext.wire_version(), version);
        assert_eq!(
            bincode::serialize(reopened.ext.as_ref()).unwrap(),
            bincode::serialize(&expected).unwrap(),
        );

        let bytes = bincode::serialize(&reopened).unwrap();
        let mut reopened: Relational = bincode::deserialize(&bytes).unwrap();
        assert_eq!(reopened.column_default(column), Some("ARRAY[]::uuid[]"));
        run(&mut reopened, "ALTER TABLE t ALTER COLUMN u DROP DEFAULT");
        assert_eq!(bincode::serialize(reopened.ext.as_ref()).unwrap(), before_ext);
    }

    #[test]
    fn present_default_preserves_sql_when_reparsing_fails() {
        let state = Relational::default();
        for sql in ["'unterminated", "unknown_function()", "1 2", "nextval('')", " 0007 "] {
            assert_eq!(present_default(&state, sql, &[]), sql, "{sql}");
        }
    }
    #[test]
    fn sequence_metadata_uses_existing_wire_and_unique_incarnations() {
        let mut state = Relational::default();
        let sql = "CREATE TABLE meta (a smallserial,b bigserial,c integer GENERATED ALWAYS AS IDENTITY (INCREMENT BY -1))";
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        let mut second = state.clone();
        state.execute(&p, &[], None).unwrap();
        second.execute(&p, &[], None).unwrap();
        assert_eq!(state.ext.ext_version, 1);
        assert_eq!(state.ext.sequences.len(), 3);
        let table = &state.tables["meta"];
        for column in table.logical_columns() {
            assert!(column.not_null);
        }
        let (_, small) = state.sequence_by_name("meta_a_seq").unwrap();
        assert_eq!(
            (small.start, small.increment, small.min, small.max),
            (1, 1, 1, 32767)
        );
        assert_eq!(small.owned_by, Some((table.id, table.columns[0].id)));
        let (_, big) = state.sequence_by_name("meta_b_seq").unwrap();
        assert_eq!(big.max, i64::MAX);
        let (_, descending) = state.sequence_by_name("meta_c_seq").unwrap();
        assert_eq!(
            (
                descending.start,
                descending.increment,
                descending.min,
                descending.max
            ),
            (-1, -1, i64::from(i32::MIN), -1)
        );
        assert!(state.column_default(table.columns[2].id).is_none());
        assert!(state.column_identity(table.columns[2].id).unwrap().always);
        assert_ne!(
            small.incarnation,
            second.sequence_by_name("meta_a_seq").unwrap().1.incarnation
        );
    }
    #[test]
    fn canonical_defaults_preserve_expression_types_and_casts() {
        for (input, target, expected) in [
            ("'x'", DataType::Text, "'x'::text"),
            ("0", DataType::Integer, "0"),
            ("0007", DataType::Integer, "7"),
            ("01.20", DataType::Numeric, "1.20"),
            ("1e2", DataType::Numeric, "'100'::numeric"),
            ("2147483648", DataType::BigInt, "'2147483648'::bigint"),
            ("'12'::bigint", DataType::BigInt, "'12'::bigint"),
            ("'-7'::smallint", DataType::SmallInt, "'-7'::smallint"),
            ("NULL::numeric", DataType::Numeric, "NULL"),
            ("(-2147483648)", DataType::Integer, "'-2147483648'::integer"),
            (
                "-2147483648::bigint",
                DataType::BigInt,
                "(- '2147483648'::bigint)",
            ),
            ("'true'::boolean", DataType::Boolean, "true"),
            ("'1.5'::real", DataType::Real, "'1.5'::real"),
            (
                "'NaN'::double precision",
                DataType::DoublePrecision,
                "'NaN'::double precision",
            ),
            ("'Infinity'::real", DataType::Real, "'Infinity'::real"),
            ("'abc'::bytea", DataType::Bytes, r"'\x616263'::bytea"),
            (r"'\x00'::bytea", DataType::Bytes, r"'\x00'::bytea"),
            (
                "'2020-01-02 03:04:05+00'::timestamptz",
                DataType::TimestampTz,
                "'2020-01-02 03:04:05+00'::timestamp with time zone",
            ),
            (
                "'2020-01-02 03:04:05'::timestamp",
                DataType::Timestamp,
                "'2020-01-02 03:04:05'::timestamp without time zone",
            ),
            ("'1 day'::interval", DataType::Interval, "'1 day'::interval"),
            ("false", DataType::Boolean, "false"),
            ("-7", DataType::BigInt, "'-7'::integer"),
            ("-1.25", DataType::DoublePrecision, "'-1.25'::numeric"),
            ("'12'::int", DataType::Integer, "12"),
            ("7::smallint", DataType::SmallInt, "(7)::smallint"),
            ("7::bigint", DataType::BigInt, "(7)::bigint"),
            ("-7::bigint", DataType::Integer, "(- (7)::bigint)"),
            (
                "CURRENT_TIMESTAMP(7)",
                DataType::TimestampTz,
                "CURRENT_TIMESTAMP(6)",
            ),
            ("NULL", DataType::Integer, "NULL"),
        ] {
            let mut p = Parser { privilege_seen: false, privilege_casts: false,
                tokens: lex(&format!("{input},")).unwrap(),
                at: 0,
                params: BTreeSet::new(),
                relations: Vec::new(),
            };
            assert_eq!(parse(&mut p, &target, false).unwrap(), expected, "{input}");
            let mut p = Parser { privilege_seen: false, privilege_casts: false,
                tokens: lex(&format!("{expected},")).unwrap(),
                at: 0,
                params: BTreeSet::new(),
                relations: Vec::new(),
            };
            assert_eq!(
                parse(&mut p, &target, false).unwrap(),
                expected,
                "stored {expected}"
            );
        }
    }
}
