//! PostgreSQL DDL storage settings. Values affect only catalog presentation.
use super::*;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use std::{fmt, marker::PhantomData};

const MAX_METADATA_ITEMS: usize = 65_536;
const MAX_OPTION_BYTES: usize = 65_536;
type Options = Vec<(String, String)>;

// Variant order is part of the persisted V1 table metadata.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum Persistence {
    #[default]
    Permanent,
    Unlogged,
}

// Field order and the order of variants in its serialized fields are persisted in V1.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableStorage {
    #[serde(deserialize_with = "bounded_options")]
    pub reloptions: Options,
    pub persistence: Persistence,
    pub clustered_index: Option<u64>,
    #[serde(deserialize_with = "bounded_index_options")]
    pub index_reloptions: BTreeMap<u64, Options>,
    #[serde(deserialize_with = "bounded_map")]
    pub columns: BTreeMap<u64, ColumnStorage>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
struct IndexOptions(#[serde(deserialize_with = "bounded_options")] pub Options);

// Field order and the order of variants in its serialized fields are persisted in V1.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColumnStorage {
    pub stattarget: Option<i16>,
    pub storage: Option<u8>,
    pub compression: Option<u8>,
    #[serde(deserialize_with = "bounded_options")]
    pub options: Options,
}

impl TableStorage {
    pub(crate) fn is_empty(&self) -> bool {
        self.reloptions.is_empty()
            && self.persistence == Persistence::Permanent
            && self.clustered_index.is_none()
            && self.index_reloptions.is_empty()
            && self.columns.is_empty()
    }
}

impl ColumnStorage {
    pub(crate) fn is_canonical(&self) -> bool {
        *self != Self::default()
            && self
                .stattarget
                .is_none_or(|value| (0..=10_000).contains(&value))
            && self
                .storage
                .is_none_or(|value| matches!(value, b'p' | b'e' | b'm' | b'x'))
            && self
                .compression
                .is_none_or(|value| matches!(value, b'p' | b'l'))
    }
}

fn bounded_options<'de, D: serde::Deserializer<'de>>(decoder: D) -> Result<Options, D::Error> {
    struct OptionList;

    impl<'de> Visitor<'de> for OptionList {
        type Value = Options;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("bounded storage options")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            if seq.size_hint().is_some_and(|len| len > MAX_METADATA_ITEMS) {
                return Err(de::Error::custom("too many storage options"));
            }
            let mut options = Vec::new();
            let mut seen = BTreeSet::new();
            while let Some((name, value)) = seq.next_element::<(&'de str, &'de str)>()? {
                if name.is_empty()
                    || name.len() > MAX_OPTION_BYTES
                    || value.len() > MAX_OPTION_BYTES
                    || options.len() == MAX_METADATA_ITEMS
                    || !seen.insert(name)
                {
                    return Err(de::Error::custom("invalid storage option"));
                }
                options.push((name.to_owned(), value.to_owned()));
            }
            Ok(options)
        }
    }

    decoder.deserialize_seq(OptionList)
}

fn bounded_index_options<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<BTreeMap<u64, Options>, D::Error> {
    bounded_map::<D, IndexOptions>(decoder).map(|map| {
        map.into_iter()
            .map(|(id, options)| (id, options.0))
            .collect()
    })
}

fn bounded_map<'de, D, V>(decoder: D) -> Result<BTreeMap<u64, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    V: Deserialize<'de>,
{
    struct MetadataMap<V>(PhantomData<V>);

    impl<'de, V: Deserialize<'de>> Visitor<'de> for MetadataMap<V> {
        type Value = BTreeMap<u64, V>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("bounded storage metadata map")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            if map.size_hint().is_some_and(|len| len > MAX_METADATA_ITEMS) {
                return Err(de::Error::custom("too many storage metadata entries"));
            }
            let mut entries = BTreeMap::new();
            while let Some((id, value)) = map.next_entry::<u64, V>()? {
                if entries.len() == MAX_METADATA_ITEMS || entries.insert(id, value).is_some() {
                    return Err(de::Error::custom("invalid storage metadata entry"));
                }
            }
            Ok(entries)
        }
    }

    decoder.deserialize_map(MetadataMap(PhantomData))
}

#[derive(Debug, Clone)]
struct OptionSpec {
    namespace: Option<String>,
    name: String,
    value: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct OptionChange {
    options: Vec<OptionSpec>,
    reset: bool,
    create_table: bool,
}

#[derive(Clone, Debug)]
pub(super) enum TableAction {
    ClusterBound(ClusterMarker),
    Options(OptionChange),
    Persistence(Persistence),
    Cluster(Option<String>),
    Column(String, ColumnAction),
}

#[derive(Clone, Debug)]
pub(crate) struct ClusterMarker {
    pub(super) key: String,
    pub(super) table: u64,
    pub(super) index: u64,
}

#[derive(Clone, Debug)]
pub(super) enum ColumnAction {
    Options(OptionChange),
    Statistics(i32),
    Storage(String),
    Compression(String),
}

#[derive(Clone, Copy)]
pub(super) enum OptionTarget {
    Heap,
    Toast,
    Btree,
    Gin,
    Column,
    View,
}

pub(super) fn parse_change(p: &mut Parser, reset: bool) -> R<OptionChange> {
    p.punct('(')?;
    let mut options = Vec::new();
    loop {
        let first = p.raw_name()?;
        let (namespace, name) = if p.sym('.') {
            (Some(first), p.raw_name()?)
        } else {
            (None, first)
        };
        let value = if matches!(p.tokens.get(p.at), Some(Token::Op(op)) if op == "=") {
            p.at += 1;
            Some(option_value(p)?)
        } else {
            None
        };
        options.push(OptionSpec {
            namespace,
            name,
            value,
        });
        if !p.sym(',') {
            break;
        }
    }
    p.punct(')')?;
    Ok(OptionChange {
        options,
        reset,
        create_table: false,
    })
}

fn option_value(p: &mut Parser) -> R<String> {
    let mut value = String::new();
    if let Some(Token::Op(sign)) = p.tokens.get(p.at)
        && matches!(sign.as_str(), "+" | "-")
    {
        value.push_str(sign);
        p.at += 1;
        if !matches!(p.tokens.get(p.at), Some(Token::Number(_))) {
            return Err(SqlError::Syntax);
        }
    }
    let Some(Token::String(raw) | Token::Number(raw) | Token::Word(raw) | Token::QuotedIdent(raw)) =
        p.tokens.get(p.at)
    else {
        return Err(SqlError::Syntax);
    };
    value.push_str(raw);
    p.at += 1;
    Ok(value)
}

pub(super) fn parse_create(p: &mut Parser) -> R<OptionChange> {
    if p.word("with") {
        let mut change = if p.word("oids") {
            OptionChange {
                options: vec![OptionSpec {
                    namespace: None,
                    name: "oids".into(),
                    value: None,
                }],
                ..OptionChange::default()
            }
        } else {
            parse_change(p, false)?
        };
        change.create_table = true;
        Ok(change)
    } else {
        if p.word("without") {
            p.need("oids")?;
        }
        Ok(OptionChange::default())
    }
}

pub(super) fn parse_table_set(p: &mut Parser) -> R<Option<TableAction>> {
    let action = if p.word("logged") {
        TableAction::Persistence(Persistence::Permanent)
    } else if p.word("unlogged") {
        TableAction::Persistence(Persistence::Unlogged)
    } else if p.word("without") {
        p.need("cluster")?;
        TableAction::Cluster(None)
    } else if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
        TableAction::Options(parse_change(p, false)?)
    } else {
        return Ok(None);
    };
    Ok(Some(action))
}

pub(super) fn parse_column(p: &mut Parser, set: bool) -> R<Option<ColumnAction>> {
    let action = if set && p.word("statistics") {
        let value = if p.word("default") {
            -1
        } else {
            option_value(p)?.parse().map_err(|_| SqlError::Syntax)?
        };
        ColumnAction::Statistics(value)
    } else if set && p.word("storage") {
        ColumnAction::Storage(p.raw_name()?)
    } else if set && p.word("compression") {
        ColumnAction::Compression(p.raw_name()?)
    } else if set && p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
        ColumnAction::Options(parse_change(p, false)?)
    } else if !set && p.word("reset") {
        ColumnAction::Options(parse_change(p, true)?)
    } else {
        return Ok(None);
    };
    Ok(Some(action))
}

enum Rule {
    Bool,
    Integer(i32, i32),
    Real(f64, f64),
    IndexCleanup,
}

fn rule(target: OptionTarget, name: &str) -> Option<Rule> {
    use OptionTarget::*;
    use Rule::*;
    match (target, name) {
        (Heap | Btree, "fillfactor") => Some(Integer(10, 100)),
        (Btree, "deduplicate_items") | (Gin, "fastupdate") => Some(Bool),
        (Gin, "gin_pending_list_limit") => Some(Integer(64, i32::MAX)),
        (Column, "n_distinct" | "n_distinct_inherited") => Some(Real(-1.0, f64::MAX)),
        (Heap, "parallel_workers") => Some(Integer(0, 1024)),
        (Heap, "toast_tuple_target") => Some(Integer(128, 8160)),
        (Heap, "user_catalog_table") | (Heap | Toast, "autovacuum_enabled" | "vacuum_truncate") => {
            Some(Bool)
        }
        (Heap | Toast, "vacuum_index_cleanup") => Some(IndexCleanup),
        (Heap | Toast, "vacuum_max_eager_freeze_failure_rate") => Some(Real(0.0, 1.0)),
        (Heap, "autovacuum_analyze_threshold") | (Heap | Toast, "autovacuum_vacuum_threshold") => {
            Some(Integer(0, i32::MAX))
        }
        (
            Heap | Toast,
            "autovacuum_vacuum_max_threshold"
            | "autovacuum_vacuum_insert_threshold"
            | "log_autovacuum_min_duration",
        ) => Some(Integer(-1, i32::MAX)),
        (Heap, "autovacuum_analyze_scale_factor")
        | (
            Heap | Toast,
            "autovacuum_vacuum_scale_factor"
            | "autovacuum_vacuum_insert_scale_factor"
            | "autovacuum_vacuum_cost_delay",
        ) => Some(Real(0.0, 100.0)),
        (Heap | Toast, "autovacuum_vacuum_cost_limit") => Some(Integer(1, 10_000)),
        (Heap | Toast, "autovacuum_freeze_min_age" | "autovacuum_multixact_freeze_min_age") => {
            Some(Integer(0, 1_000_000_000))
        }
        (Heap | Toast, "autovacuum_freeze_table_age" | "autovacuum_multixact_freeze_table_age") => {
            Some(Integer(0, 2_000_000_000))
        }
        (Heap | Toast, "autovacuum_freeze_max_age") => Some(Integer(100_000, 2_000_000_000)),
        (Heap | Toast, "autovacuum_multixact_freeze_max_age") => {
            Some(Integer(10_000, 2_000_000_000))
        }
        _ => None,
    }
}

fn invalid(message: impl Into<String>) -> SqlError {
    user_types::exact_sql_error(SqlError::InvalidParameterValue, message, None)
}

fn integer(value: &str) -> Option<i32> {
    let value = value.trim_ascii();
    let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
    let (digits, radix) = if let Some(hex) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        (hex, 16)
    } else if unsigned.starts_with('0') {
        (unsigned, 8)
    } else {
        (unsigned, 10)
    };
    let parsed = i64::from_str_radix(digits, radix).ok()?;
    i32::try_from(if value.starts_with('-') {
        -parsed
    } else {
        parsed
    })
    .ok()
}

fn validate(rule: Rule, name: &str, value: &str) -> R<()> {
    let invalid_value = |kind| {
        invalid(format!(
            "invalid value for {kind} option \"{name}\": {value}"
        ))
    };
    let bounds = |value: String, min: String, max: String| {
        user_types::exact_sql_error(
            SqlError::InvalidParameterValue,
            format!("value {value} out of bounds for option \"{name}\""),
            Some(&format!(
                "Valid values are between \"{min}\" and \"{max}\"."
            )),
        )
    };
    match rule {
        Rule::Bool if !gin_indexes::valid_boolean(value) => Err(invalid_value("boolean")),
        Rule::Integer(min, max) => {
            let value = integer(value).ok_or_else(|| invalid_value("integer"))?;
            if !(min..=max).contains(&value) {
                return Err(bounds(value.to_string(), min.to_string(), max.to_string()));
            }
            Ok(())
        }
        Rule::Real(min, max) => {
            let value = value
                .trim_ascii_start()
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite())
                .ok_or_else(|| invalid_value("floating point"))?;
            if !(min..=max).contains(&value) {
                return Err(bounds(
                    value.to_string(),
                    format!("{min:.6}"),
                    format!("{max:.6}"),
                ));
            }
            Ok(())
        }
        Rule::IndexCleanup
            if !["on", "off", "auto"]
                .iter()
                .any(|v| v.eq_ignore_ascii_case(value)) =>
        {
            Err(user_types::exact_sql_error(
                SqlError::InvalidParameterValue,
                format!("invalid value for enum option \"{name}\": {value}"),
                Some("Valid values are \"on\", \"off\", and \"auto\"."),
            ))
        }
        _ => Ok(()),
    }
}

impl OptionChange {
    pub(super) fn apply(&self, old: &[(String, String)], target: OptionTarget) -> R<Options> {
        let mut updates = Vec::new();
        let mut names = BTreeSet::new();
        for option in &self.options {
            if self.reset && option.value.is_some() {
                return Err(user_types::exact_sql_error(
                    SqlError::Syntax,
                    "RESET must not include values for parameters",
                    None,
                ));
            }
            let (target, key) = match &option.namespace {
                None => (target, option.name.clone()),
                Some(namespace) if matches!(target, OptionTarget::Heap) && namespace == "toast" => {
                    (OptionTarget::Toast, format!("toast.{}", option.name))
                }
                Some(_) if self.reset => continue,
                Some(namespace) => {
                    return Err(invalid(format!(
                        "unrecognized parameter namespace \"{namespace}\""
                    )));
                }
            };
            let value = option.value.as_deref().unwrap_or("true");
            if self.reset {
                names.insert(key);
                continue;
            }
            if self.create_table && matches!(target, OptionTarget::Heap) && option.name == "oids" {
                if !gin_indexes::valid_boolean(value) {
                    return Err(invalid(format!(
                        "invalid value for boolean option \"oids\": {value}"
                    )));
                }
                if !value.eq_ignore_ascii_case("off")
                    && value != "0"
                    && !["false", "no", "off"]
                        .iter()
                        .any(|v| v.starts_with(&value.to_ascii_lowercase()))
                {
                    return Err(user_types::exact_sql_error(
                        SqlError::Unsupported,
                        "tables declared WITH OIDS are not supported",
                        None,
                    ));
                }
                continue;
            }
            let validator = rule(target, &option.name)
                .ok_or_else(|| invalid(format!("unrecognized parameter \"{}\"", option.name)))?;
            if !names.insert(key.clone()) {
                return Err(invalid(format!(
                    "parameter \"{}\" specified more than once",
                    option.name
                )));
            }
            validate(validator, &option.name, value)?;
            if key.len() > MAX_OPTION_BYTES || value.len() > MAX_OPTION_BYTES {
                return Err(SqlError::Limit);
            }
            updates.push((key, value.to_owned()));
        }
        let options: Vec<_> = old
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .cloned()
            .chain(updates)
            .collect();
        if options.len() > MAX_METADATA_ITEMS {
            return Err(SqlError::Limit);
        }
        Ok(options)
    }
}

impl Relational {
    pub(super) fn table_storage(&self, table: u64) -> TableStorage {
        self.ext
            .table_storage
            .get(&table)
            .cloned()
            .unwrap_or_default()
    }

    pub(super) fn require_storage_metadata(&self) -> R<()> {
        if !self.v2_unique_reporting() {
            return Err(user_types::exact_sql_error(
                SqlError::Unsupported,
                "storage metadata requires the v2 engine",
                None,
            ));
        }
        Ok(())
    }

    pub(super) fn set_table_storage(&mut self, table: u64, storage: TableStorage) {
        if storage.is_empty() {
            Arc::make_mut(&mut self.ext).table_storage.remove(&table);
        } else {
            Arc::make_mut(&mut self.ext)
                .table_storage
                .insert(table, storage);
        }
    }

    pub(super) fn set_index_reloptions(&mut self, table: u64, index: u64, options: Options) {
        let mut storage = self.table_storage(table);
        if options.is_empty() {
            storage.index_reloptions.remove(&index);
        } else {
            storage.index_reloptions.insert(index, options);
        }
        self.set_table_storage(table, storage);
    }

    pub(super) fn has_index_storage(&self, index: &secondary_index::Descriptor) -> bool {
        self.ext
            .table_storage
            .get(&index.table)
            .is_some_and(|storage| {
                storage.clustered_index == Some(index.id)
                    || storage.index_reloptions.contains_key(&index.id)
            })
    }

    pub(super) fn forget_primary_index_storage(&mut self, table: u64) {
        // A later action in the same ALTER TABLE can reuse the table ID for a new primary index.
        if self.ext.table_storage.contains_key(&table) {
            let mut storage = self.table_storage(table);
            storage.index_reloptions.remove(&table);
            if storage.clustered_index == Some(table) {
                storage.clustered_index = None;
            }
            self.set_table_storage(table, storage);
        }
    }

    pub(crate) fn prune_table_storage(&mut self) {
        if self.ext.table_storage.is_empty() {
            return;
        }
        let tables: BTreeMap<_, _> = self
            .tables
            .values()
            .map(|table| (table.id, table))
            .collect();
        let indexes: BTreeMap<_, _> = self
            .secondary_descriptors_engine()
            .iter()
            .map(|index| (index.id, index.table))
            .collect();
        let ext = Arc::make_mut(&mut self.ext);
        ext.table_storage.retain(|table_id, storage| {
            let Some(table) = tables.get(table_id) else {
                return false;
            };
            let live_index = |id: &u64| {
                (*id == *table_id && table.hidden_rowid().is_none())
                    || indexes.get(id) == Some(table_id)
                    || ext
                        .gin_indexes
                        .get(id)
                        .is_some_and(|index| index.table == *table_id)
            };
            storage
                .columns
                .retain(|id, _| table.columns.iter().any(|c| c.id == *id));
            storage.index_reloptions.retain(|id, _| live_index(id));
            if storage
                .clustered_index
                .as_ref()
                .is_some_and(|id| !live_index(id))
            {
                storage.clustered_index = None;
            }
            !storage.is_empty()
        });
    }
}

pub(super) fn cluster_index(state: &Relational, table: &TableDef, key: &str) -> R<u64> {
    let name = split_key(key).1;
    if let Some((_, index)) = state.gin_index(key)
        && index.table == table.id
    {
        return Err(user_types::exact_sql_error(
            SqlError::Unsupported,
            format!(
                "cannot cluster on index \"{name}\" because access method does not support clustering"
            ),
            None,
        ));
    }
    if let Some(index) = state
        .secondary_descriptors_engine()
        .iter()
        .find(|index| index.name == key && index.table == table.id)
    {
        if index.predicate.is_some() {
            return Err(user_types::exact_sql_error(
                SqlError::Unsupported,
                format!("cannot cluster on partial index \"{name}\""),
                None,
            ));
        }
        return Ok(index.id);
    }
    if sql_index::primary_table(state, key).is_some_and(|owner| owner.id == table.id) {
        return Ok(table.id);
    }
    if state.index_table_key(key).is_some() {
        return Err(user_types::exact_sql_error(
            SqlError::WrongObjectType,
            format!("\"{name}\" is not an index for table \"{}\"", table.name),
            None,
        ));
    }
    Err(user_types::exact_sql_error(
        SqlError::UndefinedObject,
        format!(
            "index \"{name}\" for table \"{}\" does not exist",
            table.name
        ),
        None,
    ))
}

pub(super) fn alter_table(state: &mut Relational, table: &TableDef, action: &TableAction) -> R<()> {
    let mut storage = state.table_storage(table.id);
    match action {
        TableAction::ClusterBound(marker) => {
            if table.id != marker.table
                || marker.index != table.id
                    && !state
                        .secondary_descriptors_engine()
                        .iter()
                        .any(|index| index.id == marker.index && index.table == table.id)
                || marker.index == table.id && state.primary_key_constraint_name(table).is_none()
            {
                return Err(SqlError::StaleSchema);
            }
            storage.clustered_index = Some(marker.index);
        }
        TableAction::Options(change) => {
            storage.reloptions = change.apply(&storage.reloptions, OptionTarget::Heap)?
        }
        TableAction::Persistence(persistence) => storage.persistence = *persistence,
        TableAction::Cluster(index) => {
            storage.clustered_index = index
                .as_ref()
                .map(|index| {
                    cluster_index(
                        state,
                        table,
                        &relation_key(state.table_schema(table.id), index),
                    )
                })
                .transpose()?;
        }
        TableAction::Column(name, action) => {
            let column = table
                .logical_columns()
                .find(|column| column.name == *name)
                .ok_or_else(|| {
                    user_types::exact_sql_error(
                        SqlError::UnknownColumn,
                        format!(
                            "column \"{name}\" of relation \"{}\" does not exist",
                            table.name
                        ),
                        None,
                    )
                })?;
            let mut options = storage.columns.remove(&column.id).unwrap_or_default();
            alter_column(&mut options, &column.data_type, action)?;
            if options.is_canonical() {
                storage.columns.insert(column.id, options);
            }
        }
    }
    state.set_table_storage(table.id, storage);
    Ok(())
}

pub(super) fn alter_index(
    state: &mut Relational,
    index: &sql_index::DropIndex,
    change: &OptionChange,
) -> R<()> {
    let name = split_key(&index.name).1.trim_start_matches('\0');
    let Some(table) = index.table.as_ref().and_then(|key| state.tables.get(key)) else {
        if state.relation_name_taken(&index.name) {
            return Err(user_types::exact_sql_error(
                SqlError::WrongObjectType,
                format!(r#""{name}" is not an index"#),
                None,
            ));
        }
        if index.optional {
            user_types::missing_object_notice("relation", name);
            return Ok(());
        }
        return Err(user_types::exact_sql_error(
            SqlError::UnknownTable,
            format!(r#"relation "{name}" does not exist"#),
            None,
        ));
    };
    let table_id = table.id;
    if let Some((id, gin)) = state.gin_index(&index.name) {
        let options = change.apply(&gin.options, OptionTarget::Gin)?;
        if options != gin.options {
            let epoch = state.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
            Arc::make_mut(&mut state.ext)
                .gin_indexes
                .get_mut(&id)
                .ok_or(SqlError::Type)?
                .options = options;
            state.epoch = epoch;
        }
        return Ok(());
    }
    let index_id = state
        .secondary_descriptors_engine()
        .iter()
        .find(|d| d.name == index.name)
        .map_or(table_id, |d| d.id);
    let old = state
        .ext
        .table_storage
        .get(&table_id)
        .and_then(|table| table.index_reloptions.get(&index_id))
        .map_or(&[][..], Vec::as_slice);
    let options = change.apply(old, OptionTarget::Btree)?;
    if options == old {
        return Ok(());
    }
    if !options.is_empty() {
        state.require_storage_metadata()?;
    }
    let epoch = state.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
    state.set_index_reloptions(table_id, index_id, options);
    state.epoch = epoch;
    Ok(())
}

fn default_column_storage(ty: &DataType) -> u8 {
    match ty {
        DataType::Numeric => b'm',
        DataType::Text
        | DataType::Varchar
        | DataType::Bpchar
        | DataType::Bytes
        | DataType::Jsonb
        | DataType::TsVector
        | DataType::IntArray
        | DataType::BigIntArray
        | DataType::TextArray
        | DataType::UuidArray
        | DataType::RealArray
        | DataType::PgCharArray
        | DataType::SmallIntArray
        | DataType::RegDictionaryArray
        | DataType::ExpressionArray(_) => b'x',
        _ => b'p',
    }
}

fn alter_column(options: &mut ColumnStorage, ty: &DataType, action: &ColumnAction) -> R<()> {
    match action {
        ColumnAction::Options(change) => {
            options.options = change.apply(&options.options, OptionTarget::Column)?
        }
        ColumnAction::Statistics(value) => {
            if *value < -1 {
                return Err(invalid(format!("statistics target {value} is too low")));
            }
            if *value > 10_000 {
                user_types::sql_notice("lowering statistics target to 10000".into(), "22023");
            }
            options.stattarget = (*value >= 0).then_some((*value).min(10_000) as i16);
        }
        ColumnAction::Storage(value) => {
            let default = default_column_storage(ty);
            let code = match value.to_ascii_lowercase().as_str() {
                "default" => default,
                "plain" => b'p',
                "external" => b'e',
                "extended" => b'x',
                "main" => b'm',
                _ => return Err(invalid(format!("invalid storage type \"{value}\""))),
            };
            if code != b'p' && default == b'p' {
                return Err(user_types::exact_sql_error(
                    SqlError::Unsupported,
                    format!(
                        "column data type {} can only have storage PLAIN",
                        defaults::type_name(ty)
                    ),
                    None,
                ));
            }
            options.storage = (code != default).then_some(code);
        }
        ColumnAction::Compression(value) => {
            let code = match value.to_ascii_lowercase().as_str() {
                "default" => None,
                "pglz" => Some(b'p'),
                "lz4" => Some(b'l'),
                _ => return Err(invalid(format!("invalid compression method \"{value}\""))),
            };
            if code.is_some() && default_column_storage(ty) == b'p' {
                return Err(user_types::exact_sql_error(
                    SqlError::Unsupported,
                    format!(
                        "column data type {} does not support compression",
                        defaults::type_name(ty)
                    ),
                    None,
                ));
            }
            options.compression = code;
        }
    }
    Ok(())
}
