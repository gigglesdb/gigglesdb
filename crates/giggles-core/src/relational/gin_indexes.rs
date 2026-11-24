//! GIN catalog metadata. GIN indexes have no physical index pages.
use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct Definition {
    pub opclasses: Vec<Option<String>>,
    pub options: Vec<(String, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GinIndex {
    pub table: u64,
    pub name: String,
    pub columns: Vec<u64>,
    pub opclasses: Vec<u32>,
    pub options: Vec<(String, String)>,
}

struct OperatorClass {
    name: &'static str,
    oid: u32,
    matches_type: fn(&DataType) -> bool,
    default: bool,
}

impl GinIndex {
    pub(crate) fn validate(&self, table: &TableDef) -> R<()> {
        let (schema, name) = split_key(&self.name);
        ident::ident(name)?;
        if self.table != table.id
            || self.columns.is_empty()
            || self.columns.len() > 32
            || self.columns.len() != self.opclasses.len()
            || (!schema.is_empty()
                && split_key(&table.name).0 != "public"
                && schema != split_key(&table.name).0)
        {
            return Err(SqlError::Type);
        }
        for (&column, &oid) in self.columns.iter().zip(&self.opclasses) {
            let column = table
                .logical_columns()
                .find(|c| c.id == column)
                .ok_or(SqlError::UnknownColumn)?;
            if !OPCLASSES
                .iter()
                .any(|class| class.oid == oid && (class.matches_type)(&column.data_type))
            {
                return Err(SqlError::Type);
            }
        }
        if self
            .options
            .iter()
            .any(|(name, value)| match name.as_str() {
                "fastupdate" => !valid_boolean(value),
                "gin_pending_list_limit" => !value.parse::<i32>().is_ok_and(|value| value >= 64),
                _ => true,
            })
        {
            return Err(SqlError::Type);
        }
        Ok(())
    }
}

const OPCLASSES: &[OperatorClass] = &[
    OperatorClass {
        name: "array_ops",
        oid: 10064,
        matches_type: |ty| {
            matches!(
                ty,
                DataType::IntArray
                    | DataType::BigIntArray
                    | DataType::TextArray
                    | DataType::UuidArray
            )
        },
        default: true,
    },
    OperatorClass {
        name: "tsvector_ops",
        oid: 10073,
        matches_type: |ty| *ty == DataType::TsVector,
        default: true,
    },
    OperatorClass {
        name: "jsonb_ops",
        oid: 10090,
        matches_type: |ty| *ty == DataType::Jsonb,
        default: true,
    },
    OperatorClass {
        name: "jsonb_path_ops",
        oid: 10091,
        matches_type: |ty| *ty == DataType::Jsonb,
        default: false,
    },
];

fn valid_boolean(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    matches!(lower.as_str(), "0" | "1" | "on" | "of" | "off")
        || (!lower.is_empty()
            && ["true", "false", "yes", "no"]
                .iter()
                .any(|word| word.starts_with(&lower)))
}

pub(super) fn parse_options(p: &mut Parser) -> R<Vec<(String, String)>> {
    p.punct('(')?;
    let mut options = Vec::new();
    loop {
        let name = p.raw_name()?;
        let mut value = "true".to_owned();
        if matches!(p.tokens.get(p.at), Some(Token::Op(op)) if op == "=") {
            p.at += 1;
            let (Token::Word(word)
            | Token::String(word)
            | Token::Number(word)
            | Token::QuotedIdent(word)) = p.tokens.get(p.at).ok_or(SqlError::Syntax)?
            else {
                return Err(SqlError::Syntax);
            };
            value.clone_from(word);
            p.at += 1;
        }
        let error = match name.as_str() {
            "fastupdate" if !valid_boolean(&value) => Some(format!(
                "invalid value for boolean option \"fastupdate\": {value}"
            )),
            "gin_pending_list_limit" => match value.parse::<i32>() {
                Ok(value) if value < 64 => {
                    user_types::sql_error_detail(
                        "Valid values are between \"64\" and \"2147483647\".".into(),
                    );
                    Some(format!(
                        "value {value} out of bounds for option \"gin_pending_list_limit\""
                    ))
                }
                Ok(_) => None,
                Err(_) => Some(format!(
                    "invalid value for integer option \"gin_pending_list_limit\": {value}"
                )),
            },
            "fastupdate" => None,
            _ => Some(format!("unrecognized parameter \"{name}\"")),
        };
        if let Some(error) = error {
            user_types::view_replace_error(error);
            return Err(SqlError::InvalidParameterValue);
        }
        options.push((name, value));
        if !p.sym(',') {
            break;
        }
    }
    p.punct(')')?;
    Ok(options)
}

fn opclass(ty: &DataType, explicit: Option<&str>) -> R<u32> {
    if let Some(name) = explicit {
        let class = OPCLASSES
            .iter()
            .find(|class| class.name == name)
            .ok_or_else(|| {
                user_types::view_replace_error(format!(
                    "operator class \"{name}\" does not exist for access method \"gin\""
                ));
                SqlError::UndefinedObject
            })?;
        return if (class.matches_type)(ty) {
            Ok(class.oid)
        } else {
            user_types::view_replace_error(format!(
                "operator class \"{name}\" does not accept data type {}",
                type_name(ty)
            ));
            Err(SqlError::Type)
        };
    }
    OPCLASSES.iter().find(|class| class.default && (class.matches_type)(ty)).map(|class| class.oid)
        .ok_or_else(|| {
            user_types::view_replace_error(format!("data type {} has no default operator class for access method \"gin\"", type_name(ty)));
            user_types::sql_error_hint("You must specify an operator class for the index or define a default operator class for the data type.");
            SqlError::UndefinedObject
        })
}

fn type_name(ty: &DataType) -> &'static str {
    match ty {
        DataType::Jsonb => "jsonb",
        DataType::IntArray => "integer[]",
        DataType::BigIntArray => "bigint[]",
        DataType::TextArray => "text[]",
        DataType::UuidArray => "uuid[]",
        _ => defaults::type_name(ty),
    }
}

pub(super) fn create(
    state: &mut Relational,
    table: &TableDef,
    index: &sql_index::CreateIndex,
    definition: &Definition,
) -> R<()> {
    let name = relation_key(state.table_schema(table.id), &index.name);
    if state.relation_name_taken(&name) || sql_index::primary_table(state, &name).is_some() {
        if index.if_not_exists {
            user_types::sql_notice(
                format!("relation \"{}\" already exists, skipping", index.name),
                "42P07",
            );
            return Ok(());
        }
        user_types::view_replace_error(format!("relation \"{}\" already exists", index.name));
        return Err(SqlError::DuplicateTable);
    }
    if index.unique {
        user_types::view_replace_error(
            "access method \"gin\" does not support unique indexes".into(),
        );
        return Err(SqlError::Unsupported);
    }
    if index.collations.iter().any(Option::is_some) {
        return Err(SqlError::Unsupported);
    }
    let mut columns = Vec::new();
    let mut opclasses = Vec::new();
    for (column, class) in index.columns.iter().zip(&definition.opclasses) {
        let column = &table.columns[col(table, column)?];
        generated_columns::check_index_columns(state, &[column.id])?;
        columns.push(column.id);
        opclasses.push(opclass(&column.data_type, class.as_deref())?);
    }
    let created = GinIndex {
        table: table.id,
        name,
        columns,
        opclasses,
        options: definition.options.clone(),
    };
    created.validate(table)?;
    let next_epoch = state.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
    let id = allocate(&mut state.next_id)?;
    Arc::make_mut(&mut state.ext)
        .gin_indexes
        .insert(id, created);
    state.epoch = next_epoch;
    Ok(())
}

impl Relational {
    pub(crate) fn restore_table_metadata(
        &mut self,
        gin: BTreeMap<u64, GinIndex>,
        generated: BTreeMap<u64, generated_columns::GeneratedColumn>,
    ) {
        let ext = Arc::make_mut(&mut self.ext);
        ext.gin_indexes = gin;
        ext.generated_columns = generated;
    }

    pub(super) fn gin_index(&self, key: &str) -> Option<(u64, &GinIndex)> {
        self.ext
            .gin_indexes
            .iter()
            .find(|(_, index)| index.name == key)
            .map(|(id, index)| (*id, index))
    }

    pub(super) fn edit_gin_index(&mut self, key: &str, rename: Option<&str>) -> R<bool> {
        let Some((id, _)) = self.gin_index(key) else {
            return Ok(false);
        };
        let epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
        if let Some(name) = rename {
            let name = relation_key(split_key(key).0, name);
            if self.relation_name_taken(&name) || sql_index::primary_table(self, &name).is_some() {
                return Err(SqlError::DuplicateTable);
            }
            Arc::make_mut(&mut self.ext)
                .gin_indexes
                .get_mut(&id)
                .ok_or(SqlError::Type)?
                .name = name;
        } else {
            Arc::make_mut(&mut self.ext).gin_indexes.remove(&id);
        }
        self.epoch = epoch;
        Ok(true)
    }
}
