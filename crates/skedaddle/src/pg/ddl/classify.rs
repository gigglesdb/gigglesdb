use super::{Column, DdlCommand, DdlError, DdlRecord};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableDescriptor {
    pub oid: u32,
    pub schema: String,
    pub columns: Vec<Column>,
    pub relation: Value,
}

impl TableDescriptor {
    pub fn from_command(command: &DdlCommand) -> Result<Self, DdlError> {
        let descriptor = Self {
            oid: command.objid,
            schema: command
                .schema_name
                .clone()
                .ok_or_else(|| DdlError("missing table schema".into()))?,
            columns: command
                .columns
                .clone()
                .ok_or_else(|| DdlError("missing column snapshot".into()))?,
            relation: command
                .relation
                .clone()
                .ok_or_else(|| DdlError("missing relation snapshot".into()))?,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<(), DdlError> {
        const RELATION_FIELDS: &[&str] = &[
            "kind",
            "partition",
            "filenode",
            "name",
            "namespace",
            "owner",
            "acl",
            "options",
            "replica_identity",
            "tablespace",
            "persistence",
            "access_method",
            "row_security",
            "force_row_security",
            "partition_key",
            "partition_bound",
            "constraints",
            "indexes",
            "triggers",
            "policies",
            "parents",
        ];
        const ATTRIBUTE_FIELDS: &[&str] = &[
            "type_oid",
            "typmod",
            "collation",
            "acl",
            "storage",
            "compression",
            "options",
            "fdwoptions",
            "local",
            "inhcount",
        ];
        if self.oid == 0
            || self.schema.is_empty()
            || RELATION_FIELDS
                .iter()
                .any(|key| self.relation.get(key).is_none())
        {
            return Err(DdlError("incomplete retained relation metadata".into()));
        }
        let mut names = BTreeSet::new();
        for (index, column) in self.columns.iter().enumerate() {
            if column.attnum != index as i32 + 1
                || !names.insert(&column.name)
                || ATTRIBUTE_FIELDS
                    .iter()
                    .any(|key| column.attributes.get(key).is_none())
            {
                return Err(DdlError("incomplete retained column metadata".into()));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedTable {
    pub version: u64,
    pub descriptor: TableDescriptor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SafeBoundaryStop {
    pub xid: String,
    pub schema_version: u64,
    pub reason: String,
    pub remediation: &'static str,
    pub capture_continues: bool,
    pub stop_apply_before_transaction: bool,
    pub readiness_blocked: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Classification {
    Supported { advanced_tables: BTreeMap<u32, u64> },
    SafeBoundaryStop(SafeBoundaryStop),
}

/// The initial version and descriptors must come from the same consistent
/// capture baseline. Invoke once per complete transaction, in commit order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaVersions {
    installation: String,
    last_version: u64,
    tables: BTreeMap<u32, VersionedTable>,
    blocker: Option<SafeBoundaryStop>,
}

const REMEDIATION: &str = "Keep capture buffering and stop apply before this entire transaction (including earlier DML). Quiesce DDL; update and qualify the mapping or restart from a fresh consistent snapshot before resuming readiness.";

/// Complete retained classifier state. Persistence must authenticate these bytes
/// and bind them to a source and an exact durable writer boundary before restore.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaVersionsSnapshot {
    version: u32,
    installation: String,
    last_version: u64,
    tables: Vec<VersionedTable>,
    blocker: Option<StoredStop>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredStop {
    xid: String,
    schema_version: u64,
    reason: String,
    remediation: String,
    capture_continues: bool,
    stop_apply_before_transaction: bool,
    readiness_blocked: bool,
}

impl SchemaVersions {
    pub fn snapshot(&self) -> SchemaVersionsSnapshot {
        SchemaVersionsSnapshot {
            version: 1,
            installation: self.installation.clone(),
            last_version: self.last_version,
            tables: self.tables.values().cloned().collect(),
            blocker: self.blocker.as_ref().map(|s| StoredStop {
                xid: s.xid.clone(),
                schema_version: s.schema_version,
                reason: s.reason.clone(),
                remediation: s.remediation.into(),
                capture_continues: s.capture_continues,
                stop_apply_before_transaction: s.stop_apply_before_transaction,
                readiness_blocked: s.readiness_blocked,
            }),
        }
    }

    pub fn restore(installation: &str, snapshot: SchemaVersionsSnapshot) -> Result<Self, DdlError> {
        if snapshot.version != 1
            || snapshot.installation != installation
            || snapshot.last_version > i64::MAX as u64
        {
            return Err(DdlError("schema snapshot installation/version".into()));
        }
        let mut restored = Self::new(installation.into(), snapshot.last_version, [])?;
        for table in snapshot.tables {
            table.descriptor.validate()?;
            if table.version > restored.last_version
                || restored
                    .tables
                    .insert(table.descriptor.oid, table)
                    .is_some()
            {
                return Err(DdlError(
                    "schema snapshot table version/duplicate OID".into(),
                ));
            }
        }
        if let Some(s) = snapshot.blocker {
            if s.xid.is_empty()
                || !s.xid.bytes().all(|b| b.is_ascii_digit())
                || s.schema_version == 0
                || s.schema_version > i64::MAX as u64
                || s.reason.is_empty()
                || s.remediation != REMEDIATION
                || !s.capture_continues
                || !s.stop_apply_before_transaction
                || !s.readiness_blocked
            {
                return Err(DdlError("schema snapshot safe-stop contract".into()));
            }
            restored.blocker = Some(SafeBoundaryStop {
                xid: s.xid,
                schema_version: s.schema_version,
                reason: s.reason,
                remediation: REMEDIATION,
                capture_continues: true,
                stop_apply_before_transaction: true,
                readiness_blocked: true,
            });
        }
        Ok(restored)
    }

    pub fn new(
        installation: String,
        version: u64,
        tables: impl IntoIterator<Item = TableDescriptor>,
    ) -> Result<Self, DdlError> {
        super::sql::validate_id(&installation)?;
        let mut retained = BTreeMap::new();
        for descriptor in tables {
            descriptor.validate()?;
            if retained
                .insert(
                    descriptor.oid,
                    VersionedTable {
                        version,
                        descriptor,
                    },
                )
                .is_some()
            {
                return Err(DdlError("duplicate in-scope table OID".into()));
            }
        }
        Ok(Self {
            installation,
            last_version: version,
            tables: retained,
            blocker: None,
        })
    }

    pub fn table(&self, oid: u32) -> Option<&VersionedTable> {
        self.tables.get(&oid)
    }

    pub(crate) fn mapped_versions(&self) -> impl Iterator<Item = (u32, u64)> + '_ {
        self.tables.iter().map(|(&oid, table)| (oid, table.version))
    }

    pub fn last_version(&self) -> u64 {
        self.last_version
    }

    pub fn blocker(&self) -> Option<&SafeBoundaryStop> {
        self.blocker.as_ref()
    }

    /// Work on a clone: an unsupported later statement cannot partially advance
    /// descriptors for an earlier statement in the same transaction.
    pub fn classify_transaction(&mut self, records: &[DdlRecord]) -> Classification {
        if let Some(blocker) = &self.blocker {
            return Classification::SafeBoundaryStop(blocker.clone());
        }
        let mut next = self.clone();
        let mut advanced_tables = BTreeMap::new();
        for record in records {
            let result = next.advance(record, records.first().map(|r| r.xid.as_str()));
            match result {
                Ok(advanced) => advanced_tables.extend(advanced),
                Err(reason) => {
                    let blocker = SafeBoundaryStop {
                        xid: record.xid.clone(),
                        schema_version: record.version,
                        reason,
                        remediation: REMEDIATION,
                        capture_continues: true,
                        stop_apply_before_transaction: true,
                        readiness_blocked: true,
                    };
                    self.blocker = Some(blocker.clone());
                    return Classification::SafeBoundaryStop(blocker);
                }
            }
        }
        *self = next;
        Classification::Supported { advanced_tables }
    }

    fn advance(
        &mut self,
        record: &DdlRecord,
        xid: Option<&str>,
    ) -> Result<BTreeMap<u32, u64>, String> {
        record.validate(&self.installation).map_err(|e| e.0)?;
        if Some(record.xid.as_str()) != xid
            || self.last_version.checked_add(1) != Some(record.version)
        {
            return Err("DDL transaction mismatch or schema-version gap/replay".into());
        }
        if accepted_tool_internal(record, &self.installation) {
            self.last_version = record.version;
            return Ok(BTreeMap::new());
        }
        if record.origin.is_some() {
            return Err("unrecognized administrative DDL origin or tag".into());
        }
        // Empty DROP output, GRANT lacking identity, global/type changes and
        // index OIDs cannot safely be attributed by this end-only hook.
        if record.tag != "ALTER TABLE" || record.commands.is_empty() {
            return Err(format!(
                "{} cannot be proven safe for mapped tables",
                record.tag
            ));
        }
        let mut advanced = BTreeMap::new();
        for command in &record.commands {
            if command.classid != 1259
                || command.objsubid != 0
                || command.command_tag != "ALTER TABLE"
                || command.object_type != "table"
                || command.in_extension
            {
                return Err("unattributed, extension or non-table DDL".into());
            }
            let Some(table) = self.tables.get_mut(&command.objid) else {
                // Unknown OIDs may affect mapped objects through dependencies.
                // S3 qualifies only known mapped ordinary tables.
                return Err("DDL references an unknown table OID; impact cannot be proven".into());
            };
            let after = TableDescriptor::from_command(command).map_err(|e| e.0)?;
            let additions = parse_additions(&record.query).map_err(|e| e.0)?;
            validate_additions(&table.descriptor, &after, &additions)?;
            table.descriptor = after;
            table.version = record.version;
            advanced.insert(command.objid, record.version);
        }
        self.last_version = record.version;
        Ok(advanced)
    }
}

/// Shared by capture classification and installation-bound apply. The caller
/// additionally checks transaction identity and ordered version continuity.
pub(crate) fn accepted_tool_internal(record: &DdlRecord, installation: &str) -> bool {
    record.validate(installation).is_ok()
        && record.origin.as_deref() == Some(installation)
        && tool_internal(record)
}

/// Origin is an integrity convenience. Safety comes from confining metadata
/// to `_skedaddle`, excluded by MigrationDefinition::validate and both
/// runner::selection entry points; these objects are never mapped/replicated.
fn tool_internal(record: &DdlRecord) -> bool {
    if matches!(record.tag.as_str(), "GRANT" | "REVOKE") {
        return true;
    }
    if record.commands.is_empty() {
        return matches!(
            record.tag.as_str(),
            "CREATE SCHEMA" | "CREATE TABLE" | "ALTER TABLE" | "CREATE INDEX"
        );
    }
    record.commands.iter().all(|command| {
        command.schema_name.as_deref() == Some("_skedaddle")
            || (command.schema_name.is_none()
                && command.command_tag == "CREATE SCHEMA"
                && command.object_type == "schema"
                && matches!(
                    command.object_identity.as_str(),
                    "_skedaddle" | "\"_skedaddle\""
                ))
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiteralDefault {
    Integer(i32),
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddedColumn {
    pub name: String,
    pub type_name: String,
    pub default: Option<LiteralDefault>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Additions {
    pub schema: Option<String>,
    pub table: String,
    pub columns: Vec<AddedColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Word(String),
    Quoted(String),
    Literal(String),
    Symbol(char),
}

/// Intentionally a narrow grammar, not a general SQL parser. Batches, comments,
/// expressions, custom types, NOT NULL, generated/identity/serial, and alternate
/// forms fail closed. Catalog diffs are mandatory in addition to this parse.
pub fn parse_additions(sql: &str) -> Result<Additions, DdlError> {
    let tokens = tokenize(sql)?;
    let mut parser = Parser {
        tokens: &tokens,
        offset: 0,
    };
    parser.keyword("alter")?;
    parser.keyword("table")?;
    let first = parser.identifier()?;
    let (schema, table) = if parser.symbol('.') {
        (Some(first), parser.identifier()?)
    } else {
        (None, first)
    };
    let mut columns = Vec::new();
    loop {
        parser.keyword("add")?;
        parser.keyword("column")?;
        let name = parser.identifier()?;
        let type_name = match parser.next() {
            Some(Token::Word(word)) if word == "integer" || word == "int" => "integer",
            Some(Token::Word(word)) if word == "text" => "text",
            _ => return Err(unsupported()),
        }
        .to_string();
        let default = if parser.peek_keyword("default") {
            parser.keyword("default")?;
            Some(match type_name.as_str() {
                "integer" => {
                    let negative = parser.symbol('-');
                    if !negative {
                        parser.symbol('+');
                    }
                    match parser.next() {
                        Some(Token::Word(number)) if number.bytes().all(|b| b.is_ascii_digit()) => {
                            let signed = if negative {
                                format!("-{number}")
                            } else {
                                number.clone()
                            };
                            LiteralDefault::Integer(signed.parse().map_err(|_| unsupported())?)
                        }
                        _ => return Err(unsupported()),
                    }
                }
                "text" => match parser.next() {
                    Some(Token::Literal(value)) => LiteralDefault::Text(value.clone()),
                    _ => return Err(unsupported()),
                },
                _ => return Err(unsupported()),
            })
        } else {
            None
        };
        columns.push(AddedColumn {
            name,
            type_name,
            default,
        });
        if !parser.symbol(',') {
            break;
        }
    }
    parser.symbol(';');
    if parser.offset != tokens.len() {
        return Err(unsupported());
    }
    Ok(Additions {
        schema,
        table,
        columns,
    })
}

fn unsupported() -> DdlError {
    DdlError("SQL is outside the qualified S3 ADD COLUMN grammar".into())
}

fn tokenize(sql: &str) -> Result<Vec<Token>, DdlError> {
    let mut chars = sql.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '\'' || ch == '"' {
            let mut value = String::new();
            loop {
                let Some(next) = chars.next() else {
                    return Err(unsupported());
                };
                if next == ch {
                    if chars.peek() == Some(&ch) {
                        chars.next();
                        value.push(ch);
                    } else {
                        break;
                    }
                } else if next == '\\' || next == '\0' {
                    return Err(unsupported());
                } else {
                    value.push(next);
                }
            }
            tokens.push(if ch == '\'' {
                Token::Literal(value)
            } else {
                Token::Quoted(value)
            });
        } else if ch.is_ascii_alphanumeric() || ch == '_' {
            let mut word = String::from(ch);
            while chars
                .peek()
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
            {
                word.push(chars.next().expect("peeked character"));
            }
            tokens.push(Token::Word(word.to_ascii_lowercase()));
        } else if ".,;+-".contains(ch) {
            tokens.push(Token::Symbol(ch));
        } else {
            return Err(unsupported());
        }
    }
    Ok(tokens)
}

struct Parser<'a> {
    tokens: &'a [Token],
    offset: usize,
}
impl Parser<'_> {
    fn next(&mut self) -> Option<&Token> {
        let t = self.tokens.get(self.offset);
        self.offset += usize::from(t.is_some());
        t
    }
    fn peek_keyword(&self, value: &str) -> bool {
        matches!(self.tokens.get(self.offset), Some(Token::Word(word)) if word == value)
    }
    fn keyword(&mut self, value: &str) -> Result<(), DdlError> {
        if self.peek_keyword(value) {
            self.offset += 1;
            Ok(())
        } else {
            Err(unsupported())
        }
    }
    fn symbol(&mut self, value: char) -> bool {
        if self.tokens.get(self.offset) == Some(&Token::Symbol(value)) {
            self.offset += 1;
            true
        } else {
            false
        }
    }
    fn identifier(&mut self) -> Result<String, DdlError> {
        match self.next() {
            Some(Token::Word(word))
                if word
                    .as_bytes()
                    .first()
                    .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_') =>
            {
                Ok(word.clone())
            }
            Some(Token::Quoted(value)) if !value.is_empty() => Ok(value.clone()),
            _ => Err(unsupported()),
        }
    }
}

fn validate_additions(
    before: &TableDescriptor,
    after: &TableDescriptor,
    additions: &Additions,
) -> Result<(), String> {
    if before.schema != after.schema
        || before.relation != after.relation
        || before.relation["kind"] != "r"
        || before.relation["partition"] != false
        || before.relation["parents"] != serde_json::json!([])
        || additions
            .schema
            .as_ref()
            .is_some_and(|s| s != &before.schema)
        || before.relation["name"].as_str() != Some(&additions.table)
        || after.columns.len() != before.columns.len() + additions.columns.len()
        || !after.columns.starts_with(&before.columns)
    {
        return Err("DDL changes existing metadata, rewrites the table, or does not append exactly the parsed columns".into());
    }
    for (column, added) in after.columns[before.columns.len()..]
        .iter()
        .zip(&additions.columns)
    {
        let (oid, collation, storage) = if added.type_name == "integer" {
            (23, 0, "p")
        } else {
            (25, 100, "x")
        };
        if column.name != added.name
            || column.type_name != added.type_name
            || column.notnull
            || column.dropped
            || !column.identity.is_empty()
            || !column.generated.is_empty()
            || column.attributes
                != serde_json::json!({
                    "type_oid":oid,"typmod":-1,"collation":collation,"acl":null,
                    "storage":storage,"compression":"","options":null,"fdwoptions":null,"local":true,"inhcount":0
                })
        {
            return Err(
                "new column is not a qualified nullable built-in integer/text column".into(),
            );
        }
        let valid_default = match &added.default {
            None => column.default.is_none() && !column.missing && column.missing_value.is_none(),
            Some(LiteralDefault::Integer(value)) => {
                let canonical = value.to_string();
                let expression = column.default.as_deref().unwrap_or("");
                // pg_get_expr parenthesizes negative int4 constants.
                (expression == canonical
                    || expression == format!("'{canonical}'::integer")
                    || expression == format!("({canonical})"))
                    && column.missing
                    && column.missing_value == Some(serde_json::json!([value]))
            }
            Some(LiteralDefault::Text(value)) => {
                column.default == Some(format!("'{}'::text", value.replace('\'', "''")))
                    && column.missing
                    && column.missing_value == Some(serde_json::json!([value]))
            }
        };
        if !valid_default {
            return Err("default/missing value is not the captured deterministic literal".into());
        }
    }
    Ok(())
}
