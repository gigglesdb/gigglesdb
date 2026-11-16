//! Read-only source inspection; failed or incomplete evidence blocks classification.
use super::{
    GtidSet,
    client::{MysqlClient, MysqlError},
    schema::load_descriptors,
};
use crate::{
    capability::{CapabilityItem, Criticality, Evidence, ItemStatus},
    definition::ObjectSelector,
};
use mysql_async::{Conn, prelude::Queryable};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub struct Options {
    pub estimated_copy_seconds: u64,
    pub planned_catchup_seconds: u64,
    pub max_decompressed_bytes: usize,
    pub selected_tables: Vec<ObjectSelector>,
}
impl Default for Options {
    fn default() -> Self {
        // BinlogDecoder takes an explicit limit; match the existing harness/fixture configuration.
        Self {
            estimated_copy_seconds: 0,
            planned_catchup_seconds: 0,
            max_decompressed_bytes: 16 * 1024 * 1024,
            selected_tables: Vec::new(),
        }
    }
}

macro_rules! checks {
    ($($name:ident => ($id:literal, $sql:literal, $fix:literal)),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum MysqlCheck { $($name),+ }
        impl MysqlCheck {
            pub fn id(self) -> &'static str { match self { $(Self::$name => $id),+ } }
            fn sql(self) -> &'static str { match self { $(Self::$name => $sql),+ } }
            fn remediation(self) -> &'static str { match self { $(Self::$name => $fix),+ } }
        }
        const CHECKS: &[MysqlCheck] = &[$(MysqlCheck::$name),+];
    }
}
checks! {
    Version => ("mysql_version", "SELECT @@version AS version,@@version_comment AS comment", "Use MySQL 8.0.x or 8.4.x; qualify forks separately. Server replacement requires a maintenance restart."),
    LogBin => ("mysql_log_bin", "SELECT @@global.log_bin AS value", "Set log_bin=mysql-bin in my.cnf [mysqld] and restart MySQL."),
    Format => ("mysql_binlog_format", "SELECT @@global.binlog_format AS value", "SET PERSIST binlog_format='ROW'; reconnect writers (existing sessions retain their setting); no server restart."),
    RowImage => ("mysql_binlog_row_image", "SELECT @@global.binlog_row_image AS value", "SET PERSIST binlog_row_image='FULL'; reconnect writers; no server restart."),
    RowValueOptions => ("mysql_binlog_row_value_options", "SELECT @@global.binlog_row_value_options AS value", "SET PERSIST binlog_row_value_options=''; reconnect writers; no server restart. PARTIAL_JSON breaks full row images."),
    RowMetadata => ("mysql_binlog_row_metadata", "SELECT @@global.binlog_row_metadata AS value", "SET PERSIST binlog_row_metadata='FULL'; no restart. MINIMAL requires descriptor column names."),
    Checksum => ("mysql_binlog_checksum", "SELECT @@global.binlog_checksum AS value", "SET PERSIST binlog_checksum='CRC32'; no restart."),
    Retention => ("mysql_binlog_retention", "SELECT @@global.binlog_expire_logs_seconds AS value", "SET PERSIST binlog_expire_logs_seconds=<copy_seconds+catchup_seconds>; on 8.0 SET PERSIST expire_logs_days=0 first. No restart. For unlimited retention use 0 and monitor disk."),
    ExpireDays => ("mysql_expire_logs_days", "SELECT @@global.expire_logs_days AS value", "On MySQL 8.0 SET PERSIST expire_logs_days=0; SET PERSIST binlog_expire_logs_seconds=<copy_seconds+catchup_seconds>; no restart."),
    GtidMode => ("mysql_gtid_mode", "SELECT @@global.gtid_mode AS value", "Finish the documented staged GTID transition to ON or OFF using SET GLOBAL gtid_mode, then persist the final value in my.cnf; do not skip transitional steps. No restart required."),
    GtidConsistency => ("mysql_enforce_gtid_consistency", "SELECT @@global.enforce_gtid_consistency AS value", "Record enforce_gtid_consistency; for GTID mode ON use SET PERSIST enforce_gtid_consistency=ON after resolving unsafe statements. No restart."),
    GtidExecuted => ("mysql_gtid_executed", "SELECT @@global.gtid_executed AS value", "Use a source without tagged GTIDs or qualify a tagged-GTID-capable client before capture; never reset GTIDs to bypass this check. No configuration restart fixes client support."),
    Compression => ("mysql_binlog_compression", "SELECT @@global.binlog_transaction_compression AS enabled,@@global.max_binlog_cache_size AS cache", "SET PERSIST max_binlog_cache_size=<max_decompressed_bytes> or SET PERSIST binlog_transaction_compression=OFF; reconnect writers; no restart."),
    Grants => ("mysql_privileges", "SHOW GRANTS FOR CURRENT_USER()", "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO CURRENT_USER(); GRANT SELECT ON <schema>.<table> TO CURRENT_USER() for each selected table; no restart."),
    Roles => ("mysql_roles", "SELECT CURRENT_ROLE() AS value", "Activate required roles with SET ROLE <role>; grant visibility to resolve SHOW GRANTS FOR CURRENT_USER() USING <active roles>, then recollect. No restart."),
    Skedaddle => ("mysql_skedaddle_schema", "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='_skedaddle'", "Disclosed installer step: CREATE DATABASE IF NOT EXISTS _skedaddle; GRANT INSERT, UPDATE, DELETE, CREATE ON _skedaddle.* TO CURRENT_USER(); no restart. Preflight never installs it."),
    Identity => ("mysql_server_identity", "SELECT @@global.server_uuid AS uuid,@@global.server_id AS id", "Set a unique nonzero server_id in my.cnf [mysqld] and restart; preserve auto.cnf server_uuid for resume, or start a fresh migration after identity changes."),
    Table => ("mysql_table_engine", "SELECT ENGINE AS engine FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND TABLE_TYPE='BASE TABLE'", "Select an existing table, or ALTER TABLE <schema>.<table> ENGINE=InnoDB; table rebuild/locks required, no restart."),
    Key => ("mysql_primary_key", "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX", "ALTER TABLE <schema>.<table> ADD PRIMARY KEY (<non_null_unique_columns>); resolve duplicates first; table locks/rebuild may be required, no restart."),
    Spatial => ("mysql_spatial_columns", "SELECT COLUMN_NAME,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION", "Spatial columns await M7 mapping; explicitly exclude the table or ALTER TABLE <schema>.<table> DROP COLUMN <spatial_column> only after preserving its data separately; no restart."),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub check: MysqlCheck,
    pub object: Option<ObjectSelector>,
    pub sql: String,
    pub rows: Vec<Value>,
    pub error: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MysqlSnapshot {
    pub observations: Vec<Observation>,
}

fn redacted(error: mysql_async::Error) -> String {
    match error {
        mysql_async::Error::Server(e) => format!(
            "MySQL code {}, SQLSTATE {} (message redacted)",
            e.code, e.state
        ),
        _ => "MySQL query/transport error (details redacted)".into(),
    }
}
async fn observe(
    conn: &mut Conn,
    check: MysqlCheck,
    object: Option<ObjectSelector>,
    sql: String,
) -> Observation {
    let mut observation = Observation {
        check,
        object,
        sql,
        rows: Vec::new(),
        error: None,
    };
    match conn.query::<mysql_async::Row, _>(&observation.sql).await {
        Err(error) => observation.error = Some(redacted(error)),
        Ok(rows) => {
            for row in rows {
                let mut value = serde_json::Map::new();
                for (index, column) in row.columns_ref().iter().enumerate() {
                    let field = row.get::<mysql_async::Value, _>(index);
                    let field = match field {
                        Some(mysql_async::Value::NULL) => Some(Value::Null),
                        Some(mysql_async::Value::Bytes(bytes)) => {
                            String::from_utf8(bytes).ok().map(Value::String)
                        }
                        Some(mysql_async::Value::Int(n)) => Some(json!(n.to_string())),
                        Some(mysql_async::Value::UInt(n)) => Some(json!(n.to_string())),
                        _ => None,
                    };
                    match field {
                        Some(field) => {
                            value.insert(column.name_str().into_owned(), field);
                        }
                        _ => {
                            observation.error =
                                Some("Invalid metadata row (details redacted)".into())
                        }
                    }
                }
                observation.rows.push(Value::Object(value));
            }
        }
    }
    observation
}
fn literal(text: &str) -> String {
    // Hex literals do not depend on NO_BACKSLASH_ESCAPES or ANSI_QUOTES.
    format!(
        "CONVERT(X'{}' USING utf8mb4)",
        text.as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}
pub async fn collect(client: &mut MysqlClient, options: &Options) -> MysqlSnapshot {
    use MysqlCheck::*;
    let mut snapshot = MysqlSnapshot::default();
    for &check in CHECKS
        .iter()
        .filter(|&&c| !matches!(c, Table | Key | Spatial))
    {
        if check == ExpireDays && client.version != super::client::MysqlVersion::V80 {
            continue;
        }
        snapshot
            .observations
            .push(observe(&mut client.conn, check, None, check.sql().into()).await);
    }
    // Resolve active role grants without interpolating server text as executable SQL.
    if let Some(roles) = snapshot
        .observations
        .iter()
        .find(|o| o.check == Roles)
        .and_then(|o| text(o, "value"))
        && roles != "NONE"
    {
        let sql =
            role_list(roles).map(|roles| format!("SHOW GRANTS FOR CURRENT_USER() USING {roles}"));
        let observation = match sql {
            Some(sql) => observe(&mut client.conn, Grants, None, sql).await,
            None => Observation {
                check: Grants,
                object: None,
                sql: "SHOW GRANTS FOR CURRENT_USER() USING <unresolved roles>".into(),
                rows: vec![],
                error: Some("Active roles could not be resolved".into()),
            },
        };
        snapshot.observations.retain(|o| o.check != Grants);
        snapshot.observations.push(observation);
    }
    for table in &options.selected_tables {
        for check in [Table, Key, Spatial] {
            let sql = check
                .sql()
                .replacen('?', &literal(&table.schema), 1)
                .replacen('?', &literal(&table.name), 1);
            snapshot
                .observations
                .push(observe(&mut client.conn, check, Some(table.clone()), sql).await);
        }
        // Reuse the connector's descriptor validation. Raw catalog rows above remain evidence.
        match load_descriptors(
            &mut client.conn,
            &[(table.schema.clone(), table.name.clone())],
        )
        .await
        {
            Ok(descriptors) if descriptors.len() == 1 => {}
            Err(MysqlError::MissingPrimaryKey { .. }) => {} // The Key observation is the typed blocker.
            _ => {
                if let Some(observation) = snapshot
                    .observations
                    .iter_mut()
                    .rev()
                    .find(|o| o.check == Spatial)
                {
                    observation.error =
                        Some("Column descriptor validation failed (details redacted)".into());
                }
            }
        }
    }
    snapshot
}
fn text<'a>(observation: &'a Observation, key: &str) -> Option<&'a str> {
    observation.rows.first()?.get(key)?.as_str()
}
fn number(observation: &Observation, key: &str) -> Option<u64> {
    text(observation, key)?.parse().ok()
}
fn affirmative(ok: bool) -> ItemStatus {
    if ok {
        ItemStatus::Supported
    } else {
        ItemStatus::RequiresExternalChange
    }
}

// Tokenize SHOW GRANTS identifiers separately from punctuation/privilege keywords.
fn tokens(input: &str) -> Option<Vec<String>> {
    let mut chars = input.chars().peekable();
    let mut result = Vec::new();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        let mut token = String::new();
        if matches!(c, '`' | '\'') {
            token.push(c);
            loop {
                let ch = chars.next()?;
                if ch == c {
                    if chars.peek() == Some(&c) {
                        chars.next();
                        token.push(c);
                    } else {
                        break;
                    }
                } else if ch == '\\' {
                    token.push(chars.next()?);
                } else {
                    token.push(ch);
                }
            }
        } else if c.is_alphanumeric() || c == '_' {
            token.push(c);
            while chars
                .peek()
                .is_some_and(|c| c.is_alphanumeric() || *c == '_')
            {
                token.push(chars.next()?);
            }
        } else {
            token.push(c);
        }
        result.push(token);
    }
    Some(result)
}
fn role_list(input: &str) -> Option<String> {
    if input.contains('\\') {
        return None;
    } // Ambiguous SQL-mode-dependent account escaping fails closed.
    let tokens = tokens(input)?;
    let mut roles = Vec::new();
    for role in tokens.split(|t| t == ",") {
        if role.len() != 3
            || role[1] != "@"
            || !role[0].starts_with('`')
            || !role[2].starts_with('`')
        {
            return None;
        }
        roles.push(format!(
            "`{}`@`{}`",
            role[0][1..].replace('`', "``"),
            role[2][1..].replace('`', "``")
        ));
    }
    Some(roles.join(","))
}
type Grant = (Vec<String>, Option<String>, Option<String>);
fn grants(snapshot: &MysqlSnapshot) -> Option<Vec<Grant>> {
    let observation = snapshot
        .observations
        .iter()
        .find(|o| o.check == MysqlCheck::Grants)?;
    if observation.error.is_some() || observation.rows.is_empty() {
        return None;
    }
    let mut grants = Vec::new();
    for row in &observation.rows {
        let line = row.as_object()?.values().next()?.as_str()?;
        let t = tokens(line)?;
        if t.first()?.eq_ignore_ascii_case("REVOKE") {
            return None;
        } // Partial revokes fail closed.
        let Some(on) = t.iter().position(|t| t.eq_ignore_ascii_case("ON")) else {
            continue;
        };
        if !t[0].eq_ignore_ascii_case("GRANT")
            || t.get(on + 2)? != "."
            || !t.get(on + 4)?.eq_ignore_ascii_case("TO")
        {
            return None;
        }
        let privileges = t[1..on]
            .split(|t| t == ",")
            .map(|p| p.join(" ").to_ascii_uppercase())
            .collect();
        let scope = |s: &str| {
            if s == "*" {
                None
            } else {
                Some(s.strip_prefix('`').unwrap_or(s).to_owned())
            }
        };
        grants.push((privileges, scope(&t[on + 1]), scope(&t[on + 3])));
    }
    Some(grants)
}
fn permitted(
    snapshot: &MysqlSnapshot,
    privileges: &[&str],
    schema: &str,
    table: &str,
) -> ItemStatus {
    let Some(grants) = grants(snapshot) else {
        return ItemStatus::Unverified;
    };
    affirmative(privileges.iter().all(|privilege| {
        grants.iter().any(|(p, s, t)| {
            (s.is_none() || (schema != "*" && s.as_deref() == Some(schema)))
                && (t.is_none() || (table != "*" && t.as_deref() == Some(table)))
                && p.iter().any(|p| p == privilege || p == "ALL PRIVILEGES")
        })
    }))
}
fn classify_rows(o: &Observation, snapshot: &MysqlSnapshot, options: &Options) -> ItemStatus {
    use ItemStatus::*;
    use MysqlCheck::*;
    let value = text(o, "value").unwrap_or("");
    match o.check {
        Version => match (text(o, "version"), text(o, "comment")) {
            (Some(v), Some(_)) => {
                if super::client::MysqlVersion::parse(v).is_ok() {
                    Supported
                } else {
                    Unsupported
                }
            }
            _ => Unverified,
        },
        LogBin => affirmative(matches!(value, "1" | "ON")),
        Format => affirmative(value == "ROW"),
        RowImage => affirmative(value == "FULL"),
        RowValueOptions => affirmative(text(o, "value") == Some("")),
        RowMetadata => {
            if matches!(value, "FULL" | "MINIMAL") {
                Supported
            } else {
                Unverified
            }
        }
        Checksum => {
            if matches!(value, "CRC32" | "NONE") {
                Supported
            } else {
                Unsupported
            }
        }
        Retention | ExpireDays => {
            let Some(mut seconds) = number(o, "value") else {
                return Unverified;
            };
            if o.check == ExpireDays {
                seconds = seconds.saturating_mul(86400);
            }
            affirmative(
                seconds == 0
                    || seconds
                        >= options
                            .estimated_copy_seconds
                            .saturating_add(options.planned_catchup_seconds),
            )
        }
        GtidMode => affirmative(matches!(value, "ON" | "OFF")),
        GtidConsistency => {
            if matches!(value, "ON" | "OFF" | "WARN") {
                Supported
            } else {
                Unverified
            }
        }
        GtidExecuted => match value.parse::<GtidSet>() {
            Ok(set) if text(o, "value").is_some() => {
                if set.to_string().split(',').any(|g| {
                    g.split(':')
                        .nth(1)
                        .is_some_and(|s| s.chars().any(|c| !c.is_ascii_digit() && c != '-'))
                }) {
                    Unsupported
                } else {
                    Supported
                }
            }
            _ => Unverified,
        },
        Compression => match text(o, "enabled") {
            Some("0" | "OFF") => Supported,
            Some("1" | "ON") => number(o, "cache")
                .map(|n| affirmative(n <= options.max_decompressed_bytes as u64))
                .unwrap_or(Unverified),
            _ => Unverified,
        },
        Grants => {
            let replication = permitted(
                snapshot,
                &["REPLICATION SLAVE", "REPLICATION CLIENT"],
                "*",
                "*",
            );
            if replication != Supported {
                return replication;
            }
            options
                .selected_tables
                .iter()
                .map(|t| permitted(snapshot, &["SELECT"], &t.schema, &t.name))
                .find(|s| *s != Supported)
                .unwrap_or(Supported)
        }
        Roles => {
            if value == "NONE"
                || role_list(value).is_some_and(|roles| {
                    snapshot.observations.iter().any(|g| {
                        g.check == Grants
                            && g.error.is_none()
                            && g.sql == format!("SHOW GRANTS FOR CURRENT_USER() USING {roles}")
                    })
                })
            {
                Supported
            } else {
                Unverified
            }
        }
        Skedaddle => {
            if o.rows.is_empty() {
                RequiresExternalChange
            } else {
                permitted(
                    snapshot,
                    &["INSERT", "UPDATE", "DELETE", "CREATE"],
                    "_skedaddle",
                    "*",
                )
            }
        }
        Identity => affirmative(
            text(o, "uuid").is_some_and(|s| uuid::Uuid::parse_str(s).is_ok())
                && number(o, "id").is_some_and(|id| id > 0 && id <= u32::MAX.into()),
        ),
        Table => affirmative(text(o, "engine") == Some("InnoDB")),
        Key => affirmative(
            !o.rows.is_empty()
                && o.rows
                    .iter()
                    .all(|r| r["COLUMN_NAME"].as_str().is_some_and(|s| !s.is_empty())),
        ),
        Spatial => {
            if o.rows.is_empty() || o.rows.iter().any(|r| r["DATA_TYPE"].as_str().is_none()) {
                Unverified
            } else if o.rows.iter().any(|r| {
                matches!(
                    r["DATA_TYPE"].as_str(),
                    Some(
                        "geometry"
                            | "point"
                            | "linestring"
                            | "polygon"
                            | "multipoint"
                            | "multilinestring"
                            | "multipolygon"
                            | "geomcollection"
                            | "geometrycollection"
                    )
                )
            }) {
                Unsupported
            } else {
                Supported
            }
        }
    }
}
pub fn classify(snapshot: &MysqlSnapshot, options: &Options) -> Vec<CapabilityItem> {
    use MysqlCheck::*;
    let mut expected = Vec::new();
    for &check in CHECKS {
        if matches!(check, Table | Key | Spatial) {
            expected.extend(
                options
                    .selected_tables
                    .iter()
                    .map(|table| (check, Some(table.clone()))),
            );
        } else if check != ExpireDays
            || snapshot.observations.iter().any(|o| {
                o.check == Version && text(o, "version").is_some_and(|v| v.starts_with("8.0."))
            })
        {
            expected.push((check, None));
        }
    }
    let mut items = Vec::new();
    for (check, object) in expected {
        let observations: Vec<_> = snapshot
            .observations
            .iter()
            .filter(|o| o.check == check && o.object == object)
            .collect();
        let observation = observations.first().copied();
        let status = match observation {
            Some(o) if observations.len() == 1 && o.error.is_none() => {
                classify_rows(o, snapshot, options)
            }
            _ => ItemStatus::Unverified,
        };
        let mut remediation = match check {
            Retention | ExpireDays => check.remediation().replace(
                "<copy_seconds+catchup_seconds>",
                &options
                    .estimated_copy_seconds
                    .saturating_add(options.planned_catchup_seconds)
                    .to_string(),
            ),
            Compression => check.remediation().replace(
                "<max_decompressed_bytes>",
                &options.max_decompressed_bytes.to_string(),
            ),
            _ => check.remediation().to_owned(),
        };
        let identifier = |s: &str| format!("`{}`", s.replace('`', "``"));
        if let Some(table) = &object {
            remediation = remediation.replace(
                "<schema>.<table>",
                &format!("{}.{}", identifier(&table.schema), identifier(&table.name)),
            );
        }
        if check == Grants {
            remediation = format!(
                "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO CURRENT_USER(); {} No restart.",
                options
                    .selected_tables
                    .iter()
                    .map(|t| format!(
                        "GRANT SELECT ON {}.{} TO CURRENT_USER();",
                        identifier(&t.schema),
                        identifier(&t.name)
                    ))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        let mut item = CapabilityItem {
            id: check.id().into(),
            object,
            status,
            sampled: false,
            criticality: Criticality::Critical,
            remediation,
            evidence: vec![Evidence {
                source: observation
                    .map(|o| o.sql.as_str())
                    .unwrap_or(check.sql())
                    .into(),
                description: observation
                    .map(|o| json!({"rows":o.rows,"error":o.error}).to_string())
                    .unwrap_or("Missing observation".into()),
            }],
        };
        if check == RowMetadata
            && status == ItemStatus::Supported
            && observation.is_some_and(|o| text(o, "value") == Some("MINIMAL"))
        {
            item.criticality = Criticality::Advisory;
        }
        let advisory = match (check, observation) {
            (Version, Some(o)) => {
                text(o, "comment").is_some_and(|c| {
                    !matches!(
                        c,
                        "MySQL Community Server - GPL" | "MySQL Enterprise Server - Commercial"
                    )
                }) || text(o, "version").is_some_and(|v| v.contains('-'))
            }
            (Retention, Some(o)) => {
                number(o, "value") == Some(0)
                    && snapshot
                        .observations
                        .iter()
                        .find(|o| o.check == ExpireDays)
                        .is_none_or(|o| number(o, "value") == Some(0))
            }
            _ => false,
        };
        items.push(item.clone());
        if advisory {
            item.id.push_str("_advisory");
            item.criticality = Criticality::Advisory;
            item.remediation = if check == Version {
                "Fork identity is unqualified; qualify the vendor against MySQL behavior."
            } else {
                "No binlog expiry: monitor source disk and arrange explicit retention management."
            }
            .into();
            items.push(item);
        }
    }
    items
}

#[cfg(test)]
mod tests;
