//! PostgreSQL catalog snapshot loading.
use super::{
    Column, ColumnId, ColumnRef, DependentKind, DependentObject, Relation, RelationKind, Snapshot,
};
use crate::{
    error::{NyCode, Refusal},
    value::ColumnType,
};
use futures::FutureExt;
use tokio_postgres::{Client, Row, types::FromSqlOwned};
/// Schema selection and name resolution settings.
pub struct PgSchemaOptions {
    /// Exact schemas; empty selects all non-system schemas.
    pub schemas: Vec<String>,
    /// Search path retained for name resolution.
    pub search_path: Vec<String>,
}
fn unavailable() -> Refusal {
    Refusal::new(NyCode::SchemaStale, "catalog snapshot unavailable")
}
fn get<T: FromSqlOwned>(r: &Row, i: usize) -> Result<T, Refusal> {
    r.try_get(i).map_err(|_| unavailable())
}
// In pinned tokio-postgres 0.7.15, the first poll queues batch_execute's request.
// This guard also queues rollback when the loader is cancelled while awaiting I/O.
struct Rollback<'a> {
    client: &'a Client,
    armed: bool,
}
impl Drop for Rollback<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.client.batch_execute("ROLLBACK").now_or_never();
        }
    }
}
const SCHEMAS: &str = "SELECT nspname::text FROM pg_catalog.pg_namespace WHERE (cardinality($1::text[])>0 AND nspname=ANY($1)) OR (cardinality($1::text[])=0 AND nspname NOT IN ('pg_catalog','information_schema','pg_toast') AND nspname NOT LIKE 'pg_temp%' AND nspname NOT LIKE 'pg_toast_temp%') ORDER BY 1";
const RELATIONS: &str = "SELECT c.oid,n.nspname::text,c.relname::text,c.relkind::text,CASE WHEN c.relkind IN ('v','m') THEN pg_catalog.pg_get_viewdef(c.oid,true) END FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=ANY($1::text[]) ORDER BY 2,3";
const COLUMNS: &str = "SELECT a.attname::text,pg_catalog.format_type(a.atttypid,a.atttypmod),t.typtype::text,CASE WHEN t.typelem<>0 AND t.typlen=-1 THEN pg_catalog.format_type(t.typelem,NULL) END,a.atttypmod,NOT a.attnotnull,CASE WHEN a.attgenerated<>'' THEN pg_catalog.pg_get_expr(d.adbin,d.adrelid) END,CASE WHEN a.attgenerated='' THEN pg_catalog.pg_get_expr(d.adbin,d.adrelid) END,et.typtype::text,a.attnum FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid LEFT JOIN pg_catalog.pg_type et ON et.oid=t.typelem LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum";
const PRIMARY_KEY: &str = "SELECT a.attname::text FROM pg_catalog.pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord) JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE i.indrelid=$1 AND i.indisprimary AND k.ord<=i.indnkeyatts ORDER BY k.ord";
const DEPENDENTS: &str = "SELECT 'trigger'::text,tgname::text FROM pg_catalog.pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal UNION ALL SELECT 'rule',rulename::text FROM pg_catalog.pg_rewrite WHERE ev_class=$1 AND rulename<>'_RETURN' UNION ALL SELECT 'policy',polname::text FROM pg_catalog.pg_policy WHERE polrelid=$1 ORDER BY 1,2";
const VIEW_READS: &str = "SELECT DISTINCT n.nspname::text,c.relname::text,a.attname::text FROM pg_catalog.pg_rewrite r JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_rewrite'::regclass AND d.objid=r.oid AND d.refclassid='pg_catalog.pg_class'::regclass JOIN pg_catalog.pg_class c ON c.oid=d.refobjid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND (d.refobjsubid=0 OR a.attnum=d.refobjsubid) WHERE r.ev_class=$1 AND d.refobjid<>r.ev_class ORDER BY 1,2,3";
const ROUTINES: &str = "SELECT p.proname::text FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=ANY($1::text[]) UNION SELECT o.oprname::text FROM pg_catalog.pg_operator o JOIN pg_catalog.pg_namespace n ON n.oid=o.oprnamespace WHERE n.nspname=ANY($1::text[]) ORDER BY 1";
/// Capture a consistent snapshot; the supplied client must be idle and exclusively used.
pub async fn load_pg_snapshot(
    client: &Client,
    opts: &PgSchemaOptions,
    now_unix_ms: u64,
) -> Result<Snapshot, Refusal> {
    let mut rollback = Rollback {
        client,
        armed: true,
    };
    let result = async {
        client
            .batch_execute("BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|_| unavailable())?;
        let schemas = client
            .query(SCHEMAS, &[&opts.schemas])
            .await
            .map_err(|_| unavailable())?
            .iter()
            .map(|r| get::<String>(r, 0))
            .collect::<Result<Vec<_>, _>>()?;
        let mut column_ids = std::collections::BTreeMap::new();
        let mut relations = Vec::new();
        let mut dependents = Vec::new();
        for row in client
            .query(RELATIONS, &[&schemas])
            .await
            .map_err(|_| unavailable())?
        {
            let oid: u32 = get(&row, 0)?;
            let schema: String = get(&row, 1)?;
            let name: String = get(&row, 2)?;
            let kind = match get::<String>(&row, 3)?.as_str() {
                "r" | "p" => RelationKind::Table,
                "v" => RelationKind::View,
                "m" => RelationKind::MaterializedView,
                "f" => RelationKind::Foreign,
                _ => RelationKind::Other,
            };
            let view_definition = get::<Option<String>>(&row, 4)?
                .map(|s| s.trim_end().trim_end_matches(';').trim_end().to_owned());
            let mut columns = Vec::new();
            for r in client
                .query(COLUMNS, &[&oid])
                .await
                .map_err(|_| unavailable())?
            {
                let typtype = get::<String>(&r, 2)?
                    .chars()
                    .next()
                    .ok_or_else(unavailable)?;
                let column_name: String = get(&r, 0)?;
                column_ids.insert(
                    ColumnRef {
                        schema: schema.clone(),
                        table: name.clone(),
                        column: column_name.clone(),
                    },
                    ColumnId::Pg {
                        relation_oid: oid,
                        attnum: get::<i16>(&r, 9)?,
                    },
                );
                columns.push(Column {
                    name: column_name,
                    ty: map_pg_type_with_element(
                        &get::<String>(&r, 1)?,
                        typtype,
                        get::<Option<String>>(&r, 3)?.as_deref(),
                        get::<Option<String>>(&r, 8)?.and_then(|s| s.chars().next()),
                        get(&r, 4)?,
                    ),
                    nullable: get(&r, 5)?,
                    generated: get(&r, 6)?,
                    default: get(&r, 7)?,
                });
            }
            let reads: Vec<_> = columns
                .iter()
                .map(|c| ColumnRef {
                    schema: schema.clone(),
                    table: name.clone(),
                    column: c.name.clone(),
                })
                .collect();
            for c in &columns {
                if c.generated.is_some() {
                    dependents.push(DependentObject {
                        kind: DependentKind::GeneratedColumn,
                        name: format!("{name}.{}", c.name),
                        reads: reads.clone(),
                        writes_table: None,
                    });
                }
            }
            for r in client
                .query(DEPENDENTS, &[&oid])
                .await
                .map_err(|_| unavailable())?
            {
                let kind = match get::<String>(&r, 0)?.as_str() {
                    "trigger" => DependentKind::Trigger,
                    "rule" => DependentKind::Rule,
                    "policy" => DependentKind::Policy,
                    _ => return Err(unavailable()),
                };
                dependents.push(DependentObject {
                    kind,
                    name: get(&r, 1)?,
                    reads: reads.clone(),
                    writes_table: None,
                });
            }
            if matches!(kind, RelationKind::View | RelationKind::MaterializedView) {
                let mut reads = Vec::new();
                for r in client
                    .query(VIEW_READS, &[&oid])
                    .await
                    .map_err(|_| unavailable())?
                {
                    reads.push(ColumnRef {
                        schema: get(&r, 0)?,
                        table: get(&r, 1)?,
                        column: get(&r, 2)?,
                    });
                }
                dependents.push(DependentObject {
                    kind: DependentKind::View,
                    name: format!("{schema}.{name}"),
                    reads,
                    writes_table: None,
                });
            }
            let primary_key = client
                .query(PRIMARY_KEY, &[&oid])
                .await
                .map_err(|_| unavailable())?
                .iter()
                .map(|r| get(r, 0))
                .collect::<Result<Vec<_>, _>>()?;
            relations.push(Relation {
                schema,
                name,
                kind,
                columns,
                view_definition,
                primary_key,
            });
        }
        let routines = client
            .query(ROUTINES, &[&schemas])
            .await
            .map_err(|_| unavailable())?
            .iter()
            .map(|r| get(r, 0))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(
            Snapshot::new(relations, dependents, opts.search_path.clone(), now_unix_ms)
                .with_column_ids(column_ids)
                .with_user_routines(routines)
                .with_captured_schemas(schemas),
        )
    }
    .await;
    client
        .batch_execute(if result.is_ok() { "COMMIT" } else { "ROLLBACK" })
        .await
        .map_err(|_| unavailable())?;
    rollback.armed = false;
    result
}
/// Map internal and formatted PostgreSQL type names, preserving unsupported types.
pub fn map_pg_type(
    typname: &str,
    typtype: char,
    elem_typname: Option<&str>,
    typmod: i32,
) -> ColumnType {
    map_pg_type_with_element(typname, typtype, elem_typname, Some('b'), typmod)
}
fn map_pg_type_with_element(
    typname: &str,
    typtype: char,
    elem_typname: Option<&str>,
    elem_typtype: Option<char>,
    typmod: i32,
) -> ColumnType {
    if typtype == 'e' {
        return ColumnType::Enum(typname.into());
    }
    if typtype != 'b' {
        return ColumnType::Other(typname.into());
    }
    if let Some(elem) = elem_typname {
        return ColumnType::Array(Box::new(map_pg_type(elem, elem_typtype.unwrap_or('?'), None, typmod)));
    }
    let length = if typmod >= 0 { Some(typmod - 4) } else { None };
    match typname {
        "text" => ColumnType::Text,
        "varchar" | "character varying" => ColumnType::Varchar(length),
        n if n.starts_with("varchar(") || n.starts_with("character varying(") => {
            ColumnType::Varchar(length)
        }
        "bpchar" | "character" => ColumnType::Bpchar(length),
        n if n.starts_with("bpchar(") || n.starts_with("character(") => ColumnType::Bpchar(length),
        "int2" | "smallint" => ColumnType::Int2,
        "int4" | "integer" => ColumnType::Int4,
        "int8" | "bigint" => ColumnType::Int8,
        "uuid" => ColumnType::Uuid,
        "date" => ColumnType::Date,
        "timestamp" | "timestamp without time zone" => ColumnType::Timestamp,
        n if n.starts_with("timestamp(") && n.ends_with("without time zone") => {
            ColumnType::Timestamp
        }
        "timestamptz" | "timestamp with time zone" => ColumnType::Timestamptz,
        n if n.starts_with("timestamp(") && n.ends_with("with time zone") => {
            ColumnType::Timestamptz
        }
        "bool" | "boolean" => ColumnType::Bool,
        "numeric" => ColumnType::Numeric,
        n if n.starts_with("numeric(") => ColumnType::Numeric,
        "bytea" => ColumnType::Bytea,
        "json" => ColumnType::Json,
        "jsonb" => ColumnType::Jsonb,
        _ => ColumnType::Other(typname.into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn loader_maps_enum_array() {
        assert_eq!(
            map_pg_type_with_element("mood[]", 'b', Some("mood"), Some('e'), -1),
            ColumnType::Array(Box::new(ColumnType::Enum("mood".into())))
        );
    }
    #[test]
    fn maps_all_type_branches() {
        for (n, t) in [
            ("text", ColumnType::Text),
            ("varchar", ColumnType::Varchar(None)),
            ("bpchar", ColumnType::Bpchar(None)),
            ("int2", ColumnType::Int2),
            ("int4", ColumnType::Int4),
            ("int8", ColumnType::Int8),
            ("uuid", ColumnType::Uuid),
            ("date", ColumnType::Date),
            ("timestamp", ColumnType::Timestamp),
            ("timestamp without time zone", ColumnType::Timestamp),
            ("timestamptz", ColumnType::Timestamptz),
            ("bool", ColumnType::Bool),
            ("numeric", ColumnType::Numeric),
            ("bytea", ColumnType::Bytea),
            ("json", ColumnType::Json),
            ("jsonb", ColumnType::Jsonb),
            ("smallint", ColumnType::Int2),
            ("integer", ColumnType::Int4),
            ("bigint", ColumnType::Int8),
            ("boolean", ColumnType::Bool),
            ("timestamp with time zone", ColumnType::Timestamptz),
            ("timestamp(3) without time zone", ColumnType::Timestamp),
            ("timestamp(3) with time zone", ColumnType::Timestamptz),
            ("numeric(10,2)", ColumnType::Numeric),
        ] {
            assert_eq!(map_pg_type(n, 'b', None, -1), t);
        }
        for n in [
            "varchar",
            "character varying",
            "varchar(12)",
            "character varying(12)",
        ] {
            assert_eq!(map_pg_type(n, 'b', None, 16), ColumnType::Varchar(Some(12)));
        }
        for n in ["bpchar", "character", "bpchar(12)", "character(12)"] {
            assert_eq!(map_pg_type(n, 'b', None, 16), ColumnType::Bpchar(Some(12)));
        }
        assert_eq!(
            map_pg_type("mood", 'e', None, -1),
            ColumnType::Enum("mood".into())
        );
        assert_eq!(
            map_pg_type("_int4", 'b', Some("integer"), -1),
            ColumnType::Array(Box::new(ColumnType::Int4))
        );
        assert_eq!(
            map_pg_type("text", 'd', None, -1),
            ColumnType::Other("text".into())
        );
        assert_eq!(
            map_pg_type("unknown", 'b', None, -1),
            ColumnType::Other("unknown".into())
        );
    }
}
