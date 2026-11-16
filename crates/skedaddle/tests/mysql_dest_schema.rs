//! Uses hand-built descriptors identical to harness/mysql-init.sql's zoo.
use serde_json::Value;
use skedaddle::{
    mysql::{
        dest_schema::{self, MismatchKind},
        schema::{Collation, ColumnDescriptor, TableDescriptor},
    },
    pg,
    security::TlsPolicy,
};
fn column(name: &str, ordinal: u64, ty: &str) -> ColumnDescriptor {
    let data_type = ty.split(['(', ' ']).next().unwrap();
    let text = matches!(
        data_type,
        "char" | "varchar" | "text" | "mediumtext" | "enum" | "set"
    );
    ColumnDescriptor {
        name: name.into(),
        ordinal,
        data_type: data_type.into(),
        column_type: ty.into(),
        character_set: text.then(|| "utf8mb4".into()),
        collation: text.then(|| Collation {
            name: "utf8mb4_0900_ai_ci".into(),
            id: 255,
        }),
        nullable: name != "id",
        generated: false,
        column_default: None,
        default_generated: false,
        generation_expression: None,
    }
}
fn zoo(schema: &str) -> TableDescriptor {
    let columns = [
        ("id", "bigint"),
        ("i8", "tinyint"),
        ("u8", "tinyint unsigned"),
        ("i16", "smallint"),
        ("u16", "smallint unsigned"),
        ("i24", "mediumint"),
        ("u24", "mediumint unsigned"),
        ("i32", "int"),
        ("u32", "int unsigned"),
        ("i64", "bigint"),
        ("u64", "bigint unsigned"),
        ("decv", "decimal(65,30)"),
        ("fv", "float"),
        ("dv", "double"),
        ("bits", "bit(17)"),
        ("d", "date"),
        ("dt", "datetime(6)"),
        ("ts", "timestamp(6)"),
        ("tm", "time(6)"),
        ("yr", "year"),
        ("cv", "char(8)"),
        ("vv", "varchar(128)"),
        ("txt", "text"),
        ("bv", "binary(8)"),
        ("blobv", "blob"),
        ("ev", "enum('zero','one','two')"),
        ("sv", "set('a','b','c')"),
        ("jv", "json"),
        ("nullable_v", "varchar(8)"),
        ("wide_row", "mediumtext"),
    ];
    TableDescriptor {
        schema: schema.into(),
        table: "zoo".into(),
        primary_key: vec!["id".into()],
        columns: columns
            .iter()
            .enumerate()
            .map(|(i, (n, t))| column(n, i as u64 + 1, t))
            .collect(),
    }
}
#[tokio::test]
async fn pg18_install_retry_and_catalog_drift()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: mysql_dest_schema requires SKEDADDLE_TEST_CONFIG; PG18 destination schema qualification NOT RUN"
        );
        return Ok(());
    };
    let cfg: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let pg_version = cfg["pg_version"]
        .as_i64()
        .ok_or("pg_version missing or not an integer")?;
    if pg_version != 18 {
        eprintln!(
            "MYSQL SKIPPED: mysql_dest_schema qualifies PG18 only (pg_version={pg_version}); PG18 destination schema qualification NOT RUN"
        );
        return Ok(());
    }
    let (mut client, driver, evidence) = pg::tls::connect(
        &pg::tls::Config::new(
            cfg["pg_apply"]
                .as_str()
                .ok_or("pg_apply missing")?
                .parse()?,
            cfg["ca_pem"].as_str().ok_or("CA missing")?,
        ),
        &TlsPolicy::Required,
    )
    .await?;
    assert!(evidence.encrypted());
    tokio::spawn(driver);
    let schema = format!("d5_{}", uuid::Uuid::new_v4().simple());
    let table = zoo(&schema);
    let mapped = dest_schema::build(&[table]).map_err(|e| format!("{e:?}"))?;
    // A session advisory lock prevents overlap with another D5 test's
    // deliberate mutation of the shared, fixed-name collation requirement.
    client
        .query_one("SELECT pg_advisory_lock(640905)", &[])
        .await?;
    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
        dest_schema::install(&mut client,&mapped).await?;
        let oid:u32=client.query_one("SELECT oid FROM pg_class WHERE oid=to_regclass($1)",&[&format!("{schema}.zoo")]).await?.get(0);
        dest_schema::install(&mut client,&mapped).await?;
        assert_eq!(oid,client.query_one("SELECT oid FROM pg_class WHERE oid=to_regclass($1)",&[&format!("{schema}.zoo")]).await?.get::<_,u32>(0));
        assert!(dest_schema::verify_destination(&mut client,&mapped).await?.is_empty());
        client.batch_execute(&format!("ALTER TABLE {schema}.zoo ALTER COLUMN vv TYPE varchar(127)")).await?;
        assert!(dest_schema::verify_destination(&mut client,&mapped).await?.iter().any(|m|m.kind==MismatchKind::Column));
        assert!(dest_schema::install(&mut client,&mapped).await.is_err());
        let mut extra=zoo(&schema); extra.table="ahead".into();
        let rollback_plan=dest_schema::build(&[zoo(&schema),extra])?;
        assert!(dest_schema::install(&mut client,&rollback_plan).await.is_err());
        assert!(client.query_one("SELECT to_regclass($1)::oid",&[&format!("{schema}.ahead")]).await?.get::<_,Option<u32>>(0).is_none());

        client.batch_execute(&format!("ALTER TABLE {schema}.zoo ALTER COLUMN vv TYPE varchar(128); ALTER TABLE {schema}.zoo DROP CONSTRAINT zoo_ev_check")).await?;
        assert!(dest_schema::verify_destination(&mut client,&mapped).await?.iter().any(|m|m.kind==MismatchKind::Check));
        client.batch_execute(&format!("ALTER TABLE {schema}.zoo ADD CHECK(ev IN ('zero','one','two')); ALTER TABLE {schema}.zoo ADD COLUMN extra int NOT NULL")).await?;
        assert!(dest_schema::verify_destination(&mut client,&mapped).await?.iter().any(|m|m.kind==MismatchKind::ExtraRequiredColumn));
        client.batch_execute(&format!("ALTER TABLE {schema}.zoo DROP COLUMN extra; ALTER TABLE {schema}.zoo ALTER COLUMN vv TYPE varchar(128) COLLATE pg_catalog.\"C\"")).await?;
        assert!(dest_schema::verify_destination(&mut client,&mapped).await?.iter().any(|m|m.kind==MismatchKind::Column));
        client.batch_execute(&format!("ALTER TABLE {schema}.zoo ALTER COLUMN vv TYPE varchar(128) COLLATE public.skedaddle_und_ai_ci; CREATE UNIQUE INDEX zoo_ci ON {schema}.zoo(vv); INSERT INTO {schema}.zoo(id,vv) VALUES (1,'a')")).await?;
        let error=client.execute(&format!("INSERT INTO {schema}.zoo(id,vv) VALUES ($1,$2)"),&[&2i64,&"A"]).await.unwrap_err();
        assert_eq!(error.code(),Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION));
        for options in ["provider=libc,locale='C',deterministic=true", "provider=icu,locale='und-u-ks-level2',deterministic=false", "provider=icu,locale='und-u-ks-level1',deterministic=true"] {
            let tx=client.transaction().await?;
            tx.batch_execute("ALTER COLLATION public.skedaddle_und_ai_ci RENAME TO d5_original").await?;
            tx.batch_execute(&format!("CREATE COLLATION public.skedaddle_und_ai_ci ({options})")).await?;
            assert!(dest_schema::verify_destination_in(&tx,&mapped).await?.iter().any(|m|m.kind==MismatchKind::Collation));
            tx.rollback().await?;
        }
        client.batch_execute(&format!("ALTER TABLE {schema}.zoo DROP CONSTRAINT zoo_pkey")).await?;
        assert!(dest_schema::verify_destination(&mut client,&mapped).await?.iter().any(|m|m.kind==MismatchKind::PrimaryKey));
        client.batch_execute(&format!("ALTER TABLE {schema}.zoo ADD PRIMARY KEY(id)")).await?;
        assert!(dest_schema::verify_destination(&mut client,&mapped).await?.is_empty());
        let mut literal_table=TableDescriptor {schema:schema.clone(),table:"defaults".into(),columns:vec![column("id",1,"int"),column("copy",2,"int"),column("label",3,"varchar(128)")],primary_key:vec!["id".into()]};
        literal_table.columns[0].column_default=Some("42".into());literal_table.columns[1].generated=true;literal_table.columns[1].generation_expression=Some("`id`".into());literal_table.columns[2].column_default=Some("a'\\b".into());
        let defaults=dest_schema::build(&[literal_table]).map_err(|e|format!("{e:?}"))?;
        dest_schema::install(&mut client,&defaults).await?;
        let row=client.query_one(&format!("INSERT INTO {schema}.defaults DEFAULT VALUES RETURNING id,copy,label"),&[]).await?;
        assert_eq!(row.get::<_,i32>(0),42);assert_eq!(row.get::<_,i32>(1),42);assert_eq!(row.get::<_,String>(2),"a'\\b");
        assert!(dest_schema::verify_destination(&mut client,&defaults).await?.is_empty());
        Ok(())
    }.await;
    let cleanup = client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    client
        .query_one("SELECT pg_advisory_unlock(640905)", &[])
        .await?;
    cleanup?;
    result
}
