use giggles_pg_catalog::*;

fn run(sql: &str, params: &[Value]) -> ResultSet {
    let p = plan(sql).unwrap().unwrap();
    let fields = describe(&p).unwrap();
    let r = execute(&p, &CatalogSnapshot::default(), params).unwrap();
    assert_eq!(fields, r.fields);
    r
}

#[test]
fn empty_description_xmin_descriptor_and_left_join_null() {
    let r = run(
        "SELECT objoid,classoid,objsubid,description,xmin FROM pg_description WHERE objoid<>2200",
        &[],
    );
    assert_eq!(
        r.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
        [
            PgType::Oid,
            PgType::Oid,
            PgType::Int4,
            PgType::Text,
            PgType::Xid
        ]
    );
    assert_eq!(
        (
            r.fields[4].data_type.oid(),
            r.fields[4].data_type.type_size()
        ),
        (28, 4)
    );
    assert!(r.rows.is_empty());
    let r = run(
        "SELECT d.xmin,d.xmin IS NULL FROM pg_type t LEFT JOIN pg_description d ON false WHERE t.oid=23",
        &[],
    );
    assert_eq!(r.rows, [vec![Value::Null, Value::Bool(true)]]);
    assert_eq!(r.fields[0].data_type, PgType::Xid);
    let r = run("SELECT * FROM pg_shdescription", &[]);
    assert_eq!(
        r.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        ["objoid", "classoid", "objsubid", "description"]
    );
    assert!(r.rows.is_empty());
    let r = run("SELECT xmin FROM pg_shdescription", &[]);
    assert_eq!(r.fields[0].data_type, PgType::Xid);
    assert!(r.rows.is_empty());
    assert!(
        run("SELECT age(xmin) FROM pg_description WHERE objoid<>2200", &[])
            .rows
            .is_empty()
    );
}

#[test]
fn exact_full_and_incremental_comments() {
    // Exact processor INC=false SHA256 07d0e1477573babde4b5c6d765731da18d07d0a23bbe5c3ce05f6265f5416218; sequential JDBC placeholders converted separately.
    let r = run(
        r###"select D.objoid id, C.relkind::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_class C on D.objoid = C.oid
where C.relnamespace = $1::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE
-- all table-like things + seqs + iets anders$2
union all
select T.oid id, 'T'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid
  left join pg_catalog.pg_class C on T.typrelid = C.oid
where T.typnamespace = $3::oid and (C.relkind = 'c' or C.relkind is null)
--  and pg_catalog.age(D.xmin) <= #TXAGE
-- relkind = c (composite types$4)
union all
select D.objoid id, pg_catalog.translate(C.contype, 'pufc', 'kkxz')::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_constraint C on D.objoid = C.oid
where C.connamespace = $5::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE
-- table constraints
union all
select D.objoid id, 't'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_trigger T on T.oid = D.objoid
  join pg_catalog.pg_class C on C.oid = T.tgrelid
where C.relnamespace = $6::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE
-- triggers
union all
select D.objoid id, 'R'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_rewrite R on R.oid = D.objoid
  join pg_catalog.pg_class C on C.oid = R.ev_class
where C.relnamespace = $7::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE
-- rules
union all
select D.objoid id, 'F'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_proc P on P.oid = D.objoid
where P.pronamespace = $8::oid and D.classoid = 'pg_catalog.pg_proc'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE
-- more routines
union all
select D.objoid id, 'O'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_operator O on O.oid = D.objoid
where O.oprnamespace = $9::oid and D.classoid = 'pg_catalog.pg_operator'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE
-- operators
union all
select D.objoid id, 'f'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_opfamily O on O.oid = D.objoid
where O.opfnamespace = $10::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE
-- op families
union all
select D.objoid id, 'c'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_opclass O on O.oid = D.objoid
where O.opcnamespace = $11::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE
-- op class
  union all
select D.objoid id, 'C'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_collation C on C.oid = D.objoid
where C.collnamespace = $12::oid and D.classoid = 'pg_catalog.pg_collation'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE

-- collations
  union all
select D.objoid id, 'P'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
       join pg_catalog.pg_policy P on P.oid = D.objoid
       join pg_catalog.pg_class C on P.polrelid = C.oid
where C.relnamespace = $13::oid and D.classoid = 'pg_catalog.pg_policy'::regclass
--  and pg_catalog.age(D.xmin) <= #TXAGE

-- security policies (also by table name...)"###,
        &vec![Value::Int(2200); 13],
    );
    assert_eq!(
        r.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        ["id", "kind", "sub_id", "description"]
    );
    assert_eq!(
        r.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
        [PgType::Oid, PgType::Char, PgType::Int4, PgType::Text]
    );
    assert!(r.rows.is_empty());
    // Original captured pinned JDBC INC=true SHA256 b696588af93cf44bafde2524bb36839c2ad90f7e17d31f0f9473e76ce275854c.
    let p = plan(
            r###"select D.objoid id, C.relkind::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_class C on D.objoid = C.oid
where C.relnamespace = $1::oid and C.relkind != 'c' and D.classoid = 'pg_catalog.pg_class'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
-- all table-like things + seqs + iets anders?
union all
select T.oid id, 'T'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_type T on T.oid = D.objoid or T.typrelid = D.objoid
  left join pg_catalog.pg_class C on T.typrelid = C.oid
where T.typnamespace = $3::oid and (C.relkind = 'c' or C.relkind is null)
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($4::varchar::xid), -1), -1), 2147483647)
-- relkind = c (composite types?)
union all
select D.objoid id, pg_catalog.translate(C.contype, 'pufc', 'kkxz')::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_constraint C on D.objoid = C.oid
where C.connamespace = $5::oid and D.classoid = 'pg_catalog.pg_constraint'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($6::varchar::xid), -1), -1), 2147483647)
-- table constraints
union all
select D.objoid id, 't'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_trigger T on T.oid = D.objoid
  join pg_catalog.pg_class C on C.oid = T.tgrelid
where C.relnamespace = $7::oid and D.classoid = 'pg_catalog.pg_trigger'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($8::varchar::xid), -1), -1), 2147483647)
-- triggers
union all
select D.objoid id, 'R'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_rewrite R on R.oid = D.objoid
  join pg_catalog.pg_class C on C.oid = R.ev_class
where C.relnamespace = $9::oid and D.classoid = 'pg_catalog.pg_rewrite'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($10::varchar::xid), -1), -1), 2147483647)
-- rules
union all
select D.objoid id, 'F'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_proc P on P.oid = D.objoid
where P.pronamespace = $11::oid and D.classoid = 'pg_catalog.pg_proc'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($12::varchar::xid), -1), -1), 2147483647)
-- more routines
union all
select D.objoid id, 'O'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_operator O on O.oid = D.objoid
where O.oprnamespace = $13::oid and D.classoid = 'pg_catalog.pg_operator'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($14::varchar::xid), -1), -1), 2147483647)
-- operators
union all
select D.objoid id, 'f'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_opfamily O on O.oid = D.objoid
where O.opfnamespace = $15::oid and D.classoid = 'pg_catalog.pg_opfamily'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($16::varchar::xid), -1), -1), 2147483647)
-- op families
union all
select D.objoid id, 'c'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_opclass O on O.oid = D.objoid
where O.opcnamespace = $17::oid and D.classoid = 'pg_catalog.pg_opclass'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($18::varchar::xid), -1), -1), 2147483647)
-- op class
  union all
select D.objoid id, 'C'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
  join pg_catalog.pg_collation C on C.oid = D.objoid
where C.collnamespace = $19::oid and D.classoid = 'pg_catalog.pg_collation'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($20::varchar::xid), -1), -1), 2147483647)

-- collations
  union all
select D.objoid id, 'P'::char as kind, D.objsubid sub_id, D.description
from pg_catalog.pg_description D
       join pg_catalog.pg_policy P on P.oid = D.objoid
       join pg_catalog.pg_class C on P.polrelid = C.oid
where C.relnamespace = $21::oid and D.classoid = 'pg_catalog.pg_policy'::regclass
and pg_catalog.age(D.xmin) <= coalesce(nullif(greatest(pg_catalog.age($22::varchar::xid), -1), -1), 2147483647)

-- security policies (also by table name...)"###
        ).unwrap().unwrap();
    let params = p
        .parameter_types()
        .iter()
        .map(|ty| match ty {
            PgType::Oid => Value::Int(2200),
            _ => Value::Text("0".into()),
        })
        .collect::<Vec<_>>();
    assert!(
        execute(&p, &CatalogSnapshot::default(), &params)
            .unwrap()
            .rows
            .is_empty()
    );
}
