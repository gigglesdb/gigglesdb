use super::*;
use crate::relational::SqlError;
use std::collections::VecDeque;

#[derive(Default)]
struct Mock {
    calls: Vec<(String, Vec<SpiParam>)>,
    responses: VecDeque<Result<SpiResult, SpiError>>,
    notices: Vec<PlNotice>,
}

impl Spi for Mock {
    fn execute(&mut self, sql: &str, params: &[SpiParam]) -> Result<SpiResult, SpiError> {
        self.calls.push((sql.into(), params.to_vec()));
        self.responses
            .pop_front()
            .unwrap_or_else(|| panic!("unexpected SQL: {sql}"))
    }

    fn notices(&mut self, notices: Vec<PlNotice>) {
        self.notices.extend(notices);
    }
}

fn signature(name: &str) -> FunctionSignature {
    FunctionSignature {
        name: name.into(),
        kind: FunctionKind::Trigger,
        arguments: vec![],
    }
}

fn column(name: &str, data_type: DataType) -> SpiColumn {
    SpiColumn {
        name: name.into(),
        data_type,
    }
}

fn param(data_type: DataType, value: Value) -> SpiParam {
    SpiParam {
        data_type,
        value,
        type_oid: None,
    }
}

fn trigger(op: TriggerOp) -> TriggerData {
    let old = vec![Value::Int(3), Value::Int(7), Value::Text("old".into())];
    let new = vec![Value::Int(3), Value::Int(8), Value::Text("new".into())];
    TriggerData {
        name: "cleanup".into(),
        when: TriggerWhen::Before,
        level: TriggerLevel::Row,
        op,
        relation_name: "t".into(),
        schema_name: "public".into(),
        relation_oid: 40042,
        arguments: vec!["design-doc".into(), "42".into()],
        columns: vec![
            column("id", DataType::Integer),
            column("v", DataType::Integer),
            column("Col", DataType::Text),
        ],
        old: (op == TriggerOp::Delete || op == TriggerOp::Update).then_some(old),
        new: (op == TriggerOp::Insert || op == TriggerOp::Update).then_some(new),
    }
}

fn rows(columns: Vec<SpiColumn>, rows: Vec<Vec<Value>>) -> SpiResult {
    SpiResult {
        row_count: rows.len() as u64,
        columns,
        rows,
    }
}

fn one(ty: DataType, value: Value) -> SpiResult {
    rows(vec![column("?column?", ty)], vec![vec![value]])
}

fn int(value: i64) -> SpiResult {
    one(DataType::Integer, Value::Int(value))
}

fn boolean(value: bool) -> SpiResult {
    one(DataType::Boolean, Value::Bool(value))
}

fn text(value: &str) -> SpiResult {
    one(DataType::Text, Value::Text(value.into()))
}

fn affected(row_count: u64) -> SpiResult {
    SpiResult {
        row_count,
        ..SpiResult::default()
    }
}

fn failure(error: SqlError, message: &str) -> SpiError {
    SpiError {
        error: CoreError::Sql(error),
        message: message.into(),
        detail: None,
        hint: None,
    }
}

fn mock(responses: impl IntoIterator<Item = SpiResult>) -> Mock {
    Mock {
        responses: responses.into_iter().map(Ok).collect(),
        ..Mock::default()
    }
}

fn execute(
    name: &str,
    body: &str,
    ctx: &TriggerData,
    spi: &mut Mock,
) -> Result<TriggerReturn, PlRuntimeError> {
    let compiled = compile(body, &signature(name)).unwrap();
    let result = exec_trigger(&compiled, ctx, spi);
    assert!(
        spi.responses.is_empty(),
        "unused SPI responses: {:?}",
        spi.responses
    );
    result
}

#[test]
fn archdocs_body_rewrites_and_runs_for_delete_and_update() {
    const BODY: &str = r#"BEGIN
  DELETE FROM "SearchVector" WHERE
    ("entityType" = TG_ARGV[0] AND "entityId" = OLD.id)
    OR (TG_ARGV[0] = 'design-doc' AND (
      ("entityType" = 'supporting-doc' AND "entityId" IN
        (SELECT id FROM "DesignDocSupportingDoc" WHERE "designDocId" = OLD.id))
      OR ("entityType" = 'knowledge-item' AND "entityId" IN
        (SELECT id FROM "DesignDocKnowledgeItem" WHERE "designDocId" = OLD.id))
    ));
  IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;"#;
    const DELETE: &str = r#"DELETE FROM "SearchVector" WHERE
    ("entityType" = $1 AND "entityId" = $2)
    OR ($1 = 'design-doc' AND (
      ("entityType" = 'supporting-doc' AND "entityId" IN
        (SELECT id FROM "DesignDocSupportingDoc" WHERE "designDocId" = $2))
      OR ("entityType" = 'knowledge-item' AND "entityId" IN
        (SELECT id FROM "DesignDocKnowledgeItem" WHERE "designDocId" = $2))
    ))"#;
    let compiled = compile(BODY, &signature("cleanup_semantic_vectors")).unwrap();
    for op in [TriggerOp::Delete, TriggerOp::Update] {
        let ctx = trigger(op);
        let delete = op == TriggerOp::Delete;
        let mut spi = mock([affected(2), boolean(delete)]);
        let result = exec_trigger(&compiled, &ctx, &mut spi).unwrap();
        assert_eq!(
            result,
            TriggerReturn::Row(if delete {
                ctx.old.unwrap()
            } else {
                ctx.new.unwrap()
            })
        );
        assert!(spi.responses.is_empty());
        assert_eq!(
            spi.calls,
            [
                (
                    DELETE.into(),
                    vec![
                        param(DataType::Text, Value::Text("design-doc".into())),
                        param(DataType::Integer, Value::Int(3))
                    ]
                ),
                (
                    "SELECT $1 = 'DELETE'".into(),
                    vec![param(
                        DataType::Text,
                        Value::Text(if delete { "DELETE" } else { "UPDATE" }.into())
                    )]
                ),
            ]
        );
    }
}

#[test]
fn oracle_compile_errors_carry_body_byte_offsets() {
    for (name, body, token, message) in [
        (
            "bad1",
            " BEGIN RETURN NEW END ",
            "",
            "syntax error at end of input",
        ),
        (
            "bad2",
            " BEGIN IF TG_OP = 'X' THEN RETURN NEW; END ",
            "",
            "syntax error at end of input",
        ),
        (
            "bad3",
            " BEGIN /* café */ DELETE FRM t; RETURN NEW; END ",
            "FRM",
            "syntax error at or near \"FRM\"",
        ),
        (
            "bad4",
            " RETURN NEW; ",
            "RETURN",
            "syntax error at or near \"RETURN\"",
        ),
    ] {
        let error = compile(body, &signature(name)).unwrap_err();
        assert_eq!((error.sqlstate, error.message.as_str()), ("42601", message));
        assert_eq!(
            error.position,
            Some(if token.is_empty() {
                body.len()
            } else {
                body.find(token).unwrap()
            })
        );
    }
    let mut signature = signature("bad6");
    signature.arguments.push(FunctionArgument {
        name: Some("a".into()),
        data_type: DataType::Integer,
    });
    let error = compile("BEGIN RETURN NEW; END", &signature).unwrap_err();
    assert_eq!(error.sqlstate, "42P13");
    assert_eq!(
        error.message,
        "trigger functions cannot have declared arguments"
    );
    assert_eq!(error.position, None);
    assert_eq!(
        error.hint.as_deref(),
        Some("The arguments of the trigger can be accessed through TG_NARGS and TG_ARGV instead.")
    );
}

#[test]
fn oracle_raise_and_fallthrough_contexts() {
    let ctx = trigger(TriggerOp::Insert);
    let mut spi = mock([affected(1), boolean(true), int(3)]);
    let error = execute(
        "boom",
        "\nBEGIN\n  INSERT INTO log(msg) VALUES ('before boom ' || NEW.id);\n  IF NEW.id = 3 THEN RAISE EXCEPTION 'boom on %', NEW.id; END IF;\n  RETURN NEW;\nEND ",
        &ctx,
        &mut spi,
    ).unwrap_err();
    assert_eq!(
        (error.sqlstate.as_ref(), error.message.as_ref()),
        ("P0001", "boom on 3")
    );
    assert_eq!(error.context, ["PL/pgSQL function boom() line 4 at RAISE"]);

    let error = execute("noreturn", " BEGIN NULL; END ", &ctx, &mut Mock::default()).unwrap_err();
    assert_eq!(error.sqlstate.as_ref(), "2F005");
    assert_eq!(
        error.message.as_ref(),
        "control reached end of trigger procedure without RETURN"
    );
    assert_eq!(error.context, ["PL/pgSQL function noreturn()"]);
}

#[test]
fn oracle_assignment_expression_context_and_original_error() {
    let ctx = trigger(TriggerOp::Insert);
    let mut spi = Mock::default();
    spi.responses
        .push_back(Err(failure(SqlError::DivisionByZero, "division by zero")));
    let error = execute(
        "divz",
        " BEGIN NEW.v := 1 / 0; RETURN NEW; END ",
        &ctx,
        &mut spi,
    )
    .unwrap_err();
    assert_eq!(
        (error.sqlstate.as_ref(), error.message.as_ref()),
        ("22012", "division by zero")
    );
    assert_eq!(error.query, None);
    assert_eq!(
        error.context,
        [
            "PL/pgSQL assignment \"NEW.v := 1 / 0\"",
            "PL/pgSQL function divz() line 1 at assignment"
        ]
    );
    assert_eq!(
        error.spi_error.unwrap().error,
        CoreError::Sql(SqlError::DivisionByZero)
    );

    let error = execute(
        "badcol",
        " BEGIN NEW.nope := 1; RETURN NEW; END ",
        &ctx,
        &mut Mock::default(),
    )
    .unwrap_err();
    assert_eq!(
        (error.sqlstate.as_ref(), error.message.as_ref()),
        ("42703", "record \"new\" has no field \"nope\"")
    );
    assert_eq!(
        error.context,
        [
            "PL/pgSQL assignment \"NEW.nope := 1\"",
            "PL/pgSQL function badcol() line 1 at assignment"
        ]
    );
}

#[test]
fn oracle_sql_and_coercion_errors_carry_original_query() {
    let ctx = trigger(TriggerOp::Insert);
    let mut spi = Mock::default();
    let mut no_table = failure(
        SqlError::UnknownTable,
        "relation \"no_such_table\" does not exist",
    );
    no_table.detail = Some("SPI detail".into());
    no_table.hint = Some("SPI hint".into());
    spi.responses.push_back(Err(no_table));
    let error = execute(
        "badsql",
        " BEGIN DELETE FROM no_such_table WHERE id = NEW.id; RETURN NEW; END ",
        &ctx,
        &mut spi,
    )
    .unwrap_err();
    assert_eq!(error.sqlstate.as_ref(), "42P01");
    assert_eq!(
        error.message.as_ref(),
        "relation \"no_such_table\" does not exist"
    );
    assert_eq!(
        error.query.as_deref(),
        Some("DELETE FROM no_such_table WHERE id = NEW.id")
    );
    assert_eq!(error.detail.as_deref(), Some("SPI detail"));
    assert_eq!(error.hint.as_deref(), Some("SPI hint"));
    assert_eq!(
        error.context,
        ["PL/pgSQL function badsql() line 1 at SQL statement"]
    );

    let mut spi = mock([text("abc")]);
    let mut fail = failure(
        SqlError::InvalidTextRepresentation,
        "invalid input syntax for type integer: \"abc\"",
    );
    fail.hint = Some("cast hint".into());
    spi.responses.push_back(Err(fail));
    let error = execute(
        "coerce2",
        " BEGIN NEW.v := 'abc'; RETURN NEW; END ",
        &ctx,
        &mut spi,
    )
    .unwrap_err();
    assert_eq!(
        (error.sqlstate.as_ref(), error.message.as_ref()),
        ("22P02", "invalid input syntax for type integer: \"abc\"")
    );
    assert_eq!(error.query.as_deref(), Some("NEW.v := 'abc'"));
    assert_eq!(
        error.context,
        ["PL/pgSQL function coerce2() line 1 at assignment"]
    );
    assert_eq!(error.spi_error.unwrap().hint.as_deref(), Some("cast hint"));
}

#[test]
fn assigns_and_coerces_new_fields() {
    let numeric = crate::relational::Numeric::parse("12.5").unwrap();
    let mut spi = mock([
        text("41"),
        int(41),
        one(DataType::Numeric, Value::Numeric(numeric.clone())),
        text("12.5"),
    ]);
    let output = execute(
        "coerce",
        r#"BEGIN NEW.v := '41'; NEW."Col" = 12.5; RETURN NEW; END;"#,
        &trigger(TriggerOp::Insert),
        &mut spi,
    )
    .unwrap();
    assert_eq!(
        output,
        TriggerReturn::Row(vec![
            Value::Int(3),
            Value::Int(41),
            Value::Text("12.5".into())
        ])
    );
    assert_eq!(
        spi.calls,
        [
            ("SELECT '41'".into(), vec![]),
            (
                "SELECT CAST($1 AS integer)".into(),
                vec![param(DataType::Text, Value::Text("41".into()))]
            ),
            ("SELECT 12.5".into(), vec![]),
            (
                "SELECT CAST($1 AS text)".into(),
                vec![param(DataType::Numeric, Value::Numeric(numeric))]
            ),
        ]
    );
}

#[test]
fn trigger_variables_carry_typed_parameters() {
    let mut ctx = trigger(TriggerOp::Truncate);
    ctx.when = TriggerWhen::InsteadOf;
    ctx.level = TriggerLevel::Statement;
    let mut spi = mock([affected(1)]);
    let result = execute("vars", "BEGIN INSERT INTO log VALUES (TG_NAME, TG_WHEN, TG_LEVEL, TG_OP, TG_RELID, TG_RELNAME, TG_TABLE_NAME, TG_TABLE_SCHEMA, TG_NARGS, TG_ARGV[0], TG_ARGV[8]); RETURN; END", &ctx, &mut spi).unwrap();
    assert_eq!(result, TriggerReturn::Null);
    let s = |value: &str| param(DataType::Text, Value::Text(value.into()));
    let name = |value: &str| SpiParam {
        type_oid: Some(19),
        ..s(value)
    };
    assert_eq!(
        spi.calls,
        [(
            "INSERT INTO log VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)".into(),
            vec![
                name("cleanup"),
                s("INSTEAD OF"),
                s("STATEMENT"),
                s("TRUNCATE"),
                SpiParam {
                    type_oid: Some(26),
                    ..param(DataType::BigInt, Value::Int(40042))
                },
                name("t"),
                name("t"),
                name("public"),
                param(DataType::Integer, Value::Int(2)),
                s("design-doc"),
                param(DataType::Text, Value::Null),
            ],
        )]
    );
}

#[test]
fn absent_and_partial_records_follow_sql_null_tests() {
    let mut ctx = trigger(TriggerOp::Insert);
    ctx.new.as_mut().unwrap()[1] = Value::Null;
    ctx.when = TriggerWhen::After;
    let mut spi = mock([affected(1)]);
    assert_eq!(execute(
        "records",
        "BEGIN INSERT INTO log VALUES (NEW IS NULL, NEW IS NOT NULL, OLD IS NULL, OLD IS NOT NULL, OLD.v, TG_WHEN); RETURN NULL; END",
        &ctx, &mut spi,
    ).unwrap(), TriggerReturn::Null);
    assert_eq!(
        spi.calls[0],
        (
            "INSERT INTO log VALUES ($1, $2, $3, $4, $5, $6)".into(),
            vec![
                param(DataType::Boolean, Value::Bool(false)),
                param(DataType::Boolean, Value::Bool(false)),
                param(DataType::Boolean, Value::Bool(true)),
                param(DataType::Boolean, Value::Bool(false)),
                param(DataType::Integer, Value::Null),
                param(DataType::Text, Value::Text("AFTER".into())),
            ],
        )
    );
    assert_eq!(
        execute(
            "null_old",
            "BEGIN RETURN OLD; END",
            &ctx,
            &mut Mock::default()
        )
        .unwrap(),
        TriggerReturn::Null
    );
}

#[test]
fn tokenizer_skips_sql_literals_comments_and_unbound_identifiers() {
    let mut spi = mock([int(1), affected(1)]);
    let body = r#"DECLARE v int := 1;
BEGIN
  INSERT INTO log(v) SELECT v, 'v NEW.id', E'v \\' || 'NEW.id',
    E'escaped \' v', $body$v NEW.id ; /* */$body$, "V", x.v, NEW."Col"
    /* v /* NEW.id */ v */ FROM source AS x -- NEW.id
    WHERE x.v = v;
  RETURN NEW;
END"#;
    execute("tokens", body, &trigger(TriggerOp::Insert), &mut spi).unwrap();
    let expected = r#"INSERT INTO log(v) SELECT $1, 'v NEW.id', E'v \\' || 'NEW.id',
    E'escaped \' v', $body$v NEW.id ; /* */$body$, "V", x.v, $2
    /* v /* NEW.id */ v */ FROM source AS x -- NEW.id
    WHERE x.v = $1"#;
    assert_eq!(
        spi.calls[1],
        (
            expected.into(),
            vec![
                param(DataType::Integer, Value::Int(1)),
                param(DataType::Text, Value::Text("new".into())),
            ]
        )
    );
}

#[test]
fn declaration_shadowing_elsif_case_and_dynamic_trigger_index() {
    let mut spi = mock([
        int(1),
        int(0),
        boolean(false),
        boolean(true),
        int(0),
        boolean(true),
        int(1),
        affected(1),
    ]);
    let ctx = trigger(TriggerOp::Insert);
    let result = execute(
        "branches",
        r#"
<<top>>
DECLARE "I" CONSTANT int NOT NULL DEFAULT 1;
BEGIN
  DECLARE "I" integer = 0;
  BEGIN
    IF false THEN RETURN NULL;
    ELSIF "I" = 0 THEN
      CASE "I" WHEN 0, 1 THEN NULL; ELSE RETURN NULL; END CASE;
    ELSE RETURN NULL;
    END IF;
  END;
  INSERT INTO log VALUES (TG_ARGV["I"], "I");
  RETURN NEW;
END top;"#,
        &ctx,
        &mut spi,
    )
    .unwrap();
    assert_eq!(result, TriggerReturn::Row(ctx.new.unwrap()));
    assert_eq!(
        spi.calls[3],
        (
            "SELECT $1 = 0".into(),
            vec![param(DataType::Integer, Value::Int(0))]
        )
    );
    assert_eq!(
        spi.calls[5],
        (
            "SELECT ($1 = (0))".into(),
            vec![param(DataType::Integer, Value::Int(0))]
        )
    );
    assert_eq!(
        spi.calls[6],
        (
            "SELECT $1".into(),
            vec![param(DataType::Integer, Value::Int(1))]
        )
    );
    assert_eq!(
        spi.calls[7],
        (
            "INSERT INTO log VALUES ($1, $2)".into(),
            vec![
                param(DataType::Text, Value::Text("42".into())),
                param(DataType::Integer, Value::Int(1)),
            ]
        )
    );
}

#[test]
fn searched_case_no_match_uses_oracle_sqlstate_and_hint() {
    let mut spi = mock([boolean(false)]);
    let error = execute(
        "missing_case",
        "BEGIN CASE WHEN false THEN NULL; END CASE; RETURN NULL; END",
        &trigger(TriggerOp::Delete),
        &mut spi,
    )
    .unwrap_err();
    assert_eq!(
        (error.sqlstate.as_ref(), error.message.as_ref()),
        ("20000", "case not found")
    );
    assert_eq!(
        error.hint.as_deref(),
        Some("CASE statement is missing ELSE part.")
    );
    assert_eq!(
        error.context,
        ["PL/pgSQL function missing_case() line 1 at CASE"]
    );
}

#[test]
fn labeled_blocks_and_loop_control() {
    let mut spi = mock([
        int(4),
        int(1),
        int(2),
        boolean(true),  // CONTINUE at i = 4
        boolean(false), // CONTINUE at i = 2
        boolean(true),  // EXIT at i = 2
        boolean(true),  // WHILE FOUND
        boolean(true),  // EXIT WHEN true
        boolean(false), // CASE WHEN false
        boolean(true),  // EXIT skip WHEN true
    ]);
    let result = execute(
        "loops",
        r#"
BEGIN
  <<outer_loop>>
  FOR i IN REVERSE 4..1 BY 2 LOOP
    <<inner_loop>> LOOP
      CONTINUE outer_loop WHEN i = 4;
      EXIT outer_loop WHEN i = 2;
    END LOOP inner_loop;
  END LOOP outer_loop;
  WHILE FOUND LOOP
    EXIT WHEN true;
  END LOOP;
  <<skip>>
  BEGIN
    CASE WHEN false THEN RETURN NULL; ELSE NULL; END CASE;
    EXIT skip WHEN true;
    RETURN NULL;
  END skip;
  RETURN NEW;
END"#,
        &trigger(TriggerOp::Insert),
        &mut spi,
    )
    .unwrap();
    assert!(matches!(result, TriggerReturn::Row(_)));
    assert_eq!(spi.calls[3].1[0].value, Value::Int(4));
    assert_eq!(spi.calls[4].1[0].value, Value::Int(2));
    assert_eq!(
        spi.calls[6],
        (
            "SELECT $1".into(),
            vec![param(DataType::Boolean, Value::Bool(true))]
        )
    );
}

#[test]
fn found_and_diagnostics_are_updated_only_by_relevant_statements() {
    let mut spi = mock([
        affected(7),
        boolean(false),
        rows(vec![column("id", DataType::Integer)], vec![]),
        boolean(true),
        int(6),
        boolean(false),
        affected(0),
        boolean(false),
        affected(1),
    ]);
    let ctx = trigger(TriggerOp::Insert);
    let output = execute(
        "diagnostics",
        r#"
DECLARE count bigint; k int; r record;
BEGIN
  UPDATE items SET v = NEW.v WHERE id = NEW.id;
  GET DIAGNOSTICS count = ROW_COUNT;
  IF NOT FOUND OR count <> 7 THEN RETURN NULL; END IF;
  SELECT id INTO k FROM items WHERE false;
  IF FOUND OR k IS NULL THEN NULL; END IF;
  WITH q AS (SELECT 6 AS v) SELECT v INTO STRICT k FROM q;
  IF NOT FOUND OR k <> 6 THEN RETURN NULL; END IF;
  PERFORM 1 FROM items WHERE false;
  IF FOUND THEN RETURN NULL; END IF;
  DELETE FROM items WHERE id = k;
  RETURN NEW;
END"#,
        &ctx,
        &mut spi,
    )
    .unwrap();
    assert_eq!(output, TriggerReturn::Row(ctx.new.unwrap()));
    assert_eq!(
        spi.calls[1].1,
        vec![
            param(DataType::Boolean, Value::Bool(true)),
            param(DataType::BigInt, Value::Int(7))
        ]
    );
    assert_eq!(spi.calls[2].0, "SELECT id  FROM items WHERE false");
    assert_eq!(
        spi.calls[3].1,
        vec![
            param(DataType::Boolean, Value::Bool(false)),
            param(DataType::Integer, Value::Null)
        ]
    );
    assert_eq!(spi.calls[4].0, "WITH q AS (SELECT 6 AS v) SELECT v  FROM q");
    assert_eq!(
        spi.calls[8].1,
        vec![param(DataType::Integer, Value::Int(6))]
    );
}

#[test]
fn select_into_record_and_into_list() {
    let ctx = trigger(TriggerOp::Update);
    let mut spi = mock([
        rows(
            vec![column("a", DataType::Integer), column("b", DataType::Text)],
            vec![vec![Value::Int(1), Value::Text("b".into())]],
        ),
        rows(ctx.columns.clone(), vec![ctx.old.clone().unwrap()]),
        affected(1),
    ]);
    let result = execute(
        "record_target",
        r#"
DECLARE r record; a int; b text;
BEGIN
  SELECT 1, 'b' INTO a, b;
  SELECT t.id, t.v, t."Col" INTO r FROM t;
  INSERT INTO log VALUES (r."Col", a, b);
  RETURN r;
END"#,
        &ctx,
        &mut spi,
    )
    .unwrap();
    assert_eq!(result, TriggerReturn::Row(ctx.old.unwrap()));
    assert_eq!(
        spi.calls[2],
        (
            "INSERT INTO log VALUES ($1, $2, $3)".into(),
            vec![
                param(DataType::Text, Value::Text("old".into())),
                param(DataType::Integer, Value::Int(1)),
                param(DataType::Text, Value::Text("b".into())),
            ]
        )
    );
}

#[test]
fn strict_into_checks_cardinality() {
    for (values, sqlstate) in [
        (vec![], "P0002"),
        (vec![vec![Value::Int(1)], vec![Value::Int(2)]], "P0003"),
    ] {
        let mut spi = mock([rows(vec![column("id", DataType::Integer)], values)]);
        let error = execute(
            "strict",
            "DECLARE v int; BEGIN SELECT id INTO STRICT v FROM t; RETURN NEW; END",
            &trigger(TriggerOp::Insert),
            &mut spi,
        )
        .unwrap_err();
        assert_eq!(error.sqlstate.as_ref(), sqlstate);
        assert_eq!(
            error.context,
            ["PL/pgSQL function strict() line 1 at SQL statement"]
        );
    }
}

#[test]
fn raise_levels_options_and_formatting() {
    let mut spi = mock([
        int(3),
        one(DataType::Text, Value::Null),
        text("notice detail"),
        text("warning"),
        text("custom"),
        text("22012"),
        text("raise hint"),
    ]);
    let error = execute(
        "levels",
        r#"
BEGIN
  RAISE DEBUG 'debug';
  RAISE LOG 'log';
  RAISE INFO 'info';
  RAISE NOTICE E'id % at 100%%, null %', NEW.id, NULL USING DETAIL = 'notice detail';
  RAISE WARNING USING MESSAGE = 'warning';
  RAISE EXCEPTION USING MESSAGE = 'custom', ERRCODE = '22012', HINT = 'raise hint';
END"#,
        &trigger(TriggerOp::Insert),
        &mut spi,
    )
    .unwrap_err();
    assert_eq!(
        spi.notices
            .iter()
            .map(|notice| notice.level)
            .collect::<Vec<_>>(),
        [
            RaiseLevel::Debug,
            RaiseLevel::Log,
            RaiseLevel::Info,
            RaiseLevel::Notice,
            RaiseLevel::Warning,
        ]
    );
    assert_eq!(spi.notices[3].message.as_ref(), "id 3 at 100%, null <NULL>");
    assert_eq!(spi.notices[3].detail.as_deref(), Some("notice detail"));
    assert_eq!(spi.notices[4].message.as_ref(), "warning");
    assert_eq!(error.sqlstate.as_ref(), "22012");
    assert_eq!(error.message.as_ref(), "custom");
    assert_eq!(error.hint.as_deref(), Some("raise hint"));
}

#[test]
fn record_assignment_and_row_return_expressions() {
    let mut spi = mock([
        int(6),
        rows(
            trigger(TriggerOp::Update).columns,
            vec![vec![
                Value::Int(3),
                Value::Int(6),
                Value::Text("old".into()),
            ]],
        ),
    ]);
    let output = execute(
        "rows",
        r#"DECLARE r record;
BEGIN
  r := (OLD);
  r.v := 6;
  RETURN ROW(r.id, r.v, r."Col");
END"#,
        &trigger(TriggerOp::Update),
        &mut spi,
    )
    .unwrap();
    assert_eq!(
        output,
        TriggerReturn::Row(vec![
            Value::Int(3),
            Value::Int(6),
            Value::Text("old".into())
        ])
    );
    assert_eq!(
        spi.calls[1],
        (
            "SELECT $1, $2, $3".into(),
            vec![
                param(DataType::Integer, Value::Int(3)),
                param(DataType::Integer, Value::Int(6)),
                param(DataType::Text, Value::Text("old".into()))
            ]
        )
    );
}

#[test]
fn a_forward_for_sets_found_false_when_empty() {
    let mut spi = mock([boolean(true), int(2), int(1), boolean(false)]);
    let result = execute(
        "empty_for",
        "BEGIN PERFORM 1; FOR i IN 2..1 LOOP RETURN NULL; END LOOP; IF FOUND THEN RETURN NULL; END IF; RETURN NEW; END",
        &trigger(TriggerOp::Insert), &mut spi,
    ).unwrap();
    assert!(matches!(result, TriggerReturn::Row(_)));
    assert_eq!(
        spi.calls[3].1,
        vec![param(DataType::Boolean, Value::Bool(false))]
    );
}

#[test]
fn body_validation_defers_sql_beyond_the_relational_parser() {
    let body = "BEGIN PERFORM xmlparse(document '<a/>'); RETURN NEW; END";
    compile(body, &signature("valid_postgres")).unwrap();
    let invalid = "BEGIN DELETE FROM t WHERE AND true; RETURN NEW; END";
    let error = compile(invalid, &signature("invalid")).unwrap_err();
    assert_eq!(error.position, invalid.find("AND"));
    assert_eq!(error.message, "syntax error at or near \"AND\"");
}
