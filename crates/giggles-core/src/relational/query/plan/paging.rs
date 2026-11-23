//! SELECT decorations use hidden scalar outputs for ordering and duplicate keys.
use super::*;
use parse::{ParseResult, parser, top, word};

/// Keep optional SELECT operators out of every recursive Plan return frame.
#[derive(Debug, Clone, Default)]
pub(super) struct Options {
    pub with_ties: bool,
    pub distinct_on: usize,
    pub hidden: usize,
    pub grouping_sets: Option<group_sets::GroupingSets>,
    pub sample: Option<Box<Sample>>,
}

#[derive(Debug, Clone)]
pub(super) struct Sample {
    pub percent: Scalar,
    pub seed: Option<Scalar>,
}
impl Sample {
    pub fn parse(p: &mut Parser) -> ParseResult<Option<Self>> {
        if !p.word("tablesample") {
            return Ok(None);
        }
        if !p.word("system") && !p.word("bernoulli") {
            return Err(ParseError::Definite(SqlError::UndefinedObject));
        }
        p.punct('(')?;
        let percent = Scalar::parse_expr(p, 0, false)?;
        p.punct(')')?;
        let seed = if p.word("repeatable") {
            p.punct('(')?;
            let seed = Scalar::parse_expr(p, 0, false)?;
            p.punct(')')?;
            Some(seed)
        } else {
            None
        };
        Ok(Some(Self { percent, seed }))
    }
    pub fn apply(
        &self,
        relation: &mut exec::Relation<'_>,
        params: &[Value],
        parameters: &[Option<DataType>],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        execution: &Execution<'_>,
    ) -> R<()> {
        let value = |s: &Scalar| -> R<f64> {
            let mut bound = Scalar::Cast(Box::new(s.clone()), DataType::DoublePrecision).bind(
                &|_, _| Err(SqlError::UnknownColumn),
                &[],
                parameters,
            )?;
            bound.attach_clock(clock);
            match bound.eval(
                &Tuple {
                    source_versions: vec![],
                    values: vec![],
                    version: 0,
                },
                params,
                &mut 0,
            )? {
                Value::Float8(n) => Ok(n.value()),
                _ => Err(SqlError::InvalidTablesampleArgument),
            }
        };
        let percent = value(&self.percent)?;
        if !percent.is_finite() || !(0.0..=100.0).contains(&percent) {
            return Err(SqlError::InvalidTablesampleArgument);
        }
        let mut seed = if let Some(s) = &self.seed {
            value(s)?.to_bits()
        } else {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos() as u64)
        };
        let mut rows = vec![];
        for row in std::mem::take(&mut relation.rows) {
            if execution.cancelled() {
                return Err(SqlError::Aborted);
            }
            seed = seed.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^= z >> 31;
            if percent == 100.0
                || (percent > 0.0 && ((z >> 11) as f64 / (1u64 << 53) as f64) < percent / 100.0)
            {
                rows.push(row);
            }
        }
        relation.rows = rows;
        Ok(())
    }
}

fn scalar(item: &Item) -> R<Scalar> {
    super::super::grouping::output_scalar(item)
}
fn output(s: &Scalar, query: &Query) -> R<Scalar> {
    if let Scalar::Column(c) = s
        && c.qualifier.is_none()
    {
        let mut matches = query
            .items
            .iter()
            .filter(|(_, alias)| alias.as_ref() == Some(&c.name));
        if let Some((item, _)) = matches.next() {
            if matches.next().is_some() {
                return Err(SqlError::AmbiguousColumn);
            }
            return scalar(item);
        }
    }
    if let Scalar::Literal(Value::Int(n)) = s {
        let i = usize::try_from(*n)
            .ok()
            .and_then(|n| n.checked_sub(1))
            .ok_or(SqlError::InvalidColumnReference)?;
        return scalar(
            &query
                .items
                .get(i)
                .ok_or(SqlError::InvalidColumnReference)?
                .0,
        );
    }
    Ok(s.clone())
}
pub(super) fn claims(tokens: &[Token]) -> R<bool> {
    let positions = top(tokens)?;
    let distinct = tokens.get(1).is_some_and(|t| word(t, "distinct"))
        && tokens.get(2).is_some_and(|t| word(t, "on"));
    let grouped = Plan::requires_grouping_rewrite(tokens);
    if distinct || grouped {
        return Ok(true);
    }
    Ok(positions.iter().any(|i| {
        ["limit", "offset", "fetch"]
            .iter()
            .any(|w| word(&tokens[*i], w))
    }) && user_types::probe_preserving_error_message(|| Query::parse(&mut parser(tokens))).is_err())
}
pub(super) fn parse(
    tokens: &[Token],
    following: Option<&Token>,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> ParseResult<Plan> {
    let positions = top(tokens)?;
    let page = positions
        .iter()
        .copied()
        .find(|i| {
            ["limit", "offset", "fetch"]
                .iter()
                .any(|w| word(&tokens[*i], w))
        })
        .unwrap_or(tokens.len());
    let distinct = tokens.get(1).is_some_and(|t| word(t, "distinct"))
        && tokens.get(2).is_some_and(|t| word(t, "on"));
    let body_end = tokens.get(page).or(following);
    let mut body = tokens[..page].to_vec();
    let mut keys = vec![];
    if distinct {
        let mut p = parser(&body[3..]);
        if !p.sym('(') {
            return Err(ParseError::Definite(clause_syntax(p.tokens.get(p.at).or(body_end))));
        }
        loop {
            keys.push(Scalar::parse_expr(&mut p, 0, false)?);
            if !p.sym(',') {
                break;
            }
        }
        p.punct(')')?;
        body.drain(1..3 + p.at);
    }
    let grouping_sets = group_sets::rewrite(&mut body, body_end)?;
    if grouping_sets.is_none() && !distinct && Plan::requires_grouping_rewrite(tokens) {
        body.extend_from_slice(&tokens[page..]);
        return parse::select(&body, following, owner, depth, next);
    }
    let mut plan = parse::select_base(
        &body, body_end, owner, depth, next, grouping_sets.as_ref(), &mut keys,
    )?;
    plan = parse::pagination(plan, &tokens[page..], following)?;
    let Body::Select(select) = &mut plan.body else {
        return Err(ParseError::Decline);
    };
    if select.windows.is_empty() {
        plan.decorations.grouping_sets = grouping_sets;
    } else if let Some(input) = &mut select.input {
        // Window functions consume the concatenated rows of all grouping sets.
        input.query.decorations.grouping_sets = grouping_sets;
    }
    if plan.decorations.with_ties && select.query.order.is_empty() {
        return Err(ParseError::Definite(SqlError::Syntax));
    }
    let order = std::mem::take(&mut select.query.order);
    let resolved_order = order
        .iter()
        .map(|(s, d, n)| {
            let scalar = if matches!(s, Scalar::Literal(_)) {
                s.clone()
            } else {
                output(s, &select.query)?
            };
            Ok((scalar, *d, *n))
        })
        .collect::<R<Vec<_>>>()?;
    let keys = keys
        .iter()
        .map(|s| output(s, &select.query))
        .collect::<R<Vec<_>>>()?;
    for (i, s) in keys.into_iter().enumerate() {
        select
            .query
            .items
            .push((Item::Scalar(s), Some(format!("\0distinct_on_{i}"))));
        plan.decorations.distinct_on += 1;
        plan.decorations.hidden += 1;
    }
    for (i, (s, desc, nulls)) in resolved_order.into_iter().enumerate() {
        if matches!(s, Scalar::Literal(_)) {
            plan.order.push((s, desc, nulls));
            continue;
        }
        let name = format!("\0page_order_{i}");
        select
            .query
            .items
            .push((Item::Scalar(s), Some(name.clone())));
        plan.order.push((
            Scalar::Column(ColumnRef {
                qualifier: None,
                name,
            }),
            desc,
            nulls,
        ));
        plan.decorations.hidden += 1;
    }
    Ok(plan)
}

pub(super) fn validate_distinct(
    query: &Query,
    count: usize,
    order: &[(Scalar, bool, bool)],
    state: &Relational,
) -> R<()> {
    if count == 0 {
        return Ok(());
    }
    let tables = query
        .sources
        .iter()
        .map(|s| views::table(state, &s.name))
        .collect::<R<Vec<_>>>()?;
    let outer_slots = RefCell::new(vec![]);
    let resolve = |c: &ColumnRef, scope: usize| {
        let mapped = derived::column(query, c)?;
        outer::resolve(
            state,
            query,
            &[],
            &tables,
            mapped.as_ref().unwrap_or(c),
            scope,
            &outer_slots,
        )
    };
    let identity =
        |s: &Scalar| super::super::grouping::canonical(s, &resolve, tables.len(), &tables);
    let projection = super::super::grouping::projection(query, &tables)?;
    let selected = |name: &str| -> R<Scalar> {
        let (item, _) = projection
            .iter()
            .find(|(_, alias)| alias.as_deref() == Some(name))
            .ok_or(SqlError::UnknownColumn)?;
        scalar(item)
    };
    let mut remaining = (0..count)
        .map(|i| identity(&selected(&format!("\0distinct_on_{i}"))?))
        .collect::<R<Vec<_>>>()?;
    let keys = remaining.clone();
    for (s, _, _) in order {
        let value = if let Scalar::Column(c) = s {
            selected(&c.name)?
        } else if let Scalar::Literal(Value::Int(n)) = s {
            let i = usize::try_from(*n)
                .ok()
                .and_then(|n| n.checked_sub(1))
                .ok_or(SqlError::InvalidColumnReference)?;
            scalar(&projection.get(i).ok_or(SqlError::InvalidColumnReference)?.0)?
        } else {
            return Err(SqlError::InvalidColumnReference);
        };
        let value = identity(&value)?;
        if let Some(i) = remaining.iter().position(|key| *key == value) {
            remaining.remove(i);
        } else if !remaining.is_empty() && !keys.contains(&value) {
            return Err(SqlError::InvalidColumnReference);
        }
    }
    Ok(())
}

pub(super) fn trim_meta(meta: &mut bind::Meta, hidden: usize) {
    let len = meta.columns.len().saturating_sub(hidden);
    meta.columns.truncate(len);
    meta.typmods.truncate(len);
    meta.type_oids.truncate(len);
    meta.unknown.truncate(len);
}
pub(super) fn distinct(
    result: &mut exec::Relation<'_>,
    count: usize,
    collations: &[Option<BoundScalar>],
    hidden: usize,
    tie_keys: &mut Vec<Vec<Value>>,
    execution: &Execution<'_>,
) -> R<()> {
    if count == 0 {
        return Ok(());
    }
    let start = result
        .columns
        .len()
        .checked_sub(hidden)
        .ok_or(SqlError::Syntax)?;
    let mut seen = BTreeSet::new();
    let mut rows = vec![];
    let mut retained_keys = vec![];
    let mut old_keys = std::mem::take(tie_keys).into_iter();
    for row in std::mem::take(&mut result.rows) {
        let tie = old_keys.next();
        if execution.cancelled() {
            return Err(SqlError::Aborted);
        }
        let key = super::super::grouping::Key::metadata(row.values[start..start + count].to_vec(), &collations[start..start + count]);
        if !seen.contains(&key) {
            result.scope.borrow().charge(
                std::mem::size_of_val(&key)
                    + key.0.len() * std::mem::size_of::<Value>()
                    + 6 * std::mem::size_of::<usize>()
                    + key.0.iter().map(value_bytes).sum::<usize>(),
            )?;
            seen.insert(key);
            rows.push(row);
            if let Some(key) = tie {
                retained_keys.push(key);
            }
        }
    }
    result.rows = rows;
    *tie_keys = retained_keys;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(state: &mut Relational, sql: &str) -> SqlResult {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        state
            .execute(&p, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    }
    fn fixture() -> Relational {
        let mut state = Relational::default();
        run(
            &mut state,
            "CREATE TABLE h (id int PRIMARY KEY, d text, a int)",
        );
        run(
            &mut state,
            "INSERT INTO h VALUES (1,'x',1),(2,'x',2),(3,'y',2)",
        );
        state
    }
    fn rows(state: &mut Relational, sql: &str) -> Vec<Vec<Value>> {
        run(state, sql).rows.into_iter().map(|r| r.values).collect()
    }
    fn error(state: &mut Relational, sql: &str, expected: SqlError) {
        let result =
            Prepared::parse(sql, 1, state.epoch).and_then(|p| state.execute(&p, &[], None));
        assert_eq!(result.unwrap_err(), expected, "{sql}");
    }
    #[test]
    fn distinct_on_order_and_errors() {
        let mut state = fixture();
        assert_eq!(
            rows(
                &mut state,
                "SELECT DISTINCT ON(d) id FROM h ORDER BY d,a DESC,id"
            ),
            vec![vec![Value::Int(2)], vec![Value::Int(3)]]
        );
        error(
            &mut state,
            "SELECT DISTINCT ON(d) id FROM h ORDER BY a,d",
            SqlError::InvalidColumnReference,
        );
        assert_eq!(
            rows(
                &mut state,
                "SELECT DISTINCT ON(h.d) d,id FROM h ORDER BY d,id"
            )
            .len(),
            2
        );
        assert_eq!(
            rows(
                &mut state,
                "SELECT DISTINCT ON(d) d,id FROM h ORDER BY 1,id"
            )
            .len(),
            2
        );
    }
    #[test]
    fn fetch_pagination_ties_and_parameters() {
        let mut state = fixture();
        assert_eq!(
            rows(
                &mut state,
                "SELECT id FROM h ORDER BY a DESC FETCH FIRST ROW WITH TIES"
            ),
            vec![vec![Value::Int(2)], vec![Value::Int(3)]]
        );
        assert_eq!(
            rows(
                &mut state,
                "SELECT id FROM h ORDER BY id OFFSET (2-1) ROW FETCH NEXT (1+1) ROWS ONLY"
            ),
            vec![vec![Value::Int(2)], vec![Value::Int(3)]]
        );
        assert!(rows(&mut state, "SELECT id FROM h FETCH FIRST 0 ROWS ONLY").is_empty());
        assert_eq!(
            rows(
                &mut state,
                "SELECT * FROM h ORDER BY 1 FETCH FIRST 2 ROWS ONLY"
            )
            .len(),
            2
        );
        assert_eq!(
            rows(&mut state, "SELECT id FROM h ORDER BY id LIMIT E'2'").len(),
            2
        );
        assert_eq!(
            rows(&mut state, "SELECT id FROM h ORDER BY id LIMIT 1.5::float8").len(),
            2
        );
        assert_eq!(
            rows(&mut state, "SELECT id FROM h LIMIT (NULL::int)").len(),
            3
        );
        let p = Prepared::parse(
            "SELECT id FROM h ORDER BY id LIMIT ($1+1) OFFSET ($2-1)",
            1,
            state.epoch,
        )
        .unwrap();
        assert_eq!(p.describe(&state).unwrap().parameters.len(), 2);
        assert_eq!(
            state
                .execute(&p, &[Value::Int(1), Value::Int(2)], None)
                .unwrap()
                .rows
                .len(),
            2
        );
        error(
            &mut state,
            "SELECT id FROM h FETCH FIRST ROW WITH TIES",
            SqlError::Syntax,
        );
        error(
            &mut state,
            "SELECT id FROM h LIMIT (0-1)",
            SqlError::InvalidRowCountInLimitClause,
        );
        error(
            &mut state,
            "SELECT id FROM h OFFSET (0-1) ROWS",
            SqlError::InvalidRowCountInResultOffsetClause,
        );
        error(
            &mut state,
            "SELECT id FROM h LIMIT (id+1)",
            SqlError::InvalidColumnReference,
        );
    }
    #[test]
    fn grouping_sets_rollup_cube_and_empty_input() {
        use Value::{Int as I, Null as N, Text as T};
        let mut state = fixture();
        let expected = vec![
            vec![T("x".into()), I(1), I(0), I(1)],
            vec![T("x".into()), I(2), I(0), I(1)],
            vec![T("y".into()), I(2), I(0), I(1)],
            vec![T("x".into()), N, I(1), I(2)],
            vec![T("y".into()), N, I(1), I(1)],
            vec![N, N, I(3), I(3)],
        ];
        assert_eq!(
            rows(
                &mut state,
                "SELECT d,a,GROUPING(d,a),count(*) FROM h GROUP BY ROLLUP(d,a) ORDER BY 3,1,2"
            ),
            expected
        );
        assert_eq!(
            rows(
                &mut state,
                "SELECT d,a,GROUPING(d,a),count(*) FROM h GROUP BY GROUPING SETS ((d,a),(d),()) ORDER BY 3,1,2"
            ),
            expected
        );
        assert_eq!(
            rows(
                &mut state,
                "SELECT d,a,GROUPING(d,a),count(*) FROM h GROUP BY CUBE(d,a) ORDER BY 3,1,2"
            )
            .len(),
            8
        );
        assert_eq!(rows(&mut state, "SELECT d,a,GROUPING(d,a),count(*) FROM h GROUP BY d,GROUPING SETS (ROLLUP(a),()) ORDER BY 3,1,2").len(), 7);
        error(
            &mut state,
            "SELECT GROUPING(d) FROM h",
            SqlError::GroupingError,
        );
        error(
            &mut state,
            "SELECT d,count(*) FROM h GROUP BY ROLLUP(id)",
            SqlError::GroupingError,
        );
        error(
            &mut state,
            "SELECT count(*) FROM h GROUP BY GROUPING SETS ()",
            SqlError::Syntax,
        );
        error(
            &mut state,
            "SELECT count(*) FROM h GROUP BY ROLLUP()",
            SqlError::Syntax,
        );
        error(
            &mut state,
            "SELECT GROUPING(a),count(*) FROM h GROUP BY ROLLUP(d)",
            SqlError::GroupingError,
        );
        assert_eq!(
            rows(
                &mut state,
                "SELECT d,a,GROUPING(d,a),count(*) FROM h WHERE false GROUP BY ROLLUP(d,a)"
            ),
            vec![vec![N, N, I(3), I(0)]]
        );
    }
    #[test]
    fn samples_and_postfix_nulls() {
        let mut state = fixture();
        assert_eq!(
            rows(
                &mut state,
                "SELECT id FROM h TABLESAMPLE SYSTEM(100) REPEATABLE(42) ORDER BY id"
            )
            .len(),
            3
        );
        assert!(rows(&mut state, "SELECT id FROM h TABLESAMPLE BERNOULLI(0)").is_empty());
        let sql = "SELECT id FROM h TABLESAMPLE BERNOULLI(50) REPEATABLE(12) ORDER BY id";
        assert_eq!(rows(&mut state, sql), rows(&mut state, sql));
        error(
            &mut state,
            "SELECT id FROM h TABLESAMPLE SYSTEM(-1)",
            SqlError::InvalidTablesampleArgument,
        );
        error(
            &mut state,
            "SELECT id FROM h TABLESAMPLE BERNOULLI(101)",
            SqlError::InvalidTablesampleArgument,
        );
        assert_eq!(
            rows(
                &mut state,
                "SELECT NULL ISNULL,1 NOTNULL,NULL NOTNULL,1 ISNULL"
            ),
            vec![vec![
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false)
            ]]
        );
        assert_eq!(
            rows(&mut state, "SELECT id FROM h WHERE a NOTNULL ORDER BY id").len(),
            3
        );
        run(&mut state, "CREATE TABLE postfix_names(id int PRIMARY KEY,\"isnull\" int,\"notnull\" int)");
        run(&mut state, "INSERT INTO postfix_names VALUES(1,7,NULL)");
        assert_eq!(rows(&mut state, "SELECT n.isnull,n.notnull FROM postfix_names n"), vec![vec![Value::Int(7),Value::Null]]);
        assert_eq!(rows(&mut state, "SELECT 1 AS isnull,2 AS notnull"), vec![vec![Value::Int(1),Value::Int(2)]]);
    }
    #[test]
    fn prefixed_literals_resolve_like_quoted_literals() {
        let mut state = fixture();
        assert_eq!(
            rows(
                &mut state,
                r"SELECT id,E'it''s\tvalue\\ok',U&'caf\00E9',$tag$it's a label$tag$ FROM h ORDER BY id"
            )[0],
            vec![
                Value::Int(1),
                Value::Text("it's\tvalue\\ok".into()),
                Value::Text("café".into()),
                Value::Text("it's a label".into())
            ]
        );
        assert_eq!(
            rows(&mut state, "SELECT $$42$$::int"),
            vec![vec![Value::Int(42)]]
        );
    }
    #[test]
    fn grouping_and_sampling_dependencies_and_depth() {
        assert!(!Plan::claims(&lex("FETCH NEXT FROM c").unwrap()));
        assert!(!Plan::claims(&lex("SELECT grouping,cube,rollup FROM h").unwrap()));
        for sql in ["SELECT cube(a) FROM h GROUP BY a", "SELECT rollup(a) FROM h GROUP BY a",
            "SELECT a FROM h GROUP BY abs(cube(a))", "SELECT 1 AS fetch", "SELECT h.tablesample FROM h"] {
            assert!(!Plan::claims(&lex(sql).unwrap()), "{sql}");
        }
        let state = fixture();
        let labels = vec!["1 AS case"; 40].join(",");
        assert!(Prepared::parse(&format!("SELECT {labels}"), 1, state.epoch).is_ok());
        for sql in [
            "SELECT d,count(*) FROM h GROUP BY ROLLUP(d)",
            "SELECT id FROM h TABLESAMPLE SYSTEM(100)",
            "SELECT DISTINCT ON(d) id FROM h ORDER BY d,id",
            "SELECT id FROM h FETCH FIRST 2 ROWS ONLY",
        ] {
            let p = Prepared::parse(sql, 1, state.epoch).unwrap();
            assert_eq!(p.read_tables(), vec!["h"], "{sql}");
            assert_eq!(p.authorization_tables().unwrap(), p.read_tables(), "{sql}");
        }
        let sql = format!(
            "SELECT count(*) FROM h GROUP BY {}d{}",
            "ROLLUP(".repeat(40),
            ")".repeat(40)
        );
        assert!(matches!(
            Prepared::parse(&sql, 1, state.epoch),
            Err(SqlError::Limit)
        ));
    }
}

#[cfg(test)]
mod collation_rebase_tests {
    use super::*;
    fn run(state: &mut Relational, sql: &str) -> SqlResult {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        state.execute(&p, &[], None).unwrap()
    }
    #[test]
    fn grouping_sets_and_distinct_on_preserve_collation() {
        let mut state = Relational::default();
        for sql in [
            "CREATE COLLATION rebase_ci (provider=icu,locale='und-u-ks-level2',deterministic=false)",
            "CREATE TABLE rebase_groups(v text COLLATE rebase_ci)",
            "INSERT INTO rebase_groups VALUES ('ABC'),('abc'),('def')",
        ] { run(&mut state, sql); }
        for syntax in ["GROUPING SETS ((v),())", "ROLLUP(v)", "CUBE(v)"] {
            let result = run(&mut state, &format!("SELECT v,count(*),GROUPING(v) FROM rebase_groups GROUP BY {syntax}"));
            assert_eq!(result.rows.len(), 3, "{syntax}");
            let counts: Vec<_> = result.rows.iter().map(|r| r.values[1].clone()).collect();
            assert!(counts.contains(&Value::Int(2)), "{syntax}: {counts:?}");
            assert!(counts.contains(&Value::Int(3)), "{syntax}: {counts:?}");
        }
        let result = run(&mut state, "SELECT DISTINCT ON (v) v FROM rebase_groups ORDER BY v");
        assert_eq!(result.rows.len(), 2);
        let result = run(&mut state, "SELECT v,count(*) FROM rebase_groups WHERE false GROUP BY ROLLUP(v)");
        assert_eq!(result.rows[0].values, vec![Value::Null, Value::Int(0)]);
    }
}

#[cfg(test)]
mod serde_append_tests {
    use super::*;
    use bincode::Options;
    #[test]
    fn skipped_variants_have_asymmetric_wire_indices() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        enum Probe {
            Prior,
            #[serde(skip)]
            Skipped,
            Added,
        }
        let le = bincode::DefaultOptions::new().with_fixint_encoding().with_little_endian().reject_trailing_bytes();
        assert_eq!(le.serialize(&Probe::Added).unwrap(), [2, 0, 0, 0]);
        assert!(le.deserialize::<Probe>(&[2, 0, 0, 0]).is_err());
        assert_eq!(le.deserialize::<Probe>(&[1, 0, 0, 0]).unwrap(), Probe::Added);
        assert!(le.serialize(&Probe::Skipped).is_err());
        assert_eq!(le.serialize(&SqlError::InvalidTablesampleArgument).unwrap(), [64, 0, 0, 0]);
        assert_eq!(le.deserialize::<SqlError>(&[64, 0, 0, 0]).unwrap(), SqlError::InvalidTablesampleArgument);
        assert_eq!(crate::local::failure(crate::CoreError::Sql(SqlError::CollationMismatch)).code, 264);
        assert_eq!(crate::local::failure(crate::CoreError::Sql(SqlError::IndeterminateCollation)).code, 265);
        assert_eq!(crate::local::failure(crate::CoreError::Sql(SqlError::InvalidTablesampleArgument)).code, 266);
    }
}
