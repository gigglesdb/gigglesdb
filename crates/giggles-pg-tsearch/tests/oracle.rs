//! PostgreSQL 18.6 oracle fixtures in COPY text format.
use std::collections::BTreeMap;
use std::fmt::Display;

use giggles_pg_tsearch::*;

const EN: Config = Config::English;
const SIMPLE: Config = Config::Simple;
const DEFAULT: &str = "default";
const MARK: &str =
    "StartSel=<mark>, StopSel=</mark>, MaxFragments=1, MaxWords=24, MinWords=8, ShortWord=2";
const FRAGMENTS: &str = "MaxFragments=2, MaxWords=10, MinWords=3, FragmentDelimiter= ... ";

type Row = Vec<Option<String>>;

fn decode(field: &str) -> Option<String> {
    if field == "\\N" {
        return None;
    }
    let mut chars = field.chars();
    let mut decoded = String::new();
    while let Some(c) = chars.next() {
        decoded.push(if c == '\\' {
            match chars.next().expect("COPY escape") {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'b' => '\x08',
                'f' => '\x0c',
                'v' => '\x0b',
                '\\' => '\\',
                c => panic!("unrecognized COPY escape {c:?} in {field:?}"),
            }
        } else {
            c
        });
    }
    Some(decoded)
}

fn rows(input: &'static str) -> impl Iterator<Item = Row> {
    input
        .split_terminator('\n')
        .map(|line| line.split('\t').map(decode).collect())
}

fn field(row: &Row, i: usize) -> &str {
    row[i].as_deref().expect("fixture field is non-null")
}

fn id(row: &Row, i: usize) -> usize {
    field(row, i).parse().unwrap()
}

fn keyed(input: &'static str) -> BTreeMap<usize, String> {
    rows(input)
        .map(|row| (id(&row, 0), field(&row, 1).to_owned()))
        .collect()
}

fn array(values: &[String]) -> String {
    let mut text = "{".to_owned();
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            text.push(',');
        }
        let quote = value.is_empty()
            || value.eq_ignore_ascii_case("NULL")
            || value
                .chars()
                .any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\' | ' ' | '\t'..='\r'));
        if quote {
            text.push('"');
        }
        for c in value.chars() {
            if matches!(c, '"' | '\\') {
                text.push('\\');
            }
            text.push(c);
        }
        if quote {
            text.push('"');
        }
    }
    text.push('}');
    text
}

trait OracleValue: Display {
    fn check_storage(&self) {}
}

impl OracleValue for String {}
impl OracleValue for bool {}
impl OracleValue for usize {}

fn check_vector_storage(value: &TsVector) {
    let bytes = value.to_storage();
    let restored = TsVector::from_storage(&bytes).unwrap();
    assert_eq!(&restored, value);
    assert_eq!(restored.send(), value.send());
    assert_eq!(restored.to_storage(), bytes);
}

fn check_query_storage(value: &TsQuery) {
    let bytes = value.to_storage();
    let restored = TsQuery::from_storage(&bytes).unwrap();
    assert_eq!(&restored, value);
    assert_eq!(restored.send(), value.send());
    assert_eq!(restored.to_storage(), bytes);
}

impl OracleValue for TsVector {
    fn check_storage(&self) {
        check_vector_storage(self);
    }
}

impl OracleValue for TsQuery {
    fn check_storage(&self) {
        check_query_storage(self);
    }
}

fn display(result: Result<impl OracleValue, TsError>) -> String {
    match result {
        Ok(value) => {
            value.check_storage();
            value.to_string()
        }
        Err(err) => format!("ERROR {} {}", err.sqlstate, err.message),
    }
}

#[derive(Default)]
struct Assertions {
    passed: usize,
    failed: usize,
}

impl Assertions {
    fn row<T: std::fmt::Debug + Eq>(
        &mut self,
        key: impl std::fmt::Debug,
        actual: &T,
        expected: &T,
    ) {
        if actual == expected {
            self.passed += 1;
        } else {
            self.failed += 1;
            if self.failed <= 12 {
                eprintln!("{key:?}\n  actual   {actual:?}\n  expected {expected:?}");
            }
        }
    }

    fn finish(self, file: &str) {
        println!(
            "{file}: {}/{} passed",
            self.passed,
            self.passed + self.failed
        );
        assert_eq!(self.failed, 0, "{file}");
    }
}

macro_rules! fixture {
    ($file:literal) => {
        include_str!(concat!("fixtures/", $file, ".tsv"))
    };
}

#[test]
fn stem() {
    let mut tests = Assertions::default();
    for row in rows(fixture!("stem")) {
        let input = field(&row, 1);
        tests.row(
            (id(&row, 0), input),
            &array(&ts_lexize(Dictionary::EnglishStem, input)),
            &field(&row, 2).to_owned(),
        );
    }
    tests.finish("stem.tsv");
}

#[test]
fn to_tsvector_fixture() {
    let mut tests = Assertions::default();
    for row in rows(fixture!("to_tsvector")) {
        let input = field(&row, 1);
        tests.row(
            (id(&row, 0), input),
            &[EN, SIMPLE].map(|conf| display(to_tsvector(conf, input).map(|v| v.value))),
            &[field(&row, 2), field(&row, 3)].map(str::to_owned),
        );
    }
    tests.finish("to_tsvector.tsv");
}

#[test]
fn codepoints() {
    let mut tests = Assertions::default();
    for row in rows(fixture!("codepoints")) {
        let cp = id(&row, 0);
        let c = char::from_u32(cp as u32).unwrap();
        let tokens = format!("x{c}y 1{c} {c}");
        let parsed = ts_debug(SIMPLE, &tokens)
            .map(|d| format!("{}={}", d.token.kind.alias, d.token.text))
            .collect::<Vec<_>>()
            .join(" ");
        let normalized = display(to_tsvector(SIMPLE, &format!("{c}Q")).map(|v| v.value));
        tests.row(
            cp,
            &[parsed, normalized],
            &[field(&row, 1), field(&row, 2)].map(str::to_owned),
        );
    }
    tests.finish("codepoints.tsv");
}

#[test]
fn ts_parse_fixture() {
    let texts = keyed(fixture!("to_tsvector"));
    let mut tests = Assertions::default();
    let mut current = None;
    let mut iter = None;
    for row in rows(fixture!("ts_parse")) {
        let text_id = id(&row, 0);
        if current != Some(text_id) {
            assert!(
                iter.as_mut()
                    .is_none_or(|iter: &mut Parser<'_>| iter.next().is_none()),
                "extra tokens for {current:?}"
            );
            iter = Some(ts_parse(DEFAULT, &texts[&text_id]).unwrap());
            current = Some(text_id);
        }
        let token = iter.as_mut().unwrap().next();
        let expected = Some((id(&row, 1) as u8, field(&row, 2)));
        tests.row(
            (text_id, &texts[&text_id]),
            &token.map(|t| (t.kind.id, t.text)),
            &expected,
        );
    }
    assert!(iter.unwrap().next().is_none());
    tests.finish("ts_parse.tsv");
}

#[test]
fn ts_debug_fixture() {
    let texts = keyed(fixture!("to_tsvector"));
    let mut tests = Assertions::default();
    let mut current = None;
    let mut iter = ts_debug(EN, "");
    for row in rows(fixture!("ts_debug")) {
        let text_id = id(&row, 0);
        if current != Some(text_id) {
            assert!(iter.next().is_none(), "extra debug tokens for {current:?}");
            iter = ts_debug(EN, &texts[&text_id]);
            current = Some(text_id);
        }
        let actual = iter.next().map(|d| {
            let dictionary = d.dictionary.map(|d| d.name().to_owned());
            let dictionaries = dictionary
                .as_ref()
                .map_or("{}".to_owned(), |name| format!("{{{name}}}"));
            [
                Some(d.token.kind.alias.to_owned()),
                Some(d.token.text.to_owned()),
                Some(dictionaries),
                dictionary,
                d.lexemes.map(|lexemes| array(&lexemes)),
            ]
        });
        let expected = Some(std::array::from_fn(|i| row[i + 1].clone()));
        tests.row((text_id, &texts[&text_id]), &actual, &expected);
    }
    assert!(iter.next().is_none());
    tests.finish("ts_debug.tsv");
}

#[test]
fn tsquery_funcs() {
    let mut tests = Assertions::default();
    for row in rows(fixture!("tsquery_funcs")) {
        let q = field(&row, 1);
        let actual = [
            to_tsquery(EN, q),
            plainto_tsquery(EN, q),
            phraseto_tsquery(EN, q),
            websearch_to_tsquery(EN, q),
            to_tsquery(SIMPLE, q),
            websearch_to_tsquery(SIMPLE, q),
        ];
        tests.row(
            (id(&row, 0), q),
            &actual.map(|r| display(r.map(|q| q.value))),
            &std::array::from_fn(|i| field(&row, i + 2).to_owned()),
        );
    }
    tests.finish("tsquery_funcs.tsv");
}

#[test]
fn tsvector_in() {
    let mut tests = Assertions::default();
    for row in rows(fixture!("tsvector_in")) {
        let text = field(&row, 1);
        let vector = text.parse::<TsVector>();
        tests.row(
            (id(&row, 0), text),
            &[
                display(vector.clone()),
                display(vector.clone().map(|v| v.len())),
                display(vector.map(|v| v.strip())),
            ],
            &std::array::from_fn(|i| field(&row, i + 2).to_owned()),
        );
    }
    tests.finish("tsvector_in.tsv");
}

#[test]
fn tsquery_in() {
    let mut tests = Assertions::default();
    for row in rows(fixture!("tsquery_in")) {
        let text = field(&row, 1);
        let query = text.parse::<TsQuery>();
        tests.row(
            (id(&row, 0), text),
            &[
                display(query.clone()),
                display(query.clone().map(|v| v.numnode())),
                display(query.map(|v| v.querytree())),
            ],
            &std::array::from_fn(|i| field(&row, i + 2).to_owned()),
        );
    }
    tests.finish("tsquery_in.tsv");
}

fn sort_oracle<V: std::str::FromStr + Ord + Display>(
    literals: &str,
    oracle: &'static str,
    file: &str,
) {
    let mut values: Vec<(V, usize)> = rows_by_id(literals)
        .into_iter()
        .filter_map(|(id, value)| value.parse::<V>().ok().map(|value| (value, id)))
        .collect();
    values.sort();
    let mut tests = Assertions::default();
    let expected: Vec<_> = rows(oracle).collect();
    assert_eq!(values.len(), expected.len());
    for row in expected {
        let rank = id(&row, 0);
        let (actual, index) = &values[rank - 1];
        tests.row(
            rank,
            &(*index, actual.to_string()),
            &(id(&row, 1), field(&row, 2).to_owned()),
        );
    }
    tests.finish(file);
}

fn rows_by_id(literals: &str) -> BTreeMap<usize, String> {
    literals
        .split_terminator('\n')
        .map(|line| line.split('\t').map(decode).collect::<Row>())
        .map(|row| (id(&row, 0), field(&row, 1).to_owned()))
        .collect()
}

#[test]
fn tsvector_sort() {
    sort_oracle::<TsVector>(
        fixture!("tsvector_in"),
        fixture!("tsvector_sort"),
        "tsvector_sort.tsv",
    );
}

#[test]
fn tsquery_sort() {
    sort_oracle::<TsQuery>(
        fixture!("tsquery_in"),
        fixture!("tsquery_sort"),
        "tsquery_sort.tsv",
    );
}

fn text_vectors(config: Config) -> BTreeMap<usize, TsVector> {
    keyed(fixture!("to_tsvector"))
        .iter()
        .map(|(&id, text)| (id, to_tsvector(config, text).unwrap().value))
        .collect()
}

#[test]
fn setweight_concat() {
    let english = text_vectors(EN);
    let simple = text_vectors(SIMPLE);
    let mut tests = Assertions::default();
    for row in rows(fixture!("setweight_concat")) {
        let (a, b) = (id(&row, 0), id(&row, 1));
        let va = &english[&a];
        let vb = &english[&b];
        let values = [
            va.setweight(Weight::A, None)
                .concat(&vb.setweight(Weight::B, None))
                .unwrap(),
            va.concat(&simple[&b]).unwrap(),
            va.setweight(Weight::C, None),
            va.setweight(Weight::D, None),
        ];
        tests.row(
            (a, b),
            &values.map(|v| display(Ok(v))),
            &std::array::from_fn(|i| field(&row, i + 2).to_owned()),
        );
    }
    tests.finish("setweight_concat.tsv");
}

type QueryCases = BTreeMap<usize, (TsQuery, TsQuery, TsVector)>;

fn query_cases() -> QueryCases {
    keyed(fixture!("tsquery_funcs"))
        .iter()
        .map(|(&id, text)| {
            (
                id,
                (
                    websearch_to_tsquery(EN, text).unwrap().value,
                    plainto_tsquery(EN, text).unwrap().value,
                    to_tsvector(EN, text)
                        .unwrap()
                        .value
                        .setweight(Weight::B, None),
                ),
            )
        })
        .collect()
}

fn float_bits(row: &Row, i: usize) -> u32 {
    field(row, i).parse::<f32>().unwrap().to_bits()
}

#[test]
fn match_rank() {
    let vectors = text_vectors(EN);
    let queries = query_cases();
    let mut tests = Assertions::default();
    for row in rows(fixture!("match_rank")) {
        let (a, b) = (id(&row, 0), id(&row, 1));
        let vector = &vectors[&a];
        let (web, plain, query_vector) = &queries[&b];
        let joined = vector
            .setweight(Weight::A, None)
            .concat(query_vector)
            .unwrap();
        let ranks = [
            ts_rank(vector, web, None, 0).unwrap(),
            ts_rank_cd(vector, web, None, 0).unwrap(),
            ts_rank(&joined, plain, None, 0).unwrap(),
        ];
        tests.row(
            (a, b),
            &(ts_match(vector, web), ranks.map(f32::to_bits)),
            &(
                field(&row, 2) == "true",
                std::array::from_fn(|i| float_bits(&row, i + 3)),
            ),
        );
    }
    tests.finish("match_rank.tsv");
}

#[test]
fn rank_variants() {
    let vectors = text_vectors(EN);
    let queries = query_cases();
    let mut tests = Assertions::default();
    for row in rows(fixture!("rank_variants")) {
        let (a, b, n) = (id(&row, 0), id(&row, 1), id(&row, 2) as i32);
        let vector = &vectors[&a];
        let plain = &queries[&b].1;
        let explicit = [Some(0.1), Some(0.2), Some(0.4), Some(1.0)];
        let ranks = [
            ts_rank(vector, plain, None, n).unwrap(),
            ts_rank_cd(vector, plain, None, n).unwrap(),
            ts_rank(
                &vector.setweight(Weight::B, None),
                plain,
                Some(&explicit),
                n,
            )
            .unwrap(),
        ];
        tests.row(
            (a, b, n),
            &ranks.map(f32::to_bits),
            &std::array::from_fn(|i| float_bits(&row, i + 3)),
        );
    }
    tests.finish("rank_variants.tsv");
}

#[test]
fn headline() {
    let texts = keyed(fixture!("to_tsvector"));
    let queries = query_cases();
    let fox = &queries[&1].0;
    let mut tests = Assertions::default();
    for row in rows(fixture!("headline")) {
        let (a, b) = (id(&row, 0), id(&row, 1));
        let text = &texts[&a];
        let q = &queries[&b].0;
        let actual = [
            (q, None),
            (q, Some(MARK)),
            (q, Some(FRAGMENTS)),
            (fox, Some("HighlightAll=true")),
        ]
        .map(|(q, opts)| display(ts_headline(EN, text, q, opts).map(|v| v.value)));
        tests.row(
            (a, b),
            &actual,
            &std::array::from_fn(|i| field(&row, i + 2).to_owned()),
        );
    }
    tests.finish("headline.tsv");
}

#[test]
fn tsquery_ops() {
    let queries = keyed(fixture!("tsquery_in"));
    let mut tests = Assertions::default();
    for row in rows(fixture!("tsquery_ops")) {
        let (a, b) = (id(&row, 0), id(&row, 1));
        let lhs = queries[&a].parse::<TsQuery>();
        let rhs = queries[&b].parse::<TsQuery>();
        let try_both = |op: fn(&TsQuery, &TsQuery) -> Result<TsQuery, TsError>| {
            display(
                lhs.as_ref()
                    .map_err(Clone::clone)
                    .and_then(|a| rhs.as_ref().map_err(Clone::clone).and_then(|b| op(a, b))),
            )
        };
        let actual = [
            try_both(TsQuery::and),
            try_both(TsQuery::or),
            display(lhs.clone().and_then(|a| a.negate())),
            try_both(|a, b| a.phrase(b, None)),
            display(
                lhs.clone()
                    .and_then(|a| rhs.clone().map(|b| a.contains(&b))),
            ),
            display(lhs.and_then(|a| rhs.map(|b| a.contained_by(&b)))),
        ];
        tests.row(
            (a, b),
            &actual,
            &std::array::from_fn(|i| field(&row, i + 2).to_owned()),
        );
    }
    tests.finish("tsquery_ops.tsv");
}

#[test]
fn storage_rejects_corrupt_payloads() {
    let vector = "a b:2".parse::<TsVector>().unwrap().to_storage();
    let query = "cat:*A".parse::<TsQuery>().unwrap().to_storage();
    for end in 0..vector.len() {
        assert_eq!(TsVector::from_storage(&vector[..end]), Err(Corrupt));
    }
    for end in 0..query.len() {
        assert_eq!(TsQuery::from_storage(&query[..end]), Err(Corrupt));
    }
    let mut bad = vector.clone();
    bad[4] = 8; // Neither input order has a data size of 8.
    assert_eq!(TsVector::from_storage(&bad), Err(Corrupt));
    let mut bad = vector.clone();
    bad.extend_from_slice(&[0]);
    assert_eq!(TsVector::from_storage(&bad), Err(Corrupt));
    let mut bad = vector;
    bad[0] = 255;
    assert_eq!(TsVector::from_storage(&bad), Err(Corrupt));
    let mut bad = query;
    bad[7] = 2; // Noncanonical prefix flag.
    assert_eq!(TsQuery::from_storage(&bad), Err(Corrupt));
}

#[test]
fn literal_match() {
    let queries = keyed(fixture!("tsquery_in"));
    let vectors = keyed(fixture!("tsvector_in"));
    let mut tests = Assertions::default();
    for row in rows(fixture!("literal_match")) {
        let (a, b) = (id(&row, 0), id(&row, 1));
        let actual = display(
            vectors[&a]
                .parse::<TsVector>()
                .and_then(|v| queries[&b].parse::<TsQuery>().map(|q| ts_match(&v, &q))),
        );
        tests.row((a, b), &actual, &field(&row, 2).to_owned());
    }
    tests.finish("literal_match.tsv");
}

#[test]
fn binary_vectors_keep_duplicate_lexemes_and_position_zero() {
    // W1-binary-vector.sql, verified with binary COPY in PostgreSQL 18.6.
    let duplicate = b"\0\0\0\x02cat\0\0\x01\0\x01cat\0\0\x01\xc0\x02";
    let v = TsVector::recv(duplicate).unwrap();
    check_vector_storage(&v);
    assert_eq!(v.send(), duplicate);
    assert_eq!(v.to_string(), "'cat':1 'cat':2A");
    assert_eq!(v.len(), 2);
    assert!(ts_match(&v, &"cat:A".parse().unwrap()));
    assert!(!ts_match(&v, &"cat:D".parse().unwrap()));
    assert_ne!(v, "cat:1 cat:2A".parse::<TsVector>().unwrap());
    assert_eq!(v.delete(&[Some("cat")]).to_string(), "'cat':1");
    assert_eq!(
        v.setweight(Weight::B, Some(&[Some("cat")])).to_string(),
        "'cat':1 'cat':2B"
    );

    let zero = b"\0\0\0\x01zero\0\0\x01\0\0";
    let v = TsVector::recv(zero).unwrap();
    check_vector_storage(&v);
    assert_eq!(v.send(), zero);
    assert_eq!(v.to_string(), "'zero':0");
    assert_eq!(v.len(), 1);
    assert!(ts_match(&v, &"zero".parse().unwrap()));
    assert!(!ts_match(&v, &"zero <0> zero".parse().unwrap()));
    assert_eq!(
        "zero:0".parse::<TsVector>().unwrap_err().message,
        "wrong position info in tsvector: \"zero:0\""
    );
}

#[test]
fn binary_query_round_trip() {
    let original = "fox:A <2> (!dog | bird:*)".parse::<TsQuery>().unwrap();
    let expected =
        b"\0\0\0\x06\x02\x04\0\x02\x02\x03\x01\0\x01bird\0\x02\x01\x01\0\0dog\0\x01\x08\0fox\0";
    assert_eq!(original.send(), expected);
    assert_eq!(
        TsQuery::recv(expected).unwrap().to_string(),
        original.to_string()
    );
}

#[test]
fn parser_and_match_smoke() {
    let unit = "The quick brown fox jumps over the lazy dog. ";
    let count = if cfg!(debug_assertions) { 1200 } else { 24_000 };
    let text = unit.repeat(count);
    let start = std::time::Instant::now();
    let vector = to_tsvector(EN, &text).unwrap().value;
    let query = websearch_to_tsquery(EN, "\"brown fox\" -cat")
        .unwrap()
        .value;
    for _ in 0..10_000 {
        assert!(ts_match(std::hint::black_box(&vector), &query));
    }
    println!(
        "{} bytes and 10,000 matches in {:?}",
        text.len(),
        start.elapsed()
    );
}

#[test]
fn oracle_boundary_behaviors() {
    // W1-edge.sql, verified against PostgreSQL 18.6.
    let literal = format!(
        "dog:{}",
        (1..=300)
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    assert_eq!(
        literal.parse::<TsVector>().unwrap().lexemes()[0]
            .positions
            .len(),
        256
    );
    assert_eq!(
        to_tsvector(SIMPLE, &"dog ".repeat(300))
            .unwrap()
            .value
            .lexemes()[0]
            .positions
            .len(),
        255,
    );
    assert_eq!(
        "dog:1,16383D,16383A"
            .parse::<TsVector>()
            .unwrap()
            .to_string(),
        "'dog':1,16383",
    );
    assert_eq!(
        "dog:16383D,16383A".parse::<TsVector>().unwrap().to_string(),
        "'dog':16383A",
    );
    assert_eq!(TsQuery::default().send(), [0; 4]);
    check_query_storage(&TsQuery::default());
    let err = TsQuery::recv(&[0; 4]).unwrap_err();
    assert_eq!(
        (err.sqlstate, err.message.as_str()),
        ("XX000", "malformed tsquery: operand not found")
    );

    assert_eq!(Config::lookup(" pg_catalog . English ").unwrap(), EN);
    assert_eq!(
        Config::lookup("\"English\"").unwrap_err().message,
        "text search configuration \"English\" does not exist"
    );
    assert_eq!(Config::lookup("English.").unwrap_err().sqlstate, "42602");
    assert_eq!(
        Config::lookup("pg_catalog.NO_SUCH").unwrap_err().message,
        "text search configuration \"pg_catalog.no_such\" does not exist"
    );
    assert_eq!(
        TsVector::from_array(&[Some("")]).unwrap_err().sqlstate,
        "2200F"
    );

    let q = "fox".parse::<TsQuery>().unwrap();
    assert_eq!(
        ts_headline(EN, "fox", &q, Some("StartSel=,StopSel=]"))
            .unwrap()
            .value,
        ",StopSel=]fox</b>",
    );
    assert_eq!(
        ts_headline(EN, "fox", &q, Some("StartSel=+001"))
            .unwrap()
            .value,
        "1fox</b>",
    );
    assert_eq!(
        ts_headline(EN, "fox", &q, Some("\"startSel\"='[ ]', \"stopSel\"=']'"))
            .unwrap()
            .value,
        "[ ]fox]",
    );
    let err = ts_headline(EN, "fox", &q, Some("MaxWords=abc")).unwrap_err();
    assert_eq!(
        (err.sqlstate, err.message.as_str()),
        ("22P02", "invalid input syntax for type integer: \"abc\"")
    );
}

#[test]
fn binary_vector_order_and_diagnostics() {
    // W1-wire-edge.sql, verified with PostgreSQL 18.6 COPY FROM.
    let mut wire = 10u32.to_be_bytes().to_vec();
    for (i, word) in ["z", "cat", "b", "cat", "d", "a", "cat", "c", "cat", "e"]
        .iter()
        .enumerate()
    {
        wire.extend_from_slice(word.as_bytes());
        wire.extend_from_slice(&[0, 0, 1, 0, i as u8 + 1]);
    }
    let vector = TsVector::recv(&wire).unwrap();
    check_vector_storage(&vector);
    assert_eq!(
        vector.to_string(),
        "'a':6 'b':3 'c':8 'cat':7 'cat':9 'cat':2 'cat':4 'd':5 'e':10 'z':1"
    );
    let wire = b"\0\0\0\x02b\0\0\x01\0\x02a\0\0\0";
    let unsorted = TsVector::recv(wire).unwrap();
    assert_eq!(unsorted.to_string(), "'a' 'b':2");
    let canonical = unsorted.to_string().parse::<TsVector>().unwrap();
    assert!(unsorted > canonical);
    assert_eq!(unsorted.send(), canonical.send());
    check_vector_storage(&unsorted);
    check_vector_storage(&canonical);
    let stored_unsorted = TsVector::from_storage(&unsorted.to_storage()).unwrap();
    let stored_canonical = TsVector::from_storage(&canonical.to_storage()).unwrap();
    assert!(stored_unsorted > stored_canonical);
    let bad = [
        (
            &b"\0\0\0\x01\xffz\0\0\0"[..],
            "22021",
            "invalid byte sequence for encoding \"UTF8\": 0xff",
        ),
        (
            &b"\0\0\0\x01\xe2\x80z\0\0\0"[..],
            "22021",
            "invalid byte sequence for encoding \"UTF8\": 0xe2 0x80 0x7a",
        ),
        (
            &b"\0\0\0\x01a\0\0\0\x80"[..],
            "22P03",
            "incorrect binary data format",
        ),
        (&b"\0\0\0\x01ab"[..], "08P01", "invalid string in message"),
        (&b"\0\0"[..], "08P01", "insufficient data left in message"),
    ];
    for (wire, state, message) in bad {
        let err = TsVector::recv(wire).unwrap_err();
        assert_eq!((err.sqlstate, err.message.as_str()), (state, message));
    }
}

#[test]
fn long_query_spines_and_parentheses() {
    // W1-query-depth.sql, oracle PostgreSQL 18.6 at max_stack_depth = 2MB.
    let terms = 32_768;
    let input = "a & ".repeat(terms - 1) + "a";
    let literal = input.parse::<TsQuery>().unwrap();
    let plain = plainto_tsquery(SIMPLE, &"a ".repeat(terms)).unwrap().value;
    assert_eq!(literal.numnode(), 2 * terms - 1);
    assert_eq!(plain.numnode(), literal.numnode());
    assert!(ts_match(&"a:1".parse().unwrap(), &plain));
    assert!(TsQuery::recv(&literal.send()).unwrap() == literal);
    assert!(ts_match(&"a:1".parse().unwrap(), &literal));
    assert_eq!(literal.clone().to_string().matches("'a'").count(), terms);

    let within = "(".repeat(4096) + "a" + &")".repeat(4096);
    assert_eq!(within.parse::<TsQuery>().unwrap().numnode(), 1);
    let outside = "(".repeat(8192) + "a" + &")".repeat(8192);
    assert_eq!(outside.parse::<TsQuery>().unwrap_err().sqlstate, "54001");
    let many_nots = "!".repeat(33) + "a";
    assert_eq!(
        many_nots.parse::<TsQuery>().unwrap_err().message,
        "tsquery stack too small"
    );
}

#[test]
fn query_operator_operand_offset_limit() {
    // W1-query-size.sql. The limit is on each operand's offset.
    let word = "x".repeat(2046);
    let text = (word.clone() + "&").repeat(511) + &word;
    let large = text.parse::<TsQuery>().unwrap();
    let one = "a".repeat(2046).parse::<TsQuery>().unwrap();
    let joined = large.and(&one).unwrap();
    assert_eq!(joined.numnode(), 1025);
    assert_eq!(joined.negate().unwrap().numnode(), 1026);
    let error = joined.and(&one).unwrap_err();
    assert_eq!(
        (error.sqlstate, error.message.as_str()),
        ("54000", "tsquery is too large")
    );
}
