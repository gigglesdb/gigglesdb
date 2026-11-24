//! Pure-API coverage of the three AM-6 corpus cases and the Archdocs SQL's
//! full-text search expressions. SQL types, generated columns and indexes
//! are exercised through the engine by the other AM-6 workers.
use giggles_pg_tsearch::{
    Config, Dictionary, TsQuery, TsVector, Weight, plainto_tsquery, to_tsquery, to_tsvector,
    ts_headline, ts_lexize, ts_match, ts_match_text, ts_match_text_text, ts_rank,
    websearch_to_tsquery,
};

const EN: Config = Config::English;
const CORPUS: &str = include_str!("fixtures/pgf_text_search.pg18.out");

fn result_rows(transcript: &str, statement: u8) -> Vec<&str> {
    transcript
        .split_once(&format!("@@R0 BEGIN {statement}\n"))
        .unwrap()
        .1
        .lines()
        .skip(1)
        .take_while(|line| !line.starts_with("@@R0 END "))
        .collect()
}

fn bool_text(yes: bool) -> &'static str {
    if yes { "t" } else { "f" }
}

fn vector(text: &str) -> TsVector {
    to_tsvector(EN, text).unwrap().value
}

fn literal(text: &str) -> TsQuery {
    text.parse().unwrap()
}

#[test]
fn pgf_text_search() {
    let texts = ["The quick brown fox jumps", "A slow dog"];
    let v = ["'brown':3 'fox':4 'jump':5 'quick':2", "'dog':3 'slow':2"]
        .map(|text| text.parse::<TsVector>().unwrap());
    let derived = texts.map(vector);
    let select_rows = |actual: [&str; 2]| {
        [
            format!("1,{}", actual[0]),
            format!("2,{}", actual[1]),
            "3,".to_owned(),
        ]
    };

    for (i, query) in [
        "'quick' & 'fox'",
        "'quick' & 'fox'",
        "'quick' <-> 'brown' | 'dog'",
    ]
    .iter()
    .enumerate()
    {
        let q = literal(query);
        let actual = v.each_ref().map(|v| bool_text(ts_match(v, &q)));
        assert_eq!(
            select_rows(actual),
            result_rows(CORPUS, i as u8 + 4).as_slice()
        );
    }
    let actual = derived.each_ref().map(ToString::to_string);
    assert_eq!(
        select_rows(actual.each_ref().map(String::as_str)),
        result_rows(CORPUS, 3).as_slice()
    );
    let fox = literal("'fox'");
    let ranks = result_rows(CORPUS, 7);
    for (v, row) in v.iter().zip(ranks) {
        let expected = row.split_once(',').unwrap().1.parse::<f32>().unwrap();
        assert_eq!(
            ts_rank(v, &fox, None, 0).unwrap().to_bits(),
            expected.to_bits()
        );
    }
    let headlines = texts.map(|text| {
        ts_headline(EN, text, &fox, Some("StartSel=<b>,StopSel=</b>"))
            .unwrap()
            .value
    });
    assert_eq!(
        select_rows(headlines.each_ref().map(String::as_str)),
        result_rows(CORPUS, 8).as_slice()
    );

    let results = [
        to_tsquery(EN, "fox & !dog"),
        plainto_tsquery(EN, "the brown fox"),
        websearch_to_tsquery(EN, "fox -dog"),
        to_tsquery(EN, "quick & fox"),
        plainto_tsquery(EN, "quick fox"),
        websearch_to_tsquery(EN, "\"quick brown\" OR dog"),
    ];
    for (i, got) in results.into_iter().enumerate() {
        let got = got.unwrap().value.to_string();
        let expected = result_rows(CORPUS, i as u8 + 9);
        if i < 3 {
            assert_eq!(expected, [got]);
        } else {
            assert_eq!(
                expected,
                (1..=3).map(|id| format!("{id},{got}")).collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn pgy_column_tsvector() {
    let mut rows = [2, 1].map(|id| ("cat:1 dog:2".parse::<TsVector>().unwrap(), id));
    rows.sort();
    let actual = rows
        .iter()
        .map(|(v, id)| {
            let same: TsVector = "cat:1 dog:2".parse().unwrap();
            format!(
                "{id},{v},{},{}",
                bool_text(*v == same),
                bool_text(ts_match(v, &literal("cat")))
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        result_rows(include_str!("fixtures/pgy_column_tsvector.pg18.out"), 3)
    );
}

#[test]
fn pgy_column_tsquery() {
    let mut rows = [2, 1].map(|id| (literal("cat & dog"), id));
    rows.sort();
    let actual = rows
        .iter()
        .map(|(q, id)| {
            format!(
                "{id},{q},{},{}",
                bool_text(*q == literal("cat & dog")),
                bool_text(q.contains(&literal("cat")))
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        result_rows(include_str!("fixtures/pgy_column_tsquery.pg18.out"), 3)
    );
}

struct Document<'a> {
    title: &'a str,
    body: &'a str,
    match_only: &'a str,
}

impl Document<'_> {
    fn visible(&self) -> TsVector {
        vector(self.title)
            .setweight(Weight::A, None)
            .concat(&vector(self.body).setweight(Weight::B, None))
            .unwrap()
    }

    fn indexed(&self) -> TsVector {
        self.visible()
            .concat(&vector(self.match_only).setweight(Weight::C, None))
            .unwrap()
    }
}

#[test]
fn archdocs_indexed_search() {
    const MARK: &str =
        "StartSel=<mark>, StopSel=</mark>, MaxFragments=1, MaxWords=24, MinWords=8, ShortWord=2";
    let mut docs = [
        Document {
            title: "Billing gateway",
            body: "Gateway for payments; owns invoices.",
            match_only: "",
        },
        Document {
            title: "Payment service architecture",
            body: "The payment service talks to Stripe through the billing gateway. Retries are idempotent.",
            match_only: "",
        },
        Document {
            title: "Search indexing design",
            body: "We index documents into a tsvector column and rank results with ts_rank.",
            match_only: "",
        },
    ];
    let expected = [
        "'bill':1A 'gateway':2A,3B 'invoic':7B 'own':6B 'payment':5B",
        "'architectur':3A 'bill':12B 'gateway':13B 'idempot':16B 'payment':1A,5B 'retri':14B 'servic':2A,6B 'stripe':9B 'talk':7B",
        "'column':10B 'design':3A 'document':6B 'index':2A,5B 'rank':12B,16B 'result':13B 'search':1A 'ts':15B 'tsvector':9B",
    ];
    assert_eq!(
        docs.each_ref().map(|doc| doc.indexed().to_string()),
        expected
    );
    docs[0].body = "Payments are retried by the gateway";
    assert_eq!(
        docs[0].indexed().to_string(),
        "'bill':1A 'gateway':2A,8B 'payment':3B 'retri':5B"
    );
    docs[1].match_only = "journal entry about refunds";
    assert_eq!(
        docs[1].indexed().to_string(),
        "'architectur':3A 'bill':12B 'entri':18C 'gateway':13B 'idempot':16B 'journal':17C 'payment':1A,5B 'refund':20C 'retri':14B 'servic':2A,6B 'stripe':9B 'talk':7B"
    );

    type ExpectedHit = (usize, bool, f32, &'static str);
    let queries: [(&str, &[ExpectedHit]); 4] = [
        (
            "payment gateway",
            &[
                (
                    0,
                    false,
                    0.763366,
                    "Billing <mark>gateway</mark> — <mark>Payments</mark> are retried by the <mark>gateway</mark>",
                ),
                (
                    1,
                    false,
                    0.35984504,
                    "<mark>Payment</mark> service architecture — The <mark>payment</mark> service talks to Stripe through the billing <mark>gateway</mark>. Retries are idempotent",
                ),
            ],
        ),
        (
            "refunds",
            &[(
                1,
                true,
                0.12158542,
                "Payment service architecture — The payment service talks to",
            )],
        ),
        (
            "\"billing gateway\" -invoices",
            &[
                (
                    0,
                    false,
                    0.9953511,
                    "<mark>Billing</mark> <mark>gateway</mark> — Payments are retried by the <mark>gateway</mark>",
                ),
                (
                    1,
                    false,
                    0.39641288,
                    "Payment service architecture — The payment service talks to Stripe through the <mark>billing</mark> <mark>gateway</mark>. Retries are idempotent",
                ),
            ],
        ),
        ("the", &[]),
    ];
    for (input, expected) in queries {
        let query = websearch_to_tsquery(EN, input).unwrap().value;
        let mut actual = Vec::new();
        for (id, doc) in docs.iter().enumerate() {
            let indexed = doc.indexed();
            if ts_match(&indexed, &query) {
                let rank = ts_rank(&indexed, &query, None, 0).unwrap();
                let snippet = ts_headline(
                    EN,
                    &format!("{} — {}", doc.title, doc.body),
                    &query,
                    Some(MARK),
                )
                .unwrap()
                .value;
                actual.push((id, !ts_match(&doc.visible(), &query), rank, snippet));
            }
        }
        actual.sort_by(|a, b| {
            b.2.total_cmp(&a.2)
                .then(docs[a.0].title.cmp(docs[b.0].title))
        });
        assert_eq!(actual.len(), expected.len(), "{input}");
        for ((id, journal, rank, snippet), (want_id, want_journal, want_rank, want_snippet)) in
            actual.into_iter().zip(expected)
        {
            assert_eq!(
                (id, journal, rank.to_bits(), snippet.as_str()),
                (*want_id, *want_journal, want_rank.to_bits(), *want_snippet),
                "{input}"
            );
        }
    }
}

#[test]
fn archdocs_scalar_examples() {
    assert_eq!(Config::lookup("pg_catalog.english").unwrap(), EN);
    assert_eq!(Config::lookup("simple").unwrap(), Config::Simple);
    let err = Config::lookup("nosuch").unwrap_err();
    assert_eq!(
        (err.sqlstate, err.message.as_str()),
        (
            "42704",
            "text search configuration \"nosuch\" does not exist"
        )
    );
    assert_eq!(vector("The Fat Rats").to_string(), "'fat':2 'rat':3");
    assert_eq!(
        to_tsquery(EN, "fat & rats").unwrap().value.to_string(),
        "'fat' & 'rat'"
    );
    assert_eq!(
        plainto_tsquery(EN, "Fat Rats").unwrap().value.to_string(),
        "'fat' & 'rat'"
    );
    assert_eq!(
        Weight::parse(b'E').unwrap_err().message,
        "unrecognized weight: \"E\""
    );
    let v: TsVector = "a:1 b:2 c:3".parse().unwrap();
    assert_eq!(
        v.setweight(Weight::A, Some(&[Some("a"), Some("c")]))
            .to_string(),
        "'a':1A 'b':2 'c':3A"
    );
    assert_eq!(v.delete(&[Some("b")]).to_string(), "'a':1 'c':3");
    let v: TsVector = "a:1A b:2B c:3".parse().unwrap();
    assert_eq!(
        v.filter(&[Some(b'a'), Some(b'b')]).unwrap().to_string(),
        "'a':1A 'b':2B"
    );
    assert_eq!(
        TsVector::from_array(&[Some("c"), Some("a"), Some("b")])
            .unwrap()
            .to_string(),
        "'a' 'b' 'c'"
    );
    assert_eq!(
        "b:1 a:2".parse::<TsVector>().unwrap().to_array(),
        ["a", "b"]
    );
    let v: TsVector = "a:1A,2 b:3".parse().unwrap();
    let rows: Vec<_> = v
        .unnest()
        .map(|lexeme| {
            (
                lexeme.word.as_str(),
                lexeme
                    .positions
                    .iter()
                    .map(|pos| (pos.index, pos.weight.letter()))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [("a", vec![(1, 'A'), (2, 'D')]), ("b", vec![(3, 'D')])]
    );
    assert_eq!(literal("a & b").numnode(), 3);
    assert_eq!(literal("a & !b").querytree(), "'a'");
    assert_eq!("a b c".parse::<TsVector>().unwrap().len(), 3);
    assert!(ts_match_text(EN, "fox", &literal("fox")).unwrap().value);
    assert!(
        ts_match_text_text(EN, "the fox jumped", "fox")
            .unwrap()
            .value
    );
    assert_eq!(ts_lexize(Dictionary::EnglishStem, "stars"), ["star"]);
    assert_eq!(ts_lexize(Dictionary::Simple, "The"), ["the"]);
    for result in [to_tsquery(EN, "the"), websearch_to_tsquery(EN, "the a")] {
        let result = result.unwrap();
        assert!(result.value.is_empty());
        assert_eq!(
            result.notices[0].message,
            "text-search query contains only stop words or doesn't contain lexemes, ignored"
        );
    }
}
