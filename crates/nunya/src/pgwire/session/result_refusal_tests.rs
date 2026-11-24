// Result-time refusals retain one audit outcome and drain exactly one segment.
const PROTECTED_ROW: &[u8] = b"private-result-refusal-fixture";
const RESULT_READ: &str = "SELECT value FROM items";
const RESULT_WRITE: &str = "INSERT INTO items(value) VALUES (NULL)";

fn assert_result_error(out: &Actions, code: NyCode, close: bool) {
    assert_eq!(out.close, close);
    let [Backend::ErrorResponse(fields)] = out.client.as_slice() else {
        panic!("expected one error, received {:?}", out.client);
    };
    assert_eq!(*fields, nunya_error(code));
    assert!(!format!("{fields:?}").contains(std::str::from_utf8(PROTECTED_ROW).unwrap()));
}

fn assert_discarded(out: Actions) {
    assert!(!out.close);
    assert!(
        out.client.is_empty(),
        "unexpected client message: {:?}",
        out.client
    );
    assert!(out.upstream.is_empty());
}

fn assert_result_ready(s: &mut Session, upstream: u8, client: u8) {
    let out = s.backend(Backend::ReadyForQuery(upstream));
    assert!(!out.close);
    assert!(matches!(out.client.as_slice(), [Backend::ReadyForQuery(status)] if *status == client));
}

fn assert_refused_result_audit(f: &Fixture, code: NyCode, tokens: &[String]) {
    let records = f.identities.lock().unwrap();
    let refused: Vec<_> = records
        .iter()
        .filter(|r| matches!(r.outcome, Outcome::Refused(_)))
        .collect();
    let [record] = refused.as_slice() else {
        panic!("expected one refused result")
    };
    assert!(matches!(record.outcome, Outcome::Refused(actual) if actual == code));
    assert_eq!(record.rows, tokens.len() as u64);
    assert_eq!(record.tokens_released, tokens);
    assert!(
        !record
            .statement
            .contains(std::str::from_utf8(PROTECTED_ROW).unwrap())
    );
    assert!(
        record
            .tokens_released
            .iter()
            .all(|token| !token.contains(std::str::from_utf8(PROTECTED_ROW).unwrap()))
    );
    assert_eq!(
        f.ctx
            .metrics
            .snapshot()
            .statements_refused
            .iter()
            .sum::<u64>(),
        1
    );
}

fn result_row() -> Backend {
    Backend::DataRow(vec![Some(PROTECTED_ROW.to_vec())])
}

fn release_result_rows(s: &mut Session) -> Vec<String> {
    (0..2)
        .map(|_| {
            let out = s.backend(result_row());
            assert!(!out.close);
            let [Backend::DataRow(row)] = out.client.as_slice() else {
                panic!("expected a row")
            };
            let token = std::str::from_utf8(row[0].as_deref().unwrap()).unwrap();
            assert!(token.starts_with("tok_secret_"));
            s.release_done();
            token.to_owned()
        })
        .collect()
}

fn begin_result_transaction(s: &mut Session) {
    assert!(matches!(
        s.frontend(Frontend::Query("BEGIN".into()))
            .upstream
            .as_slice(),
        [Frontend::Query(_)]
    ));
    s.backend(Backend::CommandComplete("BEGIN".into()));
    assert_result_ready(s, b'T', b'T');
}

fn assert_result_commit_rolls_back(s: &mut Session) {
    let commit = s.frontend(Frontend::Query("COMMIT".into()));
    assert!(matches!(commit.upstream.as_slice(), [Frontend::Query(query)] if query == "ROLLBACK"));
    let out = s.backend(Backend::CommandComplete("ROLLBACK".into()));
    assert!(matches!(out.client.as_slice(), [Backend::CommandComplete(tag)] if tag == "ROLLBACK"));
    assert_result_ready(s, b'I', b'I');
}

fn assert_next_result_query(s: &mut Session) {
    let query = s.frontend(Frontend::Query(RESULT_READ.into()));
    assert!(!query.close);
    assert!(matches!(query.upstream.as_slice(), [Frontend::Query(_)]));
    assert!(matches!(
        s.backend(Backend::RowDescription(vec![field()]))
            .client
            .as_slice(),
        [Backend::RowDescription(_)]
    ));
    assert!(matches!(
        s.backend(result_row()).client.as_slice(),
        [Backend::DataRow(_)]
    ));
    s.release_done();
    assert!(matches!(
        s.backend(Backend::CommandComplete("SELECT 1".into()))
            .client
            .as_slice(),
        [Backend::CommandComplete(_)]
    ));
    assert_result_ready(s, b'I', b'I');
}

#[test]
fn result_refusal_simple_tightening_drains_multiple_reads() {
    for explicit in [false, true] {
        let status = if explicit { b'T' } else { b'I' };
        let f = Fixture::new();
        publish_revision(&f, "unread", Treatment::Show);
        let mut s = f.session();
        if status == b'T' {
            begin_result_transaction(&mut s);
        }
        assert_eq!(
            s.frontend(Frontend::Query(format!("{RESULT_READ}; {RESULT_READ}")))
                .upstream
                .len(),
            1
        );
        s.backend(Backend::RowDescription(vec![field()]));
        let tokens = release_result_rows(&mut s);
        assert!(publish_revision(&f, "unread", Treatment::Masked { placeholder: None }).is_some());
        assert_result_error(&s.backend(result_row()), NyCode::SchemaStale, false);
        s.release_done();
        for message in [
            result_row(),
            Backend::NoticeResponse(vec![(b'M', "notice".into())]),
            Backend::CommandComplete("SELECT 4".into()),
            Backend::RowDescription(vec![field()]),
            result_row(),
            Backend::CommandComplete("SELECT 1".into()),
        ] {
            assert_discarded(s.backend(message));
        }
        assert_result_ready(&mut s, status, if status == b'T' { b'E' } else { status });
        assert_refused_result_audit(&f, NyCode::SchemaStale, &tokens);
        if status == b'T' {
            assert_result_commit_rolls_back(&mut s);
        }
        assert_next_result_query(&mut s);
    }
}

#[test]
fn result_refusal_simple_count_mismatch_already_consumed_completion() {
    let f = Fixture::new();
    let mut s = f.session();
    s.frontend(Frontend::Query(format!("{RESULT_READ}; {RESULT_READ}")));
    s.backend(Backend::RowDescription(vec![field()]));
    let tokens = release_result_rows(&mut s);
    assert_result_error(
        &s.backend(Backend::CommandComplete("SELECT 3".into())),
        NyCode::NotAnalysable,
        false,
    );
    assert_discarded(s.backend(Backend::RowDescription(vec![field()])));
    assert_discarded(s.backend(result_row()));
    assert_discarded(s.backend(Backend::CommandComplete("SELECT 1".into())));
    assert_result_ready(&mut s, b'I', b'I');
    assert_refused_result_audit(&f, NyCode::NotAnalysable, &tokens);
    assert_next_result_query(&mut s);
}

fn queue_result_parse(s: &mut Session, name: &str, sql: &str) {
    let out = s.frontend(Frontend::Parse {
        name: name.into(),
        query: sql.into(),
        param_types: vec![],
    });
    assert!(matches!(out.upstream.as_slice(), [Frontend::Parse { .. }]));
}

fn queue_result_bind(s: &mut Session, statement: &str, portal: &str) {
    let out = s.frontend(Frontend::Bind {
        portal: portal.into(),
        statement: statement.into(),
        param_formats: vec![],
        params: vec![],
        result_formats: vec![1],
    });
    assert!(matches!(out.upstream.as_slice(), [Frontend::Bind { .. }]));
}

fn queue_result_execute(s: &mut Session, portal: &str) {
    let out = s.frontend(Frontend::Execute {
        portal: portal.into(),
        max_rows: 0,
    });
    assert!(matches!(
        out.upstream.as_slice(),
        [Frontend::Execute { .. }]
    ));
}

fn queue_result_read(s: &mut Session) {
    queue_result_parse(s, "read", RESULT_READ);
    queue_result_bind(s, "read", "reader");
    let out = s.frontend(Frontend::Describe {
        kind: b'P',
        name: "reader".into(),
    });
    assert!(matches!(
        out.upstream.as_slice(),
        [Frontend::Describe { .. }]
    ));
    queue_result_execute(s, "reader");
}

fn receive_result_acks(s: &mut Session) {
    assert!(matches!(
        s.backend(Backend::ParseComplete).client.as_slice(),
        [Backend::ParseComplete]
    ));
    assert!(matches!(
        s.backend(Backend::BindComplete).client.as_slice(),
        [Backend::BindComplete]
    ));
}

fn sync_result_refusal(s: &mut Session) {
    let skipped = s.frontend(Frontend::Parse {
        name: "skipped".into(),
        query: RESULT_READ.into(),
        param_types: vec![],
    });
    assert_discarded(skipped);
    assert!(!s.prepared.contains_key("skipped"));
    let sync = s.frontend(Frontend::Sync);
    assert!(
        matches!(sync.upstream.as_slice(), [Frontend::Parse { name, query, param_types }, Frontend::Sync]
        if name.is_empty() && query == "SELECT nunya_refused_statement(" && param_types.is_empty())
    );
}

fn complete_hidden_result_refusal(s: &mut Session) {
    assert_discarded(s.backend(Backend::ErrorResponse(vec![(b'C', "42601".into())])));
}

#[test]
fn result_refusal_extended_describe_drains_unnamed_acks_and_execute() {
    for sync_forwarded in [false, true] {
        let f = Fixture::new();
        let mut s = f.session();
        queue_result_read(&mut s);
        queue_result_parse(&mut s, "", RESULT_READ);
        queue_result_bind(&mut s, "", "");
        queue_result_execute(&mut s, "");
        if sync_forwarded {
            assert!(matches!(
                s.frontend(Frontend::Sync).upstream.as_slice(),
                [Frontend::Sync]
            ));
        }
        receive_result_acks(&mut s);
        // The gateway requested binary and PostgreSQL's Describe reports text.
        let metadata = Backend::RowDescription(vec![field()]);
        assert_result_error(&s.backend(metadata), NyCode::NotAnalysable, false);
        if !sync_forwarded {
            sync_result_refusal(&mut s);
        }
        for message in [
            result_row(),
            Backend::CommandComplete("SELECT 1".into()),
            Backend::ParseComplete,
            Backend::BindComplete,
            result_row(),
            Backend::CommandComplete("SELECT 1".into()),
        ] {
            assert_discarded(s.backend(message));
        }
        assert!(!s.prepared.contains_key(""));
        assert!(!s.portals.contains_key(""));
        if !sync_forwarded {
            complete_hidden_result_refusal(&mut s);
        }
        assert_result_ready(&mut s, b'I', b'I');
        assert_refused_result_audit(&f, NyCode::NotAnalysable, &[]);
        assert_next_result_query(&mut s);
    }
}

#[test]
fn result_refusal_extended_binary_rows_drain_and_preserve_released_tokens() {
    for explicit in [false, true] {
        let status = if explicit { b'T' } else { b'I' };
        for sync_forwarded in [false, true] {
            let f = Fixture::new();
            let mut s = f.session();
            if status == b'T' {
                begin_result_transaction(&mut s);
            }
            queue_result_read(&mut s);
            if sync_forwarded {
                assert!(matches!(
                    s.frontend(Frontend::Sync).upstream.as_slice(),
                    [Frontend::Sync]
                ));
            }
            receive_result_acks(&mut s);
            let description = s.backend(Backend::RowDescription(vec![FieldDescription {
                format: 1,
                ..field()
            }]));
            assert!(
                matches!(description.client.as_slice(), [Backend::RowDescription(fields)] if fields[0].format == 1)
            );
            let tokens = release_result_rows(&mut s);
            let refused = s.backend(Backend::DataRow(vec![Some(vec![0xff])]));
            assert_result_error(&refused, NyCode::BlindOperation, false);
            s.release_done();
            assert_discarded(s.backend(result_row()));
            assert_discarded(s.backend(Backend::CommandComplete("SELECT 4".into())));
            if !sync_forwarded {
                // The pending queue can already be empty when the agent sends Sync.
                sync_result_refusal(&mut s);
                complete_hidden_result_refusal(&mut s);
            }
            let upstream = if status == b'T' && !sync_forwarded {
                b'E'
            } else {
                status
            };
            assert_result_ready(&mut s, upstream, if status == b'T' { b'E' } else { status });
            assert_refused_result_audit(&f, NyCode::BlindOperation, &tokens);
            if status == b'T' {
                assert_result_commit_rolls_back(&mut s);
            }
            assert_next_result_query(&mut s);
        }
    }
}

#[test]
fn result_refusal_simple_unsafe_statements_close() {
    for sql in [
        format!("{RESULT_READ}; {RESULT_WRITE}"),
        format!("{RESULT_WRITE}; {RESULT_READ}"),
        format!("{RESULT_WRITE} RETURNING value"),
        format!("{RESULT_READ}; COMMIT"),
    ] {
        let f = Fixture::new();
        let mut s = f.session();
        if sql.ends_with("COMMIT") {
            begin_result_transaction(&mut s);
        }
        let out = s.frontend(Frontend::Query(sql.clone()));
        assert!(
            matches!(out.upstream.as_slice(), [Frontend::Query(_)]),
            "{sql}"
        );
        if sql == format!("{RESULT_WRITE}; {RESULT_READ}") {
            s.backend(Backend::CommandComplete("INSERT 0 1".into()));
        }
        assert_result_error(
            &s.backend(Backend::RowDescription(vec![])),
            NyCode::NotAnalysable,
            true,
        );
    }
}

#[test]
fn result_refusal_extended_unsafe_segment_closes() {
    for unsafe_operation in [
        "later write",
        "implicit write",
        "named parse",
        "named bind",
        "close",
        "returning",
    ] {
        let f = Fixture::new();
        let mut s = f.session();
        if unsafe_operation == "implicit write" {
            queue_result_parse(&mut s, "write", RESULT_WRITE);
            queue_result_bind(&mut s, "write", "writer");
            queue_result_execute(&mut s, "writer");
            receive_result_acks(&mut s);
            s.backend(Backend::CommandComplete("INSERT 0 1".into()));
        }
        if unsafe_operation == "returning" {
            queue_result_parse(&mut s, "write", &format!("{RESULT_WRITE} RETURNING value"));
            queue_result_bind(&mut s, "write", "writer");
            queue_result_execute(&mut s, "writer");
        } else {
            queue_result_read(&mut s);
        }
        match unsafe_operation {
            "later write" => {
                queue_result_parse(&mut s, "", RESULT_WRITE);
                queue_result_bind(&mut s, "", "");
                queue_result_execute(&mut s, "");
            }
            "implicit write" => {
                s.frontend(Frontend::Sync);
            }
            "named parse" => queue_result_parse(&mut s, "later", RESULT_READ),
            "named bind" => queue_result_bind(&mut s, "read", "later"),
            "close" => {
                assert!(matches!(
                    s.frontend(Frontend::Close {
                        kind: b'S',
                        name: "read".into()
                    })
                    .upstream
                    .as_slice(),
                    [Frontend::Close { .. }]
                ));
            }
            "returning" => {}
            _ => unreachable!(),
        }
        receive_result_acks(&mut s);
        let message = if unsafe_operation == "returning" {
            Backend::DataRow(vec![])
        } else {
            Backend::RowDescription(vec![])
        };
        assert_result_error(&s.backend(message), NyCode::NotAnalysable, true);
    }
}

#[test]
fn result_refusal_retains_fail_closed_protocol_and_audit_errors() {
    for extended in [false, true] {
        for failed_audit in [false, true] {
            let f = Fixture::new();
            let mut s = f.session();
            if extended {
                queue_result_read(&mut s);
                receive_result_acks(&mut s);
            } else {
                s.frontend(Frontend::Query(RESULT_READ.into()));
            }
            f.fail.store(failed_audit, Ordering::Relaxed);
            let refusal = s.backend(Backend::RowDescription(vec![]));
            if failed_audit {
                assert_result_error(&refusal, NyCode::ReadLogUnavailable, true);
            } else {
                assert_result_error(&refusal, NyCode::NotAnalysable, false);
                assert_result_error(
                    &s.backend(Backend::BindComplete),
                    NyCode::ProtocolNotAllowed,
                    true,
                );
            }
        }
    }
}

#[test]
fn result_refusal_extended_prior_write_can_still_roll_back() {
    for explicit in [false, true] {
        let f = Fixture::new();
        let mut s = f.session();
        if explicit {
            begin_result_transaction(&mut s);
        }
        queue_result_parse(&mut s, "write", RESULT_WRITE);
        queue_result_bind(&mut s, "write", "writer");
        queue_result_execute(&mut s, "writer");
        queue_result_read(&mut s);
        if explicit {
            assert!(matches!(
                s.frontend(Frontend::Sync).upstream.as_slice(),
                [Frontend::Sync]
            ));
        }
        receive_result_acks(&mut s);
        assert!(matches!(
            s.backend(Backend::CommandComplete("INSERT 0 1".into()))
                .client
                .as_slice(),
            [Backend::CommandComplete(_)]
        ));
        receive_result_acks(&mut s);
        assert_result_error(
            &s.backend(Backend::RowDescription(vec![])),
            NyCode::NotAnalysable,
            false,
        );
        assert_discarded(s.backend(Backend::CommandComplete("SELECT 0".into())));
        if explicit {
            assert_result_ready(&mut s, b'T', b'E');
            assert_result_commit_rolls_back(&mut s);
        } else {
            sync_result_refusal(&mut s);
            complete_hidden_result_refusal(&mut s);
            assert_result_ready(&mut s, b'I', b'I');
        }
        assert!(!s.write_in_flight());
        assert_refused_result_audit(&f, NyCode::NotAnalysable, &[]);
        assert_next_result_query(&mut s);
    }
}

#[test]
fn result_refusal_simple_begin_in_segment_makes_prior_write_recoverable() {
    let f = Fixture::new();
    let mut s = f.session();
    let query = s.frontend(Frontend::Query(format!(
        "BEGIN; {RESULT_WRITE}; {RESULT_READ}"
    )));
    assert!(matches!(query.upstream.as_slice(), [Frontend::Query(_)]));
    s.backend(Backend::CommandComplete("BEGIN".into()));
    s.backend(Backend::CommandComplete("INSERT 0 1".into()));
    assert_result_error(
        &s.backend(Backend::RowDescription(vec![])),
        NyCode::NotAnalysable,
        false,
    );
    assert_discarded(s.backend(Backend::CommandComplete("SELECT 0".into())));
    assert_result_ready(&mut s, b'T', b'E');
    assert_result_commit_rolls_back(&mut s);
    assert_next_result_query(&mut s);
}

#[test]
fn result_refusal_keeps_one_error_when_upstream_also_fails() {
    for extended in [false, true] {
        let f = Fixture::new();
        let mut s = f.session();
        if extended {
            queue_result_read(&mut s);
            receive_result_acks(&mut s);
        } else {
            s.frontend(Frontend::Query(RESULT_READ.into()));
        }
        assert_result_error(
            &s.backend(Backend::RowDescription(vec![])),
            NyCode::NotAnalysable,
            false,
        );
        let error = || Backend::ErrorResponse(vec![(b'C', "22000".into())]);
        assert_discarded(s.backend(error()));
        if extended {
            // A real upstream error already rolled back the segment.
            assert!(matches!(
                s.frontend(Frontend::Sync).upstream.as_slice(),
                [Frontend::Sync]
            ));
        }
        assert_result_error(&s.backend(error()), NyCode::ProtocolNotAllowed, true);
    }
}
