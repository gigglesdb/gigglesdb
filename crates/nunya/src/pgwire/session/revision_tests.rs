// Per-row release fencing uses the plan policy, never the previous reload.
fn revision_column(name: &str) -> ColumnRef {
    ColumnRef { schema: "public".into(), table: "items".into(), column: name.into() }
}
fn publish_revision(f: &Fixture, name: &str, treatment: Treatment) -> Option<u64> {
    let mut current = f.ctx.policy.write().unwrap();
    let old = current.clone();
    let mut next = (**current).clone();
    next.columns.insert(revision_column(name), treatment);
    let next = Arc::new(next);
    *current = next.clone();
    f.ctx.fence.publish(&old, &next)
}
fn revision_row(s: &mut Session) -> Actions {
    s.backend(Backend::DataRow(vec![Some(b"private".to_vec())]))
}
fn assert_revision_row(out: &Actions, tokenized: bool) {
    let [Backend::DataRow(row)] = out.client.as_slice() else { panic!("expected released row") };
    assert!(!out.close);
    assert_eq!(row[0].as_deref() != Some(b"private".as_slice()), tokenized);
}
fn assert_revision_refusal(f: &Fixture, out: &Actions) {
    assert!(out.close);
    assert!(matches!(out.client.as_slice(), [Backend::ErrorResponse(fields)]
        if fields.iter().any(|(key, value)| *key == b'C' && value == "NY008")));
    assert!(f.identities.lock().unwrap().iter().any(|r|
        matches!(r.outcome, Outcome::Refused(NyCode::SchemaStale))));
    let index = NyCode::ALL.iter().position(|c| *c == NyCode::SchemaStale).unwrap();
    assert_eq!(f.ctx.metrics.snapshot().statements_refused[index], 1);
}
#[test]
fn revision_simple_refuses_third_row_after_tightening() {
    let f = Fixture::new();
    publish_revision(&f, "value", Treatment::Show);
    let mut s = f.session();
    s.frontend(Frontend::Query("SELECT value FROM items".into()));
    s.backend(Backend::RowDescription(vec![field()]));
    for _ in 0..2 {
        assert_revision_row(&revision_row(&mut s), false);
        s.release_done();
    }
    publish_revision(&f, "value", Treatment::Masked { placeholder: None });
    assert_revision_refusal(&f, &revision_row(&mut s));
}
#[test]
fn revision_extended_resumed_portal_refuses_after_tightening() {
    // Execute's existing admission check rejects a reload already visible there.
    // Also exercise a reload racing after the resumed Execute was forwarded.
    for before_execute in [true, false] {
        let f = Fixture::new();
        publish_revision(&f, "value", Treatment::Show);
        let mut s = f.session();
        prepare_admission_portal(&mut s, "SELECT value FROM items");
        s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 2 });
        s.backend(Backend::RowDescription(vec![field()]));
        for _ in 0..2 {
            assert_revision_row(&revision_row(&mut s), false);
            s.release_done();
        }
        assert!(matches!(s.backend(Backend::PortalSuspended).client.as_slice(), [Backend::PortalSuspended]));
        if before_execute {
            publish_revision(&f, "value", Treatment::Masked { placeholder: None });
        }
        let execute = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 2 });
        if before_execute {
            assert!(execute.upstream.is_empty());
            assert!(matches!(execute.client.as_slice(), [Backend::ErrorResponse(fields)]
                if fields.iter().any(|(key, value)| *key == b'C' && value == "NY008")));
        } else {
            assert!(matches!(execute.upstream.as_slice(), [Frontend::Execute { .. }]));
            publish_revision(&f, "value", Treatment::Masked { placeholder: None });
            assert_revision_refusal(&f, &revision_row(&mut s));
        }
    }
}
#[test]
fn revision_loosening_keeps_original_tokenization() {
    let f = Fixture::new();
    let mut s = f.session();
    s.frontend(Frontend::Query("SELECT value FROM items".into()));
    s.backend(Backend::RowDescription(vec![field()]));
    assert_revision_row(&revision_row(&mut s), true);
    s.release_done();
    assert_eq!(publish_revision(&f, "value", Treatment::Show), None);
    assert_revision_row(&revision_row(&mut s), true);
    s.release_done();
}
#[test]
fn revision_unread_tightening_refuses_under_whole_route_policy() {
    let f = Fixture::new();
    publish_revision(&f, "unread", Treatment::Show);
    let mut s = f.session();
    s.frontend(Frontend::Query("SELECT value FROM items".into()));
    s.backend(Backend::RowDescription(vec![field()]));
    assert_revision_row(&revision_row(&mut s), true);
    s.release_done();
    assert!(publish_revision(&f, "unread", Treatment::Masked { placeholder: None }).is_some());
    assert_revision_refusal(&f, &revision_row(&mut s));
}
#[test]
fn revision_compares_plan_to_current_after_tighten_then_loosen() {
    let f = Fixture::new();
    publish_revision(&f, "unread", Treatment::Show);
    let mut s = f.session();
    s.frontend(Frontend::Query("SELECT value FROM items".into()));
    s.backend(Backend::RowDescription(vec![field()]));
    assert_revision_row(&revision_row(&mut s), true);
    s.release_done();
    assert!(publish_revision(&f, "unread", Treatment::Masked { placeholder: None }).is_some());
    assert_eq!(publish_revision(&f, "unread", Treatment::Show), None);
    // Force the slow path for a loosened read column too: no replan to Show.
    assert_eq!(publish_revision(&f, "value", Treatment::Show), None);
    assert_revision_row(&revision_row(&mut s), true);
    s.release_done();
}
#[test]
fn revision_release_slot_lasts_until_socket_handoff() {
    for extended in [false, true] {
        let f = Fixture::new();
        publish_revision(&f, "unread", Treatment::Show);
        let mut s = f.session();
        if extended {
            prepare_admission_portal(&mut s, "SELECT value FROM items");
            s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
        } else {
            s.frontend(Frontend::Query("SELECT value FROM items".into()));
        }
        s.backend(Backend::RowDescription(vec![field()]));
        assert_revision_row(&revision_row(&mut s), true);
        let epoch = publish_revision(&f, "unread", Treatment::Masked { placeholder: None }).unwrap();
        assert_eq!(f.ctx.fence.lagging(epoch), 1);
        s.release_done();
        assert_eq!(f.ctx.fence.lagging(epoch), 0);
    }
}

#[test]
fn revision_bound_portal_refuses_tightening_before_first_execute() {
    let f = Fixture::new();
    publish_revision(&f, "value", Treatment::Show);
    let mut s = f.session();
    prepare_admission_portal(&mut s, "SELECT value FROM items");
    publish_revision(&f, "value", Treatment::Masked { placeholder: None });
    let out = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 1 });
    assert!(out.upstream.is_empty());
    assert!(matches!(out.client.as_slice(), [Backend::ErrorResponse(fields)]
        if fields.iter().any(|(key, value)| *key == b'C' && value == "NY008")));
}

#[test]
fn revision_bound_and_resumed_portal_keeps_old_plan_on_loosening() {
    for masked in [false, true] {
        let f = Fixture::new();
        if masked {
            publish_revision(&f, "value", Treatment::Masked { placeholder: Some("MASKED".into()) });
        }
        let mut s = f.session();
        prepare_admission_portal(&mut s, "SELECT value FROM items");
        publish_revision(&f, "value", Treatment::Show);
        for _ in 0..2 {
            let out = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 1 });
            assert!(matches!(out.upstream.as_slice(), [Frontend::Execute { .. }]));
            s.backend(Backend::RowDescription(vec![field()]));
            let row = revision_row(&mut s);
            assert_revision_row(&row, true);
            let [Backend::DataRow(values)] = row.client.as_slice() else { unreachable!() };
            if masked {
                assert_eq!(values[0].as_deref(), Some(b"MASKED".as_slice()));
            } else {
                assert!(values[0].as_ref().unwrap().starts_with(b"tok_"));
            }
            s.release_done();
            assert!(matches!(s.backend(Backend::PortalSuspended).client.as_slice(), [Backend::PortalSuspended]));
        }
    }
}

fn assert_execute_refusal(out: &Actions) {
    assert!(out.upstream.is_empty());
    assert!(matches!(out.client.as_slice(), [Backend::ErrorResponse(fields)]
        if fields.iter().any(|(key, value)| *key == b'C' && value == "NY008")));
}
#[test]
fn revision_portal_refuses_unread_tightening_before_and_after_suspend() {
    for suspended in [false, true] {
        let f = Fixture::new();
        publish_revision(&f, "unread", Treatment::Show);
        let mut s = f.session();
        prepare_admission_portal(&mut s, "SELECT value FROM items");
        if suspended {
            s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 1 });
            s.backend(Backend::RowDescription(vec![field()]));
            assert_revision_row(&revision_row(&mut s), true);
            s.release_done();
            s.backend(Backend::PortalSuspended);
        }
        publish_revision(&f, "unread", Treatment::Masked { placeholder: None });
        assert_execute_refusal(&s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 1 }));
    }
}
#[test]
fn revision_show_predicate_or_expression_tightening_refuses_next_row() {
    for sql in ["SELECT 'constant' FROM items WHERE value LIKE 'a%'", "SELECT upper(value) FROM items"] {
        let f = Fixture::new();
        publish_revision(&f, "value", Treatment::Show);
        let mut s = f.session();
        let out = s.frontend(Frontend::Query(sql.into()));
        assert!(matches!(out.upstream.as_slice(), [Frontend::Query(_)]));
        s.backend(Backend::RowDescription(vec![field()]));
        assert_revision_row(&revision_row(&mut s), false);
        s.release_done();
        publish_revision(&f, "value", Treatment::Blind { domain: DomainName("secret".into()) });
        assert_revision_refusal(&f, &revision_row(&mut s));
    }
}
#[test]
fn revision_bound_update_refuses_source_tightening() {
    let f = Fixture::new();
    let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
    snapshot.relations[0].name = "customers".into();
    snapshot.relations[0].columns[0].name = "email".into();
    let mut notes = snapshot.relations[0].columns[0].clone();
    notes.name = "notes".into();
    snapshot.relations[0].columns.push(notes);
    *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
    let email = ColumnRef { schema: "public".into(), table: "customers".into(), column: "email".into() };
    {
        let mut current = f.ctx.policy.write().unwrap();
        let mut next = (**current).clone();
        next.columns.insert(email.clone(), Treatment::Show);
        next.columns.insert(ColumnRef { column: "notes".into(), ..email.clone() }, Treatment::Show);
        *current = Arc::new(next);
    }
    let mut s = f.session();
    prepare_admission_portal(&mut s, "UPDATE customers SET notes = email");
    {
        let mut current = f.ctx.policy.write().unwrap();
        let old = current.clone();
        let mut next = (**current).clone();
        next.columns.insert(email, Treatment::Blind { domain: DomainName("secret".into()) });
        *current = Arc::new(next);
        f.ctx.fence.publish(&old, &current);
    }
    assert_execute_refusal(&s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 }));
}

#[test]
#[ignore = "local per-row release cost measurement"]
fn fence_release_check_cost() {
    const ROWS: usize = 100_000;
    let f = Fixture::new();
    publish_revision(&f, "value", Treatment::Show);
    let mut s = f.session();
    s.frontend(Frontend::Query("SELECT value FROM items".into()));
    let start = std::time::Instant::now();
    let description = s.backend(Backend::RowDescription(vec![field()]));
    assert!(!description.close);
    s.release_done();
    for _ in 0..ROWS {
        let out = s.backend(Backend::DataRow(vec![Some(b"public".to_vec())]));
        assert!(matches!(out.client.as_slice(), [Backend::DataRow(_)]));
        assert!(!out.close);
        std::hint::black_box(out);
        s.release_done();
    }
    let backend = start.elapsed();
    let release = f.ctx.fence.register();
    let epoch = f.ctx.fence.epoch();
    let start = std::time::Instant::now();
    for _ in 0..ROWS {
        std::hint::black_box(release.begin(&f.ctx.fence) == epoch);
        release.end();
    }
    let check = start.elapsed();
    println!("PERF backend rows={ROWS} total_ms={:.3} ns_per_row={:.2}", backend.as_secs_f64() * 1000.0, backend.as_nanos() as f64 / ROWS as f64);
    println!("PERF release-check rows={ROWS} total_ms={:.3} ns_per_row={:.2}", check.as_secs_f64() * 1000.0, check.as_nanos() as f64 / ROWS as f64);
}

#[test]
fn serving_gate_refuses_simple_and_portal_rows() {
    for extended in [false, true] {
        let mut f = Fixture::new();
        let gate = Arc::new(crate::policy::serving::ServingGate::default());
        Arc::get_mut(&mut f.ctx).unwrap().serving_gate = Some(gate.clone());
        gate.extend_to(u64::MAX);
        let mut s = f.session();
        if extended {
            prepare_admission_portal(&mut s, "SELECT value FROM items");
            s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
        } else { s.frontend(Frontend::Query("SELECT value FROM items".into())); }
        s.backend(Backend::RowDescription(vec![field()]));
        gate.close();
        let out = revision_row(&mut s);
        assert!(out.close);
        assert!(matches!(out.client.as_slice(), [Backend::ErrorResponse(fields)]
            if fields.iter().any(|(k, v)| *k == b'C' && v == "NY013")));
        assert!(f.identities.lock().unwrap().iter().any(|r| matches!(r.outcome, Outcome::Refused(NyCode::ServingUnavailable))));
    }
}
#[tokio::test]
async fn serving_gate_refuses_new_connection() {
    let mut f = Fixture::new();
    Arc::get_mut(&mut f.ctx).unwrap().serving_gate = Some(Arc::default());
    let (client, server) = tokio::io::duplex(4096);
    let mut agent = Agent { io: Box::new(server), buffer: BytesMut::new(), ctx: f.ctx.clone() };
    // Refusal precedes startup/authentication and requires no upstream connection.
    let result = serve(&mut agent, f.ctx.clone()).await;
    assert_eq!(result.unwrap_err().code, NyCode::ServingUnavailable);
    drop(client);
}

#[test]
fn serving_gate_refuses_resumed_portal_execute() {
    let mut f = Fixture::new();
    let gate = Arc::new(crate::policy::serving::ServingGate::default());
    Arc::get_mut(&mut f.ctx).unwrap().serving_gate = Some(gate.clone());
    gate.extend_to(u64::MAX);
    let mut s = f.session();
    prepare_admission_portal(&mut s, "SELECT value FROM items");
    s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 1 });
    s.backend(Backend::RowDescription(vec![field()]));
    assert_revision_row(&revision_row(&mut s), true);
    s.release_done();
    s.backend(Backend::PortalSuspended);
    gate.close();
    let out = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 1 });
    assert!(out.close); assert!(out.upstream.is_empty());
    assert!(matches!(out.client.as_slice(), [Backend::ErrorResponse(fields)]
        if fields.iter().any(|(k, v)| *k == b'C' && v == "NY013")));
}
