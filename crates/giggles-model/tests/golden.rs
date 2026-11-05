#[test]
fn deterministic_transcript_matches_checked_in_expectation() {
    assert_eq!(
        giggles_model::fixtures::golden_transcript(),
        include_str!("fixtures/golden.txt").replace("\r\n", "\n")
    );
}
#[test]
fn all_five_typed_golden_histories_verify() {
    let histories = giggles_model::fixtures::golden_histories();
    assert_eq!(histories.len(), 5);
    for h in histories {
        giggles_model::checker::verify(h.domain, &h.events, &h.final_state)
            .unwrap_or_else(|e| panic!("{}: {e}", h.name));
    }
}
