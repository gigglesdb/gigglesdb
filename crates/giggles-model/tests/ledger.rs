use giggles_model::ledger::*;
fn fixture() -> Ledger {
    let mut l = Ledger::default();
    l.open(AccountId(1), 100).unwrap();
    l.open(AccountId(2), 10).unwrap();
    l
}
fn reserve(l: &mut Ledger) -> Result<State, Error> {
    l.reserve(TransferId(1), AccountId(1), AccountId(2), 40, 100, 1)
}
fn conserved(l: &Ledger) {
    assert_eq!(
        l.accounts()
            .values()
            .map(|a| a.available + a.held)
            .sum::<u128>(),
        110
    );
    let pending = l
        .transfers()
        .values()
        .filter(|t| t.state == State::Pending)
        .map(|t| t.amount)
        .sum::<u128>();
    assert_eq!(l.accounts().values().map(|a| a.held).sum::<u128>(), pending);
}
#[test]
fn reservation_moves_available_into_hold() {
    let mut l = fixture();
    assert_eq!(reserve(&mut l), Ok(State::Pending));
    assert_eq!(
        l.accounts()[&AccountId(1)],
        Account {
            available: 60,
            held: 40
        }
    );
    conserved(&l);
}
#[test]
fn capture_posts_equal_units_once() {
    let mut l = fixture();
    reserve(&mut l).unwrap();
    assert_eq!(l.capture(TransferId(1), 99), Ok(State::Posted));
    let before = l.clone();
    assert_eq!(l.capture(TransferId(1), 101), Ok(State::Posted));
    assert_eq!(l, before);
    assert_eq!(l.accounts()[&AccountId(2)].available, 50);
    conserved(&l);
}
#[test]
fn void_releases_hold_once() {
    let mut l = fixture();
    reserve(&mut l).unwrap();
    assert_eq!(l.void(TransferId(1)), Ok(State::Voided));
    let before = l.clone();
    assert_eq!(l.void(TransferId(1)), Ok(State::Voided));
    assert_eq!(l, before);
    assert_eq!(l.accounts()[&AccountId(1)].available, 100);
    conserved(&l);
}
#[test]
fn capture_and_void_have_one_terminal_winner() {
    for capture_first in [true, false] {
        let mut l = fixture();
        reserve(&mut l).unwrap();
        if capture_first {
            l.capture(TransferId(1), 2).unwrap();
            assert_eq!(l.void(TransferId(1)), Err(Error::Terminal));
        } else {
            l.void(TransferId(1)).unwrap();
            assert_eq!(l.capture(TransferId(1), 2), Err(Error::Terminal));
        }
        conserved(&l);
    }
}
#[test]
fn expiry_is_checked_during_capture_without_sweeper() {
    let mut l = fixture();
    reserve(&mut l).unwrap();
    let before = l.clone();
    assert_eq!(l.capture(TransferId(1), 100), Err(Error::Expired));
    assert_eq!(l, before);
    assert_eq!(l.expire(TransferId(1), 99), Err(Error::Invalid));
    assert_eq!(l.expire(TransferId(1), 100), Ok(State::Expired));
    assert_eq!(l.capture(TransferId(1), 101), Err(Error::Terminal));
    assert_eq!(l.expire(TransferId(1), 102), Ok(State::Expired));
    conserved(&l);
}
#[test]
fn same_id_payload_is_stable_and_altered_payload_conflicts() {
    let mut l = fixture();
    reserve(&mut l).unwrap();
    let before = l.clone();
    assert_eq!(reserve(&mut l), Ok(State::Pending));
    assert_eq!(l, before);
    assert_eq!(
        l.reserve(TransferId(1), AccountId(1), AccountId(2), 41, 100, 1),
        Err(Error::Conflict)
    );
    assert_eq!(l, before);
    l.void(TransferId(1)).unwrap();
    assert_eq!(reserve(&mut l), Ok(State::Voided));
    conserved(&l);
}
#[test]
fn duplicate_account_does_not_replace_funds() {
    let mut l = fixture();
    let before = l.clone();
    assert_eq!(l.open(AccountId(1), 0), Err(Error::Exists));
    assert_eq!(l, before);
}
#[test]
fn invalid_transfers_leave_no_partial_effects() {
    for mode in 0..5 {
        let mut l = fixture();
        let before = l.clone();
        let (from, to, amount, expires) = match mode {
            0 => (1, 1, 1, 100),
            1 => (1, 2, 0, 100),
            2 => (1, 2, 101, 100),
            3 => (1, 3, 1, 100),
            _ => (1, 2, 1, 1),
        };
        assert!(
            l.reserve(
                TransferId(1),
                AccountId(from),
                AccountId(to),
                amount,
                expires,
                1
            )
            .is_err()
        );
        assert_eq!(l, before);
    }
}
#[test]
fn destination_overflow_rejects_capture_atomically() {
    let mut l = Ledger::default();
    l.open(AccountId(1), 1).unwrap();
    l.open(AccountId(2), u128::MAX).unwrap();
    l.reserve(TransferId(1), AccountId(1), AccountId(2), 1, 100, 1)
        .unwrap();
    let before = l.clone();
    assert_eq!(l.capture(TransferId(1), 2), Err(Error::Overflow));
    assert_eq!(l, before);
    l.void(TransferId(1)).unwrap();
    assert_eq!(l.accounts()[&AccountId(1)].available, 1);
}
#[test]
fn full_u128_amount_has_no_float_conversion() {
    let mut l = Ledger::default();
    l.open(AccountId(1), u128::MAX).unwrap();
    l.open(AccountId(2), 0).unwrap();
    l.reserve(TransferId(1), AccountId(1), AccountId(2), u128::MAX, 100, 1)
        .unwrap();
    l.capture(TransferId(1), 2).unwrap();
    assert_eq!(l.accounts()[&AccountId(2)].available, u128::MAX);
}
#[test]
fn many_holds_cannot_overspend() {
    let mut l = fixture();
    for i in 0..10 {
        l.reserve(TransferId(i), AccountId(1), AccountId(2), 10, 100, 1)
            .unwrap();
    }
    let before = l.clone();
    assert_eq!(
        l.reserve(TransferId(11), AccountId(1), AccountId(2), 1, 100, 1),
        Err(Error::Funds)
    );
    assert_eq!(l, before);
    for i in 0..10 {
        if i % 2 == 0 {
            l.capture(TransferId(i), 2).unwrap();
        } else {
            l.void(TransferId(i)).unwrap();
        }
        conserved(&l);
    }
}
#[test]
fn missing_resolution_is_typed() {
    let mut l = fixture();
    assert_eq!(l.capture(TransferId(99), 1), Err(Error::Missing));
    assert_eq!(l.void(TransferId(99)), Err(Error::Missing));
}
