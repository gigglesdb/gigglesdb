mod common;
use common::*;
use giggles_outbox::*;
use std::sync::{Arc, Barrier};

#[test]
fn concurrent_claims_one_generation() {
    let (_, o) = db();
    seed(&o, "a");
    let barrier = Arc::new(Barrier::new(5));
    let handles: Vec<_> = (0..4)
        .map(|n| {
            let o = o.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                o.claim("tenant", &format!("owner{n}"), now(0), 10, 1)
            })
        })
        .collect();
    barrier.wait();
    let mut delivered = 0;
    for h in handles {
        match h.join().unwrap() {
            Ok(ClaimOutcome::Committed(v)) => delivered += v.len(),
            Err(Error::Conflict) => (),
            r => panic!("{r:?}"),
        }
    }
    assert_eq!(delivered, 1);
    let s = o.status("tenant", "a").unwrap().unwrap();
    assert_eq!(s.generation, 1);
    assert_eq!(s.attempts, 1);
}
#[test]
fn stale_owner_generation_stream_rejected() {
    let (_, o) = db();
    seed(&o, "a");
    let old = claim(&o, "one", 0).remove(0);
    let new = claim(&o, "two", 10).remove(0);
    assert_eq!(new.token.generation, 2);
    assert_eq!(o.ack(&old.token, now(11)), Err(Error::StaleClaim));
    let mut bad = new.token.clone();
    bad.owner = "other".into();
    assert_eq!(o.ack(&bad, now(11)), Err(Error::StaleClaim));
    bad = new.token.clone();
    bad.stream = "other".into();
    assert_eq!(o.release(&bad, now(11)), Err(Error::StaleClaim));
    bad = new.token.clone();
    bad.generation = 1;
    assert_eq!(o.renew(&bad, now(11), 10), Err(Error::StaleClaim));
    assert_eq!(
        o.ack(&new.token, now(11)).unwrap(),
        CommitOutcome::Committed
    );
    assert_eq!(o.ack(&new.token, now(12)), Err(Error::StaleClaim));
}
#[test]
fn expiry_renewal_boundary() {
    let (_, o) = db();
    seed(&o, "a");
    let d = claim(&o, "one", 0).remove(0);
    assert_eq!(
        o.renew(
            &d.token,
            TrustedTime {
                lower: 8,
                upper: 10
            },
            10
        ),
        Err(Error::StaleClaim)
    );
    o.renew(&d.token, now(9), 10).unwrap();
    assert!(claim(&o, "two", 10).is_empty());
    assert_eq!(o.ack(&d.token, now(19)), Err(Error::StaleClaim));
    let next = claim(&o, "two", 19).remove(0);
    assert_eq!(next.attempt, 2);
}
#[test]
fn release_reclaim_increments_generation() {
    let (_, o) = db();
    seed(&o, "a");
    let old = claim(&o, "one", 0).remove(0);
    o.release(&old.token, now(1)).unwrap();
    let next = claim(&o, "one", 1).remove(0);
    assert_eq!(next.token.generation, 2);
    assert_eq!(o.release(&old.token, now(2)), Err(Error::StaleClaim));
}
#[test]
fn time_regression_uncertainty_overflow() {
    let (_, o) = db();
    seed(&o, "a");
    assert_eq!(
        o.claim("tenant", "o", TrustedTime { lower: 2, upper: 1 }, 10, 1),
        Err(Error::Time)
    );
    assert_eq!(
        o.claim(
            "tenant",
            "o",
            TrustedTime {
                lower: 0,
                upper: 11
            },
            20,
            1
        ),
        Err(Error::Time)
    );
    assert_eq!(
        o.claim("tenant", "o", now(u64::MAX), 1, 1),
        Err(Error::Overflow)
    );
    assert_eq!(o.claim("tenant", "o", now(0), 86_401, 1), Err(Error::Time));
    assert_eq!(o.claim("tenant", "o", now(0), 0, 1), Err(Error::Time));
    assert_eq!(o.claim("tenant", "o", now(0), 1, 0), Err(Error::Limit));
    let d = claim(&o, "o", 10).remove(0);
    assert_eq!(o.release(&d.token, now(9)), Err(Error::Time));
    assert_eq!(o.status("tenant", "a").unwrap().unwrap().attempts, 1);
}
#[test]
fn candidate_order_and_batch_bounds() {
    let (_, o) = db();
    for id in ["z", "a", "m"] {
        seed(&o, id);
    }
    let ClaimOutcome::Committed(ds) = o.claim("tenant", "o", now(0), 10, 2).unwrap() else {
        panic!()
    };
    assert_eq!(
        ds.iter()
            .map(|d| d.token.effect_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "m"]
    );
    assert_eq!(claim(&o, "other", 0)[0].token.effect_id, "z");
    assert_eq!(o.claim("tenant", "o", now(0), 10, 33), Err(Error::Limit));
    assert_eq!(o.reconcile().unwrap().claimed, 3);
}

// Independent reference: no production state, codec, digest or candidate helper.
#[derive(Clone, Debug)]
enum ModelPhase {
    Ready,
    Held { owner: String, until: u64 },
    Done,
}
#[derive(Clone, Debug)]
struct Model {
    phase: ModelPhase,
    count: u64,
}
fn model_claim(model: &mut [Model], owner: &str, t: u64, cap: usize) -> Vec<(usize, u64)> {
    let mut out = vec![];
    for (n, m) in model.iter_mut().enumerate() {
        let eligible = match m.phase {
            ModelPhase::Ready => true,
            ModelPhase::Held { until, .. } => t >= until,
            ModelPhase::Done => false,
        };
        if eligible {
            m.count += 1;
            m.phase = ModelPhase::Held {
                owner: owner.into(),
                until: t + 10,
            };
            out.push((n, m.count));
            if out.len() == cap {
                break;
            }
        }
    }
    out
}
fn model_resolve(m: &mut Model, owner: &str, g: u64, t: u64, action: u64) -> bool {
    if let ModelPhase::Held { owner: o, until } = &mut m.phase {
        if owner != o || m.count != g || t >= *until {
            return false;
        }
        match action {
            0 => m.phase = ModelPhase::Done,
            1 => m.phase = ModelPhase::Ready,
            _ => *until = (*until).max(t + 10),
        };
        true
    } else {
        false
    }
}
#[test]
fn randomized_reference_histories() {
    for seed_num in 1u64..=4 {
        let (_, o) = db();
        for n in 0..6 {
            seed(&o, &format!("e{n}"));
        }
        let mut model = vec![
            Model {
                phase: ModelPhase::Ready,
                count: 0
            };
            6
        ];
        let mut tokens = vec![];
        let mut rng = seed_num;
        let mut t = 0;
        for _step in 0..160 {
            rng = rng.wrapping_mul(6364136223846793005u64).wrapping_add(1);
            t += rng % 3;
            let owner = if rng & 8 == 0 { "a" } else { "b" };
            if rng % 4 == 0 || tokens.is_empty() {
                let cap = (rng % 3 + 1) as usize;
                let expected = model_claim(&mut model, owner, t, cap);
                let ClaimOutcome::Committed(ds) =
                    o.claim("tenant", owner, now(t), 10, cap).unwrap()
                else {
                    panic!()
                };
                assert_eq!(
                    ds.iter()
                        .map(|d| (
                            d.token.effect_id[1..].parse::<usize>().unwrap(),
                            d.token.generation
                        ))
                        .collect::<Vec<_>>(),
                    expected
                );
                tokens.extend(ds.into_iter().map(|d| d.token));
            } else {
                let tok = &tokens[(rng as usize) % tokens.len()];
                let n = tok.effect_id[1..].parse::<usize>().unwrap();
                let action = (rng / 7) % 3;
                let expected = model_resolve(&mut model[n], &tok.owner, tok.generation, t, action);
                let actual = match action {
                    0 => o.ack(tok, now(t)),
                    1 => o.release(tok, now(t)),
                    _ => o.renew(tok, now(t), 10),
                };
                assert_eq!(
                    actual.is_ok(),
                    expected,
                    "seed={seed_num} t={t} token={tok:?}"
                );
            }
            for (n, m) in model.iter().enumerate() {
                let s = o.status("tenant", &format!("e{n}")).unwrap().unwrap();
                assert_eq!(s.generation, m.count);
                assert_eq!(s.attempts, m.count);
                match (&m.phase, &s.phase) {
                    (ModelPhase::Ready, Phase::Ready) | (ModelPhase::Done, Phase::Acked) => (),
                    (ModelPhase::Held { owner, until }, Phase::Claimed { owner: o, deadline }) => {
                        assert_eq!(owner, o);
                        assert_eq!(until, deadline)
                    }
                    other => panic!("{other:?}"),
                }
            }
        }
    }
}
#[test]
fn negative_control_without_generation_fence_diverges() {
    let mut correct = Model {
        phase: ModelPhase::Held {
            owner: "same".into(),
            until: 20,
        },
        count: 2,
    };
    assert!(!model_resolve(&mut correct, "same", 1, 11, 0));
    let broken_accepts_stale =
        matches!(correct.phase,ModelPhase::Held{ref owner,until}if owner=="same"&&11<until);
    assert!(
        broken_accepts_stale,
        "negative control must expose omitted generation check"
    );
}
#[test]
fn concurrent_expiry_and_renewal_never_validate_two_generations() {
    let (_, o) = db();
    seed(&o, "a");
    let old = claim(&o, "old", 0).remove(0);
    let barrier = Arc::new(Barrier::new(3));
    let renew = {
        let o = o.clone();
        let b = barrier.clone();
        let token = old.token.clone();
        std::thread::spawn(move || {
            b.wait();
            o.renew(&token, now(9), 10)
        })
    };
    let reclaim = {
        let o = o.clone();
        let b = barrier.clone();
        std::thread::spawn(move || {
            b.wait();
            o.claim("tenant", "new", now(10), 10, 1)
        })
    };
    barrier.wait();
    let renewed = renew.join().unwrap();
    let reclaimed = reclaim.join().unwrap();
    let current = o.status("tenant", "a").unwrap().unwrap();
    match current.generation {
        1 => {
            assert_eq!(renewed, Ok(CommitOutcome::Committed));
            match reclaimed {
                Err(Error::Conflict) => (),
                Ok(ClaimOutcome::Committed(v)) => assert!(v.is_empty()),
                other => panic!("{other:?}"),
            }
        }
        2 => {
            assert!(matches!(
                renewed,
                Err(Error::Conflict | Error::Time | Error::StaleClaim)
            ));
            assert!(matches!(reclaimed,Ok(ClaimOutcome::Committed(ref v))if v.len()==1));
            assert_eq!(o.ack(&old.token, now(11)), Err(Error::StaleClaim));
        }
        other => panic!("unexpected generation {other}"),
    }
}
