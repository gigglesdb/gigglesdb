use super::*;

fn coalesce(arguments: Vec<Bound>) -> Bound {
    Bound::Call {
        function: ScalarFunction::Coalesce,
        arguments,
    }
}

#[test]
fn coalesce_preserves_borrowed_payload_until_output_admission() {
    // These are stored-row or already-admitted parameter payloads. Selection
    // must not create a new large value before RETURN/projection budgets run.
    let payload_bytes = MAX_BOUND_VALUE_BYTES - 2 * value_bytes(&Value::Null);
    for value in [
        Value::Text("x".repeat(payload_bytes)),
        Value::Bytes(vec![255; payload_bytes]),
    ] {
        let row = [value];
        let from_row = coalesce(vec![Bound::Value(Value::Null), Bound::Field(0)]);
        {
            let selected = from_row.eval(&row).unwrap();
            assert!(matches!(selected, Cow::Borrowed(_)));
            assert!(std::ptr::eq(selected.as_ref(), &row[0]));
        }

        // A nested all-NULL branch and a frozen value take the same borrowed
        // path, including opaque bytes. This is also how admitted block-local
        // scalar leaves are represented after local materialization.
        let from_bound = coalesce(vec![
            coalesce(vec![Bound::Value(Value::Null), Bound::Value(Value::Null)]),
            Bound::Value(row.into_iter().next().unwrap()),
        ]);
        let Bound::Call { arguments, .. } = &from_bound else {
            unreachable!()
        };
        let Bound::Value(expected) = &arguments[1] else {
            unreachable!()
        };
        let selected = from_bound.eval(&[]).unwrap();
        assert!(matches!(selected, Cow::Borrowed(_)));
        assert!(std::ptr::eq(selected.as_ref(), expected));
    }
}
