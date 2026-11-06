use super::*;
use std::cell::Cell;

#[test]
fn simple_case_evaluates_operand_once_and_borrows_selected_payload() {
    for payload in [
        Value::Text("x".repeat(crate::relational::MAX_ROW_BYTES - 27)),
        Value::Bytes(vec![255; crate::relational::MAX_ROW_BYTES - 27]),
    ] {
        let row = [Value::Int(3), Value::Null, payload, Value::Null];
        let calls: [Cell<usize>; 4] = std::array::from_fn(|_| Cell::new(0));
        let expression = Bound::Case {
            operand: Some(Box::new(Bound::Field(0))),
            arms: vec![
                (Bound::Value(Value::Int(1)), Bound::Field(1)),
                (Bound::Value(Value::Int(2)), Bound::Field(1)),
                (Bound::Value(Value::Int(3)), Bound::Field(2)),
            ],
            else_expr: Some(Box::new(Bound::Field(3))),
        };
        let selected = expression
            .eval_fields(&|index| {
                calls[index].set(calls[index].get() + 1);
                &row[index]
            })
            .unwrap();
        assert_eq!(calls.map(|count| count.get()), [1, 0, 1, 0]);
        assert!(matches!(selected, Cow::Borrowed(_)));
        assert!(std::ptr::eq(selected.as_ref(), &row[2]));
    }
}

#[test]
fn case_null_comparison_and_searched_selection_keep_lazy_field_access() {
    let row = [Value::Int(4)];
    let when_calls = Cell::new(0);
    let expression = Bound::Case {
        operand: Some(Box::new(Bound::Value(Value::Null))),
        arms: vec![
            (Bound::Value(Value::Null), Bound::Value(Value::Int(1))),
            (Bound::Field(0), Bound::Value(Value::Int(2))),
        ],
        else_expr: Some(Box::new(Bound::Value(Value::Int(3)))),
    };
    let selected = expression
        .eval_fields(&|index| {
            when_calls.set(when_calls.get() + 1);
            &row[index]
        })
        .unwrap();
    assert_eq!(*selected, Value::Int(3));
    // NULL must not match the first NULL arm, or skip evaluating later WHENs.
    assert_eq!(when_calls.get(), 1);

    let row = [
        Value::Text("selected".into()),
        Value::Null,
        Value::Bool(false),
    ];
    let calls: [Cell<usize>; 3] = std::array::from_fn(|_| Cell::new(0));
    let searched = Bound::Case {
        operand: None,
        arms: vec![
            (Bound::Value(Value::Bool(false)), Bound::Field(1)),
            (Bound::Value(Value::Bool(true)), Bound::Field(0)),
            (Bound::Field(2), Bound::Field(1)),
        ],
        else_expr: Some(Box::new(Bound::Field(1))),
    };
    let selected = searched
        .eval_fields(&|index| {
            calls[index].set(calls[index].get() + 1);
            &row[index]
        })
        .unwrap();
    assert_eq!(calls.map(|count| count.get()), [1, 0, 0]);
    assert!(matches!(selected, Cow::Borrowed(_)));
    assert!(std::ptr::eq(selected.as_ref(), &row[0]));
}
