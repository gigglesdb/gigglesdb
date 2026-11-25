//! SQL ordering for flat arrays stored in the existing text representation.
use super::*;

pub(super) fn compare(ty: &DataType, collation: Option<&str>, a: &Value, b: &Value) -> R<Ordering> {
    let order = logical_types::compare_arrays(a, b, |a, b| match ty {
        DataType::IntArray | DataType::BigIntArray => {
            // Both integer array widths fit in i64. Values are canonicalized
            // at cast, storage and pgwire input boundaries.
            let integer = |s: &str| {
                s.parse::<i64>()
                    .map_err(|_| SqlError::InvalidTextRepresentation)
            };
            Ok(integer(a)?.cmp(&integer(b)?))
        }
        // Canonical UUID hex has fixed separators and sorts in byte order.
        DataType::UuidArray => Ok(a.cmp(b)),
        DataType::TextArray => Ok(super::collation::compare_text(collation, a, b)),
        DataType::ExpressionArray(array) => {
            if *array == ExpressionArray::Varchar {
                Ok(super::collation::compare_text(collation, a, b))
            } else {
                let a = array_aggregate::decode_element(a, *array)?;
                let b = array_aggregate::decode_element(b, *array)?;
                if *array == ExpressionArray::Jsonb {
                    array_aggregate::compare_jsonb(&a, &b)
                } else {
                    Ok(cmp(&a, &b))
                }
            }
        }
        _ => unreachable!("array comparison requires an array type"),
    })?;
    // Keep byte identity for uncollated arrays if an internal caller passes
    // an unnormalized value. Canonical arrays never need this tie-break.
    Ok(if collation.is_none() && !matches!(ty, DataType::ExpressionArray(_)) {
        order.then_with(|| cmp(a, b))
    } else {
        order
    })
}
