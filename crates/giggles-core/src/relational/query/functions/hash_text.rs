//! Jenkins lookup3, matching PostgreSQL's hash_bytes on little-endian targets.
// Adapted from PostgreSQL 18 src/common/hashfn.c.
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
// Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
use super::*;

type Signature = (Vec<Option<DataType>>, DataType);

pub(super) fn known(name: &str) -> bool {
    matches!(name, "hashtext" | "hashtextextended")
}

pub(super) fn signature(name: &str, count: usize) -> Option<R<Signature>> {
    Some(match (name, count) {
        ("hashtext", 1) => Ok((vec![Some(DataType::Text)], DataType::Integer)),
        ("hashtextextended", 2) => Ok((
            vec![Some(DataType::Text), Some(DataType::BigInt)],
            DataType::BigInt,
        )),
        _ if known(name) => Err(SqlError::UndefinedFunction),
        _ => return None,
    })
}

pub(super) fn bind(name: &str, source: &[Scalar], mut args: Vec<BoundScalar>) -> R<BoundScalar> {
    let undefined = || {
        let types: Vec<_> = source
            .iter()
            .zip(&args)
            .map(|(source, arg)| {
                if matches!(source, Scalar::Literal(Value::Text(_) | Value::Null)) {
                    "unknown"
                } else {
                    typeof_expr::name(&arg.ty)
                }
            })
            .collect();
        user_types::view_replace_error(format!(
            "function {name}({}) does not exist",
            types.join(", ")
        ));
        user_types::sql_error_hint(
            "No function matches the given name and argument types. You might need to add explicit type casts.",
        );
        SqlError::UndefinedFunction
    };
    let (_, ty) = signature(name, args.len())
        .and_then(Result::ok)
        .ok_or_else(undefined)?;
    if source
        .iter()
        .zip(&args)
        .enumerate()
        .any(|(index, (source, arg))| {
            let accepts_type = if index == 0 {
                matches!(
                    arg.ty,
                    DataType::Text | DataType::Varchar | DataType::Bpchar
                )
            } else {
                integer(&arg.ty)
            };
            !accepts_type && !matches!(source, Scalar::Literal(Value::Text(_) | Value::Null))
        })
    {
        return Err(undefined());
    }
    for (index, (source, arg)) in source.iter().zip(&mut args).enumerate() {
        if index == 0 {
            if arg.ty == DataType::Bpchar {
                let source = std::mem::replace(
                    arg,
                    BoundScalar {
                        ty: DataType::Text,
                        node: Node::Literal(Value::Null),
                    },
                );
                arg.node = cast_node(source, &DataType::Text);
            } else {
                arg.ty = DataType::Text;
            }
        } else if let Scalar::Literal(value @ (Value::Text(_) | Value::Null)) = source {
            arg.ty = DataType::BigInt;
            arg.node = literal_cast_node(value.clone(), &DataType::BigInt)?;
        }
    }
    collation::derive(args.iter())?;
    Ok(BoundScalar {
        ty,
        node: Node::Call(name.into(), args),
    })
}

pub(super) fn eval(args: &[BoundScalar], values: &[Value], budget: &mut usize) -> R<Value> {
    let (input, seed, extended) = match values {
        [Value::Text(input)] => (input, 0, false),
        [Value::Text(input), Value::Int(seed)] => (input, *seed as u64, true),
        _ => return Err(SqlError::UndefinedFunction),
    };
    collation::check_hash(&args[0])?;
    if input.len() > crate::relational::max_value_bytes() {
        return Err(SqlError::Limit);
    }
    let (high, low) = hash(input.as_bytes(), seed)?;
    let result = Value::Int(if extended {
        (u64::from(high) << 32 | u64::from(low)) as i64
    } else {
        i64::from(low as i32)
    });
    charge(budget, &result)?;
    Ok(result)
}

fn mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    a = a.wrapping_sub(c) ^ c.rotate_left(4);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a) ^ a.rotate_left(6);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b) ^ b.rotate_left(8);
    b = b.wrapping_add(a);
    a = a.wrapping_sub(c) ^ c.rotate_left(16);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a) ^ a.rotate_left(19);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b) ^ b.rotate_left(4);
    b = b.wrapping_add(a);
    (a, b, c)
}

fn hash(bytes: &[u8], seed: u64) -> R<(u32, u32)> {
    let mut a = 0x9e3779b9u32
        .wrapping_add(bytes.len() as u32)
        .wrapping_add(3923095);
    let mut b = a;
    let mut c = a;
    if seed != 0 {
        (a, b, c) = mix(
            a.wrapping_add((seed >> 32) as u32),
            b.wrapping_add(seed as u32),
            c,
        );
    }
    let (chunks, remainder) = bytes.as_chunks::<12>();
    for (index, chunk) in chunks.iter().enumerate() {
        if index % 4096 == 0 {
            crate::cancellation::check().map_err(|_| SqlError::QueryCanceled)?;
        }
        let word = |i| u32::from_le_bytes(chunk[i..i + 4].try_into().unwrap());
        (a, b, c) = mix(
            a.wrapping_add(word(0)),
            b.wrapping_add(word(4)),
            c.wrapping_add(word(8)),
        );
    }
    let mut tail = [0; 12];
    tail[..remainder.len()].copy_from_slice(remainder);
    a = a.wrapping_add(u32::from_le_bytes(tail[..4].try_into().unwrap()));
    b = b.wrapping_add(u32::from_le_bytes(tail[4..8].try_into().unwrap()));
    // PostgreSQL leaves the low byte of the last word for the length.
    c = c.wrapping_add(u32::from_le_bytes(tail[8..].try_into().unwrap()) << 8);
    c = (c ^ b).wrapping_sub(b.rotate_left(14));
    a = (a ^ c).wrapping_sub(c.rotate_left(11));
    b = (b ^ a).wrapping_sub(a.rotate_left(25));
    c = (c ^ b).wrapping_sub(b.rotate_left(16));
    a = (a ^ c).wrapping_sub(c.rotate_left(4));
    b = (b ^ a).wrapping_sub(a.rotate_left(14));
    c = (c ^ b).wrapping_sub(b.rotate_left(24));
    Ok((b, c))
}
