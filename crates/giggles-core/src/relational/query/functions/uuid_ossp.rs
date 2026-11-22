//! uuid-ossp scalar routines. Catalog binding controls visibility.
use super::*;
const PREFIX: &str = "\0uuid-ossp:";
pub(in crate::relational::query) fn known(name: &str) -> bool {
    matches!(
        name,
        "uuid_nil"
            | "uuid_ns_dns"
            | "uuid_ns_url"
            | "uuid_ns_oid"
            | "uuid_ns_x500"
            | "uuid_generate_v1"
            | "uuid_generate_v1mc"
            | "uuid_generate_v3"
            | "uuid_generate_v4"
            | "uuid_generate_v5"
    )
}
pub(in crate::relational::query) fn resolve_name(
    name: &mut String,
    state: &Relational,
    path: &[String],
) {
    let Some(raw) = name.strip_prefix(PREFIX) else {
        return;
    };
    let (schema, routine) = raw.split_once('\0').unwrap_or(("", raw));
    let visible = state.ext.extensions.get("uuid-ossp").is_some_and(|e| {
        if schema.is_empty() {
            e.schema == "pg_catalog" || path.contains(&e.schema)
        } else {
            schema == e.schema
        }
    });
    *name = if visible {
        routine.into()
    } else {
        format!("{UNDEFINED_ROUTINE_PREFIX}{routine}")
    };
}
pub(super) fn signature(name: &str, count: usize) -> Option<R<(Vec<Option<DataType>>, DataType)>> {
    if !known(name) {
        return None;
    }
    let args = if matches!(name, "uuid_generate_v3" | "uuid_generate_v5") {
        vec![Some(DataType::Uuid), Some(DataType::Text)]
    } else {
        vec![]
    };
    Some(if args.len() == count {
        Ok((args, DataType::Uuid))
    } else {
        Err(SqlError::UndefinedFunction)
    })
}
pub(super) fn volatile_name(name: &str) -> bool {
    let raw = name.strip_prefix(PREFIX).unwrap_or(name);
    let routine = raw.rsplit('\0').next().unwrap_or(raw);
    matches!(
        routine,
        "uuid_generate_v1" | "uuid_generate_v1mc" | "uuid_generate_v4"
    )
}
pub(super) fn immutable(name: &str) -> Option<bool> {
    known(name).then_some(!matches!(
        name,
        "uuid_generate_v1" | "uuid_generate_v1mc" | "uuid_generate_v4"
    ))
}
pub(super) fn eval(name: &str, values: &[Value]) -> Option<R<Value>> {
    if !known(name) {
        return None;
    }
    Some((|| {
        if values.contains(&Value::Null) {
            return Ok(Value::Null);
        }
        let mut bytes = [0u8; 16];
        match name {
            "uuid_nil" => {}
            "uuid_ns_dns" | "uuid_ns_url" | "uuid_ns_oid" | "uuid_ns_x500" => {
                bytes = [
                    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0, 0xc0, 0x4f,
                    0xd4, 0x30, 0xc8,
                ];
                bytes[3] = match name {
                    "uuid_ns_url" => 0x11,
                    "uuid_ns_oid" => 0x12,
                    "uuid_ns_x500" => 0x14,
                    _ => 0x10,
                };
            }
            "uuid_generate_v3" | "uuid_generate_v5" => {
                let [Value::Text(ns), Value::Text(text)] = values else {
                    return Err(SqlError::UndefinedFunction);
                };
                let digits = ns.replace('-', "");
                if digits.len() != 32 {
                    return Err(SqlError::Type);
                }
                let mut input = (0..32)
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&digits[i..i + 2], 16).map_err(|_| SqlError::Type))
                    .collect::<R<Vec<_>>>()?;
                input.extend_from_slice(text.as_bytes());
                if name == "uuid_generate_v3" {
                    for (chunk, word) in bytes
                        .as_chunks_mut::<4>()
                        .0
                        .iter_mut()
                        .zip(md5::digest(&input))
                    {
                        chunk.copy_from_slice(&word.to_le_bytes());
                    }
                } else {
                    bytes.copy_from_slice(&sha1(&input)[..16]);
                }
                bytes[6] = (bytes[6] & 15)
                    | if name == "uuid_generate_v3" {
                        0x30
                    } else {
                        0x50
                    };
                bytes[8] = (bytes[8] & 63) | 128;
            }
            "uuid_generate_v4" => return v4(),
            _ => {
                bytes = time_uuid(name == "uuid_generate_v1mc")?;
            }
        }
        Ok(format_uuid(bytes))
    })())
}
pub(super) fn v4() -> R<Value> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| SqlError::Unsupported)?;
    bytes[6] = (bytes[6] & 15) | 0x40;
    bytes[8] = (bytes[8] & 63) | 0x80;
    Ok(format_uuid(bytes))
}

pub(super) fn format_uuid(bytes: [u8; 16]) -> Value {
    let digits = u128::from_be_bytes(bytes);
    Value::Text(format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (digits >> 96) as u32,
        (digits >> 80) as u16,
        (digits >> 64) as u16,
        (digits >> 48) as u16,
        digits & 0xffff_ffff_ffff
    ))
}
fn time_uuid(fresh_node: bool) -> R<[u8; 16]> {
    use std::sync::{Mutex, OnceLock};
    static CLOCK: OnceLock<Mutex<(u64, u16, [u8; 6])>> = OnceLock::new();
    let mut state = CLOCK
        .get_or_init(|| Mutex::new((0, 0, [0; 6])))
        .lock()
        .map_err(|_| SqlError::Unsupported)?;
    if state.0 == 0 {
        let mut random = [0; 8];
        getrandom::fill(&mut random).map_err(|_| SqlError::Unsupported)?;
        state.1 = u16::from_be_bytes([random[0], random[1]]) & 0x3fff;
        state.2.copy_from_slice(&random[2..]);
        state.2[0] |= 1;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| SqlError::Unsupported)?
        .as_nanos()
        / 100
        + 0x01b21dd213814000;
    let now = u64::try_from(now).map_err(|_| SqlError::Overflow)?;
    if now <= state.0 {
        state.1 = (state.1 + 1) & 0x3fff;
    }
    state.0 = now.max(state.0 + 1);
    let timestamp = state.0;
    let mut node = state.2;
    if fresh_node {
        getrandom::fill(&mut node).map_err(|_| SqlError::Unsupported)?;
        node[0] |= 1;
    }
    let mut bytes = [0; 16];
    bytes[..4].copy_from_slice(&(timestamp as u32).to_be_bytes());
    bytes[4..6].copy_from_slice(&((timestamp >> 32) as u16).to_be_bytes());
    bytes[6..8].copy_from_slice(&(((timestamp >> 48) as u16 & 0xfff) | 0x1000).to_be_bytes());
    bytes[8..10].copy_from_slice(&(state.1 | 0x8000).to_be_bytes());
    bytes[10..].copy_from_slice(&node);
    Ok(bytes)
}
fn sha1(input: &[u8]) -> [u8; 20] {
    let mut data = input.to_vec();
    let bits = (data.len() as u64) * 8;
    data.push(128);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bits.to_be_bytes());
    let mut h = [
        0x67452301u32,
        0xefcdab89,
        0x98badcfe,
        0x10325476,
        0xc3d2e1f0,
    ];
    for block in data.as_chunks::<64>().0 {
        let mut w = [0u32; 80];
        for (i, b) in block.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*b);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, word) in w.into_iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5a827999),
                20..=39 => (b ^ c ^ d, 0x6ed9eba1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1bbcdc),
                _ => (b ^ c ^ d, 0xca62c1d6),
            };
            let t = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = t;
        }
        for (v, x) in h.iter_mut().zip([a, b, c, d, e]) {
            *v = v.wrapping_add(x);
        }
    }
    let mut out = [0; 20];
    for (b, w) in out.as_chunks_mut::<4>().0.iter_mut().zip(h) {
        b.copy_from_slice(&w.to_be_bytes());
    }
    out
}

pub(super) fn missing_diagnostic(
    name: &str, args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef], parameters: &[Option<DataType>],
) -> R<()> {
    if let Some(routine) = name.strip_prefix(UNDEFINED_ROUTINE_PREFIX)
        && known(routine)
    {
        let types = args.iter().map(|arg| {
            if matches!(arg, Scalar::Literal(Value::Text(_) | Value::Null)) {
                Ok("unknown")
            } else {
                arg.bind(resolve, tables, parameters).map(|b| typeof_expr::name(&b.ty))
            }
        }).collect::<R<Vec<_>>>()?;
        user_types::view_replace_error(format!("function {routine}({}) does not exist", types.join(", ")));
        user_types::sql_error_hint(
            "No function matches the given name and argument types. You might need to add explicit type casts.",
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn namespace_hash_oracles() {
        use super::*;
        let dns = eval("uuid_ns_dns", &[]).unwrap().unwrap();
        assert_eq!(
            eval(
                "uuid_generate_v3",
                &[dns.clone(), Value::Text("example.com".into())]
            )
            .unwrap()
            .unwrap(),
            Value::Text("9073926b-929f-31c2-abc9-fad77ae3e8eb".into())
        );
        assert_eq!(
            eval(
                "uuid_generate_v5",
                &[dns, Value::Text("example.com".into())]
            )
            .unwrap()
            .unwrap(),
            Value::Text("cfbff0d1-9375-5685-968c-48ce8b15ae17".into())
        );
    }
    #[test]
    fn sha1_fips180_vectors() {
        for (input, expected) in [
            (Vec::new(), "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            (b"abc".to_vec(), "a9993e364706816aba3e25717850c26c9cd0d89d"),
            (
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq".to_vec(),
                "84983e441c3bd26ebaae4aa1f95129e5e54670f1",
            ),
            (
                vec![b'a'; 1_000_000],
                "34aa973cd4c4daa4f61eeb2bdbad27316534016f",
            ),
        ] {
            let actual = super::sha1(&input)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            assert_eq!(actual, expected);
        }
    }
}
