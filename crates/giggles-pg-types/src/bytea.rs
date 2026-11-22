//! Independent PostgreSQL REL_18_0 byteain/hex_decode_safe semantics.
//! Source references: varlena.c:298 and encode.c:217; PostgreSQL license retained.
//! Input is valid UTF8; NUL admission belongs to the outer text domain.
//!
//! Validation finishes before the caller output limit is checked. Preparation
//! borrows the input and allocates nothing. Decoding requests one exact reserve;
//! an allocator may grant additional capacity. No PostgreSQL maximum is imposed.
//! Callers choose output policy and map errors at their own domain boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Input syntax, outer-domain admission, and caller resource failures.
pub enum ByteaInputError {
    HexInvalidDigit,
    HexOddDigits,
    EscapeSyntax,
    NulInput,
    OutputLimit,
    Allocation,
}
#[derive(Debug)]
/// Immutable validated input and exact decoded byte count.
pub struct ByteaTextPlan<'a> {
    input: &'a str,
    decoded_len: usize,
}
fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
fn scan(input: &str, mut emit: impl FnMut(u8)) -> Result<usize, ByteaInputError> {
    use ByteaInputError::*;
    let b = input.as_bytes();
    if b.contains(&0) {
        return Err(NulInput);
    }
    let mut i = if b.starts_with(b"\\x") { 2 } else { 0 };
    let is_hex = i == 2;
    let mut n = 0usize;
    while i < b.len() {
        let v;
        if is_hex {
            if matches!(b[i], b' ' | b'\n' | b'\t' | b'\r') {
                i += 1;
                continue;
            }
            let h = hex(b[i]).ok_or(HexInvalidDigit)?;
            i += 1;
            if i == b.len() {
                return Err(HexOddDigits);
            }
            let l = hex(b[i]).ok_or(HexInvalidDigit)?;
            i += 1;
            v = (h << 4) | l;
        } else if b[i] != b'\\' {
            v = b[i];
            i += 1
        } else if b.get(i + 1) == Some(&b'\\') {
            v = b'\\';
            i += 2
        } else if i + 3 < b.len()
            && matches!(b[i + 1], b'0'..=b'3')
            && matches!(b[i + 2], b'0'..=b'7')
            && matches!(b[i + 3], b'0'..=b'7')
        {
            v = (b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0');
            i += 4;
        } else {
            return Err(EscapeSyntax);
        }
        n = n.checked_add(1).ok_or(OutputLimit)?;
        emit(v);
    }
    Ok(n)
}
/// Validate and count without allocating, then enforce the caller output limit.
pub fn prepare_bytea_text(
    input: &str,
    max_output_bytes: usize,
) -> Result<ByteaTextPlan<'_>, ByteaInputError> {
    let decoded_len = scan(input, |_| {})?;
    if decoded_len > max_output_bytes {
        return Err(ByteaInputError::OutputLimit);
    }
    Ok(ByteaTextPlan { input, decoded_len })
}
impl ByteaTextPlan<'_> {
    /// Exact decoded length, suitable for caller budget admission.
    pub fn decoded_len(&self) -> usize {
        self.decoded_len
    }
    /// Reserve once and decode; allocation failure returns no partial output.
    pub fn decode(self) -> Result<Vec<u8>, ByteaInputError> {
        let mut out = Vec::new();
        out.try_reserve_exact(self.decoded_len)
            .map_err(|_| ByteaInputError::Allocation)?;
        scan(self.input, |v| out.push(v))?;
        debug_assert_eq!(out.len(), self.decoded_len);
        Ok(out)
    }
}

/// PostgreSQL's bytea output setting, independent of the encode() function.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ByteaOutput {
    #[default]
    Hex,
    Escape,
}

impl ByteaOutput {
    pub fn name(self) -> &'static str {
        match self {
            Self::Hex => "hex",
            Self::Escape => "escape",
        }
    }

    pub fn text_len(self, bytes: &[u8]) -> usize {
        match self {
            Self::Hex => bytes.len().saturating_mul(2).saturating_add(2),
            Self::Escape => bytes.iter().fold(0usize, |len, &byte| {
                len.saturating_add(match byte {
                    b'\\' => 2,
                    b' '..=b'~' => 1,
                    _ => 4,
                })
            }),
        }
    }

    pub fn format(self, bytes: &[u8]) -> String {
        let mut text = String::with_capacity(self.text_len(bytes));
        self.append(bytes, &mut text);
        text
    }

    fn append(self, bytes: &[u8], text: &mut String) {
        match self {
            Self::Hex => {
                const DIGITS: &[u8] = b"0123456789abcdef";
                text.push_str("\\x");
                for &byte in bytes {
                    text.push(char::from(DIGITS[usize::from(byte >> 4)]));
                    text.push(char::from(DIGITS[usize::from(byte & 15)]));
                }
            }
            Self::Escape => {
                for &byte in bytes {
                    match byte {
                        b'\\' => text.push_str("\\\\"),
                        b' '..=b'~' => text.push(char::from(byte)),
                        _ => {
                            text.push('\\');
                            text.push(char::from(b'0' + (byte >> 6)));
                            text.push(char::from(b'0' + ((byte >> 3) & 7)));
                            text.push(char::from(b'0' + (byte & 7)));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod output_tests {
    use super::*;

    #[test]
    fn bytea_output_escape_uses_octal_outside_printable_ascii() {
        let bytes = [0, 255, b'\\', b'A', b'\'', b'\n', 127, 128];
        assert_eq!(ByteaOutput::Escape.format(&bytes), r"\000\377\\A'\012\177\200");
        assert_eq!(ByteaOutput::Hex.format(&bytes), r"\x00ff5c41270a7f80");
        assert_eq!(ByteaOutput::Escape.format(b""), "");
        assert_eq!(ByteaOutput::Hex.format(b""), r"\x");
        let all: Vec<u8> = (0..=255).collect();
        for output in [ByteaOutput::Escape, ByteaOutput::Hex] {
            let text = output.format(&all);
            assert_eq!(text.len(), output.text_len(&all));
            assert_eq!(prepare_bytea_text(&text, all.len()).unwrap().decode().unwrap(), all);
        }
    }
}
