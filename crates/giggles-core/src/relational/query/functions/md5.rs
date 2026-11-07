//! MD5 adapted from PostgreSQL REL_18_0 src/common/md5.c (WIDE BSD license).
//! https://github.com/postgres/postgres/blob/REL_18_0/src/common/md5.c
//! Source SHA256: 5e0248eab5a3038b4c752875273c159e820e16e8a0f3f3ed4b04baa62cad348a.
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
// Copyright (C) 1995, 1996, 1997, and 1998 WIDE Project.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
// 3. Neither the name of the project nor the names of its contributors
// may be used to endorse or promote products derived from this software
// without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE PROJECT AND CONTRIBUTORS ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL THE PROJECT OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
use super::*;
type Signature = (Vec<Option<DataType>>, DataType);

pub(super) fn signature(
    name: &str,
    count: usize,
    first: Option<&DataType>,
) -> Option<R<Signature>> {
    if name != "md5" {
        return None;
    }
    Some(if count != 1 {
        Err(SqlError::UndefinedFunction)
    } else {
        Ok((
            vec![Some(if first == Some(&DataType::Bytes) {
                DataType::Bytes
            } else {
                DataType::Text
            })],
            DataType::Text,
        ))
    })
}

pub(super) fn eval(name: &str, values: &[Value], budget: &mut usize) -> Option<R<Value>> {
    if name != "md5" {
        return None;
    }
    Some((|| {
        let [value] = values else {
            return Err(SqlError::UndefinedFunction);
        };
        let input = match value {
            Value::Null => return Ok(Value::Null),
            Value::Text(input) => input.as_bytes(),
            Value::Bytes(input) => input.as_slice(),
            _ => return Err(SqlError::UndefinedFunction),
        };
        // Bound the linear scan before doing work. The kernel borrows the input;
        // scalar argument cloning before dispatch is outside this allocation proof.
        if input.len() > MAX_ROW_BYTES {
            return Err(SqlError::Limit);
        }
        reserve(budget, 32)?;
        let state = digest(input);
        let mut output = String::with_capacity(32);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for word in state {
            for byte in word.to_le_bytes() {
                output.push(HEX[usize::from(byte >> 4)] as char);
                output.push(HEX[usize::from(byte & 15)] as char);
            }
        }
        Ok(Value::Text(output))
    })())
}

// PostgreSQL's round constants and schedule, with explicit wrapping u32 math.
const T: [u32; 64] = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x2441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x4881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
];
const SHIFT: [[u32; 4]; 4] = [
    [7, 12, 17, 22],
    [5, 9, 14, 20],
    [4, 11, 16, 23],
    [6, 10, 15, 21],
];
fn block(state: &mut [u32; 4], input: &[u8; 64]) {
    let mut words = [0u32; 16];
    for (word, bytes) in words.iter_mut().zip(input.as_chunks::<4>().0) {
        *word = u32::from_le_bytes(*bytes);
    }
    let [mut a, mut b, mut c, mut d] = *state;
    for (i, constant) in T.iter().enumerate() {
        let (f, index) = match i / 16 {
            0 => ((b & c) | (!b & d), i),
            1 => ((b & d) | (c & !d), (5 * i + 1) % 16),
            2 => (b ^ c ^ d, (3 * i + 5) % 16),
            _ => (c ^ (b | !d), (7 * i) % 16),
        };
        let sum = a
            .wrapping_add(f)
            .wrapping_add(words[index])
            .wrapping_add(*constant);
        (a, b, c, d) = (
            d,
            b.wrapping_add(sum.rotate_left(SHIFT[i / 16][i % 4])),
            b,
            c,
        );
    }
    for (word, value) in state.iter_mut().zip([a, b, c, d]) {
        *word = word.wrapping_add(value);
    }
}
fn digest(input: &[u8]) -> [u32; 4] {
    let mut state = [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476];
    let (blocks, tail) = input.as_chunks::<64>();
    for bytes in blocks {
        block(&mut state, bytes);
    }
    // Only the final partial block is copied; no padded full-input buffer exists.
    let mut final_block = [0u8; 64];
    final_block[..tail.len()].copy_from_slice(tail);
    final_block[tail.len()] = 0x80;
    if tail.len() >= 56 {
        block(&mut state, &final_block);
        final_block.fill(0);
    }
    final_block[56..].copy_from_slice(&(input.len() as u64).wrapping_mul(8).to_le_bytes());
    block(&mut state, &final_block);
    state
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn md5_kernel_budget_and_maximum_input_boundaries() {
        let value = Value::Text("abc".into());
        let mut budget = MAX_RESULT_BYTES - 32;
        assert_eq!(
            eval("md5", std::slice::from_ref(&value), &mut budget)
                .unwrap()
                .unwrap(),
            Value::Text("900150983cd24fb0d6963f7d28e17f72".into())
        );
        assert_eq!(budget, MAX_RESULT_BYTES);
        let mut budget = MAX_RESULT_BYTES - 31;
        let before = budget;
        assert_eq!(
            eval("md5", std::slice::from_ref(&value), &mut budget).unwrap(),
            Err(SqlError::Limit)
        );
        assert_eq!(budget, before);
        let maximum = Value::Bytes(vec![b'a'; MAX_ROW_BYTES]);
        let mut budget = 0;
        assert_eq!(
            eval("md5", &[maximum], &mut budget).unwrap().unwrap(),
            Value::Text("2d61aa54b58c2e94403fb092c3dbc027".into())
        );
        assert_eq!(budget, 32);
        let too_large = Value::Bytes(vec![0; MAX_ROW_BYTES + 1]);
        let before = budget;
        assert_eq!(
            eval("md5", &[too_large], &mut budget).unwrap(),
            Err(SqlError::Limit)
        );
        assert_eq!(budget, before);
        assert_eq!(
            eval("md5", &[Value::Null], &mut budget).unwrap().unwrap(),
            Value::Null
        );
        assert_eq!(budget, before);
    }
}
