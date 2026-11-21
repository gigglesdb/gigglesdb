use crate::MAX_FRAME;

use super::*;

fn client_decoder() -> FrameDecoder {
    FrameDecoder::new(Direction::ClientToServer, MAX_FRAME)
}

fn execute_frame(request_id: u32, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_frame(
        &mut bytes,
        FrameKind::Execute,
        Flags::default(),
        request_id,
        body,
    );
    bytes
}

fn decode_error(decoder: &mut FrameDecoder, bytes: &[u8]) -> FrameError {
    decoder.read_buf().extend_from_slice(bytes);
    let capacity = decoder.read_buf().capacity();
    let error = decoder.next_frame().unwrap_err();
    assert_eq!(decoder.read_buf().capacity(), capacity);
    assert_eq!(decoder.next_frame(), Ok(None));
    error
}

#[test]
fn header_round_trip() {
    let bytes = execute_frame(7, b"abc");
    assert_eq!(bytes.len(), 15);
    assert_eq!(&bytes[..4], &[0, 0, 0, 11]);
    assert_eq!(bytes[4], 4);
    assert_eq!(bytes[5], 0);
    assert_eq!(&bytes[6..8], &[0, 0]);
    assert_eq!(&bytes[8..12], &[0, 0, 0, 7]);

    let mut decoder = client_decoder();
    decoder.read_buf().extend_from_slice(&bytes);
    let (header, range) = decoder.next_frame().unwrap().unwrap();
    assert_eq!(
        header,
        FrameHeader {
            kind: FrameKind::Execute,
            flags: Flags::default(),
            request_id: 7,
            body_len: 3,
        }
    );
    assert_eq!(decoder.body(range), b"abc");

    #[derive(Clone, PartialEq, prost::Message)]
    struct Message {
        #[prost(uint32, tag = "1")]
        value: u32,
    }

    let flags = Flags(Flags::MORE | Flags::SKIPPED);
    let mut encoded = bytes;
    encode_message(
        &mut encoded,
        FrameKind::Reply,
        flags,
        7,
        &Message { value: 42 },
    );
    let mut expected = execute_frame(7, b"abc");
    encode_frame(&mut expected, FrameKind::Reply, flags, 7, &[8, 42]);
    assert_eq!(encoded, expected);
}

#[test]
fn preface_accepts_v1_and_rejects_others() {
    let mut bytes = Vec::new();
    write_preface(&mut bytes);
    assert_eq!(bytes, b"GGLN\0\x01\0\0");
    assert_eq!(
        parse_preface(bytes.as_slice().try_into().unwrap()),
        Ok((1, 0))
    );
    assert_eq!(
        parse_preface(b"GGLX\0\x01\0\0"),
        Err(PrefaceError::BadMagic)
    );
    assert_eq!(
        parse_preface(b"GGLN\0\x02\0\0"),
        Err(PrefaceError::UnsupportedVersion { major: 2, minor: 0 })
    );
    assert_eq!(parse_preface(b"GGLN\0\x01\0\x05"), Ok((1, 5)));
}

#[test]
fn decoder_needs_more_on_partial() {
    let bytes = execute_frame(7, b"abc");
    let mut decoder = client_decoder();
    decoder.read_buf().extend_from_slice(&bytes[..11]);
    assert_eq!(decoder.next_frame(), Ok(None));

    decoder.read_buf().extend_from_slice(&bytes[11..HEADER_LEN]);
    assert_eq!(decoder.next_frame(), Ok(None));
    decoder.read_buf().extend_from_slice(&bytes[HEADER_LEN..]);
    let (_, range) = decoder.next_frame().unwrap().unwrap();
    assert_eq!(decoder.body(range), b"abc");
}

#[test]
fn decoder_yields_all_complete_frames_in_one_buffer() {
    let frames = [
        execute_frame(1, b"a"),
        execute_frame(2, b"bc"),
        execute_frame(3, b""),
        execute_frame(4, b"def"),
    ];
    let half = &frames[3][..frames[3].len() / 2];
    let mut decoder = client_decoder();
    for frame in &frames[..3] {
        decoder.read_buf().extend_from_slice(frame);
    }
    decoder.read_buf().extend_from_slice(half);

    let mut first_body = None;
    for (request_id, body) in [(1, &b"a"[..]), (2, &b"bc"[..]), (3, &b""[..])] {
        let (header, range) = decoder.next_frame().unwrap().unwrap();
        assert_eq!(header.request_id, request_id);
        assert_eq!(decoder.body(range.clone()), body);
        if request_id == 1 {
            first_body = Some(range);
        }
    }
    assert_eq!(decoder.next_frame(), Ok(None));
    assert_eq!(decoder.body(first_body.unwrap()), b"a");
    decoder.consume_decoded();
    assert_eq!(&decoder.read_buf()[..], half);

    decoder
        .read_buf()
        .extend_from_slice(&frames[3][half.len()..]);
    let (header, range) = decoder.next_frame().unwrap().unwrap();
    assert_eq!(header.request_id, 4);
    assert_eq!(range, HEADER_LEN..frames[3].len());
    assert_eq!(decoder.body(range), b"def");
}

#[test]
fn rejects_bad_and_oversized_length() {
    let mut bytes = execute_frame(7, b"");
    bytes[5] = 0xff;
    bytes[..4].copy_from_slice(&7u32.to_be_bytes());
    assert_eq!(
        decode_error(&mut client_decoder(), &bytes),
        FrameError::BadLength
    );

    let body_len = MAX_FRAME + 1;
    bytes[..4].copy_from_slice(&(8 + body_len as u32).to_be_bytes());
    let mut decoder = client_decoder();
    assert_eq!(
        decode_error(&mut decoder, &bytes),
        FrameError::TooLarge { body_len }
    );
    assert_eq!(decoder.read_buf().capacity(), bytes.len());

    let mut decoder = FrameDecoder::new(Direction::ClientToServer, body_len);
    assert_eq!(
        decode_error(&mut decoder, &bytes),
        FrameError::TooLarge { body_len }
    );

    bytes[5] = 0;
    bytes[..4].copy_from_slice(&(8 + MAX_FRAME as u32).to_be_bytes());
    let mut decoder = client_decoder();
    decoder.read_buf().extend_from_slice(&bytes);
    assert_eq!(decoder.next_frame(), Ok(None));
    assert_eq!(decoder.read_buf().capacity(), HEADER_LEN + MAX_FRAME);
    decoder.read_buf().resize(HEADER_LEN + MAX_FRAME, 0);
    let (header, range) = decoder.next_frame().unwrap().unwrap();
    assert_eq!(header.body_len, MAX_FRAME);
    assert_eq!(decoder.body(range).len(), MAX_FRAME);
    decoder.consume_decoded();
    assert!(decoder.read_buf().is_empty());
    assert!(decoder.read_buf().capacity() <= 4096);

    let mut decoder = FrameDecoder::new(Direction::ClientToServer, 2);
    let bytes = execute_frame(7, b"abc");
    assert_eq!(
        decode_error(&mut decoder, &bytes[..HEADER_LEN]),
        FrameError::TooLarge { body_len: 3 }
    );
}

#[test]
fn rejects_reserved_bits_unknown_flags_unknown_kind() {
    let mut bytes = execute_frame(7, b"");
    bytes[..4].copy_from_slice(&(8 + MAX_FRAME as u32).to_be_bytes());
    bytes[7] = 1;
    bytes[5] = 4;
    bytes[4] = 99;
    assert_eq!(
        decode_error(&mut client_decoder(), &bytes),
        FrameError::Reserved
    );

    bytes[7] = 0;
    assert_eq!(
        decode_error(&mut client_decoder(), &bytes),
        FrameError::UnknownFlags(4)
    );

    bytes[5] = 0;
    assert_eq!(
        decode_error(&mut client_decoder(), &bytes),
        FrameError::UnknownKind(99)
    );
}

#[test]
fn rejects_wrong_direction() {
    let mut bytes = Vec::new();
    encode_frame(&mut bytes, FrameKind::Reply, Flags::default(), 7, b"");
    assert_eq!(
        decode_error(&mut client_decoder(), &bytes),
        FrameError::WrongDirection(FrameKind::Reply)
    );

    for (direction, allowed) in [
        (
            Direction::ClientToServer,
            &[1, 2, 4, 5, 7, 8, 9, 11, 12][..],
        ),
        (Direction::ServerToClient, &[2, 3, 6, 10, 13][..]),
    ] {
        for kind in 1..=13 {
            bytes[4] = kind;
            let mut decoder = FrameDecoder::new(direction, MAX_FRAME);
            decoder.read_buf().extend_from_slice(&bytes);
            if allowed.contains(&kind) {
                assert_eq!(decoder.next_frame().unwrap().unwrap().0.kind as u8, kind);
            } else {
                let Err(FrameError::WrongDirection(rejected)) = decoder.next_frame() else {
                    panic!("kind {kind} should be rejected for {direction:?}");
                };
                assert_eq!(rejected as u8, kind);
            }
        }
    }
}

fn xorshift64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn seeded_mutation_never_panics() {
    let mut state = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..20_000 {
        let count = xorshift64(&mut state) as usize % 3 + 1;
        let mut bytes = Vec::new();
        let mut starts = [0; 3];
        for (id, start) in starts[..count].iter_mut().enumerate() {
            *start = bytes.len();
            let body_len = xorshift64(&mut state) as usize % 64;
            encode_frame(
                &mut bytes,
                FrameKind::Execute,
                Flags::default(),
                id as u32 + 1,
                &[0xa5; 64][..body_len],
            );
        }

        let start = starts[xorshift64(&mut state) as usize % count];
        let value = xorshift64(&mut state);
        match value % 4 {
            0 => {
                let index = xorshift64(&mut state) as usize % bytes.len();
                bytes[index] ^= 1 << (value >> 2 & 7);
            }
            1 => {
                let len = xorshift64(&mut state) as usize % bytes.len();
                bytes.truncate(len);
            }
            2 => {
                let replacement = xorshift64(&mut state) as u32;
                let replacement = if value & 4 == 0 {
                    replacement
                } else {
                    replacement % (8 + MAX_FRAME as u32 + 1)
                };
                bytes[start..start + 4].copy_from_slice(&replacement.to_be_bytes());
            }
            _ => {
                let index = start + 4 + (xorshift64(&mut state) as usize % 4);
                bytes[index] = xorshift64(&mut state) as u8;
            }
        }

        let mut decoder = client_decoder();
        decoder.read_buf().extend_from_slice(&bytes);
        loop {
            let result = decoder.next_frame();
            assert!(decoder.read_buf().capacity() <= HEADER_LEN + MAX_FRAME + 4096);
            let Ok(Some((header, range))) = result else {
                break;
            };
            assert_eq!(decoder.body(range).len(), header.body_len);
        }
    }
}

#[test]
fn decoder_accepts_more_and_skipped_flags() {
    for flags in [Flags::MORE, Flags::SKIPPED, Flags::MORE | Flags::SKIPPED] {
        let mut bytes = Vec::new();
        encode_frame(&mut bytes, FrameKind::Reply, Flags(flags), 9, b"xy");
        let mut decoder = FrameDecoder::new(Direction::ServerToClient, MAX_FRAME);
        decoder.read_buf().extend_from_slice(&bytes);
        let (header, range) = decoder.next_frame().unwrap().unwrap();
        assert_eq!(header.flags, Flags(flags));
        assert_eq!(header.kind, FrameKind::Reply);
        assert_eq!(header.request_id, 9);
        assert_eq!(decoder.body(range), b"xy");
        assert_eq!(decoder.next_frame(), Ok(None));
    }
}

#[test]
fn fragmented_reads_with_a_maximum_frame_then_small_frames() {
    // A socket loop appends bounded reads, drains every complete frame, then
    // consumes. Frames must come out whole and in order, and the buffer must
    // stay within one maximum frame plus a bounded read.
    const READ: usize = 4096;
    let big = vec![0x5a; MAX_FRAME];
    let mut stream = Vec::new();
    stream.extend(execute_frame(1, b"first"));
    stream.extend(execute_frame(2, &big));
    stream.extend(execute_frame(3, b"third"));
    stream.extend(execute_frame(4, &big));
    stream.extend(execute_frame(5, b""));

    let mut decoder = client_decoder();
    let mut seen = Vec::new();
    for chunk in stream.chunks(READ) {
        decoder.read_buf().extend_from_slice(chunk);
        while let Some((header, range)) = decoder.next_frame().unwrap() {
            let body = decoder.body(range);
            assert_eq!(body.len(), header.body_len);
            assert!(body.iter().all(|byte| *byte == 0x5a) || body.len() < 6);
            seen.push((header.request_id, header.body_len));
        }
        decoder.consume_decoded();
        assert!(decoder.read_buf().capacity() <= HEADER_LEN + MAX_FRAME + 2 * READ);
    }
    assert_eq!(
        seen,
        vec![(1, 5), (2, MAX_FRAME), (3, 5), (4, MAX_FRAME), (5, 0)]
    );
    assert!(decoder.read_buf().is_empty());
}
