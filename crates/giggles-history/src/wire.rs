use crate::{Attempt, History, Invariants};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};

/// Maximum wire bytes, line bytes and attempt records for version 1.
pub const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_LINE: usize = 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header {
        version: u32,
        horizon: u64,
        initial: Vec<(i64, i64)>,
        invariants: Invariants,
    },
    Attempt {
        attempt: Attempt,
    },
}

/// Read bounded JSONL: exactly one header first, followed by attempt records.
/// The incremental fill_buf loop caps allocation even for an unterminated line.
pub fn parse_jsonl(mut input: impl BufRead) -> Result<History, String> {
    let mut history = None;
    let mut bytes = 0;
    let mut line = vec![];
    let mut line_no = 0;
    loop {
        let buffer = input.fill_buf().map_err(|e| e.to_string())?;
        if buffer.is_empty() {
            if line.is_empty() {
                break;
            }
            parse_line(&line, &mut history, line_no + 1)?;
            break;
        }
        let n = buffer
            .iter()
            .position(|b| *b == b'\n')
            .map_or(buffer.len(), |p| p + 1);
        bytes += n;
        if bytes > MAX_BYTES || line.len() + n > MAX_LINE {
            return Err("JSONL byte/line limit exceeded".into());
        }
        line.extend_from_slice(&buffer[..n]);
        input.consume(n);
        if line.last() == Some(&b'\n') {
            line_no += 1;
            parse_line(&line, &mut history, line_no)?;
            line.clear();
        }
    }
    let h = history.ok_or("missing header")?;
    crate::search::validate(&h)?;
    Ok(h)
}
fn parse_line(line: &[u8], h: &mut Option<History>, number: usize) -> Result<(), String> {
    let record: Record = serde_json::from_slice(line).map_err(|e| format!("line {number}: {e}"))?;
    match record {
        Record::Header {
            version,
            horizon,
            initial,
            invariants,
        } => {
            if h.is_some() {
                return Err("duplicate header".into());
            }
            if initial.len() > 4096 || initial.windows(2).any(|r| r[0].0 >= r[1].0) {
                return Err("initial pairs must be unique ordered bounded keys".into());
            }
            *h = Some(History {
                version,
                horizon,
                initial: initial.into_iter().collect(),
                invariants,
                attempts: vec![],
            });
        }
        Record::Attempt { attempt } => {
            let history = h.as_mut().ok_or("attempt before header")?;
            if history.attempts.len() >= 128 {
                return Err("attempt record limit exceeded".into());
            }
            history.attempts.push(attempt);
        }
    }
    Ok(())
}

/// Encode stable versioned records, one compact JSON value per line.
pub fn write_jsonl(h: &History, mut output: impl Write) -> Result<(), String> {
    crate::search::validate(h)?;
    let header = Record::Header {
        version: h.version,
        horizon: h.horizon,
        initial: h.initial.iter().map(|(&k, &v)| (k, v)).collect(),
        invariants: h.invariants.clone(),
    };
    serde_json::to_writer(&mut output, &header).map_err(|e| e.to_string())?;
    writeln!(output).map_err(|e| e.to_string())?;
    for a in &h.attempts {
        serde_json::to_writer(&mut output, &Record::Attempt { attempt: a.clone() })
            .map_err(|e| e.to_string())?;
        writeln!(output).map_err(|e| e.to_string())?;
    }
    Ok(())
}
