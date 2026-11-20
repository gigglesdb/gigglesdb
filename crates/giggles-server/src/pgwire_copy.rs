//! Bounded COPY framing. SQL value conversion remains in pgwire::decode_in_zone.
fn bad_format() -> &'static str {
    crate::service::sqlstate(&giggles_core::CoreError::Sql(
        giggles_core::relational::SqlError::BadCopyFileFormat,
    ))
}
type Row = Vec<Option<Vec<u8>>>;
type Result<T> = std::result::Result<T, &'static str>;
const LIMIT: usize = giggles_core::operations::MAX_INSERT_BYTES;
pub(super) struct Decoder {
    newline: Option<u8>,
    newline_error: Option<u8>,
    columns: usize,
    streaming: bool,
    delimiter: u8,
    null: Vec<u8>,
    binary: bool,
    csv: bool,
    quote: u8,
    escape: u8,
    skip_header: bool,
    match_header: Option<Vec<String>>,
    force_not_null: Vec<bool>,
    force_null: Vec<bool>,
    message: Option<String>,
    csv_at: usize,
    csv_quoted: bool,
    header: bool,
    ended: bool,
    total: usize,
    rows: usize,
    pending: Vec<u8>,
}
impl Decoder {
    pub(super) fn text(columns: usize, delimiter: u8, null: Vec<u8>) -> Self {
        Self {
            newline: None,
            newline_error: None,
            columns,
            streaming: false,
            delimiter,
            null,
            binary: false,
            csv: false,
            quote: b'"', escape: b'"', skip_header: false, match_header: None,
            force_not_null: vec![], force_null: vec![], message: None,
            csv_at: 0,
            csv_quoted: false,
            header: false,
            ended: false,
            total: 0,
            rows: 0,
            pending: Vec::new(),
        }
    }
    pub(super) fn csv(columns: usize, delimiter: u8, null: Vec<u8>) -> Self {
        Self {
            csv: true,
            ..Self::text(columns, delimiter, null)
        }
    }
    pub(super) fn binary(columns: usize) -> Self {
        Self {
            binary: true,
            ..Self::text(columns, b'\t', b"\\N".to_vec())
        }
    }
    pub(super) fn options(&mut self, o: &giggles_core::relational::CopyOptions, names: Vec<String>) {
        use giggles_core::relational::CopyHeader;
        self.quote = o.quote; self.escape = o.escape;
        self.skip_header = o.header != CopyHeader::Off;
        self.force_not_null = names.iter().map(|n| o.force_not_null.contains(n)).collect();
        self.force_null = names.iter().map(|n| o.force_null.contains(n)).collect();
        if o.header == CopyHeader::Match { self.match_header = Some(names); }
    }
    fn accept_header(&mut self, row: &Row) -> Result<bool> {
        if !self.skip_header { return Ok(true); }
        self.skip_header = false;
        if let Some(names) = self.match_header.take() {
            if row.len() != names.len() {
                self.message = Some(format!("wrong number of fields in header line: got {}, expected {}", row.len(), names.len()));
                return Err(bad_format());
            }
            for (i, (value, name)) in row.iter().zip(names).enumerate() {
                if value.as_deref() != Some(name.as_bytes()) {
                    let got = value.as_ref().map_or_else(|| format!("null value (\"{}\")", String::from_utf8_lossy(&self.null)), |v| format!("\"{}\"", String::from_utf8_lossy(v)));
                    self.message = Some(format!("column name mismatch in header line field {}: got {got}, expected \"{name}\"", i + 1));
                    return Err(bad_format());
                }
            }
        }
        Ok(false)
    }
    pub(super) fn stream(&mut self) { self.streaming = true; }
    pub(super) fn feed(&mut self, bytes: &[u8]) -> Result<Vec<Row>> {
        self.total = self.total.checked_add(bytes.len()).ok_or("54000")?;
        if (if self.streaming { self.pending.len() + bytes.len() } else { self.total }) > LIMIT {
            return Err("54000");
        }
        if self.ended {
            return if self.binary && !bytes.is_empty() {
                Err(bad_format())
            } else {
                Ok(Vec::new())
            };
        }
        self.pending.extend_from_slice(bytes);
        if self.binary {
            self.binary_rows()
        } else if self.csv {
            self.csv_rows(false)
        } else {
            self.text_rows(false)
        }
    }
    pub(super) fn finish(&mut self) -> Result<Vec<Row>> {
        if self.ended {
            return Ok(Vec::new());
        }
        if self.binary {
            let rows = self.binary_rows()?;
            if !self.ended {
                // PostgreSQL accepts transport EOF before a complete next tuple
                // count; pgx CopyFrom ends here without writing a -1 trailer.
                if !self.header || self.pending.len() >= 2 {
                    if !self.header && self.pending.len() < 11 { self.message = Some("COPY file signature not recognized".into()); }
                    return Err(bad_format());
                }
                self.pending.clear();
                self.ended = true;
            }
            Ok(rows)
        } else if self.csv {
            self.csv_rows(true)
        } else {
            self.text_rows(true)
        }
    }
    pub(super) fn diagnostic(&self, d: &mut super::Diagnostic) {
        if let Some(message) = &self.message { d.wire_message = Some(message.clone()); }
        if let Some(b) = self.newline_error {
            let cr = b == b'\r';
            d.wire_message = Some(format!("{} {} found in data", if self.csv { "unquoted" } else { "literal" }, if cr { "carriage return" } else { "newline" }));
            d.wire_hint = Some(if self.csv {
                if cr { "Use quoted CSV field to represent carriage return." } else { "Use quoted CSV field to represent newline." }
            } else if cr { "Use \"\\r\" to represent carriage return." } else { "Use \"\\n\" to represent newline." });
        }
    }
    fn newline(&mut self, at: usize) -> Result<()> {
        let b = self.pending[at];
        let style = if b == b'\r' && self.pending.get(at + 1) == Some(&b'\n') { 2 } else if b == b'\r' { 1 } else { 0 };
        if let Some(old) = self.newline {
            if old != style {
                self.newline_error = Some(if old == 1 && style == 2 { b'\n' } else { b });
                return Err(bad_format());
            }
        } else { self.newline = Some(style); }
        Ok(())
    }
    fn csv_rows(&mut self, eof: bool) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        let mut start = 0;
        while self.csv_at < self.pending.len() {
            let b = self.pending[self.csv_at];
            if self.csv_quoted && b == self.escape {
                if self.csv_at + 1 == self.pending.len() && !eof { break; }
                if self.pending.get(self.csv_at + 1).is_some_and(|b| *b == self.quote || *b == self.escape) {
                    self.csv_at += 2;
                    continue;
                }
            }
            if b == self.quote { self.csv_quoted = !self.csv_quoted; }
            if !self.csv_quoted && matches!(b, b'\n' | b'\r') {
                if b == b'\r' && self.csv_at + 1 == self.pending.len() && !eof {
                    break;
                }
                let line = &self.pending[start..self.csv_at];
                if line == b"\\." {
                    self.ended = true;
                    break;
                }
                if !self.streaming && self.rows == giggles_core::relational::MAX_ROWS {
                    return Err("54000");
                }
                self.newline(self.csv_at)?;
                let line = &self.pending[start..self.csv_at];
                let row = self.csv_row(line)?;
                if self.accept_header(&row)? { rows.push(row); }
                self.rows += 1;
                self.csv_at += 1;
                if b == b'\r' && self.pending.get(self.csv_at) == Some(&b'\n') {
                    self.csv_at += 1;
                }
                start = self.csv_at;
            } else {
                self.csv_at += 1;
            }
        }
        if self.ended {
            self.pending.clear();
        } else if eof {
            if self.csv_quoted {
                self.message = Some("unterminated CSV quoted field".into());
                return Err(bad_format());
            }
            if start < self.pending.len() && &self.pending[start..] != b"\\." {
                if !self.streaming && self.rows == giggles_core::relational::MAX_ROWS {
                    return Err("54000");
                }
                let row = self.csv_row(&self.pending[start..])?;
                if self.accept_header(&row)? { rows.push(row); }
                self.rows += 1;
            }
            self.pending.clear();
            self.csv_at = 0;
        } else {
            self.pending.drain(..start);
            self.csv_at -= start;
        }
        Ok(rows)
    }
    fn csv_row(&self, line: &[u8]) -> Result<Row> {
        let mut fields = Vec::new();
        let (mut at, mut start) = (0, 0);
        while at <= line.len() {
            let mut field = Vec::new();
            let (mut quoted, mut was_quoted) = (false, false);
            while at < line.len() && (quoted || line[at] != self.delimiter) {
                let b = line[at];
                at += 1;
                if quoted && b == self.escape && line.get(at).is_some_and(|b| *b == self.quote || *b == self.escape) {
                    field.push(line[at]); at += 1;
                } else if b == self.quote {
                    was_quoted = true; quoted = !quoted;
                } else {
                    field.push(b);
                }
            }
            if quoted {
                return Err(bad_format());
            }
            if field.contains(&0) || std::str::from_utf8(&field).is_err() {
                return Err("22021");
            }
            let index = fields.len();
            let force_not = !self.skip_header && self.force_not_null.get(index) == Some(&true);
            let force_null = !self.skip_header && self.force_null.get(index) == Some(&true);
            fields.push(if (!was_quoted && !force_not && line[start..at] == self.null) || (was_quoted && force_null && field == self.null) {
                None
            } else {
                Some(field)
            });
            if at == line.len() {
                break;
            }
            at += 1;
            start = at;
        }
        if !self.skip_header && fields.len() > self.columns {
            return Err(bad_format());
        }
        Ok(fields)
    }
    fn text_rows(&mut self, eof: bool) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        let mut at = 0;
        let mut start = 0;
        while at < self.pending.len() {
            if self.pending[at] == b'\\' {
                if at + 1 == self.pending.len() {
                    break;
                }
                at += 2;
                continue;
            }
            if self.pending[at] == b'\n' || self.pending[at] == b'\r' {
                let cr = self.pending[at] == b'\r';
                if cr && at + 1 == self.pending.len() && !eof {
                    break;
                }
                let line = &self.pending[start..at];
                if line == b"\\." {
                    self.ended = true;
                    break;
                }
                if !self.streaming && self.rows == giggles_core::relational::MAX_ROWS {
                    return Err("54000");
                }
                self.newline(at)?;
                let line = &self.pending[start..at];
                let row = self.text_row(line)?;
                if self.accept_header(&row)? { rows.push(row); }
                self.rows += 1;
                at += 1;
                if cr && self.pending.get(at) == Some(&b'\n') {
                    at += 1;
                }
                start = at;
            } else {
                at += 1;
            }
        }
        if self.ended {
            self.pending.clear();
            return Ok(rows);
        }
        if eof && start < self.pending.len() {
            let line = &self.pending[start..];
            if line != b"\\." {
                if !self.streaming && self.rows == giggles_core::relational::MAX_ROWS {
                    return Err("54000");
                }
                let row = self.text_row(line)?;
                if self.accept_header(&row)? { rows.push(row); }
                self.rows += 1;
            }
            self.pending.clear();
        } else {
            self.pending.drain(..start);
        }
        Ok(rows)
    }
    fn text_row(&self, line: &[u8]) -> Result<Row> {
        let mut fields = Vec::new();
        let mut start = 0;
        let mut at = 0;
        while at < line.len() {
            if line[at] == b'\\' {
                at = (at + 2).min(line.len());
            } else if line[at] == self.delimiter {
                fields.push(self.field(&line[start..at])?);
                at += 1;
                start = at;
            } else {
                at += 1;
            }
        }
        fields.push(self.field(&line[start..])?);
        if !self.skip_header && fields.len() > self.columns {
            return Err(bad_format());
        }
        Ok(fields)
    }
    fn field(&self, raw: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut marker = 0;
        while marker + 1 < raw.len() {
            if raw[marker] == b'\\' {
                if raw[marker + 1] == b'.' {
                    return Err(bad_format());
                }
                marker += 2;
            } else {
                marker += 1;
            }
        }
        if raw == self.null {
            return Ok(None);
        }
        let mut out = Vec::new();
        let mut at = 0;
        while at < raw.len() {
            let mut b = raw[at];
            at += 1;
            if b == b'\\' {
                if at == raw.len() {
                    break;
                }
                b = raw[at];
                at += 1;
                b = match b {
                    b'.' => return Err(bad_format()),
                    b'b' => 8,
                    b'f' => 12,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'v' => 11,
                    b'0'..=b'7' => {
                        let mut n = b - b'0';
                        for _ in 0..2 {
                            if let Some(next @ b'0'..=b'7') = raw.get(at) {
                                n = n.wrapping_mul(8).wrapping_add(next - b'0');
                                at += 1;
                            } else {
                                break;
                            }
                        }
                        n
                    }
                    b'x' => {
                        let mut n = 0u8;
                        let mut digits = 0;
                        for _ in 0..2 {
                            if let Some(v) = raw.get(at).and_then(|x| (*x as char).to_digit(16)) {
                                n = n * 16 + v as u8;
                                at += 1;
                                digits += 1;
                            } else {
                                break;
                            }
                        }
                        if digits == 0 { b'x' } else { n }
                    }
                    other => other,
                };
            }
            if b == 0 {
                return Err("22021");
            }
            out.push(b);
        }
        std::str::from_utf8(&out).map_err(|_| "22021")?;
        Ok(Some(out))
    }
    fn binary_rows(&mut self) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        if !self.header {
            if self.pending.len() < 19 {
                return Ok(rows);
            }
            if &self.pending[..11] != b"PGCOPY\n\xff\r\n\0" {
                self.message = Some("COPY file signature not recognized".into());
                return Err(bad_format());
            }
            let flags = u32::from_be_bytes(self.pending[11..15].try_into().unwrap());
            if flags & 0xffff0000 != 0 {
                return Err(bad_format());
            }
            let extension = i32::from_be_bytes(self.pending[15..19].try_into().unwrap());
            if extension < 0 {
                return Err(bad_format());
            }
            let end = 19usize.checked_add(extension as usize).ok_or("54000")?;
            if end > LIMIT {
                return Err("54000");
            }
            if self.pending.len() < end {
                return Ok(rows);
            }
            self.pending.drain(..end);
            self.header = true;
        }
        loop {
            if self.pending.len() < 2 {
                break;
            }
            let count = i16::from_be_bytes(self.pending[..2].try_into().unwrap());
            if count == -1 {
                if self.pending.len() != 2 {
                    return Err(bad_format());
                }
                self.ended = true;
                self.pending.clear();
                break;
            }
            if count < 0 || count as usize != self.columns {
                return Err(bad_format());
            }
            if !self.streaming && self.rows == giggles_core::relational::MAX_ROWS {
                return Err("54000");
            }
            let mut row = Vec::new();
            let mut at = 2;
            for _ in 0..count {
                if self.pending.len() < at + 4 {
                    return Ok(rows);
                }
                let len = i32::from_be_bytes(self.pending[at..at + 4].try_into().unwrap());
                at += 4;
                if len == -1 {
                    row.push(None);
                    continue;
                }
                if len < 0 {
                    return Err(bad_format());
                }
                let end = at.checked_add(len as usize).ok_or("54000")?;
                if end > LIMIT {
                    return Err("54000");
                }
                if self.pending.len() < end {
                    return Ok(rows);
                }
                row.push(Some(self.pending[at..end].to_vec()));
                at = end;
            }
            self.pending.drain(..at);
            self.rows += 1;
            rows.push(row);
        }
        Ok(rows)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn newline_styles_every_split() {
        for csv in [false, true] {
            for terminator in [b"\n".as_slice(), b"\r\n", b"\r"] {
                let mut data = b"1".to_vec(); data.extend(terminator); data.extend(b"2"); data.extend(terminator);
                for split in 0..=data.len() {
                    let mut d = if csv { Decoder::csv(1, b',', vec![]) } else { Decoder::text(1, b'\t', vec![]) };
                    let mut rows = d.feed(&data[..split]).unwrap(); rows.extend(d.feed(&data[split..]).unwrap()); rows.extend(d.finish().unwrap());
                    assert_eq!(rows.len(), 2);
                }
            }
            for data in [b"1\r\n2\n".as_slice(), b"1\n2\r\n", b"1\r2\n"] {
                for split in 0..=data.len() {
                    let mut d = if csv { Decoder::csv(1, b',', vec![]) } else { Decoder::text(1, b'\t', vec![]) };
                    assert_eq!(d.feed(&data[..split]).and_then(|_| d.feed(&data[split..])).and_then(|_| d.finish()).unwrap_err(), "22P04");
                    assert!(d.newline_error.is_some());
                }
            }
        }
        for (csv, data, kind) in [
            (false, b"1\tq\r\n2\tr\r3\ts\r\n".as_slice(), b'\r'),
            (false, b"1\tq\r2\tr\r\n".as_slice(), b'\n'),
            (true, b"1,q\r\n2,r\r3,s\r\n".as_slice(), b'\r'),
            (true, b"1,q\n2,r\r\n".as_slice(), b'\r'),
        ] {
            for split in 0..=data.len() {
                let mut d = if csv {
                    Decoder::csv(2, b',', vec![])
                } else {
                    Decoder::text(2, b'\t', vec![])
                };
                let result = d.feed(&data[..split])
                    .and_then(|_| d.feed(&data[split..]))
                    .and_then(|_| d.finish());
                assert_eq!(result.unwrap_err(), "22P04", "csv {csv}, split {split}");
                assert_eq!(d.newline_error, Some(kind), "csv {csv}, split {split}");
            }
        }
    }
    #[test]
    fn csv_every_split_and_invalid_input() {
        let data = b"1,\"a,b\",\"\"\r\n2,\"line\n\"\"quote\"\"\",\r\n\\.\nignored";
        for split in 0..=data.len() {
            let mut d = Decoder::csv(3, b',', vec![]);
            let mut rows = d.feed(&data[..split]).unwrap();
            rows.extend(d.feed(&data[split..]).unwrap());
            rows.extend(d.finish().unwrap());
            assert_eq!(
                rows,
                vec![
                    vec![Some(b"1".to_vec()), Some(b"a,b".to_vec()), Some(vec![])],
                    vec![Some(b"2".to_vec()), Some(b"line\n\"quote\"".to_vec()), None],
                ],
                "split {split}"
            );
        }
        for (data, code) in [
            (b"\"unterminated".as_slice(), "22P04"),
            (b"a,\0,c", "22021"),
        ] {
            let mut d = Decoder::csv(3, b',', vec![]);
            let result = d.feed(data).and_then(|_| d.finish());
            assert_eq!(result.unwrap_err(), code);
        }
    }
    #[test]
    fn short_csv_row_is_converted_before_missing_column_error() {
        assert_eq!(Decoder::csv(3, b',', vec![]).feed(b"a,b\n").unwrap(),
            vec![vec![Some(b"a".to_vec()), Some(b"b".to_vec())]]);
    }
    #[test]
    fn text_every_split() {
        let data = b"1\t\\N\t\\x41\\101\\b\\f\\v\\Q\\\\\r\n2\t\tend\\\n\r\n\\.\nignored";
        for split in 0..=data.len() {
            let mut d = Decoder::text(3, b'\t', b"\\N".to_vec());
            let mut rows = d.feed(&data[..split]).unwrap();
            rows.extend(d.feed(&data[split..]).unwrap());
            rows.extend(d.finish().unwrap());
            assert_eq!(
                rows,
                vec![
                    vec![
                        Some(b"1".to_vec()),
                        None,
                        Some(b"AA\x08\x0c\x0bQ\\".to_vec())
                    ],
                    vec![Some(b"2".to_vec()), Some(vec![]), Some(b"end\n".to_vec())]
                ],
                "split {split}"
            );
        }
    }
    #[test]
    fn text_errors_and_eof() {
        for (bytes, code) in [
            (b"x\\.y\n".as_slice(), "22P04"),
            (b"\\0\n", "22021"),
            (b"\xff\n", "22021"),
            (b"a\tb\n", "22P04"),
        ] {
            assert_eq!(
                Decoder::text(1, b'\t', b"\\N".to_vec())
                    .feed(bytes)
                    .unwrap_err(),
                code
            );
        }
        let mut d = Decoder::text(1, b'|', b"NIL".to_vec());
        d.feed(b"tail\\").unwrap();
        assert_eq!(d.finish().unwrap(), vec![vec![Some(b"tail".to_vec())]]);
        let mut d = Decoder::text(2, b'|', b"NIL".to_vec());
        assert_eq!(
            d.feed(b"NIL|\\NIL\n").unwrap(),
            vec![vec![None, Some(b"NIL".to_vec())]]
        );
    }
    #[test]
    fn binary_every_split() {
        let mut bytes = b"PGCOPY\n\xff\r\n\0".to_vec();
        bytes.extend(0i32.to_be_bytes());
        bytes.extend(3i32.to_be_bytes());
        bytes.extend(b"ext");
        bytes.extend(2i16.to_be_bytes());
        bytes.extend(4i32.to_be_bytes());
        bytes.extend(42i32.to_be_bytes());
        bytes.extend((-1i32).to_be_bytes());
        bytes.extend((-1i16).to_be_bytes());
        for split in 0..=bytes.len() {
            let mut d = Decoder::binary(2);
            let mut rows = d.feed(&bytes[..split]).unwrap();
            rows.extend(d.feed(&bytes[split..]).unwrap());
            rows.extend(d.finish().unwrap());
            assert_eq!(rows, vec![vec![Some(42i32.to_be_bytes().to_vec()), None]]);
        }
    }
    #[test]
    fn binary_transport_eof_without_trailer_matches_postgres() {
        let mut header = b"PGCOPY\n\xff\r\n\0".to_vec();
        header.extend([0; 8]);
        for suffix in [b"".as_slice(), b"\0", b"\xff"] {
            let mut d = Decoder::binary(1);
            d.feed(&header).unwrap();
            d.feed(suffix).unwrap();
            assert!(d.finish().unwrap().is_empty());
        }
        let mut bytes = header.clone();
        bytes.extend(1i16.to_be_bytes());
        bytes.extend(4i32.to_be_bytes());
        bytes.extend(42i32.to_be_bytes());
        for split in 0..=bytes.len() {
            let mut d = Decoder::binary(1);
            let mut rows = d.feed(&bytes[..split]).unwrap();
            rows.extend(d.feed(&bytes[split..]).unwrap());
            rows.extend(d.finish().unwrap());
            assert_eq!(rows, vec![vec![Some(42i32.to_be_bytes().to_vec())]]);
        }
        for extra in [b"\0\x01".as_slice(), b"\0\x01\0\0\0\x04\0"] {
            let mut d = Decoder::binary(1);
            d.feed(&header).unwrap();
            d.feed(extra).unwrap();
            assert_eq!(d.finish().unwrap_err(), "22P04");
        }
    }
    #[test]
    fn binary_data_after_trailer_is_bad_format() {
        let mut bytes = b"PGCOPY\n\xff\r\n\0".to_vec();
        bytes.extend([0; 8]);
        bytes.extend((-1i16).to_be_bytes());
        for split in 0..=bytes.len() {
            let mut d = Decoder::binary(1);
            d.feed(&bytes[..split]).unwrap();
            let mut rest = bytes[split..].to_vec();
            rest.push(1);
            assert_eq!(d.feed(&rest).unwrap_err(), "22P04", "split {split}");
        }
        let mut d = Decoder::binary(1);
        d.feed(&bytes).unwrap();
        assert!(d.feed(&[]).unwrap().is_empty());
        assert_eq!(d.feed(b"garbage").unwrap_err(), "22P04");
    }
    #[test]
    fn bounds() {
        let mut d = Decoder::text(1, b'\t', vec![]);
        d.total = LIMIT;
        assert_eq!(d.feed(b"x").unwrap_err(), "54000");
    }
    #[test]
    fn malformed_binary_and_byte_feeds() {
        let mut header = b"PGCOPY\n\xff\r\n\0".to_vec();
        header.extend([0; 8]);
        for bytes in [
            vec![0; 19],
            {
                let mut b = header.clone();
                b[11] = 128;
                b
            },
            {
                let mut b = header.clone();
                b.extend(2i16.to_be_bytes());
                b
            },
            {
                let mut b = header.clone();
                b.extend(1i16.to_be_bytes());
                b.extend((-2i32).to_be_bytes());
                b
            },
        ] {
            assert_eq!(Decoder::binary(1).feed(&bytes).unwrap_err(), "22P04");
        }
        for bytes in [&header[..5], &header[..18]] {
            let mut d = Decoder::binary(1);
            d.feed(bytes).unwrap();
            assert_eq!(d.finish().unwrap_err(), "22P04");
        }
        let bytes = b"a\tb\r\n\\.\nignored";
        let mut d = Decoder::text(2, b'\t', b"\\N".to_vec());
        let mut rows = vec![];
        for b in bytes {
            rows.extend(d.feed(&[*b]).unwrap());
        }
        rows.extend(d.finish().unwrap());
        assert_eq!(rows, vec![vec![Some(b"a".to_vec()), Some(b"b".to_vec())]]);
        assert_eq!(
            Decoder::text(2, b'\t', b"\\N".to_vec())
                .feed(b"a\n")
                .unwrap(),
            vec![vec![Some(b"a".to_vec())]]
        );
        assert_eq!(
            Decoder::text(1, b'\t', b"x\\.y".to_vec())
                .feed(b"x\\.y\n")
                .unwrap_err(),
            "22P04"
        );
    }
    #[test]
    fn streaming_retains_record_bounds_without_cumulative_caps() {
        for csv in [false, true] {
            let mut d = if csv { Decoder::csv(1, b',', vec![]) }
                else { Decoder::text(1, b'\t', vec![]) };
            d.stream();
            for _ in 0..2000 {
                assert_eq!(d.feed(b"1\n".repeat(300).as_slice()).unwrap().len(), 300);
                assert!(d.pending.is_empty());
            }
            assert!(d.finish().unwrap().is_empty());
            d.feed(&vec![b'x'; LIMIT]).unwrap();
            assert_eq!(d.feed(b"x").unwrap_err(), "54000");
        }
    }
    #[test]
    fn cumulative_row_limit_before_materializing_another_row() {
        let limit = giggles_core::relational::MAX_ROWS;
        let mut d = Decoder::text(1, b'\t', b"\\N".to_vec());
        assert_eq!(d.feed(&vec![b'\n'; limit + 1]).unwrap_err(), "54000");
        assert_eq!(d.rows, limit);
        let mut d = Decoder::text(1, b'\t', b"\\N".to_vec());
        assert_eq!(d.feed(&vec![b'\n'; limit]).unwrap().len(), limit);
        assert_eq!(d.feed(b"\n").unwrap_err(), "54000");
        assert_eq!(d.rows, limit);
        let mut d = Decoder::text(1, b'\t', b"\\N".to_vec());
        d.feed(&vec![b'\n'; limit]).unwrap();
        d.feed(b"last").unwrap();
        assert_eq!(d.finish().unwrap_err(), "54000");
        let mut bytes = b"PGCOPY\n\xff\r\n\0".to_vec();
        bytes.extend([0; 8]);
        for _ in 0..limit + 1 {
            bytes.extend(1i16.to_be_bytes());
            bytes.extend(0i32.to_be_bytes());
        }
        let mut d = Decoder::binary(1);
        assert_eq!(d.feed(&bytes).unwrap_err(), "54000");
        assert_eq!(d.rows, limit);
        let mut d = Decoder::binary(1);
        let split = 19 + limit * 6;
        assert_eq!(d.feed(&bytes[..split]).unwrap().len(), limit);
        assert_eq!(d.feed(&bytes[split..]).unwrap_err(), "54000");
        assert_eq!(d.rows, limit);
    }
}

#[cfg(test)]
mod option_tests {
    use super::*;
    #[test]
    fn csv_options_at_every_chunk_boundary() {
        let o = giggles_core::relational::CopyOptions::parse("(FORMAT csv, HEADER MATCH, QUOTE '~', ESCAPE '!', FORCE_NOT_NULL (a), FORCE_NULL (b))", true).unwrap();
        let data = b"a,b\n,~~\n~x!~y~,~z!!q~\n";
        for split in 0..=data.len() {
            let mut d = Decoder::csv(2, b',', vec![]);
            d.options(&o, vec!["a".into(), "b".into()]);
            let mut rows = d.feed(&data[..split]).unwrap();
            rows.extend(d.feed(&data[split..]).unwrap()); rows.extend(d.finish().unwrap());
            assert_eq!(rows, vec![vec![Some(vec![]), None], vec![Some(b"x~y".to_vec()), Some(b"z!q".to_vec())]], "split {split}");
        }
    }
    #[test]
    fn header_match_and_empty_input() {
        let o = giggles_core::relational::CopyOptions::parse("(HEADER MATCH)", true).unwrap();
        let mut d = Decoder::text(1, b'\t', b"\\N".to_vec()); d.options(&o, vec!["a".into()]);
        assert!(d.finish().unwrap().is_empty());
        assert_eq!(d.feed(b"b\n").unwrap_err(), "22P04");
        assert_eq!(d.message.as_deref(), Some("column name mismatch in header line field 1: got \"b\", expected \"a\""));
    }
}
