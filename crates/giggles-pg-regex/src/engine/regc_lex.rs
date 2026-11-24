/*
 *
 * Copyright (c) 1998, 1999 Henry Spencer.  All rights reserved.
 *
 * Development of this software was funded, in part, by Cray Research Inc.,
 * UUNET Communications Services Inc., Sun Microsystems Inc., and Scriptics
 * Corporation, none of whom are responsible for the results.  The author
 * thanks all of them.
 *
 * Redistribution and use in source and binary forms -- with or without
 * modification -- are permitted for any purpose, provided that
 * redistributions in source form retain this entire copyright notice and
 * indicate the origin and nature of any modifications.
 *
 * I'd appreciate being given credit for this package in the documentation
 * of software which uses it, but that is not a requirement.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * HENRY SPENCER BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Translated to Rust from regc_lex.c.
 */

use super::regc_pg_locale::classify;
use super::regguts::{CHR_MAX, Class, Look};
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Token {
    Empty,
    Eos,
    Plain(Chr),
    Open(bool),
    Close,
    Alt,
    Dot,
    Bol,
    Eol,
    Bos,
    EosAnchor,
    WordBegin,
    WordEnd,
    WordBoundary,
    NotWordBoundary,
    Lookaround(Look),
    Backref(usize),
    Star(bool),
    Plus(bool),
    Question(bool),
    OpenBound,
    CloseBound(bool),
    Digit(usize),
    Comma,
    OpenBracket(bool),
    CloseBracket,
    Range,
    CollatingElement,
    EquivalenceClass,
    NamedClass,
    EndName,
    Class(Class, bool),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Context {
    Ere,
    Bre,
    Quote,
    EreBound,
    BreBound,
    Bracket,
    Name(u8),
}

pub(super) struct Lexer<'a> {
    input: &'a [Chr],
    pos: usize,
    context: Context,
    pub(super) flags: i32,
    pub(super) info: i64,
    pub(super) locale: RegexLocale,
    pub(super) token: Token,
    pub(super) nsub: usize,
}

impl<'a> Lexer<'a> {
    pub(super) fn new(input: &'a [Chr], flags: i32, locale: RegexLocale) -> Self {
        Self {
            input,
            pos: 0,
            context: Context::Quote,
            flags,
            info: 0,
            locale,
            token: Token::Empty,
            nsub: 0,
        }
    }

    pub(super) fn start(&mut self) -> Result<(), RegErrorCode> {
        self.prefixes()?;
        self.context = self.base_context();
        self.advance()
    }

    fn base_context(&self) -> Context {
        if self.flags & REG_QUOTE != 0 {
            Context::Quote
        } else if self.flags & REG_EXTENDED != 0 {
            Context::Ere
        } else {
            Context::Bre
        }
    }

    fn here(&self) -> Option<Chr> {
        self.input.get(self.pos).copied()
    }

    fn take(&mut self) -> Option<Chr> {
        let c = self.here()?;
        self.pos += 1;
        Some(c)
    }

    fn starts_with(&self, text: &[u8]) -> bool {
        self.input[self.pos..].len() >= text.len()
            && self.input[self.pos..self.pos + text.len()]
                .iter()
                .copied()
                .eq(text.iter().map(|&b| u32::from(b)))
    }

    fn eat(&mut self, byte: u8) -> bool {
        if self.here() != Some(byte.into()) {
            return false;
        }
        self.pos += 1;
        true
    }

    fn prefixes(&mut self) -> Result<(), RegErrorCode> {
        if self.flags & REG_QUOTE != 0 {
            return Ok(());
        }
        if self.input.len() >= 4 && self.starts_with(b"***") {
            match self.input[3] {
                63 => return Err(RegErrorCode::BadPat),
                61 => {
                    self.info |= REG_UNONPOSIX;
                    self.flags =
                        (self.flags | REG_QUOTE) & !(REG_ADVANCED | REG_EXPANDED | REG_NEWLINE);
                    self.pos += 4;
                    return Ok(());
                }
                58 => {
                    self.info |= REG_UNONPOSIX;
                    self.flags |= REG_ADVANCED;
                    self.pos += 4;
                }
                _ => return Err(RegErrorCode::BadRpt),
            }
        }
        if self.flags & REG_ADVANCED == REG_ADVANCED
            && self.starts_with(b"(?")
            && self
                .input
                .get(self.pos + 2)
                .is_some_and(|&c| classify(self.locale, Class::Alpha, c))
        {
            self.info |= REG_UNONPOSIX;
            self.pos += 2;
            while let Some(c) = self
                .here()
                .filter(|&c| classify(self.locale, Class::Alpha, c))
            {
                self.flags = match u8::try_from(c).ok() {
                    Some(b'b') => self.flags & !(REG_ADVANCED | REG_QUOTE),
                    Some(b'c') => self.flags & !REG_ICASE,
                    Some(b'e') => (self.flags | REG_EXTENDED) & !(REG_ADVF | REG_QUOTE),
                    Some(b'i') => self.flags | REG_ICASE,
                    Some(b'm' | b'n') => self.flags | REG_NEWLINE,
                    Some(b'p') => (self.flags | REG_NLSTOP) & !REG_NLANCH,
                    Some(b'q') => (self.flags | REG_QUOTE) & !REG_ADVANCED,
                    Some(b's') => self.flags & !REG_NEWLINE,
                    Some(b't') => self.flags & !REG_EXPANDED,
                    Some(b'w') => (self.flags | REG_NLANCH) & !REG_NLSTOP,
                    Some(b'x') => self.flags | REG_EXPANDED,
                    _ => return Err(RegErrorCode::BadOpt),
                };
                self.pos += 1;
            }
            if !self.eat(b')') {
                return Err(RegErrorCode::BadOpt);
            }
            if self.flags & REG_QUOTE != 0 {
                self.flags &= !(REG_EXPANDED | REG_NEWLINE);
            }
        }
        Ok(())
    }

    pub(super) fn advance(&mut self) -> Result<(), RegErrorCode> {
        self.token = self.next()?;
        Ok(())
    }

    fn skip(&mut self) {
        let start = self.pos;
        loop {
            while self
                .here()
                .is_some_and(|c| classify(self.locale, Class::Space, c))
            {
                self.pos += 1;
            }
            if !self.eat(b'#') {
                break;
            }
            while self.here().is_some_and(|c| c != u32::from(b'\n')) {
                self.pos += 1;
            }
        }
        if self.pos != start {
            self.info |= REG_UNONPOSIX;
        }
    }

    fn greedy(&mut self) -> bool {
        if self.flags & REG_ADVF != 0 && self.eat(b'?') {
            self.info |= REG_UNONPOSIX;
            false
        } else {
            true
        }
    }

    fn next(&mut self) -> Result<Token, RegErrorCode> {
        use Token::*;
        if self.token == Empty && self.flags & REG_BOSONLY != 0 {
            return Ok(Bos);
        }
        loop {
            if self.flags & REG_EXPANDED != 0
                && matches!(
                    self.context,
                    Context::Ere | Context::Bre | Context::EreBound | Context::BreBound
                )
            {
                self.skip();
            }
            let Some(c) = self.take() else {
                return match self.context {
                    Context::Ere | Context::Bre | Context::Quote => Ok(Eos),
                    Context::EreBound | Context::BreBound => Err(RegErrorCode::EBrace),
                    Context::Bracket | Context::Name(_) => Err(RegErrorCode::EBrack),
                };
            };
            let byte = u8::try_from(c).ok();
            return match self.context {
                Context::Quote => Ok(Plain(c)),
                Context::Bre => self.brenext(c),
                Context::EreBound | Context::BreBound => match byte {
                    Some(b'0'..=b'9') => Ok(Digit((c - 48) as usize)),
                    Some(b',') => Ok(Comma),
                    Some(b'}') if self.context == Context::EreBound => {
                        self.context = Context::Ere;
                        Ok(CloseBound(self.greedy()))
                    }
                    Some(b'\\') if self.context == Context::BreBound && self.eat(b'}') => {
                        self.context = Context::Bre;
                        Ok(CloseBound(true))
                    }
                    _ => Err(RegErrorCode::BadBr),
                },
                Context::Name(end) => {
                    if byte == Some(end) && self.eat(b']') {
                        self.context = Context::Bracket;
                        Ok(EndName)
                    } else {
                        Ok(Plain(c))
                    }
                }
                Context::Bracket => match byte {
                    Some(b']') if !matches!(self.token, OpenBracket(_)) => {
                        self.context = self.base_context();
                        Ok(CloseBracket)
                    }
                    Some(b'\\') => {
                        self.info |= REG_UBBS;
                        if self.flags & REG_ADVF == 0 {
                            return Ok(Plain(c));
                        }
                        self.info |= REG_UNONPOSIX;
                        let escaped = self.lexescape()?;
                        if matches!(escaped, Plain(_) | Class(_, _)) {
                            Ok(escaped)
                        } else {
                            Err(RegErrorCode::EEscape)
                        }
                    }
                    Some(b'-')
                        if !matches!(self.token, OpenBracket(_)) && !self.starts_with(b"]") =>
                    {
                        Ok(Range)
                    }
                    Some(b'[') => match self.here().ok_or(RegErrorCode::EBrack)? {
                        46 => {
                            self.pos += 1;
                            self.context = Context::Name(b'.');
                            Ok(CollatingElement)
                        }
                        61 | 58 => {
                            let delimiter = self.take().unwrap() as u8;
                            self.context = Context::Name(delimiter);
                            self.info |= REG_ULOCALE;
                            Ok(if delimiter == b'=' {
                                EquivalenceClass
                            } else {
                                NamedClass
                            })
                        }
                        _ => Ok(Plain(c)),
                    },
                    _ => Ok(Plain(c)),
                },
                Context::Ere => match byte {
                    Some(b'|') => Ok(Alt),
                    Some(b'*') => Ok(Star(self.greedy())),
                    Some(b'+') => Ok(Plus(self.greedy())),
                    Some(b'?') => Ok(Question(self.greedy())),
                    Some(b'{') => {
                        if self.flags & REG_EXPANDED != 0 {
                            self.skip();
                        }
                        if self
                            .here()
                            .is_some_and(|c| classify(self.locale, regguts::Class::Digit, c))
                        {
                            self.info |= REG_UBOUNDS;
                            self.context = Context::EreBound;
                            Ok(OpenBound)
                        } else {
                            self.info |= REG_UBRACES | REG_UUNSPEC;
                            Ok(Plain(c))
                        }
                    }
                    Some(b'(') if self.flags & REG_ADVF != 0 && self.eat(b'?') => {
                        self.info |= REG_UNONPOSIX;
                        match self.take() {
                            Some(58) => Ok(Open(false)),
                            Some(35) => {
                                while self.take().is_some_and(|c| c != 41) {}
                                continue;
                            }
                            Some(61) => self.look(Look::AheadPos),
                            Some(33) => self.look(Look::AheadNeg),
                            Some(60) if self.eat(b'=') => self.look(Look::BehindPos),
                            Some(60) if self.eat(b'!') => self.look(Look::BehindNeg),
                            _ => Err(RegErrorCode::BadRpt),
                        }
                    }
                    Some(b'(') => Ok(Open(true)),
                    Some(b')') => {
                        if matches!(self.token, Open(_)) {
                            self.info |= REG_UUNSPEC;
                        }
                        Ok(Close)
                    }
                    Some(b'[') => Ok(self.open_bracket()),
                    Some(b'.') => Ok(Dot),
                    Some(b'^') => Ok(Bol),
                    Some(b'$') => Ok(Eol),
                    Some(b'\\') if self.flags & REG_ADVF != 0 => self.lexescape(),
                    Some(b'\\') => {
                        let chr = self.take().ok_or(RegErrorCode::EEscape)?;
                        Ok(self.posix_escape(chr))
                    }
                    _ => Ok(Plain(c)),
                },
            };
        }
    }

    fn look(&mut self, kind: Look) -> Result<Token, RegErrorCode> {
        self.info |= REG_ULOOKAROUND;
        Ok(Token::Lookaround(kind))
    }

    fn open_bracket(&mut self) -> Token {
        use Token::*;
        if self.starts_with(b"[:<:]]") || self.starts_with(b"[:>:]]") {
            let open = self.input[self.pos + 2] == u32::from(b'<');
            self.pos += 6;
            self.info |= REG_UNONPOSIX;
            return if open { WordBegin } else { WordEnd };
        }
        self.context = Context::Bracket;
        OpenBracket(!self.eat(b'^'))
    }

    fn posix_escape(&mut self, chr: Chr) -> Token {
        if classify(self.locale, Class::Alnum, chr) {
            self.info |= REG_UBSALNUM | REG_UUNSPEC;
        }
        Token::Plain(chr)
    }

    fn brenext(&mut self, chr: Chr) -> Result<Token, RegErrorCode> {
        use Token::*;
        Ok(match u8::try_from(chr).ok() {
            Some(b'*') if !matches!(self.token, Empty | Open(_) | Bol) => Star(true),
            Some(b'[') => self.open_bracket(),
            Some(b'.') => Dot,
            Some(b'^') if self.token == Empty => Bol,
            Some(b'^') if matches!(self.token, Open(_)) => {
                self.info |= REG_UUNSPEC;
                Bol
            }
            Some(b'$') => {
                if self.flags & REG_EXPANDED != 0 {
                    self.skip();
                }
                if self.here().is_none() {
                    Eol
                } else if self.starts_with(br"\)") {
                    self.info |= REG_UUNSPEC;
                    Eol
                } else {
                    Plain(chr)
                }
            }
            Some(b'\\') => {
                let escaped = self.take().ok_or(RegErrorCode::EEscape)?;
                match u8::try_from(escaped).ok() {
                    Some(b'{') => {
                        self.info |= REG_UBOUNDS;
                        self.context = Context::BreBound;
                        OpenBound
                    }
                    Some(b'(') => Open(true),
                    Some(b')') => Close,
                    Some(b'<' | b'>') => {
                        self.info |= REG_UNONPOSIX;
                        if escaped == 60 { WordBegin } else { WordEnd }
                    }
                    Some(b'1'..=b'9') => {
                        self.info |= REG_UBACKREF;
                        Backref((escaped - 48) as usize)
                    }
                    _ => self.posix_escape(escaped),
                }
            }
            _ => Plain(chr),
        })
    }

    fn lexescape(&mut self) -> Result<Token, RegErrorCode> {
        use Token::*;
        let chr = self.take().ok_or(RegErrorCode::EEscape)?;
        let Ok(byte) = u8::try_from(chr) else {
            return Ok(Plain(chr));
        };
        if !byte.is_ascii_alphanumeric() {
            return Ok(Plain(chr));
        }
        self.info |= REG_UNONPOSIX;
        Ok(match byte {
            b'a' => {
                self.info |= REG_ULOCALE;
                Plain(7)
            }
            b'A' => Bos,
            b'b' => Plain(8),
            b'B' => Plain(92),
            b'c' => {
                self.info |= REG_UUNPORT;
                Plain(self.take().ok_or(RegErrorCode::EEscape)? & 31)
            }
            b'd' | b'D' | b's' | b'S' | b'w' | b'W' => {
                self.info |= REG_ULOCALE;
                let class = match byte.to_ascii_lowercase() {
                    b'd' => regguts::Class::Digit,
                    b's' => regguts::Class::Space,
                    _ => regguts::Class::Word,
                };
                Class(class, byte.is_ascii_lowercase())
            }
            b'e' => {
                self.info |= REG_UUNPORT | REG_ULOCALE;
                Plain(27)
            }
            b'f' => Plain(12),
            b'm' => WordBegin,
            b'M' => WordEnd,
            b'n' => Plain(10),
            b'r' => Plain(13),
            b't' => Plain(9),
            b'v' => Plain(11),
            b'u' | b'U' | b'x' => {
                let (min, max) = match byte {
                    b'u' => (4, 4),
                    b'U' => (8, 8),
                    _ => {
                        self.info |= REG_UUNPORT;
                        (1, 255)
                    }
                };
                let c = self.lexdigits(16, min, max)?;
                if c > CHR_MAX {
                    return Err(RegErrorCode::EEscape);
                }
                Plain(c)
            }
            b'y' | b'Y' => {
                self.info |= REG_ULOCALE;
                if byte == b'y' {
                    WordBoundary
                } else {
                    NotWordBoundary
                }
            }
            b'Z' => EosAnchor,
            b'0'..=b'9' => {
                let after_first = self.pos;
                self.pos -= 1;
                if byte != b'0' {
                    let c = self.lexdigits(10, 1, 255)? as usize;
                    if self.pos == after_first || (c > 0 && c <= self.nsub) {
                        self.info |= REG_UBACKREF;
                        return Ok(Backref(c));
                    }
                    self.pos = after_first - 1;
                }
                self.info |= REG_UUNPORT;
                let mut c = self.lexdigits(8, 1, 3)?;
                if c > 255 {
                    self.pos -= 1;
                    c >>= 3;
                }
                Plain(c)
            }
            _ => return Err(RegErrorCode::EEscape),
        })
    }

    fn lexdigits(&mut self, radix: u32, min: usize, max: usize) -> Result<Chr, RegErrorCode> {
        let start = self.pos;
        let mut value = 0u32;
        while self.pos < start + max {
            let Some(digit) = self
                .here()
                .and_then(char::from_u32)
                .and_then(|c| c.to_digit(radix))
            else {
                break;
            };
            value = value.wrapping_mul(radix).wrapping_add(digit);
            self.pos += 1;
        }
        if self.pos - start < min {
            return Err(RegErrorCode::EEscape);
        }
        Ok(value)
    }
}
