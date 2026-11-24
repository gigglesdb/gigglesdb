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
 * Translated to Rust from regcomp.c.
 */

use std::collections::BTreeSet;
use std::sync::Arc as Shared;

use super::regc_color::Colormap;
use super::regc_cvec::{Cvec, CvecSpace};
use super::regc_lex::{Lexer, Token};
use super::regc_locale::{allcases, cclasscvec, eclass, element, lookupcclass, range};
use super::regc_nfa::{Arc, FINAL, INIT, Nfa};
use super::regguts::*;
use super::*;

pub(super) fn compile(
    pattern: &[Chr],
    flags: i32,
    locale: RegexLocale,
    cancel: Option<CancelCheck<'_>>,
) -> Result<Regex, RegErrorCode> {
    if (flags & REG_QUOTE != 0 && flags & (REG_ADVANCED | REG_EXPANDED | REG_NEWLINE) != 0)
        || flags & REG_ADVANCED == REG_ADVF
        || pattern.iter().any(|&chr| chr > CHR_MAX)
    {
        return Err(RegErrorCode::InvArg);
    }
    let mut parser = Compiler {
        lex: Lexer::new(pattern, flags, locale),
        nfa: Nfa::new(cancel, 0)?,
        cmap: Colormap::new(locale),
        cvec_space: CvecSpace::default(),
        nlcolor: None,
        wordchrs: None,
        nodes: Vec::new(),
        subs: vec![None],
        lacons: Vec::new(),
        cancel,
    };
    parser.lex.start()?;
    if parser.lex.flags & REG_NEWLINE != 0 {
        parser.nlcolor = Some(parser.cmap.subcolor(10)?);
        parser.nfa.okcolors(&mut parser.cmap)?;
    }
    let root = parser.parse()?;
    parser.finish(root)
}

// With parseqatom inlined into parsebranch, a group adds a parse and a
// parsebranch frame. A suspended branch tail adds only parsebranch.
const PARSE_GROUP_FRAME: usize = 256;
const PARSE_TAIL_FRAME: usize = 160;
const PARSE_STACK_LIMIT: usize = 2_090_256;
const PARSE_TAIL_OVERHEAD: usize = 16;

struct Compiler<'a> {
    lex: Lexer<'a>,
    nfa: Nfa<'a>,
    cmap: Colormap,
    cvec_space: CvecSpace,
    nlcolor: Option<Color>,
    wordchrs: Option<StateId>,
    nodes: Vec<Node>,
    subs: Vec<Option<SubreId>>,
    lacons: Vec<(Look, StateId, StateId)>,
    cancel: Option<CancelCheck<'a>>,
}

struct Node {
    op: u8,
    flags: u8,
    capno: usize,
    backno: usize,
    min: usize,
    max: usize,
    begin: StateId,
    end: StateId,
    child: Option<SubreId>,
    sibling: Option<SubreId>,
}

struct Frame {
    begin: StateId,
    end: StateId,
    group: Option<Group>,
    in_lacon: bool,
    branches: Vec<SubreId>,
    branch: SubreId,
    seen_content: bool,
    tails: Vec<Tail>,
}

struct Group {
    left: StateId,
    capture: usize,
    look: Option<Look>,
}

// parseqatom suspends here while parsebranch handles the remainder of a branch.
struct Tail {
    top: SubreId,
    rest: SubreId,
    prefix: SubreId,
    atom: SubreId,
}

impl<'a> Compiler<'a> {
    fn subre(&mut self, op: u8, flags: u8, begin: StateId, end: StateId) -> SubreId {
        let id = self.nodes.len();
        self.nodes.push(Node {
            op,
            flags,
            begin,
            end,
            capno: 0,
            backno: 0,
            min: 1,
            max: 1,
            child: None,
            sibling: None,
        });
        id
    }

    fn frame(
        &mut self,
        begin: StateId,
        end: StateId,
        group: Option<Group>,
        in_lacon: bool,
    ) -> Result<Frame, RegErrorCode> {
        let branch = self.branch(begin, end)?;
        Ok(Frame {
            begin,
            end,
            group,
            in_lacon,
            branch,
            branches: Vec::new(),
            seen_content: false,
            tails: Vec::new(),
        })
    }

    fn branch(&mut self, begin: StateId, end: StateId) -> Result<SubreId, RegErrorCode> {
        let left = self.nfa.newstate()?;
        let right = self.nfa.newstate()?;
        self.nfa.empty(begin, left)?;
        self.nfa.empty(right, end)?;
        Ok(self.subre(b'=', 0, left, right))
    }

    fn branch_end(&self, frame: &Frame) -> bool {
        matches!(self.lex.token, Token::Alt | Token::Eos)
            || (frame.group.is_some() && self.lex.token == Token::Close)
    }

    fn parse(&mut self) -> Result<SubreId, RegErrorCode> {
        let mut frames = vec![self.frame(INIT, FINAL, None, false)?];
        let mut tails = 0;
        loop {
            let groups = frames.len() - 1;
            self.nfa.stack_used = groups * PARSE_GROUP_FRAME + tails * PARSE_TAIL_FRAME;
            let overhead = usize::from(tails != 0) * PARSE_TAIL_OVERHEAD;
            if self.nfa.stack_used + overhead > PARSE_STACK_LIMIT {
                return Err(RegErrorCode::ETooBig);
            }
            let frame = frames.last_mut().unwrap();
            if self.branch_end(frame) {
                tails -= frame.tails.len();
                self.close_branch(frame)?;
                if self.lex.token == Token::Alt {
                    self.lex.advance()?;
                    frame.branch = self.branch(frame.begin, frame.end)?;
                    frame.seen_content = false;
                    continue;
                }

                let frame = frames.pop().unwrap();
                let root = self.close_frame(&frame);
                let Some(group) = frame.group else {
                    return Ok(root);
                };
                if self.lex.token != Token::Close {
                    return Err(RegErrorCode::EParen);
                }
                self.lex.advance()?;
                self.nfa.stack_used = (groups - 1) * PARSE_GROUP_FRAME + tails * PARSE_TAIL_FRAME;
                let parent = frames.last_mut().unwrap();
                let right = self.nodes[parent.branch].end;
                if let Some(kind) = group.look {
                    self.processlacon(frame.begin, frame.end, kind, group.left, right)?;
                } else {
                    let mut atom = root;
                    if group.capture != 0 {
                        if self.nodes[atom].capno != 0 {
                            let flags = self.nodes[atom].flags;
                            let wrapper = self.subre(b'(', flags, frame.begin, frame.end);
                            self.nodes[wrapper].child = Some(atom);
                            atom = wrapper;
                        }
                        self.nodes[atom].flags |= CAP;
                        self.nodes[atom].capno = group.capture;
                        self.subs[group.capture] = Some(atom);
                    }
                    if self.quantify(parent, group.left, Some(atom), group.capture != 0, None)? {
                        tails += 1;
                    }
                }
                continue;
            }

            let frame = frames.last_mut().unwrap();
            let branch = &self.nodes[frame.branch];
            let (left, right) = if frame.seen_content {
                let left = self.nfa.newstate()?;
                self.nfa.moveins(branch.end, left)?;
                (left, branch.end)
            } else {
                (branch.begin, branch.end)
            };
            frame.seen_content = true;

            match self.lex.token {
                Token::Open(capture) => {
                    let capture = if capture && !frame.in_lacon {
                        self.lex.nsub += 1;
                        self.subs.push(None);
                        self.lex.nsub
                    } else {
                        0
                    };
                    self.lex.advance()?;
                    let begin = self.nfa.newstate()?;
                    let end = self.nfa.newstate()?;
                    self.nfa.empty(left, begin)?;
                    self.nfa.empty(end, right)?;
                    let group = Group {
                        left,
                        capture,
                        look: None,
                    };
                    let child = self.frame(begin, end, Some(group), frame.in_lacon)?;
                    frames.push(child);
                }
                Token::Lookaround(kind) => {
                    self.lex.advance()?;
                    let begin = self.nfa.newstate()?;
                    let end = self.nfa.newstate()?;
                    let group = Group {
                        left,
                        capture: 0,
                        look: Some(kind),
                    };
                    frames.push(self.frame(begin, end, Some(group), true)?);
                }
                _ => {
                    if self.parseqatom(frame, left, right)? {
                        tails += 1;
                    }
                }
            }
        }
    }

    fn close_branch(&mut self, frame: &mut Frame) -> Result<(), RegErrorCode> {
        let mut top = frame.branch;
        if !frame.seen_content {
            if frame.tails.is_empty() {
                self.lex.info |= REG_UUNSPEC;
            }
            self.nfa.empty(self.nodes[top].begin, self.nodes[top].end)?;
        }
        for tail in frame.tails.drain(..).rev() {
            self.nodes[tail.atom].sibling = Some(top);
            self.nodes[tail.rest].flags |=
                combine(self.nodes[tail.rest].flags, self.nodes[top].flags);
            self.nodes[tail.top].flags |=
                combine(self.nodes[tail.top].flags, self.nodes[tail.rest].flags);
            if self.nodes[tail.prefix].begin == self.nodes[tail.prefix].end {
                self.nodes[tail.top].child = Some(tail.atom);
            } else if self.nodes[tail.atom].op == b'='
                && self.nodes[top].op == b'='
                && up(self.nodes[tail.atom].flags | self.nodes[top].flags) & MESSY == 0
            {
                self.nodes[tail.rest].op = b'=';
                self.nodes[tail.rest].flags =
                    combine(self.nodes[tail.atom].flags, self.nodes[top].flags);
                self.nodes[tail.rest].child = None;
            }
            top = tail.top;
        }
        frame.branches.push(top);
        Ok(())
    }

    fn close_frame(&mut self, frame: &Frame) -> SubreId {
        let children = &frame.branches;
        if children.len() == 1 {
            return children[0];
        }
        let mut flags = LONGER;
        for &child in children {
            flags |= up(flags | self.nodes[child].flags);
        }
        let op = if flags & MESSY != 0 { b'|' } else { b'=' };
        let root = self.subre(op, flags, frame.begin, frame.end);
        if op == b'|' {
            self.nodes[root].child = Some(children[0]);
            for pair in children.windows(2) {
                self.nodes[pair[0]].sibling = Some(pair[1]);
            }
        }
        root
    }

    fn parseqatom(
        &mut self,
        frame: &mut Frame,
        left: StateId,
        right: StateId,
    ) -> Result<bool, RegErrorCode> {
        use Token::*;
        let mut backref = None;
        let mut atom = None;
        match self.lex.token {
            Bol | Eol | Bos | EosAnchor => {
                let at_start = matches!(self.lex.token, Bol | Bos);
                let absolute = matches!(self.lex.token, Bos | EosAnchor);
                let anchor = if at_start { Arc::Bol } else { Arc::Eol };
                self.nfa.add(anchor(1), left, right)?;
                if absolute {
                    self.nfa.add(anchor(0), left, right)?;
                } else if self.lex.flags & REG_NLANCH != 0 {
                    let newline = self.nlcolor.unwrap();
                    let arc = if at_start {
                        Arc::Behind(newline)
                    } else {
                        Arc::Ahead(newline)
                    };
                    self.nfa.add(arc, left, right)?;
                }
                self.lex.advance()?;
                return Ok(false);
            }
            WordBegin | WordEnd | WordBoundary | NotWordBoundary => {
                self.word_boundary(self.lex.token, left, right)?;
                self.lex.advance()?;
                return Ok(false);
            }
            Star(_) | Plus(_) | Question(_) | OpenBound => return Err(RegErrorCode::BadRpt),
            Close if self.lex.flags & REG_ADVANCED == REG_EXTENDED => {
                self.lex.info |= REG_UPBOTCH;
                self.onechr(u32::from(b')'), left, right)?;
                self.nfa.okcolors(&mut self.cmap)?;
            }
            Close => return Err(RegErrorCode::EParen),
            Plain(chr) => {
                self.onechr(chr, left, right)?;
                self.nfa.okcolors(&mut self.cmap)?;
            }
            Dot => {
                if self.lex.flags & REG_NLSTOP == 0 {
                    self.nfa.add(Arc::Plain(RAINBOW), left, right)?;
                } else {
                    let nl = self.nlcolor.unwrap();
                    self.color_arcs(
                        self.cmap.real_colors().filter(|&c| c != nl).collect(),
                        Arc::Plain,
                        left,
                        right,
                    )?;
                }
            }
            OpenBracket(positive) => {
                if positive {
                    self.bracket(left, right)?;
                } else {
                    self.cbracket(left, right)?;
                }
            }
            Class(class, positive) => {
                if positive {
                    self.charclass(class, left, right)?;
                    self.nfa.okcolors(&mut self.cmap)?;
                } else {
                    self.charclasscomplement(class, left, right)?;
                }
            }
            Backref(number) => {
                if frame.in_lacon {
                    return Err(RegErrorCode::ESubreg);
                }
                let target = self
                    .subs
                    .get(number)
                    .copied()
                    .flatten()
                    .ok_or(RegErrorCode::ESubreg)?;
                self.nodes[target].flags |= BRUSE;
                let node = self.subre(b'b', BACKR, left, right);
                self.nodes[node].backno = number;
                self.nfa.empty(left, right)?;
                atom = Some(node);
                backref = Some(target);
            }
            _ => return Err(RegErrorCode::Assert),
        }
        self.lex.advance()?;
        self.quantify(frame, left, atom, false, backref)
    }

    fn scannum(&mut self) -> Result<usize, RegErrorCode> {
        let mut n = 0;
        while let Token::Digit(digit) = self.lex.token {
            if n >= DUPMAX {
                break;
            }
            n = n * 10 + digit;
            self.lex.advance()?;
        }
        if n > DUPMAX || matches!(self.lex.token, Token::Digit(_)) {
            return Err(RegErrorCode::BadBr);
        }
        Ok(n)
    }

    fn bounds(&mut self) -> Result<(usize, usize, u8), RegErrorCode> {
        let preference = |greedy| if greedy { LONGER } else { SHORTER };
        let result = match self.lex.token {
            Token::Star(greedy) => (0, DUPINF, preference(greedy)),
            Token::Plus(greedy) => (1, DUPINF, preference(greedy)),
            Token::Question(greedy) => (0, 1, preference(greedy)),
            Token::OpenBound => {
                self.lex.advance()?;
                let min = self.scannum()?;
                if self.lex.token == Token::Comma {
                    self.lex.advance()?;
                    let max = if matches!(self.lex.token, Token::Digit(_)) {
                        self.scannum()?
                    } else {
                        DUPINF
                    };
                    match self.lex.token {
                        Token::CloseBound(greedy) if min <= max => (min, max, preference(greedy)),
                        _ => return Err(RegErrorCode::BadBr),
                    }
                } else if matches!(self.lex.token, Token::CloseBound(_)) {
                    (min, min, 0)
                } else {
                    return Err(RegErrorCode::BadBr);
                }
            }
            _ => return Ok((1, 1, 0)),
        };
        self.lex.advance()?;
        Ok(result)
    }

    fn quantify(
        &mut self,
        frame: &mut Frame,
        left: StateId,
        atom: Option<SubreId>,
        capture: bool,
        backref: Option<SubreId>,
    ) -> Result<bool, RegErrorCode> {
        let top = frame.branch;
        let right = self.nodes[top].end;
        let (min, max, prefer) = self.bounds()?;
        if max == 0 {
            if let Some(atom) = atom.filter(|&id| self.nodes[id].flags & CAP != 0) {
                self.nfa.delsub(left, self.nodes[atom].begin)?;
                self.nfa.delsub(self.nodes[atom].end, right)?;
            } else {
                self.nfa.delsub(left, right)?;
            }
            self.nfa.empty(left, right)?;
            return Ok(false);
        }

        let flags = self.nodes[top].flags | prefer | atom.map_or(0, |id| self.nodes[id].flags);
        if !capture && backref.is_none() && up(flags) & MESSY == 0 {
            self.repeat(left, right, min, max)?;
            self.nodes[top].flags = flags;
            return Ok(false);
        }

        let atom = atom.unwrap_or_else(|| self.subre(b'=', 0, left, right));
        if self.nodes[atom].begin == left || self.nodes[atom].end == right {
            let begin = self.nfa.newstate()?;
            let end = self.nfa.newstate()?;
            self.nfa.moveouts(left, begin)?;
            self.nfa.moveins(right, end)?;
            self.nodes[atom].begin = begin;
            self.nodes[atom].end = end;
        } else {
            self.nfa.delsub(left, self.nodes[atom].begin)?;
            self.nfa.delsub(self.nodes[atom].end, right)?;
        }

        let begin = self.nodes[atom].begin;
        let end = self.nodes[atom].end;
        let start = self.nfa.newstate()?;
        self.nfa.empty(left, start)?;

        let rest_flags = combine(prefer, self.nodes[atom].flags);
        let rest = self.subre(b'.', rest_flags, left, right);
        let prefix = self.subre(b'=', self.nodes[top].flags, self.nodes[top].begin, left);
        self.nodes[top].op = b'.';
        self.nodes[top].child = Some(prefix);
        self.nodes[prefix].sibling = Some(rest);

        let (atom, after) = if let Some(target) = backref {
            self.nfa.delsub(begin, end)?;
            self.nfa
                .dupnfa(self.nodes[target].begin, self.nodes[target].end, begin, end)?;
            self.nfa.removeconstraints(begin, end)?;
            self.nfa.empty(start, begin)?;
            self.repeat(begin, end, min, max)?;
            self.nodes[atom].min = min;
            self.nodes[atom].max = max;
            self.nodes[atom].flags |= rest_flags;
            (atom, end)
        } else if min == 1
            && max == 1
            && (prefer == 0
                || self.nodes[atom].flags & (PREF | MIXED) == 0
                || prefer == self.nodes[atom].flags & (PREF | MIXED))
        {
            self.nfa.empty(start, begin)?;
            (atom, end)
        } else if self.nodes[atom].flags & (CAP | BACKR) == 0 {
            self.nfa.empty(start, begin)?;
            self.repeat(begin, end, min, max)?;
            (self.subre(b'=', rest_flags, begin, end), end)
        } else if min > 0 && self.nodes[atom].flags & BACKR == 0 {
            self.nfa.dupnfa(begin, end, start, begin)?;
            self.repeat(
                start,
                begin,
                min - 1,
                if max == DUPINF { max } else { max - 1 },
            )?;
            let concat = self.subre(b'.', rest_flags, start, end);
            let previous = self.subre(b'=', rest_flags & PREF, start, begin);
            self.nodes[concat].child = Some(previous);
            self.nodes[previous].sibling = Some(atom);
            (concat, end)
        } else {
            let after = self.nfa.newstate()?;
            self.nfa.moveouts(end, after)?;
            self.nfa.dupnfa(begin, end, start, after)?;
            self.repeat(start, after, min, max)?;
            let iter = self.subre(b'*', rest_flags, start, after);
            self.nodes[iter].min = min;
            self.nodes[iter].max = max;
            self.nodes[iter].child = Some(atom);
            (iter, after)
        };
        self.nodes[rest].child = Some(atom);

        if self.branch_end(frame) {
            self.nfa.empty(after, right)?;
            self.nodes[prefix].sibling = Some(atom);
            self.nodes[top].flags |= combine(self.nodes[top].flags, self.nodes[atom].flags);
            if self.nodes[prefix].begin == self.nodes[prefix].end {
                frame.branch = atom;
            }
            Ok(false)
        } else {
            frame.tails.push(Tail {
                top,
                rest,
                prefix,
                atom,
            });
            frame.branch = self.subre(b'=', 0, after, right);
            frame.seen_content = false;
            Ok(true)
        }
    }

    fn repeat(
        &mut self,
        left: StateId,
        right: StateId,
        min: usize,
        max: usize,
    ) -> Result<(), RegErrorCode> {
        let caller = self.nfa.stack_used;
        let result = self.repeat_fragment(left, right, min, max);
        self.nfa.stack_used = caller;
        result
    }

    fn repeat_fragment(
        &mut self,
        left: StateId,
        mut right: StateId,
        mut min: usize,
        mut max: usize,
    ) -> Result<(), RegErrorCode> {
        const REPEAT_FRAME: usize = 80;
        let mut optional = Vec::new();
        loop {
            match (min, max) {
                (0, 0) => {
                    self.nfa.delsub(left, right)?;
                    self.nfa.empty(left, right)?;
                    break;
                }
                (0, 1) => {
                    self.nfa.empty(left, right)?;
                    break;
                }
                (0, DUPINF) | (1, DUPINF) => {
                    let start = self.nfa.newstate()?;
                    let end = if min == 0 {
                        start
                    } else {
                        self.nfa.newstate()?
                    };
                    self.nfa.moveouts(left, start)?;
                    self.nfa.moveins(right, end)?;
                    self.nfa.empty(left, start)?;
                    self.nfa.empty(end, right)?;
                    if min == 1 {
                        self.nfa.empty(end, start)?;
                    }
                    break;
                }
                (1, 1) => break,
                (0, _) => {
                    optional.push(right);
                    self.nfa.stack_used += REPEAT_FRAME;
                    min = 1;
                }
                _ => {
                    let next = self.nfa.newstate()?;
                    self.nfa.moveouts(left, next)?;
                    self.nfa.dupnfa(next, right, left, next)?;
                    if min == 1 {
                        optional.push(next);
                        self.nfa.stack_used += REPEAT_FRAME;
                    } else {
                        min -= 1;
                    }
                    if max != DUPINF {
                        max -= 1;
                    }
                    right = next;
                }
            }
        }
        for end in optional.into_iter().rev() {
            self.nfa.empty(left, end)?;
        }
        Ok(())
    }

    fn color_arcs(
        &mut self,
        colors: Vec<Color>,
        arc: fn(Color) -> Arc,
        left: StateId,
        right: StateId,
    ) -> Result<(), RegErrorCode> {
        for color in colors {
            self.nfa.add(arc(color), left, right)?;
        }
        Ok(())
    }

    fn add_cvec(&mut self, cv: Cvec, left: StateId, right: StateId) -> Result<(), RegErrorCode> {
        for color in self.cmap.subcolorcvec(cv)? {
            self.nfa.add(Arc::Plain(color), left, right)?;
        }
        Ok(())
    }

    fn onechr(&mut self, chr: Chr, left: StateId, right: StateId) -> Result<(), RegErrorCode> {
        let cv = if self.lex.flags & REG_ICASE == 0 {
            Cvec::single(chr)
        } else {
            allcases(&mut self.cvec_space, self.lex.locale, chr)
        };
        self.add_cvec(cv, left, right)
    }

    fn charclass(
        &mut self,
        class: Class,
        left: StateId,
        right: StateId,
    ) -> Result<(), RegErrorCode> {
        self.lex.info |= REG_ULOCALE;
        let cv = cclasscvec(
            &mut self.cvec_space,
            self.lex.locale,
            class,
            self.lex.flags & REG_ICASE != 0,
        );
        self.add_cvec(cv, left, right)
    }

    fn colorcomplement(
        &mut self,
        from: StateId,
        arc: fn(Color) -> Arc,
        left: StateId,
        right: StateId,
    ) -> Result<(), RegErrorCode> {
        let original: BTreeSet<_> = self.nfa.colors(from).collect();
        if original.contains(&RAINBOW) {
            return self.nfa.add(Arc::CantMatch, left, right);
        }
        let complement = self
            .cmap
            .real_colors()
            .filter(|c| !original.contains(c))
            .collect();
        self.color_arcs(complement, arc, left, right)
    }

    fn charclasscomplement(
        &mut self,
        class: Class,
        left: StateId,
        right: StateId,
    ) -> Result<(), RegErrorCode> {
        let tmp = self.nfa.newstate()?;
        self.charclass(class, tmp, tmp)?;
        self.nfa.okcolors(&mut self.cmap)?;
        self.colorcomplement(tmp, Arc::Plain, left, right)?;
        self.nfa.dropstate(tmp);
        Ok(())
    }

    fn bracket(&mut self, left: StateId, right: StateId) -> Result<(), RegErrorCode> {
        let mut complements = [false; Class::ALL.len()];
        self.lex.advance()?;
        while self.lex.token != Token::CloseBracket {
            self.brackpart(left, right, &mut complements)?;
        }
        self.nfa.okcolors(&mut self.cmap)?;
        for class in Class::ALL {
            if complements[class as usize] {
                self.charclasscomplement(class, left, right)?;
            }
        }
        if complements.into_iter().any(|present| present)
            && self.nfa.colors(left).count() == self.cmap.real_colors().count()
        {
            self.nfa.delsub(left, right)?;
            self.nfa.add(Arc::Plain(RAINBOW), left, right)?;
        }
        Ok(())
    }

    fn cbracket(&mut self, left: StateId, right: StateId) -> Result<(), RegErrorCode> {
        let temp_left = self.nfa.newstate()?;
        let temp_right = self.nfa.newstate()?;
        self.bracket(temp_left, temp_right)?;
        if self.lex.flags & REG_NLSTOP != 0 {
            self.nfa
                .add(Arc::Plain(self.nlcolor.unwrap()), temp_left, temp_right)?;
        }
        self.colorcomplement(temp_left, Arc::Plain, left, right)?;
        self.nfa.dropstate(temp_left);
        self.nfa.dropstate(temp_right);
        Ok(())
    }

    fn brackpart(
        &mut self,
        left: StateId,
        right: StateId,
        complements: &mut [bool],
    ) -> Result<(), RegErrorCode> {
        use Token::*;
        let start = match self.lex.token {
            Range => return Err(RegErrorCode::ERange),
            Plain(chr) => {
                self.lex.advance()?;
                if self.lex.token != Range {
                    return self.onechr(chr, left, right);
                }
                chr
            }
            CollatingElement => self.scan_element()?,
            EquivalenceClass => {
                let chr = self.scan_element()?;
                let cv = eclass(&mut self.cvec_space, self.lex.locale, chr, self.lex.flags);
                return self.add_cvec(cv, left, right);
            }
            NamedClass => {
                let name = self.scanplain()?;
                return self.charclass(lookupcclass(&name)?, left, right);
            }
            Class(class, positive) => {
                if positive {
                    self.charclass(class, left, right)?;
                } else {
                    complements[class as usize] = true;
                }
                return self.lex.advance();
            }
            _ => return Err(RegErrorCode::Assert),
        };
        let end = if self.lex.token == Range {
            self.lex.advance()?;
            match self.lex.token {
                Plain(chr) => {
                    self.lex.advance()?;
                    chr
                }
                Range => {
                    self.lex.advance()?;
                    u32::from(b'-')
                }
                CollatingElement => self.scan_element()?,
                _ => return Err(RegErrorCode::ERange),
            }
        } else {
            start
        };
        if start != end {
            self.lex.info |= REG_UUNPORT;
        }
        let cv = range(
            &mut self.cvec_space,
            self.lex.locale,
            start,
            end,
            self.lex.flags & REG_ICASE != 0,
            self.cancel,
        )?;
        self.add_cvec(cv, left, right)
    }

    fn scanplain(&mut self) -> Result<Vec<Chr>, RegErrorCode> {
        self.lex.advance()?;
        let mut name = Vec::new();
        while let Token::Plain(chr) = self.lex.token {
            name.push(chr);
            self.lex.advance()?;
        }
        debug_assert_eq!(self.lex.token, Token::EndName);
        self.lex.advance()?;
        Ok(name)
    }

    fn scan_element(&mut self) -> Result<Chr, RegErrorCode> {
        let name = self.scanplain()?;
        if name.len() > 1 {
            self.lex.info |= REG_ULOCALE;
        }
        element(&name)
    }

    fn wordchrs(&mut self) -> Result<StateId, RegErrorCode> {
        if let Some(state) = self.wordchrs {
            return Ok(state);
        }
        let state = self.nfa.newstate()?;
        self.charclass(Class::Word, state, state)?;
        self.nfa.okcolors(&mut self.cmap)?;
        self.wordchrs = Some(state);
        Ok(state)
    }

    fn word_constraint(
        &mut self,
        wordchrs: StateId,
        ahead: bool,
        positive: bool,
        left: StateId,
        right: StateId,
    ) -> Result<(), RegErrorCode> {
        let arc = if ahead { Arc::Ahead } else { Arc::Behind };
        if positive {
            self.color_arcs(self.nfa.colors(wordchrs).collect(), arc, left, right)
        } else {
            let anchor = if ahead { Arc::Eol } else { Arc::Bol };
            self.nfa.add(anchor(1), left, right)?;
            self.nfa.add(anchor(0), left, right)?;
            self.colorcomplement(wordchrs, arc, left, right)
        }
    }

    fn word_boundary(
        &mut self,
        token: Token,
        left: StateId,
        right: StateId,
    ) -> Result<(), RegErrorCode> {
        let wordchrs = self.wordchrs()?;
        let choices: &[(bool, bool)] = match token {
            Token::WordBegin => &[(false, true)],
            Token::WordEnd => &[(true, false)],
            Token::WordBoundary => &[(false, true), (true, false)],
            Token::NotWordBoundary => &[(true, true), (false, false)],
            _ => unreachable!(),
        };
        for &(behind, ahead) in choices {
            let middle = self.nfa.newstate()?;
            self.word_constraint(wordchrs, false, behind, left, middle)?;
            self.word_constraint(wordchrs, true, ahead, middle, right)?;
        }
        Ok(())
    }

    fn processlacon(
        &mut self,
        begin: StateId,
        end: StateId,
        kind: Look,
        left: StateId,
        right: StateId,
    ) -> Result<(), RegErrorCode> {
        if let Some(state) = self.nfa.single_color_transition(begin, end) {
            // A single-color lookaround becomes a constraint on a neighboring transition.
            let arc = if kind.ahead() {
                Arc::Ahead
            } else {
                Arc::Behind
            };
            if kind.positive() {
                self.color_arcs(self.nfa.colors(state).collect(), arc, left, right)?;
            } else {
                self.colorcomplement(state, arc, left, right)?;
                let anchor = if kind.ahead() { Arc::Eol } else { Arc::Bol };
                self.nfa.add(anchor(1), left, right)?;
                self.nfa.add(anchor(0), left, right)?;
            }
        } else {
            self.lacons.push((kind, begin, end));
            // Each lookaround keeps its own NFA states. The compile-space
            // budget bounds their count well below u32::MAX.
            let number = u32::try_from(self.lacons.len()).map_err(|_| RegErrorCode::ETooBig)?;
            self.nfa.add(Arc::Lacon(number), left, right)?;
        }
        Ok(())
    }

    fn children(&self, node: SubreId) -> impl Iterator<Item = SubreId> + '_ {
        std::iter::successors(self.nodes[node].child, |&child| self.nodes[child].sibling)
    }

    fn tree_order(&self, root: SubreId) -> Vec<SubreId> {
        let mut order = Vec::new();
        let mut todo = vec![root];
        while let Some(node) = todo.pop() {
            order.push(node);
            let start = todo.len();
            todo.extend(self.children(node));
            todo[start..].reverse();
        }
        order
    }

    fn removecaptures(&mut self, root: SubreId) {
        for id in self.tree_order(root).into_iter().rev() {
            let child_captures = self
                .children(id)
                .any(|kid| self.nodes[kid].flags & CAP != 0);
            let node = &mut self.nodes[id];
            if node.flags & BRUSE == 0 {
                node.capno = 0;
                node.flags &= !CAP;
            }
            if child_captures {
                node.flags |= CAP;
            }
            if node.flags & (CAP | BACKR) == 0 {
                node.child = None;
                node.op = b'=';
                node.flags &= !MIXED;
            }
        }
    }

    fn finish(mut self, root: SubreId) -> Result<Regex, RegErrorCode> {
        self.nfa.release_color_index();
        let bos = [self.cmap.pseudocolor()?, self.cmap.pseudocolor()?];
        let eos = [self.cmap.pseudocolor()?, self.cmap.pseudocolor()?];
        if self.lex.flags & REG_NOSUB != 0 {
            self.removecaptures(root);
        }
        let order = self.tree_order(root);
        // Arena indices are zero-based preorder IDs. Wrappers for nested
        // captures can share their child's automaton.
        let mut ids = vec![0; self.nodes.len()];
        for (id, &old) in order.iter().enumerate() {
            ids[old] = id;
        }
        let mut tree = Vec::<Subre>::with_capacity(order.len());
        let mut info = self.lex.info;
        let ncolors = self.cmap.ncolors();
        let mut node_info = vec![0; self.nodes.len()];
        let mut depths = vec![0; self.nodes.len()];
        for &id in &order {
            let mut child = self.nodes[id].child;
            while let Some(next) = child {
                depths[next] = depths[id] + 1;
                child = self.nodes[next].sibling;
            }
        }
        for &old in order.iter().rev() {
            let node = &self.nodes[old];
            let (cnfa, analysis) = if node.op == b'(' {
                let kid = node.child.unwrap();
                let child = &tree[order.len() - 1 - ids[kid]];
                (Shared::clone(&child.cnfa), node_info[kid])
            } else {
                let mut nfa = self.nfa.nfanode(node.begin, node.end, depths[old])?;
                let (analysis, matchall) = nfa.optimize(&self.cmap, bos, eos)?;
                (
                    Shared::new(nfa.compact(ncolors, bos, eos, matchall)?),
                    analysis,
                )
            };
            node_info[old] = analysis;
            tree.push(Subre {
                op: node.op,
                flags: node.flags,
                capno: node.capno,
                backno: node.backno,
                min: node.min,
                max: node.max,
                cnfa,
                child: node.child.map(|child| ids[child]),
                sibling: node.sibling.map(|sibling| ids[sibling]),
            });
        }
        tree.reverse();
        info |= node_info[root];
        if self.nodes[root].flags & SHORTER != 0 {
            info |= REG_USHORTEST;
        }

        let mut lacons = Vec::with_capacity(self.lacons.len());
        for (kind, begin, end) in self.lacons {
            let mut nfa = self.nfa.nfanode(begin, end, 0)?;
            let (_, mut matchall) = nfa.optimize(&self.cmap, bos, eos)?;
            if !kind.ahead() && nfa.makesearch(bos)? {
                matchall = matchall.map(|(min, _)| (min, DUPINF));
            }
            lacons.push(Lacon {
                kind,
                cnfa: Shared::new(nfa.compact(ncolors, bos, eos, matchall)?),
            });
        }

        self.nfa.stack_used = 0;
        let (_, mut matchall) = self.nfa.optimize(&self.cmap, bos, eos)?;
        if self.nfa.makesearch(bos)? {
            matchall = matchall.map(|(min, _)| (min, DUPINF));
        }
        let search = self.nfa.compact(ncolors, bos, eos, matchall)?;
        Ok(Regex {
            guts: Guts {
                cmap: self.cmap,
                tree,
                root: 0,
                search,
                lacons,
            },
            nsub: self.lex.nsub,
            info,
            cflags: self.lex.flags,
            locale: self.lex.locale,
        })
    }
}
