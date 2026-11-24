/*
 * re_*exec and friends - match REs
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
 * Translated to Rust from regexec.c.
 *
 */

use super::regc_pg_locale::case_pair;
use super::rege_dfa::{Dfa, Match};
use super::regguts::{DUPINF, SHORTER, SubreId};
use super::{
    CancelCheck, Chr, REG_EXPECT, REG_ICASE, REG_NOSUB, REG_UBACKREF, REG_UIMPOSSIBLE,
    RegErrorCode, RegMatch, Regex, RmDetail,
};

pub(super) type ExecResult<T> = Result<T, RegErrorCode>;
const UNSET: RegMatch = RegMatch { so: -1, eo: -1 };
const SEARCH: usize = 0;
const ROOT: usize = 1;

pub(super) fn filled<T: Clone>(size: usize, value: T) -> ExecResult<Vec<T>> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(size)
        .map_err(|_| RegErrorCode::ESpace)?;
    result.resize(size, value);
    Ok(result)
}

pub(super) fn push<T>(stack: &mut Vec<T>, value: T) -> ExecResult<()> {
    stack.try_reserve(1).map_err(|_| RegErrorCode::ESpace)?;
    stack.push(value);
    Ok(())
}

fn offsets(begin: usize, end: usize) -> RegMatch {
    RegMatch {
        so: begin as isize,
        eo: end as isize,
    }
}

pub(super) fn exec(
    regex: &Regex,
    string: &[Chr],
    search_start: usize,
    details: Option<&mut RmDetail>,
    pmatch: &mut [RegMatch],
    eflags: i32,
    cancel: Option<CancelCheck<'_>>,
) -> ExecResult<bool> {
    if cancel.is_some_and(|check| check()) {
        return Err(RegErrorCode::Cancel);
    }
    if search_start > string.len() {
        return Ok(false);
    }
    if regex.cflags & REG_EXPECT != 0 && details.is_none() {
        return Err(RegErrorCode::InvArg);
    }
    if regex.info & REG_UIMPOSSIBLE != 0 {
        return Ok(false);
    }
    let backref = regex.info & REG_UBACKREF != 0;
    let internal = backref && pmatch.len() <= regex.nsub;
    let mut work = if internal {
        filled(regex.nsub + 1, UNSET)?
    } else {
        Vec::new()
    };
    let matches = if internal {
        &mut work
    } else {
        zapallsubs(pmatch);
        let len = pmatch.len().min(regex.nsub + 1);
        &mut pmatch[..len]
    };
    let count = 2 + regex.guts.tree.len() + regex.guts.lacons.len();
    let mut dfas = Vec::new();
    dfas.try_reserve_exact(count)
        .map_err(|_| RegErrorCode::ESpace)?;
    dfas.resize_with(count, || None);
    dfas[SEARCH] = Some(Dfa::new(&regex.guts.search, eflags)?);
    let mut exec = Executor {
        regex,
        string,
        matches,
        eflags,
        cancel,
        dfas,
        zap: Vec::new(),
        dissect_stack: 0,
    };
    let mut details = if regex.cflags & REG_EXPECT != 0 {
        details
    } else {
        None
    };
    let result = if backref {
        exec.cfind(search_start, &mut details)
    } else {
        exec.find(search_start, &mut details)
    }?;
    if result {
        if internal {
            pmatch.copy_from_slice(&work[..pmatch.len()]);
        }
        if regex.cflags & REG_NOSUB != 0 {
            zapallsubs(pmatch);
        }
    }
    Ok(result)
}

fn zapallsubs(matches: &mut [RegMatch]) {
    if let Some((_, subs)) = matches.split_first_mut() {
        subs.fill(UNSET);
    }
}

pub(super) struct Executor<'r, 'm> {
    pub(super) regex: &'r Regex,
    pub(super) string: &'r [Chr],
    pub(super) eflags: i32,
    pub(super) cancel: Option<CancelCheck<'r>>,
    pub(super) dissect_stack: usize,
    pub(super) dfas: Vec<Option<Dfa<'r>>>,
    matches: &'m mut [RegMatch],
    zap: Vec<SubreId>,
}

impl Executor<'_, '_> {
    fn getsubdfa(&mut self, node: SubreId) -> ExecResult<usize> {
        let index = 2 + node;
        if self.dfas[index].is_none() {
            let sub = &self.regex.guts.tree[node];
            let mut dfa = Dfa::new(&sub.cnfa, self.eflags)?;
            if sub.op == b'b' {
                dfa.backref = Some((sub.backno, sub.min, sub.max));
            }
            self.dfas[index] = Some(dfa);
        }
        Ok(index)
    }

    fn extend(&self, details: &mut Option<&mut RmDetail>, cold: Option<usize>) {
        if let Some(detail) = details {
            let stop = self.string.len();
            detail.extend = offsets(cold.unwrap_or(stop), stop);
        }
    }

    fn find(
        &mut self,
        search_start: usize,
        details: &mut Option<&mut RmDetail>,
    ) -> ExecResult<bool> {
        let root = self.regex.guts.root;
        let stop = self.string.len();
        let found = self.shortest(SEARCH, search_start, search_start, stop, true)?;
        self.dfas[SEARCH] = None;
        self.extend(details, found.cold);
        let Some(close) = found.end else {
            return Ok(false);
        };
        if self.matches.is_empty() {
            return Ok(true);
        }

        let open = found.cold.ok_or(RegErrorCode::Assert)?;
        let sub = &self.regex.guts.tree[root];
        self.dfas[ROOT] = Some(Dfa::new(&sub.cnfa, self.eflags)?);
        let shorter = sub.flags & SHORTER != 0;
        let mut cold = None;
        for begin in open..=close {
            let result = self.endpoint(shorter, begin, begin, stop)?;
            if result.hitstop {
                cold = cold.or(Some(begin));
            }
            if let Some(end) = result.end {
                self.dfas[ROOT] = None;
                self.matches[0] = offsets(begin, end);
                self.extend(details, cold);
                return if self.matches.len() == 1 {
                    Ok(true)
                } else {
                    self.cdissect(root, begin, end)
                };
            }
        }
        Err(RegErrorCode::Assert)
    }

    fn cfind(
        &mut self,
        search_start: usize,
        details: &mut Option<&mut RmDetail>,
    ) -> ExecResult<bool> {
        let root = self.regex.guts.root;
        self.dfas[ROOT] = Some(Dfa::new(&self.regex.guts.tree[root].cnfa, self.eflags)?);
        let (found, cold) = self.cfindloop(root, search_start)?;
        self.extend(details, cold);
        Ok(found)
    }

    fn cfindloop(&mut self, root: SubreId, mut start: usize) -> ExecResult<(bool, Option<usize>)> {
        let stop = self.string.len();
        let shorter = self.regex.guts.tree[root].flags & SHORTER != 0;
        let mut cold;
        loop {
            let found = self.shortest(SEARCH, start, start, stop, true)?;
            cold = found.cold;
            let Some(close) = found.end else { break };
            let open = cold.take().ok_or(RegErrorCode::Assert)?;
            for begin in open..=close {
                let mut estart = begin;
                let mut estop = stop;
                loop {
                    let result = self.endpoint(shorter, begin, estart, estop)?;
                    if result.hitstop {
                        cold = cold.or(Some(begin));
                    }
                    let Some(end) = result.end else { break };
                    if self.cdissect(root, begin, end)? {
                        self.matches[0] = offsets(begin, end);
                        return Ok((true, cold));
                    }
                    if shorter {
                        if end == estop {
                            break;
                        }
                        estart = end + 1;
                    } else {
                        if end == begin {
                            break;
                        }
                        estop = end - 1;
                    }
                }
            }
            if close >= stop.saturating_sub(1) {
                break;
            }
            start = close + 1;
        }
        Ok((false, cold))
    }

    fn endpoint(
        &mut self,
        shorter: bool,
        begin: usize,
        min: usize,
        max: usize,
    ) -> ExecResult<Match> {
        if shorter {
            self.shortest(ROOT, begin, min, max, false)
        } else {
            self.longest(ROOT, begin, max)
        }
    }

    fn zaptreesubs(&mut self, root: SubreId) -> ExecResult<()> {
        push(&mut self.zap, root)?;
        while let Some(node) = self.zap.pop() {
            let sub = &self.regex.guts.tree[node];
            if sub.capno != 0 && sub.capno < self.matches.len() {
                self.matches[sub.capno] = UNSET;
            }
            let mut child = sub.child;
            while let Some(id) = child {
                push(&mut self.zap, id)?;
                child = self.regex.guts.tree[id].sibling;
            }
        }
        Ok(())
    }

    pub(super) fn referent(&self, backno: usize) -> Option<&[Chr]> {
        let span = self.matches[backno];
        (span.so >= 0).then(|| &self.string[span.so as usize..span.eo as usize])
    }

    pub(super) fn backref_eq(&self, a: &[Chr], b: &[Chr]) -> bool {
        if self.regex.cflags & REG_ICASE == 0 {
            a == b
        } else {
            a.iter().zip(b).all(|(&x, &y)| {
                x == y || case_pair(self.regex.locale, x).1 == case_pair(self.regex.locale, y).1
            })
        }
    }

    fn cbrdissect(&self, node: SubreId, begin: usize, end: usize) -> bool {
        let sub = &self.regex.guts.tree[node];
        let Some(referent) = self.referent(sub.backno) else {
            return false;
        };
        let len = referent.len();
        let target = &self.string[begin..end];
        if len == 0 {
            return target.is_empty() && sub.min <= sub.max;
        }
        let mut chunks = target.chunks_exact(len);
        let reps = chunks.len();
        chunks.remainder().is_empty()
            && reps >= sub.min
            && (sub.max == DUPINF || reps <= sub.max)
            && chunks.all(|span| self.backref_eq(referent, span))
    }
}

#[derive(Clone, Copy)]
struct Piece {
    node: SubreId,
    begin: usize,
    end: usize,
}

struct Frame {
    piece: Piece,
    state: Dissect,
}

enum Dissect {
    Enter,
    Capture,
    Concat(Concat),
    Alt(Option<SubreId>),
    Iter(Iteration),
}

enum Action {
    Descend(Piece),
    Return(bool),
}

impl Piece {
    fn new(node: SubreId, begin: usize, end: usize) -> Self {
        Self { node, begin, end }
    }

    fn child(self, node: SubreId) -> Self {
        Self { node, ..self }
    }
}

impl Executor<'_, '_> {
    // The frames preserve the C routines' order of DFA checks, child
    // verification and capture clearing without using the native stack.
    fn cdissect(&mut self, root: SubreId, begin: usize, end: usize) -> ExecResult<bool> {
        let result = self.dissect(root, begin, end);
        self.dissect_stack = 0;
        result
    }

    fn dissect(&mut self, root: SubreId, begin: usize, end: usize) -> ExecResult<bool> {
        const CDISSECT_FRAME: usize = 144;
        const CDISSECT_LIMIT: usize = 14_278;
        let mut stack = Vec::new();
        push(
            &mut stack,
            Frame {
                piece: Piece::new(root, begin, end),
                state: Dissect::Enter,
            },
        )?;
        let mut returned = false;
        while !stack.is_empty() {
            let depth = stack.len();
            self.dissect_stack = depth * CDISSECT_FRAME;
            let frame = stack.last_mut().unwrap();
            let piece = frame.piece;
            let sub = &self.regex.guts.tree[piece.node];
            let action = match &mut frame.state {
                Dissect::Enter => {
                    if self.cancel.is_some_and(|check| check()) {
                        return Err(RegErrorCode::Cancel);
                    }
                    if depth > CDISSECT_LIMIT {
                        return Err(RegErrorCode::ETooBig);
                    }
                    match sub.op {
                        b'=' => Action::Return(true),
                        b'b' => Action::Return(self.cbrdissect(piece.node, piece.begin, piece.end)),
                        b'(' => {
                            frame.state = Dissect::Capture;
                            Action::Descend(piece.child(sub.child.ok_or(RegErrorCode::Assert)?))
                        }
                        b'.' | b'|' | b'*' => {
                            let child = sub.child.ok_or(RegErrorCode::Assert)?;
                            frame.state = match sub.op {
                                b'.' => Dissect::Concat(Concat::new(self, child, piece)?),
                                b'|' => Dissect::Alt(Some(child)),
                                b'*' => Dissect::Iter(Iteration::new(self, piece, child)?),
                                _ => unreachable!(),
                            };
                            returned = false;
                            continue;
                        }
                        _ => return Err(RegErrorCode::Assert),
                    }
                }
                Dissect::Capture => Action::Return(returned),
                Dissect::Concat(concat) => concat.ccondissect(self, piece, returned)?,
                Dissect::Alt(next) => self.caltdissect(piece, next, returned)?,
                Dissect::Iter(iter) => iter.citerdissect(self, piece, returned)?,
            };
            match action {
                Action::Descend(child) => {
                    push(
                        &mut stack,
                        Frame {
                            piece: child,
                            state: Dissect::Enter,
                        },
                    )?;
                }
                Action::Return(matched) => {
                    if matched {
                        let n = sub.capno;
                        if n != 0 && n < self.matches.len() {
                            self.matches[n] = offsets(piece.begin, piece.end);
                        }
                    }
                    returned = matched;
                    stack.pop();
                }
            }
        }
        Ok(returned)
    }

    fn caltdissect(
        &mut self,
        piece: Piece,
        next: &mut Option<SubreId>,
        matched: bool,
    ) -> ExecResult<Action> {
        if matched {
            return Ok(Action::Return(true));
        }
        while let Some(node) = next.take() {
            *next = self.regex.guts.tree[node].sibling;
            let dfa = self.getsubdfa(node)?;
            if self.longest(dfa, piece.begin, piece.end)?.end == Some(piece.end) {
                return Ok(Action::Descend(piece.child(node)));
            }
        }
        Ok(Action::Return(false))
    }
}

struct Concat {
    left: SubreId,
    right: SubreId,
    left_dfa: usize,
    right_dfa: usize,
    shorter: bool,
    mid: Option<usize>,
    phase: ConcatPhase,
}

enum ConcatPhase {
    Probe,
    Left,
    Right,
}

impl Concat {
    fn new(exec: &mut Executor<'_, '_>, left: SubreId, piece: Piece) -> ExecResult<Self> {
        let sub = &exec.regex.guts.tree[left];
        let right = sub.sibling.ok_or(RegErrorCode::Assert)?;
        let shorter = sub.flags & SHORTER != 0;
        let left_dfa = exec.getsubdfa(left)?;
        let right_dfa = exec.getsubdfa(right)?;
        let mid = if shorter {
            exec.shortest(left_dfa, piece.begin, piece.begin, piece.end, false)?
        } else {
            exec.longest(left_dfa, piece.begin, piece.end)?
        }
        .end;
        Ok(Self {
            left,
            right,
            left_dfa,
            right_dfa,
            shorter,
            mid,
            phase: ConcatPhase::Probe,
        })
    }

    // ccondissect and crevcondissect differ only in midpoint preference.
    fn ccondissect(
        &mut self,
        exec: &mut Executor<'_, '_>,
        piece: Piece,
        matched: bool,
    ) -> ExecResult<Action> {
        while let Some(mid) = self.mid {
            match self.phase {
                ConcatPhase::Probe => {
                    if exec.longest(self.right_dfa, mid, piece.end)?.end == Some(piece.end) {
                        self.phase = ConcatPhase::Left;
                        return Ok(Action::Descend(Piece::new(self.left, piece.begin, mid)));
                    }
                }
                ConcatPhase::Left if matched => {
                    self.phase = ConcatPhase::Right;
                    return Ok(Action::Descend(Piece::new(self.right, mid, piece.end)));
                }
                ConcatPhase::Left => {}
                ConcatPhase::Right if matched => return Ok(Action::Return(true)),
                ConcatPhase::Right => exec.zaptreesubs(self.left)?,
            }
            self.mid = if self.shorter {
                if mid == piece.end {
                    None
                } else {
                    exec.shortest(self.left_dfa, piece.begin, mid + 1, piece.end, false)?
                        .end
                }
            } else if mid == piece.begin {
                None
            } else {
                exec.longest(self.left_dfa, piece.begin, mid - 1)?.end
            };
            self.phase = ConcatPhase::Probe;
        }
        Ok(Action::Return(false))
    }
}

struct Iteration {
    child: SubreId,
    dfa: usize,
    shorter: bool,
    zero: bool,
    min: usize,
    max: usize,
    endpts: Vec<usize>,
    verified: usize,
    k: usize,
    limit: usize,
    verifying: bool,
}

impl Iteration {
    fn new(exec: &mut Executor<'_, '_>, piece: Piece, child: SubreId) -> ExecResult<Self> {
        let sub = &exec.regex.guts.tree[piece.node];
        let shorter = exec.regex.guts.tree[child].flags & SHORTER != 0;
        let zero = sub.min == 0 && piece.begin == piece.end;
        let min = sub.min.max(1);
        let len = piece.end - piece.begin;
        let max = if sub.max == DUPINF {
            len
        } else {
            len.min(sub.max)
        }
        .max(min);
        let (dfa, endpts) = if shorter && zero {
            (0, Vec::new())
        } else {
            let mut endpts = filled(max + 1, 0)?;
            endpts[0] = piece.begin;
            (exec.getsubdfa(child)?, endpts)
        };
        Ok(Self {
            child,
            dfa,
            shorter,
            zero,
            min,
            max,
            endpts,
            verified: 0,
            k: 1,
            limit: if shorter { piece.begin } else { piece.end },
            verifying: false,
        })
    }

    fn needs_empty(&self, at: usize, end: usize) -> bool {
        self.k < self.min && self.min - self.k >= end - at
    }

    fn verify(&mut self, exec: &mut Executor<'_, '_>) -> ExecResult<Action> {
        exec.zaptreesubs(self.child)?;
        self.verifying = true;
        Ok(Action::Descend(Piece::new(
            self.child,
            self.endpts[self.verified],
            self.endpts[self.verified + 1],
        )))
    }

    // citerdissect and creviterdissect share verification and differ in
    // their probes and the direction in which they retry endpoints.
    fn citerdissect(
        &mut self,
        exec: &mut Executor<'_, '_>,
        piece: Piece,
        matched: bool,
    ) -> ExecResult<Action> {
        if self.shorter && self.zero {
            return Ok(Action::Return(true));
        }
        if self.verifying {
            self.verifying = false;
            if matched {
                self.verified += 1;
                if self.verified == self.k {
                    return Ok(Action::Return(true));
                }
                return self.verify(exec);
            }
            self.k = self.verified + 1;
            self.backtrack(piece.end);
        }

        while self.k > 0 {
            let k = self.k;
            let start = self.endpts[k - 1];
            let end = piece.end;
            let found = if self.shorter {
                if self.limit == start && self.limit != end && !self.needs_empty(self.limit, end) {
                    self.limit += 1;
                }
                if k == self.max {
                    self.limit = end;
                }
                exec.shortest(self.dfa, start, self.limit, end, false)?
            } else {
                exec.longest(self.dfa, start, self.limit)?
            };
            if let Some(mid) = found.end {
                self.endpts[k] = mid;
                self.verified = self.verified.min(k - 1);
                if mid == end {
                    if k >= self.min {
                        return self.verify(exec);
                    }
                } else if k >= self.max {
                    self.k -= 1;
                } else if self.shorter || mid != start || self.needs_empty(mid, end) {
                    self.k += 1;
                    self.limit = if self.shorter { mid } else { end };
                    continue;
                }
            } else {
                self.k -= 1;
            }
            self.backtrack(end);
        }
        // The longest path prefers a real empty match over zero iterations,
        // so that captures can feed a subsequent backreference.
        Ok(Action::Return(self.zero))
    }

    fn backtrack(&mut self, end: usize) {
        while self.k > 0 {
            let prev = self.endpts[self.k - 1];
            let current = self.endpts[self.k];
            if self.shorter {
                if current < end {
                    self.limit = current + 1;
                    return;
                }
            } else if current > prev {
                let limit = current - 1;
                if limit > prev || self.needs_empty(prev, end) {
                    self.limit = limit;
                    return;
                }
            }
            self.k -= 1;
        }
    }
}
