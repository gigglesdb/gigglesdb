/*
 * DFA routines
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
 * Translated to Rust from rege_dfa.c.
 */

use super::regc_color::Colormap;
use super::regexec::{ExecResult, Executor, filled, push};
use super::regguts::{Cnfa, Color, DUPINF, RAINBOW};
use super::{CancelCheck, Chr, REG_NOTBOL, REG_NOTEOL, REG_SMALL, RegErrorCode};

const NONE: usize = usize::MAX;
const WORD_BITS: usize = u32::BITS as usize;
const STARTER: u8 = 1;
const POSTSTATE: u8 = 2;
const NOPROGRESS: u8 = 4;

#[derive(Clone, Copy)]
struct StateSet {
    hash: u32,
    flags: u8,
    ins: usize,
    lastseen: Option<usize>,
}

#[derive(Clone, Copy)]
struct Link {
    target: usize,
    next: usize,
}

// Links use flat indices: link / ncolors is the source state set and
// link % ncolors is the color. Each target chains all its incoming links.
pub(super) struct Dfa<'r> {
    cnfa: &'r Cnfa,
    pub(super) backref: Option<(usize, usize, usize)>,
    sets: Vec<StateSet>,
    states: Vec<u32>,
    links: Vec<Link>,
    work: Vec<u32>,
    nssets: usize,
    wordsper: usize,
    lastpost: Option<usize>,
    lastnopr: Option<usize>,
    search: usize,
    restart: Option<(usize, Option<usize>)>,
}

fn test(bits: &[u32], state: usize) -> bool {
    bits[state / WORD_BITS] & (1 << (state % WORD_BITS)) != 0
}

fn set(bits: &mut [u32], state: usize) {
    bits[state / WORD_BITS] |= 1 << (state % WORD_BITS);
}

fn members(words: &[u32]) -> impl Iterator<Item = usize> + '_ {
    words.iter().enumerate().flat_map(|(i, &word)| {
        let mut remaining = word;
        std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            let bit = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            Some(i * WORD_BITS + bit)
        })
    })
}

fn hash(words: &[u32]) -> u32 {
    words.iter().fold(0, |hash, word| hash ^ word)
}

impl<'r> Dfa<'r> {
    pub(super) fn new(cnfa: &'r Cnfa, eflags: i32) -> ExecResult<Self> {
        let nstates = cnfa.no_progress.len();
        let nss = nstates * 2;
        let wordsper = nstates.div_ceil(WORD_BITS);
        let max = i32::MAX as usize;
        if wordsper >= max / (nss + 1) || cnfa.ncolors >= max / nss {
            return Err(RegErrorCode::ETooBig);
        }
        let nssets = if eflags & REG_SMALL != 0 { 7 } else { nss };
        let capacity = nss.max(nssets);
        let mut sets = Vec::new();
        let mut states = Vec::new();
        let mut links = Vec::new();
        sets.try_reserve_exact(capacity)
            .map_err(|_| RegErrorCode::ESpace)?;
        states
            .try_reserve_exact(capacity * wordsper)
            .map_err(|_| RegErrorCode::ESpace)?;
        links
            .try_reserve_exact(capacity * cnfa.ncolors)
            .map_err(|_| RegErrorCode::ESpace)?;
        Ok(Self {
            cnfa,
            backref: None,
            sets,
            states,
            links,
            work: filled(wordsper, 0)?,
            nssets,
            wordsper,
            lastpost: None,
            lastnopr: None,
            search: 0,
            restart: None,
        })
    }

    fn initialize(&mut self, start: usize) -> ExecResult<usize> {
        if self.sets.is_empty() {
            let starter = self.getvacant(start, start)?;
            set(&mut self.states[..self.wordsper], self.cnfa.pre);
            self.sets[starter].hash = hash(&self.states[..self.wordsper]);
            self.sets[starter].flags = STARTER | NOPROGRESS;
        }
        for state in &mut self.sets {
            state.lastseen = None;
        }
        self.sets[0].lastseen = Some(start);
        self.lastpost = None;
        self.lastnopr = None;
        Ok(0)
    }

    fn pickss(&mut self, cp: usize, start: usize) -> ExecResult<usize> {
        let used = self.sets.len();
        if used < self.nssets {
            self.sets.push(StateSet {
                hash: 0,
                flags: 0,
                ins: NONE,
                lastseen: None,
            });
            self.states.resize((used + 1) * self.wordsper, 0);
            self.links.resize(
                (used + 1) * self.cnfa.ncolors,
                Link {
                    target: NONE,
                    next: NONE,
                },
            );
            return Ok(used);
        }
        let ancient = start.max(cp.saturating_sub(self.nssets * 2 / 3));
        for index in (self.search..used).chain(0..self.search) {
            let state = self.sets[index];
            if state.flags & STARTER == 0 && state.lastseen.is_none_or(|seen| seen < ancient) {
                self.search = index + 1;
                return Ok(index);
            }
        }
        Err(RegErrorCode::Assert)
    }

    fn getvacant(&mut self, cp: usize, start: usize) -> ExecResult<usize> {
        let slot = self.pickss(cp, start)?;
        let state = self.sets[slot];

        // Clear incoming links first, including self links.
        let mut link = state.ins;
        while link != NONE {
            let next = self.links[link].next;
            self.links[link] = Link {
                target: NONE,
                next: NONE,
            };
            link = next;
        }
        self.sets[slot].ins = NONE;

        let first = slot * self.cnfa.ncolors;
        for link in first..first + self.cnfa.ncolors {
            let Link { target, next } = self.links[link];
            if target == NONE {
                continue;
            }
            let mut previous = self.sets[target].ins;
            if previous == link {
                self.sets[target].ins = next;
            } else {
                while self.links[previous].next != link {
                    previous = self.links[previous].next;
                }
                self.links[previous].next = next;
            }
            self.links[link] = Link {
                target: NONE,
                next: NONE,
            };
        }
        if state.flags & POSTSTATE != 0 {
            self.lastpost = self.lastpost.max(state.lastseen);
        }
        if state.flags & NOPROGRESS != 0 {
            self.lastnopr = self.lastnopr.max(state.lastseen);
        }
        Ok(slot)
    }

    fn lastcold(&self) -> usize {
        self.lastseen_with(NOPROGRESS, self.lastnopr).unwrap_or(0)
    }

    fn lastseen_with(&self, flag: u8, flushed: Option<usize>) -> Option<usize> {
        self.sets
            .iter()
            .filter(|ss| ss.flags & flag != 0)
            .filter_map(|ss| ss.lastseen)
            .chain(flushed)
            .max()
    }

    fn ispost(&self, state: usize) -> bool {
        self.sets[state].flags & POSTSTATE != 0
    }

    // The first pass over a transition consumes a character. The lookaround
    // closure may suspend it while another DFA scans the input.
    fn miss(&mut self, text: &Text<'_>, input: Input, start: usize) -> ExecResult<Transition> {
        let link = input.state * self.cnfa.ncolors + input.color as usize;
        if self.links[link].target != NONE {
            return Ok(Transition::Ready(Some(self.links[link].target)));
        }
        if text.cancel.is_some_and(|check| check()) {
            return Err(RegErrorCode::Cancel);
        }
        self.work.fill(0);
        let rainbow = !text.cmap.pseudo(input.color);
        let first = input.state * self.wordsper;
        for state in members(&self.states[first..first + self.wordsper]) {
            for arc in self.cnfa.outs(state) {
                if arc.co == input.color || (rainbow && arc.co == RAINBOW) {
                    set(&mut self.work, arc.to);
                }
            }
        }
        if self.work.iter().all(|&word| word == 0) {
            return Ok(Transition::Ready(None));
        }
        let miss = Miss {
            input,
            start,
            link,
            step: Closure::default(),
        };
        if self.cnfa.has_lacons {
            Ok(Transition::Pending(miss))
        } else {
            self.finish_miss(miss).map(Transition::Ready)
        }
    }

    fn consume(
        &mut self,
        text: &Text<'_>,
        input: Input,
        start: usize,
    ) -> ExecResult<Option<usize>> {
        let link = input.state * self.cnfa.ncolors + input.color as usize;
        let target = self.links[link].target;
        if target != NONE {
            return Ok(Some(target));
        }
        match self.miss(text, input, start)? {
            Transition::Ready(target) => Ok(target),
            Transition::Pending(_) => unreachable!(),
        }
    }

    // Without lookarounds, the scan cannot suspend. Backreference dissection
    // in particular can run many of these scans over overlapping spans.
    fn scan_longest<const ONE_COLOR: bool>(
        &mut self,
        text: &Text<'_>,
        start: usize,
        stop: usize,
    ) -> ExecResult<Match> {
        let len = text.chars.len();
        let starter = self.initialize(start)?;
        let color = if start == 0 {
            self.cnfa.bos[usize::from(text.flags & REG_NOTBOL == 0)]
        } else if ONE_COLOR {
            0
        } else {
            text.cmap.get(text.chars[start - 1])
        };
        let input = Input {
            state: starter,
            color,
            cp: start,
        };
        let Some(mut state) = self.consume(text, input, start)? else {
            return Ok(Match::default());
        };
        let mut last_post = self.ispost(state).then_some(start);
        self.sets[state].lastseen = Some(start);
        let limit = stop + usize::from(stop < len);
        for (i, &chr) in text.chars[start..limit].iter().enumerate() {
            let cp = start + i + 1;
            let input = Input {
                state,
                color: if ONE_COLOR { 0 } else { text.cmap.get(chr) },
                cp,
            };
            let Some(next) = self.consume(text, input, start)? else {
                return Ok(Match {
                    end: last_post.and_then(|cp| cp.checked_sub(1)),
                    ..Match::default()
                });
            };
            // All remaining characters take this cached self-transition.
            // Record the same final position without rescanning the suffix.
            let self_loop = ONE_COLOR && next == state;
            let cp = if self_loop { limit } else { cp };
            state = next;
            self.sets[state].lastseen = Some(cp);
            if self.ispost(state) {
                last_post = Some(cp);
            }
            if self_loop {
                break;
            }
        }
        let hitstop = stop == len;
        let mut end = last_post.and_then(|cp| cp.checked_sub(1));
        if hitstop {
            let color = self.cnfa.eos[usize::from(text.flags & REG_NOTEOL == 0)];
            let input = Input {
                state,
                color,
                cp: len,
            };
            if self
                .consume(text, input, start)?
                .is_some_and(|next| self.ispost(next))
            {
                end = Some(len);
            }
        }
        Ok(Match {
            end,
            hitstop,
            cold: None,
        })
    }

    fn finish_miss(&mut self, miss: Miss) -> ExecResult<Option<usize>> {
        let h = hash(&self.work);
        let cached = self.sets.iter().enumerate().position(|(i, ss)| {
            let first = i * self.wordsper;
            ss.hash == h && self.states[first..first + self.wordsper] == self.work
        });
        let target = if let Some(index) = cached {
            index
        } else {
            let index = self.getvacant(miss.input.cp, miss.start)?;
            debug_assert_ne!(index, miss.input.state);
            let first = index * self.wordsper;
            self.states[first..first + self.wordsper].copy_from_slice(&self.work);
            let mut flags = 0;
            if test(&self.work, self.cnfa.post) {
                flags |= POSTSTATE;
            }
            if members(&self.work).all(|state| self.cnfa.no_progress[state]) {
                flags |= NOPROGRESS;
            }
            self.sets[index].hash = h;
            self.sets[index].flags = flags;
            index
        };
        // A lookaround depends on the text position even for identical colors.
        if !miss.step.saw_lacon {
            self.links[miss.link] = Link {
                target,
                next: self.sets[target].ins,
            };
            self.sets[target].ins = miss.link;
        }
        Ok(Some(target))
    }
}

struct Input {
    state: usize,
    color: Color,
    cp: usize,
}

enum Transition {
    Ready(Option<usize>),
    Pending(Miss),
}

struct Miss {
    input: Input,
    start: usize,
    link: usize,
    step: Closure,
}

#[derive(Default)]
struct Closure {
    state: usize,
    arc: usize,
    changed: bool,
    saw_lacon: bool,
}

impl Closure {
    fn next(&mut self, dfa: &Dfa<'_>) -> Option<(usize, usize)> {
        loop {
            while self.state < dfa.cnfa.no_progress.len() {
                if test(&dfa.work, self.state) {
                    let outs = dfa.cnfa.outs(self.state);
                    while let Some(arc) = outs.get(self.arc) {
                        self.arc += 1;
                        if i32::from(arc.co) >= dfa.cnfa.ncolors as i32 && !test(&dfa.work, arc.to)
                        {
                            self.saw_lacon = true;
                            return Some((arc.co as usize - dfa.cnfa.ncolors, arc.to));
                        }
                    }
                }
                self.state += 1;
                self.arc = 0;
            }
            if !self.changed {
                return None;
            }
            self.state = 0;
            self.changed = false;
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Scan {
    Longest {
        start: usize,
        stop: usize,
    },
    Shortest {
        start: usize,
        min: usize,
        max: usize,
        cold: bool,
    },
    MatchUntil {
        probe: usize,
    },
}

#[derive(Default)]
pub(super) struct Match {
    pub(super) end: Option<usize>,
    pub(super) cold: Option<usize>,
    pub(super) hitstop: bool,
}

struct Text<'a> {
    chars: &'a [Chr],
    cmap: &'a Colormap,
    flags: i32,
    cancel: Option<CancelCheck<'a>>,
}

struct Run {
    scan: Scan,
    start: usize,
    cp: usize,
    state: usize,
    phase: Phase,
    result: Match,
}

enum Phase {
    Start,
    Startup,
    Character,
    End,
}

enum Step {
    Consume(Input),
    Done,
}

impl Scan {
    fn start(self) -> usize {
        match self {
            Self::Longest { start, .. } | Self::Shortest { start, .. } => start,
            Self::MatchUntil { .. } => 0,
        }
    }

    fn matchall(self, minlen: usize, maxlen: usize, len: usize) -> Match {
        let mut result = Match::default();
        match self {
            Self::Longest { start, stop } => {
                let available = stop - start;
                if available >= minlen {
                    result.hitstop = stop == len && (maxlen == DUPINF || available <= maxlen + 1);
                    result.end = Some(if maxlen == DUPINF {
                        stop
                    } else {
                        stop.min(start + maxlen)
                    });
                }
            }
            Self::Shortest {
                start,
                min,
                max,
                cold,
            } => {
                if (maxlen == DUPINF || min - start <= maxlen) && max - start >= minlen {
                    result.end = Some(min.max(start + minlen));
                    result.cold = cold.then_some(start);
                }
            }
            Self::MatchUntil { probe } => {
                debug_assert_eq!(maxlen, DUPINF);
                result.end = (probe >= minlen).then_some(probe);
            }
        }
        result
    }
}

impl Run {
    fn new(scan: Scan) -> Self {
        Self {
            scan,
            start: scan.start(),
            cp: scan.start(),
            state: NONE,
            phase: Phase::Start,
            result: Match::default(),
        }
    }

    fn advance(
        &mut self,
        dfa: &mut Dfa<'_>,
        text: &Text<'_>,
        next: Option<usize>,
    ) -> ExecResult<Step> {
        let len = text.chars.len();
        match self.phase {
            Phase::Start => {
                if let Scan::MatchUntil { probe } = self.scan
                    && let Some((cp, state)) = dfa.restart
                    && cp <= probe
                {
                    let Some(state) = state else {
                        return Ok(Step::Done);
                    };
                    self.cp = cp;
                    self.state = state;
                    return self.scan_step(dfa, text);
                }
                self.phase = Phase::Startup;
                let color = if self.cp == 0 {
                    dfa.cnfa.bos[usize::from(text.flags & REG_NOTBOL == 0)]
                } else {
                    text.cmap.get(text.chars[self.cp - 1])
                };
                return Ok(Step::Consume(Input {
                    state: dfa.initialize(self.start)?,
                    color,
                    cp: self.cp,
                }));
            }
            Phase::Startup => {
                let Some(state) = next else {
                    return Ok(Step::Done);
                };
                self.state = state;
                dfa.sets[state].lastseen = Some(self.cp);
            }
            Phase::Character => {
                if let Some(state) = next {
                    self.cp += 1;
                    self.state = state;
                    dfa.sets[state].lastseen = Some(self.cp);
                    if let Scan::Shortest { min, cold, .. } = self.scan
                        && dfa.ispost(state)
                        && self.cp > min
                    {
                        self.result.end = Some(self.cp - 1);
                        self.result.cold = cold.then(|| dfa.lastcold());
                        return Ok(Step::Done);
                    }
                } else {
                    match self.scan {
                        Scan::Longest { .. } => self.longest_end(dfa),
                        Scan::Shortest { .. } => {}
                        Scan::MatchUntil { .. } => dfa.restart = Some((self.cp, None)),
                    }
                    return Ok(Step::Done);
                }
            }
            Phase::End => {
                let post = next.is_some_and(|state| dfa.ispost(state));
                match self.scan {
                    Scan::MatchUntil { .. } => self.result.end = post.then_some(self.cp),
                    Scan::Shortest { .. } => {
                        self.result.end = post.then_some(self.cp);
                        self.result.hitstop = !post;
                    }
                    Scan::Longest { .. } => {
                        if post {
                            self.result.end = Some(self.cp);
                        } else {
                            if let Some(state) = next {
                                dfa.sets[state].lastseen = Some(len);
                            }
                            self.longest_end(dfa);
                        }
                    }
                }
                return Ok(Step::Done);
            }
        }
        self.scan_step(dfa, text)
    }

    fn scan_step(&mut self, dfa: &mut Dfa<'_>, text: &Text<'_>) -> ExecResult<Step> {
        let len = text.chars.len();
        let limit = match self.scan {
            Scan::Longest { stop, .. } | Scan::Shortest { max: stop, .. } => {
                stop + usize::from(stop < len)
            }
            Scan::MatchUntil { probe } => probe,
        };
        if self.cp < limit {
            self.phase = Phase::Character;
            return Ok(Step::Consume(Input {
                state: self.state,
                color: text.cmap.get(text.chars[self.cp]),
                cp: self.cp + 1,
            }));
        }
        let (color, cp) = match self.scan {
            Scan::MatchUntil { .. } => {
                dfa.restart = Some((self.cp, Some(self.state)));
                if self.cp < len {
                    (text.cmap.get(text.chars[self.cp]), self.cp + 1)
                } else {
                    (
                        dfa.cnfa.eos[usize::from(text.flags & REG_NOTEOL == 0)],
                        self.cp,
                    )
                }
            }
            Scan::Longest { stop, .. } if stop < len => {
                self.longest_end(dfa);
                return Ok(Step::Done);
            }
            Scan::Shortest { min, max, cold, .. } => {
                self.result.cold = cold.then(|| dfa.lastcold());
                if self.cp > min && dfa.ispost(self.state) {
                    self.result.end = Some(self.cp - 1);
                    return Ok(Step::Done);
                }
                if max < len {
                    return Ok(Step::Done);
                }
                (
                    dfa.cnfa.eos[usize::from(text.flags & REG_NOTEOL == 0)],
                    self.cp,
                )
            }
            Scan::Longest { .. } => {
                self.result.hitstop = true;
                (
                    dfa.cnfa.eos[usize::from(text.flags & REG_NOTEOL == 0)],
                    self.cp,
                )
            }
        };
        self.phase = Phase::End;
        Ok(Step::Consume(Input {
            state: self.state,
            color,
            cp,
        }))
    }

    fn longest_end(&mut self, dfa: &Dfa<'_>) {
        self.result.end = dfa
            .lastseen_with(POSTSTATE, dfa.lastpost)
            .and_then(|cp| cp.checked_sub(1));
    }
}

// Keep suspended lookaround calls off the native stack. Each lacon has a
// separate DFA, and can refer only to a previously compiled lacon.
struct Suspended {
    dfa: usize,
    run: Run,
    miss: Miss,
    to: usize,
    positive: bool,
    caller_stack: usize,
}

impl Executor<'_, '_> {
    pub(super) fn longest(&mut self, dfa: usize, start: usize, stop: usize) -> ExecResult<Match> {
        self.scan(dfa, Scan::Longest { start, stop })
    }

    pub(super) fn shortest(
        &mut self,
        dfa: usize,
        start: usize,
        min: usize,
        max: usize,
        cold: bool,
    ) -> ExecResult<Match> {
        self.scan(
            dfa,
            Scan::Shortest {
                start,
                min,
                max,
                cold,
            },
        )
    }

    fn scan(&mut self, dfa: usize, scan: Scan) -> ExecResult<Match> {
        let text = Text {
            chars: self.string,
            cmap: &self.regex.guts.cmap,
            flags: self.eflags,
            cancel: self.cancel,
        };
        if let Scan::Longest { start, stop } = scan
            && !self.dfas[dfa].as_ref().unwrap().cnfa.has_lacons
        {
            if let Some(result) = self.fast_scan(dfa, scan) {
                return Ok(result);
            }
            let dfa = self.dfas[dfa].as_mut().unwrap();
            // Color zero is the only character color when the remaining
            // four colors all belong to the start/end markers.
            return if dfa.cnfa.ncolors == 1 + dfa.cnfa.bos.len() + dfa.cnfa.eos.len() {
                dfa.scan_longest::<true>(&text, start, stop)
            } else {
                dfa.scan_longest::<false>(&text, start, stop)
            };
        }
        // lacon is inlined in miss on the oracle. Lookahead reenters
        // through shortest, whereas lookbehind's matchuntil is inlined.
        const LOOKAHEAD_FRAME: usize = 336;
        const LOOKBEHIND_FRAME: usize = 208;
        const LOOKAROUND_STACK_LIMIT: usize = 6_119 * LOOKAHEAD_FRAME;
        let mut stack: Vec<Suspended> = Vec::new();
        let mut stack_used = self.dissect_stack;
        let mut active = dfa;
        let mut run = Run::new(scan);
        let mut transition = Transition::Ready(None);
        let mut entered = false;
        loop {
            if !entered {
                if let Some(result) = self.fast_scan(active, run.scan) {
                    run.result = result;
                    if !self.resume_lacon(
                        &mut stack,
                        &mut active,
                        &mut run,
                        &mut transition,
                        &mut stack_used,
                    ) {
                        return Ok(run.result);
                    }
                }
                entered = true;
            }

            match transition {
                Transition::Ready(next) => {
                    match run.advance(self.dfas[active].as_mut().unwrap(), &text, next)? {
                        Step::Consume(input) => {
                            transition = self.dfas[active]
                                .as_mut()
                                .unwrap()
                                .miss(&text, input, run.start)?;
                        }
                        Step::Done => {
                            if !self.resume_lacon(
                                &mut stack,
                                &mut active,
                                &mut run,
                                &mut transition,
                                &mut stack_used,
                            ) {
                                return Ok(run.result);
                            }
                        }
                    }
                }
                Transition::Pending(mut miss) => {
                    let dfa = self.dfas[active].as_mut().unwrap();
                    if let Some((lacon, to)) = miss.step.next(dfa) {
                        if stack_used > LOOKAROUND_STACK_LIMIT {
                            return Err(RegErrorCode::ETooBig);
                        }
                        let cp = miss.input.cp;
                        let (index, scan, positive) = self.lacon(lacon, cp)?;
                        let caller_stack = stack_used;
                        stack_used += if matches!(scan, Scan::MatchUntil { .. }) {
                            LOOKBEHIND_FRAME
                        } else {
                            LOOKAHEAD_FRAME
                        };
                        push(
                            &mut stack,
                            Suspended {
                                dfa: active,
                                run,
                                miss,
                                to,
                                positive,
                                caller_stack,
                            },
                        )?;
                        active = index;
                        run = Run::new(scan);
                        transition = Transition::Ready(None);
                        entered = false;
                    } else {
                        transition = Transition::Ready(dfa.finish_miss(miss)?);
                    }
                }
            }
        }
    }

    fn fast_scan(&self, index: usize, scan: Scan) -> Option<Match> {
        let dfa = self.dfas[index].as_ref().unwrap();
        if let Some((backno, minreps, maxreps)) = dfa.backref
            && let Some(referent) = self.referent(backno)
        {
            let (start, min, max, shortest, cold) = match scan {
                Scan::Longest { start, stop } => (start, start, stop, false, false),
                Scan::Shortest {
                    start,
                    min,
                    max,
                    cold,
                } => (start, min, max, true, cold),
                Scan::MatchUntil { .. } => unreachable!(),
            };
            let end = self.dfa_backref(referent, [minreps, maxreps], [start, min, max], shortest);
            return Some(Match {
                end,
                cold: (cold && end.is_some()).then_some(start),
                hitstop: !shortest && max == self.string.len() && end == Some(max),
            });
        }
        dfa.cnfa
            .matchall
            .map(|(min, max)| scan.matchall(min, max, self.string.len()))
    }

    fn dfa_backref(
        &self,
        referent: &[Chr],
        [minreps, maxreps]: [usize; 2],
        [start, min, max]: [usize; 3],
        shortest: bool,
    ) -> Option<usize> {
        let len = referent.len();
        if len == 0 {
            return (min == start && minreps <= maxreps).then_some(start);
        }
        let minreps = minreps.max((min - start).div_ceil(len));
        let mut max_matches = (max - start) / len;
        if maxreps != DUPINF {
            max_matches = max_matches.min(maxreps);
        }
        if max_matches < minreps {
            return None;
        }
        if shortest {
            max_matches = minreps;
        }
        let mut count = 0;
        for span in self.string[start..max].chunks_exact(len).take(max_matches) {
            if !self.backref_eq(referent, span) {
                break;
            }
            count += 1;
        }
        (count >= minreps).then_some(start + count * len)
    }

    fn lacon(&mut self, number: usize, cp: usize) -> ExecResult<(usize, Scan, bool)> {
        let index = 2 + self.regex.guts.tree.len() + (number - 1);
        let sub = &self.regex.guts.lacons[number - 1];
        if self.dfas[index].is_none() {
            self.dfas[index] = Some(Dfa::new(&sub.cnfa, self.eflags)?);
        }
        let scan = if sub.kind.ahead() {
            Scan::Shortest {
                start: cp,
                min: cp,
                max: self.string.len(),
                cold: false,
            }
        } else {
            // matchuntil continues from its last probe for monotonic lookbehind.
            Scan::MatchUntil { probe: cp }
        };
        Ok((index, scan, sub.kind.positive()))
    }

    fn resume_lacon(
        &mut self,
        stack: &mut Vec<Suspended>,
        active: &mut usize,
        run: &mut Run,
        transition: &mut Transition,
        stack_used: &mut usize,
    ) -> bool {
        let Some(Suspended {
            dfa,
            run: parent,
            mut miss,
            to,
            positive,
            caller_stack,
        }) = stack.pop()
        else {
            return false;
        };
        if run.result.end.is_some() == positive {
            set(&mut self.dfas[dfa].as_mut().unwrap().work, to);
            miss.step.changed = true;
        }
        *stack_used = caller_stack;
        *active = dfa;
        *run = parent;
        *transition = Transition::Pending(miss);
        true
    }
}
