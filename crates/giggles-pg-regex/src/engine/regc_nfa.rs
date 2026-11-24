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
 * Translated to Rust from regc_nfa.c.
 */

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::regc_color::Colormap;
use super::regguts::{Carc, Cnfa, Color, DUPINF, MAX_COLOR, RAINBOW, StateId};
use super::{CancelCheck, REG_UEMPTYMATCH, REG_UIMPOSSIBLE, RegErrorCode};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Arc {
    Empty,
    CantMatch,
    Plain(Color),
    Bol(u8),
    Eol(u8),
    Behind(Color),
    Ahead(Color),
    Lacon(u32),
}

impl Arc {
    fn constraint(self) -> bool {
        !matches!(self, Self::Empty | Self::CantMatch | Self::Plain(_))
    }

    fn indexed_color(self) -> Option<Color> {
        match self {
            Self::Plain(c) | Self::Behind(c) | Self::Ahead(c) if c >= 0 => Some(c),
            _ => None,
        }
    }

    fn recolor(self, color: Color) -> Self {
        match self {
            Self::Plain(_) => Self::Plain(color),
            Self::Behind(_) => Self::Behind(color),
            Self::Ahead(_) => Self::Ahead(color),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Reachability {
    Unreached,
    FromPre,
    ToPost,
}

type StateEdge = (Arc, StateId);

/// Sorted adjacency. Most states need only a few transitions. Large sets
/// use a tree to keep insertion and removal logarithmic.
enum StateArcs {
    Inline {
        len: usize,
        arcs: [StateEdge; Self::INLINE_LIMIT],
    },
    Large(BTreeSet<StateEdge>),
}

impl Default for StateArcs {
    fn default() -> Self {
        Self::Inline {
            len: 0,
            arcs: [(Arc::Empty, 0); Self::INLINE_LIMIT],
        }
    }
}

impl StateArcs {
    const INLINE_LIMIT: usize = 6;

    fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => *len,
            Self::Large(arcs) => arcs.len(),
        }
    }

    fn last(&self) -> Option<&StateEdge> {
        match self {
            Self::Large(arcs) => arcs.last(),
            _ => self.as_slice().last(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn first(&self) -> Option<&StateEdge> {
        match self {
            Self::Large(arcs) => arcs.first(),
            _ => self.as_slice().first(),
        }
    }

    fn contains(&self, arc: &StateEdge) -> bool {
        match self {
            Self::Large(arcs) => arcs.contains(arc),
            _ => self.as_slice().binary_search(arc).is_ok(),
        }
    }

    fn insert(&mut self, arc: StateEdge) -> bool {
        match self {
            Self::Inline { len, arcs } => {
                let Err(index) = arcs[..*len].binary_search(&arc) else {
                    return false;
                };
                if *len < Self::INLINE_LIMIT {
                    if index != *len {
                        arcs.copy_within(index..*len, index + 1);
                    }
                    arcs[index] = arc;
                    *len += 1;
                } else {
                    let mut spilled: BTreeSet<_> = arcs.iter().copied().collect();
                    spilled.insert(arc);
                    *self = Self::Large(spilled);
                }
            }
            Self::Large(arcs) => return arcs.insert(arc),
        }
        true
    }

    fn remove(&mut self, arc: &StateEdge) -> bool {
        match self {
            Self::Inline { len, arcs } => {
                let Ok(index) = arcs[..*len].binary_search(arc) else {
                    return false;
                };
                *len -= 1;
                if index != *len {
                    arcs.copy_within(index + 1..=*len, index);
                }
            }
            Self::Large(arcs) => return arcs.remove(arc),
        }
        true
    }

    fn as_slice(&self) -> &[StateEdge] {
        match self {
            Self::Inline { len, arcs } => &arcs[..*len],
            Self::Large(_) => unreachable!(),
        }
    }

    fn snapshot(&self) -> Vec<StateEdge> {
        match self {
            Self::Large(arcs) => arcs.iter().copied().collect(),
            _ => self.as_slice().to_vec(),
        }
    }

    fn iter(&self) -> StateArcsIter<'_> {
        match self {
            Self::Large(arcs) => StateArcsIter::Tree(arcs.range(..)),
            _ => StateArcsIter::Slice(self.as_slice().iter()),
        }
    }

    fn range(&self, start: StateEdge, end: Option<StateEdge>) -> StateArcsIter<'_> {
        if let Self::Large(arcs) = self {
            use std::ops::Bound::{Excluded, Included, Unbounded};
            return StateArcsIter::Tree(
                arcs.range((Included(start), end.map_or(Unbounded, Excluded))),
            );
        }
        let arcs = self.as_slice();
        let first = arcs.partition_point(|arc| *arc < start);
        let last = end.map_or(arcs.len(), |end| arcs.partition_point(|arc| *arc < end));
        StateArcsIter::Slice(arcs[first..last].iter())
    }
}

enum StateArcsIter<'a> {
    Slice(std::slice::Iter<'a, StateEdge>),
    Tree(std::collections::btree_set::Range<'a, StateEdge>),
}

impl<'a> Iterator for StateArcsIter<'a> {
    type Item = &'a StateEdge;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Slice(arcs) => arcs.next(),
            Self::Tree(arcs) => arcs.next(),
        }
    }
}

#[derive(Default)]
struct State {
    ordinal: usize,
    ins: StateArcs,
    outs: StateArcs,
}

type Edges = Vec<(Arc, StateId, StateId)>;

pub(super) type MatchAll = Option<(usize, usize)>;

pub(super) const POST: StateId = 0;
pub(super) const PRE: StateId = 1;
pub(super) const INIT: StateId = 2;
pub(super) const FINAL: StateId = 3;

const DROPPED_ORDINAL: usize = usize::MAX;

// PostgreSQL's state and arc allocator budget, measured with its 64-bit
// structure sizes. The wrapper and prefix each add graph states per parse level.
const REG_MAX_COMPILE_SPACE: usize = 500_000 * (56 + 4 * 72);
// Synthetic stack budgets for the PostgreSQL 18.6 x86-64 oracle at
// max_stack_depth = 2MB. Each limit includes the walk's first invocation.
// The caller's recursive frames (parsing or compiling the subexpression
// tree) reduce the available depth further.
const DUPTRAVERSE: WalkDepth = WalkDepth::new(43_548, 48);
// The compiler calls duptraverse from a shallower stack through nfanode.
const NFANODE_DUPTRAVERSE: WalkDepth = WalkDepth::new(43_549, 48);
const DELTRAVERSE: WalkDepth = WalkDepth::new(43_549, 48);
const REMOVETRAVERSE: WalkDepth = WalkDepth::new(26_130, 80);
const CLEARTRAVERSE: WalkDepth = WalkDepth::new(65_325, 32);
const EMPTYREACHABLE: WalkDepth = WalkDepth::new(43_550, 48);
const FINDCONSTRAINTLOOP: WalkDepth = WalkDepth::new(26_131, 80);
const CLONESUCCESSORSTATES: WalkDepth = WalkDepth::new(16_332, 128);
const MARKREACHABLE: WalkDepth = WalkDepth::new(43_552, 48);
const MARKCANREACH: WalkDepth = WalkDepth::new(43_552, 48);
const CHECKMATCHALL: WalkDepth = WalkDepth::new(26_129, 80);

#[derive(Clone, Copy)]
struct WalkDepth {
    limit: usize,
    frame: usize,
}

impl WalkDepth {
    const fn new(limit: usize, frame: usize) -> Self {
        Self { limit, frame }
    }

    fn remaining(self, caller: usize) -> usize {
        (self.limit * self.frame).saturating_sub(caller) / self.frame
    }
}

pub(super) struct Nfa<'a> {
    states: Vec<State>,
    live: BTreeSet<StateId>,
    free: Vec<StateId>,
    next_ordinal: usize,
    index_colors: bool,
    by_color: BTreeMap<Color, BTreeSet<(Arc, StateId, StateId)>>,
    cancel: Option<CancelCheck<'a>>,
    arc_count: usize,
    internal_constraints: usize,
    state_batch: usize,
    arc_batch: usize,
    state_capacity: usize,
    arc_capacity: usize,
    space: usize,
    parent_space: usize,
    pub(super) stack_used: usize,
    fragment_ids: Vec<usize>,
}

impl<'a> Nfa<'a> {
    pub(super) fn new(
        cancel: Option<CancelCheck<'a>>,
        parent_space: usize,
    ) -> Result<Self, RegErrorCode> {
        let mut nfa = Self {
            states: Vec::new(),
            live: BTreeSet::new(),
            free: Vec::new(),
            next_ordinal: 0,
            index_colors: parent_space == 0,
            by_color: BTreeMap::new(),
            cancel,
            arc_count: 0,
            internal_constraints: 0,
            state_batch: 32,
            arc_batch: 64,
            state_capacity: 0,
            arc_capacity: 0,
            space: 0,
            parent_space,
            stack_used: 0,
            fragment_ids: Vec::new(),
        };
        for _ in POST..=FINAL {
            nfa.newstate()?;
        }
        nfa.add(Arc::Plain(RAINBOW), PRE, INIT)?;
        nfa.add(Arc::Plain(RAINBOW), FINAL, POST)?;
        for i in 0..2 {
            nfa.add(Arc::Bol(i), PRE, INIT)?;
            nfa.add(Arc::Eol(i), FINAL, POST)?;
        }
        Ok(nfa)
    }

    fn interrupt(&self) -> Result<(), RegErrorCode> {
        if self.cancel.is_some_and(|check| check()) {
            return Err(RegErrorCode::Cancel);
        }
        Ok(())
    }

    pub(super) fn newstate(&mut self) -> Result<StateId, RegErrorCode> {
        self.interrupt()?;
        let id = if let Some(id) = self.free.pop() {
            id
        } else {
            if self.states.len() == self.state_capacity {
                if self.parent_space + self.space >= REG_MAX_COMPILE_SPACE {
                    return Err(RegErrorCode::ETooBig);
                }
                self.state_capacity += self.state_batch;
                self.space += self.state_batch * 56 + 16;
                self.state_batch = (self.state_batch * 2).min(1024);
            }
            let id = self.states.len();
            self.states.push(State::default());
            id
        };
        self.states[id].ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        self.live.insert(id);
        Ok(id)
    }

    pub(super) fn add(&mut self, arc: Arc, from: StateId, to: StateId) -> Result<(), RegErrorCode> {
        self.interrupt()?;
        if self.arc_count == self.arc_capacity {
            if self.states[from].outs.contains(&(arc, to)) {
                return Ok(());
            }
            if self.parent_space + self.space >= REG_MAX_COMPILE_SPACE {
                return Err(RegErrorCode::ETooBig);
            }
            self.arc_capacity += self.arc_batch;
            self.space += self.arc_batch * 72 + 16;
            self.arc_batch = (self.arc_batch * 2).min(1024);
        }
        self.insert(arc, from, to);
        Ok(())
    }

    fn insert(&mut self, arc: Arc, from: StateId, to: StateId) {
        if !self.states[from].outs.insert((arc, to)) {
            return;
        }
        self.states[to].ins.insert((arc, from));
        self.arc_count += 1;
        self.internal_constraints += usize::from(is_internal_constraint(arc, from, to));
        if self.index_colors
            && let Some(color) = arc.indexed_color()
        {
            self.by_color
                .entry(color)
                .or_default()
                .insert((arc, from, to));
        }
    }

    fn remove(&mut self, arc: Arc, from: StateId, to: StateId) {
        if self.states[from].outs.remove(&(arc, to)) {
            self.states[to].ins.remove(&(arc, from));
            self.arc_count -= 1;
            self.internal_constraints -= usize::from(is_internal_constraint(arc, from, to));
            if self.index_colors
                && let Some(color) = arc.indexed_color()
            {
                self.by_color
                    .get_mut(&color)
                    .unwrap()
                    .remove(&(arc, from, to));
            }
        }
    }

    pub(super) fn dropstate(&mut self, state: StateId) {
        while let Some(&(arc, to)) = self.states[state].outs.first() {
            self.remove(arc, state, to);
        }
        while let Some(&(arc, from)) = self.states[state].ins.first() {
            self.remove(arc, from, state);
        }
        self.states[state] = State {
            ordinal: DROPPED_ORDINAL,
            ..State::default()
        };
        self.live.remove(&state);
        self.free.push(state);
    }

    pub(super) fn empty(&mut self, from: StateId, to: StateId) -> Result<(), RegErrorCode> {
        self.add(Arc::Empty, from, to)
    }

    pub(super) fn moveins(&mut self, from: StateId, to: StateId) -> Result<(), RegErrorCode> {
        self.interrupt()?;
        while let Some(&(arc, source)) = self.states[from].ins.first() {
            self.remove(arc, source, from);
            self.add(arc, source, to)?;
        }
        Ok(())
    }

    pub(super) fn moveouts(&mut self, from: StateId, to: StateId) -> Result<(), RegErrorCode> {
        self.interrupt()?;
        while let Some(&(arc, dest)) = self.states[from].outs.first() {
            self.remove(arc, from, dest);
            self.add(arc, to, dest)?;
        }
        Ok(())
    }

    pub(super) fn okcolors(&mut self, cm: &mut Colormap) -> Result<(), RegErrorCode> {
        for (parent, sub, replace) in cm.okcolors() {
            let old = self.by_color.get(&parent).cloned().unwrap_or_default();
            for (arc, from, to) in old {
                let new = arc.recolor(sub);
                if replace {
                    self.remove(arc, from, to);
                    self.insert(new, from, to);
                } else {
                    self.add(new, from, to)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn release_color_index(&mut self) {
        self.index_colors = false;
        self.by_color.clear();
    }

    pub(super) fn colors(&self, from: StateId) -> impl Iterator<Item = Color> + '_ {
        self.states[from].outs.iter().filter_map(|(arc, _)| {
            if let Arc::Plain(color) = arc {
                Some(*color)
            } else {
                None
            }
        })
    }

    pub(super) fn single_color_transition(
        &self,
        mut from: StateId,
        mut to: StateId,
    ) -> Option<StateId> {
        let ins = &self.states[to].ins;
        if ins.len() == 1
            && let Some(&(Arc::Empty, previous)) = ins.first()
        {
            to = previous;
        }
        let outs = &self.states[from].outs;
        if outs.len() == 1
            && let Some(&(Arc::Empty, next)) = outs.first()
        {
            from = next;
        }
        let outs = &self.states[from].outs;
        (from != to
            && !outs.is_empty()
            && outs
                .iter()
                .all(|&(arc, dest)| matches!(arc, Arc::Plain(_)) && dest == to))
        .then_some(from)
    }

    fn fragment(
        &mut self,
        start: StateId,
        stop: StateId,
        limit: usize,
    ) -> Result<Fragment, RegErrorCode> {
        debug_assert_ne!(start, stop);
        const UNVISITED: usize = usize::MAX;
        let mut states = vec![start, stop];
        let mut edges = Vec::new();
        self.fragment_ids.resize(self.states.len(), UNVISITED);
        self.fragment_ids[stop] = 1;
        let mut stack = vec![(start, 1)];
        while let Some((state, depth)) = stack.pop() {
            if depth > limit {
                for state in states {
                    self.fragment_ids[state] = UNVISITED;
                }
                return Err(RegErrorCode::ETooBig);
            }
            if self.fragment_ids[state] != UNVISITED {
                continue;
            }
            let id = if state == start {
                0
            } else {
                states.push(state);
                states.len() - 1
            };
            self.fragment_ids[state] = id;
            let outs = &self.states[state].outs;
            for &(arc, to) in outs.iter() {
                edges.push((arc, id, to));
                stack.push((to, depth + 1));
            }
        }
        for (_, _, to) in &mut edges {
            *to = self.fragment_ids[*to];
        }
        for &state in &states {
            self.fragment_ids[state] = UNVISITED;
        }
        Ok(Fragment { states, edges })
    }

    pub(super) fn delsub(&mut self, start: StateId, stop: StateId) -> Result<(), RegErrorCode> {
        if start == stop {
            return Ok(());
        }
        let Fragment { states, edges } =
            self.fragment(start, stop, DELTRAVERSE.remaining(self.stack_used))?;
        for (arc, from, to) in edges {
            self.remove(arc, states[from], states[to]);
        }
        for state in states.into_iter().skip(2) {
            if self.states[state].ins.is_empty() {
                self.dropstate(state);
            }
        }
        Ok(())
    }

    fn copy_fragment(
        &mut self,
        mut fragment: Fragment,
        from: StateId,
        to: StateId,
    ) -> Result<(), RegErrorCode> {
        let copies = &mut fragment.states;
        copies[0] = from;
        copies[1] = to;
        for state in &mut copies[2..] {
            *state = self.newstate()?;
        }
        for (arc, from, to) in fragment.edges {
            self.add(arc, copies[from], copies[to])?;
        }
        Ok(())
    }

    pub(super) fn dupnfa(
        &mut self,
        start: StateId,
        stop: StateId,
        from: StateId,
        to: StateId,
    ) -> Result<(), RegErrorCode> {
        if start == stop {
            return self.empty(from, to);
        }
        let caller = self.stack_used;
        let limit = DUPTRAVERSE
            .remaining(caller)
            .min(CLEARTRAVERSE.remaining(caller));
        let fragment = self.fragment(start, stop, limit)?;
        self.copy_fragment(fragment, from, to)
    }

    pub(super) fn removeconstraints(
        &mut self,
        start: StateId,
        stop: StateId,
    ) -> Result<(), RegErrorCode> {
        if start == stop {
            return Ok(());
        }
        let caller = self.stack_used;
        let limit = REMOVETRAVERSE
            .remaining(caller)
            .min(CLEARTRAVERSE.remaining(caller));
        let Fragment { states, edges } = self.fragment(start, stop, limit)?;
        for (arc, from, to) in edges {
            if arc.constraint() {
                let (from, to) = (states[from], states[to]);
                self.empty(from, to)?;
                self.remove(arc, from, to);
            }
        }
        Ok(())
    }

    pub(super) fn nfanode(
        &mut self,
        start: StateId,
        stop: StateId,
        ancestors: usize,
    ) -> Result<Self, RegErrorCode> {
        // nfatree tail-calls nfanode after descending through its children.
        const NFATREE_FRAME: usize = 112;
        const NFANODE_FRAME: usize = 80;
        let mut node = Self::new(self.cancel, self.space)?;
        node.stack_used = ancestors * NFATREE_FRAME + NFANODE_FRAME;
        let limit = NFANODE_DUPTRAVERSE
            .remaining(node.stack_used)
            .min(CLEARTRAVERSE.remaining(node.stack_used));
        if start == stop {
            node.empty(INIT, FINAL)?;
        } else {
            node.copy_fragment(self.fragment(start, stop, limit)?, INIT, FINAL)?;
        }
        Ok(node)
    }

    // markreachable and markcanreach use the same explicit traversal.
    // The latter considers only states marked reachable by the former.
    fn mark_states(
        &self,
        marks: &mut [Reachability],
        start: StateId,
        backwards: bool,
    ) -> Result<(), RegErrorCode> {
        let (limit, previous, mark) = if backwards {
            (MARKCANREACH, Reachability::FromPre, Reachability::ToPost)
        } else {
            (
                MARKREACHABLE,
                Reachability::Unreached,
                Reachability::FromPre,
            )
        };
        let limit = limit.remaining(self.stack_used);
        let mut stack = vec![(start, 1)];
        while let Some((state, depth)) = stack.pop() {
            if depth > limit {
                return Err(RegErrorCode::ETooBig);
            }
            if marks[state] != previous {
                continue;
            }
            marks[state] = mark;
            let edges = if backwards {
                &self.states[state].ins
            } else {
                &self.states[state].outs
            };
            stack.extend(edges.iter().map(|&(_, next)| (next, depth + 1)));
        }
        Ok(())
    }

    fn cleanup(&mut self) -> Result<(), RegErrorCode> {
        let mut marks = vec![Reachability::Unreached; self.states.len()];
        self.mark_states(&mut marks, PRE, false)?;
        self.mark_states(&mut marks, POST, true)?;
        for state in self.state_order() {
            if state > PRE && marks[state] != Reachability::ToPost {
                self.dropstate(state);
            }
        }
        Ok(())
    }

    fn hasnonemptyout(&self, state: StateId) -> bool {
        self.states[state]
            .outs
            .last()
            .is_some_and(|&(arc, _)| arc != Arc::Empty)
    }

    fn fixempties(&mut self) -> Result<(), RegErrorCode> {
        let mut states = self.state_order();
        for &state in states.iter().filter(|&&state| state > PRE) {
            let outs = &self.states[state].outs;
            if outs.len() == 1
                && let Some(&(Arc::Empty, to)) = outs.first()
            {
                if state != to {
                    self.moveins(state, to)?;
                }
                self.dropstate(state);
            }
        }
        states.retain(|&state| self.states[state].ordinal != DROPPED_ORDINAL);
        for &state in states.iter().filter(|&&state| state > PRE) {
            let ins = &self.states[state].ins;
            if ins.len() == 1
                && let Some(&(Arc::Empty, from)) = ins.first()
            {
                if state != from {
                    self.moveouts(state, from)?;
                }
                self.dropstate(state);
            }
        }

        // Only original inarcs can be copied forward. Keep epsilon
        // predecessors separately so traversal never scans the new inarcs.
        states.retain(|&state| self.states[state].ordinal != DROPPED_ORDINAL);
        let mut old_ins = Vec::new();
        let mut empty_ins = Vec::new();
        let mut offsets = Vec::with_capacity(self.states.len() + 1);
        for state in &self.states {
            offsets.push([old_ins.len(), empty_ins.len()]);
            for &(arc, from) in state.ins.iter() {
                if arc == Arc::Empty {
                    empty_ins.push(from);
                } else {
                    old_ins.push((arc, from));
                }
            }
        }
        offsets.push([old_ins.len(), empty_ins.len()]);
        let empty_predecessors =
            |state: StateId| &empty_ins[offsets[state][1]..offsets[state + 1][1]];
        let limit = EMPTYREACHABLE.remaining(self.stack_used);
        let mut seen = vec![usize::MAX; self.states.len()];
        let mut pending = Vec::new();
        let mut arcs = Vec::new();
        for &state in &states {
            if state > PRE && !self.hasnonemptyout(state) {
                continue;
            }
            seen[state] = state;
            pending.extend(empty_predecessors(state).iter().map(|&from| (from, 2)));
            while let Some((from, depth)) = pending.pop() {
                if seen[from] == state {
                    continue;
                }
                if depth > limit {
                    return Err(RegErrorCode::ETooBig);
                }
                seen[from] = state;
                pending.extend(
                    empty_predecessors(from)
                        .iter()
                        .map(|&from| (from, depth + 1)),
                );
                arcs.extend_from_slice(&old_ins[offsets[from][0]..offsets[from + 1][0]]);
            }
            // mergeins in C sorts the proposed arcs and eliminates duplicates
            // before merging. The existing inchains are ordered sets here.
            if arcs.is_empty() {
                continue;
            }
            self.interrupt()?;
            arcs.sort_unstable();
            arcs.dedup();
            for (arc, from) in arcs.drain(..) {
                self.add(arc, from, state)?;
            }
        }
        for &state in &states {
            for &from in empty_predecessors(state) {
                self.remove(Arc::Empty, from, state);
            }
        }
        for state in states {
            self.drop_if_useless(state);
        }
        Ok(())
    }

    fn findconstraintloop(&self) -> Result<Option<ConstraintLoop>, RegErrorCode> {
        const DONE: usize = usize::MAX;
        let limit = FINDCONSTRAINTLOOP.remaining(self.stack_used);
        let mut marks = vec![0; self.states.len()];
        let mut stack = Vec::new();
        for state in self.state_order() {
            if marks[state] == DONE {
                continue;
            }
            if limit == 0 {
                return Err(RegErrorCode::ETooBig);
            }
            marks[state] = 1;
            stack.push((state, self.states[state].outs.range((Arc::Bol(0), 0), None)));
            while let Some((from, outs)) = stack.last_mut() {
                if let Some(&(_, to)) = outs.next() {
                    if stack.len() == limit {
                        return Err(RegErrorCode::ETooBig);
                    }
                    match marks[to] {
                        DONE => {}
                        0 => {
                            marks[to] = stack.len() + 1;
                            stack.push((to, self.states[to].outs.range((Arc::Bol(0), 0), None)));
                        }
                        depth => {
                            return Ok(Some(ConstraintLoop {
                                states: stack[depth - 1..]
                                    .iter()
                                    .map(|(state, _)| *state)
                                    .collect(),
                                depth: stack.len() + 1,
                            }));
                        }
                    }
                } else {
                    marks[*from] = DONE;
                    stack.pop();
                }
            }
        }
        Ok(None)
    }

    fn fixconstraintloops(&mut self) -> Result<(), RegErrorCode> {
        if self.internal_constraints == 0 && FINDCONSTRAINTLOOP.remaining(self.stack_used) >= 3 {
            return Ok(());
        }
        let loops: Edges = self
            .live
            .iter()
            .flat_map(|&state| {
                self.states[state]
                    .outs
                    .iter()
                    .filter_map(move |&(arc, to)| {
                        (arc.constraint() && to == state).then_some((arc, state, to))
                    })
            })
            .collect();
        for (arc, from, to) in loops {
            self.remove(arc, from, to);
        }
        while let Some(cycle) = self.findconstraintloop()? {
            self.breakconstraintloop(cycle)?;
        }
        Ok(())
    }

    fn breakconstraintloop(&mut self, cycle: ConstraintLoop) -> Result<(), RegErrorCode> {
        // The oracle inlines breakconstraintloop into findconstraintloop.
        // The clone call also passes two stack arguments.
        let caller = self.stack_used + cycle.depth * FINDCONSTRAINTLOOP.frame + 16;
        let cycle = &cycle.states;
        let mut from = cycle[0];
        let mut to = cycle[1];
        let mut known = BTreeSet::new();

        // A unique constraint on the chosen step will already be satisfied
        // everywhere in the clone tree. Prefer a step with that property.
        for (&head, &tail) in cycle.iter().zip(cycle.iter().cycle().skip(1)) {
            let mut arcs = self.states[head]
                .outs
                .iter()
                .filter_map(|&(arc, dest)| (arc.constraint() && dest == tail).then_some(arc));
            if let Some(arc) = arcs.next()
                && arcs.next().is_none()
            {
                from = head;
                to = tail;
                known.insert(arc);
                break;
            }
        }

        let root = self.newstate()?;
        self.clonesuccessorstates(
            to,
            root,
            from,
            known,
            CLONESUCCESSORSTATES.remaining(caller),
        )?;
        let keep = !self.states[root].outs.is_empty();
        if !keep {
            self.dropstate(root);
        }
        let arcs: Vec<_> = self.states[from]
            .outs
            .iter()
            .filter_map(|&(arc, dest)| (arc.constraint() && dest == to).then_some(arc))
            .collect();
        for arc in arcs {
            if keep {
                self.add(arc, from, root)?;
            }
            self.remove(arc, from, to);
        }
        Ok(())
    }

    fn clonesuccessorstates(
        &mut self,
        source: StateId,
        root: StateId,
        predecessor: StateId,
        known: BTreeSet<Arc>,
        limit: usize,
    ) -> Result<(), RegErrorCode> {
        let mut stack = vec![(source, root, HashSet::from([predecessor]), known, 1)];
        while let Some((old, copy, mut done, known, depth)) = stack.pop() {
            let mut merge = vec![(old, depth)];
            let mut children = BTreeMap::<StateId, BTreeSet<Arc>>::new();
            while let Some((state, depth)) = merge.pop() {
                if depth > limit {
                    return Err(RegErrorCode::ETooBig);
                }
                if !done.insert(state) {
                    continue;
                }
                children.remove(&state);
                for (arc, to) in self.states[state].outs.snapshot() {
                    if !arc.constraint()
                        || !self.states[to]
                            .outs
                            .iter()
                            .any(|&(arc, _)| arc.constraint())
                    {
                        self.add(arc, copy, to)?;
                    } else if !done.contains(&to) {
                        if known.contains(&arc) {
                            merge.push((to, depth + 1));
                        } else {
                            children.entry(to).or_default().insert(arc);
                        }
                    }
                }
            }

            // Parallel arcs to an original successor share one clone. Only a
            // single inarc proves that its constraint held when it was entered.
            for (source, arcs) in children {
                let child = self.newstate()?;
                let mut child_known = known.clone();
                if arcs.len() == 1 {
                    child_known.insert(*arcs.first().unwrap());
                }
                for arc in arcs {
                    self.add(arc, copy, child)?;
                }
                stack.push((source, child, done.clone(), child_known, depth + 1));
            }
        }
        Ok(())
    }

    fn pullback(&mut self, cmap: &Colormap, bos: [Color; 2]) -> Result<(), RegErrorCode> {
        while self.internal_constraints != 0 {
            let mut progress = false;
            for state in self.state_order() {
                let mut intermediates = HashMap::new();
                let constraints: Vec<_> = self.states[state]
                    .outs
                    .range((Arc::Bol(0), 0), Some((Arc::Lacon(0), 0)))
                    .copied()
                    .collect();
                for (con, to) in constraints {
                    if matches!(con, Arc::Bol(_) | Arc::Behind(_)) {
                        progress |= self.pull(cmap, con, state, to, &mut intermediates)?;
                    }
                }
                self.drop_if_useless(state);
            }
            if !progress {
                break;
            }
        }
        let initial: Vec<_> = self.states[PRE]
            .outs
            .range((Arc::Bol(0), 0), Some((Arc::Eol(0), 0)))
            .copied()
            .collect();
        for (arc, to) in initial {
            if let Arc::Bol(marker) = arc {
                self.add(Arc::Plain(bos[usize::from(marker)]), PRE, to)?;
                self.remove(arc, PRE, to);
            }
        }
        Ok(())
    }

    fn pull(
        &mut self,
        cmap: &Colormap,
        con: Arc,
        mut from: StateId,
        to: StateId,
        intermediates: &mut HashMap<(StateId, StateId), StateId>,
    ) -> Result<bool, RegErrorCode> {
        if from <= PRE {
            return Ok(false);
        }
        if self.states[from].ins.is_empty() {
            self.remove(con, from, to);
            return Ok(true);
        }
        // Isolate the constraint before changing the incoming transitions.
        if self.states[from].outs.len() > 1 {
            let clone = self.newstate()?;
            for (arc, source) in self.states[from].ins.snapshot() {
                self.add(arc, source, clone)?;
            }
            self.add(con, clone, to)?;
            self.remove(con, from, to);
            from = clone;
        }
        for (arc, source) in self.states[from].ins.snapshot() {
            match combine_constraint(cmap, con, arc) {
                Combined::Reject => {}
                Combined::Keep => continue,
                Combined::Replace(replacement) => self.add(replacement, source, to)?,
                Combined::Swap => {
                    let middle = self.constraint_middle(intermediates, source, to)?;
                    self.add(con, source, middle)?;
                    self.add(arc, middle, to)?;
                }
            }
            self.remove(arc, source, from);
        }
        self.moveins(from, to)?;
        self.remove(con, from, to);
        Ok(true)
    }

    fn pushfwd(&mut self, cmap: &Colormap, eos: [Color; 2]) -> Result<(), RegErrorCode> {
        while self.internal_constraints != 0 {
            let mut progress = false;
            for state in self.state_order() {
                let mut intermediates = HashMap::new();
                let constraints: Vec<_> = self.states[state]
                    .ins
                    .range((Arc::Bol(0), 0), Some((Arc::Lacon(0), 0)))
                    .copied()
                    .collect();
                for (con, from) in constraints {
                    if matches!(con, Arc::Eol(_) | Arc::Ahead(_)) {
                        progress |= self.push(cmap, con, from, state, &mut intermediates)?;
                    }
                }
                self.drop_if_useless(state);
            }
            if !progress {
                break;
            }
        }
        let terminal: Vec<_> = self.states[POST]
            .ins
            .range((Arc::Eol(0), 0), Some((Arc::Behind(0), 0)))
            .copied()
            .collect();
        for (arc, from) in terminal {
            if let Arc::Eol(marker) = arc {
                self.add(Arc::Plain(eos[usize::from(marker)]), from, POST)?;
                self.remove(arc, from, POST);
            }
        }
        Ok(())
    }

    fn push(
        &mut self,
        cmap: &Colormap,
        con: Arc,
        from: StateId,
        mut to: StateId,
        intermediates: &mut HashMap<(StateId, StateId), StateId>,
    ) -> Result<bool, RegErrorCode> {
        if to <= PRE {
            return Ok(false);
        }
        if self.states[to].outs.is_empty() {
            self.remove(con, from, to);
            return Ok(true);
        }
        if self.states[to].ins.len() > 1 {
            let clone = self.newstate()?;
            for (arc, dest) in self.states[to].outs.snapshot() {
                self.add(arc, clone, dest)?;
            }
            self.add(con, from, clone)?;
            self.remove(con, from, to);
            to = clone;
        }
        for (arc, dest) in self.states[to].outs.snapshot() {
            match combine_constraint(cmap, con, arc) {
                Combined::Reject => {}
                Combined::Keep => continue,
                Combined::Replace(replacement) => self.add(replacement, from, dest)?,
                Combined::Swap => {
                    let middle = self.constraint_middle(intermediates, from, dest)?;
                    self.add(con, middle, dest)?;
                    self.add(arc, from, middle)?;
                }
            }
            self.remove(arc, to, dest);
        }
        self.moveouts(to, from)?;
        self.remove(con, from, to);
        Ok(true)
    }

    fn constraint_middle(
        &mut self,
        intermediates: &mut HashMap<(StateId, StateId), StateId>,
        from: StateId,
        to: StateId,
    ) -> Result<StateId, RegErrorCode> {
        if let Some(&state) = intermediates.get(&(from, to)) {
            return Ok(state);
        }
        let state = self.newstate()?;
        intermediates.insert((from, to), state);
        Ok(state)
    }

    fn state_order(&self) -> Vec<StateId> {
        let mut states: Vec<_> = self.live.iter().copied().collect();
        states.sort_unstable_by_key(|&state| self.states[state].ordinal);
        states
    }

    fn drop_if_useless(&mut self, state: StateId) {
        if state > PRE && (self.states[state].ins.is_empty() || self.states[state].outs.is_empty())
        {
            self.dropstate(state);
        }
    }

    pub(super) fn optimize(
        &mut self,
        cmap: &Colormap,
        bos: [Color; 2],
        eos: [Color; 2],
    ) -> Result<(i64, MatchAll), RegErrorCode> {
        let impossible: Edges = self
            .live
            .iter()
            .flat_map(|&from| {
                self.states[from].outs.iter().filter_map(move |&(arc, to)| {
                    (arc == Arc::CantMatch).then_some((arc, from, to))
                })
            })
            .collect();
        for (arc, from, to) in impossible {
            self.remove(arc, from, to);
        }
        self.cleanup()?;
        // These boundary markers need no constraint propagation. Convert
        // them before epsilon elimination can fan them out to many states.
        let outer_markers_only =
            self.internal_constraints == 0 && FINDCONSTRAINTLOOP.remaining(self.stack_used) >= 3;
        if outer_markers_only {
            self.pullback(cmap, bos)?;
            self.pushfwd(cmap, eos)?;
        }
        self.fixempties()?;
        if !outer_markers_only {
            self.fixconstraintloops()?;
            self.pullback(cmap, bos)?;
            self.pushfwd(cmap, eos)?;
        }
        self.cleanup()?;
        self.analyze(bos, eos)
    }

    fn analyze(&self, bos: [Color; 2], eos: [Color; 2]) -> Result<(i64, MatchAll), RegErrorCode> {
        if self.states[PRE].outs.is_empty() {
            return Ok((REG_UIMPOSSIBLE, None));
        }
        let matchall = self.checkmatchall(bos, eos)?;
        let empty = self.states[PRE]
            .outs
            .iter()
            .any(|&(_, next)| self.states[next].outs.iter().any(|&(_, to)| to == POST));
        let info = if empty { REG_UEMPTYMATCH } else { 0 };
        Ok((info, matchall))
    }

    pub(super) fn makesearch(&mut self, bos: [Color; 2]) -> Result<bool, RegErrorCode> {
        let unanchored = self.states[PRE]
            .outs
            .iter()
            .any(|&(arc, _)| arc != Arc::Plain(bos[0]) && arc != Arc::Plain(bos[1]));
        if unanchored {
            self.add(Arc::Plain(RAINBOW), PRE, PRE)?;
            for color in bos {
                self.add(Arc::Plain(color), PRE, PRE)?;
            }
        }
        let split: BTreeSet<_> = self.states[PRE]
            .outs
            .iter()
            .filter_map(|&(_, to)| {
                self.states[to]
                    .ins
                    .iter()
                    .any(|&(_, from)| from != PRE)
                    .then_some(to)
            })
            .collect();
        for state in split {
            let progress = self.newstate()?;
            self.interrupt()?;
            for (arc, to) in self.states[state].outs.snapshot() {
                self.add(arc, progress, to)?;
            }
            for (arc, from) in self.states[state].ins.snapshot() {
                if from != PRE {
                    self.add(arc, from, progress)?;
                    self.remove(arc, from, state);
                }
            }
        }
        Ok(unanchored)
    }

    fn checkmatchall(&self, bos: [Color; 2], eos: [Color; 2]) -> Result<MatchAll, RegErrorCode> {
        if self.live.len() > 2 * DUPINF || self.states[PRE].outs.is_empty() {
            return Ok(None);
        }
        for &state in &self.live {
            if !self.states[state].outs.iter().all(|&(arc, to)| {
                arc == Arc::Plain(RAINBOW)
                    || (state == PRE && bos.into_iter().any(|c| arc == Arc::Plain(c)))
                    || (to == POST && eos.into_iter().any(|c| arc == Arc::Plain(c)))
            }) {
                return Ok(None);
            }
        }
        for (edges, colors) in [(&self.states[PRE].outs, bos), (&self.states[POST].ins, eos)] {
            let ends = |color| {
                edges
                    .iter()
                    .filter_map(move |&(arc, to)| (arc == Arc::Plain(color)).then_some(to))
            };
            if colors
                .into_iter()
                .any(|color| !ends(color).eq(ends(RAINBOW)))
            {
                return Ok(None);
            }
        }
        let Some(lengths) = self.matchall_paths()? else {
            return Ok(None);
        };
        let min = lengths.iter().position(|&yes| yes).unwrap();
        let max = lengths.iter().rposition(|&yes| yes).unwrap();
        if lengths[min..=max].iter().all(|&yes| yes) {
            Ok(Some((min - 1, max - 1)))
        } else {
            Ok(None)
        }
    }

    fn matchall_paths(&self) -> Result<Option<[bool; DUPINF + 2]>, RegErrorCode> {
        let limit = CHECKMATCHALL.remaining(self.stack_used);
        let mut active = HashSet::from([PRE]);
        let mut done = HashMap::<StateId, [bool; DUPINF + 2]>::new();
        let mut stack = vec![(PRE, self.states[PRE].outs.iter())];
        self.interrupt()?;

        while let Some((state, outs)) = stack.last_mut() {
            if let Some(&(arc, to)) = outs.next() {
                if arc != Arc::Plain(RAINBOW)
                    || to == POST
                    || to == *state
                    || done.contains_key(&to)
                {
                    continue;
                }
                if stack.len() == limit || !active.insert(to) {
                    return Ok(None);
                }
                self.interrupt()?;
                stack.push((to, self.states[to].outs.iter()));
                continue;
            }

            let (state, _) = stack.pop().unwrap();
            let mut lengths = [false; DUPINF + 2];
            let mut self_loop = false;
            for &(arc, to) in self.states[state].outs.iter() {
                if arc != Arc::Plain(RAINBOW) {
                    continue;
                }
                if to == POST {
                    lengths[0] = true;
                } else if to == state {
                    self_loop = true;
                } else {
                    let tail = &done[&to];
                    if tail[DUPINF] != tail[DUPINF + 1] {
                        return Ok(None);
                    }
                    for i in 0..DUPINF {
                        lengths[i + 1] |= tail[i];
                    }
                    lengths[DUPINF + 1] |= tail[DUPINF + 1];
                }
            }
            let Some(min) = lengths.iter().position(|&yes| yes) else {
                return Ok(None);
            };
            if self_loop {
                lengths[min..].fill(true);
            }
            done.insert(state, lengths);
            active.remove(&state);
        }
        Ok(done.remove(&PRE))
    }

    pub(super) fn compact(
        &self,
        ncolors: usize,
        bos: [Color; 2],
        eos: [Color; 2],
        matchall: Option<(usize, usize)>,
    ) -> Result<Cnfa, RegErrorCode> {
        let mut ids = vec![0; self.states.len()];
        for (id, &state) in self.live.iter().enumerate() {
            ids[state] = id;
        }
        let mut cnfa = Cnfa {
            arcs: Vec::with_capacity(self.arc_count),
            offsets: Vec::with_capacity(self.live.len() + 1),
            ncolors,
            bos,
            eos,
            matchall,
            pre: ids[PRE],
            post: ids[POST],
            no_progress: vec![false; self.live.len()],
            has_lacons: false,
        };
        for &state in &self.live {
            cnfa.offsets.push(cnfa.arcs.len());
            // Plain colors sort before the larger lookaround colors, and the
            // dense state IDs are assigned in ascending original ID order.
            for &(arc, to) in self.states[state].outs.iter() {
                let co = match arc {
                    Arc::Plain(color) => color,
                    Arc::Lacon(i) => {
                        let color = ncolors + i as usize;
                        if color > MAX_COLOR {
                            return Err(RegErrorCode::EColors);
                        }
                        cnfa.has_lacons = true;
                        color as Color
                    }
                    _ => return Err(RegErrorCode::Assert),
                };
                cnfa.arcs.push(Carc { co, to: ids[to] });
                if state == PRE {
                    cnfa.no_progress[ids[to]] = true;
                }
            }
        }
        cnfa.offsets.push(cnfa.arcs.len());
        cnfa.no_progress[cnfa.pre] = true;
        Ok(cnfa)
    }
}

struct ConstraintLoop {
    states: Vec<StateId>,
    depth: usize,
}

struct Fragment {
    // Edges refer to these dense indices. The fragment's endpoints are
    // always first, so copying can reuse this vector for the new state IDs.
    states: Vec<StateId>,
    edges: Edges,
}

// The outer BOL/EOL arcs are not in any cycles and only need conversion to
// pseudocolors. Most compiled subexpressions have no other constraints.
fn is_internal_constraint(arc: Arc, from: StateId, to: StateId) -> bool {
    match arc {
        Arc::Bol(_) => from != PRE || to == PRE,
        Arc::Eol(_) => to != POST || from == POST,
        other => other.constraint(),
    }
}

enum Combined {
    Reject,
    Keep,
    Replace(Arc),
    Swap,
}

fn combine_constraint(cmap: &Colormap, constraint: Arc, arc: Arc) -> Combined {
    use Arc::*;
    use Combined::*;
    match (constraint, arc) {
        (Bol(_) | Eol(_), Plain(_))
        | (Bol(_), Behind(_))
        | (Behind(_), Bol(_))
        | (Eol(_), Ahead(_))
        | (Ahead(_), Eol(_)) => Reject,
        (Bol(a), Bol(b)) | (Eol(a), Eol(b)) => {
            if a == b {
                Keep
            } else {
                Reject
            }
        }
        (Behind(a) | Ahead(a), Plain(b)) | (Behind(a), Behind(b)) | (Ahead(a), Ahead(b)) => {
            if a == b || (a == RAINBOW && !cmap.pseudo(b)) {
                Keep
            } else if b == RAINBOW && !cmap.pseudo(a) {
                Replace(arc.recolor(a))
            } else {
                Reject
            }
        }
        (Bol(_) | Behind(_), Eol(_) | Ahead(_) | Lacon(_))
        | (Eol(_) | Ahead(_), Bol(_) | Behind(_) | Lacon(_)) => Swap,
        _ => unreachable!("constraint {constraint:?} meets {arc:?}"),
    }
}
