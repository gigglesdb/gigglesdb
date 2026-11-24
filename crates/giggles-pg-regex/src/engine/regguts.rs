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
 * Translated to Rust from regguts.h.
 */

use super::regc_color::Colormap;

pub(super) const CHR_MAX: u32 = 0x7fff_fffe;
pub(super) const MAX_SIMPLE_CHR: u32 = 0x7ff;
pub(super) const MAX_COLOR: usize = 32_767;
pub(super) const RAINBOW: Color = -2;
pub(super) const DUPMAX: usize = 255;
pub(super) const DUPINF: usize = DUPMAX + 1;

pub(super) type Color = i16;
pub(super) type StateId = usize;
pub(super) type SubreId = usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Class {
    Alnum,
    Alpha,
    Ascii,
    Blank,
    Cntrl,
    Digit,
    Graph,
    Lower,
    Print,
    Punct,
    Space,
    Upper,
    Xdigit,
    Word,
}

impl Class {
    pub(super) const ALL: [Self; 14] = [
        Self::Alnum,
        Self::Alpha,
        Self::Ascii,
        Self::Blank,
        Self::Cntrl,
        Self::Digit,
        Self::Graph,
        Self::Lower,
        Self::Print,
        Self::Punct,
        Self::Space,
        Self::Upper,
        Self::Xdigit,
        Self::Word,
    ];

    pub(super) fn name(self) -> &'static str {
        [
            "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print",
            "punct", "space", "upper", "xdigit", "word",
        ][self as usize]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Look {
    BehindNeg,
    BehindPos,
    AheadNeg,
    AheadPos,
}

impl Look {
    pub(super) fn ahead(self) -> bool {
        matches!(self, Self::AheadNeg | Self::AheadPos)
    }

    pub(super) fn positive(self) -> bool {
        matches!(self, Self::BehindPos | Self::AheadPos)
    }
}

pub(super) const LONGER: u8 = 1;
pub(super) const SHORTER: u8 = 2;
pub(super) const MIXED: u8 = 4;
pub(super) const CAP: u8 = 8;
pub(super) const BACKR: u8 = 16;
pub(super) const BRUSE: u8 = 32;
pub(super) const MESSY: u8 = MIXED | CAP | BACKR;
pub(super) const PREF: u8 = LONGER | SHORTER;

pub(super) fn up(flags: u8) -> u8 {
    (flags & MESSY) | ((flags << 2) & (flags << 1) & MIXED)
}

pub(super) fn combine(first: u8, second: u8) -> u8 {
    up(first | second)
        | if first & PREF != 0 {
            first & PREF
        } else {
            second & PREF
        }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Carc {
    pub(super) co: Color,
    pub(super) to: StateId,
}

// Each state holds an offset into arcs. The next offset marks the end of its list.
#[derive(Default)]
pub(super) struct Cnfa {
    pub(super) ncolors: usize,
    pub(super) pre: StateId,
    pub(super) post: StateId,
    pub(super) bos: [Color; 2],
    pub(super) eos: [Color; 2],
    pub(super) has_lacons: bool,
    pub(super) matchall: Option<(usize, usize)>,
    pub(super) no_progress: Vec<bool>,
    pub(super) offsets: Vec<usize>,
    pub(super) arcs: Vec<Carc>,
}

impl Cnfa {
    pub(super) fn outs(&self, state: StateId) -> &[Carc] {
        &self.arcs[self.offsets[state]..self.offsets[state + 1]]
    }
}

pub(super) struct Subre {
    pub(super) op: u8,
    pub(super) flags: u8,
    pub(super) capno: usize,
    pub(super) backno: usize,
    pub(super) min: usize,
    pub(super) max: usize,
    pub(super) child: Option<SubreId>,
    pub(super) sibling: Option<SubreId>,
    pub(super) cnfa: std::sync::Arc<Cnfa>,
}

pub(super) struct Lacon {
    pub(super) kind: Look,
    pub(super) cnfa: std::sync::Arc<Cnfa>,
}

pub(super) struct Guts {
    pub(super) cmap: Colormap,
    pub(super) tree: Vec<Subre>,
    pub(super) root: SubreId,
    pub(super) search: Cnfa,
    pub(super) lacons: Vec<Lacon>,
}
