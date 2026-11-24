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
 * Translated to Rust from regc_cvec.c.
 */

use super::{Chr, regguts::Class};

#[derive(Default)]
pub(super) struct Cvec {
    pub(super) chrs: Vec<Chr>,
    pub(super) ranges: Vec<(Chr, Chr)>,
    pub(super) class: Option<Class>,
}

impl Cvec {
    pub(super) fn single(chr: Chr) -> Self {
        Self {
            chrs: vec![chr],
            ..Self::default()
        }
    }
}

/// Capacity of PostgreSQL's transient character vector within one compile.
/// The C implementation reuses both capacities when the request fits and
/// replaces both when either is too small. Case folding uses the retained
/// character capacity even though Rust's `Cvec` values own their storage.
pub(super) struct CvecSpace {
    chrs: usize,
    ranges: usize,
}

impl Default for CvecSpace {
    fn default() -> Self {
        Self {
            chrs: 100,
            ranges: 20,
        }
    }
}

impl CvecSpace {
    pub(super) fn request(&mut self, chrs: usize, ranges: usize) -> usize {
        if chrs > self.chrs || ranges > self.ranges {
            *self = Self { chrs, ranges };
        }
        self.chrs
    }
}
