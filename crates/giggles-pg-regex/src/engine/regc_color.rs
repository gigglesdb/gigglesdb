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
 * Translated to Rust from regc_color.c.
 */

use std::collections::{BTreeMap, BTreeSet};

use super::regc_cvec::Cvec;
use super::regc_pg_locale::classify;
use super::regguts::{Class, Color, MAX_COLOR, MAX_SIMPLE_CHR};
use super::{Chr, RegErrorCode, RegexLocale};

#[derive(Default)]
struct ColorDesc {
    nschrs: usize,
    nuchrs: usize,
    firstchr: Chr,
    pseudo: bool,
    sub: Option<Color>,
}

#[derive(Clone, Copy)]
struct Range {
    last: Chr,
    row: usize,
}

pub(super) struct Colormap {
    low: Vec<Color>,
    ranges: BTreeMap<Chr, Range>,
    high: Vec<Vec<Color>>,
    classbits: [usize; Class::ALL.len()],
    desc: Vec<ColorDesc>,
    free: Vec<Color>,
    open: Vec<Color>,
    locale: RegexLocale,
}

impl Colormap {
    pub(super) fn new(locale: RegexLocale) -> Self {
        Self {
            low: vec![0; MAX_SIMPLE_CHR as usize + 1],
            ranges: BTreeMap::new(),
            high: vec![vec![0]],
            classbits: [0; Class::ALL.len()],
            desc: vec![ColorDesc {
                nschrs: MAX_SIMPLE_CHR as usize + 1,
                nuchrs: 1,
                ..ColorDesc::default()
            }],
            free: Vec::new(),
            open: Vec::new(),
            locale,
        }
    }

    pub(super) fn get(&self, chr: Chr) -> Color {
        if chr <= MAX_SIMPLE_CHR {
            return self.low[chr as usize];
        }
        let row = self
            .ranges
            .range(..=chr)
            .next_back()
            .filter(|(_, range)| chr <= range.last)
            .map_or(0, |(_, range)| range.row);
        let mut col = 0;
        for class in Class::ALL {
            let bit = self.classbits[class as usize];
            if bit != 0 && classify(self.locale, class, chr) {
                col |= bit;
            }
        }
        self.high[row][col]
    }

    pub(super) fn ncolors(&self) -> usize {
        self.desc.len()
    }

    pub(super) fn pseudo(&self, color: Color) -> bool {
        self.desc[color as usize].pseudo
    }

    pub(super) fn singleton_simple(&self, color: Color) -> Option<Chr> {
        let desc = self.desc.get(color as usize)?;
        if desc.nschrs == 1 && desc.nuchrs == 0 && self.get(desc.firstchr) == color {
            Some(desc.firstchr)
        } else {
            None
        }
    }

    pub(super) fn real_colors(&self) -> impl Iterator<Item = Color> + '_ {
        self.desc
            .iter()
            .enumerate()
            .filter_map(|(i, desc)| (desc.nschrs != 0 || desc.nuchrs != 0).then_some(i as Color))
    }

    fn newcolor(&mut self) -> Result<Color, RegErrorCode> {
        if let Some(i) = self.free.pop() {
            return Ok(i);
        }
        let i = self.desc.len();
        if i > MAX_COLOR {
            return Err(RegErrorCode::EColors);
        }
        self.desc.push(ColorDesc::default());
        Ok(i as Color)
    }

    pub(super) fn pseudocolor(&mut self) -> Result<Color, RegErrorCode> {
        let color = self.newcolor()?;
        self.desc[color as usize].pseudo = true;
        Ok(color)
    }

    fn newsub(&mut self, color: Color) -> Result<Color, RegErrorCode> {
        let desc = &self.desc[color as usize];
        if let Some(sub) = desc.sub {
            return Ok(sub);
        }
        if desc.nschrs + desc.nuchrs == 1 {
            return Ok(color);
        }
        let sub = self.newcolor()?;
        self.desc[color as usize].sub = Some(sub);
        self.desc[sub as usize].sub = Some(sub);
        self.open.push(color);
        Ok(sub)
    }

    pub(super) fn subcolor(&mut self, chr: Chr) -> Result<Color, RegErrorCode> {
        let color = self.low[chr as usize];
        let sub = self.newsub(color)?;
        if color != sub {
            self.desc[color as usize].nschrs -= 1;
            let desc = &mut self.desc[sub as usize];
            if desc.nschrs == 0 {
                desc.firstchr = chr;
            }
            desc.nschrs += 1;
            self.low[chr as usize] = sub;
        }
        Ok(sub)
    }

    fn subcolorhi(&mut self, row: usize, col: usize) -> Result<Color, RegErrorCode> {
        let color = self.high[row][col];
        let sub = self.newsub(color)?;
        if color != sub {
            self.desc[color as usize].nuchrs -= 1;
            self.desc[sub as usize].nuchrs += 1;
            self.high[row][col] = sub;
        }
        Ok(sub)
    }

    fn newhicolorrow(&mut self, from: usize) -> Result<usize, RegErrorCode> {
        let cols = self.high[0].len();
        if self.high.len() >= (i32::MAX as usize) / (cols * 2) {
            return Err(RegErrorCode::ESpace);
        }
        let row = self.high[from].clone();
        for &color in &row {
            self.desc[color as usize].nuchrs += 1;
        }
        let id = self.high.len();
        self.high.push(row);
        Ok(id)
    }

    fn newhicolorcols(&mut self, class: Class) -> Result<usize, RegErrorCode> {
        let cols = self.high[0].len();
        if cols >= (i32::MAX as usize) / (self.high.len() * 2) {
            return Err(RegErrorCode::ESpace);
        }
        for row in &mut self.high {
            row.try_reserve_exact(cols)
                .map_err(|_| RegErrorCode::ESpace)?;
            for i in 0..cols {
                let color = row[i];
                row.push(color);
                self.desc[color as usize].nuchrs += 1;
            }
        }
        self.classbits[class as usize] = cols;
        Ok(cols)
    }

    fn subcoloronerow(
        &mut self,
        row: usize,
        colors: &mut BTreeSet<Color>,
    ) -> Result<(), RegErrorCode> {
        for col in 0..self.high[row].len() {
            colors.insert(self.subcolorhi(row, col)?);
        }
        Ok(())
    }

    fn subcoloronerange(
        &mut self,
        mut first: Chr,
        last: Chr,
        colors: &mut BTreeSet<Color>,
    ) -> Result<(), RegErrorCode> {
        let start = self
            .ranges
            .range(..=first)
            .next_back()
            .filter(|(_, range)| range.last >= first)
            .map_or(first, |(&key, _)| key);
        let overlapping: Vec<_> = self
            .ranges
            .range(start..=last)
            .map(|(&key, &range)| (key, range))
            .collect();
        for (key, old) in overlapping {
            if first < key {
                let row = self.newhicolorrow(0)?;
                self.ranges.insert(first, Range { last: key - 1, row });
                self.subcoloronerow(row, colors)?;
                first = key;
            }
            let row = if first == key && last >= old.last {
                old.row
            } else {
                self.ranges.remove(&key);
                if key < first {
                    self.ranges.insert(
                        key,
                        Range {
                            last: first - 1,
                            row: old.row,
                        },
                    );
                }
                let row = self.newhicolorrow(old.row)?;
                self.ranges.insert(
                    first,
                    Range {
                        last: old.last.min(last),
                        row,
                    },
                );
                if old.last > last {
                    let trailing = if key < first {
                        self.newhicolorrow(old.row)?
                    } else {
                        old.row
                    };
                    self.ranges.insert(
                        last + 1,
                        Range {
                            last: old.last,
                            row: trailing,
                        },
                    );
                }
                row
            };
            self.subcoloronerow(row, colors)?;
            first = old.last + 1;
        }
        if first <= last {
            let row = self.newhicolorrow(0)?;
            self.ranges.insert(first, Range { last, row });
            self.subcoloronerow(row, colors)?;
        }
        Ok(())
    }

    pub(super) fn subcolorcvec(&mut self, cv: Cvec) -> Result<BTreeSet<Color>, RegErrorCode> {
        let mut colors = BTreeSet::new();
        for chr in cv.chrs {
            if chr <= MAX_SIMPLE_CHR {
                colors.insert(self.subcolor(chr)?);
            } else {
                self.subcoloronerange(chr, chr, &mut colors)?;
            }
        }
        for (first, last) in cv.ranges {
            for chr in first..=last.min(MAX_SIMPLE_CHR) {
                colors.insert(self.subcolor(chr)?);
            }
            if last > MAX_SIMPLE_CHR {
                self.subcoloronerange(first.max(MAX_SIMPLE_CHR + 1), last, &mut colors)?;
            }
        }
        if let Some(class) = cv.class {
            let mut bit = self.classbits[class as usize];
            if bit == 0 {
                bit = self.newhicolorcols(class)?;
            }
            for row in 0..self.high.len() {
                for col in 0..self.high[row].len() {
                    if col & bit != 0 {
                        colors.insert(self.subcolorhi(row, col)?);
                    }
                }
            }
        }
        Ok(colors)
    }

    // For each split, the caller copies the parent's arcs to the subcolor.
    // A parent with no remaining characters is replaced instead.
    pub(super) fn okcolors(&mut self) -> impl Iterator<Item = (Color, Color, bool)> + '_ {
        let mut open = std::mem::take(&mut self.open);
        open.sort_unstable();
        open.into_iter().map(|parent| {
            let desc = &mut self.desc[parent as usize];
            let sub = desc.sub.take().unwrap();
            let replace = desc.nschrs == 0 && desc.nuchrs == 0;
            self.desc[sub as usize].sub = None;
            if replace && parent != 0 {
                if self.desc[sub as usize].nuchrs == 0 {
                    // All members moved to the subcolor. Keep the parent
                    // ID and retag only this bracket's new arcs. Otherwise
                    // another [ab] retags every earlier [ab] on the NFA.
                    for color in &mut self.low {
                        if *color == sub {
                            *color = parent;
                        }
                    }
                    self.desc[parent as usize] = std::mem::take(&mut self.desc[sub as usize]);
                    self.free.push(sub);
                    return (sub, parent, true);
                }
                self.free.push(parent);
            }
            // WHITE can be empty but is never reassigned by PostgreSQL.
            (parent, sub, replace)
        })
    }
}
