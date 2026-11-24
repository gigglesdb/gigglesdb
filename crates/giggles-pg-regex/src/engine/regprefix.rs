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
 * Translated to Rust from regprefix.c.
 */

use super::regguts::RAINBOW;
use super::{Chr, PrefixKind, REG_UIMPOSSIBLE, RegErrorCode, Regex};

pub(super) fn prefix(regex: &Regex) -> Result<(PrefixKind, Vec<Chr>), RegErrorCode> {
    let guts = &regex.guts;
    let cnfa = &guts.tree[guts.root].cnfa;
    if regex.info & REG_UIMPOSSIBLE != 0 || cnfa.matchall.is_some() {
        return Ok((PrefixKind::NoMatch, Vec::new()));
    }
    let entry = cnfa.outs(cnfa.pre);
    let Some(first) = entry.first() else {
        return Ok((PrefixKind::NoMatch, Vec::new()));
    };
    if !entry
        .iter()
        .all(|arc| cnfa.bos.contains(&arc.co) && arc.to == first.to)
    {
        return Ok((PrefixKind::NoMatch, Vec::new()));
    }

    let mut prefix = Vec::new();
    prefix
        .try_reserve(cnfa.offsets.len() - 1)
        .map_err(|_| RegErrorCode::ESpace)?;
    let mut state = first.to;
    loop {
        let mut arcs = cnfa
            .outs(state)
            .iter()
            .filter(|arc| !cnfa.bos.contains(&arc.co));
        let Some(first) = arcs.next() else {
            break;
        };
        let color = first.co;
        if color == RAINBOW || color as usize >= cnfa.ncolors || cnfa.eos.contains(&color) {
            break;
        }
        let mut multiple = false;
        let same_color = arcs.all(|arc| {
            multiple = true;
            arc.co == color
        });
        if !same_color {
            break;
        }
        let Some(chr) = guts.cmap.singleton_simple(color) else {
            break;
        };
        prefix.push(chr);
        if multiple {
            break;
        }
        state = first.to;
    }

    let tail = cnfa.outs(state);
    let kind = if !tail.is_empty()
        && tail
            .iter()
            .all(|arc| arc.to == cnfa.post && cnfa.eos.contains(&arc.co))
    {
        PrefixKind::Exact
    } else if prefix.is_empty() {
        PrefixKind::NoMatch
    } else {
        PrefixKind::Prefix
    };
    Ok((kind, prefix))
}
