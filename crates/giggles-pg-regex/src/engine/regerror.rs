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
 * Translated to Rust from regerror.c and regerrs.h.
 */

macro_rules! errors {
    ($($variant:ident = $code:literal, $name:literal, $message:literal;)*) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(i32)]
        pub enum RegErrorCode { $($variant = $code,)* }

        impl RegErrorCode {
            pub fn code(self) -> i32 {
                self as i32
            }

            pub fn name(self) -> &'static str {
                match self { $(Self::$variant => $name,)* }
            }

            pub fn message(self) -> &'static str {
                match self { $(Self::$variant => $message,)* }
            }
        }
    };
}

errors! {
    BadPat = 2, "REG_BADPAT", "invalid regexp (reg version 0.8)";
    ECollate = 3, "REG_ECOLLATE", "invalid collating element";
    ECtype = 4, "REG_ECTYPE", "invalid character class";
    EEscape = 5, "REG_EESCAPE", "invalid escape \\ sequence";
    ESubreg = 6, "REG_ESUBREG", "invalid backreference number";
    EBrack = 7, "REG_EBRACK", "brackets [] not balanced";
    EParen = 8, "REG_EPAREN", "parentheses () not balanced";
    EBrace = 9, "REG_EBRACE", "braces {} not balanced";
    BadBr = 10, "REG_BADBR", "invalid repetition count(s)";
    ERange = 11, "REG_ERANGE", "invalid character range";
    ESpace = 12, "REG_ESPACE", "out of memory";
    BadRpt = 13, "REG_BADRPT", "quantifier operand invalid";
    Assert = 15, "REG_ASSERT", "\"cannot happen\" -- you found a bug";
    InvArg = 16, "REG_INVARG", "invalid argument to regex function";
    Mixed = 17, "REG_MIXED", "character widths of regex and string differ";
    BadOpt = 18, "REG_BADOPT", "invalid embedded option";
    ETooBig = 19, "REG_ETOOBIG", "regular expression is too complex";
    EColors = 20, "REG_ECOLORS", "too many colors";
    Cancel = 100, "REG_CANCEL", "operation cancelled";
}
