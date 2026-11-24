"""Generate the PostgreSQL parser transitions and en_US.UTF-8 character tables.

Usage: python generate_tables.py POSTGRES_SOURCE CODEPOINTS_TSV HIGH_TSV
Generate HIGH_TSV with codepoints_high.sql on the same en_US.UTF-8 oracle.
"""

import itertools
from pathlib import Path
import re
import sys

CRATE = Path(__file__).resolve().parents[1]
PARSER = Path(sys.argv[1]) / "src/backend/tsearch/wparser_def.c"
VECTORS = tuple(map(Path, sys.argv[2:]))

CONDITIONS = {
    "NULL": "Always",
    "p_isEOF": "Eof",
    "p_iseqC": "Eq",
    "p_isneC": "Ne",
    "p_isignore": "Ignore",
    "p_isasclet": "AsciiLetter",
    "p_isalpha": "Alpha",
    "p_isdigit": "Digit",
    "p_isalnum": "Alnum",
    "p_isnotalnum": "NotAlnum",
    "p_isspecial": "Special",
    "p_isspace": "Space",
    "p_isxdigit": "Hex",
    "p_isnotspace": "NotSpace",
    "p_isstophost": "StopHost",
    "p_ishost": "Host",
    "p_isURLPath": "UrlPath",
    "p_isurlchar": "UrlChar",
    "p_islower": "Lower",
    "p_isupper": "Upper",
    "p_ispunct": "Punct",
    "p_isprint": "Print",
    "p_isascii": "Ascii",
    "p_isnotalpha": "NotAlpha",
    "p_isnotdigit": "NotDigit",
}

ACTIONS = {
    "A_NEXT": "0",
    "A_BINGO": "BINGO",
    "A_POP": "POP",
    "A_PUSH": "PUSH",
    "A_RERUN": "RERUN",
    "A_CLEAR": "CLEAR",
    "A_MERGE": "MERGE",
    "A_CLRALL": "CLEAR_ALL",
}

HANDLERS = {
    "NULL": "None",
    "SpecialTags": "Tags",
    "SpecialFURL": "FullUrl",
    "SpecialHyphen": "Hyphen",
    "SpecialVerVersion": "Version",
}


def generate_parser():
    data = PARSER.read_text(encoding="utf-8")
    state_names = re.findall(r"^\s+TPS_(\w+)(?:\s*=\s*0)?,", data, re.M)
    types = dict(re.findall(r"^#define (\w+)\s+(\d+)$", data, re.M))
    arrays = dict(re.findall(
        r"static const TParserStateActionItem actionTPS_(\w+)\[\] = \{(.*?)\n\};",
        data, re.S,
    ))
    rows = [
        "// Generated from PostgreSQL 18.6 wparser_def.c by tools/generate_tables.py.",
        "// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group",
        "use super::*;",
        "",
        "#[derive(Clone, Copy, Debug)]",
        "pub(super) enum State {",
        *(f"    {state}," for state in state_names),
        "    Null,",
        "}",
        "",
        "#[rustfmt::skip]",
        "pub(super) const ACTIONS: &[&[Action]] = &[",
    ]
    for state in state_names:
        rows.append(f"    &[ // {state}")
        for cond, ch, flags, target, token, handler in re.findall(
            r"\{\s*(\w+),\s*(0|'(?:\\.|[^'\\])'),\s*([^,]+),"
            r"\s*TPS_(\w+),\s*(\w+),\s*(\w+)\s*\}", arrays[state]
        ):
            ch = ch if ch == "0" else "b" + ch
            flags = [ACTIONS[x.strip()] for x in flags.split("|")]
            flags = " | ".join(flag for flag in flags if flag != "0") or "0"
            token = token if token == "0" else types[token]
            rows.append(
                f"        Action::new(Condition::{CONDITIONS[cond]}, {ch}, {flags}, "
                f"State::{target}, {token}, Special::{HANDLERS[handler]}),"
            )
        assert rows[-1].startswith("        Action::new(Condition::Always")
        rows.append("    ],")
    rows.append("];")
    for name in ("tok_alias", "lex_descr"):
        table = re.search(rf"{name}\[\] = \{{(.*?)\}};", data, re.S)[1]
        rows.extend([
            "",
            f"pub(super) const {name.upper()}: [&str; 24] = [",
            *(f"    {value}," for value in re.findall(r'"[^"]*"', table)),
            "];",
        ])
    (CRATE / "src/parser/actions.rs").write_text(
        "\n".join(rows) + "\n", encoding="utf-8", newline="\n"
    )


def classify(codepoint, tokens, lexemes):
    char = chr(codepoint)
    if tokens.endswith(f" word={char}"):
        category = 1
    elif tokens.startswith(f"word=x{char}y "):
        category = 2
    else:
        category = 0
    # The probe ends with Q. Alpha code points are part of its single lexeme.
    delta = 0
    if category == 1:
        assert lexemes.startswith("'") and lexemes.endswith("q':1"), lexemes
        assert len(lexemes) == 6, lexemes
        delta = ord(lexemes[1]) - codepoint
    return category, delta


def generate_locale():
    rows = []
    sample = []
    previous_class = None
    # U+0085 and U+2028 are valid characters inside a COPY row, so split only on LF.
    lines = (line for source in VECTORS for line in source.read_text(encoding="utf-8").split("\n") if line)
    for index, line in enumerate(lines):
        codepoint, tokens, lexemes = line.split("\t")
        codepoint = int(codepoint)
        category, delta = classify(codepoint, tokens, lexemes)
        if index % 13 == 0 or category != previous_class:
            sample.append(line)
        previous_class = category
        rows.append((codepoint, category, delta))

    generated = [
        "// Generated from the PostgreSQL 18.6 en_US.UTF-8 oracle by",
        "// tools/generate_tables.py. The ranges contain alpha and word-continuation",
        "// classes, plus the simple lowercase mapping.",
        "pub(super) const RANGES: &[(u32, u32, u8, i32)] = &[",
    ]
    for (_, category, delta), values in itertools.groupby(
        enumerate(rows), lambda item: (item[1][0] - item[0], item[1][1], item[1][2])
    ):
        if category == 0:
            continue
        group = [row[0] for _, row in values]
        generated.append(
            f"    (0x{group[0]:X}, 0x{group[-1]:X}, {category}, {delta}),"
        )
    generated.append("];")
    (CRATE / "src/locale_ranges.rs").write_text(
        "\n".join(generated) + "\n", encoding="utf-8", newline="\n"
    )
    (CRATE / "tests/fixtures/codepoints.tsv").write_text(
        "\n".join(sample) + "\n", encoding="utf-8", newline="\n"
    )


if __name__ == "__main__":
    generate_parser()
    generate_locale()
