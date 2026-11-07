# Type modifiers

PostgreSQL 18 type modifiers are catalog integers including the four-byte
varlena header offset. Numeric packs precision and a signed 11-bit scale;
character types store the character limit plus four. An absent modifier is -1.
Plain SQL `char` defaults to one character, whereas an unbounded `bpchar`
value has no limit and does not pad.

Numeric coercion rounds to the requested scale first, with ties away from zero,
then checks precision against the rounded result. Negative scale rounds left
of the decimal point; scale may exceed precision. Precision is 1..1000 and
scale is -1000..1000. NaN survives a valid modifier; infinities overflow bounded
numeric. Overflow carries SQLSTATE 22003 and PostgreSQL's scale/precision detail.
Invalid modifier syntax values carry 22023.

Character limits count Unicode characters, not encoded bytes. Assignment rejects
excess non-space characters, but discards excess ordinary spaces. Explicit casts
truncate excess characters. Bounded `char(n)` pads ordinary spaces to n characters;
`varchar(n)` does not. `length(bpchar)` and bpchar equality ignore trailing
ordinary spaces, while `octet_length(bpchar)` counts stored padding. C collation
orders UTF-8 strings by bytes. The fixture preserves padded char output using
`bpcharsend`; casting bpchar to text would remove its trailing spaces.

Source references (PostgreSQL REL_18_6):

- [numeric.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/numeric.c):
  `numerictypmodin`, `apply_typmod`, `apply_typmod_special`.
- [varchar.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/varchar.c):
  `varchar_input`, `bpchar_input`, `varchar`, `bpchar`, `bcTruelen`.

`tests/fixtures/typmod_pg18.tsv` is an executable PostgreSQL 18.6 oracle capture.
Its six tab-separated columns are `kind`, `input`, `args`,
`expected_text_or_ERROR`, `sqlstate`, and `detail_or_empty`; empty trailing
columns and spaces in values are significant. `numeric_apply` arguments are
`precision,scale`. `typmod` results contain `catalog integer,format_type`.
`bpchar_lengths` returns `length,octet_length`; `bpchar_eq` returns `t` or
`f`; `bpchar_order` returns `<`, `=`, or `>` for bpchar values under C collation.
Numeric modifier and character assignment errors include the primary message in the detail column.
Regeneration uses the checked-in Python script and the owned container command
recorded in `TYPMOD-PROVENANCE.txt`; no live oracle is required for fixture tests.
