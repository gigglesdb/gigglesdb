# giggles-pg-regex

A Rust port of PostgreSQL 18.6's regular expression library. The engine
implements Henry Spencer's advanced, extended, and basic regular expressions
(ARE, ERE, and BRE). The SQL layer follows PostgreSQL's `regexp.c` and the
regex replacement code in `varlena.c`. The reference C sources are in
[`upstream/`](upstream/).

## API

- [`engine`](src/engine/mod.rs) compiles and executes patterns over slices of
  `Chr` (Unicode code points). Use `Regex::compile`, `Regex::exec`, and
  `Regex::prefix` for matching and prefix extraction. Engine match offsets
  are zero-based, with an exclusive end. `compile_with_cancel` and
  `exec_with_cancel` accept an optional cancellation callback.
- [`api`](src/api/mod.rs) accepts UTF-8 `&str` and implements the POSIX match
  operators, the SQL `regexp_*` functions, `SIMILAR TO`, and regex substring
  extraction. Positions in the SQL API are one-based Unicode scalar
  positions. Errors carry a SQLSTATE and message.
- Pass `&RegexpContext::new(locale)` for a call without cancellation, or
  construct a `RegexpContext` with `cancel: Some(&check)`. When the callback
  returns `true`, a canceled compile or match produces SQLSTATE `57014`
  with the message `canceling statement due to user request`.

```rust
use giggles_pg_regex::api::{regexp_match, RegexLocale, RegexpContext};

let ctx = RegexpContext::new(RegexLocale::CUtf8);
assert_eq!(
    regexp_match("abc123", r"([0-9]+)", None, &ctx).unwrap(),
    Some(vec![Some("123")]),
);
```

## Locale strategies

- `RegexLocale::C` follows `PG_REGEX_STRATEGY_C`, using ASCII case conversion
  and classification for locale-dependent character classes. Explicit
  classes such as `[:cntrl:]` use PostgreSQL's fixed definitions.
- `RegexLocale::CUtf8` reproduces glibc `C.UTF-8` behavior using the checked-in
  [oracle tables](upstream/oracle/). The tables were captured from the
  PostgreSQL 18.6 image pinned in [NOTICE](NOTICE), with the database
  initialized using `initdb --locale=C.UTF-8 --encoding=UTF8`. The capture
  enumerated nonzero Unicode scalar values, coalesced contiguous code points
  that matched each PostgreSQL character class, and recorded the results of
  `upper()` and `lower()` for code points with a case mapping.

Run `python -I tools/gen_ctype.py` from the crate directory to regenerate
[`ctype_tables.rs`](src/engine/ctype_tables.rs) from `upstream/oracle/classes.csv`
and `upstream/oracle/case.csv`.

## Test corpora

- The [engine corpus](tests/test_regex_corpus.rs) replays PostgreSQL 18.6's
  `test_regex` and `test_regex_utf8` expected output, including compile
  diagnostics, regex info flags, and match rows.
- The [SQL API tests](tests/api_cases.rs) replay
  [`api_cases.json`](tests/data/api_cases.json), which covers calls drawn
  from PostgreSQL's `regex.sql` and `strings.sql` regression suites, the
  GigglesDB PostgreSQL function corpus, and additional edge cases.

Run `python -I tools/gen_api_cases.py --corpus ../../compat/corpus/pg_functions`
from the crate directory with the PostgreSQL 18.6 oracle container
`pgp-oracle-regex` running in `C.UTF-8`. Adjust `--corpus` to the directory
containing the `pgf_string_*.sql` files if the corpus lives in another
checkout. The script executes the cases via Docker and rewrites
`tests/data/api_cases.json`.

## Known differences

PostgreSQL can report SQLSTATE `XX000` with an `invalid memory alloc request
size` message for very large compiled patterns such as
`((a{1,40}){1,40}){1,40}` or a 20,000-branch word alternation. The crate can
return `regular expression is too complex` or a match result for such
patterns. Examples also include `(?:a{1,255}){1,255}` and
`(?n)(Σé*٠{2,255}\101){2,255}`.

The crate models the stack budget of top-level SQL on the Debian x86-64
PostgreSQL 18.6 oracle with `max_stack_depth = '2MB'`. PostgreSQL measures
the actual backend stack. Its limits can shift with the call site,
architecture, build, and `max_stack_depth`. For example, the review
harness's PL/pgSQL wrapper accepted 43,511 literal atoms and 8,158 nested
capture groups. The crate and top-level SQL accepted 43,546 and 8,165,
respectively.

The depth tests cover graph traversal and parsing boundaries, lookaround
execution, and subexpression dissection on 2 MB Rust threads. The
reachability and epsilon traversal budgets also use the oracle's C frame
sizes because earlier graph passes can trip the guard on the same pattern.
The compiler's explicit stacks and arc order can visit a branching graph
in a different order from PostgreSQL, so its first exhausted traversal can
differ. The `cleartraverse` limit is masked by the stricter walks over the
same fragment in the measured cases. The match-all optimization considers
at most 512 states, below its standalone recursion limit.

Speed on very large patterns is not yet at PostgreSQL's level. Results are
the same, but at review time (release builds) compiling
`repeat('[ab]', 40000)` took 16 s against PostgreSQL's 0.43 s,
`((a{1,25}){1,25}){1,25}` 404 ms against 42 ms, and the backreference match
`(.*)\1$` on a 2,001-character string 26.5 s against 6.5 s. Typical patterns
are as fast as PostgreSQL or faster. The likely cause is NFA arc bookkeeping
that is linear where the C code uses linked in/out chains.

## Licences

See [NOTICE](NOTICE) for the Henry Spencer and PostgreSQL licences and
the provenance of the upstream sources and oracle tables. The remaining
crate code is licensed under MPL-2.0.
