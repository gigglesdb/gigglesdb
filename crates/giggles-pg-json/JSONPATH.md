# Standalone JSONPath qualification

This crate provides a bounded standalone evaluator, without database integration.
The grammar below is the qualified subset; it is not full PostgreSQL JSONPath.

```
path       = ["strict " | "lax "] (navigation | scalar | predicate)
navigation = ("$" [identifier] | "@") {step | descent}
step       = "." (identifier | json-string | "*")
           | "[" "*" "]"
           | "[" selector {"," selector} "]"
           | "?" "(" predicate ")"
           | ".type()" | ".size()"
selector   = bound [" to " bound]
bound      = signed-i64 | "last" [("+" | "-") unsigned-u64]
descent    = ".**" ["{" level [" to " level] "}"]
level      = unsigned-u32 | "last"
predicate  = navigation comparison navigation
           | operand comparison operand
           | "exists" "(" operand ")"
           | "(" predicate ")"
           | "!" ("(" predicate ")" | "exists" "(" operand ")")
           | "(" predicate ")" "is unknown"
           | predicate "&&" predicate | predicate "||" predicate
operand    = navigation | scalar
comparison = "==" | "!=" | "<>" | "<" | ">" | "<=" | ">="
scalar     = JSON string | JSON number | "true" | "false" | "null"
identifier = ASCII letter/underscore followed by ASCII letters/digits/underscore
```

Logical precedence is `||`, then `&&`, then comparison/unary forms. Whitespace is
accepted around steps, comparisons, logical operators and filter delimiters.
Mode prefixes require the displayed space. Range separators require literal
` to ` (additional surrounding whitespace is accepted). Scalar operands do not
have accessor continuations. Current `@` is qualified inside filters; `$` always
refers to the original target, and `$identifier` to the variables object.
Recursive descent may be followed by accessors, filters and further descent. Its depth-first
preorder includes the starting value at level zero. `last` as both bounds selects
scalar descendant leaves, excluding the starting scalar and empty containers.
Descent continuations suppress structural errors throughout their navigation context,
independently of mode: strict mode still does not unwrap or wrap arrays. Range
accessors clip their bounds in that context. Variable and resource errors propagate.
General index arithmetic, parenthesized scalar or predicate operands, other
methods, arithmetic, regex and datetime remain unqualified.
Valid unsupported features return Unsupported at parse time, including in dead
logical branches; lax and silent cannot suppress them. Syntax errors in nesting
and filter delimiters return Syntax; malformed unsupported syntax is not fully
classified. JSON quoted keys/scalars use the Unicode-aware existing decoder.

Predicates produce a singleton JSON boolean or JSON null (unknown). Comparisons
have sequence any-pair semantics. Lax mode unwraps one array level on both sides;
strict mode returns unknown on any incomparable pair even after a matching pair.
Two nulls compare equal; null versus nonnull is false for every operator except
inequality. Other mismatched types and containers are incomparable. Numeric
comparison uses exact Numeric, strings use UTF-8 byte order, and booleans order
false before true. Logical operators implement PostgreSQL JSONPath three-valued
short circuit rules. Structural failures inside comparisons/exists become unknown;
resource limits, unsupported features and missing variables never do. Filters
retain only true candidates, unwrapping the filter input once in lax mode.

The five existing Option-based jsonb_path APIs remain unchanged. Any SQL NULL
argument returns SQL NULL; JSON null remains a value. Query returns sequences;
match requires exactly one boolean or JSON null. Empty, multiple or nonboolean
match results raise Predicate (22038), or SQL NULL with silent. Silent query
retains results before structural failure; silent exists returns SQL NULL on
structural failure. Variables must be objects, with canonical last-duplicate wins.
Structural/type/subscript errors retain their distinct SQLSTATEs.

PathLimits bound input bytes, per-navigation steps (hard128), sequence width,
visited work and output bytes. Parser delimiter and predicate recursion is capped
at64; evaluator combined navigation/predicate recursion at128. Every candidate,
comparison pair, traversal and output subtree visit is charged before expansion
or cloning. JSON inputs are immutable, acyclic owned trees with existing node and
hard128 depth limits. Output subtree admission and cumulative rendering precede
query result clones; query-array renders before bounded parsing. Numeric/string
comparison cost is additionally bounded by input size, rather than charging every
byte. No allocator failure recovery claim. Query-first conservatively admits all
results. Resource errors are never silent and return no partial API result.

Durable tests/fixtures/predicates.{sql,tsv} contain 65,280 independent actual pinned
PostgreSQL18 rows. The Rust tests execute every row against this evaluator, along
with the preserved18,000 navigation and41,096 foundation/operator/mutation rows.
The manifest records image digest, server/C database collation and SHA256 of the
generator, fixtures and pinned REL18 source. Additional generated properties,
malformed syntax, unsupported dead branches, cross-product/sequence work limits
and immutable failure cases run offline.

Run `python tests/verify_predicates.py [--live]` from any directory. It records
native exit codes, full logs and before/after source/fixture hashes beneath
`target-dd68-jsonpath-predicates`. Live mode expects an owned pinned container
`dd68-jsonpath-predicates-sol-20261106` with network none, read-only filesystem,
only tmpfs writable mounts and a C-collation operator_oracle database (README
container procedure). It neither creates nor cleans up containers.

Remaining scope includes general index expressions,
other methods/arithmetic/regex/datetime, JSONPath codecs and database integration. This
qualification does not establish full JSONPath or DD68 completion.

New continuations.sql/tsv qualify 37,600 actual pinned PostgreSQL18 observations
across all five APIs, both modes/silent settings, bounds/last, nested descent,
accessors, filters, variables and three-valued predicates. Exact ordered JSON text
and SQLSTATE comparisons retain duplicates. Lax exists and predicate exists stop
after the first result; strict exists evaluates all candidates for errors.
Resource admission remains conservative, including descent candidate collection
and query-first full-result admission. See continuations-manifest.json for scoped
verification commands, source/fixture guards and limitations.

Exact `.type()` and `.size()` calls are qualified; quoted `"type"` and `"size"`
remain object keys. Both inspect arrays directly without lax unwrapping. Type
returns `null`, `boolean`, `number`, `string`, `array` or `object`. Size returns
array length, or one for any nonarray in lax mode. Strict nonarray size raises
Array (22039), or produces no item inside a descent structural suppression context.
Calls with arguments, other methods and scalar-literal continuations remain
unqualified. Methods may chain and appear in filters and comparison operands.

Private navigation results borrow input subtrees; only method scalar results are
owned. Work, sequence and intermediate scalar byte admission precede their
allocation/copy; final cumulative render/subtree admission still precedes public
copies. Intermediate method scalar admission is conservative against output_bytes,
even if a later method/filter could produce a smaller final output. No input
subtree is cloned during traversal. New methods.sql/tsv retain 20,400 actual pinned
PG18 observations, exact ordered text/SQLSTATE. methods-manifest.json records
qualification, nonempty explicit identities, original fixture guards and limits.

Selector lists preserve written order and duplicates; each selector streams through
its continuation before the next selector runs. Signed integer indexes and ranges
accept signed-i64 literals. `last` accepts one plus/minus unsigned-u64 offset, with
whitespace around that operator. Wider literals/offsets, general expressions,
variables, function calls and parenthesized index expressions stay Unsupported.
Every evaluated bound must fit PostgreSQL int32, raising Subscript (22033) even
in lax/descent context before clipping. In-range bounds otherwise use the existing
lax wrapping/clipping and strict structural errors. A later selector error retains
prior query results with silent; lax exists stops at its first result.

Each selector list is capped before parser pushes by min(steps,128), sequence and
work; execution rechecks those caps against its own limits and charges each
selector and candidate before expansion/copy. This is conservative even for
selectors whose bounds would produce no results. No target subtree cloning is
introduced. Checked i128 arithmetic prevents `last` offset overflow before int32
conversion; traversal/output/recursion admission remains unchanged.

PostgreSQL descent has an additional no-throw status rule: for a container root,
the recursive child traversal replaces the root continuation's returned subscript
error status. Unsilenced API errors still throw; child errors stop traversal.
The evaluator models this specific status transition, including predicate operand
no-throw contexts. Variable and resource errors always propagate and context flags
restore on every return. selectors.sql/tsv plus selector-contexts.sql/tsv retain
31,600 new actual PG18 observations; selectors-manifest.json records exact
qualification and explicit source/fixture/generator identities. Full JSONPath,
engine integration and DD68 remain unfinished.
