# GD-0042 reference examples

`reference_examples.json` preserves every fenced block and the two complete executable examples found in prose from these immutable Archdocs supporting-document revisions. It is an inventory of a proposed language and the current implementation's boundary, not a declaration that the proposal is implemented.

| Source | Revision | Fences | Fenced entries | Inline entries |
| --- | ---: | ---: | ---: | ---: |
| [Native tour](https://app.archdocs.io/design-docs/81f3abec-f5f0-4909-9450-ed6767cb6916/supporting/22093487-1032-431b-aefa-dd2088bb2bec?revision=1) | 1 | 10 | 13 | 0 |
| [Writes and transactions](https://app.archdocs.io/design-docs/81f3abec-f5f0-4909-9450-ed6767cb6916/supporting/9252b8cc-42d0-4d54-8764-0d4ffeb9c72b?revision=1) | 1 | 12 | 17 | 1 |
| [Schemas and migrations](https://app.archdocs.io/design-docs/81f3abec-f5f0-4909-9450-ed6767cb6916/supporting/0f861b66-2cf4-4d24-8a99-2a6b46381836?revision=2) | 2 | 9 | 32 | 0 |
| [Permissions and operations](https://app.archdocs.io/design-docs/81f3abec-f5f0-4909-9450-ed6767cb6916/supporting/a614da1c-c7e4-4668-9229-46102a9c6a14?revision=3) | 3 | 6 | 22 | 0 |
| [SQL data model](https://app.archdocs.io/design-docs/81f3abec-f5f0-4909-9450-ed6767cb6916/supporting/e5dadca6-9f0f-409b-8184-94fb2ea6960c?revision=2) | 2 | 1 | 3 | 0 |
| [PostgreSQL service comparison](https://app.archdocs.io/design-docs/81f3abec-f5f0-4909-9450-ed6767cb6916/supporting/da366690-816c-481e-93a0-f6563c096f0a?revision=2) | 2 | 6 | 6 | 1 |
| Total | | 44 | 93 | 2 |

Each block records its original language label, section, opening/closing line numbers, and exact source text including the final newline. Entries have stable IDs and byte ranges into that source. Scripts split at top-level semicolons; the tour and write examples explicitly presented as separate submissions also split at blank lines. Grammar productions remain whole, non-executable fragments. Every non-whitespace byte in a fenced block belongs to an entry. Repeated examples in different references remain separate entries.

The two inline entries separately retain their original paragraph, section, line and byte span: `outcome $key` in writes revision 1, line 233, and the complete `DELETE ... USING` statement in PostgreSQL revision 2, line 74. They run through the existing native/SQL rejection assertions. They do not increment fenced-block or fenced-entry counts. Mere syntax/type/identifier mentions, ellipsis examples and non-standalone expression fragments in prose are not counted as executable submissions.

Across all 95 entries, the default 0.1 profile classifications are 4 native parser successes, 64 native parser rejections, 1 SQL preparation success, 17 SQL preparation rejections, and 9 grammar fragments. Rejections carry explicit feature reasons and SQL failures pin their exact current error. Newer opt-in profiles have separate capability and execution tests; this corpus does not silently reclassify their syntax. Parser success alone does not claim schema, authorization, transaction, or deployment support. SQL examples enter `SqlSession`; they are never passed to the native parser or translated from native.

All four native parser successes name `deliveries`, which the current relational catalog reserves for the repair interface. Their execution expectations are explicitly rejected: creating that table returns `SqlError::Reserved`, then native binding returns `Bind("table")`. The test preserves the exact source, proves the catalog and an unrelated table's rows and versions remain unchanged, and does not rename the table to manufacture successful execution. The SQL tour query also rejects with `Reserved`. Other SQL examples can reject earlier during lexing or grammar parsing, as their exact error records show.

The accepted SQL message-read example has an explicit scalar-only schema, setup, positional bindings, and expected rows. That fixture does not make the source's rejected TIMESTAMPTZ declaration or partition directive supported. Native execution-boundary checks run with the SQL parser forbidden after fixture setup. Database artifacts are retained.

The tests are in `../reference_examples.rs`. When a reference or implementation changes, fetch the exact new revision, preserve every fenced source block, review each classification, and update the independently pinned counts and expectations. Do not remove a rejected example merely because its feature is not implemented.

`sql_interfaces.json` separately inventories 14 existing SQL caller surfaces, 32 resource limits and 39 named test references. It distinguishes trusted relational SQL, finite task-repair SQL, and clients with no SQL API. `../sql_interfaces.rs` validates those references and runs eight core SQL cases plus prepared/CAS checks. Cross-package source links are not substitutes for running the actual server, client and C ABI suites.

Run `crates/giggles-language/verify.ps1 -ArtifactDirectory <new-directory>` for core and native formatting, lint, debug/release tests and doctests. The runner records commands, exit codes, separate output streams and source hashes before and after the run. It reports concurrent source changes as an unstable verification snapshot and retains every artifact. Server/client/C ABI qualification remains a separate gate.
