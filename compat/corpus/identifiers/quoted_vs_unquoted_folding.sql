-- case: quoted_vs_unquoted_folding
-- title: Distinguish quoted names from unquoted folded names
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_quoted_vs_unquoted_folding_Foo (id integer PRIMARY KEY);
INSERT INTO r0c_quoted_vs_unquoted_folding_foo VALUES (1);
SELECT id FROM r0c_quoted_vs_unquoted_folding_foo ORDER BY id;
CREATE TABLE "r0c_quoted_vs_unquoted_folding_Bar" (id integer PRIMARY KEY);
INSERT INTO "r0c_quoted_vs_unquoted_folding_Bar" VALUES (2);
SELECT id FROM "r0c_quoted_vs_unquoted_folding_Bar" ORDER BY id;
-- @error
-- expect-sqlstate: 42P01
SELECT id FROM r0c_quoted_vs_unquoted_folding_bar ORDER BY id;
