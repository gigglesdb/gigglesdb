-- case: quoted_mixed_case_table
-- title: Preserve quoted mixed case table and column names
-- owner: E1
-- expect: compatible

CREATE TABLE "r0c_quoted_mixed_case_table_UserAccount" (id integer PRIMARY KEY, "createdAt" text);
INSERT INTO "r0c_quoted_mixed_case_table_UserAccount" VALUES (1, 'fixed-date');
SELECT id, "createdAt" FROM "r0c_quoted_mixed_case_table_UserAccount" ORDER BY id;
