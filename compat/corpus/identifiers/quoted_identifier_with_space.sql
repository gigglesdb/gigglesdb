-- case: quoted_identifier_with_space
-- title: Use spaces in quoted table and column names
-- owner: E1
-- expect: compatible

CREATE TABLE "r0c_quoted_identifier_with_space_t space" ("row id" integer PRIMARY KEY, "display name" text);
INSERT INTO "r0c_quoted_identifier_with_space_t space" VALUES (1, 'Ada');
SELECT "row id", "display name" FROM "r0c_quoted_identifier_with_space_t space" ORDER BY "row id";
