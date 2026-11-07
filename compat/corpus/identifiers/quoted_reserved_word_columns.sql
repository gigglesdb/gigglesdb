-- case: quoted_reserved_word_columns
-- title: Use quoted reserved words as column names
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_quoted_reserved_word_columns_t ("order" integer, "user" text, "select" text);
INSERT INTO r0c_quoted_reserved_word_columns_t VALUES (1, 'Ada', 'chosen');
SELECT "order", "user", "select" FROM r0c_quoted_reserved_word_columns_t ORDER BY "order";
