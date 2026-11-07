-- case: select_literals
-- title: Select integer, text, and typed NULL literals
-- owner: E3
-- expect: compatible
SELECT 1 AS a, 'x' AS b;
SELECT NULL::int AS n;
