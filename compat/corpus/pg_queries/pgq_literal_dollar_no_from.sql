-- case: pgq_literal_dollar_no_from
-- title: literal dollar no from
-- owner: E3
-- expect: compatible
-- source: Differences: literal dollar no from; PostgreSQL 18 SELECT
SELECT $$it's a label$$;
