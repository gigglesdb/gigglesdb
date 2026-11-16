-- case: pgf_sequence
-- title: Sequence session state reset and last value
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-sequence.html
CREATE SEQUENCE r0c_pgf_sequence START 10;
SELECT nextval('r0c_pgf_sequence');
SELECT currval('r0c_pgf_sequence');
SELECT lastval();
SELECT setval('r0c_pgf_sequence',20,false);
SELECT nextval('r0c_pgf_sequence');
SELECT setval('r0c_pgf_sequence',30,true);
SELECT nextval('r0c_pgf_sequence');
SELECT currval('r0c_pgf_sequence');
SELECT lastval();
SELECT nextval(NULL::regclass);
SELECT currval(NULL::regclass);
SELECT setval(NULL::regclass,2);
DROP SEQUENCE r0c_pgf_sequence;
-- @error
SELECT nextval('r0c_pgf_sequence_missing');
