-- case: pgf_format_numbers
-- title: Numeric formatting patterns and parsing
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-formatting.html
CREATE TABLE r0c_pgf_format_numbers (id integer PRIMARY KEY, x numeric);
INSERT INTO r0c_pgf_format_numbers VALUES (1,1234.567),(2,-12.3),(3,NULL);
SELECT id, to_char(x,'FM999,999.00') FROM r0c_pgf_format_numbers ORDER BY id;
SELECT id, to_char(x,'S00000.000') FROM r0c_pgf_format_numbers ORDER BY id;
SELECT id, to_char(x,'99999D99') FROM r0c_pgf_format_numbers ORDER BY id;
SELECT id, to_char(x,'99999PR') FROM r0c_pgf_format_numbers ORDER BY id;
SELECT id, to_char(x,'9.99EEEE') FROM r0c_pgf_format_numbers ORDER BY id;
SELECT id, to_char(x,'FM99999TH') FROM r0c_pgf_format_numbers ORDER BY id;
SELECT id, to_number('1,234.50','9,999.99') FROM r0c_pgf_format_numbers ORDER BY id;
SELECT id, to_number('123-','999S') FROM r0c_pgf_format_numbers ORDER BY id;
SELECT id, to_number(NULL::text,'999') FROM r0c_pgf_format_numbers ORDER BY id;
DROP TABLE r0c_pgf_format_numbers;
