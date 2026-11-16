-- case: pgy_bytea_escape
-- title: bytea escape
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: bytea_output escape; types.txt / bytea
SET bytea_output='escape';
CREATE TABLE r0c_pgy_bytea_escape (id int PRIMARY KEY,v bytea);
INSERT INTO r0c_pgy_bytea_escape VALUES (1,'\x00ff5c41'),(2,E'\\001\\\\abc'),(3,'plain');
SELECT id,v,encode(v,'hex'),octet_length(v) FROM r0c_pgy_bytea_escape ORDER BY id;
RESET bytea_output;
