-- case: pgt_prepared
-- title: SQL prepared statements and deallocation
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / SQL prepared statements and deallocation
CREATE TABLE r0c_pgt_prepared (id int PRIMARY KEY, label text);
PREPARE r0c_pgt_prepared_insert (int, text) AS INSERT INTO r0c_pgt_prepared VALUES ($1,$2) RETURNING id;
EXECUTE r0c_pgt_prepared_insert(1,'first');
PREPARE r0c_pgt_prepared_select (int) AS SELECT label FROM r0c_pgt_prepared WHERE id = $1;
EXECUTE r0c_pgt_prepared_select(1);
DEALLOCATE PREPARE r0c_pgt_prepared_insert;
DEALLOCATE ALL;
-- @error
EXECUTE r0c_pgt_prepared_select(1);
