-- case: pgp_access_method
-- title: Custom heap table access method lifecycle
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE ACCESS METHOD r0c_pgp_access_method TYPE TABLE HANDLER heap_tableam_handler;
CREATE TABLE r0c_pgp_access_method_t(id integer PRIMARY KEY) USING r0c_pgp_access_method;
INSERT INTO r0c_pgp_access_method_t VALUES(1);
SELECT id FROM r0c_pgp_access_method_t;
DROP TABLE r0c_pgp_access_method_t;
DROP ACCESS METHOD r0c_pgp_access_method;
