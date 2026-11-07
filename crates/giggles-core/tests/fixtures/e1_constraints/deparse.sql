\set VERBOSITY verbose
SET search_path=public;
CREATE TABLE deparse(id int PRIMARY KEY,n int,s text,t timestamp);
ALTER TABLE deparse ADD CHECK(n IS NULL OR abs(n)>0);
ALTER TABLE deparse ADD CHECK(lower(s) <> 'x');
ALTER TABLE deparse ADD CHECK(n BETWEEN 1 AND 4);
ALTER TABLE deparse ADD CHECK(NOT (n=2));
ALTER TABLE deparse ADD CHECK(n::bigint > 0);
ALTER TABLE deparse ADD CHECK(length(s)>0);
SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='deparse'::regclass ORDER BY conname;
SELECT oid::regprocedure,provolatile FROM pg_proc WHERE oid IN ('abs(integer)'::regprocedure,'lower(text)'::regprocedure,'now()'::regprocedure,'clock_timestamp()'::regprocedure,'nextval(regclass)'::regprocedure,'timestamptz(timestamp without time zone)'::regprocedure) ORDER BY oid::regprocedure::text;
ALTER TABLE deparse ADD CONSTRAINT bad CHECK((SELECT true));
ALTER TABLE deparse ADD CONSTRAINT bad CHECK(sum(n)>0);
CREATE TABLE composite(a int,b int,PRIMARY KEY(a,b));
CREATE TABLE composite_child(a int,b int,FOREIGN KEY(a,b) REFERENCES composite(a,b) MATCH SIMPLE ON DELETE CASCADE);
INSERT INTO composite_child VALUES(NULL,99),(99,NULL),(NULL,NULL);
INSERT INTO composite_child VALUES(99,99);
