-- case: pgy_conversion_lifecycle
-- title: conversion lifecycle
-- owner: E2
-- expect: compatible
-- source: W5 CREATE/ALTER/DROP CONVERSION
CREATE CONVERSION r0c_pgy_conversion_lifecycle_c FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1;
SELECT convert('hello'::bytea,'UTF8','LATIN1') = 'hello'::bytea;
SELECT conname,condefault FROM pg_conversion WHERE conname='r0c_pgy_conversion_lifecycle_c' ORDER BY conname;
ALTER CONVERSION r0c_pgy_conversion_lifecycle_c OWNER TO postgres;
ALTER CONVERSION r0c_pgy_conversion_lifecycle_c RENAME TO r0c_pgy_conversion_lifecycle_new;
CREATE SCHEMA r0c_pgy_conversion_lifecycle_s;
ALTER CONVERSION r0c_pgy_conversion_lifecycle_new SET SCHEMA r0c_pgy_conversion_lifecycle_s;
SELECT c.conname,n.nspname FROM pg_conversion c JOIN pg_namespace n ON n.oid=c.connamespace WHERE c.conname='r0c_pgy_conversion_lifecycle_new' ORDER BY c.conname;
DROP CONVERSION r0c_pgy_conversion_lifecycle_s.r0c_pgy_conversion_lifecycle_new;
DROP SCHEMA r0c_pgy_conversion_lifecycle_s;
