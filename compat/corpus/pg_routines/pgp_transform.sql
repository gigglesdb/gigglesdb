-- case: pgp_transform
-- title: Transform signature validation and missing transform removal
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- SQL transforms require an internal-argument conversion function.
-- @error
CREATE TRANSFORM FOR integer LANGUAGE plpgsql (FROM SQL WITH FUNCTION int4send(integer));
CREATE TRANSFORM FOR integer LANGUAGE plpgsql (TO SQL WITH FUNCTION int4recv(internal));
SELECT t.typname,l.lanname FROM pg_transform tr JOIN pg_type t ON t.oid=tr.trftype JOIN pg_language l ON l.oid=tr.trflang WHERE t.typname='int4' AND l.lanname='plpgsql';
DROP TRANSFORM FOR integer LANGUAGE plpgsql;
