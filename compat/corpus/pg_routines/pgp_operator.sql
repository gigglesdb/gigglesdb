-- case: pgp_operator
-- title: User operator schema qualified application and lifecycle
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Operator symbols cannot follow identifier naming; scoped by the case schema.
CREATE SCHEMA r0c_pgp_operator;
CREATE FUNCTION r0c_pgp_operator_f(integer,integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT $1*10+$2$$;
CREATE OPERATOR r0c_pgp_operator.## (FUNCTION=r0c_pgp_operator_f,LEFTARG=integer,RIGHTARG=integer);
SELECT 2 OPERATOR(r0c_pgp_operator.##) 3;
ALTER OPERATOR r0c_pgp_operator.## (integer,integer) OWNER TO postgres;
DROP OPERATOR r0c_pgp_operator.## (integer,integer);
DROP FUNCTION r0c_pgp_operator_f(integer,integer);
DROP SCHEMA r0c_pgp_operator;
