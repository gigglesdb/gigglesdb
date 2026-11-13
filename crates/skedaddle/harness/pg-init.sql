-- Disposable fixture credentials, never production credentials. Operational
-- roles are distinct from the writable test-setup owner and explicit admins.
CREATE ROLE skedaddle_fixture LOGIN REPLICATION PASSWORD 'fixture-test';
CREATE ROLE skedaddle_setup LOGIN PASSWORD 'setup-test';
CREATE ROLE skedaddle_capture LOGIN REPLICATION PASSWORD 'capture-test';
CREATE ROLE skedaddle_fence LOGIN CREATEROLE PASSWORD 'fence-test';
CREATE ROLE skedaddle_apply LOGIN PASSWORD 'apply-test';
CREATE ROLE skedaddle_verify LOGIN PASSWORD 'verify-test';
-- Preflight measures allocated WAL; REPLICATION alone does not allow this.
GRANT EXECUTE ON FUNCTION pg_catalog.pg_ls_waldir() TO skedaddle_fixture, skedaddle_setup;
-- rollback refuses pre-existing _skedaddle state; never reuse the matrix DB.
CREATE DATABASE skedaddle_rollback;
REVOKE ALL ON DATABASE skedaddle_rollback FROM PUBLIC;
REVOKE CREATE, TEMPORARY ON DATABASE skedaddle FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE skedaddle TO skedaddle_fixture, skedaddle_setup, skedaddle_capture,
    skedaddle_fence, skedaddle_apply, skedaddle_verify;
GRANT CREATE ON DATABASE skedaddle TO skedaddle_fixture, skedaddle_setup, skedaddle_capture;
GRANT USAGE, CREATE ON SCHEMA public TO skedaddle_fixture, skedaddle_setup, skedaddle_capture;
GRANT pg_signal_backend TO skedaddle_fence;
GRANT SET ON PARAMETER session_replication_role TO skedaddle_apply;
ALTER ROLE skedaddle_verify SET default_transaction_read_only = on;
-- Scoped sandbox objects exercise grants without granting pg_read_all_data or
-- privileges over unrelated objects. Future test schemas belong to fixture.
CREATE SCHEMA harness_scope AUTHORIZATION skedaddle_fixture;
GRANT USAGE ON SCHEMA harness_scope TO skedaddle_capture, skedaddle_fence,
    skedaddle_apply, skedaddle_verify;
SET ROLE skedaddle_fixture;
CREATE TABLE harness_scope.items(id bigint PRIMARY KEY, value text);
GRANT SELECT ON harness_scope.items TO skedaddle_capture, skedaddle_verify;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON harness_scope.items TO skedaddle_apply;
GRANT ALL ON harness_scope.items TO skedaddle_fence WITH GRANT OPTION;
CREATE PUBLICATION harness_capture FOR TABLE harness_scope.items;
RESET ROLE;
ALTER PUBLICATION harness_capture OWNER TO skedaddle_capture;
CREATE ROLE harness_app NOLOGIN;
GRANT harness_app TO skedaddle_fence WITH ADMIN OPTION;
ALTER DEFAULT PRIVILEGES FOR ROLE skedaddle_fixture
    GRANT SELECT ON TABLES TO skedaddle_capture, skedaddle_verify;
ALTER DEFAULT PRIVILEGES FOR ROLE skedaddle_fixture
    GRANT USAGE ON SCHEMAS TO skedaddle_capture, skedaddle_verify;
ALTER DEFAULT PRIVILEGES FOR ROLE skedaddle_setup
    GRANT SELECT ON TABLES TO skedaddle_verify;
ALTER DEFAULT PRIVILEGES FOR ROLE skedaddle_setup
    GRANT USAGE ON SCHEMAS TO skedaddle_verify;
