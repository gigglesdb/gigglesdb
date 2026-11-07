-- case: identity_always
-- title: Reject explicit always identity values without overriding
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_identity_always_t (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text);
INSERT INTO r0c_identity_always_t (name) VALUES ('generated');
-- @error
-- expect-sqlstate: 428C9
INSERT INTO r0c_identity_always_t (id, name) VALUES (10, 'rejected');
INSERT INTO r0c_identity_always_t (id, name) OVERRIDING SYSTEM VALUE VALUES (10, 'overridden');
SELECT id, name FROM r0c_identity_always_t ORDER BY id;
