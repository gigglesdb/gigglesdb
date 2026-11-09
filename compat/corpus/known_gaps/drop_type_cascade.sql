-- case: drop_type_cascade
-- title: DROP TYPE CASCADE
-- owner: E2
-- expect: intentional DROP TYPE CASCADE
CREATE TYPE r0c_drop_type_cascade AS ENUM ('a');
-- expect-sqlstate: 0A000
DROP TYPE r0c_drop_type_cascade CASCADE;
