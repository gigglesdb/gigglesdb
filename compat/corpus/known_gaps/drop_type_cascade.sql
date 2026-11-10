-- case: drop_type_cascade
-- title: DROP TYPE CASCADE
-- owner: E2
-- expect: compatible
CREATE TYPE r0c_drop_type_cascade AS ENUM ('a');
DROP TYPE r0c_drop_type_cascade CASCADE;
