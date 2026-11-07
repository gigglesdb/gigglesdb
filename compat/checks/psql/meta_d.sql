-- id: meta_d
-- title: Describe objects in a dedicated schema
-- owner: G
CREATE SCHEMA r0_meta_d_s;
CREATE TABLE r0_meta_d_s.t (id integer PRIMARY KEY);
\d r0_meta_d_s.*
