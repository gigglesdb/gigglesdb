-- case: pgq_merge_default_values
-- title: merge default values
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 MERGE INSERT DEFAULT VALUES
CREATE TABLE r0c_pgq_merge_default_values (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label text DEFAULT 'draft');
MERGE INTO r0c_pgq_merge_default_values t USING (VALUES (1)) s(id) ON t.id=s.id WHEN NOT MATCHED THEN INSERT DEFAULT VALUES RETURNING t.id,t.label;
