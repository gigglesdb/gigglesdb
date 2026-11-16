-- case: pgd_alter_column_options
-- title: alter column options
-- owner: G
-- expect: compatible
-- source: PG18 ALTER TABLE statistics storage compression
CREATE TABLE r0c_pgd_alter_column_options (id int PRIMARY KEY, v text);
ALTER TABLE r0c_pgd_alter_column_options ALTER COLUMN v SET STATISTICS 200;
ALTER TABLE r0c_pgd_alter_column_options ALTER COLUMN v SET STORAGE EXTERNAL;
ALTER TABLE r0c_pgd_alter_column_options ALTER COLUMN v SET COMPRESSION pglz;
ALTER TABLE r0c_pgd_alter_column_options ALTER COLUMN v SET (n_distinct=0.5);
SELECT attstattarget,attstorage,attcompression,attoptions FROM pg_attribute WHERE attrelid='r0c_pgd_alter_column_options'::regclass AND attname='v';
ALTER TABLE r0c_pgd_alter_column_options ALTER COLUMN v RESET (n_distinct);
