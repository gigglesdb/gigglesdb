-- case: pgd_idx_nine
-- title: idx nine
-- owner: G
-- expect: compatible
-- source: Differences: more than eight secondary indexes
CREATE TABLE r0c_pgd_idx_nine (id int PRIMARY KEY, v int);
CREATE INDEX r0c_pgd_idx_nine_i1 ON r0c_pgd_idx_nine (v);
CREATE INDEX r0c_pgd_idx_nine_i2 ON r0c_pgd_idx_nine (v);
CREATE INDEX r0c_pgd_idx_nine_i3 ON r0c_pgd_idx_nine (v);
CREATE INDEX r0c_pgd_idx_nine_i4 ON r0c_pgd_idx_nine (v);
CREATE INDEX r0c_pgd_idx_nine_i5 ON r0c_pgd_idx_nine (v);
CREATE INDEX r0c_pgd_idx_nine_i6 ON r0c_pgd_idx_nine (v);
CREATE INDEX r0c_pgd_idx_nine_i7 ON r0c_pgd_idx_nine (v);
CREATE INDEX r0c_pgd_idx_nine_i8 ON r0c_pgd_idx_nine (v);
CREATE INDEX r0c_pgd_idx_nine_i9 ON r0c_pgd_idx_nine (v);
SELECT count(*) FROM pg_indexes WHERE tablename='r0c_pgd_idx_nine';
DROP INDEX r0c_pgd_idx_nine_i1;
DROP INDEX r0c_pgd_idx_nine_i2;
DROP INDEX r0c_pgd_idx_nine_i3;
DROP INDEX r0c_pgd_idx_nine_i4;
DROP INDEX r0c_pgd_idx_nine_i5;
DROP INDEX r0c_pgd_idx_nine_i6;
DROP INDEX r0c_pgd_idx_nine_i7;
DROP INDEX r0c_pgd_idx_nine_i8;
DROP INDEX r0c_pgd_idx_nine_i9;
