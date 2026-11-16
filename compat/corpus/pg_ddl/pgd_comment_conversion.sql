-- case: pgd_comment_conversion
-- title: comment conversion
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE CONVERSION r0c_pgd_comment_conversion FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1;
COMMENT ON CONVERSION r0c_pgd_comment_conversion IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_conversion WHERE conname='r0c_pgd_comment_conversion'),'pg_conversion');
COMMENT ON CONVERSION r0c_pgd_comment_conversion IS NULL;
SELECT obj_description((SELECT oid FROM pg_conversion WHERE conname='r0c_pgd_comment_conversion'),'pg_conversion') IS NULL AS cleared;
DROP CONVERSION r0c_pgd_comment_conversion;
