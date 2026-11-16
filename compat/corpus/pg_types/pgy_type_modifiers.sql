-- case: pgy_type_modifiers
-- title: type modifiers
-- owner: E2
-- expect: compatible
-- source: W5 CREATE/ALTER/DROP TYPE; types.txt
CREATE TABLE r0c_pgy_type_modifiers (id int PRIMARY KEY,a varchar(12),b char(5),c numeric(10,3),d timestamp(3),e time(2),f interval second(3),g bit(7));
SELECT a.attname,format_type(a.atttypid,a.atttypmod) FROM pg_attribute a WHERE a.attrelid='r0c_pgy_type_modifiers'::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum;
SELECT column_name,data_type,character_maximum_length,numeric_precision,numeric_scale,datetime_precision FROM information_schema.columns WHERE table_name='r0c_pgy_type_modifiers' ORDER BY ordinal_position;
