-- case: pgy_conversion_default
-- title: conversion default
-- owner: E2
-- expect: compatible
-- source: W5 CREATE/ALTER/DROP CONVERSION
CREATE SCHEMA r0c_pgy_conversion_default_s;
CREATE DEFAULT CONVERSION r0c_pgy_conversion_default_s.r0c_pgy_conversion_default_c FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1;
SELECT conname,condefault FROM pg_conversion WHERE conname='r0c_pgy_conversion_default_c' ORDER BY conname;
DROP CONVERSION r0c_pgy_conversion_default_s.r0c_pgy_conversion_default_c;
DROP SCHEMA r0c_pgy_conversion_default_s;
