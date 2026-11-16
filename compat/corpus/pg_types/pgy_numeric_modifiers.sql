-- case: pgy_numeric_modifiers
-- title: numeric modifiers
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_numeric_modifiers (id int PRIMARY KEY,v numeric(4,2),coarse numeric(4,-2),tiny numeric(3,5));
INSERT INTO r0c_pgy_numeric_modifiers VALUES (1,1.005,1250,0.001234),(2,-1.005,-1250,-0.001234);
SELECT id,v,coarse,tiny FROM r0c_pgy_numeric_modifiers ORDER BY id;
-- @error
INSERT INTO r0c_pgy_numeric_modifiers VALUES (3,999.99,1,0);
-- @error
INSERT INTO r0c_pgy_numeric_modifiers VALUES (3,'Infinity',1,0);
SELECT 1::numeric / 3;
