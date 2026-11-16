-- case: pgs_uuid_max
-- title: UUID max aggregate is undefined
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / UUID max aggregate is undefined
CREATE TABLE r0c_pgs_uuid_max (id int PRIMARY KEY, v uuid);
INSERT INTO r0c_pgs_uuid_max VALUES (1,'123e4567-e89b-12d3-a456-426614174000');
-- @error
SELECT max(v) FROM r0c_pgs_uuid_max;
