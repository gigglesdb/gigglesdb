-- case: timestamptz_roundtrip
-- title: Timestamptz roundtrip
-- owner: E2
-- expect: compatible
CREATE TABLE r0c_timestamptz_roundtrip_t (value timestamptz);
INSERT INTO r0c_timestamptz_roundtrip_t VALUES ('2024-01-02 03:04:05+00');
SELECT value FROM r0c_timestamptz_roundtrip_t;
