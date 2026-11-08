-- case: left_join_chain
-- title: Left deep chain preserves unmatched rows
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_left_join_chain_t (id int PRIMARY KEY, k int);
CREATE TABLE r0c_left_join_chain_u (id int PRIMARY KEY, k int);
INSERT INTO r0c_left_join_chain_t VALUES (1,10),(2,20),(3,NULL);
INSERT INTO r0c_left_join_chain_u VALUES (11,10),(12,10),(13,30);
SELECT t.id, u.id, v.id FROM r0c_left_join_chain_t t LEFT JOIN r0c_left_join_chain_u u ON t.k = u.k LEFT JOIN r0c_left_join_chain_u v ON u.id = v.id AND v.id = 11 ORDER BY t.id, u.id, v.id;
