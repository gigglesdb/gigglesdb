-- case: pgq_queue_claim
-- title: queue claim
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 queue claiming; data-modifying WITH; row locks
CREATE TABLE r0c_pgq_queue_claim (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_queue_claim VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
BEGIN;
WITH next_job AS (SELECT id FROM r0c_pgq_queue_claim WHERE label='root' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1), claimed AS (UPDATE r0c_pgq_queue_claim SET label='running' FROM next_job WHERE r0c_pgq_queue_claim.id=next_job.id RETURNING r0c_pgq_queue_claim.id,r0c_pgq_queue_claim.label) SELECT * FROM claimed ORDER BY id;
COMMIT;
