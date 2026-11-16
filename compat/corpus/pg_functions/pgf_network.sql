-- case: pgf_network
-- title: Network containment arithmetic and address functions
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-net.html
CREATE TABLE r0c_pgf_network (id integer PRIMARY KEY, a inet);
INSERT INTO r0c_pgf_network VALUES (1,'192.168.1.5/24'),(2,'2001:db8::1/64'),(3,NULL);
SELECT id, host(a) FROM r0c_pgf_network ORDER BY id;
SELECT id, masklen(a) FROM r0c_pgf_network ORDER BY id;
SELECT id, network(a) FROM r0c_pgf_network ORDER BY id;
SELECT id, broadcast(a) FROM r0c_pgf_network ORDER BY id;
SELECT id, netmask(a) FROM r0c_pgf_network ORDER BY id;
SELECT id, family(a) FROM r0c_pgf_network ORDER BY id;
SELECT id, a <<= inet '192.168.0.0/16' FROM r0c_pgf_network ORDER BY id;
SELECT id, a && inet '192.168.1.0/24' FROM r0c_pgf_network ORDER BY id;
SELECT id, a+1 FROM r0c_pgf_network ORDER BY id;
SELECT id, set_masklen(a,16) FROM r0c_pgf_network ORDER BY id;
SELECT id, inet '192.168.1.5'-inet '192.168.1.1' FROM r0c_pgf_network ORDER BY id;
SELECT id, macaddr '08:00:2b:01:02:03' FROM r0c_pgf_network ORDER BY id;
DROP TABLE r0c_pgf_network;
