CREATE TABLE customers(id int PRIMARY KEY, email text UNIQUE, country text, phone text, failed_payments int);
INSERT INTO customers VALUES
 (1,'alice@example.test','US','555-0101',0),(2,'bob@example.test','GB','555-0102',1),
 (3,'carol@example.test','US','555-0103',2),(4,'dave@example.test','CA','555-0104',0),
 (5,'eve@example.test','GB','555-0105',3);
CREATE TABLE outbox(id serial PRIMARY KEY, to_address text, template text);
CREATE TABLE notes(id int PRIMARY KEY, body text);
CREATE VIEW v_emails AS SELECT id,email FROM customers;
CREATE ROLE nunya LOGIN PASSWORD 'nunya-harness-only';
GRANT CONNECT ON DATABASE app TO nunya;
GRANT USAGE ON SCHEMA public TO nunya;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO nunya;
GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO nunya;
