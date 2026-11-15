-- A bijection on each set of non-NULL values preserves equality classes,
-- including the shared email domain across customers and outbox. NULL stays NULL.
-- Rotate distinct replacements in reverse lexical order, with unequal lengths.
BEGIN;
CREATE TEMP TABLE ni_email_map AS
SELECT value, 'ni-' || repeat('z', (7 * rank)::int) || '-' || (100000 - rank)::text || '@b.invalid' AS replacement
FROM (
  SELECT value, row_number() OVER (ORDER BY value DESC) AS rank
  FROM (SELECT email AS value FROM customers WHERE email IS NOT NULL
        UNION SELECT to_address FROM outbox WHERE to_address IS NOT NULL) valueset
) ranked;
CREATE TEMP TABLE ni_phone_map AS
SELECT value, 'b-phone-' || repeat('x', (11 * rank)::int) || '-' || (100000 - rank)::text AS replacement
FROM (
  SELECT value, row_number() OVER (ORDER BY value DESC) AS rank
  FROM (SELECT DISTINCT phone AS value FROM customers WHERE phone IS NOT NULL) valueset
) ranked;
UPDATE customers c SET email = m.replacement FROM ni_email_map m WHERE c.email = m.value;
UPDATE outbox o SET to_address = m.replacement FROM ni_email_map m WHERE o.to_address = m.value;
UPDATE customers c SET phone = m.replacement FROM ni_phone_map m WHERE c.phone = m.value;
DROP TABLE ni_email_map, ni_phone_map;
COMMIT;
