COPY (
WITH high AS (
    SELECT cp, chr(cp) AS c FROM generate_series(196608, 1114111) cp
), relevant AS (
    SELECT cp, c FROM high
    WHERE c ~ '[[:alpha:]]' OR c <> lower(c)
       OR cp BETWEEN 917504 AND 918015
)
SELECT cp,
    (SELECT string_agg(alias || '=' || token, ' ') FROM ts_debug('simple', 'x'||c||'y 1'||c||' '||c)),
    to_tsvector('simple', c||'Q')::text
FROM relevant ORDER BY cp
) TO STDOUT;
