WITH q AS (SELECT websearch_to_tsquery('english', $2) AS query)
SELECT sd."entityType", sd."title",
  NOT ((setweight(to_tsvector('english', coalesce(sd."title", '')), 'A') || setweight(to_tsvector('english', coalesce(sd."body", '')), 'B')) @@ q.query) AS "matchedInJournal",
  ts_rank(sd."tsv", q.query) AS "rank",
  ts_headline('english', coalesce(sd."title",'') || ' — ' || coalesce(sd."body",''), q.query, 'StartSel=<mark>, StopSel=</mark>, MaxFragments=1, MaxWords=24, MinWords=8, ShortWord=2') AS snippet
FROM "SearchDocument" sd, q
WHERE sd."organizationId" = $1 AND sd."tsv" @@ q.query AND sd."entityType" = ANY($4)
ORDER BY "rank" DESC, sd."title" ASC LIMIT $3
