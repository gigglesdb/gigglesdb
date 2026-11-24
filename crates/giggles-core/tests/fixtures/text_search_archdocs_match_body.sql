ALTER TABLE "SearchDocument" ADD COLUMN "matchOnlyBody" TEXT NOT NULL DEFAULT '';

DROP INDEX "SearchDocument_tsv_idx";
ALTER TABLE "SearchDocument" DROP COLUMN "tsv";
ALTER TABLE "SearchDocument" ADD COLUMN "tsv" tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce("title", '')), 'A') ||
    setweight(to_tsvector('english', coalesce("body", '')), 'B') ||
    setweight(to_tsvector('english', coalesce("matchOnlyBody", '')), 'C')
) STORED;
CREATE INDEX "SearchDocument_tsv_idx" ON "SearchDocument" USING GIN ("tsv");
