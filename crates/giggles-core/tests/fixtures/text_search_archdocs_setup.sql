-- CreateTable
CREATE TABLE "SearchDocument" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "entityType" TEXT NOT NULL,
    "entityId" UUID NOT NULL,
    "organizationId" UUID NOT NULL,
    "architectureDocId" UUID,
    "spaceId" UUID,
    "title" TEXT NOT NULL DEFAULT '',
    "body" TEXT NOT NULL DEFAULT '',
    "tsv" tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce("title", '')), 'A') ||
        setweight(to_tsvector('english', coalesce("body", '')), 'B')
    ) STORED,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "SearchDocument_pkey" PRIMARY KEY ("id")
);

-- CreateIndex (idempotent upsert target)
CREATE UNIQUE INDEX "SearchDocument_entityType_entityId_key" ON "SearchDocument"("entityType", "entityId");

-- CreateIndex (org scoping)
CREATE INDEX "SearchDocument_organizationId_idx" ON "SearchDocument"("organizationId");

-- CreateIndex (deep-link / cascade lookups)
CREATE INDEX "SearchDocument_architectureDocId_idx" ON "SearchDocument"("architectureDocId");

-- CreateIndex (the FTS workhorse)
CREATE INDEX "SearchDocument_tsv_idx" ON "SearchDocument" USING GIN ("tsv");

-- AddForeignKey
ALTER TABLE "SearchDocument" ADD CONSTRAINT "SearchDocument_organizationId_fkey" FOREIGN KEY ("organizationId") REFERENCES "Organization"("id") ON DELETE CASCADE ON UPDATE CASCADE;
