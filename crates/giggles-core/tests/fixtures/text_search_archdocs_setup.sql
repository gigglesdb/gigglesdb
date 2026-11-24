CREATE TABLE "SearchDocument" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "entityType" TEXT NOT NULL,
    "entityId" UUID NOT NULL,
    "organizationId" UUID NOT NULL,
    "architectureDocId" UUID,
    "spaceId" UUID,
    "title" TEXT NOT NULL DEFAULT '',
    "body" TEXT NOT NULL DEFAULT '',
    "tsv" tsvector,
    "matchOnlyBody" text NOT NULL DEFAULT '',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    CONSTRAINT "SearchDocument_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "SearchDocument_entityType_entityId_key" ON "SearchDocument"("entityType", "entityId");
CREATE INDEX "SearchDocument_organizationId_idx" ON "SearchDocument"("organizationId");
INSERT INTO "SearchDocument" ("id","entityType","entityId","organizationId","title","body","createdAt","updatedAt") VALUES
 ('00000000-0000-0000-0000-000000000101','doc','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-0000000000aa','Payment service architecture','The payment service talks to Stripe through the billing gateway. Retries are idempotent.', '2026-01-01 00:00:00', '2026-01-01 00:00:00'),
 ('00000000-0000-0000-0000-000000000102','design-doc','00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-0000000000aa','Search indexing design','We index documents into a tsvector column and rank results with ts_rank.', '2026-01-01 00:00:00', '2026-01-01 00:00:00'),
 ('00000000-0000-0000-0000-000000000103','object','00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-0000000000aa','Billing gateway','Gateway for payments. Owns invoices.', '2026-01-01 00:00:00', '2026-01-01 00:00:00');
UPDATE "SearchDocument" SET "body" = 'Payments are retried by the gateway' WHERE "entityId" = '00000000-0000-0000-0000-000000000003';
UPDATE "SearchDocument" SET "matchOnlyBody" = 'journal entry about refunds' WHERE "entityType" = 'doc';
UPDATE "SearchDocument" SET "tsv" =
    setweight(to_tsvector('english', coalesce("title", '')), 'A') ||
    setweight(to_tsvector('english', coalesce("body", '')), 'B') ||
    setweight(to_tsvector('english', coalesce("matchOnlyBody", '')), 'C');
