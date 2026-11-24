CREATE TYPE "LinearOutboxStatus" AS ENUM ('PENDING', 'DONE', 'FAILED');
CREATE TABLE "LinearOutbox" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "installationId" UUID NOT NULL,
  "kind" TEXT NOT NULL,
  "payload" JSONB NOT NULL,
  "status" "LinearOutboxStatus" NOT NULL DEFAULT 'PENDING',
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "nextAttemptAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "lockedUntil" TIMESTAMP(3),
  "lastError" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "LinearOutbox_pkey" PRIMARY KEY ("id")
);
INSERT INTO "LinearOutbox" ("id","installationId","kind","payload","status","lockedUntil","createdAt","updatedAt") VALUES
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','LINK_ISSUE','{"issueRef":"ENG-1","source":"ui","target":{"kind":"doc","docId":"d1","elementId":"e1"}}','PENDING','2026-01-01 00:00:00','2026-01-01 00:00:01','2026-01-01'),
('00000000-0000-0000-0000-000000000002','10000000-0000-0000-0000-000000000001','LINK_ISSUE','{"issueRef":"ENG-1","source":"ui","target":{"kind":"doc","docId":"d1","elementId":"e1"}}','PENDING','2026-01-01 00:00:00','2026-01-01 00:00:02','2026-01-01'),
('00000000-0000-0000-0000-000000000003','10000000-0000-0000-0000-000000000001','LINK_ISSUE','{"issueRef":"ENG-1","source":"ui","target":{"kind":"doc","docId":"d1"}}','PENDING',NULL,'2026-01-01 00:00:03','2026-01-01'),
('00000000-0000-0000-0000-000000000004','10000000-0000-0000-0000-000000000001','LINK_ISSUE','{"issueRef":"ENG-2","source":"api","target":{"kind":"object","objectId":"o1","elementId":"x"}}','PENDING',NULL,'2026-01-01 00:00:04','2026-01-01'),
('00000000-0000-0000-0000-000000000005','10000000-0000-0000-0000-000000000001','LINK_ISSUE','{"issueRef":"ENG-2","source":"api","target":{"kind":"object","objectId":"o1"}}','PENDING',NULL,'2026-01-01 00:00:04','2026-01-01'),
('00000000-0000-0000-0000-000000000006','10000000-0000-0000-0000-000000000002','LINK_ISSUE','{"issueRef":"ENG-2","source":"api","target":{"kind":"object","objectId":"o1"}}','PENDING',NULL,'2026-01-01 00:00:04','2026-01-01'),
('00000000-0000-0000-0000-000000000007','10000000-0000-0000-0000-000000000001','LINK_ISSUE','{"issueRef":"ENG-3","source":"ui","target":{"kind":"doc","docId":"d2"}}','DONE',NULL,'2026-01-01 00:00:05','2026-01-01'),
('00000000-0000-0000-0000-000000000008','10000000-0000-0000-0000-000000000001','LINK_ISSUE','{"issueRef":"ENG-3","source":"ui","target":{"kind":"doc","docId":"d2"}}','PENDING',NULL,'2026-01-01 00:00:06','2026-01-01'),
('00000000-0000-0000-0000-000000000009','10000000-0000-0000-0000-000000000001','UNLINK','{"issueRef":"ENG-1"}','PENDING',NULL,'2026-01-01 00:00:07','2026-01-01'),
('00000000-0000-0000-0000-00000000000a','10000000-0000-0000-0000-000000000001','LINK_ISSUE','{"issueRef":"ENG-é \"q\"","source":null,"target":{"kind":"doc","docId":"d3","elementId":null}}','PENDING',NULL,'2026-01-01 00:00:08','2026-01-01'),
('00000000-0000-0000-0000-00000000000b','10000000-0000-0000-0000-000000000001','LINK_ISSUE','{"other":1}','PENDING',NULL,'2026-01-01 00:00:09','2026-01-01');
ALTER TABLE "LinearOutbox" ADD COLUMN "dedupeKey" TEXT;
UPDATE "LinearOutbox"
SET "dedupeKey" = encode(sha256(convert_to(jsonb_build_array(
  "payload"->>'issueRef', "payload"->>'source', "payload"#>>'{target,kind}',
  CASE WHEN "payload"#>>'{target,kind}' = 'doc' THEN "payload"#>>'{target,docId}' ELSE "payload"#>>'{target,objectId}' END,
  CASE WHEN "payload"#>>'{target,kind}' = 'doc' THEN COALESCE("payload"#>>'{target,elementId}', '') ELSE '' END
)::text, 'UTF8')), 'hex')
WHERE "kind" = 'LINK_ISSUE';
WITH duplicates AS (
  SELECT "id", row_number() OVER (PARTITION BY "installationId", "dedupeKey" ORDER BY "createdAt", "id") AS n
  FROM "LinearOutbox" WHERE "status" = 'PENDING' AND "dedupeKey" IS NOT NULL
)
UPDATE "LinearOutbox" SET "status" = 'DONE', "lockedUntil" = NULL, "lastError" = 'Duplicate pending link job', "updatedAt" = now()
WHERE "id" IN (SELECT "id" FROM duplicates WHERE n > 1);
CREATE UNIQUE INDEX "LinearOutbox_pending_dedupe_unique" ON "LinearOutbox" ("installationId", "dedupeKey")
WHERE "status" = 'PENDING' AND "dedupeKey" IS NOT NULL;
