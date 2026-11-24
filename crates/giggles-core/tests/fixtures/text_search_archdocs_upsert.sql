INSERT INTO "SearchDocument" (
    "entityType", "entityId", "organizationId", "architectureDocId", "spaceId",
    "title", "body", "matchOnlyBody", "updatedAt"
)
VALUES ($1, $2::uuid, $3::uuid, NULLIF($4, '')::uuid, NULLIF($5, '')::uuid, $6, $7, $8, now())
ON CONFLICT ("entityType", "entityId") DO UPDATE SET
    "organizationId" = EXCLUDED."organizationId",
    "architectureDocId" = EXCLUDED."architectureDocId",
    "spaceId" = EXCLUDED."spaceId",
    "title" = EXCLUDED."title",
    "body" = EXCLUDED."body",
    "matchOnlyBody" = EXCLUDED."matchOnlyBody",
    "updatedAt" = now()
