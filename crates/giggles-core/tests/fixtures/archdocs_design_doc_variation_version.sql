CREATE TABLE "DesignDocVariationVersion" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "variationId" UUID NOT NULL,
  "version" INTEGER NOT NULL,
  "content" TEXT NOT NULL,
  "brief" JSONB NOT NULL,
  "authorKind" "DesignAuthorKind" NOT NULL,
  "authorUserIds" UUID[] NOT NULL,
  "clientName" TEXT,
  "sessionUrl" TEXT,
  "isAmendment" BOOLEAN NOT NULL DEFAULT false,
  "amendmentReason" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "DesignDocVariationVersion_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "DesignDocVariationVersion_variationId_fkey" FOREIGN KEY ("variationId") REFERENCES "DesignDocVariation"("id") ON DELETE CASCADE ON UPDATE CASCADE
);
