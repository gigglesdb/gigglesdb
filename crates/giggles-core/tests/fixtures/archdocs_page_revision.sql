CREATE TABLE "PageRevision" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "pageId" UUID NOT NULL,
    "revision" INTEGER NOT NULL,
    "title" TEXT NOT NULL,
    "content" TEXT NOT NULL,
    "authorKind" "DesignAuthorKind" NOT NULL,
    "authorUserId" UUID,
    "authorUserIds" UUID[] DEFAULT ARRAY[]::UUID[],
    "clientName" TEXT,
    "sessionUrl" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "PageRevision_pkey" PRIMARY KEY ("id")
);
