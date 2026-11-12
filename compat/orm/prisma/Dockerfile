ARG BASE
FROM ${BASE}
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends openssl ca-certificates && rm -rf /var/lib/apt/lists/*
RUN npm init -y && npm install --save-exact prisma@6.19.3 @prisma/client@6.19.3
COPY . /app
RUN npx prisma generate && mkdir -p prisma/migrations/20260102000000_shop && npx prisma migrate diff --from-empty --to-schema-datamodel prisma/schema.prisma --script > prisma/migrations/20260102000000_shop/migration.sql
ENTRYPOINT ["node", "/app/probe.js"]
