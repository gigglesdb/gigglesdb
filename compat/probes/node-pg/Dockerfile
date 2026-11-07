FROM node:22-slim
WORKDIR /probe
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY probe.js ./
ENTRYPOINT ["node", "/probe/probe.js"]
