# Base stage for dependencies
FROM node:20-alpine AS base
RUN apk add --no-cache curl
WORKDIR /app
COPY package*.json ./

# Development stage (with devDependencies, tests, and source code)
FROM base AS development
RUN npm install
COPY . .
CMD ["npm", "start"]

# Production stage (optimized, without devDependencies)
FROM base AS production
ENV NODE_ENV=production
RUN npm install --only=production
COPY . .
CMD ["node", "src/server.js"]
