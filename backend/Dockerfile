FROM node:20-alpine

WORKDIR /app

# Install dependencies first (layer-cached)
COPY package.json ./
RUN npm install --omit=dev

# Copy application source
COPY . .

# Non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

EXPOSE 3000

CMD ["node", "server.js"]
