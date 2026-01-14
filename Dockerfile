# Metabase AI Assistant MCP Server
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci --only=production

# Copy source code
COPY src/ ./src/
COPY .env.example ./

# Create logs directory
RUN mkdir -p logs

# Create non-root user
RUN addgroup -g 1001 -S nodejs
RUN adduser -S mcp -u 1001
RUN chown -R mcp:nodejs /app
USER mcp

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "console.log('Health check')" || exit 1

# Expose port (for health checks)
EXPOSE 3000

# Start MCP server
CMD ["node", "src/mcp/server.js"]