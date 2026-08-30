/**
 * Remote MCP Server (SSE & HTTP Transport)
 * Supports Claude, Cursor, ChatGPT Actions, and Gemini Webhook integrations
 */

import express from 'express';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { MetabaseMCPServer } from './server.js';
import { logger } from '../utils/logger.js';
import { getToolDefinitions } from './tool-registry.js';

const app = express();
app.use(express.json());

// CORS for web-based AI clients
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-API-Key, X-Metabase-Session');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

const mcpServer = new MetabaseMCPServer();
const transports = new Map();

// Health Check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    server: 'metabase-ai-assistant',
    version: '5.0.0',
    tools_count: getToolDefinitions().length,
    timestamp: new Date().toISOString(),
  });
});

// OpenAI / Gemini Tool Spec Exporter
app.get('/tools/openapi.json', (req, res) => {
  const tools = getToolDefinitions();
  const paths = {};

  tools.forEach(tool => {
    paths[`/tools/${tool.name}`] = {
      post: {
        summary: tool.name,
        description: tool.description,
        operationId: tool.name,
        requestBody: {
          required: true,
          content: {
            'application/json': {
              schema: tool.inputSchema || { type: 'object' },
            },
          },
        },
        responses: {
          '200': {
            description: 'Successful tool execution',
            content: {
              'application/json': {
                schema: { type: 'object' },
              },
            },
          },
        },
      },
    };
  });

  res.json({
    openapi: '3.1.0',
    info: {
      title: 'Metabase AI Assistant API',
      version: '4.2.1',
      description: 'OpenAPI specification for ChatGPT Actions and Google Gemini Function Calling integrations.',
    },
    paths,
  });
});

// Direct Tool Execution Endpoint (for ChatGPT Actions & Gemini Webhooks)
app.post('/tools/:toolName', async (req, res) => {
  const { toolName } = req.params;
  const args = req.body || {};

  try {
    await mcpServer.ensureInitialized();
    const router = mcpServer.toolRouter;
    const result = await router.handleToolCall(toolName, args);
    res.json(result);
  } catch (error) {
    logger.error(`Error executing tool ${toolName}: ${error.message}`);
    res.status(500).json({
      error: error.message,
      tool: toolName,
    });
  }
});

// MCP SSE Transport Handlers (for Remote Claude / Cursor clients)
app.get('/sse', async (req, res) => {
  logger.info('New MCP SSE connection established');
  const transport = new SSEServerTransport('/messages', res);
  transports.set(transport.sessionId, transport);

  req.on('close', () => {
    logger.info(`MCP SSE connection closed: ${transport.sessionId}`);
    transports.delete(transport.sessionId);
  });

  await mcpServer.server.connect(transport);
});

app.post('/messages', async (req, res) => {
  const sessionId = req.query.sessionId;
  const transport = transports.get(sessionId);

  if (!transport) {
    res.status(404).send('Session not found');
    return;
  }

  await transport.handlePostMessage(req, res);
});

const DEFAULT_PORT = parseInt(process.env.MCP_SSE_PORT || process.env.PORT || '3055', 10);

function startServer(port) {
  const server = app.listen(port, () => {
    logger.info(`Metabase Remote MCP & AI Webhook Server running on port ${port}`);
    logger.info(`Health check: http://localhost:${port}/health`);
    logger.info(`SSE MCP Endpoint: http://localhost:${port}/sse`);
    logger.info(`OpenAPI Schema: http://localhost:${port}/tools/openapi.json`);
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      logger.warn(`Port ${port} in use, trying port ${port + 1}...`);
      startServer(port + 1);
    } else {
      logger.error(`Server error: ${err.message}`);
    }
  });
}

startServer(DEFAULT_PORT);
