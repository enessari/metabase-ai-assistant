import { CollectionsHandler } from '../src/mcp/handlers/collections.js';
import { UsersHandler } from '../src/mcp/handlers/users.js';
import { ActionsHandler } from '../src/mcp/handlers/actions.js';
import { CardsHandler } from '../src/mcp/handlers/cards.js';
import { SqlHandler } from '../src/mcp/handlers/sql.js';
import { TOOL_METADATA, getToolDefinitions } from '../src/mcp/tool-registry.js';
import { jest } from '@jest/globals';

describe('MCP Handlers Regression & Bug Fix Tests', () => {
  let mockClient;

  beforeEach(() => {
    mockClient = {
      request: jest.fn().mockResolvedValue({}),
      getQuestions: jest.fn().mockResolvedValue([]),
      getDatabaseTables: jest.fn().mockResolvedValue([{ id: 1, name: 'users', schema: 'public' }]),
      createParametricQuestion: jest.fn().mockResolvedValue({ id: 99, name: 'Test Parametric' }),
    };
  });

  describe('PR #13: ensureInitialized on handlers', () => {
    test('CollectionsHandler defines and runs ensureInitialized without error', async () => {
      const handler = new CollectionsHandler(mockClient);
      await expect(handler.ensureInitialized()).resolves.toBeUndefined();
    });

    test('UsersHandler defines and runs ensureInitialized without error', async () => {
      const handler = new UsersHandler(mockClient);
      await expect(handler.ensureInitialized()).resolves.toBeUndefined();
    });

    test('ActionsHandler defines and runs ensureInitialized without error', async () => {
      const handler = new ActionsHandler(mockClient);
      await expect(handler.ensureInitialized()).resolves.toBeUndefined();
    });
  });

  describe('PR #14: mb_card_data parameter schema', () => {
    test('mb_card_data declares parameters as array of objects in getToolDefinitions', () => {
      const tools = getToolDefinitions();
      const tool = tools.find(t => t.name === 'mb_card_data');
      expect(tool).toBeDefined();
      expect(tool.inputSchema.properties.parameters.type).toBe('array');
      expect(tool.inputSchema.properties.parameters.items.type).toBe('object');
    });
  });

  describe('PR #15: mb_card_get nullable fields and dataset_query', () => {
    test('mb_card_get TOOL_METADATA outputSchema allows null description, collection_id and includes dataset_query', () => {
      const metadata = TOOL_METADATA.mb_card_get;
      expect(metadata).toBeDefined();
      expect(metadata.outputSchema.properties.description.type).toEqual(['string', 'null']);
      expect(metadata.outputSchema.properties.collection_id.type).toEqual(['number', 'null']);
      expect(metadata.outputSchema.properties.dataset_query.type).toBe('object');
    });

    test('handleCardGet returns structuredContent with null-safe fields and dataset_query', async () => {
      mockClient.request.mockResolvedValueOnce({
        id: 42,
        name: 'Active Users',
        description: null,
        display: 'table',
        database_id: 1,
        collection_id: null,
        dataset_query: { type: 'native', native: { query: 'SELECT 1' } },
        archived: false,
        created_at: '2026-01-01T00:00:00Z',
        updated_at: '2026-01-02T00:00:00Z'
      });

      const cardsHandler = new CardsHandler(mockClient);
      const result = await cardsHandler.handleCardGet({ card_id: 42 });

      expect(result.structuredContent).toBeDefined();
      expect(result.structuredContent.id).toBe(42);
      expect(result.structuredContent.description).toBeNull();
      expect(result.structuredContent.collection_id).toBeNull();
      expect(result.structuredContent.dataset_query).toEqual({ type: 'native', native: { query: 'SELECT 1' } });
    });
  });

  describe('Issue #12: Security Provenance Envelope', () => {
    test('handleGenerateSQL includes warning banner and _provenance envelope', async () => {
      const mockAiAssistant = {
        generateSQL: jest.fn().mockResolvedValue('SELECT * FROM users;'),
      };
      const sqlHandler = new SqlHandler(mockClient, null, null, mockAiAssistant);
      const result = await sqlHandler.handleGenerateSQL({ description: 'List all users', database_id: 1 });

      expect(result.content[0].text).toContain('AI-GENERATED SQL — REVIEW BEFORE EXECUTING');
      expect(result.structuredContent).toBeDefined();
      expect(result.structuredContent._provenance.ai_generated).toBe(true);
      expect(result.structuredContent._provenance.tool).toBe('ai_sql_generate');
      expect(result.structuredContent._provenance.review_required).toBe(true);
    });
  });
});
