import { jest } from '@jest/globals';
import { CardsHandler } from '../../src/mcp/handlers/cards.js';
import { WRITE_TOOLS, createToolHandler, isReadOnlyMode } from '../../src/mcp/tool-router.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';
import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';

describe('Integration Test: Autonomous Dashboard Architect (ai_dashboard_build_full)', () => {
  let mockClient;
  let cardsHandler;
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv, METABASE_READ_ONLY_MODE: 'false', METABASE_URL: 'http://localhost:3000' };

    mockClient = {
      request: jest.fn(),
      createQuestion: jest.fn().mockImplementation((payload) => Promise.resolve({
        id: Math.floor(Math.random() * 1000) + 10,
        name: payload.name,
        display: payload.display,
        description: payload.description,
      })),
      createDashboard: jest.fn().mockImplementation((payload) => Promise.resolve({
        id: 404,
        name: payload.name,
        description: payload.description,
        collection_id: payload.collection_id || null,
        parameters: payload.parameters || [],
      })),
      updateDashboard: jest.fn().mockResolvedValue({ id: 404 }),
      addCardToDashboard: jest.fn().mockResolvedValue({ id: 808 }),
      getDashboard: jest.fn().mockResolvedValue({
        id: 404,
        name: 'Test Dashboard',
        dashcards: [],
        parameters: [],
      }),
    };

    cardsHandler = new CardsHandler(mockClient);
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  // ─────────────────────────────────────────────────────────────
  // 1. TOOL ROUTER & REGISTRY VERIFICATION
  // ─────────────────────────────────────────────────────────────
  describe('1. Tool Registry and Router Configuration', () => {
    test('ai_dashboard_build_full is registered in WRITE_TOOLS for read-only gating', () => {
      expect(WRITE_TOOLS.has('ai_dashboard_build_full')).toBe(true);
    });

    test('ai_dashboard_build_full is defined in TOOL_METADATA with write=true and valid outputSchema', () => {
      const meta = TOOL_METADATA.ai_dashboard_build_full;
      expect(meta).toBeDefined();
      expect(meta.write).toBe(true);
      expect(meta.destructive).toBe(false);
      expect(meta.outputSchema).toBeDefined();
      expect(meta.outputSchema.required).toContain('dashboard_id');
      expect(meta.outputSchema.required).toContain('cards');
      expect(meta.outputSchema.required).toContain('_provenance');
    });

    test('ai_dashboard_build_full is present in getToolDefinitions() with correct inputSchema', () => {
      const defs = getToolDefinitions();
      const toolDef = defs.find(d => d.name === 'ai_dashboard_build_full');

      expect(toolDef).toBeDefined();
      expect(toolDef.inputSchema.required).toContain('name');
      expect(toolDef.inputSchema.required).toContain('database_id');
      expect(toolDef.inputSchema.required).toContain('cards');
      expect(toolDef.inputSchema.properties.cards.minItems).toBe(4);
      expect(toolDef.annotations.readOnlyHint).toBe(false);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 2. READ-ONLY MODE SECURITY ENFORCEMENT
  // ─────────────────────────────────────────────────────────────
  describe('2. Read-Only Mode Security Enforcement', () => {
    test('blocks ai_dashboard_build_full through createToolHandler when read-only mode is active', async () => {
      process.env.METABASE_READ_ONLY_MODE = 'true';

      const routeMap = {
        'ai_dashboard_build_full': (args) => cardsHandler.handleBuildFullDashboard(args),
      };
      const toolHandler = createToolHandler(routeMap);

      const request = {
        params: {
          name: 'ai_dashboard_build_full',
          arguments: {
            name: 'Blocked Dashboard',
            database_id: 1,
            cards: [
              { name: 'C1', sql: 'SELECT 1' },
              { name: 'C2', sql: 'SELECT 2' },
              { name: 'C3', sql: 'SELECT 3' },
              { name: 'C4', sql: 'SELECT 4' },
            ],
          },
        },
      };

      await expect(toolHandler(request)).rejects.toThrow(McpError);
      await expect(toolHandler(request)).rejects.toThrow(/Read-only mode is active/);
    });

    test('CardsHandler.handleBuildFullDashboard returns blocked message when read-only mode is active', async () => {
      process.env.METABASE_READ_ONLY_MODE = 'true';

      const result = await cardsHandler.handleBuildFullDashboard({
        name: 'Blocked Direct Call',
        database_id: 1,
        cards: [
          { name: 'C1', sql: 'SELECT 1' },
          { name: 'C2', sql: 'SELECT 2' },
          { name: 'C3', sql: 'SELECT 3' },
          { name: 'C4', sql: 'SELECT 4' },
        ],
      });

      expect(result.content[0].text).toContain('Read-Only Mode Active');
      expect(result.content[0].text).toContain('Operation Blocked');
      expect(mockClient.createDashboard).not.toHaveBeenCalled();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 3. END-TO-END TOOL EXECUTION (CardsHandler.handleBuildFullDashboard)
  // ─────────────────────────────────────────────────────────────
  describe('3. End-to-End Autonomous Dashboard Building', () => {
    test('successfully builds full dashboard with 4 cards, non-overlapping coordinates, and linked filters', async () => {
      const args = {
        name: 'Executive Sales & Operations Cockpit',
        description: 'Single-call autonomous dashboard for enterprise KPIs',
        database_id: 2,
        collection_id: 5,
        theme: 'executive',
        cards: [
          {
            name: 'Total Revenue YTD',
            description: 'Sum of all settled orders',
            display: 'scalar',
            sql: 'SELECT sum(total_amount) FROM orders WHERE {{date_range}}',
          },
          {
            name: 'Active Customers',
            description: 'Distinct customer count',
            display: 'number',
            sql: 'SELECT count(DISTINCT customer_id) FROM orders WHERE {{date_range}}',
          },
          {
            name: 'Monthly Revenue Trend',
            description: 'Time series revenue trajectory',
            display: 'line',
            sql: 'SELECT date_trunc(\'month\', created_at) AS month, sum(total_amount) FROM orders WHERE {{date_range}} GROUP BY 1',
          },
          {
            name: 'Order Breakdown by Category',
            description: 'Categorical distribution',
            display: 'pie',
            sql: 'SELECT category, count(*) FROM products WHERE {{category}} GROUP BY 1',
          },
        ],
        filters: [
          {
            name: 'Date Range',
            slug: 'date_range',
            type: 'date/all-options',
            target_variable: 'date_range',
          },
          {
            name: 'Category',
            slug: 'category',
            type: 'category',
            target_variable: 'category',
          },
        ],
      };

      const res = await cardsHandler.handleBuildFullDashboard(args);

      // Verify text summary formatting
      expect(res.content).toBeDefined();
      expect(res.content[0].type).toBe('text');
      expect(res.content[0].text).toContain('Autonomous Dashboard Built Successfully!');
      expect(res.content[0].text).toContain('Executive Sales & Operations Cockpit');
      expect(res.content[0].text).toContain('Total Revenue YTD');
      expect(res.content[0].text).toContain('24-Column Grid Layout');

      // Verify structuredContent
      const structured = res.structuredContent;
      expect(structured).toBeDefined();
      expect(structured.dashboard_id).toBe(404);
      expect(structured.name).toBe('Executive Sales & Operations Cockpit');
      expect(structured.card_count).toBe(4);
      expect(structured.filter_count).toBe(2);
      expect(structured.cards).toHaveLength(4);
      expect(structured.filters).toHaveLength(2);

      // Verify 24-col coordinates in structured output
      const cardPositions = structured.cards.map(c => c.position);
      expect(cardPositions[0]).toEqual({ row: 0, col: 0, size_x: 6, size_y: 4 });
      expect(cardPositions[1]).toEqual({ row: 0, col: 6, size_x: 6, size_y: 4 });
      expect(cardPositions[2]).toEqual({ row: 4, col: 0, size_x: 12, size_y: 8 });
      expect(cardPositions[3]).toEqual({ row: 12, col: 0, size_x: 12, size_y: 6 });

      // Verify provenance
      expect(structured._provenance).toBeDefined();
      expect(structured._provenance.ai_generated).toBe(true);
      expect(structured._provenance.tool).toBe('ai_dashboard_build_full');
      expect(structured._provenance.review_required).toBe(false);

      // Verify client invocations
      expect(mockClient.createDashboard).toHaveBeenCalledTimes(1);
      expect(mockClient.createQuestion).toHaveBeenCalledTimes(4);
      expect(mockClient.addCardToDashboard).toHaveBeenCalledTimes(4);
    });

    test('handles errors during card creation gracefully and returns descriptive error text', async () => {
      mockClient.createQuestion.mockRejectedValueOnce(new Error('Database table "orders" not found'));

      const args = {
        name: 'Faulty Dashboard',
        database_id: 99,
        cards: [
          { name: 'C1', sql: 'SELECT 1' },
          { name: 'C2', sql: 'SELECT 2' },
          { name: 'C3', sql: 'SELECT 3' },
          { name: 'C4', sql: 'SELECT 4' },
        ],
      };

      const res = await cardsHandler.handleBuildFullDashboard(args);

      expect(res.content[0].text).toContain('Failed to build dashboard');
      expect(res.content[0].text).toContain('orders');
    });

    test('routes ai_dashboard_build_full correctly through handler routes() map', () => {
      const routes = cardsHandler.routes();
      expect(routes).toHaveProperty('ai_dashboard_build_full');
      expect(typeof routes['ai_dashboard_build_full']).toBe('function');
    });
  });
});
