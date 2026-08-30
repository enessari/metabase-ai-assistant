import { jest } from '@jest/globals';
import {
  DashboardArchitect,
  calculate24ColGridPositions,
  validateNoCollisions,
  generateFilterMappings,
  buildFullDashboard,
  getCardDimensionsAndArchetype,
  getDefaultVisualizationSettings,
  GRID_WIDTH,
  CARD_ARCHETYPES,
} from '../../src/analytics/dashboard-architect.js';

describe('DashboardArchitect Unit Test Suite (M3)', () => {

  // ─────────────────────────────────────────────────────────────
  // 1. 24-COLUMN GRID LAYOUT & BIN-PACKING
  // ─────────────────────────────────────────────────────────────
  describe('1. 24-Column Grid Coordinate Placement (calculate24ColGridPositions)', () => {
    test('places 4 KPI summary cards in row 0 with 6x4 dimensions spanning 24 columns', () => {
      const cards = [
        { name: 'Total Revenue', display: 'scalar' },
        { name: 'Active Users', display: 'number' },
        { name: 'NPS Score', display: 'gauge' },
        { name: 'Conversion Rate', display: 'smartscalar' },
      ];

      const positions = calculate24ColGridPositions(cards);

      expect(positions).toHaveLength(4);
      expect(positions[0]).toEqual({ row: 0, col: 0, size_x: 6, size_y: 4 });
      expect(positions[1]).toEqual({ row: 0, col: 6, size_x: 6, size_y: 4 });
      expect(positions[2]).toEqual({ row: 0, col: 12, size_x: 6, size_y: 4 });
      expect(positions[3]).toEqual({ row: 0, col: 18, size_x: 6, size_y: 4 });

      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('places full multi-tier layout: 4 KPIs, 2 Trends, 2 Breakdowns, and 1 Table with exact section coordinates', () => {
      const cards = [
        // Row 0: 4 KPIs (size 6x4)
        { name: 'Revenue', display: 'scalar' },
        { name: 'Orders', display: 'number' },
        { name: 'AOV', display: 'scalar' },
        { name: 'Returns', display: 'number' },
        // Row 4: 2 Trend charts (size 12x8)
        { name: 'Monthly Revenue Trend', display: 'line' },
        { name: 'Orders by Category', display: 'bar' },
        // Row 12: 2 Breakdown charts (size 12x6)
        { name: 'Traffic Source Share', display: 'pie' },
        { name: 'Sales by Region', display: 'row' },
        // Row 18: 1 Full-width Table (size 24x8)
        { name: 'Detailed Order Log', display: 'table' },
      ];

      const positions = calculate24ColGridPositions(cards);

      expect(positions).toHaveLength(9);

      // KPI positions (Row 0)
      expect(positions[0]).toEqual({ row: 0, col: 0, size_x: 6, size_y: 4 });
      expect(positions[1]).toEqual({ row: 0, col: 6, size_x: 6, size_y: 4 });
      expect(positions[2]).toEqual({ row: 0, col: 12, size_x: 6, size_y: 4 });
      expect(positions[3]).toEqual({ row: 0, col: 18, size_x: 6, size_y: 4 });

      // Trend positions (Row 4)
      expect(positions[4]).toEqual({ row: 4, col: 0, size_x: 12, size_y: 8 });
      expect(positions[5]).toEqual({ row: 4, col: 12, size_x: 12, size_y: 8 });

      // Breakdown positions (Row 12)
      expect(positions[6]).toEqual({ row: 12, col: 0, size_x: 12, size_y: 6 });
      expect(positions[7]).toEqual({ row: 12, col: 12, size_x: 12, size_y: 6 });

      // Table position (Row 18)
      expect(positions[8]).toEqual({ row: 18, col: 0, size_x: 24, size_y: 8 });

      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('wraps additional KPI cards (>4) onto subsequent rows without collision', () => {
      const cards = [
        { name: 'KPI 1', display: 'scalar' },
        { name: 'KPI 2', display: 'scalar' },
        { name: 'KPI 3', display: 'scalar' },
        { name: 'KPI 4', display: 'scalar' },
        { name: 'KPI 5', display: 'scalar' },
        { name: 'KPI 6', display: 'scalar' },
      ];

      const positions = calculate24ColGridPositions(cards);

      expect(positions[4]).toEqual({ row: 4, col: 0, size_x: 6, size_y: 4 });
      expect(positions[5]).toEqual({ row: 4, col: 6, size_x: 6, size_y: 4 });
      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('lays out 3 breakdown charts side-by-side with width 8', () => {
      const cards = [
        { name: 'Breakdown 1', display: 'pie' },
        { name: 'Breakdown 2', display: 'donut' },
        { name: 'Breakdown 3', display: 'row' },
      ];

      const positions = calculate24ColGridPositions(cards);

      expect(positions[0]).toEqual({ row: 0, col: 0, size_x: 8, size_y: 6 });
      expect(positions[1]).toEqual({ row: 0, col: 8, size_x: 8, size_y: 6 });
      expect(positions[2]).toEqual({ row: 0, col: 16, size_x: 8, size_y: 6 });
      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('preserves valid explicit custom positions when provided without collisions', () => {
      const customCards = [
        { name: 'Custom 1', row: 0, col: 0, size_x: 12, size_y: 5 },
        { name: 'Custom 2', row: 0, col: 12, size_x: 12, size_y: 5 },
        { name: 'Custom 3', row: 5, col: 0, size_x: 24, size_y: 10 },
        { name: 'Custom 4', row: 15, col: 0, size_x: 24, size_y: 8 },
      ];

      const positions = calculate24ColGridPositions(customCards);

      expect(positions[0]).toEqual({ row: 0, col: 0, size_x: 12, size_y: 5 });
      expect(positions[1]).toEqual({ row: 0, col: 12, size_x: 12, size_y: 5 });
      expect(positions[2]).toEqual({ row: 5, col: 0, size_x: 24, size_y: 10 });
      expect(positions[3]).toEqual({ row: 15, col: 0, size_x: 24, size_y: 8 });
      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('recovers safely from invalid or colliding explicit positions', () => {
      const collidingCards = [
        { name: 'Card 1', row: 0, col: 0, size_x: 18, size_y: 5, display: 'scalar' },
        { name: 'Card 2 (Colliding)', row: 0, col: 6, size_x: 18, size_y: 5, display: 'scalar' },
        { name: 'Card 3', row: 0, col: 0, size_x: 12, size_y: 5, display: 'line' },
        { name: 'Card 4', row: 0, col: 0, size_x: 24, size_y: 8, display: 'table' },
      ];

      const positions = calculate24ColGridPositions(collidingCards);

      expect(positions).toHaveLength(4);
      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('supports sequential bin-packing mode', () => {
      const cards = [
        { name: 'Card 1', display: 'scalar' }, // 6x4
        { name: 'Card 2', display: 'line' },   // 12x8
        { name: 'Card 3', display: 'scalar' }, // 6x4
        { name: 'Card 4', display: 'table' },  // 24x8
      ];

      const positions = calculate24ColGridPositions(cards, { forceSequential: true });

      expect(positions).toHaveLength(4);
      expect(validateNoCollisions(positions)).toBe(true);
      for (const pos of positions) {
        expect(pos.col + pos.size_x).toBeLessThanOrEqual(GRID_WIDTH);
        expect(pos.row).toBeGreaterThanOrEqual(0);
      }
    });

    test('handles empty or non-array inputs gracefully', () => {
      expect(calculate24ColGridPositions([])).toEqual([]);
      expect(calculate24ColGridPositions(null)).toEqual([]);
      expect(calculate24ColGridPositions(undefined)).toEqual([]);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 2. COLLISION & BOUNDS VALIDATOR (validateNoCollisions)
  // ─────────────────────────────────────────────────────────────
  describe('2. Collision & Boundary Validation (validateNoCollisions)', () => {
    test('throws when card extends beyond 24 columns', () => {
      const invalidCards = [
        { row: 0, col: 20, size_x: 6, size_y: 4 }, // 20 + 6 = 26 > 24
      ];
      expect(() => validateNoCollisions(invalidCards)).toThrow(/exceeds 24-column grid boundary/);
    });

    test('throws when two cards overlap in 2D space', () => {
      const overlappingCards = [
        { row: 0, col: 0, size_x: 12, size_y: 6 },
        { row: 2, col: 6, size_x: 12, size_y: 6 }, // overlaps rows 2..5, cols 6..11
      ];
      expect(() => validateNoCollisions(overlappingCards)).toThrow(/Grid collision detected/);
    });

    test('throws when card coordinates are negative or zero size', () => {
      expect(() => validateNoCollisions([{ row: -1, col: 0, size_x: 6, size_y: 4 }])).toThrow(/invalid row/);
      expect(() => validateNoCollisions([{ row: 0, col: -2, size_x: 6, size_y: 4 }])).toThrow(/invalid col/);
      expect(() => validateNoCollisions([{ row: 0, col: 0, size_x: 0, size_y: 4 }])).toThrow(/invalid size_x/);
      expect(() => validateNoCollisions([{ row: 0, col: 0, size_x: 6, size_y: 0 }])).toThrow(/invalid size_y/);
    });

    test('returns true for adjacent, non-overlapping cards', () => {
      const adjacentCards = [
        { row: 0, col: 0, size_x: 12, size_y: 6 },
        { row: 0, col: 12, size_x: 12, size_y: 6 },
        { row: 6, col: 0, size_x: 24, size_y: 8 },
      ];
      expect(validateNoCollisions(adjacentCards)).toBe(true);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 3. PARAMETER & FILTER MAPPING GENERATION
  // ─────────────────────────────────────────────────────────────
  describe('3. Parameter & Filter Mappings (generateFilterMappings)', () => {
    test('auto-detects template tags in card SQL and connects to matching dashboard filters', () => {
      const cards = [
        {
          name: 'Revenue Trend',
          sql: 'SELECT date, sum(amount) FROM orders WHERE {{date_range}} AND {{category}} GROUP BY 1',
        },
        {
          name: 'Category Breakdown',
          sql: 'SELECT category, count(*) FROM orders WHERE {{category}} GROUP BY 1',
        },
        {
          name: 'Top Users',
          sql: 'SELECT * FROM users LIMIT 10', // No filters
        },
      ];

      const filters = [
        { name: 'Date Range', slug: 'date_range', type: 'date/all-options', target_variable: 'date_range' },
        { name: 'Product Category', slug: 'category', type: 'category', target_variable: 'category' },
      ];

      const mappings = generateFilterMappings(cards, filters);

      expect(mappings).toHaveLength(3);

      // Card 0 has both filters
      expect(mappings[0]).toHaveLength(2);
      expect(mappings[0]).toEqual([
        {
          parameter_id: 'param_date_range',
          target: ['variable', ['template-tag', 'date_range']],
        },
        {
          parameter_id: 'param_category',
          target: ['variable', ['template-tag', 'category']],
        },
      ]);

      // Card 1 has category filter
      expect(mappings[1]).toHaveLength(1);
      expect(mappings[1][0]).toEqual({
        parameter_id: 'param_category',
        target: ['variable', ['template-tag', 'category']],
      });

      // Card 2 has no matching filters
      expect(mappings[2]).toHaveLength(0);
    });

    test('supports field dimension mapping when field_id is specified on filter', () => {
      const cards = [
        { name: 'Orders Card', sql: 'SELECT * FROM orders' },
      ];

      const filters = [
        { name: 'User Region', slug: 'region', type: 'string/=', field_id: 1042 },
      ];

      const mappings = generateFilterMappings(cards, filters);

      expect(mappings[0]).toHaveLength(1);
      expect(mappings[0][0]).toEqual({
        parameter_id: 'param_region',
        target: ['dimension', ['field', 1042, null]],
      });
    });

    test('handles empty filters or empty cards without error', () => {
      expect(generateFilterMappings([], [{ name: 'Test' }])).toEqual([]);
      expect(generateFilterMappings([{ sql: 'SELECT 1' }], [])).toEqual([[]]);
      expect(generateFilterMappings(null, null)).toEqual([]);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 4. AUTONOMOUS DASHBOARD BUILDER (buildFullDashboard)
  // ─────────────────────────────────────────────────────────────
  describe('4. Autonomous Dashboard Builder (buildFullDashboard)', () => {
    let mockClient;

    beforeEach(() => {
      mockClient = {
        createQuestion: jest.fn().mockImplementation((payload) => Promise.resolve({
          id: Math.floor(Math.random() * 1000) + 100,
          name: payload.name,
          display: payload.display,
        })),
        createDashboard: jest.fn().mockImplementation((payload) => Promise.resolve({
          id: 555,
          name: payload.name,
          description: payload.description,
        })),
        updateDashboard: jest.fn().mockResolvedValue({ id: 555 }),
        addCardToDashboard: jest.fn().mockResolvedValue({ id: 999 }),
        request: jest.fn(),
      };
    });

    test('throws validation error when fewer than 4 cards are provided', async () => {
      const options = {
        name: 'Incomplete Dashboard',
        databaseId: 1,
        cards: [
          { name: 'Card 1', sql: 'SELECT 1' },
          { name: 'Card 2', sql: 'SELECT 2' },
          { name: 'Card 3', sql: 'SELECT 3' },
        ],
        client: mockClient,
      };

      await expect(buildFullDashboard(options)).rejects.toThrow(/requires at least 4 card definitions/);
    });

    test('throws validation error when dashboard name or databaseId is missing', async () => {
      const validCards = [
        { name: 'Card 1', sql: 'SELECT 1' },
        { name: 'Card 2', sql: 'SELECT 2' },
        { name: 'Card 3', sql: 'SELECT 3' },
        { name: 'Card 4', sql: 'SELECT 4' },
      ];

      await expect(buildFullDashboard({ databaseId: 1, cards: validCards, client: mockClient })).rejects.toThrow(
        /Dashboard name is required/
      );

      await expect(buildFullDashboard({ name: 'Valid Name', cards: validCards, client: mockClient })).rejects.toThrow(
        /Valid database_id is required/
      );
    });

    test('executes complete single-call dashboard build lifecycle with >=4 cards and filters', async () => {
      const cards = [
        { name: 'Total Sales', display: 'scalar', sql: 'SELECT sum(total) FROM orders WHERE {{date_range}}' },
        { name: 'Total Units', display: 'number', sql: 'SELECT sum(units) FROM orders WHERE {{date_range}}' },
        { name: 'Revenue by Month', display: 'line', sql: 'SELECT date_trunc(\'month\', created_at), sum(total) FROM orders GROUP BY 1' },
        { name: 'Category Breakdown', display: 'pie', sql: 'SELECT category, count(*) FROM products GROUP BY 1' },
        { name: 'Orders Detail Table', display: 'table', sql: 'SELECT * FROM orders LIMIT 100' },
      ];

      const filters = [
        { name: 'Date Range', slug: 'date_range', type: 'date/all-options', target_variable: 'date_range' },
      ];

      const result = await buildFullDashboard({
        name: 'Executive Performance Dashboard',
        description: 'Complete quarterly overview',
        databaseId: 2,
        collectionId: 10,
        theme: 'executive',
        cards,
        filters,
        client: mockClient,
      });

      // Verify dashboard creation
      expect(mockClient.createDashboard).toHaveBeenCalledTimes(1);
      expect(mockClient.createDashboard).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'Executive Performance Dashboard',
          collection_id: 10,
        })
      );

      // Verify question creations (5 cards)
      expect(mockClient.createQuestion).toHaveBeenCalledTimes(5);

      // Verify adding cards to dashboard with positions
      expect(mockClient.addCardToDashboard).toHaveBeenCalledTimes(5);

      // Verify result structure
      expect(result.dashboard_id).toBe(555);
      expect(result.name).toBe('Executive Performance Dashboard');
      expect(result.card_count).toBe(5);
      expect(result.filter_count).toBe(1);
      expect(result.cards).toHaveLength(5);
      expect(result.url).toContain('/dashboard/555');

      // Verify provenance envelope
      expect(result._provenance).toBeDefined();
      expect(result._provenance.ai_generated).toBe(true);
      expect(result._provenance.tool).toBe('ai_dashboard_build_full');
      expect(result._provenance.review_required).toBe(false);
      expect(result._provenance.generation_parameters.grid_system).toBe('24-column');
      expect(result._provenance.generation_parameters.card_count).toBe(5);
    });

    test('attaches visualization settings and template-tags to created question entities', async () => {
      const cards = [
        { name: 'KPI 1', display: 'scalar', sql: 'SELECT 100' },
        { name: 'KPI 2', display: 'number', sql: 'SELECT 200' },
        { name: 'KPI 3', display: 'gauge', sql: 'SELECT 75' },
        { name: 'Line Chart', display: 'line', sql: 'SELECT date, val FROM t WHERE {{start_date}}' },
      ];

      const filters = [
        { name: 'Start Date', slug: 'start_date', type: 'date/all-options', target_variable: 'start_date' },
      ];

      await buildFullDashboard({
        name: 'Visual Settings Test',
        databaseId: 1,
        cards,
        filters,
        client: mockClient,
      });

      // Inspect the line chart question creation call
      const lineCall = mockClient.createQuestion.mock.calls.find(call => call[0].name === 'Line Chart');
      expect(lineCall).toBeDefined();
      expect(lineCall[0].dataset_query.native['template-tags']).toHaveProperty('start_date');
      expect(lineCall[0].visualization_settings['graph.show_values']).toBe(true);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 5. HELPER FUNCTIONS & ARCHITECT CLASS
  // ─────────────────────────────────────────────────────────────
  describe('5. Helper Functions and DashboardArchitect Class', () => {
    test('getCardDimensionsAndArchetype maps standard display types to dimensions and archetypes', () => {
      expect(getCardDimensionsAndArchetype('scalar')).toEqual({ size_x: 6, size_y: 4, archetype: CARD_ARCHETYPES.KPI });
      expect(getCardDimensionsAndArchetype('line')).toEqual({ size_x: 12, size_y: 8, archetype: CARD_ARCHETYPES.TREND });
      expect(getCardDimensionsAndArchetype('pie')).toEqual({ size_x: 12, size_y: 6, archetype: CARD_ARCHETYPES.BREAKDOWN });
      expect(getCardDimensionsAndArchetype('table')).toEqual({ size_x: 24, size_y: 8, archetype: CARD_ARCHETYPES.TABLE });
      expect(getCardDimensionsAndArchetype('unknown_widget')).toEqual({ size_x: 12, size_y: 6, archetype: CARD_ARCHETYPES.OTHER });
    });

    test('getDefaultVisualizationSettings returns appropriate config for each display type', () => {
      expect(getDefaultVisualizationSettings('scalar')).toEqual({ 'scalar.decimals': 0 });
      expect(getDefaultVisualizationSettings('line')).toEqual({ 'graph.show_values': true, 'graph.x_axis.scale': 'timeseries' });
      expect(getDefaultVisualizationSettings('pie')).toEqual({ 'pie.show_legend': true, 'pie.percent_visibility': 'inside' });
      expect(getDefaultVisualizationSettings('table')).toEqual({});
    });

    test('DashboardArchitect class provides wrapper methods for layout, mappings, and build', async () => {
      const mockClient = {
        createQuestion: jest.fn().mockResolvedValue({ id: 101, name: 'Card 1' }),
        createDashboard: jest.fn().mockResolvedValue({ id: 202, name: 'Dash 1' }),
        addCardToDashboard: jest.fn().mockResolvedValue({ id: 303 }),
      };

      const architect = new DashboardArchitect({ metabaseClient: mockClient });

      const positions = architect.calculateLayout([
        { display: 'scalar' },
        { display: 'scalar' },
        { display: 'scalar' },
        { display: 'scalar' },
      ]);
      expect(positions).toHaveLength(4);

      const mappings = architect.generateMappings(
        [{ sql: 'SELECT * FROM t WHERE {{status}}' }],
        [{ name: 'Status', slug: 'status' }]
      );
      expect(mappings[0]).toHaveLength(1);

      const buildRes = await architect.buildDashboard({
        name: 'Class Dash',
        databaseId: 1,
        cards: [
          { name: 'C1', sql: 'SELECT 1', display: 'scalar' },
          { name: 'C2', sql: 'SELECT 2', display: 'scalar' },
          { name: 'C3', sql: 'SELECT 3', display: 'line' },
          { name: 'C4', sql: 'SELECT 4', display: 'table' },
        ],
      });
      expect(buildRes.dashboard_id).toBe(202);
    });
  });
});
