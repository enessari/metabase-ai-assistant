/**
 * tests/unit/challenger-m4-2-adversarial.test.js
 * Adversarial Empirical Test Suite by Challenger M4-2
 * 
 * Tests:
 * 1. Template tag parsing (variables, spaces, multi-variables, missing parameters)
 * 2. Visual metadata formatting (currency prefixes/suffixes, percent scaling, hex colors, 10 themes)
 * 3. Read-only mode protection (WRITE_TOOLS router gating & handler dry-run branching)
 * 4. Mock client lifecycle (atomic creation, parameter mapping reference integrity, zero missing references)
 */

import { jest } from '@jest/globals';
import { DbtDashboardBuilder, THEME_PALETTES } from '../../src/dbt/dbt-dashboard-builder.js';
import {
  calculate24ColGridPositions,
  validateNoCollisions,
  generateFilterMappings,
  GRID_WIDTH,
} from '../../src/analytics/dashboard-architect.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';
import { WRITE_TOOLS, createToolHandler, isReadOnlyMode } from '../../src/mcp/tool-router.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';
import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';

describe('Challenger M4-2 Adversarial Verification Suite', () => {
  let mockClient;
  let cardIdGen = 3000;
  let dashIdGen = 700;

  beforeEach(() => {
    cardIdGen = 3000;
    dashIdGen = 700;

    mockClient = {
      questions: [],
      models: [],
      dashboards: [],
      attachments: [],

      async createQuestion(payload) {
        const card = { id: ++cardIdGen, ...payload };
        this.questions.push(card);
        return card;
      },

      async createModel(payload) {
        const model = { id: ++cardIdGen, ...payload, type: 'model' };
        this.models.push(model);
        return model;
      },

      async createDashboard(payload) {
        const dash = { id: ++dashIdGen, ...payload };
        this.dashboards.push(dash);
        return dash;
      },

      async addCardToDashboard(dashId, cardId, options) {
        const item = { dashId, cardId, ...options };
        this.attachments.push(item);
        return item;
      },
    };
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 1. TEMPLATE TAG REGEX PARSING & FILTER BINDINGS
  // ══════════════════════════════════════════════════════════════════════════
  describe('1. Template Tag Regex Parsing & Filter Bindings', () => {
    const builder = new DbtDashboardBuilder(null);

    test('ADV-1.1: Standard template tags {{date_range}} and {{country}}', () => {
      const sql = 'SELECT * FROM marts_orders WHERE 1=1 [[AND {{date_range}}]] [[AND country = {{country}}]]';
      const filters = [
        { name: 'Date Range', slug: 'date_range', type: 'date/all-options', target_variable: 'date_range' },
        { name: 'Country', slug: 'country', type: 'category', target_variable: 'country' },
      ];

      const tags = builder.buildTemplateTags(sql, filters);
      expect(tags.date_range).toBeDefined();
      expect(tags.date_range.type).toBe('date');
      expect(tags.date_range['display-name']).toBe('Date Range');

      expect(tags.country).toBeDefined();
      expect(tags.country.type).toBe('dimension');
      expect(tags.country['display-name']).toBe('Country');

      const mappings = builder.generateFilterMappings([{ sql }], filters);
      expect(mappings[0]).toHaveLength(2);
      expect(mappings[0][0].parameter_id).toBe('param_date_range');
      expect(mappings[0][0].target).toEqual(['variable', ['template-tag', 'date_range']]);
      expect(mappings[0][1].parameter_id).toBe('param_country');
      expect(mappings[0][1].target).toEqual(['variable', ['template-tag', 'country']]);
    });

    test('ADV-1.2: Multi-variable queries with 6 template tags across mixed data types', () => {
      const sql = `
        SELECT 
          region, 
          status, 
          SUM(revenue) AS total_rev
        FROM marts_orders
        WHERE 1=1
          [[AND order_date >= {{start_date}}]]
          [[AND order_date <= {{end_date}}]]
          [[AND region = {{region}}]]
          [[AND status = {{status}}]]
          [[AND user_id = {{user_id}}]]
          [[AND min_revenue >= {{min_rev}}]]
        GROUP BY 1, 2
      `;

      const filters = [
        { name: 'Start Date', slug: 'start_date', type: 'date/single' },
        { name: 'End Date', slug: 'end_date', type: 'date/single' },
        { name: 'Region', slug: 'region', type: 'category' },
        { name: 'Status', slug: 'status', type: 'category' },
        { name: 'User ID', slug: 'user_id', type: 'number/=' },
        { name: 'Min Revenue', slug: 'min_rev', target_variable: 'min_rev', type: 'number/=' },
      ];

      const tags = builder.buildTemplateTags(sql, filters);
      expect(Object.keys(tags)).toHaveLength(6);
      expect(tags.start_date.type).toBe('date');
      expect(tags.end_date.type).toBe('date');
      expect(tags.region.type).toBe('dimension');
      expect(tags.status.type).toBe('dimension');
      expect(tags.user_id.type).toBe('number');
      expect(tags.min_rev.type).toBe('number');

      const mappings = builder.generateFilterMappings([{ sql }], filters);
      expect(mappings[0]).toHaveLength(6);
    });

    test('ADV-1.3: Handles missing parameters (tag in SQL not in filters, and filter not in SQL)', () => {
      const sql = 'SELECT * FROM marts_orders WHERE 1=1 [[AND {{unmatched_tag}}]] [[AND {{date_range}}]]';
      const filters = [
        { name: 'Date Range', slug: 'date_range', type: 'date/all-options' },
        { name: 'Orphan Filter', slug: 'orphan_filter', type: 'category' },
      ];

      // Unmatched tag should still get a default tag type in template-tags
      const tags = builder.buildTemplateTags(sql, filters);
      expect(tags.unmatched_tag).toBeDefined();
      expect(tags.unmatched_tag.type).toBe('text');
      expect(tags.date_range.type).toBe('date');

      // Only date_range should be mapped in filter mappings
      const mappings = builder.generateFilterMappings([{ sql }], filters);
      expect(mappings[0]).toHaveLength(1);
      expect(mappings[0][0].parameter_id).toBe('param_date_range');
      expect(mappings[0][0].target).toEqual(['variable', ['template-tag', 'date_range']]);
    });

    test('ADV-1.4: Handles edge case SQL inputs (null, undefined, non-string, empty string, malformed)', () => {
      expect(builder.buildTemplateTags(null, [])).toEqual({});
      expect(builder.buildTemplateTags(undefined, [])).toEqual({});
      expect(builder.buildTemplateTags('', [])).toEqual({});
      expect(builder.buildTemplateTags(12345, [])).toEqual({});
      expect(builder.buildTemplateTags('SELECT * FROM tbl WHERE col = 1', [])).toEqual({});
    });

    test('ADV-1.5: Name-based fuzzy filter resolution when slug differs from tag name', () => {
      const sql = 'SELECT * FROM marts_orders WHERE 1=1 [[AND {{customer_segment}}]]';
      const filters = [
        { name: 'Customer Segment', slug: 'cust_seg_filter', type: 'category' },
      ];

      const tags = builder.buildTemplateTags(sql, filters);
      expect(tags.customer_segment).toBeDefined();
      expect(tags.customer_segment.type).toBe('dimension');

      const mappings = builder.generateFilterMappings([{ sql }], filters);
      expect(mappings[0]).toHaveLength(1);
      expect(mappings[0][0].parameter_id).toBe('param_cust_seg_filter');
      expect(mappings[0][0].target).toEqual(['variable', ['template-tag', 'customer_segment']]);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 2. LIGHTDASH / METABASE VISUAL METADATA FORMATTING
  // ══════════════════════════════════════════════════════════════════════════
  describe('2. Visual Metadata Formatting (Currency, Percent, Palettes, Themes)', () => {
    const builder = new DbtDashboardBuilder(null);

    test('ADV-2.1: Currency formatting with various currencies and custom prefixes/suffixes', () => {
      // USD
      const usdSettings = builder.formatVisualSettings(
        { display: 'scalar' },
        'executive',
        { formatting: { currency: 'USD', formatType: 'currency', decimals: 2 } }
      );
      expect(usdSettings['scalar.currency']).toBe('USD');
      expect(usdSettings['scalar.prefix']).toBe('$');
      expect(usdSettings['scalar.decimals']).toBe(2);

      // EUR with default € prefix
      const eurSettings = builder.formatVisualSettings(
        { display: 'scalar' },
        'executive',
        { formatting: { currency: 'EUR', formatType: 'currency', decimals: 0 } }
      );
      expect(eurSettings['scalar.currency']).toBe('EUR');
      expect(eurSettings['scalar.prefix']).toBe('€');
      expect(eurSettings['scalar.decimals']).toBe(0);

      // Custom GBP prefix £
      const gbpSettings = builder.formatVisualSettings(
        { display: 'scalar' },
        'executive',
        { formatting: { currency: 'GBP', prefix: '£', formatType: 'currency' } }
      );
      expect(gbpSettings['scalar.currency']).toBe('GBP');
      expect(gbpSettings['scalar.prefix']).toBe('£');
    });

    test('ADV-2.2: Percentage formatting with custom decimals and suffix', () => {
      const pctSettings = builder.formatVisualSettings(
        { display: 'scalar' },
        'modern_emerald',
        { formatting: { formatType: 'percent', decimals: 3 } }
      );
      expect(pctSettings['scalar.suffix']).toBe('%');
      expect(pctSettings['scalar.decimals']).toBe(3);
    });

    test('ADV-2.3: Custom hex color injected into palette at index 0', () => {
      const customHex = '#FF1493';
      const settings = builder.formatVisualSettings(
        { display: 'bar' },
        'indigo_violet',
        { color: customHex }
      );

      expect(settings['graph.colors'][0]).toBe(customHex);
      expect(settings['graph.colors']).toContain('#6366F1');
      expect(settings['graph.colors'].length).toBeGreaterThanOrEqual(THEME_PALETTES.indigo_violet.length);
    });

    test('ADV-2.4: Verification of all 10 theme palettes integrity', () => {
      const expectedThemes = [
        'executive', 'modern_emerald', 'indigo_violet', 'amber_warm',
        'slate_minimal', 'financial', 'operational', 'marketing',
        'dark', 'custom'
      ];

      for (const t of expectedThemes) {
        expect(THEME_PALETTES[t]).toBeDefined();
        expect(Array.isArray(THEME_PALETTES[t])).toBe(true);
        expect(THEME_PALETTES[t].length).toBeGreaterThanOrEqual(8);
        for (const hex of THEME_PALETTES[t]) {
          expect(hex).toMatch(/^#[0-9A-Fa-f]{6}$/);
        }
      }
    });

    test('ADV-2.5: Chart-specific visual defaults (Line, Area, Bar, Row, Pie, Donut, Table)', () => {
      const line = builder.formatVisualSettings({ display: 'line' });
      expect(line['graph.show_values']).toBe(true);
      expect(line['graph.x_axis.scale']).toBe('timeseries');

      const area = builder.formatVisualSettings({ display: 'area' });
      expect(area['graph.show_values']).toBe(true);
      expect(area['graph.x_axis.scale']).toBe('timeseries');

      const bar = builder.formatVisualSettings({ display: 'bar' });
      expect(bar['graph.show_values']).toBe(true);

      const row = builder.formatVisualSettings({ display: 'row' });
      expect(row['graph.show_values']).toBe(true);

      const pie = builder.formatVisualSettings({ display: 'pie' });
      expect(pie['pie.show_legend']).toBe(true);
      expect(pie['pie.percent_visibility']).toBe('inside');

      const donut = builder.formatVisualSettings({ display: 'donut' });
      expect(donut['pie.show_legend']).toBe(true);
      expect(donut['pie.percent_visibility']).toBe('inside');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 3. READ-ONLY PROTECTION (ROUTER GATING & HANDLER BRANCHING)
  // ══════════════════════════════════════════════════════════════════════════
  describe('3. Read-Only Protection & Security Gating', () => {
    test('ADV-3.1: WRITE_TOOLS contains dbt_build_dashboard_from_yaml', () => {
      expect(WRITE_TOOLS.has('dbt_build_dashboard_from_yaml')).toBe(true);
    });

    test('ADV-3.2: createToolHandler router throws McpError(InvalidRequest) when METABASE_READ_ONLY_MODE=true', async () => {
      const origEnv = process.env.METABASE_READ_ONLY_MODE;
      try {
        process.env.METABASE_READ_ONLY_MODE = 'true';

        const mockHandlerFn = jest.fn();
        const routes = {
          dbt_build_dashboard_from_yaml: mockHandlerFn,
        };

        const router = createToolHandler(routes);

        await expect(
          router({
            params: {
              name: 'dbt_build_dashboard_from_yaml',
              arguments: { yaml_content: { name: 'Test' } },
            },
          })
        ).rejects.toThrow(McpError);

        expect(mockHandlerFn).not.toHaveBeenCalled();
      } finally {
        process.env.METABASE_READ_ONLY_MODE = origEnv;
      }
    });

    test('ADV-3.3: DbtSemanticHandler blocks live mutation when METABASE_READ_ONLY_MODE=true and dry_run is false', async () => {
      const origEnv = process.env.METABASE_READ_ONLY_MODE;
      try {
        process.env.METABASE_READ_ONLY_MODE = 'true';

        const handler = new DbtSemanticHandler(mockClient);
        const response = await handler.handleDbtBuildDashboardFromYaml({
          yaml_content: {
            name: 'Blocked Dashboard',
            cards: [
              { name: 'C1', display: 'scalar' },
              { name: 'C2', display: 'line' },
              { name: 'C3', display: 'bar' },
              { name: 'C4', display: 'table' },
            ],
          },
          dry_run: false,
        });

        expect(response.isError).toBe(true);
        expect(response.content[0].text).toContain('Read-Only Mode Active');
        expect(response.content[0].text).toContain('dbt_build_dashboard_from_yaml');
        expect(mockClient.dashboards).toHaveLength(0);
        expect(mockClient.questions).toHaveLength(0);
      } finally {
        process.env.METABASE_READ_ONLY_MODE = origEnv;
      }
    });

    test('ADV-3.4: DbtSemanticHandler permits dry_run: true in read-only mode and returns structured response', async () => {
      const origEnv = process.env.METABASE_READ_ONLY_MODE;
      try {
        process.env.METABASE_READ_ONLY_MODE = 'true';

        const handler = new DbtSemanticHandler(mockClient);
        const response = await handler.handleDbtBuildDashboardFromYaml({
          yaml_content: {
            name: 'Dry Run Permitted Dashboard',
            cards: [
              { name: 'C1', display: 'scalar' },
              { name: 'C2', display: 'line' },
              { name: 'C3', display: 'bar' },
              { name: 'C4', display: 'table' },
            ],
          },
          dry_run: true,
        });

        expect(response.isError).toBeUndefined();
        expect(response.content[0].text).toContain('LIGHTDASH CODE-AS-BI DASHBOARD BUILT SUCCESSFULLY');
        expect(response.structuredContent).toBeDefined();
        expect(response.structuredContent._provenance.dry_run).toBe(true);
        expect(mockClient.dashboards).toHaveLength(0);
        expect(mockClient.questions).toHaveLength(0);
      } finally {
        process.env.METABASE_READ_ONLY_MODE = origEnv;
      }
    });

    test('ADV-3.5: Router permits execution when METABASE_READ_ONLY_MODE=false', async () => {
      const origEnv = process.env.METABASE_READ_ONLY_MODE;
      try {
        process.env.METABASE_READ_ONLY_MODE = 'false';

        const mockHandlerFn = jest.fn().mockResolvedValue({
          content: [{ type: 'text', text: 'success' }],
        });
        const routes = {
          dbt_build_dashboard_from_yaml: mockHandlerFn,
        };

        const router = createToolHandler(routes);
        const res = await router({
          params: {
            name: 'dbt_build_dashboard_from_yaml',
            arguments: { yaml_content: { name: 'Test' } },
          },
        });

        expect(res.content[0].text).toBe('success');
        expect(mockHandlerFn).toHaveBeenCalledTimes(1);
      } finally {
        process.env.METABASE_READ_ONLY_MODE = origEnv;
      }
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 4. MOCK CLIENT LIFECYCLE & REFERENCE INTEGRITY
  // ══════════════════════════════════════════════════════════════════════════
  describe('4. Mock Client Lifecycle & Reference Integrity', () => {
    test('ADV-4.1: Atomic lifecycle with 0 missing references across questions, models, dashboard and cards', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const modelSpec = {
        name: 'marts_revenue',
        tier: 'marts_fact',
        description: 'Revenue fact table',
        meta: {
          metabase: {
            filters: [
              { name: 'Date Range', slug: 'date_range', type: 'date/all-options', target_variable: 'date_range' },
              { name: 'Channel', slug: 'channel', type: 'category', target_variable: 'channel' },
            ],
          },
        },
        columns: {
          order_id: { name: 'order_id', dataType: 'integer' },
          revenue: { name: 'revenue', dataType: 'numeric' },
          order_date: { name: 'order_date', dataType: 'timestamp' },
          channel: { name: 'channel', dataType: 'varchar' },
        },
      };

      const result = await builder.buildDashboardFromYaml({
        model_name: 'marts_revenue',
        yaml_content: {
          models: [modelSpec],
        },
        theme: 'financial',
        include_models: true,
        create_models: true,
      });

      // 1. Verify dashboard entity
      expect(result.dashboard_id).toBeDefined();
      expect(mockClient.dashboards).toHaveLength(1);
      const dash = mockClient.dashboards[0];
      expect(dash.id).toBe(result.dashboard_id);
      expect(dash.parameters).toHaveLength(2);
      const paramIds = dash.parameters.map(p => p.id);
      expect(paramIds).toContain('param_date_range');
      expect(paramIds).toContain('param_channel');

      // 2. Verify questions / models created
      expect(mockClient.models.length + mockClient.questions.length).toBeGreaterThanOrEqual(4);
      const allCreatedIds = [
        ...mockClient.questions.map(q => q.id),
        ...mockClient.models.map(m => m.id),
      ];

      // 3. Verify card attachments to dashboard
      expect(mockClient.attachments).toHaveLength(result.cards.length);
      for (const att of mockClient.attachments) {
        expect(att.dashId).toBe(result.dashboard_id);
        expect(allCreatedIds).toContain(att.cardId);

        // 4. Verify parameter mappings point to valid dashboard parameters
        for (const mapping of (att.parameter_mappings || [])) {
          expect(mapping.card_id).toBe(att.cardId);
          expect(paramIds).toContain(mapping.parameter_id);
        }
      }

      // 5. Verify collision-free grid positions
      const positions = result.cards.map(c => c.position);
      expect(validateNoCollisions(positions)).toBe(true);
      for (const pos of positions) {
        expect(pos.col + pos.size_x).toBeLessThanOrEqual(GRID_WIDTH);
        expect(pos.row).toBeGreaterThanOrEqual(0);
        expect(pos.col).toBeGreaterThanOrEqual(0);
        expect(pos.size_x).toBeGreaterThanOrEqual(1);
        expect(pos.size_y).toBeGreaterThanOrEqual(1);
      }
    });

    test('ADV-4.2: Handles createModel failure gracefully by falling back to standard question without breaking dashboard creation', async () => {
      const failModelClient = {
        ...mockClient,
        async createModel() {
          throw new Error('Metabase v48 does not support /api/card model type');
        },
      };

      const builder = new DbtDashboardBuilder(failModelClient);

      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'Fallback Lifecycle Test',
          cards: [
            { name: 'KPI 1', display: 'scalar', sql: 'SELECT 1' },
            { name: 'Trend 1', display: 'line', sql: 'SELECT 1' },
            { name: 'Breakdown 1', display: 'bar', sql: 'SELECT 1' },
            { name: 'Model Mart', display: 'table', type: 'model', is_model: true, sql: 'SELECT 1' },
          ],
        },
      });

      expect(result.card_count).toBe(4);
      expect(failModelClient.dashboards).toHaveLength(1);
      expect(failModelClient.questions).toHaveLength(4);
      expect(failModelClient.attachments).toHaveLength(4);
      expect(result.cards.every(c => c.card_id !== undefined)).toBe(true);
    });

    test('ADV-4.3: High-load stress test: 20 cards bin-packing, parameter binding, and attachment integrity', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const cards = [];
      for (let i = 1; i <= 8; i++) {
        cards.push({ name: `KPI ${i}`, display: 'scalar', sql: `SELECT ${i} AS kpi [[WHERE {{date_range}}]]` });
      }
      for (let i = 1; i <= 4; i++) {
        cards.push({ name: `Trend ${i}`, display: 'line', sql: `SELECT ${i} AS trend [[WHERE {{date_range}}]]` });
      }
      for (let i = 1; i <= 6; i++) {
        cards.push({ name: `Breakdown ${i}`, display: 'bar', sql: `SELECT ${i} AS bd [[WHERE {{date_range}}]]` });
      }
      for (let i = 1; i <= 2; i++) {
        cards.push({ name: `Table ${i}`, display: 'table', sql: `SELECT ${i} AS tbl [[WHERE {{date_range}}]]` });
      }

      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'Stress 20 Cards',
          cards,
          filters: [
            { name: 'Date Range', slug: 'date_range', type: 'date/all-options' },
          ],
        },
      });

      expect(result.card_count).toBe(20);
      expect(mockClient.questions).toHaveLength(20);
      expect(mockClient.attachments).toHaveLength(20);

      const positions = result.cards.map(c => c.position);
      expect(validateNoCollisions(positions)).toBe(true);

      // Verify every card was attached with matching card_id and valid parameter mappings
      for (let i = 0; i < 20; i++) {
        const att = mockClient.attachments[i];
        expect(att.dashId).toBe(result.dashboard_id);
        expect(att.cardId).toBe(result.cards[i].card_id);
        expect(att.parameter_mappings).toHaveLength(1);
        expect(att.parameter_mappings[0].parameter_id).toBe('param_date_range');
      }
    });
  });
});
