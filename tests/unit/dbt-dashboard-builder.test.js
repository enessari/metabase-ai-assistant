/**
 * tests/unit/dbt-dashboard-builder.test.js
 * Unit Test Suite for Lightdash Code-as-BI Dashboard Builder (DbtDashboardBuilder),
 * 24-Column Grid Architect, Filter Mappings, Metabase Models (v50+), and MCP Handler.
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import {
  DbtDashboardBuilder,
  THEME_PALETTES,
} from '../../src/dbt/dbt-dashboard-builder.js';
import {
  calculate24ColGridPositions,
  validateNoCollisions,
  generateFilterMappings,
  GRID_WIDTH,
  CARD_ARCHETYPES,
} from '../../src/analytics/dashboard-architect.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';
import { WRITE_TOOLS } from '../../src/mcp/tool-router.js';

describe('DbtDashboardBuilder Unit Test Suite (M4)', () => {
  let tempDir;
  let mockClient;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-dash-test-'));

    // Create a mock Metabase API client
    let cardIdCounter = 2000;
    let dashIdCounter = 500;

    mockClient = {
      cardsCreated: [],
      modelsCreated: [],
      dashboardsCreated: [],
      cardsAttached: [],

      async createQuestion(payload) {
        const id = ++cardIdCounter;
        const created = { id, ...payload };
        this.cardsCreated.push(created);
        return created;
      },

      async createModel(payload) {
        const id = ++cardIdCounter;
        const created = { id, ...payload, type: 'model' };
        this.modelsCreated.push(created);
        return created;
      },

      async createDashboard(payload) {
        const id = ++dashIdCounter;
        const created = { id, ...payload };
        this.dashboardsCreated.push(created);
        return created;
      },

      async addCardToDashboard(dashId, cardId, options) {
        const attached = { dashId, cardId, ...options };
        this.cardsAttached.push(attached);
        return attached;
      },
    };
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 1: 24-Column Grid Layout & Collision Prevention
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 1: 24-Column Grid Layout & Collision Prevention', () => {
    const builder = new DbtDashboardBuilder(null);

    test('TC-1.1: Multi-tier placement: 4 KPIs (row 0), 2 Trends (row 4), 2 Breakdowns (row 12), 1 Table (row 18)', () => {
      const cards = [
        { name: 'KPI 1', display: 'scalar' },
        { name: 'KPI 2', display: 'number' },
        { name: 'KPI 3', display: 'gauge' },
        { name: 'KPI 4', display: 'smartscalar' },
        { name: 'Trend 1', display: 'line' },
        { name: 'Trend 2', display: 'area' },
        { name: 'Breakdown 1', display: 'pie' },
        { name: 'Breakdown 2', display: 'donut' },
        { name: 'Detail Log', display: 'table' },
      ];

      const positions = builder.calculateGridCoordinates(cards);

      expect(positions).toHaveLength(9);
      expect(positions[0]).toEqual({ row: 0, col: 0, size_x: 6, size_y: 4 });
      expect(positions[1]).toEqual({ row: 0, col: 6, size_x: 6, size_y: 4 });
      expect(positions[2]).toEqual({ row: 0, col: 12, size_x: 6, size_y: 4 });
      expect(positions[3]).toEqual({ row: 0, col: 18, size_x: 6, size_y: 4 });

      expect(positions[4]).toEqual({ row: 4, col: 0, size_x: 12, size_y: 8 });
      expect(positions[5]).toEqual({ row: 4, col: 12, size_x: 12, size_y: 8 });

      expect(positions[6]).toEqual({ row: 12, col: 0, size_x: 12, size_y: 6 });
      expect(positions[7]).toEqual({ row: 12, col: 12, size_x: 12, size_y: 6 });

      expect(positions[8]).toEqual({ row: 18, col: 0, size_x: 24, size_y: 8 });

      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('TC-1.2: Validates strict zero collision via validateNoCollisions (all col + size_x <= 24)', () => {
      const cards = [
        { name: 'Revenue', display: 'scalar' },
        { name: 'Orders', display: 'scalar' },
        { name: 'AOV', display: 'scalar' },
        { name: 'Users', display: 'scalar' },
      ];

      const positions = builder.calculateGridCoordinates(cards);
      expect(validateNoCollisions(positions)).toBe(true);

      for (const pos of positions) {
        expect(pos.col + pos.size_x).toBeLessThanOrEqual(GRID_WIDTH);
        expect(pos.size_x).toBeGreaterThanOrEqual(1);
        expect(pos.size_y).toBeGreaterThanOrEqual(1);
      }
    });

    test('TC-1.3: Odd card counts and row wrapping (6 KPIs wrap onto row 4)', () => {
      const cards = [
        { name: 'K1', display: 'scalar' },
        { name: 'K2', display: 'scalar' },
        { name: 'K3', display: 'scalar' },
        { name: 'K4', display: 'scalar' },
        { name: 'K5', display: 'scalar' },
        { name: 'K6', display: 'scalar' },
      ];

      const positions = builder.calculateGridCoordinates(cards);
      expect(positions).toHaveLength(6);
      expect(positions[4]).toEqual({ row: 4, col: 0, size_x: 6, size_y: 4 });
      expect(positions[5]).toEqual({ row: 4, col: 6, size_x: 6, size_y: 4 });
      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('TC-1.4: Tri-breakdown layout (3 breakdown charts placed at 8 width side-by-side)', () => {
      const cards = [
        { name: 'BD1', display: 'pie' },
        { name: 'BD2', display: 'donut' },
        { name: 'BD3', display: 'row' },
      ];

      const positions = builder.calculateGridCoordinates(cards);
      expect(positions).toHaveLength(3);
      expect(positions[0]).toEqual({ row: 0, col: 0, size_x: 8, size_y: 6 });
      expect(positions[1]).toEqual({ row: 0, col: 8, size_x: 8, size_y: 6 });
      expect(positions[2]).toEqual({ row: 0, col: 16, size_x: 8, size_y: 6 });
      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('TC-1.5: Explicit coordinate validation and fallback when collision detected in custom coordinates', () => {
      const collidingCards = [
        { name: 'C1', display: 'table', row: 0, col: 0, size_x: 18, size_y: 6 },
        { name: 'C2', display: 'table', row: 0, col: 6, size_x: 18, size_y: 6 }, // Collides!
      ];

      const positions = builder.calculateGridCoordinates(collidingCards);
      expect(positions).toHaveLength(2);
      expect(validateNoCollisions(positions)).toBe(true);
      expect(positions[0].row).not.toBe(positions[1].row);
    });

    test('TC-1.6: Boundary enforcement (size_x clamped to 24 and col + size_x <= 24)', () => {
      const cards = [
        { name: 'Huge Card', display: 'table', size_x: 30, size_y: 10 },
      ];

      const positions = builder.calculateGridCoordinates(cards);
      expect(positions[0].size_x).toBe(24);
      expect(positions[0].col + positions[0].size_x).toBeLessThanOrEqual(24);
      expect(validateNoCollisions(positions)).toBe(true);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 2: Parameter & Filter Mapping
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 2: Parameter & Filter Mapping', () => {
    const builder = new DbtDashboardBuilder(null);

    test('TC-2.1: Binds {{date_range}} template tags to dashboard date filter', () => {
      const cards = [
        { name: 'Revenue', sql: 'SELECT SUM(amount) FROM orders WHERE 1=1 [[AND {{date_range}}]]' },
        { name: 'Orders', sql: 'SELECT COUNT(*) FROM orders WHERE 1=1 [[AND {{date_range}}]]' },
      ];

      const filters = [
        { name: 'Date Range', slug: 'date_range', type: 'date/all-options', target_variable: 'date_range' },
      ];

      const mappings = builder.generateFilterMappings(cards, filters);
      expect(mappings).toHaveLength(2);
      expect(mappings[0]).toHaveLength(1);
      expect(mappings[0][0].parameter_id).toBe('param_date_range');
      expect(mappings[0][0].target).toEqual(['variable', ['template-tag', 'date_range']]);
    });

    test('TC-2.2: Binds multiple template tags (category, status, region) to respective filters', () => {
      const cards = [
        {
          name: 'Multi Filter Card',
          sql: 'SELECT * FROM orders WHERE 1=1 [[AND {{category}}]] [[AND {{status}}]] [[AND {{region}}]]',
        },
      ];

      const filters = [
        { name: 'Category', slug: 'category', type: 'category' },
        { name: 'Status', slug: 'status', type: 'category' },
        { name: 'Region', slug: 'region', type: 'category' },
      ];

      const mappings = builder.generateFilterMappings(cards, filters);
      expect(mappings[0]).toHaveLength(3);
      expect(mappings[0].map(m => m.parameter_id)).toEqual(['param_category', 'param_status', 'param_region']);
    });

    test('TC-2.3: Binds dimension fields to dashboard parameter mappings', () => {
      const cards = [
        { name: 'GUI Card', field_id: 42, parameter_name: 'category' },
      ];

      const filters = [
        { name: 'Category', slug: 'category', type: 'category', field_id: 42 },
      ];

      const mappings = builder.generateFilterMappings(cards, filters);
      expect(mappings[0]).toHaveLength(1);
      expect(mappings[0][0].parameter_id).toBe('param_category');
    });

    test('TC-2.4: Handles empty filters or unparameterized queries gracefully', () => {
      const cards = [
        { name: 'Plain Card', sql: 'SELECT * FROM orders' },
      ];

      const mappingsWithEmptyFilters = builder.generateFilterMappings(cards, []);
      expect(mappingsWithEmptyFilters[0]).toEqual([]);

      const mappingsWithNoCards = builder.generateFilterMappings([], [{ name: 'Date', slug: 'date' }]);
      expect(mappingsWithNoCards).toEqual([]);
    });

    test('TC-2.5: buildTemplateTags correctly generates template-tags metadata object', () => {
      const sql = 'SELECT * FROM orders WHERE 1=1 [[AND {{order_date}}]] [[AND {{user_id}}]]';
      const filters = [
        { name: 'Order Date', slug: 'order_date', type: 'date/range' },
        { name: 'User ID', slug: 'user_id', type: 'number/=' },
      ];

      const tags = builder.buildTemplateTags(sql, filters);
      expect(tags.order_date).toBeDefined();
      expect(tags.order_date.type).toBe('date');
      expect(tags.order_date['display-name']).toBe('Order Date');

      expect(tags.user_id).toBeDefined();
      expect(tags.user_id.type).toBe('number');
      expect(tags.user_id['display-name']).toBe('User Id');
    });

    test('TC-2.6: Normalizes filter slugs, IDs, and default values', () => {
      const spec = builder.parseYamlDashboardSpec({
        name: 'Normalized Filter Test',
        filters: [
          { name: 'Region Filter', default: 'EMEA' },
        ],
        cards: [
          { name: 'C1', display: 'scalar' },
          { name: 'C2', display: 'line' },
          { name: 'C3', display: 'bar' },
          { name: 'C4', display: 'table' },
        ],
      });

      expect(spec.filters).toHaveLength(1);
      expect(spec.filters[0].name).toBe('Region Filter');
      expect(spec.filters[0].default).toBe('EMEA');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 3: Metabase Model Cards (v50+)
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 3: Metabase Model Cards (v50+)', () => {
    test('TC-3.1: Creates Metabase Model cards (type: model, is_model: true) for marts_fact and marts_dim', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const modelSpec = {
        name: 'fct_orders',
        tier: 'marts_fact',
        description: 'Orders fact mart table',
        columns: {
          order_id: { name: 'order_id', dataType: 'integer' },
          amount: { name: 'amount', dataType: 'numeric' },
          order_date: { name: 'order_date', dataType: 'timestamp' },
          status: { name: 'status', dataType: 'varchar' },
        },
      };

      const result = await builder.buildDashboardFromYaml({
        model_name: 'fct_orders',
        yaml_content: { models: [modelSpec] },
        include_models: true,
      });

      expect(result.card_count).toBeGreaterThanOrEqual(4);
      expect(mockClient.modelsCreated.length).toBeGreaterThanOrEqual(1);
      expect(mockClient.modelsCreated[0].type).toBe('model');
      expect(result.grid_summary.model_count).toBeGreaterThanOrEqual(1);
    });

    test('TC-3.2: Places Model cards at full width (24x8) in detail tier', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const cards = [
        { name: 'KPI 1', display: 'scalar', sql: 'SELECT 1' },
        { name: 'Trend 1', display: 'line', sql: 'SELECT 1' },
        { name: 'Breakdown 1', display: 'bar', sql: 'SELECT 1' },
        { name: 'Curated Mart Model', display: 'table', type: 'model', is_model: true, sql: 'SELECT * FROM fct_orders' },
      ];

      const result = await builder.buildDashboardFromYaml({
        yaml_content: { name: 'Model Placement Test', cards },
      });

      const modelCard = result.cards.find(c => c.is_model);
      expect(modelCard).toBeDefined();
      expect(modelCard.position.size_x).toBe(24);
      expect(modelCard.position.size_y).toBe(8);
      expect(modelCard.position.row).toBeGreaterThanOrEqual(12);
    });

    test('TC-3.3: Model creation fallback when createModel throws error', async () => {
      const throwingClient = {
        ...mockClient,
        async createModel() {
          throw new Error('Endpoint /api/card with type: model not supported on this version');
        },
      };

      const builder = new DbtDashboardBuilder(throwingClient);
      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'Fallback Test',
          cards: [
            { name: 'K1', display: 'scalar', sql: 'SELECT 1' },
            { name: 'T1', display: 'line', sql: 'SELECT 1' },
            { name: 'B1', display: 'bar', sql: 'SELECT 1' },
            { name: 'M1', display: 'table', type: 'model', is_model: true, sql: 'SELECT 1' },
          ],
        },
      });

      expect(result.card_count).toBe(4);
      expect(throwingClient.cardsCreated.length).toBe(4);
    });

    test('TC-3.4: Ingests model column schemas, descriptions, and types for model card metadata', async () => {
      const builder = new DbtDashboardBuilder(null);
      const enriched = await builder.resolveAndEnrichCards(
        {
          name: 'Schema Test',
          resolvedModel: {
            name: 'marts_customers',
            columns: {
              customer_id: { name: 'customer_id', dataType: 'integer' },
              lifetime_value: { name: 'lifetime_value', dataType: 'numeric' },
              signup_date: { name: 'signup_date', dataType: 'date' },
              segment: { name: 'segment', dataType: 'varchar' },
            },
          },
        },
        1,
        'executive',
        { include_models: true }
      );

      expect(enriched.length).toBeGreaterThanOrEqual(4);
      const kpiCard = enriched.find(c => c.display === 'scalar');
      expect(kpiCard.name).toContain('Lifetime Value');
      expect(kpiCard.sql).toContain('lifetime_value');
    });

    test('TC-3.5: Disables model creation when include_models is false', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'No Models Test',
          cards: [
            { name: 'K1', display: 'scalar', sql: 'SELECT 1' },
            { name: 'T1', display: 'line', sql: 'SELECT 1' },
            { name: 'B1', display: 'bar', sql: 'SELECT 1' },
            { name: 'Table 1', display: 'table', sql: 'SELECT 1' },
          ],
        },
        include_models: false,
        create_models: false,
      });

      expect(mockClient.modelsCreated).toHaveLength(0);
      expect(result.cards.every(c => !c.is_model)).toBe(true);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 4: Lightdash & Metabase Visual Metadata Ingestion
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 4: Lightdash & Metabase Visual Metadata Ingestion', () => {
    const builder = new DbtDashboardBuilder(null);

    test('TC-4.1: Maps meta.metabase.formatting (currency USD, prefix $, suffix %, decimals) into visualization_settings', () => {
      const card = { display: 'scalar' };
      const visualMeta = {
        formatting: {
          currency: 'USD',
          formatType: 'currency',
          prefix: '$',
          decimals: 2,
        },
      };

      const settings = builder.formatVisualSettings(card, 'executive', visualMeta);
      expect(settings['scalar.currency']).toBe('USD');
      expect(settings['scalar.prefix']).toBe('$');
      expect(settings['scalar.decimals']).toBe(2);
    });

    test('TC-4.2: Maps meta.lightdash format types (usd, percent, number, round) into visualization_settings', () => {
      const card = { display: 'scalar' };
      const visualMeta = {
        formatting: {
          formatType: 'percent',
          suffix: '%',
          decimals: 1,
        },
      };

      const settings = builder.formatVisualSettings(card, 'executive', visualMeta);
      expect(settings['scalar.suffix']).toBe('%');
      expect(settings['scalar.decimals']).toBe(1);
    });

    test('TC-4.3: Ingests custom color palette from meta.lightdash.colors / meta.metabase.colors', () => {
      const card = { display: 'line' };
      const visualMeta = {
        color: '#FF5733',
      };

      const settings = builder.formatVisualSettings(card, 'executive', visualMeta);
      expect(settings['graph.colors'][0]).toBe('#FF5733');
    });

    test('TC-4.4: Supports all 10 theme palettes', () => {
      const themes = Object.keys(THEME_PALETTES);
      expect(themes).toContain('executive');
      expect(themes).toContain('modern_emerald');
      expect(themes).toContain('indigo_violet');
      expect(themes).toContain('amber_warm');
      expect(themes).toContain('slate_minimal');
      expect(themes).toContain('financial');
      expect(themes).toContain('operational');
      expect(themes).toContain('marketing');
      expect(themes).toContain('dark');
      expect(themes).toContain('custom');

      for (const t of themes) {
        const settings = builder.formatVisualSettings({ display: 'bar' }, t);
        expect(Array.isArray(settings['graph.colors'])).toBe(true);
        expect(settings['graph.colors'].length).toBeGreaterThanOrEqual(4);
      }
    });

    test('TC-4.5: Sets timeseries axis scale and show_values for trend line/area charts', () => {
      const lineSettings = builder.formatVisualSettings({ display: 'line' });
      expect(lineSettings['graph.show_values']).toBe(true);
      expect(lineSettings['graph.x_axis.scale']).toBe('timeseries');

      const areaSettings = builder.formatVisualSettings({ display: 'area' });
      expect(areaSettings['graph.show_values']).toBe(true);
      expect(areaSettings['graph.x_axis.scale']).toBe('timeseries');
    });

    test('TC-4.6: Sets pie legend and percent visibility for pie/donut charts', () => {
      const pieSettings = builder.formatVisualSettings({ display: 'pie' });
      expect(pieSettings['pie.show_legend']).toBe(true);
      expect(pieSettings['pie.percent_visibility']).toBe('inside');

      const donutSettings = builder.formatVisualSettings({ display: 'donut' });
      expect(donutSettings['pie.show_legend']).toBe(true);
      expect(donutSettings['pie.percent_visibility']).toBe('inside');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 5: YAML & Exposures Ingestion
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 5: YAML & Exposures Ingestion', () => {
    const builder = new DbtDashboardBuilder(null);

    test('TC-5.1: Ingests standard dbt exposures.yml with type: dashboard and depends_on', () => {
      const exposureYaml = `
version: 2
exposures:
  - name: executive_kpis
    label: Executive KPI Overview
    type: dashboard
    maturity: high
    description: High-level KPI summary for executive team
    depends_on:
      - ref('fct_orders')
      - ref('dim_customers')
    meta:
      metabase:
        theme: financial
        cards:
          - name: Total Revenue
            display: scalar
            sql: "SELECT SUM(amount) AS rev FROM {{ ref('fct_orders') }}"
          - name: Monthly Revenue Trend
            display: line
            sql: "SELECT DATE_TRUNC('month', order_date), SUM(amount) FROM {{ ref('fct_orders') }} GROUP BY 1"
          - name: Revenue by Region
            display: bar
            sql: "SELECT region, SUM(amount) FROM {{ ref('fct_orders') }} GROUP BY 1"
          - name: Recent Transactions
            display: table
            sql: "SELECT * FROM {{ ref('fct_orders') }} LIMIT 100"
`;

      const spec = builder.parseExposureYaml(exposureYaml);
      expect(spec.name).toBe('Executive KPI Overview');
      expect(spec.slug).toBe('executive_kpis');
      expect(spec.theme).toBe('financial');
      expect(spec.cards).toHaveLength(4);
      expect(spec.depends_on).toEqual(["ref('fct_orders')", "ref('dim_customers')"]);
    });

    test('TC-5.2: Ingests direct dashboard YAML (dashboard: { name, cards, filters })', () => {
      const dashYaml = `
dashboard:
  name: Marketing ROI Dashboard
  description: Multi-channel marketing campaign performance
  theme: marketing
  filters:
    - name: Campaign Date
      slug: campaign_date
      type: date/all-options
  cards:
    - name: Total Ad Spend
      display: scalar
      sql: "SELECT SUM(spend) FROM marts_marketing"
    - name: Conversions Over Time
      display: line
      sql: "SELECT date, SUM(conversions) FROM marts_marketing GROUP BY 1"
    - name: Spend by Channel
      display: pie
      sql: "SELECT channel, SUM(spend) FROM marts_marketing GROUP BY 1"
    - name: Campaign Detail Log
      display: table
      sql: "SELECT * FROM marts_marketing LIMIT 50"
`;

      const spec = builder.parseYamlDashboardSpec(dashYaml);
      expect(spec.name).toBe('Marketing ROI Dashboard');
      expect(spec.theme).toBe('marketing');
      expect(spec.filters).toHaveLength(1);
      expect(spec.cards).toHaveLength(4);
    });

    test('TC-5.3: Ingests raw YAML string with Jinja templating ({{ doc(...) }}) safely sanitized', () => {
      const jinjaYaml = `
version: 2
exposures:
  - name: ops_dashboard
    description: {{ doc("ops_overview_doc") }}
    type: dashboard
    meta:
      metabase:
        cards:
          - name: Uptime
            display: scalar
            sql: "SELECT 99.9"
          - name: Incidents Trend
            display: line
            sql: "SELECT 1"
          - name: Incidents by Service
            display: bar
            sql: "SELECT 1"
          - name: Log Table
            display: table
            sql: "SELECT 1"
`;

      const spec = builder.parseExposureYaml(jinjaYaml);
      expect(spec.slug).toBe('ops_dashboard');
      expect(spec.cards).toHaveLength(4);
    });

    test('TC-5.4: Ingests YAML file path on disk', () => {
      const filePath = path.join(tempDir, 'exposures.yml');
      const content = `
version: 2
exposures:
  - name: disk_exposure
    label: Disk File Exposure
    type: dashboard
    meta:
      metabase:
        cards:
          - name: C1
            display: scalar
          - name: C2
            display: line
          - name: C3
            display: bar
          - name: C4
            display: table
`;
      fs.writeFileSync(filePath, content, 'utf8');

      const spec = builder.parseExposureYaml(filePath);
      expect(spec.name).toBe('Disk File Exposure');
      expect(spec.cards).toHaveLength(4);
    });

    test('TC-5.5: Ingests dbt models list and auto-synthesizes executive dashboard', () => {
      const spec = builder.parseYamlDashboardSpec({
        models: [
          {
            name: 'marts_subscriptions',
            description: 'SaaS subscriptions mart',
            columns: {
              mrr: { name: 'mrr', dataType: 'numeric' },
              created_at: { name: 'created_at', dataType: 'timestamp' },
              plan_tier: { name: 'plan_tier', dataType: 'varchar' },
            },
          },
        ],
      });

      expect(spec.name).toContain('marts_subscriptions');
      expect(spec.slug).toBe('marts_subscriptions');
    });

    test('TC-5.6: Ingests dbt metrics list and auto-synthesizes executive KPI dashboard', () => {
      const spec = builder.parseYamlDashboardSpec({
        metrics: [
          { name: 'mrr', label: 'Monthly Recurring Revenue', type: 'simple', model: 'fct_subscriptions' },
          { name: 'churn_rate', label: 'Monthly Churn Rate', type: 'ratio' },
        ],
      });

      expect(spec.name).toContain('Metrics');
      expect(spec.metrics).toHaveLength(2);
    });

    test('TC-5.7: Throws descriptive error on empty or malformed YAML', () => {
      expect(() => builder.parseExposureYaml('')).toThrow('YAML content or file path must be provided');
      expect(() => builder.parseYamlDashboardSpec('bad: : : syntax')).toThrow('Failed to parse YAML dashboard spec');
      expect(() => builder.parseYamlDashboardSpec(12345)).toThrow('YAML content must be a string or object');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 6: Complete Lifecycle with Mock Client & Dry Run
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 6: Complete Lifecycle with Mock Client & Dry Run', () => {
    test('TC-6.1: Enforces minimum 4 cards requirement (auto-synthesizes if < 4 cards)', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'Partial Dashboard',
          cards: [
            { name: 'Single Card', display: 'scalar', sql: 'SELECT 100' },
          ],
        },
      });

      expect(result.card_count).toBeGreaterThanOrEqual(4);
      expect(result.cards.length).toBeGreaterThanOrEqual(4);
    });

    test('TC-6.2: Single-call execution creates questions, models, dashboard, and attaches cards with positions', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'Complete Executive Performance',
          cards: [
            { name: 'KPI Rev', display: 'scalar', sql: 'SELECT 1' },
            { name: 'Trend Rev', display: 'line', sql: 'SELECT 1' },
            { name: 'Breakdown Category', display: 'bar', sql: 'SELECT 1' },
            { name: 'Transactions Table', display: 'table', sql: 'SELECT 1' },
          ],
          filters: [
            { name: 'Date Range', slug: 'date_range', type: 'date/all-options' },
          ],
        },
        theme: 'modern_emerald',
      });

      expect(result.dashboard_id).toBeDefined();
      expect(result.name).toBe('Complete Executive Performance');
      expect(result.theme).toBe('modern_emerald');
      expect(result.card_count).toBe(4);
      expect(result.filter_count).toBe(1);

      expect(mockClient.dashboardsCreated).toHaveLength(1);
      expect(mockClient.cardsCreated).toHaveLength(4);
      expect(mockClient.cardsAttached).toHaveLength(4);

      // Verify non-overlapping positions
      const positions = result.cards.map(c => c.position);
      expect(validateNoCollisions(positions)).toBe(true);
    });

    test('TC-6.3: Dry run mode produces full plan without calling client mutation methods', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'Dry Run Test',
          cards: [
            { name: 'K1', display: 'scalar', sql: 'SELECT 1' },
            { name: 'T1', display: 'line', sql: 'SELECT 1' },
            { name: 'B1', display: 'bar', sql: 'SELECT 1' },
            { name: 'Table 1', display: 'table', sql: 'SELECT 1' },
          ],
        },
        dry_run: true,
      });

      expect(result.card_count).toBe(4);
      expect(result._provenance.dry_run).toBe(true);

      // Verify no live mutations occurred
      expect(mockClient.dashboardsCreated).toHaveLength(0);
      expect(mockClient.cardsCreated).toHaveLength(0);
      expect(mockClient.cardsAttached).toHaveLength(0);
    });

    test('TC-6.4: Produces valid _provenance metadata with governance level and timestamp', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'Provenance Test',
          cards: [
            { name: 'K1', display: 'scalar', sql: 'SELECT 1' },
            { name: 'T1', display: 'line', sql: 'SELECT 1' },
            { name: 'B1', display: 'bar', sql: 'SELECT 1' },
            { name: 'Table 1', display: 'table', sql: 'SELECT 1' },
          ],
        },
        theme: 'slate_minimal',
      });

      expect(result._provenance).toBeDefined();
      expect(result._provenance.governance_level).toBe('PRODUCTION_CODE_AS_BI');
      expect(result._provenance.builder).toBe('DbtDashboardBuilder');
      expect(result._provenance.timestamp).toBeDefined();
      expect(result._provenance.theme).toBe('slate_minimal');
      expect(result._provenance.card_count).toBe(4);
    });

    test('TC-6.5: Correctly computes grid_summary metrics', async () => {
      const builder = new DbtDashboardBuilder(mockClient);

      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'Grid Summary Test',
          cards: [
            { name: 'K1', display: 'scalar', sql: 'SELECT 1' },
            { name: 'K2', display: 'gauge', sql: 'SELECT 1' },
            { name: 'T1', display: 'line', sql: 'SELECT 1' },
            { name: 'B1', display: 'pie', sql: 'SELECT 1' },
            { name: 'Table 1', display: 'table', sql: 'SELECT 1' },
          ],
        },
      });

      const summary = result.grid_summary;
      expect(summary.grid_width).toBe(24);
      expect(summary.kpi_count).toBe(2);
      expect(summary.trend_count).toBe(1);
      expect(summary.breakdown_count).toBe(1);
      expect(summary.table_count).toBe(1);
      expect(summary.total_rows).toBeGreaterThan(0);
    });

    test('TC-6.6: Gracefully handles mock client without errors', async () => {
      const builder = new DbtDashboardBuilder(null);

      const result = await builder.buildDashboardFromYaml({
        yaml_content: {
          name: 'Null Client Test',
          cards: [
            { name: 'K1', display: 'scalar', sql: 'SELECT 1' },
            { name: 'T1', display: 'line', sql: 'SELECT 1' },
            { name: 'B1', display: 'bar', sql: 'SELECT 1' },
            { name: 'Table 1', display: 'table', sql: 'SELECT 1' },
          ],
        },
      });

      expect(result.dashboard_id).toBe(9999);
      expect(result.card_count).toBe(4);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 7: MCP Tool Registry, Router & Handler Verification
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 7: MCP Tool Registry, Router & Handler Verification', () => {
    test('TC-7.1: Tool registry contains dbt_build_dashboard_from_yaml with readOnlyHint: false and MCP 2025-11-25 schema', () => {
      const defs = getToolDefinitions();
      const toolDef = defs.find(t => t.name === 'dbt_build_dashboard_from_yaml');

      expect(toolDef).toBeDefined();
      expect(toolDef.readOnlyHint).toBe(false);
      expect(toolDef.inputSchema).toBeDefined();
      expect(toolDef.inputSchema.properties.yaml_content).toBeDefined();
      expect(toolDef.inputSchema.properties.exposure_name).toBeDefined();
      expect(toolDef.inputSchema.properties.theme).toBeDefined();
      expect(toolDef.inputSchema.properties.include_models).toBeDefined();

      const meta = TOOL_METADATA.dbt_build_dashboard_from_yaml;
      expect(meta).toBeDefined();
      expect(meta.write).toBe(true);
      expect(meta.outputSchema).toBeDefined();
      expect(meta.outputSchema.properties._provenance).toBeDefined();
    });

    test('TC-7.2: WRITE_TOOLS contains dbt_build_dashboard_from_yaml', () => {
      expect(WRITE_TOOLS.has('dbt_build_dashboard_from_yaml')).toBe(true);
    });

    test('TC-7.3: Handler blocks execution when read-only mode is active and dry_run is false', async () => {
      const origEnv = process.env.METABASE_READ_ONLY_MODE;
      try {
        process.env.METABASE_READ_ONLY_MODE = 'true';

        const handler = new DbtSemanticHandler(mockClient);
        const res = await handler.handleDbtBuildDashboardFromYaml({
          yaml_content: {
            name: 'Blocked Dash',
            cards: [
              { name: 'C1', display: 'scalar' },
              { name: 'C2', display: 'line' },
              { name: 'C3', display: 'bar' },
              { name: 'C4', display: 'table' },
            ],
          },
          dry_run: false,
        });

        expect(res.isError).toBe(true);
        expect(res.content[0].text).toContain('Read-Only Mode Active');
      } finally {
        process.env.METABASE_READ_ONLY_MODE = origEnv;
      }
    });

    test('TC-7.4: Handler permits execution in read-only mode when dry_run is true', async () => {
      const origEnv = process.env.METABASE_READ_ONLY_MODE;
      try {
        process.env.METABASE_READ_ONLY_MODE = 'true';

        const handler = new DbtSemanticHandler(mockClient);
        const res = await handler.handleDbtBuildDashboardFromYaml({
          yaml_content: {
            name: 'Permitted Dry Run Dash',
            cards: [
              { name: 'C1', display: 'scalar' },
              { name: 'C2', display: 'line' },
              { name: 'C3', display: 'bar' },
              { name: 'C4', display: 'table' },
            ],
          },
          dry_run: true,
        });

        expect(res.isError).toBeUndefined();
        expect(res.content[0].text).toContain('LIGHTDASH CODE-AS-BI DASHBOARD BUILT SUCCESSFULLY');
      } finally {
        process.env.METABASE_READ_ONLY_MODE = origEnv;
      }
    });

    test('TC-7.5: Handler returns formatStructuredResponse with markdown table and structured content', async () => {
      const origEnv = process.env.METABASE_READ_ONLY_MODE;
      try {
        process.env.METABASE_READ_ONLY_MODE = 'false';

        const handler = new DbtSemanticHandler(mockClient);
        const res = await handler.handleDbtBuildDashboardFromYaml({
          yaml_content: {
            name: 'Executive Sales Performance',
            cards: [
              { name: 'Total Sales', display: 'scalar', sql: 'SELECT 100' },
              { name: 'Sales Trend', display: 'line', sql: 'SELECT 100' },
              { name: 'Sales by Product', display: 'bar', sql: 'SELECT 100' },
              { name: 'Sales Detail', display: 'table', sql: 'SELECT 100' },
            ],
            filters: [
              { name: 'Date Range', slug: 'date_range', type: 'date/all-options' },
            ],
          },
          theme: 'executive',
        });

        expect(res.content[0].text).toContain('LIGHTDASH CODE-AS-BI DASHBOARD BUILT SUCCESSFULLY');
        expect(res.content[0].text).toContain('24-Column Executive Grid Layout');
        expect(res.structuredContent).toBeDefined();
        expect(res.structuredContent.name).toBe('Executive Sales Performance');
        expect(res.structuredContent.card_count).toBe(4);
        expect(res.structuredContent.filter_count).toBe(1);
        expect(res.structuredContent._provenance).toBeDefined();
      } finally {
        process.env.METABASE_READ_ONLY_MODE = origEnv;
      }
    });

    test('TC-7.6: Handler routes object includes dbt_build_dashboard_from_yaml', () => {
      const handler = new DbtSemanticHandler(mockClient);
      const routes = handler.routes();

      expect(routes.dbt_build_dashboard_from_yaml).toBeDefined();
      expect(typeof routes.dbt_build_dashboard_from_yaml).toBe('function');
    });

    test('TC-7.7: Handler catches exceptions and returns error envelope gracefully', async () => {
      const origEnv = process.env.METABASE_READ_ONLY_MODE;
      try {
        process.env.METABASE_READ_ONLY_MODE = 'false';

        const handler = new DbtSemanticHandler(mockClient);
        const res = await handler.handleDbtBuildDashboardFromYaml({
          yaml_content: 'corrupt yaml : : : bad',
        });

        expect(res.isError).toBe(true);
        expect(res.content[0].text).toContain('❌ dbt Dashboard Builder Error');
      } finally {
        process.env.METABASE_READ_ONLY_MODE = origEnv;
      }
    });
  });
});
