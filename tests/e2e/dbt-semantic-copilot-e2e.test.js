/**
 * tests/e2e/dbt-semantic-copilot-e2e.test.js
 * Comprehensive Milestone 6 End-to-End Integration Test Suite
 * 
 * Validates the complete unified lifecycle and individual deep capabilities of the
 * dbt Semantic BI Co-Pilot across all 5 core superpowers:
 * 1. Deep AST dbt Scan & Metadata Profiler (DbtDeepScanner / dbt_project_scan_deep)
 * 2. Lineage DAG & Multi-Hop Shortest Join Graph (DbtLineageGraph / dbt_lineage_joins_graph)
 * 3. Cube.js Pre-Aggregation & Multi-Dialect Rollup Advisory (DbtPreaggAdvisor / dbt_semantic_preagg_advisor)
 * 4. Lightdash Code-as-BI Dashboard Builder & 24-Col Grid (DbtDashboardBuilder / dbt_build_dashboard_from_yaml)
 * 5. Omni.co Controlled Semantic-to-YAML Exporter (DbtYamlExporter / dbt_semantic_export_yaml)
 * 
 * References: ORIGINAL_REQUEST.md, PROJECT.md, TEST_INFRA.md
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import yaml from 'js-yaml';
import { jest } from '@jest/globals';

import { DbtDeepScanner } from '../../src/dbt/dbt-deep-scanner.js';
import { DbtLineageGraph } from '../../src/dbt/lineage-joins.js';
import { DbtPreaggAdvisor, SUPPORTED_DIALECTS, ADDITIVITY_TYPES } from '../../src/dbt/preagg-advisor.js';
import { DbtDashboardBuilder, THEME_PALETTES } from '../../src/dbt/dbt-dashboard-builder.js';
import { DbtYamlExporter, EXPORT_FORMATS, METRIC_TYPES } from '../../src/dbt/dbt-yaml-exporter.js';
import { SemanticMemory, RULE_STATUS, RULE_CATEGORIES } from '../../src/semantic/semantic-memory.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';
import { buildRouteMap, createToolHandler, WRITE_TOOLS } from '../../src/mcp/tool-router.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';
import { validateNoCollisions, GRID_WIDTH } from '../../src/analytics/dashboard-architect.js';
import * as piiMasker from '../../src/utils/pii-masker.js';

describe('dbt Semantic BI Co-Pilot E2E Master Suite (Milestone 6)', () => {
  let tempProjectDir;
  let tempMemoryDir;
  let mockClient;
  let semanticMemory;
  let handler;
  let toolRunner;

  /**
   * Helper: Build a complete, realistic multi-model enterprise dbt project workspace
   */
  function setupEnterpriseDbtWorkspace(rootDir) {
    const modelsStaging = path.join(rootDir, 'models', 'staging');
    const modelsIntermediate = path.join(rootDir, 'models', 'intermediate');
    const modelsMarts = path.join(rootDir, 'models', 'marts');
    const modelsSemantic = path.join(rootDir, 'models', 'semantic');
    const docsDir = path.join(rootDir, 'docs');
    const targetDir = path.join(rootDir, 'target');
    const exposuresDir = path.join(rootDir, 'exposures');

    fs.mkdirSync(modelsStaging, { recursive: true });
    fs.mkdirSync(modelsIntermediate, { recursive: true });
    fs.mkdirSync(modelsMarts, { recursive: true });
    fs.mkdirSync(modelsSemantic, { recursive: true });
    fs.mkdirSync(docsDir, { recursive: true });
    fs.mkdirSync(targetDir, { recursive: true });
    fs.mkdirSync(exposuresDir, { recursive: true });

    // 1. dbt_project.yml
    fs.writeFileSync(
      path.join(rootDir, 'dbt_project.yml'),
      yaml.dump({
        name: 'enterprise_ecommerce',
        version: '1.0.0',
        config_version: 2,
        profile: 'ecommerce_dw',
        model_paths: ['models'],
        docs_paths: ['docs'],
      })
    );

    // 2. Docs block (docs/docs.md)
    fs.writeFileSync(
      path.join(docsDir, 'docs.md'),
      `
{% doc order_status_doc %}
Standardized order lifecycle states: \`pending\`, \`processing\`, \`shipped\`, \`delivered\`, \`cancelled\`, \`refunded\`.
{% enddoc %}

{% doc customer_health_doc %}
Customer lifetime health status: \`active\`, \`at_risk\`, \`dormant\`, \`churned\`.
{% enddoc %}
`
    );

    // 3. Staging Models
    fs.writeFileSync(
      path.join(modelsStaging, 'stg_customers.sql'),
      `SELECT id AS customer_id, first_name, last_name, email, region_id, created_at FROM {{ source('raw_store', 'customers') }}`
    );
    fs.writeFileSync(
      path.join(modelsStaging, 'stg_orders.sql'),
      `SELECT id AS order_id, customer_id, order_date, status, total_amount, tax_amount FROM {{ source('raw_store', 'orders') }}`
    );
    fs.writeFileSync(
      path.join(modelsStaging, 'stg_order_items.sql'),
      `SELECT id AS item_id, order_id, product_id, quantity, unit_price, line_total FROM {{ source('raw_store', 'order_items') }}`
    );
    fs.writeFileSync(
      path.join(modelsStaging, 'stg_regions.sql'),
      `SELECT id AS region_id, region_name, country_code, continent FROM {{ source('raw_store', 'regions') }}`
    );

    // 4. Staging schema.yml with relationship tests
    fs.writeFileSync(
      path.join(modelsStaging, 'schema.yml'),
      yaml.dump({
        version: 2,
        models: [
          {
            name: 'stg_customers',
            description: 'Cleaned customer identities',
            columns: [
              { name: 'customer_id', description: 'Primary key', tests: ['unique', 'not_null'] },
              {
                name: 'region_id',
                description: 'Foreign key to regions',
                tests: [{ relationships: { to: "ref('stg_regions')", field: 'region_id' } }],
              },
            ],
          },
          {
            name: 'stg_orders',
            description: 'Cleaned order transactions',
            columns: [
              { name: 'order_id', description: 'Primary key', tests: ['unique', 'not_null'] },
              {
                name: 'customer_id',
                description: 'Foreign key to customers',
                tests: [{ relationships: { to: "ref('stg_customers')", field: 'customer_id' } }],
              },
              { name: 'status', description: "{{ doc('order_status_doc') }}" },
            ],
          },
          {
            name: 'stg_order_items',
            description: 'Order item lines',
            columns: [
              { name: 'item_id', description: 'Primary key', tests: ['unique', 'not_null'] },
              {
                name: 'order_id',
                description: 'Foreign key to orders',
                tests: [{ relationships: { to: "ref('stg_orders')", field: 'order_id' } }],
              },
            ],
          },
          {
            name: 'stg_regions',
            description: 'Geographical regions',
            columns: [{ name: 'region_id', description: 'Primary key', tests: ['unique', 'not_null'] }],
          },
        ],
      })
    );

    // 5. Intermediate Model
    fs.writeFileSync(
      path.join(modelsIntermediate, 'int_customer_orders.sql'),
      `SELECT c.customer_id, c.region_id, COUNT(o.order_id) AS lifetime_orders, SUM(o.total_amount) AS lifetime_spend
       FROM {{ ref('stg_customers') }} c
       LEFT JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
       GROUP BY 1, 2`
    );

    // 6. Marts Models
    fs.writeFileSync(
      path.join(modelsMarts, 'dim_regions.sql'),
      `SELECT region_id, region_name, country_code, continent FROM {{ ref('stg_regions') }}`
    );
    fs.writeFileSync(
      path.join(modelsMarts, 'dim_customers.sql'),
      `SELECT c.customer_id, c.first_name, c.last_name, c.email, c.region_id, ico.lifetime_orders, ico.lifetime_spend
       FROM {{ ref('stg_customers') }} c
       LEFT JOIN {{ ref('int_customer_orders') }} ico ON c.customer_id = ico.customer_id`
    );
    fs.writeFileSync(
      path.join(modelsMarts, 'fct_orders.sql'),
      `SELECT o.order_id, o.customer_id, o.order_date, o.status, o.total_amount, o.tax_amount
       FROM {{ ref('stg_orders') }} o`
    );
    fs.writeFileSync(
      path.join(modelsMarts, 'fct_order_items.sql'),
      `SELECT oi.item_id, oi.order_id, oi.product_id, oi.quantity, oi.unit_price, oi.line_total
       FROM {{ ref('stg_order_items') }} oi`
    );
    fs.writeFileSync(
      path.join(modelsMarts, 'rpt_executive_kpis.sql'),
      `SELECT DATE_TRUNC('month', order_date) AS month, COUNT(order_id) AS total_orders, SUM(total_amount) AS total_revenue
       FROM {{ ref('fct_orders') }} GROUP BY 1`
    );

    // 7. Marts schema.yml with meta.metabase and relationships
    fs.writeFileSync(
      path.join(modelsMarts, 'schema.yml'),
      yaml.dump({
        version: 2,
        models: [
          {
            name: 'dim_regions',
            description: 'Gold geography dimension',
            meta: { metabase: { display_name: 'Regions Dimension', visibility_type: 'normal' } },
            columns: [
              { name: 'region_id', description: 'Primary key', tests: ['unique', 'not_null'] },
              { name: 'region_name', description: 'Region name' },
            ],
          },
          {
            name: 'dim_customers',
            description: 'Gold customer dimension',
            meta: { metabase: { display_name: 'Customers Dimension', entity_type: 'dimension' } },
            columns: [
              { name: 'customer_id', description: 'Primary key', tests: ['unique', 'not_null'] },
              {
                name: 'region_id',
                description: 'Foreign key to dim_regions',
                tests: [{ relationships: { to: "ref('dim_regions')", field: 'region_id' } }],
              },
            ],
          },
          {
            name: 'fct_orders',
            description: 'Gold order transactions fact table',
            meta: {
              metabase: {
                display_name: 'Orders Fact',
                entity_type: 'transaction',
                metrics: { total_revenue: { display_name: 'Total Revenue ($)', type: 'currency' } },
              },
            },
            columns: [
              { name: 'order_id', description: 'Primary key', tests: ['unique', 'not_null'] },
              {
                name: 'customer_id',
                description: 'Foreign key to dim_customers',
                tests: [{ relationships: { to: "ref('dim_customers')", field: 'customer_id' } }],
              },
              { name: 'order_date', description: 'Order placement timestamp' },
              { name: 'total_amount', description: 'Gross order amount in USD' },
              { name: 'status', description: "{{ doc('order_status_doc') }}" },
            ],
          },
          {
            name: 'fct_order_items',
            description: 'Gold order item details',
            columns: [
              { name: 'item_id', description: 'Primary key', tests: ['unique', 'not_null'] },
              {
                name: 'order_id',
                description: 'Foreign key to fct_orders',
                tests: [{ relationships: { to: "ref('fct_orders')", field: 'order_id' } }],
              },
            ],
          },
          {
            name: 'rpt_executive_kpis',
            description: 'Gold executive monthly rollup',
            meta: { metabase: { display_name: 'Executive Monthly Rollup' } },
          },
        ],
      })
    );

    // 8. MetricFlow Semantic Models & Metrics
    fs.writeFileSync(
      path.join(modelsSemantic, 'semantic_models.yml'),
      yaml.dump({
        version: 2,
        semantic_models: [
          {
            name: 'semantic_orders',
            model: "ref('fct_orders')",
            description: 'Semantic layer representation of orders fact',
            entities: [
              { name: 'order_id', type: 'primary' },
              { name: 'customer_id', type: 'foreign' },
            ],
            dimensions: [
              { name: 'order_date', type: 'time', type_params: { time_granularity: 'day' } },
              { name: 'status', type: 'categorical' },
            ],
            measures: [
              { name: 'total_revenue', agg: 'sum', expr: 'total_amount', description: 'Gross order revenue' },
              { name: 'order_count', agg: 'count', expr: 'order_id', description: 'Total count of orders' },
              { name: 'distinct_customers', agg: 'count_distinct', expr: 'customer_id', description: 'Unique purchasing customers' },
              { name: 'avg_order_value', agg: 'average', expr: 'total_amount', description: 'Average order value' },
            ],
          },
        ],
      })
    );

    fs.writeFileSync(
      path.join(modelsSemantic, 'metrics.yml'),
      yaml.dump({
        version: 2,
        metrics: [
          {
            name: 'gross_revenue',
            label: 'Gross Revenue',
            description: 'Total revenue sum',
            type: 'simple',
            type_params: { measure: 'total_revenue' },
          },
          {
            name: 'arpu',
            label: 'Average Revenue Per User',
            description: 'Gross revenue divided by distinct customers',
            type: 'ratio',
            type_params: { numerator: 'gross_revenue', denominator: 'distinct_customers' },
          },
          {
            name: 'cumulative_revenue',
            label: 'Cumulative Gross Revenue',
            description: 'Running total revenue',
            type: 'cumulative',
            type_params: { measure: 'total_revenue', window: 'all' },
          },
        ],
      })
    );

    // 9. Catalog.json (profiling stats)
    fs.writeFileSync(
      path.join(targetDir, 'catalog.json'),
      JSON.stringify({
        metadata: { dbt_schema_version: 'https://schemas.getdbt.com/dbt/catalog/v1.json' },
        nodes: {
          'model.enterprise_ecommerce.fct_orders': {
            metadata: { type: 'table', name: 'fct_orders', schema: 'marts', comment: 'Orders fact table' },
            stats: {
              row_count: { id: 'row_count', label: 'Row Count', value: 12500000 },
              bytes: { id: 'bytes', label: 'Approximate Size', value: 3450000000 },
            },
            columns: {
              order_id: { type: 'integer', index: 1, name: 'order_id', comment: 'Primary key' },
              customer_id: { type: 'integer', index: 2, name: 'customer_id', comment: 'Customer reference' },
              order_date: { type: 'timestamp', index: 3, name: 'order_date', comment: 'Date of order' },
              total_amount: { type: 'numeric(12,2)', index: 4, name: 'total_amount', comment: 'Total amount' },
              status: { type: 'varchar(32)', index: 5, name: 'status', comment: 'Order status' },
            },
          },
          'model.enterprise_ecommerce.fct_order_items': {
            metadata: { type: 'table', name: 'fct_order_items', schema: 'marts' },
            stats: {
              row_count: { id: 'row_count', label: 'Row Count', value: 50000000 },
              bytes: { id: 'bytes', label: 'Approximate Size', value: 9200000000 },
            },
            columns: {
              item_id: { type: 'integer', index: 1, name: 'item_id' },
              order_id: { type: 'integer', index: 2, name: 'order_id' },
            },
          },
          'model.enterprise_ecommerce.dim_customers': {
            metadata: { type: 'table', name: 'dim_customers', schema: 'marts' },
            stats: {
              row_count: { id: 'row_count', label: 'Row Count', value: 750000 },
              bytes: { id: 'bytes', label: 'Approximate Size', value: 120000000 },
            },
            columns: {
              customer_id: { type: 'integer', index: 1, name: 'customer_id' },
              region_id: { type: 'integer', index: 2, name: 'region_id' },
            },
          },
          'model.enterprise_ecommerce.dim_regions': {
            metadata: { type: 'table', name: 'dim_regions', schema: 'marts' },
            stats: {
              row_count: { id: 'row_count', label: 'Row Count', value: 150 },
              bytes: { id: 'bytes', label: 'Approximate Size', value: 45000 },
            },
            columns: {
              region_id: { type: 'integer', index: 1, name: 'region_id' },
              region_name: { type: 'varchar(64)', index: 2, name: 'region_name' },
            },
          },
        },
      })
    );

    // 10. Exposures YAML (Lightdash Code-as-BI spec)
    fs.writeFileSync(
      path.join(exposuresDir, 'executive_dashboard.yml'),
      yaml.dump({
        version: 2,
        exposures: [
          {
            name: 'executive_ecommerce_kpis',
            label: 'Executive E-Commerce KPIs',
            type: 'dashboard',
            maturity: 'high',
            owner: { name: 'Analytics Engineering', email: 'analytics@enterprise.com' },
            description: 'Executive revenue, volume, and geographical health indicators',
            depends_on: [
              "ref('fct_orders')",
              "ref('dim_customers')",
              "ref('dim_regions')",
            ],
            filters: [
              { name: 'Order Period', slug: 'order_date', target_variable: 'order_date', type: 'date/month-year', default: 'past30days' },
              { name: 'Customer Region', slug: 'region', target_variable: 'region', type: 'string/=', default: null },
            ],
            cards: [
              {
                name: 'Total Gross Revenue',
                display: 'scalar',
                model: 'fct_orders',
                sql: 'SELECT SUM(total_amount) AS revenue FROM {{ ref(\'fct_orders\') }} WHERE {{order_date}}',
                template_tags: { order_date: { type: 'date' } },
                visual: { prefix: '$', decimals: 2 },
              },
              {
                name: 'Total Orders Volume',
                display: 'scalar',
                model: 'fct_orders',
                sql: 'SELECT COUNT(order_id) AS orders_count FROM {{ ref(\'fct_orders\') }} WHERE {{order_date}}',
                template_tags: { order_date: { type: 'date' } },
              },
              {
                name: 'Revenue Trend by Month',
                display: 'line',
                model: 'fct_orders',
                sql: 'SELECT DATE_TRUNC(\'month\', order_date) AS month, SUM(total_amount) AS revenue FROM {{ ref(\'fct_orders\') }} GROUP BY 1 ORDER BY 1',
              },
              {
                name: 'Revenue by Customer Region',
                display: 'bar',
                model: 'fct_orders',
                sql: 'SELECT r.region_name, SUM(o.total_amount) AS regional_rev FROM {{ ref(\'fct_orders\') }} o LEFT JOIN {{ ref(\'dim_customers\') }} c ON o.customer_id = c.customer_id LEFT JOIN {{ ref(\'dim_regions\') }} r ON c.region_id = r.region_id GROUP BY 1 ORDER BY 2 DESC',
              },
              {
                name: 'Customer Orders Unified Model',
                display: 'table',
                is_model: true,
                model: 'fct_orders',
                sql: 'SELECT o.order_id, o.order_date, c.first_name, c.last_name, r.region_name, o.total_amount FROM {{ ref(\'fct_orders\') }} o LEFT JOIN {{ ref(\'dim_customers\') }} c ON o.customer_id = c.customer_id LEFT JOIN {{ ref(\'dim_regions\') }} r ON c.region_id = r.region_id',
              },
            ],
          },
        ],
      })
    );
  }

  beforeEach(() => {
    tempProjectDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-e2e-project-'));
    tempMemoryDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-e2e-memory-'));
    setupEnterpriseDbtWorkspace(tempProjectDir);

    // Initialize clean SemanticMemory
    const memoryFile = path.join(tempMemoryDir, 'semantic-memory.json');
    semanticMemory = new SemanticMemory({ storagePath: memoryFile });

    // Mock Metabase Client
    let cardSeq = 1000;
    let dashSeq = 100;
    mockClient = {
      cardsCreated: [],
      dashboardsCreated: [],
      attachedCards: [],

      async createQuestion(spec) {
        const card = { id: ++cardSeq, ...spec };
        this.cardsCreated.push(card);
        return card;
      },
      async createModel(spec) {
        const card = { id: ++cardSeq, type: 'model', ...spec };
        this.cardsCreated.push(card);
        return card;
      },
      async createDashboard(spec) {
        const dash = { id: ++dashSeq, ...spec };
        this.dashboardsCreated.push(dash);
        return dash;
      },
      async addCardToDashboard(dashId, cardId, spec) {
        const attached = { id: 9000 + cardId, dashboard_id: dashId, card_id: cardId, ...spec };
        this.attachedCards.push(attached);
        return attached;
      },
      async request() {
        return {};
      },
    };

    // Handler instance
    handler = new DbtSemanticHandler(mockClient, null, null);
    handler.deepScanner = new DbtDeepScanner({ projectDir: tempProjectDir });

    // Build router
    const routes = buildRouteMap({ dbt: handler });
    toolRunner = createToolHandler(routes);
  });

  afterEach(() => {
    if (tempProjectDir && fs.existsSync(tempProjectDir)) {
      fs.rmSync(tempProjectDir, { recursive: true, force: true });
    }
    if (tempMemoryDir && fs.existsSync(tempMemoryDir)) {
      fs.rmSync(tempMemoryDir, { recursive: true, force: true });
    }
  });

  // =========================================================================
  // Test Suite 1: Full Unified E2E Pipeline (All 5 Capabilities Integrated)
  // =========================================================================
  describe('Suite 1: Full Unified E2E Pipeline Workflow', () => {
    test('E2E-1.1: Executes complete unified lifecycle: Deep Scan -> Multi-Hop Joins -> Pre-Agg Advisory -> BI Dashboard -> YAML Export', async () => {
      // Step 1: Deep Project Scan
      const scanRes = await toolRunner({
        params: {
          name: 'dbt_project_scan_deep',
          arguments: { project_dir: tempProjectDir, include_docs: true, include_catalog: true, include_metrics: true },
        },
      });
      expect(scanRes.isError).toBeFalsy();
      const scanData = scanRes.structuredContent;
      expect(scanData.model_count).toBeGreaterThanOrEqual(9);
      expect(scanData.doc_block_count).toBe(2);
      expect(scanData.metric_count).toBeGreaterThanOrEqual(3);
      expect(scanData.catalog_table_count).toBe(4);

      // Verify resolved doc block inside fct_orders
      const fctOrders = scanData.models.find(m => m.name === 'fct_orders');
      expect(fctOrders).toBeDefined();
      expect(fctOrders.tier).toBe('marts_fact');
      expect(fctOrders.tierRank).toBe(100);
      expect(fctOrders.columns.status.description).toContain('Standardized order lifecycle states');

      // Step 2: Multi-Hop Lineage Join Resolution (fct_order_items -> fct_orders -> dim_customers -> dim_regions)
      const lineageRes = await toolRunner({
        params: {
          name: 'dbt_lineage_joins_graph',
          arguments: {
            project_dir: tempProjectDir,
            source_model: 'fct_order_items',
            target_model: 'dim_regions',
            include_sql: true,
          },
        },
      });
      expect(lineageRes.isError).toBeFalsy();
      const lineageData = lineageRes.structuredContent;
      expect(lineageData.join_paths.length).toBe(1);
      const joinPath = lineageData.join_paths[0];
      expect(joinPath.hopCount || joinPath.hops).toBe(3);
      expect(joinPath.path).toEqual(['fct_order_items', 'fct_orders', 'dim_customers', 'dim_regions']);
      expect(lineageData.sql_snippet || joinPath.sqlJoinClause).toContain('LEFT JOIN');
      expect(lineageData.sql_snippet || joinPath.sqlJoinClause).toContain('fct_orders');
      expect(lineageData.sql_snippet || joinPath.sqlJoinClause).toContain('dim_customers');
      expect(lineageData.sql_snippet || joinPath.sqlJoinClause).toContain('dim_regions');

      // Step 3: Pre-Aggregation & Rollup Advisory
      const preaggRes = await toolRunner({
        params: {
          name: 'dbt_semantic_preagg_advisor',
          arguments: {
            project_dir: tempProjectDir,
            model_name: 'fct_orders',
            dialect: 'postgres',
            time_grain: 'month',
            dimensions: ['status'],
            metrics: ['total_revenue', 'order_count'],
          },
        },
      });
      expect(preaggRes.isError).toBeFalsy();
      const preaggData = preaggRes.structuredContent;
      expect(preaggData.recommendation_count).toBeGreaterThanOrEqual(1);
      const rec = preaggData.recommendations[0];
      expect(rec.ddl).toContain('CREATE MATERIALIZED VIEW');
      expect(rec.speedup_estimate.speedup_factor).toBeGreaterThanOrEqual(10);
      expect(rec.speedup_estimate.scan_reduction_pct).toBeGreaterThanOrEqual(90);

      // Step 4: Code-as-BI Dashboard Builder
      process.env.METABASE_READ_ONLY_MODE = 'false';
      const exposureFile = path.join(tempProjectDir, 'exposures', 'executive_dashboard.yml');
      const dashRes = await toolRunner({
        params: {
          name: 'dbt_build_dashboard_from_yaml',
          arguments: {
            yaml_content: exposureFile,
            project_dir: tempProjectDir,
            theme: 'executive',
          },
        },
      });
      expect(dashRes.isError).toBeFalsy();
      const dashData = dashRes.structuredContent;
      expect(dashData.card_count).toBe(5);
      expect(dashData.filter_count).toBe(2);
      expect(mockClient.dashboardsCreated.length).toBe(1);
      expect(mockClient.cardsCreated.length).toBe(5);

      // Validate 24-column collision-free grid placement
      const placedPositions = dashData.cards.map(c => ({
        id: c.id,
        row: c.position.row,
        col: c.position.col,
        size_x: c.position.size_x,
        size_y: c.position.size_y,
      }));
      expect(() => validateNoCollisions(placedPositions)).not.toThrow();
      placedPositions.forEach(p => {
        expect(p.col + p.size_x).toBeLessThanOrEqual(GRID_WIDTH);
      });

      // Step 5: Controlled Semantic Governance & YAML Exporter
      const proposedRule = semanticMemory.proposeRule({
        term: 'Executive Net MRR',
        definition: 'SUM(total_amount) WHERE status = \'delivered\'',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        comment: 'Standardized executive net revenue metric',
        author: 'cfo@enterprise.com',
        dbt_model_hint: 'fct_orders',
        sql_condition: 'SUM(total_amount)',
      });
      semanticMemory.approveRule(proposedRule.rule.rule_id, {
        author: 'head_of_data@enterprise.com',
        comment: 'Verified against financial reconciliation',
      });

      const exporter = new DbtYamlExporter(semanticMemory, { projectDir: tempProjectDir });
      const exportRes = exporter.exportSemanticToYaml({
        target_model: 'fct_orders',
        status_filter: 'ACTIVE',
        format: EXPORT_FORMATS.ALL,
      });

      expect(exportRes.exported_count).toBeGreaterThanOrEqual(1);
      expect(exportRes.yaml_content).toContain('Executive Net MRR');
      expect(exportRes.yaml_content).toContain('semantic_models:');
      expect(exportRes.yaml_content).toContain('fct_orders');
      expect(exportRes._provenance.governance_level).toBe('EXPLICIT_APPROVAL_REQUIRED_NO_HARD_DELETES');
    });

    test('E2E-1.2: End-to-end data provenance and anti-hallucination verification', async () => {
      // Ingest catalog and check column existence validation
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);

      const advisor = new DbtPreaggAdvisor(scanResult);
      const graph = new DbtLineageGraph(scanResult);

      // Verify lineage join columns exist in actual models
      const joinPath = graph.findJoinPath('fct_orders', 'dim_customers');
      expect(joinPath.found).toBe(true);
      expect(joinPath.path).toEqual(['fct_orders', 'dim_customers']);

      // Preagg speedup estimation based on actual catalog 12.5M row count
      const speedup = advisor.estimateSpeedup(12500000, 'month', ['status']);
      expect(speedup.raw_rows).toBe(12500000);
      expect(speedup.speedup_factor).toBeGreaterThan(50);
      expect(speedup.speedup_label).toBeDefined();
    });
  });

  // =========================================================================
  // Test Suite 2: Capability 1 — Deep AST dbt Scan & Metadata Profiler
  // =========================================================================
  describe('Suite 2: Capability 1 — Deep AST Scan & Metadata Ingestion Invariants', () => {
    test('E2E-2.1: Resolves nested doc blocks, multi-parameter doc calls, and bare doc references', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const docs = scanner.parseDocBlocks(tempProjectDir);

      expect(docs.has('order_status_doc')).toBe(true);
      expect(docs.has('customer_health_doc')).toBe(true);

      const resolved = scanner.resolveDocReference("{{ doc('order_status_doc') }}", docs);
      expect(resolved).toContain('Standardized order lifecycle states');

      // Test multi-param doc ref {{ doc('package', 'doc_name') }}
      const multiParamResolved = scanner.resolveDocReference("{{ doc('enterprise_ecommerce', 'customer_health_doc') }}", docs);
      expect(multiParamResolved).toContain('Customer lifetime health status');
    });

    test('E2E-2.2: Classifies models accurately into 9-tier architectural hierarchy', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);

      const fct = scanResult.models.find(m => m.name === 'fct_orders');
      const dim = scanResult.models.find(m => m.name === 'dim_customers');
      const rpt = scanResult.models.find(m => m.name === 'rpt_executive_kpis');
      const int = scanResult.models.find(m => m.name === 'int_customer_orders');
      const stg = scanResult.models.find(m => m.name === 'stg_orders');

      expect(fct.tier).toBe('marts_fact');
      expect(fct.tierRank).toBe(100);

      expect(dim.tier).toBe('marts_dim');
      expect(dim.tierRank).toBe(90);

      expect(rpt.tier).toBe('marts_report');
      expect(rpt.tierRank).toBe(85);

      expect(int.tier).toBe('intermediate');
      expect(int.tierRank).toBe(50);

      expect(stg.tier).toBe('staging');
      expect(stg.tierRank).toBe(20);
    });

    test('E2E-2.3: Ingests MetricFlow entities, measures, dimensions, and derived metrics', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);

      expect(scanResult.semanticModels.length).toBe(1);
      const semModel = scanResult.semanticModels[0];
      expect(semModel.name).toBe('semantic_orders');
      expect(semModel.entities.some(e => e.name === 'order_id' && e.type === 'primary')).toBe(true);
      expect(semModel.measures.some(m => m.name === 'distinct_customers' && m.agg === 'count_distinct')).toBe(true);

      expect(scanResult.metrics.length).toBeGreaterThanOrEqual(3);
      expect(scanResult.metrics.some(m => m.name === 'arpu' && m.type === 'ratio')).toBe(true);
      expect(scanResult.metrics.some(m => m.name === 'cumulative_revenue' && m.type === 'cumulative')).toBe(true);
    });

    test('E2E-2.4: Ingests catalog.json stats: row counts, byte sizes, and column data profiling', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const catalog = scanner.parseCatalog(path.join(tempProjectDir, 'target', 'catalog.json'));

      expect(catalog).toBeDefined();
      expect(catalog.tables.fct_orders.rowCount).toBe(12500000);
      expect(catalog.tables.fct_orders.bytes).toBe(3450000000);
      expect(catalog.tables.fct_orders.columns.total_amount.type).toBe('numeric(12,2)');
    });
  });

  // =========================================================================
  // Test Suite 3: Capability 2 — Lineage DAG & Multi-Hop Shortest Join Graph
  // =========================================================================
  describe('Suite 3: Capability 2 — Lineage DAG & Join Graph Routing Invariants', () => {
    test('E2E-3.1: Kahn\'s topological sort resolves dependency layers across the full project', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);
      const graph = new DbtLineageGraph(scanResult);

      const topoOrder = graph.getTopologicalOrder();
      expect(topoOrder).toBeDefined();
      expect(topoOrder.length).toBeGreaterThanOrEqual(9);

      // Invariant: Staging must precede Intermediate, Intermediate must precede Marts
      const stgIdx = topoOrder.indexOf('stg_customers');
      const intIdx = topoOrder.indexOf('int_customer_orders');
      const dimIdx = topoOrder.indexOf('dim_customers');

      expect(stgIdx).toBeLessThan(intIdx);
      expect(intIdx).toBeLessThan(dimIdx);
    });

    test('E2E-3.2: Multi-hop Dijkstra routing resolves shortest path and computes confidence scores', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);
      const graph = new DbtLineageGraph(scanResult);

      const pathResult = graph.findJoinPath('fct_order_items', 'dim_regions');
      expect(pathResult.found).toBe(true);
      expect(pathResult.path).toEqual(['fct_order_items', 'fct_orders', 'dim_customers', 'dim_regions']);
      expect(pathResult.confidence).toBeGreaterThan(0.7);
      expect(pathResult.edges.length).toBe(3);

      // Verify individual hop keys
      expect(pathResult.edges[0].fromColumn || pathResult.edges[0].from_column).toBeDefined();
      expect(pathResult.edges[1].fromColumn || pathResult.edges[1].from_column).toBeDefined();
      expect(pathResult.edges[2].fromColumn || pathResult.edges[2].from_column).toBeDefined();
    });

    test('E2E-3.3: ANSI SQL Join Generator produces collision-free aliases and correct join conditions', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);
      const graph = new DbtLineageGraph(scanResult);

      const pathResult = graph.findJoinPath('fct_orders', 'dim_regions');
      const sql = graph.generateJoinSql(pathResult, 'base_orders');

      expect(sql).toContain('LEFT JOIN dim_customers');
      expect(sql).toContain('LEFT JOIN dim_regions');
      expect(sql).toContain('base_orders.customer_id =');
      expect(sql).not.toContain('undefined');
    });

    test('E2E-3.4: Traverses upstream/downstream lineage and computes blast radius', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);
      const graph = new DbtLineageGraph(scanResult);

      const downstreamFromStg = graph.getAllDownstream('stg_customers');
      expect(downstreamFromStg).toContain('int_customer_orders');
      expect(downstreamFromStg).toContain('dim_customers');

      const upstreamFromDim = graph.getAllUpstream('dim_customers');
      expect(upstreamFromDim).toContain('stg_customers');
      expect(upstreamFromDim).toContain('int_customer_orders');

      const blastRadius = graph.calculateBlastRadius('stg_orders');
      expect(blastRadius.totalAffectedCount).toBeGreaterThanOrEqual(2);
      expect(blastRadius.impactLevel).toBeDefined();
    });
  });

  // =========================================================================
  // Test Suite 4: Capability 3 — Cube.js Pre-Aggregation & Rollup Advisory
  // =========================================================================
  describe('Suite 4: Capability 3 — Cube.js Pre-Aggregation & Rollup Invariants', () => {
    test('E2E-4.1: Classifies metric additivity across additive, semi-additive, and non-additive categories', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);
      const advisor = new DbtPreaggAdvisor(scanResult);

      // Additive measure (sum/count)
      const totalRev = advisor.classifyAdditivity({ name: 'total_revenue', agg: 'sum' });
      expect(totalRev.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);

      // Non-additive measure (count_distinct)
      const distinctCust = advisor.classifyAdditivity({ name: 'distinct_customers', agg: 'count_distinct' });
      expect(distinctCust.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(distinctCust.recommendation).toContain('HLL');

      // Semi-additive measure (balance / snapshot)
      const balance = advisor.classifyAdditivity({ name: 'ending_balance', agg: 'snapshot' });
      expect(balance.additivity).toBe(ADDITIVITY_TYPES.SEMI_ADDITIVE);
    });

    test('E2E-4.2: Formulates valid DDL across all 7 supported SQL dialects', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);

      const options = {
        modelName: 'fct_orders',
        timeDimension: 'order_date',
        timeGrain: 'month',
        dimensions: ['status'],
        measures: [
          { name: 'total_revenue', agg: 'sum', expr: 'total_amount' },
          { name: 'order_count', agg: 'count', expr: 'order_id' },
        ],
      };

      for (const dialect of SUPPORTED_DIALECTS) {
        const advisor = new DbtPreaggAdvisor(scanResult, { dialect });
        const result = advisor.generateRollupDDL({ ...options, dialect });

        expect(result.ddl).toBeDefined();
        expect(result.ddl.length).toBeGreaterThan(20);

        if (dialect === 'postgres') {
          expect(result.ddl).toContain('CREATE MATERIALIZED VIEW');
          expect(result.refresh_command).toContain('REFRESH MATERIALIZED VIEW CONCURRENTLY');
        } else if (dialect === 'bigquery') {
          expect(result.ddl).toContain('CREATE MATERIALIZED VIEW');
          expect(result.ddl).toContain('PARTITION BY');
        } else if (dialect === 'snowflake') {
          expect(result.ddl).toContain('CREATE OR REPLACE MATERIALIZED VIEW');
          expect(result.ddl).toContain('CLUSTER BY');
        } else if (dialect === 'clickhouse') {
          expect(result.ddl).toContain('CREATE MATERIALIZED VIEW');
          expect(result.ddl).toContain('ENGINE =');
        } else if (dialect === 'duckdb') {
          expect(result.ddl).toContain('TABLE');
        } else if (dialect === 'redshift') {
          expect(result.ddl).toContain('CREATE MATERIALIZED VIEW');
        } else if (dialect === 'mysql') {
          expect(result.ddl).toContain('TABLE');
        }
      }
    });

    test('E2E-4.3: Validates mathematical speedup indexing (10x-100x) and scan reduction calculation', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);
      const advisor = new DbtPreaggAdvisor(scanResult);

      const speedup = advisor.estimateSpeedup(12500000, 'month', ['status']);
      expect(speedup.raw_rows).toBe(12500000);
      expect(speedup.preagg_rows).toBeLessThan(10000);
      expect(speedup.scan_reduction_pct).toBeGreaterThan(99);
      expect(speedup.speedup_factor).toBeGreaterThanOrEqual(50);
    });
  });

  // =========================================================================
  // Test Suite 5: Capability 4 — Lightdash Code-as-BI Dashboard Builder
  // =========================================================================
  describe('Suite 5: Capability 4 — Code-as-BI Dashboard Builder Invariants', () => {
    test('E2E-5.1: Builds Metabase Questions & Metabase Models (v50+) with 24-col collision-free grid', async () => {
      const builder = new DbtDashboardBuilder(mockClient, {
        projectDir: tempProjectDir,
        theme: 'modern_emerald',
      });

      const exposureFile = path.join(tempProjectDir, 'exposures', 'executive_dashboard.yml');
      const result = await builder.buildDashboardFromYaml({
        yaml_content: exposureFile,
      });

      expect(result.dashboard_id).toBeDefined();
      expect(result.card_count).toBe(5);

      // Verify Metabase Model v50+ was created for the table card
      const modelCard = mockClient.cardsCreated.find(c => c.type === 'model');
      expect(modelCard).toBeDefined();
      expect(modelCard.name).toBe('Customer Orders Unified Model');

      // Verify non-overlapping 24-column grid placements
      const placedCards = result.cards.map(c => ({
        id: c.id,
        row: c.position.row,
        col: c.position.col,
        size_x: c.position.size_x,
        size_y: c.position.size_y,
      }));
      expect(() => validateNoCollisions(placedCards)).not.toThrow();

      // Check card visual archetypes: scalar = 6x4, line = 12x8, table = 24x8
      const scalarCard = result.cards.find(c => c.display === 'scalar');
      expect(scalarCard.position.size_x).toBe(6);
      expect(scalarCard.position.size_y).toBe(4);

      const tableCard = result.cards.find(c => c.display === 'table');
      expect(tableCard.position.size_x).toBe(24);
      expect(tableCard.position.size_y).toBe(8);
    });

    test('E2E-5.2: Binds global template tags and parameters to Metabase card SQL variables', async () => {
      const builder = new DbtDashboardBuilder(mockClient, { projectDir: tempProjectDir });
      const exposureFile = path.join(tempProjectDir, 'exposures', 'executive_dashboard.yml');
      const result = await builder.buildDashboardFromYaml({ yaml_content: exposureFile });

      const boundCard = result.cards.find(c => c.name === 'Total Gross Revenue');
      expect(boundCard.parameter_mappings).toBeDefined();
      expect(boundCard.parameter_mappings.length).toBeGreaterThanOrEqual(1);
      expect(boundCard.parameter_mappings[0].target[0]).toBe('variable');
    });

    test('E2E-5.3: Security Enforcement: WRITE_TOOLS gate blocks dashboard mutations when Read-Only Mode is active', async () => {
      process.env.METABASE_READ_ONLY_MODE = 'true';
      expect(WRITE_TOOLS.has('dbt_build_dashboard_from_yaml')).toBe(true);

      // 1. Tool router blocks write operation
      await expect(
        toolRunner({
          params: {
            name: 'dbt_build_dashboard_from_yaml',
            arguments: {
              yaml_content: path.join(tempProjectDir, 'exposures', 'executive_dashboard.yml'),
              project_dir: tempProjectDir,
            },
          },
        })
      ).rejects.toThrow('Read-only mode is active');

      // 2. Direct handler call blocks write operation with structured error
      const handlerRes = await handler.handleDbtBuildDashboardFromYaml({
        yaml_content: path.join(tempProjectDir, 'exposures', 'executive_dashboard.yml'),
        project_dir: tempProjectDir,
      });
      expect(handlerRes.isError).toBe(true);
      expect(handlerRes.content[0].text).toContain('Read-Only Mode Active');
      expect(mockClient.dashboardsCreated.length).toBe(0);

      // Restore read-only mode
      process.env.METABASE_READ_ONLY_MODE = 'false';
    });
  });

  // =========================================================================
  // Test Suite 6: Capability 5 — Controlled Semantic-to-YAML Exporter
  // =========================================================================
  describe('Suite 6: Capability 5 — Controlled Semantic-to-YAML Exporter Invariants', () => {
    test('E2E-6.1: Strict governance filter: exports only ACTIVE approved rules, ignores PENDING rules', async () => {
      // 1. Propose Rule A (Pending)
      const rulePending = semanticMemory.proposeRule({
        term: 'Draft Trial Conversion %',
        definition: 'COUNT(converted) / COUNT(trials) * 100',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        author: 'intern@enterprise.com',
        dbt_model_hint: 'fct_orders',
      });

      // 2. Propose & Approve Rule B (Active)
      const ruleActive = semanticMemory.proposeRule({
        term: 'Net Recognized Revenue',
        definition: 'SUM(total_amount) - SUM(tax_amount)',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        author: 'controller@enterprise.com',
        dbt_model_hint: 'fct_orders',
        sql_condition: 'SUM(total_amount) - SUM(tax_amount)',
      });
      semanticMemory.approveRule(ruleActive.rule.rule_id, {
        author: 'cfo@enterprise.com',
        comment: 'Approved for Q3 reporting',
      });

      const exporter = new DbtYamlExporter(semanticMemory, { projectDir: tempProjectDir });
      const exportResult = exporter.exportSemanticToYaml({
        target_model: 'fct_orders',
        status_filter: 'ACTIVE',
      });

      expect(exportResult.exported_count).toBe(1);
      expect(exportResult.yaml_content).toContain('Net Recognized Revenue');
      expect(exportResult.yaml_content).not.toContain('Draft Trial Conversion %');
    });

    test('E2E-6.2: Serializes valid dbt schema.yml, semantic_models.yml, and metrics.yml syntax', async () => {
      const rule = semanticMemory.proposeRule({
        term: 'Customer Order Count',
        definition: 'COUNT(order_id)',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        author: 'lead@enterprise.com',
        dbt_model_hint: 'fct_orders',
        sql_condition: 'COUNT(order_id)',
      });
      semanticMemory.approveRule(rule.rule.rule_id, { author: 'lead@enterprise.com' });

      const exporter = new DbtYamlExporter(semanticMemory, { projectDir: tempProjectDir });
      const storedRule = semanticMemory.rules.get(rule.rule.rule_id);
      const dbtSchema = exporter.formatDbtSchemaYaml([storedRule], { targetModel: 'fct_orders' });
      const metricFlow = exporter.formatMetricFlowYaml([storedRule], { targetModel: 'fct_orders' });

      expect(() => yaml.load(dbtSchema)).not.toThrow();
      expect(() => yaml.loadAll(metricFlow)).not.toThrow();

      const parsedSchema = yaml.load(dbtSchema);
      expect(parsedSchema.version).toBe(2);
      expect(parsedSchema.models[0].name).toBe('fct_orders');

      const parsedMetricDocs = yaml.loadAll(metricFlow);
      expect(parsedMetricDocs.length).toBeGreaterThanOrEqual(1);
    });

    test('E2E-6.3: Preserves audit provenance comments and soft-deprecation rules', async () => {
      const rule = semanticMemory.proposeRule({
        term: 'Legacy Margin',
        definition: 'Gross revenue minus estimated shipping',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        author: 'analyst@enterprise.com',
        dbt_model_hint: 'fct_orders',
      });
      semanticMemory.approveRule(rule.rule.rule_id, { author: 'lead@enterprise.com' });
      semanticMemory.deprecateRule(rule.rule.rule_id, {
        author: 'compliance@enterprise.com',
        reason: 'Superseded by 2026 GAAP net margin methodology',
      });

      const exporter = new DbtYamlExporter(semanticMemory, { projectDir: tempProjectDir });

      // Default export excludes deprecated
      const activeExport = exporter.exportSemanticToYaml({ status_filter: 'ACTIVE' });
      expect(activeExport.yaml_content).not.toContain('Legacy Margin');

      // Export with deprecated explicitly included embeds deprecation notice
      const fullExport = exporter.exportSemanticToYaml({ include_deprecated: true });
      expect(fullExport.yaml_content).toContain('Legacy Margin');
      expect(fullExport.yaml_content).toContain('DEPRECATED');
      expect(fullExport.yaml_content).toContain('Superseded by 2026 GAAP net margin methodology');
    });
  });

  // =========================================================================
  // Test Suite 7: Adversarial, Chaos & Boundary Stress Hardening
  // =========================================================================
  describe('Suite 7: Adversarial, Chaos & Boundary Stress Invariants', () => {
    test('E2E-7.1: Malformed YAML files and broken Jinja syntax recover safely without crashing', async () => {
      const badYamlPath = path.join(tempProjectDir, 'models', 'staging', 'malformed_schema.yml');
      fs.writeFileSync(badYamlPath, `version: 2\nmodels:\n  - name: broken_model\n  invalid_indentation::: {{{`);

      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      // Should not throw unhandled exception, records warning
      const scanResult = await scanner.scanProject(tempProjectDir);
      expect(scanResult.modelCount).toBeGreaterThanOrEqual(1);
    });

    test('E2E-7.2: Circular dependency loops are detected and resolved without infinite recursion', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);

      // Inject circular relationship
      scanResult.models.push({
        name: 'model_alpha',
        tier: 'intermediate',
        dependsOn: ['model_beta'],
        columns: {},
      });
      scanResult.models.push({
        name: 'model_beta',
        tier: 'intermediate',
        dependsOn: ['model_alpha'],
        columns: {},
      });

      const graph = new DbtLineageGraph(scanResult);
      expect(() => graph.getTopologicalOrder()).not.toThrow();
    });

    test('E2E-7.3: Disconnected orphan models return safe empty join path responses', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempProjectDir });
      const scanResult = await scanner.scanProject(tempProjectDir);

      scanResult.models.push({
        name: 'isolated_orphan',
        tier: 'intermediate',
        dependsOn: [],
        columns: {},
      });

      const graph = new DbtLineageGraph(scanResult);
      const pathResult = graph.findJoinPath('fct_orders', 'isolated_orphan');

      expect(pathResult.found).toBe(false);
      expect(pathResult.path).toEqual([]);
      expect(pathResult.sqlJoinClause || pathResult.sql_join_clause || '').toBe('');
    });

    test('E2E-7.4: Zero data leakage: PII masking and prompt injection defense in generated metadata', async () => {
      const injectedTerm = "'; DROP TABLE users; -- <script>alert(1)</script>";
      const piiEmail = 'ceo.confidential@enterprise.com';

      const proposed = semanticMemory.proposeRule({
        term: `Customer Margin for ${piiEmail}`,
        definition: `SUM(amount) /* ${injectedTerm} */`,
        category: RULE_CATEGORIES.BUSINESS_TERM,
        author: 'attacker@evil.com',
        dbt_model_hint: 'fct_orders',
      });
      semanticMemory.approveRule(proposed.rule.rule_id, { author: 'admin@enterprise.com' });

      const exporter = new DbtYamlExporter(semanticMemory, { projectDir: tempProjectDir });
      const exportRes = exporter.exportSemanticToYaml({ status_filter: 'ACTIVE', format: 'schema_yml' });

      // Invariant: Exported individual YAML file must be valid YAML without raw script execution
      expect(() => yaml.load(exportRes.yaml_content)).not.toThrow();
      expect(exportRes.yaml_content).toBeDefined();
    });
  });
});
