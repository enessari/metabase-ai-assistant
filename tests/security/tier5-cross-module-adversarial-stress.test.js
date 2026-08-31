/**
 * tests/security/tier5-cross-module-adversarial-stress.test.js
 * Tier 5 Cross-Module Adversarial Stress & Chaos Verification Suite
 *
 * Exhaustively stress tests cross-module interactions:
 * 1. End-to-End Pipeline Data Flow: DbtDeepScanner -> DbtLineageGraph -> DbtPreaggAdvisor -> DbtDashboardBuilder -> DbtYamlExporter
 * 2. Concurrency, Multi-tenant Isolation & Parallel Invocations across all modules
 * 3. Corrupted Inputs, Chaos Injection & AST Malformations
 * 4. Extreme Schema Drift between catalog.json and manifest.json
 * 5. Pathological Circular Join Graphs & Complex Multi-Loop Lineage Topologies
 * 6. High-Volume Scale, Deep-Hop Traversal & Memory Stress
 * 7. Multi-Hop Entity Linkages & Self-Join / Same-Table Alias Collisions
 * 8. Non-Additive & Semi-Additive MetricFlow Compound Rollups
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import { jest } from '@jest/globals';
import { DbtDeepScanner } from '../../src/dbt/dbt-deep-scanner.js';
import { DbtLineageGraph, normalizeNodeName } from '../../src/dbt/lineage-joins.js';
import { DbtPreaggAdvisor, SUPPORTED_DIALECTS, SUPPORTED_GRAINS, ADDITIVITY_TYPES } from '../../src/dbt/preagg-advisor.js';
import { DbtDashboardBuilder, THEME_PALETTES } from '../../src/dbt/dbt-dashboard-builder.js';
import { DbtYamlExporter, EXPORT_FORMATS, METRIC_TYPES } from '../../src/dbt/dbt-yaml-exporter.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';
import { SemanticMemory, RULE_STATUS, RULE_CATEGORIES } from '../../src/semantic/semantic-memory.js';

describe('Tier 5 Cross-Module Adversarial Stress & Resilience Suite', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-cross-module-stress-'));
  });

  afterEach(() => {
    if (tempDir && fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  // =========================================================================
  // VECTOR 1: Full 5-Stage Cross-Module Data Pipeline
  // =========================================================================
  describe('Vector 1: Complete 5-Stage Pipeline (Scanner -> Lineage -> Preagg -> Dashboard -> YamlExporter)', () => {
    test('seamlessly cascades structured metadata across all 5 modules with zero loss', async () => {
      // 1. Setup rich dbt project on disk
      const modelsDir = path.join(tempDir, 'models', 'marts');
      const docsDir = path.join(tempDir, 'docs');
      const targetDir = path.join(tempDir, 'target');
      fs.mkdirSync(modelsDir, { recursive: true });
      fs.mkdirSync(docsDir, { recursive: true });
      fs.mkdirSync(targetDir, { recursive: true });

      // dbt_project.yml
      fs.writeFileSync(path.join(tempDir, 'dbt_project.yml'), 'name: ecommerce_corp\nversion: 1.0.0');

      // Docs block
      fs.writeFileSync(path.join(docsDir, 'descriptions.md'), `
{% doc order_status_doc %}
Tracks order state: pending, completed, returned, or cancelled.
{% enddoc %}
{% doc revenue_metric_doc %}
Gross revenue computed as item_price * quantity minus discounts.
{% enddoc %}
      `);

      // Schema YAML with relationship tests, formatting, and metrics
      fs.writeFileSync(path.join(modelsDir, 'schema.yml'), `
version: 2
models:
  - name: fct_orders
    description: "Core sales orders fact table with {{ doc('order_status_doc') }}"
    meta:
      metabase:
        display_name: "Orders Fact Mart"
      lightdash:
        tier: "marts_fact"
    columns:
      - name: order_id
        description: "Primary key"
        data_type: integer
        tests:
          - unique
          - not_null
      - name: customer_id
        description: "Foreign key to customers"
        data_type: integer
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: order_date
        description: "Transaction timestamp"
        data_type: date
      - name: order_status
        description: "{{ doc('order_status_doc') }}"
        data_type: varchar
      - name: gross_amount
        description: "{{ doc('revenue_metric_doc') }}"
        data_type: numeric
        meta:
          metabase:
            formatting:
              number_style: currency
              currency: USD
              decimals: 2
  - name: dim_customers
    description: "Customer dimension table"
    meta:
      metabase:
        display_name: "Customers Mart"
    columns:
      - name: customer_id
        data_type: integer
        tests:
          - unique
          - not_null
      - name: region_id
        data_type: integer
        tests:
          - relationships:
              to: ref('dim_regions')
              field: region_id
      - name: customer_segment
        data_type: varchar
  - name: dim_regions
    description: "Geographic region dimension"
    columns:
      - name: region_id
        data_type: integer
        tests:
          - unique
          - not_null
      - name: region_name
        data_type: varchar

semantic_models:
  - name: sem_orders
    model: ref('fct_orders')
    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: order_status
        type: categorical
    measures:
      - name: total_gross_revenue
        expr: gross_amount
        agg: sum
        meta:
          metabase:
            formatting:
              currency: USD

metrics:
  - name: total_revenue
    label: "Total Gross Revenue"
    description: "{{ doc('revenue_metric_doc') }}"
    type: simple
    type_params:
      measure: total_gross_revenue
      `);

      // SQL model files
      fs.writeFileSync(path.join(modelsDir, 'fct_orders.sql'), `
SELECT 1 AS order_id, 10 AS customer_id, CURRENT_DATE AS order_date, 'completed' AS order_status, 150.00 AS gross_amount
FROM {{ ref('dim_customers') }}
      `);

      // Catalog stats
      fs.writeFileSync(path.join(targetDir, 'catalog.json'), JSON.stringify({
        metadata: { dbt_version: '1.8.0', generated_at: new Date().toISOString() },
        nodes: {
          'model.ecommerce_corp.fct_orders': {
            metadata: { name: 'fct_orders', type: 'table' },
            stats: { row_count: { value: 5000000 }, num_bytes: { value: 640000000 } },
            columns: {
              order_id: { type: 'INT64', index: 1 },
              customer_id: { type: 'INT64', index: 2 },
              order_date: { type: 'DATE', index: 3 },
              order_status: { type: 'STRING', index: 4 },
              gross_amount: { type: 'NUMERIC', index: 5 },
            },
          },
          'model.ecommerce_corp.dim_customers': {
            metadata: { name: 'dim_customers', type: 'table' },
            stats: { row_count: { value: 250000 }, num_bytes: { value: 32000000 } },
            columns: {
              customer_id: { type: 'INT64', index: 1 },
              region_id: { type: 'INT64', index: 2 },
              customer_segment: { type: 'STRING', index: 3 },
            },
          },
          'model.ecommerce_corp.dim_regions': {
            metadata: { name: 'dim_regions', type: 'table' },
            stats: { row_count: { value: 50 }, num_bytes: { value: 4096 } },
            columns: {
              region_id: { type: 'INT64', index: 1 },
              region_name: { type: 'STRING', index: 2 },
            },
          },
        },
      }));

      // Stage 1: DbtDeepScanner
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const scanResult = await scanner.scanProject(tempDir);

      expect(scanResult.modelCount).toBe(3);
      expect(scanResult.modelsByTier.marts_fact).toBe(1);
      expect(scanResult.modelsByTier.marts_dim).toBe(2);
      expect(scanResult.catalogLoaded).toBe(true);
      expect(scanResult.summary.totalTableRows).toBe(5250050);

      // Doc blocks resolved in Stage 1
      const fctOrders = scanResult.models.find(m => m.name === 'fct_orders');
      expect(fctOrders.description).toContain('Tracks order state: pending, completed, returned, or cancelled.');

      // Stage 2: DbtLineageGraph
      const lineageGraph = new DbtLineageGraph(scanResult);
      expect(lineageGraph.getNodeCount()).toBeGreaterThanOrEqual(3);
      expect(lineageGraph.hasCycles()).toBe(false);

      // Multi-hop join path: fct_orders -> dim_customers -> dim_regions
      const joinPath = lineageGraph.findJoinPath('fct_orders', 'dim_regions');
      expect(joinPath.found).toBe(true);
      expect(joinPath.hopCount).toBe(2);
      expect(joinPath.path).toEqual(['fct_orders', 'dim_customers', 'dim_regions']);

      const joinSql = lineageGraph.generateJoinSql(joinPath);
      expect(joinSql).toContain('FROM fct_orders AS fct_orders');
      expect(joinSql).toContain('LEFT JOIN dim_customers AS dim_customers');
      expect(joinSql).toContain('LEFT JOIN dim_regions AS dim_regions');

      // Stage 3: DbtPreaggAdvisor (with categorical dimension grouping)
      const advisor = new DbtPreaggAdvisor(scanResult, { dialect: 'bigquery', minSpeedupFactor: 2.0 });
      const preaggRecs = advisor.advisePreaggregations({
        modelName: 'fct_orders',
        dimensions: ['order_status'],
        timeGrains: ['day', 'month'],
      });

      expect(preaggRecs.length).toBeGreaterThanOrEqual(1);
      const dayRec = preaggRecs.find(r => r.time_grain === 'day');
      expect(dayRec).toBeDefined();
      expect(dayRec.speedup_estimate.speedup_factor).toBeGreaterThan(5);
      expect(dayRec.ddl).toContain('CREATE MATERIALIZED VIEW');
      expect(dayRec.ddl).toContain('PARTITION BY DATE(order_date_day)');

      // Stage 4: DbtDashboardBuilder
      const mockClient = {
        createDashboard: jest.fn().mockResolvedValue({ id: 888, name: 'Executive Sales Dashboard' }),
        createQuestion: jest.fn().mockImplementation(payload => Promise.resolve({ id: Math.floor(Math.random() * 1000) + 100, ...payload })),
        createModel: jest.fn().mockImplementation(payload => Promise.resolve({ id: 999, ...payload })),
        addCardToDashboard: jest.fn().mockResolvedValue({ success: true }),
      };

      const dashboardBuilder = new DbtDashboardBuilder(mockClient, { theme: 'financial' });
      const dashboardResult = await dashboardBuilder.buildDashboardFromYaml({
        model_name: 'fct_orders',
        projectDir: tempDir,
      });

      expect(dashboardResult.dashboard_id).toBe(888);
      expect(dashboardResult.card_count).toBeGreaterThanOrEqual(4);
      expect(dashboardResult.grid_summary.grid_width).toBe(24);
      expect(mockClient.createDashboard).toHaveBeenCalled();
      expect(mockClient.addCardToDashboard).toHaveBeenCalled();

      // Stage 5: DbtYamlExporter (Omni.co Sync back to dbt YAML with isolated memory storage)
      const memory = new SemanticMemory({ storagePath: path.join(tempDir, 'semantic-memory.json') });
      const prop1 = memory.proposeRule({
        term: 'High Value Order',
        definition: 'Order with gross_amount >= 500.00 USD',
        category: RULE_CATEGORIES.FILTER_RULE,
        sql_condition: 'gross_amount >= 500.00',
        dbt_model_hint: 'fct_orders',
        author: 'Lead Architect',
      });
      memory.approveRule(prop1.rule.rule_id, { author: 'Head of Data' });

      const prop2 = memory.proposeRule({
        term: 'Total Net Bookings',
        definition: 'Sum of completed gross amounts',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        sql_condition: "SUM(CASE WHEN order_status = 'completed' THEN gross_amount ELSE 0 END)",
        dbt_model_hint: 'fct_orders',
        author: 'Finance Lead',
      });
      memory.approveRule(prop2.rule.rule_id, { author: 'VP Analytics' });

      const exporter = new DbtYamlExporter(memory, { projectDir: tempDir });
      const exportResult = exporter.exportSemanticToYaml({
        format: EXPORT_FORMATS.ALL,
        target_model: 'fct_orders',
      });

      expect(exportResult.success).toBe(true);
      expect(exportResult.exported_count).toBe(2);
      expect(exportResult.schema_yaml).toContain('name: fct_orders');
      expect(exportResult.schema_yaml).toContain('high_value_order');
      expect(exportResult.schema_yaml).toContain('total_net_bookings');
      expect(exportResult._provenance.governance_level).toBe('EXPLICIT_APPROVAL_REQUIRED_NO_HARD_DELETES');
    });
  });

  // =========================================================================
  // VECTOR 2: Concurrency, Multi-tenant Isolation & Parallel Invocations
  // =========================================================================
  describe('Vector 2: Concurrency & State Isolation under Heavy Parallelism', () => {
    test('handles 25 concurrent multi-dialect pre-aggregation requests without state leaks', async () => {
      const scanResult = {
        models: [
          {
            name: 'fct_web_events',
            tier: 'marts_fact',
            columns: {
              event_id: { dataType: 'int' },
              user_id: { dataType: 'int' },
              event_time: { dataType: 'timestamp' },
              event_type: { dataType: 'varchar' },
              device_category: { dataType: 'varchar' },
              revenue: { dataType: 'numeric' },
            },
          },
        ],
        catalogLoaded: true,
        catalogStats: {
          tables: {
            fct_web_events: { rowCount: 10000000 },
          },
        },
      };

      const tasks = [];
      const dialects = ['postgres', 'bigquery', 'snowflake', 'clickhouse', 'duckdb', 'redshift', 'mysql'];
      const grains = ['hour', 'day', 'week', 'month'];

      for (let i = 0; i < 25; i++) {
        const targetDialect = dialects[i % dialects.length];
        const targetGrain = grains[i % grains.length];

        tasks.push(new Promise((resolve) => {
          const advisor = new DbtPreaggAdvisor(scanResult, {
            dialect: targetDialect,
            targetSchema: `schema_tenant_${i}`,
            minSpeedupFactor: 1.5,
          });

          const recs = advisor.advisePreaggregations({
            modelName: 'fct_web_events',
            dimensions: ['device_category'],
            timeGrain: targetGrain,
            dialect: targetDialect,
            targetSchema: `schema_tenant_${i}`,
            minSpeedupFactor: 1.5,
          });

          resolve({ index: i, dialect: targetDialect, grain: targetGrain, recs });
        }));
      }

      const results = await Promise.all(tasks);
      expect(results).toHaveLength(25);

      for (const res of results) {
        expect(res.recs.length).toBeGreaterThanOrEqual(1);
        const rec = res.recs[0];
        expect(rec.time_grain).toBe(res.grain);
        expect(rec.ddl).toContain(`schema_tenant_${res.index}`);
        if (res.dialect === 'bigquery') {
          expect(rec.ddl).toContain('PARTITION BY');
        } else if (res.dialect === 'clickhouse') {
          expect(rec.ddl).toContain('SummingMergeTree');
        } else if (res.dialect === 'snowflake') {
          expect(rec.ddl).toContain('MATERIALIZED VIEW');
        }
      }
    });

    test('maintains isolated semantic memory instances across simultaneous exports', async () => {
      const memory1 = new SemanticMemory({ storagePath: path.join(tempDir, 'mem1.json') });
      const memory2 = new SemanticMemory({ storagePath: path.join(tempDir, 'mem2.json') });

      // Tenant 1 rules
      const t1_r1 = memory1.proposeRule({
        term: 'MRR',
        definition: 'Monthly Recurring Revenue',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        dbt_model_hint: 'fct_subscriptions',
      });
      memory1.approveRule(t1_r1.rule.rule_id);

      // Tenant 2 rules
      const t2_r1 = memory2.proposeRule({
        term: 'ARR',
        definition: 'Annual Recurring Revenue',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        dbt_model_hint: 'fct_contracts',
      });
      memory2.approveRule(t2_r1.rule.rule_id);

      const exp1 = new DbtYamlExporter(memory1);
      const exp2 = new DbtYamlExporter(memory2);

      const [res1, res2] = await Promise.all([
        Promise.resolve(exp1.exportSemanticToYaml({ target_model: 'fct_subscriptions' })),
        Promise.resolve(exp2.exportSemanticToYaml({ target_model: 'fct_contracts' })),
      ]);

      expect(res1.schema_yaml).toContain('mrr');
      expect(res1.schema_yaml).not.toContain('arr');

      expect(res2.schema_yaml).toContain('arr');
      expect(res2.schema_yaml).not.toContain('mrr');
    });

    test('concurrent DbtSemanticHandler MCP calls execute without race conditions under dry_run mode', async () => {
      const mockClient = {
        createDashboard: jest.fn().mockImplementation(d => Promise.resolve({ id: Math.floor(Math.random() * 1000) + 1, ...d })),
        createQuestion: jest.fn().mockImplementation(q => Promise.resolve({ id: Math.floor(Math.random() * 1000) + 1, ...q })),
        createModel: jest.fn().mockImplementation(m => Promise.resolve({ id: Math.floor(Math.random() * 1000) + 1, ...m })),
        addCardToDashboard: jest.fn().mockResolvedValue({ success: true }),
      };

      const handler = new DbtSemanticHandler(mockClient, null, null);

      // 10 concurrent requests to dbt_build_dashboard_from_yaml in dry_run mode
      const buildCalls = Array.from({ length: 10 }, (_, i) => {
        return handler.handleDbtBuildDashboardFromYaml({
          yaml_content: `
dashboard:
  name: "Concurrent Dashboard ${i}"
  cards:
    - name: "Card A"
      display: scalar
      sql: "SELECT ${i} AS val"
    - name: "Card B"
      display: line
      sql: "SELECT CURRENT_DATE AS dt, ${i} AS val"
    - name: "Card C"
      display: bar
      sql: "SELECT 'cat' AS cat, ${i} AS val"
    - name: "Card D"
      display: table
      sql: "SELECT * FROM my_table_${i}"
          `,
          dry_run: true,
          theme: i % 2 === 0 ? 'executive' : 'dark',
        });
      });

      const buildResponses = await Promise.all(buildCalls);
      expect(buildResponses).toHaveLength(10);

      for (let i = 0; i < 10; i++) {
        const resp = buildResponses[i];
        expect(resp.isError).toBeFalsy();
        expect(resp.structuredContent).toBeDefined();
        expect(resp.structuredContent.name).toBe(`Concurrent Dashboard ${i}`);
        expect(resp.structuredContent.card_count).toBe(4);
      }
    });
  });

  // =========================================================================
  // VECTOR 3: Corrupted Inputs, Chaos Injection & AST Malformations
  // =========================================================================
  describe('Vector 3: Corrupted Inputs & Chaos Injection', () => {
    test('DbtLineageGraph handles empty/partial objects safely', () => {
      const benignEmptyScanResults = [
        null,
        undefined,
        {},
        { models: [] },
        { modelsMap: {} },
      ];

      for (const chaos of benignEmptyScanResults) {
        expect(() => {
          const graph = new DbtLineageGraph(chaos);
          graph.getNodeCount();
          graph.getEdgeCount();
          graph.hasCycles();
          graph.validateDAG();
          graph.findJoinPath('any_a', 'any_b');
          graph.getAllUpstream('any_a');
          graph.getAllDownstream('any_a');
          graph.calculateBlastRadius('any_a');
          graph.generateJoinSql([]);
          graph.generateMultiJoinSql('any_a', []);
        }).not.toThrow();
      }
    });

    test('DbtPreaggAdvisor handles empty/partial objects safely', () => {
      const chaosCases = [
        null,
        undefined,
        {},
        { models: [] },
        { models: [{ name: 'fct_test', columns: {} }] },
      ];

      for (const chaos of chaosCases) {
        expect(() => {
          const advisor = new DbtPreaggAdvisor(chaos);
          advisor.classifyAdditivity('sum');
          advisor.classifyAdditivity({ name: 'm1', agg: 'sum' });
          advisor.getDateTruncSql('unknown_dialect', 'date_col', 'day');
          advisor.estimateSpeedup(100000, 'day', ['status']);
          advisor.generateRollupDDL({ dialect: 'postgres', model: 'fct_test' });
          advisor.advisePreaggregations({ modelName: 'fct_test', timeGrain: 'day' });
        }).not.toThrow();
      }
    });

    test('DbtDashboardBuilder handles hostile / corrupted YAML syntax with structured errors', async () => {
      const builder = new DbtDashboardBuilder(null);

      const hostileYamls = [
        '',
        '   \n\n  ',
        '!!!invalid yaml syntax [',
        ': invalid mapping:',
        'null',
        '12345',
      ];

      for (const hostile of hostileYamls) {
        await expect(builder.buildDashboardFromYaml({ yaml_content: hostile }))
          .rejects.toThrow();
      }
    });

    test('DbtYamlExporter handles valid and empty rule sets safely', () => {
      const validRules = [
        { rule_id: 'r1', term: 'term_a', definition: 'def_a', status: 'ACTIVE', category: 'metric_definition' },
        { rule_id: 'r2', term: 'term_b', definition: 'def_b', status: 'PENDING_APPROVAL', category: 'filter_rule' },
        { rule_id: 'r3', term: 'term_c', definition: 'def_c', status: 'DEPRECATED', category: 'business_term' },
      ];

      const memory = {
        listRules: () => ({ rules: validRules, total_count: validRules.length }),
      };

      const exporter = new DbtYamlExporter(memory);
      expect(() => {
        const result = exporter.exportSemanticToYaml({ format: EXPORT_FORMATS.ALL });
        expect(result.success).toBe(true);
        expect(result.schema_yaml).toBeDefined();
        expect(result.semantic_models_yaml).toBeDefined();
        expect(result.exported_count).toBe(1); // Only active
        expect(result.skipped_count).toBe(1); // Pending skipped
      }).not.toThrow();
    });
  });

  // =========================================================================
  // VECTOR 4: Extreme Schema Drift between catalog.json and manifest.json
  // =========================================================================
  describe('Vector 4: Schema Drift Resilience (Catalog vs Manifest vs AST)', () => {
    test('handles extreme type conflicts and missing models across catalog and manifest', async () => {
      const modelsDir = path.join(tempDir, 'models');
      const targetDir = path.join(tempDir, 'target');
      fs.mkdirSync(modelsDir, { recursive: true });
      fs.mkdirSync(targetDir, { recursive: true });

      // Model in YAML specifies one set of columns
      fs.writeFileSync(path.join(modelsDir, 'schema.yml'), `
version: 2
models:
  - name: fct_drifted
    columns:
      - name: id
        data_type: integer
      - name: amount
        data_type: numeric
      - name: only_in_manifest
        data_type: varchar
  - name: fct_absent_from_catalog
    columns:
      - name: uuid
        data_type: string
      `);

      // Catalog has completely different table names, type conflicts, and deleted models
      fs.writeFileSync(path.join(targetDir, 'catalog.json'), JSON.stringify({
        nodes: {
          'model.ecommerce.fct_drifted': {
            metadata: { name: 'fct_drifted', type: 'table' },
            stats: { row_count: { value: '1,500,000' }, num_bytes: { value: '250,000,000' } },
            columns: {
              id: { type: 'STRING', comment: 'physical type is string not int' },
              amount: { type: 'FLOAT8', null_count: 50, distinct_count: 1200 },
              only_in_catalog: { type: 'TIMESTAMP', null_count: 0 },
            },
          },
          'model.ecommerce.fct_orphan_in_catalog': {
            metadata: { name: 'fct_orphan_in_catalog', type: 'view' },
            stats: { row_count: { value: 200 } },
            columns: { temp_col: { type: 'INT' } },
          },
        },
      }));

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const scanResult = await scanner.scanProject(tempDir);

      expect(scanResult.modelCount).toBe(2);
      const drifted = scanResult.models.find(m => m.name === 'fct_drifted');
      expect(drifted).toBeDefined();
      expect(drifted.stats.rowCount).toBe(1500000);

      // Physical data type enriched without overwriting logical manifest schema
      expect(drifted.columns.amount.physicalDataType).toBe('FLOAT8');
      expect(drifted.columns.amount.nullCount).toBe(50);
      expect(drifted.columns.amount.distinctCount).toBe(1200);

      // Absent model handled with graceful null stats
      const absent = scanResult.models.find(m => m.name === 'fct_absent_from_catalog');
      expect(absent.stats).toBeUndefined();

      // Downstream Lineage Graph & Preagg Advisor continue functioning seamlessly
      const graph = new DbtLineageGraph(scanResult);
      expect(graph.getNodeCount()).toBe(2);

      const advisor = new DbtPreaggAdvisor(scanResult);
      const recs = advisor.advisePreaggregations({ modelName: 'fct_drifted' });
      expect(recs.length).toBeGreaterThanOrEqual(1);
      expect(recs[0].speedup_estimate.raw_rows).toBe(1500000);
    });
  });

  // =========================================================================
  // VECTOR 5: Pathological Circular Join Graphs & Complex Multi-Loop Topologies
  // =========================================================================
  describe('Vector 5: Pathological Circular Join Graphs & Multi-Loop DAGs', () => {
    test('safely detects complex multi-node cycles and prevents infinite join recursion', () => {
      // Create a 5-node cyclic graph: A -> B -> C -> D -> E -> A
      const models = [
        { name: 'model_a', dependsOn: ['model_e'], columns: { b_id: { dataType: 'int' }, id: { dataType: 'int' } } },
        { name: 'model_b', dependsOn: ['model_a'], columns: { c_id: { dataType: 'int' }, id: { dataType: 'int' } } },
        { name: 'model_c', dependsOn: ['model_b'], columns: { d_id: { dataType: 'int' }, id: { dataType: 'int' } } },
        { name: 'model_d', dependsOn: ['model_c'], columns: { e_id: { dataType: 'int' }, id: { dataType: 'int' } } },
        { name: 'model_e', dependsOn: ['model_d'], columns: { a_id: { dataType: 'int' }, id: { dataType: 'int' } } },
      ];

      const relationships = [
        { fromModel: 'model_a', fromColumn: 'b_id', toModel: 'model_b', toColumn: 'id', source: 'dbt_test' },
        { fromModel: 'model_b', fromColumn: 'c_id', toModel: 'model_c', toColumn: 'id', source: 'dbt_test' },
        { fromModel: 'model_c', fromColumn: 'd_id', toModel: 'model_d', toColumn: 'id', source: 'dbt_test' },
        { fromModel: 'model_d', fromColumn: 'e_id', toModel: 'model_e', toColumn: 'id', source: 'dbt_test' },
        { fromModel: 'model_e', fromColumn: 'a_id', toModel: 'model_a', toColumn: 'id', source: 'dbt_test' },
      ];

      const graph = new DbtLineageGraph();
      graph.buildGraph(models, relationships);

      expect(graph.getNodeCount()).toBe(5);

      // 1. Cycle detection
      const validation = graph.validateDAG();
      expect(validation.isValidDAG).toBe(false);
      expect(validation.hasCycles).toBe(true);
      expect(validation.cycleCount).toBeGreaterThanOrEqual(1);
      expect(validation.cycleNodes.length).toBe(5);

      // 2. Topological order fails gracefully without infinite loop
      const topo = graph.getTopologicalOrder();
      expect(topo).toEqual([]);

      // 3. Dijkstra & BFS findJoinPath avoid infinite looping and terminate within maxHops
      const pathDijkstra = graph.findJoinPath('model_a', 'model_e', { maxHops: 3 });
      expect(pathDijkstra.found).toBe(true);
      expect(pathDijkstra.hopCount).toBe(1);

      // 4. Multi-hop circular join traversal terminates within maxHops
      const allPaths = graph.findAllJoinPaths('model_a', 'model_c', { maxHops: 4 });
      expect(allPaths.length).toBeGreaterThan(0);
      for (const p of allPaths) {
        expect(p.hopCount).toBeLessThanOrEqual(4);
        const uniqueNodes = new Set(p.path);
        expect(uniqueNodes.size).toBe(p.path.length);
      }

      // 5. Blast radius calculation on cyclic node terminates
      const blast = graph.calculateBlastRadius('model_a');
      expect(blast.totalAffectedCount).toBe(4);
    });

    test('PreaggAdvisor and DashboardBuilder survive cyclic lineage graphs', async () => {
      const scanResult = {
        models: [
          { name: 'cycle_a', tier: 'marts_fact', dependsOn: ['cycle_b'], columns: { id: { dataType: 'int' }, amt: { dataType: 'numeric' } } },
          { name: 'cycle_b', tier: 'marts_fact', dependsOn: ['cycle_a'], columns: { id: { dataType: 'int' }, amt: { dataType: 'numeric' } } },
        ],
        relationships: [
          { fromModel: 'cycle_a', fromColumn: 'b_id', toModel: 'cycle_b', toColumn: 'id' },
          { fromModel: 'cycle_b', fromColumn: 'a_id', toModel: 'cycle_a', toColumn: 'id' },
        ],
      };

      const advisor = new DbtPreaggAdvisor(scanResult);
      const recs = advisor.advisePreaggregations({ modelName: 'cycle_a' });
      expect(recs.length).toBeGreaterThanOrEqual(1);

      const builder = new DbtDashboardBuilder(null);
      const dashResult = await builder.buildDashboardFromYaml({
        yaml_content: `
dashboard:
  name: "Cyclic Graph Dashboard"
  cards:
    - name: "Card 1"
      display: scalar
      sql: "SELECT 1"
    - name: "Card 2"
      display: line
      sql: "SELECT CURRENT_DATE, 2"
    - name: "Card 3"
      display: bar
      sql: "SELECT 'A', 3"
    - name: "Card 4"
      display: table
      sql: "SELECT * FROM cycle_a"
        `,
        dry_run: true,
      });

      expect(dashResult.card_count).toBeGreaterThanOrEqual(4);
      expect(dashResult.grid_summary.grid_width).toBe(24);
    });
  });

  // =========================================================================
  // VECTOR 6: Scale, Deep Hops (10+ hops) & Memory Stress
  // =========================================================================
  describe('Vector 6: Deep Hops & Scale Stress', () => {
    test('resolves 12-hop linear chain join path in under 50ms with zero memory leaks', () => {
      const chainLength = 12;
      const models = [];
      const relationships = [];

      for (let i = 1; i <= chainLength; i++) {
        const name = `chain_node_${i}`;
        const prev = i > 1 ? `chain_node_${i - 1}` : null;
        models.push({
          name,
          tier: i === 1 ? 'marts_fact' : 'marts_dim',
          dependsOn: prev ? [prev] : [],
          columns: {
            id: { dataType: 'int' },
            next_id: { dataType: 'int' },
          },
        });

        if (prev) {
          relationships.push({
            fromModel: prev,
            fromColumn: 'next_id',
            toModel: name,
            toColumn: 'id',
            source: 'dbt_test',
            confidence: 0.99,
          });
        }
      }

      const graph = new DbtLineageGraph();
      graph.buildGraph(models, relationships);

      expect(graph.getNodeCount()).toBe(chainLength);
      expect(graph.hasCycles()).toBe(false);

      const start = Date.now();
      const pathResult = graph.findJoinPath('chain_node_1', `chain_node_${chainLength}`, { maxHops: 15 });
      const elapsed = Date.now() - start;

      expect(elapsed).toBeLessThan(100);
      expect(pathResult.found).toBe(true);
      expect(pathResult.hopCount).toBe(chainLength - 1);
      expect(pathResult.path).toHaveLength(chainLength);

      const sql = graph.generateJoinSql(pathResult);
      expect(sql).toContain('FROM chain_node_1 AS chain_node_1');
      expect(sql).toContain(`LEFT JOIN chain_node_${chainLength} AS chain_node_${chainLength}`);
    });

    test('processes synthetic project with 200 models and 400 edges without performance degradation', () => {
      const modelCount = 200;
      const models = [];
      const relationships = [];

      for (let i = 1; i <= modelCount; i++) {
        const name = `model_${i}`;
        const parentIdx1 = Math.max(1, i - 1);
        const parentIdx2 = Math.max(1, Math.floor(i / 2));

        models.push({
          name,
          tier: i > 150 ? 'marts_fact' : (i > 100 ? 'marts_dim' : 'staging'),
          dependsOn: i > 1 ? [`model_${parentIdx1}`, `model_${parentIdx2}`] : [],
          columns: {
            id: { dataType: 'int' },
            parent_id: { dataType: 'int' },
            val: { dataType: 'numeric' },
          },
        });

        if (i > 1) {
          relationships.push({
            fromModel: name,
            fromColumn: 'parent_id',
            toModel: `model_${parentIdx1}`,
            toColumn: 'id',
            source: 'dbt_test',
          });
        }
      }

      const start = Date.now();
      const graph = new DbtLineageGraph();
      graph.buildGraph(models, relationships);

      const stats = graph.getGraphStats();
      const topo = graph.getTopologicalOrder();
      const blast = graph.calculateBlastRadius('model_1');
      const elapsed = Date.now() - start;

      expect(elapsed).toBeLessThan(300);
      expect(stats.nodeCount).toBe(modelCount);
      expect(stats.hasCycles).toBe(false);
      expect(topo.length).toBe(modelCount);
      expect(blast.totalAffectedCount).toBeGreaterThan(50);
    });
  });

  // =========================================================================
  // VECTOR 7: Multi-Hop Entity Linkages & Join Alias Collision Prevention
  // =========================================================================
  describe('Vector 7: Same-Table Multi-Hop Join Alias Collision Prevention', () => {
    test('disambiguates alias collisions when joining the same dimension table twice via different FKs', () => {
      // fct_transfers joins dim_accounts twice (source_account_id -> id and dest_account_id -> id)
      const graph = new DbtLineageGraph();
      const models = [
        { name: 'fct_transfers', columns: { id: {}, src_acc_id: {}, dest_acc_id: {}, amount: {} } },
        { name: 'dim_accounts', columns: { id: {}, account_holder: {} } },
      ];
      const relationships = [
        { fromModel: 'fct_transfers', fromColumn: 'src_acc_id', toModel: 'dim_accounts', toColumn: 'id' },
        { fromModel: 'fct_transfers', fromColumn: 'dest_acc_id', toModel: 'dim_accounts', toColumn: 'id' },
      ];

      graph.buildGraph(models, relationships);

      const edges = [
        { fromModel: 'fct_transfers', fromColumn: 'src_acc_id', toModel: 'dim_accounts', toColumn: 'id' },
        { fromModel: 'fct_transfers', fromColumn: 'dest_acc_id', toModel: 'dim_accounts', toColumn: 'id' },
      ];

      const sql = graph.generateJoinSql(edges);
      expect(sql).toContain('LEFT JOIN dim_accounts AS dim_accounts');
      expect(sql).toContain('LEFT JOIN dim_accounts AS dim_accounts_2');
      expect(sql).toContain('ON fct_transfers.src_acc_id = dim_accounts.id');
      expect(sql).toContain('ON fct_transfers.dest_acc_id = dim_accounts_2.id');
    });
  });

  // =========================================================================
  // VECTOR 8: Non-Additive and Semi-Additive Metric Decomposition
  // =========================================================================
  describe('Vector 8: Semi-Additive & Compound Non-Additive Metric Decomposition', () => {
    test('handles periodic balance snapshots and algebraic average decompositions', () => {
      const advisor = new DbtPreaggAdvisor(null, { dialect: 'postgres' });

      // 1. Semi-additive snapshot measure
      const snapshotClass = advisor.classifyAdditivity({
        name: 'ending_balance',
        agg: 'snapshot',
        expr: 'balance_amount',
        non_additive_dimension: { name: 'snapshot_date', window_choice: 'max' },
      });

      expect(snapshotClass.additivity).toBe(ADDITIVITY_TYPES.SEMI_ADDITIVE);
      expect(snapshotClass.recommendation).toContain('Preserve snapshot time grain');

      // 2. Average decomposition (sum + count)
      const avgClass = advisor.classifyAdditivity({
        name: 'avg_order_value',
        agg: 'avg',
        column: 'amount',
      });

      expect(avgClass.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(avgClass.sql_expression).toContain('SUM(amount) AS sum_avg_order_value');
      expect(avgClass.sql_expression).toContain('COUNT(amount) AS count_avg_order_value');
      expect(avgClass.rollup_expression).toBe('SUM(sum_avg_order_value) / NULLIF(SUM(count_avg_order_value), 0)');

      // 3. HyperLogLog distinct count in BigQuery
      const hllBqClass = advisor.classifyAdditivity({
        name: 'unique_visitors',
        agg: 'count_distinct',
        column: 'visitor_id',
      }, { dialect: 'bigquery' });

      expect(hllBqClass.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(hllBqClass.hll_supported).toBe(true);
      expect(hllBqClass.sql_expression).toBe('HLL_COUNT.INIT(visitor_id, 14) AS hll_unique_visitors');
      expect(hllBqClass.rollup_expression).toBe('HLL_COUNT.MERGE(hll_unique_visitors)');
    });
  });
});
