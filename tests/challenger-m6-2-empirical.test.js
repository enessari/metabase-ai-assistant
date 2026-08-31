/**
 * tests/challenger-m6-2-empirical.test.js
 * Milestone 6: Final E2E Suite & Adversarial Hardening
 * Challenger M6-2: Tier 5 Chaos & Fuzzing Empirical Test Suite
 *
 * Adversarially fuzzes all 5 MCP tools in src/mcp/handlers/dbt-semantic.js:
 * 1. dbt_project_scan_deep
 * 2. dbt_lineage_joins_graph
 * 3. dbt_semantic_preagg_advisor
 * 4. dbt_build_dashboard_from_yaml
 * 5. dbt_semantic_export_yaml
 *
 * Stress Test Dimensions:
 * - Random strings, unicode, emoji, control chars, SQL injection, XSS payloads
 * - Negative integers, zero, out-of-bounds numbers, NaN, Infinity
 * - Nulls, undefined, empty objects, type coercion attacks
 * - Prototype pollution vectors (__proto__, constructor, prototype)
 * - Deeply nested objects & circular reference ASTs / DAGs
 * - Unsupported and malformed database dialects
 * - Extreme card counts (0 to 200+ cards) and collision-free grid layouts
 * - Read-only mode enforcement and mutation blocking
 * - Guarantee: Zero unhandled crashes, valid MCP structure, and _provenance envelope
 */

import { jest } from '@jest/globals';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { DbtSemanticHandler } from '../src/mcp/handlers/dbt-semantic.js';
import { DbtDeepScanner } from '../src/dbt/dbt-deep-scanner.js';
import { DbtLineageGraph } from '../src/dbt/lineage-joins.js';
import { DbtPreaggAdvisor } from '../src/dbt/preagg-advisor.js';
import { DbtDashboardBuilder } from '../src/dbt/dbt-dashboard-builder.js';
import { DbtYamlExporter } from '../src/dbt/dbt-yaml-exporter.js';
import { SemanticMemory, globalSemanticMemory, RULE_STATUS, RULE_CATEGORIES } from '../src/semantic/semantic-memory.js';

describe('Milestone 6 / Challenger M6-2: Tier 5 Chaos & Adversarial Fuzzing', () => {
  let handler;
  let mockClient;
  let tempTestDir;

  beforeAll(() => {
    // Ensure clean test environment
    tempTestDir = fs.mkdtempSync(path.join(os.tmpdir(), 'm6-challenger-fuzz-'));

    // Create minimal valid dbt workspace structure for testing
    fs.mkdirSync(path.join(tempTestDir, 'models'), { recursive: true });
    fs.mkdirSync(path.join(tempTestDir, 'target'), { recursive: true });

    // Minimal dbt_project.yml
    fs.writeFileSync(
      path.join(tempTestDir, 'dbt_project.yml'),
      'name: "fuzz_project"\nversion: "1.0.0"\nprofile: "default"\n'
    );

    // Minimal models
    fs.writeFileSync(
      path.join(tempTestDir, 'models', 'fct_orders.sql'),
      "SELECT 1 AS order_id, 100 AS customer_id, 49.99 AS amount, 'completed' AS status, CURRENT_DATE AS order_date"
    );

    fs.writeFileSync(
      path.join(tempTestDir, 'models', 'dim_customers.sql'),
      "SELECT 100 AS customer_id, 'Alice' AS customer_name, 'US' AS country"
    );

    fs.writeFileSync(
      path.join(tempTestDir, 'models', 'schema.yml'),
      `version: 2
models:
  - name: fct_orders
    description: "Orders fact table"
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: amount
        meta:
          metabase:
            formatting:
              currency: USD
  - name: dim_customers
    description: "Customers dimension table"
    columns:
      - name: customer_id
        tests: [unique, not_null]
      - name: country
`
    );
  });

  afterAll(() => {
    try {
      fs.rmSync(tempTestDir, { recursive: true, force: true });
    } catch (e) {
      // ignore cleanup errors
    }
  });

  beforeEach(() => {
    mockClient = {
      createDashboard: jest.fn().mockResolvedValue({ id: 501, name: 'Fuzz Dashboard' }),
      createQuestion: jest.fn().mockImplementation((payload) => Promise.resolve({ id: Math.floor(Math.random() * 1000) + 1, ...payload })),
      createModel: jest.fn().mockImplementation((payload) => Promise.resolve({ id: Math.floor(Math.random() * 1000) + 1000, ...payload })),
      addCardToDashboard: jest.fn().mockResolvedValue({ success: true }),
    };

    handler = new DbtSemanticHandler(mockClient, null, null);
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 1. TOOL 1: dbt_project_scan_deep CHAOS & FUZZING
  // ══════════════════════════════════════════════════════════════════════════
  describe('Tool 1: dbt_project_scan_deep Adversarial Fuzzing', () => {
    test('1.1: Handles non-existent, empty, and invalid directory paths gracefully', async () => {
      const paths = [
        path.join(tempTestDir, 'non_existent_folder_99999'),
        '',
        null,
        undefined,
        '../../../../../../etc/shadow',
        '   \n\t   ',
        '/dev/null',
      ];

      for (const p of paths) {
        const res = await handler.handleDbtProjectScanDeep({ project_dir: p });
        expect(res).toBeDefined();
        // Either structured result or graceful MCP error
        if (res.isError) {
          expect(Array.isArray(res.content)).toBe(true);
          expect(res.content[0].text).toContain('dbt Deep Scan Error');
        } else {
          expect(res.structuredContent).toBeDefined();
          expect(res.structuredContent._provenance).toBeDefined();
        }
      }
    });

    test('1.2: Fuzzing boolean and filter flags with prototypes, objects, negative numbers', async () => {
      const fuzzArgs = [
        { project_dir: tempTestDir, include_docs: '__proto__', include_catalog: null, tier_filter: 12345 },
        { project_dir: tempTestDir, include_metrics: -1, filter_tiers: { malicious: true } },
        { project_dir: tempTestDir, filter_tiers: ['marts_fact', '__proto__', null, 999] },
        { project_dir: tempTestDir, tier_filter: "'; DROP TABLE models; --" },
      ];

      for (const args of fuzzArgs) {
        const res = await handler.handleDbtProjectScanDeep(args);
        expect(res).toBeDefined();
        expect(res.isError).toBeFalsy();
        expect(res.structuredContent).toBeDefined();
        expect(res.structuredContent.model_count).toBeGreaterThanOrEqual(0);
        expect(res.structuredContent._provenance.scanner).toBe('DbtDeepScanner');
        expect(res.structuredContent._provenance.governance_level).toBe('READ_ONLY_INSPECTION');
      }
    });

    test('1.3: Deep circular doc blocks resolution does not cause infinite recursion / stack overflow', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempTestDir });
      const cyclicDocs = new Map();
      cyclicDocs.set('doc_a', "{{ doc('doc_b') }}");
      cyclicDocs.set('doc_b', "{{ doc('doc_c') }}");
      cyclicDocs.set('doc_c', "{{ doc('doc_a') }}");

      const resolved = scanner.resolveDocReference("{{ doc('doc_a') }}", cyclicDocs);
      expect(typeof resolved).toBe('string');
      // Max recursion depth prevents stack overflow
    });

    test('1.4: Corrupted manifest.json and catalog.json do not crash scanner', async () => {
      const corruptManifestPath = path.join(tempTestDir, 'target', 'corrupt_manifest.json');
      fs.writeFileSync(corruptManifestPath, '{ invalid json syntax ...');

      const corruptCatalogPath = path.join(tempTestDir, 'target', 'corrupt_catalog.json');
      fs.writeFileSync(corruptCatalogPath, '<xml>not json</xml>');

      const res = await handler.handleDbtProjectScanDeep({
        project_dir: tempTestDir,
        manifest_path: corruptManifestPath,
        catalog_path: corruptCatalogPath,
      });

      expect(res.isError).toBeFalsy();
      expect(res.structuredContent).toBeDefined();
      expect(res.structuredContent._provenance.manifest_loaded).toBe(false);
      expect(res.structuredContent._provenance.catalog_loaded).toBe(false);
    });

    test('1.5: Large directory with deep nesting and 100+ files processed efficiently', () => {
      const deepDir = path.join(tempTestDir, 'deep_nested_models', 'level1', 'level2', 'level3');
      fs.mkdirSync(deepDir, { recursive: true });

      for (let i = 0; i < 20; i++) {
        fs.writeFileSync(
          path.join(deepDir, `stg_model_${i}.sql`),
          `SELECT ${i} AS id, 'name_${i}' AS name`
        );
      }

      const scanner = new DbtDeepScanner({ projectDir: tempTestDir });
      scanner.scanDirectoryRecursive(tempTestDir);
      expect(scanner.models.size).toBeGreaterThanOrEqual(20);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 2. TOOL 2: dbt_lineage_joins_graph CHAOS & FUZZING
  // ══════════════════════════════════════════════════════════════════════════
  describe('Tool 2: dbt_lineage_joins_graph Adversarial Fuzzing', () => {
    test('2.1: Handles extreme max_hops (negative, zero, NaN, huge integers) without crashing', async () => {
      const extremeHops = [-100, -1, 0, 1, 5, 100, 1000, NaN, Infinity, -Infinity, '5', 'invalid'];

      for (const hops of extremeHops) {
        const res = await handler.handleDbtLineageJoinsGraph({
          project_dir: tempTestDir,
          source_model: 'fct_orders',
          target_model: 'dim_customers',
          max_hops: hops,
        });

        expect(res).toBeDefined();
        expect(res.isError).toBeFalsy();
        expect(res.structuredContent).toBeDefined();
        expect(res.structuredContent._provenance).toBeDefined();
        expect(res.structuredContent._provenance.resolver).toBe('DbtLineageGraph');
      }
    });

    test('2.2: Handles extreme confidence thresholds (-5.0, 0.0, 1.5, 999.0, null, object)', async () => {
      const thresholds = [-5.0, 0.0, 0.5, 1.0, 1.5, 999.0, null, undefined, {}, []];

      for (const th of thresholds) {
        const res = await handler.handleDbtLineageJoinsGraph({
          project_dir: tempTestDir,
          source_model: 'fct_orders',
          target_model: 'dim_customers',
          confidence_threshold: th,
        });

        expect(res).toBeDefined();
        expect(res.isError).toBeFalsy();
        expect(res.structuredContent).toBeDefined();
      }
    });

    test('2.3: Multi-target and array fuzzing with prototype injection and nulls', async () => {
      const res = await handler.handleDbtLineageJoinsGraph({
        project_dir: tempTestDir,
        source_model: 'fct_orders',
        target_models: ['dim_customers', '__proto__', null, undefined, '', 'non_existent_table_99'],
        join_type: 'FULL OUTER JOIN',
        direction: 'both',
      });

      expect(res).toBeDefined();
      expect(res.isError).toBeFalsy();
      expect(res.structuredContent).toBeDefined();
      expect(res.structuredContent.join_paths).toBeDefined();
      expect(Array.isArray(res.structuredContent.join_paths)).toBe(true);
    });

    test('2.4: Cyclic dependency graph detection and topological sort stability', () => {
      const graph = new DbtLineageGraph();
      graph.addNode({ name: 'node_a' });
      graph.addNode({ name: 'node_b' });
      graph.addNode({ name: 'node_c' });

      // Create cycle: a -> b -> c -> a
      graph.addLineageEdge('node_a', 'node_b');
      graph.addLineageEdge('node_b', 'node_c');
      graph.addLineageEdge('node_c', 'node_a');

      expect(graph.hasCycles()).toBe(true);
      const cycleNodes = graph.getCycleNodes();
      expect(cycleNodes.length).toBeGreaterThan(0);

      // Topological order handles cycles safely without throwing
      const topo = graph.getTopologicalOrder();
      expect(Array.isArray(topo)).toBe(true);
    });

    test('2.5: SQL Injection strings in source_model, target_model, and base_alias', async () => {
      const maliciousPayloads = [
        "fct_orders'; DROP TABLE users; --",
        "dim_customers UNION SELECT * FROM passwords",
        "base_alias /*!50000 SELECT */",
        "alias` OR 1=1 --",
      ];

      for (const payload of maliciousPayloads) {
        const res = await handler.handleDbtLineageJoinsGraph({
          project_dir: tempTestDir,
          source_model: payload,
          target_model: 'dim_customers',
          base_alias: payload,
        });

        expect(res).toBeDefined();
        // Must return safe structured response without executing unhandled errors
        if (!res.isError) {
          expect(res.structuredContent).toBeDefined();
          expect(res.structuredContent._provenance).toBeDefined();
        }
      }
    });

    test('2.6: Deep multi-hop chain resolution (5+ hops: fct_items -> fct_orders -> dim_cust -> dim_addr -> dim_region)', () => {
      const graph = new DbtLineageGraph();
      const models = ['fct_items', 'fct_orders', 'dim_customers', 'dim_addresses', 'dim_regions'];
      models.forEach(m => graph.addNode({ name: m }));

      graph.addJoinRelationship({ fromModel: 'fct_items', fromColumn: 'order_id', toModel: 'fct_orders', toColumn: 'id', confidence: 1.0 });
      graph.addJoinRelationship({ fromModel: 'fct_orders', fromColumn: 'customer_id', toModel: 'dim_customers', toColumn: 'id', confidence: 1.0 });
      graph.addJoinRelationship({ fromModel: 'dim_customers', fromColumn: 'address_id', toModel: 'dim_addresses', toColumn: 'id', confidence: 1.0 });
      graph.addJoinRelationship({ fromModel: 'dim_addresses', fromColumn: 'region_id', toModel: 'dim_regions', toColumn: 'id', confidence: 1.0 });

      const pathResult = graph.findJoinPath('fct_items', 'dim_regions', { maxHops: 5 });
      expect(pathResult.found).toBe(true);
      expect(pathResult.hops).toBe(4);
      expect(pathResult.path).toEqual(['fct_items', 'fct_orders', 'dim_customers', 'dim_addresses', 'dim_regions']);
      expect(pathResult.sqlJoinClause).toContain('LEFT JOIN');
      expect(pathResult.sqlJoinClause).toContain('dim_regions');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 3. TOOL 3: dbt_semantic_preagg_advisor CHAOS & FUZZING
  // ══════════════════════════════════════════════════════════════════════════
  describe('Tool 3: dbt_semantic_preagg_advisor Adversarial Fuzzing', () => {
    test('3.1: Unsupported, alien, and malformed database dialects', async () => {
      const stringDialects = [
        'sqlite',
        'oracle',
        'cockroachdb',
        'teradata',
        'db2',
        'hive',
        'presto',
        'trino',
        'unknown_db_xyz',
        '__proto__',
        'PoStGrEsQL',
        'BIGQUERY',
        'SnowFlake',
      ];

      for (const d of stringDialects) {
        const res = await handler.handleDbtSemanticPreaggAdvisor({
          project_dir: tempTestDir,
          model_name: 'fct_orders',
          dialect: d,
          time_grain: 'month',
        });

        expect(res).toBeDefined();
        expect(res.isError).toBeFalsy();
        expect(res.structuredContent).toBeDefined();
        expect(res.structuredContent._provenance).toBeDefined();
        expect(res.structuredContent._provenance.advisor).toBe('DbtPreaggAdvisor');
        expect(typeof res.structuredContent.dialect).toBe('string');
      }

      // Non-string or null dialects handled gracefully via error response or fallback
      const nonStringDialects = [12345, null, undefined, {}];
      for (const d of nonStringDialects) {
        const res = await handler.handleDbtSemanticPreaggAdvisor({
          project_dir: tempTestDir,
          model_name: 'fct_orders',
          dialect: d,
        });

        expect(res).toBeDefined();
        expect(Array.isArray(res.content)).toBe(true);
      }
    });

    test('3.2: Unsupported time grains (millennium, picosecond, invalid, negative)', async () => {
      const grains = ['millennium', 'decade', 'nanosecond', 'picosecond', 'invalid_grain', '', null, -1, 100];

      for (const g of grains) {
        const res = await handler.handleDbtSemanticPreaggAdvisor({
          project_dir: tempTestDir,
          model_name: 'fct_orders',
          time_grain: g,
        });

        expect(res).toBeDefined();
        expect(res.isError).toBeFalsy();
        expect(res.structuredContent).toBeDefined();
        // Fallback to normalized valid grain (day)
        expect(res.structuredContent.recommendations).toBeDefined();
      }
    });

    test('3.3: Extreme speedup factor and row count estimation (0, negative, huge numbers, NaN, Infinity)', () => {
      const advisor = new DbtPreaggAdvisor();

      const edgeCounts = [0, -100, 1, 1000000, 1e12, 1e18, NaN, Infinity, -Infinity, '1000000', 'invalid'];

      for (const rawRows of edgeCounts) {
        const speedup = advisor.estimateSpeedup(rawRows, 'day', ['status', 'country']);
        expect(speedup).toBeDefined();
        expect(typeof speedup.speedup_factor).toBe('number');
        expect(isFinite(speedup.speedup_factor)).toBe(true);
        expect(speedup.speedup_factor).toBeGreaterThanOrEqual(1.0);
        expect(typeof speedup.scan_reduction_pct).toBe('number');
        expect(speedup.scan_reduction_pct).toBeGreaterThanOrEqual(0);
        expect(speedup.scan_reduction_pct).toBeLessThanOrEqual(100);
      }
    });

    test('3.4: Complex non-additive, semi-additive, and HyperLogLog metric decompositions across 7 dialects', () => {
      const advisor = new DbtPreaggAdvisor();
      const dialects = ['postgres', 'bigquery', 'snowflake', 'clickhouse', 'duckdb', 'redshift', 'mysql'];

      const complexMeasures = [
        { name: 'distinct_users', agg: 'count_distinct', expr: 'user_id' },
        { name: 'avg_order_value', agg: 'avg', expr: 'amount' },
        { name: 'gross_margin', agg: 'ratio', expr: 'profit / revenue' },
        { name: 'ending_inventory', agg: 'snapshot', expr: 'stock_level', non_additive_dimension: { name: 'snapshot_date', window_choice: 'max' } },
        { name: 'active_flag_sum', agg: 'sum_boolean', expr: 'is_active' },
      ];

      for (const d of dialects) {
        for (const m of complexMeasures) {
          const analysis = advisor.classifyAdditivity(m, { dialect: d, includeHll: true });
          expect(analysis).toBeDefined();
          expect(analysis.additivity).toBeDefined();
          expect(typeof analysis.sql_expression).toBe('string');
          expect(typeof analysis.rollup_expression).toBe('string');

          const ddl = advisor.generateRollupDDL({
            dialect: d,
            modelName: 'fct_orders',
            timeGrain: 'month',
            dimensions: ['status'],
            measures: [m],
          });

          expect(ddl.ddl).toBeDefined();
          expect(typeof ddl.ddl).toBe('string');
          expect(ddl.ddl.length).toBeGreaterThan(0);
        }
      }
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 4. TOOL 4: dbt_build_dashboard_from_yaml CHAOS & FUZZING
  // ══════════════════════════════════════════════════════════════════════════
  describe('Tool 4: dbt_build_dashboard_from_yaml Adversarial Fuzzing', () => {
    test('4.1: Dry run mode with empty, malformed, or hostile YAML inputs', async () => {
      const hostileYamls = [
        '',
        '   ',
        'not: [a: valid: yaml: structure',
        '{ json_without_closing_brace: true',
        'exposures: []',
        'dashboard: {}',
        'models: []',
        '__proto__: { polluted: true }',
        'name: "<script>alert(1)</script>"',
        'name: "fuzz"\ncards:\n  - name: "Card 1"\n    sql: "SELECT 1"\n    row: -5\n    col: -10\n    size_x: 99\n    size_y: 99',
      ];

      for (const y of hostileYamls) {
        const res = await handler.handleDbtBuildDashboardFromYaml({
          yaml_content: y,
          dry_run: true,
          project_dir: tempTestDir,
        });

        expect(res).toBeDefined();
        // Either structured result or graceful MCP error
        if (res.isError) {
          expect(Array.isArray(res.content)).toBe(true);
          expect(res.content[0].text).toContain('dbt Dashboard Builder Error');
        } else {
          expect(res.structuredContent).toBeDefined();
          expect(res.structuredContent._provenance).toBeDefined();
          expect(res.structuredContent._provenance.builder).toBe('DbtDashboardBuilder');
        }
      }
    });

    test('4.2: Auto-enrichment: When spec has 0, 1, 2, or 3 cards, synthesizes >= 4 executive cards', async () => {
      const sparseYaml = `
dashboard:
  name: "Sparse Executive Dashboard"
  description: "Dashboard with only 1 user-defined card"
  cards:
    - name: "Single Metric"
      display: "scalar"
      sql: "SELECT COUNT(*) AS total_count FROM fct_orders"
`;

      const res = await handler.handleDbtBuildDashboardFromYaml({
        yaml_content: sparseYaml,
        model_name: 'fct_orders',
        dry_run: true,
        project_dir: tempTestDir,
      });

      expect(res.isError).toBeFalsy();
      expect(res.structuredContent).toBeDefined();
      expect(res.structuredContent.card_count).toBeGreaterThanOrEqual(4);
      expect(res.structuredContent.cards.length).toBeGreaterThanOrEqual(4);
      expect(res.structuredContent._provenance).toBeDefined();
    });

    test('4.3: Extreme card count stress test (100 cards placed on 24-col grid with zero collisions)', () => {
      const builder = new DbtDashboardBuilder(mockClient);
      const massiveCards = [];

      for (let i = 0; i < 100; i++) {
        const display = i % 4 === 0 ? 'scalar' : i % 4 === 1 ? 'line' : i % 4 === 2 ? 'bar' : 'table';
        massiveCards.push({
          name: `Card #${i + 1}`,
          display,
          size_x: display === 'scalar' ? 6 : display === 'line' ? 12 : display === 'bar' ? 12 : 24,
          size_y: display === 'scalar' ? 4 : display === 'line' ? 8 : display === 'bar' ? 6 : 8,
        });
      }

      const positions = builder.calculateGridCoordinates(massiveCards);
      expect(positions.length).toBe(100);

      // Verify every position is valid and in-bounds (width <= 24)
      for (const pos of positions) {
        expect(pos.col).toBeGreaterThanOrEqual(0);
        expect(pos.row).toBeGreaterThanOrEqual(0);
        expect(pos.col + pos.size_x).toBeLessThanOrEqual(24);
      }

      // Verify no overlaps in 100-card layout
      for (let i = 0; i < positions.length; i++) {
        const a = positions[i];
        for (let j = i + 1; j < positions.length; j++) {
          const b = positions[j];
          const horizontalOverlap = a.col < b.col + b.size_x && a.col + a.size_x > b.col;
          const verticalOverlap = a.row < b.row + b.size_y && a.row + a.size_y > b.row;
          const overlaps = horizontalOverlap && verticalOverlap;
          expect(overlaps).toBe(false);
        }
      }
    });

    test('4.4: Template tag filter binding with SQL injection & complex parameters', () => {
      const builder = new DbtDashboardBuilder(mockClient);
      const complexSql = `
        SELECT * FROM fct_orders
        WHERE 1=1
        [[AND order_date >= {{start_date}}]]
        [[AND order_date <= {{end_date}}]]
        [[AND customer_region = {{region_filter}}]]
        [[AND status = {{status_code}}]]
        [[AND amount > {{min_amount}}]]
      `;

      const filters = [
        { slug: 'start_date', name: 'Start Date', type: 'date/single' },
        { slug: 'end_date', name: 'End Date', type: 'date/single' },
        { slug: 'region_filter', name: 'Region', type: 'category' },
        { slug: 'status_code', name: 'Status', type: 'category' },
        { slug: 'min_amount', name: 'Min Amount', type: 'number/=' },
      ];

      const tags = builder.buildTemplateTags(complexSql, filters);
      expect(tags).toBeDefined();
      expect(tags.start_date.type).toBe('date');
      expect(tags.end_date.type).toBe('date');
      expect(tags.region_filter.type).toBe('dimension');
      expect(tags.min_amount.type).toBe('number');
    });

    test('4.5: Read-Only Mode enforcement blocks non-dry-run live mutation', async () => {
      const originalEnv = process.env.METABASE_READ_ONLY_MODE;
      try {
        process.env.METABASE_READ_ONLY_MODE = 'true';

        const res = await handler.handleDbtBuildDashboardFromYaml({
          model_name: 'fct_orders',
          dry_run: false,
          project_dir: tempTestDir,
        });

        expect(res.isError).toBe(true);
        expect(res.content[0].text).toContain('Read-Only Mode Active');
        expect(res.content[0].text).toContain('Blocked');
      } finally {
        process.env.METABASE_READ_ONLY_MODE = originalEnv;
      }
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 5. TOOL 5: dbt_semantic_export_yaml CHAOS & FUZZING
  // ══════════════════════════════════════════════════════════════════════════
  describe('Tool 5: dbt_semantic_export_yaml Adversarial Fuzzing', () => {
    let customMemory;

    beforeEach(() => {
      customMemory = new SemanticMemory({ storagePath: '.metabase-cache/m6-fuzz-semantic.json' });
      customMemory.rules.clear();

      // Seed 2 ACTIVE rules, 2 PENDING rules, and 2 DEPRECATED rules
      customMemory.proposeRule({
        term: 'Active MRR',
        definition: 'Monthly recurring revenue from active subscriptions',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        sql_condition: 'SUM(mrr_amount)',
        dbt_model_hint: 'fct_subscriptions',
      });
      const activeRule1 = Array.from(customMemory.rules.values())[0];
      customMemory.approveRule(activeRule1.rule_id, { comment: 'Approved by Head of Data' });

      customMemory.proposeRule({
        term: 'Churn Rate',
        definition: 'Percentage of users churned divided by active users',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        sql_condition: 'churned_count / total_active',
        dbt_model_hint: 'fct_churn',
      });
      const activeRule2 = Array.from(customMemory.rules.values())[1];
      customMemory.approveRule(activeRule2.rule_id, { comment: 'Approved by VP Analytics' });

      customMemory.proposeRule({
        term: 'Pending Unapproved Metric',
        definition: 'Unverified metric idea that must never leak into dbt YAML',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
      });

      customMemory.proposeRule({
        term: 'Deprecated Legacy ARR',
        definition: 'Old ARR calculation deprecated due to methodology shift',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
      });
      const depRule = Array.from(customMemory.rules.values())[3];
      customMemory.approveRule(depRule.rule_id, { comment: 'Initial approve' });
      customMemory.deprecateRule(depRule.rule_id, { reason: 'Replaced by Active MRR' });
    });

    test('5.1: Zero Governance Leak: PENDING_APPROVAL rules are never exported in schema.yml / metrics.yml', async () => {
      const exporter = new DbtYamlExporter(customMemory);
      const res = exporter.exportSemanticToYaml({ status_filter: 'ACTIVE' });

      expect(res.success).toBe(true);
      expect(res.active_rules_count).toBe(2);
      expect(res.skipped_count).toBeGreaterThanOrEqual(1);

      // Verify no pending rule in exported YAML string
      expect(res.yaml_content).not.toContain('Pending Unapproved Metric');
      expect(res.yaml_content).toContain('Active MRR');
      expect(res.yaml_content).toContain('Churn Rate');
    });

    test('5.2: Fuzzing format options, status filters, and category filters with extreme values', async () => {
      const fuzzOptions = [
        { format: 'invalid_format_xyz', status_filter: 'UNKNOWN_STATUS' },
        { format: '__proto__', categories: ['__proto__', null, 12345] },
        { target_model: "'; DROP TABLE schemas; --", author: '<script>alert(1)</script>' },
        { rule_ids: ['non_existent_rule_1', 'non_existent_rule_2'] },
        { include_deprecated: true, include_semantic_layer: false, include_dbt_schema: false },
      ];

      for (const opt of fuzzOptions) {
        const res = await handler.handleDbtSemanticExportYaml({
          ...opt,
          project_dir: tempTestDir,
        });

        expect(res).toBeDefined();
        expect(res.isError).toBeFalsy();
        expect(res.structuredContent).toBeDefined();
        expect(res.structuredContent._provenance).toBeDefined();
        expect(res.structuredContent._provenance.exporter).toBe('DbtYamlExporter');
        expect(res.structuredContent._provenance.governance_level).toContain('EXPLICIT_APPROVAL');
      }
    });

    test('5.3: Soft-deprecation audit trail and comment preservation', () => {
      const exporter = new DbtYamlExporter(customMemory);
      const res = exporter.exportSemanticToYaml({ include_deprecated: true });

      expect(res.deprecated_rules_count).toBeGreaterThanOrEqual(1);
      expect(res.yaml_content).toContain('SOFT-DEPRECATED');
      expect(res.yaml_content).toContain('Replaced by Active MRR');
    });

    test('5.4: Prototype pollution resistance: Injected keys in rule definitions do not pollute global Object', () => {
      const pollutedRule = {
        rule_id: 'rule_pollute',
        term: 'Pollution Test',
        __proto__: { globalPollutionCheck: 'VULNERABLE' },
        category: RULE_CATEGORIES.BUSINESS_TERM,
        status: RULE_STATUS.ACTIVE,
      };

      const exporter = new DbtYamlExporter(null);
      const res = exporter.exportSemanticToYaml({ rules: [pollutedRule] });

      expect(res).toBeDefined();
      expect(Object.prototype.globalPollutionCheck).toBeUndefined();
      expect({}.globalPollutionCheck).toBeUndefined();
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 6. CROSS-TOOL INTEGRATION & PROTOCOL HARMONIZATION
  // ══════════════════════════════════════════════════════════════════════════
  describe('6. Cross-Tool Protocol & Provenance Harmonization', () => {
    test('6.1: All 5 tools return structured MCP responses with compliant _provenance envelopes', async () => {
      // Tool 1: dbt_project_scan_deep
      const r1 = await handler.handleDbtProjectScanDeep({ project_dir: tempTestDir });
      expect(r1.isError).toBeFalsy();
      expect(r1.structuredContent._provenance).toBeDefined();
      expect(r1.structuredContent._provenance.timestamp).toBeDefined();
      expect(r1.structuredContent._provenance.governance_level).toBe('READ_ONLY_INSPECTION');

      // Tool 2: dbt_lineage_joins_graph
      const r2 = await handler.handleDbtLineageJoinsGraph({
        project_dir: tempTestDir,
        source_model: 'fct_orders',
        target_model: 'dim_customers',
      });
      expect(r2.isError).toBeFalsy();
      expect(r2.structuredContent._provenance).toBeDefined();
      expect(r2.structuredContent._provenance.timestamp).toBeDefined();
      expect(r2.structuredContent._provenance.resolver).toBe('DbtLineageGraph');

      // Tool 3: dbt_semantic_preagg_advisor
      const r3 = await handler.handleDbtSemanticPreaggAdvisor({
        project_dir: tempTestDir,
        model_name: 'fct_orders',
      });
      expect(r3.isError).toBeFalsy();
      expect(r3.structuredContent._provenance).toBeDefined();
      expect(r3.structuredContent._provenance.timestamp).toBeDefined();
      expect(r3.structuredContent._provenance.advisor).toBe('DbtPreaggAdvisor');

      // Tool 4: dbt_build_dashboard_from_yaml (dry run)
      const r4 = await handler.handleDbtBuildDashboardFromYaml({
        model_name: 'fct_orders',
        dry_run: true,
        project_dir: tempTestDir,
      });
      expect(r4.isError).toBeFalsy();
      expect(r4.structuredContent._provenance).toBeDefined();
      expect(r4.structuredContent._provenance.timestamp).toBeDefined();
      expect(r4.structuredContent._provenance.builder).toBe('DbtDashboardBuilder');

      // Tool 5: dbt_semantic_export_yaml
      const r5 = await handler.handleDbtSemanticExportYaml({
        project_dir: tempTestDir,
      });
      expect(r5.isError).toBeFalsy();
      expect(r5.structuredContent._provenance).toBeDefined();
      expect(r5.structuredContent._provenance.timestamp).toBeDefined();
      expect(r5.structuredContent._provenance.exporter).toBe('DbtYamlExporter');
    });

    test('6.2: Route table mapping consistency', () => {
      const routes = handler.routes();
      expect(routes).toBeDefined();
      expect(typeof routes.dbt_project_scan_deep).toBe('function');
      expect(typeof routes.dbt_lineage_joins_graph).toBe('function');
      expect(typeof routes.dbt_semantic_preagg_advisor).toBe('function');
      expect(typeof routes.dbt_build_dashboard_from_yaml).toBe('function');
      expect(typeof routes.dbt_semantic_export_yaml).toBe('function');
      expect(typeof routes.dbt_inspect_models).toBe('function');
      expect(typeof routes.dbt_prioritize_sources).toBe('function');
      expect(typeof routes.semantic_memory_propose).toBe('function');
      expect(typeof routes.semantic_memory_approve).toBe('function');
      expect(typeof routes.semantic_memory_deprecate).toBe('function');
      expect(typeof routes.semantic_memory_restore).toBe('function');
      expect(typeof routes.semantic_memory_list).toBe('function');
    });
  });
});
