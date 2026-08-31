/**
 * tests/unit/lineage-joins.test.js
 * Unit Test Suite for DbtLineageGraph, Multi-Hop Join Resolver, and dbt_lineage_joins_graph MCP Tool
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import { DbtLineageGraph, normalizeNodeName } from '../../src/dbt/lineage-joins.js';
import { DbtDeepScanner } from '../../src/dbt/dbt-deep-scanner.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';

describe('DbtLineageGraph & dbt_lineage_joins_graph Unit Tests', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-lineage-test-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 1: DAG Construction & Node/Edge Indexing
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 1: DAG Construction & Node/Edge Indexing', () => {
    test('TC-1.1: Builds directed graph from scanResult models, sources, seeds, exposures, and metrics', () => {
      const scanResult = {
        projectDir: '/test/project',
        manifestLoaded: true,
        models: [
          { name: 'stg_orders', tier: 'staging', tierRank: 30, dependsOn: ['raw_stripe.charges'] },
          { name: 'int_orders_cleaned', tier: 'intermediate', tierRank: 60, dependsOn: ['stg_orders'] },
          { name: 'fct_orders', tier: 'marts_fact', tierRank: 85, dependsOn: ['int_orders_cleaned'] },
        ],
        sources: [
          { sourceName: 'raw_stripe', tableName: 'charges', tier: 'raw', tierRank: 10 },
        ],
        seeds: [
          { name: 'seed_country_codes', tier: 'staging', tierRank: 40 },
        ],
        exposures: [
          { name: 'executive_dashboard', dependsOn: ['fct_orders'] },
        ],
        metrics: [
          { name: 'total_revenue', model: 'fct_orders' },
        ],
      };

      const graph = new DbtLineageGraph(scanResult);

      expect(graph.getNodeCount()).toBe(7);
      expect(graph.nodes.has('raw_stripe.charges')).toBe(true);
      expect(graph.nodes.has('stg_orders')).toBe(true);
      expect(graph.nodes.has('int_orders_cleaned')).toBe(true);
      expect(graph.nodes.has('fct_orders')).toBe(true);
      expect(graph.nodes.has('seed_country_codes')).toBe(true);
      expect(graph.nodes.has('executive_dashboard')).toBe(true);
      expect(graph.nodes.has('total_revenue')).toBe(true);
    });

    test('TC-1.2: Indexes direct dependencies (dependsOn) from SQL ref() and source() tags', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph(
        [
          { name: 'stg_users', dependsOn: ["source('raw_app', 'users')"] },
          { name: 'dim_users', dependsOn: ["ref('stg_users')"] },
        ],
        [],
        [{ sourceName: 'raw_app', tableName: 'users' }]
      );

      const stgParents = graph.getDirectParents('stg_users');
      expect(stgParents).toContain('raw_app.users');

      const dimParents = graph.getDirectParents('dim_users');
      expect(dimParents).toContain('stg_users');

      const stgChildren = graph.getDirectChildren('stg_users');
      expect(stgChildren).toContain('dim_users');
    });

    test('TC-1.3: Indexes explicit column relationship tests from schema.yml', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        {
          name: 'fct_orders',
          columns: {
            customer_id: {
              tests: [
                {
                  relationships: {
                    to: "ref('dim_customers')",
                    field: 'id',
                  },
                },
              ],
            },
          },
        },
        {
          name: 'dim_customers',
          columns: { id: { tests: ['unique', 'not_null'] } },
        },
      ]);

      const edges = graph.getRelationships();
      expect(edges.length).toBeGreaterThan(0);
      const edge = edges.find(e => e.fromModel === 'fct_orders' && e.toModel === 'dim_customers');
      expect(edge).toBeDefined();
      expect(edge.fromColumn).toBe('customer_id');
      expect(edge.toColumn).toBe('id');
      expect(edge.source).toBe('dbt_test');
      expect(edge.confidence).toBe(1.0);
    });

    test('TC-1.4: Ingests MetricFlow entity primary and foreign relationships', () => {
      const graph = new DbtLineageGraph();
      const semanticModels = [
        {
          name: 'orders_source',
          model: 'fct_orders',
          entities: [
            { name: 'order_id', type: 'primary' },
            { name: 'customer', type: 'foreign', expr: 'customer_id' },
          ],
        },
        {
          name: 'customers_source',
          model: 'dim_customers',
          entities: [
            { name: 'customer', type: 'primary', expr: 'id' },
          ],
        },
      ];

      graph.buildGraph(
        [{ name: 'fct_orders' }, { name: 'dim_customers' }],
        [],
        [],
        [],
        [],
        [],
        semanticModels
      );

      const pathResult = graph.findJoinPath('fct_orders', 'dim_customers');
      expect(pathResult.found).toBe(true);
      expect(pathResult.edges[0].source).toBe('metricflow_entity');
      expect(pathResult.edges[0].confidence).toBe(0.95);
    });

    test('TC-1.5: Normalizes complex node references (ref with package, source with quotes, uniqueId)', () => {
      expect(normalizeNodeName("{{ ref('pkg_name', 'stg_orders') }}")).toBe('stg_orders');
      expect(normalizeNodeName("ref('stg_orders')")).toBe('stg_orders');
      expect(normalizeNodeName("{{ source('raw_stripe', 'charges') }}")).toBe('raw_stripe.charges');
      expect(normalizeNodeName("model.my_project.fct_orders")).toBe('fct_orders');
      expect(normalizeNodeName("source.my_project.stripe.charges")).toBe('stripe.charges');
      expect(normalizeNodeName("'bare_table'")).toBe('bare_table');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 2: Upstream & Downstream Lineage Traversal
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 2: Upstream & Downstream Lineage Traversal', () => {
    let linearGraph;

    beforeEach(() => {
      linearGraph = new DbtLineageGraph();
      linearGraph.buildGraph([
        { name: 'src_raw_orders', tier: 'raw', tierRank: 10 },
        { name: 'stg_orders', tier: 'staging', tierRank: 30, dependsOn: ['src_raw_orders'] },
        { name: 'int_orders_cleaned', tier: 'intermediate', tierRank: 60, dependsOn: ['stg_orders'] },
        { name: 'fct_orders', tier: 'marts_fact', tierRank: 85, dependsOn: ['int_orders_cleaned'] },
        { name: 'rpt_monthly_sales', tier: 'marts_report', tierRank: 95, dependsOn: ['fct_orders'] },
      ]);
    });

    test('TC-2.1: Resolves direct upstream parents of a model', () => {
      const parents = linearGraph.getDirectParents('fct_orders');
      expect(parents).toEqual(['int_orders_cleaned']);
    });

    test('TC-2.2: Resolves multi-level transitive upstream dependencies with depth numbers', () => {
      const upstreamWithDepth = linearGraph.getAllUpstream('rpt_monthly_sales', { includeNodeInfo: true });
      expect(upstreamWithDepth.length).toBe(4);

      const fct = upstreamWithDepth.find(u => u.name === 'fct_orders');
      const int = upstreamWithDepth.find(u => u.name === 'int_orders_cleaned');
      const stg = upstreamWithDepth.find(u => u.name === 'stg_orders');
      const raw = upstreamWithDepth.find(u => u.name === 'src_raw_orders');

      expect(fct.depth).toBe(1);
      expect(int.depth).toBe(2);
      expect(stg.depth).toBe(3);
      expect(raw.depth).toBe(4);
    });

    test('TC-2.3: Resolves direct downstream children of a model', () => {
      const children = linearGraph.getDirectChildren('stg_orders');
      expect(children).toEqual(['int_orders_cleaned']);
    });

    test('TC-2.4: Resolves multi-level transitive downstream dependents with depth numbers', () => {
      const downstreamWithDepth = linearGraph.getAllDownstream('src_raw_orders', { includeNodeInfo: true });
      expect(downstreamWithDepth.length).toBe(4);

      const stg = downstreamWithDepth.find(d => d.name === 'stg_orders');
      const rpt = downstreamWithDepth.find(d => d.name === 'rpt_monthly_sales');

      expect(stg.depth).toBe(1);
      expect(rpt.depth).toBe(4);
    });

    test('TC-2.5: Handles models with 0 dependencies gracefully', () => {
      const emptyGraph = new DbtLineageGraph();
      emptyGraph.buildGraph([{ name: 'orphan_model' }]);

      expect(emptyGraph.getAllUpstream('orphan_model')).toEqual([]);
      expect(emptyGraph.getAllDownstream('orphan_model')).toEqual([]);
      expect(emptyGraph.getDirectParents('orphan_model')).toEqual([]);
      expect(emptyGraph.getDirectChildren('orphan_model')).toEqual([]);
    });

    test('TC-2.6: Filters upstream/downstream by tier and limits maxDepth', () => {
      const filtered = linearGraph.getAllUpstream('rpt_monthly_sales', {
        maxDepth: 2,
        includeNodeInfo: true,
      });

      expect(filtered.length).toBe(2);
      expect(filtered.map(f => f.name)).toEqual(['fct_orders', 'int_orders_cleaned']);

      const tierFiltered = linearGraph.getAllUpstream('rpt_monthly_sales', {
        filterTiers: ['raw', 'staging'],
        includeNodeInfo: true,
      });
      expect(tierFiltered.map(t => t.name)).toEqual(['stg_orders', 'src_raw_orders']);
    });

    test('TC-2.7: Returns rich node info when includeNodeInfo is true', () => {
      const info = linearGraph.getAllUpstream('fct_orders', { includeNodeInfo: true });
      expect(info[0]).toHaveProperty('name');
      expect(info[0]).toHaveProperty('depth');
      expect(info[0]).toHaveProperty('type');
      expect(info[0]).toHaveProperty('tier');
      expect(info[0]).toHaveProperty('tierRank');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 3: Semantic Relationship Test & Foreign Key Ingestion
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 3: Semantic Relationship Test & Foreign Key Ingestion', () => {
    test('TC-3.1: Ingests column tests: { relationships: { to: "ref(\'dim_customers\')", field: "id" } }', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        {
          name: 'fct_orders',
          columns: {
            customer_id: {
              tests: [{ relationships: { to: "ref('dim_customers')", field: 'id' } }],
            },
          },
        },
        { name: 'dim_customers', columns: { id: {} } },
      ]);

      const pathResult = graph.findJoinPath('fct_orders', 'dim_customers');
      expect(pathResult.found).toBe(true);
      expect(pathResult.edges[0].fromColumn).toBe('customer_id');
      expect(pathResult.edges[0].toColumn).toBe('id');
    });

    test('TC-3.2: Assigns confidence score 1.0 to verified dbt schema relationship tests', () => {
      const graph = new DbtLineageGraph();
      graph.addJoinRelationship({
        fromModel: 'fct_orders',
        fromColumn: 'customer_id',
        toModel: 'dim_customers',
        toColumn: 'id',
        source: 'dbt_test',
      });

      const pathResult = graph.findJoinPath('fct_orders', 'dim_customers');
      expect(pathResult.confidenceScore).toBe(1.0);
    });

    test('TC-3.3: Assigns confidence score 0.95 to MetricFlow entity matches', () => {
      const graph = new DbtLineageGraph();
      graph.addJoinRelationship({
        fromModel: 'fct_orders',
        fromColumn: 'customer_id',
        toModel: 'dim_customers',
        toColumn: 'id',
        source: 'metricflow_entity',
      });

      const pathResult = graph.findJoinPath('fct_orders', 'dim_customers');
      expect(pathResult.confidenceScore).toBe(0.95);
    });

    test('TC-3.4: Infers candidate join keys from column naming conventions (_id suffix) with confidence 0.75', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        {
          name: 'fct_orders',
          columns: {
            order_id: {},
            customer_id: {},
          },
        },
        {
          name: 'dim_customers',
          columns: {
            id: {},
            name: {},
          },
        },
      ]);

      const pathResult = graph.findJoinPath('fct_orders', 'dim_customers');
      expect(pathResult.found).toBe(true);
      expect(pathResult.edges[0].source).toBe('inferred_fk');
      expect(pathResult.edges[0].confidence).toBe(0.75);
      expect(pathResult.edges[0].fromColumn).toBe('customer_id');
      expect(pathResult.edges[0].toColumn).toBe('id');
    });

    test('TC-3.5: Generates bidirectional join edges for all valid relationships', () => {
      const graph = new DbtLineageGraph();
      graph.addJoinRelationship({
        fromModel: 'fct_orders',
        fromColumn: 'customer_id',
        toModel: 'dim_customers',
        toColumn: 'id',
        source: 'dbt_test',
        relationshipType: 'many_to_one',
      });

      const fwd = graph.findJoinPath('fct_orders', 'dim_customers');
      expect(fwd.found).toBe(true);
      expect(fwd.edges[0].relationshipType).toBe('many_to_one');

      const rev = graph.findJoinPath('dim_customers', 'fct_orders');
      expect(rev.found).toBe(true);
      expect(rev.edges[0].fromModel).toBe('dim_customers');
      expect(rev.edges[0].toModel).toBe('fct_orders');
      expect(rev.edges[0].relationshipType).toBe('one_to_many');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 4: Multi-Hop Shortest Path Resolution
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 4: Multi-Hop Shortest Path Resolution', () => {
    let multiHopGraph;

    beforeEach(() => {
      multiHopGraph = new DbtLineageGraph();
      multiHopGraph.buildGraph([
        { name: 'fct_orders', columns: { customer_id: {} } },
        { name: 'dim_customers', columns: { id: {}, region_id: {} } },
        { name: 'dim_regions', columns: { id: {}, country_id: {} } },
        { name: 'dim_countries', columns: { id: {} } },
      ], [
        { fromModel: 'fct_orders', fromColumn: 'customer_id', toModel: 'dim_customers', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'dim_customers', fromColumn: 'region_id', toModel: 'dim_regions', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'dim_regions', fromColumn: 'country_id', toModel: 'dim_countries', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
      ]);
    });

    test('TC-4.1: Resolves direct 1-hop join path (fct_orders -> dim_customers)', () => {
      const res = multiHopGraph.findJoinPath('fct_orders', 'dim_customers');
      expect(res.found).toBe(true);
      expect(res.hops).toBe(1);
      expect(res.path).toEqual(['fct_orders', 'dim_customers']);
      expect(res.confidenceScore).toBe(1.0);
    });

    test('TC-4.2: Resolves 2-hop join path (fct_orders -> dim_customers -> dim_regions)', () => {
      const res = multiHopGraph.findJoinPath('fct_orders', 'dim_regions');
      expect(res.found).toBe(true);
      expect(res.hops).toBe(2);
      expect(res.path).toEqual(['fct_orders', 'dim_customers', 'dim_regions']);
      expect(res.edges.length).toBe(2);
      expect(res.confidenceScore).toBe(1.0);
    });

    test('TC-4.3: Resolves 3-hop join path (fct_orders -> dim_customers -> dim_regions -> dim_countries)', () => {
      const res = multiHopGraph.findJoinPath('fct_orders', 'dim_countries');
      expect(res.found).toBe(true);
      expect(res.hops).toBe(3);
      expect(res.path).toEqual(['fct_orders', 'dim_customers', 'dim_regions', 'dim_countries']);
      expect(res.edges.length).toBe(3);
      expect(res.confidenceScore).toBe(1.0);
    });

    test('TC-4.4: Enforces max_hops limit and returns found: false when path is too long', () => {
      const res = multiHopGraph.findJoinPath('fct_orders', 'dim_countries', { maxHops: 2 });
      expect(res.found).toBe(false);
      expect(res.hops).toBe(0);
      expect(res.path).toEqual([]);
      expect(res.message).toMatch(/No join path found/i);
    });

    test('TC-4.5: Filters paths below confidence_threshold', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'table_a' },
        { name: 'table_b' },
      ], [
        { fromModel: 'table_a', fromColumn: 'b_id', toModel: 'table_b', toColumn: 'id', source: 'inferred_fk', confidence: 0.60 },
      ]);

      const res = graph.findJoinPath('table_a', 'table_b', { confidenceThreshold: 0.80 });
      expect(res.found).toBe(false);
    });

    test('TC-4.6: Supports BFS algorithm for unweighted hop search', () => {
      const res = multiHopGraph.findJoinPath('fct_orders', 'dim_regions', { algorithm: 'bfs' });
      expect(res.found).toBe(true);
      expect(res.hops).toBe(2);
      expect(res.path).toEqual(['fct_orders', 'dim_customers', 'dim_regions']);
    });

    test('TC-4.7: Discovers all simple paths via findAllJoinPaths sorted by cost and confidence', () => {
      const diamondGraph = new DbtLineageGraph();
      diamondGraph.buildGraph([
        { name: 'A' }, { name: 'B' }, { name: 'C' }, { name: 'D' },
      ], [
        { fromModel: 'A', fromColumn: 'b_id', toModel: 'B', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'B', fromColumn: 'd_id', toModel: 'D', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'A', fromColumn: 'c_id', toModel: 'C', toColumn: 'id', source: 'inferred_fk', confidence: 0.75 },
        { fromModel: 'C', fromColumn: 'd_id', toModel: 'D', toColumn: 'id', source: 'inferred_fk', confidence: 0.75 },
      ]);

      const allPaths = diamondGraph.findAllJoinPaths('A', 'D', { minConfidence: 0.5 });
      expect(allPaths.length).toBe(2);
      expect(allPaths[0].path).toEqual(['A', 'B', 'D']);
      expect(allPaths[1].path).toEqual(['A', 'C', 'D']);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 5: Diamond Join Graphs & Ambiguity Disambiguation
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 5: Diamond Join Graphs & Ambiguity Disambiguation', () => {
    test('TC-5.1: Resolves diamond graph choosing shorter path (A -> B -> D over A -> C -> E -> D)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'A' }, { name: 'B' }, { name: 'C' }, { name: 'E' }, { name: 'D' },
      ], [
        // Short route: 2 hops
        { fromModel: 'A', fromColumn: 'b_id', toModel: 'B', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'B', fromColumn: 'd_id', toModel: 'D', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        // Long route: 3 hops
        { fromModel: 'A', fromColumn: 'c_id', toModel: 'C', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'C', fromColumn: 'e_id', toModel: 'E', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'E', fromColumn: 'd_id', toModel: 'D', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
      ]);

      const pathResult = graph.findJoinPath('A', 'D');
      expect(pathResult.found).toBe(true);
      expect(pathResult.hops).toBe(2);
      expect(pathResult.path).toEqual(['A', 'B', 'D']);
    });

    test('TC-5.2: Resolves equal-hop diamond graph choosing higher confidence path', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'A' }, { name: 'B' }, { name: 'C' }, { name: 'D' },
      ], [
        // High confidence route (dbt_test: 1.0 * 1.0 = 1.0)
        { fromModel: 'A', fromColumn: 'b_id', toModel: 'B', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'B', fromColumn: 'd_id', toModel: 'D', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        // Low confidence route (inferred_fk: 0.75 * 0.75 = 0.5625)
        { fromModel: 'A', fromColumn: 'c_id', toModel: 'C', toColumn: 'id', source: 'inferred_fk', confidence: 0.75 },
        { fromModel: 'C', fromColumn: 'd_id', toModel: 'D', toColumn: 'id', source: 'inferred_fk', confidence: 0.75 },
      ]);

      const pathResult = graph.findJoinPath('A', 'D');
      expect(pathResult.found).toBe(true);
      expect(pathResult.path).toEqual(['A', 'B', 'D']);
      expect(pathResult.confidenceScore).toBe(1.0);
    });

    test('TC-5.3: Discovers bidirectional join capability (fct -> dim and dim -> fct)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'fct_orders' }, { name: 'dim_customers' },
      ], [
        { fromModel: 'fct_orders', fromColumn: 'customer_id', toModel: 'dim_customers', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
      ]);

      const fwd = graph.findJoinPath('fct_orders', 'dim_customers');
      expect(fwd.found).toBe(true);
      expect(fwd.path).toEqual(['fct_orders', 'dim_customers']);

      const rev = graph.findJoinPath('dim_customers', 'fct_orders');
      expect(rev.found).toBe(true);
      expect(rev.path).toEqual(['dim_customers', 'fct_orders']);
    });

    test('TC-5.4: Handles self-join / zero-hop path search', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([{ name: 'fct_orders' }]);

      const res = graph.findJoinPath('fct_orders', 'fct_orders');
      expect(res.found).toBe(true);
      expect(res.hops).toBe(0);
      expect(res.path).toEqual(['fct_orders']);
      expect(res.confidenceScore).toBe(1.0);
      expect(res.edges).toEqual([]);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 6: Cycle Detection & Graph Robustness & Blast Radius
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 6: Cycle Detection & Graph Robustness & Blast Radius', () => {
    test('TC-6.1: Detects cycle in circular dependency graph (A -> B -> C -> A)', () => {
      const cyclicGraph = new DbtLineageGraph();
      cyclicGraph.buildGraph([
        { name: 'model_a', dependsOn: ['model_c'] },
        { name: 'model_b', dependsOn: ['model_a'] },
        { name: 'model_c', dependsOn: ['model_b'] },
      ]);

      const validation = cyclicGraph.validateDAG();
      expect(validation.isValidDAG).toBe(false);
      expect(validation.hasCycles).toBe(true);
      expect(validation.cycleCount).toBeGreaterThan(0);
      expect(validation.cycleNodes).toContain('model_a');
      expect(validation.cycleNodes).toContain('model_b');
      expect(validation.cycleNodes).toContain('model_c');
    });

    test('TC-6.2: Computes topological order for acyclic graphs with wave groupings', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'src_users', tier: 'raw', tierRank: 10 },
        { name: 'src_orders', tier: 'raw', tierRank: 10 },
        { name: 'stg_users', tier: 'staging', tierRank: 30, dependsOn: ['src_users'] },
        { name: 'stg_orders', tier: 'staging', tierRank: 30, dependsOn: ['src_orders'] },
        { name: 'fct_orders', tier: 'marts_fact', tierRank: 85, dependsOn: ['stg_orders', 'stg_users'] },
      ]);

      const order = graph.getTopologicalOrder();
      expect(order.indexOf('src_users')).toBeLessThan(order.indexOf('stg_users'));
      expect(order.indexOf('src_orders')).toBeLessThan(order.indexOf('stg_orders'));
      expect(order.indexOf('stg_users')).toBeLessThan(order.indexOf('fct_orders'));
      expect(order.indexOf('stg_orders')).toBeLessThan(order.indexOf('fct_orders'));

      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves.length).toBe(3);
      expect(waves[0]).toEqual(expect.arrayContaining(['src_orders', 'src_users']));
      expect(waves[1]).toEqual(expect.arrayContaining(['stg_orders', 'stg_users']));
      expect(waves[2]).toEqual(['fct_orders']);
    });

    test('TC-6.3: Returns topologicalOrder: [] and has_cycles: true for cyclic graphs without infinite loop', () => {
      const cyclicGraph = new DbtLineageGraph();
      cyclicGraph.buildGraph([
        { name: 'cycle_1', dependsOn: ['cycle_2'] },
        { name: 'cycle_2', dependsOn: ['cycle_1'] },
      ]);

      const order = cyclicGraph.getTopologicalOrder();
      expect(order).toEqual([]);
      expect(cyclicGraph.hasCycles()).toBe(true);
    });

    test('TC-6.4: Gracefully handles disconnected nodes and missing target models', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'isolated_a' },
        { name: 'isolated_b' },
      ]);

      const res = graph.findJoinPath('isolated_a', 'isolated_b');
      expect(res.found).toBe(false);
      expect(res.hops).toBe(0);

      const missingRes = graph.findJoinPath('isolated_a', 'non_existent_table');
      expect(missingRes.found).toBe(false);
    });

    test('TC-6.5: Calculates accurate blast radius and impact level for models', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'stg_orders', tier: 'staging' },
        { name: 'int_orders', tier: 'intermediate', dependsOn: ['stg_orders'] },
        { name: 'fct_orders', tier: 'marts_fact', dependsOn: ['int_orders'] },
        { name: 'rpt_sales', tier: 'marts_report', dependsOn: ['fct_orders'] },
        { name: 'sales_dashboard', type: 'exposure', dependsOn: ['rpt_sales'] },
        { name: 'revenue_metric', type: 'metric', dependsOn: ['fct_orders'] },
      ]);

      const blast = graph.calculateBlastRadius('stg_orders');
      expect(blast.targetModel).toBe('stg_orders');
      expect(blast.totalAffectedCount).toBe(5);
      expect(blast.maxDownstreamDepth).toBe(4);
      expect(blast.impactLevel).toBe('CRITICAL');
      expect(blast.affectedMarts).toContain('fct_orders');
      expect(blast.affectedMarts).toContain('rpt_sales');
      expect(blast.affectedExposures).toContain('sales_dashboard');
      expect(blast.affectedMetrics).toContain('revenue_metric');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 7: ANSI SQL Join Generation & MCP Handler End-to-End
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 7: ANSI SQL Join Generation & MCP Handler End-to-End', () => {
    let graph;

    beforeEach(() => {
      graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'fct_orders' },
        { name: 'dim_customers' },
        { name: 'dim_regions' },
      ], [
        { fromModel: 'fct_orders', fromColumn: 'customer_id', toModel: 'dim_customers', toColumn: 'id', source: 'dbt_test' },
        { fromModel: 'dim_customers', fromColumn: 'region_id', toModel: 'dim_regions', toColumn: 'id', source: 'dbt_test' },
      ]);
    });

    test('TC-7.1: Generates single LEFT JOIN SQL clause with ON condition', () => {
      const pathResult = graph.findJoinPath('fct_orders', 'dim_customers');
      const sql = graph.generateJoinSql(pathResult.edges);

      expect(sql).toContain('FROM fct_orders AS fct_orders');
      expect(sql).toContain('LEFT JOIN dim_customers AS dim_customers');
      expect(sql).toContain('ON fct_orders.customer_id = dim_customers.id');
    });

    test('TC-7.2: Generates multi-hop chained LEFT JOIN SQL clauses', () => {
      const pathResult = graph.findJoinPath('fct_orders', 'dim_regions');
      const sql = graph.generateJoinSql(pathResult.edges);

      expect(sql).toContain('FROM fct_orders AS fct_orders');
      expect(sql).toContain('LEFT JOIN dim_customers AS dim_customers');
      expect(sql).toContain('ON fct_orders.customer_id = dim_customers.id');
      expect(sql).toContain('LEFT JOIN dim_regions AS dim_regions');
      expect(sql).toContain('ON dim_customers.region_id = dim_regions.id');
    });

    test('TC-7.3: Supports custom join types (INNER, RIGHT, FULL OUTER)', () => {
      const pathResult = graph.findJoinPath('fct_orders', 'dim_customers');
      const sqlInner = graph.generateJoinSql(pathResult.edges, null, { defaultJoinType: 'INNER' });
      expect(sqlInner).toContain('INNER JOIN dim_customers AS dim_customers');

      const sqlRight = graph.generateJoinSql(pathResult.edges, null, { defaultJoinType: 'RIGHT' });
      expect(sqlRight).toContain('RIGHT JOIN dim_customers AS dim_customers');
    });

    test('TC-7.4: Supports custom base table alias and short aliases', () => {
      const pathResult = graph.findJoinPath('fct_orders', 'dim_customers');
      const sqlWithBase = graph.generateJoinSql(pathResult.edges, 'o');

      expect(sqlWithBase).toContain('FROM fct_orders AS o');
      expect(sqlWithBase).toContain('LEFT JOIN dim_customers AS dim_customers');
      expect(sqlWithBase).toContain('ON o.customer_id = dim_customers.id');

      const sqlShort = graph.generateJoinSql(pathResult.edges, 'o', { shortAlias: true });
      expect(sqlShort).toContain('FROM fct_orders AS o');
      expect(sqlShort).toContain('LEFT JOIN dim_customers AS c');
      expect(sqlShort).toContain('ON o.customer_id = c.id');
    });

    test('TC-7.5: Avoids collision when table appears multiple times in joins', () => {
      const doubleJoinGraph = new DbtLineageGraph();
      doubleJoinGraph.buildGraph([
        { name: 'fct_orders' },
        { name: 'dim_addresses' },
      ], [
        { fromModel: 'fct_orders', fromColumn: 'shipping_address_id', toModel: 'dim_addresses', toColumn: 'id', source: 'dbt_test' },
        { fromModel: 'fct_orders', fromColumn: 'billing_address_id', toModel: 'dim_addresses', toColumn: 'id', source: 'dbt_test' },
      ]);

      const edges = [
        { fromModel: 'fct_orders', fromColumn: 'shipping_address_id', toModel: 'dim_addresses', toColumn: 'id' },
        { fromModel: 'fct_orders', fromColumn: 'billing_address_id', toModel: 'dim_addresses', toColumn: 'id' },
      ];

      const sql = doubleJoinGraph.generateJoinSql(edges);
      expect(sql).toContain('LEFT JOIN dim_addresses AS dim_addresses');
      expect(sql).toContain('LEFT JOIN dim_addresses AS dim_addresses_2');
      expect(sql).toContain('ON fct_orders.shipping_address_id = dim_addresses.id');
      expect(sql).toContain('ON fct_orders.billing_address_id = dim_addresses_2.id');
    });

    test('TC-7.6: handleDbtLineageJoinsGraph returns dual markdown and structuredContent payload with _provenance', async () => {
      // Setup test dbt project directory with models and tests
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });

      const schemaYaml = `
version: 2
models:
  - name: fct_orders
    description: "Orders fact table"
    columns:
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: id
  - name: dim_customers
    description: "Customers dimension table"
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: region_id
        tests:
          - relationships:
              to: ref('dim_regions')
              field: id
  - name: dim_regions
    description: "Regions dimension table"
    columns:
      - name: id
`;
      fs.writeFileSync(path.join(modelsDir, 'schema.yml'), schemaYaml);
      fs.writeFileSync(path.join(modelsDir, 'fct_orders.sql'), 'select 1 as id');
      fs.writeFileSync(path.join(modelsDir, 'dim_customers.sql'), 'select 1 as id');
      fs.writeFileSync(path.join(modelsDir, 'dim_regions.sql'), 'select 1 as id');

      const handler = new DbtSemanticHandler();
      const response = await handler.handleDbtLineageJoinsGraph({
        project_dir: tempDir,
        source_model: 'fct_orders',
        target_model: 'dim_regions',
      });

      expect(response).toBeDefined();
      expect(response.content).toBeDefined();
      expect(response.content[0].type).toBe('text');
      expect(response.content[0].text).toContain('dbt MULTI-HOP LINEAGE & SEMANTIC JOIN GRAPH');
      expect(response.content[0].text).toContain('fct_orders');
      expect(response.content[0].text).toContain('dim_regions');

      expect(response.structuredContent).toBeDefined();
      const payload = response.structuredContent;
      expect(payload.node_count).toBeGreaterThanOrEqual(3);
      expect(payload.has_cycles).toBe(false);
      expect(payload.join_paths.length).toBe(1);
      expect(payload.join_paths[0].hops).toBe(2);
      expect(payload.join_paths[0].path).toEqual(['fct_orders', 'dim_customers', 'dim_regions']);
      expect(payload._provenance.governance_level).toBe('READ_ONLY_INSPECTION');
      expect(payload._provenance.resolver).toBe('DbtLineageGraph');
    });

    test('TC-7.7: handleDbtLineageJoinsGraph handles target_models array with unified multi-join SQL', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });

      const schemaYaml = `
version: 2
models:
  - name: fct_orders
    columns:
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: id
      - name: product_id
        tests:
          - relationships:
              to: ref('dim_products')
              field: id
  - name: dim_customers
    columns:
      - name: id
  - name: dim_products
    columns:
      - name: id
`;
      fs.writeFileSync(path.join(modelsDir, 'schema.yml'), schemaYaml);
      fs.writeFileSync(path.join(modelsDir, 'fct_orders.sql'), 'select 1 as id');
      fs.writeFileSync(path.join(modelsDir, 'dim_customers.sql'), 'select 1 as id');
      fs.writeFileSync(path.join(modelsDir, 'dim_products.sql'), 'select 1 as id');

      const handler = new DbtSemanticHandler();
      const response = await handler.handleDbtLineageJoinsGraph({
        project_dir: tempDir,
        source_model: 'fct_orders',
        target_models: ['dim_customers', 'dim_products'],
      });

      expect(response.structuredContent.join_paths.length).toBe(2);
      expect(response.structuredContent.sql_snippet).toContain('LEFT JOIN dim_customers AS dim_customers');
      expect(response.structuredContent.sql_snippet).toContain('LEFT JOIN dim_products AS dim_products');
    });

    test('TC-7.8: handleDbtLineageJoinsGraph handles missing project directory gracefully', async () => {
      const handler = new DbtSemanticHandler();
      const response = await handler.handleDbtLineageJoinsGraph({
        project_dir: '/invalid/directory/path/that/does/not/exist',
      });

      expect(response.isError).toBe(true);
      expect(response.content[0].text).toContain('❌ dbt Lineage & Joins Error');
    });

    test('TC-7.9: Tool registry registers dbt_lineage_joins_graph with readOnlyHint: true and valid outputSchema', () => {
      const allTools = getToolDefinitions();
      const toolDef = allTools.find(t => t.name === 'dbt_lineage_joins_graph');

      expect(toolDef).toBeDefined();
      expect(toolDef.readOnlyHint).toBe(true);
      expect(toolDef.annotations.readOnlyHint).toBe(true);
      expect(toolDef.inputSchema.properties).toHaveProperty('source_model');
      expect(toolDef.inputSchema.properties).toHaveProperty('target_model');
      expect(toolDef.inputSchema.properties).toHaveProperty('max_hops');

      const meta = TOOL_METADATA.dbt_lineage_joins_graph;
      expect(meta).toBeDefined();
      expect(meta.outputSchema).toBeDefined();
      expect(meta.outputSchema.properties).toHaveProperty('join_paths');
      expect(meta.outputSchema.properties).toHaveProperty('_provenance');
    });
  });
});
