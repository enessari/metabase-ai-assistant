/**
 * tests/challenger-m2-2-empirical.test.js
 * Adversarial Verification Suite by Challenger M2-2
 *
 * Focus Areas:
 * 1. Dijkstra Shortest Path with Varied Edge Weights & Hop Counts (1-10 hops)
 * 2. Ambiguous Join Paths & Multi-FK Topologies (Role-playing foreign keys)
 * 3. ANSI SQL Join Generation, Alias Collision Prevention & Base Alias Customization
 * 4. Comprehensive Testing of all 9 Graph / Lineage / Join Action Types
 * 5. MCP Handler End-to-End Verification with Real / Mocked Project Structures
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import { DbtLineageGraph, normalizeNodeName } from '../src/dbt/lineage-joins.js';
import { DbtSemanticHandler } from '../src/mcp/handlers/dbt-semantic.js';
import { TOOL_METADATA, getToolDefinitions } from '../src/mcp/tool-registry.js';

describe('Challenger M2-2: Multi-Hop Dijkstra Routing, Ambiguity & SQL Aliasing Adversarial Suite', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'challenger-m2-2-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 1. DIJKSTRA SHORTEST PATH ROUTING (1-10 HOPS & VARIED CONFIDENCE WEIGHTS)
  // ══════════════════════════════════════════════════════════════════════════
  describe('1. Dijkstra Multi-Hop Routing Scalability & Weighted Cost Calculations', () => {
    test('ADV-1.1: Resolves exact linear chains from 1 to 10 hops', () => {
      const graph = new DbtLineageGraph();
      const nodeCount = 11; // N_0 to N_10
      const models = [];
      const relationships = [];

      for (let i = 0; i < nodeCount; i++) {
        models.push({ name: `node_${i}` });
        if (i < nodeCount - 1) {
          relationships.push({
            fromModel: `node_${i}`,
            fromColumn: `node_${i + 1}_id`,
            toModel: `node_${i + 1}`,
            toColumn: 'id',
            source: 'dbt_test',
            confidence: 1.0,
            weight: 1.0,
          });
        }
      }

      graph.buildGraph(models, relationships);

      // Verify each hop length from 1 to 10
      for (let k = 1; k <= 10; k++) {
        const result = graph.findJoinPath('node_0', `node_${k}`, { maxHops: 10 });
        expect(result.found).toBe(true);
        expect(result.hopCount).toBe(k);
        expect(result.hops).toBe(k);
        expect(result.path.length).toBe(k + 1);
        expect(result.path[0]).toBe('node_0');
        expect(result.path[k]).toBe(`node_${k}`);
        expect(result.edges.length).toBe(k);
        expect(result.confidence).toBeCloseTo(1.0, 4);
        expect(result.totalCost).toBeCloseTo(k * 1.0, 4);
      }
    });

    test('ADV-1.2: Strict boundary enforcement: maxHops truncation vs discovery', () => {
      const graph = new DbtLineageGraph();
      const models = Array.from({ length: 8 }, (_, i) => ({ name: `stage_${i}` }));
      const relationships = [];

      for (let i = 0; i < 7; i++) {
        relationships.push({
          fromModel: `stage_${i}`,
          fromColumn: `stage_${i + 1}_id`,
          toModel: `stage_${i + 1}`,
          toColumn: 'id',
          source: 'dbt_test',
          confidence: 1.0,
        });
      }

      graph.buildGraph(models, relationships);

      // stage_0 to stage_6 is 6 hops
      const failResult = graph.findJoinPath('stage_0', 'stage_6', { maxHops: 5 });
      expect(failResult.found).toBe(false);
      expect(failResult.hopCount).toBe(0);
      expect(failResult.message).toContain('No join path found');

      const passResult = graph.findJoinPath('stage_0', 'stage_6', { maxHops: 6 });
      expect(passResult.found).toBe(true);
      expect(passResult.hopCount).toBe(6);
    });

    test('ADV-1.3: Dijkstra Cost vs Hops Trade-off (Prefers lower weight path over fewer hops when configured)', () => {
      const graph = new DbtLineageGraph();
      // Topology:
      // Path 1 (Short but heavy weight / low confidence):
      //   src -> mid_unreliable -> tgt (2 hops, conf 0.50, weight 2.0 each -> totalCost 4.0, conf 0.25)
      // Path 2 (Longer but light weight / high confidence):
      //   src -> h1 -> h2 -> h3 -> tgt (4 hops, conf 1.0, weight 0.8 each -> totalCost 3.2, conf 1.0)

      graph.buildGraph([
        { name: 'src' },
        { name: 'mid_unreliable' },
        { name: 'h1' },
        { name: 'h2' },
        { name: 'h3' },
        { name: 'tgt' },
      ]);

      // Route 1 (cost 4.0)
      graph.addJoinRelationship({ fromModel: 'src', fromColumn: 'u_id', toModel: 'mid_unreliable', toColumn: 'id', confidence: 0.5, weight: 2.0 });
      graph.addJoinRelationship({ fromModel: 'mid_unreliable', fromColumn: 'tgt_id', toModel: 'tgt', toColumn: 'id', confidence: 0.5, weight: 2.0 });

      // Route 2 (cost 3.2)
      graph.addJoinRelationship({ fromModel: 'src', fromColumn: 'h1_id', toModel: 'h1', toColumn: 'id', confidence: 1.0, weight: 0.8 });
      graph.addJoinRelationship({ fromModel: 'h1', fromColumn: 'h2_id', toModel: 'h2', toColumn: 'id', confidence: 1.0, weight: 0.8 });
      graph.addJoinRelationship({ fromModel: 'h2', fromColumn: 'h3_id', toModel: 'h3', toColumn: 'id', confidence: 1.0, weight: 0.8 });
      graph.addJoinRelationship({ fromModel: 'h3', fromColumn: 'tgt_id', toModel: 'tgt', toColumn: 'id', confidence: 1.0, weight: 0.8 });

      // Dijkstra search with maxHops: 5, minConfidence: 0.1
      const dijkstraResult = graph.findJoinPath('src', 'tgt', { maxHops: 5, minConfidence: 0.1, algorithm: 'dijkstra' });
      expect(dijkstraResult.found).toBe(true);
      expect(dijkstraResult.totalCost).toBeCloseTo(3.2, 2);
      expect(dijkstraResult.hopCount).toBe(4);
      expect(dijkstraResult.path).toEqual(['src', 'h1', 'h2', 'h3', 'tgt']);

      // BFS unweighted search should choose the 2-hop route
      const bfsResult = graph.findJoinPath('src', 'tgt', { maxHops: 5, minConfidence: 0.1, algorithm: 'bfs' });
      expect(bfsResult.found).toBe(true);
      expect(bfsResult.hopCount).toBe(2);
      expect(bfsResult.path).toEqual(['src', 'mid_unreliable', 'tgt']);
    });

    test('ADV-1.4: Cumulative multiplicative confidence filtering across multi-hop path', () => {
      const graph = new DbtLineageGraph();
      // 4 hops where each hop has confidence 0.80 -> cumulative confidence = 0.80^4 = 0.4096
      graph.buildGraph([
        { name: 'm0' }, { name: 'm1' }, { name: 'm2' }, { name: 'm3' }, { name: 'm4' }
      ]);
      for (let i = 0; i < 4; i++) {
        graph.addJoinRelationship({
          fromModel: `m${i}`,
          fromColumn: `m${i + 1}_id`,
          toModel: `m${i + 1}`,
          toColumn: 'id',
          confidence: 0.80,
          weight: 1.25,
        });
      }

      // If minConfidence is 0.50, cumulative 0.4096 should be rejected
      const rejected = graph.findJoinPath('m0', 'm4', { minConfidence: 0.50 });
      expect(rejected.found).toBe(false);

      // If minConfidence is 0.40, cumulative 0.4096 should pass
      const passed = graph.findJoinPath('m0', 'm4', { minConfidence: 0.40 });
      expect(passed.found).toBe(true);
      expect(passed.confidence).toBeCloseTo(0.4096, 3);
      expect(passed.hopCount).toBe(4);
    });

    test('ADV-1.5: Dense lattice graph with 25 nodes and multiple competing routes', () => {
      const graph = new DbtLineageGraph();
      const rows = 5;
      const cols = 5;

      for (let r = 0; r < rows; r++) {
        for (let c = 0; c < cols; c++) {
          graph.addNode({ name: `grid_${r}_${c}` });
        }
      }

      // Add horizontal and vertical edges
      for (let r = 0; r < rows; r++) {
        for (let c = 0; c < cols; c++) {
          if (c < cols - 1) {
            graph.addJoinRelationship({
              fromModel: `grid_${r}_${c}`,
              fromColumn: 'right_id',
              toModel: `grid_${r}_${c + 1}`,
              toColumn: 'id',
              confidence: 0.95,
              weight: 1.0,
            });
          }
          if (r < rows - 1) {
            graph.addJoinRelationship({
              fromModel: `grid_${r}_${c}`,
              fromColumn: 'down_id',
              toModel: `grid_${r + 1}_${c}`,
              toColumn: 'id',
              confidence: 0.95,
              weight: 1.0,
            });
          }
        }
      }

      // Find path from top-left (grid_0_0) to bottom-right (grid_4_4) -> 8 hops
      const latticeResult = graph.findJoinPath('grid_0_0', 'grid_4_4', { maxHops: 10, minConfidence: 0.5 });
      expect(latticeResult.found).toBe(true);
      expect(latticeResult.hopCount).toBe(8);
      expect(latticeResult.path[0]).toBe('grid_0_0');
      expect(latticeResult.path[8]).toBe('grid_4_4');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 2. AMBIGUOUS JOIN PATHS & MULTI-FK TOPOLOGIES
  // ══════════════════════════════════════════════════════════════════════════
  describe('2. Ambiguous Join Paths & Role-Playing Dimensions', () => {
    let salesGraph;

    beforeEach(() => {
      salesGraph = new DbtLineageGraph();
      salesGraph.buildGraph([
        { name: 'fct_orders', tier: 'marts_fact' },
        { name: 'dim_addresses', tier: 'marts_dim' },
        { name: 'dim_customers', tier: 'marts_dim' },
        { name: 'dim_regions', tier: 'marts_dim' },
      ]);

      // Role-playing foreign keys between fct_orders and dim_addresses
      // 1. Billing address
      salesGraph.addJoinRelationship({
        fromModel: 'fct_orders',
        fromColumn: 'billing_address_id',
        toModel: 'dim_addresses',
        toColumn: 'address_id',
        source: 'dbt_test',
        confidence: 1.0,
        weight: 1.0,
      });

      // 2. Shipping address
      salesGraph.addJoinRelationship({
        fromModel: 'fct_orders',
        fromColumn: 'shipping_address_id',
        toModel: 'dim_addresses',
        toColumn: 'address_id',
        source: 'dbt_test',
        confidence: 1.0,
        weight: 1.0,
      });

      // 3. Indirect address via customer
      salesGraph.addJoinRelationship({
        fromModel: 'fct_orders',
        fromColumn: 'customer_id',
        toModel: 'dim_customers',
        toColumn: 'customer_id',
        source: 'dbt_test',
        confidence: 1.0,
        weight: 1.0,
      });

      salesGraph.addJoinRelationship({
        fromModel: 'dim_customers',
        fromColumn: 'home_address_id',
        toModel: 'dim_addresses',
        toColumn: 'address_id',
        source: 'dbt_test',
        confidence: 1.0,
        weight: 1.0,
      });
    });

    test('ADV-2.1: findAllJoinPaths discovers both direct role-playing paths and indirect paths', () => {
      const allPaths = salesGraph.findAllJoinPaths('fct_orders', 'dim_addresses', { maxHops: 3 });

      expect(allPaths.length).toBeGreaterThanOrEqual(3);

      // Verify direct 1-hop paths exist for both billing and shipping
      const oneHopPaths = allPaths.filter(p => p.hopCount === 1);
      expect(oneHopPaths.length).toBe(2);

      const fromColumns = oneHopPaths.map(p => p.edges[0].fromColumn);
      expect(fromColumns).toContain('billing_address_id');
      expect(fromColumns).toContain('shipping_address_id');

      // Verify indirect 2-hop path exists via dim_customers
      const twoHopPaths = allPaths.filter(p => p.hopCount === 2);
      expect(twoHopPaths.length).toBe(1);
      expect(twoHopPaths[0].path).toEqual(['fct_orders', 'dim_customers', 'dim_addresses']);
    });

    test('ADV-2.2: Generates valid SQL for ambiguous role-playing joins with alias differentiation', () => {
      const allPaths = salesGraph.findAllJoinPaths('fct_orders', 'dim_addresses', { maxHops: 1 });
      expect(allPaths.length).toBe(2);

      // Path 1: billing address
      const sqlBilling = salesGraph.generateJoinSql(allPaths[0].edges, 'orders');
      expect(sqlBilling).toContain('FROM fct_orders AS orders');
      expect(sqlBilling).toMatch(/LEFT JOIN dim_addresses AS dim_addresses/);

      // Path 2: shipping address
      const sqlShipping = salesGraph.generateJoinSql(allPaths[1].edges, 'orders');
      expect(sqlShipping).toContain('FROM fct_orders AS orders');
      expect(sqlShipping).toMatch(/LEFT JOIN dim_addresses AS dim_addresses/);

      // Combined sequential join of both edges in a single query
      const combinedEdges = [allPaths[0].edges[0], allPaths[1].edges[0]];
      const sqlCombined = salesGraph.generateJoinSql(combinedEdges, 'orders');

      expect(sqlCombined).toContain('FROM fct_orders AS orders');
      expect(sqlCombined).toContain('LEFT JOIN dim_addresses AS dim_addresses');
      expect(sqlCombined).toContain('LEFT JOIN dim_addresses AS dim_addresses_2');
      expect(sqlCombined).toContain('ON orders.billing_address_id = dim_addresses.address_id');
      expect(sqlCombined).toContain('ON orders.shipping_address_id = dim_addresses_2.address_id');
    });

    test('ADV-2.3: Bidirectional join from address back to orders resolves multiple parent paths', () => {
      const reversePaths = salesGraph.findAllJoinPaths('dim_addresses', 'fct_orders', { maxHops: 1 });
      expect(reversePaths.length).toBe(2);

      const toColumns = reversePaths.map(p => p.edges[0].toColumn);
      expect(toColumns).toContain('billing_address_id');
      expect(toColumns).toContain('shipping_address_id');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 3. ANSI SQL JOIN SYNTAX CORRECTNESS, ALIASING & COLLISION PREVENTION
  // ══════════════════════════════════════════════════════════════════════════
  describe('3. ANSI SQL Join Syntax, Collision Prevention & Aliasing Options', () => {
    let complexGraph;

    beforeEach(() => {
      complexGraph = new DbtLineageGraph();
      complexGraph.buildGraph([
        { name: 'fct_sales', tier: 'marts_fact' },
        { name: 'dim_users', tier: 'marts_dim' },
        { name: 'dim_orgs', tier: 'marts_dim' },
        { name: 'dim_tiers', tier: 'marts_dim' },
      ]);

      complexGraph.addJoinRelationship({ fromModel: 'fct_sales', fromColumn: 'buyer_id', toModel: 'dim_users', toColumn: 'user_id' });
      complexGraph.addJoinRelationship({ fromModel: 'fct_sales', fromColumn: 'seller_id', toModel: 'dim_users', toColumn: 'user_id' });
      complexGraph.addJoinRelationship({ fromModel: 'dim_users', fromColumn: 'org_id', toModel: 'dim_orgs', toColumn: 'org_id' });
      complexGraph.addJoinRelationship({ fromModel: 'dim_orgs', fromColumn: 'tier_id', toModel: 'dim_tiers', toColumn: 'tier_id' });
    });

    test('ADV-3.1: 3-instance join collision avoidance on same table', () => {
      // Suppose we have buyer, seller, and referrer all joining dim_users
      complexGraph.addJoinRelationship({ fromModel: 'fct_sales', fromColumn: 'referrer_id', toModel: 'dim_users', toColumn: 'user_id' });

      const edges = [
        { fromModel: 'fct_sales', fromColumn: 'buyer_id', toModel: 'dim_users', toColumn: 'user_id' },
        { fromModel: 'fct_sales', fromColumn: 'seller_id', toModel: 'dim_users', toColumn: 'user_id' },
        { fromModel: 'fct_sales', fromColumn: 'referrer_id', toModel: 'dim_users', toColumn: 'user_id' },
      ];

      const sql = complexGraph.generateJoinSql(edges, 's');

      expect(sql).toContain('FROM fct_sales AS s');
      expect(sql).toContain('LEFT JOIN dim_users AS dim_users\n  ON s.buyer_id = dim_users.user_id');
      expect(sql).toContain('LEFT JOIN dim_users AS dim_users_2\n  ON s.seller_id = dim_users_2.user_id');
      expect(sql).toContain('LEFT JOIN dim_users AS dim_users_3\n  ON s.referrer_id = dim_users_3.user_id');
    });

    test('ADV-3.2: Short alias option formats single-character stem aliases', () => {
      const path = ['fct_sales', 'dim_users', 'dim_orgs'];
      const sql = complexGraph.generateJoinSql(path, null, { shortAlias: true });

      // fct_sales stem -> s, dim_users stem -> u, dim_orgs stem -> o
      expect(sql).toContain('FROM fct_sales AS s');
      expect(sql).toContain('LEFT JOIN dim_users AS u');
      expect(sql).toContain('LEFT JOIN dim_orgs AS o');
    });

    test('ADV-3.3: Custom aliasMap overrides default table aliases', () => {
      const path = ['fct_sales', 'dim_users', 'dim_orgs'];
      const sql = complexGraph.generateJoinSql(path, 'root_sales', {
        aliasMap: {
          dim_users: 'usr',
          dim_orgs: 'organization',
        },
      });

      expect(sql).toContain('FROM fct_sales AS root_sales');
      expect(sql).toContain('LEFT JOIN dim_users AS usr\n  ON root_sales.buyer_id = usr.user_id');
      expect(sql).toContain('LEFT JOIN dim_orgs AS organization\n  ON usr.org_id = organization.org_id');
    });

    test('ADV-3.4: includeSelect and selectColumns customization', () => {
      const pathResult = complexGraph.findJoinPath('fct_sales', 'dim_tiers', { maxHops: 4 });
      const sql = complexGraph.generateJoinSql(pathResult.edges, 's', {
        includeSelect: true,
        selectColumns: ['s.sale_id', 's.amount', 'dim_tiers.tier_name'],
        joinType: 'INNER',
      });

      expect(sql.startsWith('SELECT s.sale_id, s.amount, dim_tiers.tier_name\nFROM fct_sales AS s')).toBe(true);
      expect(sql).toContain('INNER JOIN dim_users AS dim_users');
      expect(sql).toContain('INNER JOIN dim_orgs AS dim_orgs');
      expect(sql).toContain('INNER JOIN dim_tiers AS dim_tiers');
    });

    test('ADV-3.5: Multi-target simultaneous join synthesis (generateMultiJoinSql)', () => {
      const pathUsers = complexGraph.findJoinPath('fct_sales', 'dim_users');
      const pathTiers = complexGraph.findJoinPath('fct_sales', 'dim_tiers', { maxHops: 4 });

      const multiSql = complexGraph.generateMultiJoinSql('fct_sales', [pathUsers, pathTiers], {
        baseAlias: 'sales',
        includeSelect: true,
        joinType: 'LEFT',
      });

      expect(multiSql).toContain('SELECT *');
      expect(multiSql).toContain('FROM fct_sales AS sales');
      // dim_users was already joined in pathUsers and along pathTiers; verify it is not joined redundantly twice
      const dimUsersCount = (multiSql.match(/JOIN dim_users AS dim_users/g) || []).length;
      expect(dimUsersCount).toBe(1);
      expect(multiSql).toContain('LEFT JOIN dim_orgs AS dim_orgs');
      expect(multiSql).toContain('LEFT JOIN dim_tiers AS dim_tiers');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 4. TESTING ALL 9 LINEAGE & JOIN ACTION TYPES
  // ══════════════════════════════════════════════════════════════════════════
  describe('4. Comprehensive Verification of All 9 Action Types', () => {
    let graph;

    beforeEach(() => {
      graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'src_pos.raw_transactions', tier: 'raw', tierRank: 10 },
        { name: 'stg_pos_transactions', tier: 'staging', tierRank: 30, dependsOn: ['src_pos.raw_transactions'] },
        { name: 'int_transactions_curated', tier: 'intermediate', tierRank: 60, dependsOn: ['stg_pos_transactions'] },
        { name: 'fct_pos_sales', tier: 'marts_fact', tierRank: 85, dependsOn: ['int_transactions_curated'] },
        { name: 'dim_stores', tier: 'marts_dim', tierRank: 80, dependsOn: ['stg_pos_transactions'] },
        { name: 'dim_products', tier: 'marts_dim', tierRank: 80 },
        { name: 'rpt_store_revenue', tier: 'marts_report', tierRank: 95, dependsOn: ['fct_pos_sales', 'dim_stores'] },
        { name: 'exec_dashboard_exposure', tier: 'marts_report', tierRank: 98, type: 'exposure', dependsOn: ['rpt_store_revenue'] },
        { name: 'monthly_gmv_metric', tier: 'marts_report', tierRank: 92, type: 'metric', dependsOn: ['fct_pos_sales'] },
      ]);

      // Semantic joins
      graph.addJoinRelationship({ fromModel: 'fct_pos_sales', fromColumn: 'store_id', toModel: 'dim_stores', toColumn: 'id', source: 'dbt_test', confidence: 1.0 });
      graph.addJoinRelationship({ fromModel: 'fct_pos_sales', fromColumn: 'product_id', toModel: 'dim_products', toColumn: 'id', source: 'dbt_test', confidence: 1.0 });
    });

    // Action 1: find_join_path
    test('Action 1: find_join_path returns optimal join route between models', () => {
      const res = graph.findJoinPath('fct_pos_sales', 'dim_stores');
      expect(res.found).toBe(true);
      expect(res.hopCount).toBe(1);
      expect(res.edges[0].fromColumn).toBe('store_id');
      expect(res.edges[0].toColumn).toBe('id');
    });

    // Action 2: find_all_join_paths
    test('Action 2: find_all_join_paths enumerates all valid join routes', () => {
      const allPaths = graph.findAllJoinPaths('fct_pos_sales', 'dim_stores');
      expect(allPaths.length).toBeGreaterThanOrEqual(1);
      expect(allPaths[0].found).toBe(true);
    });

    // Action 3: get_upstream
    test('Action 3: get_upstream resolves complete ancestor chain with depths', () => {
      const upstream = graph.getAllUpstream('rpt_store_revenue', { includeNodeInfo: true });
      expect(upstream.length).toBe(5); // fct_pos_sales, dim_stores, int_transactions_curated, stg_pos_transactions, src_pos.raw_transactions
      const names = upstream.map(u => u.name);
      expect(names).toContain('fct_pos_sales');
      expect(names).toContain('dim_stores');
      expect(names).toContain('int_transactions_curated');
      expect(names).toContain('stg_pos_transactions');
      expect(names).toContain('src_pos.raw_transactions');
    });

    // Action 4: get_downstream
    test('Action 4: get_downstream resolves complete descendant chain with depths', () => {
      const downstream = graph.getAllDownstream('stg_pos_transactions', { includeNodeInfo: true });
      expect(downstream.length).toBe(6); // int_transactions_curated, dim_stores, fct_pos_sales, rpt_store_revenue, exec_dashboard_exposure, monthly_gmv_metric
      const names = downstream.map(d => d.name);
      expect(names).toContain('int_transactions_curated');
      expect(names).toContain('fct_pos_sales');
      expect(names).toContain('rpt_store_revenue');
      expect(names).toContain('exec_dashboard_exposure');
    });

    // Action 5: validate_dag
    test('Action 5: validate_dag confirms acyclic DAG and reports node count', () => {
      const val = graph.validateDAG();
      expect(val.isValidDAG).toBe(true);
      expect(val.hasCycles).toBe(false);
      expect(val.totalNodes).toBe(9);
      expect(val.cycles).toEqual([]);
    });

    // Action 6: get_topological_order
    test('Action 6: get_topological_order returns valid build execution waves', () => {
      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves.length).toBeGreaterThanOrEqual(4);

      // Wave 0 must contain roots with 0 in-degree (src_pos.raw_transactions and dim_products)
      expect(waves[0]).toContain('src_pos.raw_transactions');
      expect(waves[0]).toContain('dim_products');

      // Flatten waves and verify topological constraint: every node appears after its parents
      const flat = waves.flat();
      expect(flat.indexOf('src_pos.raw_transactions')).toBeLessThan(flat.indexOf('stg_pos_transactions'));
      expect(flat.indexOf('stg_pos_transactions')).toBeLessThan(flat.indexOf('int_transactions_curated'));
      expect(flat.indexOf('int_transactions_curated')).toBeLessThan(flat.indexOf('fct_pos_sales'));
      expect(flat.indexOf('fct_pos_sales')).toBeLessThan(flat.indexOf('rpt_store_revenue'));
      expect(flat.indexOf('rpt_store_revenue')).toBeLessThan(flat.indexOf('exec_dashboard_exposure'));
    });

    // Action 7: blast_radius
    test('Action 7: blast_radius assesses impact on downstream marts, exposures, and metrics', () => {
      const blast = graph.calculateBlastRadius('stg_pos_transactions');
      expect(blast.impactLevel).toBe('CRITICAL');
      expect(blast.affectedExposures).toContain('exec_dashboard_exposure');
      expect(blast.affectedMetrics).toContain('monthly_gmv_metric');
      expect(blast.affectedMarts).toContain('fct_pos_sales');
      expect(blast.affectedMarts).toContain('dim_stores');
      expect(blast.affectedMarts).toContain('rpt_store_revenue');
      expect(blast.totalAffectedCount).toBe(6);
    });

    // Action 8: get_lineage_graph
    test('Action 8: get_lineage_graph & getGraphStats return comprehensive statistics and subgraphs', () => {
      const stats = graph.getGraphStats();
      expect(stats.nodeCount).toBe(9);
      expect(stats.tierCounts.marts_fact).toBe(1);
      expect(stats.tierCounts.marts_dim).toBe(2);
      expect(stats.tierCounts.marts_report).toBe(3);
      expect(stats.hasCycles).toBe(false);

      const subgraph = graph.getLineageSubgraph(['fct_pos_sales', 'rpt_store_revenue']);
      expect(subgraph.nodes.length).toBe(2);
      expect(subgraph.edges.length).toBe(1);
      expect(subgraph.edges[0]).toEqual({ from: 'fct_pos_sales', to: 'rpt_store_revenue', type: 'lineage' });
    });

    // Action 9: generate_join_sql
    test('Action 9: generate_join_sql formats clean ANSI SQL query with custom alias and options', () => {
      const joinPath = graph.findJoinPath('fct_pos_sales', 'dim_stores');
      const sql = graph.generateJoinSql(joinPath, 'sales_fact', {
        joinType: 'LEFT',
        includeSelect: true,
        selectColumns: ['sales_fact.*', 'dim_stores.store_name'],
      });

      expect(sql).toContain('SELECT sales_fact.*, dim_stores.store_name');
      expect(sql).toContain('FROM fct_pos_sales AS sales_fact');
      expect(sql).toContain('LEFT JOIN dim_stores AS dim_stores');
      expect(sql).toContain('  ON sales_fact.store_id = dim_stores.id');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 5. MCP HANDLER END-TO-END VERIFICATION (dbt_lineage_joins_graph)
  // ══════════════════════════════════════════════════════════════════════════
  describe('5. MCP Handler Execution & Protocol Compliance', () => {
    let handler;
    let mockDbtDir;

    beforeEach(() => {
      handler = new DbtSemanticHandler();
      mockDbtDir = path.join(tempDir, 'mock_dbt_project');
      fs.mkdirSync(path.join(mockDbtDir, 'models', 'marts'), { recursive: true });
      fs.mkdirSync(path.join(mockDbtDir, 'models', 'staging'), { recursive: true });

      // Write mock dbt_project.yml
      fs.writeFileSync(path.join(mockDbtDir, 'dbt_project.yml'), 'name: mock_shop\nversion: 1.0.0\n');

      // Write mock schema.yml with relationship tests
      const schemaYml = `
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
      - name: store_id
        tests:
          - relationships:
              to: ref('dim_stores')
              field: id
  - name: dim_customers
    description: "Customer dimension"
    columns:
      - name: id
        tests: [unique, not_null]
      - name: region_id
        tests:
          - relationships:
              to: ref('dim_regions')
              field: id
  - name: dim_regions
    description: "Region dimension"
    columns:
      - name: id
        tests: [unique, not_null]
  - name: dim_stores
    description: "Store dimension"
    columns:
      - name: id
        tests: [unique, not_null]
`;
      fs.writeFileSync(path.join(mockDbtDir, 'models', 'marts', 'schema.yml'), schemaYml);

      // Write SQL models
      fs.writeFileSync(path.join(mockDbtDir, 'models', 'marts', 'fct_orders.sql'), "select * from {{ ref('stg_orders') }}");
      fs.writeFileSync(path.join(mockDbtDir, 'models', 'marts', 'dim_customers.sql'), "select * from {{ ref('stg_customers') }}");
      fs.writeFileSync(path.join(mockDbtDir, 'models', 'marts', 'dim_regions.sql'), "select 1 as id");
      fs.writeFileSync(path.join(mockDbtDir, 'models', 'marts', 'dim_stores.sql'), "select 1 as id");
      fs.writeFileSync(path.join(mockDbtDir, 'models', 'staging', 'stg_orders.sql'), "select 1 as id");
      fs.writeFileSync(path.join(mockDbtDir, 'models', 'staging', 'stg_customers.sql'), "select 1 as id");
    });

    test('ADV-5.1: handleDbtLineageJoinsGraph resolves single multi-hop join through MCP', async () => {
      const response = await handler.handleDbtLineageJoinsGraph({
        project_dir: mockDbtDir,
        source_model: 'fct_orders',
        target_model: 'dim_regions',
        max_hops: 5,
        base_alias: 'o',
      });

      expect(response.content).toBeDefined();
      expect(response.content[0].type).toBe('text');
      expect(response.structuredContent).toBeDefined();

      const payload = response.structuredContent;
      expect(payload.node_count).toBeGreaterThanOrEqual(6);
      expect(payload.has_cycles).toBe(false);
      expect(payload.join_paths.length).toBe(1);

      const joinPath = payload.join_paths[0];
      expect(joinPath.found).toBe(true);
      expect(joinPath.hops).toBe(2);
      expect(joinPath.path).toEqual(['fct_orders', 'dim_customers', 'dim_regions']);

      expect(payload.sql_snippet).toContain('FROM fct_orders AS o');
      expect(payload.sql_snippet).toContain('LEFT JOIN dim_customers AS dim_customers');
      expect(payload.sql_snippet).toContain('LEFT JOIN dim_regions AS dim_regions');

      expect(payload._provenance.governance_level).toBe('READ_ONLY_INSPECTION');
      expect(payload._provenance.resolver).toBe('DbtLineageGraph');
    });

    test('ADV-5.2: handleDbtLineageJoinsGraph resolves target_models array with unified multi-join SQL', async () => {
      const response = await handler.handleDbtLineageJoinsGraph({
        project_dir: mockDbtDir,
        source_model: 'fct_orders',
        target_models: ['dim_customers', 'dim_regions', 'dim_stores'],
        base_alias: 'ord',
      });

      const payload = response.structuredContent;
      expect(payload.join_paths.length).toBe(3);
      expect(payload.join_paths.every(jp => jp.found)).toBe(true);

      expect(payload.sql_snippet).toContain('FROM fct_orders AS ord');
      expect(payload.sql_snippet).toContain('LEFT JOIN dim_customers AS dim_customers');
      expect(payload.sql_snippet).toContain('LEFT JOIN dim_regions AS dim_regions');
      expect(payload.sql_snippet).toContain('LEFT JOIN dim_stores AS dim_stores');
    });

    test('ADV-5.3: handleDbtLineageJoinsGraph supports lineage-only queries without targets', async () => {
      const response = await handler.handleDbtLineageJoinsGraph({
        project_dir: mockDbtDir,
        source_model: 'fct_orders',
        direction: 'upstream',
      });

      const payload = response.structuredContent;
      expect(payload.lineage).toBeDefined();
      expect(payload.lineage.model).toBe('fct_orders');
      expect(payload.lineage.upstream.length).toBeGreaterThanOrEqual(1);
      expect(payload.lineage.upstream.some(u => u.name === 'stg_orders')).toBe(true);
      expect(payload.lineage.downstream.length).toBe(0);
    });

    test('ADV-5.4: handleDbtLineageJoinsGraph gracefully handles non-existent models', async () => {
      const response = await handler.handleDbtLineageJoinsGraph({
        project_dir: mockDbtDir,
        source_model: 'fct_orders',
        target_model: 'non_existent_dimension',
      });

      const payload = response.structuredContent;
      expect(payload.join_paths.length).toBe(1);
      expect(payload.join_paths[0].found).toBe(false);
      expect(payload.join_paths[0].message).toContain('No join path found');
    });

    test('ADV-5.5: MCP Tool Registry definition conforms to MCP 2025-11-25 Specification', () => {
      const toolDefs = getToolDefinitions();
      const lineageTool = toolDefs.find(t => t.name === 'dbt_lineage_joins_graph');

      expect(lineageTool).toBeDefined();
      expect(lineageTool.title).toBe('Multi-Hop Lineage & Semantic Join Graph Resolver');
      expect(lineageTool.readOnlyHint).toBe(true);
      expect(lineageTool.inputSchema.properties).toHaveProperty('project_dir');
      expect(lineageTool.inputSchema.properties).toHaveProperty('source_model');
      expect(lineageTool.inputSchema.properties).toHaveProperty('target_model');
      expect(lineageTool.inputSchema.properties).toHaveProperty('target_models');
      expect(lineageTool.inputSchema.properties).toHaveProperty('max_hops');
      expect(lineageTool.inputSchema.properties).toHaveProperty('join_type');
      expect(lineageTool.inputSchema.properties).toHaveProperty('include_sql');
      expect(lineageTool.inputSchema.properties).toHaveProperty('confidence_threshold');
      expect(lineageTool.inputSchema.properties).toHaveProperty('base_alias');

      const meta = TOOL_METADATA.dbt_lineage_joins_graph;
      expect(meta).toBeDefined();
      expect(meta.outputSchema).toBeDefined();
      expect(meta.outputSchema.properties).toHaveProperty('join_paths');
      expect(meta.outputSchema.properties).toHaveProperty('lineage');
      expect(meta.outputSchema.properties).toHaveProperty('sql_snippet');
    });
  });
});
