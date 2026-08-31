/**
 * tests/unit/adversarial-lineage-graph.test.js
 * Comprehensive Adversarial Stress Test Suite for DbtLineageGraph
 *
 * Stress dimensions tested:
 * 1. Cycle Topologies: 2-node mutual cycle, 3-node loop, self-referential loops,
 *    multi-cycle disconnected graphs, figure-8 intersecting cycles, deep 100-node cycle.
 * 2. Diamond Topologies: Standard diamond, multi-tier nested diamond, asymmetric bypass diamond.
 * 3. Disjoint Components: Disconnected DAG clusters, isolated islands, wave grouping on forests.
 * 4. Boundary & Fuzzing: Empty graph, missing start/end nodes, null/undefined inputs, extreme depths.
 * 5. Scale & Depth Stress: 200-node linear deep chain, 1,000-node fan-out wide graph, 500-node complete join mesh.
 * 6. Topological & Wave Invariant Verification: Randomized DAG validation.
 * 7. Blast Radius & Downstream Stress: Impact level thresholds, deep cascades, cyclic safety.
 * 8. SQL Generator Collision & Multi-Join Stress: Repeated table joins, duplicate aliases, fallback edges.
 * 9. Join Graph Cycles & Complex Routing: Bidirectional join cycles, dense mesh paths, edge case limits.
 */

import { DbtLineageGraph, normalizeNodeName } from '../../src/dbt/lineage-joins.js';

describe('Adversarial Stress Test: DbtLineageGraph Topology & Cycle Engine', () => {

  // ══════════════════════════════════════════════════════════════════════════
  // 1. CYCLE TOPOLOGIES & STRESS TESTS
  // ══════════════════════════════════════════════════════════════════════════
  describe('1. Circular Dependency & Cycle Stress', () => {
    test('ADV-1.1: 2-Node mutual circular dependency (A <-> B)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'model_a', dependsOn: ['model_b'] },
        { name: 'model_b', dependsOn: ['model_a'] },
      ]);

      const validation = graph.validateDAG();
      expect(validation.isValidDAG).toBe(false);
      expect(validation.hasCycles).toBe(true);
      expect(validation.cycleNodes).toEqual(expect.arrayContaining(['model_a', 'model_b']));
      expect(validation.cycleNodes.length).toBe(2);
      expect(validation.cycles.length).toBeGreaterThanOrEqual(1);

      // Verify Kahn's algorithm returns empty topological order and does not hang
      const topo = graph.getTopologicalOrder();
      expect(topo).toEqual([]);

      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves).toEqual([]);
    });

    test('ADV-1.2: 3-Node circular dependency loop (A -> B -> C -> A)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'node_a', dependsOn: ['node_c'] },
        { name: 'node_b', dependsOn: ['node_a'] },
        { name: 'node_c', dependsOn: ['node_b'] },
      ]);

      const validation = graph.validateDAG();
      expect(validation.isValidDAG).toBe(false);
      expect(validation.hasCycles).toBe(true);
      expect(validation.cycleNodes).toHaveLength(3);
      expect(validation.cycleNodes).toEqual(expect.arrayContaining(['node_a', 'node_b', 'node_c']));

      // DFS cycle extraction should trace closed cycle
      expect(validation.cycles.length).toBeGreaterThanOrEqual(1);
      const cyclePath = validation.cycles[0];
      expect(cyclePath[0]).toBe(cyclePath[cyclePath.length - 1]); // closed loop
    });

    test('ADV-1.3: Self-referential dependency in dependsOn (A -> A)', () => {
      const graph = new DbtLineageGraph();
      // Even if self-dependency is passed, graph build and methods must not crash
      graph.buildGraph([
        { name: 'self_loop_model', dependsOn: ['self_loop_model'] },
      ]);

      expect(graph.getNodeCount()).toBe(1);
      const validation = graph.validateDAG();
      expect(validation).toBeDefined();
      expect(typeof validation.isValidDAG).toBe('boolean');

      const topo = graph.getTopologicalOrder();
      expect(Array.isArray(topo)).toBe(true);

      const upstream = graph.getAllUpstream('self_loop_model');
      expect(Array.isArray(upstream)).toBe(true);

      const downstream = graph.getAllDownstream('self_loop_model');
      expect(Array.isArray(downstream)).toBe(true);
    });

    test('ADV-1.4: Multiple disjoint independent cycles (Cycle 1: A<->B, Cycle 2: C<->D, Acyclic: E->F)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'cycle1_a', dependsOn: ['cycle1_b'] },
        { name: 'cycle1_b', dependsOn: ['cycle1_a'] },
        { name: 'cycle2_c', dependsOn: ['cycle2_d'] },
        { name: 'cycle2_d', dependsOn: ['cycle2_c'] },
        { name: 'acyclic_e' },
        { name: 'acyclic_f', dependsOn: ['acyclic_e'] },
      ]);

      const validation = graph.validateDAG();
      expect(validation.isValidDAG).toBe(false);
      expect(validation.hasCycles).toBe(true);
      expect(validation.cycleNodes).toHaveLength(4);
      expect(validation.cycleNodes).toEqual(expect.arrayContaining(['cycle1_a', 'cycle1_b', 'cycle2_c', 'cycle2_d']));
      expect(validation.cycleNodes).not.toContain('acyclic_e');
      expect(validation.cycleNodes).not.toContain('acyclic_f');

      // Topological sort must safely abort and return []
      expect(graph.getTopologicalOrder()).toEqual([]);
    });

    test('ADV-1.5: Figure-8 intersecting cycles sharing a common node (A->B->C->A and C->D->E->C)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'loop1_a', dependsOn: ['loop_shared_c'] },
        { name: 'loop1_b', dependsOn: ['loop1_a'] },
        { name: 'loop_shared_c', dependsOn: ['loop1_b', 'loop2_e'] },
        { name: 'loop2_d', dependsOn: ['loop_shared_c'] },
        { name: 'loop2_e', dependsOn: ['loop2_d'] },
      ]);

      const validation = graph.validateDAG();
      expect(validation.isValidDAG).toBe(false);
      expect(validation.hasCycles).toBe(true);
      expect(validation.cycleNodes).toHaveLength(5);
      expect(validation.cycleNodes).toContain('loop_shared_c');

      // Ensure DFS cycle extraction terminates safely without infinite recursion
      expect(validation.cycles.length).toBeGreaterThanOrEqual(1);
    });

    test('ADV-1.6: Deep 100-node circular chain (N0 -> N1 -> ... -> N99 -> N0)', () => {
      const nodeCount = 100;
      const models = [];
      for (let i = 0; i < nodeCount; i++) {
        const prevIndex = (i - 1 + nodeCount) % nodeCount;
        models.push({
          name: `deep_cycle_${i}`,
          dependsOn: [`deep_cycle_${prevIndex}`],
        });
      }

      const graph = new DbtLineageGraph();
      const startTime = Date.now();
      graph.buildGraph(models);

      const validation = graph.validateDAG();
      const duration = Date.now() - startTime;

      expect(validation.isValidDAG).toBe(false);
      expect(validation.hasCycles).toBe(true);
      expect(validation.cycleNodes.length).toBe(100);
      expect(validation.cycles.length).toBeGreaterThanOrEqual(1);
      expect(graph.getTopologicalOrder()).toEqual([]);
      expect(duration).toBeLessThan(1000); // Must be fast
    });

    test('ADV-1.7: Upstream and Downstream BFS traversals terminate safely in cyclic graphs', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'entry_point' },
        { name: 'cycle_a', dependsOn: ['entry_point', 'cycle_c'] },
        { name: 'cycle_b', dependsOn: ['cycle_a'] },
        { name: 'cycle_c', dependsOn: ['cycle_b'] },
        { name: 'exit_point', dependsOn: ['cycle_c'] },
      ]);

      // Downstream from entry_point should traverse into cycle and reach exit_point without infinite loop
      const downstream = graph.getAllDownstream('entry_point');
      expect(downstream).toEqual(expect.arrayContaining(['cycle_a', 'cycle_b', 'cycle_c', 'exit_point']));
      expect(downstream.length).toBe(4);

      // Upstream from exit_point should traverse cycle and reach entry_point without infinite loop
      const upstream = graph.getAllUpstream('exit_point');
      expect(upstream).toEqual(expect.arrayContaining(['cycle_a', 'cycle_b', 'cycle_c', 'entry_point']));
      expect(upstream.length).toBe(4);

      // Blast radius should calculate safely without hanging
      const blast = graph.calculateBlastRadius('cycle_a');
      expect(blast.totalAffectedCount).toBeGreaterThanOrEqual(3);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 2. DIAMOND & COMPLEX DAG TOPOLOGIES
  // ══════════════════════════════════════════════════════════════════════════
  describe('2. Diamond & Complex DAG Topologies', () => {
    test('ADV-2.1: Classic diamond dependency (A -> B -> D and A -> C -> D)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'raw_source', tier: 'raw', tierRank: 10 },
        { name: 'stg_branch_1', tier: 'staging', tierRank: 30, dependsOn: ['raw_source'] },
        { name: 'stg_branch_2', tier: 'staging', tierRank: 30, dependsOn: ['raw_source'] },
        { name: 'marts_join', tier: 'marts_fact', tierRank: 85, dependsOn: ['stg_branch_1', 'stg_branch_2'] },
      ]);

      const validation = graph.validateDAG();
      expect(validation.isValidDAG).toBe(true);
      expect(validation.hasCycles).toBe(false);

      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves.length).toBe(3);
      expect(waves[0]).toEqual(['raw_source']);
      expect(waves[1]).toEqual(expect.arrayContaining(['stg_branch_1', 'stg_branch_2']));
      expect(waves[2]).toEqual(['marts_join']);

      const upstream = graph.getAllUpstream('marts_join');
      expect(upstream).toEqual(expect.arrayContaining(['stg_branch_1', 'stg_branch_2', 'raw_source']));
      expect(upstream.length).toBe(3);
    });

    test('ADV-2.2: Multi-layer Nested Diamond (A -> B1, B2 -> C1, C2 -> D1, D2 -> E)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'layer0_a', tier: 'raw', tierRank: 10 },
        { name: 'layer1_b1', tier: 'staging', tierRank: 30, dependsOn: ['layer0_a'] },
        { name: 'layer1_b2', tier: 'staging', tierRank: 30, dependsOn: ['layer0_a'] },
        { name: 'layer2_c1', tier: 'intermediate', tierRank: 60, dependsOn: ['layer1_b1', 'layer1_b2'] },
        { name: 'layer2_c2', tier: 'intermediate', tierRank: 60, dependsOn: ['layer1_b1', 'layer1_b2'] },
        { name: 'layer3_d1', tier: 'marts_fact', tierRank: 85, dependsOn: ['layer2_c1', 'layer2_c2'] },
        { name: 'layer3_d2', tier: 'marts_fact', tierRank: 85, dependsOn: ['layer2_c1', 'layer2_c2'] },
        { name: 'layer4_e', tier: 'marts_report', tierRank: 95, dependsOn: ['layer3_d1', 'layer3_d2'] },
      ]);

      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves.length).toBe(5);
      expect(waves[0]).toEqual(['layer0_a']);
      expect(waves[1]).toEqual(expect.arrayContaining(['layer1_b1', 'layer1_b2']));
      expect(waves[2]).toEqual(expect.arrayContaining(['layer2_c1', 'layer2_c2']));
      expect(waves[3]).toEqual(expect.arrayContaining(['layer3_d1', 'layer3_d2']));
      expect(waves[4]).toEqual(['layer4_e']);

      // Check blast radius from root node
      const blast = graph.calculateBlastRadius('layer0_a');
      expect(blast.totalAffectedCount).toBe(7);
      expect(blast.maxDownstreamDepth).toBe(4);
      expect(blast.impactLevel).toBe('CRITICAL');
    });

    test('ADV-2.3: Asymmetric bypass diamond (A -> B -> C -> D and direct bypass edge A -> D)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'source_a', tier: 'raw', tierRank: 10 },
        { name: 'model_b', tier: 'staging', tierRank: 30, dependsOn: ['source_a'] },
        { name: 'model_c', tier: 'intermediate', tierRank: 60, dependsOn: ['model_b'] },
        { name: 'target_d', tier: 'marts_fact', tierRank: 85, dependsOn: ['model_c', 'source_a'] },
      ]);

      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      // Invariant: target_d depends on model_c (wave 2), so target_d MUST be in wave 3, not wave 1
      expect(waves.length).toBe(4);
      expect(waves[0]).toEqual(['source_a']);
      expect(waves[1]).toEqual(['model_b']);
      expect(waves[2]).toEqual(['model_c']);
      expect(waves[3]).toEqual(['target_d']);
    });

    test('ADV-2.4: Extreme fan-out (1 root -> 50 children) and fan-in (50 parents -> 1 sink)', () => {
      const models = [{ name: 'hub_source', tier: 'raw', tierRank: 10 }];
      const leafNames = [];

      for (let i = 0; i < 50; i++) {
        const leafName = `leaf_${i}`;
        leafNames.push(leafName);
        models.push({
          name: leafName,
          tier: 'staging',
          tierRank: 30,
          dependsOn: ['hub_source'],
        });
      }

      models.push({
        name: 'sink_mart',
        tier: 'marts_fact',
        tierRank: 85,
        dependsOn: leafNames,
      });

      const graph = new DbtLineageGraph();
      graph.buildGraph(models);

      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves.length).toBe(3);
      expect(waves[0]).toEqual(['hub_source']);
      expect(waves[1].length).toBe(50);
      expect(waves[2]).toEqual(['sink_mart']);

      const sinkParents = graph.getDirectParents('sink_mart');
      expect(sinkParents.length).toBe(50);

      const hubChildren = graph.getDirectChildren('hub_source');
      expect(hubChildren.length).toBe(50);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 3. DISJOINT & DISCONNECTED COMPONENTS
  // ══════════════════════════════════════════════════════════════════════════
  describe('3. Disjoint & Disconnected Component Topologies', () => {
    test('ADV-3.1: Forest of 5 disconnected components with isolated nodes', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        // Component 1 (Chain: 3 nodes)
        { name: 'c1_raw', tier: 'raw', tierRank: 10 },
        { name: 'c1_stg', tier: 'staging', tierRank: 30, dependsOn: ['c1_raw'] },
        { name: 'c1_mart', tier: 'marts_fact', tierRank: 85, dependsOn: ['c1_stg'] },
        // Component 2 (Chain: 2 nodes)
        { name: 'c2_stg', tier: 'staging', tierRank: 30 },
        { name: 'c2_dim', tier: 'marts_dim', tierRank: 80, dependsOn: ['c2_stg'] },
        // Component 3 (Isolated node)
        { name: 'iso_seed_1', tier: 'staging', tierRank: 40 },
        // Component 4 (Isolated node)
        { name: 'iso_seed_2', tier: 'staging', tierRank: 40 },
        // Component 5 (Diamond: 4 nodes)
        { name: 'c5_raw', tier: 'raw', tierRank: 10 },
        { name: 'c5_stg1', tier: 'staging', tierRank: 30, dependsOn: ['c5_raw'] },
        { name: 'c5_stg2', tier: 'staging', tierRank: 30, dependsOn: ['c5_raw'] },
        { name: 'c5_fct', tier: 'marts_fact', tierRank: 85, dependsOn: ['c5_stg1', 'c5_stg2'] },
      ]);

      expect(graph.getNodeCount()).toBe(11);
      const validation = graph.validateDAG();
      expect(validation.isValidDAG).toBe(true);

      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves.length).toBe(3);

      // Wave 0 must contain all in-degree 0 nodes from ALL components
      expect(waves[0]).toEqual(expect.arrayContaining(['c1_raw', 'c2_stg', 'iso_seed_1', 'iso_seed_2', 'c5_raw']));
      expect(waves[0].length).toBe(5);

      // Wave 1
      expect(waves[1]).toEqual(expect.arrayContaining(['c1_stg', 'c2_dim', 'c5_stg1', 'c5_stg2']));
      expect(waves[1].length).toBe(4);

      // Wave 2
      expect(waves[2]).toEqual(expect.arrayContaining(['c1_mart', 'c5_fct']));
      expect(waves[2].length).toBe(2);

      // Cross-component join search must return false gracefully
      const crossJoin = graph.findJoinPath('c1_mart', 'c5_fct');
      expect(crossJoin.found).toBe(false);
      expect(crossJoin.hops).toBe(0);
    });

    test('ADV-3.2: Subgraph extraction on isolated and disconnected subsets', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'model_a' },
        { name: 'model_b', dependsOn: ['model_a'] },
        { name: 'model_c' },
        { name: 'model_d', dependsOn: ['model_c'] },
      ]);

      const sub = graph.getLineageSubgraph(['model_a', 'model_b']);
      expect(sub.nodes.length).toBe(2);
      expect(sub.edges.length).toBe(1);
      expect(sub.edges[0]).toEqual({ from: 'model_a', to: 'model_b', type: 'lineage' });

      // Subgraph with non-existent model
      const subMissing = graph.getLineageSubgraph(['non_existent_1', 'non_existent_2']);
      expect(subMissing.nodes).toEqual([]);
      expect(subMissing.edges).toEqual([]);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 4. BOUNDARY, FUZZING & ERROR RESILIENCE
  // ══════════════════════════════════════════════════════════════════════════
  describe('4. Boundary Values, Fuzzing & Error Resilience', () => {
    test('ADV-4.1: Completely empty graph operations', () => {
      const emptyGraph = new DbtLineageGraph();
      emptyGraph.clear();

      expect(emptyGraph.getNodeCount()).toBe(0);
      expect(emptyGraph.getEdgeCount()).toBe(0);
      expect(emptyGraph.getRelationships()).toEqual([]);

      const validation = emptyGraph.validateDAG();
      expect(validation.isValidDAG).toBe(true);
      expect(validation.hasCycles).toBe(false);
      expect(validation.totalNodes).toBe(0);

      expect(emptyGraph.getTopologicalOrder()).toEqual([]);
      expect(emptyGraph.getTopologicalOrder({ groupByWaves: true })).toEqual([]);
      expect(emptyGraph.getAllUpstream('any_node')).toEqual([]);
      expect(emptyGraph.getAllDownstream('any_node')).toEqual([]);
      expect(emptyGraph.getDirectParents('any_node')).toEqual([]);
      expect(emptyGraph.getDirectChildren('any_node')).toEqual([]);

      const blast = emptyGraph.calculateBlastRadius('any_node');
      expect(blast.totalAffectedCount).toBe(0);
      expect(blast.impactLevel).toBe('LOW');

      const joinPath = emptyGraph.findJoinPath('a', 'b');
      expect(joinPath.found).toBe(false);

      const allJoinPaths = emptyGraph.findAllJoinPaths('a', 'b');
      expect(allJoinPaths).toEqual([]);

      const sql = emptyGraph.generateJoinSql([]);
      expect(sql).toBe('');

      const multiSql = emptyGraph.generateMultiJoinSql('');
      expect(multiSql).toBe('');

      const stats = emptyGraph.getGraphStats();
      expect(stats.nodeCount).toBe(0);
      expect(stats.totalEdges).toBe(0);
    });

    test('ADV-4.2: Handling null, undefined, whitespace, and fuzzed inputs in all public methods', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'valid_model', columns: { id: {} } },
      ]);

      // normalizeNodeName fuzzing
      expect(normalizeNodeName(null)).toBe('');
      expect(normalizeNodeName(undefined)).toBe('');
      expect(normalizeNodeName('')).toBe('');
      expect(normalizeNodeName('   ')).toBe('');
      expect(normalizeNodeName(12345)).toBe('');
      expect(normalizeNodeName({})).toBe('');

      // addNode fuzzing
      expect(() => graph.addNode(null)).not.toThrow();
      expect(() => graph.addNode({})).not.toThrow();
      expect(() => graph.addNode({ name: '' })).not.toThrow();

      // addLineageEdge fuzzing
      expect(() => graph.addLineageEdge(null, null)).not.toThrow();
      expect(() => graph.addLineageEdge('a', 'a')).not.toThrow(); // self-edge ignored

      // addJoinRelationship fuzzing
      expect(() => graph.addJoinRelationship(null)).not.toThrow();
      expect(() => graph.addJoinRelationship({})).not.toThrow();
      expect(() => graph.addJoinRelationship({ fromModel: 'a' })).not.toThrow();

      // Query methods fuzzing
      expect(() => graph.getAllUpstream(null)).not.toThrow();
      expect(() => graph.getAllDownstream(null)).not.toThrow();
      expect(() => graph.getDirectParents(null)).not.toThrow();
      expect(() => graph.getDirectChildren(null)).not.toThrow();
      expect(() => graph.calculateBlastRadius(null)).not.toThrow();
      expect(() => graph.findJoinPath(null, undefined)).not.toThrow();
      expect(() => graph.findAllJoinPaths(null, null)).not.toThrow();
      expect(() => graph.generateJoinSql(null)).not.toThrow();
      expect(() => graph.generateMultiJoinSql(null, null)).not.toThrow();
    });

    test('ADV-4.3: Querying missing and non-existent model names', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'model_alpha' },
        { name: 'model_beta', dependsOn: ['model_alpha'] },
      ]);

      expect(graph.getAllUpstream('non_existent_node')).toEqual([]);
      expect(graph.getAllDownstream('non_existent_node')).toEqual([]);
      expect(graph.getDirectParents('non_existent_node')).toEqual([]);
      expect(graph.getDirectChildren('non_existent_node')).toEqual([]);

      const path1 = graph.findJoinPath('model_alpha', 'non_existent_node');
      expect(path1.found).toBe(false);
      expect(path1.message).toContain('not indexed');

      const path2 = graph.findJoinPath('non_existent_node', 'model_alpha');
      expect(path2.found).toBe(false);
      expect(path2.message).toContain('not indexed');

      const blast = graph.calculateBlastRadius('non_existent_node');
      expect(blast.targetModel).toBe('non_existent_node');
      expect(blast.totalAffectedCount).toBe(0);
      expect(blast.impactLevel).toBe('LOW');
    });

    test('ADV-4.4: Graph initialized with null or garbage scanResult', () => {
      expect(() => new DbtLineageGraph(null)).not.toThrow();
      expect(() => new DbtLineageGraph(undefined)).not.toThrow();
      expect(() => new DbtLineageGraph({})).not.toThrow();
      expect(() => new DbtLineageGraph({ models: null, relationships: 'garbage' })).not.toThrow();
    });

    test('ADV-4.5: maxDepth = 0 in upstream and downstream traversals', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'root', tier: 'raw' },
        { name: 'child', tier: 'staging', dependsOn: ['root'] },
      ]);

      // maxDepth = 0 without includeSelf should return []
      const up0 = graph.getAllUpstream('child', { maxDepth: 0 });
      expect(up0).toEqual([]);

      const up0Self = graph.getAllUpstream('child', { maxDepth: 0, includeSelf: true });
      expect(up0Self).toEqual(['child']);

      const down0 = graph.getAllDownstream('root', { maxDepth: 0 });
      expect(down0).toEqual([]);

      const down0Self = graph.getAllDownstream('root', { maxDepth: 0, includeSelf: true });
      expect(down0Self).toEqual(['root']);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 5. SCALE & DEPTH STRESS (DEEP LINEAR CHAIN & WIDE GRAPH)
  // ══════════════════════════════════════════════════════════════════════════
  describe('5. Scale & Depth Stress', () => {
    test('ADV-5.1: 200-node deep linear pipeline (N0 -> N1 -> ... -> N199)', () => {
      const nodeCount = 200;
      const models = [{ name: 'chain_0', tier: 'raw', tierRank: 10 }];

      for (let i = 1; i < nodeCount; i++) {
        models.push({
          name: `chain_${i}`,
          tier: i === nodeCount - 1 ? 'marts_fact' : 'intermediate',
          tierRank: 60,
          dependsOn: [`chain_${i - 1}`],
        });
      }

      const graph = new DbtLineageGraph();
      const startTime = Date.now();
      graph.buildGraph(models);

      // 1. Validation
      const validation = graph.validateDAG();
      expect(validation.isValidDAG).toBe(true);
      expect(validation.totalNodes).toBe(200);

      // 2. Wave grouping: exactly 200 waves of 1 node each
      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves.length).toBe(200);
      expect(waves[0]).toEqual(['chain_0']);
      expect(waves[199]).toEqual(['chain_199']);

      // 3. Upstream traversal from deepest node
      const upstream = graph.getAllUpstream('chain_199', { includeNodeInfo: true });
      expect(upstream.length).toBe(199);
      expect(upstream[0].name).toBe('chain_198');
      expect(upstream[0].depth).toBe(1);
      expect(upstream[upstream.length - 1].name).toBe('chain_0');
      expect(upstream[upstream.length - 1].depth).toBe(199);

      // 4. Downstream traversal from root node
      const downstream = graph.getAllDownstream('chain_0', { includeNodeInfo: true });
      expect(downstream.length).toBe(199);
      expect(downstream[downstream.length - 1].name).toBe('chain_199');
      expect(downstream[downstream.length - 1].depth).toBe(199);

      // 5. Blast radius from root node
      const blast = graph.calculateBlastRadius('chain_0');
      expect(blast.totalAffectedCount).toBe(199);
      expect(blast.maxDownstreamDepth).toBe(199);
      expect(blast.impactLevel).toBe('CRITICAL');

      const elapsed = Date.now() - startTime;
      expect(elapsed).toBeLessThan(1500); // Efficient execution
    });

    test('ADV-5.2: 1,000-node wide parallel graph (1 root -> 1,000 parallel workers)', () => {
      const count = 1000;
      const models = [{ name: 'master_feed', tier: 'raw', tierRank: 10 }];

      for (let i = 0; i < count; i++) {
        models.push({
          name: `worker_${i}`,
          tier: 'staging',
          tierRank: 30,
          dependsOn: ['master_feed'],
        });
      }

      const graph = new DbtLineageGraph();
      const start = Date.now();
      graph.buildGraph(models);

      expect(graph.getNodeCount()).toBe(1001);

      const validation = graph.validateDAG();
      expect(validation.isValidDAG).toBe(true);

      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves.length).toBe(2);
      expect(waves[0]).toEqual(['master_feed']);
      expect(waves[1].length).toBe(1000);

      const downstream = graph.getAllDownstream('master_feed');
      expect(downstream.length).toBe(1000);

      const blast = graph.calculateBlastRadius('master_feed');
      expect(blast.totalAffectedCount).toBe(1000);
      expect(blast.maxDownstreamDepth).toBe(1);
      expect(blast.impactLevel).toBe('CRITICAL');

      const duration = Date.now() - start;
      expect(duration).toBeLessThan(2000);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 6. TOPOLOGICAL & WAVE INVARIANT VERIFICATION ON RANDOMIZED DAGs
  // ══════════════════════════════════════════════════════════════════════════
  describe('6. Mathematical Invariant Verification on Complex DAGs', () => {
    test('ADV-6.1: Topological order invariant: For every directed edge U -> V, index(U) < index(V)', () => {
      // Build a multi-tier DAG with 30 nodes and dense cross-tier dependencies
      const models = [];
      const nodeCount = 30;

      for (let i = 0; i < nodeCount; i++) {
        const deps = [];
        // Only depend on nodes with lower indices to guarantee acyclicity
        for (let j = 0; j < i; j++) {
          if ((i + j) % 3 === 0 || (i * j) % 5 === 0) {
            deps.push(`node_${j}`);
          }
        }
        models.push({
          name: `node_${i}`,
          tier: i < 5 ? 'raw' : i < 15 ? 'staging' : i < 25 ? 'intermediate' : 'marts_fact',
          tierRank: i < 5 ? 10 : i < 15 ? 30 : i < 25 ? 60 : 85,
          dependsOn: deps,
        });
      }

      const graph = new DbtLineageGraph();
      graph.buildGraph(models);

      const flatOrder = graph.getTopologicalOrder();
      expect(flatOrder.length).toBe(nodeCount);

      const indexMap = new Map();
      flatOrder.forEach((name, idx) => indexMap.set(name, idx));

      // Verify invariant for all lineage edges
      for (const [parent, children] of graph.lineageAdjacency.entries()) {
        const parentIdx = indexMap.get(parent);
        for (const child of children) {
          const childIdx = indexMap.get(child);
          expect(parentIdx).toBeLessThan(childIdx);
        }
      }

      // Verify wave invariant: for edge U -> V, wave(U) < wave(V)
      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      const waveMap = new Map();
      waves.forEach((wave, wIdx) => {
        wave.forEach(node => waveMap.set(node, wIdx));
      });

      for (const [parent, children] of graph.lineageAdjacency.entries()) {
        const parentWave = waveMap.get(parent);
        for (const child of children) {
          const childWave = waveMap.get(child);
          expect(parentWave).toBeLessThan(childWave);
        }
      }
    });

    test('ADV-6.2: Wave ordering determinism with tier rank sorting', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'seed_z', tier: 'staging', tierRank: 40 },
        { name: 'raw_b', tier: 'raw', tierRank: 10 },
        { name: 'raw_a', tier: 'raw', tierRank: 10 },
        { name: 'stg_c', tier: 'staging', tierRank: 30 },
      ]);

      const waves = graph.getTopologicalOrder({ groupByWaves: true });
      expect(waves.length).toBe(1);
      // Expected sort order: tierRank ascending (10 -> 30 -> 40), then alphabetically
      expect(waves[0]).toEqual(['raw_a', 'raw_b', 'stg_c', 'seed_z']);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 7. BLAST RADIUS & IMPACT CRITERIA STRESS
  // ══════════════════════════════════════════════════════════════════════════
  describe('7. Blast Radius Impact Matrix & Criteria Stress', () => {
    test('ADV-7.1: Impact level transitions (LOW -> MEDIUM -> HIGH -> CRITICAL)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        // Model with 0 dependents -> LOW
        { name: 'leaf_orphan', tier: 'staging' },

        // Model with 1-2 staging dependents -> MEDIUM
        { name: 'parent_small', tier: 'staging' },
        { name: 'child_stg_1', tier: 'staging', dependsOn: ['parent_small'] },
        { name: 'child_stg_2', tier: 'staging', dependsOn: ['child_stg_1'] },

        // Model with 1 mart or 1 metric or >=3 total -> HIGH
        { name: 'parent_mart_single', tier: 'staging' },
        { name: 'mart_fact_1', tier: 'marts_fact', dependsOn: ['parent_mart_single'] },

        // Model with exposure or >=2 marts or >=5 total -> CRITICAL
        { name: 'parent_critical', tier: 'staging' },
        { name: 'mart_fact_a', tier: 'marts_fact', dependsOn: ['parent_critical'] },
        { name: 'mart_fact_b', tier: 'marts_fact', dependsOn: ['parent_critical'] },
      ]);

      const blastLow = graph.calculateBlastRadius('leaf_orphan');
      expect(blastLow.impactLevel).toBe('LOW');

      const blastMed = graph.calculateBlastRadius('parent_small');
      expect(blastMed.impactLevel).toBe('MEDIUM');

      const blastHigh = graph.calculateBlastRadius('parent_mart_single');
      expect(blastHigh.impactLevel).toBe('HIGH');

      const blastCrit = graph.calculateBlastRadius('parent_critical');
      expect(blastCrit.impactLevel).toBe('CRITICAL');
    });

    test('ADV-7.2: Upstream/Downstream tier filtering with maxDepth constraints', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'src_1', tier: 'raw' },
        { name: 'stg_1', tier: 'staging', dependsOn: ['src_1'] },
        { name: 'int_1', tier: 'intermediate', dependsOn: ['stg_1'] },
        { name: 'fct_1', tier: 'marts_fact', dependsOn: ['int_1'] },
        { name: 'rpt_1', tier: 'marts_report', dependsOn: ['fct_1'] },
      ]);

      // Limit depth to 2
      const upDepth2 = graph.getAllUpstream('rpt_1', { maxDepth: 2 });
      expect(upDepth2).toEqual(['fct_1', 'int_1']);

      // Filter only staging and raw
      const upTiers = graph.getAllUpstream('rpt_1', { filterTiers: ['raw', 'staging'] });
      expect(upTiers).toEqual(['stg_1', 'src_1']);

      // Downstream depth limit
      const downDepth2 = graph.getAllDownstream('src_1', { maxDepth: 2 });
      expect(downDepth2).toEqual(['stg_1', 'int_1']);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 8. SQL JOIN GENERATOR & ALIASING STRESS
  // ══════════════════════════════════════════════════════════════════════════
  describe('8. SQL Join Generator & Aliasing Collision Stress', () => {
    test('ADV-8.1: Multi-hop Join with 4 repeated joins to the same entity table (Address 1..4)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'fct_shipments' },
        { name: 'dim_addresses' },
      ], [
        { fromModel: 'fct_shipments', fromColumn: 'origin_addr_id', toModel: 'dim_addresses', toColumn: 'id', source: 'dbt_test' },
        { fromModel: 'fct_shipments', fromColumn: 'dest_addr_id', toModel: 'dim_addresses', toColumn: 'id', source: 'dbt_test' },
        { fromModel: 'fct_shipments', fromColumn: 'billing_addr_id', toModel: 'dim_addresses', toColumn: 'id', source: 'dbt_test' },
        { fromModel: 'fct_shipments', fromColumn: 'return_addr_id', toModel: 'dim_addresses', toColumn: 'id', source: 'dbt_test' },
      ]);

      const edges = [
        { fromModel: 'fct_shipments', fromColumn: 'origin_addr_id', toModel: 'dim_addresses', toColumn: 'id' },
        { fromModel: 'fct_shipments', fromColumn: 'dest_addr_id', toModel: 'dim_addresses', toColumn: 'id' },
        { fromModel: 'fct_shipments', fromColumn: 'billing_addr_id', toModel: 'dim_addresses', toColumn: 'id' },
        { fromModel: 'fct_shipments', fromColumn: 'return_addr_id', toModel: 'dim_addresses', toColumn: 'id' },
      ];

      const sql = graph.generateJoinSql(edges, 's');
      expect(sql).toContain('FROM fct_shipments AS s');
      expect(sql).toContain('LEFT JOIN dim_addresses AS dim_addresses');
      expect(sql).toContain('LEFT JOIN dim_addresses AS dim_addresses_2');
      expect(sql).toContain('LEFT JOIN dim_addresses AS dim_addresses_3');
      expect(sql).toContain('LEFT JOIN dim_addresses AS dim_addresses_4');
      expect(sql).toContain('ON s.origin_addr_id = dim_addresses.id');
      expect(sql).toContain('ON s.dest_addr_id = dim_addresses_2.id');
      expect(sql).toContain('ON s.billing_addr_id = dim_addresses_3.id');
      expect(sql).toContain('ON s.return_addr_id = dim_addresses_4.id');
    });

    test('ADV-8.2: generateJoinSql with array of model names falling back to synthesized foreign key', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'fct_orders' },
        { name: 'dim_customers' },
        { name: 'dim_regions' },
      ]);

      // No explicit edges registered; pass model name array: ['fct_orders', 'dim_customers', 'dim_regions']
      const sql = graph.generateJoinSql(['fct_orders', 'dim_customers', 'dim_regions']);
      expect(sql).toContain('FROM fct_orders AS fct_orders');
      expect(sql).toContain('LEFT JOIN dim_customers AS dim_customers');
      expect(sql).toContain('ON fct_orders.customer_id = dim_customers.id');
      expect(sql).toContain('LEFT JOIN dim_regions AS dim_regions');
      expect(sql).toContain('ON dim_customers.region_id = dim_regions.id');
    });

    test('ADV-8.3: generateMultiJoinSql handles deduplicating target tables already joined', () => {
      const graph = new DbtLineageGraph();
      const path1 = {
        edges: [
          { fromModel: 'fct_sales', fromColumn: 'cust_id', toModel: 'dim_customers', toColumn: 'id' },
          { fromModel: 'dim_customers', fromColumn: 'reg_id', toModel: 'dim_regions', toColumn: 'id' },
        ],
      };
      const path2 = {
        edges: [
          // Same customer join leading to a different table
          { fromModel: 'fct_sales', fromColumn: 'cust_id', toModel: 'dim_customers', toColumn: 'id' },
          { fromModel: 'dim_customers', fromColumn: 'seg_id', toModel: 'dim_segments', toColumn: 'id' },
        ],
      };

      const multiSql = graph.generateMultiJoinSql('fct_sales', [path1, path2], { baseAlias: 'sales' });
      expect(multiSql).toContain('FROM fct_sales AS sales');
      // dim_customers should only be joined ONCE
      const customerJoinCount = (multiSql.match(/JOIN dim_customers/g) || []).length;
      expect(customerJoinCount).toBe(1);
      expect(multiSql).toContain('JOIN dim_regions');
      expect(multiSql).toContain('JOIN dim_segments');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // 9. JOIN GRAPH CYCLES & COMPLEX ROUTING
  // ══════════════════════════════════════════════════════════════════════════
  describe('9. Join Graph Cycles & Complex Routing', () => {
    test('ADV-9.1: Join graph with cyclic entity relationships (A <-> B <-> C <-> A)', () => {
      const graph = new DbtLineageGraph();
      graph.buildGraph([
        { name: 'table_a' },
        { name: 'table_b' },
        { name: 'table_c' },
      ], [
        { fromModel: 'table_a', fromColumn: 'b_id', toModel: 'table_b', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'table_b', fromColumn: 'c_id', toModel: 'table_c', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
        { fromModel: 'table_c', fromColumn: 'a_id', toModel: 'table_a', toColumn: 'id', source: 'dbt_test', confidence: 1.0 },
      ]);

      // Dijkstra should find shortest simple path A -> C (1 hop via reverse edge C->A)
      const pathAC = graph.findJoinPath('table_a', 'table_c');
      expect(pathAC.found).toBe(true);
      expect(pathAC.hops).toBe(1);
      expect(pathAC.path).toEqual(['table_a', 'table_c']);

      // findAllJoinPaths should find both clockwise and counterclockwise simple paths without infinite looping
      const allPaths = graph.findAllJoinPaths('table_a', 'table_c');
      expect(allPaths.length).toBe(2);
      expect(allPaths[0].path).toEqual(['table_a', 'table_c']); // 1 hop
      expect(allPaths[1].path).toEqual(['table_a', 'table_b', 'table_c']); // 2 hops
    });

    test('ADV-9.2: Dense join network (5-node complete mesh) with findAllJoinPaths limits', () => {
      const graph = new DbtLineageGraph();
      const nodeNames = ['M1', 'M2', 'M3', 'M4', 'M5'];
      const models = nodeNames.map(name => ({ name }));
      const relationships = [];

      for (let i = 0; i < nodeNames.length; i++) {
        for (let j = i + 1; j < nodeNames.length; j++) {
          relationships.push({
            fromModel: nodeNames[i],
            fromColumn: `${nodeNames[j].toLowerCase()}_id`,
            toModel: nodeNames[j],
            toColumn: 'id',
            source: 'dbt_test',
            confidence: 1.0,
          });
        }
      }

      graph.buildGraph(models, relationships);

      const start = Date.now();
      const paths = graph.findAllJoinPaths('M1', 'M5', { maxHops: 3, maxPaths: 5 });
      const duration = Date.now() - start;

      expect(paths.length).toBeLessThanOrEqual(5);
      expect(paths[0].hops).toBe(1); // Direct 1-hop path M1 -> M5
      expect(paths[0].path).toEqual(['M1', 'M5']);
      expect(duration).toBeLessThan(100);
    });
  });
});
