/**
 * src/dbt/lineage-joins.js
 * Model Dependency DAG & Multi-Hop Semantic Join Resolver
 *
 * Implements:
 * 1. Lineage DAG construction, upstream/downstream traversal, depth calculation, blast radius
 * 2. Cycle detection & topological ordering via Kahn's algorithm and DFS cycle tracing
 * 3. Multi-hop semantic join path resolution via Dijkstra (weighted confidence) & BFS (min hops)
 * 4. ANSI SQL join clause generator with automatic alias generation and collision prevention
 */

/**
 * Normalizes dbt model/source references into uniform identifier strings.
 * Handles ref('...'), source('...', '...'), uniqueIds (model.pkg.name), and bare strings.
 */
export function normalizeNodeName(raw) {
  if (!raw || typeof raw !== 'string') return '';
  const trimmed = raw.trim();

  // Handle ref('model_name') or ref("model_name") or {{ ref(...) }}
  const refMatch = trimmed.match(/(?:\{\{\s*)?ref\(\s*(?:['"][a-zA-Z0-9_.-]+['"]\s*,\s*)?['"]([a-zA-Z0-9_.-]+)['"]\s*\)(?:\s*\}\})?/i);
  if (refMatch) {
    return refMatch[1];
  }

  // Handle source('source_name', 'table_name') or {{ source(...) }}
  const sourceMatch = trimmed.match(/(?:\{\{\s*)?source\(\s*['"]([a-zA-Z0-9_.-]+)['"]\s*,\s*['"]([a-zA-Z0-9_.-]+)['"]\s*\)(?:\s*\}\})?/i);
  if (sourceMatch) {
    return `${sourceMatch[1]}.${sourceMatch[2]}`;
  }

  // Handle uniqueId like model.my_project.stg_orders, source.my_project.stripe.charges, seed.my_project.raw_zipcodes
  if (trimmed.startsWith('model.') || trimmed.startsWith('seed.') || trimmed.startsWith('snapshot.')) {
    const parts = trimmed.split('.');
    return parts[parts.length - 1];
  }
  if (trimmed.startsWith('source.')) {
    const parts = trimmed.split('.');
    if (parts.length >= 4) {
      return `${parts[2]}.${parts[3]}`;
    }
    return parts[parts.length - 1];
  }

  // Strip leading/trailing quotes if any
  return trimmed.replace(/^['"]|['"]$/g, '');
}

/**
 * Extract singular entity name from a model name.
 * e.g. "dim_customers" -> "customer", "stg_orders" -> "order", "users" -> "user"
 */
function extractSingularStem(modelName) {
  if (!modelName) return '';
  let stem = modelName
    .replace(/^(?:dim_|fct_|stg_|int_|rpt_|src_|raw_)/i, '')
    .toLowerCase();
  if (stem.endsWith('ies')) {
    stem = stem.slice(0, -3) + 'y';
  } else if (stem.endsWith('ses')) {
    stem = stem.slice(0, -2);
  } else if (stem.endsWith('s') && !stem.endsWith('ss')) {
    stem = stem.slice(0, -1);
  }
  return stem;
}

/**
 * Min-Priority Queue for Dijkstra shortest path search.
 */
class PriorityQueue {
  constructor() {
    this.elements = [];
  }

  enqueue(element, priority) {
    this.elements.push({ element, priority });
    this.elements.sort((a, b) => a.priority - b.priority);
  }

  dequeue() {
    return this.elements.shift()?.element;
  }

  isEmpty() {
    return this.elements.length === 0;
  }

  size() {
    return this.elements.length;
  }
}

/**
 * DbtLineageGraph
 * Dual-topology graph engine for dbt dependency DAGs and multi-hop semantic join routing.
 */
export class DbtLineageGraph {
  constructor(scanResultOrOptions = null) {
    this.nodes = new Map();                    // name -> NodeInfo
    this.lineageAdjacency = new Map();         // parent -> Set<child> (downstream edges)
    this.lineageReverseAdjacency = new Map();  // child -> Set<parent> (upstream edges)
    this.joinAdjacency = new Map();            // model -> JoinEdge[] (bidirectional join edges)
    this.relationships = [];                   // all canonical JoinEdge[]
    this.projectDir = null;
    this.manifestLoaded = false;

    if (scanResultOrOptions) {
      if (typeof scanResultOrOptions === 'object') {
        this.initFromScanResult(scanResultOrOptions);
      }
    }
  }

  /**
   * Initialize graph from a ProjectScanResult object (or deep scanner output)
   */
  initFromScanResult(scanResult) {
    this.projectDir = scanResult.projectDir || null;
    this.manifestLoaded = Boolean(scanResult.manifestLoaded);

    const models = Array.isArray(scanResult.models) ? scanResult.models : Object.values(scanResult.modelsMap || {});
    const sources = Array.isArray(scanResult.sources) ? scanResult.sources : Object.values(scanResult.sourcesMap || {});
    const seeds = Array.isArray(scanResult.seeds) ? scanResult.seeds : [];
    const snapshots = Array.isArray(scanResult.snapshots) ? scanResult.snapshots : [];
    const exposures = Array.isArray(scanResult.exposures) ? scanResult.exposures : Object.values(scanResult.exposuresMap || {});
    const metrics = Array.isArray(scanResult.metrics) ? scanResult.metrics : Object.values(scanResult.metricsMap || {});
    const semanticModels = Array.isArray(scanResult.semanticModels) ? scanResult.semanticModels : Object.values(scanResult.semanticModelsMap || {});
    const relationships = Array.isArray(scanResult.relationships) ? scanResult.relationships : [];

    this.buildGraph(models, relationships, sources, seeds, exposures, metrics, semanticModels, snapshots);
  }

  /**
   * Build complete dual-topology graph from collections of dbt components
   */
  buildGraph(
    models = [],
    relationships = [],
    sources = [],
    seeds = [],
    exposures = [],
    metrics = [],
    semanticModels = [],
    snapshots = []
  ) {
    this.clear();

    // 1. Ingest Sources
    for (const src of sources) {
      const srcName = src.sourceName ? `${src.sourceName}.${src.tableName}` : (src.name || src.tableName);
      if (!srcName) continue;
      this.addNode({
        name: srcName,
        type: 'source',
        tier: src.tier || 'raw',
        tierRank: src.tierRank || 10,
        tierDescription: 'Raw Ingestion Source',
        description: src.description || '',
        columns: src.columns || {},
        filePath: src.filePath || null,
        meta: src.meta || {},
        tags: src.tags || [],
      });
    }

    // 2. Ingest Seeds
    for (const seed of seeds) {
      if (!seed.name) continue;
      this.addNode({
        name: seed.name,
        type: 'seed',
        tier: seed.tier || 'staging',
        tierRank: seed.tierRank || 40,
        tierDescription: 'Static Seed Dataset',
        description: seed.description || '',
        columns: seed.columns || {},
        filePath: seed.filePath || null,
        meta: seed.meta || {},
        tags: seed.tags || [],
      });
    }

    // 3. Ingest Snapshots
    for (const snap of snapshots) {
      if (!snap.name) continue;
      this.addNode({
        name: snap.name,
        type: 'snapshot',
        tier: snap.tier || 'intermediate',
        tierRank: snap.tierRank || 60,
        tierDescription: 'Type-2 SCD Snapshot',
        description: snap.description || '',
        columns: snap.columns || {},
        filePath: snap.filePath || null,
        meta: snap.meta || {},
        tags: snap.tags || [],
      });
    }

    // 4. Ingest Models
    for (const m of models) {
      if (!m.name) continue;
      this.addNode({
        name: m.name,
        type: m.type || 'model',
        tier: m.tier || 'staging',
        tierRank: m.tierRank || 30,
        tierDescription: m.tierDescription || '',
        description: m.description || '',
        columns: m.columns || {},
        filePath: m.filePath || null,
        meta: m.meta || {},
        tags: m.tags || [],
        dependsOn: m.dependsOn || m.depends_on?.nodes || [],
        tests: m.tests || [],
      });
    }

    // 5. Ingest Exposures
    for (const exp of exposures) {
      if (!exp.name) continue;
      this.addNode({
        name: exp.name,
        type: 'exposure',
        tier: 'marts_report',
        tierRank: 95,
        tierDescription: 'Downstream Exposure / Dashboard',
        description: exp.description || '',
        columns: {},
        filePath: exp.filePath || null,
        meta: exp.meta || {},
        tags: exp.tags || [],
        dependsOn: exp.dependsOn || exp.depends_on?.nodes || [],
      });
    }

    // 6. Ingest Metrics
    for (const met of metrics) {
      if (!met.name) continue;
      this.addNode({
        name: met.name,
        type: 'metric',
        tier: 'marts_report',
        tierRank: 92,
        tierDescription: 'MetricFlow Business Metric',
        description: met.description || '',
        columns: {},
        filePath: met.filePath || null,
        meta: met.meta || {},
        tags: met.tags || [],
        dependsOn: met.model ? [met.model] : (met.dependsOn || []),
      });
    }

    // 7. Ingest Semantic Models (MetricFlow)
    for (const sm of semanticModels) {
      if (!sm.name) continue;
      if (!this.nodes.has(sm.name)) {
        this.addNode({
          name: sm.name,
          type: 'semantic_model',
          tier: 'marts_fact',
          tierRank: 85,
          tierDescription: 'MetricFlow Semantic Model',
          description: sm.description || '',
          columns: {},
          filePath: sm.filePath || null,
          meta: sm.meta || {},
          tags: sm.tags || [],
          dependsOn: sm.model ? [sm.model] : [],
        });
      }
    }

    // 8. Connect Lineage Dependencies (DAG Edges: parent -> child)
    for (const node of this.nodes.values()) {
      const deps = node.dependsOn || [];
      for (const rawDep of deps) {
        const parentName = normalizeNodeName(rawDep);
        if (parentName && parentName !== node.name) {
          // If parent node not yet present, create placeholder node
          if (!this.nodes.has(parentName)) {
            const isSource = parentName.includes('.');
            this.addNode({
              name: parentName,
              type: isSource ? 'source' : 'model',
              tier: isSource ? 'raw' : 'staging',
              tierRank: isSource ? 10 : 30,
              tierDescription: isSource ? 'External Source Reference' : 'Unindexed Model Reference',
              description: '',
              columns: {},
              filePath: null,
              meta: {},
              tags: [],
            });
          }
          this.addLineageEdge(parentName, node.name);
        }
      }
    }

    // 9. Ingest Explicit Relationships
    for (const rel of relationships) {
      this.addJoinRelationship(rel);
    }

    // 10. Extract column relationships from model tests if not already in relationships
    for (const model of models) {
      if (model.columns && typeof model.columns === 'object') {
        for (const [colName, colDef] of Object.entries(model.columns)) {
          if (Array.isArray(colDef?.tests)) {
            for (const test of colDef.tests) {
              if (typeof test === 'object' && test.relationships) {
                const r = test.relationships;
                const toTable = normalizeNodeName(r.to);
                const toColumn = r.field;
                if (toTable && toColumn) {
                  this.addJoinRelationship({
                    fromModel: model.name,
                    fromColumn: colName,
                    toModel: toTable,
                    toColumn,
                    relationshipType: 'many_to_one',
                    source: 'dbt_test',
                    confidence: 1.0,
                    sourceFile: model.filePath,
                  });
                }
              }
            }
          }
        }
      }
    }

    // 11. Extract MetricFlow Semantic Model entity joins
    this.extractMetricFlowEntities(semanticModels);

    // 12. Infer candidate joins by column naming heuristics (_id FK matching)
    this.inferForeignKeys();

    return this;
  }

  /**
   * Reset all internal graph states
   */
  clear() {
    this.nodes.clear();
    this.lineageAdjacency.clear();
    this.lineageReverseAdjacency.clear();
    this.joinAdjacency.clear();
    this.relationships = [];
  }

  /**
   * Add a node to the graph
   */
  addNode(nodeInfo) {
    if (!nodeInfo || !nodeInfo.name) return;
    const name = nodeInfo.name;
    if (!this.nodes.has(name)) {
      this.nodes.set(name, {
        name,
        type: nodeInfo.type || 'model',
        tier: nodeInfo.tier || 'staging',
        tierRank: typeof nodeInfo.tierRank === 'number' ? nodeInfo.tierRank : 30,
        tierDescription: nodeInfo.tierDescription || '',
        description: nodeInfo.description || '',
        columns: nodeInfo.columns || {},
        filePath: nodeInfo.filePath || null,
        meta: nodeInfo.meta || {},
        tags: nodeInfo.tags || [],
        dependsOn: nodeInfo.dependsOn || [],
      });
      this.lineageAdjacency.set(name, new Set());
      this.lineageReverseAdjacency.set(name, new Set());
      this.joinAdjacency.set(name, []);
    } else {
      // Merge updates into existing node if new info has richer metadata
      const existing = this.nodes.get(name);
      if (nodeInfo.description && !existing.description) existing.description = nodeInfo.description;
      if (nodeInfo.filePath && !existing.filePath) existing.filePath = nodeInfo.filePath;
      if (nodeInfo.columns && Object.keys(nodeInfo.columns).length > 0 && Object.keys(existing.columns).length === 0) {
        existing.columns = nodeInfo.columns;
      }
      if (nodeInfo.tier && existing.tier === 'staging' && nodeInfo.tier !== 'staging') {
        existing.tier = nodeInfo.tier;
        existing.tierRank = nodeInfo.tierRank;
        existing.tierDescription = nodeInfo.tierDescription;
      }
    }
  }

  /**
   * Add a directed lineage edge (parent -> child in execution DAG)
   */
  addLineageEdge(parentName, childName) {
    if (!parentName || !childName || parentName === childName) return;
    if (!this.lineageAdjacency.has(parentName)) {
      this.lineageAdjacency.set(parentName, new Set());
    }
    if (!this.lineageReverseAdjacency.has(childName)) {
      this.lineageReverseAdjacency.set(childName, new Set());
    }
    this.lineageAdjacency.get(parentName).add(childName);
    this.lineageReverseAdjacency.get(childName).add(parentName);
  }

  /**
   * Add a semantic join relationship (Bidirectional multi-graph edge)
   */
  addJoinRelationship(rel) {
    if (!rel || !rel.fromModel || !rel.toModel || !rel.fromColumn || !rel.toColumn) return;

    const fromModel = normalizeNodeName(rel.fromModel);
    const toModel = normalizeNodeName(rel.toModel);
    const fromColumn = String(rel.fromColumn).trim();
    const toColumn = String(rel.toColumn).trim();

    if (!fromModel || !toModel || !fromColumn || !toColumn) return;

    // Check for duplicate canonical edge
    const edgeIdForward = `${fromModel}.${fromColumn}->${toModel}.${toColumn}`;
    const edgeIdReverse = `${toModel}.${toColumn}->${fromModel}.${fromColumn}`;

    // Determine confidence and weight
    let confidence = typeof rel.confidence === 'number' ? rel.confidence : null;
    let source = rel.source || 'dbt_test';

    if (confidence === null) {
      if (source === 'dbt_test') confidence = 1.0;
      else if (source === 'metricflow_entity') confidence = 0.95;
      else if (source === 'meta_join') confidence = 0.90;
      else if (source === 'inferred_fk') confidence = 0.75;
      else confidence = 0.70;
    }

    // Weight formula: higher confidence gives lower weight (Dijkstra minimizes weight)
    const weight = rel.weight || Number((1.0 / Math.max(confidence, 0.1)).toFixed(3));

    const forwardEdge = {
      id: edgeIdForward,
      fromModel,
      from_model: fromModel,
      fromColumn,
      from_column: fromColumn,
      toModel,
      to_model: toModel,
      toColumn,
      to_column: toColumn,
      relationshipType: rel.relationshipType || 'many_to_one',
      relationship_type: rel.relationshipType || 'many_to_one',
      joinType: rel.joinType || 'LEFT',
      join_type: rel.joinType || 'LEFT',
      source,
      confidence,
      weight,
      sourceFile: rel.sourceFile || null,
    };

    const reverseRelationshipType =
      rel.relationshipType === 'many_to_one' ? 'one_to_many' :
      rel.relationshipType === 'one_to_many' ? 'many_to_one' :
      rel.relationshipType || 'one_to_one';

    const reverseEdge = {
      id: edgeIdReverse,
      fromModel: toModel,
      from_model: toModel,
      fromColumn: toColumn,
      from_column: toColumn,
      toModel: fromModel,
      to_model: fromModel,
      toColumn: fromColumn,
      to_column: fromColumn,
      relationshipType: reverseRelationshipType,
      relationship_type: reverseRelationshipType,
      joinType: rel.joinType || 'LEFT',
      join_type: rel.joinType || 'LEFT',
      source,
      confidence,
      weight,
      sourceFile: rel.sourceFile || null,
    };

    // Store in global canonical list if not present
    if (!this.relationships.some(r => r.id === edgeIdForward || r.id === edgeIdReverse)) {
      this.relationships.push(forwardEdge);
    }

    // Add to forward adjacency
    if (!this.joinAdjacency.has(fromModel)) this.joinAdjacency.set(fromModel, []);
    const fwdList = this.joinAdjacency.get(fromModel);
    if (!fwdList.some(e => e.id === edgeIdForward)) {
      fwdList.push(forwardEdge);
    }

    // Add to reverse adjacency
    if (!this.joinAdjacency.has(toModel)) this.joinAdjacency.set(toModel, []);
    const revList = this.joinAdjacency.get(toModel);
    if (!revList.some(e => e.id === edgeIdReverse)) {
      revList.push(reverseEdge);
    }
  }

  /**
   * Extract relationships from MetricFlow semantic models
   */
  extractMetricFlowEntities(semanticModels = []) {
    const primaryEntities = new Map(); // entityName -> { modelName, fieldName, sourceFile }

    for (const sm of semanticModels) {
      if (!Array.isArray(sm.entities)) continue;
      const modelName = sm.model || sm.name;
      for (const entity of sm.entities) {
        if (entity.type === 'primary') {
          primaryEntities.set(entity.name, {
            modelName,
            fieldName: entity.expr || entity.name,
            sourceFile: sm.filePath,
          });
        }
      }
    }

    for (const sm of semanticModels) {
      if (!Array.isArray(sm.entities)) continue;
      const fromModel = sm.model || sm.name;
      for (const entity of sm.entities) {
        if (entity.type === 'foreign' && primaryEntities.has(entity.name)) {
          const target = primaryEntities.get(entity.name);
          if (target.modelName !== fromModel) {
            this.addJoinRelationship({
              fromModel,
              fromColumn: entity.expr || entity.name,
              toModel: target.modelName,
              toColumn: target.fieldName,
              relationshipType: 'many_to_one',
              source: 'metricflow_entity',
              confidence: 0.95,
              sourceFile: sm.filePath || target.sourceFile,
            });
          }
        }
      }
    }
  }

  /**
   * Infer candidate joins from column naming conventions (e.g. customer_id in fct_orders matching id in dim_customers)
   */
  inferForeignKeys() {
    const modelNames = Array.from(this.nodes.keys());
    const modelStemMap = new Map(); // stem -> modelName[]

    for (const name of modelNames) {
      const stem = extractSingularStem(name);
      if (!stem) continue;
      if (!modelStemMap.has(stem)) modelStemMap.set(stem, []);
      modelStemMap.get(stem).push(name);
    }

    for (const [modelName, node] of this.nodes.entries()) {
      if (!node.columns || typeof node.columns !== 'object') continue;

      for (const colName of Object.keys(node.columns)) {
        const lowerCol = colName.toLowerCase();
        if (!lowerCol.endsWith('_id') && !lowerCol.endsWith('_fk')) continue;

        // Try candidate stem: e.g. "customer_id" -> stem "customer"
        const colStem = lowerCol.replace(/_(?:id|fk)$/, '');
        if (modelStemMap.has(colStem)) {
          const targetModels = modelStemMap.get(colStem);
          for (const targetModel of targetModels) {
            if (targetModel === modelName) continue;

            const targetNode = this.nodes.get(targetModel);
            const targetCols = targetNode?.columns ? Object.keys(targetNode.columns) : [];

            // Look for 'id', '<colStem>_id', or matching column name in target
            let targetPk = null;
            if (targetCols.includes('id')) targetPk = 'id';
            else if (targetCols.includes(`${colStem}_id`)) targetPk = `${colStem}_id`;
            else if (targetCols.includes(colName)) targetPk = colName;
            else if (targetCols.length === 0) targetPk = 'id'; // default heuristic

            if (targetPk) {
              this.addJoinRelationship({
                fromModel: modelName,
                fromColumn: colName,
                toModel: targetModel,
                toColumn: targetPk,
                relationshipType: 'many_to_one',
                source: 'inferred_fk',
                confidence: 0.75,
                sourceFile: node.filePath,
              });
            }
          }
        }
      }
    }
  }

  /**
   * Find the optimal semantic join path between sourceModel and targetModel using Dijkstra (default) or BFS.
   *
   * @param {string} sourceModel - Starting model name
   * @param {string} targetModel - Destination model name
   * @param {Object} options - Path search options
   * @returns {Object} JoinPathResult
   */
  findJoinPath(sourceModel, targetModel, options = {}) {
    const src = normalizeNodeName(sourceModel);
    const tgt = normalizeNodeName(targetModel);
    const maxHops = options.maxHops || options.max_hops || 5;
    const minConfidence = typeof options.minConfidence === 'number'
      ? options.minConfidence
      : (typeof options.confidenceThreshold === 'number' ? options.confidenceThreshold : (typeof options.confidence_threshold === 'number' ? options.confidence_threshold : 0.5));
    const defaultJoinType = options.defaultJoinType || options.joinType || options.join_type || 'LEFT';
    const algorithm = (options.algorithm || 'dijkstra').toLowerCase();
    const baseAlias = options.baseAlias || options.base_alias || null;

    // Self-join / zero-hop case
    if (src === tgt) {
      return {
        found: true,
        sourceModel: src,
        source_model: src,
        targetModel: tgt,
        target_model: tgt,
        hopCount: 0,
        hops: 0,
        path: [src],
        edges: [],
        confidence: 1.0,
        confidenceScore: 1.0,
        confidence_score: 1.0,
        totalCost: 0,
        sqlJoinClause: '',
        sql_join_clause: '',
      };
    }

    // Check if models exist in graph or have edges
    if (!this.joinAdjacency.has(src) || !this.joinAdjacency.has(tgt)) {
      return {
        found: false,
        sourceModel: src,
        source_model: src,
        targetModel: tgt,
        target_model: tgt,
        hopCount: 0,
        hops: 0,
        path: [],
        edges: [],
        confidence: 0,
        confidenceScore: 0,
        confidence_score: 0,
        totalCost: Infinity,
        sqlJoinClause: '',
        sql_join_clause: '',
        message: `No join path found between ${src} and ${tgt} within ${maxHops} hops (node not indexed)`,
      };
    }

    if (algorithm === 'bfs') {
      return this._findJoinPathBFS(src, tgt, maxHops, minConfidence, defaultJoinType, baseAlias, options);
    } else {
      return this._findJoinPathDijkstra(src, tgt, maxHops, minConfidence, defaultJoinType, baseAlias, options);
    }
  }

  /**
   * Dijkstra search minimizing cumulative path weight and maximizing confidence score.
   */
  _findJoinPathDijkstra(src, tgt, maxHops, minConfidence, defaultJoinType, baseAlias, options) {
    const pq = new PriorityQueue();
    // state: { currentModel, path, edges, totalCost, cumulativeConfidence }
    pq.enqueue({
      currentModel: src,
      path: [src],
      edges: [],
      totalCost: 0,
      cumulativeConfidence: 1.0,
    }, 0);

    // bestCost map: model -> minimum cost seen
    const bestCost = new Map();
    bestCost.set(src, 0);

    let bestSolution = null;

    while (!pq.isEmpty()) {
      const state = pq.dequeue();
      const { currentModel, path, edges, totalCost, cumulativeConfidence } = state;

      if (currentModel === tgt) {
        bestSolution = state;
        break;
      }

      if (edges.length >= maxHops) {
        continue;
      }

      const outgoing = this.joinAdjacency.get(currentModel) || [];
      for (const edge of outgoing) {
        const nextModel = edge.toModel;

        // Prevent simple cycles in path
        if (path.includes(nextModel)) continue;

        // Enforce confidence threshold on individual edge
        if (edge.confidence < minConfidence) continue;

        const nextConfidence = Number((cumulativeConfidence * edge.confidence).toFixed(4));
        if (nextConfidence < minConfidence) continue;

        const nextCost = Number((totalCost + edge.weight).toFixed(4));

        if (!bestCost.has(nextModel) || nextCost < bestCost.get(nextModel)) {
          bestCost.set(nextModel, nextCost);

          const edgeWithJoinType = {
            ...edge,
            joinType: edge.joinType || defaultJoinType,
            join_type: edge.join_type || defaultJoinType,
          };

          pq.enqueue({
            currentModel: nextModel,
            path: [...path, nextModel],
            edges: [...edges, edgeWithJoinType],
            totalCost: nextCost,
            cumulativeConfidence: nextConfidence,
          }, nextCost);
        }
      }
    }

    if (!bestSolution) {
      return {
        found: false,
        sourceModel: src,
        source_model: src,
        targetModel: tgt,
        target_model: tgt,
        hopCount: 0,
        hops: 0,
        path: [],
        edges: [],
        confidence: 0,
        confidenceScore: 0,
        confidence_score: 0,
        totalCost: Infinity,
        sqlJoinClause: '',
        sql_join_clause: '',
        message: `No join path found between ${src} and ${tgt} within ${maxHops} hops`,
      };
    }

    const sql = this.generateJoinSql(bestSolution.edges, baseAlias, { ...options, defaultJoinType });

    return {
      found: true,
      sourceModel: src,
      source_model: src,
      targetModel: tgt,
      target_model: tgt,
      hopCount: bestSolution.edges.length,
      hops: bestSolution.edges.length,
      path: bestSolution.path,
      edges: bestSolution.edges,
      confidence: bestSolution.cumulativeConfidence,
      confidenceScore: bestSolution.cumulativeConfidence,
      confidence_score: bestSolution.cumulativeConfidence,
      totalCost: bestSolution.totalCost,
      sqlJoinClause: sql,
      sql_join_clause: sql,
    };
  }

  /**
   * BFS unweighted minimum hop path search.
   */
  _findJoinPathBFS(src, tgt, maxHops, minConfidence, defaultJoinType, baseAlias, options) {
    const queue = [{
      currentModel: src,
      path: [src],
      edges: [],
      cumulativeConfidence: 1.0,
      totalCost: 0,
    }];
    const visited = new Set([src]);

    while (queue.length > 0) {
      const state = queue.shift();
      const { currentModel, path, edges, cumulativeConfidence, totalCost } = state;

      if (currentModel === tgt) {
        const sql = this.generateJoinSql(edges, baseAlias, { ...options, defaultJoinType });
        return {
          found: true,
          sourceModel: src,
          source_model: src,
          targetModel: tgt,
          target_model: tgt,
          hopCount: edges.length,
          hops: edges.length,
          path,
          edges,
          confidence: cumulativeConfidence,
          confidenceScore: cumulativeConfidence,
          confidence_score: cumulativeConfidence,
          totalCost,
          sqlJoinClause: sql,
          sql_join_clause: sql,
        };
      }

      if (edges.length >= maxHops) continue;

      const outgoing = this.joinAdjacency.get(currentModel) || [];
      for (const edge of outgoing) {
        const nextModel = edge.toModel;
        if (visited.has(nextModel)) continue;
        if (edge.confidence < minConfidence) continue;

        const nextConfidence = Number((cumulativeConfidence * edge.confidence).toFixed(4));
        if (nextConfidence < minConfidence) continue;

        visited.add(nextModel);
        const edgeWithJoinType = {
          ...edge,
          joinType: edge.joinType || defaultJoinType,
          join_type: edge.join_type || defaultJoinType,
        };

        queue.push({
          currentModel: nextModel,
          path: [...path, nextModel],
          edges: [...edges, edgeWithJoinType],
          cumulativeConfidence: nextConfidence,
          totalCost: totalCost + edge.weight,
        });
      }
    }

    return {
      found: false,
      sourceModel: src,
      source_model: src,
      targetModel: tgt,
      target_model: tgt,
      hopCount: 0,
      hops: 0,
      path: [],
      edges: [],
      confidence: 0,
      confidenceScore: 0,
      confidence_score: 0,
      totalCost: Infinity,
      sqlJoinClause: '',
      sql_join_clause: '',
      message: `No join path found between ${src} and ${tgt} within ${maxHops} hops`,
    };
  }

  /**
   * Find all simple paths between sourceModel and targetModel up to maxHops, sorted by cost & confidence.
   */
  findAllJoinPaths(sourceModel, targetModel, options = {}) {
    const src = normalizeNodeName(sourceModel);
    const tgt = normalizeNodeName(targetModel);
    const maxHops = options.maxHops || options.max_hops || 5;
    const minConfidence = typeof options.minConfidence === 'number'
      ? options.minConfidence
      : (typeof options.confidenceThreshold === 'number' ? options.confidenceThreshold : (typeof options.confidence_threshold === 'number' ? options.confidence_threshold : 0.5));
    const defaultJoinType = options.defaultJoinType || options.joinType || options.join_type || 'LEFT';
    const baseAlias = options.baseAlias || options.base_alias || null;
    const maxPaths = options.maxPaths || options.limit || 10;

    const results = [];

    const dfs = (currentModel, path, edges, cumulativeCost, cumulativeConfidence) => {
      if (currentModel === tgt && edges.length > 0) {
        const sql = this.generateJoinSql(edges, baseAlias, { ...options, defaultJoinType });
        results.push({
          found: true,
          sourceModel: src,
          source_model: src,
          targetModel: tgt,
          target_model: tgt,
          hopCount: edges.length,
          hops: edges.length,
          path: [...path],
          edges: [...edges],
          confidence: cumulativeConfidence,
          confidenceScore: cumulativeConfidence,
          confidence_score: cumulativeConfidence,
          totalCost: cumulativeCost,
          sqlJoinClause: sql,
          sql_join_clause: sql,
        });
        return;
      }

      if (edges.length >= maxHops) return;

      const outgoing = this.joinAdjacency.get(currentModel) || [];
      for (const edge of outgoing) {
        const nextModel = edge.toModel;
        if (path.includes(nextModel)) continue;
        if (edge.confidence < minConfidence) continue;

        const nextConfidence = Number((cumulativeConfidence * edge.confidence).toFixed(4));
        if (nextConfidence < minConfidence) continue;

        const edgeWithJoinType = {
          ...edge,
          joinType: edge.joinType || defaultJoinType,
          join_type: edge.join_type || defaultJoinType,
        };

        dfs(
          nextModel,
          [...path, nextModel],
          [...edges, edgeWithJoinType],
          cumulativeCost + edge.weight,
          nextConfidence
        );
      }
    };

    if (this.joinAdjacency.has(src) && this.joinAdjacency.has(tgt)) {
      dfs(src, [src], [], 0, 1.0);
    }

    // Sort by fewest hops first, then highest confidence, then lowest cost
    results.sort((a, b) => {
      if (a.hopCount !== b.hopCount) return a.hopCount - b.hopCount;
      if (b.confidence !== a.confidence) return b.confidence - a.confidence;
      return a.totalCost - b.totalCost;
    });

    return results.slice(0, maxPaths);
  }

  /**
   * Traverse upstream lineage dependencies (parents) of a model level-by-level via BFS.
   *
   * @param {string} modelName - Model to inspect
   * @param {Object} options - Traversal options (maxDepth, includeSelf, includeNodeInfo, filterTiers)
   * @returns {string[]|Array<{ name: string, depth: number, type: string, tier: string, tierRank: number, description: string, filePath: string }>}
   */
  getAllUpstream(modelName, options = {}) {
    const startNode = normalizeNodeName(modelName);
    const maxDepth = typeof options.maxDepth === 'number' ? options.maxDepth : Infinity;
    const includeSelf = Boolean(options.includeSelf);
    const includeNodeInfo = Boolean(options.includeNodeInfo);
    const filterTiers = Array.isArray(options.filterTiers) ? new Set(options.filterTiers) : null;

    if (!this.nodes.has(startNode) && !this.lineageReverseAdjacency.has(startNode)) {
      return [];
    }

    const visited = new Map(); // nodeName -> depth
    const queue = [{ name: startNode, depth: 0 }];
    visited.set(startNode, 0);

    const orderedResults = [];

    while (queue.length > 0) {
      const { name, depth } = queue.shift();

      if (name === startNode) {
        if (includeSelf) {
          orderedResults.push({ name, depth });
        }
      } else {
        orderedResults.push({ name, depth });
      }

      if (depth >= maxDepth) continue;

      const parents = this.lineageReverseAdjacency.get(name) || new Set();
      for (const parent of parents) {
        if (!visited.has(parent)) {
          visited.set(parent, depth + 1);
          queue.push({ name: parent, depth: depth + 1 });
        }
      }
    }

    // Filter and format results
    let filtered = orderedResults;
    if (filterTiers) {
      filtered = filtered.filter(item => {
        const node = this.nodes.get(item.name);
        return node && filterTiers.has(node.tier);
      });
    }

    if (includeNodeInfo) {
      return filtered.map(item => {
        const node = this.nodes.get(item.name) || {};
        return {
          name: item.name,
          depth: item.depth,
          type: node.type || (item.name.includes('.') ? 'source' : 'model'),
          tier: node.tier || (item.name.includes('.') ? 'raw' : 'staging'),
          tierRank: node.tierRank || 30,
          description: node.description || '',
          filePath: node.filePath || null,
        };
      });
    }

    return filtered.map(item => item.name);
  }

  /**
   * Traverse downstream lineage dependents (children) of a model level-by-level via BFS.
   *
   * @param {string} modelName - Model to inspect
   * @param {Object} options - Traversal options (maxDepth, includeSelf, includeNodeInfo, filterTiers)
   * @returns {string[]|Array<{ name: string, depth: number, type: string, tier: string, tierRank: number, description: string, filePath: string }>}
   */
  getAllDownstream(modelName, options = {}) {
    const startNode = normalizeNodeName(modelName);
    const maxDepth = typeof options.maxDepth === 'number' ? options.maxDepth : Infinity;
    const includeSelf = Boolean(options.includeSelf);
    const includeNodeInfo = Boolean(options.includeNodeInfo);
    const filterTiers = Array.isArray(options.filterTiers) ? new Set(options.filterTiers) : null;

    if (!this.nodes.has(startNode) && !this.lineageAdjacency.has(startNode)) {
      return [];
    }

    const visited = new Map(); // nodeName -> depth
    const queue = [{ name: startNode, depth: 0 }];
    visited.set(startNode, 0);

    const orderedResults = [];

    while (queue.length > 0) {
      const { name, depth } = queue.shift();

      if (name === startNode) {
        if (includeSelf) {
          orderedResults.push({ name, depth });
        }
      } else {
        orderedResults.push({ name, depth });
      }

      if (depth >= maxDepth) continue;

      const children = this.lineageAdjacency.get(name) || new Set();
      for (const child of children) {
        if (!visited.has(child)) {
          visited.set(child, depth + 1);
          queue.push({ name: child, depth: depth + 1 });
        }
      }
    }

    // Filter and format results
    let filtered = orderedResults;
    if (filterTiers) {
      filtered = filtered.filter(item => {
        const node = this.nodes.get(item.name);
        return node && filterTiers.has(node.tier);
      });
    }

    if (includeNodeInfo) {
      return filtered.map(item => {
        const node = this.nodes.get(item.name) || {};
        return {
          name: item.name,
          depth: item.depth,
          type: node.type || 'model',
          tier: node.tier || 'staging',
          tierRank: node.tierRank || 30,
          description: node.description || '',
          filePath: node.filePath || null,
        };
      });
    }

    return filtered.map(item => item.name);
  }

  /**
   * Get direct 1-hop upstream parents
   */
  getDirectParents(modelName) {
    const name = normalizeNodeName(modelName);
    const parents = this.lineageReverseAdjacency.get(name);
    return parents ? Array.from(parents) : [];
  }

  /**
   * Get direct 1-hop downstream children
   */
  getDirectChildren(modelName) {
    const name = normalizeNodeName(modelName);
    const children = this.lineageAdjacency.get(name);
    return children ? Array.from(children) : [];
  }

  /**
   * Compute blast radius and downstream impact analysis for modifying a model
   */
  calculateBlastRadius(modelName) {
    const name = normalizeNodeName(modelName);
    const node = this.nodes.get(name) || {};
    const downstreamWithInfo = this.getAllDownstream(name, { includeNodeInfo: true });

    let maxDownstreamDepth = 0;
    const affectedModels = [];
    const affectedMarts = [];
    const affectedExposures = [];
    const affectedMetrics = [];

    for (const item of downstreamWithInfo) {
      if (item.depth > maxDownstreamDepth) {
        maxDownstreamDepth = item.depth;
      }
      affectedModels.push(item.name);

      if (['marts_fact', 'marts_dim', 'marts_report'].includes(item.tier)) {
        affectedMarts.push(item.name);
      }
      if (item.type === 'exposure') {
        affectedExposures.push(item.name);
      }
      if (item.type === 'metric') {
        affectedMetrics.push(item.name);
      }
    }

    // Determine impact level
    let impactLevel = 'LOW';
    const totalAffected = downstreamWithInfo.length;

    if (affectedExposures.length > 0 || affectedMarts.length >= 2 || totalAffected >= 5) {
      impactLevel = 'CRITICAL';
    } else if (affectedMarts.length >= 1 || affectedMetrics.length > 0 || totalAffected >= 3) {
      impactLevel = 'HIGH';
    } else if (totalAffected >= 1) {
      impactLevel = 'MEDIUM';
    }

    return {
      targetModel: name,
      target_model: name,
      tier: node.tier || 'staging',
      totalAffectedCount: totalAffected,
      total_affected_count: totalAffected,
      maxDownstreamDepth,
      max_downstream_depth: maxDownstreamDepth,
      impactLevel,
      impact_level: impactLevel,
      affectedModels,
      affected_models: affectedModels,
      affectedMarts,
      affected_marts: affectedMarts,
      affectedExposures,
      affected_exposures: affectedExposures,
      affectedMetrics,
      affected_metrics: affectedMetrics,
      downstreamDetails: downstreamWithInfo,
      downstream_details: downstreamWithInfo,
    };
  }

  /**
   * Validate DAG integrity and detect circular dependencies using Kahn's algorithm and DFS cycle search.
   *
   * @returns {Object} { isValidDAG, hasCycle, hasCycles, cycleCount, cycles, cycleNodes, cycle_nodes, totalNodes, message }
   */
  validateDAG() {
    const allNodeNames = Array.from(this.nodes.keys());
    const totalNodes = allNodeNames.length;

    if (totalNodes === 0) {
      return {
        isValidDAG: true,
        is_valid_dag: true,
        hasCycle: false,
        hasCycles: false,
        has_cycles: false,
        cycleCount: 0,
        cycle_count: 0,
        cycles: [],
        cycleNodes: [],
        cycle_nodes: [],
        totalNodes: 0,
        total_nodes: 0,
        message: 'DAG is empty (0 nodes).',
      };
    }

    // Calculate in-degrees for all nodes
    const inDegree = new Map();
    for (const name of allNodeNames) {
      inDegree.set(name, 0);
    }
    for (const children of this.lineageAdjacency.values()) {
      for (const child of children) {
        if (inDegree.has(child)) {
          inDegree.set(child, inDegree.get(child) + 1);
        } else {
          inDegree.set(child, 1);
        }
      }
    }

    // Seed queue with inDegree === 0
    const queue = [];
    for (const [name, deg] of inDegree.entries()) {
      if (deg === 0) {
        queue.push(name);
      }
    }

    let processedCount = 0;
    while (queue.length > 0) {
      const u = queue.shift();
      processedCount++;

      const children = this.lineageAdjacency.get(u) || new Set();
      for (const v of children) {
        const newDeg = (inDegree.get(v) || 1) - 1;
        inDegree.set(v, newDeg);
        if (newDeg === 0) {
          queue.push(v);
        }
      }
    }

    const hasCycle = processedCount < inDegree.size;
    const cycleNodes = [];
    const cycles = [];

    if (hasCycle) {
      for (const [name, deg] of inDegree.entries()) {
        if (deg > 0) {
          cycleNodes.push(name);
        }
      }

      // Extract exact cycle chains via DFS 3-color search
      const visitedState = new Map(); // 0: unvisited, 1: visiting (in stack), 2: visited
      for (const node of cycleNodes) visitedState.set(node, 0);

      const stack = [];
      const findCycleDfs = (u) => {
        visitedState.set(u, 1);
        stack.push(u);

        const neighbors = this.lineageAdjacency.get(u) || new Set();
        for (const v of neighbors) {
          if (!cycleNodes.includes(v)) continue;
          if (visitedState.get(v) === 1) {
            // Cycle found!
            const cycleStartIndex = stack.indexOf(v);
            if (cycleStartIndex !== -1) {
              const cyclePath = stack.slice(cycleStartIndex);
              cyclePath.push(v); // close loop
              cycles.push(cyclePath);
            }
          } else if (visitedState.get(v) === 0) {
            findCycleDfs(v);
          }
        }

        stack.pop();
        visitedState.set(u, 2);
      };

      for (const node of cycleNodes) {
        if (visitedState.get(node) === 0) {
          findCycleDfs(node);
        }
      }
    }

    return {
      isValidDAG: !hasCycle,
      is_valid_dag: !hasCycle,
      hasCycle,
      hasCycles: hasCycle,
      has_cycles: hasCycle,
      cycleCount: cycles.length || (hasCycle ? 1 : 0),
      cycle_count: cycles.length || (hasCycle ? 1 : 0),
      cycles,
      cycleNodes,
      cycle_nodes: cycleNodes,
      totalNodes: inDegree.size,
      total_nodes: inDegree.size,
      message: hasCycle
        ? `⚠️ Cyclic dependency detected among ${cycleNodes.length} nodes: [${cycleNodes.slice(0, 5).join(', ')}${cycleNodes.length > 5 ? '...' : ''}]`
        : `✅ Valid DAG with ${inDegree.size} nodes and 0 cycles.`,
    };
  }

  /**
   * Alias for validateDAG()
   */
  detectCycles() {
    return this.validateDAG();
  }

  /**
   * Boolean check for cycles
   */
  hasCycles() {
    return this.validateDAG().hasCycles;
  }

  /**
   * List of nodes involved in cycles
   */
  getCycleNodes() {
    return this.validateDAG().cycleNodes;
  }

  /**
   * Compute topological build / execution order using Kahn's algorithm.
   *
   * @param {Object} options - Sorting options (groupByWaves, filterTiers)
   * @returns {string[]|Array<string[]>} Linear order or wave groupings
   */
  getTopologicalOrder(options = {}) {
    const groupByWaves = Boolean(options.groupByWaves || options.group_by_waves);
    const filterTiers = Array.isArray(options.filterTiers) ? new Set(options.filterTiers) : null;

    const allNodeNames = Array.from(this.nodes.keys());
    if (allNodeNames.length === 0) return [];

    const inDegree = new Map();
    for (const name of allNodeNames) {
      inDegree.set(name, 0);
    }
    for (const children of this.lineageAdjacency.values()) {
      for (const child of children) {
        if (inDegree.has(child)) {
          inDegree.set(child, inDegree.get(child) + 1);
        } else {
          inDegree.set(child, 1);
        }
      }
    }

    let currentWave = [];
    for (const [name, deg] of inDegree.entries()) {
      if (deg === 0) {
        currentWave.push(name);
      }
    }

    const waves = [];
    const flatOrder = [];
    let processedCount = 0;

    while (currentWave.length > 0) {
      // Sort wave deterministically by tierRank (ascending) then name
      currentWave.sort((a, b) => {
        const nodeA = this.nodes.get(a);
        const nodeB = this.nodes.get(b);
        const rankA = nodeA?.tierRank || 30;
        const rankB = nodeB?.tierRank || 30;
        if (rankA !== rankB) return rankA - rankB;
        return a.localeCompare(b);
      });

      let waveFiltered = currentWave;
      if (filterTiers) {
        waveFiltered = currentWave.filter(name => {
          const node = this.nodes.get(name);
          return node && filterTiers.has(node.tier);
        });
      }

      if (waveFiltered.length > 0) {
        waves.push(waveFiltered);
      }

      const nextWave = [];
      for (const u of currentWave) {
        flatOrder.push(u);
        processedCount++;

        const children = this.lineageAdjacency.get(u) || new Set();
        for (const v of children) {
          const newDeg = (inDegree.get(v) || 1) - 1;
          inDegree.set(v, newDeg);
          if (newDeg === 0) {
            nextWave.push(v);
          }
        }
      }

      currentWave = nextWave;
    }

    // If graph has cycle, Kahn's algorithm cannot complete topological order
    if (processedCount < inDegree.size) {
      return [];
    }

    if (groupByWaves) {
      return waves;
    }

    if (filterTiers) {
      return flatOrder.filter(name => {
        const node = this.nodes.get(name);
        return node && filterTiers.has(node.tier);
      });
    }

    return flatOrder;
  }

  /**
   * Generate clean, standard ANSI SQL JOIN clause for a single path or collection of edges.
   *
   * @param {JoinPathResult|JoinEdge[]|string[]} pathOrEdges - Path object, edge list, or node name list
   * @param {string} baseAlias - Optional base table alias
   * @param {Object} options - Formatting options
   * @returns {string} Syntactically valid ANSI SQL snippet
   */
  generateJoinSql(pathOrEdges, baseAlias = null, options = {}) {
    if (!pathOrEdges) return '';

    let edges = [];
    let rootModel = null;

    if (Array.isArray(pathOrEdges)) {
      if (pathOrEdges.length === 0) return '';
      if (typeof pathOrEdges[0] === 'string') {
        // Path is array of model names: ['fct_orders', 'dim_customers', 'dim_regions']
        rootModel = pathOrEdges[0];
        for (let i = 0; i < pathOrEdges.length - 1; i++) {
          const u = pathOrEdges[i];
          const v = pathOrEdges[i + 1];
          const outgoing = this.joinAdjacency.get(u) || [];
          const matchedEdge = outgoing.find(e => e.toModel === v);
          if (matchedEdge) {
            edges.push(matchedEdge);
          } else {
            // Synthesize fallback edge if not explicitly in adjacency
            edges.push({
              fromModel: u,
              fromColumn: `${extractSingularStem(v)}_id`,
              toModel: v,
              toColumn: 'id',
              joinType: options.defaultJoinType || options.joinType || 'LEFT',
            });
          }
        }
      } else if (typeof pathOrEdges[0] === 'object') {
        edges = pathOrEdges;
        rootModel = edges[0]?.fromModel || null;
      }
    } else if (typeof pathOrEdges === 'object') {
      if (Array.isArray(pathOrEdges.edges)) {
        edges = pathOrEdges.edges;
        rootModel = pathOrEdges.sourceModel || pathOrEdges.source_model || edges[0]?.fromModel;
      }
    }

    if (!rootModel && edges.length > 0) {
      rootModel = edges[0].fromModel;
    }

    if (!rootModel) return '';

    const defaultJoinType = (options.defaultJoinType || options.joinType || options.join_type || 'LEFT').toUpperCase();
    const shortAlias = Boolean(options.shortAlias || options.short_alias);
    const aliasMap = options.aliasMap || options.alias_map || {};

    // Helper for table aliasing
    const tableAliasCounts = new Map();
    const resolveAlias = (table, isRoot = false) => {
      if (isRoot && baseAlias) return baseAlias;
      if (aliasMap[table]) return aliasMap[table];
      if (shortAlias) {
        const letter = table.replace(/^(?:dim_|fct_|stg_|int_|rpt_|src_|raw_)/i, '')[0].toLowerCase();
        return letter;
      }
      return table;
    };

    const rootAlias = resolveAlias(rootModel, true);
    tableAliasCounts.set(rootAlias, 1);

    const modelToActiveAlias = new Map();
    modelToActiveAlias.set(rootModel, rootAlias);

    const lines = [];
    if (options.includeSelect || options.include_select) {
      const selectCols = Array.isArray(options.selectColumns) ? options.selectColumns.join(', ') : '*';
      lines.push(`SELECT ${selectCols}`);
    }

    // Format FROM clause
    if (rootAlias === rootModel) {
      lines.push(`FROM ${rootModel} AS ${rootAlias}`);
    } else {
      lines.push(`FROM ${rootModel} AS ${rootAlias}`);
    }

    // Format JOIN clauses
    for (const edge of edges) {
      const fromTable = edge.fromModel || edge.from_model;
      const fromCol = edge.fromColumn || edge.from_column;
      const toTable = edge.toModel || edge.to_model;
      const toCol = edge.toColumn || edge.to_column;
      const specifiedJoinType = options.defaultJoinType || options.joinType || options.join_type;
      const joinType = (specifiedJoinType || edge.joinType || edge.join_type || defaultJoinType).toUpperCase();

      const fromAlias = modelToActiveAlias.get(fromTable) || fromTable;

      // Handle duplicate joins to same target table by suffixing instance index if collision
      let toAliasBase = resolveAlias(toTable, false);
      let toAlias = toAliasBase;
      const currentCount = tableAliasCounts.get(toAliasBase) || 0;
      if (currentCount > 0) {
        toAlias = `${toAliasBase}_${currentCount + 1}`;
      }
      tableAliasCounts.set(toAliasBase, currentCount + 1);
      modelToActiveAlias.set(toTable, toAlias);

      const joinClause = `${joinType} JOIN ${toTable} AS ${toAlias}`;
      const onCondition = `  ON ${fromAlias}.${fromCol} = ${toAlias}.${toCol}`;

      lines.push(joinClause);
      lines.push(onCondition);
    }

    return lines.join('\n');
  }

  /**
   * Generate unified SQL join query for a source model joining multiple target models simultaneously.
   */
  generateMultiJoinSql(sourceModel, joinPaths = [], options = {}) {
    const src = normalizeNodeName(sourceModel);
    if (!src) return '';
    if (!Array.isArray(joinPaths) || joinPaths.length === 0) {
      return `FROM ${src} AS ${options.baseAlias || options.base_alias || src}`;
    }

    const defaultJoinType = (options.defaultJoinType || options.joinType || options.join_type || 'LEFT').toUpperCase();
    const baseAlias = options.baseAlias || options.base_alias || null;
    const rootAlias = baseAlias || src;

    const joinedTables = new Set([src]);
    const modelToActiveAlias = new Map();
    modelToActiveAlias.set(src, rootAlias);

    const lines = [];
    if (options.includeSelect || options.include_select) {
      lines.push(`SELECT *`);
    }
    lines.push(`FROM ${src} AS ${rootAlias}`);

    for (const jp of joinPaths) {
      const edges = jp.edges || [];
      for (const edge of edges) {
        const fromTable = edge.fromModel || edge.from_model;
        const fromCol = edge.fromColumn || edge.from_column;
        const toTable = edge.toModel || edge.to_model;
        const toCol = edge.toColumn || edge.to_column;
        const joinType = (edge.joinType || edge.join_type || defaultJoinType).toUpperCase();

        if (joinedTables.has(toTable)) {
          // Already joined in this query
          continue;
        }

        const fromAlias = modelToActiveAlias.get(fromTable) || fromTable;
        const toAlias = toTable;
        modelToActiveAlias.set(toTable, toAlias);
        joinedTables.add(toTable);

        lines.push(`${joinType} JOIN ${toTable} AS ${toAlias}`);
        lines.push(`  ON ${fromAlias}.${fromCol} = ${toAlias}.${toCol}`);
      }
    }

    return lines.join('\n');
  }

  /**
   * Extract a connected subgraph containing the given models and their immediate neighbors.
   */
  getLineageSubgraph(modelNames = [], options = {}) {
    const targetSet = new Set(modelNames.map(m => normalizeNodeName(m)));
    const nodes = [];
    const edges = [];

    for (const name of targetSet) {
      const node = this.nodes.get(name);
      if (node) nodes.push(node);

      const children = this.lineageAdjacency.get(name) || new Set();
      for (const child of children) {
        if (targetSet.has(child) || options.includeBoundary) {
          edges.push({ from: name, to: child, type: 'lineage' });
        }
      }
    }

    return { nodes, edges };
  }

  /**
   * Return comprehensive graph statistics summary
   */
  getGraphStats() {
    const tierCounts = {
      marts_fact: 0,
      marts_dim: 0,
      marts_report: 0,
      intermediate: 0,
      staging: 0,
      raw: 0,
      other: 0,
    };

    for (const node of this.nodes.values()) {
      if (tierCounts[node.tier] !== undefined) {
        tierCounts[node.tier]++;
      } else {
        tierCounts.other++;
      }
    }

    let lineageEdgeCount = 0;
    for (const children of this.lineageAdjacency.values()) {
      lineageEdgeCount += children.size;
    }

    const validation = this.validateDAG();

    return {
      nodeCount: this.nodes.size,
      node_count: this.nodes.size,
      lineageEdgeCount,
      lineage_edge_count: lineageEdgeCount,
      joinEdgeCount: this.relationships.length,
      join_edge_count: this.relationships.length,
      totalEdges: lineageEdgeCount + this.relationships.length,
      total_edges: lineageEdgeCount + this.relationships.length,
      hasCycles: validation.hasCycles,
      has_cycles: validation.hasCycles,
      cycleCount: validation.cycleCount,
      cycle_count: validation.cycleCount,
      tierCounts,
      tier_counts: tierCounts,
      manifestLoaded: this.manifestLoaded,
      manifest_loaded: this.manifestLoaded,
    };
  }

  /**
   * Quick accessors
   */
  getNodeCount() {
    return this.nodes.size;
  }

  getEdgeCount() {
    let lineageEdgeCount = 0;
    for (const children of this.lineageAdjacency.values()) {
      lineageEdgeCount += children.size;
    }
    return lineageEdgeCount + this.relationships.length;
  }

  getRelationships() {
    return this.relationships;
  }
}
