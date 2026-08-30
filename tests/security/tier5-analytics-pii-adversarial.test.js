import { jest } from '@jest/globals';
import {
  extractQueryAST,
  parseExplainPlan,
  generateIndexRecommendations,
  generateMaterializedViewRecommendations,
  adviseQueryIndexes,
  normalizeDialect,
} from '../../src/analytics/index-advisor.js';
import {
  calculateMean,
  calculateStdDev,
  calculateQuartiles,
  calculateMAD,
  renderSparkline,
  inferColumns,
  normalizeDataset,
  runZScoreDetection,
  runIQRDetection,
  runRollingBandDetection,
  runSeasonalDetection,
  runDeltaDetection,
  analyzeDimensionalRootCause,
  detectAnomalies,
} from '../../src/analytics/anomaly-detector.js';
import {
  maskString,
  maskValue,
  maskRow,
  maskTabularResult,
  maskObject,
  maskCSV,
  maskEmail,
  maskPhone,
  maskSSN,
  maskCard,
  maskIP,
  maskSecrets,
  isValidLuhn,
  pseudonymizeValue,
  detectSensitiveCategoryByColumnName,
  isPiiMaskingEnabled,
  REDACTION_TOKENS,
} from '../../src/utils/pii-masker.js';
import { AnalyticsHandler } from '../../src/mcp/handlers/analytics.js';

describe('Tier 5 Adversarial Coverage Hardening: Index Advisor, Anomaly Detector & PII Masker', () => {

  // =========================================================================
  // SECTION 1: INDEX ADVISOR (R3) ADVERSARIAL STRESS TESTING
  // =========================================================================
  describe('1. Index Advisor (R3) & AnalyticsHandler.handleQueryIndexAdvisor', () => {

    describe('1.1 Extreme Multi-Table Joins (5+ tables) & AST Extraction', () => {
      test('correctly parses 6-table join with mixed join types and aliases', () => {
        const complexSql = `
          SELECT 
            o.id AS order_id,
            oi.quantity,
            p.name AS product_name,
            c.category_name,
            u.email AS customer_email,
            w.warehouse_code
          FROM orders AS o
          INNER JOIN order_items oi ON o.id = oi.order_id
          LEFT JOIN products p ON oi.product_id = p.id
          LEFT JOIN categories c ON p.category_id = c.id
          JOIN users u ON o.user_id = u.id
          JOIN warehouses w ON oi.warehouse_id = w.id
          WHERE o.status = 'completed'
            AND o.created_at >= '2026-01-01'
            AND oi.unit_price > 50
            AND c.is_active = 1
            AND w.region = 'US-EAST'
          ORDER BY o.created_at DESC, oi.quantity DESC;
        `;

        const ast = extractQueryAST(complexSql);

        // Verify all 6 tables extracted
        expect(ast.tables).toContain('orders');
        expect(ast.tables).toContain('order_items');
        expect(ast.tables).toContain('products');
        expect(ast.tables).toContain('categories');
        expect(ast.tables).toContain('users');
        expect(ast.tables).toContain('warehouses');
        expect(ast.tables.length).toBe(6);

        // Verify table alias mapping
        expect(ast.tableAliasMap['o']).toBe('orders');
        expect(ast.tableAliasMap['oi']).toBe('order_items');
        expect(ast.tableAliasMap['p']).toBe('products');
        expect(ast.tableAliasMap['c']).toBe('categories');
        expect(ast.tableAliasMap['u']).toBe('users');
        expect(ast.tableAliasMap['w']).toBe('warehouses');

        // Verify join conditions extracted across tables
        expect(ast.joinConditions.length).toBeGreaterThanOrEqual(5);
        const orderItemJoin = ast.joinConditions.find(j => j.left.column === 'id' && j.right.column === 'order_id');
        expect(orderItemJoin).toBeDefined();

        // Verify recommendations generated for multiple joined tables
        const recs = generateIndexRecommendations(ast, {}, 'postgres');
        const recommendedTables = recs.map(r => r.table);
        expect(recommendedTables).toContain('orders');
        expect(recommendedTables).toContain('order_items');
      });

      test('handles comma-separated multi-table FROM syntax', () => {
        const sql = `
          SELECT u.name, o.total, d.discount_code
          FROM users u, orders o, discounts d
          WHERE u.id = o.user_id AND o.discount_id = d.id AND u.status = 'active';
        `;
        const ast = extractQueryAST(sql);
        expect(ast.tables).toContain('users');
        expect(ast.tables).toContain('orders');
        expect(ast.tables).toContain('discounts');
      });
    });

    describe('1.2 Subqueries in SELECT and WHERE & CASE WHEN Expressions', () => {
      test('parses query with subqueries in WHERE clause without crashing', () => {
        const sqlWithSubquery = `
          SELECT o.id, o.total_amount, o.user_id
          FROM orders o
          WHERE o.user_id IN (
            SELECT u.id 
            FROM users u 
            WHERE u.country = 'US' AND u.is_vip = 1
          )
          AND o.status = 'shipped'
          AND o.order_date >= '2026-03-01'
          ORDER BY o.order_date DESC;
        `;

        const ast = extractQueryAST(sqlWithSubquery);
        expect(ast.tables).toContain('orders');
        const statusFilter = ast.filterPredicates.find(f => f.column === 'status');
        expect(statusFilter).toBeDefined();
        expect(statusFilter.type).toBe('equality');

        const recs = generateIndexRecommendations(ast, {}, 'postgres');
        expect(recs.length).toBeGreaterThan(0);
        expect(recs[0].table).toBe('orders');
      });

      test('parses query with complex CASE WHEN and nested functions in SELECT', () => {
        const sqlWithCase = `
          SELECT 
            o.id,
            CASE 
              WHEN o.total > 1000 THEN 'HIGH_VALUE'
              WHEN o.total > 200 THEN 'MEDIUM_VALUE'
              ELSE 'LOW_VALUE'
            END AS value_tier,
            COALESCE(u.company_name, CONCAT(u.first_name, ' ', u.last_name)) AS customer_name,
            COUNT(oi.id) AS item_count
          FROM orders o
          JOIN users u ON o.user_id = u.id
          JOIN order_items oi ON o.id = oi.order_id
          WHERE o.status = 'completed'
            AND o.created_at BETWEEN '2026-01-01' AND '2026-12-31'
          GROUP BY o.id, value_tier, customer_name
          ORDER BY o.total DESC;
        `;

        const ast = extractQueryAST(sqlWithCase);
        expect(ast.tables).toContain('orders');
        expect(ast.tables).toContain('users');
        expect(ast.hasAggregations).toBe(true);

        const mvRecs = generateMaterializedViewRecommendations(ast, sqlWithCase, 'postgres');
        expect(mvRecs.length).toBeGreaterThan(0);
        expect(mvRecs[0].view_name).toContain('claude_ai_mv_orders_summary');
      });
    });

    describe('1.3 Composite Index Ordering: (Equality -> Join FK -> Range/IN -> Sort/Group)', () => {
      test('strictly preserves (Equality -> Range -> Sort/Group) index order regardless of query clause order', () => {
        // Query written intentionally with scrambled clause order: ORDER BY date, WHERE range then equality, GROUP BY category
        const sql = `
          SELECT category_id, status, created_at, amount
          FROM transactions
          WHERE created_at >= '2026-01-01'
            AND status = 'settled'
            AND account_id = 9999
            AND amount BETWEEN 100 AND 5000
          GROUP BY category_id
          ORDER BY created_at DESC;
        `;

        const ast = extractQueryAST(sql);
        const recs = generateIndexRecommendations(ast, {}, 'postgres');

        expect(recs.length).toBeGreaterThan(0);
        const transIdx = recs.find(r => r.table === 'transactions');
        expect(transIdx).toBeDefined();

        const cols = transIdx.columns;
        // Equality columns (status, account_id) MUST come before Range columns (created_at, amount)
        const statusIdx = cols.indexOf('status');
        const accountIdx = cols.indexOf('account_id');
        const createdAtIdx = cols.indexOf('created_at');
        const amountIdx = cols.indexOf('amount');
        const categoryIdx = cols.indexOf('category_id');

        expect(statusIdx).toBeLessThan(createdAtIdx);
        expect(accountIdx).toBeLessThan(createdAtIdx);
        expect(statusIdx).toBeLessThan(amountIdx);
        expect(accountIdx).toBeLessThan(amountIdx);

        // Sort/Group column (category_id) should be placed at the tail
        expect(createdAtIdx).toBeLessThan(categoryIdx);
      });

      test('generates covering index with INCLUDE clause in PostgreSQL for unindexed select columns', () => {
        const sql = `
          SELECT name, description, category_id
          FROM inventory
          WHERE status = 'in_stock' AND quantity > 10;
        `;
        const ast = extractQueryAST(sql);
        const recs = generateIndexRecommendations(ast, {}, 'postgres');

        expect(recs.length).toBeGreaterThan(0);
        const idx = recs[0];
        expect(idx.ddl).toContain('INCLUDE (name, description, category_id)');
      });

      test('generates partial index in PostgreSQL & SQLite for static low-cardinality status predicates', () => {
        const sql = `
          SELECT id, user_id, amount
          FROM subscriptions
          WHERE status = 'active' AND plan_id = 42;
        `;
        const ast = extractQueryAST(sql);
        const recs = generateIndexRecommendations(ast, {}, 'postgres');

        const partialIdx = recs.find(r => r.index_type === 'partial_btree');
        expect(partialIdx).toBeDefined();
        expect(partialIdx.ddl).toContain("WHERE status = 'active'");
      });
    });

    describe('1.4 Multi-Dialect DDL Generation Across All Engines', () => {
      const sampleAst = {
        tables: ['analytics_events'],
        tableAliasMap: { ae: 'analytics_events' },
        filterPredicates: [
          { table: 'analytics_events', column: 'tenant_id', type: 'equality', value: '123' },
          { table: 'analytics_events', column: 'event_time', type: 'range', operator: '>=' },
        ],
        joinConditions: [],
        groupByColumns: [{ table: 'analytics_events', column: 'event_name' }],
        orderByColumns: [{ table: 'analytics_events', column: 'event_time', direction: 'DESC' }],
        aggregations: ['COUNT(*)'],
        selectColumns: ['tenant_id', 'event_name', 'event_time'],
        hasAggregations: true,
      };

      const baseSql = `SELECT tenant_id, event_name, COUNT(*) FROM analytics_events WHERE tenant_id = '123' AND event_time >= '2026-01-01' GROUP BY tenant_id, event_name;`;

      test('PostgreSQL DDL: generates CONCURRENT B-tree index and Materialized View with CONCURRENT refresh', () => {
        const idxRecs = generateIndexRecommendations(sampleAst, {}, 'postgres');
        expect(idxRecs[0].ddl).toMatch(/CREATE INDEX CONCURRENTLY IF NOT EXISTS/i);
        expect(idxRecs[0].ddl).toContain('ON analytics_events (tenant_id, event_time, event_name)');

        const mvRecs = generateMaterializedViewRecommendations(sampleAst, baseSql, 'postgres');
        expect(mvRecs[0].ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS');
        expect(mvRecs[0].refresh_strategy).toContain('REFRESH MATERIALIZED VIEW CONCURRENTLY');
      });

      test('MySQL DDL: generates standard CREATE INDEX and CREATE VIEW fallback', () => {
        const idxRecs = generateIndexRecommendations(sampleAst, {}, 'mysql');
        expect(idxRecs[0].ddl).toMatch(/^CREATE INDEX claude_ai_idx_analytics_events/i);

        const mvRecs = generateMaterializedViewRecommendations(sampleAst, baseSql, 'mysql');
        expect(mvRecs[0].ddl).toContain('CREATE VIEW IF NOT EXISTS');
        expect(mvRecs[0].refresh_strategy).toContain('Standard View created');
      });

      test('SQLite DDL: generates CREATE INDEX IF NOT EXISTS and CREATE VIEW fallback', () => {
        const idxRecs = generateIndexRecommendations(sampleAst, {}, 'sqlite');
        expect(idxRecs[0].ddl).toMatch(/^CREATE INDEX IF NOT EXISTS/i);

        const mvRecs = generateMaterializedViewRecommendations(sampleAst, baseSql, 'sqlite');
        expect(mvRecs[0].ddl).toContain('CREATE VIEW IF NOT EXISTS');
      });

      test('BigQuery DDL: generates table CLUSTER BY recommendation and BigQuery Materialized View', () => {
        const idxRecs = generateIndexRecommendations(sampleAst, {}, 'bigquery');
        expect(idxRecs[0].index_type).toBe('clustering');
        expect(idxRecs[0].ddl).toContain('CLUSTER BY tenant_id, event_time, event_name');

        const mvRecs = generateMaterializedViewRecommendations(sampleAst, baseSql, 'bigquery');
        expect(mvRecs[0].ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS `claude_ai_mv_analytics_events_summary`');
        expect(mvRecs[0].ddl).toContain('OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)');
      });

      test('Snowflake DDL: generates ALTER TABLE CLUSTER BY and Snowflake Materialized View', () => {
        const idxRecs = generateIndexRecommendations(sampleAst, {}, 'snowflake');
        expect(idxRecs[0].index_type).toBe('clustering');
        expect(idxRecs[0].ddl).toBe('ALTER TABLE analytics_events CLUSTER BY (tenant_id, event_time, event_name);');

        const mvRecs = generateMaterializedViewRecommendations(sampleAst, baseSql, 'snowflake');
        expect(mvRecs[0].ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS claude_ai_mv_analytics_events_summary');
        expect(mvRecs[0].refresh_strategy).toContain('Snowflake serverless background maintenance');
      });

      test('Redshift DDL: generates ALTER TABLE ALTER SORTKEY', () => {
        const idxRecs = generateIndexRecommendations(sampleAst, {}, 'redshift');
        expect(idxRecs[0].ddl).toBe('ALTER TABLE analytics_events ALTER SORTKEY (tenant_id, event_time, event_name);');
      });

      test('ClickHouse DDL: generates ALTER TABLE ADD INDEX minmax and SummingMergeTree Materialized View', () => {
        const idxRecs = generateIndexRecommendations(sampleAst, {}, 'clickhouse');
        expect(idxRecs[0].ddl).toContain('ALTER TABLE analytics_events ADD INDEX');
        expect(idxRecs[0].ddl).toContain('TYPE minmax GRANULARITY 1;');

        const mvRecs = generateMaterializedViewRecommendations(sampleAst, baseSql, 'clickhouse');
        expect(mvRecs[0].ddl).toContain('ENGINE = SummingMergeTree()');
        expect(mvRecs[0].ddl).toContain('ORDER BY (event_name)');
      });

      test('SQL Server DDL: generates CREATE NONCLUSTERED INDEX with optional INCLUDE', () => {
        const idxRecs = generateIndexRecommendations(sampleAst, {}, 'sqlserver');
        expect(idxRecs[0].ddl).toMatch(/^CREATE NONCLUSTERED INDEX claude_ai_idx_analytics_events/i);
      });

      test('Dialect normalization falls back to postgres on unrecognized dialect strings', () => {
        expect(normalizeDialect('unknown_engine_v1')).toBe('postgres');
        expect(normalizeDialect(undefined)).toBe('postgres');
      });
    });

    describe('1.5 EXPLAIN Plan Parsing & Heuristic Scan Fallback', () => {
      test('parses Postgres Seq Scan and Sort Method disk spill', () => {
        const explainOutput = `
          Bitmap Heap Scan on large_orders (cost=100.00..5000.00 rows=1000)
            -> Seq Scan on large_orders (cost=0.00..4500.00 rows=50000)
          Sort Method: external merge Disk: 4096kB
          Execution Time: 142.50 ms
        `;

        const parsed = parseExplainPlan(explainOutput, 'postgres');
        expect(parsed.scans_detected.length).toBe(1);
        expect(parsed.scans_detected[0].scan_type).toBe('Seq Scan');
        expect(parsed.scans_detected[0].table).toBe('large_orders');
        expect(parsed.bottlenecks).toContain('Bitmap Heap Scan indicates non-covering index; candidate for composite or covering index.');
        expect(parsed.execution_time_ms).toBe(142.5);
      });

      test('parses MySQL type: ALL table scan and filesort', () => {
        const mysqlExplainText = `
          id: 1
          select_type: SIMPLE
          table: inventory
          type: ALL
          Extra: Using filesort; Using temporary
        `;

        const parsed = parseExplainPlan(mysqlExplainText, 'mysql');
        expect(parsed.scans_detected.length).toBe(1);
        expect(parsed.scans_detected[0].table).toBe('inventory');
        expect(parsed.bottlenecks.some(b => b.includes('filesort'))).toBe(true);
        expect(parsed.bottlenecks.some(b => b.includes('temporary'))).toBe(true);
      });

      test('parses SQLite SCAN TABLE and TEMP B-TREE', () => {
        const sqliteExplain = `
          0 0 0 SCAN TABLE audit_logs
          0 0 0 USE TEMP B-TREE FOR ORDER BY
        `;
        const parsed = parseExplainPlan(sqliteExplain, 'sqlite');
        expect(parsed.scans_detected.length).toBe(1);
        expect(parsed.scans_detected[0].table).toBe('audit_logs');
        expect(parsed.bottlenecks.some(b => b.includes('TEMP B-TREE'))).toBe(true);
      });

      test('returns heuristic fallback mode when explain plan is null or empty', () => {
        const parsed = parseExplainPlan(null, 'postgres');
        expect(parsed.explain_mode).toBe('heuristic_fallback');
        expect(parsed.scans_detected).toEqual([]);
        expect(parsed.bottlenecks[0]).toContain('deterministic AST analysis');
      });
    });

    describe('1.6 AnalyticsHandler.handleQueryIndexAdvisor Integration', () => {
      test('executes handler with raw SQL, dialect detection, and structured response', async () => {
        const mockMetabaseClient = {
          getDatabase: jest.fn().mockResolvedValue({ id: 5, engine: 'postgres' }),
          executeNativeQuery: jest.fn().mockResolvedValue(`Seq Scan on metrics (cost=0.00..1200.00)\nExecution Time: 45.2 ms`),
        };

        const handler = new AnalyticsHandler(mockMetabaseClient);
        const response = await handler.handleQueryIndexAdvisor({
          database_id: 5,
          sql: `SELECT id, name FROM metrics WHERE status = 'failed' AND created_at >= '2026-01-01' ORDER BY created_at DESC;`,
          run_explain: true,
        });

        expect(response.content).toBeDefined();
        expect(response.content[0].type).toBe('text');
        expect(response.content[0].text).toContain('AI Query Index & Materialized View Advisory');
        expect(response.content[0].text).toContain('`postgres`');
        expect(response.content[0].text).toContain('metrics');
        expect(response.content[0].text).toContain('CREATE INDEX CONCURRENTLY IF NOT EXISTS');

        const structured = response.structuredContent;
        expect(structured).toBeDefined();
        expect(structured.dialect).toBe('postgres');
        expect(structured.index_recommendations.length).toBeGreaterThan(0);
        expect(structured._provenance.tool).toBe('ai_query_index_advisor');
      });

      test('resolves SQL from question/card definition if card_id provided', async () => {
        const mockMetabaseClient = {
          getQuestion: jest.fn().mockResolvedValue({
            id: 42,
            database_id: 2,
            dataset_query: {
              native: {
                query: "SELECT customer_id, count(*) FROM orders WHERE order_status = 'pending' GROUP BY customer_id;",
              }
            }
          }),
          getDatabase: jest.fn().mockResolvedValue({ id: 2, engine: 'mysql' }),
          executeNativeQuery: jest.fn().mockResolvedValue('EXPLAIN result'),
        };

        const handler = new AnalyticsHandler(mockMetabaseClient);
        const response = await handler.handleQueryIndexAdvisor({
          card_id: 42,
          database_id: 2,
        });

        expect(mockMetabaseClient.getQuestion).toHaveBeenCalledWith(42);
        expect(response.structuredContent).toBeDefined();
        expect(response.structuredContent.dialect).toBe('mysql');
      });

      test('gracefully handles missing SQL and invalid card_id', async () => {
        const handler = new AnalyticsHandler({});
        const response = await handler.handleQueryIndexAdvisor({});
        expect(response.isError).toBe(true);
        expect(response.content[0].text).toContain('Query index advisory failed');
      });
    });
  });

  // =========================================================================
  // SECTION 2: ANOMALY DETECTOR (R4) ADVERSARIAL STRESS TESTING
  // =========================================================================
  describe('2. Anomaly Detector (R4) & AnalyticsHandler.handleDetectAnomalies', () => {

    describe('2.1 Edge Case Datasets: Constant Series & Zero Variance', () => {
      test('handles all-zero and all-constant series without NaN or division by zero', () => {
        const zeros = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        const constants = [42, 42, 42, 42, 42, 42, 42, 42];

        expect(calculateMean(zeros)).toBe(0);
        expect(calculateStdDev(zeros)).toBe(0);
        expect(calculateMAD(zeros)).toBe(0);
        expect(renderSparkline(zeros)).toBe('▄'.repeat(10));

        expect(calculateMean(constants)).toBe(42);
        expect(calculateStdDev(constants)).toBe(0);
        expect(calculateMAD(constants)).toBe(0);
        expect(renderSparkline(constants)).toBe('▄'.repeat(8));

        const result = detectAnomalies({ data: constants });
        expect(result.total_points_analyzed).toBe(8);
        expect(result.anomalies_detected_count).toBe(0);
        expect(isNaN(result.baseline_summary.mean)).toBe(false);
        expect(isNaN(result.baseline_summary.std_dev)).toBe(false);
      });

      test('handles empty or null datasets gracefully', () => {
        expect(calculateMean([])).toBe(0);
        expect(calculateStdDev([])).toBe(0);
        expect(calculateQuartiles([])).toEqual({ median: 0, q1: 0, q3: 0, iqr: 0 });
        expect(calculateMAD([])).toBe(0);
        expect(renderSparkline([])).toBe('');

        const emptyRes = detectAnomalies({ data: [] });
        expect(emptyRes.total_points_analyzed).toBe(0);
        expect(emptyRes.anomalies_detected_count).toBe(0);
        expect(emptyRes.baseline_summary.trend).toBe('no_data');
      });
    });

    describe('2.2 Edge Case Datasets: Negative Values & High Volatility Noise', () => {
      test('handles negative values in time series correctly', () => {
        const negativeSeries = [
          { timestamp: '2026-01-01', value: -100 },
          { timestamp: '2026-01-02', value: -95 },
          { timestamp: '2026-01-03', value: -105 },
          { timestamp: '2026-01-04', value: -98 },
          { timestamp: '2026-01-05', value: -102 },
          { timestamp: '2026-01-06', value: -600 }, // Severe negative drop
          { timestamp: '2026-01-07', value: 150 },  // Positive spike
          { timestamp: '2026-01-08', value: -100 },
          { timestamp: '2026-01-09', value: -97 },
        ];

        const result = detectAnomalies({ data: negativeSeries });
        expect(result.anomalies_detected_count).toBeGreaterThanOrEqual(1);

        const dropAnomaly = result.anomalies.find(a => a.timestamp === '2026-01-06');
        expect(dropAnomaly).toBeDefined();
        expect(dropAnomaly.actual_value).toBe(-600);
        expect(dropAnomaly.type).toBe('drop');
      });

      test('detects transient 1-point spike in noisy series', () => {
        const noisySeries = [];
        for (let i = 1; i <= 30; i++) {
          const dateStr = `2026-05-${String(i).padStart(2, '0')}`;
          // Base 1000 + minor noise
          const noise = (i % 3 === 0 ? 30 : -20);
          const val = (i === 15) ? 10000 : 1000 + noise; // 10x spike on day 15
          noisySeries.push({ timestamp: dateStr, value: val });
        }

        const result = detectAnomalies({ data: noisySeries, sensitivity: 'medium' });
        const spike = result.anomalies.find(a => a.timestamp === '2026-05-15');
        expect(spike).toBeDefined();
        expect(spike.type).toBe('spike');
        expect(spike.severity).toBe('CRITICAL');
        expect(spike.percentage_deviation).toBeGreaterThanOrEqual(500);
      });

      test('detects sudden step drop to zero (Zero Drop) as CRITICAL severity', () => {
        const series = [
          { timestamp: '2026-06-01', value: 500 },
          { timestamp: '2026-06-02', value: 520 },
          { timestamp: '2026-06-03', value: 510 },
          { timestamp: '2026-06-04', value: 490 },
          { timestamp: '2026-06-05', value: 0 }, // Complete outage / zero drop
          { timestamp: '2026-06-06', value: 505 },
        ];

        const result = detectAnomalies({ data: series });
        const zeroAnomaly = result.anomalies.find(a => a.timestamp === '2026-06-05');
        expect(zeroAnomaly).toBeDefined();
        expect(zeroAnomaly.severity).toBe('CRITICAL');
        expect(zeroAnomaly.type).toBe('drop');
      });
    });

    describe('2.3 Multi-Algorithm Detection, Sensitivity & Direction Filtering', () => {
      const testSeries = [
        100, 102, 98, 101, 99, 100, 97, 103,
        500, // Spike at index 8
        100, 99, 101,
        10,  // Drop at index 12
        100, 102, 98, 100
      ];

      test('direction=spike filters out drop anomalies', () => {
        const spikeOnly = detectAnomalies({ data: testSeries, direction: 'spike' });
        expect(spikeOnly.anomalies.every(a => a.type === 'spike')).toBe(true);
        expect(spikeOnly.anomalies.some(a => a.actual_value === 500)).toBe(true);
        expect(spikeOnly.anomalies.some(a => a.actual_value === 10)).toBe(false);
      });

      test('direction=drop filters out spike anomalies', () => {
        const dropOnly = detectAnomalies({ data: testSeries, direction: 'drop' });
        expect(dropOnly.anomalies.every(a => a.type === 'drop')).toBe(true);
        expect(dropOnly.anomalies.some(a => a.actual_value === 10)).toBe(true);
        expect(dropOnly.anomalies.some(a => a.actual_value === 500)).toBe(false);
      });

      test('sensitivity=high vs sensitivity=low modulates detection strictness', () => {
        const slightOutlierSeries = [
          100, 101, 99, 100, 102, 100, 99, 101, 100,
          135, // Mild outlier
          100, 101, 99, 100,
        ];

        const highSens = detectAnomalies({ data: slightOutlierSeries, sensitivity: 'high' });
        const lowSens = detectAnomalies({ data: slightOutlierSeries, sensitivity: 'low' });

        expect(highSens.anomalies_detected_count).toBeGreaterThanOrEqual(lowSens.anomalies_detected_count);
      });

      test('method=seasonal_decomposition calculates cyclical residuals', () => {
        // 21 days with weekly cycle
        const cyclicSeries = [];
        for (let i = 0; i < 21; i++) {
          const dayOfWeek = i % 7;
          const cycleBase = (dayOfWeek === 5 || dayOfWeek === 6) ? 300 : 100; // weekend bump
          const val = (i === 10) ? 900 : cycleBase; // spike on day 10 (Wednesday)
          cyclicSeries.push(val);
        }

        const seasonRes = runSeasonalDetection(cyclicSeries, 'medium');
        expect(seasonRes[10].isAnomaly).toBe(true);
      });

      test('caps returned anomalies to maxAnomalies parameter', () => {
        const manyAnomalies = Array(50).fill(100).map((v, i) => (i % 2 === 0 ? 1000 : 100));
        const capped = detectAnomalies({ data: manyAnomalies, maxAnomalies: 5 });
        expect(capped.anomalies.length).toBeLessThanOrEqual(5);
      });
    });

    describe('2.4 High-Cardinality Multi-Segment Dimensional Drilldown', () => {
      test('identifies top contributing dimension in multi-segment anomaly', () => {
        // Multi-segment dataset with 4 regions across 3 timestamps
        const multiSegmentData = [
          // Baseline timestamp T1
          { timestamp: '2026-07-01', region: 'North', revenue: 100 },
          { timestamp: '2026-07-01', region: 'South', revenue: 100 },
          { timestamp: '2026-07-01', region: 'East', revenue: 100 },
          { timestamp: '2026-07-01', region: 'West', revenue: 100 },
          // Baseline timestamp T2
          { timestamp: '2026-07-02', region: 'North', revenue: 105 },
          { timestamp: '2026-07-02', region: 'South', revenue: 95 },
          { timestamp: '2026-07-02', region: 'East', revenue: 102 },
          { timestamp: '2026-07-02', region: 'West', revenue: 98 },
          // Anomaly timestamp T3 (Total 1400 vs baseline ~400, driven almost entirely by 'North' surging to 1000)
          { timestamp: '2026-07-03', region: 'North', revenue: 1100 }, // +1000 spike
          { timestamp: '2026-07-03', region: 'South', revenue: 100 },
          { timestamp: '2026-07-03', region: 'East', revenue: 100 },
          { timestamp: '2026-07-03', region: 'West', revenue: 100 },
        ];

        const rootCause = analyzeDimensionalRootCause(
          multiSegmentData,
          'timestamp',
          'revenue',
          'region',
          '2026-07-03'
        );

        expect(rootCause).toBeDefined();
        expect(rootCause.dimension).toBe('region');
        expect(rootCause.top_contributor).toBe('North');
        expect(rootCause.contribution_pct).toBeGreaterThanOrEqual(80);
        expect(rootCause.breakdown.length).toBe(4);
      });

      test('auto-infers obscure time and metric column names', () => {
        const records = [
          { dt_bucket: '2026-01-01', metric_total_amount: 150, customer_tier: 'Gold' },
          { dt_bucket: '2026-01-02', metric_total_amount: 160, customer_tier: 'Silver' },
        ];

        const inferred = inferColumns(records);
        expect(inferred.timeColumn).toBe('dt_bucket');
        expect(inferred.metricColumn).toBe('metric_total_amount');
        expect(inferred.dimensionColumn).toBe('customer_tier');
      });

      test('normalizes Metabase dataset format { data: { cols, rows } }', () => {
        const mbDataset = {
          data: {
            cols: [{ name: 'day' }, { name: 'sales_amt' }],
            rows: [
              ['2026-08-01', 500],
              ['2026-08-02', 550],
              ['2026-08-03', 9500], // spike
            ],
          },
        };

        const normalized = normalizeDataset(mbDataset);
        expect(normalized.length).toBe(3);
        expect(normalized[0]).toEqual({ day: '2026-08-01', sales_amt: 500 });
      });
    });

    describe('2.5 Sparkline Rendering & Severity Tiers', () => {
      test('renders correct Unicode sparklines across diverse distributions', () => {
        expect(renderSparkline([])).toBe('');
        expect(renderSparkline([10, 20, 30, 40, 50, 60, 70, 80])).toBe(' ▂▃▄▅▆▇█');
        expect(renderSparkline([100, 100, 100])).toBe('▄▄▄');
        expect(renderSparkline([10, 80, 10, 80])).toBe(' █ █');
      });
    });

    describe('2.6 AnalyticsHandler.handleDetectAnomalies Integration', () => {
      test('executes handler with raw dataset and returns formatted structured output', async () => {
        const handler = new AnalyticsHandler({});
        const rawData = [
          { date: '2026-08-01', sales: 100 },
          { date: '2026-08-02', sales: 105 },
          { date: '2026-08-03', sales: 95 },
          { date: '2026-08-04', sales: 100 },
          { date: '2026-08-05', sales: 850 }, // Anomaly
          { date: '2026-08-06', sales: 102 },
        ];

        const response = await handler.handleDetectAnomalies({
          data: rawData,
          time_column: 'date',
          metric_column: 'sales',
        });

        expect(response.content).toBeDefined();
        expect(response.content[0].text).toContain('Proactive KPI Anomaly & Outlier Detection Report');
        expect(response.content[0].text).toContain('`sales`');
        expect(response.content[0].text).toContain('Trend Sparkline:');
        expect(response.content[0].text).toContain('2026-08-05');

        const structured = response.structuredContent;
        expect(structured.anomalies_detected_count).toBeGreaterThanOrEqual(1);
        expect(structured.sparkline).toBeDefined();
        expect(structured._provenance.tool).toBe('ai_analytics_detect_anomalies');
      });

      test('resolves dataset via mocked Metabase Client executeNativeQuery', async () => {
        const mockMetabaseClient = {
          executeNativeQuery: jest.fn().mockResolvedValue([
            { created_at: '2026-08-01', value: 200 },
            { created_at: '2026-08-02', value: 210 },
            { created_at: '2026-08-03', value: 205 },
            { created_at: '2026-08-04', value: 2000 }, // 10x spike
            { created_at: '2026-08-05', value: 195 },
          ]),
        };

        const handler = new AnalyticsHandler(mockMetabaseClient);
        const response = await handler.handleDetectAnomalies({
          database_id: 1,
          sql: 'SELECT created_at, value FROM daily_orders;',
        });

        expect(mockMetabaseClient.executeNativeQuery).toHaveBeenCalledWith(1, 'SELECT created_at, value FROM daily_orders;');
        expect(response.structuredContent.anomalies_detected_count).toBeGreaterThanOrEqual(1);
      });

      test('resolves dataset from table_name with aggregation query generation', async () => {
        const mockMetabaseClient = {
          executeNativeQuery: jest.fn().mockResolvedValue([
            { timestamp: '2026-08-01', value: 100 },
            { timestamp: '2026-08-02', value: 105 },
            { timestamp: '2026-08-03', value: 95 },
            { timestamp: '2026-08-04', value: 100 },
            { timestamp: '2026-08-05', value: 3500 }, // massive spike
            { timestamp: '2026-08-06', value: 102 },
          ]),
        };

        const handler = new AnalyticsHandler(mockMetabaseClient);
        const response = await handler.handleDetectAnomalies({
          database_id: 1,
          table_name: 'orders',
        });

        expect(mockMetabaseClient.executeNativeQuery).toHaveBeenCalledWith(
          1,
          'SELECT created_at AS timestamp, COUNT(*) AS value FROM orders GROUP BY 1 ORDER BY 1 LIMIT 500;'
        );
        expect(response.structuredContent.anomalies_detected_count).toBeGreaterThanOrEqual(1);
      });

      test('returns structured error when no dataset can be resolved', async () => {
        const handler = new AnalyticsHandler({});
        const response = await handler.handleDetectAnomalies({});
        expect(response.isError).toBe(true);
        expect(response.content[0].text).toContain('No valid dataset could be retrieved');
      });
    });
  });

  // =========================================================================
  // SECTION 3: PII MASKER (R5) ADVERSARIAL STRESS TESTING
  // =========================================================================
  describe('3. PII Masker (R5) Adversarial Stress & Deep Traversal', () => {

    describe('3.1 Mixed Unstructured Text & Freeform Logs', () => {
      test('sanitizes dense unstructured log string with interleaved secrets, tokens, IPs, emails, cards, and SSNs', () => {
        const complexLog = `
          [2026-08-31 01:23:45.678] ERROR [AuthService] Login failure for user clark.kent@dailyplanet.com 
          (IP: 192.168.1.150, IPv6: 2001:0db8:85a3:0000:0000:8a2e:0370:7334)
          Phone: +1-555-890-1234, SSN: 123-45-6789, Card: 4532-0151-1283-0366
          Provided Key: sk-proj-abcdef1234567890abcdef1234567890
          DB URI: postgres://dbadmin:SuperSecretPass123!@db.internal.net:5432/prod
          Auth Header: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.doNotLeakThisSignature
          Hash: $2a$12$e8Mc8jPqR7Q1s7/5rXn2QOWrF2O4v6YV3Z6qX0r7P4v8Z9Q0W1E2.
        `;

        const sanitized = maskString(complexLog);

        // Assert zero unmasked sensitive fragments
        expect(sanitized).not.toContain('clark.kent@dailyplanet.com');
        expect(sanitized).toContain('c***t@dailyplanet.com');

        expect(sanitized).not.toContain('192.168.1.150');
        expect(sanitized).toContain('192.168.*.*');

        expect(sanitized).not.toContain('2001:0db8:85a3:0000:0000:8a2e:0370:7334');
        expect(sanitized).toContain('2001:0db8:*:*:*:*:*:*');

        expect(sanitized).not.toContain('555-890-1234');
        expect(sanitized).toContain('***-***-1234');

        expect(sanitized).not.toContain('123-45-6789');
        expect(sanitized).toContain('***-**-6789');

        expect(sanitized).not.toContain('4532-0151-1283-0366');
        expect(sanitized).toContain('****-****-****-0366');

        expect(sanitized).not.toContain('sk-proj-abcdef1234567890abcdef1234567890');
        expect(sanitized).toContain(REDACTION_TOKENS.SECRET);

        expect(sanitized).not.toContain('SuperSecretPass123!');
        expect(sanitized).toContain(REDACTION_TOKENS.PASSWORD);

        expect(sanitized).not.toContain('doNotLeakThisSignature');
        expect(sanitized).toContain(REDACTION_TOKENS.SECRET);

        expect(sanitized).not.toContain('$2a$12$e8Mc8jPqR7Q1s7/5rXn2QOWrF2O4v6YV3Z6qX0r7P4v8Z9Q0W1E2.');
        expect(sanitized).toContain(REDACTION_TOKENS.PASSWORD);
      });

      test('sanitizes Anthropic, OpenAI, GitHub, Slack, and Stripe API keys', () => {
        const ghToken = ['ghp', '1234567890abcdefghijklmnopqrstuvwxyz12'].join('_');
        const slackToken = ['xoxb', '1234567890-123456789012-abcdef123456'].join('-');
        const stripeKey = ['sk', 'live', '51Abcd12345678901234567890'].join('_');
        const keys = `
          OPENAI: sk-proj-12345678901234567890123456789012
          ANTHROPIC: sk-ant-123456789012345678901234567890
          GITHUB: ${ghToken}
          SLACK: ${slackToken}
          STRIPE: ${stripeKey}
          AWS: AKIAIOSFODNN7EXAMPLE
        `;

        const masked = maskSecrets(keys);
        expect(masked).not.toContain('sk-proj-12345678901234567890123456789012');
        expect(masked).not.toContain(ghToken);
        expect(masked).not.toContain(['xoxb', '1234567890'].join('-'));
        expect(masked).not.toContain('AKIAIOSFODNN7EXAMPLE');
      });
    });

    describe('3.2 International Phone Numbers & Diverse Formats', () => {
      test('masks international phone numbers across various country formats and URL encodings', () => {
        const internationalPhones = [
          '+44 20 7946 0991',     // UK
          '+33 1 42 68 55 55',     // France
          '+49 30 123456',         // Germany
          '+90 532 123 4567',      // Turkey
          '+81 3 1234 5678',       // Japan
          '+1 (555) 987-6543',     // US with parens
          '%2B1-555-890-1234',     // URL encoded +1
          '+15551234567',          // Compact E.164
        ];

        for (const phone of internationalPhones) {
          const masked = maskPhone(phone);
          // Masked output should contain asterisks and preserve last 4 digits
          expect(masked).toContain('*');
          expect(masked).not.toBe(phone);
        }
      });
    });

    describe('3.3 Credit Card Luhn Verification: Valid vs Invalid Numbers', () => {
      test('masks valid Luhn numbers in unstructured text while preserving non-card digit sequences', () => {
        const validVisa = '4532 0151 1283 0366'; // Valid Luhn Visa
        const validMastercard = '5425-2334-3010-9903'; // Valid Luhn Mastercard
        const invalidCardContinuous = '4532015112830367'; // Invalid Luhn 16-digit sequence
        const orderIdNumber = '9876543210987654'; // 16-digit order id that fails Luhn

        expect(isValidLuhn(validVisa)).toBe(true);
        expect(isValidLuhn(validMastercard)).toBe(true);
        expect(isValidLuhn(invalidCardContinuous)).toBe(false);
        expect(isValidLuhn(orderIdNumber)).toBe(false);

        // Unstructured string masking:
        const textWithValid = `Customer paid with ${validVisa}.`;
        const textWithInvalid = `Transaction ref: ${invalidCardContinuous}.`;

        expect(maskString(textWithValid)).toBe('Customer paid with **** **** **** 0366.');
        // Invalid card sequence in general text should not be masked as a card
        expect(maskString(textWithInvalid)).toBe(`Transaction ref: ${invalidCardContinuous}.`);

        // But in column-aware mode where column is explicitly a card column, it masks
        expect(maskValue(invalidCardContinuous, 'credit_card')).toBe('************0367');
      });
    });

    describe('3.4 Deep Circular Reference Traversal & Deep Objects', () => {
      test('traverses deeply nested objects (>20 levels) without stack overflow', () => {
        let deepObj = { email: 'deep.user@example.com', level: 25 };
        for (let i = 24; i >= 1; i--) {
          deepObj = { level: i, nested: deepObj };
        }

        const masked = maskObject(deepObj);
        let curr = masked;
        while (curr.nested) {
          curr = curr.nested;
        }
        expect(curr.email).toBe('d***r@example.com');
      });

      test('neutralizes circular references without infinite recursion (RangeError)', () => {
        const circularObj = {
          name: 'Parent',
          email: 'admin@company.org',
          phone: '+1-555-123-4567',
        };
        circularObj.self = circularObj;
        circularObj.child = {
          name: 'Child',
          parent: circularObj,
          apiKey: 'sk-proj-123456789012345678901234',
        };

        const result = maskObject(circularObj);
        expect(result.email).toBe('a***n@company.org');
        expect(result.self).toBe('[CIRCULAR]');
        expect(result.child.parent).toBe('[CIRCULAR]');
        expect(result.child.apiKey).toBe(REDACTION_TOKENS.SECRET);
      });

      test('neutralizes mutually circular arrays and objects', () => {
        const objA = { id: 1, secret: ['xoxb', '1234567890-123456789012-abcdef123456'].join('-') };
        const objB = { id: 2, email: 'user.b@test.com' };
        objA.refB = objB;
        objB.refA = objA;

        const masked = maskTabularResult([objA, objB]);
        expect(masked[0].secret).toBe(REDACTION_TOKENS.SECRET);
        expect(masked[0].refB.refA.refB).toBe('[CIRCULAR]');
        expect(masked[1].email).toBe('u***b@test.com');
      });
    });

    describe('3.5 Deterministic Pseudonymization with HMAC Salts', () => {
      test('produces consistent deterministic anon_ hashes for analytical grouping', () => {
        const email = 'constant.user@example.com';
        const saltA = 'salt_production_2026';
        const saltB = 'salt_staging_2026';

        const hash1 = pseudonymizeValue(email, saltA);
        const hash2 = pseudonymizeValue(email, saltA);
        const hashOtherSalt = pseudonymizeValue(email, saltB);

        expect(hash1).toMatch(/^anon_[a-f0-9]{12}$/);
        expect(hash1).toBe(hash2); // Deterministic
        expect(hash1).not.toBe(hashOtherSalt); // Salt isolated
      });
    });

    describe('3.6 CSV Sanitization & Column Heuristic Detection', () => {
      test('sanitizes RFC 4180 CSV with multiline quotes and commas', () => {
        const csv = `id,name,email,comment,card\n1,"Doe, John",john.doe@example.com,"Multi-line\ncomment with phone: 555-123-4567",4532-0151-1283-0366\n`;
        const sanitizedCSV = maskCSV(csv);

        expect(sanitizedCSV).toContain('j***e@example.com');
        expect(sanitizedCSV).toContain('***-***-4567');
        expect(sanitizedCSV).toContain('****-****-****-0366');
        expect(sanitizedCSV).toContain('"Doe, John"');
      });

      test('detects sensitive category by column name heuristics correctly', () => {
        expect(detectSensitiveCategoryByColumnName('user_email')).toBe('email');
        expect(detectSensitiveCategoryByColumnName('phone_number')).toBe('phone');
        expect(detectSensitiveCategoryByColumnName('social_security_number')).toBe('ssn');
        expect(detectSensitiveCategoryByColumnName('credit_card_num')).toBe('card');
        expect(detectSensitiveCategoryByColumnName('password_hash')).toBe('password');
        expect(detectSensitiveCategoryByColumnName('api_secret_token')).toBe('secret');
        expect(detectSensitiveCategoryByColumnName('client_ip_address')).toBe('ip');
        expect(detectSensitiveCategoryByColumnName('product_title')).toBeNull();
      });

      test('isPiiMaskingEnabled responds to environment variables and option flags', () => {
        expect(isPiiMaskingEnabled({})).toBe(true);
        expect(isPiiMaskingEnabled({ maskPii: false })).toBe(false);
        expect(isPiiMaskingEnabled({ mask_pii: false })).toBe(false);
        expect(isPiiMaskingEnabled({ sanitize: false })).toBe(false);
      });
    });
  });
});
