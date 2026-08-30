import { jest } from '@jest/globals';
import {
  extractQueryAST,
  parseExplainPlan,
  generateIndexRecommendations,
  generateMaterializedViewRecommendations,
  adviseQueryIndexes,
  normalizeDialect,
} from '../../src/analytics/index-advisor.js';

describe('AI Query Index & Materialized View Advisor Unit Tests (R3)', () => {
  describe('Dialect Normalization', () => {
    test('normalizes standard dialect strings', () => {
      expect(normalizeDialect('postgres')).toBe('postgres');
      expect(normalizeDialect('PostgreSQL 14')).toBe('postgres');
      expect(normalizeDialect('mysql')).toBe('mysql');
      expect(normalizeDialect('mariadb')).toBe('mysql');
      expect(normalizeDialect('sqlite')).toBe('sqlite');
      expect(normalizeDialect('h2')).toBe('sqlite');
      expect(normalizeDialect('bigquery')).toBe('bigquery');
      expect(normalizeDialect('snowflake')).toBe('snowflake');
      expect(normalizeDialect('redshift')).toBe('redshift');
      expect(normalizeDialect('clickhouse')).toBe('clickhouse');
      expect(normalizeDialect('sqlserver')).toBe('sqlserver');
      expect(normalizeDialect(null)).toBe('postgres');
    });
  });

  describe('extractQueryAST: SQL Lexical & Clause Analysis', () => {
    test('extracts single table with equality and range filters', () => {
      const sql = `
        SELECT id, name, email, created_at
        FROM users
        WHERE status = 'active' AND created_at >= '2026-01-01'
        ORDER BY created_at DESC;
      `;
      const ast = extractQueryAST(sql);

      expect(ast.tables).toEqual(['users']);
      expect(ast.filterPredicates.length).toBe(2);

      const statusFilter = ast.filterPredicates.find(f => f.column === 'status');
      expect(statusFilter).toBeDefined();
      expect(statusFilter.type).toBe('equality');
      expect(statusFilter.operator).toBe('=');
      expect(statusFilter.value).toBe('active');

      const dateFilter = ast.filterPredicates.find(f => f.column === 'created_at');
      expect(dateFilter).toBeDefined();
      expect(dateFilter.type).toBe('range');
      expect(dateFilter.operator).toBe('>=');

      expect(ast.orderByColumns).toEqual([
        { raw: 'created_at DESC', table: 'users', column: 'created_at', direction: 'DESC' },
      ]);
    });

    test('extracts multi-table joins, aliases, and join foreign key conditions', () => {
      const sql = `
        SELECT o.id, o.total_amount, c.name, c.email
        FROM orders AS o
        INNER JOIN customers c ON o.customer_id = c.id
        LEFT JOIN payments p ON p.order_id = o.id
        WHERE o.status = 'completed' AND o.created_at BETWEEN '2026-01-01' AND '2026-06-30'
        ORDER BY o.total_amount DESC;
      `;
      const ast = extractQueryAST(sql);

      expect(ast.tables).toContain('orders');
      expect(ast.tables).toContain('customers');
      expect(ast.tables).toContain('payments');
      expect(ast.tableAliasMap['o']).toBe('orders');
      expect(ast.tableAliasMap['c']).toBe('customers');
      expect(ast.tableAliasMap['p']).toBe('payments');

      expect(ast.joinConditions.length).toBeGreaterThanOrEqual(2);
      const custJoin = ast.joinConditions.find(j => j.left.column === 'customer_id' || j.right.column === 'customer_id');
      expect(custJoin).toBeDefined();

      const betweenFilter = ast.filterPredicates.find(f => f.operator === 'BETWEEN');
      expect(betweenFilter).toBeDefined();
      expect(betweenFilter.column).toBe('created_at');
      expect(betweenFilter.type).toBe('range');
    });

    test('extracts IN clauses, IS NULL, LIKE prefix vs wildcard', () => {
      const sql = `
        SELECT *
        FROM products
        WHERE category_id IN (1, 2, 3)
          AND deleted_at IS NULL
          AND sku LIKE 'ELEC%'
          AND description LIKE '%portable%';
      `;
      const ast = extractQueryAST(sql);

      const inFilter = ast.filterPredicates.find(f => f.column === 'category_id');
      expect(inFilter.type).toBe('in');
      expect(inFilter.values).toEqual(['1', '2', '3']);

      const nullFilter = ast.filterPredicates.find(f => f.column === 'deleted_at');
      expect(nullFilter.type).toBe('equality');
      expect(nullFilter.operator).toBe('IS NULL');

      const prefixLike = ast.filterPredicates.find(f => f.column === 'sku');
      expect(prefixLike.type).toBe('range');
      expect(prefixLike.isPrefix).toBe(true);

      const wildcardLike = ast.filterPredicates.find(f => f.column === 'description');
      expect(wildcardLike.type).toBe('like_wildcard');
      expect(wildcardLike.isPrefix).toBe(false);
    });

    test('extracts aggregations and GROUP BY dimensions', () => {
      const sql = `
        SELECT country, DATE_TRUNC('month', created_at) AS order_month, COUNT(id) AS total_orders, SUM(amount) AS total_revenue
        FROM orders
        WHERE status = 'paid'
        GROUP BY country, 2
        ORDER BY total_revenue DESC;
      `;
      const ast = extractQueryAST(sql);

      expect(ast.hasAggregations).toBe(true);
      expect(ast.aggregations.length).toBeGreaterThanOrEqual(2);
      expect(ast.aggregations.some(a => a.startsWith('COUNT'))).toBe(true);
      expect(ast.aggregations.some(a => a.startsWith('SUM'))).toBe(true);
      expect(ast.groupByColumns.length).toBeGreaterThanOrEqual(1);
      expect(ast.groupByColumns[0].column).toBe('country');
    });
  });

  describe('generateIndexRecommendations: (Equality -> Range -> Sort/Group) Rule', () => {
    test('orders composite index columns strictly: Equality -> Range -> Sort/Group', () => {
      const sql = `
        SELECT id, amount, created_at, status, region
        FROM orders
        WHERE region = 'EMEA'
          AND status = 'completed'
          AND created_at >= '2026-01-01'
        ORDER BY amount DESC;
      `;
      const ast = extractQueryAST(sql);
      const recs = generateIndexRecommendations(ast, {}, 'postgres');

      expect(recs.length).toBeGreaterThanOrEqual(1);
      const primaryIndex = recs.find(r => r.table === 'orders');
      expect(primaryIndex).toBeDefined();

      // Equality columns: region, status
      // Range columns: created_at
      // Sort columns: amount
      const cols = primaryIndex.columns;
      const regionIdx = cols.indexOf('region');
      const statusIdx = cols.indexOf('status');
      const dateIdx = cols.indexOf('created_at');
      const amountIdx = cols.indexOf('amount');

      expect(regionIdx).toBeLessThan(dateIdx);
      expect(statusIdx).toBeLessThan(dateIdx);
      expect(dateIdx).toBeLessThan(amountIdx);
    });

    test('generates PostgreSQL DDL with CONCURRENTLY and covering INCLUDE columns', () => {
      const sql = `
        SELECT id, tracking_number
        FROM shipments
        WHERE status = 'in_transit'
        ORDER BY shipped_at ASC;
      `;
      const ast = extractQueryAST(sql);
      const recs = generateIndexRecommendations(ast, {}, 'postgres');

      const rec = recs.find(r => r.table === 'shipments');
      expect(rec).toBeDefined();
      expect(rec.ddl).toContain('CREATE INDEX CONCURRENTLY IF NOT EXISTS claude_ai_idx_shipments_');
      expect(rec.ddl).toContain('ON shipments');
    });

    test('generates MySQL DDL syntax', () => {
      const sql = `SELECT * FROM users WHERE email = 'test@example.com' AND age > 21;`;
      const ast = extractQueryAST(sql);
      const recs = generateIndexRecommendations(ast, {}, 'mysql');

      const rec = recs[0];
      expect(rec.ddl).toMatch(/^CREATE INDEX claude_ai_idx_users_/);
      expect(rec.ddl).toContain('ON users (email, age);');
    });

    test('generates SQLite DDL syntax', () => {
      const sql = `SELECT * FROM items WHERE type = 'book' ORDER BY price ASC;`;
      const ast = extractQueryAST(sql);
      const recs = generateIndexRecommendations(ast, {}, 'sqlite');

      const rec = recs[0];
      expect(rec.ddl).toContain('CREATE INDEX IF NOT EXISTS claude_ai_idx_items_');
      expect(rec.ddl).toContain('ON items (type, price);');
    });

    test('generates Snowflake clustering recommendation DDL', () => {
      const sql = `SELECT * FROM events WHERE tenant_id = 't1' AND event_time >= '2026-01-01';`;
      const ast = extractQueryAST(sql);
      const recs = generateIndexRecommendations(ast, {}, 'snowflake');

      const rec = recs[0];
      expect(rec.index_type).toBe('clustering');
      expect(rec.ddl).toBe('ALTER TABLE events CLUSTER BY (tenant_id, event_time);');
    });

    test('generates Partial Index for static status predicates in PostgreSQL', () => {
      const sql = `
        SELECT id, user_id, amount
        FROM orders
        WHERE status = 'pending'
          AND created_at > '2026-01-01';
      `;
      const ast = extractQueryAST(sql);
      const recs = generateIndexRecommendations(ast, {}, 'postgres');

      const partialRec = recs.find(r => r.index_type === 'partial_btree');
      expect(partialRec).toBeDefined();
      expect(partialRec.ddl).toContain("WHERE status = 'pending'");
    });
  });

  describe('generateMaterializedViewRecommendations', () => {
    test('recommends Materialized View for heavy aggregations with GROUP BY and joins', () => {
      const sql = `
        SELECT u.country, DATE_TRUNC('month', o.created_at) AS m, COUNT(o.id) AS count, SUM(o.total) AS total
        FROM orders o
        JOIN users u ON o.user_id = u.id
        WHERE o.status = 'completed'
        GROUP BY u.country, 2;
      `;
      const ast = extractQueryAST(sql);
      const recs = generateMaterializedViewRecommendations(ast, sql, 'postgres');

      expect(recs.length).toBe(1);
      const mv = recs[0];
      expect(mv.view_name).toBe('claude_ai_mv_orders_summary');
      expect(mv.priority).toBe('HIGH');
      expect(mv.ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS claude_ai_mv_orders_summary AS');
      expect(mv.ddl).toContain('CREATE UNIQUE INDEX IF NOT EXISTS');
      expect(mv.refresh_strategy).toContain('REFRESH MATERIALIZED VIEW CONCURRENTLY');
    });

    test('generates BigQuery Materialized View DDL with refresh options', () => {
      const sql = `
        SELECT store_id, SUM(sales) AS total_sales
        FROM transactions
        GROUP BY store_id;
      `;
      const ast = extractQueryAST(sql);
      const recs = generateMaterializedViewRecommendations(ast, sql, 'bigquery');

      expect(recs.length).toBe(1);
      const mv = recs[0];
      expect(mv.ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS `claude_ai_mv_transactions_summary`');
      expect(mv.ddl).toContain('OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)');
    });
  });

  describe('parseExplainPlan: Scan and Bottleneck Parsing', () => {
    test('parses PostgreSQL Seq Scan and Sort disk spill', () => {
      const explainText = `
        Seq Scan on orders  (cost=0.00..18450.00 rows=50000 width=120)
          Filter: (status = 'active'::text)
        Sort Method: external merge Disk: 15240kB
        Execution Time: 450.25 ms
      `;
      const parsed = parseExplainPlan(explainText, 'postgres');

      expect(parsed.explain_mode).toBe('explain_plan_parsed');
      expect(parsed.scans_detected.length).toBe(1);
      expect(parsed.scans_detected[0].table).toBe('orders');
      expect(parsed.scans_detected[0].scan_type).toBe('Seq Scan');
      expect(parsed.execution_time_ms).toBe(450.25);
      expect(parsed.bottlenecks.some(b => b.includes('external merge'))).toBe(true);
    });

    test('parses MySQL type ALL table scan and Using filesort', () => {
      const mysqlExplain = JSON.stringify({
        query_block: {
          table: {
            table_name: 'customers',
            access_type: 'ALL',
            rows_examined_per_scan: 100000,
            using_filesort: true,
          }
        }
      });
      const parsed = parseExplainPlan(mysqlExplain, 'mysql');

      expect(parsed.scans_detected.length).toBe(1);
      expect(parsed.scans_detected[0].table).toBe('customers');
      expect(parsed.scans_detected[0].scan_type).toBe('ALL (Table Scan)');
    });

    test('parses SQLite SCAN TABLE', () => {
      const sqliteExplain = [
        { id: 2, parent: 0, notused: 0, detail: 'SCAN TABLE products' },
        { id: 3, parent: 0, notused: 0, detail: 'USE TEMP B-TREE FOR ORDER BY' },
      ];
      const parsed = parseExplainPlan(sqliteExplain, 'sqlite');

      expect(parsed.scans_detected.length).toBe(1);
      expect(parsed.scans_detected[0].table).toBe('products');
      expect(parsed.scans_detected[0].scan_type).toBe('SCAN TABLE');
    });

    test('falls back gracefully to deterministic heuristics when explain output is null', () => {
      const parsed = parseExplainPlan(null, 'postgres');
      expect(parsed.explain_mode).toBe('heuristic_fallback');
      expect(parsed.scans_detected).toEqual([]);
    });
  });

  describe('adviseQueryIndexes Workflow & Provenance', () => {
    test('produces comprehensive advisory result with _provenance envelope', async () => {
      const sql = `
        SELECT u.id, u.email, o.total_amount
        FROM users u
        JOIN orders o ON o.user_id = u.id
        WHERE u.status = 'active' AND o.created_at >= '2026-01-01'
        ORDER BY o.total_amount DESC;
      `;

      const result = await adviseQueryIndexes({
        databaseId: 1,
        sql,
        dialect: 'postgres',
        runExplain: false,
      });

      expect(result.sql).toBe(sql);
      expect(result.dialect).toBe('postgres');
      expect(result.query_analysis).toBeDefined();
      expect(result.query_analysis.tables).toContain('users');
      expect(result.query_analysis.tables).toContain('orders');
      expect(result.index_recommendations.length).toBeGreaterThan(0);
      expect(result.estimated_impact).toBeDefined();

      expect(result._provenance).toEqual({
        ai_generated: true,
        tool: 'ai_query_index_advisor',
        review_required: true,
        timestamp: expect.any(String),
        dialect: 'postgres',
      });
    });

    test('throws error if SQL is empty or invalid', async () => {
      await expect(adviseQueryIndexes({ sql: '' })).rejects.toThrow('Valid SQL query or card_id');
      await expect(adviseQueryIndexes({ sql: null })).rejects.toThrow('Valid SQL query or card_id');
    });
  });
});
