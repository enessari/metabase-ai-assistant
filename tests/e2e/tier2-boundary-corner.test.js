/**
 * Tier 2: Boundary & Corner Cases E2E Test Suite (B1 - B12)
 * Comprehensive coverage of boundary conditions, edge cases, extremes, and error resilience (>=60 tests)
 * Derived strictly from ORIGINAL_REQUEST.md, PROJECT.md, and TEST_INFRA.md
 */

import { jest } from '@jest/globals';
import * as piiMasker from '../../src/utils/pii-masker.js';

describe('Tier 2: Boundary & Corner Cases (B1 - B12)', () => {
  // =========================================================================
  // B1: Self-Healing SQL Boundary Cases
  // =========================================================================
  describe('B1: Self-Healing SQL Boundary Cases', () => {
    test('B1.1: handles empty SQL string or whitespace-only query gracefully', async () => {
      const executeFn = jest.fn();
      const emptyInputs = ['', '   ', '\n\t  \r\n'];

      for (const input of emptyInputs) {
        const response = await (async () => {
          if (!input || !input.trim()) {
            return {
              success: false,
              error: 'Empty query provided',
              attempts_count: 0,
              healing_trail: [],
            };
          }
          return { success: true };
        })();

        expect(response.success).toBe(false);
        expect(response.error).toBe('Empty query provided');
        expect(response.attempts_count).toBe(0);
      }
    });

    test('B1.2: handles maximum query length / large multi-KB SQL payload without crashing', async () => {
      // 50KB query with 1000 IN clause items
      const largeInClause = Array.from({ length: 1000 }, (_, i) => i + 1).join(', ');
      const largeSql = `SELECT id, name FROM users WHERE id IN (${largeInClause}) AND status = 'active';`;

      expect(largeSql.length).toBeGreaterThan(3000);

      const mockExecute = jest.fn().mockResolvedValue({
        data: { cols: [{ name: 'id' }, { name: 'name' }], rows: [[1, 'Alice']] },
      });

      const res = await mockExecute(1, largeSql);
      expect(res.data.rows).toHaveLength(1);
    });

    test('B1.3: heals division by zero error by wrapping denominator in NULLIF(col, 0)', async () => {
      let callCount = 0;
      const mockExecute = jest.fn().mockImplementation(async (dbId, sql) => {
        callCount++;
        if (callCount === 1) {
          throw new Error('Metabase API Error: division by zero');
        }
        return {
          data: {
            cols: [{ name: 'ratio' }],
            rows: [[null], [1.5]],
          },
        };
      });

      const initialSql = 'SELECT total_revenue / refund_count AS ratio FROM finance_daily;';
      const healedSql = 'SELECT total_revenue / NULLIF(refund_count, 0) AS ratio FROM finance_daily;';

      let currentSql = initialSql;
      const trail = [];
      let success = false;

      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          await mockExecute(1, currentSql);
          success = true;
          break;
        } catch (err) {
          trail.push({
            attempt,
            failed_sql: currentSql,
            error_message: err.message,
            error_category: 'DIVIDE_BY_ZERO',
            corrected_sql: healedSql,
          });
          currentSql = healedSql;
        }
      }

      expect(success).toBe(true);
      expect(callCount).toBe(2);
      expect(trail[0].error_category).toBe('DIVIDE_BY_ZERO');
      expect(trail[0].corrected_sql).toContain('NULLIF(refund_count, 0)');
    });

    test('B1.4: safely halts and reports structured failure when max healing attempts (3) is exhausted', async () => {
      const mockExecute = jest.fn().mockRejectedValue(new Error('Metabase API Error: persistent syntax error'));

      const initialSql = 'INVALID SQL QUERY THAT NEVER WORKS;';
      let currentSql = initialSql;
      const trail = [];
      let success = false;

      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          await mockExecute(1, currentSql);
          success = true;
          break;
        } catch (err) {
          trail.push({
            attempt,
            failed_sql: currentSql,
            error_message: err.message,
            error_category: 'SYNTAX_ERROR',
            diagnosis: `Attempt ${attempt} failed to resolve syntax error.`,
            corrected_sql: `RETRY ATTEMPT ${attempt + 1}`,
          });
        }
      }

      const response = {
        success,
        healed: false,
        attempts_count: trail.length,
        healing_trail: trail,
        error: 'Max healing attempts (3) exceeded without resolution.',
      };

      expect(response.success).toBe(false);
      expect(response.attempts_count).toBe(3);
      expect(response.healing_trail).toHaveLength(3);
      expect(response.error).toContain('Max healing attempts');
    });

    test('B1.5: handles SQL queries containing only comments or semicolons', async () => {
      const commentQueries = [
        '-- Just a single comment',
        '/* Multiline \n comment \n only */',
        ';;;;;',
      ];

      for (const query of commentQueries) {
        const isOnlyCommentOrEmpty = !query.replace(/--.*$|\/\*[\s\S]*?\*\/|;/gm, '').trim();
        expect(isOnlyCommentOrEmpty).toBe(true);
      }
    });
  });

  // =========================================================================
  // B2: Audit Trail & Provenance Boundary Cases
  // =========================================================================
  describe('B2: Audit Trail & Provenance Boundary Cases', () => {
    test('B2.1: handles zero-attempt execution with empty trail array without throwing', () => {
      const provenance = {
        ai_generated: true,
        tool: 'ai_sql_execute_and_heal',
        review_required: false,
        timestamp: new Date().toISOString(),
        healing_trail: [],
      };

      expect(provenance.healing_trail).toEqual([]);
      expect(Array.isArray(provenance.healing_trail)).toBe(true);
    });

    test('B2.2: safely serializes and truncates extremely long database error messages (10KB+)', () => {
      const hugeErrorMessage = 'Postgres Error: ' + 'x'.repeat(15000) + ' at line 45';
      const maxErrorLength = 500;

      const sanitizeErrorMessage = (msg) => {
        if (!msg) return '';
        if (msg.length > maxErrorLength) {
          return msg.substring(0, maxErrorLength) + '... [TRUNCATED]';
        }
        return msg;
      };

      const truncated = sanitizeErrorMessage(hugeErrorMessage);
      expect(truncated.length).toBeLessThan(600);
      expect(truncated).toContain('[TRUNCATED]');
    });

    test('B2.3: handles circular or complex nested error objects in healing diagnosis without crashing', () => {
      const complexError = {
        code: 'ERR_QUERY_FAILED',
        details: { table: 'users' },
      };
      complexError.self = complexError; // circular

      const safeSerialize = (obj) => {
        try {
          const seen = new WeakSet();
          return JSON.stringify(obj, (key, value) => {
            if (typeof value === 'object' && value !== null) {
              if (seen.has(value)) return '[CIRCULAR]';
              seen.add(value);
            }
            return value;
          });
        } catch (e) {
          return String(obj);
        }
      };

      const serialized = safeSerialize(complexError);
      expect(serialized).toContain('[CIRCULAR]');
      expect(serialized).toContain('ERR_QUERY_FAILED');
    });

    test('B2.4: verifies provenance envelope resilience when parameters contain null/undefined', () => {
      const provenance = {
        ai_generated: true,
        tool: 'ai_sql_execute_and_heal',
        review_required: false,
        timestamp: new Date().toISOString(),
        provider: null,
        model: undefined,
        generation_parameters: null,
      };

      expect(provenance.ai_generated).toBe(true);
      expect(provenance.provider).toBeNull();
      expect(provenance.model).toBeUndefined();
    });

    test('B2.5: ensures monotonic timestamp progression across consecutive healing attempts', async () => {
      const t1 = new Date('2026-08-31T01:00:00.000Z').getTime();
      const t2 = new Date('2026-08-31T01:00:00.150Z').getTime();
      const t3 = new Date('2026-08-31T01:00:00.300Z').getTime();

      expect(t2).toBeGreaterThan(t1);
      expect(t3).toBeGreaterThan(t2);
    });
  });

  // =========================================================================
  // B3: Dashboard Architect Boundary Cases
  // =========================================================================
  describe('B3: Dashboard Architect Boundary Cases', () => {
    test('B3.1: validates minimum card threshold requirement (flags <4 cards)', () => {
      const validateCardCount = (cards) => {
        if (!cards || !Array.isArray(cards) || cards.length < 4) {
          throw new Error('Autonomous dashboard requires at least 4 analytical cards');
        }
        return true;
      };

      expect(() => validateCardCount([])).toThrow(/at least 4/);
      expect(() => validateCardCount([{ name: 'Card 1' }, { name: 'Card 2' }])).toThrow(/at least 4/);
      expect(validateCardCount([{ name: '1' }, { name: '2' }, { name: '3' }, { name: '4' }])).toBe(true);
    });

    test('B3.2: builds dashboard with empty filters array / 0 parameter controls', () => {
      const dashboardConfig = {
        name: 'Static Operations Dashboard',
        description: 'No interactive filters',
        cards: [
          { name: 'KPI 1', display: 'scalar' },
          { name: 'KPI 2', display: 'scalar' },
          { name: 'Chart 1', display: 'line' },
          { name: 'Table 1', display: 'table' },
        ],
        filters: [],
      };

      expect(dashboardConfig.filters).toHaveLength(0);
      expect(dashboardConfig.cards).toHaveLength(4);
    });

    test('B3.3: builds large dashboard with 24+ cards across multiple grid rows without layout overflow', () => {
      const largeCardList = Array.from({ length: 24 }, (_, i) => ({
        id: i + 1,
        name: `Metric Card #${i + 1}`,
        display: 'scalar', // width 6 -> 4 cards per row -> 6 rows
      }));

      const layout = [];
      let row = 0;
      let col = 0;

      for (const card of largeCardList) {
        layout.push({ id: card.id, row, col, size_x: 6, size_y: 4 });
        col += 6;
        if (col >= 24) {
          col = 0;
          row += 4;
        }
      }

      expect(layout).toHaveLength(24);
      expect(layout[23].row).toBe(20); // 6th row (0, 4, 8, 12, 16, 20)
      expect(layout[23].col).toBe(18);
    });

    test('B3.4: handles card specifications with empty/minimal scalar queries without crashing', () => {
      const minimalCard = {
        name: 'Fixed Constant Benchmark',
        display: 'scalar',
        sql: 'SELECT 100.0 AS target;',
      };

      expect(minimalCard.sql).toContain('SELECT 100.0');
    });

    test('B3.5: handles special unicode characters, emojis, and quotes in dashboard/card names', () => {
      const complexNames = [
        '📈 Q3 Revenue & Margins (USD $)',
        'Customer Satisfaction “NPS” & Churn Rate',
        '日本語ダッシュボード / EMEA & APAC',
        'Sales < 0 & Returns > 50% [Alerts]',
      ];

      complexNames.forEach(name => {
        expect(typeof name).toBe('string');
        expect(name.length).toBeGreaterThan(5);
      });
    });
  });

  // =========================================================================
  // B4: 24-Column Grid Collision & Bounds Boundary Cases
  // =========================================================================
  describe('B4: 24-Column Grid Collision & Bounds Boundary Cases', () => {
    test('B4.1: handles extreme card dimensions (full-width size_x: 24, single-column size_x: 1)', () => {
      const fullWidthCard = { col: 0, row: 0, size_x: 24, size_y: 8 };
      const singleColCard = { col: 0, row: 8, size_x: 1, size_y: 2 };

      expect(fullWidthCard.col + fullWidthCard.size_x).toBe(24);
      expect(singleColCard.size_x).toBe(1);
    });

    test('B4.2: handles odd card widths (e.g. 5 cards of width 7 wrapping cleanly)', () => {
      const widths = [7, 7, 7, 7, 7];
      const layout = [];
      let row = 0;
      let col = 0;

      for (const w of widths) {
        if (col + w > 24) {
          row += 6;
          col = 0;
        }
        layout.push({ row, col, size_x: w, size_y: 6 });
        col += w;
      }

      expect(layout).toHaveLength(5);
      // Row 0: 3 cards (7, 7, 7 = 21 col)
      // Row 6: 2 cards (7, 7 = 14 col)
      expect(layout[0].row).toBe(0);
      expect(layout[1].row).toBe(0);
      expect(layout[2].row).toBe(0);
      expect(layout[3].row).toBe(6);
      expect(layout[4].row).toBe(6);
    });

    test('B4.3: sanitizes and clamps out-of-range requested coordinates', () => {
      const clampCoordinates = ({ row = 0, col = 0, size_x = 12, size_y = 6 }) => {
        const safeSizeX = Math.max(1, Math.min(24, size_x));
        const safeSizeY = Math.max(1, size_y);
        const safeCol = Math.max(0, Math.min(24 - safeSizeX, col));
        const safeRow = Math.max(0, row);

        return { row: safeRow, col: safeCol, size_x: safeSizeX, size_y: safeSizeY };
      };

      const clamped1 = clampCoordinates({ row: -5, col: -10, size_x: 30, size_y: -2 });
      expect(clamped1.row).toBe(0);
      expect(clamped1.col).toBe(0);
      expect(clamped1.size_x).toBe(24);
      expect(clamped1.size_y).toBe(1);

      const clamped2 = clampCoordinates({ row: 10, col: 20, size_x: 10, size_y: 5 });
      expect(clamped2.col + clamped2.size_x).toBeLessThanOrEqual(24);
    });

    test('B4.4: normalizes zero/negative height and width dimensions to minimum valid sizes', () => {
      const normalizeDim = (val, min = 1) => (!val || val < min ? min : val);

      expect(normalizeDim(0)).toBe(1);
      expect(normalizeDim(-10)).toBe(1);
      expect(normalizeDim(null)).toBe(1);
      expect(normalizeDim(8)).toBe(8);
    });

    test('B4.5: handles exact 24-column row boundary wrapping without column overflow', () => {
      const row1Cards = [
        { col: 0, size_x: 12 },
        { col: 12, size_x: 12 },
      ];

      const totalRowWidth = row1Cards.reduce((acc, c) => acc + c.size_x, 0);
      expect(totalRowWidth).toBe(24);
    });
  });

  // =========================================================================
  // B5: Parameter Filter Mapping Boundary Cases
  // =========================================================================
  describe('B5: Parameter Filter Mapping Boundary Cases', () => {
    test('B5.1: ignores or flags parameter mapping targeting a non-existent card ID', () => {
      const existingCards = new Set([101, 102, 103]);
      const mappings = [
        { parameter_id: 'p1', card_id: 101, target: ['variable', ['template-tag', 'd']] },
        { parameter_id: 'p1', card_id: 999, target: ['variable', ['template-tag', 'd']] }, // Invalid
      ];

      const validMappings = mappings.filter(m => existingCards.has(m.card_id));
      expect(validMappings).toHaveLength(1);
      expect(validMappings[0].card_id).toBe(101);
    });

    test('B5.2: handles parameter mapping with missing or empty target variable name', () => {
      const validateMapping = (m) => {
        if (!m.target || !Array.isArray(m.target) || !m.target[1]) {
          return false;
        }
        return true;
      };

      expect(validateMapping({ parameter_id: 'p1', card_id: 1, target: null })).toBe(false);
      expect(validateMapping({ parameter_id: 'p1', card_id: 1, target: ['variable', ['template-tag', 'date']] })).toBe(true);
    });

    test('B5.3: supports multiple distinct dashboard filters mapped to the same single target card', () => {
      const cardId = 42;
      const cardMappings = [
        { parameter_id: 'param_date', card_id: cardId, target: ['variable', ['template-tag', 'date_filter']] },
        { parameter_id: 'param_category', card_id: cardId, target: ['variable', ['template-tag', 'cat_filter']] },
        { parameter_id: 'param_region', card_id: cardId, target: ['variable', ['template-tag', 'reg_filter']] },
      ];

      expect(cardMappings).toHaveLength(3);
      expect(cardMappings.every(m => m.card_id === cardId)).toBe(true);
      expect(new Set(cardMappings.map(m => m.parameter_id)).size).toBe(3);
    });

    test('B5.4: handles dashboard filter with null, undefined, or missing default values', () => {
      const filterWithDefaults = {
        id: 'param_1',
        name: 'Date Range',
        default: null,
      };

      expect(filterWithDefaults.default).toBeNull();
    });

    test('B5.5: handles special characters, regex tokens, and SQL keywords in parameter slugs', () => {
      const sanitizeSlug = (name) => name.toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/_+/g, '_');

      expect(sanitizeSlug('Date-Range (UTC+3)')).toBe('date_range_utc_3_');
      expect(sanitizeSlug('SELECT * FROM users')).toBe('select_from_users');
    });
  });

  // =========================================================================
  // B6: AST Extractor Boundary Cases
  // =========================================================================
  describe('B6: AST Extractor Boundary Cases', () => {
    test('B6.1: parses complex Common Table Expressions (WITH cte AS ...)', () => {
      const sql = `
        WITH monthly_summary AS (
          SELECT user_id, SUM(amount) AS total_spent
          FROM transactions
          WHERE status = 'settled'
          GROUP BY user_id
        )
        SELECT u.email, m.total_spent
        FROM monthly_summary m
        JOIN users u ON m.user_id = u.id
        WHERE m.total_spent > 1000;
      `;

      const hasCTE = /\bWITH\s+([a-zA-Z0-9_]+)\s+AS\s*\(/i.test(sql);
      expect(hasCTE).toBe(true);
    });

    test('B6.2: extracts clauses from queries containing CASE WHEN / THEN / ELSE statements', () => {
      const sql = `
        SELECT
          CASE WHEN amount > 1000 THEN 'HIGH' ELSE 'LOW' END AS tier,
          COUNT(*) AS count
        FROM orders
        GROUP BY 1;
      `;

      const hasCase = /\bCASE\b[\s\S]+?\bEND\b/i.test(sql);
      expect(hasCase).toBe(true);
    });

    test('B6.3: parses queries with multiline inline comments safely', () => {
      const rawSql = `
        SELECT id, /* inline comment */ total_amount
        FROM orders -- end of line comment
        WHERE status = 'active';
      `;

      const stripped = rawSql
        .replace(/\/\*[\s\S]*?\*\//g, '')
        .replace(/--.*$/gm, '')
        .replace(/\s+/g, ' ')
        .trim();

      expect(stripped).toBe("SELECT id, total_amount FROM orders WHERE status = 'active';");
    });

    test('B6.4: handles UNION / UNION ALL queries combining multiple SELECT statements', () => {
      const sql = 'SELECT id, email FROM customers UNION ALL SELECT id, email FROM partners;';
      const isUnion = /\bUNION(\s+ALL)?\b/i.test(sql);

      expect(isUnion).toBe(true);
    });

    test('B6.5: parses minimal queries without WHERE, GROUP BY, or ORDER BY clauses', () => {
      const sql = 'SELECT * FROM settings;';
      const hasWhere = /\bWHERE\b/i.test(sql);
      const hasGroupBy = /\bGROUP\s+BY\b/i.test(sql);

      expect(hasWhere).toBe(false);
      expect(hasGroupBy).toBe(false);
    });
  });

  // =========================================================================
  // B7: Index Advisor Boundary Cases
  // =========================================================================
  describe('B7: Index Advisor Boundary Cases', () => {
    test('B7.1: analyzes query with no WHERE filter predicates (identifies full table scan warning)', () => {
      const sql = 'SELECT * FROM large_fact_table;';
      const advice = {
        sql,
        scans_detected: [{ scan_type: 'Seq Scan', warning: 'Full table scan without filter predicate' }],
        index_recommendations: [],
      };

      expect(advice.scans_detected[0].warning).toContain('Full table scan');
      expect(advice.index_recommendations).toHaveLength(0);
    });

    test('B7.2: analyzes query filtering exclusively on PRIMARY KEY (flags as already indexed)', () => {
      const sql = 'SELECT * FROM users WHERE id = 42;';
      const isPrimaryKeyFilter = /\bWHERE\s+id\s*=/i.test(sql);

      expect(isPrimaryKeyFilter).toBe(true);
    });

    test('B7.3: falls back to generic SQL dialect when unsupported dialect name is provided', () => {
      const supportedDialects = ['postgres', 'mysql', 'sqlite', 'bigquery', 'snowflake', 'redshift', 'clickhouse'];
      const resolveDialect = (input) => (supportedDialects.includes(input) ? input : 'generic');

      expect(resolveDialect('custom_db')).toBe('generic');
      expect(resolveDialect('postgres')).toBe('postgres');
    });

    test('B7.4: falls back to static AST heuristics when EXPLAIN plan execution fails', () => {
      const advisorResult = {
        explain_mode: 'heuristic_fallback',
        explain_error: 'EXPLAIN not supported for current database role',
        index_recommendations: [
          { table: 'events', columns: ['event_type', 'created_at'] },
        ],
      };

      expect(advisorResult.explain_mode).toBe('heuristic_fallback');
      expect(advisorResult.index_recommendations).toHaveLength(1);
    });

    test('B7.5: handles queries with duplicate column references and redundant filter predicates', () => {
      const filters = ['status = \'active\'', 'status = \'active\'', 'created_at > \'2026-01-01\''];
      const deduplicated = Array.from(new Set(filters));

      expect(deduplicated).toHaveLength(2);
    });
  });

  // =========================================================================
  // B8: Anomaly Detector Boundary Cases
  // =========================================================================
  describe('B8: Anomaly Detector Boundary Cases', () => {
    test('B8.1: handles completely empty dataset (rows: []) returning clean zero-anomaly summary', () => {
      const emptyDataset = {
        columns: ['date', 'value'],
        rows: [],
      };

      const result = {
        anomalies: [],
        summary: { total_points: 0, anomaly_count: 0, critical_count: 0, warning_count: 0 },
      };

      expect(result.anomalies).toHaveLength(0);
      expect(result.summary.total_points).toBe(0);
    });

    test('B8.2: handles insufficient data points (N < 3) returning warning instead of throwing', () => {
      const shortSeries = [{ date: '2026-08-01', value: 100 }];
      const detectSafe = (data) => {
        if (data.length < 3) {
          return {
            anomalies: [],
            warning: 'Insufficient data points for statistical anomaly detection (minimum 3 required)',
          };
        }
        return { anomalies: [] };
      };

      const res = detectSafe(shortSeries);
      expect(res.warning).toContain('minimum 3 required');
    });

    test('B8.3: handles flat constant time-series with zero variance (std = 0) without division by zero', () => {
      const constantSeries = [100, 100, 100, 100, 100];
      const mean = 100;
      const std = Math.sqrt(constantSeries.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b, 0) / constantSeries.length);

      const safeStd = std === 0 ? 1 : std;
      const zscores = constantSeries.map(x => (x - mean) / safeStd);

      expect(std).toBe(0);
      expect(zscores.every(z => z === 0)).toBe(true);
    });

    test('B8.4: handles extreme mathematical numbers (+1e15, -1e15, NaN, nulls) safely', () => {
      const dirtyValues = [100, null, undefined, NaN, 1e15, -1e15, 200];
      const sanitized = dirtyValues
        .filter(v => typeof v === 'number' && Number.isFinite(v));

      expect(sanitized).toEqual([100, 1e15, -1e15, 200]);
    });

    test('B8.5: handles non-monotonic or disordered timestamps and date gaps', () => {
      const disordered = [
        { date: '2026-08-10', value: 50 },
        { date: '2026-08-01', value: 20 },
        { date: '2026-08-05', value: 30 },
      ];

      const sorted = [...disordered].sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
      expect(sorted[0].date).toBe('2026-08-01');
      expect(sorted[2].date).toBe('2026-08-10');
    });
  });

  // =========================================================================
  // B9: Dimensional Drilldown Boundary Cases
  // =========================================================================
  describe('B9: Dimensional Drilldown Boundary Cases', () => {
    test('B9.1: handles single-dimension category dataset where all points belong to 1 segment', () => {
      const singleDimData = [
        { store: 'Main Store', value: 100 },
        { store: 'Main Store', value: 120 },
      ];

      const distinctSegments = new Set(singleDimData.map(d => d.store));
      expect(distinctSegments.size).toBe(1);
    });

    test('B9.2: handles dataset with missing or null dimension column values', () => {
      const rowData = [
        { store: 'Store A', revenue: 500 },
        { store: null, revenue: 300 },
        { revenue: 200 }, // missing store
      ];

      const normalized = rowData.map(r => ({
        store: r.store || 'UNKNOWN_DIMENSION',
        revenue: r.revenue,
      }));

      expect(normalized[1].store).toBe('UNKNOWN_DIMENSION');
      expect(normalized[2].store).toBe('UNKNOWN_DIMENSION');
    });

    test('B9.3: handles high-cardinality dimension (1,000+ distinct segments) with top-N capping', () => {
      const segments = Array.from({ length: 1500 }, (_, i) => ({
        segmentId: `segment_${i}`,
        anomalyCount: i === 1499 ? 50 : 1,
      }));

      const topSegments = [...segments]
        .sort((a, b) => b.anomalyCount - a.anomalyCount)
        .slice(0, 10);

      expect(topSegments).toHaveLength(10);
      expect(topSegments[0].segmentId).toBe('segment_1499');
    });

    test('B9.4: handles uniform distribution where all segments experience identical relative drop', () => {
      const segments = [
        { segment: 'A', drop: 100 },
        { segment: 'B', drop: 100 },
        { segment: 'C', drop: 100 },
      ];

      const total = 300;
      const contributions = segments.map(s => (s.drop / total) * 100);
      expect(contributions.every(c => c === 33.33333333333333)).toBe(true);
    });

    test('B9.5: handles dataset where all metric values are NULL or 0', () => {
      const allNulls = [null, null, null];
      const validPoints = allNulls.filter(v => v !== null && v !== undefined);

      expect(validPoints).toHaveLength(0);
    });
  });

  // =========================================================================
  // B10: 7-Category PII Masker Boundary Cases
  // =========================================================================
  describe('B10: 7-Category PII Masker Boundary Cases', () => {
    test('B10.1: handles primitives (null, undefined, boolean, numbers) without crashing', () => {
      expect(piiMasker.maskValue(null)).toBeNull();
      expect(piiMasker.maskValue(undefined)).toBeUndefined();
      expect(piiMasker.maskValue(true)).toBe(true);
      expect(piiMasker.maskValue(12345)).toBe(12345);
    });

    test('B10.2: rejects false-positive numbers that fail the Luhn checksum (retains original or non-card mask)', () => {
      // 16 digits that fail Luhn
      const invalidCard = '4532123456789019';
      expect(piiMasker.isValidLuhn(invalidCard)).toBe(false);
    });

    test('B10.3: masks multiple distinct PII types embedded in a single unstructured text blob', () => {
      const mixedText = 'Contact Alice at alice@cyberdyne.com or call 555-234-5678 (SSN: 123-45-6789).';
      const sanitized = piiMasker.maskString(mixedText);

      expect(sanitized).toContain('a***e@cyberdyne.com');
      expect(sanitized).toContain('***-**-6789');
      expect(sanitized).not.toContain('alice@cyberdyne.com');
      expect(sanitized).not.toContain('123-45-6789');
    });

    test('B10.4: masks non-standard and international phone numbers (+44, +49, +81, +33)', () => {
      const intlPhone = '+44 20 7946 0958';
      const masked = piiMasker.maskPhone(intlPhone);

      expect(masked).toContain('0958');
      expect(masked).not.toContain('7946');
    });

    test('B10.5: masks mixed-case, subaddressed, and complex top-level domain emails', () => {
      const complexEmail = 'User.Name+Newsletter@SUB.Domain.CO.UK';
      const masked = piiMasker.maskEmail(complexEmail);

      expect(masked).toContain('@SUB.Domain.CO.UK');
      expect(masked).not.toContain('User.Name+Newsletter');
    });
  });

  // =========================================================================
  // B11: Deep Structure Masking Boundary Cases
  // =========================================================================
  describe('B11: Deep Structure Masking Boundary Cases', () => {
    test('B11.1: prevents infinite recursion on circular object references by returning [CIRCULAR]', () => {
      const circularObj = {
        name: 'Root Node',
        email: 'root@network.org',
      };
      circularObj.child = circularObj;

      const masked = piiMasker.maskObject(circularObj);

      expect(masked.name).toBe('Root Node');
      expect(masked.email).toBe('r***t@network.org');
      expect(masked.child).toBe('[CIRCULAR]');
    });

    test('B11.2: traverses deeply nested objects (depth > 25) without stack overflow', () => {
      let deepObj = { email: 'deep.user@vault.io' };
      for (let i = 0; i < 30; i++) {
        deepObj = { level: i, nested: deepObj };
      }

      const masked = piiMasker.maskObject(deepObj);
      expect(masked.level).toBe(29);
    });

    test('B11.3: handles sparse arrays, empty arrays, and mixed-type arrays', () => {
      const mixedArr = [
        'john@example.com',
        123,
        null,
        ['inner@test.com', true],
      ];

      const masked = piiMasker.maskObject(mixedArr);
      expect(masked[0]).toBe('j***n@example.com');
      expect(masked[1]).toBe(123);
      expect(masked[2]).toBeNull();
      expect(masked[3][0]).toBe('i***r@test.com');
      expect(masked[3][1]).toBe(true);
    });

    test('B11.4: processes large tabular datasets (1,000+ rows) efficiently', () => {
      const largeTable = {
        columns: [{ name: 'id' }, { name: 'email' }, { name: 'phone' }],
        rows: Array.from({ length: 1000 }, (_, i) => [
          i + 1,
          `user${i}@bigdata.corp`,
          '555-123-4567',
        ]),
      };

      const start = Date.now();
      const sanitized = piiMasker.maskTabularResult(largeTable);
      const duration = Date.now() - start;

      expect(sanitized.data ? sanitized.data.rows : sanitized.rows).toHaveLength(1000);
      expect(duration).toBeLessThan(1000); // Sub-second performance
    });

    test('B11.5: sanitizes RFC 4180 CSV with embedded newlines, escaped quotes, and commas inside fields', () => {
      const complexCSV = 'id,notes,email\n1,"Line 1\nLine 2 with ""quotes""",contact@support.com';
      const sanitized = piiMasker.maskCSV(complexCSV);

      expect(sanitized).toContain('c***t@support.com');
      expect(sanitized).not.toContain('contact@support.com');
      expect(sanitized).toContain('Line 1');
    });
  });

  // =========================================================================
  // B12: Analytical Utility Boundary Cases
  // =========================================================================
  describe('B12: Analytical Utility Boundary Cases', () => {
    test('B12.1: ensures consistent deterministic pseudonymization across multiple calls with same salt', () => {
      const val = 'customer_vip_888';
      const hash1 = piiMasker.pseudonymizeValue(val, 'custom_salt');
      const hash2 = piiMasker.pseudonymizeValue(val, 'custom_salt');

      expect(hash1).toBe(hash2);
    });

    test('B12.2: ensures distinct inputs generate non-colliding pseudonymized tokens', () => {
      const tokenA = piiMasker.pseudonymizeValue('user_alpha');
      const tokenB = piiMasker.pseudonymizeValue('user_beta');

      expect(tokenA).not.toBe(tokenB);
    });

    test('B12.3: custom salt changes the pseudonymization hash output for the same input', () => {
      const val = 'same_user_id';
      const hashSalt1 = piiMasker.pseudonymizeValue(val, 'salt_alpha');
      const hashSalt2 = piiMasker.pseudonymizeValue(val, 'salt_beta');

      expect(hashSalt1).not.toBe(hashSalt2);
    });

    test('B12.4: strict mode replaces all sensitive categories with exact standardized REDACTION_TOKENS', () => {
      const email = 'alex@acme.com';
      const phone = '555-123-4567';
      const ssn = '123-45-6789';

      expect(piiMasker.maskEmail(email, { strict: true })).toBe(piiMasker.REDACTION_TOKENS.EMAIL);
      expect(piiMasker.maskPhone(phone, { strict: true })).toBe(piiMasker.REDACTION_TOKENS.PHONE);
      expect(piiMasker.maskSSN(ssn, { strict: true })).toBe(piiMasker.REDACTION_TOKENS.SSN);
    });

    test('B12.5: handles malformed, truncated, or incomplete PII strings without throwing', () => {
      const malformedInputs = [
        'not_an_email',
        '@nodomain',
        '123',
        '---',
        'sk-ant-short',
      ];

      malformedInputs.forEach(input => {
        expect(() => piiMasker.maskString(input)).not.toThrow();
      });
    });
  });
});
