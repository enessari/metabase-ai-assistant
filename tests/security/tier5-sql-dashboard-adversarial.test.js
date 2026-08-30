import { jest } from '@jest/globals';
import {
  SqlHealingEngine,
  executeAndHealSQL,
  repairSQLError,
  classifySQLError,
  extractErrorIdentifiers,
  extractTablesFromSQL,
  analyzeSelectClause,
  levenshteinDistance,
  stringSimilarity,
  findBestMatch,
  ERROR_CATEGORIES,
} from '../../src/ai/sql-healing-engine.js';
import { SqlHandler } from '../../src/mcp/handlers/sql.js';
import {
  DashboardArchitect,
  buildFullDashboard,
  calculate24ColGridPositions,
  validateNoCollisions,
  generateFilterMappings,
  getCardDimensionsAndArchetype,
  getDefaultVisualizationSettings,
  GRID_WIDTH,
  CARD_ARCHETYPES,
} from '../../src/analytics/dashboard-architect.js';
import { CardsHandler } from '../../src/mcp/handlers/cards.js';

describe('Tier 5 Adversarial Coverage Hardening (SQL Healing Engine & Dashboard Architect)', () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    process.env = {
      ...originalEnv,
      METABASE_READ_ONLY_MODE: 'false',
      METABASE_URL: 'http://localhost:3000',
    };
  });

  afterEach(() => {
    process.env = { ...originalEnv };
  });

  // =========================================================================
  // SECTION 1: COMPLEX MULTI-ERROR CASCADING SQL HEALING (R1)
  // =========================================================================
  describe('1. Complex Multi-Error Cascading SQL Healing & Recovery', () => {
    test('autonomously heals a 4-step cascading error query: Syntax Typo -> Missing Column -> GROUP BY Violation -> Division by Zero', async () => {
      let callCount = 0;
      const executionHistory = [];

      const mockClient = {
        executeNativeQuery: jest.fn().mockImplementation(async (dbId, sql) => {
          callCount++;
          executionHistory.push({ attempt: callCount, sql });

          if (callCount === 1) {
            // Attempt 1 fails with syntax error: "SELEC" and "FORM"
            expect(sql).toContain('SELEC');
            expect(sql).toContain('FORM');
            throw new Error('syntax error at or near "SELEC"');
          }
          if (callCount === 2) {
            // Attempt 2 fixed syntax, but fails with missing column: "user_mail"
            expect(sql).toContain('SELECT');
            expect(sql).toContain('FROM');
            expect(sql).toContain('user_mail');
            throw new Error('column "user_mail" does not exist');
          }
          if (callCount === 3) {
            // Attempt 3 replaced "user_mail" -> "email", but fails with GROUP BY violation
            expect(sql).toContain('email');
            throw new Error('column "sales.category" must appear in the GROUP BY clause or be used in an aggregate function');
          }
          if (callCount === 4) {
            // Attempt 4 added GROUP BY, but fails with division by zero
            expect(sql).toContain('GROUP BY');
            throw new Error('division by zero');
          }

          // Attempt 5: All 4 errors healed!
          expect(sql).toContain('NULLIF');
          expect(sql).toContain('GROUP BY');
          return {
            data: {
              cols: [
                { name: 'category', base_type: 'type/Text' },
                { name: 'email', base_type: 'type/Text' },
                { name: 'avg_rev_per_order', base_type: 'type/Decimal' },
              ],
              rows: [
                ['Electronics', 'alice@company.com', 450.0],
                ['Apparel', 'bob@company.com', 120.5],
              ],
            },
          };
        }),
      };

      const initialSql = 'SELEC category, user_mail, sum(revenue) / total_orders AS avg_rev_per_order FORM sales;';

      const result = await executeAndHealSQL({
        databaseId: 1,
        sql: initialSql,
        maxAttempts: 5,
        client: mockClient,
      });

      expect(result.success).toBe(true);
      expect(result.attempts_used).toBe(5);
      expect(result.healed).toBe(true);
      expect(result.healing_trail).toHaveLength(4);

      // Verify Attempt 1 Trail (Syntax repair: SELEC -> SELECT, FORM -> FROM)
      expect(result.healing_trail[0].attempt).toBe(1);
      expect(result.healing_trail[0].error_category).toBe(ERROR_CATEGORIES.SYNTAX_ERROR);
      expect(result.healing_trail[0].corrected_sql).toContain('SELECT category, user_mail, sum(revenue) / total_orders');
      expect(result.healing_trail[0].corrected_sql).toContain('FROM sales;');

      // Verify Attempt 2 Trail (Missing column repair: user_mail -> email)
      expect(result.healing_trail[1].attempt).toBe(2);
      expect(result.healing_trail[1].error_category).toBe(ERROR_CATEGORIES.MISSING_COLUMN);
      expect(result.healing_trail[1].corrected_sql).toContain('email');

      // Verify Attempt 3 Trail (Group By violation repair)
      expect(result.healing_trail[2].attempt).toBe(3);
      expect(result.healing_trail[2].error_category).toBe(ERROR_CATEGORIES.GROUP_BY_VIOLATION);
      expect(result.healing_trail[2].corrected_sql).toContain('GROUP BY');

      // Verify Attempt 4 Trail (Division by zero repair: NULLIF)
      expect(result.healing_trail[3].attempt).toBe(4);
      expect(result.healing_trail[3].error_category).toBe(ERROR_CATEGORIES.DIVIDE_BY_ZERO);
      expect(result.healing_trail[3].corrected_sql).toContain('NULLIF(total_orders, 0)');

      // Verify Final Executed SQL
      expect(result.final_sql).toContain('SELECT category, email');
      expect(result.final_sql).toContain('NULLIF(total_orders, 0)');
      expect(result.final_sql).toContain('GROUP BY');

      // Verify Provenance
      expect(result._provenance).toBeDefined();
      expect(result._provenance.ai_generated).toBe(true);
      expect(result._provenance.tool).toBe('ai_sql_execute_and_heal');
      expect(result._provenance.generation_parameters.attempts_used).toBe(5);
      expect(result._provenance.generation_parameters.healed).toBe(true);
      expect(result._provenance.healing_trail).toHaveLength(4);
    });

    test('gracefully exhausts retries and preserves diagnostic trail when maxAttempts < required healing steps', async () => {
      let callCount = 0;
      const mockClient = {
        executeNativeQuery: jest.fn().mockImplementation(async () => {
          callCount++;
          if (callCount === 1) {
            throw new Error('syntax error at or near "SELEC"');
          }
          if (callCount === 2) {
            throw new Error('column "user_mail" does not exist');
          }
          throw new Error('column "category" must appear in the GROUP BY clause');
        }),
      };

      const initialSql = 'SELEC category, user_mail, COUNT(*) FORM logs;';

      const result = await executeAndHealSQL({
        databaseId: 1,
        sql: initialSql,
        maxAttempts: 3, // Allowed only 3 attempts for a 4-step problem
        client: mockClient,
      });

      expect(result.success).toBe(false);
      expect(result.attempts_used).toBe(3);
      expect(result.healed).toBe(false);
      expect(result.healing_trail).toHaveLength(3);

      // Attempt 1: Syntax repair
      expect(result.healing_trail[0].error_category).toBe(ERROR_CATEGORIES.SYNTAX_ERROR);
      expect(result.healing_trail[0].corrected_sql).toBe('SELECT category, user_mail, COUNT(*) FROM logs;');

      // Attempt 2: Column synonym repair
      expect(result.healing_trail[1].error_category).toBe(ERROR_CATEGORIES.MISSING_COLUMN);
      expect(result.healing_trail[1].corrected_sql).toBe('SELECT category, email, COUNT(*) FROM logs;');

      // Attempt 3: Cutoff reached
      expect(result.healing_trail[2].attempt).toBe(3);
      expect(result.healing_trail[2].error_category).toBe(ERROR_CATEGORIES.GROUP_BY_VIOLATION);
      expect(result.healing_trail[2].diagnosis).toContain('Max attempts (3) reached without resolution.');
      expect(result.healing_trail[2].corrected_sql).toBeNull();

      expect(result.error).toContain('GROUP BY clause');
      expect(result._provenance.generation_parameters.healed).toBe(false);
    });

    test('recovers from complex table schema qualification and column typos via catalog introspection', async () => {
      let callCount = 0;
      const mockCatalog = [
        { name: 'audit_events', schema: 'security_logs', fields: [{ name: 'event_id' }, { name: 'user_email' }] },
        { name: 'orders', schema: 'public', fields: [{ name: 'id' }, { name: 'total_amount' }] },
      ];

      const mockClient = {
        getDatabaseTables: jest.fn().mockResolvedValue(mockCatalog),
        executeNativeQuery: jest.fn().mockImplementation(async (dbId, sql) => {
          callCount++;
          if (callCount === 1) {
            throw new Error('relation "audit_events" does not exist');
          }
          if (callCount === 2) {
            throw new Error('column "user_mail" does not exist');
          }
          return {
            data: {
              cols: [{ name: 'event_id' }, { name: 'user_email' }],
              rows: [[1001, 'security@enterprise.com']],
            },
          };
        }),
      };

      const result = await executeAndHealSQL({
        databaseId: 42,
        sql: 'SELECT event_id, user_mail FROM audit_events WHERE event_id > 0;',
        maxAttempts: 3,
        client: mockClient,
      });

      expect(result.success).toBe(true);
      expect(result.attempts_used).toBe(3);
      expect(result.healed).toBe(true);
      expect(mockClient.getDatabaseTables).toHaveBeenCalledWith(42);
      expect(result.final_sql).toContain('security_logs.audit_events');
      expect(result.final_sql).toContain('email');
    });
  });

  // =========================================================================
  // SECTION 2: EXTREME SQL CONSTRUCTS & DIALECT EDGE CASES
  // =========================================================================
  describe('2. Extreme SQL Queries, Complex Dialects, & Interleaved Comments', () => {
    test('processes and heals queries with nested CTEs, window functions, and multiple subqueries', async () => {
      const complexCteSql = `
        WITH ranked_orders AS (
          SELEC customer_id, total_amount, created_at,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn,
                DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY total_amount DESC) AS spend_rank
          FORM orders
          WHER status = 'COMPLETED'
        ),
        customer_summary AS (
          SELEC customer_id, SUM(total_amount) AS ltv, COUNT(*) AS order_cnt
          FORM ranked_orders
          WHER rn <= 5
          GROP BY customer_id
        )
        SELEC c.email, cs.ltv, cs.order_cnt
        FORM customers c
        JOIN customer_summary cs ON c.id = cs.customer_id
        WHER cs.ltv > 500
        OERDER BY cs.ltv DESC
        LIMT 25;
      `;

      const repair = await repairSQLError({
        sql: complexCteSql,
        errorCategory: ERROR_CATEGORIES.SYNTAX_ERROR,
        errorMessage: 'syntax error at or near "SELEC"',
      });

      expect(repair.ruleApplied).toBe(true);

      // Verify keyword typos were replaced at word boundaries
      expect(repair.repairedSql).not.toMatch(/\bSELEC\b/);
      expect(repair.repairedSql).not.toMatch(/\bFORM\b/);
      expect(repair.repairedSql).not.toMatch(/\bWHER\b/);
      expect(repair.repairedSql).not.toMatch(/\bGROP BY\b/);
      expect(repair.repairedSql).not.toMatch(/\bOERDER BY\b/);
      expect(repair.repairedSql).not.toMatch(/\bLIMT\b/);

      expect(repair.repairedSql).toContain('SELECT customer_id, total_amount, created_at');
      expect(repair.repairedSql).toContain('ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn');
      expect(repair.repairedSql).toContain('FROM orders');
      expect(repair.repairedSql).toContain('GROUP BY customer_id');
      expect(repair.repairedSql).toContain('ORDER BY cs.ltv DESC');
      expect(repair.repairedSql).toContain('LIMIT 25;');

      // Verify table extraction from nested CTE query
      const tables = extractTablesFromSQL(repair.repairedSql);
      expect(tables).toContain('orders');
      expect(tables).toContain('ranked_orders');
      expect(tables).toContain('customers');
      expect(tables).toContain('customer_summary');
    });

    test('repairs trailing commas and typos across UNION ALL branches without corrupting query structure', async () => {
      const unionSql = `
        SELEC id, user_mail, total_amount,
        FORM online_orders
        WHERE status = 'PAID'
        UNION ALL
        SELEC id, user_mail, total_amount,
        FORM pos_orders
        WHERE status = 'COMPLETED'
        ORDER BY id DESC,
        LIMT 50;
      `;

      const res = await repairSQLError({
        sql: unionSql,
        errorCategory: ERROR_CATEGORIES.SYNTAX_ERROR,
        errorMessage: 'syntax error near "FORM"',
      });

      expect(res.ruleApplied).toBe(true);
      expect(res.repairedSql).toContain('SELECT id, user_mail, total_amount');
      expect(res.repairedSql).toContain('FROM online_orders');
      expect(res.repairedSql).toContain('UNION ALL');
      expect(res.repairedSql).toContain('FROM pos_orders');
      expect(res.repairedSql).not.toContain(',\n        FROM');
      expect(res.repairedSql).not.toContain(',\n        LIMIT');
      expect(res.repairedSql).toContain('LIMIT 50;');
    });

    test('replaces keywords in queries with interleaved comments and preserves comment blocks', async () => {
      const sqlWithComments = `
        /* Top Level Header Block Comment */
        SELEC /* col comment */ id, -- trailing line comment
               email,
               created_at
        FORM /* table comment */ users
        WHER status = 'ACTIVE' -- filter active
        OERDER BY created_at DESC;
      `;

      const res = await repairSQLError({
        sql: sqlWithComments,
        errorCategory: ERROR_CATEGORIES.SYNTAX_ERROR,
        errorMessage: 'syntax error near "SELEC"',
      });

      expect(res.ruleApplied).toBe(true);
      expect(res.repairedSql).toContain('/* Top Level Header Block Comment */');
      expect(res.repairedSql).toContain('/* col comment */');
      expect(res.repairedSql).toContain('/* table comment */');
      expect(res.repairedSql).toContain('SELECT /* col comment */ id');
      expect(res.repairedSql).toContain('FROM /* table comment */ users');
      expect(res.repairedSql).toContain('WHERE status = \'ACTIVE\'');
      expect(res.repairedSql).toContain('ORDER BY created_at DESC;');
    });

    test('repairs multiple division operations with complex arithmetic expressions in denominator', async () => {
      const divSql = 'SELECT category, rev / (cost + tax) AS margin_rate, total / count_items AS item_avg FROM financial_data;';

      const res = await repairSQLError({
        sql: divSql,
        errorCategory: ERROR_CATEGORIES.DIVIDE_BY_ZERO,
        errorMessage: 'division by zero error',
      });

      expect(res.ruleApplied).toBe(true);
      expect(res.repairedSql).toContain('rev / NULLIF((cost + tax), 0)');
      expect(res.repairedSql).toContain('total / NULLIF(count_items, 0)');
      expect(res.diagnosis).toContain('NULLIF');
    });

    test('does not duplicate NULLIF if denominator is already wrapped in NULLIF', async () => {
      const alreadyWrapped = 'SELECT rev / NULLIF(units, 0) AS rev_per_unit FROM sales;';

      const res = await repairSQLError({
        sql: alreadyWrapped,
        errorCategory: ERROR_CATEGORIES.DIVIDE_BY_ZERO,
        errorMessage: 'division by zero',
      });

      expect(res.repairedSql).toBe(alreadyWrapped);
    });

    test('falls back to AI Assistant when deterministic repair does not resolve unknown dialect error', async () => {
      const mockAssistant = {
        aiProvider: 'openai',
        model: 'gpt-4o',
        getAIResponse: jest.fn().mockResolvedValue('```sql\nSELECT DATE_TRUNC(\'month\', created_at) AS m, COUNT(*) FROM orders GROUP BY 1;\n```'),
      };

      const customDialectSql = 'SELECT STRFTIME_UNSUPPORTED(created_at) AS m, COUNT(*) FROM orders;';

      const res = await repairSQLError({
        sql: customDialectSql,
        errorCategory: ERROR_CATEGORIES.UNKNOWN,
        errorMessage: 'function STRFTIME_UNSUPPORTED not found',
        assistant: mockAssistant,
      });

      expect(res.ruleApplied).toBe(true);
      expect(mockAssistant.getAIResponse).toHaveBeenCalled();
      expect(res.repairedSql).toBe("SELECT DATE_TRUNC('month', created_at) AS m, COUNT(*) FROM orders GROUP BY 1;");
      expect(res.diagnosis).toContain('AI assistant generated corrected SQL');
    });

    test('handles AI Assistant failure gracefully and preserves original SQL without throwing', async () => {
      const mockAssistant = {
        getAIResponse: jest.fn().mockRejectedValue(new Error('Rate limit exceeded (429)')),
      };

      const sql = 'SELECT invalid_function() FROM test;';
      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.UNKNOWN,
        errorMessage: 'invalid function',
        assistant: mockAssistant,
      });

      expect(res.repairedSql).toBe(sql);
      expect(res.ruleApplied).toBe(false);
      expect(res.diagnosis).toContain('LLM fallback failed');
    });
  });

  // =========================================================================
  // SECTION 3: ERROR CLASSIFICATION STABILITY & IDENTIFIER EXTRACTION
  // =========================================================================
  describe('3. Error Classification Robustness & Provenance Integrity', () => {
    test('reliably classifies exotic database error dialect signatures', () => {
      const testMatrix = [
        // Postgres
        { msg: 'ERROR: relation "public.user_profiles" does not exist at character 15', expected: ERROR_CATEGORIES.INVALID_TABLE },
        { msg: 'ERROR: column reference "org_id" is ambiguous', expected: ERROR_CATEGORIES.AMBIGUOUS_COLUMN },
        { msg: 'ERROR: column "users.created_date" must appear in the GROUP BY clause', expected: ERROR_CATEGORIES.GROUP_BY_VIOLATION },
        { msg: 'ERROR: division by zero', expected: ERROR_CATEGORIES.DIVIDE_BY_ZERO },

        // MySQL / MariaDB
        { msg: "Error 1054: Unknown column 'customer_mail' in 'field list'", expected: ERROR_CATEGORIES.MISSING_COLUMN },
        { msg: "Error 1146: Table 'analytics.metrics_daily' doesn't exist", expected: ERROR_CATEGORIES.INVALID_TABLE },
        { msg: "Error 1052: Column 'id' in field list is ambiguous", expected: ERROR_CATEGORIES.AMBIGUOUS_COLUMN },
        { msg: 'Error 1064: You have an error in your SQL syntax near "LIMITT 10"', expected: ERROR_CATEGORIES.SYNTAX_ERROR },

        // BigQuery / Snowflake / ClickHouse / SQLite
        { msg: 'Unrecognized name: user_account_id at [1:25]', expected: ERROR_CATEGORIES.MISSING_COLUMN },
        { msg: 'Table or view `prj.ds.non_existent` not found', expected: ERROR_CATEGORIES.INVALID_TABLE },
        { msg: 'no such table: legacy_orders', expected: ERROR_CATEGORIES.INVALID_TABLE },
        { msg: 'no such column: total_rev', expected: ERROR_CATEGORIES.MISSING_COLUMN },
        { msg: 'SQL compilation error: types VARCHAR and NUMBER are not comparable', expected: ERROR_CATEGORIES.TYPE_MISMATCH },
        { msg: 'CANNOT CAST TYPE TIMESTAMP TO INTEGER', expected: ERROR_CATEGORIES.TYPE_MISMATCH },

        // Unknown / Fallback
        { msg: 'Fatal network timeout communicating with upstream shard 4', expected: ERROR_CATEGORIES.UNKNOWN },
        { msg: '', expected: ERROR_CATEGORIES.UNKNOWN },
        { msg: null, expected: ERROR_CATEGORIES.UNKNOWN },
        { msg: undefined, expected: ERROR_CATEGORIES.UNKNOWN },
        { msg: 12345, expected: ERROR_CATEGORIES.UNKNOWN },
      ];

      for (const { msg, expected } of testMatrix) {
        expect(classifySQLError(msg)).toBe(expected);
      }
    });

    test('extracts identifiers across quoted, backticked, and unquoted formats', () => {
      // Missing column extraction
      expect(extractErrorIdentifiers('column "user_phone" does not exist', ERROR_CATEGORIES.MISSING_COLUMN)).toEqual({ columnName: 'user_phone' });
      expect(extractErrorIdentifiers("Unknown column 'postal_code' in 'field list'", ERROR_CATEGORIES.MISSING_COLUMN)).toEqual({ columnName: 'postal_code' });
      expect(extractErrorIdentifiers('unrecognized name: billing_address', ERROR_CATEGORIES.MISSING_COLUMN)).toEqual({ columnName: 'billing_address' });
      expect(extractErrorIdentifiers('no such column: unit_cost', ERROR_CATEGORIES.MISSING_COLUMN)).toEqual({ columnName: 'unit_cost' });

      // Invalid table extraction
      expect(extractErrorIdentifiers('relation "audit_events" does not exist', ERROR_CATEGORIES.INVALID_TABLE)).toEqual({ tableName: 'audit_events' });
      expect(extractErrorIdentifiers("Table 'warehouse.dim_customers' doesn't exist", ERROR_CATEGORIES.INVALID_TABLE)).toEqual({ tableName: 'warehouse.dim_customers' });
      expect(extractErrorIdentifiers('table or view `analytics.quarterly_summary` not found', ERROR_CATEGORIES.INVALID_TABLE)).toEqual({ tableName: 'analytics.quarterly_summary' });
      expect(extractErrorIdentifiers('no such table: temp_session_data', ERROR_CATEGORIES.INVALID_TABLE)).toEqual({ tableName: 'temp_session_data' });

      // Ambiguous column extraction
      expect(extractErrorIdentifiers('column reference "customer_id" is ambiguous', ERROR_CATEGORIES.AMBIGUOUS_COLUMN)).toEqual({ columnName: 'customer_id' });
    });

    test('SqlHandler.handleExecuteAndHealSQL enforces read-only mode and blocks write operations', async () => {
      process.env.METABASE_READ_ONLY_MODE = 'true';

      const mockClient = {
        executeNativeQuery: jest.fn(),
      };
      const handler = new SqlHandler(mockClient);

      const writeQueries = [
        'DROP TABLE users CASCADE;',
        'TRUNCATE TABLE transactions;',
        'DELETE FROM accounts WHERE balance < 0;',
        'ALTER TABLE orders ADD COLUMN backdoored text;',
        'INSERT INTO users (id, name) VALUES (1, "attacker");',
        'UPDATE users SET role = "admin" WHERE id = 1;',
      ];

      for (const writeSql of writeQueries) {
        const response = await handler.handleExecuteAndHealSQL({
          database_id: 1,
          sql: writeSql,
        });

        expect(response.content[0].text).toContain('Read-Only Mode Active');
        expect(response.content[0].text).toContain('Operation Blocked');
        expect(mockClient.executeNativeQuery).not.toHaveBeenCalled();
      }
    });
  });

  // =========================================================================
  // SECTION 4: DASHBOARD ARCHITECT — LARGE 12 & 20-CARD 24-COL GRID LAYOUTS (R2)
  // =========================================================================
  describe('4. Dashboard Architect Large 12 & 20-Card Mathematical Grid Layout Invariance', () => {
    test('computes collision-free 24-col grid layout for 12-card dashboard with all archetypes', () => {
      const cards12 = [
        // 4 KPIs (Row 0, cols 0, 6, 12, 18, 6x4)
        { name: 'MRR', display: 'scalar' },
        { name: 'ARR', display: 'number' },
        { name: 'Churn Rate', display: 'gauge' },
        { name: 'Net Retention', display: 'smartscalar' },

        // 3 Trends (Row 4 & 12, 12x8)
        { name: 'Revenue Trend', display: 'line' },
        { name: 'Customer Acquisition', display: 'bar' },
        { name: 'Cohort Retention', display: 'area' },

        // 3 Breakdowns (Width 8 for 3 cards: row 20, cols 0, 8, 16, 8x6)
        { name: 'Revenue by Plan', display: 'pie' },
        { name: 'Leads by Channel', display: 'donut' },
        { name: 'Sales by Region', display: 'row' },

        // 2 Tables (Full width 24x8: row 26 & row 34)
        { name: 'Top Customer Accounts', display: 'table' },
        { name: 'Regional Performance Matrix', display: 'pivot' },
      ];

      const positions = calculate24ColGridPositions(cards12);

      expect(positions).toHaveLength(12);

      // Validate mathematically with strict collision check
      expect(validateNoCollisions(positions)).toBe(true);

      // Verify exact layout structure
      // Row 0: KPIs
      expect(positions[0]).toEqual({ row: 0, col: 0, size_x: 6, size_y: 4 });
      expect(positions[1]).toEqual({ row: 0, col: 6, size_x: 6, size_y: 4 });
      expect(positions[2]).toEqual({ row: 0, col: 12, size_x: 6, size_y: 4 });
      expect(positions[3]).toEqual({ row: 0, col: 18, size_x: 6, size_y: 4 });

      // Row 4 & Row 12: Trends (3 trend cards -> 2 on row 4, 1 on row 12)
      expect(positions[4]).toEqual({ row: 4, col: 0, size_x: 12, size_y: 8 });
      expect(positions[5]).toEqual({ row: 4, col: 12, size_x: 12, size_y: 8 });
      expect(positions[6]).toEqual({ row: 12, col: 0, size_x: 12, size_y: 8 });

      // Row 20: 3 Breakdowns (width 8 each: cols 0, 8, 16)
      expect(positions[7]).toEqual({ row: 20, col: 0, size_x: 8, size_y: 6 });
      expect(positions[8]).toEqual({ row: 20, col: 8, size_x: 8, size_y: 6 });
      expect(positions[9]).toEqual({ row: 20, col: 16, size_x: 8, size_y: 6 });

      // Row 26 & Row 34: 2 Tables (width 24 each: col 0)
      expect(positions[10]).toEqual({ row: 26, col: 0, size_x: 24, size_y: 8 });
      expect(positions[11]).toEqual({ row: 34, col: 0, size_x: 24, size_y: 8 });

      // Mathematical boundary check on all 12 cards
      for (const pos of positions) {
        expect(pos.col + pos.size_x).toBeLessThanOrEqual(GRID_WIDTH);
        expect(pos.row).toBeGreaterThanOrEqual(0);
        expect(pos.col).toBeGreaterThanOrEqual(0);
        expect(pos.size_x).toBeGreaterThan(0);
        expect(pos.size_y).toBeGreaterThan(0);
      }
    });

    test('computes collision-free 24-col grid layout for massive 20-card dashboard', () => {
      const cards20 = [
        // 8 KPIs (Row 0 & Row 4, 4 per row)
        { name: 'KPI 1', display: 'scalar' },
        { name: 'KPI 2', display: 'scalar' },
        { name: 'KPI 3', display: 'number' },
        { name: 'KPI 4', display: 'gauge' },
        { name: 'KPI 5', display: 'scalar' },
        { name: 'KPI 6', display: 'number' },
        { name: 'KPI 7', display: 'smartscalar' },
        { name: 'KPI 8', display: 'gauge' },

        // 4 Trends (2 rows of 2, 12x8)
        { name: 'Trend 1', display: 'line' },
        { name: 'Trend 2', display: 'bar' },
        { name: 'Trend 3', display: 'area' },
        { name: 'Trend 4', display: 'combo' },

        // 6 Breakdowns (2 rows of 3, 8x6)
        { name: 'Breakdown 1', display: 'pie' },
        { name: 'Breakdown 2', display: 'donut' },
        { name: 'Breakdown 3', display: 'row' },
        { name: 'Breakdown 4', display: 'funnel' },
        { name: 'Breakdown 5', display: 'progress' },
        { name: 'Breakdown 6', display: 'scatter' },

        // 2 Tables (2 rows of 1, 24x8)
        { name: 'Table 1', display: 'table' },
        { name: 'Table 2', display: 'pivot' },
      ];

      const positions = calculate24ColGridPositions(cards20);

      expect(positions).toHaveLength(20);
      expect(validateNoCollisions(positions)).toBe(true);

      // Verify every pair of cards has 0 intersection area
      for (let i = 0; i < positions.length; i++) {
        for (let j = i + 1; j < positions.length; j++) {
          const a = positions[i];
          const b = positions[j];

          const overlapX = Math.max(0, Math.min(a.col + a.size_x, b.col + b.size_x) - Math.max(a.col, b.col));
          const overlapY = Math.max(0, Math.min(a.row + a.size_y, b.row + b.size_y) - Math.max(a.row, b.row));
          const intersectionArea = overlapX * overlapY;

          expect(intersectionArea).toBe(0);
        }
      }
    });

    test('fuzz-tests arbitrary card counts (1 to 25) with randomized archetype sequences', () => {
      const archetypes = ['scalar', 'number', 'gauge', 'line', 'bar', 'area', 'pie', 'donut', 'row', 'table', 'pivot'];

      for (let cardCount = 1; cardCount <= 25; cardCount++) {
        const randomCards = Array.from({ length: cardCount }, (_, idx) => ({
          name: `Card ${idx + 1}`,
          display: archetypes[idx % archetypes.length],
        }));

        const positions = calculate24ColGridPositions(randomCards);

        expect(positions).toHaveLength(cardCount);
        expect(validateNoCollisions(positions)).toBe(true);

        for (const pos of positions) {
          expect(pos.col + pos.size_x).toBeLessThanOrEqual(GRID_WIDTH);
          expect(pos.row).toBeGreaterThanOrEqual(0);
          expect(pos.col).toBeGreaterThanOrEqual(0);
        }
      }
    });

    test('validateNoCollisions rejects diagonal overlap or out of bound positions', () => {
      // Exactly out of bounds: col 20 + size_x 5 = 25 > 24
      expect(() => validateNoCollisions([{ row: 0, col: 20, size_x: 5, size_y: 4 }])).toThrow(
        /exceeds 24-column grid boundary/
      );

      // Overlapping by 1 column and 1 row
      const partialOverlap = [
        { row: 0, col: 0, size_x: 12, size_y: 6 },
        { row: 5, col: 11, size_x: 12, size_y: 6 }, // overlaps at row 5, col 11
      ];
      expect(() => validateNoCollisions(partialOverlap)).toThrow(/Grid collision detected/);

      // Corner touching (no overlap)
      const cornerTouching = [
        { row: 0, col: 0, size_x: 12, size_y: 6 },
        { row: 6, col: 12, size_x: 12, size_y: 6 }, // touches at (6, 12), intersection is 0
      ];
      expect(validateNoCollisions(cornerTouching)).toBe(true);
    });
  });

  // =========================================================================
  // SECTION 5: TEMPLATE TAG FILTER AUTO-LINKING & PARAMETERS
  // =========================================================================
  describe('5. Template Tag Filter Auto-Linking, Parameter Slugs, & Missing Variables', () => {
    test('auto-links multiple complex filters with hyphenated, snake_case, and custom slugs across diverse SQL queries', () => {
      const cards = [
        {
          name: 'Revenue Card',
          sql: 'SELECT date, sum(amount) FROM orders WHERE {{fiscal_quarter_range}} AND {{dept_code_filter}} GROUP BY 1',
        },
        {
          name: 'Customer Card',
          sql: 'SELECT * FROM customers WHERE {{customer_tier}} AND {{is_active_flag}}',
        },
        {
          name: 'Complex Join Card',
          sql: 'SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE {{fiscal_quarter_range}} AND {{customer_tier}}',
        },
        {
          name: 'Unfiltered Static Card',
          sql: 'SELECT 1 AS status, "Healthy" AS health',
        },
      ];

      const filters = [
        { name: 'Fiscal Quarter', slug: 'fiscal_quarter_range', type: 'date/all-options', target_variable: 'fiscal_quarter_range' },
        { name: 'Department Code', slug: 'dept_code_filter', type: 'category', target_variable: 'dept_code_filter' },
        { name: 'Customer Tier', slug: 'customer_tier', type: 'category', target_variable: 'customer_tier' },
        { name: 'Active Status', slug: 'is_active_flag', type: 'category', target_variable: 'is_active_flag' },
        { name: 'Unmatched Filter', slug: 'unmatched_var', type: 'string/=', target_variable: 'unmatched_var' },
      ];

      const mappings = generateFilterMappings(cards, filters);

      expect(mappings).toHaveLength(4);

      // Card 0: fiscal_quarter_range & dept_code_filter
      expect(mappings[0]).toHaveLength(2);
      expect(mappings[0].map(m => m.target[1][1])).toEqual(['fiscal_quarter_range', 'dept_code_filter']);

      // Card 1: customer_tier & is_active_flag
      expect(mappings[1]).toHaveLength(2);
      expect(mappings[1].map(m => m.target[1][1])).toEqual(['customer_tier', 'is_active_flag']);

      // Card 2: fiscal_quarter_range & customer_tier
      expect(mappings[2]).toHaveLength(2);
      expect(mappings[2].map(m => m.target[1][1])).toEqual(['fiscal_quarter_range', 'customer_tier']);

      // Card 3: 0 mappings
      expect(mappings[3]).toHaveLength(0);
    });

    test('handles cards with undeclared template tags in SQL gracefully during buildFullDashboard', async () => {
      const mockClient = {
        createQuestion: jest.fn().mockImplementation((payload) => Promise.resolve({
          id: 101,
          name: payload.name,
          display: payload.display,
        })),
        createDashboard: jest.fn().mockResolvedValue({ id: 999, name: 'Filter Test Dashboard' }),
        updateDashboard: jest.fn().mockResolvedValue({ id: 999 }),
        addCardToDashboard: jest.fn().mockResolvedValue({ id: 1 }),
      };

      const cards = [
        {
          name: 'Card with Extra Tag',
          display: 'scalar',
          sql: 'SELECT count(*) FROM orders WHERE {{known_filter}} AND {{unknown_standalone_tag}}',
        },
        { name: 'C2', display: 'scalar', sql: 'SELECT 1' },
        { name: 'C3', display: 'line', sql: 'SELECT 2' },
        { name: 'C4', display: 'table', sql: 'SELECT 3' },
      ];

      const filters = [
        { name: 'Known Filter', slug: 'known_filter', type: 'category', target_variable: 'known_filter' },
      ];

      const result = await buildFullDashboard({
        name: 'Filter Test Dashboard',
        databaseId: 1,
        cards,
        filters,
        client: mockClient,
      });

      expect(result.dashboard_id).toBe(999);
      expect(result.card_count).toBe(4);

      // Verify createQuestion was called with templateTags synthesized for both known and standalone tags
      const questionCall = mockClient.createQuestion.mock.calls.find(c => c[0].name === 'Card with Extra Tag');
      expect(questionCall).toBeDefined();
      const tags = questionCall[0].dataset_query.native['template-tags'];
      expect(tags).toHaveProperty('known_filter');
      expect(tags).toHaveProperty('unknown_standalone_tag');
      expect(tags.unknown_standalone_tag['display-name']).toBe('Unknown Standalone Tag');
    });

    test('CardsHandler.handleBuildFullDashboard executes end-to-end with complete structuredContent and audit logging', async () => {
      const mockClient = {
        createQuestion: jest.fn().mockImplementation((p) => Promise.resolve({ id: Math.floor(Math.random() * 100) + 1, name: p.name })),
        createDashboard: jest.fn().mockResolvedValue({ id: 777, name: 'Executive Sales Ops' }),
        updateDashboard: jest.fn().mockResolvedValue({ id: 777 }),
        addCardToDashboard: jest.fn().mockResolvedValue({ id: 501 }),
      };

      const mockActivityLogger = {
        logActivity: jest.fn().mockResolvedValue(true),
      };

      const handler = new CardsHandler(mockClient, mockActivityLogger);

      const buildArgs = {
        name: 'Executive Sales Ops',
        database_id: 1,
        collection_id: 10,
        theme: 'executive',
        cards: [
          { name: 'Total Revenue', display: 'scalar', sql: 'SELECT sum(amount) FROM orders WHERE {{period}}' },
          { name: 'Order Count', display: 'number', sql: 'SELECT count(*) FROM orders WHERE {{period}}' },
          { name: 'Revenue Trend', display: 'line', sql: 'SELECT date_trunc(\'month\', created_at), sum(amount) FROM orders GROUP BY 1' },
          { name: 'Customer Accounts', display: 'table', sql: 'SELECT * FROM users LIMIT 50' },
        ],
        filters: [
          { name: 'Period', slug: 'period', type: 'date/all-options', target_variable: 'period' },
        ],
      };

      const res = await handler.handleBuildFullDashboard(buildArgs);

      expect(res.content).toBeDefined();
      expect(res.content[0].text).toContain('Autonomous Dashboard Built Successfully!');
      expect(res.content[0].text).toContain('Executive Sales Ops (ID: 777)');
      expect(res.content[0].text).toContain('Cards Created:** 4');
      expect(res.content[0].text).toContain('Filters Configured:** 1');

      expect(res.structuredContent).toBeDefined();
      expect(res.structuredContent.dashboard_id).toBe(777);
      expect(res.structuredContent.cards).toHaveLength(4);
      expect(res.structuredContent._provenance.tool).toBe('ai_dashboard_build_full');
      expect(res.structuredContent._provenance.generation_parameters.card_count).toBe(4);
    });
  });
});
