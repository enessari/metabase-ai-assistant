import { jest } from '@jest/globals';
import {
  SqlHealingEngine,
  executeAndHealSQL,
  classifySQLError,
  extractErrorIdentifiers,
  extractTablesFromSQL,
  analyzeSelectClause,
  repairSQLError,
  levenshteinDistance,
  stringSimilarity,
  findBestMatch,
  ERROR_CATEGORIES,
  SYNTAX_TYPO_MAP,
  COLUMN_SYNONYM_MAP,
  TABLE_PLURAL_MAP,
} from '../../src/ai/sql-healing-engine.js';

describe('SqlHealingEngine Unit Test Suite (M2)', () => {

  // ─────────────────────────────────────────────────────────────
  // 1. ERROR CLASSIFICATION
  // ─────────────────────────────────────────────────────────────
  describe('1. classifySQLError', () => {
    test('classifies SYNTAX_ERROR variants across multiple dialects', () => {
      const syntaxErrors = [
        'syntax error at or near "FORM"',
        'unexpected token at position 12',
        'mismatched parenthesis in query expression',
        'PARSE_ERROR: line 1:15 mismatched input',
        'You have an error in your SQL syntax near "LIMT 10"',
        'near "FORM": syntax error',
        'unexpected end of statement',
        'unrecognized token in expression',
      ];

      for (const err of syntaxErrors) {
        expect(classifySQLError(err)).toBe(ERROR_CATEGORIES.SYNTAX_ERROR);
      }
    });

    test('classifies MISSING_COLUMN variants across multiple dialects', () => {
      const colErrors = [
        'column "user_mail" does not exist',
        "Unknown column 'user_mail' in 'field list'",
        'column not found: created_date',
        "invalid identifier 'user_email'",
        'no such column: amount',
        "cannot find column 'price'",
        'Attribute "user_id" not found',
        'unrecognized name: customer_id',
        'Error: column does not exist on table users',
      ];

      for (const err of colErrors) {
        expect(classifySQLError(err)).toBe(ERROR_CATEGORIES.MISSING_COLUMN);
      }
    });

    test('classifies INVALID_TABLE variants across multiple dialects', () => {
      const tableErrors = [
        'relation "orders_archive" does not exist',
        "Table 'ecommerce.order' doesn't exist",
        'no such table: customer',
        'table not found: user_accounts',
        'table or view `sales_metrics` not found',
        'cannot find table "public.orders"',
        "Object 'products_v2' does not exist",
      ];

      for (const err of tableErrors) {
        expect(classifySQLError(err)).toBe(ERROR_CATEGORIES.INVALID_TABLE);
      }
    });

    test('classifies AMBIGUOUS_COLUMN variants', () => {
      const ambigErrors = [
        'column reference "id" is ambiguous',
        "Column 'id' in field list is ambiguous",
        "Column 'created_at' in where clause is ambiguous",
        'ambiguous column name: id',
      ];

      for (const err of ambigErrors) {
        expect(classifySQLError(err)).toBe(ERROR_CATEGORIES.AMBIGUOUS_COLUMN);
      }
    });

    test('classifies GROUP_BY_VIOLATION variants', () => {
      const groupErrors = [
        'column "orders.status" must appear in the GROUP BY clause or be used in an aggregate function',
        "Expression 'orders.category' is not in GROUP BY clause",
        'which is not functionally dependent on columns in GROUP BY clause',
        'Expression not in GROUP BY key: metrics.created_at',
        'aggregate_without_group_by in query',
      ];

      for (const err of groupErrors) {
        expect(classifySQLError(err)).toBe(ERROR_CATEGORIES.GROUP_BY_VIOLATION);
      }
    });

    test('classifies TYPE_MISMATCH variants', () => {
      const typeErrors = [
        'operator does not exist: integer = character varying',
        'cannot cast type timestamp to integer',
        'types boolean and integer are not comparable',
        'Conversion failed when converting the varchar value to data type int',
        'type mismatch in WHERE predicate',
        'incompatible types in expression',
      ];

      for (const err of typeErrors) {
        expect(classifySQLError(err)).toBe(ERROR_CATEGORIES.TYPE_MISMATCH);
      }
    });

    test('classifies DIVIDE_BY_ZERO variants', () => {
      const divErrors = [
        'division by zero',
        'division by 0',
        'divide by zero error encountered',
        'division_by_zero in expression',
      ];

      for (const err of divErrors) {
        expect(classifySQLError(err)).toBe(ERROR_CATEGORIES.DIVIDE_BY_ZERO);
      }
    });

    test('handles unknown error types, null, undefined, and non-string inputs gracefully', () => {
      expect(classifySQLError(null)).toBe(ERROR_CATEGORIES.UNKNOWN);
      expect(classifySQLError(undefined)).toBe(ERROR_CATEGORIES.UNKNOWN);
      expect(classifySQLError('A completely unclassified network timeout')).toBe(ERROR_CATEGORIES.UNKNOWN);
      expect(classifySQLError(new Error('Connection reset by peer'))).toBe(ERROR_CATEGORIES.UNKNOWN);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 2. FUZZY MATCHING & LEVENSHTEIN DISTANCE
  // ─────────────────────────────────────────────────────────────
  describe('2. Levenshtein Distance & Fuzzy Match Algorithms', () => {
    test('levenshteinDistance computes exact edit distance', () => {
      expect(levenshteinDistance('email', 'email')).toBe(0);
      expect(levenshteinDistance('user_mail', 'user_email')).toBe(1);
      expect(levenshteinDistance('orders', 'order')).toBe(1);
      expect(levenshteinDistance('created_time', 'created_at')).toBe(4);
      expect(levenshteinDistance('', 'test')).toBe(4);
      expect(levenshteinDistance(null, 'test')).toBe(4);
    });

    test('stringSimilarity returns 0.0 to 1.0 similarity score', () => {
      expect(stringSimilarity('email', 'email')).toBe(1.0);
      expect(stringSimilarity('user_email', 'user_mail')).toBeGreaterThan(0.7);
      expect(stringSimilarity('totally_different', 'something_else')).toBeLessThan(0.4);
      expect(stringSimilarity('', '')).toBe(1.0);
      expect(stringSimilarity(null, 'test')).toBe(0.0);
    });

    test('findBestMatch identifies closest column or table from candidates', () => {
      const columns = ['id', 'email', 'created_at', 'total_amount', 'user_id', 'status'];

      expect(findBestMatch('user_mail', columns, 0.4)).toBe('email');
      expect(findBestMatch('created_date', columns, 0.4)).toBe('created_at');
      expect(findBestMatch('amount', columns, 0.4)).toBe('total_amount');

      // Candidate objects
      const tableObjects = [
        { name: 'orders', schema: 'public' },
        { name: 'users', schema: 'public' },
        { name: 'products', schema: 'public' },
      ];
      expect(findBestMatch('order', tableObjects, 0.5)).toBe('orders');
      expect(findBestMatch('user', tableObjects, 0.5)).toBe('users');
      expect(findBestMatch('xyz123', tableObjects, 0.8)).toBeNull();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 3. ERROR IDENTIFIER & CLAUSE EXTRACTION
  // ─────────────────────────────────────────────────────────────
  describe('3. Error Identifier and Clause Extractors', () => {
    test('extractErrorIdentifiers extracts column and table names correctly', () => {
      expect(
        extractErrorIdentifiers('column "user_mail" does not exist', ERROR_CATEGORIES.MISSING_COLUMN)
      ).toEqual({ columnName: 'user_mail' });

      expect(
        extractErrorIdentifiers("Table 'ecommerce.orders_old' doesn't exist", ERROR_CATEGORIES.INVALID_TABLE)
      ).toEqual({ tableName: 'ecommerce.orders_old' });

      expect(
        extractErrorIdentifiers('column "status" must appear in the GROUP BY clause', ERROR_CATEGORIES.GROUP_BY_VIOLATION)
      ).toEqual({ columnName: 'status' });
    });

    test('extractTablesFromSQL extracts table names from simple and joined queries', () => {
      expect(extractTablesFromSQL('SELECT * FROM users WHERE active = true')).toEqual(['users']);
      expect(
        extractTablesFromSQL('SELECT o.id, u.email FROM orders o JOIN users u ON o.user_id = u.id')
      ).toEqual(['orders', 'users']);
      expect(
        extractTablesFromSQL('SELECT * FROM public."order_items" INNER JOIN public.products ON 1=1')
      ).toEqual(['order_items', 'products']);
    });

    test('analyzeSelectClause extracts columns, detects aggregates, and unaggregated columns', () => {
      const analysis1 = analyzeSelectClause('SELECT category, status, COUNT(*), SUM(amount) FROM orders GROUP BY category');
      expect(analysis1.hasAggregates).toBe(true);
      expect(analysis1.nonAggregatedColumns).toContain('category');
      expect(analysis1.nonAggregatedColumns).toContain('status');

      const analysis2 = analyzeSelectClause('SELECT id, name, email FROM users');
      expect(analysis2.hasAggregates).toBe(false);
      expect(analysis2.selectColumns).toEqual(['id', 'name', 'email']);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 4. DETERMINISTIC RULE-BASED REPAIRS
  // ─────────────────────────────────────────────────────────────
  describe('4. Deterministic Rule-Based SQL Repairs', () => {
    test('repairs single-token and multi-token syntax typos', async () => {
      const sqlWithTypos = 'SELEC id, name FORM users WHER status = 1 GROP BY status OERDER BY id HAVNG id > 0 LIMT 10;';
      const res = await repairSQLError({
        sql: sqlWithTypos,
        errorCategory: ERROR_CATEGORIES.SYNTAX_ERROR,
        errorMessage: 'syntax error at or near "SELEC"',
      });

      expect(res.repairedSql).toBe('SELECT id, name FROM users WHERE status = 1 GROUP BY status ORDER BY id HAVING id > 0 LIMIT 10;');
      expect(res.ruleApplied).toBe(true);
      expect(res.diagnosis).toContain('SELEC');
      expect(res.diagnosis).toContain('FORM');
      expect(res.diagnosis).toContain('WHER');
    });

    test('repairs dangling commas before FROM, WHERE, ORDER BY, LIMIT, and parentheses', async () => {
      const sqlWithCommas = 'SELECT id, email, FROM users WHERE active = true, ORDER BY id, LIMIT 5;';
      const res = await repairSQLError({
        sql: sqlWithCommas,
        errorCategory: ERROR_CATEGORIES.SYNTAX_ERROR,
        errorMessage: 'syntax error at or near "FROM"',
      });

      expect(res.repairedSql).not.toContain(', FROM');
      expect(res.repairedSql).not.toContain(', ORDER');
      expect(res.repairedSql).not.toContain(', LIMIT');
      expect(res.ruleApplied).toBe(true);
    });

    test('repairs unclosed parenthesis', async () => {
      const unclosedSql = 'SELECT COUNT(id FROM users;';
      const res = await repairSQLError({
        sql: unclosedSql,
        errorCategory: ERROR_CATEGORIES.SYNTAX_ERROR,
        errorMessage: 'mismatched parenthesis in query',
      });

      expect(res.repairedSql).toBe('SELECT COUNT(id FROM users);');
      expect(res.ruleApplied).toBe(true);
    });

    test('repairs missing column using synonym mapping', async () => {
      const sql = 'SELECT id, user_mail, created_time FROM users;';
      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.MISSING_COLUMN,
        errorMessage: 'column "user_mail" does not exist',
      });

      expect(res.repairedSql).toBe('SELECT id, email, created_time FROM users;');
      expect(res.ruleApplied).toBe(true);
      expect(res.diagnosis).toContain("Replaced missing column 'user_mail' with 'email'");
    });

    test('repairs missing column using catalog fuzzy matching', async () => {
      const sql = 'SELECT cust_name, amnt FROM orders;';
      const catalog = {
        tables: [
          {
            name: 'orders',
            fields: [{ name: 'id' }, { name: 'customer_name' }, { name: 'total_amount' }],
          },
        ],
      };

      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.MISSING_COLUMN,
        errorMessage: 'column "cust_name" does not exist',
        catalog,
      });

      expect(res.repairedSql).toContain('customer_name');
      expect(res.ruleApplied).toBe(true);
    });

    test('repairs invalid table singular to plural', async () => {
      const sql = 'SELECT * FROM order WHERE id = 1;';
      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.INVALID_TABLE,
        errorMessage: 'relation "order" does not exist',
      });

      expect(res.repairedSql).toBe('SELECT * FROM orders WHERE id = 1;');
      expect(res.ruleApplied).toBe(true);
    });

    test('repairs invalid table with schema prefix from catalog', async () => {
      const sql = 'SELECT * FROM audit_logs WHERE id = 1;';
      const catalog = [
        { name: 'audit_logs', schema: 'analytics' },
        { name: 'users', schema: 'public' },
      ];

      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.INVALID_TABLE,
        errorMessage: 'relation "audit_logs" does not exist',
        catalog,
      });

      expect(res.repairedSql).toBe('SELECT * FROM analytics.audit_logs WHERE id = 1;');
      expect(res.ruleApplied).toBe(true);
    });

    test('repairs division-by-zero with NULLIF wrapper', async () => {
      const sql = 'SELECT category, revenue / total_orders AS avg_val FROM metrics;';
      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.DIVIDE_BY_ZERO,
        errorMessage: 'division by zero',
      });

      expect(res.repairedSql).toContain('revenue / NULLIF(total_orders, 0) AS avg_val');
      expect(res.ruleApplied).toBe(true);
    });

    test('repairs GROUP BY violation by adding unaggregated select columns', async () => {
      const sql = 'SELECT category, status, COUNT(*) AS cnt FROM orders;';
      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.GROUP_BY_VIOLATION,
        errorMessage: 'column "orders.status" must appear in the GROUP BY clause or be used in an aggregate function',
      });

      expect(res.repairedSql).toContain('GROUP BY');
      expect(res.repairedSql).toContain('category');
      expect(res.repairedSql).toContain('status');
      expect(res.ruleApplied).toBe(true);
    });

    test('repairs ambiguous column by qualifying with primary table prefix', async () => {
      const sql = 'SELECT id, email FROM orders o JOIN users u ON o.user_id = u.id;';
      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.AMBIGUOUS_COLUMN,
        errorMessage: 'column reference "id" is ambiguous',
      });

      expect(res.repairedSql).toContain('o.id');
      expect(res.ruleApplied).toBe(true);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 5. LLM-ASSISTED FALLBACK REPAIR
  // ─────────────────────────────────────────────────────────────
  describe('5. LLM-Assisted Fallback Repair', () => {
    test('invokes assistant when deterministic fix produces no change or for UNKNOWN category', async () => {
      const mockAssistant = {
        aiProvider: 'anthropic',
        model: 'claude-3-sonnet-20240229',
        getAIResponse: jest.fn().mockResolvedValue('```sql\nSELECT DATE_TRUNC(\'month\', created_at) AS m, SUM(total) FROM orders GROUP BY 1;\n```'),
      };

      const sql = 'SELECT STRFTIME(created_at) AS m, SUM(total) FROM orders;';
      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.UNKNOWN,
        errorMessage: 'Function STRFTIME does not exist',
        assistant: mockAssistant,
      });

      expect(mockAssistant.getAIResponse).toHaveBeenCalled();
      expect(res.repairedSql).toBe("SELECT DATE_TRUNC('month', created_at) AS m, SUM(total) FROM orders GROUP BY 1;");
      expect(res.ruleApplied).toBe(true);
      expect(res.diagnosis).toContain('AI assistant generated corrected SQL');
    });

    test('handles LLM failures gracefully without throwing', async () => {
      const mockAssistant = {
        getAIResponse: jest.fn().mockRejectedValue(new Error('Anthropic API connection timeout')),
      };

      const sql = 'SELECT unknown_func() FROM t;';
      const res = await repairSQLError({
        sql,
        errorCategory: ERROR_CATEGORIES.UNKNOWN,
        errorMessage: 'unknown_func not found',
        assistant: mockAssistant,
      });

      expect(res.repairedSql).toBe(sql);
      expect(res.diagnosis).toContain('LLM fallback failed');
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 6. SqlHealingEngine CLASS CONSTRUCTOR & INTERFACES
  // ─────────────────────────────────────────────────────────────
  describe('6. SqlHealingEngine Class', () => {
    test('instantiates and provides classifyError and repairError methods', async () => {
      const engine = new SqlHealingEngine({ maxAttempts: 3, dialect: 'postgres' });
      expect(engine.classifyError('column "foo" does not exist')).toBe(ERROR_CATEGORIES.MISSING_COLUMN);

      const res = await engine.repairError({
        sql: 'SELECT * FORM users;',
        errorCategory: ERROR_CATEGORIES.SYNTAX_ERROR,
        errorMessage: 'syntax error near FORM',
      });
      expect(res.repairedSql).toBe('SELECT * FROM users;');
    });
  });
});
