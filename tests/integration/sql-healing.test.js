import { jest } from '@jest/globals';
import { executeAndHealSQL, SqlHealingEngine } from '../../src/ai/sql-healing-engine.js';
import { SqlHandler } from '../../src/mcp/handlers/sql.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';

describe('Autonomous Self-Healing SQL Engine Integration Test Suite (M2)', () => {
  const originalEnv = { ...process.env };

  afterEach(() => {
    process.env = { ...originalEnv };
  });

  // ─────────────────────────────────────────────────────────────
  // 1. ITERATIVE EXECUTE AND HEAL LOOP
  // ─────────────────────────────────────────────────────────────
  describe('1. executeAndHealSQL End-to-End Iterative Engine', () => {
    test('succeeds on first attempt without healing if query is valid', async () => {
      const mockClient = {
        executeNativeQuery: jest.fn().mockResolvedValue({
          data: {
            cols: [{ name: 'id', base_type: 'type/Integer' }, { name: 'email', base_type: 'type/Text' }],
            rows: [[1, 'alice@example.com'], [2, 'bob@example.com']],
          },
        }),
      };

      const result = await executeAndHealSQL({
        databaseId: 1,
        sql: 'SELECT id, email FROM users LIMIT 10;',
        maxAttempts: 3,
        client: mockClient,
      });

      expect(result.success).toBe(true);
      expect(result.attempts_used).toBe(1);
      expect(result.healed).toBe(false);
      expect(result.healing_trail).toEqual([]);
      expect(result.final_sql).toBe('SELECT id, email FROM users LIMIT 10;');
      expect(result.data.row_count).toBe(2);
      expect(mockClient.executeNativeQuery).toHaveBeenCalledTimes(1);

      // Verify PII masking on emails
      expect(result.data.rows[0][1]).toBe('a***e@example.com');
    });

    test('recovers from deliberate syntax typo on attempt 2', async () => {
      let callCount = 0;
      const mockClient = {
        executeNativeQuery: jest.fn().mockImplementation(async (dbId, sql) => {
          callCount++;
          if (callCount === 1) {
            expect(sql).toContain('FORM');
            throw new Error('syntax error at or near "FORM"');
          }
          expect(sql).toContain('FROM');
          return {
            data: {
              cols: [{ name: 'total', base_type: 'type/Decimal' }],
              rows: [[1500.50]],
            },
          };
        }),
      };

      const result = await executeAndHealSQL({
        databaseId: 1,
        sql: 'SELECT total FORM orders LIMIT 1;',
        maxAttempts: 3,
        client: mockClient,
      });

      expect(result.success).toBe(true);
      expect(result.attempts_used).toBe(2);
      expect(result.healed).toBe(true);
      expect(result.healing_trail.length).toBe(1);
      expect(result.healing_trail[0]).toEqual({
        attempt: 1,
        failed_sql: 'SELECT total FORM orders LIMIT 1;',
        error_message: 'syntax error at or near "FORM"',
        error_category: 'SYNTAX_ERROR',
        diagnosis: expect.stringContaining("Fixed typo 'FORM' -> 'FROM'"),
        corrected_sql: 'SELECT total FROM orders LIMIT 1;',
        timestamp: expect.any(String),
      });
      expect(result.final_sql).toBe('SELECT total FROM orders LIMIT 1;');
    });

    test('recovers from multiple cascading errors (Syntax -> Column Typo -> Success)', async () => {
      let callCount = 0;
      const mockClient = {
        executeNativeQuery: jest.fn().mockImplementation(async (dbId, sql) => {
          callCount++;
          if (callCount === 1) {
            throw new Error('syntax error at or near "SELEC"');
          }
          if (callCount === 2) {
            throw new Error('column "user_mail" does not exist');
          }
          return {
            data: {
              cols: [{ name: 'email', base_type: 'type/Text' }],
              rows: [['test@example.com']],
            },
          };
        }),
      };

      const result = await executeAndHealSQL({
        databaseId: 1,
        sql: 'SELEC user_mail FROM users;',
        maxAttempts: 3,
        client: mockClient,
      });

      expect(result.success).toBe(true);
      expect(result.attempts_used).toBe(3);
      expect(result.healed).toBe(true);
      expect(result.healing_trail.length).toBe(2);

      // Attempt 1 fixed SELEC -> SELECT
      expect(result.healing_trail[0].error_category).toBe('SYNTAX_ERROR');
      expect(result.healing_trail[0].corrected_sql).toBe('SELECT user_mail FROM users;');

      // Attempt 2 fixed user_mail -> email
      expect(result.healing_trail[1].error_category).toBe('MISSING_COLUMN');
      expect(result.healing_trail[1].corrected_sql).toBe('SELECT email FROM users;');

      expect(result.final_sql).toBe('SELECT email FROM users;');
    });

    test('reaches max attempts cutoff gracefully when query cannot be repaired', async () => {
      const mockClient = {
        executeNativeQuery: jest.fn().mockRejectedValue(new Error('Fatal non-recoverable database corrupt engine error')),
      };

      const result = await executeAndHealSQL({
        databaseId: 1,
        sql: 'SELECT * FROM unknown_table;',
        maxAttempts: 3,
        client: mockClient,
      });

      expect(result.success).toBe(false);
      expect(result.attempts_used).toBe(3);
      expect(result.healing_trail.length).toBe(3);
      expect(result.error).toContain('Fatal non-recoverable');
      expect(result._provenance.generation_parameters.healed).toBe(false);
    });

    test('recovers via AI assistant when deterministic fix produces no change', async () => {
      let callCount = 0;
      const mockAssistant = {
        aiProvider: 'anthropic',
        model: 'claude-3-sonnet-20240229',
        getAIResponse: jest.fn().mockResolvedValue('SELECT DATE_TRUNC(\'day\', created_at), COUNT(*) FROM orders GROUP BY 1;'),
      };

      const mockClient = {
        executeNativeQuery: jest.fn().mockImplementation(async (dbId, sql) => {
          callCount++;
          if (callCount === 1) {
            throw new Error('dialect function strftime_not_supported()');
          }
          return {
            data: {
              cols: [{ name: 'day' }, { name: 'count' }],
              rows: [['2026-08-31', 50]],
            },
          };
        }),
      };

      const result = await executeAndHealSQL({
        databaseId: 1,
        sql: 'SELECT strftime_not_supported(created_at), COUNT(*) FROM orders;',
        maxAttempts: 2,
        client: mockClient,
        assistant: mockAssistant,
      });

      expect(result.success).toBe(true);
      expect(result.attempts_used).toBe(2);
      expect(mockAssistant.getAIResponse).toHaveBeenCalled();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 2. SqlHandler.handleExecuteAndHealSQL INTEGRATION
  // ─────────────────────────────────────────────────────────────
  describe('2. SqlHandler.handleExecuteAndHealSQL Handler', () => {
    let mockClient;
    let mockActivityLogger;

    beforeEach(() => {
      mockClient = {
        executeNativeQuery: jest.fn().mockResolvedValue({
          data: {
            cols: [{ name: 'id', base_type: 'type/Integer' }, { name: 'email', base_type: 'type/Text' }],
            rows: [[101, 'john.doe@company.org']],
          },
        }),
      };

      mockActivityLogger = {
        logActivity: jest.fn().mockResolvedValue(true),
      };
    });

    test('validates required database_id and sql parameters', async () => {
      const handler = new SqlHandler(mockClient);

      await expect(handler.handleExecuteAndHealSQL({ sql: 'SELECT 1;' })).rejects.toThrow('database_id is required');
      await expect(handler.handleExecuteAndHealSQL({ database_id: 1 })).rejects.toThrow('sql is required');
    });

    test('blocks DML operations in read-only mode with formatted security warning', async () => {
      process.env.METABASE_READ_ONLY_MODE = 'true';
      const handler = new SqlHandler(mockClient);

      const res = await handler.handleExecuteAndHealSQL({
        database_id: 1,
        sql: 'DELETE FROM users WHERE id = 1;',
      });

      expect(res.content[0].text).toContain('🔒 **Read-Only Mode Active**');
      expect(res.content[0].text).toContain('⛔ **Operation Blocked:** `DELETE`');
      expect(mockClient.executeNativeQuery).not.toHaveBeenCalled();
    });

    test('formats clean markdown table and healing summary table when healed', async () => {
      let callCount = 0;
      mockClient.executeNativeQuery.mockImplementation(async (dbId, sql) => {
        callCount++;
        if (callCount === 1) {
          throw new Error('column "user_mail" does not exist');
        }
        return {
          data: {
            cols: [{ name: 'email', base_type: 'type/Text' }],
            rows: [['admin@enterprise.com']],
          },
        };
      });

      const handler = new SqlHandler(mockClient, null, mockActivityLogger);
      const res = await handler.handleExecuteAndHealSQL({
        database_id: 1,
        sql: 'SELECT user_mail FROM users;',
      });

      expect(res.content[0].text).toContain('✅ **Query healed & executed successfully** (2 attempts used');
      expect(res.content[0].text).toContain('🛠️ **Autonomous Healing Summary:**');
      expect(res.content[0].text).toContain('| 1 | `MISSING_COLUMN` |');
      expect(res.content[0].text).toContain('SELECT email FROM users;');

      expect(res.structuredContent).toBeDefined();
      expect(res.structuredContent.success).toBe(true);
      expect(res.structuredContent.healed).toBe(true);
      expect(res.structuredContent.attempts_used).toBe(2);
      expect(res.structuredContent._provenance.healing_trail.length).toBe(1);

      // Verify activity logging
      expect(mockActivityLogger.logActivity).toHaveBeenCalledWith(
        expect.objectContaining({
          operation_type: 'ai_sql_execute_and_heal',
          status: 'success',
        })
      );
    });

    test('returns failure details cleanly when healing fails', async () => {
      mockClient.executeNativeQuery.mockRejectedValue(new Error('Fatal syntax unrecoverable error'));

      const handler = new SqlHandler(mockClient, null, mockActivityLogger);
      const res = await handler.handleExecuteAndHealSQL({
        database_id: 1,
        sql: 'SELECT * FROM nowhere;',
        max_attempts: 2,
      });

      expect(res.content[0].text).toContain('❌ **Query execution & healing failed** after 2 attempts');
      expect(res.structuredContent.success).toBe(false);
      expect(res.structuredContent.healed).toBe(false);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // 3. TOOL REGISTRY & ANNOTATIONS INTEGRATION
  // ─────────────────────────────────────────────────────────────
  describe('3. Tool Registry ai_sql_execute_and_heal Metadata', () => {
    test('ai_sql_execute_and_heal is registered with complete schema and provenance', () => {
      const tools = getToolDefinitions();
      const healTool = tools.find(t => t.name === 'ai_sql_execute_and_heal');

      expect(healTool).toBeDefined();
      expect(healTool.description).toContain('autonomous self-healing');
      expect(healTool.inputSchema.required).toEqual(['database_id', 'sql']);
      expect(healTool.annotations.readOnlyHint).toBe(true);
      expect(healTool.outputSchema).toBeDefined();
      expect(healTool.outputSchema.properties._provenance).toBeDefined();
    });
  });
});
