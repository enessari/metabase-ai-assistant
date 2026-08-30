import { isReadOnlyMode, WRITE_TOOLS } from '../src/mcp/tool-router.js';
import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';

describe('Read-Only Mode Enforcement Tests', () => {
  const originalEnv = process.env.METABASE_READ_ONLY_MODE;

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env.METABASE_READ_ONLY_MODE = originalEnv;
    } else {
      delete process.env.METABASE_READ_ONLY_MODE;
    }
  });

  describe('isReadOnlyMode helper', () => {
    test('defaults to true when METABASE_READ_ONLY_MODE is unset', () => {
      delete process.env.METABASE_READ_ONLY_MODE;
      expect(isReadOnlyMode()).toBe(true);
    });

    test('returns true when METABASE_READ_ONLY_MODE=true', () => {
      process.env.METABASE_READ_ONLY_MODE = 'true';
      expect(isReadOnlyMode()).toBe(true);
    });

    test('returns false only when METABASE_READ_ONLY_MODE=false', () => {
      process.env.METABASE_READ_ONLY_MODE = 'false';
      expect(isReadOnlyMode()).toBe(false);
    });
  });

  describe('WRITE_TOOLS registry', () => {
    test('contains essential write operations', () => {
      const essentialWriteTools = [
        'sql_execute',
        'sql_submit',
        'sql_cancel',
        'db_table_create',
        'db_view_create',
        'db_matview_create',
        'db_index_create',
        'db_ai_drop',
        'mb_question_create',
        'mb_card_update',
        'mb_card_delete',
        'mb_card_archive',
        'mb_dashboard_create',
        'mb_dashboard_update',
        'mb_dashboard_delete',
        'mb_collection_create',
        'mb_collection_move',
        'mb_user_create',
        'mb_user_update',
        'mb_user_disable',
        'mb_auto_describe',
      ];

      for (const tool of essentialWriteTools) {
        expect(WRITE_TOOLS.has(tool)).toBe(true);
      }
    });

    test('does not contain read-only exploration tools', () => {
      const readOnlyTools = [
        'db_list',
        'db_schemas',
        'db_tables',
        'db_test_speed',
        'mb_dashboards',
        'mb_dashboard_get',
        'mb_questions',
        'mb_card_get',
        'mb_collection_list',
        'mb_user_list',
        'ai_sql_generate',
        'ai_sql_optimize',
        'ai_sql_explain',
        'ai_relationships_suggest',
      ];

      for (const tool of readOnlyTools) {
        expect(WRITE_TOOLS.has(tool)).toBe(false);
      }
    });
  });

  describe('Server Read-Only Gating Logic', () => {
    test('write tool throws McpError(InvalidRequest) when read-only mode is active', () => {
      process.env.METABASE_READ_ONLY_MODE = 'true';

      const writeToolName = 'mb_card_delete';
      expect(isReadOnlyMode()).toBe(true);
      expect(WRITE_TOOLS.has(writeToolName)).toBe(true);

      const checkReadOnlyGate = (toolName) => {
        if (isReadOnlyMode() && WRITE_TOOLS.has(toolName)) {
          throw new McpError(
            ErrorCode.InvalidRequest,
            `🔒 Read-only mode is active. The tool '${toolName}' is a write operation and has been blocked.`
          );
        }
      };

      expect(() => checkReadOnlyGate(writeToolName)).toThrow(McpError);
      try {
        checkReadOnlyGate(writeToolName);
      } catch (err) {
        expect(err.code).toBe(ErrorCode.InvalidRequest);
        expect(err.message).toContain('Read-only mode is active');
      }
    });

    test('read-only tool passes gate without error', () => {
      process.env.METABASE_READ_ONLY_MODE = 'true';
      const readToolName = 'db_list';

      const checkReadOnlyGate = (toolName) => {
        if (isReadOnlyMode() && WRITE_TOOLS.has(toolName)) {
          throw new McpError(
            ErrorCode.InvalidRequest,
            `🔒 Read-only mode is active.`
          );
        }
      };

      expect(() => checkReadOnlyGate(readToolName)).not.toThrow();
    });
  });
});
