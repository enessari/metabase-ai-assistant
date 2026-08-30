import { logger } from '../../utils/logger.js';
import { CacheKeys, globalCache } from '../../utils/cache.js';
import { getJobStore } from '../job-store.js';
import {
  ResponseFormat,
  formatListResponse,
  minimalDatabase,
} from '../../utils/response-optimizer.js';
import { isReadOnlyMode, detectWriteOperation } from './database.js';
import { BaseHandler } from './base.js';
import { maskRow, isPiiMaskingEnabled } from '../../utils/pii-masker.js';
import { executeAndHealSQL } from '../../ai/sql-healing-engine.js';

/**
 * Handler for SQL Execution Operations
 */
export class SqlHandler extends BaseHandler {
  constructor(metabaseClient, cache, activityLogger, aiAssistant) {
    if (metabaseClient && typeof metabaseClient === 'object' && metabaseClient.metabaseClient) {
      super(metabaseClient);
      this.cache = metabaseClient.cache || globalCache;
    } else {
      super({ metabaseClient, activityLogger, aiAssistant, cache });
      this.cache = cache || globalCache;
    }
    this.activeJobs = new Map();
    this.jobCounter = 0;
  }

  routes() {
    return {
      'sql_execute': (args) => this.handleExecuteSQL(args),
      'sql_submit': (args) => this.handleSQLSubmit(args),
      'sql_status': (args) => this.handleSQLStatus(args),
      'sql_cancel': (args) => this.handleSQLCancel(args),
      'ai_sql_execute_and_heal': (args) => this.handleExecuteAndHealSQL(args),
    };
  }

  async handleExecuteSQL(args) {
    const databaseId = args.database_id;
    const sql = args.sql;
    const fullResults = args.full_results === true;

    // Read-Only Mode Security Check
    if (isReadOnlyMode()) {
      const blockedOperation = detectWriteOperation(sql);
      if (blockedOperation) {
        logger.warn(`Read-only mode: Blocked ${blockedOperation} operation`, { sql: sql.substring(0, 100) });

        return {
          content: [
            {
              type: 'text',
              text: `🔒 **Read-Only Mode Active**\n\n` +
                `⛔ **Operation Blocked:** \`${blockedOperation}\`\n\n` +
                `This MCP server is running in read-only mode for security.\n` +
                `Write operations (INSERT, UPDATE, DELETE, DROP, etc.) are not allowed.\n\n` +
                `To enable write operations, set \`METABASE_READ_ONLY_MODE=false\` in your environment.\n\n` +
                `🔍 **Attempted Query:**\n\`\`\`sql\n${sql.substring(0, 200)}${sql.length > 200 ? '...' : ''}\n\`\`\``,
            },
          ],
        };
      }
    }

    const startTime = Date.now();
    let result = null;
    let error = null;

    try {
      result = await this.metabaseClient.executeNativeQuery(databaseId, sql);
      const executionTime = Date.now() - startTime;

      // Log the activity
      if (this.activityLogger) {
        await this.activityLogger.logSQLExecution(sql, databaseId, result, executionTime);
      }

      // Format the result for display
      const rawRows = result.data?.rows || [];
      const columns = result.data?.cols || [];
      const columnNames = columns.map(col => (typeof col === 'object' && col !== null ? (col.name || col.display_name || '') : String(col)));

      const maskingEnabled = isPiiMaskingEnabled(args);
      const maskOptions = {
        strict: args.mask_strict === true,
        preserveDomain: args.preserve_domain !== false,
        pseudonymize: args.pseudonymize === true,
        salt: args.salt || 'metabase_ai_salt',
      };

      const rows = maskingEnabled ? rawRows.map(row => maskRow(row, columnNames, maskOptions)) : rawRows;

      let output = `✅ **Query successful** (${executionTime}ms)\n`;
      output += `📊 ${columns.length} columns, ${rows.length} rows\n\n`;

      if (rows.length > 0) {
        // Show sample data (max 5 rows)
        output += `**Data:**\n\`\`\`\n`;
        const headers = columns.map(col => col.name);
        output += headers.join(' | ') + '\n';
        output += headers.map(() => '---').join(' | ') + '\n';

        rows.slice(0, 5).forEach((row) => {
          const formattedRow = row.map(cell => {
            if (cell === null) return 'NULL';

            // Smart truncation logic
            let truncateLimit = 100; // Increased base limit from 30

            // Disable truncation for small result sets (DDL/procedures) or explicit full_results
            if (fullResults || rows.length <= 2) {
              truncateLimit = 50000;
            }
            // Check specific DDL-related column names
            else if (columns.some(c => /definition|ddl|source|create_statement|routine_definition/i.test(c.name))) {
              truncateLimit = 10000;
            }

            if (typeof cell === 'string' && cell.length > truncateLimit) {
              return cell.substring(0, truncateLimit - 3) + '...';
            }
            return String(cell);
          });
          output += formattedRow.join(' | ') + '\n';
        });
        output += '\`\`\`\n';

        if (rows.length > 5) {
          output += `_+${rows.length - 5} more rows_\n`;
        }

        // Large result warning
        if (rows.length > 100) {
          output += `\n⚠️ **Large result:** ${rows.length} rows returned. Use LIMIT for better performance.\n`;
        }
      } else {
        // Empty result - smart detection
        output += `ℹ️ No results.\n`;

        // Try to detect if table has data but query returned nothing
        try {
          const fromMatch = sql.match(/FROM\s+["']?([^\s"'.(]+)["']?/i) ||
            sql.match(/FROM\s+["']?[^"'.]+["']?\.["']?([^\s"']+)["']?/i);
          if (fromMatch) {
            const tableName = fromMatch[1];
            const countQuery = `SELECT COUNT(*) FROM ${tableName} LIMIT 1`;
            try {
              const countResult = await this.metabaseClient.executeNativeQuery(databaseId, countQuery);
              const tableRowCount = countResult.data?.rows?.[0]?.[0] || 0;

              if (tableRowCount > 0) {
                output += `\n⚠️ **Note:** \`${tableName}\` has ${tableRowCount.toLocaleString()} rows but query returned nothing.\n`;
                output += `Possible causes: WHERE clause too restrictive, column name typo, JOIN mismatch\n`;
                output += `💡 Use \`db_table_profile\` to inspect table structure.\n`;
              }
            } catch (e) { /* ignore */ }
          }
        } catch (e) { /* ignore */ }
      }

      // Tool suggestions (only for SELECT queries with few results)
      if (sql.toLowerCase().trim().startsWith('select') && rows.length <= 5) {
        output += `\n💡 Related: \`db_table_profile\`, \`mb_field_values\`\n`;
      }

      return {
        content: [
          {
            type: 'text',
            text: output,
          },
        ],
        structuredContent: {
          columns: columns.map(c => ({ name: c.name, base_type: c.base_type || c.type || 'unknown' })),
          rows: rows.slice(0, fullResults ? rows.length : 200),
          row_count: rows.length,
          execution_time_ms: executionTime,
          truncated: rows.length > 200 && !fullResults,
        },
      };

    } catch (err) {
      error = err;
      const executionTime = Date.now() - startTime;

      // Log the failed activity
      if (this.activityLogger) {
        await this.activityLogger.logActivity({
          operation_type: 'sql_execute',
          operation_category: 'query',
          database_id: databaseId,
          source_sql: sql,
          execution_time_ms: executionTime,
          status: 'error',
          error_message: err.message
        });
      }

      // Compact error format - no query repetition
      const shortSql = sql.length > 80 ? sql.substring(0, 77) + '...' : sql;
      const output = `❌ SQL Error: ${err.message}\nQuery: ${shortSql}`;

      return {
        content: [
          {
            type: 'text',
            text: output,
          },
        ],
      };
    }
  }

  /**
   * Submit a long-running SQL query asynchronously
   * Returns immediately with job_id, executes query in background
   */
  async handleSQLSubmit(args) {
    try {
      const databaseId = args.database_id;
      const sql = args.sql;
      const timeoutSeconds = Math.min(args.timeout_seconds || 300, 1800); // Max 30 minutes

      // Check read-only mode for write operations
      if (isReadOnlyMode() && detectWriteOperation(sql)) {
        return {
          content: [{ type: 'text', text: '❌ Write operations blocked in read-only mode' }],
        };
      }

      // Get job store and create job
      const jobStore = getJobStore();
      const job = jobStore.create(databaseId, sql, timeoutSeconds);

      // Add job marker to SQL for cancellation support
      const markedSql = `/* job:${job.id} */ ${sql}`;

      // Start query execution in background (non-blocking)
      this.executeQueryBackground(job.id, databaseId, markedSql, timeoutSeconds * 1000);

      const output = `✅ **Query Submitted**\n` +
        `📋 Job ID: \`${job.id}\`\n` +
        `⏱️ Timeout: ${timeoutSeconds} seconds\n` +
        `📊 Status: pending\n\n` +
        `💡 Use \`sql_status\` with this job_id to check progress.`;

      return {
        content: [{ type: 'text', text: output }],
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Failed to submit query: ${error.message}` }],
      };
    }
  }

  /**
   * Execute query in background and update job status
   */
  async executeQueryBackground(jobId, databaseId, sql, timeoutMs) {
    const jobStore = getJobStore();
    const job = jobStore.get(jobId);

    if (!job) return;

    jobStore.markRunning(jobId);

    try {
      const result = await this.metabaseClient.executeNativeQueryWithTimeout(
        databaseId,
        sql,
        timeoutMs,
        job.abortController.signal
      );

      const rows = result.data?.rows || [];
      jobStore.markComplete(jobId, result, rows.length);

      logger.info(`Query job ${jobId} completed with ${rows.length} rows`);

    } catch (error) {
      if (error.message.includes('cancelled')) {
        jobStore.markCancelled(jobId);
      } else if (error.message.includes('timed out')) {
        jobStore.markTimeout(jobId);
        // Try to cancel on database
        await this.metabaseClient.cancelPostgresQuery(databaseId, `job:${jobId}`);
      } else {
        jobStore.markFailed(jobId, error);
      }

      logger.error(`Query job ${jobId} failed: ${error.message}`);
    }
  }

  /**
   * Check status of an async query
   */
  async handleSQLStatus(args) {
    try {
      const jobStore = getJobStore();
      const job = jobStore.get(args.job_id);

      if (!job) {
        return {
          content: [{ type: 'text', text: `❌ Job not found: ${args.job_id}` }],
        };
      }

      const elapsedSeconds = jobStore.getElapsedSeconds(args.job_id);

      let output = `📋 **Job Status: ${job.id}**\n`;
      output += `📊 Status: ${job.status}\n`;
      output += `⏱️ Elapsed: ${elapsedSeconds} seconds\n`;

      let rows = [];

      if (job.status === 'running' || job.status === 'pending') {
        let waitSeconds = 3;
        if (elapsedSeconds > 60) waitSeconds = 30;
        else if (elapsedSeconds > 30) waitSeconds = 10;
        else if (elapsedSeconds > 10) waitSeconds = 5;

        output += `\n💡 Query is still running. Please wait **${waitSeconds} seconds** before checking again.\n`;
        output += `(Use \`sql_cancel\` to stop if needed)`;
      } else if (job.status === 'complete') {
        const rawRows = job.result?.data?.rows || [];
        const columns = job.result?.data?.cols || [];
        const columnNames = columns.map(col => (typeof col === 'object' && col !== null ? (col.name || col.display_name || '') : String(col)));

        const maskingEnabled = isPiiMaskingEnabled(args);
        const maskOptions = {
          strict: args.mask_strict === true,
          preserveDomain: args.preserve_domain !== false,
          pseudonymize: args.pseudonymize === true,
          salt: args.salt || 'metabase_ai_salt',
        };

        rows = maskingEnabled ? rawRows.map(row => maskRow(row, columnNames, maskOptions)) : rawRows;

        output += `✅ **Query Complete!**\n`;
        output += `📊 ${columns.length} columns, ${rows.length} rows\n\n`;

        if (rows.length > 0) {
          output += `**Data:**\n\`\`\`\n`;
          const headers = columns.map(col => col.name);
          output += headers.join(' | ') + '\n';
          output += headers.map(() => '---').join(' | ') + '\n';

          rows.slice(0, 5).forEach((row) => {
            const formattedRow = row.map(cell => {
              if (cell === null) return 'NULL';
              const str = String(cell);
              return str.length > 30 ? str.substring(0, 27) + '...' : str;
            });
            output += formattedRow.join(' | ') + '\n';
          });
          output += '\`\`\`\n';

          if (rows.length > 5) {
            output += `_+${rows.length - 5} more rows_\n`;
          }
        }
      } else if (job.status === 'failed' || job.status === 'timeout' || job.status === 'cancelled') {
        output += `\n❌ ${job.error || 'Query did not complete'}`;
      }

      return {
        content: [{ type: 'text', text: output }],
        structuredContent: {
          job_id: job.id,
          status: job.status,
          submitted_at: job.submittedAt || null,
          elapsed_seconds: elapsedSeconds,
          ...(job.status === 'complete' && job.result ? {
            result: {
              columns: (job.result.data?.cols || []).map(c => ({ name: c.name })),
              rows: rows.slice(0, 200),
              row_count: (job.result.data?.rows || []).length,
            }
          } : {}),
          ...(job.error ? { error: job.error } : {}),
        },
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Failed to check status: ${error.message}` }],
      };
    }
  }

  /**
   * Cancel a running async query
   */
  async handleSQLCancel(args) {
    try {
      const jobStore = getJobStore();
      const job = jobStore.get(args.job_id);

      if (!job) {
        return {
          content: [{ type: 'text', text: `❌ Job not found: ${args.job_id}` }],
        };
      }

      if (job.status !== 'running' && job.status !== 'pending') {
        return {
          content: [{ type: 'text', text: `ℹ️ Job is not running (status: ${job.status})` }],
        };
      }

      // Abort the HTTP request
      job.abortController.abort();

      // Try to cancel on database
      const dbCancelled = await this.metabaseClient.cancelPostgresQuery(
        job.database_id,
        `job:${job.id}`
      );

      jobStore.markCancelled(args.job_id);

      const output = `✅ **Query Cancelled**\n` +
        `📋 Job ID: ${args.job_id}\n` +
        `🗄️ Database cancel: ${dbCancelled ? 'sent' : 'not available'}`;

      return {
        content: [{ type: 'text', text: output }],
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Failed to cancel: ${error.message}` }],
      };
    }
  }


  async handleGetDatabases() {

    // Use cache for database list
    const cacheKey = CacheKeys.databases();
    const cached = await this.cache.getOrSet(cacheKey, async () => {
      const response = await this.metabaseClient.getDatabases();
      return response.data || response;
    });

    const databases = cached.data;
    const source = cached.source;

    // Log cache status
    if (source === 'cache') {
      logger.debug('Databases fetched from cache');
    }

    // Use response optimizer for compact output
    const optimizedResponse = formatListResponse(
      '📊 Available Databases',
      databases,
      minimalDatabase,
      { format: ResponseFormat.COMPACT }
    );

    // Structured content for all paths
    const structuredContent = {
      databases: databases.map(db => ({ id: db.id, name: db.name, engine: db.engine })),
      source: source,
    };

    // If optimization returned a result, use it; otherwise fall back to standard format
    if (optimizedResponse) {
      // Add cache indicator
      optimizedResponse.content[0].text += source === 'cache' ? '\n\n_📦 From cache_' : '';
      optimizedResponse.structuredContent = structuredContent;
      return optimizedResponse;
    }

    return {
      content: [
        {
          type: 'text',
          text: `Found ${databases.length} databases:\n${databases
            .map(db => `- ${db.name} (${db.engine}) - ID: ${db.id}`)
            .join('\n')}${source === 'cache' ? '\n\n_📦 From cache_' : ''}`,
        },
      ],
      structuredContent,
    };
  }


  async handleGetDatabaseSchemas(args) {

    const response = await this.metabaseClient.getDatabaseSchemas(args.database_id || args);

    const schemas = Array.isArray(response) ? response : (response.data || []);
    return {
      content: [
        {
          type: 'text',
          text: `Database Schemas:\n${JSON.stringify(schemas, null, 2)}`,
        },
      ],
      structuredContent: {
        database_id: args.database_id || args,
        schemas: schemas,
      },
    };
  }


  async handleGetDatabaseTables(args) {

    const response = await this.metabaseClient.getDatabaseTables(args.database_id || args);
    const tables = response.tables || response.data || response; // Handle multiple formats

    return {
      content: [
        {
          type: 'text',
          text: `Found ${tables.length} tables:\n${tables
            .map(table => `- ${table.name} (${table.fields?.length || 0} fields)`)
            .join('\n')}`,
        },
      ],
      structuredContent: {
        database_id: args.database_id || args,
        tables: tables.map(t => ({ id: t.id, name: t.name, schema: t.schema || 'public' })),
      },
    };
  }


  async handleGenerateSQL(args) {
    if (!this.aiAssistant) {
      throw new Error('AI assistant not configured. Please set ANTHROPIC_API_KEY or OPENAI_API_KEY.');
    }

    const { description, database_id } = args;
    const tables = await this.metabaseClient.getDatabaseTables(database_id);
    const sql = await this.aiAssistant.generateSQL(description, tables);

    return {
      content: [
        {
          type: 'text',
          text: `⚠️ **[AI-GENERATED SQL — REVIEW BEFORE EXECUTING]**\n\n` +
            `Generated SQL for: "${description}"\n\n\`\`\`sql\n${sql}\n\`\`\``,
        },
      ],
      structuredContent: {
        sql,
        description,
        database_id,
        _provenance: {
          ai_generated: true,
          tool: 'ai_sql_generate',
          review_required: true,
          timestamp: new Date().toISOString(),
          provider: this.aiAssistant?.aiProvider || 'anthropic',
          model: this.aiAssistant?.model || 'claude-3-sonnet-20240229',
          generation_parameters: {
            database_id,
            enforce_read_only: true,
          },
        },
      },
    };
  }

  async handleOptimizeQuery(args) {
    if (!this.aiAssistant) {
      throw new Error('AI assistant not configured. Please set ANTHROPIC_API_KEY or OPENAI_API_KEY.');
    }

    const optimization = await this.aiAssistant.optimizeQuery(args.sql);

    return {
      content: [
        {
          type: 'text',
          text: `⚠️ **[AI-GENERATED CONTENT — REVIEW BEFORE EXECUTING]**\n\n` +
            `Optimized SQL:\n\n\`\`\`sql\n${optimization.optimized_sql}\n\`\`\`\n\nOptimizations applied:\n${optimization.optimizations?.join('\n- ') || 'None'}\n\nExpected improvements:\n${optimization.improvements || 'Not specified'}`,
        },
      ],
      structuredContent: {
        original_sql: args.sql,
        optimized_sql: optimization.optimized_sql,
        optimizations: optimization.optimizations || [],
        improvements: optimization.improvements || null,
        _provenance: {
          ai_generated: true,
          tool: 'ai_sql_optimize',
          review_required: true,
          timestamp: new Date().toISOString(),
          provider: this.aiAssistant?.aiProvider || 'anthropic',
          model: this.aiAssistant?.model || 'claude-3-sonnet-20240229',
          generation_parameters: {
            enforce_read_only: true,
          },
        },
      },
    };
  }

  async handleExplainQuery(args) {
    if (!this.aiAssistant) {
      throw new Error('AI assistant not configured. Please set ANTHROPIC_API_KEY or OPENAI_API_KEY.');
    }

    const explanation = await this.aiAssistant.explainQuery(args.sql);

    return {
      content: [
        {
          type: 'text',
          text: `⚠️ **[AI-GENERATED CONTENT — REVIEW BEFORE EXECUTING]**\n\nQuery Explanation:\n\n${explanation}`,
        },
      ],
      structuredContent: {
        sql: args.sql,
        explanation,
        _provenance: {
          ai_generated: true,
          tool: 'ai_sql_explain',
          review_required: false,
          timestamp: new Date().toISOString(),
          provider: this.aiAssistant?.aiProvider || 'anthropic',
          model: this.aiAssistant?.model || 'claude-3-sonnet-20240229',
          generation_parameters: {
            enforce_read_only: true,
          },
        },
      },
    };
  }

  async handleTestConnectionSpeed(args) {
    const databaseId = args.database_id;
    const startTime = Date.now();

    try {
      await this.metabaseClient.executeNativeQuery(databaseId, 'SELECT 1');
      const responseTime = Date.now() - startTime;

      let speedLabel = 'Fast';
      if (responseTime > 5000) speedLabel = 'Slow';
      else if (responseTime > 2000) speedLabel = 'Moderate';
      else if (responseTime > 500) speedLabel = 'Good';

      return {
        content: [{
          type: 'text',
          text: `🏎️ **Database Speed Test**\n\n` +
            `📊 Database ID: ${databaseId}\n` +
            `⏱️ Response Time: ${responseTime}ms\n` +
            `📈 Rating: ${speedLabel}\n\n` +
            `💡 Recommended timeout: ${Math.max(responseTime * 10, 30000)}ms`
        }],
        structuredContent: {
          database_id: databaseId,
          latency_ms: responseTime,
          status: speedLabel,
          details: { recommended_timeout_ms: Math.max(responseTime * 10, 30000) },
        },
      };
    } catch (error) {
      return {
        content: [{
          type: 'text',
          text: `❌ Speed test failed: ${error.message}`
        }],
      };
    }
  }

  /**
   * Autonomous Self-Healing SQL Execution Handler
   * Intercepts SQL errors, applies deterministic or LLM repairs, retries up to max_attempts,
   * and records complete audit trail in _provenance.healing_trail.
   */
  async handleExecuteAndHealSQL(args) {
    const databaseId = args.database_id;
    const sql = args.sql;
    const maxAttempts = args.max_attempts || 3;
    const explanation = args.explanation || '';
    const fullResults = args.full_results === true;

    if (!databaseId) {
      throw new Error('database_id is required for ai_sql_execute_and_heal');
    }
    if (!sql) {
      throw new Error('sql is required for ai_sql_execute_and_heal');
    }

    // Read-Only Mode Security Check on original query
    if (isReadOnlyMode()) {
      const blockedOperation = detectWriteOperation(sql);
      if (blockedOperation) {
        logger.warn(`Read-only mode: Blocked ${blockedOperation} operation in ai_sql_execute_and_heal`, { sql: sql.substring(0, 100) });

        return {
          content: [
            {
              type: 'text',
              text: `🔒 **Read-Only Mode Active**\n\n` +
                `⛔ **Operation Blocked:** \`${blockedOperation}\`\n\n` +
                `This MCP server is running in read-only mode for security.\n` +
                `Write operations (INSERT, UPDATE, DELETE, DROP, etc.) are not allowed.\n\n` +
                `To enable write operations, set \`METABASE_READ_ONLY_MODE=false\` in your environment.\n\n` +
                `🔍 **Attempted Query:**\n\`\`\`sql\n${sql.substring(0, 200)}${sql.length > 200 ? '...' : ''}\n\`\`\``,
            },
          ],
        };
      }
    }

    const maskOptions = {
      strict: args.mask_strict === true,
      preserveDomain: args.preserve_domain !== false,
      pseudonymize: args.pseudonymize === true,
      salt: args.salt || 'metabase_ai_salt',
    };

    const startTime = Date.now();
    const result = await executeAndHealSQL({
      databaseId,
      sql,
      maxAttempts,
      client: this.metabaseClient,
      assistant: this.aiAssistant,
      maskPii: args.mask_pii !== false && args.maskPii !== false,
      maskOptions,
      explanation,
    });
    const executionTime = Date.now() - startTime;

    // Log activity if logger present
    if (this.activityLogger) {
      await this.activityLogger.logActivity({
        operation_type: 'ai_sql_execute_and_heal',
        operation_category: 'query',
        database_id: databaseId,
        source_sql: sql,
        execution_time_ms: executionTime,
        status: result.success ? 'success' : 'error',
        error_message: result.error || null,
        metadata: {
          attempts_used: result.attempts_used,
          healed: result.healed,
          final_sql: result.final_sql,
        },
      });
    }

    const rows = result.data?.rows || [];
    const columns = result.data?.columns || [];

    let output = '';
    if (result.success) {
      if (result.healed) {
        output += `✅ **Query healed & executed successfully** (${result.attempts_used} attempts used, ${executionTime}ms)\n`;
        output += `📊 ${columns.length} columns, ${rows.length} rows\n\n`;

        output += `🛠️ **Autonomous Healing Summary:**\n`;
        output += `| Attempt | Error Category | Root Cause / Diagnosis | Corrected SQL |\n`;
        output += `| --- | --- | --- | --- |\n`;
        for (const trail of result.healing_trail) {
          const shortSql = trail.corrected_sql
            ? (trail.corrected_sql.length > 50 ? trail.corrected_sql.substring(0, 47) + '...' : trail.corrected_sql)
            : '—';
          output += `| ${trail.attempt} | \`${trail.error_category}\` | ${(trail.diagnosis || '').replace(/\|/g, '\\|')} | \`${shortSql.replace(/\|/g, '\\|')}\` |\n`;
        }
        output += `\n**Final Executed SQL:**\n\`\`\`sql\n${result.final_sql}\n\`\`\`\n\n`;
      } else {
        output += `✅ **Query successful** (executed on 1st attempt, ${executionTime}ms)\n`;
        output += `📊 ${columns.length} columns, ${rows.length} rows\n\n`;
      }

      if (rows.length > 0) {
        output += `**Data:**\n\`\`\`\n`;
        const headers = columns.map(col => col.name || col);
        output += headers.join(' | ') + '\n';
        output += headers.map(() => '---').join(' | ') + '\n';

        rows.slice(0, 5).forEach((row) => {
          const formattedRow = row.map(cell => {
            if (cell === null) return 'NULL';
            let truncateLimit = fullResults || rows.length <= 2 ? 50000 : 100;
            if (typeof cell === 'string' && cell.length > truncateLimit) {
              return cell.substring(0, truncateLimit - 3) + '...';
            }
            return String(cell);
          });
          output += formattedRow.join(' | ') + '\n';
        });
        output += '\`\`\`\n';

        if (rows.length > 5) {
          output += `_+${rows.length - 5} more rows_\n`;
        }
      } else {
        output += `ℹ️ Query executed successfully but returned 0 rows.\n`;
      }
    } else {
      output += `❌ **Query execution & healing failed** after ${result.attempts_used} attempts\n\n`;
      output += `**Last Error:** \`${result.error}\`\n\n`;
      if (result.healing_trail.length > 0) {
        output += `🛠️ **Healing Audit Trail:**\n`;
        for (const trail of result.healing_trail) {
          output += `- **Attempt ${trail.attempt}** [${trail.error_category}]: ${trail.error_message}\n  _Diagnosis_: ${trail.diagnosis}\n`;
        }
      }
    }

    return {
      content: [
        {
          type: 'text',
          text: output,
        },
      ],
      structuredContent: {
        success: result.success,
        original_sql: result.original_sql,
        final_sql: result.final_sql,
        attempts_used: result.attempts_used,
        healed: result.healed,
        columns: columns.map(c => ({
          name: typeof c === 'object' && c ? (c.name || c.display_name || '') : String(c),
          base_type: typeof c === 'object' && c ? (c.base_type || c.type || 'unknown') : 'unknown',
        })),
        rows: rows.slice(0, fullResults ? rows.length : 200),
        row_count: rows.length,
        execution_time_ms: executionTime,
        truncated: rows.length > 200 && !fullResults,
        _provenance: result._provenance,
      },
    };
  }

}
