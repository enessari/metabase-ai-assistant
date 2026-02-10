import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';

/**
 * Handler for SQL Execution Operations
 */
export class SqlHandler {
    constructor(metabaseClient, cache) {
        this.metabaseClient = metabaseClient;
        this.cache = cache;
        this.activeJobs = new Map();
        this.jobCounter = 0;
    }

    routes() {
        return {
            'sql_execute': (args) => this.handleExecuteSQL(args),
            'sql_submit': (args) => this.handleSQLSubmit(args),
            'sql_status': (args) => this.handleSQLStatus(args),
            'sql_cancel': (args) => this.handleSQLCancel(args),
        };
    }

    async handleExecuteSQL(args) {
    await this.ensureInitialized();

    const databaseId = args.database_id;
    const sql = args.sql;
    const fullResults = args.full_results === true;

    if (this.initError) {
      throw new McpError(ErrorCode.InternalError, `Failed to initialize: ${this.initError.message}`);
    }

    // Read-Only Mode Security Check
    const isReadOnlyMode = process.env.METABASE_READ_ONLY_MODE !== 'false';
    if (isReadOnlyMode) {
      const writePattern = /\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|EXEC|EXECUTE)\b/i;
      if (writePattern.test(sql)) {
        const blockedOperation = sql.match(writePattern)?.[0]?.toUpperCase() || 'WRITE';
        logger.warn(`Read-only mode: Blocked ${blockedOperation} operation`, { sql: sql.substring(0, 100) });

        return {
          content: [
            {
              type: 'text',
              text: `🔒 **Read-Only Mode Active**\\n\\n` +
                `⛔ **Operation Blocked:** \`${blockedOperation}\`\\n\\n` +
                `This MCP server is running in read-only mode for security.\\n` +
                `Write operations (INSERT, UPDATE, DELETE, DROP, etc.) are not allowed.\\n\\n` +
                `To enable write operations, set \`METABASE_READ_ONLY_MODE=false\` in your environment.\\n\\n` +
                `🔍 **Attempted Query:**\\n\`\`\`sql\\n${sql.substring(0, 200)}${sql.length > 200 ? '...' : ''}\\n\`\`\``,
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
      const rows = result.data.rows || [];
      const columns = result.data.cols || [];

      let output = `✅ **Query successful** (${executionTime}ms)\\n`;
      output += `📊 ${columns.length} columns, ${rows.length} rows\\n\\n`;

      if (rows.length > 0) {
        // Show sample data (max 5 rows)
        output += `**Data:**\\n\`\`\`\\n`;
        const headers = columns.map(col => col.name);
        output += headers.join(' | ') + '\\n';
        output += headers.map(() => '---').join(' | ') + '\\n';

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
          output += formattedRow.join(' | ') + '\\n';
        });
        output += '\`\`\`\\n';

        if (rows.length > 5) {
          output += `_+${rows.length - 5} more rows_\\n`;
        }

        // Large result warning
        if (rows.length > 100) {
          output += `\\n⚠️ **Large result:** ${rows.length} rows returned. Use LIMIT for better performance.\\n`;
        }
      } else {
        // Empty result - smart detection
        output += `ℹ️ No results.\\n`;

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
                output += `\\n⚠️ **Note:** \`${tableName}\` has ${tableRowCount.toLocaleString()} rows but query returned nothing.\\n`;
                output += `Possible causes: WHERE clause too restrictive, column name typo, JOIN mismatch\\n`;
                output += `💡 Use \`db_table_profile\` to inspect table structure.\\n`;
              }
            } catch (e) { /* ignore */ }
          }
        } catch (e) { /* ignore */ }
      }

      // Tool suggestions (only for SELECT queries with few results)
      if (sql.toLowerCase().trim().startsWith('select') && rows.length <= 5) {
        output += `\\n💡 Related: \`db_table_profile\`, \`mb_field_values\`\\n`;
      }

      return {
        content: [
          {
            type: 'text',
            text: output,
          },
        ],
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
      const output = `❌ SQL Error: ${err.message}\\nQuery: ${shortSql}`;

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
      await this.ensureInitialized();

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

      const output = `✅ **Query Submitted**\\n` +
        `📋 Job ID: \`${job.id}\`\\n` +
        `⏱️ Timeout: ${timeoutSeconds} seconds\\n` +
        `📊 Status: pending\\n\\n` +
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

      let output = `📋 **Job Status: ${job.id}**\\n`;
      output += `📊 Status: ${job.status}\\n`;
      output += `⏱️ Elapsed: ${elapsedSeconds} seconds\\n`;

      if (job.status === 'running' || job.status === 'pending') {
        let waitSeconds = 3;
        if (elapsedSeconds > 60) waitSeconds = 30;
        else if (elapsedSeconds > 30) waitSeconds = 10;
        else if (elapsedSeconds > 10) waitSeconds = 5;

        output += `\\n💡 Query is still running. Please wait **${waitSeconds} seconds** before checking again.\\n`;
        output += `(Use \`sql_cancel\` to stop if needed)`;
      } else if (job.status === 'complete') {
        const rows = job.result?.data?.rows || [];
        const columns = job.result?.data?.cols || [];

        output += `✅ **Query Complete!**\\n`;
        output += `📊 ${columns.length} columns, ${rows.length} rows\\n\\n`;

        if (rows.length > 0) {
          output += `**Data:**\\n\`\`\`\\n`;
          const headers = columns.map(col => col.name);
          output += headers.join(' | ') + '\\n';
          output += headers.map(() => '---').join(' | ') + '\\n';

          rows.slice(0, 5).forEach((row) => {
            const formattedRow = row.map(cell => {
              if (cell === null) return 'NULL';
              const str = String(cell);
              return str.length > 30 ? str.substring(0, 27) + '...' : str;
            });
            output += formattedRow.join(' | ') + '\\n';
          });
          output += '\`\`\`\\n';

          if (rows.length > 5) {
            output += `_+${rows.length - 5} more rows_\\n`;
          }
        }
      } else if (job.status === 'failed' || job.status === 'timeout' || job.status === 'cancelled') {
        output += `\\n❌ ${job.error || 'Query did not complete'}`;
      }

      return {
        content: [{ type: 'text', text: output }],
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

      const output = `✅ **Query Cancelled**\\n` +
        `📋 Job ID: ${args.job_id}\\n` +
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
}
