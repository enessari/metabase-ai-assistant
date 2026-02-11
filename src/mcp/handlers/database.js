/**
 * Database Handler Module
 * Handles database exploration and SQL execution operations
 */

import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';

/**
 * Check if read-only mode is enabled
 * @returns {boolean}
 */
export function isReadOnlyMode() {
    return process.env.METABASE_READ_ONLY_MODE !== 'false';
}

/**
 * Check if SQL contains write operations
 * @param {string} sql - SQL query to check
 * @returns {string|null} - Matched operation or null
 */
export function detectWriteOperation(sql) {
    const writePattern = /\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|EXEC|EXECUTE)\b/i;
    const match = sql.match(writePattern);
    return match ? match[0].toUpperCase() : null;
}

/**
 * Handle get databases request
 * @param {object} context - Handler context with metabaseClient, activityLogger
 * @returns {Promise<object>}
 */
export async function handleGetDatabases(context) {
    const { metabaseClient, activityLogger } = context;

    const databases = await metabaseClient.getDatabases();
    const dbList = Array.isArray(databases) ? databases : (databases?.data || []);

    // Log activity
    if (activityLogger) {
        await activityLogger.logActivity({
            operation_type: 'db_list',
            operation_category: 'exploration',
            status: 'success',
            result_count: dbList.length
        });
    }

    return {
        content: [
            {
                type: 'text',
                text: `📊 **Available Databases (${dbList.length})**\\n\\n` +
                    dbList.map(db =>
                        `• **${db.name}** (ID: ${db.id})\\n  Engine: ${db.engine} | Tables: ${db.tables?.length || 'N/A'}`
                    ).join('\\n'),
            },
        ],
    };
}

/**
 * Handle get database schemas request
 * @param {number} databaseId
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleGetDatabaseSchemas(databaseId, context) {
    const { metabaseClient } = context;

    const schemas = await metabaseClient.getDatabaseSchemas(databaseId);

    return {
        content: [
            {
                type: 'text',
                text: `📁 **Schemas in Database ${databaseId}**\\n\\n` +
                    (schemas.length > 0
                        ? schemas.map(s => `• ${s}`).join('\\n')
                        : 'No schemas found'),
            },
        ],
    };
}

/**
 * Handle get database tables request
 * @param {number} databaseId
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleGetDatabaseTables(databaseId, context) {
    const { metabaseClient } = context;

    const metadata = await metabaseClient.getDatabase(databaseId);
    const tables = metadata?.tables || [];

    return {
        content: [
            {
                type: 'text',
                text: `📋 **Tables in Database ${databaseId}** (${tables.length} total)\\n\\n` +
                    tables.map(t =>
                        `• **${t.schema}.${t.name}** (ID: ${t.id})\\n  Fields: ${t.fields?.length || 'N/A'}`
                    ).join('\\n'),
            },
        ],
    };
}

/**
 * Handle SQL execution request with read-only mode protection
 * @param {number} databaseId
 * @param {string} sql
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleExecuteSQL(databaseId, sql, context) {
    const { metabaseClient, activityLogger } = context;

    // Read-Only Mode Security Check
    if (isReadOnlyMode()) {
        const blockedOperation = detectWriteOperation(sql);
        if (blockedOperation) {
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

    try {
        const result = await metabaseClient.executeNativeQuery(databaseId, sql);
        const executionTime = Date.now() - startTime;

        // Log the activity
        if (activityLogger) {
            await activityLogger.logSQLExecution(sql, databaseId, result, executionTime);
        }

        // Format the result for display
        const rows = result.data?.rows || [];
        const columns = result.data?.cols || [];

        let output = `✅ **Query executed successfully!**\\n\\n`;
        output += `📊 **Results Summary:**\\n`;
        output += `• Database ID: ${databaseId}\\n`;
        output += `• Columns: ${columns.length} (${columns.map(col => col.name).join(', ')})\\n`;
        output += `• Rows returned: ${rows.length}\\n`;
        output += `• Execution time: ${executionTime}ms\\n\\n`;

        if (rows.length > 0) {
            output += `📋 **Sample Data (first 5 rows):**\\n\`\`\`\\n`;

            // Create table header
            const headers = columns.map(col => col.name);
            output += headers.join(' | ') + '\\n';
            output += headers.map(() => '---').join(' | ') + '\\n';

            // Add data rows
            rows.slice(0, 5).forEach((row) => {
                const formattedRow = row.map(cell => {
                    if (cell === null) return 'NULL';
                    if (typeof cell === 'string' && cell.length > 50) {
                        return cell.substring(0, 47) + '...';
                    }
                    return String(cell);
                });
                output += formattedRow.join(' | ') + '\\n';
            });

            output += '\`\`\`\\n';

            if (rows.length > 5) {
                output += `\\n... and ${rows.length - 5} more rows\\n`;
            }
        } else {
            output += `ℹ️ No data returned by the query.\\n`;
        }

        // Add query info
        output += `\\n🔍 **Query Details:**\\n\`\`\`sql\\n${sql}\\n\`\`\``;

        return {
            content: [{ type: 'text', text: output }],
        };

    } catch (err) {
        const executionTime = Date.now() - startTime;

        // Log the failed activity
        if (activityLogger) {
            await activityLogger.logActivity({
                operation_type: 'sql_execute',
                operation_category: 'query',
                database_id: databaseId,
                source_sql: sql,
                execution_time_ms: executionTime,
                status: 'error',
                error_message: err.message
            });
        }

        const output = `❌ **Query execution failed!**\\n\\n` +
            `🚫 **Error Details:**\\n` +
            `• Database ID: ${databaseId}\\n` +
            `• Execution time: ${executionTime}ms\\n` +
            `• Error: ${err.message}\\n\\n` +
            `🔍 **Failed Query:**\\n\`\`\`sql\\n${sql}\\n\`\`\``;

        return {
            content: [{ type: 'text', text: output }],
        };
    }
}

/**
 * Handle test database speed request
 * @param {number} databaseId
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleTestDatabaseSpeed(databaseId, context) {
    const { metabaseClient } = context;

    const startTime = Date.now();

    try {
        // Simple SELECT 1 query to test latency
        await metabaseClient.executeNativeQuery(databaseId, 'SELECT 1');
        const latency = Date.now() - startTime;

        let speedRating = 'Fast';
        let emoji = '🟢';

        if (latency > 500) {
            speedRating = 'Slow';
            emoji = '🔴';
        } else if (latency > 200) {
            speedRating = 'Moderate';
            emoji = '🟡';
        }

        return {
            content: [
                {
                    type: 'text',
                    text: `${emoji} **Database Speed Test**\\n\\n` +
                        `• Database ID: ${databaseId}\\n` +
                        `• Response Time: ${latency}ms\\n` +
                        `• Rating: **${speedRating}**\\n\\n` +
                        `💡 Recommendations:\\n` +
                        (latency > 500
                            ? '• Consider using shorter queries\\n• Set higher timeout values'
                            : '• Database is performing well'),
                },
            ],
            structuredContent: {
                database_id: databaseId,
                latency_ms: latency,
                status: speedRating,
                details: { emoji },
            },
        };
    } catch (err) {
        return {
            content: [
                {
                    type: 'text',
                    text: `❌ **Speed Test Failed**\\n\\n` +
                        `• Database ID: ${databaseId}\\n` +
                        `• Error: ${err.message}`,
                },
            ],
        };
    }
}

/**
 * Handle get database connection info request
 * @param {number} databaseId
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleGetConnectionInfo(databaseId, context) {
    const { metabaseClient } = context;

    const db = await metabaseClient.getDatabase(databaseId);

    return {
        content: [
            {
                type: 'text',
                text: `🔌 **Database Connection Info**\\n\\n` +
                    `• Name: ${db.name}\\n` +
                    `• ID: ${db.id}\\n` +
                    `• Engine: ${db.engine}\\n` +
                    `• Native Query Enabled: ${db.native_permissions === 'write'}\\n` +
                    `• Auto Run Queries: ${db.auto_run_queries}\\n` +
                    `• Tables: ${db.tables?.length || 0}`,
            },
        ],
        structuredContent: {
            id: db.id,
            name: db.name,
            engine: db.engine,
            details: {
                native_permissions: db.native_permissions,
                auto_run_queries: db.auto_run_queries,
                tables_count: db.tables?.length || 0,
            },
        },
    };
}

export default {
    handleGetDatabases,
    handleGetDatabaseSchemas,
    handleGetDatabaseTables,
    handleExecuteSQL,
    handleTestDatabaseSpeed,
    handleGetConnectionInfo,
    isReadOnlyMode,
    detectWriteOperation,
};
