/**
 * AI Handler Module
 * Handles AI-powered SQL generation, optimization, and explanation
 */

import { logger } from '../../utils/logger.js';

/**
 * Handle generate SQL from natural language request
 * @param {string|object} argsOrDescription
 * @param {number|object} databaseIdOrContext
 * @param {object} [context]
 * @returns {Promise<object>}
 */
export async function handleGenerateSQL(argsOrDescription, databaseIdOrContext, context) {
    let description;
    let databaseId;
    let ctx;

    if (typeof argsOrDescription === 'object' && argsOrDescription !== null) {
        description = argsOrDescription.description;
        databaseId = argsOrDescription.database_id;
        ctx = databaseIdOrContext || {};
    } else {
        description = argsOrDescription;
        databaseId = databaseIdOrContext;
        ctx = context || {};
    }

    const { aiAssistant, metabaseClient } = ctx;

    if (!aiAssistant) {
        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **AI Assistant Not Available**\n\n` +
                        `Please configure ANTHROPIC_API_KEY or OPENAI_API_KEY in your environment.`,
                },
            ],
        };
    }

    try {
        let tables = databaseId;
        if (metabaseClient && typeof metabaseClient.getDatabaseTables === 'function' && databaseId) {
            try {
                tables = await metabaseClient.getDatabaseTables(databaseId);
            } catch (err) {
                logger.warn(`Could not fetch schema for db ${databaseId}:`, err.message);
            }
        }

        const sql = await aiAssistant.generateSQL(description, tables);

        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **[AI-GENERATED SQL — REVIEW BEFORE EXECUTING]**\n\n` +
                        `✅ **SQL Generated**\n\n` +
                        `📝 **Request:** ${description}\n\n` +
                        `💻 **Generated Query:**\n\`\`\`sql\n${sql}\n\`\`\`\n\n` +
                        `💡 You can execute this query using the \`sql_execute\` tool.`,
                },
            ],
            structuredContent: {
                sql,
                description,
                database_id: databaseId,
                _provenance: {
                    ai_generated: true,
                    tool: 'ai_sql_generate',
                    review_required: true,
                    timestamp: new Date().toISOString(),
                    provider: aiAssistant.aiProvider || 'anthropic',
                    model: aiAssistant.model || 'claude-3-sonnet-20240229',
                    generation_parameters: {
                        database_id: databaseId,
                        enforce_read_only: true,
                    },
                },
            },
        };
    } catch (err) {
        return {
            content: [
                {
                    type: 'text',
                    text: `❌ **SQL Generation Failed**\n\n` +
                        `Error: ${err.message}`,
                },
            ],
        };
    }
}

/**
 * Handle optimize SQL query request
 * @param {string|object} argsOrSql
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleOptimizeQuery(argsOrSql, context) {
    const sql = typeof argsOrSql === 'object' && argsOrSql !== null ? argsOrSql.sql : argsOrSql;
    const ctx = (typeof argsOrSql === 'object' && argsOrSql !== null ? context : context) || {};
    const { aiAssistant } = ctx;

    if (!aiAssistant) {
        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **AI Assistant Not Available**\n\n` +
                        `Please configure ANTHROPIC_API_KEY or OPENAI_API_KEY in your environment.`,
                },
            ],
        };
    }

    try {
        const result = await aiAssistant.optimizeQuery(sql);
        const optText = typeof result === 'string' ? result : JSON.stringify(result, null, 2);

        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **[AI-GENERATED CONTENT — REVIEW BEFORE EXECUTING]**\n\n` +
                        `✅ **Query Optimization Analysis**\n\n` +
                        `📊 **Original Query:**\n\`\`\`sql\n${sql}\n\`\`\`\n\n` +
                        `🚀 **Optimization Suggestions:**\n${optText}`,
                },
            ],
            structuredContent: {
                original_sql: sql,
                optimized_sql: result?.optimized_sql || (typeof result === 'string' ? result : null),
                optimizations: result?.optimizations || [],
                improvements: result?.improvements || null,
                _provenance: {
                    ai_generated: true,
                    tool: 'ai_sql_optimize',
                    review_required: true,
                    timestamp: new Date().toISOString(),
                    provider: aiAssistant.aiProvider || 'anthropic',
                    model: aiAssistant.model || 'claude-3-sonnet-20240229',
                    generation_parameters: {
                        enforce_read_only: true,
                    },
                },
            },
        };
    } catch (err) {
        return {
            content: [
                {
                    type: 'text',
                    text: `❌ **Optimization Failed**\n\n` +
                        `Error: ${err.message}`,
                },
            ],
        };
    }
}

/**
 * Handle explain SQL query request
 * @param {string|object} argsOrSql
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleExplainQuery(argsOrSql, context) {
    const sql = typeof argsOrSql === 'object' && argsOrSql !== null ? argsOrSql.sql : argsOrSql;
    const ctx = (typeof argsOrSql === 'object' && argsOrSql !== null ? context : context) || {};
    const { aiAssistant } = ctx;

    if (!aiAssistant) {
        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **AI Assistant Not Available**\n\n` +
                        `Please configure ANTHROPIC_API_KEY or OPENAI_API_KEY in your environment.`,
                },
            ],
        };
    }

    try {
        const explanation = await aiAssistant.explainQuery(sql);

        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **[AI-GENERATED CONTENT — REVIEW BEFORE EXECUTING]**\n\n` +
                        `✅ **Query Explanation**\n\n` +
                        `📊 **Query:**\n\`\`\`sql\n${sql}\n\`\`\`\n\n` +
                        `📖 **Explanation:**\n${explanation}`,
                },
            ],
            structuredContent: {
                sql,
                explanation,
                _provenance: {
                    ai_generated: true,
                    tool: 'ai_sql_explain',
                    review_required: false,
                    timestamp: new Date().toISOString(),
                    provider: aiAssistant.aiProvider || 'anthropic',
                    model: aiAssistant.model || 'claude-3-sonnet-20240229',
                    generation_parameters: {
                        enforce_read_only: true,
                    },
                },
            },
        };
    } catch (err) {
        return {
            content: [
                {
                    type: 'text',
                    text: `❌ **Explanation Failed**\n\n` +
                        `Error: ${err.message}`,
                },
            ],
        };
    }
}

/**
 * Handle auto-describe request (AI-powered descriptions for tables/fields)
 * @param {object} args
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleAutoDescribe(args, context) {
    const { aiAssistant, metabaseClient } = context || {};

    if (!aiAssistant) {
        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **AI Assistant Not Available**\n\n` +
                        `Please configure ANTHROPIC_API_KEY or OPENAI_API_KEY in your environment.`,
                },
            ],
        };
    }

    const database = metabaseClient?.getDatabase ? await metabaseClient.getDatabase(args.database_id) : null;
    const tables = database?.tables || [];

    let described = 0;
    const results = [];

    for (const table of tables.slice(0, 10)) { // Limit to 10 tables
        try {
            const description = await aiAssistant.describeTable(table);
            results.push({ table: table.name, description });
            described++;
        } catch (err) {
            logger.warn(`Failed to describe table ${table.name}:`, err.message);
        }
    }

    return {
        content: [
            {
                type: 'text',
                text: `⚠️ **[AI-GENERATED CONTENT — REVIEW BEFORE EXECUTING]**\n\n` +
                    `✅ **Auto-Describe Complete**\n\n` +
                    `• Database: ${args.database_id}\n` +
                    `• Tables Described: ${described}\n\n` +
                    `📋 **Descriptions:**\n` +
                    results.map(r => `• **${r.table}:** ${r.description}`).join('\n'),
            },
        ],
        structuredContent: {
            database_id: args.database_id,
            described_count: described,
            descriptions: results,
            _provenance: {
                ai_generated: true,
                tool: 'mb_auto_describe',
                review_required: true,
                timestamp: new Date().toISOString(),
                provider: aiAssistant.aiProvider || 'anthropic',
                model: aiAssistant.model || 'claude-3-sonnet-20240229',
                generation_parameters: {
                    database_id: args.database_id,
                    target_type: args.target_type || 'all',
                },
            },
        },
    };
}

export default {
    handleGenerateSQL,
    handleOptimizeQuery,
    handleExplainQuery,
    handleAutoDescribe,
};
