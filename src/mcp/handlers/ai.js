/**
 * AI Handler Module
 * Handles AI-powered SQL generation, optimization, and explanation
 */

import { logger } from '../../utils/logger.js';

/**
 * Handle generate SQL from natural language request
 * @param {string} description
 * @param {number} databaseId
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleGenerateSQL(description, databaseId, context) {
    const { aiAssistant, metabaseClient } = context;

    if (!aiAssistant) {
        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **AI Assistant Not Available**\\n\\n` +
                        `Please configure ANTHROPIC_API_KEY or OPENAI_API_KEY in your environment.`,
                },
            ],
        };
    }

    try {
        const sql = await aiAssistant.generateSQL(description, databaseId);

        return {
            content: [
                {
                    type: 'text',
                    text: `✅ **SQL Generated**\\n\\n` +
                        `📝 **Request:** ${description}\\n\\n` +
                        `💻 **Generated Query:**\\n\`\`\`sql\\n${sql}\\n\`\`\`\\n\\n` +
                        `💡 You can execute this query using the \`sql_execute\` tool.`,
                },
            ],
        };
    } catch (err) {
        return {
            content: [
                {
                    type: 'text',
                    text: `❌ **SQL Generation Failed**\\n\\n` +
                        `Error: ${err.message}`,
                },
            ],
        };
    }
}

/**
 * Handle optimize SQL query request
 * @param {string} sql
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleOptimizeQuery(sql, context) {
    const { aiAssistant } = context;

    if (!aiAssistant) {
        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **AI Assistant Not Available**\\n\\n` +
                        `Please configure ANTHROPIC_API_KEY or OPENAI_API_KEY in your environment.`,
                },
            ],
        };
    }

    try {
        const result = await aiAssistant.optimizeQuery(sql);

        return {
            content: [
                {
                    type: 'text',
                    text: `✅ **Query Optimization Analysis**\\n\\n` +
                        `📊 **Original Query:**\\n\`\`\`sql\\n${sql}\\n\`\`\`\\n\\n` +
                        `🚀 **Optimization Suggestions:**\\n${result}`,
                },
            ],
        };
    } catch (err) {
        return {
            content: [
                {
                    type: 'text',
                    text: `❌ **Optimization Failed**\\n\\n` +
                        `Error: ${err.message}`,
                },
            ],
        };
    }
}

/**
 * Handle explain SQL query request
 * @param {string} sql
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleExplainQuery(sql, context) {
    const { aiAssistant } = context;

    if (!aiAssistant) {
        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **AI Assistant Not Available**\\n\\n` +
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
                    text: `✅ **Query Explanation**\\n\\n` +
                        `📊 **Query:**\\n\`\`\`sql\\n${sql}\\n\`\`\`\\n\\n` +
                        `📖 **Explanation:**\\n${explanation}`,
                },
            ],
        };
    } catch (err) {
        return {
            content: [
                {
                    type: 'text',
                    text: `❌ **Explanation Failed**\\n\\n` +
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
    const { aiAssistant, metabaseClient } = context;

    if (!aiAssistant) {
        return {
            content: [
                {
                    type: 'text',
                    text: `⚠️ **AI Assistant Not Available**\\n\\n` +
                        `Please configure ANTHROPIC_API_KEY or OPENAI_API_KEY in your environment.`,
                },
            ],
        };
    }

    const database = await metabaseClient.getDatabase(args.database_id);
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
                text: `✅ **Auto-Describe Complete**\\n\\n` +
                    `• Database: ${args.database_id}\\n` +
                    `• Tables Described: ${described}\\n\\n` +
                    `📋 **Descriptions:**\\n` +
                    results.map(r => `• **${r.table}:** ${r.description}`).join('\\n'),
            },
        ],
    };
}

export default {
    handleGenerateSQL,
    handleOptimizeQuery,
    handleExplainQuery,
    handleAutoDescribe,
};
