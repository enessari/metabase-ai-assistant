/**
 * Question Handler Module
 * Handles question/chart creation and management operations
 */

import { logger } from '../../utils/logger.js';

/**
 * Handle create question request
 * @param {object} args
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleCreateQuestion(args, context) {
    const { metabaseClient } = context;

    const question = await metabaseClient.createSQLQuestion(
        args.name,
        args.description,
        args.database_id,
        args.sql,
        args.collection_id
    );

    return {
        content: [
            {
                type: 'text',
                text: `✅ **Question Created!**\\n\\n` +
                    `• Name: ${question.name}\\n` +
                    `• ID: ${question.id}\\n` +
                    `• Database: ${args.database_id}\\n` +
                    `• Collection: ${args.collection_id || 'Root'}`,
            },
        ],
    };
}

/**
 * Handle get questions list request
 * @param {number|null} collectionId
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleGetQuestions(collectionId, context) {
    const { metabaseClient } = context;

    const questions = await metabaseClient.getQuestions(collectionId);

    return {
        content: [
            {
                type: 'text',
                text: `📊 **Available Questions (${questions.length})**\\n\\n` +
                    questions.map(q =>
                        `• **${q.name}** (ID: ${q.id})\\n  Type: ${q.display || 'table'}`
                    ).join('\\n'),
            },
        ],
    };
}

/**
 * Handle create parametric question request
 * @param {object} args
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleCreateParametricQuestion(args, context) {
    const { metabaseClient } = context;

    const question = await metabaseClient.createParametricQuestion({
        name: args.name,
        description: args.description,
        database_id: args.database_id,
        sql: args.sql,
        parameters: args.parameters,
        visualization: args.visualization || 'table',
        collection_id: args.collection_id
    });

    return {
        content: [
            {
                type: 'text',
                text: `✅ **Parametric Question Created!**\\n\\n` +
                    `• Name: ${question.name}\\n` +
                    `• ID: ${question.id}\\n` +
                    `• Parameters: ${args.parameters?.length || 0}\\n` +
                    `• Visualization: ${args.visualization || 'table'}`,
            },
        ],
    };
}

/**
 * Handle create metric request
 * @param {object} args
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleCreateMetric(args, context) {
    const { metabaseClient } = context;

    const metric = await metabaseClient.createMetric({
        name: args.name,
        description: args.description,
        table_id: args.table_id,
        definition: {
            aggregation: [args.aggregation?.type || 'count'],
            source_table: args.table_id
        }
    });

    return {
        content: [
            {
                type: 'text',
                text: `✅ **Metric Created!**\\n\\n` +
                    `• Name: ${metric.name}\\n` +
                    `• ID: ${metric.id}\\n` +
                    `• Table: ${args.table_id}\\n` +
                    `• Aggregation: ${args.aggregation?.type || 'count'}`,
            },
        ],
    };
}

export default {
    handleCreateQuestion,
    handleGetQuestions,
    handleCreateParametricQuestion,
    handleCreateMetric,
};
