/**
 * Dashboard Handler Module
 * Handles dashboard creation, management, and visualization operations
 */

import { logger } from '../../utils/logger.js';

/**
 * Handle create dashboard request
 * @param {object} args
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleCreateDashboard(args, context) {
    const { metabaseClient } = context;

    const dashboard = await metabaseClient.createDashboard({
        name: args.name,
        description: args.description,
        collection_id: args.collection_id
    });

    return {
        content: [
            {
                type: 'text',
                text: `✅ **Dashboard Created!**\\n\\n` +
                    `• Name: ${dashboard.name}\\n` +
                    `• ID: ${dashboard.id}\\n` +
                    `• Collection: ${args.collection_id || 'Root'}`,
            },
        ],
    };
}

/**
 * Handle get dashboards list request
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleGetDashboards(context) {
    const { metabaseClient } = context;

    const dashboards = await metabaseClient.getDashboards();

    return {
        content: [
            {
                type: 'text',
                text: `📊 **Available Dashboards (${dashboards.length})**\\n\\n` +
                    dashboards.map(d =>
                        `• **${d.name}** (ID: ${d.id})\\n  Collection: ${d.collection_id || 'Root'}`
                    ).join('\\n'),
            },
        ],
    };
}

/**
 * Handle add card to dashboard request
 * @param {object} args
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleAddCardToDashboard(args, context) {
    const { metabaseClient } = context;

    const position = args.position || {};

    await metabaseClient.addCardToDashboard(args.dashboard_id, args.question_id, {
        row: position.row || 0,
        col: position.col || 0,
        sizeX: position.sizeX || 6,
        sizeY: position.sizeY || 4
    });

    return {
        content: [
            {
                type: 'text',
                text: `✅ **Card Added to Dashboard!**\\n\\n` +
                    `• Dashboard ID: ${args.dashboard_id}\\n` +
                    `• Question ID: ${args.question_id}\\n` +
                    `• Position: (${position.row || 0}, ${position.col || 0})\\n` +
                    `• Size: ${position.sizeX || 6}x${position.sizeY || 4}`,
            },
        ],
    };
}

/**
 * Handle add dashboard filter request
 * @param {object} args
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleAddDashboardFilter(args, context) {
    const { metabaseClient } = context;

    await metabaseClient.addDashboardFilter(args.dashboard_id, {
        name: args.name,
        type: args.type,
        field_id: args.field_id,
        default_value: args.default_value
    });

    return {
        content: [
            {
                type: 'text',
                text: `✅ **Filter Added!**\\n\\n` +
                    `• Dashboard ID: ${args.dashboard_id}\\n` +
                    `• Filter Name: ${args.name}\\n` +
                    `• Type: ${args.type}`,
            },
        ],
    };
}

/**
 * Handle optimize dashboard layout request
 * @param {object} args
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleOptimizeDashboardLayout(args, context) {
    const { metabaseClient } = context;

    const dashboard = await metabaseClient.getDashboard(args.dashboard_id);
    const cards = dashboard.ordered_cards || [];

    const layoutStyle = args.layout_style || 'executive';
    const gridWidth = args.grid_width || 12;

    // Calculate optimized positions
    const optimizedCards = cards.map((card, index) => {
        const row = Math.floor(index / 2) * 4;
        const col = (index % 2) * 6;

        return {
            ...card,
            row,
            col,
            sizeX: 6,
            sizeY: 4
        };
    });

    // Update dashboard
    await metabaseClient.updateDashboard(args.dashboard_id, {
        ordered_cards: optimizedCards
    });

    return {
        content: [
            {
                type: 'text',
                text: `✅ **Dashboard Layout Optimized!**\\n\\n` +
                    `• Dashboard ID: ${args.dashboard_id}\\n` +
                    `• Style: ${layoutStyle}\\n` +
                    `• Cards Reorganized: ${cards.length}`,
            },
        ],
    };
}

/**
 * Handle create executive dashboard request
 * @param {object} args
 * @param {object} context
 * @returns {Promise<object>}
 */
export async function handleCreateExecutiveDashboard(args, context) {
    const { metabaseClient } = context;

    // Create the dashboard first
    const dashboard = await metabaseClient.createDashboard({
        name: args.name,
        description: `Executive dashboard for ${args.business_domain || 'general'} metrics`,
        collection_id: args.collection_id
    });

    return {
        content: [
            {
                type: 'text',
                text: `✅ **Executive Dashboard Created!**\\n\\n` +
                    `• Dashboard ID: ${dashboard.id}\\n` +
                    `• Name: ${dashboard.name}\\n` +
                    `• Business Domain: ${args.business_domain || 'general'}\\n` +
                    `• Time Period: ${args.time_period || 'last_30_days'}\\n\\n` +
                    `📝 Next Steps:\\n` +
                    `• Add questions to this dashboard\\n` +
                    `• Configure filters for interactive analysis`,
            },
        ],
    };
}

export default {
    handleCreateDashboard,
    handleGetDashboards,
    handleAddCardToDashboard,
    handleAddDashboardFilter,
    handleOptimizeDashboardLayout,
    handleCreateExecutiveDashboard,
};
