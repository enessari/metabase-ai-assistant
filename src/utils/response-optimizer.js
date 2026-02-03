/**
 * Response Optimizer Utility
 * Reduces token usage by providing minimal response formats
 * Inspired by jerichosequitin/metabase-mcp response optimization
 */

/**
 * Format options for response optimization
 */
export const ResponseFormat = {
    FULL: 'full',       // Complete response with all details
    MINIMAL: 'minimal', // ID and name only
    COMPACT: 'compact', // Essential fields only
};

/**
 * Extract essential fields from database object
 * @param {object} db - Database object
 * @returns {object} Minimal database info
 */
export function minimalDatabase(db) {
    return {
        id: db.id,
        name: db.name,
        engine: db.engine,
    };
}

/**
 * Extract essential fields from table object
 * @param {object} table - Table object
 * @returns {object} Minimal table info
 */
export function minimalTable(table) {
    return {
        id: table.id,
        name: table.name,
        schema: table.schema,
        field_count: table.fields?.length || 0,
    };
}

/**
 * Extract essential fields from dashboard object
 * @param {object} dashboard - Dashboard object
 * @returns {object} Minimal dashboard info
 */
export function minimalDashboard(dashboard) {
    return {
        id: dashboard.id,
        name: dashboard.name,
        collection_id: dashboard.collection_id,
        card_count: dashboard.ordered_cards?.length || 0,
    };
}

/**
 * Extract essential fields from question/card object
 * @param {object} question - Question object
 * @returns {object} Minimal question info
 */
export function minimalQuestion(question) {
    return {
        id: question.id,
        name: question.name,
        display: question.display || 'table',
        collection_id: question.collection_id,
    };
}

/**
 * Format list response for AI consumption
 * @param {string} title - Response title
 * @param {Array} items - Array of items
 * @param {Function} formatFn - Function to format each item
 * @param {object} options - Format options
 * @returns {object} MCP response
 */
export function formatListResponse(title, items, formatFn, options = {}) {
    const { format = ResponseFormat.COMPACT, limit = 50 } = options;

    const limitedItems = items.slice(0, limit);
    const hasMore = items.length > limit;

    if (format === ResponseFormat.MINIMAL) {
        // Ultra-minimal: just IDs and names in one line
        const itemList = limitedItems.map(item => {
            const minimal = formatFn(item);
            return `${minimal.name}(${minimal.id})`;
        }).join(', ');

        return {
            content: [{
                type: 'text',
                text: `${title}: ${itemList}${hasMore ? ` (+${items.length - limit} more)` : ''}`
            }]
        };
    }

    if (format === ResponseFormat.COMPACT) {
        // Compact: essential fields in a readable format
        const formattedItems = limitedItems.map(formatFn);

        return {
            content: [{
                type: 'text',
                text: `**${title}** (${items.length} total)\\n\\n` +
                    formattedItems.map(item =>
                        `• ${item.name} (ID: ${item.id})` +
                        (item.engine ? ` - ${item.engine}` : '') +
                        (item.schema ? ` [${item.schema}]` : '') +
                        (item.display ? ` [${item.display}]` : '')
                    ).join('\\n') +
                    (hasMore ? `\\n\\n_Showing ${limit} of ${items.length} items_` : '')
            }]
        };
    }

    // Full format: detailed response (default behavior)
    return null; // Let calling function handle full format
}

/**
 * Format SQL result for optimal token usage
 * @param {object} result - SQL query result
 * @param {object} options - Format options
 * @returns {object} Optimized result
 */
export function formatSQLResult(result, options = {}) {
    const { maxRows = 10, maxColWidth = 30 } = options;

    const rows = result.data?.rows || [];
    const columns = result.data?.cols || [];

    // Truncate long cell values
    const truncatedRows = rows.slice(0, maxRows).map(row =>
        row.map(cell => {
            if (cell === null) return 'NULL';
            const str = String(cell);
            return str.length > maxColWidth ? str.substring(0, maxColWidth - 3) + '...' : str;
        })
    );

    return {
        columns: columns.map(c => c.name),
        rows: truncatedRows,
        total_rows: rows.length,
        truncated: rows.length > maxRows,
    };
}

/**
 * Calculate estimated token count for a response
 * @param {string} text - Response text
 * @returns {number} Estimated token count
 */
export function estimateTokens(text) {
    // Rough estimate: 1 token ≈ 4 characters for English
    return Math.ceil(text.length / 4);
}

/**
 * Create summary statistics for list response
 * @param {Array} items - Array of items
 * @param {string} type - Type of items (databases, tables, etc.)
 * @returns {string} Summary text
 */
export function createListSummary(items, type) {
    if (items.length === 0) {
        return `No ${type} found.`;
    }

    if (items.length <= 5) {
        return `Found ${items.length} ${type}.`;
    }

    return `Found ${items.length} ${type}. Showing essential info only and use specific IDs for details.`;
}

export default {
    ResponseFormat,
    minimalDatabase,
    minimalTable,
    minimalDashboard,
    minimalQuestion,
    formatListResponse,
    formatSQLResult,
    estimateTokens,
    createListSummary,
};
