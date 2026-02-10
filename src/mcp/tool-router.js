/**
 * Tool Router - Dynamic dispatch for MCP tool calls
 * Replaces the 200+ case switch statement in server.js
 */
import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../utils/logger.js';

/**
 * Tools that perform write/mutate operations.
 * These are blocked when METABASE_READ_ONLY_MODE is active.
 */
const WRITE_TOOLS = new Set([
    // SQL write
    'sql_execute', // checked inline for DML
    // DDL operations
    'db_table_create', 'db_view_create', 'db_matview_create', 'db_index_create', 'db_ai_drop',
    // Card/Question mutations
    'mb_question_create', 'mb_question_create_parametric', 'mb_card_update', 'mb_card_delete', 'mb_card_archive',
    // Dashboard mutations
    'mb_dashboard_create', 'mb_dashboard_update', 'mb_dashboard_delete',
    'mb_dashboard_add_card', 'mb_dashboard_add_card_sql', 'mb_dashboard_update_layout',
    'mb_dashboard_card_update', 'mb_dashboard_card_remove', 'mb_dashboard_add_filter',
    'mb_dashboard_layout_optimize', 'mb_dashboard_template_executive',
    // Direct SQL dashboard
    'mb_create_parametric_question', 'mb_link_dashboard_filter',
    // Collection mutations
    'mb_collection_create', 'mb_collection_move', 'mb_collection_permissions_update',
    // User management
    'mb_user_create', 'mb_user_update', 'mb_user_disable',
    'mb_permission_group_create', 'mb_permission_group_delete',
    'mb_permission_group_add_user', 'mb_permission_group_remove_user',
    // Actions & alerts
    'mb_action_create', 'mb_action_execute', 'mb_alert_create', 'mb_pulse_create',
    // Metadata mutations
    'mb_field_metadata', 'mb_table_metadata',
    // Metric & segment
    'mb_metric_create', 'mb_segment_create',
    // Copy/clone (creates new items)
    'mb_card_copy', 'mb_card_clone', 'mb_dashboard_copy', 'mb_collection_copy',
    // Bookmarks
    'mb_bookmark_create', 'mb_bookmark_delete',
    // Maintenance
    'db_vacuum_analyze',
    // Cache/sync
    'mb_cache_invalidate', 'db_sync_schema',
    // Cleanup
    'mb_meta_auto_cleanup',
    // Relationships
    'mb_relationships_create',
    // Parametric
    'parametric_question_create', 'parametric_dashboard_create',
    // Definition tables
    'definition_tables_init',
    // Activity
    'activity_log_init', 'activity_cleanup',
]);

/**
 * Check if read-only mode is active
 */
export function isReadOnlyMode() {
    return process.env.METABASE_READ_ONLY_MODE !== 'false';
}

/**
 * Build a route map from handler instances
 * Each handler exposes a routes() method returning { toolName: handlerFn }
 */
export function buildRouteMap(handlers) {
    const routes = {};

    for (const [category, handler] of Object.entries(handlers)) {
        if (typeof handler.routes !== 'function') {
            logger.warn(`Handler '${category}' has no routes() method, skipping`);
            continue;
        }

        const handlerRoutes = handler.routes();
        for (const [toolName, fn] of Object.entries(handlerRoutes)) {
            if (routes[toolName]) {
                logger.warn(`Duplicate route: '${toolName}' in '${category}' overrides previous`);
            }
            routes[toolName] = fn.bind(handler);
        }
    }

    logger.info(`Tool router initialized with ${Object.keys(routes).length} routes`);
    return routes;
}

/**
 * Create a request handler using the route map
 */
export function createToolHandler(routeMap) {
    return async (request) => {
        const { name, arguments: args } = request.params;

        // ── Read-only gate ──
        if (isReadOnlyMode() && WRITE_TOOLS.has(name)) {
            throw new McpError(
                ErrorCode.InvalidRequest,
                `🔒 Read-only mode is active. The tool '${name}' is a write operation and has been blocked.\n` +
                `To enable write operations, set \`METABASE_READ_ONLY_MODE=false\` in your environment.`
            );
        }

        const handler = routeMap[name];
        if (!handler) {
            throw new McpError(
                ErrorCode.MethodNotFound,
                `Unknown tool: ${name}`
            );
        }

        try {
            return await handler(args);
        } catch (error) {
            logger.error(`Tool ${name} failed:`, error);

            let errorMessage = error.message;
            let errorCode = ErrorCode.InternalError;

            if (error.message.includes('authentication failed')) {
                errorMessage = 'Database authentication failed. Check connection credentials.';
                errorCode = ErrorCode.InvalidRequest;
            } else if (error.message.includes('prefix')) {
                errorMessage = `Security violation: ${error.message}`;
                errorCode = ErrorCode.InvalidRequest;
            } else if (error.message.includes('connection')) {
                errorMessage = 'Database connection failed. Check network and credentials.';
                errorCode = ErrorCode.InternalError;
            } else if (error.message.includes('not found')) {
                errorMessage = `Resource not found: ${error.message}`;
                errorCode = ErrorCode.InvalidRequest;
            } else if (error.message.includes('is not a function')) {
                errorMessage = `Unexpected API response format. Details: ${error.message.substring(0, 100)}`;
                errorCode = ErrorCode.InternalError;
            } else if (error.message.includes('Cannot read properties of undefined') || error.message.includes('Cannot read property')) {
                errorMessage = `Expected data not found. Details: ${error.message.substring(0, 100)}`;
                errorCode = ErrorCode.InternalError;
            } else if (error.message.includes('timeout') || error.message.includes('ETIMEDOUT')) {
                errorMessage = `Request timed out. Try a smaller query or use LIMIT.`;
                errorCode = ErrorCode.InternalError;
            }

            throw new McpError(errorCode, errorMessage);
        }
    };
}
