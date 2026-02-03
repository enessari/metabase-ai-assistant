/**
 * Handlers Index
 * Central export point for all handler modules
 */

// Database operations
export * from './database.js';
export { default as databaseHandlers } from './database.js';

// Dashboard operations
export * from './dashboard.js';
export { default as dashboardHandlers } from './dashboard.js';

// Question/Chart operations
export * from './questions.js';
export { default as questionHandlers } from './questions.js';

// AI-powered operations
export * from './ai.js';
export { default as aiHandlers } from './ai.js';

/**
 * Create handler context from server instance
 * @param {object} server - MetabaseMCPServer instance
 * @returns {object} Handler context
 */
export function createHandlerContext(server) {
    return {
        metabaseClient: server.metabaseClient,
        aiAssistant: server.aiAssistant,
        activityLogger: server.activityLogger,
        metadataClient: server.metadataClient,
        connectionManager: server.connectionManager,
    };
}
