import { logger } from '../../utils/logger.js';

/**
 * BaseHandler
 * Provides unified context access, client injection, and lifecycle safety across all MCP handlers.
 */
export class BaseHandler {
  /**
   * @param {object|any} contextOrClient
   */
  constructor(contextOrClient) {
    if (contextOrClient && typeof contextOrClient === 'object' && contextOrClient.metabaseClient) {
      this.context = contextOrClient;
      this.metabaseClient = contextOrClient.metabaseClient;
      this.activityLogger = contextOrClient.activityLogger || null;
      this.aiAssistant = contextOrClient.aiAssistant || null;
      this.metadataClient = contextOrClient.metadataClient || null;
      this.connectionManager = contextOrClient.connectionManager || null;
      this.cache = contextOrClient.cache || null;
    } else {
      this.metabaseClient = contextOrClient;
      this.context = { metabaseClient: contextOrClient };
      this.activityLogger = null;
      this.aiAssistant = null;
      this.metadataClient = null;
      this.connectionManager = null;
      this.cache = null;
    }
  }

  /**
   * Safe no-op lifecycle guarantee.
   * Server-level ensureInitialized() handles main authentication before dispatch.
   */
  async ensureInitialized() {}

  /**
   * Return tool-to-method route map for dynamic dispatch
   * @returns {Object.<string, Function>}
   */
  routes() {
    return {};
  }
}
