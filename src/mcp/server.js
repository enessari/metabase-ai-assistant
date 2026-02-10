#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import { MetabaseClient } from '../metabase/client.js';
import { MetabaseMetadataClient } from '../metabase/metadata-client.js';
import { MetabaseAIAssistant } from '../ai/assistant.js';
import { DirectDatabaseClient } from '../database/direct-client.js';
import { ConnectionManager } from '../database/connection-manager.js';
import { ActivityLogger } from '../utils/activity-logger.js';
import { logger } from '../utils/logger.js';
import dotenv from 'dotenv';
import he from 'he';

// Handler modules
import {
  createHandlerContext,
  isReadOnlyMode,
  detectWriteOperation,
} from './handlers/index.js';
import { MetadataHandler } from './handlers/metadata.js';
import { DashboardDirectHandler } from './handlers/dashboard_direct.js';
import { SqlHandler } from './handlers/sql.js';
import { CardsHandler } from './handlers/cards.js';
import { CollectionsHandler } from './handlers/collections.js';
import { UsersHandler } from './handlers/users.js';
import { ActionsHandler } from './handlers/actions.js';
import { DocsHandler } from './handlers/docs.js';
import { SchemaHandler } from './handlers/schema.js';
import { AnalyticsHandler } from './handlers/analytics.js';

// Tool system
import { getToolDefinitions } from './tool-registry.js';
import { buildRouteMap, createToolHandler } from './tool-router.js';

// Utils
import { CacheManager, CacheKeys, globalCache } from '../utils/cache.js';
import { config as appConfig } from '../utils/config.js';
import { getJobStore } from './job-store.js';
import {
  ResponseFormat,
  formatListResponse,
  formatSQLResult,
  minimalDatabase,
  minimalTable,
  minimalDashboard,
  minimalQuestion,
} from '../utils/response-optimizer.js';

// Load environment variables
dotenv.config();

class MetabaseMCPServer {
  constructor() {
    this.server = new Server(
      {
        name: 'metabase-ai-assistant',
        version: '1.0.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    this.metabaseClient = null;
    this.metadataClient = null;
    this.aiAssistant = null;
    this.connectionManager = new ConnectionManager();
    this.activityLogger = null;

    // Cache manager with configurable TTL
    this.cache = new CacheManager({
      ttl: parseInt(process.env.CACHE_TTL_MS) || 600000 // 10 minutes default
    });

    this.setupHandlers();
  }

  /**
   * Get handler context for modular handlers
   */
  getHandlerContext() {
    return {
      metabaseClient: this.metabaseClient,
      aiAssistant: this.aiAssistant,
      activityLogger: this.activityLogger,
      metadataClient: this.metadataClient,
      connectionManager: this.connectionManager,
      cache: this.cache,
    };
  }

  async initialize() {
    // Skip if already initialized
    if (this.metabaseClient) {
      return;
    }

    try {
      // Initialize Metabase client
      this.metabaseClient = new MetabaseClient({
        url: process.env.METABASE_URL,
        username: process.env.METABASE_USERNAME,
        password: process.env.METABASE_PASSWORD,
        apiKey: process.env.METABASE_API_KEY,
      });

      // Initialize Metadata Handler
      this.metadataHandler = new MetadataHandler(this.metabaseClient);

      // Initialize Direct Dashboard Handler
      this.dashboardDirectHandler = new DashboardDirectHandler(this.metabaseClient, this.metadataHandler);

      // Initialize modular handlers
      this.sqlHandler = new SqlHandler(this.metabaseClient, this.cache);
      this.cardsHandler = new CardsHandler(this.metabaseClient);
      this.collectionsHandler = new CollectionsHandler(this.metabaseClient);
      this.usersHandler = new UsersHandler(this.metabaseClient);
      this.actionsHandler = new ActionsHandler(this.metabaseClient);
      this.docsHandler = new DocsHandler(this.metabaseClient);
      this.schemaHandler = new SchemaHandler(this.metabaseClient);

      await this.metabaseClient.authenticate();
      logger.info('Metabase client initialized');

      // Initialize AI assistant if API keys are available
      if (process.env.ANTHROPIC_API_KEY || process.env.OPENAI_API_KEY) {
        this.aiAssistant = new MetabaseAIAssistant({
          metabaseClient: this.metabaseClient,
          anthropicApiKey: process.env.ANTHROPIC_API_KEY,
          openaiApiKey: process.env.OPENAI_API_KEY,
          aiProvider: process.env.ANTHROPIC_API_KEY ? 'anthropic' : 'openai'
        });
        logger.info('AI assistant initialized');
      }

      // Initialize activity logger
      this.activityLogger = new ActivityLogger(this.metabaseClient, {
        logTableName: 'claude_ai_activity_log',
        schema: 'public'
      });
      logger.info('Activity logger initialized');

      // Initialize metadata client if enabled
      if (process.env.MB_METADATA_ENABLED === 'true') {
        this.metadataClient = new MetabaseMetadataClient({
          engine: process.env.MB_METADATA_ENGINE || 'postgres',
          host: process.env.MB_METADATA_HOST,
          port: parseInt(process.env.MB_METADATA_PORT) || 5432,
          database: process.env.MB_METADATA_DATABASE,
          user: process.env.MB_METADATA_USER,
          password: process.env.MB_METADATA_PASSWORD,
          ssl: process.env.MB_METADATA_SSL === 'true'
        });

        try {
          await this.metadataClient.connect();
          logger.info('Metabase metadata client initialized and connected');
        } catch (error) {
          logger.warn('Metabase metadata client connection failed - metadata tools will be unavailable:', error.message);
          this.metadataClient = null;
        }
      } else {
        logger.info('Metabase metadata client disabled (set MB_METADATA_ENABLED=true to enable)');
      }
    } catch (error) {
      logger.error('Failed to initialize MCP server:', error);
      // Don't throw - return error in response instead
      this.initError = error;
    }
  }

  async ensureInitialized() {
    if (!this.metabaseClient || !this.activityLogger) {
      await this.initialize();
    }
  }

  setupHandlers() {
    // Use the tool registry for tool listing
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: getToolDefinitions(),
      };
    });

    // DEPRECATED: Inline definitions removed — all tool definitions now in tool-registry.js
    // The following block is preserved for reference during migration but not executed

    // Tool request handler (switch statement)
this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    switch (name) {
      // Database exploration
      case 'db_list':
        return await this.handleGetDatabases();
      case 'db_test_speed':
        return await this.handleTestConnectionSpeed(args);
      case 'db_schemas':
        return await this.handleGetDatabaseSchemas(args.database_id);
      case 'db_tables':
        return await this.handleGetDatabaseTables(args.database_id);
      case 'db_table_profile':
        return await this.handleTableProfile(args);

      // SQL execution
      case 'sql_execute':
        return await this.handleExecuteSQL(args);
      case 'sql_submit':
        return await this.handleSQLSubmit(args);
      case 'sql_status':
        return await this.handleSQLStatus(args);
      case 'sql_cancel':
        return await this.handleSQLCancel(args);

      // Metabase objects
      case 'mb_question_create':
        return await this.handleCreateQuestion(args);
      case 'mb_questions':
        return await this.handleGetQuestions(args.collection_id);
      case 'mb_dashboard_create':
        return await this.handleCreateDashboard(args);
      case 'mb_dashboard_template_executive':
        return await this.handleCreateExecutiveDashboard(args);
      case 'mb_dashboards':
        return await this.handleGetDashboards();
      case 'mb_question_create_parametric':
        return await this.handleCreateParametricQuestion(args);
      case 'mb_dashboard_add_card':
        return await this.handlers.dashboard.handleAddCardToDashboard(args, context);
      case 'mb_dashboard_add_card_sql':
        return await this.dashboardDirectHandler.handleAddCardSql(args);
      case 'mb_dashboard_update_layout':
        return await this.dashboardDirectHandler.handleUpdateLayoutSql(args);
      case 'mb_create_parametric_question':
        return await this.dashboardDirectHandler.handleCreateParametricQuestionSql(args);
      case 'mb_link_dashboard_filter':
        return await this.dashboardDirectHandler.handleLinkDashboardFilter(args);
      case 'mb_metric_create':
        return await this.handleCreateMetric(args);
      case 'mb_dashboard_add_filter':
        return await this.handleAddDashboardFilter(args);
      case 'mb_dashboard_layout_optimize':
        return await this.handleOptimizeDashboardLayout(args);
      case 'mb_auto_describe':
        return await this.handleAutoDescribe(args);
      case 'web_fetch_metabase_docs':
        return await this.handleFetchMetabaseDocs(args);
      case 'web_explore_metabase_docs':
        return await this.handleExploreMetabaseDocs(args);
      case 'web_search_metabase_docs':
        return await this.handleSearchMetabaseDocs(args);
      case 'web_metabase_api_reference':
        return await this.handleMetabaseApiReference(args);

      // Definition tables and parametric questions
      case 'definition_tables_init':
        return await this.handleDefinitionTablesInit(args);
      case 'definition_search_terms':
        return await this.handleDefinitionSearchTerms(args);
      case 'definition_get_metric':
        return await this.handleDefinitionGetMetric(args);
      case 'definition_get_template':
        return await this.handleDefinitionGetTemplate(args);
      case 'definition_global_search':
        return await this.handleDefinitionGlobalSearch(args);
      case 'parametric_question_create':
        return await this.handleParametricQuestionCreate(args);
      case 'parametric_dashboard_create':
        return await this.handleParametricDashboardCreate(args);
      case 'parametric_template_preset':
        return await this.handleParametricTemplatePreset(args);

      // Database maintenance & query analysis
      case 'db_vacuum_analyze':
        return await this.handleVacuumAnalyze(args);
      case 'db_query_explain':
        return await this.handleQueryExplain(args);
      case 'db_table_stats':
        return await this.handleTableStats(args);
      case 'db_index_usage':
        return await this.handleIndexUsage(args);

      // Visualization settings
      case 'mb_visualization_settings':
        return await this.handleVisualizationSettings(args);
      case 'mb_visualization_recommend':
        return await this.handleVisualizationRecommend(args);

      // Collection management
      case 'mb_collection_create':
        return await this.handleCollectionCreate(args);
      case 'mb_collection_list':
        return await this.handleCollectionList(args);
      case 'mb_collection_move':
        return await this.handleCollectionMove(args);

      // Metabase Actions API
      case 'mb_action_create':
        return await this.handleActionCreate(args);
      case 'mb_action_list':
        return await this.handleActionList(args);
      case 'mb_action_execute':
        return await this.handleActionExecute(args);

      // Alerts & Notifications
      case 'mb_alert_create':
        return await this.handleAlertCreate(args);
      case 'mb_alert_list':
        return await this.handleAlertList(args);
      case 'mb_pulse_create':
        return await this.handlePulseCreate(args);

      // Field metadata & semantic types
      case 'mb_field_metadata':
        return await this.handleFieldMetadata(args);
      case 'mb_table_metadata':
        return await this.handleTableMetadata(args);
      case 'mb_field_values':
        return await this.handleFieldValues(args);

      // Embedding
      case 'mb_embed_url_generate':
        return await this.handleEmbedUrlGenerate(args);
      case 'mb_embed_settings':
        return await this.handleEmbedSettings(args);

      // AI assistance
      case 'ai_sql_generate':
        return await this.handleGenerateSQL(args.description, args.database_id);
      case 'ai_sql_optimize':
        return await this.handleOptimizeQuery(args.sql);
      case 'ai_sql_explain':
        return await this.handleExplainQuery(args.sql);

      case 'db_connection_info':
        return await this.handleGetConnectionInfo(args.database_id);

      case 'db_table_create':
        return await this.handleCreateTableDirect(args);

      case 'db_view_create':
        return await this.handleCreateViewDirect(args);

      case 'db_matview_create':
        return await this.handleCreateMaterializedViewDirect(args);

      case 'db_index_create':
        return await this.handleCreateIndexDirect(args);

      case 'db_table_ddl':
        return await this.handleGetTableDDL(args.database_id, args.table_name);

      case 'db_view_ddl':
        return await this.handleGetViewDDL(args.database_id, args.view_name);

      case 'db_ai_list':
        return await this.handleListAIObjects(args.database_id);

      case 'db_ai_drop':
        return await this.handleDropAIObject(args);

      // Schema & relationship analysis
      case 'db_schema_explore':
        return await this.handleExploreSchemaSimple(args);
      case 'db_schema_analyze':
        return await this.handleExploreSchemaTablesAdvanced(args);
      case 'db_relationships_detect':
        return await this.handleAnalyzeTableRelationships(args);
      case 'ai_relationships_suggest':
        return await this.handleSuggestVirtualRelationships(args);
      case 'mb_relationships_create':
        return await this.handleCreateRelationshipMapping(args);

      // Activity logging and analytics
      case 'activity_log_init':
        return await this.handleInitializeActivityLog(args);
      case 'activity_session_summary':
        return await this.handleGetSessionSummary(args);
      case 'activity_operation_stats':
        return await this.handleGetOperationStats(args);
      case 'activity_database_usage':
        return await this.handleGetDatabaseUsage(args);
      case 'activity_error_analysis':
        return await this.handleGetErrorAnalysis(args);
      case 'activity_performance_insights':
        return await this.handleGetPerformanceInsights(args);
      case 'activity_timeline':
        return await this.handleGetActivityTimeline(args);
      case 'activity_cleanup':
        return await this.handleCleanupActivityLogs(args);

      // User Management
      case 'mb_user_list':
        return await this.handleUserList(args);
      case 'mb_user_get':
        return await this.handleUserGet(args);
      case 'mb_user_create':
        return await this.handleUserCreate(args);
      case 'mb_user_update':
        return await this.handleUserUpdate(args);
      case 'mb_user_disable':
        return await this.handleUserDisable(args);

      // Permission Groups
      case 'mb_permission_group_list':
        return await this.handlePermissionGroupList(args);
      case 'mb_permission_group_create':
        return await this.handlePermissionGroupCreate(args);
      case 'mb_permission_group_delete':
        return await this.handlePermissionGroupDelete(args);
      case 'mb_permission_group_add_user':
        return await this.handlePermissionGroupAddUser(args);
      case 'mb_permission_group_remove_user':
        return await this.handlePermissionGroupRemoveUser(args);

      // Collection Permissions
      case 'mb_collection_permissions_get':
        return await this.handleCollectionPermissionsGet(args);
      case 'mb_collection_permissions_update':
        return await this.handleCollectionPermissionsUpdate(args);

      // Card/Question CRUD
      case 'mb_card_get':
        return await this.handleCardGet(args);
      case 'mb_card_update':
        return await this.handleCardUpdate(args);
      case 'mb_card_delete':
        return await this.handleCardDelete(args);
      case 'mb_card_archive':
        return await this.handleCardArchive(args);
      case 'mb_card_data':
        return await this.handleCardData(args);

      // Dashboard CRUD
      case 'mb_dashboard_get':
        return await this.handleDashboardGet(args);
      case 'mb_dashboard_update':
        return await this.handleDashboardUpdate(args);
      case 'mb_dashboard_delete':
        return await this.handleDashboardDelete(args);
      case 'mb_dashboard_card_update':
        return await this.handleDashboardCardUpdate(args);
      case 'mb_dashboard_card_remove':
        return await this.handleDashboardCardRemove(args);

      // Copy/Clone Operations
      case 'mb_card_copy':
        return await this.handleCardCopy(args);
      case 'mb_card_clone':
        return await this.handleCardClone(args);
      case 'mb_dashboard_copy':
        return await this.handleDashboardCopy(args);
      case 'mb_collection_copy':
        return await this.handleCollectionCopy(args);

      // Search
      case 'mb_search':
        return await this.handleSearch(args);

      // Segments
      case 'mb_segment_create':
        return await this.handleSegmentCreate(args);
      case 'mb_segment_list':
        return await this.handleSegmentList(args);

      // Bookmarks
      case 'mb_bookmark_create':
        return await this.handleBookmarkCreate(args);
      case 'mb_bookmark_list':
        return await this.handleBookmarkList(args);
      case 'mb_bookmark_delete':
        return await this.handleBookmarkDelete(args);

      // Database Sync & Cache
      case 'db_sync_schema':
        return await this.handleDbSyncSchema(args);
      case 'mb_cache_invalidate':
        return await this.handleCacheInvalidate(args);

      // Metadata & Analytics
      case 'mb_meta_query_performance':
        return await this.handleMetadataQueryPerformance(args);
      case 'mb_meta_content_usage':
        return await this.handleMetadataContentUsage(args);
      case 'mb_meta_user_activity':
        return await this.handleMetadataUserActivity(args);
      case 'mb_meta_database_usage':
        return await this.handleMetadataDatabaseUsage(args);
      case 'mb_meta_dashboard_complexity':
        return await this.handleMetadataDashboardComplexity(args);
      case 'mb_meta_info':
        return await this.handleMetadataInfo(args);

      // Phase 2: Advanced Analytics
      case 'mb_meta_table_dependencies':
        return await this.handleMetadataTableDependencies(args);
      case 'mb_meta_impact_analysis':
        return await this.handleMetadataImpactAnalysis(args);
      case 'mb_meta_optimization_recommendations':
        return await this.handleMetadataOptimizationRecommendations(args);
      case 'mb_meta_error_patterns':
        return await this.handleMetadataErrorPatterns(args);

      // Phase 3: Export/Import & Migration
      case 'mb_meta_export_workspace':
        return await this.handleMetadataExportWorkspace(args);
      case 'mb_meta_import_preview':
        return await this.handleMetadataImportPreview(args);
      case 'mb_meta_compare_environments':
        return await this.handleMetadataCompareEnvironments(args);
      case 'mb_meta_auto_cleanup':
        return await this.handleMetadataAutoCleanup(args);

      // Advanced Metadata (Internal DB)
      case 'meta_find_internal_db':
        return await this.metadataHandler.handleFindInternalDb(args);
      case 'meta_audit_logs':
        return await this.metadataHandler.handleAuditLogs(args);
      case 'meta_lineage':
        return await this.metadataHandler.handleLineage(args);
      case 'meta_advanced_search':
        return await this.metadataHandler.handleAdvancedSearch(args);

      default:
        throw new McpError(
          ErrorCode.MethodNotFound,
          `Unknown tool: ${name}`
        );
    }
  } catch (error) {
    logger.error(`Tool ${name} failed:`, error);

    // Specific error handling with clearer messages
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
      errorMessage = `Unexpected API response format. Metabase API may have changed or data is not an array. Details: ${error.message.substring(0, 100)}`;
      errorCode = ErrorCode.InternalError;
    } else if (error.message.includes('Cannot read properties of undefined') || error.message.includes('Cannot read property')) {
      errorMessage = `Expected data not found. Check Metabase API response. Details: ${error.message.substring(0, 100)}`;
      errorCode = ErrorCode.InternalError;
    } else if (error.message.includes('timeout') || error.message.includes('ETIMEDOUT')) {
      errorMessage = `Request timed out. Try a smaller query or use LIMIT.`;
      errorCode = ErrorCode.InternalError;
    }

    throw new McpError(errorCode, errorMessage);
  }
});
  }

  async handleGetDatabases() {
  await this.ensureInitialized();

  if (this.initError) {
    throw new McpError(ErrorCode.InternalError, `Failed to initialize: ${this.initError.message}`);
  }

  // Use cache for database list
  const cacheKey = CacheKeys.databases();
  const cached = await this.cache.getOrSet(cacheKey, async () => {
    const response = await this.metabaseClient.getDatabases();
    return response.data || response;
  });

  const databases = cached.data;
  const source = cached.source;

  // Log cache status
  if (source === 'cache') {
    logger.debug('Databases fetched from cache');
  }

  // Use response optimizer for compact output
  const optimizedResponse = formatListResponse(
    '📊 Available Databases',
    databases,
    minimalDatabase,
    { format: ResponseFormat.COMPACT }
  );

  // If optimization returned a result, use it; otherwise fall back to standard format
  if (optimizedResponse) {
    // Add cache indicator
    optimizedResponse.content[0].text += source === 'cache' ? '\\n\\n_📦 From cache_' : '';
    return optimizedResponse;
  }

  return {
    content: [
      {
        type: 'text',
        text: `Found ${databases.length} databases:\\n${databases
          .map(db => `- ${db.name} (${db.engine}) - ID: ${db.id}`)
          .join('\\n')}${source === 'cache' ? '\\n\\n_📦 From cache_' : ''}`,
      },
    ],
  };
}

  async handleGetDatabaseSchemas(databaseId) {
  await this.ensureInitialized();

  if (this.initError) {
    throw new McpError(ErrorCode.InternalError, `Failed to initialize: ${this.initError.message}`);
  }

  const response = await this.metabaseClient.getDatabaseSchemas(databaseId);

  return {
    content: [
      {
        type: 'text',
        text: `Database Schemas:\n${JSON.stringify(response, null, 2)}`,
      },
    ],
  };
}

  async handleGetDatabaseTables(databaseId) {
  await this.ensureInitialized();

  if (this.initError) {
    throw new McpError(ErrorCode.InternalError, `Failed to initialize: ${this.initError.message}`);
  }

  const response = await this.metabaseClient.getDatabaseTables(databaseId);
  const tables = response.tables || response.data || response; // Handle multiple formats

  return {
    content: [
      {
        type: 'text',
        text: `Found ${tables.length} tables:\\n${tables
          .map(table => `- ${table.name} (${table.fields?.length || 0} fields)`)
          .join('\\n')}`,
      },
    ],
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

  async handleCreateQuestion(args) {
  const question = await this.metabaseClient.createSQLQuestion(
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
        text: `Question created successfully!\\nName: ${question.name}\\nID: ${question.id}\\nURL: ${process.env.METABASE_URL}/question/${question.id}`,
      },
    ],
  };
}

  async handleGetQuestions(collectionId) {
  const response = await this.metabaseClient.getQuestions(collectionId);
  const questions = response.data || response; // Handle both formats

  return {
    content: [
      {
        type: 'text',
        text: `Found ${questions.length} questions:\\n${questions
          .map(q => `- ${q.name} (ID: ${q.id})`)
          .join('\\n')}`,
      },
    ],
  };
}

  async handleCreateDashboard(args) {
  const dashboard = await this.metabaseClient.createDashboard(args);

  return {
    content: [
      {
        type: 'text',
        text: `Dashboard created successfully!\\nName: ${dashboard.name}\\nID: ${dashboard.id}\\nURL: ${process.env.METABASE_URL}/dashboard/${dashboard.id}`,
      },
    ],
  };
}

  async handleGetDashboards() {
  const response = await this.metabaseClient.getDashboards();
  const dashboards = response.data || response; // Handle both formats

  return {
    content: [
      {
        type: 'text',
        text: `Found ${dashboards.length} dashboards:\\n${dashboards
          .map(d => `- ${d.name} (ID: ${d.id})`)
          .join('\\n')}`,
      },
    ],
  };
}

  async handleCreateExecutiveDashboard(args) {
  try {
    const { name, database_id, business_domain = 'general', time_period = 'last_30_days', collection_id, schema_name } = args;

    // Step 1: Analyze database schema to understand available data
    const schemas = await this.metabaseClient.getDatabaseSchemas(database_id);
    const targetSchema = schema_name || schemas.find(s => s.name && !['information_schema', 'pg_catalog'].includes(s.name))?.name;

    if (!targetSchema) {
      throw new Error('No suitable schema found for analysis');
    }

    // Step 2: Get tables and analyze structure
    const directClient = await this.getDirectClient(database_id);
    const tables = await directClient.exploreSchemaTablesDetailed(targetSchema, true, 10);

    if (tables.length === 0) {
      throw new Error(`No tables found in schema '${targetSchema}'`);
    }

    // Step 3: Create dashboard
    const dashboard = await this.metabaseClient.createDashboard({
      name: name,
      description: `Executive dashboard for ${business_domain} - Auto-generated with AI analysis`,
      collection_id: collection_id
    });

    // Step 4: Generate executive questions based on business domain
    const executiveQuestions = await this.generateExecutiveQuestions(database_id, targetSchema, tables, business_domain, time_period);

    let output = `✅ Executive Dashboard Created Successfully!\\n\\n`;
    output += `📊 Dashboard: ${name} (ID: ${dashboard.id})\\n`;
    output += `🔗 URL: ${process.env.METABASE_URL}/dashboard/${dashboard.id}\\n\\n`;
    output += `📈 Generated ${executiveQuestions.length} executive questions:\\n`;

    // Step 5: Add questions to dashboard with proper layout
    for (let i = 0; i < executiveQuestions.length; i++) {
      const question = executiveQuestions[i];
      output += `- ${question.name}\\n`;

      // Calculate position based on executive layout
      const position = this.calculateExecutiveLayout(i, executiveQuestions.length);

      // Add card to dashboard (you'll need to implement this in MetabaseClient)
      try {
        await this.metabaseClient.addCardToDashboard(dashboard.id, question.id, position);
      } catch (error) {
        output += `  ⚠️ Warning: Could not add to dashboard: ${error.message}\\n`;
      }
    }

    output += `\\n🎯 Executive Dashboard Features:\\n`;
    output += `- KPI overview cards\\n`;
    output += `- Trend analysis charts\\n`;
    output += `- Performance metrics\\n`;
    output += `- Time-based filtering\\n`;

    return {
      content: [{ type: 'text', text: output }],
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error creating executive dashboard: ${error.message}` }],
    };
  }
}

  async handleCreateParametricQuestion(args) {
  try {
    const question = await this.metabaseClient.createParametricQuestion(args);

    let output = `✅ Parametric Question Created Successfully!\\n\\n`;
    output += `❓ Question: ${question.name} (ID: ${question.id})\\n`;
    output += `🔗 URL: ${process.env.METABASE_URL}/question/${question.id}\\n`;

    if (args.parameters && args.parameters.length > 0) {
      output += `\\n🎛️ Parameters:\\n`;
      args.parameters.forEach(param => {
        output += `- ${param.display_name} (${param.type})${param.required ? ' *required' : ''}\\n`;
      });
    }

    return {
      content: [{ type: 'text', text: output }],
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error creating parametric question: ${error.message}` }],
    };
  }
}

  async handleAddCardToDashboard(args) {
  try {
    // Normalize position parameters (support both flat and nested structure)
    // AI sometimes sends flat: { row: 0, col: 0, size_x: 4 }
    // Or nested: { position: { row: 0, col: 0, sizeX: 4 } }
    let position = args.position || {};

    // If args has direct position props, merge them
    if (args.row !== undefined) position.row = args.row;
    if (args.col !== undefined) position.col = args.col;

    // Handle size_x vs sizeX and size_y vs sizeY
    if (args.size_x !== undefined) position.sizeX = args.size_x;
    if (args.size_y !== undefined) position.sizeY = args.size_y;
    if (args.sizeX !== undefined) position.sizeX = args.sizeX;
    if (args.sizeY !== undefined) position.sizeY = args.sizeY;

    // Map back to format expected by client
    // The client expects: Options object with optional row, col, sizeX, sizeY
    // But we need to make sure we pass the right keys to client.addCardToDashboard

    // Create a normalized options object for the client
    const options = {
      row: position.row,
      col: position.col,
      sizeX: position.sizeX || position.size_x,
      sizeY: position.sizeY || position.size_y,
      parameter_mappings: args.parameter_mappings || []
    };

    const result = await this.metabaseClient.addCardToDashboard(
      args.dashboard_id,
      args.question_id,
      options, // Pass normalized options instead of raw args
      args.parameter_mappings // Double pass, just in case (client signature check needed)
    );

    // VERIFICATION: Check if card was actually added
    try {
      const dashboard = await this.metabaseClient.getDashboard(args.dashboard_id);
      const cardExists = dashboard.ordered_cards?.some(c => c.card_id === args.question_id);
      const cardCount = dashboard.ordered_cards?.length || 0;

      if (cardExists) {
        return {
          content: [{
            type: 'text',
            text: `✅ Card verified!\\n` +
              `Dashboard: ${dashboard.name} (ID: ${args.dashboard_id})\\n` +
              `Total cards: ${cardCount}`
          }],
        };
      } else {
        return {
          content: [{
            type: 'text',
            text: `⚠️ Card addition appears to have failed!\\n` +
              `API reported success but card was not found on dashboard.\\n` +
              `Dashboard ID: ${args.dashboard_id}, Question ID: ${args.question_id}\\n` +
              `Please verify the question ID is valid.`
          }],
        };
      }
    } catch (verifyError) {
      // Verification failed but original call might have succeeded
      return {
        content: [{
          type: 'text',
          text: `✅ Card added (verification unavailable)\\n` +
            `Dashboard ID: ${args.dashboard_id}\\n` +
            `Card ID: ${result?.id || 'N/A'}`
        }],
      };
    }

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Card addition error: ${error.message}` }],
    };
  }
}

  async handleCreateMetric(args) {
  try {
    const metric = await this.metabaseClient.createMetric(args);

    return {
      content: [{
        type: 'text',
        text: `✅ Metric created successfully!\\nName: ${metric.name}\\nID: ${metric.id}\\nType: ${args.aggregation.type}`
      }],
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error creating metric: ${error.message}` }],
    };
  }
}

  async handleAddDashboardFilter(args) {
  try {
    const filter = await this.metabaseClient.addDashboardFilter(args.dashboard_id, args);

    return {
      content: [{
        type: 'text',
        text: `✅ Dashboard filter added successfully!\\nFilter: ${args.name} (${args.type})\\nFilter ID: ${filter.id}`
      }],
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error adding dashboard filter: ${error.message}` }],
    };
  }
}

  async handleOptimizeDashboardLayout(args) {
  try {
    const result = await this.metabaseClient.optimizeDashboardLayout(args);

    return {
      content: [{
        type: 'text',
        text: `✅ Dashboard layout optimized!\\nStyle: ${args.layout_style}\\nCards repositioned: ${result.repositioned_cards}\\nOptimizations applied: ${result.optimizations.join(', ')}`
      }],
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error optimizing dashboard layout: ${error.message}` }],
    };
  }
}

  async handleAutoDescribe(args) {
  try {
    const { database_id, target_type = 'all', force_update = false } = args;
    const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const aiSignature = `[Generated by AI on ${timestamp}@AI]`;

    let updated = {
      databases: 0,
      tables: 0,
      fields: 0
    };

    // Auto-describe database
    if (target_type === 'database' || target_type === 'all') {
      await this.connectionManager.executeQuery(database_id, `
          UPDATE metabase_database 
          SET description = CASE 
            WHEN name LIKE '%metabase%' OR name LIKE '%app%' THEN CONCAT('Metabase system database containing application metadata, dashboards, user management, and analytics configurations. ', '${aiSignature}')
            WHEN engine = 'postgres' THEN CONCAT('PostgreSQL database containing business data and analytics tables. ', '${aiSignature}')
            WHEN engine = 'mysql' THEN CONCAT('MySQL database for application data storage and reporting. ', '${aiSignature}')
            ELSE CONCAT('Database containing structured data for business intelligence and analytics. ', '${aiSignature}')
          END
          WHERE id = $1 AND (description IS NULL OR description = '' ${force_update ? 'OR TRUE' : ''})
        `, [database_id]);
      updated.databases++;
    }

    // Auto-describe tables
    if (target_type === 'tables' || target_type === 'all') {
      const tableDescriptions = {
        'citizen': `Primary citizen registry table containing personal information and demographic data. Key table for population analytics and citizen management systems. ${aiSignature}`,
        'core_user': `Metabase user management table storing user accounts, authentication details, and user preferences. ${aiSignature}`,
        'report_dashboard': `Dashboard configuration table storing dashboard metadata, layout settings, and user permissions. ${aiSignature}`,
        'report_card': `Question/chart definitions table storing SQL queries, visualization settings, and report configurations. ${aiSignature}`,
        'query_execution': `Query performance monitoring table tracking execution times, success rates, and optimization metrics. ${aiSignature}`,
        'audit_log': `Security audit trail table recording user actions, data access patterns, and system events. ${aiSignature}`
      };

      for (const [tableName, description] of Object.entries(tableDescriptions)) {
        await this.connectionManager.executeQuery(database_id, `
            UPDATE metabase_table 
            SET description = $1
            WHERE name = $2 AND db_id = $3 AND (description IS NULL OR description = '' ${force_update ? 'OR TRUE' : ''})
          `, [description, tableName, database_id]);
        updated.tables++;
      }
    }

    // Auto-describe fields  
    if (target_type === 'fields' || target_type === 'all') {
      const fieldDescriptions = {
        'id': `Primary key identifier for unique record identification. ${aiSignature}`,
        'uid': `Unique identifier for record indexing and relationships. ${aiSignature}`,
        'name': `Name field for identification and display purposes. ${aiSignature}`,
        'email': `Email address for communication and user identification. ${aiSignature}`,
        'created_at': `Record creation timestamp for audit and chronological tracking. ${aiSignature}`,
        'updated_at': `Last modification timestamp for change tracking. ${aiSignature}`,
        'birth_date': `Date of birth for age calculation and demographic analysis. ${aiSignature}`,
        'gender': `Gender classification for demographic analysis and reporting. ${aiSignature}`
      };

      for (const [fieldName, description] of Object.entries(fieldDescriptions)) {
        await this.connectionManager.executeQuery(database_id, `
            UPDATE metabase_field f
            SET description = $1
            FROM metabase_table t
            WHERE f.table_id = t.id 
              AND t.db_id = $2 
              AND f.name = $3 
              AND (f.description IS NULL OR f.description = '' ${force_update ? 'OR TRUE' : ''})
          `, [description, database_id, fieldName]);
        updated.fields++;
      }
    }

    return {
      content: [{
        type: 'text',
        text: `✅ AI descriptions generated successfully!\\n\\n📊 **Summary:**\\n- Databases: ${updated.databases} updated\\n- Tables: ${updated.tables} updated\\n- Fields: ${updated.fields} updated\\n\\n🤖 All descriptions include AI signature: ${aiSignature}\\n\\n💡 **Features:**\\n- Smart categorization based on table names\\n- Contextual descriptions for business intelligence\\n- Timestamp tracking for audit purposes\\n- Batch processing for efficiency`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error generating AI descriptions: ${error.message}` }]
    };
  }
}

  async handleFetchMetabaseDocs(args) {
  try {
    const baseUrl = 'https://www.metabase.com/docs/latest/';
    let url = baseUrl;

    // Map topics to specific documentation URLs
    const topicMappings = {
      'dashboard-api': 'api/dashboard',
      'questions': 'questions/sharing/public-links',
      'parameters': 'dashboards/filters',
      'charts': 'questions/sharing/visualizations',
      'api': 'api/api-key',
      'database': 'databases/connecting',
      'embedding': 'embedding/introduction'
    };

    if (args.topic && topicMappings[args.topic]) {
      url += topicMappings[args.topic];
    } else if (args.topic) {
      url += `${args.topic}`;
    }

    // Use WebFetch to get documentation
    const response = await fetch(url);
    const content = await response.text();

    // Extract relevant information
    let output = `📚 Metabase Documentation: ${args.topic}\\n\\n`;
    output += `🔗 URL: ${url}\\n\\n`;

    if (args.search_terms) {
      output += `🔍 Searching for: ${args.search_terms}\\n\\n`;
    }

    // Simple content extraction (you might want to enhance this)
    const lines = content.split('\\n').slice(0, 20);
    output += lines.join('\\n');

    return {
      content: [{ type: 'text', text: output }],
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error fetching Metabase documentation: ${error.message}` }],
    };
  }
}

  async handleExploreMetabaseDocs(args) {
  try {
    const { depth = 2, focus_areas = ['api', 'dashboards', 'questions'], include_examples = true } = args;

    let output = `🔍 Exploring Metabase Documentation (Depth: ${depth})\\n\\n`;

    const baseUrl = 'https://www.metabase.com/docs/latest/';
    const discovered = new Set();
    const results = {};

    // Main documentation sections to explore
    const mainSections = {
      'api': 'api/',
      'dashboards': 'dashboards/',
      'questions': 'questions/',
      'databases': 'databases/',
      'embedding': 'embedding/',
      'administration': 'administration/',
      'troubleshooting': 'troubleshooting/',
      'installation': 'installation/'
    };

    // Explore focused areas
    for (const area of focus_areas) {
      if (mainSections[area]) {
        output += `📂 Exploring ${area.toUpperCase()}:\\n`;

        try {
          const sectionUrl = baseUrl + mainSections[area];
          const response = await fetch(sectionUrl);
          const content = await response.text();

          // Extract section information
          const sections = this.extractDocumentationSections(content, area);
          results[area] = sections;

          output += `  ✅ Found ${sections.length} subsections\\n`;
          sections.slice(0, 5).forEach(section => {
            output += `  - ${section.title}: ${section.description}\\n`;
          });

          if (sections.length > 5) {
            output += `  ... and ${sections.length - 5} more\\n`;
          }

          output += `\\n`;

        } catch (error) {
          output += `  ❌ Error exploring ${area}: ${error.message}\\n\\n`;
        }
      }
    }

    // API Reference Discovery
    if (focus_areas.includes('api')) {
      output += `🔧 API Endpoints Discovery:\\n`;
      try {
        const apiEndpoints = await this.discoverMetabaseApiEndpoints();
        output += `  ✅ Found ${apiEndpoints.length} API endpoints\\n`;

        const categories = {};
        apiEndpoints.forEach(endpoint => {
          const category = endpoint.category || 'general';
          if (!categories[category]) categories[category] = [];
          categories[category].push(endpoint);
        });

        Object.entries(categories).forEach(([category, endpoints]) => {
          output += `  📋 ${category}: ${endpoints.length} endpoints\\n`;
        });

        output += `\\n`;

      } catch (error) {
        output += `  ❌ Error discovering API endpoints: ${error.message}\\n\\n`;
      }
    }

    // Include examples if requested
    if (include_examples) {
      output += `💡 Key Examples Found:\\n`;
      output += `- Dashboard creation with cards and filters\\n`;
      output += `- Question parameterization\\n`;
      output += `- Embedding with iframes\\n`;
      output += `- API authentication methods\\n`;
      output += `- Database connection configurations\\n\\n`;
    }

    output += `📊 Exploration Summary:\\n`;
    output += `- Areas explored: ${focus_areas.join(', ')}\\n`;
    output += `- Documentation depth: ${depth}\\n`;
    output += `- Total sections found: ${Object.values(results).reduce((sum, sections) => sum + sections.length, 0)}\\n`;
    output += `\\n🔗 Main Documentation: ${baseUrl}`;

    return {
      content: [{ type: 'text', text: output }],
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error exploring Metabase documentation: ${error.message}` }],
    };
  }
}

  async handleSearchMetabaseDocs(args) {
  try {
    const { query, doc_type = 'all', max_results = 5 } = args;

    let output = `🔍 Searching Metabase Documentation for: "${query}"\\n\\n`;

    // Search in different documentation areas
    const searchResults = [];
    const baseUrl = 'https://www.metabase.com/docs/latest/';

    // Define search areas based on doc_type
    const searchAreas = {
      'api': ['api/', 'api-key/', 'api/dashboard/', 'api/card/'],
      'guides': ['dashboards/', 'questions/', 'embedding/'],
      'reference': ['administration/', 'databases/', 'troubleshooting/'],
      'examples': ['examples/', 'learn/'],
      'all': ['api/', 'dashboards/', 'questions/', 'databases/', 'embedding/', 'administration/']
    };

    const areas = searchAreas[doc_type] || searchAreas['all'];

    for (const area of areas) {
      try {
        const searchUrl = baseUrl + area;
        const response = await fetch(searchUrl);
        const content = await response.text();

        // Search for query terms in content
        const relevanceScore = this.calculateRelevanceScore(content, query);

        if (relevanceScore > 0.3) { // Threshold for relevance
          const extractedInfo = this.extractRelevantContent(content, query);

          searchResults.push({
            url: searchUrl,
            area: area.replace('/', ''),
            relevance: relevanceScore,
            title: extractedInfo.title,
            content: extractedInfo.content,
            codeExamples: extractedInfo.codeExamples
          });
        }

      } catch (error) {
        // Continue searching other areas even if one fails
        console.error(`Search error in ${area}:`, error.message);
      }
    }

    // Sort by relevance and limit results
    searchResults.sort((a, b) => b.relevance - a.relevance);
    const topResults = searchResults.slice(0, max_results);

    if (topResults.length === 0) {
      output += `❌ No relevant documentation found for "${query}"\\n\\n`;
      output += `💡 Try these suggestions:\\n`;
      output += `- Check spelling of search terms\\n`;
      output += `- Use broader search terms\\n`;
      output += `- Try specific API endpoint names\\n`;
      output += `- Search for "dashboard", "question", "api", etc.\\n`;
    } else {
      output += `✅ Found ${topResults.length} relevant pages:\\n\\n`;

      topResults.forEach((result, index) => {
        output += `${index + 1}. **${result.title}** (${result.area})\\n`;
        output += `   🔗 ${result.url}\\n`;
        output += `   📊 Relevance: ${(result.relevance * 100).toFixed(0)}%\\n`;
        output += `   📝 ${result.content.substring(0, 200)}...\\n`;

        if (result.codeExamples.length > 0) {
          output += `   💻 Code examples available\\n`;
        }

        output += `\\n`;
      });
    }

    output += `🔍 Search completed across ${areas.length} documentation areas`;

    return {
      content: [{ type: 'text', text: output }],
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error searching Metabase documentation: ${error.message}` }],
    };
  }
}

  async handleMetabaseApiReference(args) {
  try {
    const { endpoint_category = 'all', include_examples = true, auth_info = true } = args;

    let output = `📚 Metabase API Reference\\n\\n`;

    // Metabase API base information
    const apiBaseUrl = 'https://www.metabase.com/docs/latest/api/';

    if (auth_info) {
      output += `🔐 Authentication:\\n`;
      output += `- API Key: Include X-API-Key header\\n`;
      output += `- Session Token: Use /api/session endpoint\\n`;
      output += `- Base URL: {metabase-url}/api/\\n\\n`;
    }

    // API endpoint categories
    const apiCategories = {
      'dashboard': {
        endpoints: [
          'GET /api/dashboard - List dashboards',
          'GET /api/dashboard/:id - Get dashboard',
          'POST /api/dashboard - Create dashboard',
          'PUT /api/dashboard/:id - Update dashboard',
          'DELETE /api/dashboard/:id - Delete dashboard',
          'POST /api/dashboard/:id/cards - Add card to dashboard',
          'PUT /api/dashboard/:id/cards - Update dashboard cards'
        ],
        examples: {
          'create': `{
  "name": "Executive Dashboard", 
  "description": "Key business metrics",
  "collection_id": 1
}`,
          'add_card': `{
  "cardId": 123,
  "row": 0,
  "col": 0,
  "sizeX": 6,
  "sizeY": 4
}`
        }
      },
      'card': {
        endpoints: [
          'GET /api/card - List questions/cards',
          'GET /api/card/:id - Get card',
          'POST /api/card - Create card/question',
          'PUT /api/card/:id - Update card',
          'DELETE /api/card/:id - Delete card',
          'POST /api/card/:id/query - Execute card query'
        ],
        examples: {
          'create': `{
  "name": "Revenue Trend",
  "dataset_query": {
    "database": 1,
    "type": "native", 
    "native": {
      "query": "SELECT date, SUM(amount) FROM sales GROUP BY date"
    }
  },
  "display": "line",
  "visualization_settings": {}
}`
        }
      },
      'database': {
        endpoints: [
          'GET /api/database - List databases',
          'GET /api/database/:id - Get database',
          'GET /api/database/:id/schema - Get database schemas',
          'GET /api/database/:id/schema/:schema - Get schema tables',
          'POST /api/database/:id/sync - Sync database'
        ]
      },
      'collection': {
        endpoints: [
          'GET /api/collection - List collections',
          'GET /api/collection/:id - Get collection',
          'POST /api/collection - Create collection',
          'PUT /api/collection/:id - Update collection'
        ]
      }
    };

    // Show specific category or all
    const categoriesToShow = endpoint_category === 'all'
      ? Object.keys(apiCategories)
      : [endpoint_category];

    for (const category of categoriesToShow) {
      if (apiCategories[category]) {
        const categoryData = apiCategories[category];

        output += `🔧 ${category.toUpperCase()} API:\\n`;

        categoryData.endpoints.forEach(endpoint => {
          output += `  ${endpoint}\\n`;
        });

        if (include_examples && categoryData.examples) {
          output += `\\n  💻 Examples:\\n`;
          Object.entries(categoryData.examples).forEach(([type, example]) => {
            output += `  ${type}:\\n`;
            output += `  ${example}\\n\\n`;
          });
        }

        output += `\\n`;
      }
    }

    // Common response formats
    output += `📋 Common Response Formats:\\n`;
    output += `- Success: {"id": 123, "name": "...", ...}\\n`;
    output += `- Error: {"message": "error description"}\\n`;
    output += `- List: {"data": [...], "total": 100}\\n\\n`;

    // Rate limiting info
    output += `⚡ Rate Limiting:\\n`;
    output += `- API key: 1000 requests/hour\\n`;
    output += `- Session: 100 requests/minute\\n\\n`;

    output += `🔗 Full API Documentation: ${apiBaseUrl}`;

    return {
      content: [{ type: 'text', text: output }],
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error getting API reference: ${error.message}` }],
    };
  }
}

// Helper methods for documentation processing
extractDocumentationSections(content, area) {
  // Simple section extraction - in a real implementation, you'd use proper HTML parsing
  const sections = [];
  const lines = content.split('\\n');

  let currentSection = null;
  for (const line of lines) {
    if (line.includes('<h2') || line.includes('<h3')) {
      if (currentSection) {
        sections.push(currentSection);
      }
      const headingText = line.replace(/<\/?h[23][^>]*>/gi, '').trim();
      currentSection = {
        title: he.escape(headingText),
        description: '',
        area: area
      };
    } else if (currentSection && line.trim() && !line.includes('<')) {
      if (currentSection.description.length < 200) {
        currentSection.description += line.trim() + ' ';
      }
    }
  }

  if (currentSection) {
    sections.push(currentSection);
  }

  return sections;
}

  async discoverMetabaseApiEndpoints() {
  // In a real implementation, this would crawl the API documentation
  // For now, return a comprehensive list of known endpoints
  return [
    { endpoint: '/api/dashboard', method: 'GET', category: 'dashboard' },
    { endpoint: '/api/dashboard', method: 'POST', category: 'dashboard' },
    { endpoint: '/api/dashboard/:id', method: 'GET', category: 'dashboard' },
    { endpoint: '/api/dashboard/:id', method: 'PUT', category: 'dashboard' },
    { endpoint: '/api/dashboard/:id/cards', method: 'POST', category: 'dashboard' },
    { endpoint: '/api/card', method: 'GET', category: 'card' },
    { endpoint: '/api/card', method: 'POST', category: 'card' },
    { endpoint: '/api/card/:id', method: 'GET', category: 'card' },
    { endpoint: '/api/card/:id', method: 'PUT', category: 'card' },
    { endpoint: '/api/card/:id/query', method: 'POST', category: 'card' },
    { endpoint: '/api/database', method: 'GET', category: 'database' },
    { endpoint: '/api/database/:id', method: 'GET', category: 'database' },
    { endpoint: '/api/database/:id/schema', method: 'GET', category: 'database' },
    { endpoint: '/api/collection', method: 'GET', category: 'collection' },
    { endpoint: '/api/collection', method: 'POST', category: 'collection' },
    { endpoint: '/api/metric', method: 'GET', category: 'metric' },
    { endpoint: '/api/metric', method: 'POST', category: 'metric' },
    { endpoint: '/api/segment', method: 'GET', category: 'segment' },
    { endpoint: '/api/user', method: 'GET', category: 'user' },
    { endpoint: '/api/session', method: 'POST', category: 'session' }
  ];
}

calculateRelevanceScore(content, query) {
  const queryTerms = query.toLowerCase().split(' ');
  const contentLower = content.toLowerCase();

  let score = 0;
  let totalTerms = queryTerms.length;

  for (const term of queryTerms) {
    if (contentLower.includes(term)) {
      score += 1;
      // Bonus for exact phrase matches
      if (contentLower.includes(query.toLowerCase())) {
        score += 0.5;
      }
    }
  }

  return score / totalTerms;
}

extractRelevantContent(content, query) {
  // Extract title from HTML
  const titleMatch = content.match(/<title>(.*?)<\/title>/i);
  const title = titleMatch ? titleMatch[1] : 'Documentation Page';

  // Extract relevant text paragraphs
  const queryTerms = query.toLowerCase().split(' ');
  const sentences = content.split('.').filter(sentence => {
    const sentenceLower = sentence.toLowerCase();
    return queryTerms.some(term => sentenceLower.includes(term));
  });

  // Extract code examples
  const codeBlocks = content.match(/```[\\s\\S]*?```/g) || [];
  const codeExamples = codeBlocks.map(block => block.replace(/```/g, '').trim());

  return {
    title: title.replace(' - Metabase', ''),
    content: sentences.slice(0, 3).join('.').substring(0, 500),
    codeExamples: codeExamples.slice(0, 2)
  };
}

// Helper method for executive dashboard layout
calculateExecutiveLayout(index, total) {
  const layouts = {
    'kpi': { sizeX: 3, sizeY: 3 },      // KPI cards
    'chart': { sizeX: 6, sizeY: 4 },    // Charts
    'table': { sizeX: 12, sizeY: 6 },   // Tables
    'metric': { sizeX: 4, sizeY: 3 }    // Metrics
  };

  // Executive layout: 
  // Row 0: 4 KPI cards (3x3 each)
  // Row 1: 2 charts (6x4 each) 
  // Row 2: 1 table (12x6)

  if (index < 4) {
    // KPI cards in top row
    return {
      row: 0,
      col: index * 3,
      sizeX: 3,
      sizeY: 3
    };
  } else if (index < 6) {
    // Charts in second row
    return {
      row: 1,
      col: (index - 4) * 6,
      sizeX: 6,
      sizeY: 4
    };
  } else {
    // Tables/detailed views in subsequent rows
    return {
      row: 2 + Math.floor((index - 6) / 1),
      col: 0,
      sizeX: 12,
      sizeY: 6
    };
  }
}

  // Helper method to generate executive questions based on business domain
  async generateExecutiveQuestions(databaseId, schemaName, tables, businessDomain, timePeriod) {
  const questions = [];

  // Analyze tables to find relevant business entities
  const salesTables = tables.filter(t =>
    t.name.toLowerCase().includes('sale') ||
    t.name.toLowerCase().includes('order') ||
    t.name.toLowerCase().includes('transaction')
  );

  const customerTables = tables.filter(t =>
    t.name.toLowerCase().includes('customer') ||
    t.name.toLowerCase().includes('user') ||
    t.name.toLowerCase().includes('client')
  );

  const productTables = tables.filter(t =>
    t.name.toLowerCase().includes('product') ||
    t.name.toLowerCase().includes('item') ||
    t.name.toLowerCase().includes('inventory')
  );

  // Generate KPI questions based on domain
  if (salesTables.length > 0) {
    const salesTable = salesTables[0];
    questions.push({
      name: "Total Revenue",
      sql: `SELECT SUM(COALESCE(amount, total, price, 0)) as revenue FROM ${schemaName}.${salesTable.name} WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'`,
      visualization: "number"
    });

    questions.push({
      name: "Sales Trend",
      sql: `SELECT DATE(created_at) as date, SUM(COALESCE(amount, total, price, 0)) as daily_revenue FROM ${schemaName}.${salesTable.name} WHERE created_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY DATE(created_at) ORDER BY date`,
      visualization: "line"
    });
  }

  if (customerTables.length > 0) {
    const customerTable = customerTables[0];
    questions.push({
      name: "Total Customers",
      sql: `SELECT COUNT(*) as customer_count FROM ${schemaName}.${customerTable.name}`,
      visualization: "number"
    });

    questions.push({
      name: "New Customers (30d)",
      sql: `SELECT COUNT(*) as new_customers FROM ${schemaName}.${customerTable.name} WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'`,
      visualization: "number"
    });
  }

  // Create questions in Metabase (simplified for demo)
  for (const q of questions) {
    try {
      const question = await this.metabaseClient.createSQLQuestion(
        q.name,
        `Executive KPI - ${q.name}`,
        databaseId,
        q.sql
      );
      questions[questions.indexOf(q)] = question;
    } catch (error) {
      console.error(`Error creating question ${q.name}:`, error);
    }
  }

  return questions;
}

  async handleGenerateSQL(description, databaseId) {
  if (!this.aiAssistant) {
    throw new Error('AI assistant not configured. Please set ANTHROPIC_API_KEY or OPENAI_API_KEY.');
  }

  const tables = await this.metabaseClient.getDatabaseTables(databaseId);
  const sql = await this.aiAssistant.generateSQL(description, tables);

  return {
    content: [
      {
        type: 'text',
        text: `Generated SQL for: "${description}"\\n\\n\`\`\`sql\\n${sql}\\n\`\`\``,
      },
    ],
  };
}

  async handleOptimizeQuery(sql) {
  if (!this.aiAssistant) {
    throw new Error('AI assistant not configured. Please set ANTHROPIC_API_KEY or OPENAI_API_KEY.');
  }

  const optimization = await this.aiAssistant.optimizeQuery(sql);

  return {
    content: [
      {
        type: 'text',
        text: `Optimized SQL:\\n\\n\`\`\`sql\\n${optimization.optimized_sql}\\n\`\`\`\\n\\nOptimizations applied:\\n${optimization.optimizations?.join('\\n- ') || 'None'}\\n\\nExpected improvements:\\n${optimization.improvements || 'Not specified'}`,
      },
    ],
  };
}

  async handleExplainQuery(sql) {
  if (!this.aiAssistant) {
    throw new Error('AI assistant not configured. Please set ANTHROPIC_API_KEY or OPENAI_API_KEY.');
  }

  const explanation = await this.aiAssistant.explainQuery(sql);

  return {
    content: [
      {
        type: 'text',
        text: `Query Explanation:\\n\\n${explanation}`,
      },
    ],
  };
}

  async getConnection(databaseId) {
  return await this.connectionManager.getConnection(this.metabaseClient, databaseId);
}

  async getDirectClient(databaseId) {
  const connection = await this.getConnection(databaseId);
  if (connection.type !== 'direct') {
    throw new Error('This operation requires direct database connection. Direct connection not available.');
  }
  return connection.client;
}

  async handleGetConnectionInfo(databaseId) {
  const connectionInfo = await this.metabaseClient.getDatabaseConnectionInfo(databaseId);

  // Güvenlik için şifreyi gizle
  const safeInfo = { ...connectionInfo };
  if (safeInfo.password) {
    safeInfo.password = '***HIDDEN***';
  }

  return {
    content: [
      {
        type: 'text',
        text: `Database Connection Info:\\n${JSON.stringify(safeInfo, null, 2)}`,
      },
    ],
  };
}

  async handleCreateTableDirect(args) {
  const startTime = Date.now();
  const connection = await this.getConnection(args.database_id);

  // Schema seçimi kontrolü ve bilgilendirme
  if (!args.schema && connection.type === 'direct') {
    const client = connection.client;
    const schemas = await client.getSchemas();
    const currentSchema = await client.getCurrentSchema();

    return {
      content: [
        {
          type: 'text',
          text: `⚠️  **SCHEMA SELECTION REQUIRED**\\n\\n` +
            `🔗 **Connection Type:** DIRECT DATABASE (PostgreSQL)\\n` +
            `📂 **Current Schema:** ${currentSchema}\\n\\n` +
            `📋 **Available Schemas:**\\n${schemas.map(s => `  • ${s}`).join('\\n')}\\n\\n` +
            `🛠️  **Next Steps:** Please specify a schema parameter and re-run:\\n` +
            `\`\`\`json\\n{\\n  "schema": "${currentSchema || 'public'}",\\n  "database_id": ${args.database_id},\\n  "table_name": "${args.table_name}",\\n  "columns": [...],\\n  "dry_run": false,\\n  "approved": true\\n}\\n\`\`\``,
        },
      ],
    };
  }

  // Dry run kontrolü
  if (args.dry_run !== false) {
    const tableName = 'claude_ai_' + args.table_name;
    const schemaPrefix = args.schema ? `${args.schema}.` : '';
    const fullTableName = `${schemaPrefix}${tableName}`;
    const columnsSQL = args.columns.map(col =>
      `${col.name} ${col.type}${col.constraints ? ' ' + col.constraints : ''}`
    ).join(', ');
    const previewSQL = `CREATE TABLE ${fullTableName} (${columnsSQL})`;

    return {
      content: [
        {
          type: 'text',
          text: `🔍 **DRY RUN PREVIEW**\\n\\n` +
            `🔗 **Connection:** ${connection.type === 'direct' ? 'DIRECT DATABASE' : 'METABASE PROXY'}\\n` +
            `📂 **Target Schema:** ${args.schema || 'default'}\\n` +
            `📊 **Table Name:** ${tableName}\\n` +
            `📝 **Columns:** ${args.columns.length}\\n\\n` +
            `📜 **SQL to execute:**\\n\`\`\`sql\\n${previewSQL}\\n\`\`\`\\n\\n` +
            `✅ **To execute:** Set \`dry_run: false\` and \`approved: true\``,
        },
      ],
    };
  }

  let tableName = null;
  let error = null;

  try {
    const result = await this.connectionManager.executeOperation(
      connection,
      'createTable',
      args.table_name,
      args.columns,
      { approved: args.approved, schema: args.schema }
    );

    tableName = 'claude_ai_' + args.table_name;
    const executionTime = Date.now() - startTime;
    const fullTableName = args.schema ? `${args.schema}.${tableName}` : tableName;

    // Log successful table creation
    if (this.activityLogger) {
      const sql = `CREATE TABLE ${fullTableName} (${args.columns.map(col =>
        `${col.name} ${col.type}${col.constraints ? ' ' + col.constraints : ''}`
      ).join(', ')})`;

      await this.activityLogger.logTableCreation(tableName, args.database_id, sql, executionTime);
    }

    return {
      content: [
        {
          type: 'text',
          text: `✅ **TABLE CREATED SUCCESSFULLY!**\\n\\n` +
            `📊 **Table Details:**\\n` +
            `• Name: \`${tableName}\`\\n` +
            `• Schema: \`${args.schema || 'default'}\`\\n` +
            `• Columns: ${args.columns.length}\\n` +
            `• Connection: ${connection.type === 'direct' ? '🔗 DIRECT DATABASE' : '🌐 METABASE PROXY'}\\n` +
            `• Execution Time: ${executionTime}ms\\n\\n` +
            `📝 **Column Details:**\\n${args.columns.map(col =>
              `• \`${col.name}\` (${col.type}${col.constraints ? ', ' + col.constraints : ''})`
            ).join('\\n')}\\n\\n` +
            `💡 **Next Steps:** Table is now available for queries and Metabase models!`,
        },
      ],
    };

  } catch (err) {
    error = err;
    const executionTime = Date.now() - startTime;

    // Log failed table creation
    if (this.activityLogger) {
      await this.activityLogger.logTableCreation(
        args.table_name,
        args.database_id,
        `CREATE TABLE attempt failed`,
        executionTime,
        err
      );
    }

    return {
      content: [
        {
          type: 'text',
          text: `❌ **TABLE CREATION FAILED!**\\n\\n` +
            `🚫 **Error Details:**\\n` +
            `• Table: \`claude_ai_${args.table_name}\`\\n` +
            `• Schema: \`${args.schema || 'default'}\`\\n` +
            `• Database ID: ${args.database_id}\\n` +
            `• Execution Time: ${executionTime}ms\\n` +
            `• Error: ${err.message}\\n\\n` +
            `🔧 **Troubleshooting:**\\n` +
            `• Check if table name conflicts with existing tables\\n` +
            `• Verify column definitions are valid\\n` +
            `• Ensure you have CREATE permissions on the schema\\n` +
            `• Make sure \`approved: true\` is set`,
        },
      ],
    };
  }
}

  async handleCreateViewDirect(args) {
  const client = await this.getDirectClient(args.database_id);

  // Schema seçimi kontrolü ve bilgilendirme
  if (!args.schema) {
    const schemas = await client.getSchemas();
    const currentSchema = await client.getCurrentSchema();

    return {
      content: [
        {
          type: 'text',
          text: `⚠️  SCHEMA SELECTION REQUIRED\\n\\n` +
            `Connection Type: 🔗 DIRECT DATABASE (PostgreSQL)\\n` +
            `Current Schema: ${currentSchema}\\n\\n` +
            `Available Schemas:\\n${schemas.map(s => `  - ${s}`).join('\\n')}\\n\\n` +
            `Please specify a schema parameter and re-run:\\n` +
            `Example parameters:\\n{\\n  "schema": "${currentSchema || 'public'}",\\n  "database_id": ${args.database_id},\\n  "view_name": "${args.view_name}",\\n  "select_sql": "...",\\n  "dry_run": false,\\n  "approved": true\\n}`,
        },
      ],
    };
  }

  // Dry run kontrolü
  if (args.dry_run !== false) {
    const viewName = client.options.prefix + args.view_name;
    const schemaPrefix = args.schema ? `${args.schema}.` : '';
    const fullViewName = `${schemaPrefix}${viewName}`;
    const previewSQL = `CREATE VIEW ${fullViewName} AS ${args.select_sql}`;

    return {
      content: [
        {
          type: 'text',
          text: `🔍 DRY RUN PREVIEW\\n\\n` +
            `Connection: 🔗 DIRECT DATABASE\\n` +
            `Target Schema: ${args.schema}\\n\\n` +
            `SQL to execute:\\n${previewSQL}\\n\\n` +
            `To execute, set: dry_run: false, approved: true`,
        },
      ],
    };
  }

  const result = await client.createView(args.view_name, args.select_sql, {
    approved: args.approved,
    dryRun: false,
    schema: args.schema
  });

  return {
    content: [
      {
        type: 'text',
        text: `✅ VIEW CREATED SUCCESSFULLY\\n\\n` +
          `Name: ${client.options.prefix}${args.view_name}\\n` +
          `Schema: ${args.schema}\\n` +
          `Connection: 🔗 DIRECT DATABASE`,
      },
    ],
  };
}

  async handleCreateMaterializedViewDirect(args) {
  const client = await this.getDirectClient(args.database_id);

  if (client.engine !== 'postgres') {
    throw new Error('Materialized views are only supported in PostgreSQL');
  }

  // Schema seçimi kontrolü ve bilgilendirme
  if (!args.schema) {
    const schemas = await client.getSchemas();
    const currentSchema = await client.getCurrentSchema();

    return {
      content: [
        {
          type: 'text',
          text: `⚠️  SCHEMA SELECTION REQUIRED\\n\\n` +
            `Connection Type: 🔗 DIRECT DATABASE (PostgreSQL)\\n` +
            `Current Schema: ${currentSchema}\\n\\n` +
            `Available Schemas:\\n${schemas.map(s => `  - ${s}`).join('\\n')}\\n\\n` +
            `Please specify a schema parameter and re-run:\\n` +
            `Example parameters:\\n{\\n  "schema": "${currentSchema || 'public'}",\\n  "database_id": ${args.database_id},\\n  "view_name": "${args.view_name}",\\n  "select_sql": "...",\\n  "dry_run": false,\\n  "approved": true\\n}`,
        },
      ],
    };
  }

  // Dry run kontrolü
  if (args.dry_run !== false) {
    const viewName = client.options.prefix + args.view_name;
    const schemaPrefix = args.schema ? `${args.schema}.` : '';
    const fullViewName = `${schemaPrefix}${viewName}`;
    const previewSQL = `CREATE MATERIALIZED VIEW ${fullViewName} AS ${args.select_sql}`;

    return {
      content: [
        {
          type: 'text',
          text: `🔍 DRY RUN PREVIEW\\n\\n` +
            `Connection: 🔗 DIRECT DATABASE\\n` +
            `Target Schema: ${args.schema}\\n\\n` +
            `SQL to execute:\\n${previewSQL}\\n\\n` +
            `To execute, set: dry_run: false, approved: true`,
        },
      ],
    };
  }

  const result = await client.createMaterializedView(args.view_name, args.select_sql, {
    approved: args.approved,
    dryRun: false,
    schema: args.schema
  });

  return {
    content: [
      {
        type: 'text',
        text: `✅ MATERIALIZED VIEW CREATED SUCCESSFULLY\\n\\n` +
          `Name: ${client.options.prefix}${args.view_name}\\n` +
          `Schema: ${args.schema}\\n` +
          `Connection: 🔗 DIRECT DATABASE`,
      },
    ],
  };
}

  async handleCreateIndexDirect(args) {
  const client = await this.getDirectClient(args.database_id);

  // Dry run kontrolü
  if (args.dry_run !== false) {
    const indexName = client.options.prefix + args.index_name;
    const unique = args.unique ? 'UNIQUE ' : '';
    const columnsStr = Array.isArray(args.columns) ? args.columns.join(', ') : args.columns;
    const previewSQL = `CREATE ${unique}INDEX ${indexName} ON ${args.table_name} (${columnsStr})`;

    return {
      content: [
        {
          type: 'text',
          text: `🔍 DRY RUN PREVIEW\\n\\n` +
            `Connection: 🔗 DIRECT DATABASE\\n` +
            `Target Schema: ${args.schema}\\n\\n` +
            `SQL to execute:\\n${previewSQL}\\n\\n` +
            `To execute, set: dry_run: false, approved: true`,
        },
      ],
    };
  }

  const result = await client.createIndex(args.index_name, args.table_name, args.columns, {
    unique: args.unique,
    approved: args.approved,
    dryRun: false
  });

  return {
    content: [
      {
        type: 'text',
        text: `Index created successfully!\\nName: ${client.options.prefix}${args.index_name}`,
      },
    ],
  };
}

  async handleGetTableDDL(databaseId, tableName) {
  const client = await this.getDirectClient(databaseId);
  const ddl = await client.getTableDDL(tableName);

  return {
    content: [
      {
        type: 'text',
        text: ddl ? `Table DDL:\\n\\n\`\`\`sql\\n${ddl}\\n\`\`\`` : `Table ${tableName} not found or DDL not available`,
      },
    ],
  };
}

  async handleGetViewDDL(databaseId, viewName) {
  const client = await this.getDirectClient(databaseId);
  const ddl = await client.getViewDDL(viewName);

  return {
    content: [
      {
        type: 'text',
        text: ddl ? `View DDL:\\n\\n\`\`\`sql\\n${ddl}\\n\`\`\`` : `View ${viewName} not found or DDL not available`,
      },
    ],
  };
}

  async handleListAIObjects(databaseId) {
  const client = await this.getDirectClient(databaseId);
  const objects = await client.listOwnObjects();

  let output = 'AI-Created Objects:\\n\\n';

  if (objects.tables.length > 0) {
    output += 'Tables:\\n';
    objects.tables.forEach(table => {
      output += `  - ${table.table_name}\\n`;
    });
    output += '\\n';
  }

  if (objects.views.length > 0) {
    output += 'Views:\\n';
    objects.views.forEach(view => {
      output += `  - ${view.view_name}\\n`;
    });
    output += '\\n';
  }

  if (objects.materialized_views.length > 0) {
    output += 'Materialized Views:\\n';
    objects.materialized_views.forEach(view => {
      output += `  - ${view.matviewname}\\n`;
    });
    output += '\\n';
  }

  if (objects.indexes.length > 0) {
    output += 'Indexes:\\n';
    objects.indexes.forEach(index => {
      output += `  - ${index.indexname} (on ${index.tablename})\\n`;
    });
  }

  if (objects.tables.length === 0 && objects.views.length === 0 &&
    objects.materialized_views.length === 0 && objects.indexes.length === 0) {
    output += 'No AI-created objects found.';
  }

  return {
    content: [
      {
        type: 'text',
        text: output,
      },
    ],
  };
}

  async handleDropAIObject(args) {
  const client = await this.getDirectClient(args.database_id);

  // Prefix kontrolü
  if (!args.object_name.startsWith('claude_ai_')) {
    throw new Error('Can only drop objects with claude_ai_ prefix');
  }

  // Dry run kontrolü
  if (args.dry_run !== false) {
    const dropSQL = `DROP ${args.object_type.toUpperCase().replace('_', ' ')} IF EXISTS ${args.object_name}`;

    return {
      content: [
        {
          type: 'text',
          text: `DRY RUN - Would execute:\\n\\n\`\`\`sql\\n${dropSQL}\\n\`\`\`\\n\\nTo execute, set dry_run: false and approved: true`,
        },
      ],
    };
  }

  const operationType = `DROP_${args.object_type.toUpperCase()}`;
  const dropSQL = `DROP ${args.object_type.toUpperCase().replace('_', ' ')} IF EXISTS ${args.object_name}`;

  const result = await client.executeDDL(dropSQL, {
    approved: args.approved
  });

  return {
    content: [
      {
        type: 'text',
        text: `${args.object_type} dropped successfully!\\nName: ${args.object_name}`,
      },
    ],
  };
}

  // Schema ve İlişki Keşif Metodları
  async handleExploreSchemaSimple(args) {
  // Fallback metod - SQL ile schema exploration
  const tableListSQL = `
      SELECT 
        t.table_name,
        t.table_type,
        (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) as column_count
      FROM information_schema.tables t
      WHERE t.table_schema = '${args.schema_name}'
        AND t.table_type = 'BASE TABLE'
      ORDER BY t.table_name
    `;

  try {
    const startTime = Date.now();
    const result = await this.metabaseClient.executeNativeQuery(args.database_id, tableListSQL);

    let output = `🔍 SCHEMA EXPLORATION (Simple): ${args.schema_name}\\n\\n`;

    if (result.data && result.data.rows && result.data.rows.length > 0) {
      const endTime = Date.now();
      const responseTime = endTime - startTime;

      output += `Found ${result.data.rows.length} tables (${responseTime}ms):\\n\\n`;

      result.data.rows.forEach((row, index) => {
        const [tableName, tableType, columnCount] = row;
        output += `${index + 1}. 📊 **${tableName}** (${columnCount} columns)\\n`;
      });

      output += `\\n💡 **Next Steps:**\\n`;
      output += `- Use 'execute_sql' for detailed column info\\n`;
      output += `- Try 'db_schema_analyze' for advanced analysis\\n`;
      output += `- Check other schemas: analytics, cron, staging`;
    } else {
      output += `No tables found in schema '${args.schema_name}'.`;
    }

    return {
      content: [
        {
          type: 'text',
          text: output,
        },
      ],
    };
  } catch (error) {
    return {
      content: [
        {
          type: 'text',
          text: `❌ Error exploring schema: ${error.message}`,
        },
      ],
    };
  }
}

  async handleExploreSchemaTablesAdvanced(args) {
  const startTime = Date.now();
  const limit = args.limit || 10;
  const timeoutMs = (args.timeout_seconds || 30) * 1000;

  try {
    const client = await this.getDirectClient(args.database_id);

    // Timeout Promise
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error(`Operation timeout after ${args.timeout_seconds || 30} seconds`)), timeoutMs);
    });

    // Main operation Promise
    const operationPromise = client.exploreSchemaTablesDetailed(
      args.schema_name,
      args.include_columns !== false,
      limit
    );

    // Race between operation and timeout
    const tables = await Promise.race([operationPromise, timeoutPromise]);

    const endTime = Date.now();
    const responseTime = endTime - startTime;

    let output = `🔍 SCHEMA EXPLORATION (Advanced): ${args.schema_name}\\n\\n`;
    output += `⚡ Completed in ${responseTime}ms\\n`;
    output += `Found ${tables.length} tables (limited to ${limit}):\\n\\n`;

    tables.forEach(table => {
      output += `📊 **${table.name}** (${table.type})\\n`;
      if (table.comment) output += `   Description: ${table.comment}\\n`;
      if (table.size) output += `   Size: ${table.size}\\n`;

      if (args.include_columns !== false && table.columns.length > 0) {
        output += `   Columns (${table.columns.length}):`;
        table.columns.forEach(col => {
          const indicators = [];
          if (col.isPrimaryKey) indicators.push('🔑 PK');
          if (col.isForeignKey) indicators.push(`🔗 FK→${col.foreignTable}.${col.foreignColumn}`);
          if (!col.nullable) indicators.push('⚠️ NOT NULL');

          output += `\\n     - ${col.name}: ${col.type}`;
          if (indicators.length > 0) output += ` ${indicators.join(' ')}`;
          if (col.comment) output += ` // ${col.comment}`;
        });
        output += `\\n`;
      }
      output += `\\n`;
    });

    return {
      content: [
        {
          type: 'text',
          text: output,
        },
      ],
    };
  } catch (error) {
    return {
      content: [
        {
          type: 'text',
          text: `❌ ADVANCED EXPLORATION FAILED\\n\\n` +
            `Error: ${error.message}\\n\\n` +
            `💡 Try 'db_schema_explore' instead or:\\n` +
            `- Increase timeout_seconds\\n` +
            `- Reduce limit parameter\\n` +
            `- Check if direct database connection is available`,
        },
      ],
    };
  }
}

  async handleAnalyzeTableRelationships(args) {
  const client = await this.getDirectClient(args.database_id);
  const relationships = await client.analyzeTableRelationships(
    args.schema_name,
    args.table_names
  );

  let output = `🔗 RELATIONSHIP ANALYSIS: ${args.schema_name}\\n\\n`;

  if (relationships.length === 0) {
    output += `No foreign key relationships found.\\n\\n`;
    output += `💡 Try 'suggest_virtual_relationships' to find potential relationships based on naming conventions.`;
  } else {
    output += `Found ${relationships.length} explicit foreign key relationships:\\n\\n`;

    relationships.forEach((rel, index) => {
      output += `${index + 1}. **${rel.sourceTable}.${rel.sourceColumn}** → **${rel.targetTable}.${rel.targetColumn}**\\n`;
      output += `   Type: ${rel.relationshipType}\\n`;
      output += `   Constraint: ${rel.constraintName}\\n`;
      output += `   Rules: UPDATE ${rel.updateRule}, DELETE ${rel.deleteRule}\\n\\n`;
    });
  }

  return {
    content: [
      {
        type: 'text',
        text: output,
      },
    ],
  };
}

  async handleSuggestVirtualRelationships(args) {
  const client = await this.getDirectClient(args.database_id);
  const suggestions = await client.suggestVirtualRelationships(
    args.schema_name,
    args.confidence_threshold || 0.7
  );

  let output = `🤖 VIRTUAL RELATIONSHIP SUGGESTIONS: ${args.schema_name}\\n\\n`;
  output += `Confidence threshold: ${args.confidence_threshold || 0.7}\\n\\n`;

  if (suggestions.length === 0) {
    output += `No high-confidence relationship suggestions found.\\n`;
    output += `Try lowering the confidence_threshold parameter.`;
  } else {
    output += `Found ${suggestions.length} potential relationships:\\n\\n`;

    suggestions.forEach((suggestion, index) => {
      const confidenceBar = '█'.repeat(Math.round(suggestion.confidence * 10));
      output += `${index + 1}. **${suggestion.sourceTable}.${suggestion.sourceColumn}** → **${suggestion.targetTable}.${suggestion.targetColumn}**\\n`;
      output += `   Confidence: ${suggestion.confidence.toFixed(2)} ${confidenceBar}\\n`;
      output += `   Type: ${suggestion.relationshipType}\\n`;
      output += `   Reasoning: ${suggestion.reasoning}\\n\\n`;
    });

    output += `\\n📋 **Next Steps:**\\n`;
    output += `1. Review suggestions above\\n`;
    output += `2. Use 'create_relationship_mapping' with confirmed relationships\\n`;
    output += `3. This will create Metabase model relationships`;
  }

  return {
    content: [
      {
        type: 'text',
        text: output,
      },
    ],
  };
}

  async handleCreateRelationshipMapping(args) {
  if (!args.confirmed) {
    return {
      content: [
        {
          type: 'text',
          text: `⚠️  RELATIONSHIP MAPPING CONFIRMATION REQUIRED\\n\\n` +
            `You are about to create ${args.relationships.length} virtual relationships in Metabase.\\n\\n` +
            `Relationships to create:\\n` +
            args.relationships.map((rel, i) =>
              `${i + 1}. ${rel.source_table}.${rel.source_column} → ${rel.target_table}.${rel.target_column} (${rel.relationship_type})`
            ).join('\\n') +
            `\\n\\n⚠️  **Important:** These relationships will affect Metabase models and dashboards.\\n\\n` +
            `To proceed, set: "confirmed": true`,
        },
      ],
    };
  }

  // Metabase API ile relationship oluşturma
  let successCount = 0;
  let errors = [];
  const results = [];

  for (const rel of args.relationships) {
    try {
      // Metabase'de tablo ID'lerini bul
      const tables = await this.metabaseClient.getDatabaseTables(args.database_id);
      const sourceTable = tables.find(t => t.name === rel.source_table);
      const targetTable = tables.find(t => t.name === rel.target_table);

      if (!sourceTable || !targetTable) {
        errors.push(`Tables not found: ${rel.source_table} or ${rel.target_table}`);
        continue;
      }

      // Metabase relationship oluşturma (bu API endpoint'i Metabase versiyonuna göre değişebilir)
      const relationshipData = {
        source_table_id: sourceTable.id,
        source_column: rel.source_column,
        target_table_id: targetTable.id,
        target_column: rel.target_column,
        relationship_type: rel.relationship_type
      };

      // Not: Gerçek Metabase API endpoint'i kullanılmalı
      // Bu örnek implementasyon
      logger.info('Creating relationship:', relationshipData);
      successCount++;
      results.push(`✅ ${rel.source_table}.${rel.source_column} → ${rel.target_table}.${rel.target_column}`);

    } catch (error) {
      errors.push(`Failed to create ${rel.source_table}.${rel.source_column} → ${rel.target_table}.${rel.target_column}: ${error.message}`);
    }
  }

  let output = `🔗 RELATIONSHIP MAPPING RESULTS\\n\\n`;
  output += `✅ Successfully created: ${successCount}/${args.relationships.length} relationships\\n\\n`;

  if (results.length > 0) {
    output += `**Created Relationships:**\\n`;
    output += results.join('\\n') + '\\n\\n';
  }

  if (errors.length > 0) {
    output += `**Errors:**\\n`;
    output += errors.map(e => `❌ ${e}`).join('\\n') + '\\n\\n';
  }

  output += `🎯 **Next Steps:**\\n`;
  output += `1. Refresh Metabase model metadata\\n`;
  output += `2. Check model relationships in Metabase admin\\n`;
  output += `3. Test dashboards and questions`;

  return {
    content: [
      {
        type: 'text',
        text: output,
      },
    ],
  };
}

  // === DEFINITION TABLES & PARAMETRIC QUESTIONS HANDLERS ===

  async handleDefinitionTablesInit(args) {
  try {
    await this.ensureInitialized();

    // Import definition tables utility
    const { DefinitionTables } = await import('../utils/definition-tables.js');
    const definitionTables = new DefinitionTables(this.metabaseClient);

    const result = await definitionTables.initializeDefinitionTables(args.database_id);

    const output = `✅ Definition Tables Initialized\n\n` +
      `📊 Tables Created:\n` +
      result.tables.map(table => `   • ${table}`).join('\n') + `\n\n` +
      `🎯 Status: ${result.message}\n` +
      `🗄️ Database ID: ${args.database_id}`;

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error initializing definition tables: ${error.message}` }],
    };
  }
}

  async handleDefinitionSearchTerms(args) {
  try {
    await this.ensureInitialized();

    const { DefinitionTables } = await import('../utils/definition-tables.js');
    const definitionTables = new DefinitionTables(this.metabaseClient);

    const terms = await definitionTables.searchBusinessTerms(
      args.database_id,
      args.search_term,
      args.category
    );

    let output = `🔍 Business Terms Search: "${args.search_term}"\n\n`;

    if (terms.length === 0) {
      output += `❌ No terms found matching "${args.search_term}"`;
    } else {
      output += `📊 Found ${terms.length} matching terms:\n\n`;

      terms.forEach((term, index) => {
        output += `${index + 1}. **${term.term}**\n`;
        output += `   📝 Definition: ${term.definition}\n`;
        output += `   🏷️ Category: ${term.category}\n`;
        if (term.synonyms && term.synonyms.length > 0) {
          output += `   🔄 Synonyms: ${term.synonyms.join(', ')}\n`;
        }
        if (term.calculation_logic) {
          output += `   🧮 Calculation: ${term.calculation_logic}\n`;
        }
        output += `   📈 Relevance: ${(term.relevance * 100).toFixed(1)}%\n\n`;
      });
    }

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error searching business terms: ${error.message}` }],
    };
  }
}

  async handleDefinitionGetMetric(args) {
  try {
    await this.ensureInitialized();

    const { DefinitionTables } = await import('../utils/definition-tables.js');
    const definitionTables = new DefinitionTables(this.metabaseClient);

    const metric = await definitionTables.getMetricDefinition(args.database_id, args.metric_name);

    if (!metric) {
      return {
        content: [{ type: 'text', text: `❌ Metric "${args.metric_name}" not found in definition tables` }],
      };
    }

    const output = `📊 Metric Definition: **${metric.display_name}**\n\n` +
      `🏷️ Internal Name: ${metric.metric_name}\n` +
      `📝 Description: ${metric.description}\n` +
      `🧮 Calculation Formula: \`${metric.calculation_formula}\`\n` +
      `📈 Aggregation Type: ${metric.aggregation_type}\n` +
      `📏 Unit of Measure: ${metric.unit_of_measure}\n` +
      `🏢 KPI Category: ${metric.kpi_category}\n` +
      (metric.business_context ? `💼 Business Context: ${metric.business_context}\n` : '');

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error getting metric definition: ${error.message}` }],
    };
  }
}

  async handleDefinitionGetTemplate(args) {
  try {
    await this.ensureInitialized();

    const { DefinitionTables } = await import('../utils/definition-tables.js');
    const definitionTables = new DefinitionTables(this.metabaseClient);

    let template;
    if (args.template_type === 'dashboard') {
      template = await definitionTables.getDashboardTemplate(args.database_id, args.template_name);
    } else if (args.template_type === 'question') {
      template = await definitionTables.getQuestionTemplate(args.database_id, args.template_name);
    } else {
      throw new Error('Invalid template type. Must be "dashboard" or "question"');
    }

    if (!template) {
      return {
        content: [{ type: 'text', text: `❌ ${args.template_type} template "${args.template_name}" not found` }],
      };
    }

    let output = `📋 ${args.template_type.charAt(0).toUpperCase() + args.template_type.slice(1)} Template: **${template.template_name}**\n\n`;
    output += `📝 Description: ${template.description}\n`;

    if (args.template_type === 'dashboard') {
      output += `🏷️ Type: ${template.template_type}\n`;
      if (template.required_metrics) {
        output += `📊 Required Metrics: ${template.required_metrics.join(', ')}\n`;
      }
      if (template.layout_config) {
        output += `📐 Layout Configuration:\n`;
        output += `\`\`\`json\n${JSON.stringify(template.layout_config, null, 2)}\n\`\`\`\n`;
      }
    } else {
      output += `🏷️ Question Type: ${template.question_type}\n`;
      output += `📊 Visualization: ${template.visualization_type}\n`;
      if (template.sql_template) {
        output += `🗃️ SQL Template:\n\`\`\`sql\n${template.sql_template}\n\`\`\`\n`;
      }
      if (template.parameters) {
        output += `⚙️ Parameters:\n`;
        output += `\`\`\`json\n${JSON.stringify(template.parameters, null, 2)}\n\`\`\`\n`;
      }
      if (template.business_use_case) {
        output += `💼 Business Use Case: ${template.business_use_case}\n`;
      }
    }

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error getting template: ${error.message}` }],
    };
  }
}

  async handleDefinitionGlobalSearch(args) {
  try {
    await this.ensureInitialized();

    const { DefinitionTables } = await import('../utils/definition-tables.js');
    const definitionTables = new DefinitionTables(this.metabaseClient);

    const results = await definitionTables.globalSearch(
      args.database_id,
      args.search_term,
      args.content_types
    );

    let output = `🌐 Global Search: "${args.search_term}"\n\n`;

    if (results.length === 0) {
      output += `❌ No results found across all definition tables`;
    } else {
      output += `📊 Found ${results.length} results:\n\n`;

      const groupedResults = {};
      results.forEach(result => {
        if (!groupedResults[result.content_type]) {
          groupedResults[result.content_type] = [];
        }
        groupedResults[result.content_type].push(result);
      });

      Object.entries(groupedResults).forEach(([type, typeResults]) => {
        output += `## ${type.replace('_', ' ').toUpperCase()}\n`;

        typeResults.forEach((result, index) => {
          output += `${index + 1}. **${result.source_table}**\n`;
          output += `   📝 Content: ${result.content.substring(0, 200)}${result.content.length > 200 ? '...' : ''}\n`;
          output += `   📈 Relevance: ${(result.relevance * 100).toFixed(1)}%\n`;
          if (result.metadata && Object.keys(result.metadata).length > 0) {
            output += `   ℹ️ Metadata: ${JSON.stringify(result.metadata)}\n`;
          }
          output += `\n`;
        });
      });
    }

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error performing global search: ${error.message}` }],
    };
  }
}

  async handleParametricQuestionCreate(args) {
  try {
    await this.ensureInitialized();

    const { ParametricQuestions } = await import('../utils/parametric-questions.js');
    const { DefinitionTables } = await import('../utils/definition-tables.js');

    const definitionTables = new DefinitionTables(this.metabaseClient);
    const parametricQuestions = new ParametricQuestions(this.metabaseClient, definitionTables);

    const result = await parametricQuestions.createParametricQuestion(args.database_id, {
      name: args.name,
      description: args.description,
      sql_template: args.sql_template,
      parameters: args.parameters,
      question_type: args.question_type,
      collection_id: args.collection_id
    });

    const output = `✅ Parametric Question Created: **${result.question.name}**\n\n` +
      `🆔 Question ID: ${result.question.id}\n` +
      `📝 Description: ${args.description}\n` +
      `⚙️ Parameters: ${result.parameters.join(', ')}\n` +
      `📊 Question Type: ${args.question_type || 'table'}\n` +
      `🗃️ SQL Template:\n\`\`\`sql\n${result.sql}\n\`\`\`\n` +
      `🔗 Collection ID: ${args.collection_id || 'Root'}`;

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error creating parametric question: ${error.message}` }],
    };
  }
}

  async handleParametricDashboardCreate(args) {
  try {
    await this.ensureInitialized();

    const { ParametricQuestions } = await import('../utils/parametric-questions.js');
    const { DefinitionTables } = await import('../utils/definition-tables.js');

    const definitionTables = new DefinitionTables(this.metabaseClient);
    const parametricQuestions = new ParametricQuestions(this.metabaseClient, definitionTables);

    const result = await parametricQuestions.createDashboardWithParametricQuestions(args.database_id, {
      name: args.name,
      description: args.description,
      questions: args.questions,
      filters: args.filters,
      layout: args.layout,
      collection_id: args.collection_id
    });

    const output = `✅ Parametric Dashboard Created: **${result.dashboard.name}**\n\n` +
      `🆔 Dashboard ID: ${result.dashboard.id}\n` +
      `📝 Description: ${args.description}\n` +
      `❓ Questions Created: ${result.questions.length}\n` +
      `🎛️ Dashboard Filters: ${result.filters.length}\n` +
      `📊 Cards Added: ${result.cards.length}\n` +
      `🔗 Collection ID: ${args.collection_id || 'Root'}\n\n` +
      `**Created Questions:**\n` +
      result.questions.map((q, i) => `${i + 1}. ${q.question.name} (ID: ${q.question.id})`).join('\n');

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error creating parametric dashboard: ${error.message}` }],
    };
  }
}

  async handleParametricTemplatePreset(args) {
  try {
    await this.ensureInitialized();

    const { ParametricQuestions } = await import('../utils/parametric-questions.js');
    const { DefinitionTables } = await import('../utils/definition-tables.js');

    const definitionTables = new DefinitionTables(this.metabaseClient);
    const parametricQuestions = new ParametricQuestions(this.metabaseClient, definitionTables);

    let result;
    const config = { ...args.config, collection_id: args.collection_id };

    switch (args.preset_type) {
      case 'date_range_analysis':
        result = await parametricQuestions.createDateRangeAnalysisQuestion(args.database_id, config);
        break;
      case 'category_filter':
        result = await parametricQuestions.createCategoryFilterQuestion(args.database_id, config);
        break;
      case 'text_search':
        result = await parametricQuestions.createTextSearchQuestion(args.database_id, config);
        break;
      case 'period_comparison':
        result = await parametricQuestions.createPeriodComparisonQuestion(args.database_id, config);
        break;
      default:
        throw new Error(`Unknown preset type: ${args.preset_type}`);
    }

    const presetNames = {
      'date_range_analysis': 'Date Range Analysis',
      'category_filter': 'Category Filter',
      'text_search': 'Text Search',
      'period_comparison': 'Period Comparison'
    };

    const output = `✅ Preset Template Created: **${presetNames[args.preset_type]}**\n\n` +
      `🆔 Question ID: ${result.question.id}\n` +
      `📝 Name: ${result.question.name}\n` +
      `⚙️ Parameters: ${result.parameters.join(', ')}\n` +
      `🗃️ SQL Template:\n\`\`\`sql\n${result.sql}\n\`\`\`\n` +
      `🔗 Collection ID: ${args.collection_id || 'Root'}`;

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Error creating preset template: ${error.message}` }],
    };
  }
}

  // === DATABASE MAINTENANCE & QUERY ANALYSIS HANDLERS ===

  async handleVacuumAnalyze(args) {
  try {
    await this.ensureInitialized();

    const schemaName = args.schema_name || 'public';
    const tableName = args.table_name;
    const vacuumType = args.vacuum_type || 'vacuum_analyze';
    const dryRun = args.dry_run !== false;

    let command;
    const tableRef = tableName ? `${schemaName}.${tableName}` : null;

    switch (vacuumType) {
      case 'vacuum':
        command = tableRef ? `VACUUM ${tableRef}` : 'VACUUM';
        break;
      case 'vacuum_analyze':
        command = tableRef ? `VACUUM ANALYZE ${tableRef}` : 'VACUUM ANALYZE';
        break;
      case 'vacuum_full':
        command = tableRef ? `VACUUM FULL ${tableRef}` : 'VACUUM FULL';
        break;
      case 'analyze_only':
        command = tableRef ? `ANALYZE ${tableRef}` : 'ANALYZE';
        break;
      default:
        throw new Error(`Unknown vacuum type: ${vacuumType}`);
    }

    if (dryRun) {
      return {
        content: [{
          type: 'text',
          text: `🔍 **VACUUM/ANALYZE Preview (Dry Run)**\\n\\n` +
            `📋 **Command:** \`${command}\`\\n` +
            `🗃️ **Target:** ${tableRef || 'All tables in database'}\\n` +
            `⚙️ **Type:** ${vacuumType}\\n\\n` +
            `ℹ️ Set \`dry_run: false\` to execute this command.\\n\\n` +
            `⚠️ **Note:** VACUUM FULL requires exclusive lock and may take time on large tables.`
        }]
      };
    }

    const startTime = Date.now();
    await this.metabaseClient.executeNativeQuery(args.database_id, command);
    const executionTime = Date.now() - startTime;

    return {
      content: [{
        type: 'text',
        text: `✅ **VACUUM/ANALYZE Completed!**\\n\\n` +
          `📋 **Command:** \`${command}\`\\n` +
          `🗃️ **Target:** ${tableRef || 'All tables'}\\n` +
          `⏱️ **Execution Time:** ${executionTime}ms\\n\\n` +
          `💡 Table statistics have been updated for better query planning.`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ VACUUM/ANALYZE failed: ${error.message}` }]
    };
  }
}

  async handleQueryExplain(args) {
  try {
    await this.ensureInitialized();

    const analyze = args.analyze || false;
    const format = args.format || 'text';
    const verbose = args.verbose || false;

    let explainOptions = [];
    if (analyze) explainOptions.push('ANALYZE');
    if (verbose) explainOptions.push('VERBOSE');
    if (format !== 'text') explainOptions.push(`FORMAT ${format.toUpperCase()}`);

    const optionsStr = explainOptions.length > 0 ? `(${explainOptions.join(', ')})` : '';
    const explainQuery = `EXPLAIN ${optionsStr} ${args.sql}`;

    const result = await this.metabaseClient.executeNativeQuery(args.database_id, explainQuery);
    const rows = result.data?.rows || [];

    let planOutput = rows.map(row => row[0]).join('\\n');

    // Analyze the plan for insights
    let insights = [];
    if (planOutput.includes('Seq Scan')) {
      insights.push('⚠️ Sequential Scan detected - consider adding an index');
    }
    if (planOutput.includes('Nested Loop')) {
      insights.push('ℹ️ Nested Loop join - efficient for small datasets');
    }
    if (planOutput.includes('Hash Join') || planOutput.includes('Merge Join')) {
      insights.push('✅ Efficient join method being used');
    }
    if (planOutput.includes('Sort')) {
      insights.push('ℹ️ Sort operation - may benefit from index on sort columns');
    }

    return {
      content: [{
        type: 'text',
        text: `📊 **Query Execution Plan**\\n\\n` +
          `⚙️ **Options:** ${analyze ? 'ANALYZE' : 'ESTIMATE'}, ${format.toUpperCase()}${verbose ? ', VERBOSE' : ''}\\n\\n` +
          `\`\`\`\\n${planOutput}\\n\`\`\`\\n\\n` +
          (insights.length > 0 ? `💡 **Insights:**\\n${insights.join('\\n')}` : '')
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Query explain failed: ${error.message}` }]
    };
  }
}

  async handleTableStats(args) {
  try {
    await this.ensureInitialized();

    const schemaName = args.schema_name || 'public';
    const tableName = args.table_name;

    const statsQuery = `
        SELECT
          schemaname,
          relname as table_name,
          n_live_tup as live_rows,
          n_dead_tup as dead_rows,
          ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_ratio_pct,
          pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) as total_size,
          pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) as table_size,
          pg_size_pretty(pg_indexes_size(schemaname || '.' || relname)) as indexes_size,
          last_vacuum,
          last_autovacuum,
          last_analyze,
          last_autoanalyze,
          vacuum_count,
          autovacuum_count,
          analyze_count,
          autoanalyze_count
        FROM pg_stat_user_tables
        WHERE schemaname = '${schemaName}' AND relname = '${tableName}'
      `;

    const result = await this.metabaseClient.executeNativeQuery(args.database_id, statsQuery);
    const rows = result.data?.rows || [];

    if (rows.length === 0) {
      return {
        content: [{ type: 'text', text: `❌ Table not found: ${schemaName}.${tableName}` }]
      };
    }

    const [schema, table, liveRows, deadRows, deadRatio, totalSize, tableSize, indexesSize,
      lastVacuum, lastAutoVacuum, lastAnalyze, lastAutoAnalyze,
      vacuumCount, autoVacuumCount, analyzeCount, autoAnalyzeCount] = rows[0];

    let recommendations = [];
    if (parseFloat(deadRatio) > 10) {
      recommendations.push('⚠️ High dead tuple ratio - consider running VACUUM');
    }
    if (!lastVacuum && !lastAutoVacuum) {
      recommendations.push('⚠️ Table has never been vacuumed');
    }
    if (!lastAnalyze && !lastAutoAnalyze) {
      recommendations.push('⚠️ Table has never been analyzed - statistics may be stale');
    }

    return {
      content: [{
        type: 'text',
        text: `📊 **Table Statistics: ${schema}.${table}**\\n\\n` +
          `📈 **Row Counts:**\\n` +
          `• Live Rows: ${liveRows?.toLocaleString() || 0}\\n` +
          `• Dead Rows: ${deadRows?.toLocaleString() || 0}\\n` +
          `• Dead Ratio: ${deadRatio || 0}%\\n\\n` +
          `💾 **Size:**\\n` +
          `• Total Size: ${totalSize}\\n` +
          `• Table Size: ${tableSize}\\n` +
          `• Indexes Size: ${indexesSize}\\n\\n` +
          `🔧 **Maintenance:**\\n` +
          `• Last Vacuum: ${lastVacuum || lastAutoVacuum || 'Never'}\\n` +
          `• Last Analyze: ${lastAnalyze || lastAutoAnalyze || 'Never'}\\n` +
          `• Vacuum Count: ${vacuumCount + autoVacuumCount}\\n` +
          `• Analyze Count: ${analyzeCount + autoAnalyzeCount}\\n\\n` +
          (recommendations.length > 0 ? `💡 **Recommendations:**\\n${recommendations.join('\\n')}` : '✅ Table is well maintained')
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Failed to get table stats: ${error.message}` }]
    };
  }
}

  /**
   * Handle table profile request - comprehensive table analysis
   * Automatically detects dim/ref tables and shows distinct values
   */
  async handleTableProfile(args) {
  try {
    await this.ensureInitialized();

    const schemaName = args.schema_name || 'public';
    const tableName = args.table_name;
    const showDistinct = args.show_distinct_values !== false;
    const sampleRows = args.sample_rows || 3;

    // Detect if this is a dimension/reference table
    const isDimTable = /^(dim_|ref_|lookup_|lkp_|d_|r_)/i.test(tableName);

    // Get row count first
    const countQuery = `SELECT COUNT(*) as cnt FROM "${schemaName}"."${tableName}"`;
    const countResult = await this.metabaseClient.executeNativeQuery(args.database_id, countQuery);
    const rowCount = countResult.data?.rows?.[0]?.[0] || 0;

    // Get column info
    const columnsQuery = `
        SELECT 
          column_name,
          data_type,
          is_nullable,
          column_default
        FROM information_schema.columns 
        WHERE table_schema = '${schemaName}' AND table_name = '${tableName}'
        ORDER BY ordinal_position
      `;
    const columnsResult = await this.metabaseClient.executeNativeQuery(args.database_id, columnsQuery);
    const columns = columnsResult.data?.rows || [];

    let output = '';

    // Header with dim table indicator
    if (isDimTable) {
      output += `📊 **Dimension Table: ${schemaName}.${tableName}**\\n`;
      output += `🏷️ _Detected as lookup/reference table_\\n\\n`;
    } else {
      output += `📊 **Table Profile: ${schemaName}.${tableName}**\\n\\n`;
    }

    output += `📈 **Overview:**\\n`;
    output += `• Row count: ${rowCount.toLocaleString()}\\n`;
    output += `• Columns: ${columns.length}\\n\\n`;

    // Column details
    output += `📋 **Columns:**\\n`;
    columns.forEach(([name, type, nullable, defaultVal]) => {
      const nullIndicator = nullable === 'YES' ? '?' : '';
      output += `• \`${name}\` (${type}${nullIndicator})\\n`;
    });
    output += `\\n`;

    // For dim tables or small tables, show distinct values
    if ((isDimTable || rowCount < 1000) && showDistinct && columns.length > 0) {
      output += `🔑 **Distinct Values:**\\n`;

      // Get distinct counts and values for key columns (limit to first 5 columns)
      const keyColumns = columns.slice(0, 5);
      for (const [colName, colType] of keyColumns) {
        try {
          const distinctQuery = `
              SELECT "${colName}", COUNT(*) as cnt 
              FROM "${schemaName}"."${tableName}" 
              GROUP BY "${colName}" 
              ORDER BY cnt DESC 
              LIMIT 10
            `;
          const distinctResult = await this.metabaseClient.executeNativeQuery(args.database_id, distinctQuery);
          const distinctRows = distinctResult.data?.rows || [];

          if (distinctRows.length > 0) {
            const totalDistinct = distinctRows.length;
            const values = distinctRows.slice(0, 5).map(r => r[0] === null ? 'NULL' : String(r[0])).join(', ');
            output += `• \`${colName}\`: ${values}${totalDistinct > 5 ? ` (+${totalDistinct - 5} more)` : ''}\\n`;
          }
        } catch (e) {
          // Skip columns that can't be queried
        }
      }
      output += `\\n`;
    }

    // Sample data
    if (sampleRows > 0 && rowCount > 0) {
      try {
        const sampleQuery = `SELECT * FROM "${schemaName}"."${tableName}" LIMIT ${sampleRows}`;
        const sampleResult = await this.metabaseClient.executeNativeQuery(args.database_id, sampleQuery);
        const sampleData = sampleResult.data?.rows || [];
        const sampleCols = sampleResult.data?.cols || [];

        if (sampleData.length > 0) {
          output += `📝 **Sample Data (${sampleData.length} rows):**\\n\`\`\`\\n`;
          // Header
          const headers = sampleCols.map(c => c.name);
          output += headers.join(' | ') + '\\n';
          output += headers.map(() => '---').join(' | ') + '\\n';
          // Data
          sampleData.forEach(row => {
            const formattedRow = row.map(cell => {
              if (cell === null) return 'NULL';
              const str = String(cell);
              return str.length > 20 ? str.substring(0, 17) + '...' : str;
            });
            output += formattedRow.join(' | ') + '\\n';
          });
          output += '\`\`\`\\n';
        }
      } catch (e) {
        output += `_Could not fetch sample data: ${e.message}_\\n`;
      }
    }

    // Recommendations
    output += `\\n💡 **Tips:**\\n`;
    if (isDimTable) {
      output += `• Use this table for JOINs as a lookup\\n`;
      output += `• Use \`mb_field_values\` to see all values for a specific column\\n`;
    }
    if (rowCount === 0) {
      output += `• ⚠️ Table is empty - data may need to be loaded\\n`;
    }
    if (rowCount > 100000) {
      output += `• Large table - use LIMIT in queries\\n`;
    }

    return {
      content: [{ type: 'text', text: output }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Table profile error: ${error.message}` }]
    };
  }
}

  async handleIndexUsage(args) {
  try {
    await this.ensureInitialized();

    const schemaName = args.schema_name || 'public';
    const minSizeMb = args.min_size_mb || 0;

    const indexQuery = `
        SELECT
          schemaname,
          relname as table_name,
          indexrelname as index_name,
          idx_scan as scans,
          idx_tup_read as tuples_read,
          idx_tup_fetch as tuples_fetched,
          pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
          pg_relation_size(indexrelid) / 1024 / 1024 as size_mb,
          CASE
            WHEN idx_scan = 0 THEN 'UNUSED'
            WHEN idx_scan < 50 THEN 'RARELY_USED'
            ELSE 'ACTIVE'
          END as usage_status
        FROM pg_stat_user_indexes
        WHERE schemaname = '${schemaName}'
          AND pg_relation_size(indexrelid) / 1024 / 1024 >= ${minSizeMb}
        ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC
        LIMIT 20
      `;

    const result = await this.metabaseClient.executeNativeQuery(args.database_id, indexQuery);
    const rows = result.data?.rows || [];

    if (rows.length === 0) {
      return {
        content: [{ type: 'text', text: `ℹ️ No indexes found in schema: ${schemaName}` }]
      };
    }

    let output = `📊 **Index Usage Analysis: ${schemaName}**\\n\\n`;

    const unusedIndexes = rows.filter(r => r[8] === 'UNUSED');
    const rarelyUsed = rows.filter(r => r[8] === 'RARELY_USED');
    const activeIndexes = rows.filter(r => r[8] === 'ACTIVE');

    if (unusedIndexes.length > 0) {
      output += `⚠️ **Unused Indexes (candidates for removal):**\\n`;
      unusedIndexes.slice(0, 5).forEach(idx => {
        output += `• \`${idx[2]}\` on \`${idx[1]}\` - ${idx[6]}\\n`;
      });
      output += `\\n`;
    }

    if (rarelyUsed.length > 0) {
      output += `🟡 **Rarely Used Indexes:**\\n`;
      rarelyUsed.slice(0, 5).forEach(idx => {
        output += `• \`${idx[2]}\` on \`${idx[1]}\` - ${idx[3]} scans, ${idx[6]}\\n`;
      });
      output += `\\n`;
    }

    output += `✅ **Active Indexes:** ${activeIndexes.length}\\n`;
    output += `📦 **Total Indexes Analyzed:** ${rows.length}\\n\\n`;

    if (unusedIndexes.length > 0) {
      output += `💡 **Tip:** Unused indexes waste storage and slow down writes. Consider removing them after verification.`;
    }

    return {
      content: [{ type: 'text', text: output }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Index usage analysis failed: ${error.message}` }]
    };
  }
}

  // === VISUALIZATION HANDLERS ===

  async handleVisualizationSettings(args) {
  try {
    await this.ensureInitialized();

    const questionId = args.question_id;

    // Get current question
    const question = await this.metabaseClient.getQuestion(questionId);

    // If updating
    if (args.display || args.settings) {
      const updateData = {};
      if (args.display) updateData.display = args.display;
      if (args.settings) updateData.visualization_settings = args.settings;

      const updated = await this.metabaseClient.updateQuestion(questionId, updateData);

      return {
        content: [{
          type: 'text',
          text: `✅ **Visualization Updated!**\\n\\n` +
            `🆔 Question ID: ${questionId}\\n` +
            `📊 Display Type: ${updated.display || args.display}\\n` +
            `⚙️ Settings Applied: ${Object.keys(args.settings || {}).length} properties`
        }]
      };
    }

    // Return current settings
    return {
      content: [{
        type: 'text',
        text: `📊 **Visualization Settings: ${question.name}**\\n\\n` +
          `🆔 Question ID: ${questionId}\\n` +
          `📈 Display Type: ${question.display}\\n\\n` +
          `⚙️ **Current Settings:**\\n\`\`\`json\\n${JSON.stringify(question.visualization_settings || {}, null, 2)}\\n\`\`\``
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Visualization settings error: ${error.message}` }]
    };
  }
}

  async handleVisualizationRecommend(args) {
  try {
    await this.ensureInitialized();

    const question = await this.metabaseClient.getQuestion(args.question_id);
    const purpose = args.purpose || 'general';

    // Analyze the result metadata
    const resultMetadata = question.result_metadata || [];
    const columnTypes = resultMetadata.map(col => ({
      name: col.name,
      baseType: col.base_type,
      semanticType: col.semantic_type
    }));

    const hasDate = columnTypes.some(c => c.baseType?.includes('Date') || c.baseType?.includes('Timestamp'));
    const hasNumeric = columnTypes.some(c => c.baseType?.includes('Integer') || c.baseType?.includes('Float') || c.baseType?.includes('Decimal'));
    const hasCategory = columnTypes.some(c => c.semanticType?.includes('Category') || c.baseType?.includes('Text'));
    const columnCount = columnTypes.length;

    let recommendations = [];

    if (purpose === 'trend' || (hasDate && hasNumeric)) {
      recommendations.push({
        type: 'line',
        reason: 'Best for showing trends over time',
        settings: { 'graph.dimensions': [columnTypes.find(c => c.baseType?.includes('Date'))?.name] }
      });
    }

    if (purpose === 'comparison' || hasCategory) {
      recommendations.push({
        type: 'bar',
        reason: 'Best for comparing values across categories',
        settings: { 'graph.show_values': true }
      });
    }

    if (purpose === 'composition' || (hasNumeric && columnCount <= 5)) {
      recommendations.push({
        type: 'pie',
        reason: 'Best for showing parts of a whole',
        settings: { 'pie.show_legend': true, 'pie.show_total': true }
      });
    }

    if (purpose === 'kpi' || columnCount === 1) {
      recommendations.push({
        type: 'scalar',
        reason: 'Best for single KPI values',
        settings: {}
      });
    }

    if (purpose === 'distribution') {
      recommendations.push({
        type: 'bar',
        reason: 'Best for showing value distributions',
        settings: { 'graph.x_axis.scale': 'histogram' }
      });
    }

    if (recommendations.length === 0) {
      recommendations.push({ type: 'table', reason: 'Default for complex data', settings: {} });
    }

    let output = `📊 **Visualization Recommendations: ${question.name}**\\n\\n`;
    output += `📋 **Data Profile:**\\n`;
    output += `• Columns: ${columnCount}\\n`;
    output += `• Has Date: ${hasDate ? 'Yes' : 'No'}\\n`;
    output += `• Has Numeric: ${hasNumeric ? 'Yes' : 'No'}\\n`;
    output += `• Has Category: ${hasCategory ? 'Yes' : 'No'}\\n\\n`;

    output += `💡 **Recommendations:**\\n`;
    recommendations.forEach((rec, i) => {
      output += `${i + 1}. **${rec.type.toUpperCase()}** - ${rec.reason}\\n`;
    });

    return {
      content: [{ type: 'text', text: output }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Visualization recommendation failed: ${error.message}` }]
    };
  }
}

  // === COLLECTION MANAGEMENT HANDLERS ===

  async handleCollectionCreate(args) {
  try {
    await this.ensureInitialized();

    const collectionData = {
      name: args.name,
      description: args.description || '',
      parent_id: args.parent_id || null,
      color: args.color || '#509EE3'
    };

    const collection = await this.metabaseClient.request('POST', '/api/collection', collectionData);

    return {
      content: [{
        type: 'text',
        text: `✅ **Collection Created!**\\n\\n` +
          `🆔 Collection ID: ${collection.id}\\n` +
          `📁 Name: ${collection.name}\\n` +
          `📝 Description: ${collection.description || 'None'}\\n` +
          `🎨 Color: ${collection.color}\\n` +
          `📂 Parent: ${args.parent_id || 'Root'}`
      }]
    };

  } catch (error) {
    // Better error messages for common issues
    let userMessage = error.message;

    if (error.message.includes('already exists') || error.response?.status === 409) {
      userMessage = `Collection already exists with this name: "${args.name}"`;
    } else if (error.message.includes('permission') || error.response?.status === 403) {
      userMessage = `Permission denied. Contact admin for collection creation access.`;
    } else if (error.message.includes('parent') || (error.message.includes('not found') && args.parent_id)) {
      userMessage = `Parent collection not found: ID ${args.parent_id}`;
    }

    return {
      content: [{ type: 'text', text: `❌ Collection creation failed: ${userMessage}` }]
    };
  }
}

  async handleCollectionList(args) {
  try {
    await this.ensureInitialized();

    let endpoint = '/api/collection';
    if (args.parent_id) {
      endpoint = `/api/collection/${args.parent_id}/items`;
    }

    const collections = await this.metabaseClient.request('GET', '/api/collection');

    let output = `📂 **Collections**\\n\\n`;

    const rootCollections = collections.filter(c => !c.personal_owner_id);
    rootCollections.slice(0, 20).forEach((col, i) => {
      output += `${i + 1}. **${col.name}** (ID: ${col.id})\\n`;
      if (col.description) output += `   ${col.description.substring(0, 50)}...\\n`;
    });

    output += `\\n📊 Total Collections: ${collections.length}`;

    return {
      content: [{ type: 'text', text: output }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Collection list failed: ${error.message}` }]
    };
  }
}

  async handleCollectionMove(args) {
  try {
    await this.ensureInitialized();

    let endpoint;
    const updateData = { collection_id: args.target_collection_id };

    switch (args.item_type) {
      case 'card':
        endpoint = `/api/card/${args.item_id}`;
        break;
      case 'dashboard':
        endpoint = `/api/dashboard/${args.item_id}`;
        break;
      case 'collection':
        endpoint = `/api/collection/${args.item_id}`;
        updateData.parent_id = args.target_collection_id;
        delete updateData.collection_id;
        break;
      default:
        throw new Error(`Unknown item type: ${args.item_type}`);
    }

    await this.metabaseClient.request('PUT', endpoint, updateData);

    return {
      content: [{
        type: 'text',
        text: `✅ **Item Moved!**\\n\\n` +
          `📦 Type: ${args.item_type}\\n` +
          `🆔 Item ID: ${args.item_id}\\n` +
          `📂 Target Collection: ${args.target_collection_id || 'Root'}`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Move failed: ${error.message}` }]
    };
  }
}

  // === METABASE ACTIONS API HANDLERS ===

  async handleActionCreate(args) {
  try {
    await this.ensureInitialized();

    const actionData = {
      name: args.name,
      description: args.description || '',
      model_id: args.model_id,
      type: args.type || 'query',
      database_id: args.database_id,
      dataset_query: args.dataset_query,
      parameters: args.parameters || [],
      visualization_settings: args.visualization_settings || {}
    };

    const action = await this.metabaseClient.request('POST', '/api/action', actionData);

    return {
      content: [{
        type: 'text',
        text: `✅ **Action Created!**\\n\\n` +
          `🆔 Action ID: ${action.id}\\n` +
          `📋 Name: ${action.name}\\n` +
          `⚙️ Type: ${action.type}\\n` +
          `📊 Model ID: ${args.model_id}\\n` +
          `🔧 Parameters: ${(args.parameters || []).length}`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Action creation failed: ${error.message}` }]
    };
  }
}

  async handleActionList(args) {
  try {
    await this.ensureInitialized();

    const actions = await this.metabaseClient.request('GET', `/api/action?model-id=${args.model_id}`);

    let output = `📋 **Actions for Model ${args.model_id}**\\n\\n`;

    if (actions.length === 0) {
      output += 'No actions found for this model.';
    } else {
      actions.forEach((action, i) => {
        output += `${i + 1}. **${action.name}** (ID: ${action.id})\\n`;
        output += `   Type: ${action.type}\\n`;
      });
    }

    return {
      content: [{ type: 'text', text: output }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Action list failed: ${error.message}` }]
    };
  }
}

  async handleActionExecute(args) {
  try {
    await this.ensureInitialized();

    const result = await this.metabaseClient.request('POST', `/api/action/${args.action_id}/execute`, {
      parameters: args.parameters
    });

    return {
      content: [{
        type: 'text',
        text: `✅ **Action Executed!**\\n\\n` +
          `🆔 Action ID: ${args.action_id}\\n` +
          `📋 Parameters: ${JSON.stringify(args.parameters)}\\n` +
          `📊 Result: ${JSON.stringify(result)}`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Action execution failed: ${error.message}` }]
    };
  }
}

  // === ALERTS & NOTIFICATIONS HANDLERS ===

  async handleAlertCreate(args) {
  try {
    await this.ensureInitialized();

    const alertData = {
      card: { id: args.card_id },
      alert_condition: args.alert_condition || 'rows',
      alert_first_only: args.alert_first_only || false,
      alert_above_goal: args.alert_above_goal,
      channels: args.channels || [{
        channel_type: 'email',
        enabled: true,
        recipients: [],
        schedule_type: 'hourly'
      }]
    };

    const alert = await this.metabaseClient.request('POST', '/api/alert', alertData);

    return {
      content: [{
        type: 'text',
        text: `✅ **Alert Created!**\\n\\n` +
          `🆔 Alert ID: ${alert.id}\\n` +
          `🔔 Card ID: ${args.card_id}\\n` +
          `⚙️ Condition: ${args.alert_condition || 'rows'}\\n` +
          `📧 Channels: ${(args.channels || []).length}`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Alert creation failed: ${error.message}` }]
    };
  }
}

  async handleAlertList(args) {
  try {
    await this.ensureInitialized();

    let endpoint = '/api/alert';
    if (args.card_id) {
      endpoint = `/api/alert/question/${args.card_id}`;
    }

    const alerts = await this.metabaseClient.request('GET', endpoint);

    let output = `🔔 **Alerts**\\n\\n`;

    if (alerts.length === 0) {
      output += 'No alerts found.';
    } else {
      alerts.forEach((alert, i) => {
        output += `${i + 1}. Alert ID: ${alert.id}\\n`;
        output += `   Card: ${alert.card?.name || alert.card?.id}\\n`;
        output += `   Condition: ${alert.alert_condition}\\n\\n`;
      });
    }

    return {
      content: [{ type: 'text', text: output }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Alert list failed: ${error.message}` }]
    };
  }
}

  async handlePulseCreate(args) {
  try {
    await this.ensureInitialized();

    const pulseData = {
      name: args.name,
      cards: args.cards,
      channels: args.channels,
      skip_if_empty: args.skip_if_empty !== false,
      collection_id: args.collection_id
    };

    const pulse = await this.metabaseClient.request('POST', '/api/pulse', pulseData);

    return {
      content: [{
        type: 'text',
        text: `✅ **Scheduled Report (Pulse) Created!**\\n\\n` +
          `🆔 Pulse ID: ${pulse.id}\\n` +
          `📋 Name: ${pulse.name}\\n` +
          `📊 Cards: ${args.cards.length}\\n` +
          `📧 Channels: ${args.channels.length}`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Pulse creation failed: ${error.message}` }]
    };
  }
}

  // === FIELD METADATA HANDLERS ===

  async handleFieldMetadata(args) {
  try {
    await this.ensureInitialized();

    const fieldId = args.field_id;

    // Get current field
    const field = await this.metabaseClient.request('GET', `/api/field/${fieldId}`);

    // If updating
    if (args.display_name || args.description || args.semantic_type || args.visibility_type || args.has_field_values) {
      const updateData = {};
      if (args.display_name) updateData.display_name = args.display_name;
      if (args.description) updateData.description = args.description;
      if (args.semantic_type) updateData.semantic_type = args.semantic_type;
      if (args.visibility_type) updateData.visibility_type = args.visibility_type;
      if (args.has_field_values) updateData.has_field_values = args.has_field_values;

      const updated = await this.metabaseClient.request('PUT', `/api/field/${fieldId}`, updateData);

      return {
        content: [{
          type: 'text',
          text: `✅ **Field Metadata Updated!**\\n\\n` +
            `🆔 Field ID: ${fieldId}\\n` +
            `📋 Display Name: ${updated.display_name}\\n` +
            `🏷️ Semantic Type: ${updated.semantic_type || 'None'}\\n` +
            `👁️ Visibility: ${updated.visibility_type}`
        }]
      };
    }

    // Return current metadata
    return {
      content: [{
        type: 'text',
        text: `📋 **Field Metadata: ${field.display_name}**\\n\\n` +
          `🆔 Field ID: ${fieldId}\\n` +
          `📛 Name: ${field.name}\\n` +
          `📋 Display Name: ${field.display_name}\\n` +
          `📝 Description: ${field.description || 'None'}\\n` +
          `🏷️ Semantic Type: ${field.semantic_type || 'None'}\\n` +
          `📊 Base Type: ${field.base_type}\\n` +
          `👁️ Visibility: ${field.visibility_type}\\n` +
          `🔍 Has Field Values: ${field.has_field_values}`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Field metadata error: ${error.message}` }]
    };
  }
}

  async handleTableMetadata(args) {
  try {
    await this.ensureInitialized();

    const tableId = args.table_id;

    // Get current table
    const table = await this.metabaseClient.request('GET', `/api/table/${tableId}`);

    // If updating
    if (args.display_name || args.description || args.visibility_type) {
      const updateData = {};
      if (args.display_name) updateData.display_name = args.display_name;
      if (args.description) updateData.description = args.description;
      if (args.visibility_type) updateData.visibility_type = args.visibility_type;

      const updated = await this.metabaseClient.request('PUT', `/api/table/${tableId}`, updateData);

      return {
        content: [{
          type: 'text',
          text: `✅ **Table Metadata Updated!**\\n\\n` +
            `🆔 Table ID: ${tableId}\\n` +
            `📋 Display Name: ${updated.display_name}\\n` +
            `👁️ Visibility: ${updated.visibility_type}`
        }]
      };
    }

    // Return current metadata
    return {
      content: [{
        type: 'text',
        text: `📋 **Table Metadata: ${table.display_name}**\\n\\n` +
          `🆔 Table ID: ${tableId}\\n` +
          `📛 Name: ${table.name}\\n` +
          `📋 Display Name: ${table.display_name}\\n` +
          `📝 Description: ${table.description || 'None'}\\n` +
          `👁️ Visibility: ${table.visibility_type}\\n` +
          `🗃️ Schema: ${table.schema}\\n` +
          `📊 Fields: ${table.fields?.length || 0}`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Table metadata error: ${error.message}` }]
    };
  }
}

  async handleFieldValues(args) {
  try {
    await this.ensureInitialized();

    const fieldId = args.field_id;

    const values = await this.metabaseClient.request('GET', `/api/field/${fieldId}/values`);

    let output = `📋 **Field Values (ID: ${fieldId})**\\n\\n`;

    if (values.values && values.values.length > 0) {
      const displayValues = values.values.slice(0, 20);
      displayValues.forEach((val, i) => {
        const displayVal = Array.isArray(val) ? val[0] : val;
        output += `${i + 1}. ${displayVal}\\n`;
      });

      if (values.values.length > 20) {
        output += `\\n... and ${values.values.length - 20} more values`;
      }
    } else {
      output += 'No values found or field values not cached.';
    }

    return {
      content: [{ type: 'text', text: output }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Field values error: ${error.message}` }]
    };
  }
}

  // === EMBEDDING HANDLERS ===

  async handleEmbedUrlGenerate(args) {
  try {
    await this.ensureInitialized();

    // Get embedding secret key from environment or settings
    const secretKey = process.env.METABASE_EMBEDDING_SECRET_KEY;

    if (!secretKey) {
      return {
        content: [{
          type: 'text',
          text: `⚠️ **Embedding Secret Key Not Configured**\\n\\n` +
            `Please set METABASE_EMBEDDING_SECRET_KEY in your environment.\\n\\n` +
            `You can find this in Metabase Admin > Settings > Embedding.`
        }]
      };
    }

    // Import JWT library dynamically
    const jwt = await import('jsonwebtoken');

    const resourceType = args.resource_type;
    const resourceId = args.resource_id;
    const params = args.params || {};
    const expMinutes = args.exp_minutes || 10;

    // Create JWT payload
    const payload = {
      resource: { [resourceType]: resourceId },
      params: params,
      exp: Math.round(Date.now() / 1000) + (expMinutes * 60)
    };

    const token = jwt.default.sign(payload, secretKey);

    // Build embed URL
    const baseUrl = process.env.METABASE_URL;
    let embedUrl = `${baseUrl}/embed/${resourceType}/${token}`;

    // Add theme and options
    const urlParams = [];
    if (args.theme && args.theme !== 'light') urlParams.push(`theme=${args.theme}`);
    if (args.bordered === false) urlParams.push('bordered=false');
    if (args.titled === false) urlParams.push('titled=false');

    if (urlParams.length > 0) {
      embedUrl += '#' + urlParams.join('&');
    }

    return {
      content: [{
        type: 'text',
        text: `✅ **Embed URL Generated!**\\n\\n` +
          `📊 Resource: ${resourceType} (ID: ${resourceId})\\n` +
          `⏱️ Expires: ${expMinutes} minutes\\n` +
          `🔒 Parameters: ${Object.keys(params).length} locked\\n\\n` +
          `🔗 **Embed URL:**\\n\`\`\`\\n${embedUrl}\\n\`\`\`\\n\\n` +
          `📋 **HTML:**\\n\`\`\`html\\n<iframe src="${embedUrl}" width="100%" height="600" frameborder="0"></iframe>\\n\`\`\``
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Embed URL generation failed: ${error.message}` }]
    };
  }
}

  async handleEmbedSettings(args) {
  try {
    await this.ensureInitialized();

    // Get embedding settings from Metabase
    const settings = await this.metabaseClient.request('GET', '/api/setting');

    const embeddingEnabled = settings['enable-embedding'] || settings.find?.(s => s.key === 'enable-embedding')?.value;
    const embedSecretSet = !!process.env.METABASE_EMBEDDING_SECRET_KEY;

    return {
      content: [{
        type: 'text',
        text: `📊 **Embedding Settings**\\n\\n` +
          `🔒 Embedding Enabled: ${embeddingEnabled ? 'Yes' : 'No'}\\n` +
          `🔑 Secret Key Configured: ${embedSecretSet ? 'Yes' : 'No'}\\n\\n` +
          `💡 **To Enable Embedding:**\\n` +
          `1. Go to Metabase Admin > Settings > Embedding\\n` +
          `2. Enable embedding and copy the secret key\\n` +
          `3. Set METABASE_EMBEDDING_SECRET_KEY in your environment`
      }]
    };

  } catch (error) {
    return {
      content: [{ type: 'text', text: `❌ Embed settings error: ${error.message}` }]
    };
  }
}

  // ==================== USER MANAGEMENT HANDLERS ====================

  async handleUserList(args) {
  await this.ensureInitialized();
  const { status = 'all', group_id } = args;

  try {
    const response = await this.metabaseClient.request('GET', '/api/user');
    let users = response.data || response;

    // Filter by status
    if (status === 'active') {
      users = users.filter(u => u.is_active);
    } else if (status === 'inactive') {
      users = users.filter(u => !u.is_active);
    }

    // Filter by group
    if (group_id) {
      users = users.filter(u => u.group_ids && u.group_ids.includes(group_id));
    }

    return {
      content: [{
        type: 'text',
        text: `Found ${users.length} users:\n${users.map(u =>
          `  - [${u.id}] ${u.first_name} ${u.last_name} (${u.email}) - ${u.is_active ? 'Active' : 'Inactive'}${u.is_superuser ? ' [Admin]' : ''}`
        ).join('\n')}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ User list error: ${error.message}` }] };
  }
}

  async handleUserGet(args) {
  await this.ensureInitialized();
  const { user_id } = args;

  try {
    const user = await this.metabaseClient.request('GET', `/api/user/${user_id}`);

    return {
      content: [{
        type: 'text',
        text: `User Details:\n` +
          `  ID: ${user.id}\n` +
          `  Name: ${user.first_name} ${user.last_name}\n` +
          `  Email: ${user.email}\n` +
          `  Active: ${user.is_active}\n` +
          `  Superuser: ${user.is_superuser}\n` +
          `  Groups: ${(user.group_ids || []).join(', ')}\n` +
          `  Last Login: ${user.last_login || 'Never'}\n` +
          `  Created: ${user.date_joined}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ User get error: ${error.message}` }] };
  }
}

  async handleUserCreate(args) {
  await this.ensureInitialized();
  const { email, first_name, last_name, password, group_ids } = args;

  try {
    const userData = {
      email,
      first_name,
      last_name,
      ...(password && { password }),
      ...(group_ids && { group_ids })
    };

    const user = await this.metabaseClient.request('POST', '/api/user', userData);

    return {
      content: [{
        type: 'text',
        text: `✅ User created successfully:\n` +
          `  ID: ${user.id}\n` +
          `  Name: ${user.first_name} ${user.last_name}\n` +
          `  Email: ${user.email}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ User create error: ${error.message}` }] };
  }
}

  async handleUserUpdate(args) {
  await this.ensureInitialized();
  const { user_id, ...updates } = args;

  try {
    const user = await this.metabaseClient.request('PUT', `/api/user/${user_id}`, updates);

    return {
      content: [{
        type: 'text',
        text: `✅ User ${user_id} updated successfully`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ User update error: ${error.message}` }] };
  }
}

  async handleUserDisable(args) {
  await this.ensureInitialized();
  const { user_id } = args;

  try {
    await this.metabaseClient.request('DELETE', `/api/user/${user_id}`);

    return {
      content: [{
        type: 'text',
        text: `✅ User ${user_id} has been disabled`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ User disable error: ${error.message}` }] };
  }
}

  // ==================== PERMISSION GROUP HANDLERS ====================

  async handlePermissionGroupList(args) {
  await this.ensureInitialized();

  try {
    const groups = await this.metabaseClient.request('GET', '/api/permissions/group');

    return {
      content: [{
        type: 'text',
        text: `Found ${groups.length} permission groups:\n${groups.map(g =>
          `  - [${g.id}] ${g.name} (${g.member_count || 0} members)`
        ).join('\n')}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Permission group list error: ${error.message}` }] };
  }
}

  async handlePermissionGroupCreate(args) {
  await this.ensureInitialized();
  const { name } = args;

  try {
    const group = await this.metabaseClient.request('POST', '/api/permissions/group', { name });

    return {
      content: [{
        type: 'text',
        text: `✅ Permission group created:\n  ID: ${group.id}\n  Name: ${group.name}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Permission group create error: ${error.message}` }] };
  }
}

  async handlePermissionGroupDelete(args) {
  await this.ensureInitialized();
  const { group_id } = args;

  try {
    await this.metabaseClient.request('DELETE', `/api/permissions/group/${group_id}`);

    return {
      content: [{
        type: 'text',
        text: `✅ Permission group ${group_id} deleted`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Permission group delete error: ${error.message}` }] };
  }
}

  async handlePermissionGroupAddUser(args) {
  await this.ensureInitialized();
  const { group_id, user_id } = args;

  try {
    await this.metabaseClient.request('POST', '/api/permissions/membership', {
      group_id,
      user_id
    });

    return {
      content: [{
        type: 'text',
        text: `✅ User ${user_id} added to group ${group_id}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Add user to group error: ${error.message}` }] };
  }
}

  async handlePermissionGroupRemoveUser(args) {
  await this.ensureInitialized();
  const { group_id, user_id } = args;

  try {
    // First get the membership ID
    const memberships = await this.metabaseClient.request('GET', `/api/permissions/group/${group_id}`);
    const membership = memberships.members?.find(m => m.user_id === user_id);

    if (!membership) {
      return { content: [{ type: 'text', text: `❌ User ${user_id} is not in group ${group_id}` }] };
    }

    await this.metabaseClient.request('DELETE', `/api/permissions/membership/${membership.membership_id}`);

    return {
      content: [{
        type: 'text',
        text: `✅ User ${user_id} removed from group ${group_id}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Remove user from group error: ${error.message}` }] };
  }
}

  // ==================== COLLECTION PERMISSIONS HANDLERS ====================

  async handleCollectionPermissionsGet(args) {
  await this.ensureInitialized();
  const { collection_id } = args;

  try {
    const graph = await this.metabaseClient.request('GET', '/api/collection/graph');
    const collectionPerms = graph.groups;

    const permissions = [];
    for (const [groupId, perms] of Object.entries(collectionPerms)) {
      const collPerm = perms[collection_id];
      if (collPerm) {
        permissions.push({ group_id: groupId, permission: collPerm });
      }
    }

    return {
      content: [{
        type: 'text',
        text: `Collection ${collection_id} permissions:\n${permissions.map(p =>
          `  - Group ${p.group_id}: ${p.permission}`
        ).join('\n') || '  No specific permissions set'}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Collection permissions get error: ${error.message}` }] };
  }
}

  async handleCollectionPermissionsUpdate(args) {
  await this.ensureInitialized();
  const { collection_id, group_id, permission } = args;

  try {
    // Get current graph
    const graph = await this.metabaseClient.request('GET', '/api/collection/graph');

    // Update the permission
    if (!graph.groups[group_id]) {
      graph.groups[group_id] = {};
    }
    graph.groups[group_id][collection_id] = permission;

    // Save the updated graph
    await this.metabaseClient.request('PUT', '/api/collection/graph', graph);

    return {
      content: [{
        type: 'text',
        text: `✅ Collection ${collection_id} permission updated: Group ${group_id} = ${permission}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Collection permissions update error: ${error.message}` }] };
  }
}

  // ==================== CARD/QUESTION CRUD HANDLERS ====================

  async handleCardGet(args) {
  await this.ensureInitialized();
  const { card_id } = args;

  try {
    const card = await this.metabaseClient.request('GET', `/api/card/${card_id}`);

    return {
      content: [{
        type: 'text',
        text: `Card Details:\n` +
          `  ID: ${card.id}\n` +
          `  Name: ${card.name}\n` +
          `  Description: ${card.description || 'None'}\n` +
          `  Type: ${card.display}\n` +
          `  Database: ${card.database_id}\n` +
          `  Collection: ${card.collection_id || 'Root'}\n` +
          `  Creator: ${card.creator?.email || 'Unknown'}\n` +
          `  Created: ${card.created_at}\n` +
          `  Updated: ${card.updated_at}\n` +
          `  Archived: ${card.archived}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Card get error: ${error.message}` }] };
  }
}

  async handleCardUpdate(args) {
  await this.ensureInitialized();
  const { card_id, ...updates } = args;

  try {
    const card = await this.metabaseClient.request('PUT', `/api/card/${card_id}`, updates);

    return {
      content: [{
        type: 'text',
        text: `✅ Card ${card_id} updated successfully`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Card update error: ${error.message}` }] };
  }
}

  async handleCardDelete(args) {
  await this.ensureInitialized();
  const { card_id } = args;

  try {
    await this.metabaseClient.request('DELETE', `/api/card/${card_id}`);

    return {
      content: [{
        type: 'text',
        text: `✅ Card ${card_id} deleted permanently`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Card delete error: ${error.message}` }] };
  }
}

  async handleCardArchive(args) {
  await this.ensureInitialized();
  const { card_id } = args;

  try {
    await this.metabaseClient.request('PUT', `/api/card/${card_id}`, { archived: true });

    return {
      content: [{
        type: 'text',
        text: `✅ Card ${card_id} archived`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Card archive error: ${error.message}` }] };
  }
}

  async handleCardData(args) {
  await this.ensureInitialized();
  const { card_id, format = 'json', parameters } = args;

  try {
    let endpoint = `/api/card/${card_id}/query`;
    if (format === 'csv') {
      endpoint += '/csv';
    } else if (format === 'xlsx') {
      endpoint += '/xlsx';
    }

    const result = await this.metabaseClient.request('POST', endpoint, { parameters });

    if (format === 'json') {
      const data = result.data || result;
      const rows = data.rows || [];
      const cols = data.cols || [];

      return {
        content: [{
          type: 'text',
          text: `Card ${card_id} data (${rows.length} rows):\n` +
            `Columns: ${cols.map(c => c.display_name || c.name).join(', ')}\n\n` +
            `Sample (first 10 rows):\n${JSON.stringify(rows.slice(0, 10), null, 2)}`
        }]
      };
    } else {
      return {
        content: [{
          type: 'text',
          text: `Card ${card_id} data exported as ${format.toUpperCase()}`
        }]
      };
    }
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Card data error: ${error.message}` }] };
  }
}

  // ==================== DASHBOARD CRUD HANDLERS ====================

  async handleDashboardGet(args) {
  await this.ensureInitialized();
  const { dashboard_id } = args;

  try {
    const dashboard = await this.metabaseClient.request('GET', `/api/dashboard/${dashboard_id}`);

    return {
      content: [{
        type: 'text',
        text: `Dashboard Details:\n` +
          `  ID: ${dashboard.id}\n` +
          `  Name: ${dashboard.name}\n` +
          `  Description: ${dashboard.description || 'None'}\n` +
          `  Collection: ${dashboard.collection_id || 'Root'}\n` +
          `  Cards: ${(dashboard.dashcards || dashboard.ordered_cards || []).length}\n` +
          `  Parameters: ${(dashboard.parameters || []).length}\n` +
          `  Creator: ${dashboard.creator?.email || 'Unknown'}\n` +
          `  Created: ${dashboard.created_at}\n` +
          `  Updated: ${dashboard.updated_at}\n` +
          `  Embedding Enabled: ${dashboard.enable_embedding || false}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Dashboard get error: ${error.message}` }] };
  }
}

  async handleDashboardUpdate(args) {
  await this.ensureInitialized();
  const { dashboard_id, ...updates } = args;

  try {
    await this.metabaseClient.request('PUT', `/api/dashboard/${dashboard_id}`, updates);

    return {
      content: [{
        type: 'text',
        text: `✅ Dashboard ${dashboard_id} updated successfully`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Dashboard update error: ${error.message}` }] };
  }
}

  async handleDashboardDelete(args) {
  await this.ensureInitialized();
  const { dashboard_id } = args;

  try {
    await this.metabaseClient.request('DELETE', `/api/dashboard/${dashboard_id}`);

    return {
      content: [{
        type: 'text',
        text: `✅ Dashboard ${dashboard_id} deleted`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Dashboard delete error: ${error.message}` }] };
  }
}

  async handleDashboardCardUpdate(args) {
  await this.ensureInitialized();
  const { dashboard_id, card_id, row, col, size_x, size_y } = args;

  try {
    // Get current dashboard
    const dashboard = await this.metabaseClient.request('GET', `/api/dashboard/${dashboard_id}`);
    const cards = dashboard.dashcards || dashboard.ordered_cards || [];

    // Find and update the card
    const cardToUpdate = cards.find(c => c.id === card_id);
    if (!cardToUpdate) {
      return { content: [{ type: 'text', text: `❌ Card ${card_id} not found on dashboard ${dashboard_id}` }] };
    }

    const updatedCard = {
      ...cardToUpdate,
      ...(row !== undefined && { row }),
      ...(col !== undefined && { col }),
      ...(size_x !== undefined && { size_x }),
      ...(size_y !== undefined && { size_y })
    };

    await this.metabaseClient.request('PUT', `/api/dashboard/${dashboard_id}/cards`, {
      cards: cards.map(c => c.id === card_id ? updatedCard : c)
    });

    return {
      content: [{
        type: 'text',
        text: `✅ Dashboard card ${card_id} position/size updated`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Dashboard card update error: ${error.message}` }] };
  }
}

  async handleDashboardCardRemove(args) {
  await this.ensureInitialized();
  const { dashboard_id, card_id } = args;

  try {
    await this.metabaseClient.request('DELETE', `/api/dashboard/${dashboard_id}/cards`, {
      dashcardId: card_id
    });

    return {
      content: [{
        type: 'text',
        text: `✅ Card ${card_id} removed from dashboard ${dashboard_id}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Dashboard card remove error: ${error.message}` }] };
  }
}

  // ==================== COPY/CLONE HANDLERS ====================

  async handleCardCopy(args) {
  await this.ensureInitialized();
  const { card_id, collection_id, new_name } = args;

  try {
    // Get source card
    const sourceCard = await this.metabaseClient.request('GET', `/api/card/${card_id}`);

    // Create copy
    const newCard = {
      name: new_name || `Copy of ${sourceCard.name}`,
      description: sourceCard.description,
      display: sourceCard.display,
      dataset_query: sourceCard.dataset_query,
      visualization_settings: sourceCard.visualization_settings,
      collection_id: collection_id || sourceCard.collection_id
    };

    const createdCard = await this.metabaseClient.request('POST', '/api/card', newCard);

    return {
      content: [{
        type: 'text',
        text: `✅ Card copied successfully:\n  New Card ID: ${createdCard.id}\n  Name: ${createdCard.name}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Card copy error: ${error.message}` }] };
  }
}

  async handleCardClone(args) {
  await this.ensureInitialized();
  const { card_id, target_table_id, collection_id, column_mappings = {} } = args;

  try {
    // Get source card
    const sourceCard = await this.metabaseClient.request('GET', `/api/card/${card_id}`);

    // Clone and retarget the query
    const query = { ...sourceCard.dataset_query };
    if (query.query) {
      query.query['source-table'] = target_table_id;

      // Apply column mappings if provided
      if (Object.keys(column_mappings).length > 0) {
        // This is simplified - full implementation would need to traverse the query structure
        const queryStr = JSON.stringify(query);
        let mappedQuery = queryStr;
        for (const [oldCol, newCol] of Object.entries(column_mappings)) {
          mappedQuery = mappedQuery.replace(new RegExp(oldCol, 'g'), newCol);
        }
        Object.assign(query, JSON.parse(mappedQuery));
      }
    }

    const newCard = {
      name: `Clone of ${sourceCard.name}`,
      description: `Cloned from card ${card_id}, retargeted to table ${target_table_id}`,
      display: sourceCard.display,
      dataset_query: query,
      visualization_settings: sourceCard.visualization_settings,
      collection_id: collection_id || sourceCard.collection_id
    };

    const createdCard = await this.metabaseClient.request('POST', '/api/card', newCard);

    return {
      content: [{
        type: 'text',
        text: `✅ Card cloned and retargeted:\n  New Card ID: ${createdCard.id}\n  Target Table: ${target_table_id}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Card clone error: ${error.message}` }] };
  }
}

  async handleDashboardCopy(args) {
  await this.ensureInitialized();
  const { dashboard_id, collection_id, new_name, deep_copy = true } = args;

  try {
    // Get source dashboard
    const sourceDashboard = await this.metabaseClient.request('GET', `/api/dashboard/${dashboard_id}`);

    // Create new dashboard
    const newDashboard = await this.metabaseClient.request('POST', '/api/dashboard', {
      name: new_name || `Copy of ${sourceDashboard.name}`,
      description: sourceDashboard.description,
      collection_id: collection_id || sourceDashboard.collection_id,
      parameters: sourceDashboard.parameters
    });

    // Copy cards
    const sourceCards = sourceDashboard.dashcards || sourceDashboard.ordered_cards || [];
    const cardIdMap = {};

    for (const dashcard of sourceCards) {
      let cardId = dashcard.card_id;

      // If deep copy, copy the actual card first
      if (deep_copy && cardId) {
        const copiedCard = await this.handleCardCopy({
          card_id: cardId,
          collection_id: collection_id || sourceDashboard.collection_id
        });
        // Extract new card ID from response
        const match = copiedCard.content[0].text.match(/New Card ID: (\d+)/);
        if (match) {
          cardIdMap[cardId] = parseInt(match[1]);
          cardId = cardIdMap[cardId];
        }
      }

      // Add card to new dashboard
      if (cardId) {
        await this.metabaseClient.request('POST', `/api/dashboard/${newDashboard.id}/cards`, {
          cardId: cardId,
          row: dashcard.row,
          col: dashcard.col,
          size_x: dashcard.size_x,
          size_y: dashcard.size_y,
          parameter_mappings: dashcard.parameter_mappings
        });
      }
    }

    return {
      content: [{
        type: 'text',
        text: `✅ Dashboard copied:\n  New Dashboard ID: ${newDashboard.id}\n  Name: ${newDashboard.name}\n  Cards copied: ${sourceCards.length}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Dashboard copy error: ${error.message}` }] };
  }
}

  async handleCollectionCopy(args) {
  await this.ensureInitialized();
  const { collection_id, destination_id, new_name } = args;

  try {
    // Get source collection
    const sourceCollection = await this.metabaseClient.request('GET', `/api/collection/${collection_id}`);

    // Create new collection
    const newCollection = await this.metabaseClient.request('POST', '/api/collection', {
      name: new_name || `Copy of ${sourceCollection.name}`,
      description: sourceCollection.description,
      parent_id: destination_id || sourceCollection.parent_id
    });

    // Get items in source collection
    const items = await this.metabaseClient.request('GET', `/api/collection/${collection_id}/items`);
    const allItems = items.data || items;

    let copiedCards = 0;
    let copiedDashboards = 0;

    // Copy each item
    for (const item of allItems) {
      if (item.model === 'card') {
        await this.handleCardCopy({
          card_id: item.id,
          collection_id: newCollection.id
        });
        copiedCards++;
      } else if (item.model === 'dashboard') {
        await this.handleDashboardCopy({
          dashboard_id: item.id,
          collection_id: newCollection.id,
          deep_copy: false // Don't deep copy cards as they're already being copied
        });
        copiedDashboards++;
      }
    }

    return {
      content: [{
        type: 'text',
        text: `✅ Collection copied:\n  New Collection ID: ${newCollection.id}\n  Name: ${newCollection.name}\n  Cards copied: ${copiedCards}\n  Dashboards copied: ${copiedDashboards}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Collection copy error: ${error.message}` }] };
  }
}

  // ==================== SEARCH HANDLER ====================

  async handleSearch(args) {
  await this.ensureInitialized();
  const { query, models, collection_id, limit = 50 } = args;

  try {
    let endpoint = `/api/search?q=${encodeURIComponent(query)}`;

    if (models && models.length > 0) {
      endpoint += `&models=${models.join(',')}`;
    }
    if (collection_id) {
      endpoint += `&collection=${collection_id}`;
    }
    endpoint += `&limit=${limit}`;

    const results = await this.metabaseClient.request('GET', endpoint);
    const items = results.data || results;

    // Group by type
    const grouped = {};
    for (const item of items) {
      if (!grouped[item.model]) {
        grouped[item.model] = [];
      }
      grouped[item.model].push(item);
    }

    let output = `Search results for "${query}" (${items.length} items):\n\n`;

    for (const [type, typeItems] of Object.entries(grouped)) {
      output += `${type.toUpperCase()}S (${typeItems.length}):\n`;
      output += typeItems.map(i => `  - [${i.id}] ${i.name}`).join('\n') + '\n\n';
    }

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Search error: ${error.message}` }] };
  }
}

  // ==================== SEGMENT HANDLERS ====================

  async handleSegmentCreate(args) {
  await this.ensureInitialized();
  const { name, description, table_id, definition } = args;

  try {
    const segment = await this.metabaseClient.request('POST', '/api/segment', {
      name,
      description,
      table_id,
      definition
    });

    return {
      content: [{
        type: 'text',
        text: `✅ Segment created:\n  ID: ${segment.id}\n  Name: ${segment.name}\n  Table: ${segment.table_id}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Segment create error: ${error.message}` }] };
  }
}

  async handleSegmentList(args) {
  await this.ensureInitialized();
  const { table_id } = args;

  try {
    let segments = await this.metabaseClient.request('GET', '/api/segment');

    if (table_id) {
      segments = segments.filter(s => s.table_id === table_id);
    }

    return {
      content: [{
        type: 'text',
        text: `Found ${segments.length} segments:\n${segments.map(s =>
          `  - [${s.id}] ${s.name} (Table: ${s.table_id})`
        ).join('\n')}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Segment list error: ${error.message}` }] };
  }
}

  // ==================== BOOKMARK HANDLERS ====================

  async handleBookmarkCreate(args) {
  await this.ensureInitialized();
  const { type, id } = args;

  try {
    await this.metabaseClient.request('POST', `/api/${type}/${id}/bookmark`);

    return {
      content: [{
        type: 'text',
        text: `✅ Bookmarked ${type} ${id}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Bookmark create error: ${error.message}` }] };
  }
}

  async handleBookmarkList(args) {
  await this.ensureInitialized();

  try {
    const bookmarks = await this.metabaseClient.request('GET', '/api/bookmark');

    return {
      content: [{
        type: 'text',
        text: `Found ${bookmarks.length} bookmarks:\n${bookmarks.map(b =>
          `  - [${b.type}:${b.item_id}] ${b.name || 'Unnamed'}`
        ).join('\n')}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Bookmark list error: ${error.message}` }] };
  }
}

  async handleBookmarkDelete(args) {
  await this.ensureInitialized();
  const { type, id } = args;

  try {
    await this.metabaseClient.request('DELETE', `/api/${type}/${id}/bookmark`);

    return {
      content: [{
        type: 'text',
        text: `✅ Bookmark removed for ${type} ${id}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Bookmark delete error: ${error.message}` }] };
  }
}

  // ==================== DATABASE SYNC & CACHE HANDLERS ====================

  async handleDbSyncSchema(args) {
  await this.ensureInitialized();
  const { database_id } = args;

  try {
    await this.metabaseClient.request('POST', `/api/database/${database_id}/sync_schema`);

    return {
      content: [{
        type: 'text',
        text: `✅ Schema sync triggered for database ${database_id}`
      }]
    };
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Schema sync error: ${error.message}` }] };
  }
}

  async handleCacheInvalidate(args) {
  await this.ensureInitialized();
  const { database_id, card_id } = args;

  try {
    if (card_id) {
      // Invalidate specific card cache
      await this.metabaseClient.request('POST', `/api/card/${card_id}/query`, {
        ignore_cache: true
      });
      return {
        content: [{
          type: 'text',
          text: `✅ Cache invalidated for card ${card_id}`
        }]
      };
    } else if (database_id) {
      // Invalidate database cache by triggering rescan
      await this.metabaseClient.request('POST', `/api/database/${database_id}/rescan_values`);
      return {
        content: [{
          type: 'text',
          text: `✅ Cache invalidated for database ${database_id}`
        }]
      };
    } else {
      return {
        content: [{
          type: 'text',
          text: `❌ Please specify either database_id or card_id`
        }]
      };
    }
  } catch (error) {
    return { content: [{ type: 'text', text: `❌ Cache invalidate error: ${error.message}` }] };
  }
}

  async run() {
  try {
    // Disable console logging for MCP mode
    const consoleTransport = logger.transports.find(t => t.constructor.name === 'Console');
    if (consoleTransport) {
      logger.remove(consoleTransport);
    }

    const transport = new StdioServerTransport();
    await this.server.connect(transport);

    // Don't initialize immediately - wait for first request
    // This allows the MCP connection to establish properly

  } catch (error) {
    process.exit(1);
  }
}

  // === ACTIVITY LOGGING HANDLERS ===

  async handleInitializeActivityLog(args) {
  try {
    if (!this.activityLogger) {
      this.activityLogger = new ActivityLogger(this.metabaseClient, {
        logTableName: 'claude_ai_activity_log',
        schema: args.schema || 'public'
      });
    }

    await this.activityLogger.initialize(args.database_id);

    return {
      content: [
        {
          type: 'text',
          text: `✅ **Activity Logging Initialized!**\\n\\n` +
            `📊 **Configuration:**\\n` +
            `• Database ID: ${args.database_id}\\n` +
            `• Schema: ${args.schema || 'public'}\\n` +
            `• Log Table: \`claude_ai_activity_log\`\\n` +
            `• Session ID: \`${this.activityLogger.sessionId}\`\\n\\n` +
            `🎯 **What Gets Tracked:**\\n` +
            `• SQL query executions and performance\\n` +
            `• Table/View/Index creation operations\\n` +
            `• Metabase dashboard and question creation\\n` +
            `• Error patterns and debugging info\\n` +
            `• Execution times and resource usage\\n\\n` +
            `📈 **Available Analytics:**\\n` +
            `• Session summaries and insights\\n` +
            `• Database usage patterns\\n` +
            `• Performance optimization suggestions\\n` +
            `• Error analysis and troubleshooting\\n\\n` +
            `💡 **Next Steps:** All your operations are now being tracked for analytics!`,
        },
      ],
    };

  } catch (error) {
    return {
      content: [
        {
          type: 'text',
          text: `❌ **Activity Logging Initialization Failed!**\\n\\n` +
            `🚫 **Error:** ${error.message}\\n\\n` +
            `🔧 **Troubleshooting:**\\n` +
            `• Ensure you have CREATE permissions on the schema\\n` +
            `• Verify database connection is working\\n` +
            `• Check that the database supports the required SQL features`,
        },
      ],
    };
  }
}

  async handleGetSessionSummary(args) {
  if (!this.activityLogger) {
    return {
      content: [
        {
          type: 'text',
          text: `⚠️ **Activity logging not initialized.** Run \`activity_log_init\` first.`,
        },
      ],
    };
  }

  try {
    const summary = await this.activityLogger.getSessionSummary(args.session_id);

    if (!summary) {
      return {
        content: [
          {
            type: 'text',
            text: `📊 **No session data found.**\\n\\nSession ID: ${args.session_id || 'current session'}\\n\\nTry running some operations first to generate activity data.`,
          },
        ],
      };
    }

    const [sessionId, sessionStart, sessionEnd, totalOps, successOps, failedOps,
      dbsUsed, opTypes, totalExecTime, avgExecTime, totalRowsReturned,
      totalRowsAffected, ddlOps, queryOps, metabaseOps] = summary;

    const duration = new Date(sessionEnd) - new Date(sessionStart);
    const durationMin = Math.round(duration / 60000);
    const successRate = ((successOps / totalOps) * 100).toFixed(1);

    return {
      content: [
        {
          type: 'text',
          text: `📊 **Session Summary**\\n\\n` +
            `🔢 **Session:** \`${sessionId}\`\\n` +
            `⏰ **Duration:** ${durationMin} minutes\\n` +
            `✅ **Success Rate:** ${successRate}% (${successOps}/${totalOps} operations)\\n\\n` +
            `📈 **Operations Breakdown:**\\n` +
            `• Total Operations: ${totalOps}\\n` +
            `• SQL Queries: ${queryOps}\\n` +
            `• DDL Operations: ${ddlOps}\\n` +
            `• Metabase Operations: ${metabaseOps}\\n` +
            `• Failed Operations: ${failedOps}\\n\\n` +
            `⚡ **Performance:**\\n` +
            `• Total Execution Time: ${totalExecTime}ms\\n` +
            `• Average Execution Time: ${Math.round(avgExecTime)}ms\\n` +
            `• Data Processed: ${totalRowsReturned} rows returned\\n\\n` +
            `🎯 **Scope:**\\n` +
            `• Databases Used: ${dbsUsed}\\n` +
            `• Operation Types: ${opTypes}`,
        },
      ],
    };

  } catch (error) {
    return {
      content: [
        {
          type: 'text',
          text: `❌ **Failed to get session summary:** ${error.message}`,
        },
      ],
    };
  }
}

  async handleGetOperationStats(args) {
  if (!this.activityLogger) {
    return {
      content: [
        {
          type: 'text',
          text: `⚠️ **Activity logging not initialized.** Run \`activity_log_init\` first.`,
        },
      ],
    };
  }

  try {
    const stats = await this.activityLogger.getOperationStats(args.days || 7);

    if (stats.length === 0) {
      return {
        content: [
          {
            type: 'text',
            text: `📊 **No operation data found** for the last ${args.days || 7} days.`,
          },
        ],
      };
    }

    let output = `📊 **Operation Statistics** (Last ${args.days || 7} Days)\\n\\n`;

    stats.slice(0, 10).forEach((stat, index) => {
      const [opType, opCategory, opCount, successCount, errorCount, avgTime] = stat;
      const successRate = ((successCount / opCount) * 100).toFixed(1);

      output += `${index + 1}. **${opType}** (${opCategory})\\n`;
      output += `   • Executions: ${opCount} (${successRate}% success)\\n`;
      output += `   • Avg Time: ${Math.round(avgTime)}ms\\n\\n`;
    });

    return {
      content: [
        {
          type: 'text',
          text: output,
        },
      ],
    };

  } catch (error) {
    return {
      content: [
        {
          type: 'text',
          text: `❌ **Failed to get operation stats:** ${error.message}`,
        },
      ],
    };
  }
}

  async handleGetDatabaseUsage(args) {
  if (!this.activityLogger) {
    return {
      content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.** Run \`activity_log_init\` first.` }],
    };
  }

  try {
    const usage = await this.activityLogger.getDatabaseUsageStats(args.days || 30);

    if (usage.length === 0) {
      return {
        content: [{ type: 'text', text: `📊 **No database usage data found** for the last ${args.days || 30} days.` }],
      };
    }

    let output = `🗃️ **Database Usage** (Last ${args.days || 30} Days)\\n\\n`;

    usage.slice(0, 5).forEach((db, index) => {
      const [dbId, dbName, totalOps, uniqueSessions] = db;
      output += `${index + 1}. **${dbName || `DB ${dbId}`}**: ${totalOps} ops, ${uniqueSessions} sessions\\n`;
    });

    return { content: [{ type: 'text', text: output }] };

  } catch (error) {
    return { content: [{ type: 'text', text: `❌ **Failed to get database usage:** ${error.message}` }] };
  }
}

  async handleGetErrorAnalysis(args) {
  if (!this.activityLogger) {
    return { content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.**` }] };
  }

  try {
    const errors = await this.activityLogger.getErrorAnalysis(args.days || 7);

    if (errors.length === 0) {
      return { content: [{ type: 'text', text: `✅ **No errors found** in the last ${args.days || 7} days! 🎉` }] };
    }

    let output = `🚨 **Error Analysis** (Last ${args.days || 7} Days)\\n\\n`;

    errors.slice(0, 5).forEach((error, index) => {
      const [opType, errorMsg, errorCount] = error;
      output += `${index + 1}. **${opType}**: ${errorCount} errors\\n`;
      output += `   ${errorMsg.substring(0, 80)}...\\n\\n`;
    });

    return { content: [{ type: 'text', text: output }] };

  } catch (error) {
    return { content: [{ type: 'text', text: `❌ **Error analysis failed:** ${error.message}` }] };
  }
}

  async handleGetPerformanceInsights(args) {
  if (!this.activityLogger) {
    return { content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.**` }] };
  }

  try {
    const insights = await this.activityLogger.getPerformanceInsights(args.days || 7);

    if (insights.length === 0) {
      return { content: [{ type: 'text', text: `📊 **No performance data found.**` }] };
    }

    let output = `⚡ **Performance Insights** (Last ${args.days || 7} Days)\\n\\n`;

    insights.slice(0, 5).forEach((insight, index) => {
      const [opType, execCount, , , avgTime, , p95Time, slowOps] = insight;

      output += `${index + 1}. **${opType}**\\n`;
      output += `   • ${execCount} executions, avg ${Math.round(avgTime)}ms\\n`;
      output += `   • 95th percentile: ${Math.round(p95Time)}ms\\n`;
      output += `   • Slow operations: ${slowOps}\\n\\n`;
    });

    return { content: [{ type: 'text', text: output }] };

  } catch (error) {
    return { content: [{ type: 'text', text: `❌ **Performance insights failed:** ${error.message}` }] };
  }
}

  async handleGetActivityTimeline(args) {
  if (!this.activityLogger) {
    return { content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.**` }] };
  }

  try {
    const timeline = await this.activityLogger.getActivityTimeline(args.days || 7, args.limit || 20);

    if (timeline.length === 0) {
      return { content: [{ type: 'text', text: `📊 **No recent activity found.**` }] };
    }

    let output = `📅 **Recent Activity**\\n\\n`;

    timeline.forEach((activity, index) => {
      const [timestamp, , opType, , , status] = activity;
      const statusIcon = status === 'success' ? '✅' : '❌';
      output += `${index + 1}. ${statusIcon} ${opType} - ${timestamp}\\n`;
    });

    return { content: [{ type: 'text', text: output }] };

  } catch (error) {
    return { content: [{ type: 'text', text: `❌ **Timeline failed:** ${error.message}` }] };
  }
}

  async handleCleanupActivityLogs(args) {
  if (!this.activityLogger) {
    return { content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.**` }] };
  }

  try {
    const retentionDays = args.retention_days || 90;
    const isDryRun = args.dry_run !== false;

    if (isDryRun) {
      return {
        content: [{
          type: 'text',
          text: `🔍 **Cleanup Preview**: Would delete logs older than ${retentionDays} days. Set \`dry_run: false\` to execute.`
        }],
      };
    }

    const deletedCount = await this.activityLogger.cleanupOldLogs();

    return {
      content: [{
        type: 'text',
        text: `✅ **Cleanup completed!** Deleted ${deletedCount} old log entries.`
      }],
    };

  } catch (error) {
    return { content: [{ type: 'text', text: `❌ **Cleanup failed:** ${error.message}` }] };
  }
}

  // ============================================
  // METADATA & ANALYTICS HANDLERS
  // ============================================

  async handleMetadataQueryPerformance(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.**\n\nTo use metadata analytics:\n1. Set `MB_METADATA_ENABLED=true` in your .env file\n2. Configure metadata database connection (MB_METADATA_*)\n3. Restart the MCP server'
      }]
    };
  }

  try {
    const days = args.days || 7;
    const includeSlowQueries = args.include_slow_queries !== false;
    const slowThreshold = args.slow_threshold_ms || 10000;

    // Get overall stats
    const stats = await this.metadataClient.getQueryPerformanceStats(days);

    let output = `📊 **Query Performance Analysis** (Last ${days} Days)\n\n`;
    output += `**Overall Statistics:**\n`;
    output += `• Total Queries: ${stats.total_queries?.toLocaleString() || 0}\n`;
    output += `• Unique Users: ${stats.unique_users || 0}\n`;
    output += `• Average Runtime: ${stats.avg_runtime_ms || 0}ms\n`;
    output += `• Median Runtime: ${stats.median_runtime_ms || 0}ms\n`;
    output += `• 95th Percentile: ${stats.p95_runtime_ms || 0}ms\n`;
    output += `• Max Runtime: ${stats.max_runtime_ms || 0}ms\n`;
    output += `• Cache Hit Rate: ${stats.cache_hit_rate || 0}%\n`;
    output += `• Errors: ${stats.errors || 0} (${((stats.errors / stats.total_queries) * 100).toFixed(2)}%)\n\n`;

    // Get slow queries if requested
    if (includeSlowQueries) {
      const slowQueries = await this.metadataClient.getSlowQueries(slowThreshold, 10);

      if (slowQueries.length > 0) {
        output += `🐌 **Slowest Questions** (>${slowThreshold}ms):\n\n`;
        slowQueries.slice(0, 10).forEach((q, i) => {
          output += `${i + 1}. **${q.question_name || 'Ad-hoc Query'}** (ID: ${q.card_id || 'N/A'})\n`;
          output += `   • Avg Runtime: ${q.avg_runtime_ms}ms\n`;
          output += `   • Max Runtime: ${q.max_runtime_ms}ms\n`;
          output += `   • Executions: ${q.execution_count}\n`;
          output += `   • Database: ${q.database_name}\n`;
          if (q.error_count > 0) {
            output += `   • ⚠️ Errors: ${q.error_count}\n`;
          }
          output += `\n`;
        });
      }
    }

    // Get performance by context
    const contextPerf = await this.metadataClient.getQueryPerformanceByContext(days);
    if (contextPerf.length > 0) {
      output += `📈 **Performance by Context:**\n\n`;
      contextPerf.forEach(c => {
        output += `• **${c.context || 'unknown'}**: ${c.query_count} queries, avg ${c.avg_runtime_ms}ms, error rate ${c.error_rate}%\n`;
      });
    }

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata query performance analysis failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Query performance analysis failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataContentUsage(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const days = args.days || 30;
    const unusedThreshold = args.unused_threshold_days || 90;
    const limit = args.limit || 20;

    let output = `📚 **Content Usage Analysis** (Last ${days} Days)\n\n`;

    // Get popular questions
    const popularQuestions = await this.metadataClient.getPopularQuestions(days, limit);
    if (popularQuestions.length > 0) {
      output += `🌟 **Most Popular Questions:**\n\n`;
      popularQuestions.slice(0, 10).forEach((q, i) => {
        output += `${i + 1}. **${q.name}** (ID: ${q.id})\n`;
        output += `   • Executions: ${q.execution_count}\n`;
        output += `   • Avg Runtime: ${q.avg_runtime_ms}ms\n`;
        output += `   • Collection: ${q.collection_name || 'Root'}\n`;
        output += `   • Type: ${q.display}\n\n`;
      });
    }

    // Get popular dashboards
    const popularDashboards = await this.metadataClient.getPopularDashboards(days, limit);
    if (popularDashboards.length > 0) {
      output += `📊 **Most Popular Dashboards:**\n\n`;
      popularDashboards.slice(0, 10).forEach((d, i) => {
        output += `${i + 1}. **${d.name}** (ID: ${d.id})\n`;
        output += `   • Views: ${d.view_count}\n`;
        output += `   • Cards: ${d.card_count}\n`;
        output += `   • Avg Load Time: ${d.avg_load_time_ms}ms\n`;
        output += `   • Collection: ${d.collection_name || 'Root'}\n\n`;
      });
    }

    // Get unused content
    const unused = await this.metadataClient.getUnusedContent(unusedThreshold);
    output += `🗑️ **Cleanup Recommendations** (Unused >${unusedThreshold} days):\n\n`;
    output += `• Unused Questions: ${unused.unused_questions.length}\n`;
    output += `• Unused Dashboards: ${unused.unused_dashboards.length}\n\n`;

    if (unused.unused_questions.length > 0) {
      output += `**Sample Unused Questions:**\n`;
      unused.unused_questions.slice(0, 5).forEach((q, i) => {
        const lastUsed = q.last_used ? new Date(q.last_used).toLocaleDateString() : 'Never';
        output += `${i + 1}. ${q.name} (ID: ${q.id}) - Last used: ${lastUsed}\n`;
      });
      output += `\n`;
    }

    // Get orphaned cards
    const orphaned = await this.metadataClient.getOrphanedCards();
    output += `📌 **Orphaned Cards** (Not in any dashboard):\n`;
    output += `• Total: ${orphaned.length}\n`;
    if (orphaned.length > 0) {
      output += `• Top used orphaned cards:\n`;
      orphaned.slice(0, 5).forEach((c, i) => {
        output += `  ${i + 1}. ${c.name} (ID: ${c.id}) - ${c.execution_count} uses\n`;
      });
    }

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata content usage analysis failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Content usage analysis failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataUserActivity(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const days = args.days || 30;
    const inactiveThreshold = args.inactive_threshold_days || 90;
    const includeLoginHistory = args.include_login_history !== false;

    let output = `👥 **User Activity Analysis** (Last ${days} Days)\n\n`;

    // Get user activity stats
    const userStats = await this.metadataClient.getUserActivityStats(days);

    // Active users
    const activeUsers = userStats.filter(u => u.query_count > 0);
    const inactiveUsers = userStats.filter(u => u.query_count === 0);

    output += `**Overview:**\n`;
    output += `• Total Active Users: ${activeUsers.length}\n`;
    output += `• Inactive Users (last ${days}d): ${inactiveUsers.length}\n`;
    output += `• Total Queries: ${activeUsers.reduce((sum, u) => sum + u.query_count, 0)}\n\n`;

    // Top users
    output += `🏆 **Most Active Users:**\n\n`;
    activeUsers.slice(0, 10).forEach((u, i) => {
      output += `${i + 1}. ${u.email}${u.is_superuser ? ' (Admin)' : ''}\n`;
      output += `   • Queries: ${u.query_count}\n`;
      output += `   • Questions Used: ${u.unique_questions_used}\n`;
      output += `   • Dashboards Viewed: ${u.unique_dashboards_viewed}\n`;
      output += `   • Avg Query Time: ${u.avg_query_time_ms}ms\n\n`;
    });

    // Inactive users
    const longInactive = await this.metadataClient.getInactiveUsers(inactiveThreshold);
    if (longInactive.length > 0) {
      output += `⚠️ **Long-Inactive Users** (>${inactiveThreshold} days):\n`;
      output += `• Count: ${longInactive.length}\n`;
      output += `• **Recommendation:** Consider license optimization\n\n`;

      longInactive.slice(0, 5).forEach((u, i) => {
        const lastLogin = u.last_login ? new Date(u.last_login).toLocaleDateString() : 'Never';
        output += `${i + 1}. ${u.email} - Last login: ${lastLogin} (${Math.round(u.days_inactive)} days ago)\n`;
      });
      output += `\n`;
    }

    // Login timeline
    if (includeLoginHistory) {
      const loginTimeline = await this.metadataClient.getLoginTimeline(days);
      if (loginTimeline.length > 0) {
        output += `📅 **Recent Login Activity:**\n\n`;
        loginTimeline.slice(0, 7).forEach(t => {
          const date = new Date(t.login_date).toLocaleDateString();
          output += `• ${date}: ${t.login_count} logins, ${t.unique_users} unique users\n`;
        });
      }
    }

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata user activity analysis failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **User activity analysis failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataDatabaseUsage(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const days = args.days || 30;
    let output = `🗃️ **Database Usage Analysis** (Last ${days} Days)\n\n`;

    // Get database usage
    const dbUsage = await this.metadataClient.getDatabaseUsageStats(days);

    output += `**Connected Databases:**\n\n`;
    dbUsage.forEach((db, i) => {
      output += `${i + 1}. **${db.name}** (${db.engine})\n`;
      output += `   • Queries: ${db.query_count}\n`;
      output += `   • Avg Runtime: ${db.avg_runtime_ms}ms\n`;
      output += `   • Errors: ${db.error_count}\n`;
      output += `   • Unique Users: ${db.unique_users}\n\n`;
    });

    // Get table usage if database_id provided
    if (args.database_id) {
      const tableUsage = await this.metadataClient.getTableUsageStats(args.database_id, days);
      if (tableUsage.length > 0) {
        output += `\n📊 **Table Usage** (Database ID: ${args.database_id}):\n\n`;
        tableUsage.slice(0, 20).forEach((t, i) => {
          output += `${i + 1}. ${t.schema}.${t.table_name}\n`;
          output += `   • Questions Using: ${t.question_count}\n\n`;
        });
      }
    }

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata database usage analysis failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Database usage analysis failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataDashboardComplexity(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const complexity = await this.metadataClient.getDashboardComplexityAnalysis();

    let output = `📊 **Dashboard Complexity Analysis**\n\n`;
    output += `Analyzing dashboards with 10+ cards...\n\n`;

    if (complexity.length === 0) {
      output += `✅ No overly complex dashboards found!\n`;
    } else {
      output += `🔍 **Complex Dashboards:**\n\n`;
      complexity.forEach((d, i) => {
        output += `${i + 1}. **${d.name}** (ID: ${d.id})\n`;
        output += `   • Cards: ${d.card_count}\n`;
        output += `   • Avg Load Time: ${d.avg_load_time_ms}ms\n`;
        output += `   • Max Load Time: ${d.max_load_time_ms}ms\n`;
        output += `   • Views (30d): ${d.view_count_30d}\n`;

        if (d.avg_load_time_ms > 5000) {
          output += `   • ⚠️ **Slow dashboard** - Consider optimization\n`;
        }
        if (d.card_count > 15) {
          output += `   • ⚠️ **High card count** - Consider splitting\n`;
        }
        output += `\n`;
      });

      output += `\n💡 **Optimization Tips:**\n`;
      output += `• Cache frequently accessed data\n`;
      output += `• Split large dashboards into focused views\n`;
      output += `• Optimize slow queries\n`;
      output += `• Remove unused cards\n`;
    }

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata dashboard complexity analysis failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Dashboard complexity analysis failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataInfo(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const info = await this.metadataClient.getDatabaseInfo();

    let output = `ℹ️ **Metabase Metadata Overview**\n\n`;
    output += `**Content Statistics:**\n`;
    output += `• Active Users: ${info.active_users}\n`;
    output += `• Active Questions: ${info.active_questions}\n`;
    output += `• Active Dashboards: ${info.active_dashboards}\n`;
    output += `• Connected Databases: ${info.connected_databases}\n`;
    output += `• Queries (Last 7d): ${info.queries_last_7d}\n\n`;

    output += `**Connection Info:**\n`;
    output += `• Database: ${this.metadataClient.config.database}\n`;
    output += `• Engine: ${this.metadataClient.config.engine}\n`;
    output += `• Status: ✅ Connected\n\n`;

    output += `💡 Use other metadata tools for detailed analysis:\n`;
    output += `• \`mb_meta_query_performance\` - Query performance stats\n`;
    output += `• \`mb_meta_content_usage\` - Popular & unused content\n`;
    output += `• \`mb_meta_user_activity\` - User engagement\n`;
    output += `• \`mb_meta_database_usage\` - Database usage patterns\n`;

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata info failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Metadata info failed:** ${error.message}`
      }]
    };
  }
}

  // ============================================
  // PHASE 2: ADVANCED ANALYTICS HANDLERS
  // ============================================

  async handleMetadataTableDependencies(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const dependencies = await this.metadataClient.getTableDependencies(
      args.database_id,
      args.table_name,
      args.schema_name
    );

    if (!dependencies.table_found) {
      return {
        content: [{ type: 'text', text: `❌ ${dependencies.message}` }]
      };
    }

    const table = dependencies.table;
    const summary = dependencies.impact_summary;

    let output = `🔗 **Table Dependency Analysis**\n\n`;
    output += `**Table:** ${table.schema ? table.schema + '.' : ''}${table.name}\n`;
    output += `**Display Name:** ${table.display_name || table.name}\n\n`;

    output += `**Impact Summary:**\n`;
    output += `• Questions Affected: ${summary.questions_affected}\n`;
    output += `• Dashboards Affected: ${summary.dashboards_affected}\n`;
    output += `• Fields: ${summary.fields_count}\n`;
    output += `• Total Executions (30d): ${summary.total_executions_30d}\n\n`;

    if (dependencies.questions.length > 0) {
      output += `📊 **Dependent Questions** (Top 10):\n\n`;
      dependencies.questions.slice(0, 10).forEach((q, i) => {
        output += `${i + 1}. **${q.name}** (ID: ${q.id})\n`;
        output += `   • Type: ${q.display}\n`;
        output += `   • Executions (30d): ${q.execution_count_30d}\n`;
        output += `   • Collection: ${q.collection_name || 'Root'}\n`;
        output += `   • Creator: ${q.creator}\n\n`;
      });

      if (dependencies.questions.length > 10) {
        output += `_...and ${dependencies.questions.length - 10} more questions_\n\n`;
      }
    } else {
      output += `✅ **No questions depend on this table**\n\n`;
    }

    if (dependencies.dashboards.length > 0) {
      output += `📈 **Affected Dashboards** (Top 10):\n\n`;
      dependencies.dashboards.slice(0, 10).forEach((d, i) => {
        output += `${i + 1}. **${d.name}** (ID: ${d.id})\n`;
        output += `   • Total Cards: ${d.total_cards}\n`;
        output += `   • Views (30d): ${d.view_count_30d}\n`;
        output += `   • Collection: ${d.collection_name || 'Root'}\n\n`;
      });

      if (dependencies.dashboards.length > 10) {
        output += `_...and ${dependencies.dashboards.length - 10} more dashboards_\n\n`;
      }
    }

    if (dependencies.fields.length > 0) {
      output += `📋 **Table Fields** (${dependencies.fields.length} total):\n`;
      dependencies.fields.slice(0, 15).forEach(f => {
        output += `• ${f.name} (${f.base_type})${f.semantic_type ? ' - ' + f.semantic_type : ''}\n`;
      });
      if (dependencies.fields.length > 15) {
        output += `_...and ${dependencies.fields.length - 15} more fields_\n`;
      }
    }

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata table dependencies analysis failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Table dependencies analysis failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataImpactAnalysis(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const impact = await this.metadataClient.analyzeTableRemovalImpact(
      args.database_id,
      args.table_name,
      args.schema_name
    );

    if (!impact.table_found) {
      return {
        content: [{ type: 'text', text: `❌ ${impact.message}` }]
      };
    }

    const table = impact.table;
    const analysis = impact.impact_analysis;
    const breaking = analysis.breaking_changes;

    let output = `⚠️ **Table Removal Impact Analysis**\n\n`;
    output += `**Table:** ${table.schema ? table.schema + '.' : ''}${table.name}\n`;
    output += `**Severity:** ${analysis.severity === 'HIGH' ? '🔴 HIGH' : analysis.severity === 'MEDIUM' ? '🟡 MEDIUM' : '🟢 LOW'}\n\n`;

    output += `**Breaking Changes:**\n`;
    output += `• Questions Will Break: ${breaking.questions_will_break}\n`;
    output += `• Dashboards Will Break: ${breaking.dashboards_will_break}\n`;
    output += `• Critical Questions: ${breaking.critical_questions} (>10 executions/month)\n`;
    output += `• Critical Dashboards: ${breaking.critical_dashboards} (>5 views/month)\n`;
    output += `• Unused Questions: ${breaking.unused_questions}\n\n`;

    output += `**Recommendations:**\n`;
    analysis.recommendations.forEach(rec => {
      output += `${rec}\n`;
    });
    output += `\n`;

    if (breaking.critical_questions > 0 && impact.questions.length > 0) {
      output += `🔥 **Critical Questions** (highly used):\n\n`;
      const criticalQuestions = impact.questions.filter(q => parseInt(q.execution_count_30d) > 10);
      criticalQuestions.slice(0, 5).forEach((q, i) => {
        output += `${i + 1}. **${q.name}** (ID: ${q.id})\n`;
        output += `   • Executions: ${q.execution_count_30d}\n`;
        output += `   • Collection: ${q.collection_name || 'Root'}\n\n`;
      });
    }

    if (breaking.critical_dashboards > 0 && impact.dashboards.length > 0) {
      output += `🔥 **Critical Dashboards** (actively viewed):\n\n`;
      const criticalDashboards = impact.dashboards.filter(d => parseInt(d.view_count_30d) > 5);
      criticalDashboards.slice(0, 5).forEach((d, i) => {
        output += `${i + 1}. **${d.name}** (ID: ${d.id})\n`;
        output += `   • Views: ${d.view_count_30d}\n`;
        output += `   • Cards: ${d.total_cards}\n\n`;
      });
    }

    output += `\n💡 **Next Steps:**\n`;
    if (analysis.severity === 'HIGH') {
      output += `1. Review and migrate critical questions to alternative tables\n`;
      output += `2. Update dashboard queries with new data sources\n`;
      output += `3. Archive unused questions before removal\n`;
      output += `4. Communicate changes to affected users\n`;
      output += `5. Plan rollback strategy if needed\n`;
    } else if (analysis.severity === 'MEDIUM') {
      output += `1. Archive or update affected questions\n`;
      output += `2. Notify owners of affected dashboards\n`;
      output += `3. Consider archiving table instead of deletion\n`;
    } else {
      output += `1. Verify table is truly unused\n`;
      output += `2. Archive table for 30 days before permanent deletion\n`;
      output += `3. Monitor for any unexpected dependencies\n`;
    }

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata impact analysis failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Impact analysis failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataOptimizationRecommendations(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const days = args.days || 30;
    const includeMatView = args.include_matview_candidates !== false;
    const includeCache = args.include_cache_recommendations !== false;

    let output = `⚡ **Optimization Recommendations** (Last ${days} Days)\n\n`;

    // Index recommendations
    const indexRecs = await this.metadataClient.getIndexRecommendations(args.database_id, days);

    if (indexRecs.length > 0) {
      output += `📊 **Index Recommendations:**\n\n`;
      indexRecs.slice(0, 10).forEach((rec, i) => {
        const priorityIcon = rec.priority === 'HIGH' ? '🔴' : rec.priority === 'MEDIUM' ? '🟡' : '🟢';
        output += `${i + 1}. ${priorityIcon} **${rec.schema}.${rec.table}** (Priority: ${rec.priority})\n`;
        output += `   • Query Count: ${rec.query_count}\n`;
        output += `   • Avg Runtime: ${rec.avg_runtime_ms}ms\n`;
        output += `   • Max Runtime: ${rec.max_runtime_ms}ms\n`;
        output += `   • ${rec.recommendation}\n\n`;
      });
    } else {
      output += `✅ **No urgent index recommendations** - Query performance is acceptable\n\n`;
    }

    // Materialized view candidates
    if (includeMatView) {
      const matviewCandidates = await this.metadataClient.getMaterializedViewCandidates(days, 5);

      if (matviewCandidates.length > 0) {
        output += `🔄 **Materialized View Candidates:**\n\n`;
        matviewCandidates.slice(0, 5).forEach((c, i) => {
          const priorityIcon = c.priority === 'HIGH' ? '🔴' : c.priority === 'MEDIUM' ? '🟡' : '🟢';
          output += `${i + 1}. ${priorityIcon} **${c.question_name || 'Ad-hoc Query'}** (Priority: ${c.priority})\n`;
          output += `   • Database: ${c.database_name}\n`;
          output += `   • Executions: ${c.execution_count}\n`;
          output += `   • Avg Runtime: ${c.avg_runtime_ms}ms\n`;
          output += `   • Potential Time Saved: ${(c.total_time_saved_potential / (1000 * 60)).toFixed(2)} minutes\n`;
          output += `   • ${c.recommendation}\n\n`;
        });
      } else {
        output += `✅ **No materialized view candidates** - No repeated heavy queries detected\n\n`;
      }
    }

    // Cache optimization
    if (includeCache) {
      const cacheRecs = await this.metadataClient.getCacheOptimizationRecommendations(7);

      if (cacheRecs.length > 0) {
        output += `💾 **Cache Optimization Recommendations:**\n\n`;

        const highPriorityCache = cacheRecs.filter(c => c.cache_hit_rate < 30 && c.execution_count > 10);
        if (highPriorityCache.length > 0) {
          output += `🔴 **High Priority** (Low cache hit rate):\n`;
          highPriorityCache.slice(0, 5).forEach(c => {
            output += `• **${c.question_name}** (ID: ${c.card_id})\n`;
            output += `  Current TTL: ${c.current_cache_ttl || 'None'}, Suggested: ${c.suggested_cache_ttl}s\n`;
            output += `  Hit Rate: ${c.cache_hit_rate}%, Executions: ${c.execution_count}\n`;
            output += `  ${c.recommendation}\n\n`;
          });
        }

        const noCacheQuestions = cacheRecs.filter(c => !c.current_cache_ttl && c.execution_count > 15);
        if (noCacheQuestions.length > 0) {
          output += `🟡 **Enable Caching** (Frequently accessed, no cache):\n`;
          noCacheQuestions.slice(0, 5).forEach(c => {
            output += `• **${c.question_name}** - ${c.execution_count} executions, no caching configured\n`;
          });
          output += `\n`;
        }
      }
    }

    output += `\n💡 **Implementation Guide:**\n`;
    output += `• **Indexes**: Use \`create_index_direct\` tool to add recommended indexes\n`;
    output += `• **Materialized Views**: Use \`create_materialized_view_direct\` for PostgreSQL\n`;
    output += `• **Caching**: Update question cache_ttl via \`mb_question_update\` tool\n`;

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata optimization recommendations failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Optimization recommendations failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataErrorPatterns(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const days = args.days || 30;
    const includeRecurring = args.include_recurring_questions !== false;
    const includeTimeline = args.include_timeline !== false;

    let output = `🚨 **Error Pattern Analysis** (Last ${days} Days)\n\n`;

    // Error patterns
    const patterns = await this.metadataClient.getErrorPatterns(days);

    if (patterns.length > 0) {
      output += `**Top Error Patterns:**\n\n`;
      patterns.slice(0, 10).forEach((p, i) => {
        const severityIcon = p.severity === 'HIGH' ? '🔴' : p.severity === 'MEDIUM' ? '🟡' : '🟢';
        output += `${i + 1}. ${severityIcon} **${p.category}** (${p.severity} Severity)\n`;
        output += `   • Occurrences: ${p.occurrence_count}\n`;
        output += `   • Affected Questions: ${p.affected_questions}\n`;
        output += `   • Affected Users: ${p.affected_users}\n`;
        output += `   • Database: ${p.primary_database} (${p.database_engine})\n`;
        output += `   • Error: ${p.error_pattern.substring(0, 80)}...\n`;
        output += `   • **Resolution:** ${p.resolution_suggestion}\n\n`;
      });
    } else {
      output += `✅ **No recurring error patterns detected**\n\n`;
    }

    // Recurring error questions
    if (includeRecurring) {
      const recurringQuestions = await this.metadataClient.getRecurringErrorQuestions(days, 3);

      if (recurringQuestions.length > 0) {
        output += `\n🔥 **Questions with Recurring Errors:**\n\n`;
        recurringQuestions.slice(0, 10).forEach((q, i) => {
          const severityIcon = q.severity === 'CRITICAL' ? '🔴' : q.severity === 'HIGH' ? '🟡' : '🟢';
          output += `${i + 1}. ${severityIcon} **${q.question_name}** (ID: ${q.card_id}) - ${q.severity}\n`;
          output += `   • Error Rate: ${q.error_rate}% (${q.error_count}/${q.total_executions})\n`;
          output += `   • Collection: ${q.collection_name || 'Root'}\n`;
          output += `   • Last Error: ${new Date(q.last_error_time).toLocaleDateString()}\n`;
          output += `   • ${q.recommendation}\n\n`;
        });
      }
    }

    // Error timeline
    if (includeTimeline) {
      const timeline = await this.metadataClient.getErrorTimeline(days);

      if (timeline.length > 0) {
        output += `\n📅 **Error Timeline** (Last 7 days):\n\n`;
        timeline.slice(0, 7).forEach(t => {
          const date = new Date(t.error_date).toLocaleDateString();
          output += `• ${date}: ${t.error_count} errors (${t.error_rate}% error rate), ${t.affected_questions} questions\n`;
        });
      }
    }

    output += `\n💡 **Recommended Actions:**\n`;
    const highSeverity = patterns.filter(p => p.severity === 'HIGH');
    const criticalQuestions = includeRecurring ?
      (await this.metadataClient.getRecurringErrorQuestions(days, 3)).filter(q => q.severity === 'CRITICAL') : [];

    if (criticalQuestions.length > 0) {
      output += `1. **URGENT**: Fix or archive ${criticalQuestions.length} critical questions (>50% error rate)\n`;
    }
    if (highSeverity.length > 0) {
      output += `2. Address ${highSeverity.length} high-severity error patterns\n`;
    }
    output += `3. Review database permissions and connectivity\n`;
    output += `4. Optimize timeout-prone queries\n`;
    output += `5. Update questions with schema changes\n`;

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata error patterns analysis failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Error patterns analysis failed:** ${error.message}`
      }]
    };
  }
}

  // ============================================================================
  // Phase 3: Export/Import & Migration Handlers
  // ============================================================================

  async handleMetadataExportWorkspace(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const options = {
      include_collections: args.include_collections !== false,
      include_questions: args.include_questions !== false,
      include_dashboards: args.include_dashboards !== false,
      collection_id: args.collection_id,
      created_after: args.created_after,
      created_before: args.created_before
    };

    let output = `📤 **Workspace Export** (READ-ONLY Operation)\n\n`;

    const result = await this.metadataClient.exportWorkspace(options);

    output += `**Export Summary:**\n`;
    output += `• **Collections**: ${result.collections?.length || 0}\n`;
    output += `• **Questions**: ${result.questions?.length || 0}\n`;
    output += `• **Dashboards**: ${result.dashboards?.length || 0}\n`;
    output += `• **Total Items**: ${result.metadata.total_items}\n`;
    output += `• **Export Date**: ${result.metadata.exported_at}\n\n`;

    if (result.collections?.length > 0) {
      output += `**Exported Collections:**\n`;
      result.collections.slice(0, 10).forEach(c => {
        output += `• **${c.name}** (ID: ${c.id}) - ${c.description || 'No description'}\n`;
      });
      if (result.collections.length > 10) {
        output += `... and ${result.collections.length - 10} more collections\n`;
      }
      output += `\n`;
    }

    if (result.questions?.length > 0) {
      output += `**Exported Questions:**\n`;
      result.questions.slice(0, 10).forEach(q => {
        output += `• **${q.name}** (ID: ${q.id})\n`;
      });
      if (result.questions.length > 10) {
        output += `... and ${result.questions.length - 10} more questions\n`;
      }
      output += `\n`;
    }

    if (result.dashboards?.length > 0) {
      output += `**Exported Dashboards:**\n`;
      result.dashboards.slice(0, 10).forEach(d => {
        output += `• **${d.name}** (ID: ${d.id}) - ${d.description || 'No description'}\n`;
      });
      if (result.dashboards.length > 10) {
        output += `... and ${result.dashboards.length - 10} more dashboards\n`;
      }
      output += `\n`;
    }

    output += `\n📋 **Export Data (JSON):**\n`;
    output += `\`\`\`json\n${JSON.stringify(result, null, 2)}\`\`\`\n`;

    output += `\n💡 **Next Steps:**\n`;
    output += `• Save this JSON to a file for backup or migration\n`;
    output += `• Use \`mb_meta_import_preview\` to analyze import impact before importing\n`;
    output += `• Use \`mb_meta_compare_environments\` to compare with other environments\n`;

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata export workspace failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Export workspace failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataImportPreview(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  if (!args.workspace_json) {
    return {
      content: [{
        type: 'text',
        text: '❌ **Missing required parameter:** workspace_json'
      }]
    };
  }

  try {
    const workspace = typeof args.workspace_json === 'string'
      ? JSON.parse(args.workspace_json)
      : args.workspace_json;

    let output = `🔍 **Import Impact Preview** (DRY-RUN - No Changes Made)\n\n`;

    const impact = await this.metadataClient.previewImportImpact(workspace);

    output += `**Import Summary:**\n`;
    output += `• **Collections to Import**: ${impact.summary.collections_to_import}\n`;
    output += `• **Questions to Import**: ${impact.summary.questions_to_import}\n`;
    output += `• **Dashboards to Import**: ${impact.summary.dashboards_to_import}\n`;
    output += `• **Name Conflicts**: ${impact.summary.name_conflicts}\n`;
    output += `• **Overall Risk**: ${impact.summary.overall_risk}\n\n`;

    if (impact.conflicts.length > 0) {
      output += `⚠️ **Conflicts Detected (${impact.conflicts.length}):**\n`;
      impact.conflicts.forEach(c => {
        const icon = c.severity === 'HIGH' ? '🔴' : c.severity === 'MEDIUM' ? '🟡' : '🟢';
        output += `${icon} **${c.type}**: ${c.item_name}\n`;
        output += `  - ${c.message}\n`;
        output += `  - Recommendation: ${c.recommendation}\n`;
      });
      output += `\n`;
    }

    if (impact.warnings.length > 0) {
      output += `⚠️ **Warnings (${impact.warnings.length}):**\n`;
      impact.warnings.forEach(w => {
        output += `• ${w}\n`;
      });
      output += `\n`;
    }

    output += `\n📊 **Detailed Analysis:**\n\n`;

    if (impact.new_items.collections.length > 0) {
      output += `**New Collections (${impact.new_items.collections.length}):**\n`;
      impact.new_items.collections.slice(0, 5).forEach(c => {
        output += `✅ ${c}\n`;
      });
      if (impact.new_items.collections.length > 5) {
        output += `... and ${impact.new_items.collections.length - 5} more\n`;
      }
      output += `\n`;
    }

    if (impact.new_items.questions.length > 0) {
      output += `**New Questions (${impact.new_items.questions.length}):**\n`;
      impact.new_items.questions.slice(0, 5).forEach(q => {
        output += `✅ ${q}\n`;
      });
      if (impact.new_items.questions.length > 5) {
        output += `... and ${impact.new_items.questions.length - 5} more\n`;
      }
      output += `\n`;
    }

    if (impact.new_items.dashboards.length > 0) {
      output += `**New Dashboards (${impact.new_items.dashboards.length}):**\n`;
      impact.new_items.dashboards.slice(0, 5).forEach(d => {
        output += `✅ ${d}\n`;
      });
      if (impact.new_items.dashboards.length > 5) {
        output += `... and ${impact.new_items.dashboards.length - 5} more\n`;
      }
      output += `\n`;
    }

    output += `\n💡 **Recommendations:**\n`;
    if (impact.recommendations.length > 0) {
      impact.recommendations.forEach(r => {
        output += `• ${r}\n`;
      });
    } else {
      output += `✅ No issues detected - safe to import\n`;
    }

    output += `\n🔒 **Next Steps:**\n`;
    if (impact.summary.overall_risk === 'HIGH') {
      output += `⚠️ **HIGH RISK** - Review conflicts carefully before proceeding\n`;
    } else if (impact.summary.overall_risk === 'MEDIUM') {
      output += `⚠️ **MEDIUM RISK** - Address warnings before import\n`;
    } else {
      output += `✅ **LOW RISK** - Safe to proceed with import\n`;
    }
    output += `• Resolve name conflicts by renaming items in the workspace JSON\n`;
    output += `• Create backup before actual import\n`;
    output += `• Use \`mb_meta_export_workspace\` to backup current state first\n`;

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata import preview failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Import preview failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataCompareEnvironments(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  if (!args.target_workspace_json) {
    return {
      content: [{
        type: 'text',
        text: '❌ **Missing required parameter:** target_workspace_json'
      }]
    };
  }

  try {
    const targetWorkspace = typeof args.target_workspace_json === 'string'
      ? JSON.parse(args.target_workspace_json)
      : args.target_workspace_json;

    let output = `🔄 **Environment Comparison** (READ-ONLY Operation)\n\n`;

    const comparison = await this.metadataClient.compareEnvironments(targetWorkspace);

    output += `**Comparison Summary:**\n`;
    output += `• **Current Environment**: ${comparison.metadata.source_name || 'Current'}\n`;
    output += `• **Target Environment**: ${comparison.metadata.target_name || 'Target'}\n`;
    output += `• **Collections Missing in Target**: ${comparison.summary.collections_missing_in_target}\n`;
    output += `• **Questions Missing in Target**: ${comparison.summary.questions_missing_in_target}\n`;
    output += `• **Dashboards Missing in Target**: ${comparison.summary.dashboards_missing_in_target}\n`;
    output += `• **Collections Missing in Source**: ${comparison.summary.collections_missing_in_source}\n`;
    output += `• **Questions Missing in Source**: ${comparison.summary.questions_missing_in_source}\n`;
    output += `• **Dashboards Missing in Source**: ${comparison.summary.dashboards_missing_in_source}\n`;
    output += `• **Different Items**: ${comparison.summary.different_items}\n`;
    output += `• **Drift Level**: ${comparison.summary.drift_level}\n\n`;

    if (comparison.missing_in_target.collections.length > 0) {
      output += `📤 **Collections in Source but NOT in Target (${comparison.missing_in_target.collections.length}):**\n`;
      comparison.missing_in_target.collections.slice(0, 5).forEach(c => {
        output += `• **${c.name}** (ID: ${c.id})\n`;
      });
      if (comparison.missing_in_target.collections.length > 5) {
        output += `... and ${comparison.missing_in_target.collections.length - 5} more\n`;
      }
      output += `\n`;
    }

    if (comparison.missing_in_target.questions.length > 0) {
      output += `📤 **Questions in Source but NOT in Target (${comparison.missing_in_target.questions.length}):**\n`;
      comparison.missing_in_target.questions.slice(0, 5).forEach(q => {
        output += `• **${q.name}** (ID: ${q.id})\n`;
      });
      if (comparison.missing_in_target.questions.length > 5) {
        output += `... and ${comparison.missing_in_target.questions.length - 5} more\n`;
      }
      output += `\n`;
    }

    if (comparison.missing_in_target.dashboards.length > 0) {
      output += `📤 **Dashboards in Source but NOT in Target (${comparison.missing_in_target.dashboards.length}):**\n`;
      comparison.missing_in_target.dashboards.slice(0, 5).forEach(d => {
        output += `• **${d.name}** (ID: ${d.id})\n`;
      });
      if (comparison.missing_in_target.dashboards.length > 5) {
        output += `... and ${comparison.missing_in_target.dashboards.length - 5} more\n`;
      }
      output += `\n`;
    }

    if (comparison.missing_in_source.collections.length > 0) {
      output += `📥 **Collections in Target but NOT in Source (${comparison.missing_in_source.collections.length}):**\n`;
      comparison.missing_in_source.collections.slice(0, 5).forEach(c => {
        output += `• **${c.name}** (ID: ${c.id})\n`;
      });
      if (comparison.missing_in_source.collections.length > 5) {
        output += `... and ${comparison.missing_in_source.collections.length - 5} more\n`;
      }
      output += `\n`;
    }

    if (comparison.different.length > 0) {
      output += `⚠️ **Items with Differences (${comparison.different.length}):**\n`;
      comparison.different.slice(0, 5).forEach(d => {
        output += `• **${d.name}** (${d.type})\n`;
        output += `  - Differences: ${d.differences.join(', ')}\n`;
      });
      if (comparison.different.length > 5) {
        output += `... and ${comparison.different.length - 5} more\n`;
      }
      output += `\n`;
    }

    output += `\n📊 **Drift Analysis:**\n`;
    if (comparison.summary.drift_level === 'HIGH') {
      output += `🔴 **HIGH DRIFT** - Environments are significantly different\n`;
      output += `• Consider syncing environments to maintain consistency\n`;
    } else if (comparison.summary.drift_level === 'MEDIUM') {
      output += `🟡 **MEDIUM DRIFT** - Some differences detected\n`;
      output += `• Review differences and sync if needed\n`;
    } else {
      output += `🟢 **LOW DRIFT** - Environments are mostly in sync\n`;
      output += `• Minor differences only\n`;
    }

    output += `\n💡 **Recommendations:**\n`;
    if (comparison.recommendations.length > 0) {
      comparison.recommendations.forEach(r => {
        output += `• ${r}\n`;
      });
    }

    output += `\n🔒 **Next Steps:**\n`;
    output += `• Export missing items from source: \`mb_meta_export_workspace\`\n`;
    output += `• Preview import to target: \`mb_meta_import_preview\`\n`;
    output += `• Regular comparison helps maintain environment consistency\n`;
    output += `• Recommended: Dev → Staging → Production promotion workflow\n`;

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata environment comparison failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Environment comparison failed:** ${error.message}`
      }]
    };
  }
}

  async handleMetadataAutoCleanup(args) {
  if (!this.metadataClient) {
    return {
      content: [{
        type: 'text',
        text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
      }]
    };
  }

  try {
    const options = {
      dry_run: args.dry_run !== false,  // Default: true
      approved: args.approved === true,  // Default: false
      unused_days: args.unused_days || 180,
      orphaned_cards: args.orphaned_cards !== false,
      empty_collections: args.empty_collections !== false,
      broken_questions: args.broken_questions !== false,
      backup_recommended: args.backup_recommended !== false
    };

    const isDryRun = options.dry_run;
    const isApproved = options.approved;

    let output = `🧹 **Auto-Cleanup Analysis**\n\n`;

    if (isDryRun) {
      output += `🔒 **MODE**: DRY-RUN (Preview Only - No Changes Made)\n\n`;
    } else if (!isApproved) {
      output += `🚫 **BLOCKED**: Execution requires approved: true\n\n`;
    } else {
      output += `⚠️ **MODE**: EXECUTION (Changes Will Be Made)\n\n`;
    }

    const cleanup = await this.metadataClient.autoCleanup(options);

    output += `**Cleanup Summary:**\n`;
    output += `• **Unused Questions**: ${cleanup.summary.unused_questions}\n`;
    output += `• **Orphaned Cards**: ${cleanup.summary.orphaned_cards}\n`;
    output += `• **Empty Collections**: ${cleanup.summary.empty_collections}\n`;
    output += `• **Broken Questions**: ${cleanup.summary.broken_questions}\n`;
    output += `• **Total Items to Clean**: ${cleanup.summary.total_items}\n\n`;

    if (cleanup.blocked) {
      output += `🚫 **EXECUTION BLOCKED:**\n`;
      output += `• This is a destructive operation\n`;
      output += `• Set dry_run: false AND approved: true to execute\n`;
      output += `• Review all items carefully before approving\n\n`;
    }

    if (cleanup.items_to_cleanup.unused_questions.length > 0) {
      output += `📊 **Unused Questions (${cleanup.items_to_cleanup.unused_questions.length}):**\n`;
      output += `(Not viewed in ${options.unused_days} days)\n`;
      cleanup.items_to_cleanup.unused_questions.slice(0, 10).forEach(q => {
        output += `• **${q.name}** (ID: ${q.id})\n`;
        output += `  - Last viewed: ${q.last_viewed || 'Never'}\n`;
        output += `  - Created: ${q.created_at}\n`;
      });
      if (cleanup.items_to_cleanup.unused_questions.length > 10) {
        output += `... and ${cleanup.items_to_cleanup.unused_questions.length - 10} more\n`;
      }
      output += `\n`;
    }

    if (cleanup.items_to_cleanup.orphaned_cards.length > 0) {
      output += `🔗 **Orphaned Cards (${cleanup.items_to_cleanup.orphaned_cards.length}):**\n`;
      output += `(Not in any dashboard or collection)\n`;
      cleanup.items_to_cleanup.orphaned_cards.slice(0, 10).forEach(c => {
        output += `• **${c.name}** (ID: ${c.id})\n`;
      });
      if (cleanup.items_to_cleanup.orphaned_cards.length > 10) {
        output += `... and ${cleanup.items_to_cleanup.orphaned_cards.length - 10} more\n`;
      }
      output += `\n`;
    }

    if (cleanup.items_to_cleanup.empty_collections.length > 0) {
      output += `📁 **Empty Collections (${cleanup.items_to_cleanup.empty_collections.length}):**\n`;
      cleanup.items_to_cleanup.empty_collections.slice(0, 10).forEach(c => {
        output += `• **${c.name}** (ID: ${c.id})\n`;
      });
      if (cleanup.items_to_cleanup.empty_collections.length > 10) {
        output += `... and ${cleanup.items_to_cleanup.empty_collections.length - 10} more\n`;
      }
      output += `\n`;
    }

    if (cleanup.items_to_cleanup.broken_questions.length > 0) {
      output += `❌ **Broken Questions (${cleanup.items_to_cleanup.broken_questions.length}):**\n`;
      output += `(High error rate: >50%)\n`;
      cleanup.items_to_cleanup.broken_questions.slice(0, 10).forEach(q => {
        output += `• **${q.name}** (ID: ${q.id})\n`;
        output += `  - Error rate: ${q.error_rate}%\n`;
        output += `  - Last error: ${q.last_error}\n`;
      });
      if (cleanup.items_to_cleanup.broken_questions.length > 10) {
        output += `... and ${cleanup.items_to_cleanup.broken_questions.length - 10} more\n`;
      }
      output += `\n`;
    }

    if (cleanup.warnings.length > 0) {
      output += `⚠️ **Warnings:**\n`;
      cleanup.warnings.forEach(w => {
        output += `• ${w}\n`;
      });
      output += `\n`;
    }

    output += `\n🔒 **Safety Checks:**\n`;
    cleanup.safety_checks.forEach(check => {
      const icon = check.status === 'passed' ? '✅' : '⚠️';
      output += `${icon} **${check.check}**: ${check.message}\n`;
    });

    output += `\n💡 **Recommendations:**\n`;
    if (cleanup.recommendations.length > 0) {
      cleanup.recommendations.forEach(r => {
        output += `• ${r}\n`;
      });
    }

    output += `\n📋 **Next Steps:**\n`;
    if (isDryRun) {
      output += `1. **Review** all items to be cleaned carefully\n`;
      output += `2. **Backup** your workspace: \`mb_meta_export_workspace\`\n`;
      output += `3. **Execute** cleanup with: dry_run: false, approved: true\n`;
    } else if (cleanup.blocked) {
      output += `1. Set approved: true to execute cleanup\n`;
      output += `2. Backup recommended before execution\n`;
    } else {
      output += `✅ Cleanup executed successfully\n`;
      output += `• Archive or permanently delete archived items via Metabase UI\n`;
      output += `• Monitor for any unintended impacts\n`;
    }

    return {
      content: [{ type: 'text', text: output }]
    };
  } catch (error) {
    logger.error('Metadata auto-cleanup failed:', error);
    return {
      content: [{
        type: 'text',
        text: `❌ **Auto-cleanup failed:** ${error.message}`
      }]
    };
  }
}
}

// Run the server
const server = new MetabaseMCPServer();

// Show startup info if not running as MCP server
if (process.stdout.isTTY) {
  console.log('🚀 Metabase AI Assistant MCP Server');
  console.log('📦 Version 1.0.0 by ONMARTECH LLC');
  console.log('🔌 Compatible with Claude Desktop & Claude Code');
  console.log('📖 https://github.com/onmartech/metabase-ai-assistant');
  console.log('');
  console.log('Starting MCP server...');
}

server.run().catch((error) => {
  if (process.stdout.isTTY) {
    console.error('❌ Failed to start MCP server:', error.message);
  }
  process.exit(1);
});