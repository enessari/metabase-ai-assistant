#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import dotenv from 'dotenv';
import { MetabaseClient } from '../metabase/client.js';
import { MetabaseAIAssistant } from '../ai/assistant.js';
import { ActivityLogger } from '../utils/activity-logger.js';
import { MetabaseMetadataClient } from '../metabase/metadata-client.js';
import { logger } from '../utils/logger.js';

// Handler modules
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
import { isReadOnlyMode } from './tool-router.js';

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
        version: '4.2.0',
        description: 'AI-powered database operations, SQL queries, metrics, and dashboard automation for Metabase. 134 tools with structured output for enterprise BI.',
      },
      {
        capabilities: {
          tools: { listChanged: true },
        },
      }
    );

    this.metabaseClient = null;
    this.aiAssistant = null;
    this.activityLogger = null;
    this.metadataClient = null;
    this.initError = null;
    this.cache = globalCache;

    // Handler instances (initialized in initialize())
    this.metadataHandler = null;
    this.dashboardDirectHandler = null;
    this.sqlHandler = null;
    this.cardsHandler = null;
    this.collectionsHandler = null;
    this.usersHandler = null;
    this.actionsHandler = null;
    this.docsHandler = null;
    this.schemaHandler = null;
    this.analyticsHandler = null;

    this.setupHandlers();
  }

  async initialize() {
    if (this.metabaseClient) return;

    try {
      this.metabaseClient = new MetabaseClient({
        url: process.env.METABASE_URL,
        username: process.env.METABASE_USERNAME,
        password: process.env.METABASE_PASSWORD,
        apiKey: process.env.METABASE_API_KEY,
      });

      // Core handlers (no extra deps)
      this.metadataHandler = new MetadataHandler(this.metabaseClient);
      this.dashboardDirectHandler = new DashboardDirectHandler(this.metabaseClient, this.metadataHandler);

      await this.metabaseClient.authenticate();
      logger.info('Metabase client initialized');

      // AI assistant (optional)
      if (process.env.ANTHROPIC_API_KEY || process.env.OPENAI_API_KEY) {
        this.aiAssistant = new MetabaseAIAssistant({
          metabaseClient: this.metabaseClient,
          anthropicApiKey: process.env.ANTHROPIC_API_KEY,
          openaiApiKey: process.env.OPENAI_API_KEY,
          aiProvider: process.env.ANTHROPIC_API_KEY ? 'anthropic' : 'openai'
        });
        logger.info('AI assistant initialized');
      }

      // Activity logger
      this.activityLogger = new ActivityLogger(this.metabaseClient, {
        logTableName: 'claude_ai_activity_log',
        schema: 'public'
      });
      logger.info('Activity logger initialized');

      // Metadata client (optional - uses Metabase API, no direct DB connection needed)
      if (process.env.MB_METADATA_ENABLED === 'true' && appConfig.METABASE_INTERNAL_DB_ID) {
        try {
          this.metadataClient = new MetabaseMetadataClient({
            metabaseClient: this.metabaseClient,
            internalDbId: appConfig.METABASE_INTERNAL_DB_ID
          });
          logger.info(`Metabase metadata client initialized (DB ID: ${appConfig.METABASE_INTERNAL_DB_ID})`);
        } catch (error) {
          logger.warn('Metadata client initialization failed:', error.message);
          this.metadataClient = null;
        }
      } else if (process.env.MB_METADATA_ENABLED === 'true' && !appConfig.METABASE_INTERNAL_DB_ID) {
        logger.warn('MB_METADATA_ENABLED=true but METABASE_INTERNAL_DB_ID is not set. Use meta_find_internal_db tool to find it.');
      }

      // Modular handlers (initialized after all deps are ready)
      this.sqlHandler = new SqlHandler(this.metabaseClient, this.cache, this.activityLogger, this.aiAssistant);
      this.cardsHandler = new CardsHandler(this.metabaseClient);
      this.collectionsHandler = new CollectionsHandler(this.metabaseClient);
      this.usersHandler = new UsersHandler(this.metabaseClient);
      this.actionsHandler = new ActionsHandler(this.metabaseClient);
      this.docsHandler = new DocsHandler(this.metabaseClient);
      this.schemaHandler = new SchemaHandler(this.metabaseClient, this.activityLogger);
      this.analyticsHandler = new AnalyticsHandler(this.metabaseClient, this.metadataClient, this.activityLogger);
    } catch (error) {
      logger.error('Failed to initialize MCP server:', error);
      this.initError = error;
    }
  }

  async ensureInitialized() {
    if (!this.metabaseClient || !this.activityLogger) {
      await this.initialize();
    }
  }

  setupHandlers() {
    // Tool listing from registry
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return { tools: getToolDefinitions() };
    });

    // Tool dispatch
    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      try {
        await this.ensureInitialized();
        return await this.dispatchTool(name, args);
      } catch (error) {
        if (error instanceof McpError) throw error;

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
        } else if (error.message.includes('not found')) {
          errorMessage = `Resource not found: ${error.message}`;
          errorCode = ErrorCode.InvalidRequest;
        } else if (error.message.includes('is not a function')) {
          errorMessage = `Unexpected API format. Details: ${error.message.substring(0, 100)}`;
        } else if (error.message.includes('Cannot read properties of undefined')) {
          errorMessage = `Expected data not found. Details: ${error.message.substring(0, 100)}`;
        } else if (error.message.includes('timeout') || error.message.includes('ETIMEDOUT')) {
          errorMessage = `Request timed out. Try a smaller query or use LIMIT.`;
        }

        throw new McpError(errorCode, errorMessage);
      }
    });
  }

  async dispatchTool(name, args) {
    switch (name) {
      // ── Database Exploration ──
      case 'db_list': return await this.sqlHandler.handleGetDatabases(args);
      case 'db_test_speed': return await this.sqlHandler.handleTestConnectionSpeed(args);
      case 'db_schemas': return await this.sqlHandler.handleGetDatabaseSchemas(args);
      case 'db_tables': return await this.sqlHandler.handleGetDatabaseTables(args);
      case 'db_table_profile': return await this.schemaHandler.handleTableProfile(args);
      case 'db_connection_info': return await this.schemaHandler.handleGetConnectionInfo(args);

      // ── SQL Execution ──
      case 'sql_execute': return await this.sqlHandler.handleExecuteSQL(args);
      case 'sql_submit': return await this.sqlHandler.handleSQLSubmit(args);
      case 'sql_status': return await this.sqlHandler.handleSQLStatus(args);
      case 'sql_cancel': return await this.sqlHandler.handleSQLCancel(args);

      // ── DDL Operations ──
      case 'db_table_create': return await this.schemaHandler.handleCreateTableDirect(args);
      case 'db_view_create': return await this.schemaHandler.handleCreateViewDirect(args);
      case 'db_matview_create': return await this.schemaHandler.handleCreateMaterializedViewDirect(args);
      case 'db_index_create': return await this.schemaHandler.handleCreateIndexDirect(args);
      case 'db_table_ddl': return await this.schemaHandler.handleGetTableDDL(args);
      case 'db_view_ddl': return await this.schemaHandler.handleGetViewDDL(args);
      case 'db_ai_list': return await this.schemaHandler.handleListAIObjects(args);
      case 'db_ai_drop': return await this.schemaHandler.handleDropAIObject(args);

      // ── Schema & Relationships ──
      case 'db_schema_explore': return await this.schemaHandler.handleExploreSchemaSimple(args);
      case 'db_schema_analyze': return await this.schemaHandler.handleExploreSchemaTablesAdvanced(args);
      case 'db_relationships_detect': return await this.schemaHandler.handleAnalyzeTableRelationships(args);
      case 'ai_relationships_suggest': return await this.schemaHandler.handleSuggestVirtualRelationships(args);
      case 'mb_relationships_create': return await this.schemaHandler.handleCreateRelationshipMapping(args);

      // ── Database Maintenance ──
      case 'db_vacuum_analyze': return await this.schemaHandler.handleVacuumAnalyze(args);
      case 'db_query_explain': return await this.schemaHandler.handleQueryExplain(args);
      case 'db_table_stats': return await this.schemaHandler.handleTableStats(args);
      case 'db_index_usage': return await this.schemaHandler.handleIndexUsage(args);

      // ── Cards / Questions ──
      case 'mb_question_create': return await this.cardsHandler.handleCreateQuestion(args);
      case 'mb_questions': return await this.cardsHandler.handleGetQuestions(args);
      case 'mb_question_create_parametric': return await this.cardsHandler.handleCreateParametricQuestion(args);
      case 'mb_card_get': return await this.cardsHandler.handleCardGet(args);
      case 'mb_card_update': return await this.cardsHandler.handleCardUpdate(args);
      case 'mb_card_delete': return await this.cardsHandler.handleCardDelete(args);
      case 'mb_card_archive': return await this.cardsHandler.handleCardArchive(args);
      case 'mb_card_data': return await this.cardsHandler.handleCardData(args);
      case 'mb_card_copy': return await this.cardsHandler.handleCardCopy(args);
      case 'mb_card_clone': return await this.cardsHandler.handleCardClone(args);

      // ── Dashboards ──
      case 'mb_dashboard_create': return await this.cardsHandler.handleCreateDashboard(args);
      case 'mb_dashboards': return await this.cardsHandler.handleGetDashboards(args);
      case 'mb_dashboard_template_executive': return await this.cardsHandler.handleCreateExecutiveDashboard(args);
      case 'mb_dashboard_add_card': return await this.cardsHandler.handleAddCardToDashboard(args);
      case 'mb_dashboard_get': return await this.cardsHandler.handleDashboardGet(args);
      case 'mb_dashboard_update': return await this.cardsHandler.handleDashboardUpdate(args);
      case 'mb_dashboard_delete': return await this.cardsHandler.handleDashboardDelete(args);
      case 'mb_dashboard_card_update': return await this.cardsHandler.handleDashboardCardUpdate(args);
      case 'mb_dashboard_card_remove': return await this.cardsHandler.handleDashboardCardRemove(args);
      case 'mb_dashboard_copy': return await this.cardsHandler.handleDashboardCopy(args);
      case 'mb_metric_create': return await this.cardsHandler.handleCreateMetric(args);
      case 'mb_dashboard_add_filter': return await this.cardsHandler.handleAddDashboardFilter(args);
      case 'mb_dashboard_layout_optimize': return await this.cardsHandler.handleOptimizeDashboardLayout(args);
      case 'mb_auto_describe': return await this.cardsHandler.handleAutoDescribe(args);

      // ── Direct SQL Dashboard ──
      case 'mb_dashboard_add_card_sql': return await this.dashboardDirectHandler.handleAddCardSql(args);
      case 'mb_dashboard_update_layout': return await this.dashboardDirectHandler.handleUpdateLayoutSql(args);
      case 'mb_create_parametric_question': return await this.dashboardDirectHandler.handleCreateParametricQuestionSql(args);
      case 'mb_link_dashboard_filter': return await this.dashboardDirectHandler.handleLinkDashboardFilter(args);

      // ── Visualization ──
      case 'mb_visualization_settings': return await this.cardsHandler.handleVisualizationSettings(args);
      case 'mb_visualization_recommend': return await this.cardsHandler.handleVisualizationRecommend(args);

      // ── Collections ──
      case 'mb_collection_create': return await this.collectionsHandler.handleCollectionCreate(args);
      case 'mb_collection_list': return await this.collectionsHandler.handleCollectionList(args);
      case 'mb_collection_move': return await this.collectionsHandler.handleCollectionMove(args);
      case 'mb_collection_copy': return await this.collectionsHandler.handleCollectionCopy(args);
      case 'mb_collection_permissions_get': return await this.collectionsHandler.handleCollectionPermissionsGet(args);
      case 'mb_collection_permissions_update': return await this.collectionsHandler.handleCollectionPermissionsUpdate(args);

      // ── Users & Permissions ──
      case 'mb_user_list': return await this.usersHandler.handleUserList(args);
      case 'mb_user_get': return await this.usersHandler.handleUserGet(args);
      case 'mb_user_create': return await this.usersHandler.handleUserCreate(args);
      case 'mb_user_update': return await this.usersHandler.handleUserUpdate(args);
      case 'mb_user_disable': return await this.usersHandler.handleUserDisable(args);
      case 'mb_permission_group_list': return await this.usersHandler.handlePermissionGroupList(args);
      case 'mb_permission_group_create': return await this.usersHandler.handlePermissionGroupCreate(args);
      case 'mb_permission_group_delete': return await this.usersHandler.handlePermissionGroupDelete(args);
      case 'mb_permission_group_add_user': return await this.usersHandler.handlePermissionGroupAddUser(args);
      case 'mb_permission_group_remove_user': return await this.usersHandler.handlePermissionGroupRemoveUser(args);

      // ── Actions & Alerts ──
      case 'mb_action_create': return await this.actionsHandler.handleActionCreate(args);
      case 'mb_action_list': return await this.actionsHandler.handleActionList(args);
      case 'mb_action_execute': return await this.actionsHandler.handleActionExecute(args);
      case 'mb_alert_create': return await this.actionsHandler.handleAlertCreate(args);
      case 'mb_alert_list': return await this.actionsHandler.handleAlertList(args);
      case 'mb_pulse_create': return await this.actionsHandler.handlePulseCreate(args);

      // ── Field Metadata ──
      case 'mb_field_metadata': return await this.cardsHandler.handleFieldMetadata(args);
      case 'mb_table_metadata': return await this.cardsHandler.handleTableMetadata(args);
      case 'mb_field_values': return await this.cardsHandler.handleFieldValues(args);

      // ── Embedding ──
      case 'mb_embed_url_generate': return await this.cardsHandler.handleEmbedUrlGenerate(args);
      case 'mb_embed_settings': return await this.cardsHandler.handleEmbedSettings(args);

      // ── Search ──
      case 'mb_search': return await this.cardsHandler.handleSearch(args);

      // ── Segments ──
      case 'mb_segment_create': return await this.cardsHandler.handleSegmentCreate(args);
      case 'mb_segment_list': return await this.cardsHandler.handleSegmentList(args);

      // ── Bookmarks ──
      case 'mb_bookmark_create': return await this.cardsHandler.handleBookmarkCreate(args);
      case 'mb_bookmark_list': return await this.cardsHandler.handleBookmarkList(args);
      case 'mb_bookmark_delete': return await this.cardsHandler.handleBookmarkDelete(args);

      // ── Sync & Cache ──
      case 'db_sync_schema': return await this.cardsHandler.handleDbSyncSchema(args);
      case 'mb_cache_invalidate': return await this.cardsHandler.handleCacheInvalidate(args);

      // ── Documentation ──
      case 'web_fetch_metabase_docs': return await this.docsHandler.handleFetchMetabaseDocs(args);
      case 'web_explore_metabase_docs': return await this.docsHandler.handleExploreMetabaseDocs(args);
      case 'web_search_metabase_docs': return await this.docsHandler.handleSearchMetabaseDocs(args);
      case 'web_metabase_api_reference': return await this.docsHandler.handleMetabaseApiReference(args);

      // ── Definition Tables ──
      case 'definition_tables_init': return await this.schemaHandler.handleDefinitionTablesInit(args);
      case 'definition_search_terms': return await this.schemaHandler.handleDefinitionSearchTerms(args);
      case 'definition_get_metric': return await this.schemaHandler.handleDefinitionGetMetric(args);
      case 'definition_get_template': return await this.schemaHandler.handleDefinitionGetTemplate(args);
      case 'definition_global_search': return await this.schemaHandler.handleDefinitionGlobalSearch(args);

      // ── Parametric ──
      case 'parametric_question_create': return await this.schemaHandler.handleParametricQuestionCreate(args);
      case 'parametric_dashboard_create': return await this.schemaHandler.handleParametricDashboardCreate(args);
      case 'parametric_template_preset': return await this.schemaHandler.handleParametricTemplatePreset(args);

      // ── AI Assistance ──
      case 'ai_sql_generate': return await this.sqlHandler.handleGenerateSQL(args);
      case 'ai_sql_optimize': return await this.sqlHandler.handleOptimizeQuery(args);
      case 'ai_sql_explain': return await this.sqlHandler.handleExplainQuery(args);

      // ── Activity Logging ──
      case 'activity_log_init': return await this.analyticsHandler.handleInitializeActivityLog(args);
      case 'activity_session_summary': return await this.analyticsHandler.handleGetSessionSummary(args);
      case 'activity_operation_stats': return await this.analyticsHandler.handleGetOperationStats(args);
      case 'activity_database_usage': return await this.analyticsHandler.handleGetDatabaseUsage(args);
      case 'activity_error_analysis': return await this.analyticsHandler.handleGetErrorAnalysis(args);
      case 'activity_performance_insights': return await this.analyticsHandler.handleGetPerformanceInsights(args);
      case 'activity_timeline': return await this.analyticsHandler.handleGetActivityTimeline(args);
      case 'activity_cleanup': return await this.analyticsHandler.handleCleanupActivityLogs(args);

      // ── Metadata Analytics ──
      case 'mb_meta_query_performance': return await this.analyticsHandler.handleMetadataQueryPerformance(args);
      case 'mb_meta_content_usage': return await this.analyticsHandler.handleMetadataContentUsage(args);
      case 'mb_meta_user_activity': return await this.analyticsHandler.handleMetadataUserActivity(args);
      case 'mb_meta_database_usage': return await this.analyticsHandler.handleMetadataDatabaseUsage(args);
      case 'mb_meta_dashboard_complexity': return await this.analyticsHandler.handleMetadataDashboardComplexity(args);
      case 'mb_meta_info': return await this.analyticsHandler.handleMetadataInfo(args);

      // ── Advanced Analytics ──
      case 'mb_meta_table_dependencies': return await this.analyticsHandler.handleMetadataTableDependencies(args);
      case 'mb_meta_impact_analysis': return await this.analyticsHandler.handleMetadataImpactAnalysis(args);
      case 'mb_meta_optimization_recommendations': return await this.analyticsHandler.handleMetadataOptimizationRecommendations(args);
      case 'mb_meta_error_patterns': return await this.analyticsHandler.handleMetadataErrorPatterns(args);

      // ── Export/Import ──
      case 'mb_meta_export_workspace': return await this.analyticsHandler.handleMetadataExportWorkspace(args);
      case 'mb_meta_import_preview': return await this.analyticsHandler.handleMetadataImportPreview(args);
      case 'mb_meta_compare_environments': return await this.analyticsHandler.handleMetadataCompareEnvironments(args);
      case 'mb_meta_auto_cleanup': return await this.analyticsHandler.handleMetadataAutoCleanup(args);

      // ── Internal Metadata (via MetadataHandler) ──
      case 'meta_find_internal_db': return await this.metadataHandler.handleFindInternalDb(args);
      case 'meta_audit_logs': return await this.metadataHandler.handleAuditLogs(args);
      case 'meta_lineage': return await this.metadataHandler.handleLineage(args);
      case 'meta_advanced_search': return await this.metadataHandler.handleAdvancedSearch(args);

      default:
        throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${name}`);
    }
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    await this.initialize();
    logger.info('Metabase AI Assistant MCP server running on stdio');
  }
}

// Run the server
const server = new MetabaseMCPServer();

if (process.stdout.isTTY) {
  console.log('🚀 Metabase AI Assistant MCP Server');
  console.log('📦 Version 4.0.0');
  console.log('🔧 Env: ' + (process.env.METABASE_URL || 'Not set'));
  console.log('🔒 Read-only: ' + (isReadOnlyMode() ? 'YES' : 'NO'));
  console.log('');
  console.log('Starting MCP server...');
}

server.run().catch((error) => {
  if (process.stdout.isTTY) {
    console.error('❌ Failed to start MCP server:', error.message);
  }
  process.exit(1);
});