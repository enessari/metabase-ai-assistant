import { logger } from '../utils/logger.js';

/**
 * Metabase Metadata Database Client
 *
 * Queries Metabase's application database via the Metabase API
 * using executeNativeQuery. This eliminates the need for direct
 * database credentials — only METABASE_INTERNAL_DB_ID is required.
 *
 * This client is READ-ONLY for security.
 *
 * Based on: Metabase Internal Database Reference Guide (ONMARTECH LLC)
 */
export class MetabaseMetadataClient {
  constructor(config) {
    this.metabaseClient = config.metabaseClient;
    this.internalDbId = config.internalDbId;
    this.config = {
      engine: 'api',
      database: `internal-db-${config.internalDbId}`
    };

    if (!this.metabaseClient) {
      throw new Error('MetabaseMetadataClient requires a metabaseClient instance');
    }
    if (!this.internalDbId) {
      throw new Error('MetabaseMetadataClient requires internalDbId (METABASE_INTERNAL_DB_ID)');
    }

    logger.info(`MetabaseMetadataClient initialized (API mode, DB ID: ${this.internalDbId})`);
  }

  /**
   * Execute read-only query via Metabase API
   * Returns rows as array of objects (like pg client.query().rows)
   */
  async executeQuery(sql, params = []) {
    try {
      // Security check - only SELECT queries allowed
      const sqlUpper = sql.trim().toUpperCase();
      if (!sqlUpper.startsWith('SELECT') && !sqlUpper.startsWith('WITH')) {
        throw new Error('Only SELECT queries are allowed on metadata database');
      }

      logger.debug('Executing metadata query via API:', { sql: sql.substring(0, 100) });

      const result = await this.metabaseClient.executeNativeQuery(this.internalDbId, sql);

      // Convert Metabase API response format to row-object format
      // API returns: { data: { rows: [[v1,v2,...], ...], cols: [{name:'col1'}, ...] } }
      // We need:     [{ col1: v1, col2: v2, ... }, ...]
      const cols = result.data?.cols || [];
      const rows = result.data?.rows || [];
      const colNames = cols.map(c => c.name);

      return rows.map(row => {
        const obj = {};
        colNames.forEach((name, i) => {
          obj[name] = row[i];
        });
        return obj;
      });
    } catch (error) {
      logger.error('Metadata query execution failed:', { sql: sql.substring(0, 200), error: error.message });
      throw error;
    }
  }

  // ============================================
  // Query Performance Analytics
  // ============================================

  /**
   * Get query performance statistics
   *
   * @param {number} days - Number of days to analyze (default: 7)
   * @returns {Object} Performance statistics
   */
  async getQueryPerformanceStats(days = 7) {
    const sql = `
      SELECT
        COUNT(*) as total_queries,
        COUNT(DISTINCT executor_id) as unique_users,
        AVG(running_time)::int as avg_runtime_ms,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY running_time)::int as median_runtime_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY running_time)::int as p95_runtime_ms,
        MAX(running_time) as max_runtime_ms,
        AVG(result_rows)::int as avg_rows,
        SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END)::int as cache_hits,
        SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END)::int as errors,
        ROUND(100.0 * SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END) / COUNT(*), 2) as cache_hit_rate
      FROM query_execution
      WHERE started_at > NOW() - INTERVAL '${days} days'
    `;

    const result = await this.executeQuery(sql);
    return result[0] || {};
  }

  /**
   * Get slowest queries
   *
   * @param {number} thresholdMs - Minimum runtime in milliseconds
   * @param {number} limit - Number of results to return
   * @returns {Array} Slow queries with details
   */
  async getSlowQueries(thresholdMs = 10000, limit = 20) {
    const sql = `
      SELECT
        qe.card_id,
        rc.name as question_name,
        qe.database_id,
        md.name as database_name,
        COUNT(*) as execution_count,
        AVG(qe.running_time)::int as avg_runtime_ms,
        MAX(qe.running_time) as max_runtime_ms,
        AVG(qe.result_rows)::int as avg_rows,
        MAX(qe.started_at) as last_executed,
        SUM(CASE WHEN qe.error IS NOT NULL THEN 1 ELSE 0 END)::int as error_count
      FROM query_execution qe
      LEFT JOIN report_card rc ON qe.card_id = rc.id
      LEFT JOIN metabase_database md ON qe.database_id = md.id
      WHERE qe.started_at > NOW() - INTERVAL '7 days'
        AND qe.running_time > ${thresholdMs}
      GROUP BY qe.card_id, rc.name, qe.database_id, md.name
      ORDER BY avg_runtime_ms DESC
      LIMIT ${limit}
    `;

    return await this.executeQuery(sql);
  }

  /**
   * Get query execution timeline (hourly breakdown)
   *
   * @param {number} days - Number of days to analyze
   * @returns {Array} Hourly query statistics
   */
  async getQueryExecutionTimeline(days = 7) {
    const sql = `
      SELECT
        DATE_TRUNC('hour', started_at) as hour,
        COUNT(*) as query_count,
        AVG(running_time)::int as avg_runtime_ms,
        SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END)::int as errors,
        SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END)::int as cache_hits
      FROM query_execution
      WHERE started_at > NOW() - INTERVAL '${days} days'
      GROUP BY DATE_TRUNC('hour', started_at)
      ORDER BY hour DESC
    `;

    return await this.executeQuery(sql);
  }

  /**
   * Get query performance by context (question, dashboard, ad-hoc)
   */
  async getQueryPerformanceByContext(days = 7) {
    const sql = `
      SELECT
        context,
        COUNT(*) as query_count,
        AVG(running_time)::int as avg_runtime_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY running_time)::int as p95_runtime_ms,
        SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END)::int as errors,
        ROUND(100.0 * SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as error_rate
      FROM query_execution
      WHERE started_at > NOW() - INTERVAL '${days} days'
      GROUP BY context
      ORDER BY query_count DESC
    `;

    return await this.executeQuery(sql);
  }

  /**
   * Get error analysis
   */
  async getErrorAnalysis(days = 7, limit = 20) {
    const sql = `
      SELECT
        LEFT(error, 200) as error_message,
        COUNT(*) as occurrence_count,
        qe.database_id,
        md.name as database_name,
        MAX(qe.started_at) as last_occurred
      FROM query_execution qe
      LEFT JOIN metabase_database md ON qe.database_id = md.id
      WHERE qe.started_at > NOW() - INTERVAL '${days} days'
        AND qe.error IS NOT NULL
      GROUP BY LEFT(error, 200), qe.database_id, md.name
      ORDER BY occurrence_count DESC
      LIMIT ${limit}
    `;

    return await this.executeQuery(sql);
  }

  // ============================================
  // Content Usage Analytics
  // ============================================

  /**
   * Get most popular questions
   *
   * @param {number} days - Number of days to analyze
   * @param {number} limit - Number of results
   * @returns {Array} Popular questions
   */
  async getPopularQuestions(days = 30, limit = 20) {
    const sql = `
      SELECT
        rc.id,
        rc.name,
        rc.display,
        c.name as collection_name,
        u.email as creator_email,
        COUNT(qe.id) as execution_count,
        AVG(qe.running_time)::int as avg_runtime_ms,
        MAX(qe.started_at) as last_executed,
        rc.created_at
      FROM report_card rc
      LEFT JOIN query_execution qe ON rc.id = qe.card_id
      LEFT JOIN collection c ON rc.collection_id = c.id
      LEFT JOIN core_user u ON rc.creator_id = u.id
      WHERE rc.archived = false
        AND qe.started_at > NOW() - INTERVAL '${days} days'
      GROUP BY rc.id, rc.name, rc.display, c.name, u.email, rc.created_at
      ORDER BY execution_count DESC
      LIMIT ${limit}
    `;

    return await this.executeQuery(sql);
  }

  /**
   * Get unused content (orphaned questions and dashboards)
   *
   * @param {number} days - Days without usage to consider "unused"
   * @returns {Object} Unused questions and dashboards
   */
  async getUnusedContent(days = 90) {
    const unusedQuestions = await this.executeQuery(`
      SELECT
        rc.id,
        rc.name,
        rc.display,
        c.name as collection_name,
        rc.created_at,
        MAX(qe.started_at) as last_used
      FROM report_card rc
      LEFT JOIN query_execution qe ON rc.id = qe.card_id
      LEFT JOIN collection c ON rc.collection_id = c.id
      WHERE rc.archived = false
        AND (
          qe.started_at IS NULL
          OR qe.started_at < NOW() - INTERVAL '${days} days'
        )
      GROUP BY rc.id, rc.name, rc.display, c.name, rc.created_at
      ORDER BY rc.created_at DESC
      LIMIT 50
    `);

    const unusedDashboards = await this.executeQuery(`
      SELECT
        rd.id,
        rd.name,
        c.name as collection_name,
        rd.created_at,
        COUNT(rdc.id) as card_count
      FROM report_dashboard rd
      LEFT JOIN report_dashboardcard rdc ON rd.id = rdc.dashboard_id
      LEFT JOIN collection c ON rd.collection_id = c.id
      WHERE rd.archived = false
        AND NOT EXISTS (
          SELECT 1 FROM query_execution qe
          WHERE qe.dashboard_id = rd.id
          AND qe.started_at > NOW() - INTERVAL '${days} days'
        )
      GROUP BY rd.id, rd.name, c.name, rd.created_at
      ORDER BY rd.created_at DESC
      LIMIT 50
    `);

    return {
      unused_questions: unusedQuestions,
      unused_dashboards: unusedDashboards
    };
  }

  /**
   * Get most popular dashboards
   */
  async getPopularDashboards(days = 30, limit = 20) {
    const sql = `
      SELECT
        rd.id,
        rd.name,
        c.name as collection_name,
        u.email as creator_email,
        COUNT(DISTINCT qe.id) as view_count,
        COUNT(rdc.id) as card_count,
        AVG(qe.running_time)::int as avg_load_time_ms,
        MAX(qe.started_at) as last_viewed,
        rd.created_at
      FROM report_dashboard rd
      LEFT JOIN query_execution qe ON rd.id = qe.dashboard_id
      LEFT JOIN report_dashboardcard rdc ON rd.id = rdc.dashboard_id
      LEFT JOIN collection c ON rd.collection_id = c.id
      LEFT JOIN core_user u ON rd.creator_id = u.id
      WHERE rd.archived = false
        AND qe.started_at > NOW() - INTERVAL '${days} days'
      GROUP BY rd.id, rd.name, c.name, u.email, rd.created_at
      ORDER BY view_count DESC
      LIMIT ${limit}
    `;

    return await this.executeQuery(sql);
  }

  /**
   * Get orphaned cards (questions not in any dashboard)
   */
  async getOrphanedCards() {
    const sql = `
      SELECT
        rc.id,
        rc.name,
        rc.display,
        c.name as collection_name,
        rc.created_at,
        COUNT(qe.id) as execution_count
      FROM report_card rc
      LEFT JOIN report_dashboardcard rdc ON rc.id = rdc.card_id
      LEFT JOIN query_execution qe ON rc.id = qe.card_id
        AND qe.started_at > NOW() - INTERVAL '30 days'
      LEFT JOIN collection c ON rc.collection_id = c.id
      WHERE rc.archived = false
        AND rdc.id IS NULL
      GROUP BY rc.id, rc.name, rc.display, c.name, rc.created_at
      ORDER BY execution_count DESC
      LIMIT 100
    `;

    return await this.executeQuery(sql);
  }

  // ============================================
  // User Activity Analytics
  // ============================================

  /**
   * Get user activity statistics
   *
   * @param {number} days - Number of days to analyze
   * @returns {Array} User activity stats
   */
  async getUserActivityStats(days = 30) {
    const sql = `
      SELECT
        u.id,
        u.email,
        u.first_name,
        u.last_name,
        u.is_superuser,
        u.last_login,
        COUNT(qe.id) as query_count,
        COUNT(DISTINCT qe.card_id) as unique_questions_used,
        COUNT(DISTINCT qe.dashboard_id) as unique_dashboards_viewed,
        AVG(qe.running_time)::int as avg_query_time_ms
      FROM core_user u
      LEFT JOIN query_execution qe ON u.id = qe.executor_id
        AND qe.started_at > NOW() - INTERVAL '${days} days'
      WHERE u.is_active = true
      GROUP BY u.id, u.email, u.first_name, u.last_name, u.is_superuser, u.last_login
      ORDER BY query_count DESC
    `;

    return await this.executeQuery(sql);
  }

  /**
   * Get inactive users
   *
   * @param {number} days - Days without activity
   * @returns {Array} Inactive users
   */
  async getInactiveUsers(days = 90) {
    const sql = `
      SELECT
        u.id,
        u.email,
        u.first_name,
        u.last_name,
        u.last_login,
        u.date_joined,
        EXTRACT(DAY FROM (NOW() - u.last_login)) as days_inactive
      FROM core_user u
      WHERE u.is_active = true
        AND (
          u.last_login IS NULL
          OR u.last_login < NOW() - INTERVAL '${days} days'
        )
      ORDER BY u.last_login ASC NULLS FIRST
    `;

    return await this.executeQuery(sql);
  }

  /**
   * Get login activity timeline
   */
  async getLoginTimeline(days = 30) {
    const sql = `
      SELECT
        DATE(timestamp) as login_date,
        COUNT(*) as login_count,
        COUNT(DISTINCT user_id) as unique_users
      FROM login_history
      WHERE timestamp > NOW() - INTERVAL '${days} days'
      GROUP BY DATE(timestamp)
      ORDER BY login_date DESC
    `;

    return await this.executeQuery(sql);
  }

  // ============================================
  // Database Usage Analytics
  // ============================================

  /**
   * Get database usage statistics
   */
  async getDatabaseUsageStats(days = 30) {
    const sql = `
      SELECT
        md.id,
        md.name,
        md.engine,
        COUNT(qe.id) as query_count,
        AVG(qe.running_time)::int as avg_runtime_ms,
        SUM(CASE WHEN qe.error IS NOT NULL THEN 1 ELSE 0 END)::int as error_count,
        COUNT(DISTINCT qe.executor_id) as unique_users
      FROM metabase_database md
      LEFT JOIN query_execution qe ON md.id = qe.database_id
        AND qe.started_at > NOW() - INTERVAL '${days} days'
      GROUP BY md.id, md.name, md.engine
      ORDER BY query_count DESC
    `;

    return await this.executeQuery(sql);
  }

  /**
   * Get table usage statistics
   */
  async getTableUsageStats(databaseId, days = 30) {
    const sql = `
      SELECT
        mt.id,
        mt.name as table_name,
        mt.schema,
        mt.display_name,
        COUNT(DISTINCT rc.id) as question_count
      FROM metabase_table mt
      LEFT JOIN report_card rc ON rc.table_id = mt.id
      WHERE mt.db_id = ${databaseId}
        AND mt.active = true
      GROUP BY mt.id, mt.name, mt.schema, mt.display_name
      ORDER BY question_count DESC
    `;

    return await this.executeQuery(sql);
  }

  // ============================================
  // Dashboard Analytics
  // ============================================

  /**
   * Get dashboard complexity analysis
   */
  async getDashboardComplexityAnalysis() {
    const sql = `
      SELECT
        rd.id,
        rd.name,
        COUNT(rdc.id) as card_count,
        AVG(qe.running_time)::int as avg_load_time_ms,
        MAX(qe.running_time) as max_load_time_ms,
        COUNT(DISTINCT qe.id) as view_count_30d
      FROM report_dashboard rd
      LEFT JOIN report_dashboardcard rdc ON rd.id = rdc.dashboard_id
      LEFT JOIN query_execution qe ON rd.id = qe.dashboard_id
        AND qe.started_at > NOW() - INTERVAL '30 days'
      WHERE rd.archived = false
      GROUP BY rd.id, rd.name
      HAVING COUNT(rdc.id) > 10  -- Complex dashboards with 10+ cards
      ORDER BY card_count DESC
    `;

    return await this.executeQuery(sql);
  }

  // ============================================
  // Utility Methods
  // ============================================

  /**
   * Test connection to metadata database via API
   */
  async testConnection() {
    try {
      const result = await this.executeQuery('SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = \'public\'');
      return {
        success: true,
        database: this.config.database,
        engine: this.config.engine,
        internal_db_id: this.internalDbId,
        table_count: result[0]?.table_count || 0
      };
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Get metadata database info
   */
  async getDatabaseInfo() {
    const sql = `
      SELECT
        (SELECT COUNT(*) FROM core_user WHERE is_active = true) as active_users,
        (SELECT COUNT(*) FROM report_card WHERE archived = false) as active_questions,
        (SELECT COUNT(*) FROM report_dashboard WHERE archived = false) as active_dashboards,
        (SELECT COUNT(*) FROM metabase_database) as connected_databases,
        (SELECT COUNT(*) FROM query_execution WHERE started_at > NOW() - INTERVAL '7 days') as queries_last_7d
    `;

    const result = await this.executeQuery(sql);
    return result[0] || {};
  }

  // ============================================
  // PHASE 2: DEPENDENCY & IMPACT ANALYSIS
  // ============================================

  /**
   * Get dependency graph for a table
   * Shows all questions and dashboards that depend on a specific table
   *
   * @param {number} databaseId - Database ID
   * @param {string} tableName - Table name to analyze
   * @param {string} schemaName - Schema name (optional)
   * @returns {Object} Dependency information
   */
  async getTableDependencies(databaseId, tableName, schemaName = null) {
    // First get table ID
    let tableQuery = `
      SELECT id, name, schema, display_name
      FROM metabase_table
      WHERE db_id = ${databaseId}
        AND name = '${tableName}'
    `;

    if (schemaName) {
      tableQuery += ` AND schema = '${schemaName}'`;
    }

    const tables = await this.executeQuery(tableQuery);

    if (tables.length === 0) {
      return {
        table_found: false,
        message: `Table ${schemaName ? schemaName + '.' : ''}${tableName} not found in database ${databaseId}`
      };
    }

    const table = tables[0];
    const tableId = table.id;

    // Get questions using this table
    const questionsQuery = `
      SELECT
        rc.id,
        rc.name,
        rc.display,
        rc.query_type,
        c.name as collection_name,
        u.email as creator,
        rc.created_at,
        COUNT(DISTINCT qe.id) as execution_count_30d
      FROM report_card rc
      LEFT JOIN collection c ON rc.collection_id = c.id
      LEFT JOIN core_user u ON rc.creator_id = u.id
      LEFT JOIN query_execution qe ON rc.id = qe.card_id
        AND qe.started_at > NOW() - INTERVAL '30 days'
      WHERE rc.table_id = ${tableId}
        AND rc.archived = false
      GROUP BY rc.id, rc.name, rc.display, rc.query_type, c.name, u.email, rc.created_at
      ORDER BY execution_count_30d DESC
    `;

    const questions = await this.executeQuery(questionsQuery);

    // Get dashboards that contain these questions
    const questionIds = questions.map(q => q.id).join(',');
    let dashboards = [];

    if (questionIds) {
      const dashboardsQuery = `
        SELECT DISTINCT
          rd.id,
          rd.name,
          c.name as collection_name,
          COUNT(DISTINCT rdc.id) as total_cards,
          COUNT(DISTINCT qe.id) as view_count_30d
        FROM report_dashboard rd
        JOIN report_dashboardcard rdc ON rd.id = rdc.dashboard_id
        LEFT JOIN collection c ON rd.collection_id = c.id
        LEFT JOIN query_execution qe ON rd.id = qe.dashboard_id
          AND qe.started_at > NOW() - INTERVAL '30 days'
        WHERE rdc.card_id IN (${questionIds})
          AND rd.archived = false
        GROUP BY rd.id, rd.name, c.name
        ORDER BY view_count_30d DESC
      `;

      dashboards = await this.executeQuery(dashboardsQuery);
    }

    // Get fields from this table
    const fieldsQuery = `
      SELECT
        id,
        name,
        display_name,
        base_type,
        semantic_type,
        has_field_values
      FROM metabase_field
      WHERE table_id = ${tableId}
        AND active = true
      ORDER BY name
    `;

    const fields = await this.executeQuery(fieldsQuery);

    return {
      table_found: true,
      table: {
        id: table.id,
        name: table.name,
        schema: table.schema,
        display_name: table.display_name
      },
      questions: questions,
      dashboards: dashboards,
      fields: fields,
      impact_summary: {
        questions_affected: questions.length,
        dashboards_affected: dashboards.length,
        fields_count: fields.length,
        total_executions_30d: questions.reduce((sum, q) => sum + (parseInt(q.execution_count_30d) || 0), 0)
      }
    };
  }

  /**
   * Get field dependencies
   * Shows which questions use a specific field
   */
  async getFieldDependencies(fieldId) {
    const fieldQuery = `
      SELECT
        mf.id,
        mf.name,
        mf.display_name,
        mf.base_type,
        mf.semantic_type,
        mt.name as table_name,
        mt.schema,
        md.name as database_name
      FROM metabase_field mf
      JOIN metabase_table mt ON mf.table_id = mt.id
      JOIN metabase_database md ON mt.db_id = md.id
      WHERE mf.id = ${fieldId}
    `;

    const fields = await this.executeQuery(fieldQuery);

    if (fields.length === 0) {
      return {
        field_found: false,
        message: `Field ID ${fieldId} not found`
      };
    }

    const field = fields[0];

    // Find questions that might use this field
    // This is tricky because field usage is in JSON columns
    const questionsQuery = `
      SELECT
        rc.id,
        rc.name,
        rc.display,
        rc.query_type,
        c.name as collection_name,
        COUNT(DISTINCT qe.id) as execution_count_30d
      FROM report_card rc
      LEFT JOIN collection c ON rc.collection_id = c.id
      LEFT JOIN query_execution qe ON rc.id = qe.card_id
        AND qe.started_at > NOW() - INTERVAL '30 days'
      WHERE rc.table_id = (SELECT table_id FROM metabase_field WHERE id = ${fieldId})
        AND rc.archived = false
      GROUP BY rc.id, rc.name, rc.display, rc.query_type, c.name
      ORDER BY execution_count_30d DESC
    `;

    const questions = await this.executeQuery(questionsQuery);

    return {
      field_found: true,
      field: field,
      potentially_affected_questions: questions,
      impact_summary: {
        questions_on_same_table: questions.length
      }
    };
  }

  /**
   * Get complete dependency chain
   * Database → Tables → Questions → Dashboards
   */
  async getDatabaseDependencyChain(databaseId) {
    const dbQuery = `
      SELECT id, name, engine
      FROM metabase_database
      WHERE id = ${databaseId}
    `;

    const databases = await this.executeQuery(dbQuery);

    if (databases.length === 0) {
      return {
        database_found: false,
        message: `Database ID ${databaseId} not found`
      };
    }

    const database = databases[0];

    // Get all tables
    const tablesQuery = `
      SELECT
        mt.id,
        mt.name,
        mt.schema,
        mt.display_name,
        COUNT(DISTINCT rc.id) as question_count
      FROM metabase_table mt
      LEFT JOIN report_card rc ON mt.id = rc.table_id
        AND rc.archived = false
      WHERE mt.db_id = ${databaseId}
        AND mt.active = true
      GROUP BY mt.id, mt.name, mt.schema, mt.display_name
      HAVING COUNT(DISTINCT rc.id) > 0
      ORDER BY question_count DESC
    `;

    const tables = await this.executeQuery(tablesQuery);

    // Get all questions
    const questionsQuery = `
      SELECT
        rc.id,
        rc.name,
        rc.table_id,
        mt.name as table_name,
        COUNT(DISTINCT rdc.dashboard_id) as dashboard_count
      FROM report_card rc
      JOIN metabase_table mt ON rc.table_id = mt.id
      LEFT JOIN report_dashboardcard rdc ON rc.id = rdc.card_id
      WHERE rc.database_id = ${databaseId}
        AND rc.archived = false
      GROUP BY rc.id, rc.name, rc.table_id, mt.name
      ORDER BY dashboard_count DESC
    `;

    const questions = await this.executeQuery(questionsQuery);

    // Get all dashboards
    const dashboardsQuery = `
      SELECT DISTINCT
        rd.id,
        rd.name,
        COUNT(DISTINCT rdc.id) as card_count
      FROM report_dashboard rd
      JOIN report_dashboardcard rdc ON rd.id = rdc.dashboard_id
      JOIN report_card rc ON rdc.card_id = rc.id
      WHERE rc.database_id = ${databaseId}
        AND rd.archived = false
      GROUP BY rd.id, rd.name
      ORDER BY card_count DESC
    `;

    const dashboards = await this.executeQuery(dashboardsQuery);

    return {
      database_found: true,
      database: database,
      tables: tables,
      questions: questions,
      dashboards: dashboards,
      summary: {
        total_tables: tables.length,
        total_questions: questions.length,
        total_dashboards: dashboards.length
      }
    };
  }

  /**
   * Analyze impact of removing a table
   * Returns breaking changes and affected objects
   */
  async analyzeTableRemovalImpact(databaseId, tableName, schemaName = null) {
    const dependencies = await this.getTableDependencies(databaseId, tableName, schemaName);

    if (!dependencies.table_found) {
      return dependencies;
    }

    // Categorize impact severity
    const criticalQuestions = dependencies.questions.filter(q => parseInt(q.execution_count_30d) > 10);
    const unusedQuestions = dependencies.questions.filter(q => parseInt(q.execution_count_30d) === 0);

    const criticalDashboards = dependencies.dashboards.filter(d => parseInt(d.view_count_30d) > 5);

    return {
      ...dependencies,
      impact_analysis: {
        severity: criticalQuestions.length > 0 || criticalDashboards.length > 0 ? 'HIGH' :
          dependencies.questions.length > 0 ? 'MEDIUM' : 'LOW',
        breaking_changes: {
          questions_will_break: dependencies.questions.length,
          dashboards_will_break: dependencies.dashboards.length,
          critical_questions: criticalQuestions.length,
          critical_dashboards: criticalDashboards.length,
          unused_questions: unusedQuestions.length
        },
        recommendations: this._generateRemovalRecommendations(dependencies, criticalQuestions, criticalDashboards)
      }
    };
  }

  _generateRemovalRecommendations(dependencies, criticalQuestions, criticalDashboards) {
    const recommendations = [];

    if (criticalQuestions.length > 0) {
      recommendations.push(`⚠️ HIGH IMPACT: ${criticalQuestions.length} frequently-used questions will break`);
      recommendations.push('→ Review and migrate critical questions before removal');
    }

    if (criticalDashboards.length > 0) {
      recommendations.push(`⚠️ HIGH IMPACT: ${criticalDashboards.length} active dashboards will be affected`);
      recommendations.push('→ Update dashboards or find alternative data sources');
    }

    if (dependencies.questions.length > 0 && criticalQuestions.length === 0) {
      recommendations.push(`⚠️ MEDIUM IMPACT: ${dependencies.questions.length} questions will break (low usage)`);
      recommendations.push('→ Consider archiving instead of deleting');
    }

    if (dependencies.questions.length === 0) {
      recommendations.push('✅ LOW IMPACT: No questions depend on this table');
      recommendations.push('→ Safe to remove if table is no longer needed');
    }

    return recommendations;
  }

  // ============================================
  // OPTIMIZATION RECOMMENDATIONS
  // ============================================

  /**
   * Get index recommendations based on query patterns
   */
  async getIndexRecommendations(databaseId, days = 30) {
    // Analyze which tables/columns are frequently queried
    const sql = `
      SELECT
        mt.schema,
        mt.name as table_name,
        COUNT(qe.id) as query_count,
        AVG(qe.running_time)::int as avg_runtime_ms,
        MAX(qe.running_time) as max_runtime_ms,
        COUNT(DISTINCT mf.id) as field_count
      FROM metabase_table mt
      LEFT JOIN report_card rc ON mt.id = rc.table_id
      LEFT JOIN query_execution qe ON rc.id = qe.card_id
        AND qe.started_at > NOW() - INTERVAL '${days} days'
      LEFT JOIN metabase_field mf ON mt.id = mf.table_id
        AND mf.active = true
      WHERE mt.db_id = ${databaseId}
        AND mt.active = true
      GROUP BY mt.id, mt.schema, mt.name
      HAVING COUNT(qe.id) > 10
        AND AVG(qe.running_time) > 1000
      ORDER BY query_count DESC, avg_runtime_ms DESC
      LIMIT 20
    `;

    const candidates = await this.executeQuery(sql);

    // Generate recommendations
    const recommendations = candidates.map(c => ({
      schema: c.schema,
      table: c.table_name,
      query_count: parseInt(c.query_count),
      avg_runtime_ms: parseInt(c.avg_runtime_ms),
      max_runtime_ms: parseInt(c.max_runtime_ms),
      recommendation: this._generateIndexRecommendation(c),
      priority: this._calculateIndexPriority(c)
    }));

    return recommendations.sort((a, b) => {
      const priorityOrder = { HIGH: 3, MEDIUM: 2, LOW: 1 };
      return priorityOrder[b.priority] - priorityOrder[a.priority];
    });
  }

  _generateIndexRecommendation(tableStats) {
    const queryCount = parseInt(tableStats.query_count);
    const avgRuntime = parseInt(tableStats.avg_runtime_ms);

    if (queryCount > 50 && avgRuntime > 5000) {
      return `Consider adding indexes on frequently filtered/joined columns. High query volume (${queryCount}) with slow performance (avg ${avgRuntime}ms).`;
    } else if (queryCount > 20 && avgRuntime > 2000) {
      return `Review query patterns and add indexes on WHERE clause columns. Moderate usage with performance issues.`;
    } else {
      return `Monitor query patterns. Consider indexes if performance degrades further.`;
    }
  }

  _calculateIndexPriority(tableStats) {
    const queryCount = parseInt(tableStats.query_count);
    const avgRuntime = parseInt(tableStats.avg_runtime_ms);

    if (queryCount > 50 && avgRuntime > 5000) return 'HIGH';
    if (queryCount > 20 && avgRuntime > 2000) return 'MEDIUM';
    return 'LOW';
  }

  /**
   * Get materialized view candidates
   * Identifies repeated expensive queries that could benefit from materialization
   */
  async getMaterializedViewCandidates(days = 30, minExecutions = 5) {
    const sql = `
      SELECT
        qe.hash,
        rc.id as card_id,
        rc.name as question_name,
        rc.query_type,
        md.name as database_name,
        md.engine,
        COUNT(*) as execution_count,
        AVG(qe.running_time)::int as avg_runtime_ms,
        MAX(qe.running_time) as max_runtime_ms,
        AVG(qe.result_rows)::int as avg_rows,
        SUM(qe.running_time) as total_time_ms
      FROM query_execution qe
      LEFT JOIN report_card rc ON qe.card_id = rc.id
      LEFT JOIN metabase_database md ON qe.database_id = md.id
      WHERE qe.started_at > NOW() - INTERVAL '${days} days'
        AND qe.hash IS NOT NULL
        AND qe.running_time > 5000
        AND qe.result_rows IS NOT NULL
      GROUP BY qe.hash, rc.id, rc.name, rc.query_type, md.name, md.engine
      HAVING COUNT(*) >= ${minExecutions}
      ORDER BY total_time_ms DESC
      LIMIT 20
    `;

    const candidates = await this.executeQuery(sql);

    return candidates.map(c => ({
      ...c,
      execution_count: parseInt(c.execution_count),
      avg_runtime_ms: parseInt(c.avg_runtime_ms),
      max_runtime_ms: parseInt(c.max_runtime_ms),
      avg_rows: parseInt(c.avg_rows),
      total_time_saved_potential: parseInt(c.total_time_ms) * 0.9, // 90% time savings estimate
      recommendation: this._generateMaterializedViewRecommendation(c),
      priority: this._calculateMatViewPriority(c)
    }));
  }

  _generateMaterializedViewRecommendation(queryStats) {
    const execCount = parseInt(queryStats.execution_count);
    const avgRuntime = parseInt(queryStats.avg_runtime_ms);
    const totalTime = parseInt(queryStats.total_time_ms);

    const hoursSaved = (totalTime * 0.9) / (1000 * 60 * 60); // 90% savings in hours

    return `Create materialized view to cache results. Executed ${execCount} times with avg ${avgRuntime}ms. Potential savings: ${hoursSaved.toFixed(2)} hours in last 30 days.`;
  }

  _calculateMatViewPriority(queryStats) {
    const execCount = parseInt(queryStats.execution_count);
    const totalTime = parseInt(queryStats.total_time_ms);

    if (execCount > 20 && totalTime > 600000) return 'HIGH'; // 10+ minutes total
    if (execCount > 10 && totalTime > 180000) return 'MEDIUM'; // 3+ minutes total
    return 'LOW';
  }

  /**
   * Get cache optimization recommendations
   */
  async getCacheOptimizationRecommendations(days = 7) {
    const sql = `
      SELECT
        rc.id as card_id,
        rc.name as question_name,
        rc.cache_ttl,
        COUNT(qe.id) as execution_count,
        AVG(qe.running_time)::int as avg_runtime_ms,
        SUM(CASE WHEN qe.cache_hit THEN 1 ELSE 0 END) as cache_hits,
        SUM(CASE WHEN NOT qe.cache_hit OR qe.cache_hit IS NULL THEN 1 ELSE 0 END) as cache_misses,
        ROUND(100.0 * SUM(CASE WHEN qe.cache_hit THEN 1 ELSE 0 END) / COUNT(qe.id), 2) as cache_hit_rate
      FROM report_card rc
      JOIN query_execution qe ON rc.id = qe.card_id
      WHERE qe.started_at > NOW() - INTERVAL '${days} days'
        AND rc.archived = false
      GROUP BY rc.id, rc.name, rc.cache_ttl
      HAVING COUNT(qe.id) >= 5
      ORDER BY execution_count DESC
      LIMIT 30
    `;

    const cards = await this.executeQuery(sql);

    return cards.map(c => {
      const hitRate = parseFloat(c.cache_hit_rate) || 0;
      const currentTTL = c.cache_ttl;
      const execCount = parseInt(c.execution_count);

      let recommendation = '';
      let suggestedTTL = currentTTL;

      if (hitRate < 30 && execCount > 10) {
        suggestedTTL = currentTTL ? currentTTL * 2 : 3600; // 1 hour default
        recommendation = `Low cache hit rate (${hitRate}%). Increase cache TTL to ${suggestedTTL}s to improve performance.`;
      } else if (hitRate > 80 && currentTTL) {
        recommendation = `Excellent cache hit rate (${hitRate}%). Current TTL (${currentTTL}s) is optimal.`;
      } else if (!currentTTL && execCount > 15) {
        suggestedTTL = 3600;
        recommendation = `No caching configured. Enable caching with TTL ${suggestedTTL}s (1 hour) for frequently accessed query.`;
      } else {
        recommendation = `Cache performance is acceptable (${hitRate}% hit rate).`;
      }

      return {
        card_id: c.card_id,
        question_name: c.question_name,
        current_cache_ttl: currentTTL,
        suggested_cache_ttl: suggestedTTL,
        execution_count: execCount,
        cache_hit_rate: hitRate,
        recommendation: recommendation
      };
    });
  }

  // ============================================
  // ERROR PATTERN ANALYSIS
  // ============================================

  /**
   * Analyze error patterns and categorize them
   */
  async getErrorPatterns(days = 30) {
    const sql = `
      SELECT
        SUBSTRING(error, 1, 100) as error_pattern,
        COUNT(*) as occurrence_count,
        COUNT(DISTINCT qe.card_id) as affected_questions,
        COUNT(DISTINCT qe.database_id) as affected_databases,
        COUNT(DISTINCT qe.executor_id) as affected_users,
        MIN(qe.started_at) as first_occurrence,
        MAX(qe.started_at) as last_occurrence,
        md.name as primary_database,
        md.engine as database_engine
      FROM query_execution qe
      LEFT JOIN metabase_database md ON qe.database_id = md.id
      WHERE qe.started_at > NOW() - INTERVAL '${days} days'
        AND qe.error IS NOT NULL
      GROUP BY SUBSTRING(error, 1, 100), md.name, md.engine
      HAVING COUNT(*) > 1
      ORDER BY occurrence_count DESC
      LIMIT 30
    `;

    const patterns = await this.executeQuery(sql);

    return patterns.map(p => ({
      ...p,
      occurrence_count: parseInt(p.occurrence_count),
      affected_questions: parseInt(p.affected_questions),
      affected_databases: parseInt(p.affected_databases),
      affected_users: parseInt(p.affected_users),
      category: this._categorizeError(p.error_pattern),
      severity: this._calculateErrorSeverity(p),
      resolution_suggestion: this._suggestErrorResolution(p.error_pattern, p.database_engine)
    }));
  }

  _categorizeError(errorMessage) {
    const msg = errorMessage.toLowerCase();

    if (msg.includes('timeout') || msg.includes('timed out')) return 'TIMEOUT';
    if (msg.includes('permission') || msg.includes('denied') || msg.includes('access')) return 'PERMISSION';
    if (msg.includes('syntax') || msg.includes('parse')) return 'SYNTAX';
    if (msg.includes('not found') || msg.includes('does not exist')) return 'NOT_FOUND';
    if (msg.includes('connection') || msg.includes('connect')) return 'CONNECTION';
    if (msg.includes('memory') || msg.includes('resources')) return 'RESOURCE';
    if (msg.includes('constraint') || msg.includes('unique') || msg.includes('foreign key')) return 'CONSTRAINT';

    return 'OTHER';
  }

  _calculateErrorSeverity(errorStats) {
    const occurrences = parseInt(errorStats.occurrence_count);
    const affectedQuestions = parseInt(errorStats.affected_questions);
    const affectedUsers = parseInt(errorStats.affected_users);

    if (occurrences > 50 || (affectedQuestions > 10 && affectedUsers > 5)) return 'HIGH';
    if (occurrences > 10 || (affectedQuestions > 3 && affectedUsers > 2)) return 'MEDIUM';
    return 'LOW';
  }

  _suggestErrorResolution(errorMessage, engine) {
    const msg = errorMessage.toLowerCase();
    const category = this._categorizeError(msg);

    const resolutions = {
      'TIMEOUT': 'Optimize query performance, add indexes, or increase timeout limit. Consider breaking into smaller queries.',
      'PERMISSION': 'Check database user permissions. Grant necessary SELECT/EXECUTE privileges.',
      'SYNTAX': 'Review SQL syntax. May need to update query for database engine compatibility.',
      'NOT_FOUND': 'Verify table/column names. Schema may have changed. Update question definitions.',
      'CONNECTION': 'Check database connectivity, firewall rules, and connection pooling settings.',
      'RESOURCE': 'Query consuming too many resources. Add LIMIT clause, optimize JOINs, or schedule during off-peak hours.',
      'CONSTRAINT': 'Data integrity issue. Review constraint violations and data quality.',
      'OTHER': 'Review full error details and database logs for specific resolution.'
    };

    let suggestion = resolutions[category] || resolutions['OTHER'];

    // Engine-specific suggestions
    if (engine === 'postgres' && category === 'TIMEOUT') {
      suggestion += ' Consider using EXPLAIN ANALYZE to identify bottlenecks.';
    }

    return suggestion;
  }

  /**
   * Get temporal error analysis (errors over time)
   */
  async getErrorTimeline(days = 30) {
    const sql = `
      SELECT
        DATE_TRUNC('day', started_at) as error_date,
        COUNT(*) as error_count,
        COUNT(DISTINCT card_id) as affected_questions,
        COUNT(DISTINCT database_id) as affected_databases,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM query_execution WHERE started_at > NOW() - INTERVAL '${days} days'), 2) as error_rate
      FROM query_execution
      WHERE started_at > NOW() - INTERVAL '${days} days'
        AND error IS NOT NULL
      GROUP BY DATE_TRUNC('day', started_at)
      ORDER BY error_date DESC
    `;

    return await this.executeQuery(sql);
  }

  /**
   * Get questions with recurring errors
   */
  async getRecurringErrorQuestions(days = 30, minErrors = 3) {
    const sql = `
      SELECT
        rc.id as card_id,
        rc.name as question_name,
        rc.query_type,
        c.name as collection_name,
        COUNT(qe.id) as total_executions,
        SUM(CASE WHEN qe.error IS NOT NULL THEN 1 ELSE 0 END) as error_count,
        ROUND(100.0 * SUM(CASE WHEN qe.error IS NOT NULL THEN 1 ELSE 0 END) / COUNT(qe.id), 2) as error_rate,
        MAX(qe.started_at) as last_error_time,
        SUBSTRING(MAX(qe.error), 1, 200) as latest_error
      FROM report_card rc
      JOIN query_execution qe ON rc.id = qe.card_id
      LEFT JOIN collection c ON rc.collection_id = c.id
      WHERE qe.started_at > NOW() - INTERVAL '${days} days'
        AND rc.archived = false
      GROUP BY rc.id, rc.name, rc.query_type, c.name
      HAVING SUM(CASE WHEN qe.error IS NOT NULL THEN 1 ELSE 0 END) >= ${minErrors}
      ORDER BY error_rate DESC, error_count DESC
      LIMIT 20
    `;

    const questions = await this.executeQuery(sql);

    return questions.map(q => ({
      ...q,
      total_executions: parseInt(q.total_executions),
      error_count: parseInt(q.error_count),
      error_rate: parseFloat(q.error_rate),
      severity: parseFloat(q.error_rate) > 50 ? 'CRITICAL' :
        parseFloat(q.error_rate) > 20 ? 'HIGH' : 'MEDIUM',
      recommendation: parseFloat(q.error_rate) > 50
        ? 'CRITICAL: More than half of executions fail. Archive or fix immediately.'
        : parseFloat(q.error_rate) > 20
          ? 'HIGH: Frequent failures. Prioritize fixing this question.'
          : 'MEDIUM: Occasional failures. Monitor and investigate.'
    }));
  }

  // ============================================
  // PHASE 3: EXPORT/IMPORT & MIGRATION
  // ============================================

  /**
   * Export workspace (questions, dashboards, collections)
   * READ-ONLY operation - Safe to execute
   *
   * @param {Object} options - Export options
   * @returns {Object} Exported workspace data
   */
  async exportWorkspace(options = {}) {
    const {
      include_questions = true,
      include_dashboards = true,
      include_collections = true,
      include_metrics = false,
      include_segments = false,
      collection_ids = null, // null = all, or array of IDs
      archived = false // include archived items
    } = options;

    const workspace = {
      export_info: {
        timestamp: new Date().toISOString(),
        database: this.config.database,
        engine: this.config.engine
      },
      collections: [],
      questions: [],
      dashboards: []
    };

    try {
      // Export collections
      if (include_collections) {
        let collectionsQuery = `
          SELECT
            id, name, slug, description, color, personal_owner_id,
            location, authority_level, archived, created_at
          FROM collection
          WHERE 1=1
        `;

        if (collection_ids) {
          collectionsQuery += ` AND id IN (${collection_ids.join(',')})`;
        }

        if (!archived) {
          collectionsQuery += ` AND (archived = false OR archived IS NULL)`;
        }

        collectionsQuery += ` ORDER BY id`;

        workspace.collections = await this.executeQuery(collectionsQuery);
      }

      // Export questions
      if (include_questions) {
        let questionsQuery = `
          SELECT
            rc.id, rc.name, rc.description, rc.display, rc.query_type,
            rc.database_id, rc.table_id, rc.collection_id,
            rc.dataset_query, rc.visualization_settings, rc.parameters,
            rc.result_metadata, rc.cache_ttl,
            rc.created_at, rc.updated_at,
            u.email as creator_email,
            c.name as collection_name
          FROM report_card rc
          LEFT JOIN core_user u ON rc.creator_id = u.id
          LEFT JOIN collection c ON rc.collection_id = c.id
          WHERE 1=1
        `;

        if (collection_ids) {
          questionsQuery += ` AND rc.collection_id IN (${collection_ids.join(',')})`;
        }

        if (!archived) {
          questionsQuery += ` AND rc.archived = false`;
        }

        questionsQuery += ` ORDER BY rc.id`;

        workspace.questions = await this.executeQuery(questionsQuery);
      }

      // Export dashboards
      if (include_dashboards) {
        let dashboardsQuery = `
          SELECT
            rd.id, rd.name, rd.description, rd.collection_id,
            rd.parameters, rd.width,
            rd.created_at, rd.updated_at,
            u.email as creator_email,
            c.name as collection_name
          FROM report_dashboard rd
          LEFT JOIN core_user u ON rd.creator_id = u.id
          LEFT JOIN collection c ON rd.collection_id = c.id
          WHERE 1=1
        `;

        if (collection_ids) {
          dashboardsQuery += ` AND rd.collection_id IN (${collection_ids.join(',')})`;
        }

        if (!archived) {
          dashboardsQuery += ` AND rd.archived = false`;
        }

        dashboardsQuery += ` ORDER BY rd.id`;

        workspace.dashboards = await this.executeQuery(dashboardsQuery);

        // Get dashboard cards for each dashboard
        for (const dashboard of workspace.dashboards) {
          const cardsQuery = `
            SELECT
              id, card_id, row, col, size_x, size_y,
              parameter_mappings, visualization_settings
            FROM report_dashboardcard
            WHERE dashboard_id = ${dashboard.id}
            ORDER BY row, col
          `;

          dashboard.cards = await this.executeQuery(cardsQuery);
        }
      }

      workspace.summary = {
        collections_count: workspace.collections.length,
        questions_count: workspace.questions.length,
        dashboards_count: workspace.dashboards.length
      };

      return workspace;
    } catch (error) {
      logger.error('Workspace export failed:', error);
      throw error;
    }
  }

  /**
   * Preview import impact (dry-run analysis)
   * Analyzes what would change without making changes
   *
   * @param {Object} workspace - Workspace data to import
   * @returns {Object} Impact analysis
   */
  async previewImportImpact(workspace) {
    const impact = {
      new_items: { collections: 0, questions: 0, dashboards: 0 },
      existing_items: { collections: 0, questions: 0, dashboards: 0 },
      conflicts: [],
      warnings: [],
      recommendations: []
    };

    try {
      // Check collections
      if (workspace.collections) {
        for (const col of workspace.collections) {
          const existing = await this.executeQuery(`
            SELECT id, name FROM collection WHERE name = '${col.name.replace(/'/g, "''")}'
          `);

          if (existing.length > 0) {
            impact.existing_items.collections++;
            impact.conflicts.push({
              type: 'collection',
              name: col.name,
              issue: 'Collection with same name exists',
              existing_id: existing[0].id
            });
          } else {
            impact.new_items.collections++;
          }
        }
      }

      // Check questions
      if (workspace.questions) {
        for (const q of workspace.questions) {
          const existing = await this.executeQuery(`
            SELECT id, name FROM report_card WHERE name = '${q.name.replace(/'/g, "''")}'
          `);

          if (existing.length > 0) {
            impact.existing_items.questions++;
            impact.conflicts.push({
              type: 'question',
              name: q.name,
              issue: 'Question with same name exists',
              existing_id: existing[0].id
            });
          } else {
            impact.new_items.questions++;
          }
        }
      }

      // Check dashboards
      if (workspace.dashboards) {
        for (const d of workspace.dashboards) {
          const existing = await this.executeQuery(`
            SELECT id, name FROM report_dashboard WHERE name = '${d.name.replace(/'/g, "''")}'
          `);

          if (existing.length > 0) {
            impact.existing_items.dashboards++;
            impact.conflicts.push({
              type: 'dashboard',
              name: d.name,
              issue: 'Dashboard with same name exists',
              existing_id: existing[0].id
            });
          } else {
            impact.new_items.dashboards++;
          }
        }
      }

      // Generate warnings
      if (impact.conflicts.length > 0) {
        impact.warnings.push(`⚠️ ${impact.conflicts.length} naming conflicts detected - items may be overwritten`);
      }

      if (impact.existing_items.questions > 0 || impact.existing_items.dashboards > 0) {
        impact.warnings.push(`⚠️ Some items already exist - consider backup before import`);
      }

      // Generate recommendations
      impact.recommendations.push('✓ Create backup of current workspace before import');

      if (impact.conflicts.length > 0) {
        impact.recommendations.push('✓ Resolve naming conflicts or use merge strategy');
      }

      if (impact.new_items.questions > 10 || impact.new_items.dashboards > 5) {
        impact.recommendations.push('✓ Import in batches to easier rollback if needed');
      }

      impact.recommendations.push('✓ Test import in dev/staging environment first');
      impact.recommendations.push('✓ Set approved: true to execute import');

      impact.severity = impact.conflicts.length > 10 ? 'HIGH' :
        impact.conflicts.length > 0 ? 'MEDIUM' : 'LOW';

      return impact;
    } catch (error) {
      logger.error('Import impact preview failed:', error);
      throw error;
    }
  }

  /**
   * Compare two environments
   * Useful for dev → staging → prod migrations
   *
   * @param {Object} targetWorkspace - Workspace from target environment
   * @returns {Object} Comparison results
   */
  async compareEnvironments(targetWorkspace) {
    const comparison = {
      missing_in_target: { collections: [], questions: [], dashboards: [] },
      missing_in_source: { collections: [], questions: [], dashboards: [] },
      different: { questions: [], dashboards: [] },
      identical: { questions: 0, dashboards: 0 }
    };

    try {
      // Get current workspace (source)
      const sourceWorkspace = await this.exportWorkspace({
        include_questions: true,
        include_dashboards: true,
        include_collections: true
      });

      // Compare collections
      const sourceCollectionNames = new Set(sourceWorkspace.collections.map(c => c.name));
      const targetCollectionNames = new Set(targetWorkspace.collections.map(c => c.name));

      for (const col of sourceWorkspace.collections) {
        if (!targetCollectionNames.has(col.name)) {
          comparison.missing_in_target.collections.push(col.name);
        }
      }

      for (const col of targetWorkspace.collections) {
        if (!sourceCollectionNames.has(col.name)) {
          comparison.missing_in_source.collections.push(col.name);
        }
      }

      // Compare questions
      const sourceQuestions = new Map(sourceWorkspace.questions.map(q => [q.name, q]));
      const targetQuestions = new Map(targetWorkspace.questions.map(q => [q.name, q]));

      for (const [name, sourceQ] of sourceQuestions) {
        if (!targetQuestions.has(name)) {
          comparison.missing_in_target.questions.push(name);
        } else {
          const targetQ = targetQuestions.get(name);
          // Compare SQL/query
          if (JSON.stringify(sourceQ.dataset_query) !== JSON.stringify(targetQ.dataset_query)) {
            comparison.different.questions.push({
              name: name,
              reason: 'Different SQL query',
              source_id: sourceQ.id,
              target_id: targetQ.id
            });
          } else {
            comparison.identical.questions++;
          }
        }
      }

      for (const name of targetQuestions.keys()) {
        if (!sourceQuestions.has(name)) {
          comparison.missing_in_source.questions.push(name);
        }
      }

      // Compare dashboards
      const sourceDashboards = new Map(sourceWorkspace.dashboards.map(d => [d.name, d]));
      const targetDashboards = new Map(targetWorkspace.dashboards.map(d => [d.name, d]));

      for (const [name, sourceD] of sourceDashboards) {
        if (!targetDashboards.has(name)) {
          comparison.missing_in_target.dashboards.push(name);
        } else {
          const targetD = targetDashboards.get(name);
          if (sourceD.cards?.length !== targetD.cards?.length) {
            comparison.different.dashboards.push({
              name: name,
              reason: `Different card count (source: ${sourceD.cards?.length}, target: ${targetD.cards?.length})`,
              source_id: sourceD.id,
              target_id: targetD.id
            });
          } else {
            comparison.identical.dashboards++;
          }
        }
      }

      for (const name of targetDashboards.keys()) {
        if (!sourceDashboards.has(name)) {
          comparison.missing_in_source.dashboards.push(name);
        }
      }

      comparison.summary = {
        total_differences:
          comparison.missing_in_target.questions.length +
          comparison.missing_in_target.dashboards.length +
          comparison.missing_in_source.questions.length +
          comparison.missing_in_source.dashboards.length +
          comparison.different.questions.length +
          comparison.different.dashboards.length,
        drift_detected: false
      };

      comparison.summary.drift_detected = comparison.summary.total_differences > 0;

      return comparison;
    } catch (error) {
      logger.error('Environment comparison failed:', error);
      throw error;
    }
  }

  /**
   * Auto cleanup unused content with safety checks
   * DRY-RUN by default
   *
   * @param {Object} options - Cleanup options
   * @returns {Object} Cleanup results or preview
   */
  async autoCleanup(options = {}) {
    const {
      dry_run = true, // SAFETY: Default to dry-run
      approved = false, // SAFETY: Requires explicit approval
      unused_days = 180, // Content not used in 180 days
      orphaned_cards = true, // Clean orphaned cards
      empty_collections = true, // Clean empty collections
      broken_questions = true, // Questions with 100% error rate
      backup_recommended = true
    } = options;

    const cleanup = {
      dry_run: dry_run,
      approved: approved,
      items_to_cleanup: {
        unused_questions: [],
        orphaned_cards: [],
        empty_collections: [],
        broken_questions: []
      },
      safety_checks: {
        backup_required: backup_recommended,
        approval_required: !approved,
        warnings: []
      },
      summary: {
        total_items: 0,
        estimated_space_saved: 0
      }
    };

    try {
      // Find unused questions
      const unusedQuestions = await this.executeQuery(`
        SELECT
          rc.id, rc.name, rc.collection_id,
          MAX(qe.started_at) as last_used,
          EXTRACT(DAY FROM (NOW() - MAX(qe.started_at))) as days_unused
        FROM report_card rc
        LEFT JOIN query_execution qe ON rc.id = qe.card_id
        WHERE rc.archived = false
        GROUP BY rc.id, rc.name, rc.collection_id
        HAVING MAX(qe.started_at) < NOW() - INTERVAL '${unused_days} days'
          OR MAX(qe.started_at) IS NULL
        ORDER BY days_unused DESC
        LIMIT 100
      `);

      cleanup.items_to_cleanup.unused_questions = unusedQuestions.map(q => ({
        id: q.id,
        name: q.name,
        last_used: q.last_used,
        days_unused: q.days_unused,
        action: 'ARCHIVE'
      }));

      // Find orphaned cards
      if (orphaned_cards) {
        const orphaned = await this.executeQuery(`
          SELECT rc.id, rc.name
          FROM report_card rc
          LEFT JOIN report_dashboardcard rdc ON rc.id = rdc.card_id
          WHERE rc.archived = false
            AND rdc.id IS NULL
            AND NOT EXISTS (
              SELECT 1 FROM query_execution qe
              WHERE qe.card_id = rc.id
              AND qe.started_at > NOW() - INTERVAL '90 days'
            )
          LIMIT 50
        `);

        cleanup.items_to_cleanup.orphaned_cards = orphaned.map(c => ({
          id: c.id,
          name: c.name,
          action: 'ARCHIVE'
        }));
      }

      // Find empty collections
      if (empty_collections) {
        const empty = await this.executeQuery(`
          SELECT c.id, c.name
          FROM collection c
          WHERE c.archived = false
            AND c.personal_owner_id IS NULL
            AND NOT EXISTS (
              SELECT 1 FROM report_card rc WHERE rc.collection_id = c.id AND rc.archived = false
            )
            AND NOT EXISTS (
              SELECT 1 FROM report_dashboard rd WHERE rd.collection_id = c.id AND rd.archived = false
            )
          LIMIT 20
        `);

        cleanup.items_to_cleanup.empty_collections = empty.map(c => ({
          id: c.id,
          name: c.name,
          action: 'DELETE'
        }));
      }

      // Find broken questions (100% error rate)
      if (broken_questions) {
        const broken = await this.executeQuery(`
          SELECT
            rc.id, rc.name,
            COUNT(qe.id) as total_executions,
            SUM(CASE WHEN qe.error IS NOT NULL THEN 1 ELSE 0 END) as error_count
          FROM report_card rc
          JOIN query_execution qe ON rc.id = qe.card_id
          WHERE qe.started_at > NOW() - INTERVAL '30 days'
            AND rc.archived = false
          GROUP BY rc.id, rc.name
          HAVING SUM(CASE WHEN qe.error IS NOT NULL THEN 1 ELSE 0 END) = COUNT(qe.id)
            AND COUNT(qe.id) >= 3
          LIMIT 20
        `);

        cleanup.items_to_cleanup.broken_questions = broken.map(q => ({
          id: q.id,
          name: q.name,
          total_executions: parseInt(q.total_executions),
          error_rate: 100,
          action: 'ARCHIVE'
        }));
      }

      // Calculate summary
      cleanup.summary.total_items =
        cleanup.items_to_cleanup.unused_questions.length +
        cleanup.items_to_cleanup.orphaned_cards.length +
        cleanup.items_to_cleanup.empty_collections.length +
        cleanup.items_to_cleanup.broken_questions.length;

      // Safety checks
      if (cleanup.summary.total_items > 50) {
        cleanup.safety_checks.warnings.push('⚠️ HIGH VOLUME: More than 50 items to cleanup - consider batching');
      }

      if (cleanup.items_to_cleanup.broken_questions.length > 0) {
        cleanup.safety_checks.warnings.push('⚠️ BROKEN QUESTIONS: Review before archiving - may need fixes');
      }

      if (!dry_run && !approved) {
        cleanup.safety_checks.warnings.push('🚫 BLOCKED: Set approved: true to execute cleanup');
        cleanup.blocked = true;
      }

      if (backup_recommended) {
        cleanup.safety_checks.warnings.push('💾 BACKUP RECOMMENDED: Export workspace before cleanup');
      }

      // Safety recommendations
      cleanup.recommendations = [
        '1. Run export_workspace first to create backup',
        '2. Review items_to_cleanup list carefully',
        '3. Test cleanup with small batch first (dry_run: false, limit items)',
        '4. Set approved: true only after verification',
        '5. Monitor for 24-48 hours after cleanup'
      ];

      return cleanup;
    } catch (error) {
      logger.error('Auto cleanup failed:', error);
      throw error;
    }
  }
}
