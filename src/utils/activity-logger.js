import { logger } from './logger.js';

export class ActivityLogger {
  constructor(metabaseClient, options = {}) {
    this.metabaseClient = metabaseClient;
    this.options = {
      logTableName: options.logTableName || 'claude_ai_activity_log',
      schema: options.schema || 'public',
      enableMetrics: options.enableMetrics !== false,
      retentionDays: options.retentionDays || 90,
      batchSize: options.batchSize || 100,
      autoFlush: options.autoFlush !== false,
      flushInterval: options.flushInterval || 30000 // 30 seconds
    };
    
    this.pendingLogs = [];
    this.sessionId = this.generateSessionId();
    this.initialized = false;
    
    if (this.options.autoFlush) {
      this.startAutoFlush();
    }
  }

  generateSessionId() {
    return `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  async initialize(databaseId) {
    try {
      this.databaseId = databaseId;
      await this.ensureLogTableExists();
      this.initialized = true;
      logger.info('ActivityLogger initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize ActivityLogger:', error);
      throw error;
    }
  }

  async ensureLogTableExists() {
    const createTableSQL = `
      CREATE TABLE IF NOT EXISTS ${this.options.schema}.${this.options.logTableName} (
        id SERIAL PRIMARY KEY,
        session_id VARCHAR(100) NOT NULL,
        timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        operation_type VARCHAR(50) NOT NULL,
        operation_category VARCHAR(50) NOT NULL,
        database_id INTEGER,
        database_name VARCHAR(200),
        target_object_type VARCHAR(50),
        target_object_id INTEGER,
        target_object_name VARCHAR(500),
        source_sql TEXT,
        execution_time_ms INTEGER,
        rows_affected INTEGER,
        rows_returned INTEGER,
        status VARCHAR(20) NOT NULL,
        error_message TEXT,
        user_context JSONB,
        metadata JSONB,
        ai_generated BOOLEAN DEFAULT TRUE,
        created_date DATE GENERATED ALWAYS AS (timestamp::date) STORED
      );

      -- Create indexes for better query performance
      CREATE INDEX IF NOT EXISTS idx_${this.options.logTableName}_timestamp 
        ON ${this.options.schema}.${this.options.logTableName}(timestamp DESC);
      
      CREATE INDEX IF NOT EXISTS idx_${this.options.logTableName}_session 
        ON ${this.options.schema}.${this.options.logTableName}(session_id);
      
      CREATE INDEX IF NOT EXISTS idx_${this.options.logTableName}_operation 
        ON ${this.options.schema}.${this.options.logTableName}(operation_type, operation_category);
      
      CREATE INDEX IF NOT EXISTS idx_${this.options.logTableName}_database 
        ON ${this.options.schema}.${this.options.logTableName}(database_id);
        
      CREATE INDEX IF NOT EXISTS idx_${this.options.logTableName}_date 
        ON ${this.options.schema}.${this.options.logTableName}(created_date);
    `;

    try {
      await this.metabaseClient.executeNativeQuery(this.databaseId, createTableSQL);
      logger.info(`Activity log table ${this.options.logTableName} initialized`);
    } catch (error) {
      // Table might already exist, try to continue
      logger.warn('Log table creation failed, table might already exist:', error.message);
    }
  }

  async logActivity(activity) {
    if (!this.initialized) {
      logger.warn('ActivityLogger not initialized, skipping log');
      return;
    }

    const logEntry = {
      session_id: this.sessionId,
      timestamp: new Date().toISOString(),
      operation_type: activity.operation_type,
      operation_category: activity.operation_category,
      database_id: activity.database_id || this.databaseId,
      database_name: activity.database_name,
      target_object_type: activity.target_object_type,
      target_object_id: activity.target_object_id,
      target_object_name: activity.target_object_name,
      source_sql: activity.source_sql,
      execution_time_ms: activity.execution_time_ms,
      rows_affected: activity.rows_affected,
      rows_returned: activity.rows_returned,
      status: activity.status || 'success',
      error_message: activity.error_message,
      user_context: JSON.stringify(activity.user_context || {}),
      metadata: JSON.stringify(activity.metadata || {}),
      ai_generated: activity.ai_generated !== false
    };

    this.pendingLogs.push(logEntry);

    // Auto-flush if batch size reached
    if (this.pendingLogs.length >= this.options.batchSize) {
      await this.flushLogs();
    }

    return logEntry;
  }

  async flushLogs() {
    if (this.pendingLogs.length === 0) return;

    const logsToFlush = [...this.pendingLogs];
    this.pendingLogs = [];

    try {
      await this.insertLogsBatch(logsToFlush);
      logger.debug(`Flushed ${logsToFlush.length} activity logs`);
    } catch (error) {
      logger.error('Failed to flush activity logs:', error);
      // Put logs back for retry
      this.pendingLogs = [...logsToFlush, ...this.pendingLogs];
    }
  }

  async insertLogsBatch(logs) {
    if (logs.length === 0) return;

    const columns = [
      'session_id', 'timestamp', 'operation_type', 'operation_category',
      'database_id', 'database_name', 'target_object_type', 'target_object_id',
      'target_object_name', 'source_sql', 'execution_time_ms', 'rows_affected',
      'rows_returned', 'status', 'error_message', 'user_context', 'metadata', 'ai_generated'
    ];

    const values = logs.map(log => 
      columns.map(col => {
        const value = log[col];
        if (value === null || value === undefined) return 'NULL';
        if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
        if (typeof value === 'boolean') return value;
        return value;
      }).join(', ')
    ).join('), (');

    const insertSQL = `
      INSERT INTO ${this.options.schema}.${this.options.logTableName} 
      (${columns.join(', ')})
      VALUES (${values})
    `;

    await this.metabaseClient.executeNativeQuery(this.databaseId, insertSQL);
  }

  startAutoFlush() {
    this.flushInterval = setInterval(async () => {
      await this.flushLogs();
    }, this.options.flushInterval);
  }

  async shutdown() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
    }
    
    await this.flushLogs();
    logger.info('ActivityLogger shutdown complete');
  }

  // Predefined activity loggers for common operations
  async logSQLExecution(sql, databaseId, result, executionTime) {
    return await this.logActivity({
      operation_type: 'sql_execute',
      operation_category: 'query',
      database_id: databaseId,
      source_sql: sql,
      execution_time_ms: executionTime,
      rows_returned: result?.data?.rows?.length || 0,
      metadata: {
        columns: result?.data?.cols?.map(c => c.name) || [],
        sql_type: this.detectSQLType(sql)
      }
    });
  }

  async logTableCreation(tableName, databaseId, sql, executionTime, error = null) {
    return await this.logActivity({
      operation_type: 'table_create',
      operation_category: 'ddl',
      database_id: databaseId,
      target_object_type: 'table',
      target_object_name: tableName,
      source_sql: sql,
      execution_time_ms: executionTime,
      status: error ? 'error' : 'success',
      error_message: error?.message,
      metadata: {
        operation_detail: 'CREATE TABLE'
      }
    });
  }

  async logViewCreation(viewName, databaseId, sql, executionTime, error = null) {
    return await this.logActivity({
      operation_type: 'view_create',
      operation_category: 'ddl',
      database_id: databaseId,
      target_object_type: 'view',
      target_object_name: viewName,
      source_sql: sql,
      execution_time_ms: executionTime,
      status: error ? 'error' : 'success',
      error_message: error?.message,
      metadata: {
        operation_detail: 'CREATE VIEW'
      }
    });
  }

  async logMaterializedViewCreation(viewName, databaseId, sql, executionTime, error = null) {
    return await this.logActivity({
      operation_type: 'materialized_view_create',
      operation_category: 'ddl',
      database_id: databaseId,
      target_object_type: 'materialized_view',
      target_object_name: viewName,
      source_sql: sql,
      execution_time_ms: executionTime,
      status: error ? 'error' : 'success',
      error_message: error?.message,
      metadata: {
        operation_detail: 'CREATE MATERIALIZED VIEW'
      }
    });
  }

  async logMetabaseQuestionCreation(question, executionTime, error = null) {
    return await this.logActivity({
      operation_type: 'question_create',
      operation_category: 'metabase',
      database_id: question.database_id,
      target_object_type: 'question',
      target_object_id: question.id,
      target_object_name: question.name,
      source_sql: question.dataset_query?.native?.query,
      execution_time_ms: executionTime,
      status: error ? 'error' : 'success',
      error_message: error?.message,
      metadata: {
        collection_id: question.collection_id,
        display_type: question.display,
        visualization_settings: question.visualization_settings
      }
    });
  }

  async logMetabaseDashboardCreation(dashboard, executionTime, error = null) {
    return await this.logActivity({
      operation_type: 'dashboard_create',
      operation_category: 'metabase',
      target_object_type: 'dashboard',
      target_object_id: dashboard.id,
      target_object_name: dashboard.name,
      execution_time_ms: executionTime,
      status: error ? 'error' : 'success',
      error_message: error?.message,
      metadata: {
        collection_id: dashboard.collection_id,
        cards_count: dashboard.ordered_cards?.length || 0,
        parameters: dashboard.parameters
      }
    });
  }

  async logMetricCreation(metric, tableId, executionTime, error = null) {
    return await this.logActivity({
      operation_type: 'metric_create',
      operation_category: 'metabase',
      target_object_type: 'metric',
      target_object_id: metric.id,
      target_object_name: metric.name,
      execution_time_ms: executionTime,
      status: error ? 'error' : 'success',
      error_message: error?.message,
      metadata: {
        table_id: tableId,
        definition: metric.definition
      }
    });
  }

  detectSQLType(sql) {
    const upperSQL = sql.trim().toUpperCase();
    
    if (upperSQL.startsWith('SELECT')) return 'SELECT';
    if (upperSQL.startsWith('INSERT')) return 'INSERT';
    if (upperSQL.startsWith('UPDATE')) return 'UPDATE';
    if (upperSQL.startsWith('DELETE')) return 'DELETE';
    if (upperSQL.startsWith('CREATE TABLE')) return 'CREATE_TABLE';
    if (upperSQL.startsWith('CREATE VIEW')) return 'CREATE_VIEW';
    if (upperSQL.startsWith('CREATE MATERIALIZED VIEW')) return 'CREATE_MATERIALIZED_VIEW';
    if (upperSQL.startsWith('CREATE INDEX')) return 'CREATE_INDEX';
    if (upperSQL.startsWith('DROP')) return 'DROP';
    if (upperSQL.startsWith('ALTER')) return 'ALTER';
    
    return 'OTHER';
  }

  // Analytics and insights methods
  async getSessionSummary(sessionId = null) {
    const targetSession = sessionId || this.sessionId;
    
    const summarySQL = `
      SELECT 
        session_id,
        MIN(timestamp) as session_start,
        MAX(timestamp) as session_end,
        COUNT(*) as total_operations,
        COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_operations,
        COUNT(CASE WHEN status = 'error' THEN 1 END) as failed_operations,
        COUNT(DISTINCT database_id) as databases_used,
        COUNT(DISTINCT operation_type) as operation_types,
        SUM(execution_time_ms) as total_execution_time,
        AVG(execution_time_ms) as avg_execution_time,
        SUM(COALESCE(rows_returned, 0)) as total_rows_returned,
        SUM(COALESCE(rows_affected, 0)) as total_rows_affected,
        COUNT(CASE WHEN operation_category = 'ddl' THEN 1 END) as ddl_operations,
        COUNT(CASE WHEN operation_category = 'query' THEN 1 END) as query_operations,
        COUNT(CASE WHEN operation_category = 'metabase' THEN 1 END) as metabase_operations
      FROM ${this.options.schema}.${this.options.logTableName}
      WHERE session_id = '${targetSession}'
      GROUP BY session_id
    `;

    const result = await this.metabaseClient.executeNativeQuery(this.databaseId, summarySQL);
    return result.data.rows[0] || null;
  }

  async getOperationStats(days = 7) {
    const statsSQL = `
      SELECT 
        operation_type,
        operation_category,
        COUNT(*) as operation_count,
        COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
        COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count,
        ROUND(AVG(execution_time_ms), 2) as avg_execution_time,
        SUM(COALESCE(rows_returned, 0)) as total_rows_returned,
        SUM(COALESCE(rows_affected, 0)) as total_rows_affected,
        COUNT(DISTINCT session_id) as unique_sessions,
        MAX(timestamp) as last_execution
      FROM ${this.options.schema}.${this.options.logTableName}
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
      GROUP BY operation_type, operation_category
      ORDER BY operation_count DESC
    `;

    const result = await this.metabaseClient.executeNativeQuery(this.databaseId, statsSQL);
    return result.data.rows;
  }

  async getDatabaseUsageStats(days = 30) {
    const usageSQL = `
      SELECT 
        database_id,
        database_name,
        COUNT(*) as total_operations,
        COUNT(DISTINCT session_id) as unique_sessions,
        COUNT(CASE WHEN operation_category = 'ddl' THEN 1 END) as ddl_operations,
        COUNT(CASE WHEN operation_category = 'query' THEN 1 END) as query_operations,
        COUNT(CASE WHEN operation_category = 'metabase' THEN 1 END) as metabase_operations,
        AVG(execution_time_ms) as avg_execution_time,
        COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count,
        MIN(timestamp) as first_used,
        MAX(timestamp) as last_used
      FROM ${this.options.schema}.${this.options.logTableName}
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
        AND database_id IS NOT NULL
      GROUP BY database_id, database_name
      ORDER BY total_operations DESC
    `;

    const result = await this.metabaseClient.executeNativeQuery(this.databaseId, usageSQL);
    return result.data.rows;
  }

  async getErrorAnalysis(days = 7) {
    const errorSQL = `
      SELECT 
        operation_type,
        error_message,
        COUNT(*) as error_count,
        COUNT(DISTINCT session_id) as affected_sessions,
        MIN(timestamp) as first_occurrence,
        MAX(timestamp) as last_occurrence,
        AVG(execution_time_ms) as avg_execution_time
      FROM ${this.options.schema}.${this.options.logTableName}
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
        AND status = 'error'
        AND error_message IS NOT NULL
      GROUP BY operation_type, error_message
      ORDER BY error_count DESC
      LIMIT 20
    `;

    const result = await this.metabaseClient.executeNativeQuery(this.databaseId, errorSQL);
    return result.data.rows;
  }

  async getPerformanceInsights(days = 7) {
    const perfSQL = `
      SELECT 
        operation_type,
        COUNT(*) as execution_count,
        MIN(execution_time_ms) as min_time,
        MAX(execution_time_ms) as max_time,
        AVG(execution_time_ms) as avg_time,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY execution_time_ms) as median_time,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time_ms) as p95_time,
        COUNT(CASE WHEN execution_time_ms > 5000 THEN 1 END) as slow_operations,
        AVG(COALESCE(rows_returned, 0)) as avg_rows_returned
      FROM ${this.options.schema}.${this.options.logTableName}
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
        AND execution_time_ms IS NOT NULL
        AND execution_time_ms > 0
      GROUP BY operation_type
      ORDER BY avg_time DESC
    `;

    const result = await this.metabaseClient.executeNativeQuery(this.databaseId, perfSQL);
    return result.data.rows;
  }

  async getActivityTimeline(days = 7, limit = 100) {
    const timelineSQL = `
      SELECT 
        timestamp,
        session_id,
        operation_type,
        operation_category,
        target_object_name,
        status,
        execution_time_ms,
        rows_returned,
        error_message
      FROM ${this.options.schema}.${this.options.logTableName}
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
      ORDER BY timestamp DESC
      LIMIT ${limit}
    `;

    const result = await this.metabaseClient.executeNativeQuery(this.databaseId, timelineSQL);
    return result.data.rows;
  }

  async cleanupOldLogs() {
    const cleanupSQL = `
      DELETE FROM ${this.options.schema}.${this.options.logTableName}
      WHERE timestamp < NOW() - INTERVAL '${this.options.retentionDays} days'
    `;

    const result = await this.metabaseClient.executeNativeQuery(this.databaseId, cleanupSQL);
    const rowsDeleted = result.data.rows_affected || 0;
    
    logger.info(`Cleaned up ${rowsDeleted} old activity log entries`);
    return rowsDeleted;
  }
}