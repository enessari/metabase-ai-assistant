import { logger } from '../utils/logger.js';

export class ConnectionManager {
  constructor() {
    this.connections = new Map();
    this.fallbackMode = true; // Use Metabase as fallback
  }

  async getConnection(metabaseClient, databaseId) {
    const cacheKey = `db_${databaseId}`;
    
    if (this.connections.has(cacheKey)) {
      return this.connections.get(cacheKey);
    }

    try {
      // Önce direct connection dene
      const directConnection = await this.createDirectConnection(metabaseClient, databaseId);
      this.connections.set(cacheKey, directConnection);
      return directConnection;
    } catch (error) {
      logger.warn(`Direct connection failed for DB ${databaseId}:`, error.message);
      
      if (this.fallbackMode) {
        // Fallback: Metabase proxy connection
        const proxyConnection = this.createProxyConnection(metabaseClient, databaseId);
        this.connections.set(cacheKey, proxyConnection);
        return proxyConnection;
      }
      
      throw error;
    }
  }

  async createDirectConnection(metabaseClient, databaseId) {
    const { DirectDatabaseClient } = await import('./direct-client.js');
    
    // Connection info al (şifre sorunu olabilir)
    const connectionInfo = await metabaseClient.getDatabaseConnectionInfo(databaseId);
    
    // Şifre masked ise hata fırlat
    if (connectionInfo.password === '**MetabasePass**' || !connectionInfo.password) {
      throw new Error('Password not available from Metabase API');
    }
    
    const client = new DirectDatabaseClient(connectionInfo, {
      prefix: 'claude_ai_',
      requireApproval: true,
      dryRun: false
    });
    
    await client.connect();
    
    return {
      type: 'direct',
      client: client,
      databaseId: databaseId
    };
  }

  createProxyConnection(metabaseClient, databaseId) {
    return {
      type: 'proxy',
      client: metabaseClient,
      databaseId: databaseId
    };
  }

  async executeOperation(connection, operation, ...args) {
    switch (connection.type) {
      case 'direct':
        return await this.executeDirectOperation(connection, operation, ...args);
      
      case 'proxy':
        return await this.executeProxyOperation(connection, operation, ...args);
      
      default:
        throw new Error(`Unknown connection type: ${connection.type}`);
    }
  }

  async executeDirectOperation(connection, operation, ...args) {
    const client = connection.client;
    
    switch (operation) {
      case 'createTable':
        return await client.createTable(...args);
      
      case 'createView':
        return await client.createView(...args);
      
      case 'createMaterializedView':
        return await client.createMaterializedView(...args);
      
      case 'createIndex':
        return await client.createIndex(...args);
      
      case 'getTableDDL':
        return await client.getTableDDL(...args);
      
      case 'getViewDDL':
        return await client.getViewDDL(...args);
      
      case 'listOwnObjects':
        return await client.listOwnObjects();
      
      case 'executeDDL':
        return await client.executeDDL(...args);
      
      default:
        throw new Error(`Unsupported direct operation: ${operation}`);
    }
  }

  async executeProxyOperation(connection, operation, ...args) {
    const client = connection.client;
    const databaseId = connection.databaseId;
    
    switch (operation) {
      case 'createTable':
        return await this.createTableViaProxy(client, databaseId, ...args);
      
      case 'createView':
        return await this.createViewViaProxy(client, databaseId, ...args);
      
      case 'createMaterializedView':
        return await this.createMaterializedViewViaProxy(client, databaseId, ...args);
      
      case 'createIndex':
        return await this.createIndexViaProxy(client, databaseId, ...args);
      
      case 'getTableDDL':
        return await this.getTableDDLViaProxy(client, databaseId, ...args);
      
      case 'getViewDDL':
        return await this.getViewDDLViaProxy(client, databaseId, ...args);
      
      case 'listOwnObjects':
        return await this.listOwnObjectsViaProxy(client, databaseId);
      
      case 'executeDDL':
        return await this.executeDDLViaProxy(client, databaseId, ...args);
      
      default:
        throw new Error(`Unsupported proxy operation: ${operation}`);
    }
  }

  // Proxy implementations using Metabase SQL
  async createTableViaProxy(client, databaseId, tableName, columns, options = {}) {
    if (!tableName.startsWith('claude_ai_')) {
      tableName = 'claude_ai_' + tableName;
    }

    const columnsSQL = columns.map(col => 
      `${col.name} ${col.type}${col.constraints ? ' ' + col.constraints : ''}`
    ).join(', ');

    const sql = `CREATE TABLE ${tableName} (${columnsSQL})`;
    
    const result = await client.executeNativeQuery(databaseId, sql);
    
    return {
      success: true,
      tableName: tableName,
      sql: sql,
      message: 'Table created via Metabase proxy',
      result: result
    };
  }

  async createViewViaProxy(client, databaseId, viewName, selectSQL, options = {}) {
    if (!viewName.startsWith('claude_ai_')) {
      viewName = 'claude_ai_' + viewName;
    }

    const sql = `CREATE VIEW ${viewName} AS ${selectSQL}`;
    
    const result = await client.executeNativeQuery(databaseId, sql);
    
    return {
      success: true,
      viewName: viewName,
      sql: sql,
      message: 'View created via Metabase proxy',
      result: result
    };
  }

  async createMaterializedViewViaProxy(client, databaseId, viewName, selectSQL, options = {}) {
    if (!viewName.startsWith('claude_ai_')) {
      viewName = 'claude_ai_' + viewName;
    }

    const sql = `CREATE MATERIALIZED VIEW ${viewName} AS ${selectSQL}`;
    
    const result = await client.executeNativeQuery(databaseId, sql);
    
    return {
      success: true,
      viewName: viewName,
      sql: sql,
      message: 'Materialized view created via Metabase proxy',
      result: result
    };
  }

  async createIndexViaProxy(client, databaseId, indexName, tableName, columns, options = {}) {
    if (!indexName.startsWith('claude_ai_')) {
      indexName = 'claude_ai_' + indexName;
    }

    const unique = options.unique ? 'UNIQUE ' : '';
    const columnsStr = Array.isArray(columns) ? columns.join(', ') : columns;
    
    const sql = `CREATE ${unique}INDEX ${indexName} ON ${tableName} (${columnsStr})`;
    
    const result = await client.executeNativeQuery(databaseId, sql);
    
    return {
      success: true,
      indexName: indexName,
      sql: sql,
      message: 'Index created via Metabase proxy',
      result: result
    };
  }

  async getTableDDLViaProxy(client, databaseId, tableName) {
    // PostgreSQL specific query
    const sql = `
      SELECT 
        'CREATE TABLE ' || schemaname || '.' || tablename || ' (' ||
        string_agg(
          column_name || ' ' || data_type ||
          CASE 
            WHEN character_maximum_length IS NOT NULL 
            THEN '(' || character_maximum_length || ')'
            ELSE ''
          END ||
          CASE 
            WHEN is_nullable = 'NO' THEN ' NOT NULL'
            ELSE ''
          END,
          ', '
        ) || ');' as ddl
      FROM information_schema.columns 
      WHERE table_name = '${tableName}'
      GROUP BY schemaname, tablename
    `;
    
    const result = await client.executeNativeQuery(databaseId, sql);
    return result.data.rows[0]?.[0] || null;
  }

  async getViewDDLViaProxy(client, databaseId, viewName) {
    const sql = `
      SELECT 'CREATE VIEW ' || schemaname || '.' || viewname || ' AS ' || definition as ddl
      FROM pg_views 
      WHERE viewname = '${viewName}'
    `;
    
    const result = await client.executeNativeQuery(databaseId, sql);
    return result.data.rows[0]?.[0] || null;
  }

  async listOwnObjectsViaProxy(client, databaseId) {
    const objects = {
      tables: [],
      views: [],
      materialized_views: [],
      indexes: []
    };

    // Tables
    const tablesSQL = `
      SELECT table_name, table_schema 
      FROM information_schema.tables 
      WHERE table_name LIKE 'claude_ai_%'
    `;
    const tablesResult = await client.executeNativeQuery(databaseId, tablesSQL);
    objects.tables = tablesResult.data.rows.map(row => ({ table_name: row[0], table_schema: row[1] }));

    // Views  
    const viewsSQL = `
      SELECT table_name as view_name, table_schema 
      FROM information_schema.views 
      WHERE table_name LIKE 'claude_ai_%'
    `;
    const viewsResult = await client.executeNativeQuery(databaseId, viewsSQL);
    objects.views = viewsResult.data.rows.map(row => ({ view_name: row[0], table_schema: row[1] }));

    // Materialized Views
    const matViewsSQL = `
      SELECT matviewname, schemaname 
      FROM pg_matviews 
      WHERE matviewname LIKE 'claude_ai_%'
    `;
    try {
      const matViewsResult = await client.executeNativeQuery(databaseId, matViewsSQL);
      objects.materialized_views = matViewsResult.data.rows.map(row => ({ matviewname: row[0], schemaname: row[1] }));
    } catch (error) {
      // pg_matviews may not be accessible
      logger.warn('Could not query materialized views:', error.message);
    }

    // Indexes
    const indexesSQL = `
      SELECT indexname, schemaname, tablename 
      FROM pg_indexes 
      WHERE indexname LIKE 'claude_ai_%'
    `;
    try {
      const indexesResult = await client.executeNativeQuery(databaseId, indexesSQL);
      objects.indexes = indexesResult.data.rows.map(row => ({ indexname: row[0], schemaname: row[1], tablename: row[2] }));
    } catch (error) {
      // pg_indexes may not be accessible
      logger.warn('Could not query indexes:', error.message);
    }

    return objects;
  }

  async executeDDLViaProxy(client, databaseId, sql, options = {}) {
    const result = await client.executeNativeQuery(databaseId, sql);
    
    return {
      success: true,
      sql: sql,
      message: 'DDL executed via Metabase proxy',
      result: result
    };
  }

  async disconnect(connection) {
    if (connection.type === 'direct' && connection.client) {
      await connection.client.disconnect();
    }
    
    // Remove from cache
    this.connections.delete(`db_${connection.databaseId}`);
  }

  async disconnectAll() {
    for (const [key, connection] of this.connections) {
      await this.disconnect(connection);
    }
    this.connections.clear();
  }
}