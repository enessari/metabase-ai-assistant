import { Client } from 'pg';
import mysql from 'mysql2/promise';
import { logger } from '../utils/logger.js';

export class DirectDatabaseClient {
  constructor(connectionInfo, options = {}) {
    this.connectionInfo = connectionInfo;
    this.engine = connectionInfo.engine;
    this.client = null;
    
    // Güvenlik ayarları
    this.options = {
      prefix: options.prefix || 'claude_ai_',
      defaultSchema: options.defaultSchema || null, // null = user's default schema
      allowedSchemas: options.allowedSchemas || null, // null = all schemas allowed
      allowedOperations: options.allowedOperations || [
        'CREATE_TABLE',
        'CREATE_VIEW', 
        'CREATE_MATERIALIZED_VIEW',
        'CREATE_INDEX',
        'DROP_TABLE',
        'DROP_VIEW',
        'DROP_MATERIALIZED_VIEW',
        'DROP_INDEX',
        'SELECT',
        'INSERT',
        'UPDATE',
        'DELETE'
      ],
      maxExecutionTime: options.maxExecutionTime || 30000,
      requireApproval: options.requireApproval !== false,
      dryRun: options.dryRun || false
    };
  }

  async connect() {
    try {
      switch (this.engine) {
        case 'postgres':
          this.client = new Client({
            host: this.connectionInfo.host,
            port: this.connectionInfo.port,
            database: this.connectionInfo.dbname,
            user: this.connectionInfo.user,
            password: this.connectionInfo.password,
            ssl: this.connectionInfo.ssl
          });
          await this.client.connect();
          break;

        case 'mysql':
          this.client = await mysql.createConnection({
            host: this.connectionInfo.host,
            port: this.connectionInfo.port,
            database: this.connectionInfo.dbname,
            user: this.connectionInfo.user,
            password: this.connectionInfo.password,
            ssl: this.connectionInfo.ssl
          });
          break;

        default:
          throw new Error(`Unsupported database engine: ${this.engine}`);
      }

      logger.info(`Connected to ${this.engine} database: ${this.connectionInfo.name}`);
      return true;
    } catch (error) {
      logger.error('Database connection failed:', error);
      throw error;
    }
  }

  async disconnect() {
    if (this.client) {
      if (this.engine === 'postgres') {
        await this.client.end();
      } else if (this.engine === 'mysql') {
        await this.client.end();
      }
      this.client = null;
      logger.info('Database connection closed');
    }
  }

  // Güvenli DDL operations
  async executeDDL(sql, options = {}) {
    await this.validateDDL(sql);
    
    if (this.options.requireApproval && !options.approved) {
      throw new Error('DDL operation requires approval. Set approved: true to execute.');
    }

    if (this.options.dryRun) {
      logger.info('DRY RUN - Would execute DDL:', sql);
      return { success: true, dryRun: true, sql };
    }

    try {
      const result = await this.executeQuery(sql);
      logger.info('DDL executed successfully:', sql);
      return result;
    } catch (error) {
      logger.error('DDL execution failed:', error);
      throw error;
    }
  }

  async validateDDL(sql) {
    const upperSQL = sql.toUpperCase().trim();
    
    // 1. Operation type kontrolü
    const operationType = this.extractOperationType(upperSQL);
    if (!this.options.allowedOperations.includes(operationType)) {
      throw new Error(`Operation not allowed: ${operationType}`);
    }

    // 2. Prefix kontrolü
    if (this.requiresPrefix(operationType)) {
      const objectName = this.extractObjectName(sql, operationType);
      if (!objectName.startsWith(this.options.prefix)) {
        throw new Error(`Object name must start with prefix: ${this.options.prefix}`);
      }
    }

    // 3. Dangerous operations kontrolü
    this.checkDangerousOperations(upperSQL);

    // 4. Table/view existence kontrolü for DROP operations
    if (operationType.startsWith('DROP_')) {
      const objectName = this.extractObjectName(sql, operationType);
      await this.validateDropOperation(objectName, operationType);
    }

    return true;
  }

  extractOperationType(sql) {
    if (sql.startsWith('CREATE TABLE')) return 'CREATE_TABLE';
    if (sql.startsWith('CREATE VIEW')) return 'CREATE_VIEW';
    if (sql.startsWith('CREATE MATERIALIZED VIEW')) return 'CREATE_MATERIALIZED_VIEW';
    if (sql.startsWith('CREATE INDEX') || sql.startsWith('CREATE UNIQUE INDEX')) return 'CREATE_INDEX';
    if (sql.startsWith('DROP TABLE')) return 'DROP_TABLE';
    if (sql.startsWith('DROP VIEW')) return 'DROP_VIEW';
    if (sql.startsWith('DROP MATERIALIZED VIEW')) return 'DROP_MATERIALIZED_VIEW';
    if (sql.startsWith('DROP INDEX')) return 'DROP_INDEX';
    if (sql.startsWith('SELECT')) return 'SELECT';
    if (sql.startsWith('INSERT')) return 'INSERT';
    if (sql.startsWith('UPDATE')) return 'UPDATE';
    if (sql.startsWith('DELETE')) return 'DELETE';
    
    throw new Error('Unsupported operation type');
  }

  requiresPrefix(operationType) {
    return [
      'CREATE_TABLE',
      'CREATE_VIEW', 
      'CREATE_MATERIALIZED_VIEW',
      'CREATE_INDEX',
      'DROP_TABLE',
      'DROP_VIEW',
      'DROP_MATERIALIZED_VIEW',
      'DROP_INDEX'
    ].includes(operationType);
  }

  extractObjectName(sql, operationType) {
    // Basit regex ile object name çıkarma
    let pattern;
    
    switch (operationType) {
      case 'CREATE_TABLE':
        pattern = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\.]+)/i;
        break;
      case 'CREATE_VIEW':
        pattern = /CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([\w\.]+)/i;
        break;
      case 'CREATE_MATERIALIZED_VIEW':
        pattern = /CREATE\s+MATERIALIZED\s+VIEW\s+([\w\.]+)/i;
        break;
      case 'CREATE_INDEX':
        pattern = /CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\.]+)/i;
        break;
      case 'DROP_TABLE':
        pattern = /DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([\w\.]+)/i;
        break;
      case 'DROP_VIEW':
        pattern = /DROP\s+VIEW\s+(?:IF\s+EXISTS\s+)?([\w\.]+)/i;
        break;
      case 'DROP_MATERIALIZED_VIEW':
        pattern = /DROP\s+MATERIALIZED\s+VIEW\s+(?:IF\s+EXISTS\s+)?([\w\.]+)/i;
        break;
      case 'DROP_INDEX':
        pattern = /DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?([\w\.]+)/i;
        break;
      default:
        throw new Error('Cannot extract object name for this operation');
    }

    const match = sql.match(pattern);
    if (!match) {
      throw new Error('Could not extract object name from SQL');
    }

    // Schema.table formatından sadece table adını al
    const fullName = match[1];
    const parts = fullName.split('.');
    return parts[parts.length - 1];
  }

  checkDangerousOperations(sql) {
    const dangerous = [
      'DROP DATABASE',
      'DROP SCHEMA',
      'TRUNCATE',
      'ALTER SYSTEM',
      'CREATE USER',
      'DROP USER',
      'GRANT',
      'REVOKE'
    ];

    for (const op of dangerous) {
      if (sql.includes(op)) {
        throw new Error(`Dangerous operation not allowed: ${op}`);
      }
    }
  }

  async validateDropOperation(objectName, operationType) {
    // Sadece kendi prefix'li objeleri silme izni
    if (!objectName.startsWith(this.options.prefix)) {
      throw new Error(`Cannot drop object not created by AI: ${objectName}`);
    }

    // Objenin gerçekten var olduğunu kontrol et
    const exists = await this.checkObjectExists(objectName, operationType);
    if (!exists) {
      logger.warn(`Object ${objectName} does not exist, but continuing with DROP IF EXISTS`);
    }
  }

  async checkObjectExists(objectName, operationType) {
    let checkSQL;
    
    if (this.engine === 'postgres') {
      switch (operationType) {
        case 'DROP_TABLE':
          checkSQL = `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)`;
          break;
        case 'DROP_VIEW':
          checkSQL = `SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = $1)`;
          break;
        case 'DROP_MATERIALIZED_VIEW':
          checkSQL = `SELECT EXISTS (SELECT FROM pg_matviews WHERE matviewname = $1)`;
          break;
        case 'DROP_INDEX':
          checkSQL = `SELECT EXISTS (SELECT FROM pg_indexes WHERE indexname = $1)`;
          break;
        default:
          return true;
      }
      
      const result = await this.client.query(checkSQL, [objectName]);
      return result.rows[0].exists;
    }
    
    return true; // Default olarak var kabul et
  }

  // DDL Helper Methods
  async createTable(tableName, columns, options = {}) {
    if (!tableName.startsWith(this.options.prefix)) {
      tableName = this.options.prefix + tableName;
    }

    // Schema handling
    const schema = options.schema || this.options.defaultSchema;
    const fullTableName = schema ? `${schema}.${tableName}` : tableName;

    const columnsSQL = columns.map(col => 
      `${col.name} ${col.type}${col.constraints ? ' ' + col.constraints : ''}`
    ).join(', ');

    const sql = `CREATE TABLE ${fullTableName} (${columnsSQL})`;
    
    return await this.executeDDL(sql, options);
  }

  async createView(viewName, selectSQL, options = {}) {
    if (!viewName.startsWith(this.options.prefix)) {
      viewName = this.options.prefix + viewName;
    }

    // Schema handling
    const schema = options.schema || this.options.defaultSchema;
    const fullViewName = schema ? `${schema}.${viewName}` : viewName;

    const sql = `CREATE VIEW ${fullViewName} AS ${selectSQL}`;
    
    return await this.executeDDL(sql, options);
  }

  async createMaterializedView(viewName, selectSQL, options = {}) {
    if (!viewName.startsWith(this.options.prefix)) {
      viewName = this.options.prefix + viewName;
    }

    // Schema handling
    const schema = options.schema || this.options.defaultSchema;
    const fullViewName = schema ? `${schema}.${viewName}` : viewName;

    const sql = `CREATE MATERIALIZED VIEW ${fullViewName} AS ${selectSQL}`;
    
    return await this.executeDDL(sql, options);
  }

  async createIndex(indexName, tableName, columns, options = {}) {
    if (!indexName.startsWith(this.options.prefix)) {
      indexName = this.options.prefix + indexName;
    }

    const unique = options.unique ? 'UNIQUE ' : '';
    const columnsStr = Array.isArray(columns) ? columns.join(', ') : columns;
    
    const sql = `CREATE ${unique}INDEX ${indexName} ON ${tableName} (${columnsStr})`;
    
    return await this.executeDDL(sql, options);
  }

  // DDL okuma metodları
  async getTableDDL(tableName) {
    if (this.engine === 'postgres') {
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
        WHERE table_name = $1
        GROUP BY schemaname, tablename
      `;
      
      const result = await this.client.query(sql, [tableName]);
      return result.rows[0]?.ddl;
    }
    
    return null;
  }

  async getViewDDL(viewName) {
    if (this.engine === 'postgres') {
      const sql = `
        SELECT 'CREATE VIEW ' || schemaname || '.' || viewname || ' AS ' || definition as ddl
        FROM pg_views 
        WHERE viewname = $1
      `;
      
      const result = await this.client.query(sql, [viewName]);
      return result.rows[0]?.ddl;
    }
    
    return null;
  }

  async listOwnObjects() {
    const objects = {
      tables: [],
      views: [],
      materialized_views: [],
      indexes: []
    };

    if (this.engine === 'postgres') {
      // Tables with schema info
      const tablesResult = await this.client.query(`
        SELECT table_name, table_schema 
        FROM information_schema.tables 
        WHERE table_name LIKE $1
        ORDER BY table_schema, table_name
      `, [this.options.prefix + '%']);
      objects.tables = tablesResult.rows;

      // Views with schema info
      const viewsResult = await this.client.query(`
        SELECT table_name as view_name, table_schema 
        FROM information_schema.views 
        WHERE table_name LIKE $1
        ORDER BY table_schema, table_name
      `, [this.options.prefix + '%']);
      objects.views = viewsResult.rows;

      // Materialized Views with schema info
      const matViewsResult = await this.client.query(`
        SELECT matviewname, schemaname 
        FROM pg_matviews 
        WHERE matviewname LIKE $1
        ORDER BY schemaname, matviewname
      `, [this.options.prefix + '%']);
      objects.materialized_views = matViewsResult.rows;

      // Indexes with schema info
      const indexesResult = await this.client.query(`
        SELECT indexname, schemaname, tablename 
        FROM pg_indexes 
        WHERE indexname LIKE $1
        ORDER BY schemaname, indexname
      `, [this.options.prefix + '%']);
      objects.indexes = indexesResult.rows;
    }

    return objects;
  }

  async getSchemas() {
    if (this.engine === 'postgres') {
      const result = await this.client.query(`
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
      `);
      return result.rows.map(row => row.schema_name);
    }
    return [];
  }

  async getCurrentSchema() {
    if (this.engine === 'postgres') {
      const result = await this.client.query('SELECT current_schema()');
      return result.rows[0].current_schema;
    }
    return null;
  }

  // Schema ve tablo keşfi metodları
  async exploreSchemaTablesDetailed(schemaName, includeColumns = true, limit = null) {
    if (this.engine === 'postgres') {
      const tableQuery = `
        SELECT 
          t.table_name,
          t.table_type,
          obj_description(c.oid) as table_comment,
          pg_size_pretty(pg_total_relation_size(c.oid)) as table_size
        FROM information_schema.tables t
        LEFT JOIN pg_class c ON c.relname = t.table_name
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE t.table_schema = $1
          AND n.nspname = $1
        ORDER BY t.table_name
        ${limit ? `LIMIT ${limit}` : ''}
      `;
      
      const tables = await this.client.query(tableQuery, [schemaName]);
      const result = [];
      
      for (const table of tables.rows) {
        const tableInfo = {
          name: table.table_name,
          type: table.table_type,
          comment: table.table_comment,
          size: table.table_size,
          columns: []
        };
        
        if (includeColumns) {
          const columnQuery = `
            SELECT 
              c.column_name,
              c.data_type,
              c.is_nullable,
              c.column_default,
              c.character_maximum_length,
              c.numeric_precision,
              c.numeric_scale,
              col_description(pgc.oid, c.ordinal_position) as column_comment,
              CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
              CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key,
              fk.foreign_table_name,
              fk.foreign_column_name
            FROM information_schema.columns c
            LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
            LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema
            LEFT JOIN (
              SELECT ku.column_name, tc.table_name
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage ku 
                ON tc.constraint_name = ku.constraint_name
              WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = $1
            ) pk ON pk.column_name = c.column_name AND pk.table_name = c.table_name
            LEFT JOIN (
              SELECT 
                kcu.column_name,
                kcu.table_name,
                ccu.table_name as foreign_table_name,
                ccu.column_name as foreign_column_name
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
              JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
              WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = $1
            ) fk ON fk.column_name = c.column_name AND fk.table_name = c.table_name
            WHERE c.table_schema = $1 AND c.table_name = $2
            ORDER BY c.ordinal_position
          `;
          
          const columns = await this.client.query(columnQuery, [schemaName, table.table_name]);
          tableInfo.columns = columns.rows.map(col => ({
            name: col.column_name,
            type: col.data_type,
            nullable: col.is_nullable === 'YES',
            default: col.column_default,
            length: col.character_maximum_length,
            precision: col.numeric_precision,
            scale: col.numeric_scale,
            comment: col.column_comment,
            isPrimaryKey: col.is_primary_key,
            isForeignKey: col.is_foreign_key,
            foreignTable: col.foreign_table_name,
            foreignColumn: col.foreign_column_name
          }));
        }
        
        result.push(tableInfo);
      }
      
      return result;
    }
    return [];
  }

  async analyzeTableRelationships(schemaName, tableNames = null) {
    if (this.engine === 'postgres') {
      const tableFilter = tableNames ? 
        `AND tc.table_name = ANY($2)` : '';
      const params = tableNames ? [schemaName, tableNames] : [schemaName];
      
      const query = `
        SELECT DISTINCT
          tc.table_name as source_table,
          kcu.column_name as source_column,
          ccu.table_name as target_table,
          ccu.column_name as target_column,
          tc.constraint_name,
          rc.update_rule,
          rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu 
          ON ccu.constraint_name = tc.constraint_name
        JOIN information_schema.referential_constraints rc 
          ON rc.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = $1
          ${tableFilter}
        ORDER BY tc.table_name, kcu.column_name
      `;
      
      const result = await this.client.query(query, params);
      return result.rows.map(row => ({
        sourceTable: row.source_table,
        sourceColumn: row.source_column,
        targetTable: row.target_table,
        targetColumn: row.target_column,
        constraintName: row.constraint_name,
        updateRule: row.update_rule,
        deleteRule: row.delete_rule,
        relationshipType: 'many-to-one' // FK genelde many-to-one
      }));
    }
    return [];
  }

  async suggestVirtualRelationships(schemaName, confidenceThreshold = 0.7) {
    if (this.engine === 'postgres') {
      // Naming convention bazlı ilişki önerileri
      const query = `
        WITH table_columns AS (
          SELECT 
            t.table_name,
            c.column_name,
            c.data_type,
            CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
          FROM information_schema.tables t
          JOIN information_schema.columns c ON t.table_name = c.table_name
          LEFT JOIN (
            SELECT ku.column_name, tc.table_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku 
              ON tc.constraint_name = ku.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = $1
          ) pk ON pk.column_name = c.column_name AND pk.table_name = c.table_name
          WHERE t.table_schema = $1
            AND t.table_type = 'BASE TABLE'
        ),
        potential_relationships AS (
          SELECT DISTINCT
            tc1.table_name as source_table,
            tc1.column_name as source_column,
            tc2.table_name as target_table,
            tc2.column_name as target_column,
            CASE 
              WHEN tc1.column_name = tc2.column_name THEN 0.9
              WHEN tc1.column_name LIKE '%_id' AND tc2.column_name = 'id' 
                AND tc1.column_name = tc2.table_name || '_id' THEN 0.95
              WHEN tc1.column_name LIKE '%_id' AND tc2.is_primary_key THEN 0.8
              WHEN tc1.column_name LIKE tc2.table_name || '%' THEN 0.7
              ELSE 0.5
            END as confidence
          FROM table_columns tc1
          JOIN table_columns tc2 
            ON tc1.data_type = tc2.data_type
            AND tc1.table_name != tc2.table_name
          WHERE tc2.is_primary_key = true
            AND tc1.column_name != 'id'
        )
        SELECT * FROM potential_relationships
        WHERE confidence >= $2
        ORDER BY confidence DESC, source_table, source_column
      `;
      
      const result = await this.client.query(query, [schemaName, confidenceThreshold]);
      return result.rows.map(row => ({
        sourceTable: row.source_table,
        sourceColumn: row.source_column,
        targetTable: row.target_table,
        targetColumn: row.target_column,
        confidence: parseFloat(row.confidence),
        relationshipType: 'many-to-one',
        reasoning: this.explainRelationshipSuggestion(row)
      }));
    }
    return [];
  }

  explainRelationshipSuggestion(relationship) {
    const { source_column, target_column, target_table, confidence } = relationship;
    
    if (confidence >= 0.95) {
      return `Strong match: ${source_column} follows naming convention for ${target_table}.${target_column}`;
    } else if (confidence >= 0.9) {
      return `Very likely: Column names match exactly`;
    } else if (confidence >= 0.8) {
      return `Likely: ${source_column} appears to reference primary key of ${target_table}`;
    } else if (confidence >= 0.7) {
      return `Possible: Column name suggests relationship with ${target_table}`;
    }
    return `Low confidence: Data types match but naming unclear`;
  }

  // Genel query execution
  async executeQuery(sql) {
    if (!this.client) {
      throw new Error('Database not connected');
    }

    try {
      if (this.engine === 'postgres') {
        return await this.client.query(sql);
      } else if (this.engine === 'mysql') {
        const [rows, fields] = await this.client.execute(sql);
        return { rows, fields };
      }
    } catch (error) {
      logger.error('Query execution failed:', error);
      throw error;
    }
  }
}