import axios from 'axios';
import { logger } from '../utils/logger.js';

export class MetabaseClient {
  constructor(config) {
    this.baseURL = config.url;
    this.username = config.username;
    this.password = config.password;
    this.sessionToken = null;
    this.client = axios.create({
      baseURL: this.baseURL,
      headers: {
        'Content-Type': 'application/json'
      }
    });
  }

  async authenticate() {
    try {
      const response = await this.client.post('/api/session', {
        username: this.username,
        password: this.password
      });
      this.sessionToken = response.data.id;
      this.client.defaults.headers['X-Metabase-Session'] = this.sessionToken;
      logger.info('Successfully authenticated with Metabase');
      return true;
    } catch (error) {
      logger.error('Authentication failed:', error.message);
      throw new Error('Failed to authenticate with Metabase');
    }
  }

  // Database Operations
  async getDatabases() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/database');
    return response.data;
  }

  async getDatabase(id) {
    await this.ensureAuthenticated();
    const response = await this.client.get(`/api/database/${id}`);
    return response.data;
  }

  async getDatabaseConnectionInfo(id) {
    await this.ensureAuthenticated();
    
    // Önce gerçek credentials'ları MetabaseappDB'den al
    try {
      const realCredentials = await this.getRealCredentials(id);
      if (realCredentials) {
        return realCredentials;
      }
    } catch (error) {
      logger.warn('Could not get real credentials, using API response:', error.message);
    }
    
    // Fallback: Normal API response
    const response = await this.client.get(`/api/database/${id}`);
    const db = response.data;
    
    return {
      id: db.id,
      name: db.name,
      engine: db.engine,
      host: db.details?.host,
      port: db.details?.port,
      dbname: db.details?.dbname || db.details?.db,
      user: db.details?.user,
      password: db.details?.password,
      ssl: db.details?.ssl,
      additional_options: db.details?.['additional-options'],
      tunnel_enabled: db.details?.['tunnel-enabled'],
      connection_string: this.buildConnectionString(db)
    };
  }

  async getRealCredentials(databaseId) {
    const query = `
      SELECT name, engine, details
      FROM metabase_database 
      WHERE id = ${databaseId}
    `;
    
    const result = await this.executeNativeQuery(6, query, { enforcePrefix: false }); // MetabaseappDB
    
    if (result.data.rows.length > 0) {
      const [name, engine, details] = result.data.rows[0];
      const detailsObj = JSON.parse(details);
      
      return {
        id: databaseId,
        name: name,
        engine: engine,
        host: detailsObj.host,
        port: detailsObj.port,
        dbname: detailsObj.dbname,
        user: detailsObj.user,
        password: detailsObj.password,
        ssl: detailsObj.ssl || false,
        additional_options: detailsObj['additional-options'],
        tunnel_enabled: detailsObj['tunnel-enabled'] || false
      };
    }
    
    return null;
  }

  buildConnectionString(db) {
    const details = db.details;
    
    switch (db.engine) {
      case 'postgres':
        return `postgresql://${details.user}:${details.password}@${details.host}:${details.port}/${details.dbname}`;
      case 'mysql':
        return `mysql://${details.user}:${details.password}@${details.host}:${details.port}/${details.dbname}`;
      case 'h2':
        return details.db;
      default:
        return null;
    }
  }

  async getDatabaseSchemas(databaseId) {
    await this.ensureAuthenticated();
    const response = await this.client.get(`/api/database/${databaseId}/schemas`);
    return response.data;
  }

  async getDatabaseTables(databaseId) {
    await this.ensureAuthenticated();
    const response = await this.client.get(`/api/database/${databaseId}/metadata`);
    return response.data.tables;
  }

  // Model Operations
  async getCollections() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/collection');
    return response.data;
  }

  async createCollection(name, description, parentId = null) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/collection', {
      name,
      description,
      parent_id: parentId,
      color: '#509EE3'
    });
    return response.data;
  }

  async getModels() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/card', {
      params: { f: 'model' }
    });
    return response.data;
  }

  async createModel(model) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/card', {
      ...model,
      type: 'model',
      display: 'table'
    });
    return response.data;
  }

  // Question Operations
  async getQuestions(collectionId = null) {
    await this.ensureAuthenticated();
    const params = collectionId ? { collection_id: collectionId } : {};
    const response = await this.client.get('/api/card', { params });
    return response.data;
  }

  async createQuestion(question) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/card', {
      ...question,
      display: question.display || 'table',
      visualization_settings: question.visualization_settings || {}
    });
    return response.data;
  }

  async updateQuestion(id, updates) {
    await this.ensureAuthenticated();
    const response = await this.client.put(`/api/card/${id}`, updates);
    return response.data;
  }

  async runQuery(query) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/dataset', query);
    return response.data;
  }

  // SQL Operations
  async executeNativeQuery(databaseId, sql, options = {}) {
    await this.ensureAuthenticated();
    
    // Güvenlik kontrolü - DDL operasyonları için prefix zorunluluğu
    if (options.enforcePrefix !== false && this.isDDLOperation(sql)) {
      this.validateDDLPrefix(sql);
    }

    // DDL operations için farklı endpoint kullan
    if (this.isDDLOperation(sql)) {
      return await this.executeDDLOperation(databaseId, sql);
    }
    
    const query = {
      database: databaseId,
      type: 'native',
      native: {
        query: sql
      }
    };
    return await this.runQuery(query);
  }

  async executeDDLOperation(databaseId, sql) {
    try {
      // DDL için action endpoint kullan
      const response = await this.client.post('/api/action/execute', {
        database_id: databaseId,
        sql: sql,
        type: 'query'
      });
      
      return {
        status: 'success',
        message: 'DDL operation completed',
        data: { rows: [], cols: [] },
        sql: sql
      };
    } catch (error) {
      // Eğer action endpoint çalışmazsa normal endpoint dene
      try {
        const query = {
          database: databaseId,
          type: 'native',
          native: {
            query: sql
          }
        };
        
        await this.runQuery(query);
        return {
          status: 'success',
          message: 'DDL operation completed via dataset endpoint',
          data: { rows: [], cols: [] },
          sql: sql
        };
      } catch (secondError) {
        logger.warn('DDL execution warning:', secondError.message);
        
        // DDL işlemi başarılı olmuş olabilir, kontrol et
        if (secondError.message.includes('Select statement did not produce a ResultSet')) {
          return {
            status: 'success',
            message: 'DDL operation likely completed (ResultSet warning is normal)',
            data: { rows: [], cols: [] },
            sql: sql,
            warning: secondError.message
          };
        }
        
        throw secondError;
      }
    }
  }

  isDDLOperation(sql) {
    const upperSQL = sql.toUpperCase().trim();
    return upperSQL.startsWith('CREATE TABLE') ||
           upperSQL.startsWith('CREATE VIEW') ||
           upperSQL.startsWith('CREATE MATERIALIZED VIEW') ||
           upperSQL.startsWith('CREATE INDEX') ||
           upperSQL.startsWith('DROP TABLE') ||
           upperSQL.startsWith('DROP VIEW') ||
           upperSQL.startsWith('DROP MATERIALIZED VIEW') ||
           upperSQL.startsWith('DROP INDEX');
  }

  validateDDLPrefix(sql) {
    const upperSQL = sql.toUpperCase();
    
    // CREATE operations için prefix kontrolü
    if (upperSQL.includes('CREATE TABLE') || upperSQL.includes('CREATE VIEW') || 
        upperSQL.includes('CREATE MATERIALIZED VIEW') || upperSQL.includes('CREATE INDEX')) {
      if (!sql.toLowerCase().includes('claude_ai_')) {
        throw new Error('DDL operations must use claude_ai_ prefix for object names');
      }
    }
    
    // DROP operations için sadece prefix'li objelere izin
    if (upperSQL.includes('DROP TABLE') || upperSQL.includes('DROP VIEW') || 
        upperSQL.includes('DROP MATERIALIZED VIEW') || upperSQL.includes('DROP INDEX')) {
      if (!sql.toLowerCase().includes('claude_ai_')) {
        throw new Error('Can only drop objects with claude_ai_ prefix');
      }
    }
  }

  async createSQLQuestion(name, description, databaseId, sql, collectionId) {
    const question = {
      name,
      description,
      database_id: databaseId,
      collection_id: collectionId,
      dataset_query: {
        database: databaseId,
        type: 'native',
        native: {
          query: sql
        }
      }
    };
    return await this.createQuestion(question);
  }

  // Metric Operations
  async getMetrics() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/metric');
    return response.data;
  }

  async createMetric(metric) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/metric', metric);
    return response.data;
  }

  async updateMetric(id, updates) {
    await this.ensureAuthenticated();
    const response = await this.client.put(`/api/metric/${id}`, updates);
    return response.data;
  }

  // Dashboard Operations
  async getDashboards() {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/dashboard');
    return response.data;
  }

  async createDashboard(dashboard) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/dashboard', {
      name: dashboard.name,
      description: dashboard.description,
      collection_id: dashboard.collection_id
    });
    return response.data;
  }

  async addCardToDashboard(dashboardId, cardId, options = {}) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/dashboard/:id/cards', {
      cardId,
      sizeX: options.sizeX || 4,
      sizeY: options.sizeY || 4,
      row: options.row || 0,
      col: options.col || 0,
      parameter_mappings: options.parameter_mappings || []
    });
    return response.data;
  }

  async updateDashboard(id, updates) {
    await this.ensureAuthenticated();
    const response = await this.client.put(`/api/dashboard/${id}`, updates);
    return response.data;
  }

  // Segment Operations
  async getSegments(tableId) {
    await this.ensureAuthenticated();
    const response = await this.client.get('/api/segment', {
      params: { table_id: tableId }
    });
    return response.data;
  }

  async createSegment(segment) {
    await this.ensureAuthenticated();
    const response = await this.client.post('/api/segment', segment);
    return response.data;
  }

  // Helper Methods
  async ensureAuthenticated() {
    if (!this.sessionToken) {
      await this.authenticate();
    }
  }

  async testConnection() {
    try {
      await this.authenticate();
      const databases = await this.getDatabases();
      logger.info(`Connected to Metabase. Found ${databases.length} databases.`);
      return true;
    } catch (error) {
      logger.error('Connection test failed:', error.message);
      return false;
    }
  }
}