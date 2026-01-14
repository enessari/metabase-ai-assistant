import { Anthropic } from '@anthropic-ai/sdk';
import OpenAI from 'openai';
import { logger } from '../utils/logger.js';
import { FileOperations } from '../utils/file-operations.js';

export class MetabaseAIAssistant {
  constructor(config) {
    this.metabaseClient = config.metabaseClient;
    this.aiProvider = config.aiProvider || 'anthropic';
    this.fileOps = new FileOperations(config.fileOptions);
    
    if (this.aiProvider === 'anthropic') {
      this.ai = new Anthropic({
        apiKey: config.anthropicApiKey
      });
    } else {
      this.ai = new OpenAI({
        apiKey: config.openaiApiKey
      });
    }
  }

  async analyzeRequest(userRequest) {
    const prompt = `
    Analyze the following user request for Metabase operations.
    Determine what type of operation is needed and extract relevant parameters.
    
    User Request: "${userRequest}"
    
    Respond with a JSON object containing:
    - operation_type: (model|question|sql|metric|dashboard|segment)
    - action: (create|update|query|analyze)
    - parameters: relevant extracted parameters
    - suggested_approach: brief description of recommended approach
    `;

    const response = await this.getAIResponse(prompt);
    return JSON.parse(response);
  }

  async generateSQL(description, schema) {
    const prompt = `
    Generate SQL query based on the following description:
    "${description}"
    
    Available schema:
    ${JSON.stringify(schema, null, 2)}
    
    Requirements:
    - Use proper SQL syntax
    - Include appropriate JOINs if needed
    - Add meaningful aliases
    - Consider performance optimization
    
    Return only the SQL query without explanation.
    `;

    return await this.getAIResponse(prompt);
  }

  async suggestVisualization(data, questionType) {
    // Analyze data structure first
    const dataAnalysis = this.analyzeDataStructure(data);
    
    const prompt = `
    Based on the following data structure and question type, suggest the best visualization:
    
    Question Type: ${questionType}
    Data Analysis: ${JSON.stringify(dataAnalysis, null, 2)}
    Data Sample: ${JSON.stringify(data.slice(0, 3), null, 2)}
    
    Available visualization types:
    - table: For detailed data viewing
    - bar: For categorical comparisons  
    - line: For trends over time
    - area: For cumulative trends
    - pie: For part-to-whole relationships (max 10 categories)
    - number: For single metrics/KPIs
    - gauge: For metrics with targets
    - scatter: For correlation analysis
    - funnel: For conversion analysis
    - combo: For multiple metrics
    - waterfall: For incremental changes
    - map: For geographical data
    
    Respond with JSON containing:
    - visualization_type: best chart type from above
    - settings: detailed visualization settings object
    - reasoning: brief explanation of why this visualization was chosen
    - alternative_options: array of 2-3 other suitable options
    `;

    const response = await this.getAIResponse(prompt);
    return JSON.parse(response);
  }

  analyzeDataStructure(data) {
    if (!data || data.length === 0) {
      return { isEmpty: true };
    }

    const sample = data[0];
    const columns = Object.keys(sample);
    const analysis = {
      columnCount: columns.length,
      rowCount: data.length,
      hasTimeColumn: false,
      hasNumericColumns: false,
      hasCategoricalColumns: false,
      hasGeographicColumns: false,
      numericColumns: [],
      categoricalColumns: [],
      timeColumns: [],
      geographicColumns: []
    };

    // Analyze each column
    columns.forEach(col => {
      const values = data.map(row => row[col]).filter(v => v !== null && v !== undefined);
      
      if (values.length === 0) return;

      // Check for time/date columns
      if (this.isTimeColumn(col, values)) {
        analysis.hasTimeColumn = true;
        analysis.timeColumns.push(col);
      }
      // Check for geographic columns
      else if (this.isGeographicColumn(col, values)) {
        analysis.hasGeographicColumns = true;
        analysis.geographicColumns.push(col);
      }
      // Check for numeric columns
      else if (this.isNumericColumn(values)) {
        analysis.hasNumericColumns = true;
        analysis.numericColumns.push(col);
      }
      // Otherwise categorical
      else {
        analysis.hasCategoricalColumns = true;
        analysis.categoricalColumns.push({
          name: col,
          uniqueValues: [...new Set(values)].length
        });
      }
    });

    return analysis;
  }

  isTimeColumn(columnName, values) {
    const timePattern = /date|time|created|updated|timestamp/i;
    if (timePattern.test(columnName)) return true;
    
    // Check if values look like dates
    const sampleValue = values[0];
    if (typeof sampleValue === 'string') {
      return !isNaN(Date.parse(sampleValue));
    }
    
    return false;
  }

  isGeographicColumn(columnName, values) {
    const geoPattern = /country|state|city|region|location|lat|lng|longitude|latitude/i;
    return geoPattern.test(columnName);
  }

  isNumericColumn(values) {
    const numericValues = values.filter(v => typeof v === 'number' && !isNaN(v));
    return numericValues.length > values.length * 0.8; // 80% numeric
  }

  async createModel(description, databaseId, options = {}) {
    logger.info(`Creating model for: ${description}`);
    
    try {
      // Get comprehensive database schema
      const schemas = await this.metabaseClient.getDatabaseSchemas(databaseId);
      const allTables = [];
      
      for (const schema of schemas) {
        const tables = await this.metabaseClient.getDatabaseTables(databaseId, schema);
        allTables.push(...tables);
      }

      // Enhanced model creation prompt
      const modelPrompt = `
      Create a comprehensive Metabase model based on the description: "${description}"
      
      Available database schema:
      ${JSON.stringify(allTables, null, 2)}
      
      Requirements:
      1. Generate optimized SQL query for the model
      2. Include proper JOINs for related tables
      3. Add meaningful column aliases
      4. Consider indexing and performance
      5. Include data validation where appropriate
      
      Respond with JSON containing:
      - sql: the SQL query
      - model_name: descriptive model name
      - description: detailed model description
      - suggested_fields: array of important fields with display names
      - relationships: suggested relationships with other models
      - semantic_type: semantic types for key fields
      `;

      const modelSpec = JSON.parse(await this.getAIResponse(modelPrompt));
      
      // Create the model with enhanced configuration
      const model = await this.metabaseClient.createModel({
        name: modelSpec.model_name || this.generateName(description, 'Model'),
        description: modelSpec.description || description,
        database_id: databaseId,
        collection_id: options.collection_id,
        dataset_query: {
          database: databaseId,
          type: 'native',
          native: { 
            query: modelSpec.sql,
            template_tags: {}
          }
        },
        display: 'table',
        visualization_settings: {
          'table.pivot_column': null,
          'table.cell_column': null
        }
      });

      // Add semantic types and field descriptions if model creation was successful
      if (model.id && modelSpec.suggested_fields) {
        await this.enhanceModelFields(model.id, modelSpec.suggested_fields);
      }

      logger.info(`Enhanced model created: ${model.id} - ${model.name}`);
      return {
        model,
        suggestions: {
          fields: modelSpec.suggested_fields,
          relationships: modelSpec.relationships
        }
      };
      
    } catch (error) {
      logger.error(`Failed to create model: ${error.message}`);
      throw error;
    }
  }

  async enhanceModelFields(modelId, suggestedFields) {
    try {
      // Get model fields
      const modelFields = await this.metabaseClient.getModelFields(modelId);
      
      // Update field properties based on suggestions
      for (const suggestion of suggestedFields) {
        const field = modelFields.find(f => 
          f.name.toLowerCase() === suggestion.field_name.toLowerCase()
        );
        
        if (field) {
          await this.metabaseClient.updateField(field.id, {
            display_name: suggestion.display_name,
            description: suggestion.description,
            semantic_type: suggestion.semantic_type,
            visibility_type: suggestion.visibility_type || 'normal'
          });
        }
      }
      
      logger.info(`Enhanced ${suggestedFields.length} fields for model ${modelId}`);
    } catch (error) {
      logger.warn(`Failed to enhance model fields: ${error.message}`);
    }
  }

  async createQuestion(description, databaseId, collectionId) {
    logger.info(`Creating question for: ${description}`);
    
    // Get database schema
    const tables = await this.metabaseClient.getDatabaseTables(databaseId);
    
    // Generate SQL
    const sql = await this.generateSQL(description, tables);
    
    // Execute query to get sample data
    const result = await this.metabaseClient.executeNativeQuery(databaseId, sql + ' LIMIT 10');
    
    // Suggest visualization
    const vizSuggestion = await this.suggestVisualization(result.data.rows, description);
    
    // Create question
    const question = await this.metabaseClient.createSQLQuestion(
      this.generateName(description, 'Question'),
      description,
      databaseId,
      sql,
      collectionId
    );
    
    // Update with visualization settings
    if (vizSuggestion.visualization_type !== 'table') {
      await this.metabaseClient.updateQuestion(question.id, {
        display: vizSuggestion.visualization_type,
        visualization_settings: vizSuggestion.settings
      });
    }
    
    logger.info(`Question created: ${question.id}`);
    return question;
  }

  async createMetric(description, tableId, options = {}) {
    logger.info(`Creating metric for: ${description}`);
    
    try {
      // Get table schema for better context
      const tableInfo = await this.metabaseClient.getTable(tableId);
      const fields = await this.metabaseClient.getTableFields(tableId);
      
      const metricPrompt = `
      Create a comprehensive metric definition based on: "${description}"
      
      Available table: ${tableInfo.display_name || tableInfo.name}
      Available fields: ${JSON.stringify(fields.map(f => ({
        name: f.name,
        display_name: f.display_name,
        base_type: f.base_type,
        semantic_type: f.semantic_type
      })), null, 2)}
      
      Create a metric that:
      1. Uses appropriate aggregation function
      2. Includes meaningful filters if needed
      3. Has clear business meaning
      4. Follows metric best practices
      
      Respond with JSON containing:
      - name: clear, business-friendly metric name
      - description: detailed description explaining what it measures
      - aggregation: aggregation definition array
      - filter: filter conditions (if any)
      - field_id: the field ID to aggregate on
      - semantic_type: metric semantic type
      - points_of_interest: key insights this metric provides
      `;

      const metricDef = JSON.parse(await this.getAIResponse(metricPrompt));
      
      // Find the field ID if specified by name
      let fieldId = metricDef.field_id;
      if (!fieldId && metricDef.field_name) {
        const field = fields.find(f => 
          f.name.toLowerCase() === metricDef.field_name.toLowerCase() ||
          f.display_name.toLowerCase() === metricDef.field_name.toLowerCase()
        );
        fieldId = field ? field.id : null;
      }

      // Build metric definition
      const definition = {
        'source-table': tableId
      };

      // Add aggregation
      if (metricDef.aggregation) {
        if (fieldId) {
          definition.aggregation = [metricDef.aggregation[0], ['field', fieldId, null]];
        } else {
          definition.aggregation = metricDef.aggregation;
        }
      }

      // Add filters if specified
      if (metricDef.filter && metricDef.filter.length > 0) {
        definition.filter = metricDef.filter;
      }

      const metric = await this.metabaseClient.createMetric({
        name: metricDef.name,
        description: metricDef.description,
        table_id: tableId,
        definition,
        show_in_getting_started: options.featured || false
      });

      logger.info(`Enhanced metric created: ${metric.id} - ${metric.name}`);
      
      return {
        metric,
        insights: metricDef.points_of_interest || [],
        suggested_visualizations: this.suggestMetricVisualizations(metricDef)
      };
      
    } catch (error) {
      logger.error(`Failed to create metric: ${error.message}`);
      throw error;
    }
  }

  suggestMetricVisualizations(metricDef) {
    const suggestions = [];
    
    // Single number for KPIs
    suggestions.push({
      type: 'number',
      title: `${metricDef.name} - Current Value`,
      description: 'Display current metric value'
    });

    // Line chart for trends
    suggestions.push({
      type: 'line',
      title: `${metricDef.name} - Trend Over Time`,
      description: 'Show how metric changes over time'
    });

    // Gauge for performance metrics
    if (metricDef.semantic_type === 'performance' || 
        metricDef.name.toLowerCase().includes('rate') ||
        metricDef.name.toLowerCase().includes('ratio')) {
      suggestions.push({
        type: 'gauge',
        title: `${metricDef.name} - Performance Gauge`,
        description: 'Show metric with target ranges'
      });
    }

    return suggestions;
  }

  async createDashboard(description, questions = [], options = {}) {
    logger.info(`Creating dashboard: ${description}`);
    
    try {
      // Analyze questions to understand dashboard type and optimal layout
      const dashboardAnalysis = await this.analyzeDashboardRequirements(description, questions);
      
      // Create dashboard with enhanced configuration
      const dashboard = await this.metabaseClient.createDashboard({
        name: dashboardAnalysis.name || this.generateName(description, 'Dashboard'),
        description: dashboardAnalysis.description || description,
        collection_id: options.collection_id,
        parameters: dashboardAnalysis.suggested_filters || []
      });

      if (questions.length > 0) {
        // Generate intelligent layout
        const layout = await this.generateOptimalLayout(questions, dashboardAnalysis);
        
        // Add questions to dashboard with optimized positioning
        for (let i = 0; i < questions.length; i++) {
          const cardLayout = layout[i];
          await this.metabaseClient.addCardToDashboard(
            dashboard.id,
            questions[i].id,
            cardLayout
          );
        }
      }

      // Add recommended filters if any
      if (dashboardAnalysis.recommended_filters) {
        for (const filter of dashboardAnalysis.recommended_filters) {
          await this.metabaseClient.addDashboardFilter(dashboard.id, filter);
        }
      }
      
      logger.info(`Enhanced dashboard created: ${dashboard.id} - ${dashboard.name}`);
      
      return {
        dashboard,
        analysis: dashboardAnalysis,
        layout_suggestions: dashboardAnalysis.layout_tips
      };
      
    } catch (error) {
      logger.error(`Failed to create dashboard: ${error.message}`);
      throw error;
    }
  }

  async analyzeDashboardRequirements(description, questions) {
    const questionTypes = questions.map(q => ({
      id: q.id,
      name: q.name,
      display: q.display || 'table',
      description: q.description
    }));

    const analysisPrompt = `
    Analyze dashboard requirements based on:
    Description: "${description}"
    Questions: ${JSON.stringify(questionTypes, null, 2)}
    
    Determine:
    1. Dashboard type (executive, operational, analytical, marketing, financial)
    2. Target audience (executives, analysts, managers, end-users)
    3. Primary purpose (monitoring, analysis, reporting)
    4. Optimal layout strategy
    5. Recommended filters
    
    Respond with JSON containing:
    - dashboard_type: type classification
    - target_audience: primary users
    - name: improved dashboard name
    - description: enhanced description
    - layout_strategy: (executive-summary|analytical-deep-dive|operational-monitoring|marketing-funnel)
    - recommended_filters: array of useful filters
    - layout_tips: layout optimization suggestions
    `;

    const response = await this.getAIResponse(analysisPrompt);
    return JSON.parse(response);
  }

  async generateOptimalLayout(questions, dashboardAnalysis) {
    const GRID_WIDTH = 12;
    const layout = [];
    let currentRow = 0;
    let currentCol = 0;

    // Layout strategies based on dashboard type
    const strategies = {
      'executive-summary': this.getExecutiveLayout,
      'analytical-deep-dive': this.getAnalyticalLayout,
      'operational-monitoring': this.getOperationalLayout,
      'marketing-funnel': this.getMarketingLayout
    };

    const strategy = strategies[dashboardAnalysis.layout_strategy] || this.getDefaultLayout;
    return strategy.call(this, questions);
  }

  getExecutiveLayout(questions) {
    const layout = [];
    let currentRow = 0;

    for (let i = 0; i < questions.length; i++) {
      const question = questions[i];
      let cardLayout;

      // First row: Key metrics (numbers/gauges)
      if (i < 4 && (question.display === 'number' || question.display === 'gauge')) {
        cardLayout = {
          row: 0,
          col: i * 3,
          sizeX: 3,
          sizeY: 3
        };
      }
      // Second row: Main charts
      else if (i >= 4 && i < 6) {
        cardLayout = {
          row: 4,
          col: (i - 4) * 6,
          sizeX: 6,
          sizeY: 5
        };
      }
      // Additional rows: Supporting charts
      else {
        const row = Math.floor((i - 6) / 2) * 5 + 10;
        const col = ((i - 6) % 2) * 6;
        cardLayout = {
          row: row,
          col: col,
          sizeX: 6,
          sizeY: 4
        };
      }

      layout.push(cardLayout);
    }

    return layout;
  }

  getAnalyticalLayout(questions) {
    const layout = [];
    let currentRow = 0;

    // Analytical layout: Focus on detailed charts
    for (let i = 0; i < questions.length; i++) {
      const question = questions[i];
      
      if (question.display === 'table') {
        // Full width for tables
        layout.push({
          row: currentRow,
          col: 0,
          sizeX: 12,
          sizeY: 6
        });
        currentRow += 7;
      } else {
        // Charts take half width
        const col = (i % 2) * 6;
        const row = currentRow + Math.floor(i / 2) * 5;
        
        layout.push({
          row: row,
          col: col,
          sizeX: 6,
          sizeY: 5
        });
        
        if (i % 2 === 1) currentRow += 6;
      }
    }

    return layout;
  }

  getOperationalLayout(questions) {
    // Operational: Compact, monitoring-focused
    return questions.map((question, i) => {
      const row = Math.floor(i / 3) * 4;
      const col = (i % 3) * 4;
      
      return {
        row: row,
        col: col,
        sizeX: 4,
        sizeY: 4
      };
    });
  }

  getMarketingLayout(questions) {
    const layout = [];
    
    // Marketing: Funnel-like progression
    for (let i = 0; i < questions.length; i++) {
      if (i < 3) {
        // Top metrics
        layout.push({
          row: 0,
          col: i * 4,
          sizeX: 4,
          sizeY: 3
        });
      } else if (i < 5) {
        // Main funnel charts
        layout.push({
          row: 4,
          col: (i - 3) * 6,
          sizeX: 6,
          sizeY: 5
        });
      } else {
        // Supporting analytics
        const row = Math.floor((i - 5) / 2) * 4 + 10;
        const col = ((i - 5) % 2) * 6;
        layout.push({
          row: row,
          col: col,
          sizeX: 6,
          sizeY: 4
        });
      }
    }
    
    return layout;
  }

  getDefaultLayout(questions) {
    // Default responsive layout
    return questions.map((question, i) => {
      const row = Math.floor(i / 2) * 5;
      const col = (i % 2) * 6;
      
      return {
        row: row,
        col: col,
        sizeX: 6,
        sizeY: 4
      };
    });
  }

  async optimizeQuery(sql) {
    const prompt = `
    Optimize the following SQL query for better performance:
    
    ${sql}
    
    Provide:
    1. Optimized query
    2. List of optimizations applied
    3. Expected performance improvements
    
    Return as JSON with: optimized_sql, optimizations[], improvements
    `;

    const response = await this.getAIResponse(prompt);
    return JSON.parse(response);
  }

  async explainQuery(sql) {
    const prompt = `
    Explain the following SQL query in simple terms:
    
    ${sql}
    
    Provide:
    1. What the query does
    2. Tables and relationships used
    3. Any potential issues or improvements
    `;

    return await this.getAIResponse(prompt);
  }

  // Helper methods
  async getAIResponse(prompt) {
    try {
      if (this.aiProvider === 'anthropic') {
        const response = await this.ai.messages.create({
          model: 'claude-3-sonnet-20240229',
          max_tokens: 4000,
          messages: [{ role: 'user', content: prompt }]
        });
        return response.content[0].text;
      } else {
        const response = await this.ai.chat.completions.create({
          model: 'gpt-4-turbo-preview',
          messages: [{ role: 'user', content: prompt }],
          response_format: { type: 'text' }
        });
        return response.choices[0].message.content;
      }
    } catch (error) {
      logger.error('AI response error:', error);
      throw error;
    }
  }

  generateName(description, type) {
    const words = description.split(' ').slice(0, 5).join(' ');
    return `${words} - ${type} (AI Generated)`;
  }

  // File Operations
  async exportDashboard(dashboardId, format = 'json') {
    try {
      const dashboard = await this.metabaseClient.getDashboard(dashboardId);
      const result = await this.fileOps.exportDashboard(dashboard, format);
      
      logger.info(`Dashboard ${dashboardId} exported to ${result.path}`);
      return result;
    } catch (error) {
      logger.error(`Failed to export dashboard ${dashboardId}:`, error);
      throw error;
    }
  }

  async exportQuestion(questionId, format = 'json') {
    try {
      const question = await this.metabaseClient.getQuestion(questionId);
      const result = await this.fileOps.exportQuestion(question, format);
      
      logger.info(`Question ${questionId} exported to ${result.path}`);
      return result;
    } catch (error) {
      logger.error(`Failed to export question ${questionId}:`, error);
      throw error;
    }
  }

  async exportModel(modelId, format = 'json') {
    try {
      const model = await this.metabaseClient.getModel(modelId);
      const result = await this.fileOps.exportModel(model, format);
      
      logger.info(`Model ${modelId} exported to ${result.path}`);
      return result;
    } catch (error) {
      logger.error(`Failed to export model ${modelId}:`, error);
      throw error;
    }
  }

  async exportQueryResults(sql, databaseId, filename, format = 'csv') {
    try {
      const results = await this.metabaseClient.executeNativeQuery(databaseId, sql);
      const result = await this.fileOps.exportQueryResults(results, filename, format);
      
      logger.info(`Query results exported to ${result.path}`);
      return result;
    } catch (error) {
      logger.error(`Failed to export query results:`, error);
      throw error;
    }
  }

  async batchExportDashboards(dashboardIds, format = 'json') {
    try {
      const dashboards = await Promise.all(
        dashboardIds.map(id => this.metabaseClient.getDashboard(id))
      );
      
      const results = await this.fileOps.batchExport(dashboards, 'dashboard', format);
      
      logger.info(`Batch exported ${dashboards.length} dashboards`);
      return results;
    } catch (error) {
      logger.error('Failed to batch export dashboards:', error);
      throw error;
    }
  }

  async batchExportQuestions(questionIds, format = 'json') {
    try {
      const questions = await Promise.all(
        questionIds.map(id => this.metabaseClient.getQuestion(id))
      );
      
      const results = await this.fileOps.batchExport(questions, 'question', format);
      
      logger.info(`Batch exported ${questions.length} questions`);
      return results;
    } catch (error) {
      logger.error('Failed to batch export questions:', error);
      throw error;
    }
  }

  async createBackup() {
    try {
      // Gather all Metabase configuration
      const [databases, collections, dashboards, questions, metrics] = await Promise.all([
        this.metabaseClient.getDatabases(),
        this.metabaseClient.getCollections(),
        this.metabaseClient.getDashboards(),
        this.metabaseClient.getQuestions(),
        this.metabaseClient.getMetrics()
      ]);

      const config = {
        databases,
        collections,
        dashboards,
        questions,
        metrics
      };

      const result = await this.fileOps.backupMetabaseConfig(config);
      
      logger.info(`Full Metabase backup created: ${result.path}`);
      return result;
    } catch (error) {
      logger.error('Failed to create backup:', error);
      throw error;
    }
  }

  async restoreFromBackup(backupFilename) {
    try {
      const backup = await this.fileOps.restoreMetabaseConfig(backupFilename);
      
      logger.info(`Backup loaded from: ${backupFilename}`);
      return {
        success: true,
        backup,
        note: 'Backup loaded successfully. Manual restoration steps may be required.'
      };
    } catch (error) {
      logger.error('Failed to restore from backup:', error);
      throw error;
    }
  }

  async listExportedFiles(directory = '') {
    try {
      const files = await this.fileOps.listFiles(directory);
      return files;
    } catch (error) {
      logger.error(`Failed to list files in ${directory}:`, error);
      throw error;
    }
  }

  async getStorageStats() {
    try {
      const stats = await this.fileOps.getStorageStats();
      return stats;
    } catch (error) {
      logger.error('Failed to get storage stats:', error);
      throw error;
    }
  }

  async cleanupOldFiles(maxAgeInDays = 7) {
    try {
      const maxAge = maxAgeInDays * 24 * 60 * 60 * 1000;
      const deleted = await this.fileOps.cleanupOldFiles(maxAge);
      
      logger.info(`Cleanup completed. Deleted ${deleted.length} old files`);
      return deleted;
    } catch (error) {
      logger.error('Failed to cleanup old files:', error);
      throw error;
    }
  }

  // Enhanced dashboard creation with file export
  async createDashboardWithExport(description, questions = [], options = {}) {
    try {
      // Create dashboard
      const dashboardResult = await this.createDashboard(description, questions, options);
      
      // Export dashboard if requested
      if (options.autoExport) {
        const exportResult = await this.exportDashboard(
          dashboardResult.dashboard.id, 
          options.exportFormat || 'json'
        );
        
        dashboardResult.exportPath = exportResult.path;
      }
      
      return dashboardResult;
    } catch (error) {
      logger.error('Failed to create dashboard with export:', error);
      throw error;
    }
  }

  // Database schema documentation generator
  async generateDatabaseDocumentation(databaseId, options = {}) {
    try {
      const database = await this.metabaseClient.getDatabase(databaseId);
      const schemas = await this.metabaseClient.getDatabaseSchemas(databaseId);
      
      let documentation = `# Database Documentation: ${database.name}\n\n`;
      documentation += `**Database ID:** ${database.id}\n`;
      documentation += `**Engine:** ${database.engine}\n`;
      documentation += `**Created:** ${database.created_at}\n\n`;
      
      for (const schema of schemas) {
        documentation += `## Schema: ${schema}\n\n`;
        
        const tables = await this.metabaseClient.getDatabaseTables(databaseId, schema);
        
        for (const table of tables) {
          documentation += `### ${table.display_name || table.name}\n`;
          if (table.description) {
            documentation += `${table.description}\n`;
          }
          
          documentation += `**Table ID:** ${table.id}\n`;
          documentation += `**Rows:** ${table.row_count || 'Unknown'}\n\n`;
          
          if (table.fields && table.fields.length > 0) {
            documentation += `**Columns:**\n`;
            table.fields.forEach(field => {
              documentation += `- **${field.display_name || field.name}** (${field.base_type})`;
              if (field.description) {
                documentation += `: ${field.description}`;
              }
              documentation += '\n';
            });
            documentation += '\n';
          }
        }
      }
      
      const filename = `db-documentation-${database.id}-${Date.now()}.md`;
      const result = await this.fileOps.writeFile(`documentation/${filename}`, documentation);
      
      logger.info(`Database documentation generated: ${result.path}`);
      return result;
    } catch (error) {
      logger.error('Failed to generate database documentation:', error);
      throw error;
    }
  }
}