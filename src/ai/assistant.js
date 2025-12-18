import { Anthropic } from '@anthropic-ai/sdk';
import OpenAI from 'openai';
import { logger } from '../utils/logger.js';

export class MetabaseAIAssistant {
  constructor(config) {
    this.metabaseClient = config.metabaseClient;
    this.aiProvider = config.aiProvider || 'anthropic';
    
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
    const prompt = `
    Based on the following data structure and question type, suggest the best visualization:
    
    Question Type: ${questionType}
    Data Sample: ${JSON.stringify(data.slice(0, 3), null, 2)}
    
    Respond with:
    - visualization_type: (table|bar|line|pie|number|scatter|map)
    - settings: visualization settings object
    - reasoning: brief explanation
    `;

    const response = await this.getAIResponse(prompt);
    return JSON.parse(response);
  }

  async createModel(description, databaseId) {
    logger.info(`Creating model for: ${description}`);
    
    // Get database schema
    const tables = await this.metabaseClient.getDatabaseTables(databaseId);
    
    // Generate SQL for the model
    const sql = await this.generateSQL(description, tables);
    
    // Create the model
    const model = await this.metabaseClient.createModel({
      name: this.generateName(description, 'Model'),
      description,
      database_id: databaseId,
      dataset_query: {
        database: databaseId,
        type: 'native',
        native: { query: sql }
      }
    });
    
    logger.info(`Model created: ${model.id}`);
    return model;
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

  async createMetric(description, tableId) {
    const prompt = `
    Create a metric definition based on:
    "${description}"
    
    Provide:
    - name: metric name
    - description: detailed description
    - aggregation: (count|sum|avg|min|max|distinct)
    - field: field to aggregate (if applicable)
    - filter: filter conditions (if any)
    `;

    const metricDef = JSON.parse(await this.getAIResponse(prompt));
    
    const metric = await this.metabaseClient.createMetric({
      name: metricDef.name,
      description: metricDef.description,
      table_id: tableId,
      definition: {
        aggregation: [metricDef.aggregation, metricDef.field].filter(Boolean),
        filter: metricDef.filter
      }
    });
    
    logger.info(`Metric created: ${metric.id}`);
    return metric;
  }

  async createDashboard(description, questions = []) {
    logger.info(`Creating dashboard: ${description}`);
    
    // Create dashboard
    const dashboard = await this.metabaseClient.createDashboard({
      name: this.generateName(description, 'Dashboard'),
      description
    });
    
    // Suggest layout for cards
    const layoutPrompt = `
    Suggest a dashboard layout for ${questions.length} cards.
    Provide a grid layout (12 columns wide) with:
    - card positions (row, col)
    - card sizes (sizeX, sizeY)
    
    Return as JSON array of layout objects.
    `;
    
    const layout = JSON.parse(await this.getAIResponse(layoutPrompt));
    
    // Add questions to dashboard
    for (let i = 0; i < questions.length; i++) {
      await this.metabaseClient.addCardToDashboard(
        dashboard.id,
        questions[i].id,
        layout[i] || { row: Math.floor(i / 3) * 4, col: (i % 3) * 4, sizeX: 4, sizeY: 4 }
      );
    }
    
    logger.info(`Dashboard created: ${dashboard.id}`);
    return dashboard;
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
}