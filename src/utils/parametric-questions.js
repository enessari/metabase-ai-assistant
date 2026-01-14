import { logger } from './logger.js';

export class ParametricQuestions {
  constructor(metabaseClient, definitionTables) {
    this.client = metabaseClient;
    this.definitions = definitionTables;
    this.parameterTypes = {
      'date/single': { widget: 'date/single', type: 'date', required: true },
      'date/range': { widget: 'date/range', type: 'date', required: true },
      'date/relative': { widget: 'date/relative', type: 'date', required: false },
      'string/=': { widget: 'string/=', type: 'string', required: false },
      'string/contains': { widget: 'string/contains', type: 'string', required: false },
      'number/=': { widget: 'number/=', type: 'number', required: false },
      'number/between': { widget: 'number/between', type: 'number', required: false },
      'category': { widget: 'category', type: 'string', required: false }
    };
  }

  async createParametricQuestion(databaseId, config) {
    try {
      logger.info('Creating parametric question:', config.name);

      // Build SQL with parameters
      const sqlWithParams = await this.buildParametricSQL(config);
      
      // Create parameter definitions
      const parameters = await this.createParameterDefinitions(config.parameters);
      
      // Get visualization recommendation
      const visualization = await this.getVisualizationConfig(config);

      // Create the question
      const questionData = {
        name: config.name,
        description: config.description,
        database_id: databaseId,
        collection_id: config.collection_id,
        dataset_query: {
          database: databaseId,
          type: 'native',
          native: {
            query: sqlWithParams,
            'template-tags': parameters
          }
        },
        display: visualization.type,
        visualization_settings: visualization.settings,
        parameters: Object.values(parameters).map(param => ({
          id: param.id,
          type: param.type,
          target: param.target || ['variable', ['template-tag', param.name]],
          name: param['display-name'],
          slug: param.name,
          default: param.default
        }))
      };

      const result = await this.client.createQuestion(questionData);

      // Log activity
      await this.logParametricQuestionCreation(databaseId, result, config);

      return {
        success: true,
        question: result,
        parameters: Object.keys(parameters),
        sql: sqlWithParams
      };

    } catch (error) {
      logger.error('Failed to create parametric question:', error);
      throw error;
    }
  }

  async buildParametricSQL(config) {
    let sql = config.sql_template || config.sql;
    
    // Apply definition lookups if available
    if (config.use_definitions) {
      sql = await this.applyDefinitionLookups(sql, config.database_id);
    }

    // Apply parameter substitutions
    if (config.parameters) {
      sql = await this.applyParameterSubstitutions(sql, config.parameters);
    }

    // Apply date period substitutions
    if (config.date_periods) {
      sql = await this.applyDatePeriodSubstitutions(sql, config.date_periods, config.database_id);
    }

    return sql;
  }

  async applyDefinitionLookups(sql, databaseId) {
    // Replace business terms with their definitions
    const termMatches = sql.match(/\{\{define:([^}]+)\}\}/g);
    if (termMatches) {
      for (const match of termMatches) {
        const termName = match.replace(/\{\{define:([^}]+)\}\}/, '$1');
        const definition = await this.definitions.searchBusinessTerms(databaseId, termName);
        if (definition.length > 0) {
          const replacement = `-- ${definition[0].definition}\n`;
          sql = sql.replace(match, replacement);
        }
      }
    }

    // Replace metric calculations
    const metricMatches = sql.match(/\{\{metric:([^}]+)\}\}/g);
    if (metricMatches) {
      for (const match of metricMatches) {
        const metricName = match.replace(/\{\{metric:([^}]+)\}\}/, '$1');
        const metric = await this.definitions.getMetricDefinition(databaseId, metricName);
        if (metric) {
          sql = sql.replace(match, metric.calculation_formula);
        }
      }
    }

    return sql;
  }

  async applyParameterSubstitutions(sql, parameters) {
    for (const [paramName, paramConfig] of Object.entries(parameters)) {
      // Replace simple parameter placeholders
      const simplePattern = new RegExp(`\\$\\{${paramName}\\}`, 'g');
      sql = sql.replace(simplePattern, `{{${paramName}}}`);

      // Handle field filter patterns
      if (paramConfig.type && paramConfig.type.startsWith('date/') || paramConfig.field_id) {
        const fieldFilterPattern = new RegExp(`WHERE\\s+${paramName}\\s*=`, 'gi');
        sql = sql.replace(fieldFilterPattern, `WHERE {{${paramName}}}`);
      }

      // Handle optional clauses
      if (paramConfig.optional) {
        const optionalPattern = new RegExp(`([\\s\\S]*?)\\{\\{${paramName}\\}\\}([\\s\\S]*?)`, 'g');
        sql = sql.replace(optionalPattern, (match, before, after) => {
          return `${before}[[{{${paramName}}}]]${after}`;
        });
      }
    }

    return sql;
  }

  async applyDatePeriodSubstitutions(sql, datePeriods, databaseId) {
    for (const [periodName, periodConfig] of Object.entries(datePeriods)) {
      const period = await this.definitions.getDatePeriod(databaseId, periodConfig.period_type || periodName);
      if (period) {
        const periodPattern = new RegExp(`\\{\\{period:${periodName}\\}\\}`, 'g');
        sql = sql.replace(periodPattern, period.sql_expression);
      }
    }

    return sql;
  }

  async createParameterDefinitions(parameters) {
    const templateTags = {};

    for (const [paramName, paramConfig] of Object.entries(parameters)) {
      const paramId = this.generateParameterId();
      const paramType = paramConfig.type || 'text';

      let templateTag = {
        id: paramId,
        name: paramName,
        'display-name': paramConfig.display_name || this.formatDisplayName(paramName),
        type: this.getMetabaseParameterType(paramType),
        required: paramConfig.required !== false,
        default: paramConfig.default || null
      };

      // Field filter configuration
      if (paramConfig.field_id) {
        templateTag.dimension = ['field', paramConfig.field_id, null];
        templateTag['widget-type'] = this.getWidgetType(paramType);
      }

      // Category/dropdown configuration
      if (paramConfig.options) {
        templateTag['widget-type'] = 'category';
        templateTag.values_source_type = 'static-list';
        templateTag.values_source_config = {
          values: paramConfig.options.map(opt => [opt, opt])
        };
      }

      // Date configuration
      if (paramType.startsWith('date/')) {
        templateTag['widget-type'] = paramType;
        if (paramConfig.default_period) {
          templateTag.default = paramConfig.default_period;
        }
      }

      // Number range configuration
      if (paramType === 'number/between') {
        templateTag['widget-type'] = 'number/between';
      }

      // Text search configuration
      if (paramType === 'string/contains') {
        templateTag['widget-type'] = 'string/contains';
      }

      templateTags[paramId] = templateTag;
    }

    return templateTags;
  }

  getMetabaseParameterType(type) {
    const typeMap = {
      'date/single': 'date',
      'date/range': 'date',
      'date/relative': 'date',
      'string/=': 'text',
      'string/contains': 'text',
      'number/=': 'number',
      'number/between': 'number',
      'category': 'category'
    };
    return typeMap[type] || 'text';
  }

  getWidgetType(type) {
    const widgetMap = {
      'date/single': 'date/single',
      'date/range': 'date/range', 
      'date/relative': 'date/relative',
      'string/=': 'string/=',
      'string/contains': 'string/contains',
      'number/=': 'number/=',
      'number/between': 'number/between',
      'category': 'category'
    };
    return widgetMap[type] || 'string/=';
  }

  generateParameterId() {
    return Math.random().toString(36).substr(2, 9);
  }

  formatDisplayName(name) {
    return name.split('_').map(word => 
      word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
    ).join(' ');
  }

  async getVisualizationConfig(config) {
    // Simple visualization logic based on question type
    const questionType = config.question_type || 'table';
    
    const visualizations = {
      'trend_analysis': { type: 'line', settings: { 'graph.dimensions': ['date'], 'graph.metrics': ['value'] } },
      'comparison': { type: 'bar', settings: { 'graph.dimensions': ['category'], 'graph.metrics': ['value'] } },
      'ranking': { type: 'bar', settings: { 'graph.dimensions': ['name'], 'graph.metrics': ['value'] } },
      'distribution': { type: 'pie', settings: { 'pie.dimension': 'category', 'pie.metric': 'value' } },
      'kpi': { type: 'scalar', settings: { 'scalar.field': 'value' } },
      'table': { type: 'table', settings: {} }
    };

    return visualizations[questionType] || visualizations['table'];
  }

  async createDashboardWithParametricQuestions(databaseId, config) {
    try {
      logger.info('Creating dashboard with parametric questions:', config.name);

      // Create the dashboard
      const dashboard = await this.client.createDashboard({
        name: config.name,
        description: config.description,
        collection_id: config.collection_id
      });

      // Create shared dashboard filters
      const dashboardFilters = await this.createDashboardFilters(dashboard.id, config.filters || {});

      // Create parametric questions
      const questions = [];
      for (const questionConfig of config.questions || []) {
        const question = await this.createParametricQuestion(databaseId, {
          ...questionConfig,
          collection_id: config.collection_id
        });
        questions.push(question);
      }

      // Add questions to dashboard with parameter mapping
      const cards = [];
      for (let i = 0; i < questions.length; i++) {
        const question = questions[i];
        const layout = config.layout?.[i] || this.getDefaultLayout(i, questions.length);
        
        const card = await this.client.addCardToDashboard(dashboard.id, question.question.id, {
          row: layout.row,
          col: layout.col,
          sizeX: layout.sizeX,
          sizeY: layout.sizeY,
          parameter_mappings: await this.createParameterMappings(dashboardFilters, question.parameters)
        });
        
        cards.push(card);
      }

      return {
        success: true,
        dashboard,
        questions,
        cards,
        filters: dashboardFilters
      };

    } catch (error) {
      logger.error('Failed to create dashboard with parametric questions:', error);
      throw error;
    }
  }

  async createDashboardFilters(dashboardId, filterConfigs) {
    const filters = [];

    for (const [filterName, filterConfig] of Object.entries(filterConfigs)) {
      const filter = await this.client.addDashboardFilter(dashboardId, {
        name: filterConfig.display_name || this.formatDisplayName(filterName),
        slug: filterName,
        type: filterConfig.type || 'date/range',
        default_value: filterConfig.default,
        required: filterConfig.required !== false
      });

      filters.push({ name: filterName, filter, config: filterConfig });
    }

    return filters;
  }

  async createParameterMappings(dashboardFilters, questionParameters) {
    const mappings = [];

    for (const filter of dashboardFilters) {
      if (questionParameters.includes(filter.name)) {
        mappings.push({
          parameter_id: filter.filter.id,
          target: ['variable', ['template-tag', filter.name]]
        });
      }
    }

    return mappings;
  }

  getDefaultLayout(index, totalQuestions) {
    const layouts = {
      1: [{ row: 0, col: 0, sizeX: 12, sizeY: 6 }],
      2: [
        { row: 0, col: 0, sizeX: 6, sizeY: 6 },
        { row: 0, col: 6, sizeX: 6, sizeY: 6 }
      ],
      3: [
        { row: 0, col: 0, sizeX: 4, sizeY: 6 },
        { row: 0, col: 4, sizeX: 4, sizeY: 6 },
        { row: 0, col: 8, sizeX: 4, sizeY: 6 }
      ],
      4: [
        { row: 0, col: 0, sizeX: 6, sizeY: 4 },
        { row: 0, col: 6, sizeX: 6, sizeY: 4 },
        { row: 4, col: 0, sizeX: 6, sizeY: 4 },
        { row: 4, col: 6, sizeX: 6, sizeY: 4 }
      ]
    };

    const layoutSet = layouts[Math.min(totalQuestions, 4)] || layouts[4];
    return layoutSet[index] || { row: Math.floor(index / 2) * 6, col: (index % 2) * 6, sizeX: 6, sizeY: 6 };
  }

  // Preset question templates with common parametric patterns
  async createDateRangeAnalysisQuestion(databaseId, config) {
    return await this.createParametricQuestion(databaseId, {
      name: config.name || 'Date Range Analysis',
      description: config.description || 'Analysis with configurable date range',
      sql_template: `
        SELECT 
          ${config.date_column || 'created_date'} as date,
          ${config.metrics?.join(', ') || 'COUNT(*) as count'}
        FROM ${config.table}
        WHERE {{date_range}}
        ${config.group_by ? `GROUP BY ${config.group_by}` : ''}
        ORDER BY ${config.date_column || 'created_date'}
      `,
      parameters: {
        date_range: {
          type: 'date/range',
          display_name: 'Date Range',
          field_id: config.date_field_id,
          default: 'last_30_days',
          required: true
        }
      },
      question_type: 'trend_analysis',
      collection_id: config.collection_id
    });
  }

  async createCategoryFilterQuestion(databaseId, config) {
    return await this.createParametricQuestion(databaseId, {
      name: config.name || 'Category Analysis',
      description: config.description || 'Analysis with category filtering',
      sql_template: `
        SELECT 
          ${config.category_column} as category,
          ${config.metrics?.join(', ') || 'COUNT(*) as count'}
        FROM ${config.table}
        WHERE [[{{category_filter}}]]
        ${config.additional_filters ? `AND ${config.additional_filters}` : ''}
        GROUP BY ${config.category_column}
        ORDER BY count DESC
      `,
      parameters: {
        category_filter: {
          type: 'string/=',
          display_name: config.category_display_name || 'Category',
          field_id: config.category_field_id,
          optional: true,
          options: config.category_options
        }
      },
      question_type: 'ranking',
      collection_id: config.collection_id
    });
  }

  async createTextSearchQuestion(databaseId, config) {
    return await this.createParametricQuestion(databaseId, {
      name: config.name || 'Text Search Analysis',
      description: config.description || 'Analysis with text search capability',
      sql_template: `
        SELECT 
          ${config.display_columns?.join(', ') || '*'}
        FROM ${config.table}
        WHERE [[{{search_term}} AND (
          ${config.search_columns?.map(col => `${col} ILIKE '%{{search_term}}%'`).join(' OR ') || 'name ILIKE \'%{{search_term}}%\''}
        )]]
        ${config.additional_filters ? `AND ${config.additional_filters}` : ''}
        ORDER BY ${config.order_by || 'id DESC'}
        LIMIT {{limit}}
      `,
      parameters: {
        search_term: {
          type: 'string/contains',
          display_name: 'Search Term',
          optional: true,
          placeholder: 'Enter search term...'
        },
        limit: {
          type: 'number/=',
          display_name: 'Results Limit',
          default: 100,
          required: false
        }
      },
      question_type: 'table',
      collection_id: config.collection_id
    });
  }

  async createPeriodComparisonQuestion(databaseId, config) {
    return await this.createParametricQuestion(databaseId, {
      name: config.name || 'Period Comparison',
      description: config.description || 'Compare metrics across different time periods',
      sql_template: `
        SELECT 
          '{{period_1}}' as period_name,
          ${config.metrics?.join(', ') || 'COUNT(*) as count'}
        FROM ${config.table}
        WHERE {{period_1_filter}}
        
        UNION ALL
        
        SELECT 
          '{{period_2}}' as period_name,
          ${config.metrics?.join(', ') || 'COUNT(*) as count'}
        FROM ${config.table}
        WHERE {{period_2_filter}}
      `,
      parameters: {
        period_1: {
          type: 'category',
          display_name: 'First Period',
          options: ['This Month', 'Last Month', 'This Quarter', 'Last Quarter'],
          default: 'This Month'
        },
        period_1_filter: {
          type: 'date/range',
          display_name: 'First Period Range',
          field_id: config.date_field_id,
          required: true
        },
        period_2: {
          type: 'category',
          display_name: 'Second Period',
          options: ['This Month', 'Last Month', 'This Quarter', 'Last Quarter'],
          default: 'Last Month'
        },
        period_2_filter: {
          type: 'date/range', 
          display_name: 'Second Period Range',
          field_id: config.date_field_id,
          required: true
        }
      },
      question_type: 'comparison',
      collection_id: config.collection_id
    });
  }

  async logParametricQuestionCreation(databaseId, question, config) {
    try {
      const logEntry = {
        operation: 'create_parametric_question',
        question_id: question.id,
        question_name: question.name,
        parameters: Object.keys(config.parameters || {}),
        database_id: databaseId,
        collection_id: config.collection_id,
        question_type: config.question_type,
        has_field_filters: Object.values(config.parameters || {}).some(p => p.field_id),
        parameter_count: Object.keys(config.parameters || {}).length
      };

      // Log to activity logger if available
      if (this.activityLogger) {
        await this.activityLogger.logActivity('parametric_question_created', logEntry);
      }

      logger.info('Parametric question created successfully:', logEntry);
    } catch (error) {
      logger.warn('Failed to log parametric question creation:', error.message);
    }
  }

  // Utility methods for managing parametric questions
  async getQuestionParameters(questionId) {
    try {
      // This would typically fetch from Metabase API
      const question = await this.client.getQuestion(questionId);
      return {
        template_tags: question.dataset_query?.native?.['template-tags'] || {},
        parameters: question.parameters || []
      };
    } catch (error) {
      logger.error('Failed to get question parameters:', error);
      return null;
    }
  }

  async updateQuestionParameters(questionId, newParameters) {
    try {
      const question = await this.client.getQuestion(questionId);
      const templateTags = await this.createParameterDefinitions(newParameters);
      
      const updatedQuery = {
        ...question.dataset_query,
        native: {
          ...question.dataset_query.native,
          'template-tags': templateTags
        }
      };

      return await this.client.updateQuestion(questionId, {
        dataset_query: updatedQuery,
        parameters: Object.values(templateTags).map(param => ({
          id: param.id,
          type: param.type,
          target: ['variable', ['template-tag', param.name]],
          name: param['display-name'],
          slug: param.name,
          default: param.default
        }))
      });
    } catch (error) {
      logger.error('Failed to update question parameters:', error);
      throw error;
    }
  }

  async validateParametricSQL(sql, parameters) {
    const warnings = [];
    const errors = [];

    // Check for unmatched parameter references
    const paramRefs = sql.match(/\{\{[^}]+\}\}/g) || [];
    const definedParams = Object.keys(parameters);

    for (const ref of paramRefs) {
      const paramName = ref.replace(/[{}]/g, '');
      if (!definedParams.includes(paramName)) {
        errors.push(`Parameter '${paramName}' is referenced in SQL but not defined`);
      }
    }

    // Check for unused parameters
    for (const paramName of definedParams) {
      if (!sql.includes(`{{${paramName}}}`)) {
        warnings.push(`Parameter '${paramName}' is defined but not used in SQL`);
      }
    }

    // Check for field filter syntax
    const fieldFilterRefs = sql.match(/WHERE\s+\{\{[^}]+\}\}/gi) || [];
    for (const ref of fieldFilterRefs) {
      const paramName = ref.replace(/WHERE\s+\{\{([^}]+)\}\}/i, '$1');
      const param = parameters[paramName];
      if (param && !param.field_id) {
        warnings.push(`Parameter '${paramName}' is used as field filter but no field_id specified`);
      }
    }

    return { valid: errors.length === 0, errors, warnings };
  }
}