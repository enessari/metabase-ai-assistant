import { logger } from './logger.js';

export class DefinitionTables {
  constructor(metabaseClient) {
    this.client = metabaseClient;
    this.definitionTables = {
      business_terms: 'claude_ai_business_terms',
      metrics_definitions: 'claude_ai_metrics_definitions', 
      dashboard_templates: 'claude_ai_dashboard_templates',
      question_templates: 'claude_ai_question_templates',
      filter_presets: 'claude_ai_filter_presets',
      date_periods: 'claude_ai_date_periods',
      search_indexes: 'claude_ai_search_indexes'
    };
  }

  async initializeDefinitionTables(databaseId) {
    try {
      logger.info('Initializing definition lookup tables...');
      
      const tables = await this.createAllDefinitionTables(databaseId);
      await this.populateDefaultData(databaseId);
      
      logger.info('Definition tables initialized successfully');
      return {
        success: true,
        tables: Object.keys(this.definitionTables),
        message: 'All definition lookup tables created and populated'
      };
      
    } catch (error) {
      logger.error('Failed to initialize definition tables:', error);
      throw error;
    }
  }

  async createAllDefinitionTables(databaseId) {
    const tableCreations = [];

    // Business Terms Table
    tableCreations.push(this.createBusinessTermsTable(databaseId));
    
    // Metrics Definitions Table  
    tableCreations.push(this.createMetricsDefinitionsTable(databaseId));
    
    // Dashboard Templates Table
    tableCreations.push(this.createDashboardTemplatesTable(databaseId));
    
    // Question Templates Table
    tableCreations.push(this.createQuestionTemplatesTable(databaseId));
    
    // Filter Presets Table
    tableCreations.push(this.createFilterPresetsTable(databaseId));
    
    // Date Periods Table
    tableCreations.push(this.createDatePeriodsTable(databaseId));
    
    // Search Indexes Table
    tableCreations.push(this.createSearchIndexesTable(databaseId));

    return await Promise.all(tableCreations);
  }

  async createBusinessTermsTable(databaseId) {
    const sql = `
      CREATE TABLE IF NOT EXISTS ${this.definitionTables.business_terms} (
        id SERIAL PRIMARY KEY,
        term VARCHAR(255) NOT NULL UNIQUE,
        definition TEXT NOT NULL,
        category VARCHAR(100),
        synonyms TEXT[],
        related_terms TEXT[],
        source_table VARCHAR(255),
        source_column VARCHAR(255),
        business_owner VARCHAR(255),
        technical_owner VARCHAR(255),
        data_type VARCHAR(50),
        calculation_logic TEXT,
        usage_examples TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT 'claude_ai',
        tags TEXT[],
        approval_status VARCHAR(20) DEFAULT 'draft',
        version INTEGER DEFAULT 1
      );

      CREATE INDEX IF NOT EXISTS idx_business_terms_search 
      ON ${this.definitionTables.business_terms} USING GIN(to_tsvector('english', term || ' ' || definition));
      
      CREATE INDEX IF NOT EXISTS idx_business_terms_category 
      ON ${this.definitionTables.business_terms}(category);
    `;

    return await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
  }

  async createMetricsDefinitionsTable(databaseId) {
    const sql = `
      CREATE TABLE IF NOT EXISTS ${this.definitionTables.metrics_definitions} (
        id SERIAL PRIMARY KEY,
        metric_name VARCHAR(255) NOT NULL UNIQUE,
        display_name VARCHAR(255) NOT NULL,
        description TEXT NOT NULL,
        calculation_formula TEXT NOT NULL,
        aggregation_type VARCHAR(50) NOT NULL,
        source_table VARCHAR(255),
        source_columns TEXT[],
        filters_applied TEXT,
        business_context TEXT,
        target_value DECIMAL,
        threshold_warning DECIMAL,
        threshold_critical DECIMAL,
        unit_of_measure VARCHAR(50),
        frequency VARCHAR(50) DEFAULT 'daily',
        owner VARCHAR(255),
        stakeholders TEXT[],
        related_metrics TEXT[],
        kpi_category VARCHAR(100),
        dashboard_usage TEXT[],
        last_calculated TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT 'claude_ai',
        is_active BOOLEAN DEFAULT true,
        version INTEGER DEFAULT 1
      );

      CREATE INDEX IF NOT EXISTS idx_metrics_search 
      ON ${this.definitionTables.metrics_definitions} USING GIN(to_tsvector('english', metric_name || ' ' || description));
      
      CREATE INDEX IF NOT EXISTS idx_metrics_category 
      ON ${this.definitionTables.metrics_definitions}(kpi_category);
    `;

    return await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
  }

  async createDashboardTemplatesTable(databaseId) {
    const sql = `
      CREATE TABLE IF NOT EXISTS ${this.definitionTables.dashboard_templates} (
        id SERIAL PRIMARY KEY,
        template_name VARCHAR(255) NOT NULL UNIQUE,
        template_type VARCHAR(100) NOT NULL,
        description TEXT,
        target_audience TEXT[],
        layout_config JSONB,
        default_filters JSONB,
        required_metrics TEXT[],
        optional_metrics TEXT[],
        card_configurations JSONB,
        refresh_schedule VARCHAR(50),
        access_permissions JSONB,
        business_context TEXT,
        usage_instructions TEXT,
        sample_data JSONB,
        prerequisites TEXT[],
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT 'claude_ai',
        is_active BOOLEAN DEFAULT true,
        usage_count INTEGER DEFAULT 0,
        rating DECIMAL(2,1),
        tags TEXT[]
      );

      CREATE INDEX IF NOT EXISTS idx_dashboard_templates_type 
      ON ${this.definitionTables.dashboard_templates}(template_type);
      
      CREATE INDEX IF NOT EXISTS idx_dashboard_templates_search 
      ON ${this.definitionTables.dashboard_templates} USING GIN(to_tsvector('english', template_name || ' ' || description));
    `;

    return await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
  }

  async createQuestionTemplatesTable(databaseId) {
    const sql = `
      CREATE TABLE IF NOT EXISTS ${this.definitionTables.question_templates} (
        id SERIAL PRIMARY KEY,
        template_name VARCHAR(255) NOT NULL UNIQUE,
        question_type VARCHAR(100) NOT NULL,
        description TEXT,
        sql_template TEXT NOT NULL,
        parameters JSONB,
        visualization_type VARCHAR(50) DEFAULT 'table',
        visualization_settings JSONB,
        database_compatibility TEXT[],
        required_tables TEXT[],
        optional_tables TEXT[],
        business_use_case TEXT,
        complexity_level VARCHAR(20) DEFAULT 'medium',
        execution_time_estimate INTEGER,
        sample_output JSONB,
        parameterization JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT 'claude_ai',
        is_active BOOLEAN DEFAULT true,
        usage_count INTEGER DEFAULT 0,
        performance_rating DECIMAL(2,1),
        tags TEXT[]
      );

      CREATE INDEX IF NOT EXISTS idx_question_templates_type 
      ON ${this.definitionTables.question_templates}(question_type);
      
      CREATE INDEX IF NOT EXISTS idx_question_templates_search 
      ON ${this.definitionTables.question_templates} USING GIN(to_tsvector('english', template_name || ' ' || description));
    `;

    return await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
  }

  async createFilterPresetsTable(databaseId) {
    const sql = `
      CREATE TABLE IF NOT EXISTS ${this.definitionTables.filter_presets} (
        id SERIAL PRIMARY KEY,
        preset_name VARCHAR(255) NOT NULL UNIQUE,
        preset_type VARCHAR(100) NOT NULL,
        description TEXT,
        filter_config JSONB NOT NULL,
        default_values JSONB,
        applicable_contexts TEXT[],
        database_fields JSONB,
        parameter_mapping JSONB,
        validation_rules JSONB,
        usage_examples JSONB,
        business_context TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT 'claude_ai',
        is_active BOOLEAN DEFAULT true,
        usage_frequency INTEGER DEFAULT 0,
        tags TEXT[]
      );

      CREATE INDEX IF NOT EXISTS idx_filter_presets_type 
      ON ${this.definitionTables.filter_presets}(preset_type);
    `;

    return await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
  }

  async createDatePeriodsTable(databaseId) {
    const sql = `
      CREATE TABLE IF NOT EXISTS ${this.definitionTables.date_periods} (
        id SERIAL PRIMARY KEY,
        period_name VARCHAR(255) NOT NULL UNIQUE,
        period_type VARCHAR(100) NOT NULL,
        description TEXT,
        sql_expression TEXT NOT NULL,
        relative_expression TEXT,
        business_calendar_alignment BOOLEAN DEFAULT false,
        fiscal_year_start INTEGER DEFAULT 1,
        working_days_only BOOLEAN DEFAULT false,
        exclude_holidays BOOLEAN DEFAULT false,
        time_zone VARCHAR(50) DEFAULT 'UTC',
        examples JSONB,
        use_cases TEXT[],
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT 'claude_ai',
        is_active BOOLEAN DEFAULT true,
        usage_count INTEGER DEFAULT 0
      );

      CREATE INDEX IF NOT EXISTS idx_date_periods_type 
      ON ${this.definitionTables.date_periods}(period_type);
    `;

    return await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
  }

  async createSearchIndexesTable(databaseId) {
    const sql = `
      CREATE TABLE IF NOT EXISTS ${this.definitionTables.search_indexes} (
        id SERIAL PRIMARY KEY,
        index_name VARCHAR(255) NOT NULL UNIQUE,
        source_table VARCHAR(255) NOT NULL,
        indexed_content TEXT NOT NULL,
        content_type VARCHAR(100) NOT NULL,
        search_vector tsvector,
        metadata JSONB,
        relevance_score DECIMAL(3,2) DEFAULT 1.0,
        last_indexed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT 'claude_ai',
        is_active BOOLEAN DEFAULT true
      );

      CREATE INDEX IF NOT EXISTS idx_search_content 
      ON ${this.definitionTables.search_indexes} USING GIN(search_vector);
      
      CREATE INDEX IF NOT EXISTS idx_search_type 
      ON ${this.definitionTables.search_indexes}(content_type);
    `;

    return await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
  }

  async populateDefaultData(databaseId) {
    await this.populateBusinessTerms(databaseId);
    await this.populateMetricsDefinitions(databaseId);
    await this.populateDashboardTemplates(databaseId);
    await this.populateQuestionTemplates(databaseId);
    await this.populateFilterPresets(databaseId);
    await this.populateDatePeriods(databaseId);
  }

  async populateBusinessTerms(databaseId) {
    const terms = [
      {
        term: 'Customer Lifetime Value',
        definition: 'The predicted net profit attributed to the entire future relationship with a customer',
        category: 'customer_metrics',
        synonyms: ['CLV', 'LTV', 'Customer LTV'],
        calculation_logic: 'Average Purchase Value × Purchase Frequency × Customer Lifespan',
        usage_examples: 'Used in customer segmentation and retention strategies'
      },
      {
        term: 'Monthly Recurring Revenue',
        definition: 'The predictable revenue that a company expects to receive every month',
        category: 'revenue_metrics',
        synonyms: ['MRR'],
        calculation_logic: 'Sum of all monthly subscription revenues',
        usage_examples: 'Key metric for SaaS businesses to track growth'
      },
      {
        term: 'Churn Rate',
        definition: 'The percentage of customers who stop using a service during a given time period',
        category: 'retention_metrics',
        synonyms: ['Customer Attrition Rate', 'Turnover Rate'],
        calculation_logic: '(Customers Lost / Total Customers at Start) × 100',
        usage_examples: 'Critical for subscription businesses to monitor retention'
      }
    ];

    for (const term of terms) {
      const sql = `
        INSERT INTO ${this.definitionTables.business_terms} 
        (term, definition, category, synonyms, calculation_logic, usage_examples)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (term) DO NOTHING
      `;
      
      try {
        await this.client.executeNativeQuery(databaseId, 
          sql.replace(/\$\d+/g, (match, index) => {
            const values = [term.term, term.definition, term.category, 
                          `{${term.synonyms.join(',')}}`, term.calculation_logic, term.usage_examples];
            return `'${values[match.slice(1) - 1]}'`;
          }), { enforcePrefix: false }
        );
      } catch (error) {
        logger.warn(`Failed to insert business term ${term.term}:`, error.message);
      }
    }
  }

  async populateMetricsDefinitions(databaseId) {
    const metrics = [
      {
        metric_name: 'daily_active_users',
        display_name: 'Daily Active Users',
        description: 'Number of unique users who engage with the product in a 24-hour period',
        calculation_formula: 'COUNT(DISTINCT user_id) WHERE last_activity_date = CURRENT_DATE',
        aggregation_type: 'count_distinct',
        kpi_category: 'engagement',
        unit_of_measure: 'users'
      },
      {
        metric_name: 'conversion_rate',
        display_name: 'Conversion Rate',
        description: 'Percentage of visitors who complete a desired action',
        calculation_formula: '(conversions / total_visitors) * 100',
        aggregation_type: 'percentage',
        kpi_category: 'sales',
        unit_of_measure: 'percentage'
      },
      {
        metric_name: 'average_order_value',
        display_name: 'Average Order Value',
        description: 'Average amount spent per order',
        calculation_formula: 'SUM(order_value) / COUNT(orders)',
        aggregation_type: 'average',
        kpi_category: 'revenue',
        unit_of_measure: 'currency'
      }
    ];

    for (const metric of metrics) {
      const sql = `
        INSERT INTO ${this.definitionTables.metrics_definitions} 
        (metric_name, display_name, description, calculation_formula, aggregation_type, kpi_category, unit_of_measure)
        VALUES ('${metric.metric_name}', '${metric.display_name}', '${metric.description}', 
                '${metric.calculation_formula}', '${metric.aggregation_type}', 
                '${metric.kpi_category}', '${metric.unit_of_measure}')
        ON CONFLICT (metric_name) DO NOTHING
      `;
      
      try {
        await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      } catch (error) {
        logger.warn(`Failed to insert metric ${metric.metric_name}:`, error.message);
      }
    }
  }

  async populateDashboardTemplates(databaseId) {
    const templates = [
      {
        template_name: 'Executive Summary',
        template_type: 'executive',
        description: 'High-level KPIs and metrics for C-level executives',
        required_metrics: ['revenue', 'growth_rate', 'customer_count', 'churn_rate'],
        layout_config: {
          grid_width: 12,
          cards: [
            { metric: 'revenue', position: { row: 0, col: 0, sizeX: 3, sizeY: 2 } },
            { metric: 'growth_rate', position: { row: 0, col: 3, sizeX: 3, sizeY: 2 } },
            { metric: 'customer_count', position: { row: 0, col: 6, sizeX: 3, sizeY: 2 } },
            { metric: 'churn_rate', position: { row: 0, col: 9, sizeX: 3, sizeY: 2 } }
          ]
        }
      },
      {
        template_name: 'Sales Performance',
        template_type: 'operational',
        description: 'Detailed sales metrics and pipeline analysis',
        required_metrics: ['total_sales', 'conversion_rate', 'pipeline_value', 'sales_by_region'],
        layout_config: {
          grid_width: 12,
          cards: [
            { metric: 'total_sales', position: { row: 0, col: 0, sizeX: 4, sizeY: 3 } },
            { metric: 'conversion_rate', position: { row: 0, col: 4, sizeX: 4, sizeY: 3 } },
            { metric: 'pipeline_value', position: { row: 0, col: 8, sizeX: 4, sizeY: 3 } },
            { metric: 'sales_by_region', position: { row: 3, col: 0, sizeX: 12, sizeY: 4 } }
          ]
        }
      }
    ];

    for (const template of templates) {
      const sql = `
        INSERT INTO ${this.definitionTables.dashboard_templates} 
        (template_name, template_type, description, required_metrics, layout_config)
        VALUES ('${template.template_name}', '${template.template_type}', '${template.description}', 
                '{${template.required_metrics.join(',')}}', '${JSON.stringify(template.layout_config)}')
        ON CONFLICT (template_name) DO NOTHING
      `;
      
      try {
        await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      } catch (error) {
        logger.warn(`Failed to insert dashboard template ${template.template_name}:`, error.message);
      }
    }
  }

  async populateQuestionTemplates(databaseId) {
    const templates = [
      {
        template_name: 'Top Products by Revenue',
        question_type: 'ranking',
        description: 'Shows the highest revenue-generating products',
        sql_template: `
          SELECT 
            product_name,
            SUM(revenue) as total_revenue,
            COUNT(DISTINCT order_id) as order_count
          FROM sales_data 
          WHERE {{date_range}}
          GROUP BY product_name 
          ORDER BY total_revenue DESC 
          LIMIT {{limit}}
        `,
        parameters: {
          date_range: { type: 'date/range', required: true, default: 'last_30_days' },
          limit: { type: 'number', required: false, default: 10 }
        },
        visualization_type: 'bar'
      },
      {
        template_name: 'Customer Growth Trend',
        question_type: 'trend_analysis',
        description: 'Shows customer acquisition trends over time',
        sql_template: `
          SELECT 
            DATE_TRUNC('{{period}}', created_date) as period,
            COUNT(*) as new_customers,
            SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC('{{period}}', created_date)) as cumulative_customers
          FROM customers
          WHERE created_date >= {{start_date}}
          GROUP BY DATE_TRUNC('{{period}}', created_date)
          ORDER BY period
        `,
        parameters: {
          period: { type: 'category', options: ['day', 'week', 'month'], default: 'month' },
          start_date: { type: 'date/single', required: true, default: '1_year_ago' }
        },
        visualization_type: 'line'
      }
    ];

    for (const template of templates) {
      const sql = `
        INSERT INTO ${this.definitionTables.question_templates} 
        (template_name, question_type, description, sql_template, parameters, visualization_type)
        VALUES ('${template.template_name}', '${template.question_type}', '${template.description}', 
                '${template.sql_template.replace(/'/g, "''")}', '${JSON.stringify(template.parameters)}', 
                '${template.visualization_type}')
        ON CONFLICT (template_name) DO NOTHING
      `;
      
      try {
        await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      } catch (error) {
        logger.warn(`Failed to insert question template ${template.template_name}:`, error.message);
      }
    }
  }

  async populateFilterPresets(databaseId) {
    const presets = [
      {
        preset_name: 'Date Range - Last 30 Days',
        preset_type: 'date',
        description: 'Filter for the last 30 days of data',
        filter_config: {
          type: 'date/range',
          operator: 'between',
          value: 'last_30_days'
        },
        default_values: {
          start: '30 days ago',
          end: 'today'
        }
      },
      {
        preset_name: 'Active Customers Only',
        preset_type: 'category',
        description: 'Filter to show only active customer records',
        filter_config: {
          type: 'string/=',
          field: 'status',
          operator: 'equals',
          value: 'active'
        },
        default_values: {
          status: 'active'
        }
      },
      {
        preset_name: 'High Value Orders',
        preset_type: 'number',
        description: 'Filter for orders above a certain value threshold',
        filter_config: {
          type: 'number/>=',
          field: 'order_value',
          operator: 'greater_than_or_equal',
          value: 1000
        },
        default_values: {
          order_value: 1000
        }
      }
    ];

    for (const preset of presets) {
      const sql = `
        INSERT INTO ${this.definitionTables.filter_presets} 
        (preset_name, preset_type, description, filter_config, default_values)
        VALUES ('${preset.preset_name}', '${preset.preset_type}', '${preset.description}', 
                '${JSON.stringify(preset.filter_config)}', '${JSON.stringify(preset.default_values)}')
        ON CONFLICT (preset_name) DO NOTHING
      `;
      
      try {
        await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      } catch (error) {
        logger.warn(`Failed to insert filter preset ${preset.preset_name}:`, error.message);
      }
    }
  }

  async populateDatePeriods(databaseId) {
    const periods = [
      {
        period_name: 'Last 7 Days',
        period_type: 'relative',
        description: 'Rolling 7-day period ending today',
        sql_expression: "date_field >= CURRENT_DATE - INTERVAL '7 days'",
        relative_expression: 'last_7_days'
      },
      {
        period_name: 'Current Month',
        period_type: 'calendar',
        description: 'Current calendar month from first to last day',
        sql_expression: "date_field >= DATE_TRUNC('month', CURRENT_DATE) AND date_field < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'",
        relative_expression: 'this_month'
      },
      {
        period_name: 'Last Quarter',
        period_type: 'calendar',
        description: 'Previous complete quarter',
        sql_expression: "date_field >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months') AND date_field < DATE_TRUNC('quarter', CURRENT_DATE)",
        relative_expression: 'last_quarter'
      },
      {
        period_name: 'Year to Date',
        period_type: 'calendar',
        description: 'From beginning of current year to today',
        sql_expression: "date_field >= DATE_TRUNC('year', CURRENT_DATE) AND date_field <= CURRENT_DATE",
        relative_expression: 'year_to_date'
      },
      {
        period_name: 'Fiscal Year to Date',
        period_type: 'fiscal',
        description: 'From beginning of fiscal year to today',
        sql_expression: "date_field >= CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 4 THEN DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '3 months' ELSE DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '9 months' END",
        relative_expression: 'fiscal_ytd',
        fiscal_year_start: 4
      }
    ];

    for (const period of periods) {
      const sql = `
        INSERT INTO ${this.definitionTables.date_periods} 
        (period_name, period_type, description, sql_expression, relative_expression, fiscal_year_start)
        VALUES ('${period.period_name}', '${period.period_type}', '${period.description}', 
                '${period.sql_expression}', '${period.relative_expression}', 
                ${period.fiscal_year_start || 1})
        ON CONFLICT (period_name) DO NOTHING
      `;
      
      try {
        await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      } catch (error) {
        logger.warn(`Failed to insert date period ${period.period_name}:`, error.message);
      }
    }
  }

  // Search and lookup methods
  async searchBusinessTerms(databaseId, searchTerm, category = null) {
    let sql = `
      SELECT 
        term,
        definition,
        category,
        synonyms,
        calculation_logic,
        usage_examples,
        ts_rank(to_tsvector('english', term || ' ' || definition), plainto_tsquery('english', $1)) as relevance
      FROM ${this.definitionTables.business_terms}
      WHERE to_tsvector('english', term || ' ' || definition) @@ plainto_tsquery('english', $1)
    `;
    
    if (category) {
      sql += ` AND category = $2`;
    }
    
    sql += ` ORDER BY relevance DESC, term ASC LIMIT 20`;

    const searchQuery = sql.replace('$1', `'${searchTerm}'`).replace('$2', category ? `'${category}'` : '');
    
    try {
      const result = await this.client.executeNativeQuery(databaseId, searchQuery, { enforcePrefix: false });
      return result.data.rows.map(row => ({
        term: row[0],
        definition: row[1], 
        category: row[2],
        synonyms: row[3],
        calculation_logic: row[4],
        usage_examples: row[5],
        relevance: row[6]
      }));
    } catch (error) {
      logger.error('Failed to search business terms:', error);
      return [];
    }
  }

  async getMetricDefinition(databaseId, metricName) {
    const sql = `
      SELECT 
        metric_name,
        display_name,
        description,
        calculation_formula,
        aggregation_type,
        unit_of_measure,
        kpi_category,
        business_context
      FROM ${this.definitionTables.metrics_definitions}
      WHERE metric_name = '${metricName}' OR display_name ILIKE '%${metricName}%'
      LIMIT 1
    `;

    try {
      const result = await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      if (result.data.rows.length > 0) {
        const row = result.data.rows[0];
        return {
          metric_name: row[0],
          display_name: row[1],
          description: row[2],
          calculation_formula: row[3],
          aggregation_type: row[4],
          unit_of_measure: row[5],
          kpi_category: row[6],
          business_context: row[7]
        };
      }
      return null;
    } catch (error) {
      logger.error('Failed to get metric definition:', error);
      return null;
    }
  }

  async getDashboardTemplate(databaseId, templateName) {
    const sql = `
      SELECT 
        template_name,
        template_type,
        description,
        layout_config,
        required_metrics,
        default_filters
      FROM ${this.definitionTables.dashboard_templates}
      WHERE template_name = '${templateName}' OR template_name ILIKE '%${templateName}%'
      LIMIT 1
    `;

    try {
      const result = await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      if (result.data.rows.length > 0) {
        const row = result.data.rows[0];
        return {
          template_name: row[0],
          template_type: row[1],
          description: row[2],
          layout_config: JSON.parse(row[3] || '{}'),
          required_metrics: row[4],
          default_filters: JSON.parse(row[5] || '{}')
        };
      }
      return null;
    } catch (error) {
      logger.error('Failed to get dashboard template:', error);
      return null;
    }
  }

  async getQuestionTemplate(databaseId, templateName) {
    const sql = `
      SELECT 
        template_name,
        question_type,
        description,
        sql_template,
        parameters,
        visualization_type,
        business_use_case
      FROM ${this.definitionTables.question_templates}
      WHERE template_name = '${templateName}' OR template_name ILIKE '%${templateName}%'
      LIMIT 1
    `;

    try {
      const result = await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      if (result.data.rows.length > 0) {
        const row = result.data.rows[0];
        return {
          template_name: row[0],
          question_type: row[1],
          description: row[2],
          sql_template: row[3],
          parameters: JSON.parse(row[4] || '{}'),
          visualization_type: row[5],
          business_use_case: row[6]
        };
      }
      return null;
    } catch (error) {
      logger.error('Failed to get question template:', error);
      return null;
    }
  }

  async getFilterPreset(databaseId, presetName) {
    const sql = `
      SELECT 
        preset_name,
        preset_type,
        description,
        filter_config,
        default_values
      FROM ${this.definitionTables.filter_presets}
      WHERE preset_name = '${presetName}' OR preset_name ILIKE '%${presetName}%'
      LIMIT 1
    `;

    try {
      const result = await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      if (result.data.rows.length > 0) {
        const row = result.data.rows[0];
        return {
          preset_name: row[0],
          preset_type: row[1],
          description: row[2],
          filter_config: JSON.parse(row[3] || '{}'),
          default_values: JSON.parse(row[4] || '{}')
        };
      }
      return null;
    } catch (error) {
      logger.error('Failed to get filter preset:', error);
      return null;
    }
  }

  async getDatePeriod(databaseId, periodName) {
    const sql = `
      SELECT 
        period_name,
        period_type,
        description,
        sql_expression,
        relative_expression
      FROM ${this.definitionTables.date_periods}
      WHERE period_name = '${periodName}' OR period_name ILIKE '%${periodName}%'
      LIMIT 1
    `;

    try {
      const result = await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      if (result.data.rows.length > 0) {
        const row = result.data.rows[0];
        return {
          period_name: row[0],
          period_type: row[1],
          description: row[2],
          sql_expression: row[3],
          relative_expression: row[4]
        };
      }
      return null;
    } catch (error) {
      logger.error('Failed to get date period:', error);
      return null;
    }
  }

  async updateSearchIndex(databaseId, tableName, content, contentType, metadata = {}) {
    const sql = `
      INSERT INTO ${this.definitionTables.search_indexes}
      (index_name, source_table, indexed_content, content_type, search_vector, metadata)
      VALUES (
        '${tableName}_${Date.now()}',
        '${tableName}',
        '${content.replace(/'/g, "''")}',
        '${contentType}',
        to_tsvector('english', '${content.replace(/'/g, "''")}'),
        '${JSON.stringify(metadata)}'
      )
      ON CONFLICT (index_name) DO UPDATE SET
        indexed_content = EXCLUDED.indexed_content,
        search_vector = EXCLUDED.search_vector,
        metadata = EXCLUDED.metadata,
        last_indexed = CURRENT_TIMESTAMP
    `;

    try {
      await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
      logger.info(`Updated search index for ${tableName}`);
    } catch (error) {
      logger.error('Failed to update search index:', error);
    }
  }

  async globalSearch(databaseId, searchTerm, contentTypes = null) {
    let sql = `
      SELECT 
        source_table,
        content_type,
        indexed_content,
        metadata,
        ts_rank(search_vector, plainto_tsquery('english', $1)) as relevance
      FROM ${this.definitionTables.search_indexes}
      WHERE search_vector @@ plainto_tsquery('english', $1)
        AND is_active = true
    `;

    if (contentTypes && contentTypes.length > 0) {
      sql += ` AND content_type = ANY('{${contentTypes.join(',')}}'::text[])`;
    }

    sql += ` ORDER BY relevance DESC, last_indexed DESC LIMIT 50`;

    const searchQuery = sql.replace('$1', `'${searchTerm}'`);

    try {
      const result = await this.client.executeNativeQuery(databaseId, searchQuery, { enforcePrefix: false });
      return result.data.rows.map(row => ({
        source_table: row[0],
        content_type: row[1],
        content: row[2],
        metadata: JSON.parse(row[3] || '{}'),
        relevance: row[4]
      }));
    } catch (error) {
      logger.error('Failed to perform global search:', error);
      return [];
    }
  }

  async getDefinitionStats(databaseId) {
    const stats = {};

    for (const [key, tableName] of Object.entries(this.definitionTables)) {
      try {
        const sql = `SELECT COUNT(*) as count FROM ${tableName} WHERE is_active = true`;
        const result = await this.client.executeNativeQuery(databaseId, sql, { enforcePrefix: false });
        stats[key] = result.data.rows[0][0];
      } catch (error) {
        stats[key] = 0;
        logger.warn(`Failed to get count for ${tableName}:`, error.message);
      }
    }

    return {
      summary: stats,
      total_definitions: Object.values(stats).reduce((sum, count) => sum + count, 0),
      last_updated: new Date().toISOString()
    };
  }
}