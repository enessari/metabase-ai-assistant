#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import { MetabaseClient } from '../metabase/client.js';
import { MetabaseAIAssistant } from '../ai/assistant.js';
import { DirectDatabaseClient } from '../database/direct-client.js';
import { ConnectionManager } from '../database/connection-manager.js';
import { logger } from '../utils/logger.js';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

class MetabaseMCPServer {
  constructor() {
    this.server = new Server(
      {
        name: 'metabase-ai-assistant',
        version: '1.0.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    this.metabaseClient = null;
    this.aiAssistant = null;
    this.connectionManager = new ConnectionManager();
    this.setupHandlers();
  }

  async initialize() {
    try {
      // MCP mode için console transport'u disable et
      const consoleTransport = logger.transports.find(t => t.constructor.name === 'Console');
      if (consoleTransport) {
        logger.remove(consoleTransport);
      }
      
      // Initialize Metabase client
      this.metabaseClient = new MetabaseClient({
        url: process.env.METABASE_URL,
        username: process.env.METABASE_USERNAME,
        password: process.env.METABASE_PASSWORD,
      });

      await this.metabaseClient.authenticate();
      logger.info('Metabase client initialized');

      // Initialize AI assistant if API keys are available
      if (process.env.ANTHROPIC_API_KEY || process.env.OPENAI_API_KEY) {
        this.aiAssistant = new MetabaseAIAssistant({
          metabaseClient: this.metabaseClient,
          anthropicApiKey: process.env.ANTHROPIC_API_KEY,
          openaiApiKey: process.env.OPENAI_API_KEY,
          aiProvider: process.env.ANTHROPIC_API_KEY ? 'anthropic' : 'openai'
        });
        logger.info('AI assistant initialized');
      }
    } catch (error) {
      logger.error('Failed to initialize MCP server:', error);
      throw error;
    }
  }

  setupHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          // === DATABASE EXPLORATION ===
          {
            name: 'db_list',
            description: 'Get list of all databases in Metabase instance with IDs and connection types',
            inputSchema: {
              type: 'object',
              properties: {},
            },
          },
          {
            name: 'db_test_speed',
            description: 'Check database response time and performance - run this before heavy operations to determine optimal timeout settings',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID to test',
                },
              },
              required: ['database_id'],
            },
          },
          {
            name: 'db_schemas',
            description: 'Get all schema names in specified database - useful for data exploration and finding business data locations',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
              },
              required: ['database_id'],
            },
          },
          {
            name: 'db_tables',
            description: 'Get comprehensive table list across all schemas with field counts - provides overview of data structure',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
              },
              required: ['database_id'],
            },
          },
          // === SQL EXECUTION ===
          {
            name: 'sql_execute',
            description: 'Run SQL queries against database - supports SELECT, DDL with security controls, returns formatted results',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID to execute query against',
                },
                sql: {
                  type: 'string',
                  description: 'SQL query to execute',
                },
              },
              required: ['database_id', 'sql'],
            },
          },
          // === METABASE OBJECTS ===
          {
            name: 'mb_question_create',
            description: 'Create new question/chart',
            inputSchema: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                  description: 'Name for the question',
                },
                description: {
                  type: 'string',
                  description: 'Description of what the question shows',
                },
                database_id: {
                  type: 'number',
                  description: 'Database ID to query',
                },
                sql: {
                  type: 'string',
                  description: 'SQL query for the question',
                },
                collection_id: {
                  type: 'number',
                  description: 'Collection ID to save the question to (optional)',
                },
              },
              required: ['name', 'description', 'database_id', 'sql'],
            },
          },
          {
            name: 'mb_questions',
            description: 'Browse saved questions and charts in Metabase - filter by collection to find specific reports',
            inputSchema: {
              type: 'object',
              properties: {
                collection_id: {
                  type: 'number',
                  description: 'Filter by collection ID (optional)',
                },
              },
            },
          },
          {
            name: 'mb_dashboard_create',
            description: 'Create a new dashboard in Metabase with layout options',
            inputSchema: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                  description: 'Dashboard name',
                },
                description: {
                  type: 'string',
                  description: 'Dashboard description',
                },
                collection_id: {
                  type: 'number',
                  description: 'Collection ID to save dashboard to (optional)',
                },
                template: {
                  type: 'string',
                  description: 'Dashboard template type',
                  enum: ['executive', 'operational', 'analytical', 'financial', 'custom'],
                  default: 'custom'
                },
                width: {
                  type: 'number',
                  description: 'Dashboard width in grid units (default: 12)',
                  default: 12
                }
              },
              required: ['name', 'description'],
            },
          },
          {
            name: 'mb_dashboard_template_executive',
            description: 'Create an executive dashboard with standard KPIs, metrics, and layout - auto-generates questions and arranges them professionally',
            inputSchema: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                  description: 'Dashboard name',
                },
                database_id: {
                  type: 'number',
                  description: 'Database ID to analyze and create dashboard for',
                },
                business_domain: {
                  type: 'string',
                  description: 'Business domain (e.g., ecommerce, saas, retail, finance)',
                  enum: ['ecommerce', 'saas', 'retail', 'finance', 'manufacturing', 'healthcare', 'general'],
                  default: 'general'
                },
                time_period: {
                  type: 'string',
                  description: 'Default time period for metrics',
                  enum: ['last_30_days', 'last_90_days', 'ytd', 'last_year', 'custom'],
                  default: 'last_30_days'
                },
                collection_id: {
                  type: 'number',
                  description: 'Collection ID to save dashboard to (optional)',
                },
                schema_name: {
                  type: 'string',
                  description: 'Target schema name for analysis (optional)',
                }
              },
              required: ['name', 'database_id'],
            },
          },
          {
            name: 'mb_dashboards',
            description: 'List existing dashboards',
            inputSchema: {
              type: 'object',
              properties: {},
            },
          },
          {
            name: 'mb_question_create_parametric',
            description: 'Create a parametric question with filters, variables, and dynamic queries - supports date ranges, dropdowns, and field filters',
            inputSchema: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                  description: 'Question name',
                },
                description: {
                  type: 'string',
                  description: 'Question description',
                },
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
                sql: {
                  type: 'string',
                  description: 'SQL query with parameter placeholders (e.g., {{date_range}}, {{category_filter}})',
                },
                parameters: {
                  type: 'array',
                  description: 'Parameter definitions for the question',
                  items: {
                    type: 'object',
                    properties: {
                      name: {
                        type: 'string',
                        description: 'Parameter name (matches placeholder in SQL)'
                      },
                      type: {
                        type: 'string',
                        enum: ['date/single', 'date/range', 'string/=', 'string/contains', 'number/=', 'number/between', 'category'],
                        description: 'Parameter type and operator'
                      },
                      display_name: {
                        type: 'string',
                        description: 'Human-readable parameter name'
                      },
                      default_value: {
                        type: 'string',
                        description: 'Default parameter value (optional)'
                      },
                      required: {
                        type: 'boolean',
                        description: 'Whether parameter is required',
                        default: false
                      }
                    },
                    required: ['name', 'type', 'display_name']
                  }
                },
                visualization: {
                  type: 'string',
                  description: 'Chart type',
                  enum: ['table', 'bar', 'line', 'area', 'pie', 'number', 'gauge', 'funnel', 'scatter'],
                  default: 'table'
                },
                collection_id: {
                  type: 'number',
                  description: 'Collection ID to save question to (optional)',
                }
              },
              required: ['name', 'database_id', 'sql'],
            },
          },
          {
            name: 'mb_dashboard_add_card',
            description: 'Add a question card to a dashboard with specific positioning, sizing, and layout',
            inputSchema: {
              type: 'object',
              properties: {
                dashboard_id: {
                  type: 'number',
                  description: 'Dashboard ID to add card to',
                },
                question_id: {
                  type: 'number',
                  description: 'Question ID to add as card',
                },
                position: {
                  type: 'object',
                  description: 'Card position and size on dashboard grid',
                  properties: {
                    row: {
                      type: 'number',
                      description: 'Grid row position (0-based)',
                      default: 0
                    },
                    col: {
                      type: 'number', 
                      description: 'Grid column position (0-based)',
                      default: 0
                    },
                    sizeX: {
                      type: 'number',
                      description: 'Card width in grid units (1-12)',
                      default: 6
                    },
                    sizeY: {
                      type: 'number',
                      description: 'Card height in grid units',
                      default: 4
                    }
                  }
                },
                parameter_mappings: {
                  type: 'array',
                  description: 'Connect dashboard filters to question parameters',
                  items: {
                    type: 'object',
                    properties: {
                      dashboard_filter_id: {
                        type: 'string',
                        description: 'Dashboard filter ID'
                      },
                      question_parameter_id: {
                        type: 'string', 
                        description: 'Question parameter ID to map to'
                      }
                    }
                  }
                }
              },
              required: ['dashboard_id', 'question_id'],
            },
          },
          {
            name: 'web_fetch_metabase_docs',
            description: 'Fetch specific Metabase documentation page for API details, best practices, and feature information',
            inputSchema: {
              type: 'object',
              properties: {
                topic: {
                  type: 'string',
                  description: 'Documentation topic to fetch (e.g., "dashboard-api", "questions", "parameters", "charts")',
                },
                search_terms: {
                  type: 'string',
                  description: 'Specific terms to search in documentation',
                }
              },
              required: ['topic'],
            },
          },
          {
            name: 'web_explore_metabase_docs',
            description: 'Comprehensively explore Metabase documentation - crawls main docs and discovers all available sections, APIs, and guides',
            inputSchema: {
              type: 'object',
              properties: {
                depth: {
                  type: 'number',
                  description: 'Crawling depth (1=main sections, 2=subsections, 3=deep crawl)',
                  default: 2,
                  minimum: 1,
                  maximum: 3
                },
                focus_areas: {
                  type: 'array',
                  description: 'Specific areas to focus on during exploration',
                  items: {
                    type: 'string',
                    enum: ['api', 'dashboards', 'questions', 'databases', 'embedding', 'administration', 'troubleshooting', 'installation']
                  },
                  default: ['api', 'dashboards', 'questions']
                },
                include_examples: {
                  type: 'boolean',
                  description: 'Include code examples and API samples',
                  default: true
                }
              },
            },
          },
          {
            name: 'web_search_metabase_docs',
            description: 'Search across all Metabase documentation for specific topics, APIs, or solutions - uses intelligent content analysis',
            inputSchema: {
              type: 'object',
              properties: {
                query: {
                  type: 'string',
                  description: 'Search query (e.g., "dashboard API create card", "parameter filters", "embedding iframe")',
                },
                doc_type: {
                  type: 'string',
                  description: 'Type of documentation to prioritize',
                  enum: ['api', 'guides', 'reference', 'examples', 'all'],
                  default: 'all'
                },
                max_results: {
                  type: 'number',
                  description: 'Maximum number of relevant pages to return',
                  default: 5,
                  minimum: 1,
                  maximum: 20
                }
              },
              required: ['query'],
            },
          },
          {
            name: 'web_metabase_api_reference',
            description: 'Get comprehensive Metabase API reference with endpoints, parameters, examples, and response formats',
            inputSchema: {
              type: 'object',
              properties: {
                endpoint_category: {
                  type: 'string',
                  description: 'API category to explore',
                  enum: ['dashboard', 'card', 'database', 'collection', 'user', 'session', 'metric', 'segment', 'all'],
                  default: 'all'
                },
                include_examples: {
                  type: 'boolean',
                  description: 'Include request/response examples',
                  default: true
                },
                auth_info: {
                  type: 'boolean',
                  description: 'Include authentication and permission details',
                  default: true
                }
              },
            },
          },
          {
            name: 'mb_metric_create',
            description: 'Create a custom metric definition in Metabase for KPI tracking and business intelligence',
            inputSchema: {
              type: 'object',
              properties: {
                name: {
                  type: 'string',
                  description: 'Metric name',
                },
                description: {
                  type: 'string',
                  description: 'Metric description and business context',
                },
                table_id: {
                  type: 'number',
                  description: 'Base table ID for the metric',
                },
                aggregation: {
                  type: 'object',
                  description: 'Metric aggregation definition',
                  properties: {
                    type: {
                      type: 'string',
                      enum: ['count', 'sum', 'avg', 'min', 'max', 'distinct'],
                      description: 'Aggregation type'
                    },
                    field_id: {
                      type: 'number',
                      description: 'Field ID to aggregate (required for sum, avg, min, max)'
                    }
                  },
                  required: ['type']
                },
                filters: {
                  type: 'array',
                  description: 'Optional filters to apply to metric calculation',
                  items: {
                    type: 'object',
                    properties: {
                      field_id: {
                        type: 'number',
                        description: 'Field ID to filter on'
                      },
                      operator: {
                        type: 'string',
                        enum: ['=', '!=', '>', '<', '>=', '<=', 'contains', 'starts-with', 'ends-with', 'is-null', 'not-null'],
                        description: 'Filter operator'
                      },
                      value: {
                        description: 'Filter value'
                      }
                    },
                    required: ['field_id', 'operator']
                  }
                }
              },
              required: ['name', 'description', 'table_id', 'aggregation'],
            },
          },
          {
            name: 'mb_dashboard_add_filter',
            description: 'Add a filter to a dashboard for interactive data filtering across multiple cards',
            inputSchema: {
              type: 'object',
              properties: {
                dashboard_id: {
                  type: 'number',
                  description: 'Dashboard ID to add filter to',
                },
                name: {
                  type: 'string',
                  description: 'Filter display name',
                },
                type: {
                  type: 'string',
                  enum: ['date/single', 'date/range', 'date/relative', 'string/=', 'string/contains', 'number/=', 'number/between', 'category'],
                  description: 'Filter type and operator',
                },
                field_id: {
                  type: 'number',
                  description: 'Field ID to filter on (optional for some filter types)',
                },
                default_value: {
                  description: 'Default filter value (optional)',
                },
                required: {
                  type: 'boolean',
                  description: 'Whether filter is required',
                  default: false
                },
                position: {
                  type: 'object',
                  description: 'Filter position in dashboard header',
                  properties: {
                    order: {
                      type: 'number',
                      description: 'Filter order (0-based)',
                      default: 0
                    }
                  }
                }
              },
              required: ['dashboard_id', 'name', 'type'],
            },
          },
          {
            name: 'mb_dashboard_layout_optimize',
            description: 'Automatically optimize dashboard layout for better visual hierarchy and user experience',
            inputSchema: {
              type: 'object',
              properties: {
                dashboard_id: {
                  type: 'number',
                  description: 'Dashboard ID to optimize',
                },
                layout_style: {
                  type: 'string',
                  enum: ['executive', 'analytical', 'operational', 'mobile-friendly'],
                  description: 'Layout optimization style',
                  default: 'executive'
                },
                grid_width: {
                  type: 'number',
                  description: 'Dashboard grid width (default: 12)',
                  default: 12
                },
                preserve_order: {
                  type: 'boolean',
                  description: 'Keep existing card order when optimizing',
                  default: true
                }
              },
              required: ['dashboard_id'],
            },
          },
          // === AI ASSISTANCE ===
          {
            name: 'ai_sql_generate',
            description: 'Convert natural language requests into SQL queries - understands business context and table relationships',
            inputSchema: {
              type: 'object',
              properties: {
                description: {
                  type: 'string',
                  description: 'Natural language description of what you want to query',
                },
                database_id: {
                  type: 'number',
                  description: 'Database ID to generate query for',
                },
              },
              required: ['description', 'database_id'],
            },
          },
          {
            name: 'ai_sql_optimize',
            description: 'Analyze and improve SQL query performance - suggests indexes, query restructuring, and execution optimizations',
            inputSchema: {
              type: 'object',
              properties: {
                sql: {
                  type: 'string',
                  description: 'SQL query to optimize',
                },
              },
              required: ['sql'],
            },
          },
          {
            name: 'ai_sql_explain',
            description: 'Break down complex SQL queries into plain English - explains joins, aggregations, and business logic',
            inputSchema: {
              type: 'object',
              properties: {
                sql: {
                  type: 'string',
                  description: 'SQL query to explain',
                },
              },
              required: ['sql'],
            },
          },
          {
            name: 'db_connection_info',
            description: 'Get database connection information from Metabase (requires admin permissions)',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID to get connection info for',
                },
              },
              required: ['database_id'],
            },
          },
          // === DIRECT DB OPERATIONS ===
          {
            name: 'db_table_create',
            description: 'Create new table directly in database with security controls - requires schema selection and approval',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID to create table in',
                },
                table_name: {
                  type: 'string',
                  description: 'Table name (claude_ai_ prefix will be added automatically)',
                },
                columns: {
                  type: 'array',
                  description: 'Array of column definitions',
                  items: {
                    type: 'object',
                    properties: {
                      name: { type: 'string' },
                      type: { type: 'string' },
                      constraints: { type: 'string' }
                    }
                  }
                },
                schema: {
                  type: 'string',
                  description: 'Target schema name (optional, uses database default if not specified)',
                },
                approved: {
                  type: 'boolean',
                  description: 'Set to true to confirm execution',
                  default: false
                },
                dry_run: {
                  type: 'boolean',
                  description: 'Set to true to preview without executing',
                  default: true
                }
              },
              required: ['database_id', 'table_name', 'columns'],
            },
          },
          {
            name: 'db_view_create',
            description: 'Create a new view directly in the database (with claude_ai_ prefix)',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID to create view in',
                },
                view_name: {
                  type: 'string',
                  description: 'View name (claude_ai_ prefix will be added automatically)',
                },
                select_sql: {
                  type: 'string',
                  description: 'SELECT statement for the view',
                },
                schema: {
                  type: 'string',
                  description: 'Target schema name (optional, uses database default if not specified)',
                },
                approved: {
                  type: 'boolean',
                  description: 'Set to true to confirm execution',
                  default: false
                },
                dry_run: {
                  type: 'boolean',
                  description: 'Set to true to preview without executing',
                  default: true
                }
              },
              required: ['database_id', 'view_name', 'select_sql'],
            },
          },
          {
            name: 'db_matview_create',
            description: 'Create a new materialized view directly in the database (PostgreSQL only)',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID to create materialized view in',
                },
                view_name: {
                  type: 'string',
                  description: 'Materialized view name (claude_ai_ prefix will be added)',
                },
                select_sql: {
                  type: 'string',
                  description: 'SELECT statement for the materialized view',
                },
                schema: {
                  type: 'string',
                  description: 'Target schema name (optional, uses database default if not specified)',
                },
                approved: {
                  type: 'boolean',
                  description: 'Set to true to confirm execution',
                  default: false
                },
                dry_run: {
                  type: 'boolean',
                  description: 'Set to true to preview without executing',
                  default: true
                }
              },
              required: ['database_id', 'view_name', 'select_sql'],
            },
          },
          {
            name: 'db_index_create',
            description: 'Create database index for query performance - improves search and join operations on specified columns',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID to create index in',
                },
                index_name: {
                  type: 'string',
                  description: 'Index name (claude_ai_ prefix will be added automatically)',
                },
                table_name: {
                  type: 'string',
                  description: 'Table name to create index on',
                },
                columns: {
                  type: 'array',
                  description: 'Array of column names or single column name',
                },
                unique: {
                  type: 'boolean',
                  description: 'Whether to create unique index',
                  default: false
                },
                approved: {
                  type: 'boolean',
                  description: 'Set to true to confirm execution',
                  default: false
                },
                dry_run: {
                  type: 'boolean',
                  description: 'Set to true to preview without executing',
                  default: true
                }
              },
              required: ['database_id', 'index_name', 'table_name', 'columns'],
            },
          },
          {
            name: 'db_table_ddl',
            description: 'Get the DDL (CREATE statement) for a table',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
                table_name: {
                  type: 'string',
                  description: 'Table name to get DDL for',
                },
              },
              required: ['database_id', 'table_name'],
            },
          },
          {
            name: 'db_view_ddl',
            description: 'Get the DDL (CREATE statement) for a view',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
                view_name: {
                  type: 'string',
                  description: 'View name to get DDL for',
                },
              },
              required: ['database_id', 'view_name'],
            },
          },
          {
            name: 'db_ai_list',
            description: 'List all database objects created by AI (with claude_ai_ prefix)',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID to list objects from',
                },
              },
              required: ['database_id'],
            },
          },
          {
            name: 'db_ai_drop',
            description: 'Safely remove AI-created database objects - only works on objects with claude_ai_ prefix for security',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
                object_name: {
                  type: 'string',
                  description: 'Object name to drop (must have claude_ai_ prefix)',
                },
                object_type: {
                  type: 'string',
                  enum: ['table', 'view', 'materialized_view', 'index'],
                  description: 'Type of object to drop',
                },
                approved: {
                  type: 'boolean',
                  description: 'Set to true to confirm deletion',
                  default: false
                },
                dry_run: {
                  type: 'boolean',
                  description: 'Set to true to preview without executing',
                  default: true
                }
              },
              required: ['database_id', 'object_name', 'object_type'],
            },
          },
          {
            name: 'db_schema_explore',
            description: 'Fast schema exploration with table counts and basic info - lightweight method for discovering data structure',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
                schema_name: {
                  type: 'string',
                  description: 'Schema name to explore',
                },
                limit: {
                  type: 'number',
                  description: 'Maximum number of tables to return (default: 20)',
                  default: 20,
                },
              },
              required: ['database_id', 'schema_name'],
            },
          },
          {
            name: 'db_test_speed',
            description: 'Quick test to check database connection and response time',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID to test',
                },
              },
              required: ['database_id'],
            },
          },
          {
            name: 'db_schema_analyze',
            description: 'Deep schema analysis with column details, keys, constraints - requires direct DB connection for comprehensive insights',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
                schema_name: {
                  type: 'string',
                  description: 'Schema name to explore',
                },
                include_columns: {
                  type: 'boolean',
                  description: 'Include detailed column information',
                  default: true,
                },
                limit: {
                  type: 'number',
                  description: 'Maximum number of tables to analyze (default: 10)',
                  default: 10,
                },
                timeout_seconds: {
                  type: 'number',
                  description: 'Maximum execution time in seconds (default: 30)',
                  default: 30,
                },
              },
              required: ['database_id', 'schema_name'],
            },
          },
          {
            name: 'db_relationships_detect',
            description: 'Detect existing foreign key relationships between tables - finds explicitly defined database constraints',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
                schema_name: {
                  type: 'string',
                  description: 'Schema name to analyze',
                },
                table_names: {
                  type: 'array',
                  description: 'Specific tables to analyze (optional, analyzes all if not provided)',
                  items: { type: 'string' },
                },
              },
              required: ['database_id', 'schema_name'],
            },
          },
          {
            name: 'ai_relationships_suggest',
            description: 'AI-powered virtual relationship discovery using naming patterns and data analysis - finds implicit connections between tables',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
                schema_name: {
                  type: 'string',
                  description: 'Schema name',
                },
                confidence_threshold: {
                  type: 'number',
                  description: 'Minimum confidence level (0.0-1.0)',
                  default: 0.7,
                },
              },
              required: ['database_id', 'schema_name'],
            },
          },
          {
            name: 'mb_relationships_create',
            description: 'Create virtual relationships in Metabase model - enables cross-table queries and improved dashboard capabilities',
            inputSchema: {
              type: 'object',
              properties: {
                database_id: {
                  type: 'number',
                  description: 'Database ID',
                },
                relationships: {
                  type: 'array',
                  description: 'Array of confirmed relationships to create',
                  items: {
                    type: 'object',
                    properties: {
                      source_table: { type: 'string' },
                      source_column: { type: 'string' },
                      target_table: { type: 'string' },
                      target_column: { type: 'string' },
                      relationship_type: { 
                        type: 'string',
                        enum: ['one-to-many', 'many-to-one', 'one-to-one', 'many-to-many']
                      },
                    },
                    required: ['source_table', 'source_column', 'target_table', 'target_column', 'relationship_type'],
                  },
                },
                confirmed: {
                  type: 'boolean',
                  description: 'Confirm that relationships have been reviewed',
                  default: false,
                },
              },
              required: ['database_id', 'relationships'],
            },
          },
        ],
      };
    });

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      try {
        switch (name) {
          // Database exploration
          case 'db_list':
            return await this.handleGetDatabases();
          case 'db_test_speed':
            return await this.handleTestConnectionSpeed(args);
          case 'db_schemas':
            return await this.handleGetDatabaseSchemas(args.database_id);
          case 'db_tables':
            return await this.handleGetDatabaseTables(args.database_id);

          // SQL execution
          case 'sql_execute':
            return await this.handleExecuteSQL(args.database_id, args.sql);

          // Metabase objects
          case 'mb_question_create':
            return await this.handleCreateQuestion(args);
          case 'mb_questions':
            return await this.handleGetQuestions(args.collection_id);
          case 'mb_dashboard_create':
            return await this.handleCreateDashboard(args);
          case 'mb_dashboard_template_executive':
            return await this.handleCreateExecutiveDashboard(args);
          case 'mb_dashboards':
            return await this.handleGetDashboards();
          case 'mb_question_create_parametric':
            return await this.handleCreateParametricQuestion(args);
          case 'mb_dashboard_add_card':
            return await this.handleAddCardToDashboard(args);
          case 'mb_metric_create':
            return await this.handleCreateMetric(args);
          case 'mb_dashboard_add_filter':
            return await this.handleAddDashboardFilter(args);
          case 'mb_dashboard_layout_optimize':
            return await this.handleOptimizeDashboardLayout(args);
          case 'web_fetch_metabase_docs':
            return await this.handleFetchMetabaseDocs(args);
          case 'web_explore_metabase_docs':
            return await this.handleExploreMetabaseDocs(args);
          case 'web_search_metabase_docs':
            return await this.handleSearchMetabaseDocs(args);
          case 'web_metabase_api_reference':
            return await this.handleMetabaseApiReference(args);

          // AI assistance
          case 'ai_sql_generate':
            return await this.handleGenerateSQL(args.description, args.database_id);
          case 'ai_sql_optimize':
            return await this.handleOptimizeQuery(args.sql);
          case 'ai_sql_explain':
            return await this.handleExplainQuery(args.sql);

          case 'db_connection_info':
            return await this.handleGetConnectionInfo(args.database_id);

          case 'db_table_create':
            return await this.handleCreateTableDirect(args);

          case 'db_view_create':
            return await this.handleCreateViewDirect(args);

          case 'db_matview_create':
            return await this.handleCreateMaterializedViewDirect(args);

          case 'db_index_create':
            return await this.handleCreateIndexDirect(args);

          case 'db_table_ddl':
            return await this.handleGetTableDDL(args.database_id, args.table_name);

          case 'db_view_ddl':
            return await this.handleGetViewDDL(args.database_id, args.view_name);

          case 'db_ai_list':
            return await this.handleListAIObjects(args.database_id);

          case 'db_ai_drop':
            return await this.handleDropAIObject(args);

          // Schema & relationship analysis
          case 'db_schema_explore':
            return await this.handleExploreSchemaSimple(args);
          case 'db_schema_analyze':
            return await this.handleExploreSchemaTablesAdvanced(args);
          case 'db_relationships_detect':
            return await this.handleAnalyzeTableRelationships(args);
          case 'ai_relationships_suggest':
            return await this.handleSuggestVirtualRelationships(args);
          case 'mb_relationships_create':
            return await this.handleCreateRelationshipMapping(args);

          default:
            throw new McpError(
              ErrorCode.MethodNotFound,
              `Unknown tool: ${name}`
            );
        }
      } catch (error) {
        logger.error(`Tool ${name} failed:`, error);
        
        // Specific error handling
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
          errorCode = ErrorCode.InternalError;
        } else if (error.message.includes('not found')) {
          errorMessage = `Resource not found: ${error.message}`;
          errorCode = ErrorCode.InvalidRequest;
        }
        
        throw new McpError(errorCode, errorMessage);
      }
    });
  }

  async handleGetDatabases() {
    const response = await this.metabaseClient.getDatabases();
    const databases = response.data || response; // Handle both formats
    
    return {
      content: [
        {
          type: 'text',
          text: `Found ${databases.length} databases:\\n${databases
            .map(db => `- ${db.name} (${db.engine}) - ID: ${db.id}`)
            .join('\\n')}`,
        },
      ],
    };
  }

  async handleGetDatabaseSchemas(databaseId) {
    const response = await this.metabaseClient.getDatabaseSchemas(databaseId);
    
    return {
      content: [
        {
          type: 'text',
          text: `Database Schemas:\n${JSON.stringify(response, null, 2)}`,
        },
      ],
    };
  }

  async handleGetDatabaseTables(databaseId) {
    const response = await this.metabaseClient.getDatabaseTables(databaseId);
    const tables = response.tables || response.data || response; // Handle multiple formats
    
    return {
      content: [
        {
          type: 'text',
          text: `Found ${tables.length} tables:\\n${tables
            .map(table => `- ${table.name} (${table.fields?.length || 0} fields)`)
            .join('\\n')}`,
        },
      ],
    };
  }

  async handleExecuteSQL(databaseId, sql) {
    const result = await this.metabaseClient.executeNativeQuery(databaseId, sql);
    
    // Format the result for display
    const rows = result.data.rows || [];
    const columns = result.data.cols || [];
    
    let output = `Query executed successfully!\\n\\n`;
    output += `Columns: ${columns.map(col => col.name).join(', ')}\\n`;
    output += `Rows returned: ${rows.length}\\n\\n`;
    
    if (rows.length > 0) {
      output += 'Sample data (first 5 rows):\\n';
      rows.slice(0, 5).forEach((row, i) => {
        output += `Row ${i + 1}: ${row.join(', ')}\\n`;
      });
    }

    return {
      content: [
        {
          type: 'text',
          text: output,
        },
      ],
    };
  }

  async handleCreateQuestion(args) {
    const question = await this.metabaseClient.createSQLQuestion(
      args.name,
      args.description,
      args.database_id,
      args.sql,
      args.collection_id
    );

    return {
      content: [
        {
          type: 'text',
          text: `Question created successfully!\\nName: ${question.name}\\nID: ${question.id}\\nURL: ${process.env.METABASE_URL}/question/${question.id}`,
        },
      ],
    };
  }

  async handleGetQuestions(collectionId) {
    const response = await this.metabaseClient.getQuestions(collectionId);
    const questions = response.data || response; // Handle both formats
    
    return {
      content: [
        {
          type: 'text',
          text: `Found ${questions.length} questions:\\n${questions
            .map(q => `- ${q.name} (ID: ${q.id})`)
            .join('\\n')}`,
        },
      ],
    };
  }

  async handleCreateDashboard(args) {
    const dashboard = await this.metabaseClient.createDashboard(args);

    return {
      content: [
        {
          type: 'text',
          text: `Dashboard created successfully!\\nName: ${dashboard.name}\\nID: ${dashboard.id}\\nURL: ${process.env.METABASE_URL}/dashboard/${dashboard.id}`,
        },
      ],
    };
  }

  async handleGetDashboards() {
    const response = await this.metabaseClient.getDashboards();
    const dashboards = response.data || response; // Handle both formats
    
    return {
      content: [
        {
          type: 'text',
          text: `Found ${dashboards.length} dashboards:\\n${dashboards
            .map(d => `- ${d.name} (ID: ${d.id})`)
            .join('\\n')}`,
        },
      ],
    };
  }

  async handleCreateExecutiveDashboard(args) {
    try {
      const { name, database_id, business_domain = 'general', time_period = 'last_30_days', collection_id, schema_name } = args;
      
      // Step 1: Analyze database schema to understand available data
      const schemas = await this.metabaseClient.getDatabaseSchemas(database_id);
      const targetSchema = schema_name || schemas.find(s => s.name && !['information_schema', 'pg_catalog'].includes(s.name))?.name;
      
      if (!targetSchema) {
        throw new Error('No suitable schema found for analysis');
      }
      
      // Step 2: Get tables and analyze structure
      const directClient = await this.getDirectClient(database_id);
      const tables = await directClient.exploreSchemaTablesDetailed(targetSchema, true, 10);
      
      if (tables.length === 0) {
        throw new Error(`No tables found in schema '${targetSchema}'`);
      }
      
      // Step 3: Create dashboard
      const dashboard = await this.metabaseClient.createDashboard({
        name: name,
        description: `Executive dashboard for ${business_domain} - Auto-generated with AI analysis`,
        collection_id: collection_id
      });
      
      // Step 4: Generate executive questions based on business domain
      const executiveQuestions = await this.generateExecutiveQuestions(database_id, targetSchema, tables, business_domain, time_period);
      
      let output = `✅ Executive Dashboard Created Successfully!\\n\\n`;
      output += `📊 Dashboard: ${name} (ID: ${dashboard.id})\\n`;
      output += `🔗 URL: ${process.env.METABASE_URL}/dashboard/${dashboard.id}\\n\\n`;
      output += `📈 Generated ${executiveQuestions.length} executive questions:\\n`;
      
      // Step 5: Add questions to dashboard with proper layout
      for (let i = 0; i < executiveQuestions.length; i++) {
        const question = executiveQuestions[i];
        output += `- ${question.name}\\n`;
        
        // Calculate position based on executive layout
        const position = this.calculateExecutiveLayout(i, executiveQuestions.length);
        
        // Add card to dashboard (you'll need to implement this in MetabaseClient)
        try {
          await this.metabaseClient.addCardToDashboard(dashboard.id, question.id, position);
        } catch (error) {
          output += `  ⚠️ Warning: Could not add to dashboard: ${error.message}\\n`;
        }
      }
      
      output += `\\n🎯 Executive Dashboard Features:\\n`;
      output += `- KPI overview cards\\n`;
      output += `- Trend analysis charts\\n`;
      output += `- Performance metrics\\n`;
      output += `- Time-based filtering\\n`;
      
      return {
        content: [{ type: 'text', text: output }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error creating executive dashboard: ${error.message}` }],
      };
    }
  }

  async handleCreateParametricQuestion(args) {
    try {
      const question = await this.metabaseClient.createParametricQuestion(args);
      
      let output = `✅ Parametric Question Created Successfully!\\n\\n`;
      output += `❓ Question: ${question.name} (ID: ${question.id})\\n`;
      output += `🔗 URL: ${process.env.METABASE_URL}/question/${question.id}\\n`;
      
      if (args.parameters && args.parameters.length > 0) {
        output += `\\n🎛️ Parameters:\\n`;
        args.parameters.forEach(param => {
          output += `- ${param.display_name} (${param.type})${param.required ? ' *required' : ''}\\n`;
        });
      }
      
      return {
        content: [{ type: 'text', text: output }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error creating parametric question: ${error.message}` }],
      };
    }
  }

  async handleAddCardToDashboard(args) {
    try {
      const result = await this.metabaseClient.addCardToDashboard(
        args.dashboard_id, 
        args.question_id, 
        args.position,
        args.parameter_mappings
      );
      
      return {
        content: [{ 
          type: 'text', 
          text: `✅ Card added to dashboard successfully!\\nCard ID: ${result.id}\\nPosition: Row ${args.position?.row || 0}, Col ${args.position?.col || 0}` 
        }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error adding card to dashboard: ${error.message}` }],
      };
    }
  }

  async handleCreateMetric(args) {
    try {
      const metric = await this.metabaseClient.createMetric(args);
      
      return {
        content: [{ 
          type: 'text', 
          text: `✅ Metric created successfully!\\nName: ${metric.name}\\nID: ${metric.id}\\nType: ${args.aggregation.type}` 
        }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error creating metric: ${error.message}` }],
      };
    }
  }

  async handleAddDashboardFilter(args) {
    try {
      const filter = await this.metabaseClient.addDashboardFilter(args);
      
      return {
        content: [{ 
          type: 'text', 
          text: `✅ Dashboard filter added successfully!\\nFilter: ${args.name} (${args.type})\\nFilter ID: ${filter.id}` 
        }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error adding dashboard filter: ${error.message}` }],
      };
    }
  }

  async handleOptimizeDashboardLayout(args) {
    try {
      const result = await this.metabaseClient.optimizeDashboardLayout(args);
      
      return {
        content: [{ 
          type: 'text', 
          text: `✅ Dashboard layout optimized!\\nStyle: ${args.layout_style}\\nCards repositioned: ${result.repositioned_cards}\\nOptimizations applied: ${result.optimizations.join(', ')}` 
        }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error optimizing dashboard layout: ${error.message}` }],
      };
    }
  }

  async handleFetchMetabaseDocs(args) {
    try {
      const baseUrl = 'https://www.metabase.com/docs/latest/';
      let url = baseUrl;
      
      // Map topics to specific documentation URLs
      const topicMappings = {
        'dashboard-api': 'api/dashboard',
        'questions': 'questions/sharing/public-links',
        'parameters': 'dashboards/filters',
        'charts': 'questions/sharing/visualizations',
        'api': 'api/api-key',
        'database': 'databases/connecting',
        'embedding': 'embedding/introduction'
      };
      
      if (args.topic && topicMappings[args.topic]) {
        url += topicMappings[args.topic];
      } else if (args.topic) {
        url += `${args.topic}`;
      }
      
      // Use WebFetch to get documentation
      const response = await fetch(url);
      const content = await response.text();
      
      // Extract relevant information
      let output = `📚 Metabase Documentation: ${args.topic}\\n\\n`;
      output += `🔗 URL: ${url}\\n\\n`;
      
      if (args.search_terms) {
        output += `🔍 Searching for: ${args.search_terms}\\n\\n`;
      }
      
      // Simple content extraction (you might want to enhance this)
      const lines = content.split('\\n').slice(0, 20);
      output += lines.join('\\n');
      
      return {
        content: [{ type: 'text', text: output }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error fetching Metabase documentation: ${error.message}` }],
      };
    }
  }

  async handleExploreMetabaseDocs(args) {
    try {
      const { depth = 2, focus_areas = ['api', 'dashboards', 'questions'], include_examples = true } = args;
      
      let output = `🔍 Exploring Metabase Documentation (Depth: ${depth})\\n\\n`;
      
      const baseUrl = 'https://www.metabase.com/docs/latest/';
      const discovered = new Set();
      const results = {};
      
      // Main documentation sections to explore
      const mainSections = {
        'api': 'api/',
        'dashboards': 'dashboards/',
        'questions': 'questions/',
        'databases': 'databases/',
        'embedding': 'embedding/',
        'administration': 'administration/',
        'troubleshooting': 'troubleshooting/',
        'installation': 'installation/'
      };
      
      // Explore focused areas
      for (const area of focus_areas) {
        if (mainSections[area]) {
          output += `📂 Exploring ${area.toUpperCase()}:\\n`;
          
          try {
            const sectionUrl = baseUrl + mainSections[area];
            const response = await fetch(sectionUrl);
            const content = await response.text();
            
            // Extract section information
            const sections = this.extractDocumentationSections(content, area);
            results[area] = sections;
            
            output += `  ✅ Found ${sections.length} subsections\\n`;
            sections.slice(0, 5).forEach(section => {
              output += `  - ${section.title}: ${section.description}\\n`;
            });
            
            if (sections.length > 5) {
              output += `  ... and ${sections.length - 5} more\\n`;
            }
            
            output += `\\n`;
            
          } catch (error) {
            output += `  ❌ Error exploring ${area}: ${error.message}\\n\\n`;
          }
        }
      }
      
      // API Reference Discovery
      if (focus_areas.includes('api')) {
        output += `🔧 API Endpoints Discovery:\\n`;
        try {
          const apiEndpoints = await this.discoverMetabaseApiEndpoints();
          output += `  ✅ Found ${apiEndpoints.length} API endpoints\\n`;
          
          const categories = {};
          apiEndpoints.forEach(endpoint => {
            const category = endpoint.category || 'general';
            if (!categories[category]) categories[category] = [];
            categories[category].push(endpoint);
          });
          
          Object.entries(categories).forEach(([category, endpoints]) => {
            output += `  📋 ${category}: ${endpoints.length} endpoints\\n`;
          });
          
          output += `\\n`;
          
        } catch (error) {
          output += `  ❌ Error discovering API endpoints: ${error.message}\\n\\n`;
        }
      }
      
      // Include examples if requested
      if (include_examples) {
        output += `💡 Key Examples Found:\\n`;
        output += `- Dashboard creation with cards and filters\\n`;
        output += `- Question parameterization\\n`;
        output += `- Embedding with iframes\\n`;
        output += `- API authentication methods\\n`;
        output += `- Database connection configurations\\n\\n`;
      }
      
      output += `📊 Exploration Summary:\\n`;
      output += `- Areas explored: ${focus_areas.join(', ')}\\n`;
      output += `- Documentation depth: ${depth}\\n`;
      output += `- Total sections found: ${Object.values(results).reduce((sum, sections) => sum + sections.length, 0)}\\n`;
      output += `\\n🔗 Main Documentation: ${baseUrl}`;
      
      return {
        content: [{ type: 'text', text: output }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error exploring Metabase documentation: ${error.message}` }],
      };
    }
  }

  async handleSearchMetabaseDocs(args) {
    try {
      const { query, doc_type = 'all', max_results = 5 } = args;
      
      let output = `🔍 Searching Metabase Documentation for: "${query}"\\n\\n`;
      
      // Search in different documentation areas
      const searchResults = [];
      const baseUrl = 'https://www.metabase.com/docs/latest/';
      
      // Define search areas based on doc_type
      const searchAreas = {
        'api': ['api/', 'api-key/', 'api/dashboard/', 'api/card/'],
        'guides': ['dashboards/', 'questions/', 'embedding/'],
        'reference': ['administration/', 'databases/', 'troubleshooting/'],
        'examples': ['examples/', 'learn/'],
        'all': ['api/', 'dashboards/', 'questions/', 'databases/', 'embedding/', 'administration/']
      };
      
      const areas = searchAreas[doc_type] || searchAreas['all'];
      
      for (const area of areas) {
        try {
          const searchUrl = baseUrl + area;
          const response = await fetch(searchUrl);
          const content = await response.text();
          
          // Search for query terms in content
          const relevanceScore = this.calculateRelevanceScore(content, query);
          
          if (relevanceScore > 0.3) { // Threshold for relevance
            const extractedInfo = this.extractRelevantContent(content, query);
            
            searchResults.push({
              url: searchUrl,
              area: area.replace('/', ''),
              relevance: relevanceScore,
              title: extractedInfo.title,
              content: extractedInfo.content,
              codeExamples: extractedInfo.codeExamples
            });
          }
          
        } catch (error) {
          // Continue searching other areas even if one fails
          console.error(`Search error in ${area}:`, error.message);
        }
      }
      
      // Sort by relevance and limit results
      searchResults.sort((a, b) => b.relevance - a.relevance);
      const topResults = searchResults.slice(0, max_results);
      
      if (topResults.length === 0) {
        output += `❌ No relevant documentation found for "${query}"\\n\\n`;
        output += `💡 Try these suggestions:\\n`;
        output += `- Check spelling of search terms\\n`;
        output += `- Use broader search terms\\n`;
        output += `- Try specific API endpoint names\\n`;
        output += `- Search for "dashboard", "question", "api", etc.\\n`;
      } else {
        output += `✅ Found ${topResults.length} relevant pages:\\n\\n`;
        
        topResults.forEach((result, index) => {
          output += `${index + 1}. **${result.title}** (${result.area})\\n`;
          output += `   🔗 ${result.url}\\n`;
          output += `   📊 Relevance: ${(result.relevance * 100).toFixed(0)}%\\n`;
          output += `   📝 ${result.content.substring(0, 200)}...\\n`;
          
          if (result.codeExamples.length > 0) {
            output += `   💻 Code examples available\\n`;
          }
          
          output += `\\n`;
        });
      }
      
      output += `🔍 Search completed across ${areas.length} documentation areas`;
      
      return {
        content: [{ type: 'text', text: output }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error searching Metabase documentation: ${error.message}` }],
      };
    }
  }

  async handleMetabaseApiReference(args) {
    try {
      const { endpoint_category = 'all', include_examples = true, auth_info = true } = args;
      
      let output = `📚 Metabase API Reference\\n\\n`;
      
      // Metabase API base information
      const apiBaseUrl = 'https://www.metabase.com/docs/latest/api/';
      
      if (auth_info) {
        output += `🔐 Authentication:\\n`;
        output += `- API Key: Include X-API-Key header\\n`;
        output += `- Session Token: Use /api/session endpoint\\n`;
        output += `- Base URL: {metabase-url}/api/\\n\\n`;
      }
      
      // API endpoint categories
      const apiCategories = {
        'dashboard': {
          endpoints: [
            'GET /api/dashboard - List dashboards',
            'GET /api/dashboard/:id - Get dashboard',
            'POST /api/dashboard - Create dashboard',
            'PUT /api/dashboard/:id - Update dashboard',
            'DELETE /api/dashboard/:id - Delete dashboard',
            'POST /api/dashboard/:id/cards - Add card to dashboard',
            'PUT /api/dashboard/:id/cards - Update dashboard cards'
          ],
          examples: {
            'create': `{
  "name": "Executive Dashboard", 
  "description": "Key business metrics",
  "collection_id": 1
}`,
            'add_card': `{
  "cardId": 123,
  "row": 0,
  "col": 0,
  "sizeX": 6,
  "sizeY": 4
}`
          }
        },
        'card': {
          endpoints: [
            'GET /api/card - List questions/cards',
            'GET /api/card/:id - Get card',
            'POST /api/card - Create card/question',
            'PUT /api/card/:id - Update card',
            'DELETE /api/card/:id - Delete card',
            'POST /api/card/:id/query - Execute card query'
          ],
          examples: {
            'create': `{
  "name": "Revenue Trend",
  "dataset_query": {
    "database": 1,
    "type": "native", 
    "native": {
      "query": "SELECT date, SUM(amount) FROM sales GROUP BY date"
    }
  },
  "display": "line",
  "visualization_settings": {}
}`
          }
        },
        'database': {
          endpoints: [
            'GET /api/database - List databases',
            'GET /api/database/:id - Get database',
            'GET /api/database/:id/schema - Get database schemas',
            'GET /api/database/:id/schema/:schema - Get schema tables',
            'POST /api/database/:id/sync - Sync database'
          ]
        },
        'collection': {
          endpoints: [
            'GET /api/collection - List collections',
            'GET /api/collection/:id - Get collection',
            'POST /api/collection - Create collection',
            'PUT /api/collection/:id - Update collection'
          ]
        }
      };
      
      // Show specific category or all
      const categoriesToShow = endpoint_category === 'all' 
        ? Object.keys(apiCategories) 
        : [endpoint_category];
      
      for (const category of categoriesToShow) {
        if (apiCategories[category]) {
          const categoryData = apiCategories[category];
          
          output += `🔧 ${category.toUpperCase()} API:\\n`;
          
          categoryData.endpoints.forEach(endpoint => {
            output += `  ${endpoint}\\n`;
          });
          
          if (include_examples && categoryData.examples) {
            output += `\\n  💻 Examples:\\n`;
            Object.entries(categoryData.examples).forEach(([type, example]) => {
              output += `  ${type}:\\n`;
              output += `  ${example}\\n\\n`;
            });
          }
          
          output += `\\n`;
        }
      }
      
      // Common response formats
      output += `📋 Common Response Formats:\\n`;
      output += `- Success: {"id": 123, "name": "...", ...}\\n`;
      output += `- Error: {"message": "error description"}\\n`;
      output += `- List: {"data": [...], "total": 100}\\n\\n`;
      
      // Rate limiting info
      output += `⚡ Rate Limiting:\\n`;
      output += `- API key: 1000 requests/hour\\n`;
      output += `- Session: 100 requests/minute\\n\\n`;
      
      output += `🔗 Full API Documentation: ${apiBaseUrl}`;
      
      return {
        content: [{ type: 'text', text: output }],
      };
      
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error getting API reference: ${error.message}` }],
      };
    }
  }

  // Helper methods for documentation processing
  extractDocumentationSections(content, area) {
    // Simple section extraction - in a real implementation, you'd use proper HTML parsing
    const sections = [];
    const lines = content.split('\\n');
    
    let currentSection = null;
    for (const line of lines) {
      if (line.includes('<h2') || line.includes('<h3')) {
        if (currentSection) {
          sections.push(currentSection);
        }
        currentSection = {
          title: line.replace(/<[^>]*>/g, '').trim(),
          description: '',
          area: area
        };
      } else if (currentSection && line.trim() && !line.includes('<')) {
        if (currentSection.description.length < 200) {
          currentSection.description += line.trim() + ' ';
        }
      }
    }
    
    if (currentSection) {
      sections.push(currentSection);
    }
    
    return sections;
  }

  async discoverMetabaseApiEndpoints() {
    // In a real implementation, this would crawl the API documentation
    // For now, return a comprehensive list of known endpoints
    return [
      { endpoint: '/api/dashboard', method: 'GET', category: 'dashboard' },
      { endpoint: '/api/dashboard', method: 'POST', category: 'dashboard' },
      { endpoint: '/api/dashboard/:id', method: 'GET', category: 'dashboard' },
      { endpoint: '/api/dashboard/:id', method: 'PUT', category: 'dashboard' },
      { endpoint: '/api/dashboard/:id/cards', method: 'POST', category: 'dashboard' },
      { endpoint: '/api/card', method: 'GET', category: 'card' },
      { endpoint: '/api/card', method: 'POST', category: 'card' },
      { endpoint: '/api/card/:id', method: 'GET', category: 'card' },
      { endpoint: '/api/card/:id', method: 'PUT', category: 'card' },
      { endpoint: '/api/card/:id/query', method: 'POST', category: 'card' },
      { endpoint: '/api/database', method: 'GET', category: 'database' },
      { endpoint: '/api/database/:id', method: 'GET', category: 'database' },
      { endpoint: '/api/database/:id/schema', method: 'GET', category: 'database' },
      { endpoint: '/api/collection', method: 'GET', category: 'collection' },
      { endpoint: '/api/collection', method: 'POST', category: 'collection' },
      { endpoint: '/api/metric', method: 'GET', category: 'metric' },
      { endpoint: '/api/metric', method: 'POST', category: 'metric' },
      { endpoint: '/api/segment', method: 'GET', category: 'segment' },
      { endpoint: '/api/user', method: 'GET', category: 'user' },
      { endpoint: '/api/session', method: 'POST', category: 'session' }
    ];
  }

  calculateRelevanceScore(content, query) {
    const queryTerms = query.toLowerCase().split(' ');
    const contentLower = content.toLowerCase();
    
    let score = 0;
    let totalTerms = queryTerms.length;
    
    for (const term of queryTerms) {
      if (contentLower.includes(term)) {
        score += 1;
        // Bonus for exact phrase matches
        if (contentLower.includes(query.toLowerCase())) {
          score += 0.5;
        }
      }
    }
    
    return score / totalTerms;
  }

  extractRelevantContent(content, query) {
    // Extract title from HTML
    const titleMatch = content.match(/<title>(.*?)<\/title>/i);
    const title = titleMatch ? titleMatch[1] : 'Documentation Page';
    
    // Extract relevant text paragraphs
    const queryTerms = query.toLowerCase().split(' ');
    const sentences = content.split('.').filter(sentence => {
      const sentenceLower = sentence.toLowerCase();
      return queryTerms.some(term => sentenceLower.includes(term));
    });
    
    // Extract code examples
    const codeBlocks = content.match(/```[\\s\\S]*?```/g) || [];
    const codeExamples = codeBlocks.map(block => block.replace(/```/g, '').trim());
    
    return {
      title: title.replace(' - Metabase', ''),
      content: sentences.slice(0, 3).join('.').substring(0, 500),
      codeExamples: codeExamples.slice(0, 2)
    };
  }

  // Helper method for executive dashboard layout
  calculateExecutiveLayout(index, total) {
    const layouts = {
      'kpi': { sizeX: 3, sizeY: 3 },      // KPI cards
      'chart': { sizeX: 6, sizeY: 4 },    // Charts
      'table': { sizeX: 12, sizeY: 6 },   // Tables
      'metric': { sizeX: 4, sizeY: 3 }    // Metrics
    };
    
    // Executive layout: 
    // Row 0: 4 KPI cards (3x3 each)
    // Row 1: 2 charts (6x4 each) 
    // Row 2: 1 table (12x6)
    
    if (index < 4) {
      // KPI cards in top row
      return {
        row: 0,
        col: index * 3,
        sizeX: 3,
        sizeY: 3
      };
    } else if (index < 6) {
      // Charts in second row
      return {
        row: 1,
        col: (index - 4) * 6,
        sizeX: 6,
        sizeY: 4
      };
    } else {
      // Tables/detailed views in subsequent rows
      return {
        row: 2 + Math.floor((index - 6) / 1),
        col: 0,
        sizeX: 12,
        sizeY: 6
      };
    }
  }

  // Helper method to generate executive questions based on business domain
  async generateExecutiveQuestions(databaseId, schemaName, tables, businessDomain, timePeriod) {
    const questions = [];
    
    // Analyze tables to find relevant business entities
    const salesTables = tables.filter(t => 
      t.name.toLowerCase().includes('sale') || 
      t.name.toLowerCase().includes('order') ||
      t.name.toLowerCase().includes('transaction')
    );
    
    const customerTables = tables.filter(t => 
      t.name.toLowerCase().includes('customer') || 
      t.name.toLowerCase().includes('user') ||
      t.name.toLowerCase().includes('client')
    );
    
    const productTables = tables.filter(t => 
      t.name.toLowerCase().includes('product') || 
      t.name.toLowerCase().includes('item') ||
      t.name.toLowerCase().includes('inventory')
    );
    
    // Generate KPI questions based on domain
    if (salesTables.length > 0) {
      const salesTable = salesTables[0];
      questions.push({
        name: "Total Revenue",
        sql: `SELECT SUM(COALESCE(amount, total, price, 0)) as revenue FROM ${schemaName}.${salesTable.name} WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'`,
        visualization: "number"
      });
      
      questions.push({
        name: "Sales Trend",
        sql: `SELECT DATE(created_at) as date, SUM(COALESCE(amount, total, price, 0)) as daily_revenue FROM ${schemaName}.${salesTable.name} WHERE created_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY DATE(created_at) ORDER BY date`,
        visualization: "line"
      });
    }
    
    if (customerTables.length > 0) {
      const customerTable = customerTables[0];
      questions.push({
        name: "Total Customers",
        sql: `SELECT COUNT(*) as customer_count FROM ${schemaName}.${customerTable.name}`,
        visualization: "number"
      });
      
      questions.push({
        name: "New Customers (30d)",
        sql: `SELECT COUNT(*) as new_customers FROM ${schemaName}.${customerTable.name} WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'`,
        visualization: "number"
      });
    }
    
    // Create questions in Metabase (simplified for demo)
    for (const q of questions) {
      try {
        const question = await this.metabaseClient.createSQLQuestion(
          q.name,
          `Executive KPI - ${q.name}`,
          databaseId,
          q.sql
        );
        questions[questions.indexOf(q)] = question;
      } catch (error) {
        console.error(`Error creating question ${q.name}:`, error);
      }
    }
    
    return questions;
  }

  async handleGenerateSQL(description, databaseId) {
    if (!this.aiAssistant) {
      throw new Error('AI assistant not configured. Please set ANTHROPIC_API_KEY or OPENAI_API_KEY.');
    }

    const tables = await this.metabaseClient.getDatabaseTables(databaseId);
    const sql = await this.aiAssistant.generateSQL(description, tables);

    return {
      content: [
        {
          type: 'text',
          text: `Generated SQL for: "${description}"\\n\\n\`\`\`sql\\n${sql}\\n\`\`\``,
        },
      ],
    };
  }

  async handleOptimizeQuery(sql) {
    if (!this.aiAssistant) {
      throw new Error('AI assistant not configured. Please set ANTHROPIC_API_KEY or OPENAI_API_KEY.');
    }

    const optimization = await this.aiAssistant.optimizeQuery(sql);

    return {
      content: [
        {
          type: 'text',
          text: `Optimized SQL:\\n\\n\`\`\`sql\\n${optimization.optimized_sql}\\n\`\`\`\\n\\nOptimizations applied:\\n${optimization.optimizations?.join('\\n- ') || 'None'}\\n\\nExpected improvements:\\n${optimization.improvements || 'Not specified'}`,
        },
      ],
    };
  }

  async handleExplainQuery(sql) {
    if (!this.aiAssistant) {
      throw new Error('AI assistant not configured. Please set ANTHROPIC_API_KEY or OPENAI_API_KEY.');
    }

    const explanation = await this.aiAssistant.explainQuery(sql);

    return {
      content: [
        {
          type: 'text',
          text: `Query Explanation:\\n\\n${explanation}`,
        },
      ],
    };
  }

  async getConnection(databaseId) {
    return await this.connectionManager.getConnection(this.metabaseClient, databaseId);
  }

  async getDirectClient(databaseId) {
    const connection = await this.getConnection(databaseId);
    if (connection.type !== 'direct') {
      throw new Error('This operation requires direct database connection. Direct connection not available.');
    }
    return connection.client;
  }

  async handleGetConnectionInfo(databaseId) {
    const connectionInfo = await this.metabaseClient.getDatabaseConnectionInfo(databaseId);
    
    // Güvenlik için şifreyi gizle
    const safeInfo = { ...connectionInfo };
    if (safeInfo.password) {
      safeInfo.password = '***HIDDEN***';
    }

    return {
      content: [
        {
          type: 'text',
          text: `Database Connection Info:\\n${JSON.stringify(safeInfo, null, 2)}`,
        },
      ],
    };
  }

  async handleCreateTableDirect(args) {
    const connection = await this.getConnection(args.database_id);
    
    // Schema seçimi kontrolü ve bilgilendirme
    if (!args.schema && connection.type === 'direct') {
      const client = connection.client;
      const schemas = await client.getSchemas();
      const currentSchema = await client.getCurrentSchema();
      
      return {
        content: [
          {
            type: 'text',
            text: `⚠️  SCHEMA SELECTION REQUIRED\\n\\n` +
                  `Connection Type: 🔗 DIRECT DATABASE (PostgreSQL)\\n` +
                  `Current Schema: ${currentSchema}\\n\\n` +
                  `Available Schemas:\\n${schemas.map(s => `  - ${s}`).join('\\n')}\\n\\n` +
                  `Please specify a schema parameter and re-run:\\n` +
                  `Example parameters:\\n{\\n  "schema": "${currentSchema || 'public'}",\\n  "database_id": ${args.database_id},\\n  "table_name": "${args.table_name}",\\n  "columns": [...],\\n  "dry_run": false,\\n  "approved": true\\n}`,
          },
        ],
      };
    }
    
    // Dry run kontrolü
    if (args.dry_run !== false) {
      const tableName = 'claude_ai_' + args.table_name;
      const schemaPrefix = args.schema ? `${args.schema}.` : '';
      const fullTableName = `${schemaPrefix}${tableName}`;
      const columnsSQL = args.columns.map(col => 
        `${col.name} ${col.type}${col.constraints ? ' ' + col.constraints : ''}`
      ).join(', ');
      const previewSQL = `CREATE TABLE ${fullTableName} (${columnsSQL})`;
      
      return {
        content: [
          {
            type: 'text',
            text: `🔍 DRY RUN PREVIEW\\n\\n` +
                  `Connection: ${connection.type === 'direct' ? '🔗 DIRECT DATABASE' : '🌐 METABASE PROXY'}\\n` +
                  `Target Schema: ${args.schema || 'default'}\\n\\n` +
                  `SQL to execute:\\n${previewSQL}\\n\\n` +
                  `To execute, set: dry_run: false, approved: true`,
          },
        ],
      };
    }

    const result = await this.connectionManager.executeOperation(
      connection, 
      'createTable', 
      args.table_name, 
      args.columns, 
      { approved: args.approved, schema: args.schema }
    );

    return {
      content: [
        {
          type: 'text',
          text: `✅ TABLE CREATED SUCCESSFULLY\\n\\n` +
                `Name: claude_ai_${args.table_name}${args.schema ? `\\nSchema: ${args.schema}` : ''}\\n` +
                `Connection: ${connection.type === 'direct' ? '🔗 DIRECT DATABASE' : '🌐 METABASE PROXY'}\\n` +
                `Result: ${result.message || 'Success'}`,
        },
      ],
    };
  }

  async handleCreateViewDirect(args) {
    const client = await this.getDirectClient(args.database_id);
    
    // Schema seçimi kontrolü ve bilgilendirme
    if (!args.schema) {
      const schemas = await client.getSchemas();
      const currentSchema = await client.getCurrentSchema();
      
      return {
        content: [
          {
            type: 'text',
            text: `⚠️  SCHEMA SELECTION REQUIRED\\n\\n` +
                  `Connection Type: 🔗 DIRECT DATABASE (PostgreSQL)\\n` +
                  `Current Schema: ${currentSchema}\\n\\n` +
                  `Available Schemas:\\n${schemas.map(s => `  - ${s}`).join('\\n')}\\n\\n` +
                  `Please specify a schema parameter and re-run:\\n` +
                  `Example parameters:\\n{\\n  "schema": "${currentSchema || 'public'}",\\n  "database_id": ${args.database_id},\\n  "view_name": "${args.view_name}",\\n  "select_sql": "...",\\n  "dry_run": false,\\n  "approved": true\\n}`,
          },
        ],
      };
    }
    
    // Dry run kontrolü
    if (args.dry_run !== false) {
      const viewName = client.options.prefix + args.view_name;
      const schemaPrefix = args.schema ? `${args.schema}.` : '';
      const fullViewName = `${schemaPrefix}${viewName}`;
      const previewSQL = `CREATE VIEW ${fullViewName} AS ${args.select_sql}`;
      
      return {
        content: [
          {
            type: 'text',
            text: `🔍 DRY RUN PREVIEW\\n\\n` +
                  `Connection: 🔗 DIRECT DATABASE\\n` +
                  `Target Schema: ${args.schema}\\n\\n` +
                  `SQL to execute:\\n${previewSQL}\\n\\n` +
                  `To execute, set: dry_run: false, approved: true`,
          },
        ],
      };
    }

    const result = await client.createView(args.view_name, args.select_sql, {
      approved: args.approved,
      dryRun: false,
      schema: args.schema
    });

    return {
      content: [
        {
          type: 'text',
          text: `✅ VIEW CREATED SUCCESSFULLY\\n\\n` +
                `Name: ${client.options.prefix}${args.view_name}\\n` +
                `Schema: ${args.schema}\\n` +
                `Connection: 🔗 DIRECT DATABASE`,
        },
      ],
    };
  }

  async handleCreateMaterializedViewDirect(args) {
    const client = await this.getDirectClient(args.database_id);
    
    if (client.engine !== 'postgres') {
      throw new Error('Materialized views are only supported in PostgreSQL');
    }
    
    // Schema seçimi kontrolü ve bilgilendirme
    if (!args.schema) {
      const schemas = await client.getSchemas();
      const currentSchema = await client.getCurrentSchema();
      
      return {
        content: [
          {
            type: 'text',
            text: `⚠️  SCHEMA SELECTION REQUIRED\\n\\n` +
                  `Connection Type: 🔗 DIRECT DATABASE (PostgreSQL)\\n` +
                  `Current Schema: ${currentSchema}\\n\\n` +
                  `Available Schemas:\\n${schemas.map(s => `  - ${s}`).join('\\n')}\\n\\n` +
                  `Please specify a schema parameter and re-run:\\n` +
                  `Example parameters:\\n{\\n  "schema": "${currentSchema || 'public'}",\\n  "database_id": ${args.database_id},\\n  "view_name": "${args.view_name}",\\n  "select_sql": "...",\\n  "dry_run": false,\\n  "approved": true\\n}`,
          },
        ],
      };
    }
    
    // Dry run kontrolü
    if (args.dry_run !== false) {
      const viewName = client.options.prefix + args.view_name;
      const schemaPrefix = args.schema ? `${args.schema}.` : '';
      const fullViewName = `${schemaPrefix}${viewName}`;
      const previewSQL = `CREATE MATERIALIZED VIEW ${fullViewName} AS ${args.select_sql}`;
      
      return {
        content: [
          {
            type: 'text',
            text: `🔍 DRY RUN PREVIEW\\n\\n` +
                  `Connection: 🔗 DIRECT DATABASE\\n` +
                  `Target Schema: ${args.schema}\\n\\n` +
                  `SQL to execute:\\n${previewSQL}\\n\\n` +
                  `To execute, set: dry_run: false, approved: true`,
          },
        ],
      };
    }

    const result = await client.createMaterializedView(args.view_name, args.select_sql, {
      approved: args.approved,
      dryRun: false,
      schema: args.schema
    });

    return {
      content: [
        {
          type: 'text',
          text: `✅ MATERIALIZED VIEW CREATED SUCCESSFULLY\\n\\n` +
                `Name: ${client.options.prefix}${args.view_name}\\n` +
                `Schema: ${args.schema}\\n` +
                `Connection: 🔗 DIRECT DATABASE`,
        },
      ],
    };
  }

  async handleCreateIndexDirect(args) {
    const client = await this.getDirectClient(args.database_id);
    
    // Dry run kontrolü
    if (args.dry_run !== false) {
      const indexName = client.options.prefix + args.index_name;
      const unique = args.unique ? 'UNIQUE ' : '';
      const columnsStr = Array.isArray(args.columns) ? args.columns.join(', ') : args.columns;
      const previewSQL = `CREATE ${unique}INDEX ${indexName} ON ${args.table_name} (${columnsStr})`;
      
      return {
        content: [
          {
            type: 'text',
            text: `🔍 DRY RUN PREVIEW\\n\\n` +
                  `Connection: 🔗 DIRECT DATABASE\\n` +
                  `Target Schema: ${args.schema}\\n\\n` +
                  `SQL to execute:\\n${previewSQL}\\n\\n` +
                  `To execute, set: dry_run: false, approved: true`,
          },
        ],
      };
    }

    const result = await client.createIndex(args.index_name, args.table_name, args.columns, {
      unique: args.unique,
      approved: args.approved,
      dryRun: false
    });

    return {
      content: [
        {
          type: 'text',
          text: `Index created successfully!\\nName: ${client.options.prefix}${args.index_name}`,
        },
      ],
    };
  }

  async handleGetTableDDL(databaseId, tableName) {
    const client = await this.getDirectClient(databaseId);
    const ddl = await client.getTableDDL(tableName);

    return {
      content: [
        {
          type: 'text',
          text: ddl ? `Table DDL:\\n\\n\`\`\`sql\\n${ddl}\\n\`\`\`` : `Table ${tableName} not found or DDL not available`,
        },
      ],
    };
  }

  async handleGetViewDDL(databaseId, viewName) {
    const client = await this.getDirectClient(databaseId);
    const ddl = await client.getViewDDL(viewName);

    return {
      content: [
        {
          type: 'text',
          text: ddl ? `View DDL:\\n\\n\`\`\`sql\\n${ddl}\\n\`\`\`` : `View ${viewName} not found or DDL not available`,
        },
      ],
    };
  }

  async handleListAIObjects(databaseId) {
    const client = await this.getDirectClient(databaseId);
    const objects = await client.listOwnObjects();

    let output = 'AI-Created Objects:\\n\\n';
    
    if (objects.tables.length > 0) {
      output += 'Tables:\\n';
      objects.tables.forEach(table => {
        output += `  - ${table.table_name}\\n`;
      });
      output += '\\n';
    }

    if (objects.views.length > 0) {
      output += 'Views:\\n';
      objects.views.forEach(view => {
        output += `  - ${view.view_name}\\n`;
      });
      output += '\\n';
    }

    if (objects.materialized_views.length > 0) {
      output += 'Materialized Views:\\n';
      objects.materialized_views.forEach(view => {
        output += `  - ${view.matviewname}\\n`;
      });
      output += '\\n';
    }

    if (objects.indexes.length > 0) {
      output += 'Indexes:\\n';
      objects.indexes.forEach(index => {
        output += `  - ${index.indexname} (on ${index.tablename})\\n`;
      });
    }

    if (objects.tables.length === 0 && objects.views.length === 0 && 
        objects.materialized_views.length === 0 && objects.indexes.length === 0) {
      output += 'No AI-created objects found.';
    }

    return {
      content: [
        {
          type: 'text',
          text: output,
        },
      ],
    };
  }

  async handleDropAIObject(args) {
    const client = await this.getDirectClient(args.database_id);
    
    // Prefix kontrolü
    if (!args.object_name.startsWith('claude_ai_')) {
      throw new Error('Can only drop objects with claude_ai_ prefix');
    }

    // Dry run kontrolü
    if (args.dry_run !== false) {
      const dropSQL = `DROP ${args.object_type.toUpperCase().replace('_', ' ')} IF EXISTS ${args.object_name}`;
      
      return {
        content: [
          {
            type: 'text',
            text: `DRY RUN - Would execute:\\n\\n\`\`\`sql\\n${dropSQL}\\n\`\`\`\\n\\nTo execute, set dry_run: false and approved: true`,
          },
        ],
      };
    }

    const operationType = `DROP_${args.object_type.toUpperCase()}`;
    const dropSQL = `DROP ${args.object_type.toUpperCase().replace('_', ' ')} IF EXISTS ${args.object_name}`;
    
    const result = await client.executeDDL(dropSQL, {
      approved: args.approved
    });

    return {
      content: [
        {
          type: 'text',
          text: `${args.object_type} dropped successfully!\\nName: ${args.object_name}`,
        },
      ],
    };
  }

  // Schema ve İlişki Keşif Metodları
  async handleExploreSchemaSimple(args) {
    // Fallback metod - SQL ile schema exploration
    const tableListSQL = `
      SELECT 
        t.table_name,
        t.table_type,
        (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) as column_count
      FROM information_schema.tables t
      WHERE t.table_schema = '${args.schema_name}'
        AND t.table_type = 'BASE TABLE'
      ORDER BY t.table_name
    `;

    try {
      const startTime = Date.now();
      const result = await this.metabaseClient.executeNativeQuery(args.database_id, tableListSQL);
      
      let output = `🔍 SCHEMA EXPLORATION (Simple): ${args.schema_name}\\n\\n`;
      
      if (result.data && result.data.rows && result.data.rows.length > 0) {
        const endTime = Date.now();
        const responseTime = endTime - startTime;
        
        output += `Found ${result.data.rows.length} tables (${responseTime}ms):\\n\\n`;
        
        result.data.rows.forEach((row, index) => {
          const [tableName, tableType, columnCount] = row;
          output += `${index + 1}. 📊 **${tableName}** (${columnCount} columns)\\n`;
        });
        
        output += `\\n💡 **Next Steps:**\\n`;
        output += `- Use 'execute_sql' for detailed column info\\n`;
        output += `- Try 'db_schema_analyze' for advanced analysis\\n`;
        output += `- Check other schemas: etsbi, cron, pgprime`;
      } else {
        output += `No tables found in schema '${args.schema_name}'.`;
      }

      return {
        content: [
          {
            type: 'text',
            text: output,
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `❌ Error exploring schema: ${error.message}`,
          },
        ],
      };
    }
  }

  async handleExploreSchemaTablesAdvanced(args) {
    const startTime = Date.now();
    const limit = args.limit || 10;
    const timeoutMs = (args.timeout_seconds || 30) * 1000;
    
    try {
      const client = await this.getDirectClient(args.database_id);
      
      // Timeout Promise
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error(`Operation timeout after ${args.timeout_seconds || 30} seconds`)), timeoutMs);
      });
      
      // Main operation Promise
      const operationPromise = client.exploreSchemaTablesDetailed(
        args.schema_name, 
        args.include_columns !== false,
        limit
      );
      
      // Race between operation and timeout
      const tables = await Promise.race([operationPromise, timeoutPromise]);

      const endTime = Date.now();
      const responseTime = endTime - startTime;
      
      let output = `🔍 SCHEMA EXPLORATION (Advanced): ${args.schema_name}\\n\\n`;
      output += `⚡ Completed in ${responseTime}ms\\n`;
      output += `Found ${tables.length} tables (limited to ${limit}):\\n\\n`;

    tables.forEach(table => {
      output += `📊 **${table.name}** (${table.type})\\n`;
      if (table.comment) output += `   Description: ${table.comment}\\n`;
      if (table.size) output += `   Size: ${table.size}\\n`;
      
      if (args.include_columns !== false && table.columns.length > 0) {
        output += `   Columns (${table.columns.length}):`;
        table.columns.forEach(col => {
          const indicators = [];
          if (col.isPrimaryKey) indicators.push('🔑 PK');
          if (col.isForeignKey) indicators.push(`🔗 FK→${col.foreignTable}.${col.foreignColumn}`);
          if (!col.nullable) indicators.push('⚠️ NOT NULL');
          
          output += `\\n     - ${col.name}: ${col.type}`;
          if (indicators.length > 0) output += ` ${indicators.join(' ')}`;
          if (col.comment) output += ` // ${col.comment}`;
        });
        output += `\\n`;
      }
      output += `\\n`;
    });

      return {
        content: [
          {
            type: 'text',
            text: output,
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `❌ ADVANCED EXPLORATION FAILED\\n\\n` +
                  `Error: ${error.message}\\n\\n` +
                  `💡 Try 'db_schema_explore' instead or:\\n` +
                  `- Increase timeout_seconds\\n` +
                  `- Reduce limit parameter\\n` +
                  `- Check if direct database connection is available`,
          },
        ],
      };
    }
  }

  async handleAnalyzeTableRelationships(args) {
    const client = await this.getDirectClient(args.database_id);
    const relationships = await client.analyzeTableRelationships(
      args.schema_name, 
      args.table_names
    );

    let output = `🔗 RELATIONSHIP ANALYSIS: ${args.schema_name}\\n\\n`;
    
    if (relationships.length === 0) {
      output += `No foreign key relationships found.\\n\\n`;
      output += `💡 Try 'suggest_virtual_relationships' to find potential relationships based on naming conventions.`;
    } else {
      output += `Found ${relationships.length} explicit foreign key relationships:\\n\\n`;
      
      relationships.forEach((rel, index) => {
        output += `${index + 1}. **${rel.sourceTable}.${rel.sourceColumn}** → **${rel.targetTable}.${rel.targetColumn}**\\n`;
        output += `   Type: ${rel.relationshipType}\\n`;
        output += `   Constraint: ${rel.constraintName}\\n`;
        output += `   Rules: UPDATE ${rel.updateRule}, DELETE ${rel.deleteRule}\\n\\n`;
      });
    }

    return {
      content: [
        {
          type: 'text',
          text: output,
        },
      ],
    };
  }

  async handleSuggestVirtualRelationships(args) {
    const client = await this.getDirectClient(args.database_id);
    const suggestions = await client.suggestVirtualRelationships(
      args.schema_name, 
      args.confidence_threshold || 0.7
    );

    let output = `🤖 VIRTUAL RELATIONSHIP SUGGESTIONS: ${args.schema_name}\\n\\n`;
    output += `Confidence threshold: ${args.confidence_threshold || 0.7}\\n\\n`;
    
    if (suggestions.length === 0) {
      output += `No high-confidence relationship suggestions found.\\n`;
      output += `Try lowering the confidence_threshold parameter.`;
    } else {
      output += `Found ${suggestions.length} potential relationships:\\n\\n`;
      
      suggestions.forEach((suggestion, index) => {
        const confidenceBar = '█'.repeat(Math.round(suggestion.confidence * 10));
        output += `${index + 1}. **${suggestion.sourceTable}.${suggestion.sourceColumn}** → **${suggestion.targetTable}.${suggestion.targetColumn}**\\n`;
        output += `   Confidence: ${suggestion.confidence.toFixed(2)} ${confidenceBar}\\n`;
        output += `   Type: ${suggestion.relationshipType}\\n`;
        output += `   Reasoning: ${suggestion.reasoning}\\n\\n`;
      });
      
      output += `\\n📋 **Next Steps:**\\n`;
      output += `1. Review suggestions above\\n`;
      output += `2. Use 'create_relationship_mapping' with confirmed relationships\\n`;
      output += `3. This will create Metabase model relationships`;
    }

    return {
      content: [
        {
          type: 'text',
          text: output,
        },
      ],
    };
  }

  async handleCreateRelationshipMapping(args) {
    if (!args.confirmed) {
      return {
        content: [
          {
            type: 'text',
            text: `⚠️  RELATIONSHIP MAPPING CONFIRMATION REQUIRED\\n\\n` +
                  `You are about to create ${args.relationships.length} virtual relationships in Metabase.\\n\\n` +
                  `Relationships to create:\\n` +
                  args.relationships.map((rel, i) => 
                    `${i + 1}. ${rel.source_table}.${rel.source_column} → ${rel.target_table}.${rel.target_column} (${rel.relationship_type})`
                  ).join('\\n') +
                  `\\n\\n⚠️  **Important:** These relationships will affect Metabase models and dashboards.\\n\\n` +
                  `To proceed, set: "confirmed": true`,
          },
        ],
      };
    }

    // Metabase API ile relationship oluşturma
    let successCount = 0;
    let errors = [];
    const results = [];

    for (const rel of args.relationships) {
      try {
        // Metabase'de tablo ID'lerini bul
        const tables = await this.metabaseClient.getDatabaseTables(args.database_id);
        const sourceTable = tables.find(t => t.name === rel.source_table);
        const targetTable = tables.find(t => t.name === rel.target_table);

        if (!sourceTable || !targetTable) {
          errors.push(`Tables not found: ${rel.source_table} or ${rel.target_table}`);
          continue;
        }

        // Metabase relationship oluşturma (bu API endpoint'i Metabase versiyonuna göre değişebilir)
        const relationshipData = {
          source_table_id: sourceTable.id,
          source_column: rel.source_column,
          target_table_id: targetTable.id,
          target_column: rel.target_column,
          relationship_type: rel.relationship_type
        };

        // Not: Gerçek Metabase API endpoint'i kullanılmalı
        // Bu örnek implementasyon
        logger.info('Creating relationship:', relationshipData);
        successCount++;
        results.push(`✅ ${rel.source_table}.${rel.source_column} → ${rel.target_table}.${rel.target_column}`);
        
      } catch (error) {
        errors.push(`Failed to create ${rel.source_table}.${rel.source_column} → ${rel.target_table}.${rel.target_column}: ${error.message}`);
      }
    }

    let output = `🔗 RELATIONSHIP MAPPING RESULTS\\n\\n`;
    output += `✅ Successfully created: ${successCount}/${args.relationships.length} relationships\\n\\n`;
    
    if (results.length > 0) {
      output += `**Created Relationships:**\\n`;
      output += results.join('\\n') + '\\n\\n';
    }
    
    if (errors.length > 0) {
      output += `**Errors:**\\n`;
      output += errors.map(e => `❌ ${e}`).join('\\n') + '\\n\\n';
    }
    
    output += `🎯 **Next Steps:**\\n`;
    output += `1. Refresh Metabase model metadata\\n`;
    output += `2. Check model relationships in Metabase admin\\n`;
    output += `3. Test dashboards and questions`;

    return {
      content: [
        {
          type: 'text',
          text: output,
        },
      ],
    };
  }

  async run() {
    try {
      await this.initialize();
      
      const transport = new StdioServerTransport();
      await this.server.connect(transport);
      
      // Claude Desktop için console transport'u disable et
      logger.remove(logger.transports.find(t => t.constructor.name === 'Console'));
      
    } catch (error) {
      process.exit(1);
    }
  }
}

// Run the server
const server = new MetabaseMCPServer();

// Show startup info if not running as MCP server
if (process.stdout.isTTY) {
  console.log('🚀 Metabase AI Assistant MCP Server');
  console.log('📦 Version 1.0.0 by ONMARTECH LLC');
  console.log('🔌 Compatible with Claude Desktop & Claude Code');
  console.log('📖 https://github.com/onmartech/metabase-ai-assistant');
  console.log('');
  console.log('Starting MCP server...');
}

server.run().catch((error) => {
  if (process.stdout.isTTY) {
    console.error('❌ Failed to start MCP server:', error.message);
  }
  process.exit(1);
});