/**
 * Tool Registry - All MCP tool definitions
 * Extracted from server.js for modularity
 * 
 * MCP Spec 2025-11-25 compliant:
 * - Tool annotations (readOnlyHint, destructiveHint, idempotentHint, openWorldHint)
 * - Human-readable title fields
 * - inputSchema improvements (additionalProperties: false for no-param tools)
 */

/**
 * Metadata map: title + annotation hints for every tool.
 * Tools not listed here get default annotations (readOnly=true).
 * destructiveHint=true means data loss is possible if misused.
 */
const TOOL_METADATA = {
  // ── Database Exploration (read-only) ──
  db_list: {
    title: 'List Databases', outputSchema: {
      type: 'object',
      properties: {
        databases: {
          type: 'array', items: {
            type: 'object', properties: {
              id: { type: 'number' }, name: { type: 'string' }, engine: { type: 'string' }
            }, required: ['id', 'name', 'engine']
          }
        },
        source: { type: 'string', description: 'cache or live' }
      }, required: ['databases']
    }
  },
  db_test_speed: {
    title: 'Test Database Speed', outputSchema: {
      type: 'object',
      properties: {
        database_id: { type: 'number' }, latency_ms: { type: 'number' },
        status: { type: 'string' }, details: { type: 'object' }
      }, required: ['database_id', 'latency_ms', 'status']
    }
  },
  db_schemas: {
    title: 'List Database Schemas', outputSchema: {
      type: 'object',
      properties: {
        database_id: { type: 'number' },
        schemas: { type: 'array', items: { type: 'string' } }
      }, required: ['schemas']
    }
  },
  db_tables: {
    title: 'List Database Tables', outputSchema: {
      type: 'object',
      properties: {
        database_id: { type: 'number' },
        tables: {
          type: 'array', items: {
            type: 'object', properties: {
              id: { type: 'number' }, name: { type: 'string' }, schema: { type: 'string' }
            }, required: ['id', 'name']
          }
        }
      }, required: ['tables']
    }
  },
  db_table_profile: {
    title: 'Profile Table', outputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string' }, row_count: { type: 'number' },
        columns: {
          type: 'array', items: {
            type: 'object', properties: {
              name: { type: 'string' }, type: { type: 'string' }
            }
          }
        },
        sample: { type: 'array' }
      }, required: ['table', 'columns']
    }
  },
  db_connection_info: {
    title: 'Get Connection Info', outputSchema: {
      type: 'object',
      properties: {
        id: { type: 'number' }, name: { type: 'string' },
        engine: { type: 'string' }, details: { type: 'object' }
      }, required: ['id', 'name', 'engine']
    }
  },
  db_table_ddl: { title: 'Get Table DDL' },
  db_view_ddl: { title: 'Get View DDL' },
  db_ai_list: { title: 'List AI Objects' },
  db_schema_explore: { title: 'Explore Schema' },
  db_schema_analyze: { title: 'Analyze Schema' },
  db_relationships_detect: { title: 'Detect Relationships' },
  db_table_stats: { title: 'Get Table Statistics' },
  db_index_usage: { title: 'Get Index Usage' },
  db_query_explain: { title: 'Explain Query Plan' },

  // ── SQL Execution (mixed read/write) ──
  sql_execute: {
    title: 'Execute SQL', write: true, destructive: false, idempotent: false, outputSchema: {
      type: 'object',
      properties: {
        columns: {
          type: 'array', items: {
            type: 'object', properties: {
              name: { type: 'string' }, base_type: { type: 'string' }
            }
          }
        },
        rows: { type: 'array', items: { type: 'array' } },
        row_count: { type: 'number' },
        execution_time_ms: { type: 'number' },
        truncated: { type: 'boolean' }
      }, required: ['columns', 'rows', 'row_count', 'execution_time_ms']
    }
  },
  sql_submit: { title: 'Submit Async SQL', write: true, destructive: false, idempotent: false },
  sql_status: {
    title: 'Check SQL Job Status', outputSchema: {
      type: 'object',
      properties: {
        job_id: { type: 'string' }, status: { type: 'string' },
        submitted_at: { type: 'string' },
        result: { type: 'object' }
      }, required: ['job_id', 'status']
    }
  },
  sql_cancel: { title: 'Cancel SQL Job', write: true, destructive: false, idempotent: true },

  // ── DDL Operations (write, destructive) ──
  db_table_create: { title: 'Create Table', write: true, destructive: false, idempotent: false },
  db_view_create: { title: 'Create View', write: true, destructive: false, idempotent: false },
  db_matview_create: { title: 'Create Materialized View', write: true, destructive: false, idempotent: false },
  db_index_create: { title: 'Create Index', write: true, destructive: false, idempotent: false },
  db_ai_drop: { title: 'Drop AI Object', write: true, destructive: true, idempotent: true },
  db_vacuum_analyze: { title: 'Vacuum & Analyze', write: true, destructive: false, idempotent: true },

  // ── AI Features (read-only) ──
  ai_sql_generate: { title: 'Generate SQL with AI' },
  ai_sql_optimize: { title: 'Optimize SQL with AI' },
  ai_sql_explain: { title: 'Explain SQL with AI' },
  ai_relationships_suggest: { title: 'Suggest Relationships with AI' },
  mb_auto_describe: { title: 'Auto-Describe with AI' },

  // ── Cards / Questions ──
  mb_question_create: { title: 'Create Question', write: true, destructive: false, idempotent: false },
  mb_questions: {
    title: 'List Questions', outputSchema: {
      type: 'object',
      properties: {
        questions: {
          type: 'array', items: {
            type: 'object', properties: {
              id: { type: 'number' }, name: { type: 'string' }
            }, required: ['id', 'name']
          }
        },
        count: { type: 'number' }
      }, required: ['questions']
    }
  },
  mb_question_create_parametric: { title: 'Create Parametric Question', write: true, destructive: false, idempotent: false },
  mb_card_get: {
    title: 'Get Card', outputSchema: {
      type: 'object',
      properties: {
        id: { type: 'number' }, name: { type: 'string' },
        description: { type: 'string' }, display: { type: 'string' },
        database_id: { type: 'number' }, collection_id: { type: 'number' },
        archived: { type: 'boolean' },
        created_at: { type: 'string' }, updated_at: { type: 'string' }
      }, required: ['id', 'name', 'display']
    }
  },
  mb_card_update: { title: 'Update Card', write: true, destructive: false, idempotent: true },
  mb_card_delete: { title: 'Delete Card', write: true, destructive: true, idempotent: true },
  mb_card_archive: { title: 'Archive Card', write: true, destructive: false, idempotent: true },
  mb_card_data: { title: 'Get Card Data' },
  mb_card_copy: { title: 'Copy Card', write: true, destructive: false, idempotent: false },
  mb_card_clone: { title: 'Clone Card', write: true, destructive: false, idempotent: false },

  // ── Dashboards ──
  mb_dashboard_create: { title: 'Create Dashboard', write: true, destructive: false, idempotent: false },
  mb_dashboards: {
    title: 'List Dashboards', outputSchema: {
      type: 'object',
      properties: {
        dashboards: {
          type: 'array', items: {
            type: 'object', properties: {
              id: { type: 'number' }, name: { type: 'string' }
            }, required: ['id', 'name']
          }
        },
        count: { type: 'number' }
      }, required: ['dashboards']
    }
  },
  mb_dashboard_template_executive: { title: 'Create Executive Dashboard', write: true, destructive: false, idempotent: false },
  mb_dashboard_add_card: { title: 'Add Card to Dashboard', write: true, destructive: false, idempotent: false },
  mb_dashboard_add_card_sql: { title: 'Add Cards via SQL', write: true, destructive: false, idempotent: false },
  mb_dashboard_update_layout: { title: 'Update Dashboard Layout', write: true, destructive: false, idempotent: true },
  mb_dashboard_get: {
    title: 'Get Dashboard', outputSchema: {
      type: 'object',
      properties: {
        id: { type: 'number' }, name: { type: 'string' },
        description: { type: 'string' },
        cards: { type: 'array' }, parameters: { type: 'array' }
      }, required: ['id', 'name']
    }
  },
  mb_dashboard_update: { title: 'Update Dashboard', write: true, destructive: false, idempotent: true },
  mb_dashboard_delete: { title: 'Delete Dashboard', write: true, destructive: true, idempotent: true },
  mb_dashboard_card_update: { title: 'Update Dashboard Card', write: true, destructive: false, idempotent: true },
  mb_dashboard_card_remove: { title: 'Remove Dashboard Card', write: true, destructive: true, idempotent: true },
  mb_dashboard_copy: { title: 'Copy Dashboard', write: true, destructive: false, idempotent: false },
  mb_dashboard_add_filter: { title: 'Add Dashboard Filter', write: true, destructive: false, idempotent: false },
  mb_dashboard_layout_optimize: { title: 'Optimize Dashboard Layout', write: true, destructive: false, idempotent: true },
  mb_create_parametric_question: { title: 'Create Parametric Question (SQL)', write: true, destructive: false, idempotent: false },
  mb_link_dashboard_filter: { title: 'Link Dashboard Filter', write: true, destructive: false, idempotent: true },

  // ── Metrics ──
  mb_metric_create: { title: 'Create Metric', write: true, destructive: false, idempotent: false },

  // ── Visualization ──
  mb_visualization_settings: { title: 'Get Visualization Settings' },
  mb_visualization_recommend: { title: 'Recommend Visualization' },

  // ── Collections ──
  mb_collection_create: { title: 'Create Collection', write: true, destructive: false, idempotent: false },
  mb_collection_list: {
    title: 'List Collections', outputSchema: {
      type: 'object',
      properties: {
        collections: {
          type: 'array', items: {
            type: 'object', properties: {
              id: { type: 'number' }, name: { type: 'string' }
            }, required: ['id', 'name']
          }
        },
        count: { type: 'number' }
      }, required: ['collections']
    }
  },
  mb_collection_move: { title: 'Move Collection', write: true, destructive: false, idempotent: true },
  mb_collection_copy: { title: 'Copy Collection', write: true, destructive: false, idempotent: false },
  mb_collection_permissions_get: { title: 'Get Collection Permissions' },
  mb_collection_permissions_update: { title: 'Update Collection Permissions', write: true, destructive: false, idempotent: true },

  // ── Users & Permissions ──
  mb_user_list: {
    title: 'List Users', outputSchema: {
      type: 'object',
      properties: {
        users: {
          type: 'array', items: {
            type: 'object', properties: {
              id: { type: 'number' }, email: { type: 'string' },
              first_name: { type: 'string' }, last_name: { type: 'string' },
              is_active: { type: 'boolean' }
            }, required: ['id', 'email']
          }
        },
        count: { type: 'number' }
      }, required: ['users']
    }
  },
  mb_user_get: { title: 'Get User' },
  mb_user_create: { title: 'Create User', write: true, destructive: false, idempotent: false },
  mb_user_update: { title: 'Update User', write: true, destructive: false, idempotent: true },
  mb_user_disable: { title: 'Disable User', write: true, destructive: true, idempotent: true },
  mb_permission_group_list: { title: 'List Permission Groups' },
  mb_permission_group_create: { title: 'Create Permission Group', write: true, destructive: false, idempotent: false },
  mb_permission_group_delete: { title: 'Delete Permission Group', write: true, destructive: true, idempotent: true },
  mb_permission_group_add_user: { title: 'Add User to Group', write: true, destructive: false, idempotent: true },
  mb_permission_group_remove_user: { title: 'Remove User from Group', write: true, destructive: false, idempotent: true },

  // ── Actions & Alerts ──
  mb_action_create: { title: 'Create Action', write: true, destructive: false, idempotent: false },
  mb_action_list: { title: 'List Actions' },
  mb_action_execute: { title: 'Execute Action', write: true, destructive: false, idempotent: false },
  mb_alert_create: { title: 'Create Alert', write: true, destructive: false, idempotent: false },
  mb_alert_list: { title: 'List Alerts' },
  mb_pulse_create: { title: 'Create Pulse', write: true, destructive: false, idempotent: false },

  // ── Field & Table Metadata ──
  mb_field_metadata: { title: 'Get/Update Field Metadata', write: true, destructive: false, idempotent: true },
  mb_table_metadata: { title: 'Get/Update Table Metadata', write: true, destructive: false, idempotent: true },
  mb_field_values: { title: 'Get Field Values' },

  // ── Embedding ──
  mb_embed_url_generate: { title: 'Generate Embed URL' },
  mb_embed_settings: { title: 'Get Embed Settings' },

  // ── Search ──
  mb_search: {
    title: 'Search Metabase', outputSchema: {
      type: 'object',
      properties: {
        results: {
          type: 'array', items: {
            type: 'object', properties: {
              id: { type: 'number' }, name: { type: 'string' },
              model: { type: 'string' }
            }, required: ['id', 'name', 'model']
          }
        },
        count: { type: 'number' }
      }, required: ['results']
    }
  },

  // ── Segments ──
  mb_segment_create: { title: 'Create Segment', write: true, destructive: false, idempotent: false },
  mb_segment_list: { title: 'List Segments' },

  // ── Bookmarks ──
  mb_bookmark_create: { title: 'Create Bookmark', write: true, destructive: false, idempotent: true },
  mb_bookmark_list: { title: 'List Bookmarks' },
  mb_bookmark_delete: { title: 'Delete Bookmark', write: true, destructive: false, idempotent: true },

  // ── Sync & Cache ──
  db_sync_schema: { title: 'Sync Database Schema', write: true, destructive: false, idempotent: true },
  mb_cache_invalidate: { title: 'Invalidate Cache', write: true, destructive: false, idempotent: true },

  // ── Relationships ──
  mb_relationships_create: { title: 'Create Relationship Mapping', write: true, destructive: false, idempotent: false },

  // ── Documentation (read-only) ──
  web_fetch_metabase_docs: { title: 'Fetch Metabase Docs' },
  web_explore_metabase_docs: { title: 'Explore Metabase Docs' },
  web_search_metabase_docs: { title: 'Search Metabase Docs' },
  web_metabase_api_reference: { title: 'Metabase API Reference' },

  // ── Internal Metadata (read-only) ──
  meta_find_internal_db: { title: 'Find Internal Database' },
  meta_audit_logs: { title: 'Query Audit Logs' },
  meta_lineage: { title: 'Trace Data Lineage' },
  meta_advanced_search: { title: 'Advanced Metadata Search' },

  // ── Definition Tables ──
  definition_tables_init: { title: 'Initialize Definition Tables', write: true, destructive: false, idempotent: true },
  definition_search_terms: { title: 'Search Definition Terms' },
  definition_get_metric: { title: 'Get Metric Definition' },
  definition_get_template: { title: 'Get Template Definition' },
  definition_global_search: { title: 'Global Definition Search' },

  // ── Parametric ──
  parametric_question_create: { title: 'Create Parametric Question', write: true, destructive: false, idempotent: false },
  parametric_dashboard_create: { title: 'Create Parametric Dashboard', write: true, destructive: false, idempotent: false },
  parametric_template_preset: { title: 'Apply Template Preset' },

  // ── Activity Logging ──
  activity_log_init: { title: 'Initialize Activity Log', write: true, destructive: false, idempotent: true },
  activity_session_summary: { title: 'Get Session Summary' },
  activity_operation_stats: { title: 'Get Operation Stats' },
  activity_database_usage: { title: 'Get Database Usage' },
  activity_error_analysis: { title: 'Analyze Errors' },
  activity_performance_insights: { title: 'Get Performance Insights' },
  activity_timeline: { title: 'Get Activity Timeline' },
  activity_cleanup: { title: 'Cleanup Activity Logs', write: true, destructive: true, idempotent: true },

  // ── Metadata Analytics ──
  mb_meta_query_performance: { title: 'Query Performance Analytics' },
  mb_meta_content_usage: { title: 'Content Usage Analytics' },
  mb_meta_user_activity: { title: 'User Activity Analytics' },
  mb_meta_database_usage: { title: 'Database Usage Analytics' },
  mb_meta_dashboard_complexity: { title: 'Dashboard Complexity Analysis' },
  mb_meta_info: { title: 'Metadata Info' },
  mb_meta_table_dependencies: { title: 'Table Dependencies' },
  mb_meta_impact_analysis: { title: 'Impact Analysis' },
  mb_meta_optimization_recommendations: { title: 'Optimization Recommendations' },
  mb_meta_error_patterns: { title: 'Error Pattern Analysis' },

  // ── Export/Import ──
  mb_meta_export_workspace: { title: 'Export Workspace' },
  mb_meta_import_preview: { title: 'Preview Import' },
  mb_meta_compare_environments: { title: 'Compare Environments' },
  mb_meta_auto_cleanup: { title: 'Auto Cleanup Metadata', write: true, destructive: true, idempotent: true },
};

/**
 * Enrich raw tool definitions with MCP 2025-11-25 compliant annotations.
 * - Adds `title` from TOOL_METADATA
 * - Adds `annotations` object with readOnlyHint, destructiveHint, idempotentHint, openWorldHint
 * - Adds `additionalProperties: false` to no-param inputSchemas  
 */
function enrichTools(tools) {
  return tools.map(tool => {
    const meta = TOOL_METADATA[tool.name] || {};
    const isWrite = meta.write === true;

    // Build annotations (MCP Spec 2025-11-25)
    const annotations = {
      readOnlyHint: !isWrite,
      destructiveHint: meta.destructive === true,
      idempotentHint: meta.idempotent !== undefined ? meta.idempotent : !isWrite,
      openWorldHint: false,
    };

    // Enrich inputSchema: add additionalProperties for empty schemas
    const inputSchema = { ...tool.inputSchema };
    if (inputSchema.type === 'object' &&
      inputSchema.properties &&
      Object.keys(inputSchema.properties).length === 0 &&
      inputSchema.additionalProperties === undefined) {
      inputSchema.additionalProperties = false;
    }

    return {
      ...tool,
      ...(meta.title && { title: meta.title }),
      inputSchema,
      ...(meta.outputSchema && { outputSchema: meta.outputSchema }),
      annotations,
    };
  });
}

export function getToolDefinitions() {
  return enrichTools([
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
    {
      name: 'db_table_profile',
      description: 'Get comprehensive table profile: row count, column types, distinct values, sample data. Auto-detects dimension/reference tables (dim_, ref_, lookup_ prefix). Ideal for understanding lookup tables before writing queries.',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID',
          },
          table_name: {
            type: 'string',
            description: 'Table name (with or without schema prefix)',
          },
          schema_name: {
            type: 'string',
            description: 'Schema name (default: public)',
            default: 'public',
          },
          show_distinct_values: {
            type: 'boolean',
            description: 'Show distinct values for each column (auto-enabled for dim/ref tables)',
            default: true,
          },
          sample_rows: {
            type: 'number',
            description: 'Number of sample rows to display (default: 3)',
            default: 3,
          },
        },
        required: ['database_id', 'table_name'],
      },
    },
    // === SQL EXECUTION ===
    {
      name: 'sql_execute',
      description: 'Run SQL queries against database - supports SELECT, DDL with security controls, returns formatted results. For long-running queries (>60s), use sql_submit instead.',
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
          full_results: {
            type: 'boolean',
            description: 'Set to true to disable result truncation (useful for DDL/definitions)',
          },
        },
        required: ['database_id', 'sql'],
      },
    },
    {
      name: 'sql_submit',
      description: 'Submit a long-running SQL query asynchronously. Returns immediately with job_id. Use sql_status to check progress. Ideal for queries that may take minutes.',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID',
          },
          sql: {
            type: 'string',
            description: 'SQL query to execute',
          },
          timeout_seconds: {
            type: 'number',
            description: 'Query timeout in seconds (default: 300, max: 1800)',
            default: 300,
          },
        },
        required: ['database_id', 'sql'],
      },
    },
    {
      name: 'sql_status',
      description: 'Check status of an async query submitted via sql_submit. Returns results when complete.',
      inputSchema: {
        type: 'object',
        properties: {
          job_id: {
            type: 'string',
            description: 'Job ID returned from sql_submit',
          },
        },
        required: ['job_id'],
      },
    },
    {
      name: 'sql_cancel',
      description: 'Cancel a running async query. Also attempts to cancel on database server.',
      inputSchema: {
        type: 'object',
        properties: {
          job_id: {
            type: 'string',
            description: 'Job ID to cancel',
          },
        },
        required: ['job_id'],
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
      name: 'mb_dashboard_add_card_sql',
      description: 'Add multiple cards to a dashboard using direct SQL inserts. Bypasses API limits, ensures precise positioning, and prevents timeouts. Use this for complex layouts.',
      inputSchema: {
        type: 'object',
        properties: {
          dashboard_id: {
            type: 'number',
            description: 'Target Dashboard ID'
          },
          cards: {
            type: 'array',
            description: 'List of cards to add with layout config',
            items: {
              type: 'object',
              properties: {
                card_id: { type: 'number' },
                row: { type: 'number', description: 'Grid row (0-based)' },
                col: { type: 'number', description: 'Grid col (0-based)' },
                size_x: { type: 'number', default: 4 },
                size_y: { type: 'number', default: 4 },
                visualization_settings: { type: 'object', description: 'Optional override settings' },
                parameter_mappings: { type: 'array', description: 'Optional filter mappings' }
              },
              required: ['card_id', 'row', 'col']
            }
          }
        },
        required: ['dashboard_id', 'cards']
      }
    },
    {
      name: 'mb_dashboard_update_layout',
      description: 'Batch update position and size of multiple dashboard cards via direct SQL. Guarantees layout application.',
      inputSchema: {
        type: 'object',
        properties: {
          dashboard_id: { type: 'number' },
          updates: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                card_id: { type: 'number' },
                row: { type: 'number' },
                col: { type: 'number' },
                size_x: { type: 'number' },
                size_y: { type: 'number' }
              },
              required: ['card_id']
            }
          }
        },
        required: ['dashboard_id', 'updates']
      }
    },
    {
      name: 'mb_create_parametric_question',
      description: 'Create a native SQL question with parameters (variables) directly via SQL. Essential for creating cards that accept dashboard filters.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string' },
          description: { type: 'string' },
          database_id: { type: 'number', description: 'ID of the database to query against (not the internal one)' },
          query_sql: { type: 'string', description: 'SQL query with {{variable}} tags' },
          parameters: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                name: { type: 'string' },
                display_name: { type: 'string' },
                type: { type: 'string', enum: ['text', 'number', 'date', 'dimension'] },
                required: { type: 'boolean' },
                default: { type: 'string' }
              },
              required: ['name', 'type']
            }
          },
          collection_id: { type: 'number', description: 'Optional collection to place question in' }
        },
        required: ['name', 'database_id', 'query_sql']
      }
    },
    {
      name: 'mb_link_dashboard_filter',
      description: 'Link a dashboard filter to a card parameter via SQL. Updates parameter_mappings.',
      inputSchema: {
        type: 'object',
        properties: {
          dashboard_id: { type: 'number' },
          card_id: { type: 'number' },
          mappings: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                parameter_id: { type: 'string', description: 'The GUID of the dashboard parameter' },
                target_type: { type: 'string', enum: ['variable', 'dimension'] },
                target_value: { type: 'string', description: 'Variable name (without {{}}) or Field ID for dimension' }
              },
              required: ['parameter_id', 'target_type', 'target_value']
            }
          }
        },
        required: ['dashboard_id', 'card_id', 'mappings']
      }
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
            maximum: 3
          }
        },
        required: ['query'],
      },
    },
    // === ADVANCED METADATA TOOLS (Internal DB) ===
    {
      name: 'meta_find_internal_db',
      description: 'Auto-detect the Metabase Internal Application Database ID from connected databases. Required for advanced metadata tools.',
      inputSchema: {
        type: 'object',
        properties: {},
      },
    },
    {
      name: 'meta_audit_logs',
      description: 'Analyze query performance and usage history from internal logs. Finds slow queries and top users.',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 30)',
            default: 30
          },
          limit: {
            type: 'number',
            description: 'Max results to return (default: 50)',
            default: 50
          },
          internal_db_id: {
            type: 'number',
            description: 'Internal Database ID (optional if configured in env)'
          }
        },
      },
    },
    {
      name: 'meta_lineage',
      description: 'Find dependencies: Which dashboards and questions use a specific table or field? (Impact Analysis)',
      inputSchema: {
        type: 'object',
        properties: {
          search_term: {
            type: 'string',
            description: 'Table name, field name, or SQL fragment to search for usage'
          },
          internal_db_id: {
            type: 'number',
            description: 'Internal Database ID (optional if configured in env)'
          }
        },
        required: ['search_term']
      },
    },
    {
      name: 'meta_advanced_search',
      description: 'Deep search within SQL code, visualization settings, and descriptions across all questions and dashboards.',
      inputSchema: {
        type: 'object',
        properties: {
          query: {
            type: 'string',
            description: 'Search query (searches SQL, names, descriptions)'
          },
          internal_db_id: {
            type: 'number',
            description: 'Internal Database ID (optional if configured in env)'
          }
        },
        required: ['query']
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
    {
      name: 'mb_auto_describe',
      description: 'Automatically generate AI-powered descriptions for databases, tables, and fields with timestamp signatures',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to generate descriptions for',
          },
          target_type: {
            type: 'string',
            enum: ['database', 'tables', 'fields', 'all'],
            description: 'What to generate descriptions for',
            default: 'all'
          },
          force_update: {
            type: 'boolean',
            description: 'Update existing descriptions (default: false)',
            default: false
          }
        },
        required: ['database_id'],
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

    // === ACTIVITY LOGGING & ANALYTICS ===
    {
      name: 'activity_log_init',
      description: 'Initialize activity logging system for a database - creates log table and starts tracking operations',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to initialize logging for',
          },
          schema: {
            type: 'string',
            description: 'Schema name for log table (default: public)',
            default: 'public',
          },
        },
        required: ['database_id'],
      },
    },
    {
      name: 'activity_session_summary',
      description: 'Get comprehensive summary of current or specified session activities and performance',
      inputSchema: {
        type: 'object',
        properties: {
          session_id: {
            type: 'string',
            description: 'Session ID to analyze (optional, defaults to current session)',
          },
        },
      },
    },
    {
      name: 'activity_operation_stats',
      description: 'Analyze operation statistics and patterns over specified time period',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 7)',
            default: 7,
          },
        },
      },
    },
    {
      name: 'activity_database_usage',
      description: 'Get database usage patterns and statistics showing which databases are most active',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 30)',
            default: 30,
          },
        },
      },
    },
    {
      name: 'activity_error_analysis',
      description: 'Analyze error patterns and common failure points to identify improvement opportunities',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 7)',
            default: 7,
          },
        },
      },
    },
    {
      name: 'activity_performance_insights',
      description: 'Get performance insights showing slow operations and optimization opportunities',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 7)',
            default: 7,
          },
        },
      },
    },
    {
      name: 'activity_timeline',
      description: 'Get chronological timeline of recent activities for debugging and monitoring',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to show (default: 7)',
            default: 7,
          },
          limit: {
            type: 'number',
            description: 'Maximum number of activities to return (default: 100)',
            default: 100,
          },
        },
      },
    },
    {
      name: 'activity_cleanup',
      description: 'Clean up old activity logs to maintain performance and storage efficiency',
      inputSchema: {
        type: 'object',
        properties: {
          retention_days: {
            type: 'number',
            description: 'Keep logs newer than this many days (default: 90)',
            default: 90,
          },
          dry_run: {
            type: 'boolean',
            description: 'Preview what would be deleted without actually deleting',
            default: true,
          },
        },
      },
    },

    // === DEFINITION TABLES & PARAMETRIC QUESTIONS ===
    {
      name: 'definition_tables_init',
      description: 'Initialize definition lookup tables system for documentation, metrics, templates, and search',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to create definition tables in'
          }
        },
        required: ['database_id']
      }
    },
    {
      name: 'definition_search_terms',
      description: 'Search business terms and definitions with relevance ranking',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to search in'
          },
          search_term: {
            type: 'string',
            description: 'Term to search for in definitions'
          },
          category: {
            type: 'string',
            description: 'Optional category filter (customer_metrics, revenue_metrics, etc.)'
          }
        },
        required: ['database_id', 'search_term']
      }
    },
    {
      name: 'definition_get_metric',
      description: 'Get metric definition with calculation formula and business context',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to search in'
          },
          metric_name: {
            type: 'string',
            description: 'Metric name to lookup'
          }
        },
        required: ['database_id', 'metric_name']
      }
    },
    {
      name: 'definition_get_template',
      description: 'Get dashboard or question template with layout and configuration',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to search in'
          },
          template_name: {
            type: 'string',
            description: 'Template name to lookup'
          },
          template_type: {
            type: 'string',
            enum: ['dashboard', 'question'],
            description: 'Type of template to retrieve'
          }
        },
        required: ['database_id', 'template_name', 'template_type']
      }
    },
    {
      name: 'definition_global_search',
      description: 'Search across all definition tables with unified results',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to search in'
          },
          search_term: {
            type: 'string',
            description: 'Term to search across all definitions'
          },
          content_types: {
            type: 'array',
            items: { type: 'string' },
            description: 'Filter by content types (business_terms, metrics, templates, etc.)'
          }
        },
        required: ['database_id', 'search_term']
      }
    },
    {
      name: 'parametric_question_create',
      description: 'Create parametric question with date, text search, and category filters',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to create question in'
          },
          name: {
            type: 'string',
            description: 'Question name'
          },
          description: {
            type: 'string',
            description: 'Question description'
          },
          sql_template: {
            type: 'string',
            description: 'SQL template with parameter placeholders (e.g., {{date_range}}, {{search_term}})'
          },
          parameters: {
            type: 'object',
            description: 'Parameter definitions with types and defaults',
            additionalProperties: {
              type: 'object',
              properties: {
                type: {
                  type: 'string',
                  enum: ['date/single', 'date/range', 'date/relative', 'string/=', 'string/contains', 'number/=', 'number/between', 'category']
                },
                display_name: { type: 'string' },
                required: { type: 'boolean', default: false },
                default: { type: 'string' },
                field_id: { type: 'number' },
                options: { type: 'array', items: { type: 'string' } }
              }
            }
          },
          question_type: {
            type: 'string',
            enum: ['trend_analysis', 'comparison', 'ranking', 'distribution', 'kpi', 'table'],
            default: 'table'
          },
          collection_id: {
            type: 'number',
            description: 'Collection ID to save question to'
          }
        },
        required: ['database_id', 'name', 'sql_template', 'parameters']
      }
    },
    {
      name: 'parametric_dashboard_create',
      description: 'Create dashboard with parametric questions and shared filters',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to create dashboard in'
          },
          name: {
            type: 'string',
            description: 'Dashboard name'
          },
          description: {
            type: 'string',
            description: 'Dashboard description'
          },
          questions: {
            type: 'array',
            description: 'Array of question configurations',
            items: {
              type: 'object',
              properties: {
                name: { type: 'string' },
                sql_template: { type: 'string' },
                parameters: { type: 'object' },
                question_type: { type: 'string' }
              }
            }
          },
          filters: {
            type: 'object',
            description: 'Dashboard-level filters',
            additionalProperties: {
              type: 'object',
              properties: {
                type: { type: 'string' },
                display_name: { type: 'string' },
                default: { type: 'string' },
                required: { type: 'boolean' }
              }
            }
          },
          layout: {
            type: 'array',
            description: 'Layout configuration for questions'
          },
          collection_id: {
            type: 'number',
            description: 'Collection ID to save dashboard to'
          }
        },
        required: ['database_id', 'name', 'questions']
      }
    },
    {
      name: 'parametric_template_preset',
      description: 'Create parametric question from preset templates (date range analysis, category filter, text search, period comparison)',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to create question in'
          },
          preset_type: {
            type: 'string',
            enum: ['date_range_analysis', 'category_filter', 'text_search', 'period_comparison'],
            description: 'Preset template type'
          },
          config: {
            type: 'object',
            description: 'Configuration for the preset template',
            properties: {
              name: { type: 'string' },
              table: { type: 'string' },
              date_column: { type: 'string' },
              category_column: { type: 'string' },
              search_columns: { type: 'array', items: { type: 'string' } },
              metrics: { type: 'array', items: { type: 'string' } },
              date_field_id: { type: 'number' },
              category_field_id: { type: 'number' }
            }
          },
          collection_id: {
            type: 'number',
            description: 'Collection ID to save question to'
          }
        },
        required: ['database_id', 'preset_type', 'config']
      }
    },

    // === DATABASE MAINTENANCE & QUERY ANALYSIS ===
    {
      name: 'db_vacuum_analyze',
      description: 'Run VACUUM and ANALYZE on PostgreSQL tables to optimize storage and update statistics (PostgreSQL only)',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID'
          },
          table_name: {
            type: 'string',
            description: 'Table name to vacuum/analyze (optional, all tables if not specified)'
          },
          schema_name: {
            type: 'string',
            description: 'Schema name (default: public)',
            default: 'public'
          },
          vacuum_type: {
            type: 'string',
            enum: ['vacuum', 'vacuum_analyze', 'vacuum_full', 'analyze_only'],
            description: 'Type of maintenance operation',
            default: 'vacuum_analyze'
          },
          dry_run: {
            type: 'boolean',
            description: 'Preview command without executing',
            default: true
          }
        },
        required: ['database_id']
      }
    },
    {
      name: 'db_query_explain',
      description: 'Get execution plan for a SQL query - shows how PostgreSQL will execute the query',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID'
          },
          sql: {
            type: 'string',
            description: 'SQL query to analyze'
          },
          analyze: {
            type: 'boolean',
            description: 'Actually run the query to get real execution times',
            default: false
          },
          format: {
            type: 'string',
            enum: ['text', 'json', 'yaml'],
            description: 'Output format for the plan',
            default: 'text'
          },
          verbose: {
            type: 'boolean',
            description: 'Include additional details in the plan',
            default: false
          }
        },
        required: ['database_id', 'sql']
      }
    },
    {
      name: 'db_table_stats',
      description: 'Get table statistics including row count, size, dead tuples, last vacuum/analyze times',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID'
          },
          table_name: {
            type: 'string',
            description: 'Table name to get stats for'
          },
          schema_name: {
            type: 'string',
            description: 'Schema name (default: public)',
            default: 'public'
          }
        },
        required: ['database_id', 'table_name']
      }
    },
    {
      name: 'db_index_usage',
      description: 'Analyze index usage statistics - find unused or rarely used indexes',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID'
          },
          schema_name: {
            type: 'string',
            description: 'Schema name to analyze (default: public)',
            default: 'public'
          },
          min_size_mb: {
            type: 'number',
            description: 'Minimum index size in MB to include',
            default: 0
          }
        },
        required: ['database_id']
      }
    },

    // === VISUALIZATION SETTINGS ===
    {
      name: 'mb_visualization_settings',
      description: 'Get or update visualization settings for a question (chart type, colors, labels, etc.)',
      inputSchema: {
        type: 'object',
        properties: {
          question_id: {
            type: 'number',
            description: 'Question ID to get/update visualization for'
          },
          display: {
            type: 'string',
            enum: ['table', 'bar', 'line', 'area', 'pie', 'scalar', 'row', 'funnel', 'scatter', 'map', 'gauge', 'progress', 'waterfall', 'combo'],
            description: 'Chart display type'
          },
          settings: {
            type: 'object',
            description: 'Visualization settings object',
            properties: {
              'graph.dimensions': { type: 'array', description: 'Dimension fields' },
              'graph.metrics': { type: 'array', description: 'Metric fields' },
              'graph.colors': { type: 'array', description: 'Custom colors' },
              'graph.x_axis.title_text': { type: 'string' },
              'graph.y_axis.title_text': { type: 'string' },
              'graph.show_values': { type: 'boolean' },
              'graph.label_value_frequency': { type: 'string' },
              'pie.show_legend': { type: 'boolean' },
              'pie.show_total': { type: 'boolean' },
              'table.pivot': { type: 'boolean' },
              'table.cell_column': { type: 'string' }
            }
          }
        },
        required: ['question_id']
      }
    },
    {
      name: 'mb_visualization_recommend',
      description: 'AI-powered visualization recommendation based on query results and data types',
      inputSchema: {
        type: 'object',
        properties: {
          question_id: {
            type: 'number',
            description: 'Question ID to analyze'
          },
          data_sample: {
            type: 'object',
            description: 'Sample data to analyze (optional if question_id provided)'
          },
          purpose: {
            type: 'string',
            enum: ['comparison', 'trend', 'distribution', 'composition', 'relationship', 'kpi'],
            description: 'Purpose of the visualization'
          }
        },
        required: ['question_id']
      }
    },

    // === COLLECTION MANAGEMENT ===
    {
      name: 'mb_collection_create',
      description: 'Create a new collection in Metabase for organizing questions and dashboards',
      inputSchema: {
        type: 'object',
        properties: {
          name: {
            type: 'string',
            description: 'Collection name'
          },
          description: {
            type: 'string',
            description: 'Collection description'
          },
          parent_id: {
            type: 'number',
            description: 'Parent collection ID (null for root level)'
          },
          color: {
            type: 'string',
            description: 'Collection color (hex code)',
            default: '#509EE3'
          }
        },
        required: ['name']
      }
    },
    {
      name: 'mb_collection_list',
      description: 'List all collections with hierarchy and item counts',
      inputSchema: {
        type: 'object',
        properties: {
          parent_id: {
            type: 'number',
            description: 'Filter by parent collection ID (null for root)'
          },
          include_items: {
            type: 'boolean',
            description: 'Include items in each collection',
            default: false
          }
        }
      }
    },
    {
      name: 'mb_collection_move',
      description: 'Move questions, dashboards, or collections to a different collection',
      inputSchema: {
        type: 'object',
        properties: {
          item_type: {
            type: 'string',
            enum: ['card', 'dashboard', 'collection'],
            description: 'Type of item to move'
          },
          item_id: {
            type: 'number',
            description: 'ID of the item to move'
          },
          target_collection_id: {
            type: 'number',
            description: 'Target collection ID (null for root)'
          }
        },
        required: ['item_type', 'item_id', 'target_collection_id']
      }
    },

    // === METABASE ACTIONS API ===
    {
      name: 'mb_action_create',
      description: 'Create a Metabase Action for data modification (INSERT, UPDATE, DELETE)',
      inputSchema: {
        type: 'object',
        properties: {
          name: {
            type: 'string',
            description: 'Action name'
          },
          description: {
            type: 'string',
            description: 'Action description'
          },
          model_id: {
            type: 'number',
            description: 'Model ID the action belongs to'
          },
          type: {
            type: 'string',
            enum: ['query', 'implicit'],
            description: 'Action type',
            default: 'query'
          },
          database_id: {
            type: 'number',
            description: 'Database ID for query actions'
          },
          dataset_query: {
            type: 'object',
            description: 'Query definition for query actions',
            properties: {
              native: {
                type: 'object',
                properties: {
                  query: { type: 'string', description: 'SQL query with {{parameter}} placeholders' },
                  template_tags: { type: 'object', description: 'Parameter definitions' }
                }
              }
            }
          },
          parameters: {
            type: 'array',
            description: 'Action parameters',
            items: {
              type: 'object',
              properties: {
                id: { type: 'string' },
                name: { type: 'string' },
                type: { type: 'string', enum: ['string', 'number', 'date'] },
                required: { type: 'boolean' }
              }
            }
          },
          visualization_settings: {
            type: 'object',
            description: 'Form visualization settings'
          }
        },
        required: ['name', 'model_id', 'type']
      }
    },
    {
      name: 'mb_action_list',
      description: 'List all actions for a model',
      inputSchema: {
        type: 'object',
        properties: {
          model_id: {
            type: 'number',
            description: 'Model ID to get actions for'
          }
        },
        required: ['model_id']
      }
    },
    {
      name: 'mb_action_execute',
      description: 'Execute a Metabase action with parameters',
      inputSchema: {
        type: 'object',
        properties: {
          action_id: {
            type: 'number',
            description: 'Action ID to execute'
          },
          parameters: {
            type: 'object',
            description: 'Parameter values for the action'
          }
        },
        required: ['action_id', 'parameters']
      }
    },

    // === ALERTS & NOTIFICATIONS ===
    {
      name: 'mb_alert_create',
      description: 'Create an alert for a question that triggers on specified conditions',
      inputSchema: {
        type: 'object',
        properties: {
          card_id: {
            type: 'number',
            description: 'Question/Card ID to create alert for'
          },
          alert_condition: {
            type: 'string',
            enum: ['rows', 'goal'],
            description: 'Alert condition type',
            default: 'rows'
          },
          alert_first_only: {
            type: 'boolean',
            description: 'Only alert on first occurrence',
            default: false
          },
          alert_above_goal: {
            type: 'boolean',
            description: 'Alert when above goal (for goal condition)',
            default: true
          },
          channels: {
            type: 'array',
            description: 'Notification channels',
            items: {
              type: 'object',
              properties: {
                channel_type: { type: 'string', enum: ['email', 'slack'] },
                enabled: { type: 'boolean' },
                recipients: { type: 'array', items: { type: 'object' } },
                details: { type: 'object' },
                schedule_type: { type: 'string', enum: ['hourly', 'daily', 'weekly'] },
                schedule_hour: { type: 'number' },
                schedule_day: { type: 'string' }
              }
            }
          }
        },
        required: ['card_id']
      }
    },
    {
      name: 'mb_alert_list',
      description: 'List all alerts, optionally filtered by question',
      inputSchema: {
        type: 'object',
        properties: {
          card_id: {
            type: 'number',
            description: 'Filter alerts by question ID (optional)'
          }
        }
      }
    },
    {
      name: 'mb_pulse_create',
      description: 'Create a scheduled report (pulse) that sends dashboards/questions on a schedule',
      inputSchema: {
        type: 'object',
        properties: {
          name: {
            type: 'string',
            description: 'Pulse name'
          },
          cards: {
            type: 'array',
            description: 'Cards to include in the pulse',
            items: {
              type: 'object',
              properties: {
                id: { type: 'number' },
                include_csv: { type: 'boolean' },
                include_xls: { type: 'boolean' }
              }
            }
          },
          channels: {
            type: 'array',
            description: 'Delivery channels (email/slack)',
            items: {
              type: 'object',
              properties: {
                channel_type: { type: 'string' },
                schedule_type: { type: 'string' },
                schedule_hour: { type: 'number' },
                schedule_day: { type: 'string' },
                recipients: { type: 'array' }
              }
            }
          },
          skip_if_empty: {
            type: 'boolean',
            description: 'Skip sending if no results',
            default: true
          },
          collection_id: {
            type: 'number',
            description: 'Collection to save pulse in'
          }
        },
        required: ['name', 'cards', 'channels']
      }
    },

    // === FIELD METADATA & SEMANTIC TYPES ===
    {
      name: 'mb_field_metadata',
      description: 'Get or update field metadata including display name, description, and semantic type',
      inputSchema: {
        type: 'object',
        properties: {
          field_id: {
            type: 'number',
            description: 'Field ID to get/update'
          },
          display_name: {
            type: 'string',
            description: 'Human-readable display name'
          },
          description: {
            type: 'string',
            description: 'Field description'
          },
          semantic_type: {
            type: 'string',
            enum: [
              'type/PK', 'type/FK', 'type/Name', 'type/Title', 'type/Description',
              'type/City', 'type/State', 'type/Country', 'type/ZipCode', 'type/Latitude', 'type/Longitude',
              'type/Email', 'type/URL', 'type/ImageURL', 'type/AvatarURL',
              'type/Number', 'type/Currency', 'type/Cost', 'type/Price', 'type/Discount', 'type/Income', 'type/Quantity',
              'type/Score', 'type/Percentage', 'type/Duration',
              'type/CreationDate', 'type/CreationTimestamp', 'type/JoinDate', 'type/Birthdate',
              'type/Category', 'type/Comment', 'type/SerializedJSON',
              'type/Product', 'type/User', 'type/Company'
            ],
            description: 'Semantic type for the field'
          },
          visibility_type: {
            type: 'string',
            enum: ['normal', 'details-only', 'sensitive', 'hidden', 'retired'],
            description: 'Field visibility'
          },
          has_field_values: {
            type: 'string',
            enum: ['none', 'list', 'search'],
            description: 'How to show field values in filters'
          }
        },
        required: ['field_id']
      }
    },
    {
      name: 'mb_table_metadata',
      description: 'Get or update table metadata including display name, description, and visibility',
      inputSchema: {
        type: 'object',
        properties: {
          table_id: {
            type: 'number',
            description: 'Table ID to get/update'
          },
          display_name: {
            type: 'string',
            description: 'Human-readable display name'
          },
          description: {
            type: 'string',
            description: 'Table description'
          },
          visibility_type: {
            type: 'string',
            enum: ['visible', 'hidden', 'technical', 'cruft'],
            description: 'Table visibility type'
          }
        },
        required: ['table_id']
      }
    },
    {
      name: 'mb_field_values',
      description: 'Get distinct values for a field (for filter dropdowns)',
      inputSchema: {
        type: 'object',
        properties: {
          field_id: {
            type: 'number',
            description: 'Field ID to get values for'
          }
        },
        required: ['field_id']
      }
    },

    // === EMBEDDING ===
    {
      name: 'mb_embed_url_generate',
      description: 'Generate signed embedding URL for a dashboard or question',
      inputSchema: {
        type: 'object',
        properties: {
          resource_type: {
            type: 'string',
            enum: ['dashboard', 'question'],
            description: 'Type of resource to embed'
          },
          resource_id: {
            type: 'number',
            description: 'ID of the dashboard or question'
          },
          params: {
            type: 'object',
            description: 'Locked parameter values for the embedding'
          },
          exp_minutes: {
            type: 'number',
            description: 'Token expiration in minutes',
            default: 10
          },
          preview: {
            type: 'boolean',
            description: 'Include preview-mode frame styles',
            default: false
          },
          theme: {
            type: 'string',
            enum: ['light', 'night', 'transparent'],
            description: 'Embed theme',
            default: 'light'
          },
          bordered: {
            type: 'boolean',
            description: 'Show border around embed',
            default: true
          },
          titled: {
            type: 'boolean',
            description: 'Show title in embed',
            default: true
          }
        },
        required: ['resource_type', 'resource_id']
      }
    },
    {
      name: 'mb_embed_settings',
      description: 'Get embedding settings and enabled features for the Metabase instance',
      inputSchema: {
        type: 'object',
        properties: {}
      }
    },
    // ==================== USER MANAGEMENT ====================
    {
      name: 'mb_user_list',
      description: 'List all Metabase users with filtering options',
      inputSchema: {
        type: 'object',
        properties: {
          status: {
            type: 'string',
            enum: ['active', 'inactive', 'all'],
            default: 'all',
            description: 'Filter by user status'
          },
          group_id: {
            type: 'number',
            description: 'Filter by permission group ID (optional)'
          }
        }
      }
    },
    {
      name: 'mb_user_get',
      description: 'Get detailed information about a specific user',
      inputSchema: {
        type: 'object',
        properties: {
          user_id: {
            type: 'number',
            description: 'User ID to retrieve'
          }
        },
        required: ['user_id']
      }
    },
    {
      name: 'mb_user_create',
      description: 'Create a new Metabase user',
      inputSchema: {
        type: 'object',
        properties: {
          email: {
            type: 'string',
            description: 'User email address (also used as login)'
          },
          first_name: {
            type: 'string',
            description: 'User first name'
          },
          last_name: {
            type: 'string',
            description: 'User last name'
          },
          password: {
            type: 'string',
            description: 'Initial password (optional, will send invite email if not provided)'
          },
          group_ids: {
            type: 'array',
            items: { type: 'number' },
            description: 'Array of permission group IDs to assign'
          }
        },
        required: ['email', 'first_name', 'last_name']
      }
    },
    {
      name: 'mb_user_update',
      description: 'Update an existing user',
      inputSchema: {
        type: 'object',
        properties: {
          user_id: {
            type: 'number',
            description: 'User ID to update'
          },
          email: { type: 'string', description: 'New email address' },
          first_name: { type: 'string', description: 'New first name' },
          last_name: { type: 'string', description: 'New last name' },
          is_superuser: { type: 'boolean', description: 'Set superuser status' },
          group_ids: {
            type: 'array',
            items: { type: 'number' },
            description: 'New permission group IDs'
          }
        },
        required: ['user_id']
      }
    },
    {
      name: 'mb_user_disable',
      description: 'Disable (deactivate) a user account',
      inputSchema: {
        type: 'object',
        properties: {
          user_id: {
            type: 'number',
            description: 'User ID to disable'
          }
        },
        required: ['user_id']
      }
    },
    // ==================== PERMISSION GROUPS ====================
    {
      name: 'mb_permission_group_list',
      description: 'List all permission groups in Metabase',
      inputSchema: {
        type: 'object',
        properties: {}
      }
    },
    {
      name: 'mb_permission_group_create',
      description: 'Create a new permission group',
      inputSchema: {
        type: 'object',
        properties: {
          name: {
            type: 'string',
            description: 'Name of the permission group'
          }
        },
        required: ['name']
      }
    },
    {
      name: 'mb_permission_group_delete',
      description: 'Delete a permission group',
      inputSchema: {
        type: 'object',
        properties: {
          group_id: {
            type: 'number',
            description: 'Group ID to delete'
          }
        },
        required: ['group_id']
      }
    },
    {
      name: 'mb_permission_group_add_user',
      description: 'Add a user to a permission group',
      inputSchema: {
        type: 'object',
        properties: {
          group_id: {
            type: 'number',
            description: 'Permission group ID'
          },
          user_id: {
            type: 'number',
            description: 'User ID to add'
          }
        },
        required: ['group_id', 'user_id']
      }
    },
    {
      name: 'mb_permission_group_remove_user',
      description: 'Remove a user from a permission group',
      inputSchema: {
        type: 'object',
        properties: {
          group_id: {
            type: 'number',
            description: 'Permission group ID'
          },
          user_id: {
            type: 'number',
            description: 'User ID to remove'
          }
        },
        required: ['group_id', 'user_id']
      }
    },
    // ==================== COLLECTION PERMISSIONS ====================
    {
      name: 'mb_collection_permissions_get',
      description: 'Get permissions graph for a collection',
      inputSchema: {
        type: 'object',
        properties: {
          collection_id: {
            type: 'number',
            description: 'Collection ID (use "root" for root collection)'
          }
        },
        required: ['collection_id']
      }
    },
    {
      name: 'mb_collection_permissions_update',
      description: 'Update permissions for a collection',
      inputSchema: {
        type: 'object',
        properties: {
          collection_id: {
            type: 'number',
            description: 'Collection ID'
          },
          group_id: {
            type: 'number',
            description: 'Permission group ID'
          },
          permission: {
            type: 'string',
            enum: ['none', 'read', 'write'],
            description: 'Permission level to set'
          }
        },
        required: ['collection_id', 'group_id', 'permission']
      }
    },
    // ==================== CARD/QUESTION CRUD ====================
    {
      name: 'mb_card_get',
      description: 'Get detailed information about a specific card/question',
      inputSchema: {
        type: 'object',
        properties: {
          card_id: {
            type: 'number',
            description: 'Card/Question ID'
          }
        },
        required: ['card_id']
      }
    },
    {
      name: 'mb_card_update',
      description: 'Update an existing card/question',
      inputSchema: {
        type: 'object',
        properties: {
          card_id: {
            type: 'number',
            description: 'Card/Question ID'
          },
          name: { type: 'string', description: 'New name' },
          description: { type: 'string', description: 'New description' },
          visualization_settings: {
            type: 'object',
            description: 'New visualization settings'
          },
          display: {
            type: 'string',
            enum: ['table', 'bar', 'line', 'area', 'pie', 'scatter', 'funnel', 'map', 'row', 'waterfall', 'combo', 'gauge', 'progress', 'scalar', 'smartscalar', 'pivot'],
            description: 'Chart type'
          },
          collection_id: {
            type: 'number',
            description: 'Move to collection ID'
          }
        },
        required: ['card_id']
      }
    },
    {
      name: 'mb_card_delete',
      description: 'Permanently delete a card/question',
      inputSchema: {
        type: 'object',
        properties: {
          card_id: {
            type: 'number',
            description: 'Card/Question ID to delete'
          }
        },
        required: ['card_id']
      }
    },
    {
      name: 'mb_card_archive',
      description: 'Archive a card/question (soft delete)',
      inputSchema: {
        type: 'object',
        properties: {
          card_id: {
            type: 'number',
            description: 'Card/Question ID to archive'
          }
        },
        required: ['card_id']
      }
    },
    {
      name: 'mb_card_data',
      description: 'Execute a card/question and get the results in specified format',
      inputSchema: {
        type: 'object',
        properties: {
          card_id: {
            type: 'number',
            description: 'Card/Question ID'
          },
          format: {
            type: 'string',
            enum: ['json', 'csv', 'xlsx'],
            default: 'json',
            description: 'Output format'
          },
          parameters: {
            type: 'object',
            description: 'Optional parameters for parametric questions'
          }
        },
        required: ['card_id']
      }
    },
    // ==================== DASHBOARD CRUD ====================
    {
      name: 'mb_dashboard_get',
      description: 'Get detailed information about a dashboard',
      inputSchema: {
        type: 'object',
        properties: {
          dashboard_id: {
            type: 'number',
            description: 'Dashboard ID'
          }
        },
        required: ['dashboard_id']
      }
    },
    {
      name: 'mb_dashboard_update',
      description: 'Update dashboard properties',
      inputSchema: {
        type: 'object',
        properties: {
          dashboard_id: {
            type: 'number',
            description: 'Dashboard ID'
          },
          name: { type: 'string', description: 'New name' },
          description: { type: 'string', description: 'New description' },
          collection_id: { type: 'number', description: 'Move to collection' },
          enable_embedding: { type: 'boolean', description: 'Enable embedding' }
        },
        required: ['dashboard_id']
      }
    },
    {
      name: 'mb_dashboard_delete',
      description: 'Delete a dashboard',
      inputSchema: {
        type: 'object',
        properties: {
          dashboard_id: {
            type: 'number',
            description: 'Dashboard ID to delete'
          }
        },
        required: ['dashboard_id']
      }
    },
    {
      name: 'mb_dashboard_card_update',
      description: 'Update card position and size on a dashboard',
      inputSchema: {
        type: 'object',
        properties: {
          dashboard_id: {
            type: 'number',
            description: 'Dashboard ID'
          },
          card_id: {
            type: 'number',
            description: 'Dashboard card ID'
          },
          row: { type: 'number', description: 'New row position' },
          col: { type: 'number', description: 'New column position' },
          size_x: { type: 'number', description: 'Width in grid units' },
          size_y: { type: 'number', description: 'Height in grid units' }
        },
        required: ['dashboard_id', 'card_id']
      }
    },
    {
      name: 'mb_dashboard_card_remove',
      description: 'Remove a card from a dashboard',
      inputSchema: {
        type: 'object',
        properties: {
          dashboard_id: {
            type: 'number',
            description: 'Dashboard ID'
          },
          card_id: {
            type: 'number',
            description: 'Dashboard card ID to remove'
          }
        },
        required: ['dashboard_id', 'card_id']
      }
    },
    // ==================== COPY/CLONE OPERATIONS ====================
    {
      name: 'mb_card_copy',
      description: 'Copy a card/question to a new location',
      inputSchema: {
        type: 'object',
        properties: {
          card_id: {
            type: 'number',
            description: 'Source card ID'
          },
          collection_id: {
            type: 'number',
            description: 'Destination collection ID'
          },
          new_name: {
            type: 'string',
            description: 'Name for the copy (optional, defaults to "Copy of [original]")'
          }
        },
        required: ['card_id']
      }
    },
    {
      name: 'mb_card_clone',
      description: 'Clone a card and retarget to a different table (for template cards)',
      inputSchema: {
        type: 'object',
        properties: {
          card_id: {
            type: 'number',
            description: 'Source card ID'
          },
          target_table_id: {
            type: 'number',
            description: 'New target table ID'
          },
          collection_id: {
            type: 'number',
            description: 'Destination collection ID'
          },
          column_mappings: {
            type: 'object',
            description: 'Column name mappings from source to target table'
          }
        },
        required: ['card_id', 'target_table_id']
      }
    },
    {
      name: 'mb_dashboard_copy',
      description: 'Copy a dashboard with all its cards',
      inputSchema: {
        type: 'object',
        properties: {
          dashboard_id: {
            type: 'number',
            description: 'Source dashboard ID'
          },
          collection_id: {
            type: 'number',
            description: 'Destination collection ID'
          },
          new_name: {
            type: 'string',
            description: 'Name for the copy'
          },
          deep_copy: {
            type: 'boolean',
            default: true,
            description: 'If true, also copies all cards. If false, links to existing cards.'
          }
        },
        required: ['dashboard_id']
      }
    },
    {
      name: 'mb_collection_copy',
      description: 'Copy an entire collection with all contents',
      inputSchema: {
        type: 'object',
        properties: {
          collection_id: {
            type: 'number',
            description: 'Source collection ID'
          },
          destination_id: {
            type: 'number',
            description: 'Destination parent collection ID'
          },
          new_name: {
            type: 'string',
            description: 'Name for the copy'
          }
        },
        required: ['collection_id']
      }
    },
    // ==================== SEARCH ====================
    {
      name: 'mb_search',
      description: 'Search across all Metabase items (cards, dashboards, collections, tables)',
      inputSchema: {
        type: 'object',
        properties: {
          query: {
            type: 'string',
            description: 'Search query text'
          },
          models: {
            type: 'array',
            items: {
              type: 'string',
              enum: ['card', 'dashboard', 'collection', 'table', 'database', 'pulse']
            },
            description: 'Filter by item types (default: all)'
          },
          collection_id: {
            type: 'number',
            description: 'Search within specific collection'
          },
          limit: {
            type: 'number',
            default: 50,
            description: 'Maximum results to return'
          }
        },
        required: ['query']
      }
    },
    // ==================== SEGMENTS ====================
    {
      name: 'mb_segment_create',
      description: 'Create a segment (reusable filter) for a table',
      inputSchema: {
        type: 'object',
        properties: {
          name: {
            type: 'string',
            description: 'Segment name'
          },
          description: {
            type: 'string',
            description: 'Segment description'
          },
          table_id: {
            type: 'number',
            description: 'Table ID to create segment for'
          },
          definition: {
            type: 'object',
            description: 'MBQL filter definition'
          }
        },
        required: ['name', 'table_id', 'definition']
      }
    },
    {
      name: 'mb_segment_list',
      description: 'List all segments',
      inputSchema: {
        type: 'object',
        properties: {
          table_id: {
            type: 'number',
            description: 'Filter by table ID (optional)'
          }
        }
      }
    },
    // ==================== BOOKMARKS ====================
    {
      name: 'mb_bookmark_create',
      description: 'Bookmark an item for quick access',
      inputSchema: {
        type: 'object',
        properties: {
          type: {
            type: 'string',
            enum: ['card', 'dashboard', 'collection'],
            description: 'Item type to bookmark'
          },
          id: {
            type: 'number',
            description: 'Item ID to bookmark'
          }
        },
        required: ['type', 'id']
      }
    },
    {
      name: 'mb_bookmark_list',
      description: 'List all bookmarked items',
      inputSchema: {
        type: 'object',
        properties: {}
      }
    },
    {
      name: 'mb_bookmark_delete',
      description: 'Remove a bookmark',
      inputSchema: {
        type: 'object',
        properties: {
          type: {
            type: 'string',
            enum: ['card', 'dashboard', 'collection'],
            description: 'Item type'
          },
          id: {
            type: 'number',
            description: 'Item ID'
          }
        },
        required: ['type', 'id']
      }
    },
    // ==================== DATABASE SYNC & CACHE ====================
    {
      name: 'db_sync_schema',
      description: 'Trigger schema sync for a database',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to sync'
          }
        },
        required: ['database_id']
      }
    },
    {
      name: 'mb_cache_invalidate',
      description: 'Invalidate cache for specific items or entire database',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID (optional, invalidates all if provided alone)'
          },
          card_id: {
            type: 'number',
            description: 'Specific card ID to invalidate'
          }
        }
      }
    },

    // === METADATA & ANALYTICS ===
    {
      name: 'mb_meta_query_performance',
      description: 'Get comprehensive query performance statistics from Metabase metadata - analyze execution times, cache hit rates, error rates, and identify slow queries. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 7)',
            default: 7
          },
          include_slow_queries: {
            type: 'boolean',
            description: 'Include detailed slow query analysis (default: true)',
            default: true
          },
          slow_threshold_ms: {
            type: 'number',
            description: 'Threshold for slow queries in milliseconds (default: 10000)',
            default: 10000
          }
        }
      }
    },
    {
      name: 'mb_meta_content_usage',
      description: 'Analyze content usage patterns - find popular questions/dashboards, unused content, orphaned cards. Great for content cleanup and optimization. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 30)',
            default: 30
          },
          unused_threshold_days: {
            type: 'number',
            description: 'Days without usage to consider content "unused" (default: 90)',
            default: 90
          },
          limit: {
            type: 'number',
            description: 'Number of top items to return (default: 20)',
            default: 20
          }
        }
      }
    },
    {
      name: 'mb_meta_user_activity',
      description: 'Get user activity statistics - active users, inactive users, query patterns, login history. Useful for license optimization and user engagement analysis. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 30)',
            default: 30
          },
          inactive_threshold_days: {
            type: 'number',
            description: 'Days without activity to consider user "inactive" (default: 90)',
            default: 90
          },
          include_login_history: {
            type: 'boolean',
            description: 'Include login timeline data (default: true)',
            default: true
          }
        }
      }
    },
    {
      name: 'mb_meta_database_usage',
      description: 'Analyze database usage patterns - query counts, performance, errors by database and table. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 30)',
            default: 30
          },
          database_id: {
            type: 'number',
            description: 'Optional: analyze specific database tables'
          }
        }
      }
    },
    {
      name: 'mb_meta_dashboard_complexity',
      description: 'Analyze dashboard complexity - card counts, load times, performance issues. Identify dashboards that need optimization. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {}
      }
    },
    {
      name: 'mb_meta_info',
      description: 'Get overview of Metabase metadata database - active users, questions, dashboards, recent activity. Quick health check. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {}
      }
    },

    // === PHASE 2: ADVANCED ANALYTICS ===
    {
      name: 'mb_meta_table_dependencies',
      description: 'Analyze table dependencies - find all questions and dashboards that depend on a specific table. Essential for impact analysis before schema changes. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID containing the table'
          },
          table_name: {
            type: 'string',
            description: 'Name of the table to analyze'
          },
          schema_name: {
            type: 'string',
            description: 'Schema name (optional, recommended for disambiguation)'
          }
        },
        required: ['database_id', 'table_name']
      }
    },
    {
      name: 'mb_meta_impact_analysis',
      description: 'Analyze impact of removing a table - breaking changes, affected questions/dashboards, severity assessment, and recommendations. Critical for safe database migrations. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID'
          },
          table_name: {
            type: 'string',
            description: 'Table name to analyze for removal impact'
          },
          schema_name: {
            type: 'string',
            description: 'Schema name (optional)'
          }
        },
        required: ['database_id', 'table_name']
      }
    },
    {
      name: 'mb_meta_optimization_recommendations',
      description: 'Get comprehensive optimization recommendations - index suggestions, materialized view candidates, and cache optimization. Data-driven performance improvements. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          database_id: {
            type: 'number',
            description: 'Database ID to analyze'
          },
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 30)',
            default: 30
          },
          include_matview_candidates: {
            type: 'boolean',
            description: 'Include materialized view recommendations (default: true)',
            default: true
          },
          include_cache_recommendations: {
            type: 'boolean',
            description: 'Include cache optimization suggestions (default: true)',
            default: true
          }
        },
        required: ['database_id']
      }
    },
    {
      name: 'mb_meta_error_patterns',
      description: 'Analyze error patterns and categorize recurring errors - identify systemic issues, suggest resolutions, find questions with high error rates. Proactive error management. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          days: {
            type: 'number',
            description: 'Number of days to analyze (default: 30)',
            default: 30
          },
          include_recurring_questions: {
            type: 'boolean',
            description: 'Include questions with recurring errors (default: true)',
            default: true
          },
          include_timeline: {
            type: 'boolean',
            description: 'Include temporal error analysis (default: true)',
            default: true
          }
        }
      }
    },

    // === PHASE 3: EXPORT/IMPORT & MIGRATION ===
    {
      name: 'mb_meta_export_workspace',
      description: '📤 Export workspace to JSON (questions, dashboards, collections). READ-ONLY operation - safe to execute. Perfect for backups and migrations. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          include_questions: {
            type: 'boolean',
            description: 'Include questions (default: true)',
            default: true
          },
          include_dashboards: {
            type: 'boolean',
            description: 'Include dashboards (default: true)',
            default: true
          },
          include_collections: {
            type: 'boolean',
            description: 'Include collections (default: true)',
            default: true
          },
          collection_ids: {
            type: 'array',
            description: 'Optional: Export specific collections only (array of IDs)',
            items: { type: 'number' }
          },
          archived: {
            type: 'boolean',
            description: 'Include archived items (default: false)',
            default: false
          }
        }
      }
    },
    {
      name: 'mb_meta_import_preview',
      description: '🔍 Preview import impact WITHOUT making changes (dry-run). Analyzes conflicts, detects issues, provides recommendations. ALWAYS run this before actual import. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          workspace: {
            type: 'object',
            description: 'Workspace data from export_workspace'
          }
        },
        required: ['workspace']
      }
    },
    {
      name: 'mb_meta_compare_environments',
      description: '🔄 Compare current environment with another (dev → staging → prod). Identifies drift, missing items, and differences. READ-ONLY operation. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          target_workspace: {
            type: 'object',
            description: 'Workspace export from target environment'
          }
        },
        required: ['target_workspace']
      }
    },
    {
      name: 'mb_meta_auto_cleanup',
      description: '🧹 Auto-cleanup unused content with SAFETY CHECKS. ⚠️ DRY-RUN by default, requires approved:true for execution. Finds unused questions (180+ days), orphaned cards, empty collections, broken questions. Requires MB_METADATA_ENABLED=true.',
      inputSchema: {
        type: 'object',
        properties: {
          dry_run: {
            type: 'boolean',
            description: '🔒 SAFETY: Dry-run mode (default: true). Set false to execute',
            default: true
          },
          approved: {
            type: 'boolean',
            description: '🔒 SAFETY: Requires explicit approval (default: false). Set true to execute',
            default: false
          },
          unused_days: {
            type: 'number',
            description: 'Days without usage to consider content unused (default: 180)',
            default: 180
          },
          orphaned_cards: {
            type: 'boolean',
            description: 'Include orphaned cards (not in dashboards) (default: true)',
            default: true
          },
          empty_collections: {
            type: 'boolean',
            description: 'Include empty collections (default: true)',
            default: true
          },
          broken_questions: {
            type: 'boolean',
            description: 'Include questions with 100% error rate (default: true)',
            default: true
          }
        }
      }
    }
  ]);
}
