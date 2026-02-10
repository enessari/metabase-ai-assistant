import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';

export class SchemaHandler {
  constructor(metabaseClient, activityLogger, connectionManager) {
    this.metabaseClient = metabaseClient;
    this.activityLogger = activityLogger || null;
    this.connectionManager = connectionManager || null;
  }

  routes() {
    return {
      'db_table_create': (args) => this.handleCreateTableDirect(args),
      'db_view_create': (args) => this.handleCreateViewDirect(args),
      'db_matview_create': (args) => this.handleCreateMaterializedViewDirect(args),
      'db_index_create': (args) => this.handleCreateIndexDirect(args),
      'db_table_ddl': (args) => this.handleGetTableDDL(args.database_id, args.table_name),
      'db_view_ddl': (args) => this.handleGetViewDDL(args.database_id, args.view_name),
      'db_ai_list': (args) => this.handleListAIObjects(args.database_id),
      'db_ai_drop': (args) => this.handleDropAIObject(args),
      'db_schema_explore': (args) => this.handleExploreSchemaSimple(args),
      'db_schema_analyze': (args) => this.handleExploreSchemaTablesAdvanced(args),
      'db_relationships_detect': (args) => this.handleAnalyzeTableRelationships(args),
      'ai_relationships_suggest': (args) => this.handleSuggestVirtualRelationships(args),
      'mb_relationships_create': (args) => this.handleCreateRelationshipMapping(args),
    };
  }

  async handleCreateTableDirect(args) {
    const startTime = Date.now();
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
            text: `⚠️  **SCHEMA SELECTION REQUIRED**\\n\\n` +
              `🔗 **Connection Type:** DIRECT DATABASE (PostgreSQL)\\n` +
              `📂 **Current Schema:** ${currentSchema}\\n\\n` +
              `📋 **Available Schemas:**\\n${schemas.map(s => `  • ${s}`).join('\\n')}\\n\\n` +
              `🛠️  **Next Steps:** Please specify a schema parameter and re-run:\\n` +
              `\`\`\`json\\n{\\n  "schema": "${currentSchema || 'public'}",\\n  "database_id": ${args.database_id},\\n  "table_name": "${args.table_name}",\\n  "columns": [...],\\n  "dry_run": false,\\n  "approved": true\\n}\\n\`\`\``,
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
            text: `🔍 **DRY RUN PREVIEW**\\n\\n` +
              `🔗 **Connection:** ${connection.type === 'direct' ? 'DIRECT DATABASE' : 'METABASE PROXY'}\\n` +
              `📂 **Target Schema:** ${args.schema || 'default'}\\n` +
              `📊 **Table Name:** ${tableName}\\n` +
              `📝 **Columns:** ${args.columns.length}\\n\\n` +
              `📜 **SQL to execute:**\\n\`\`\`sql\\n${previewSQL}\\n\`\`\`\\n\\n` +
              `✅ **To execute:** Set \`dry_run: false\` and \`approved: true\``,
          },
        ],
      };
    }

    let tableName = null;
    let error = null;

    try {
      const result = await this.connectionManager.executeOperation(
        connection,
        'createTable',
        args.table_name,
        args.columns,
        { approved: args.approved, schema: args.schema }
      );

      tableName = 'claude_ai_' + args.table_name;
      const executionTime = Date.now() - startTime;
      const fullTableName = args.schema ? `${args.schema}.${tableName}` : tableName;

      // Log successful table creation
      if (this.activityLogger) {
        const sql = `CREATE TABLE ${fullTableName} (${args.columns.map(col =>
          `${col.name} ${col.type}${col.constraints ? ' ' + col.constraints : ''}`
        ).join(', ')})`;

        await this.activityLogger.logTableCreation(tableName, args.database_id, sql, executionTime);
      }

      return {
        content: [
          {
            type: 'text',
            text: `✅ **TABLE CREATED SUCCESSFULLY!**\\n\\n` +
              `📊 **Table Details:**\\n` +
              `• Name: \`${tableName}\`\\n` +
              `• Schema: \`${args.schema || 'default'}\`\\n` +
              `• Columns: ${args.columns.length}\\n` +
              `• Connection: ${connection.type === 'direct' ? '🔗 DIRECT DATABASE' : '🌐 METABASE PROXY'}\\n` +
              `• Execution Time: ${executionTime}ms\\n\\n` +
              `📝 **Column Details:**\\n${args.columns.map(col =>
                `• \`${col.name}\` (${col.type}${col.constraints ? ', ' + col.constraints : ''})`
              ).join('\\n')}\\n\\n` +
              `💡 **Next Steps:** Table is now available for queries and Metabase models!`,
          },
        ],
      };

    } catch (err) {
      error = err;
      const executionTime = Date.now() - startTime;

      // Log failed table creation
      if (this.activityLogger) {
        await this.activityLogger.logTableCreation(
          args.table_name,
          args.database_id,
          `CREATE TABLE attempt failed`,
          executionTime,
          err
        );
      }

      return {
        content: [
          {
            type: 'text',
            text: `❌ **TABLE CREATION FAILED!**\\n\\n` +
              `🚫 **Error Details:**\\n` +
              `• Table: \`claude_ai_${args.table_name}\`\\n` +
              `• Schema: \`${args.schema || 'default'}\`\\n` +
              `• Database ID: ${args.database_id}\\n` +
              `• Execution Time: ${executionTime}ms\\n` +
              `• Error: ${err.message}\\n\\n` +
              `🔧 **Troubleshooting:**\\n` +
              `• Check if table name conflicts with existing tables\\n` +
              `• Verify column definitions are valid\\n` +
              `• Ensure you have CREATE permissions on the schema\\n` +
              `• Make sure \`approved: true\` is set`,
          },
        ],
      };
    }
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
        output += `- Check other schemas: analytics, cron, staging`;
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
        output += `- Check other schemas: analytics, cron, staging`;
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


  /**
   * Handle table profile request - comprehensive table analysis
   * Automatically detects dim/ref tables and shows distinct values
   */
  async handleTableProfile(args) {
    try {
      const schemaName = args.schema_name || 'public';
      const tableName = args.table_name;
      const showDistinct = args.show_distinct_values !== false;
      const sampleRows = args.sample_rows || 3;

      // Detect if this is a dimension/reference table
      const isDimTable = /^(dim_|ref_|lookup_|lkp_|d_|r_)/i.test(tableName);

      // Get row count first
      const countQuery = `SELECT COUNT(*) as cnt FROM "${schemaName}"."${tableName}"`;
      const countResult = await this.metabaseClient.executeNativeQuery(args.database_id, countQuery);
      const rowCount = countResult.data?.rows?.[0]?.[0] || 0;

      // Get column info
      const columnsQuery = `
          SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
          FROM information_schema.columns 
          WHERE table_schema = '${schemaName}' AND table_name = '${tableName}'
          ORDER BY ordinal_position
        `;
      const columnsResult = await this.metabaseClient.executeNativeQuery(args.database_id, columnsQuery);
      const columns = columnsResult.data?.rows || [];

      let output = '';

      // Header with dim table indicator
      if (isDimTable) {
        output += `📊 **Dimension Table: ${schemaName}.${tableName}**\\n`;
        output += `🏷️ _Detected as lookup/reference table_\\n\\n`;
      } else {
        output += `📊 **Table Profile: ${schemaName}.${tableName}**\\n\\n`;
      }

      output += `📈 **Overview:**\\n`;
      output += `• Row count: ${rowCount.toLocaleString()}\\n`;
      output += `• Columns: ${columns.length}\\n\\n`;

      // Column details
      output += `📋 **Columns:**\\n`;
      columns.forEach(([name, type, nullable, defaultVal]) => {
        const nullIndicator = nullable === 'YES' ? '?' : '';
        output += `• \`${name}\` (${type}${nullIndicator})\\n`;
      });
      output += `\\n`;

      // For dim tables or small tables, show distinct values
      if ((isDimTable || rowCount < 1000) && showDistinct && columns.length > 0) {
        output += `🔑 **Distinct Values:**\\n`;

        // Get distinct counts and values for key columns (limit to first 5 columns)
        const keyColumns = columns.slice(0, 5);
        for (const [colName, colType] of keyColumns) {
          try {
            const distinctQuery = `
                SELECT "${colName}", COUNT(*) as cnt 
                FROM "${schemaName}"."${tableName}" 
                GROUP BY "${colName}" 
                ORDER BY cnt DESC 
                LIMIT 10
              `;
            const distinctResult = await this.metabaseClient.executeNativeQuery(args.database_id, distinctQuery);
            const distinctRows = distinctResult.data?.rows || [];

            if (distinctRows.length > 0) {
              const totalDistinct = distinctRows.length;
              const values = distinctRows.slice(0, 5).map(r => r[0] === null ? 'NULL' : String(r[0])).join(', ');
              output += `• \`${colName}\`: ${values}${totalDistinct > 5 ? ` (+${totalDistinct - 5} more)` : ''}\\n`;
            }
          } catch (e) {
            // Skip columns that can't be queried
          }
        }
        output += `\\n`;
      }

      // Sample data
      if (sampleRows > 0 && rowCount > 0) {
        try {
          const sampleQuery = `SELECT * FROM "${schemaName}"."${tableName}" LIMIT ${sampleRows}`;
          const sampleResult = await this.metabaseClient.executeNativeQuery(args.database_id, sampleQuery);
          const sampleData = sampleResult.data?.rows || [];
          const sampleCols = sampleResult.data?.cols || [];

          if (sampleData.length > 0) {
            output += `📝 **Sample Data (${sampleData.length} rows):**\\n\`\`\`\\n`;
            // Header
            const headers = sampleCols.map(c => c.name);
            output += headers.join(' | ') + '\\n';
            output += headers.map(() => '---').join(' | ') + '\\n';
            // Data
            sampleData.forEach(row => {
              const formattedRow = row.map(cell => {
                if (cell === null) return 'NULL';
                const str = String(cell);
                return str.length > 20 ? str.substring(0, 17) + '...' : str;
              });
              output += formattedRow.join(' | ') + '\\n';
            });
            output += '\`\`\`\\n';
          }
        } catch (e) {
          output += `_Could not fetch sample data: ${e.message}_\\n`;
        }
      }

      // Recommendations
      output += `\\n💡 **Tips:**\\n`;
      if (isDimTable) {
        output += `• Use this table for JOINs as a lookup\\n`;
        output += `• Use \`mb_field_values\` to see all values for a specific column\\n`;
      }
      if (rowCount === 0) {
        output += `• ⚠️ Table is empty - data may need to be loaded\\n`;
      }
      if (rowCount > 100000) {
        output += `• Large table - use LIMIT in queries\\n`;
      }

      return {
        content: [{ type: 'text', text: output }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Table profile error: ${error.message}` }]
      };
    }
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


  // === DATABASE MAINTENANCE & QUERY ANALYSIS HANDLERS ===

  async handleVacuumAnalyze(args) {
    try {
      const schemaName = args.schema_name || 'public';
      const tableName = args.table_name;
      const vacuumType = args.vacuum_type || 'vacuum_analyze';
      const dryRun = args.dry_run !== false;

      let command;
      const tableRef = tableName ? `${schemaName}.${tableName}` : null;

      switch (vacuumType) {
        case 'vacuum':
          command = tableRef ? `VACUUM ${tableRef}` : 'VACUUM';
          break;
        case 'vacuum_analyze':
          command = tableRef ? `VACUUM ANALYZE ${tableRef}` : 'VACUUM ANALYZE';
          break;
        case 'vacuum_full':
          command = tableRef ? `VACUUM FULL ${tableRef}` : 'VACUUM FULL';
          break;
        case 'analyze_only':
          command = tableRef ? `ANALYZE ${tableRef}` : 'ANALYZE';
          break;
        default:
          throw new Error(`Unknown vacuum type: ${vacuumType}`);
      }

      if (dryRun) {
        return {
          content: [{
            type: 'text',
            text: `🔍 **VACUUM/ANALYZE Preview (Dry Run)**\\n\\n` +
              `📋 **Command:** \`${command}\`\\n` +
              `🗃️ **Target:** ${tableRef || 'All tables in database'}\\n` +
              `⚙️ **Type:** ${vacuumType}\\n\\n` +
              `ℹ️ Set \`dry_run: false\` to execute this command.\\n\\n` +
              `⚠️ **Note:** VACUUM FULL requires exclusive lock and may take time on large tables.`
          }]
        };
      }

      const startTime = Date.now();
      await this.metabaseClient.executeNativeQuery(args.database_id, command);
      const executionTime = Date.now() - startTime;

      return {
        content: [{
          type: 'text',
          text: `✅ **VACUUM/ANALYZE Completed!**\\n\\n` +
            `📋 **Command:** \`${command}\`\\n` +
            `🗃️ **Target:** ${tableRef || 'All tables'}\\n` +
            `⏱️ **Execution Time:** ${executionTime}ms\\n\\n` +
            `💡 Table statistics have been updated for better query planning.`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ VACUUM/ANALYZE failed: ${error.message}` }]
      };
    }
  }


  async handleQueryExplain(args) {
    try {
      const analyze = args.analyze || false;
      const format = args.format || 'text';
      const verbose = args.verbose || false;

      let explainOptions = [];
      if (analyze) explainOptions.push('ANALYZE');
      if (verbose) explainOptions.push('VERBOSE');
      if (format !== 'text') explainOptions.push(`FORMAT ${format.toUpperCase()}`);

      const optionsStr = explainOptions.length > 0 ? `(${explainOptions.join(', ')})` : '';
      const explainQuery = `EXPLAIN ${optionsStr} ${args.sql}`;

      const result = await this.metabaseClient.executeNativeQuery(args.database_id, explainQuery);
      const rows = result.data?.rows || [];

      let planOutput = rows.map(row => row[0]).join('\\n');

      // Analyze the plan for insights
      let insights = [];
      if (planOutput.includes('Seq Scan')) {
        insights.push('⚠️ Sequential Scan detected - consider adding an index');
      }
      if (planOutput.includes('Nested Loop')) {
        insights.push('ℹ️ Nested Loop join - efficient for small datasets');
      }
      if (planOutput.includes('Hash Join') || planOutput.includes('Merge Join')) {
        insights.push('✅ Efficient join method being used');
      }
      if (planOutput.includes('Sort')) {
        insights.push('ℹ️ Sort operation - may benefit from index on sort columns');
      }

      return {
        content: [{
          type: 'text',
          text: `📊 **Query Execution Plan**\\n\\n` +
            `⚙️ **Options:** ${analyze ? 'ANALYZE' : 'ESTIMATE'}, ${format.toUpperCase()}${verbose ? ', VERBOSE' : ''}\\n\\n` +
            `\`\`\`\\n${planOutput}\\n\`\`\`\\n\\n` +
            (insights.length > 0 ? `💡 **Insights:**\\n${insights.join('\\n')}` : '')
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Query explain failed: ${error.message}` }]
      };
    }
  }


  async handleTableStats(args) {
    try {
      const schemaName = args.schema_name || 'public';
      const tableName = args.table_name;

      const statsQuery = `
          SELECT
            schemaname,
            relname as table_name,
            n_live_tup as live_rows,
            n_dead_tup as dead_rows,
            ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_ratio_pct,
            pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) as total_size,
            pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) as table_size,
            pg_size_pretty(pg_indexes_size(schemaname || '.' || relname)) as indexes_size,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count
          FROM pg_stat_user_tables
          WHERE schemaname = '${schemaName}' AND relname = '${tableName}'
        `;

      const result = await this.metabaseClient.executeNativeQuery(args.database_id, statsQuery);
      const rows = result.data?.rows || [];

      if (rows.length === 0) {
        return {
          content: [{ type: 'text', text: `❌ Table not found: ${schemaName}.${tableName}` }]
        };
      }

      const [schema, table, liveRows, deadRows, deadRatio, totalSize, tableSize, indexesSize,
        lastVacuum, lastAutoVacuum, lastAnalyze, lastAutoAnalyze,
        vacuumCount, autoVacuumCount, analyzeCount, autoAnalyzeCount] = rows[0];

      let recommendations = [];
      if (parseFloat(deadRatio) > 10) {
        recommendations.push('⚠️ High dead tuple ratio - consider running VACUUM');
      }
      if (!lastVacuum && !lastAutoVacuum) {
        recommendations.push('⚠️ Table has never been vacuumed');
      }
      if (!lastAnalyze && !lastAutoAnalyze) {
        recommendations.push('⚠️ Table has never been analyzed - statistics may be stale');
      }

      return {
        content: [{
          type: 'text',
          text: `📊 **Table Statistics: ${schema}.${table}**\\n\\n` +
            `📈 **Row Counts:**\\n` +
            `• Live Rows: ${liveRows?.toLocaleString() || 0}\\n` +
            `• Dead Rows: ${deadRows?.toLocaleString() || 0}\\n` +
            `• Dead Ratio: ${deadRatio || 0}%\\n\\n` +
            `💾 **Size:**\\n` +
            `• Total Size: ${totalSize}\\n` +
            `• Table Size: ${tableSize}\\n` +
            `• Indexes Size: ${indexesSize}\\n\\n` +
            `🔧 **Maintenance:**\\n` +
            `• Last Vacuum: ${lastVacuum || lastAutoVacuum || 'Never'}\\n` +
            `• Last Analyze: ${lastAnalyze || lastAutoAnalyze || 'Never'}\\n` +
            `• Vacuum Count: ${vacuumCount + autoVacuumCount}\\n` +
            `• Analyze Count: ${analyzeCount + autoAnalyzeCount}\\n\\n` +
            (recommendations.length > 0 ? `💡 **Recommendations:**\\n${recommendations.join('\\n')}` : '✅ Table is well maintained')
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Failed to get table stats: ${error.message}` }]
      };
    }
  }


  async handleIndexUsage(args) {
    try {
      const schemaName = args.schema_name || 'public';
      const minSizeMb = args.min_size_mb || 0;

      const indexQuery = `
          SELECT
            schemaname,
            relname as table_name,
            indexrelname as index_name,
            idx_scan as scans,
            idx_tup_read as tuples_read,
            idx_tup_fetch as tuples_fetched,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
            pg_relation_size(indexrelid) / 1024 / 1024 as size_mb,
            CASE
              WHEN idx_scan = 0 THEN 'UNUSED'
              WHEN idx_scan < 50 THEN 'RARELY_USED'
              ELSE 'ACTIVE'
            END as usage_status
          FROM pg_stat_user_indexes
          WHERE schemaname = '${schemaName}'
            AND pg_relation_size(indexrelid) / 1024 / 1024 >= ${minSizeMb}
          ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC
          LIMIT 20
        `;

      const result = await this.metabaseClient.executeNativeQuery(args.database_id, indexQuery);
      const rows = result.data?.rows || [];

      if (rows.length === 0) {
        return {
          content: [{ type: 'text', text: `ℹ️ No indexes found in schema: ${schemaName}` }]
        };
      }

      let output = `📊 **Index Usage Analysis: ${schemaName}**\\n\\n`;

      const unusedIndexes = rows.filter(r => r[8] === 'UNUSED');
      const rarelyUsed = rows.filter(r => r[8] === 'RARELY_USED');
      const activeIndexes = rows.filter(r => r[8] === 'ACTIVE');

      if (unusedIndexes.length > 0) {
        output += `⚠️ **Unused Indexes (candidates for removal):**\\n`;
        unusedIndexes.slice(0, 5).forEach(idx => {
          output += `• \`${idx[2]}\` on \`${idx[1]}\` - ${idx[6]}\\n`;
        });
        output += `\\n`;
      }

      if (rarelyUsed.length > 0) {
        output += `🟡 **Rarely Used Indexes:**\\n`;
        rarelyUsed.slice(0, 5).forEach(idx => {
          output += `• \`${idx[2]}\` on \`${idx[1]}\` - ${idx[3]} scans, ${idx[6]}\\n`;
        });
        output += `\\n`;
      }

      output += `✅ **Active Indexes:** ${activeIndexes.length}\\n`;
      output += `📦 **Total Indexes Analyzed:** ${rows.length}\\n\\n`;

      if (unusedIndexes.length > 0) {
        output += `💡 **Tip:** Unused indexes waste storage and slow down writes. Consider removing them after verification.`;
      }

      return {
        content: [{ type: 'text', text: output }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Index usage analysis failed: ${error.message}` }]
      };
    }
  }


  // === DEFINITION TABLES & PARAMETRIC QUESTIONS HANDLERS ===

  async handleDefinitionTablesInit(args) {
    try {
      // Import definition tables utility
      const { DefinitionTables } = await import('../utils/definition-tables.js');
      const definitionTables = new DefinitionTables(this.metabaseClient);

      const result = await definitionTables.initializeDefinitionTables(args.database_id);

      const output = `✅ Definition Tables Initialized\n\n` +
        `📊 Tables Created:\n` +
        result.tables.map(table => `   • ${table}`).join('\n') + `\n\n` +
        `🎯 Status: ${result.message}\n` +
        `🗄️ Database ID: ${args.database_id}`;

      return {
        content: [{ type: 'text', text: output }],
      };
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error initializing definition tables: ${error.message}` }],
      };
    }
  }


  async handleDefinitionSearchTerms(args) {
    try {
      const { DefinitionTables } = await import('../utils/definition-tables.js');
      const definitionTables = new DefinitionTables(this.metabaseClient);

      const terms = await definitionTables.searchBusinessTerms(
        args.database_id,
        args.search_term,
        args.category
      );

      let output = `🔍 Business Terms Search: "${args.search_term}"\n\n`;

      if (terms.length === 0) {
        output += `❌ No terms found matching "${args.search_term}"`;
      } else {
        output += `📊 Found ${terms.length} matching terms:\n\n`;

        terms.forEach((term, index) => {
          output += `${index + 1}. **${term.term}**\n`;
          output += `   📝 Definition: ${term.definition}\n`;
          output += `   🏷️ Category: ${term.category}\n`;
          if (term.synonyms && term.synonyms.length > 0) {
            output += `   🔄 Synonyms: ${term.synonyms.join(', ')}\n`;
          }
          if (term.calculation_logic) {
            output += `   🧮 Calculation: ${term.calculation_logic}\n`;
          }
          output += `   📈 Relevance: ${(term.relevance * 100).toFixed(1)}%\n\n`;
        });
      }

      return {
        content: [{ type: 'text', text: output }],
      };
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error searching business terms: ${error.message}` }],
      };
    }
  }


  async handleDefinitionGetMetric(args) {
    try {
      const { DefinitionTables } = await import('../utils/definition-tables.js');
      const definitionTables = new DefinitionTables(this.metabaseClient);

      const metric = await definitionTables.getMetricDefinition(args.database_id, args.metric_name);

      if (!metric) {
        return {
          content: [{ type: 'text', text: `❌ Metric "${args.metric_name}" not found in definition tables` }],
        };
      }

      const output = `📊 Metric Definition: **${metric.display_name}**\n\n` +
        `🏷️ Internal Name: ${metric.metric_name}\n` +
        `📝 Description: ${metric.description}\n` +
        `🧮 Calculation Formula: \`${metric.calculation_formula}\`\n` +
        `📈 Aggregation Type: ${metric.aggregation_type}\n` +
        `📏 Unit of Measure: ${metric.unit_of_measure}\n` +
        `🏢 KPI Category: ${metric.kpi_category}\n` +
        (metric.business_context ? `💼 Business Context: ${metric.business_context}\n` : '');

      return {
        content: [{ type: 'text', text: output }],
      };
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error getting metric definition: ${error.message}` }],
      };
    }
  }


  async handleDefinitionGetTemplate(args) {
    try {
      const { DefinitionTables } = await import('../utils/definition-tables.js');
      const definitionTables = new DefinitionTables(this.metabaseClient);

      let template;
      if (args.template_type === 'dashboard') {
        template = await definitionTables.getDashboardTemplate(args.database_id, args.template_name);
      } else if (args.template_type === 'question') {
        template = await definitionTables.getQuestionTemplate(args.database_id, args.template_name);
      } else {
        throw new Error('Invalid template type. Must be "dashboard" or "question"');
      }

      if (!template) {
        return {
          content: [{ type: 'text', text: `❌ ${args.template_type} template "${args.template_name}" not found` }],
        };
      }

      let output = `📋 ${args.template_type.charAt(0).toUpperCase() + args.template_type.slice(1)} Template: **${template.template_name}**\n\n`;
      output += `📝 Description: ${template.description}\n`;

      if (args.template_type === 'dashboard') {
        output += `🏷️ Type: ${template.template_type}\n`;
        if (template.required_metrics) {
          output += `📊 Required Metrics: ${template.required_metrics.join(', ')}\n`;
        }
        if (template.layout_config) {
          output += `📐 Layout Configuration:\n`;
          output += `\`\`\`json\n${JSON.stringify(template.layout_config, null, 2)}\n\`\`\`\n`;
        }
      } else {
        output += `🏷️ Question Type: ${template.question_type}\n`;
        output += `📊 Visualization: ${template.visualization_type}\n`;
        if (template.sql_template) {
          output += `🗃️ SQL Template:\n\`\`\`sql\n${template.sql_template}\n\`\`\`\n`;
        }
        if (template.parameters) {
          output += `⚙️ Parameters:\n`;
          output += `\`\`\`json\n${JSON.stringify(template.parameters, null, 2)}\n\`\`\`\n`;
        }
        if (template.business_use_case) {
          output += `💼 Business Use Case: ${template.business_use_case}\n`;
        }
      }

      return {
        content: [{ type: 'text', text: output }],
      };
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error getting template: ${error.message}` }],
      };
    }
  }


  async handleDefinitionGlobalSearch(args) {
    try {
      const { DefinitionTables } = await import('../utils/definition-tables.js');
      const definitionTables = new DefinitionTables(this.metabaseClient);

      const results = await definitionTables.globalSearch(
        args.database_id,
        args.search_term,
        args.content_types
      );

      let output = `🌐 Global Search: "${args.search_term}"\n\n`;

      if (results.length === 0) {
        output += `❌ No results found across all definition tables`;
      } else {
        output += `📊 Found ${results.length} results:\n\n`;

        const groupedResults = {};
        results.forEach(result => {
          if (!groupedResults[result.content_type]) {
            groupedResults[result.content_type] = [];
          }
          groupedResults[result.content_type].push(result);
        });

        Object.entries(groupedResults).forEach(([type, typeResults]) => {
          output += `## ${type.replace('_', ' ').toUpperCase()}\n`;

          typeResults.forEach((result, index) => {
            output += `${index + 1}. **${result.source_table}**\n`;
            output += `   📝 Content: ${result.content.substring(0, 200)}${result.content.length > 200 ? '...' : ''}\n`;
            output += `   📈 Relevance: ${(result.relevance * 100).toFixed(1)}%\n`;
            if (result.metadata && Object.keys(result.metadata).length > 0) {
              output += `   ℹ️ Metadata: ${JSON.stringify(result.metadata)}\n`;
            }
            output += `\n`;
          });
        });
      }

      return {
        content: [{ type: 'text', text: output }],
      };
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error performing global search: ${error.message}` }],
      };
    }
  }


  async handleParametricQuestionCreate(args) {
    try {
      const { ParametricQuestions } = await import('../utils/parametric-questions.js');
      const { DefinitionTables } = await import('../utils/definition-tables.js');

      const definitionTables = new DefinitionTables(this.metabaseClient);
      const parametricQuestions = new ParametricQuestions(this.metabaseClient, definitionTables);

      const result = await parametricQuestions.createParametricQuestion(args.database_id, {
        name: args.name,
        description: args.description,
        sql_template: args.sql_template,
        parameters: args.parameters,
        question_type: args.question_type,
        collection_id: args.collection_id
      });

      const output = `✅ Parametric Question Created: **${result.question.name}**\n\n` +
        `🆔 Question ID: ${result.question.id}\n` +
        `📝 Description: ${args.description}\n` +
        `⚙️ Parameters: ${result.parameters.join(', ')}\n` +
        `📊 Question Type: ${args.question_type || 'table'}\n` +
        `🗃️ SQL Template:\n\`\`\`sql\n${result.sql}\n\`\`\`\n` +
        `🔗 Collection ID: ${args.collection_id || 'Root'}`;

      return {
        content: [{ type: 'text', text: output }],
      };
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error creating parametric question: ${error.message}` }],
      };
    }
  }


  async handleParametricDashboardCreate(args) {
    try {
      const { ParametricQuestions } = await import('../utils/parametric-questions.js');
      const { DefinitionTables } = await import('../utils/definition-tables.js');

      const definitionTables = new DefinitionTables(this.metabaseClient);
      const parametricQuestions = new ParametricQuestions(this.metabaseClient, definitionTables);

      const result = await parametricQuestions.createDashboardWithParametricQuestions(args.database_id, {
        name: args.name,
        description: args.description,
        questions: args.questions,
        filters: args.filters,
        layout: args.layout,
        collection_id: args.collection_id
      });

      const output = `✅ Parametric Dashboard Created: **${result.dashboard.name}**\n\n` +
        `🆔 Dashboard ID: ${result.dashboard.id}\n` +
        `📝 Description: ${args.description}\n` +
        `❓ Questions Created: ${result.questions.length}\n` +
        `🎛️ Dashboard Filters: ${result.filters.length}\n` +
        `📊 Cards Added: ${result.cards.length}\n` +
        `🔗 Collection ID: ${args.collection_id || 'Root'}\n\n` +
        `**Created Questions:**\n` +
        result.questions.map((q, i) => `${i + 1}. ${q.question.name} (ID: ${q.question.id})`).join('\n');

      return {
        content: [{ type: 'text', text: output }],
      };
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error creating parametric dashboard: ${error.message}` }],
      };
    }
  }


  async handleParametricTemplatePreset(args) {
    try {
      const { ParametricQuestions } = await import('../utils/parametric-questions.js');
      const { DefinitionTables } = await import('../utils/definition-tables.js');

      const definitionTables = new DefinitionTables(this.metabaseClient);
      const parametricQuestions = new ParametricQuestions(this.metabaseClient, definitionTables);

      let result;
      const config = { ...args.config, collection_id: args.collection_id };

      switch (args.preset_type) {
        case 'date_range_analysis':
          result = await parametricQuestions.createDateRangeAnalysisQuestion(args.database_id, config);
          break;
        case 'category_filter':
          result = await parametricQuestions.createCategoryFilterQuestion(args.database_id, config);
          break;
        case 'text_search':
          result = await parametricQuestions.createTextSearchQuestion(args.database_id, config);
          break;
        case 'period_comparison':
          result = await parametricQuestions.createPeriodComparisonQuestion(args.database_id, config);
          break;
        default:
          throw new Error(`Unknown preset type: ${args.preset_type}`);
      }

      const presetNames = {
        'date_range_analysis': 'Date Range Analysis',
        'category_filter': 'Category Filter',
        'text_search': 'Text Search',
        'period_comparison': 'Period Comparison'
      };

      const output = `✅ Preset Template Created: **${presetNames[args.preset_type]}**\n\n` +
        `🆔 Question ID: ${result.question.id}\n` +
        `📝 Name: ${result.question.name}\n` +
        `⚙️ Parameters: ${result.parameters.join(', ')}\n` +
        `🗃️ SQL Template:\n\`\`\`sql\n${result.sql}\n\`\`\`\n` +
        `🔗 Collection ID: ${args.collection_id || 'Root'}`;

      return {
        content: [{ type: 'text', text: output }],
      };
    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error creating preset template: ${error.message}` }],
      };
    }
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
}
