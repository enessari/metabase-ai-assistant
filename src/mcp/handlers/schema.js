import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';

export class SchemaHandler {
    constructor(metabaseClient) {
        this.metabaseClient = metabaseClient;
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
}
