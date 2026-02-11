import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';

export class CardsHandler {
  constructor(metabaseClient) {
    this.metabaseClient = metabaseClient;
  }

  routes() {
    return {
      'mb_question_create': (args) => this.handleCreateQuestion(args),
      'mb_questions': (args) => this.handleGetQuestions(args?.collection_id),
      'mb_question_create_parametric': (args) => this.handleCreateParametricQuestion(args),
      'mb_card_get': (args) => this.handleCardGet(args),
      'mb_card_update': (args) => this.handleCardUpdate(args),
      'mb_card_delete': (args) => this.handleCardDelete(args),
      'mb_card_archive': (args) => this.handleCardArchive(args),
      'mb_card_data': (args) => this.handleCardData(args),
      'mb_card_copy': (args) => this.handleCardCopy(args),
      'mb_card_clone': (args) => this.handleCardClone(args),
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

  async handleGetQuestions(args) {
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
      structuredContent: {
        questions: questions.map(q => ({ id: q.id, name: q.name })),
        count: questions.length,
      },
    };
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

  async handleCardGet(args) {
    const { card_id } = args;

    try {
      const card = await this.metabaseClient.request('GET', `/api/card/${card_id}`);

      return {
        content: [{
          type: 'text',
          text: `Card Details:\n` +
            `  ID: ${card.id}\n` +
            `  Name: ${card.name}\n` +
            `  Description: ${card.description || 'None'}\n` +
            `  Type: ${card.display}\n` +
            `  Database: ${card.database_id}\n` +
            `  Collection: ${card.collection_id || 'Root'}\n` +
            `  Creator: ${card.creator?.email || 'Unknown'}\n` +
            `  Created: ${card.created_at}\n` +
            `  Updated: ${card.updated_at}\n` +
            `  Archived: ${card.archived}`
        }],
        structuredContent: {
          id: card.id,
          name: card.name,
          description: card.description || null,
          display: card.display,
          database_id: card.database_id,
          collection_id: card.collection_id || null,
          archived: card.archived,
          created_at: card.created_at,
          updated_at: card.updated_at,
        },
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Card get error: ${error.message}` }] };
    }
  }

  async handleCardUpdate(args) {
    const { card_id, ...updates } = args;

    try {
      const card = await this.metabaseClient.request('PUT', `/api/card/${card_id}`, updates);

      return {
        content: [{
          type: 'text',
          text: `✅ Card ${card_id} updated successfully`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Card update error: ${error.message}` }] };
    }
  }

  async handleCardDelete(args) {
    const { card_id } = args;

    try {
      await this.metabaseClient.request('DELETE', `/api/card/${card_id}`);

      return {
        content: [{
          type: 'text',
          text: `✅ Card ${card_id} deleted permanently`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Card delete error: ${error.message}` }] };
    }
  }

  async handleCardArchive(args) {
    const { card_id } = args;

    try {
      await this.metabaseClient.request('PUT', `/api/card/${card_id}`, { archived: true });

      return {
        content: [{
          type: 'text',
          text: `✅ Card ${card_id} archived`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Card archive error: ${error.message}` }] };
    }
  }

  async handleCardData(args) {
    const { card_id, format = 'json', parameters } = args;

    try {
      let endpoint = `/api/card/${card_id}/query`;
      if (format === 'csv') {
        endpoint += '/csv';
      } else if (format === 'xlsx') {
        endpoint += '/xlsx';
      }

      const result = await this.metabaseClient.request('POST', endpoint, { parameters });

      if (format === 'json') {
        const data = result.data || result;
        const rows = data.rows || [];
        const cols = data.cols || [];

        return {
          content: [{
            type: 'text',
            text: `Card ${card_id} data (${rows.length} rows):\n` +
              `Columns: ${cols.map(c => c.display_name || c.name).join(', ')}\n\n` +
              `Sample (first 10 rows):\n${JSON.stringify(rows.slice(0, 10), null, 2)}`
          }]
        };
      } else {
        return {
          content: [{
            type: 'text',
            text: `Card ${card_id} data exported as ${format.toUpperCase()}`
          }]
        };
      }
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Card data error: ${error.message}` }] };
    }
  }

  async handleCardCopy(args) {
    const { card_id, collection_id, new_name } = args;

    try {
      // Get source card
      const sourceCard = await this.metabaseClient.request('GET', `/api/card/${card_id}`);

      // Create copy
      const newCard = {
        name: new_name || `Copy of ${sourceCard.name}`,
        description: sourceCard.description,
        display: sourceCard.display,
        dataset_query: sourceCard.dataset_query,
        visualization_settings: sourceCard.visualization_settings,
        collection_id: collection_id || sourceCard.collection_id
      };

      const createdCard = await this.metabaseClient.request('POST', '/api/card', newCard);

      return {
        content: [{
          type: 'text',
          text: `✅ Card copied successfully:\n  New Card ID: ${createdCard.id}\n  Name: ${createdCard.name}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Card copy error: ${error.message}` }] };
    }
  }

  async handleCardClone(args) {
    const { card_id, target_table_id, collection_id, column_mappings = {} } = args;

    try {
      // Get source card
      const sourceCard = await this.metabaseClient.request('GET', `/api/card/${card_id}`);

      // Clone and retarget the query
      const query = { ...sourceCard.dataset_query };
      if (query.query) {
        query.query['source-table'] = target_table_id;

        // Apply column mappings if provided
        if (Object.keys(column_mappings).length > 0) {
          // This is simplified - full implementation would need to traverse the query structure
          const queryStr = JSON.stringify(query);
          let mappedQuery = queryStr;
          for (const [oldCol, newCol] of Object.entries(column_mappings)) {
            mappedQuery = mappedQuery.replace(new RegExp(oldCol, 'g'), newCol);
          }
          Object.assign(query, JSON.parse(mappedQuery));
        }
      }

      const newCard = {
        name: `Clone of ${sourceCard.name}`,
        description: `Cloned from card ${card_id}, retargeted to table ${target_table_id}`,
        display: sourceCard.display,
        dataset_query: query,
        visualization_settings: sourceCard.visualization_settings,
        collection_id: collection_id || sourceCard.collection_id
      };

      const createdCard = await this.metabaseClient.request('POST', '/api/card', newCard);

      return {
        content: [{
          type: 'text',
          text: `✅ Card cloned and retargeted:\n  New Card ID: ${createdCard.id}\n  Target Table: ${target_table_id}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Card clone error: ${error.message}` }] };
    }
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
      structuredContent: {
        dashboards: dashboards.map(d => ({ id: d.id, name: d.name })),
        count: dashboards.length,
      },
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
      // Normalize position parameters (support both flat and nested structure)
      // AI sometimes sends flat: { row: 0, col: 0, size_x: 4 }
      // Or nested: { position: { row: 0, col: 0, sizeX: 4 } }
      let position = args.position || {};

      // If args has direct position props, merge them
      if (args.row !== undefined) position.row = args.row;
      if (args.col !== undefined) position.col = args.col;

      // Handle size_x vs sizeX and size_y vs sizeY
      if (args.size_x !== undefined) position.sizeX = args.size_x;
      if (args.size_y !== undefined) position.sizeY = args.size_y;
      if (args.sizeX !== undefined) position.sizeX = args.sizeX;
      if (args.sizeY !== undefined) position.sizeY = args.sizeY;

      // Map back to format expected by client
      // The client expects: Options object with optional row, col, sizeX, sizeY
      // But we need to make sure we pass the right keys to client.addCardToDashboard

      // Create a normalized options object for the client
      const options = {
        row: position.row,
        col: position.col,
        sizeX: position.sizeX || position.size_x,
        sizeY: position.sizeY || position.size_y,
        parameter_mappings: args.parameter_mappings || []
      };

      const result = await this.metabaseClient.addCardToDashboard(
        args.dashboard_id,
        args.question_id,
        options, // Pass normalized options instead of raw args
        args.parameter_mappings // Double pass, just in case (client signature check needed)
      );

      // VERIFICATION: Check if card was actually added
      try {
        const dashboard = await this.metabaseClient.getDashboard(args.dashboard_id);
        const cardExists = dashboard.ordered_cards?.some(c => c.card_id === args.question_id);
        const cardCount = dashboard.ordered_cards?.length || 0;

        if (cardExists) {
          return {
            content: [{
              type: 'text',
              text: `✅ Card verified!\\n` +
                `Dashboard: ${dashboard.name} (ID: ${args.dashboard_id})\\n` +
                `Total cards: ${cardCount}`
            }],
          };
        } else {
          return {
            content: [{
              type: 'text',
              text: `⚠️ Card addition appears to have failed!\\n` +
                `API reported success but card was not found on dashboard.\\n` +
                `Dashboard ID: ${args.dashboard_id}, Question ID: ${args.question_id}\\n` +
                `Please verify the question ID is valid.`
            }],
          };
        }
      } catch (verifyError) {
        // Verification failed but original call might have succeeded
        return {
          content: [{
            type: 'text',
            text: `✅ Card added (verification unavailable)\\n` +
              `Dashboard ID: ${args.dashboard_id}\\n` +
              `Card ID: ${result?.id || 'N/A'}`
          }],
        };
      }

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Card addition error: ${error.message}` }],
      };
    }
  }


  // ==================== DASHBOARD CRUD HANDLERS ====================

  async handleDashboardGet(args) {
    const { dashboard_id } = args;

    try {
      const dashboard = await this.metabaseClient.request('GET', `/api/dashboard/${dashboard_id}`);

      const cards = dashboard.dashcards || dashboard.ordered_cards || [];
      return {
        content: [{
          type: 'text',
          text: `Dashboard Details:\n` +
            `  ID: ${dashboard.id}\n` +
            `  Name: ${dashboard.name}\n` +
            `  Description: ${dashboard.description || 'None'}\n` +
            `  Collection: ${dashboard.collection_id || 'Root'}\n` +
            `  Cards: ${cards.length}\n` +
            `  Parameters: ${(dashboard.parameters || []).length}\n` +
            `  Creator: ${dashboard.creator?.email || 'Unknown'}\n` +
            `  Created: ${dashboard.created_at}\n` +
            `  Updated: ${dashboard.updated_at}\n` +
            `  Embedding Enabled: ${dashboard.enable_embedding || false}`
        }],
        structuredContent: {
          id: dashboard.id,
          name: dashboard.name,
          description: dashboard.description || null,
          cards: cards.map(c => ({ id: c.id, card_id: c.card_id })),
          parameters: dashboard.parameters || [],
        },
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Dashboard get error: ${error.message}` }] };
    }
  }


  async handleDashboardUpdate(args) {
    const { dashboard_id, ...updates } = args;

    try {
      await this.metabaseClient.request('PUT', `/api/dashboard/${dashboard_id}`, updates);

      return {
        content: [{
          type: 'text',
          text: `✅ Dashboard ${dashboard_id} updated successfully`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Dashboard update error: ${error.message}` }] };
    }
  }


  async handleDashboardDelete(args) {
    const { dashboard_id } = args;

    try {
      await this.metabaseClient.request('DELETE', `/api/dashboard/${dashboard_id}`);

      return {
        content: [{
          type: 'text',
          text: `✅ Dashboard ${dashboard_id} deleted`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Dashboard delete error: ${error.message}` }] };
    }
  }


  async handleDashboardCardUpdate(args) {
    const { dashboard_id, card_id, row, col, size_x, size_y } = args;

    try {
      // Get current dashboard
      const dashboard = await this.metabaseClient.request('GET', `/api/dashboard/${dashboard_id}`);
      const cards = dashboard.dashcards || dashboard.ordered_cards || [];

      // Find and update the card
      const cardToUpdate = cards.find(c => c.id === card_id);
      if (!cardToUpdate) {
        return { content: [{ type: 'text', text: `❌ Card ${card_id} not found on dashboard ${dashboard_id}` }] };
      }

      const updatedCard = {
        ...cardToUpdate,
        ...(row !== undefined && { row }),
        ...(col !== undefined && { col }),
        ...(size_x !== undefined && { size_x }),
        ...(size_y !== undefined && { size_y })
      };

      await this.metabaseClient.request('PUT', `/api/dashboard/${dashboard_id}/cards`, {
        cards: cards.map(c => c.id === card_id ? updatedCard : c)
      });

      return {
        content: [{
          type: 'text',
          text: `✅ Dashboard card ${card_id} position/size updated`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Dashboard card update error: ${error.message}` }] };
    }
  }


  async handleDashboardCardRemove(args) {
    const { dashboard_id, card_id } = args;

    try {
      await this.metabaseClient.request('DELETE', `/api/dashboard/${dashboard_id}/cards`, {
        dashcardId: card_id
      });

      return {
        content: [{
          type: 'text',
          text: `✅ Card ${card_id} removed from dashboard ${dashboard_id}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Dashboard card remove error: ${error.message}` }] };
    }
  }


  async handleDashboardCopy(args) {
    const { dashboard_id, collection_id, new_name, deep_copy = true } = args;

    try {
      // Get source dashboard
      const sourceDashboard = await this.metabaseClient.request('GET', `/api/dashboard/${dashboard_id}`);

      // Create new dashboard
      const newDashboard = await this.metabaseClient.request('POST', '/api/dashboard', {
        name: new_name || `Copy of ${sourceDashboard.name}`,
        description: sourceDashboard.description,
        collection_id: collection_id || sourceDashboard.collection_id,
        parameters: sourceDashboard.parameters
      });

      // Copy cards
      const sourceCards = sourceDashboard.dashcards || sourceDashboard.ordered_cards || [];
      const cardIdMap = {};

      for (const dashcard of sourceCards) {
        let cardId = dashcard.card_id;

        // If deep copy, copy the actual card first
        if (deep_copy && cardId) {
          const copiedCard = await this.handleCardCopy({
            card_id: cardId,
            collection_id: collection_id || sourceDashboard.collection_id
          });
          // Extract new card ID from response
          const match = copiedCard.content[0].text.match(/New Card ID: (\d+)/);
          if (match) {
            cardIdMap[cardId] = parseInt(match[1]);
            cardId = cardIdMap[cardId];
          }
        }

        // Add card to new dashboard
        if (cardId) {
          await this.metabaseClient.request('POST', `/api/dashboard/${newDashboard.id}/cards`, {
            cardId: cardId,
            row: dashcard.row,
            col: dashcard.col,
            size_x: dashcard.size_x,
            size_y: dashcard.size_y,
            parameter_mappings: dashcard.parameter_mappings
          });
        }
      }

      return {
        content: [{
          type: 'text',
          text: `✅ Dashboard copied:\n  New Dashboard ID: ${newDashboard.id}\n  Name: ${newDashboard.name}\n  Cards copied: ${sourceCards.length}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Dashboard copy error: ${error.message}` }] };
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
      const filter = await this.metabaseClient.addDashboardFilter(args.dashboard_id, args);

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


  async handleAutoDescribe(args) {
    try {
      const { database_id, target_type = 'all', force_update = false } = args;
      const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
      const aiSignature = `[Generated by AI on ${timestamp}@AI]`;

      let updated = {
        databases: 0,
        tables: 0,
        fields: 0
      };

      // Auto-describe database
      if (target_type === 'database' || target_type === 'all') {
        await this.connectionManager.executeQuery(database_id, `
            UPDATE metabase_database 
            SET description = CASE 
              WHEN name LIKE '%metabase%' OR name LIKE '%app%' THEN CONCAT('Metabase system database containing application metadata, dashboards, user management, and analytics configurations. ', '${aiSignature}')
              WHEN engine = 'postgres' THEN CONCAT('PostgreSQL database containing business data and analytics tables. ', '${aiSignature}')
              WHEN engine = 'mysql' THEN CONCAT('MySQL database for application data storage and reporting. ', '${aiSignature}')
              ELSE CONCAT('Database containing structured data for business intelligence and analytics. ', '${aiSignature}')
            END
            WHERE id = $1 AND (description IS NULL OR description = '' ${force_update ? 'OR TRUE' : ''})
          `, [database_id]);
        updated.databases++;
      }

      // Auto-describe tables
      if (target_type === 'tables' || target_type === 'all') {
        const tableDescriptions = {
          'citizen': `Primary citizen registry table containing personal information and demographic data. Key table for population analytics and citizen management systems. ${aiSignature}`,
          'core_user': `Metabase user management table storing user accounts, authentication details, and user preferences. ${aiSignature}`,
          'report_dashboard': `Dashboard configuration table storing dashboard metadata, layout settings, and user permissions. ${aiSignature}`,
          'report_card': `Question/chart definitions table storing SQL queries, visualization settings, and report configurations. ${aiSignature}`,
          'query_execution': `Query performance monitoring table tracking execution times, success rates, and optimization metrics. ${aiSignature}`,
          'audit_log': `Security audit trail table recording user actions, data access patterns, and system events. ${aiSignature}`
        };

        for (const [tableName, description] of Object.entries(tableDescriptions)) {
          await this.connectionManager.executeQuery(database_id, `
              UPDATE metabase_table 
              SET description = $1
              WHERE name = $2 AND db_id = $3 AND (description IS NULL OR description = '' ${force_update ? 'OR TRUE' : ''})
            `, [description, tableName, database_id]);
          updated.tables++;
        }
      }

      // Auto-describe fields  
      if (target_type === 'fields' || target_type === 'all') {
        const fieldDescriptions = {
          'id': `Primary key identifier for unique record identification. ${aiSignature}`,
          'uid': `Unique identifier for record indexing and relationships. ${aiSignature}`,
          'name': `Name field for identification and display purposes. ${aiSignature}`,
          'email': `Email address for communication and user identification. ${aiSignature}`,
          'created_at': `Record creation timestamp for audit and chronological tracking. ${aiSignature}`,
          'updated_at': `Last modification timestamp for change tracking. ${aiSignature}`,
          'birth_date': `Date of birth for age calculation and demographic analysis. ${aiSignature}`,
          'gender': `Gender classification for demographic analysis and reporting. ${aiSignature}`
        };

        for (const [fieldName, description] of Object.entries(fieldDescriptions)) {
          await this.connectionManager.executeQuery(database_id, `
              UPDATE metabase_field f
              SET description = $1
              FROM metabase_table t
              WHERE f.table_id = t.id 
                AND t.db_id = $2 
                AND f.name = $3 
                AND (f.description IS NULL OR f.description = '' ${force_update ? 'OR TRUE' : ''})
            `, [description, database_id, fieldName]);
          updated.fields++;
        }
      }

      return {
        content: [{
          type: 'text',
          text: `✅ AI descriptions generated successfully!\\n\\n📊 **Summary:**\\n- Databases: ${updated.databases} updated\\n- Tables: ${updated.tables} updated\\n- Fields: ${updated.fields} updated\\n\\n🤖 All descriptions include AI signature: ${aiSignature}\\n\\n💡 **Features:**\\n- Smart categorization based on table names\\n- Contextual descriptions for business intelligence\\n- Timestamp tracking for audit purposes\\n- Batch processing for efficiency`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error generating AI descriptions: ${error.message}` }]
      };
    }
  }


  // === VISUALIZATION HANDLERS ===

  async handleVisualizationSettings(args) {
    try {
      const questionId = args.question_id;

      // Get current question
      const question = await this.metabaseClient.getQuestion(questionId);

      // If updating
      if (args.display || args.settings) {
        const updateData = {};
        if (args.display) updateData.display = args.display;
        if (args.settings) updateData.visualization_settings = args.settings;

        const updated = await this.metabaseClient.updateQuestion(questionId, updateData);

        return {
          content: [{
            type: 'text',
            text: `✅ **Visualization Updated!**\\n\\n` +
              `🆔 Question ID: ${questionId}\\n` +
              `📊 Display Type: ${updated.display || args.display}\\n` +
              `⚙️ Settings Applied: ${Object.keys(args.settings || {}).length} properties`
          }]
        };
      }

      // Return current settings
      return {
        content: [{
          type: 'text',
          text: `📊 **Visualization Settings: ${question.name}**\\n\\n` +
            `🆔 Question ID: ${questionId}\\n` +
            `📈 Display Type: ${question.display}\\n\\n` +
            `⚙️ **Current Settings:**\\n\`\`\`json\\n${JSON.stringify(question.visualization_settings || {}, null, 2)}\\n\`\`\``
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Visualization settings error: ${error.message}` }]
      };
    }
  }


  async handleVisualizationRecommend(args) {
    try {
      const question = await this.metabaseClient.getQuestion(args.question_id);
      const purpose = args.purpose || 'general';

      // Analyze the result metadata
      const resultMetadata = question.result_metadata || [];
      const columnTypes = resultMetadata.map(col => ({
        name: col.name,
        baseType: col.base_type,
        semanticType: col.semantic_type
      }));

      const hasDate = columnTypes.some(c => c.baseType?.includes('Date') || c.baseType?.includes('Timestamp'));
      const hasNumeric = columnTypes.some(c => c.baseType?.includes('Integer') || c.baseType?.includes('Float') || c.baseType?.includes('Decimal'));
      const hasCategory = columnTypes.some(c => c.semanticType?.includes('Category') || c.baseType?.includes('Text'));
      const columnCount = columnTypes.length;

      let recommendations = [];

      if (purpose === 'trend' || (hasDate && hasNumeric)) {
        recommendations.push({
          type: 'line',
          reason: 'Best for showing trends over time',
          settings: { 'graph.dimensions': [columnTypes.find(c => c.baseType?.includes('Date'))?.name] }
        });
      }

      if (purpose === 'comparison' || hasCategory) {
        recommendations.push({
          type: 'bar',
          reason: 'Best for comparing values across categories',
          settings: { 'graph.show_values': true }
        });
      }

      if (purpose === 'composition' || (hasNumeric && columnCount <= 5)) {
        recommendations.push({
          type: 'pie',
          reason: 'Best for showing parts of a whole',
          settings: { 'pie.show_legend': true, 'pie.show_total': true }
        });
      }

      if (purpose === 'kpi' || columnCount === 1) {
        recommendations.push({
          type: 'scalar',
          reason: 'Best for single KPI values',
          settings: {}
        });
      }

      if (purpose === 'distribution') {
        recommendations.push({
          type: 'bar',
          reason: 'Best for showing value distributions',
          settings: { 'graph.x_axis.scale': 'histogram' }
        });
      }

      if (recommendations.length === 0) {
        recommendations.push({ type: 'table', reason: 'Default for complex data', settings: {} });
      }

      let output = `📊 **Visualization Recommendations: ${question.name}**\\n\\n`;
      output += `📋 **Data Profile:**\\n`;
      output += `• Columns: ${columnCount}\\n`;
      output += `• Has Date: ${hasDate ? 'Yes' : 'No'}\\n`;
      output += `• Has Numeric: ${hasNumeric ? 'Yes' : 'No'}\\n`;
      output += `• Has Category: ${hasCategory ? 'Yes' : 'No'}\\n\\n`;

      output += `💡 **Recommendations:**\\n`;
      recommendations.forEach((rec, i) => {
        output += `${i + 1}. **${rec.type.toUpperCase()}** - ${rec.reason}\\n`;
      });

      return {
        content: [{ type: 'text', text: output }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Visualization recommendation failed: ${error.message}` }]
      };
    }
  }


  // === FIELD METADATA HANDLERS ===

  async handleFieldMetadata(args) {
    try {
      const fieldId = args.field_id;

      // Get current field
      const field = await this.metabaseClient.request('GET', `/api/field/${fieldId}`);

      // If updating
      if (args.display_name || args.description || args.semantic_type || args.visibility_type || args.has_field_values) {
        const updateData = {};
        if (args.display_name) updateData.display_name = args.display_name;
        if (args.description) updateData.description = args.description;
        if (args.semantic_type) updateData.semantic_type = args.semantic_type;
        if (args.visibility_type) updateData.visibility_type = args.visibility_type;
        if (args.has_field_values) updateData.has_field_values = args.has_field_values;

        const updated = await this.metabaseClient.request('PUT', `/api/field/${fieldId}`, updateData);

        return {
          content: [{
            type: 'text',
            text: `✅ **Field Metadata Updated!**\\n\\n` +
              `🆔 Field ID: ${fieldId}\\n` +
              `📋 Display Name: ${updated.display_name}\\n` +
              `🏷️ Semantic Type: ${updated.semantic_type || 'None'}\\n` +
              `👁️ Visibility: ${updated.visibility_type}`
          }]
        };
      }

      // Return current metadata
      return {
        content: [{
          type: 'text',
          text: `📋 **Field Metadata: ${field.display_name}**\\n\\n` +
            `🆔 Field ID: ${fieldId}\\n` +
            `📛 Name: ${field.name}\\n` +
            `📋 Display Name: ${field.display_name}\\n` +
            `📝 Description: ${field.description || 'None'}\\n` +
            `🏷️ Semantic Type: ${field.semantic_type || 'None'}\\n` +
            `📊 Base Type: ${field.base_type}\\n` +
            `👁️ Visibility: ${field.visibility_type}\\n` +
            `🔍 Has Field Values: ${field.has_field_values}`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Field metadata error: ${error.message}` }]
      };
    }
  }


  async handleTableMetadata(args) {
    try {
      const tableId = args.table_id;

      // Get current table
      const table = await this.metabaseClient.request('GET', `/api/table/${tableId}`);

      // If updating
      if (args.display_name || args.description || args.visibility_type) {
        const updateData = {};
        if (args.display_name) updateData.display_name = args.display_name;
        if (args.description) updateData.description = args.description;
        if (args.visibility_type) updateData.visibility_type = args.visibility_type;

        const updated = await this.metabaseClient.request('PUT', `/api/table/${tableId}`, updateData);

        return {
          content: [{
            type: 'text',
            text: `✅ **Table Metadata Updated!**\\n\\n` +
              `🆔 Table ID: ${tableId}\\n` +
              `📋 Display Name: ${updated.display_name}\\n` +
              `👁️ Visibility: ${updated.visibility_type}`
          }]
        };
      }

      // Return current metadata
      return {
        content: [{
          type: 'text',
          text: `📋 **Table Metadata: ${table.display_name}**\\n\\n` +
            `🆔 Table ID: ${tableId}\\n` +
            `📛 Name: ${table.name}\\n` +
            `📋 Display Name: ${table.display_name}\\n` +
            `📝 Description: ${table.description || 'None'}\\n` +
            `👁️ Visibility: ${table.visibility_type}\\n` +
            `🗃️ Schema: ${table.schema}\\n` +
            `📊 Fields: ${table.fields?.length || 0}`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Table metadata error: ${error.message}` }]
      };
    }
  }


  async handleFieldValues(args) {
    try {
      const fieldId = args.field_id;

      const values = await this.metabaseClient.request('GET', `/api/field/${fieldId}/values`);

      let output = `📋 **Field Values (ID: ${fieldId})**\\n\\n`;

      if (values.values && values.values.length > 0) {
        const displayValues = values.values.slice(0, 20);
        displayValues.forEach((val, i) => {
          const displayVal = Array.isArray(val) ? val[0] : val;
          output += `${i + 1}. ${displayVal}\\n`;
        });

        if (values.values.length > 20) {
          output += `\\n... and ${values.values.length - 20} more values`;
        }
      } else {
        output += 'No values found or field values not cached.';
      }

      return {
        content: [{ type: 'text', text: output }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Field values error: ${error.message}` }]
      };
    }
  }


  // === EMBEDDING HANDLERS ===

  async handleEmbedUrlGenerate(args) {
    try {
      // Get embedding secret key from environment or settings
      const secretKey = process.env.METABASE_EMBEDDING_SECRET_KEY;

      if (!secretKey) {
        return {
          content: [{
            type: 'text',
            text: `⚠️ **Embedding Secret Key Not Configured**\\n\\n` +
              `Please set METABASE_EMBEDDING_SECRET_KEY in your environment.\\n\\n` +
              `You can find this in Metabase Admin > Settings > Embedding.`
          }]
        };
      }

      // Import JWT library dynamically
      const jwt = await import('jsonwebtoken');

      const resourceType = args.resource_type;
      const resourceId = args.resource_id;
      const params = args.params || {};
      const expMinutes = args.exp_minutes || 10;

      // Create JWT payload
      const payload = {
        resource: { [resourceType]: resourceId },
        params: params,
        exp: Math.round(Date.now() / 1000) + (expMinutes * 60)
      };

      const token = jwt.default.sign(payload, secretKey);

      // Build embed URL
      const baseUrl = process.env.METABASE_URL;
      let embedUrl = `${baseUrl}/embed/${resourceType}/${token}`;

      // Add theme and options
      const urlParams = [];
      if (args.theme && args.theme !== 'light') urlParams.push(`theme=${args.theme}`);
      if (args.bordered === false) urlParams.push('bordered=false');
      if (args.titled === false) urlParams.push('titled=false');

      if (urlParams.length > 0) {
        embedUrl += '#' + urlParams.join('&');
      }

      return {
        content: [{
          type: 'text',
          text: `✅ **Embed URL Generated!**\\n\\n` +
            `📊 Resource: ${resourceType} (ID: ${resourceId})\\n` +
            `⏱️ Expires: ${expMinutes} minutes\\n` +
            `🔒 Parameters: ${Object.keys(params).length} locked\\n\\n` +
            `🔗 **Embed URL:**\\n\`\`\`\\n${embedUrl}\\n\`\`\`\\n\\n` +
            `📋 **HTML:**\\n\`\`\`html\\n<iframe src="${embedUrl}" width="100%" height="600" frameborder="0"></iframe>\\n\`\`\``
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Embed URL generation failed: ${error.message}` }]
      };
    }
  }


  async handleEmbedSettings(args) {
    try {
      // Get embedding settings from Metabase
      const settings = await this.metabaseClient.request('GET', '/api/setting');

      const embeddingEnabled = settings['enable-embedding'] || settings.find?.(s => s.key === 'enable-embedding')?.value;
      const embedSecretSet = !!process.env.METABASE_EMBEDDING_SECRET_KEY;

      return {
        content: [{
          type: 'text',
          text: `📊 **Embedding Settings**\\n\\n` +
            `🔒 Embedding Enabled: ${embeddingEnabled ? 'Yes' : 'No'}\\n` +
            `🔑 Secret Key Configured: ${embedSecretSet ? 'Yes' : 'No'}\\n\\n` +
            `💡 **To Enable Embedding:**\\n` +
            `1. Go to Metabase Admin > Settings > Embedding\\n` +
            `2. Enable embedding and copy the secret key\\n` +
            `3. Set METABASE_EMBEDDING_SECRET_KEY in your environment`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Embed settings error: ${error.message}` }]
      };
    }
  }


  // ==================== SEARCH HANDLER ====================

  async handleSearch(args) {
    const { query, models, collection_id, limit = 50 } = args;

    try {
      let endpoint = `/api/search?q=${encodeURIComponent(query)}`;

      if (models && models.length > 0) {
        endpoint += `&models=${models.join(',')}`;
      }
      if (collection_id) {
        endpoint += `&collection=${collection_id}`;
      }
      endpoint += `&limit=${limit}`;

      const results = await this.metabaseClient.request('GET', endpoint);
      const items = results.data || results;

      // Group by type
      const grouped = {};
      for (const item of items) {
        if (!grouped[item.model]) {
          grouped[item.model] = [];
        }
        grouped[item.model].push(item);
      }

      let output = `Search results for "${query}" (${items.length} items):\n\n`;

      for (const [type, typeItems] of Object.entries(grouped)) {
        output += `${type.toUpperCase()}S (${typeItems.length}):\n`;
        output += typeItems.map(i => `  - [${i.id}] ${i.name}`).join('\n') + '\n\n';
      }

      return {
        content: [{ type: 'text', text: output }],
        structuredContent: {
          results: items.map(i => ({ id: i.id, name: i.name, model: i.model })),
          count: items.length,
        },
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Search error: ${error.message}` }] };
    }
  }


  // ==================== SEGMENT HANDLERS ====================

  async handleSegmentCreate(args) {
    const { name, description, table_id, definition } = args;

    try {
      const segment = await this.metabaseClient.request('POST', '/api/segment', {
        name,
        description,
        table_id,
        definition
      });

      return {
        content: [{
          type: 'text',
          text: `✅ Segment created:\n  ID: ${segment.id}\n  Name: ${segment.name}\n  Table: ${segment.table_id}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Segment create error: ${error.message}` }] };
    }
  }


  async handleSegmentList(args) {
    const { table_id } = args;

    try {
      let segments = await this.metabaseClient.request('GET', '/api/segment');

      if (table_id) {
        segments = segments.filter(s => s.table_id === table_id);
      }

      return {
        content: [{
          type: 'text',
          text: `Found ${segments.length} segments:\n${segments.map(s =>
            `  - [${s.id}] ${s.name} (Table: ${s.table_id})`
          ).join('\n')}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Segment list error: ${error.message}` }] };
    }
  }


  // ==================== BOOKMARK HANDLERS ====================

  async handleBookmarkCreate(args) {
    const { type, id } = args;

    try {
      await this.metabaseClient.request('POST', `/api/${type}/${id}/bookmark`);

      return {
        content: [{
          type: 'text',
          text: `✅ Bookmarked ${type} ${id}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Bookmark create error: ${error.message}` }] };
    }
  }


  async handleBookmarkList(args) {
    try {
      const bookmarks = await this.metabaseClient.request('GET', '/api/bookmark');

      return {
        content: [{
          type: 'text',
          text: `Found ${bookmarks.length} bookmarks:\n${bookmarks.map(b =>
            `  - [${b.type}:${b.item_id}] ${b.name || 'Unnamed'}`
          ).join('\n')}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Bookmark list error: ${error.message}` }] };
    }
  }


  async handleBookmarkDelete(args) {
    const { type, id } = args;

    try {
      await this.metabaseClient.request('DELETE', `/api/${type}/${id}/bookmark`);

      return {
        content: [{
          type: 'text',
          text: `✅ Bookmark removed for ${type} ${id}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Bookmark delete error: ${error.message}` }] };
    }
  }


  // ==================== DATABASE SYNC & CACHE HANDLERS ====================

  async handleDbSyncSchema(args) {
    const { database_id } = args;

    try {
      await this.metabaseClient.request('POST', `/api/database/${database_id}/sync_schema`);

      return {
        content: [{
          type: 'text',
          text: `✅ Schema sync triggered for database ${database_id}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Schema sync error: ${error.message}` }] };
    }
  }


  async handleCacheInvalidate(args) {
    const { database_id, card_id } = args;

    try {
      if (card_id) {
        // Invalidate specific card cache
        await this.metabaseClient.request('POST', `/api/card/${card_id}/query`, {
          ignore_cache: true
        });
        return {
          content: [{
            type: 'text',
            text: `✅ Cache invalidated for card ${card_id}`
          }]
        };
      } else if (database_id) {
        // Invalidate database cache by triggering rescan
        await this.metabaseClient.request('POST', `/api/database/${database_id}/rescan_values`);
        return {
          content: [{
            type: 'text',
            text: `✅ Cache invalidated for database ${database_id}`
          }]
        };
      } else {
        return {
          content: [{
            type: 'text',
            text: `❌ Please specify either database_id or card_id`
          }]
        };
      }
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Cache invalidate error: ${error.message}` }] };
    }
  }


  async handleCollectionCopy(args) {
    const { collection_id, destination_id, new_name } = args;

    try {
      // Get source collection
      const sourceCollection = await this.metabaseClient.request('GET', `/api/collection/${collection_id}`);

      // Create new collection
      const newCollection = await this.metabaseClient.request('POST', '/api/collection', {
        name: new_name || `Copy of ${sourceCollection.name}`,
        description: sourceCollection.description,
        parent_id: destination_id || sourceCollection.parent_id
      });

      // Get items in source collection
      const items = await this.metabaseClient.request('GET', `/api/collection/${collection_id}/items`);
      const allItems = items.data || items;

      let copiedCards = 0;
      let copiedDashboards = 0;

      // Copy each item
      for (const item of allItems) {
        if (item.model === 'card') {
          await this.handleCardCopy({
            card_id: item.id,
            collection_id: newCollection.id
          });
          copiedCards++;
        } else if (item.model === 'dashboard') {
          await this.handleDashboardCopy({
            dashboard_id: item.id,
            collection_id: newCollection.id,
            deep_copy: false // Don't deep copy cards as they're already being copied
          });
          copiedDashboards++;
        }
      }

      return {
        content: [{
          type: 'text',
          text: `✅ Collection copied:\n  New Collection ID: ${newCollection.id}\n  Name: ${newCollection.name}\n  Cards copied: ${copiedCards}\n  Dashboards copied: ${copiedDashboards}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Collection copy error: ${error.message}` }] };
    }
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
}
