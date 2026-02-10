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
    await this.ensureInitialized();
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
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Card get error: ${error.message}` }] };
    }
  }

  async handleCardUpdate(args) {
    await this.ensureInitialized();
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
    await this.ensureInitialized();
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
    await this.ensureInitialized();
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
    await this.ensureInitialized();
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
    await this.ensureInitialized();
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
    await this.ensureInitialized();
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
}
