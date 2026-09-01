/**
 * dbt-Smart Card & Question Creator
 * Generates verified, business-rule-compliant Metabase questions/cards using
 * active dbt semantic memory, auto-healing SQL execution, and executive visual styling.
 */

import { globalSemanticMemory } from '../semantic/semantic-memory.js';
import { SqlHealingEngine } from '../ai/sql-healing-engine.js';
import { logger } from '../utils/logger.js';

export class DbtSmartCardBuilder {
  constructor(metabaseClient, assistant = null) {
    this.client = metabaseClient;
    this.assistant = assistant;
    this.healingEngine = new SqlHealingEngine({
      metabaseClient: metabaseClient || null,
      aiAssistant: assistant || null,
    });
  }

  /**
   * Determine best Metabase display chart type from columns and rows
   */
  inferDisplayType(columns = [], rows = []) {
    if (!columns || columns.length === 0) return 'table';

    // 1. Single value / scalar metric -> number card
    if (columns.length === 1 && rows.length === 1) {
      return 'scalar';
    }

    const colNames = columns.map(c => String(c.name || c).toLowerCase());
    const hasTemporalCol = colNames.some(n => n.includes('date') || n.includes('month') || n.includes('day') || n.includes('time') || n.includes('year'));
    const hasNumericCol = columns.length >= 2;

    // 2. Date + Metric -> Line trend
    if (hasTemporalCol && hasNumericCol) {
      return 'line';
    }

    // 3. Category + Metric -> Bar chart
    if (columns.length === 2 && !hasTemporalCol) {
      return 'bar';
    }

    // 4. Default to detailed table
    return 'table';
  }

  /**
   * Build smart Metabase card with dbt semantic memory injection and healing
   */
  async createSmartCard(options = {}) {
    const {
      question,
      database_id,
      collection_id = null,
      custom_name = null,
      description = null,
      dry_run = false,
    } = options;

    if (!question || !database_id) {
      throw new Error('question and database_id are required to create a dbt-smart card.');
    }

    // 1. Fetch active dbt semantic memory rules
    const semanticRulesContext = globalSemanticMemory.getActiveContextForQuery([question]);
    
    // 2. Fetch database tables
    const tables = await this.client.getDatabaseTables(database_id);

    // 3. Generate initial SQL with AI assistant + injected business rules
    let initialSql = '';
    if (this.assistant) {
      initialSql = await this.assistant.generateSQL(
        `${question}\n\n${semanticRulesContext}`,
        tables
      );
    } else {
      // Fallback simple query
      initialSql = `SELECT * FROM ${tables[0]?.name || 'orders'} LIMIT 50;`;
    }

    // 4. Execute and self-heal SQL query
    const healResult = await this.healingEngine.executeAndHeal({
      sql: initialSql,
      database_id,
      max_retries: 3,
      context_description: question,
    });

    if (healResult.error) {
      throw new Error(`Failed to generate working SQL for question "${question}": ${healResult.error}`);
    }

    const finalSql = healResult.final_sql || initialSql;
    const sampleData = healResult.data || {};
    const columns = sampleData.columns || [];
    const rows = sampleData.rows || [];

    // 5. Infer display visualization settings
    const displayType = this.inferDisplayType(columns, rows);
    const cardName = custom_name || `[dbt Verified] ${question.length > 50 ? question.substring(0, 47) + '...' : question}`;

    const cardPayload = {
      name: cardName,
      description: description || `Auto-generated from natural language with dbt semantic rules: "${question}"`,
      collection_id: collection_id || null,
      dataset_query: {
        database: database_id,
        type: 'native',
        native: {
          query: finalSql,
          'template-tags': {},
        },
      },
      display: displayType,
      visualization_settings: {
        'table.pivot': false,
        'table.cell_column': columns[0]?.name || null,
      },
    };

    let createdCard = null;
    if (!dry_run) {
      try {
        createdCard = await this.client.post('/card', cardPayload);
      } catch (err) {
        throw new Error(`Failed to save question in Metabase: ${err.message}`);
      }
    }

    return {
      card_id: createdCard?.id || 'dry_run_card_id',
      name: cardPayload.name,
      display: cardPayload.display,
      sql: finalSql,
      healing_applied: healResult.healing_applied || false,
      healing_trail: healResult.healing_trail || [],
      semantic_rules_applied: semanticRulesContext ? true : false,
      sample_rows_count: rows.length,
      dry_run,
      card: createdCard || cardPayload,
    };
  }
}
