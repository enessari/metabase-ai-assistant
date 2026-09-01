/**
 * Metabase-to-dbt Reverse Lineage & Exposure Generator
 * Inspects all Metabase Dashboards and Questions, extracts underlying dbt model dependencies,
 * and generates standard dbt exposures YAML (models/exposures/_metabase__exposures.yml).
 */

import yaml from 'js-yaml';
import { logger } from '../utils/logger.js';

export class MetabaseReverseLineage {
  constructor(metabaseClient) {
    this.client = metabaseClient;
  }

  /**
   * Extract referenced table/model names from SQL query
   */
  extractTablesFromSql(sql) {
    if (!sql || typeof sql !== 'string') return [];
    const tables = new Set();

    // Regex for FROM / JOIN table names
    const regex = /\b(?:FROM|JOIN)\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)/gi;
    let match;
    while ((match = regex.exec(sql)) !== null) {
      let tableName = match[1];
      if (tableName.includes('.')) {
        tableName = tableName.split('.').pop();
      }
      tables.add(tableName.toLowerCase());
    }

    return Array.from(tables);
  }

  /**
   * Generate dbt exposures YAML from Metabase dashboards and cards
   */
  async generateExposures(options = {}) {
    const {
      metabase_base_url = 'https://metabase.company.com',
      include_cards = true,
      include_dashboards = true,
      owner_name = 'Data Engineering & Analytics Team',
      owner_email = 'analytics@company.com',
    } = options;

    const exposures = [];

    // 1. Scan Dashboards
    if (include_dashboards) {
      try {
        const dashboards = await this.client.get('/dashboard');
        for (const dash of dashboards) {
          if (dash.archived) continue;

          // Fetch dashboard details to see cards inside
          const fullDash = await this.client.get(`/dashboard/${dash.id}`);
          const modelDeps = new Set();

          if (Array.isArray(fullDash.ordered_cards)) {
            for (const dashCard of fullDash.ordered_cards) {
              const card = dashCard.card;
              if (!card) continue;

              if (card.dataset_query?.type === 'native' && card.dataset_query.native?.query) {
                const tables = this.extractTablesFromSql(card.dataset_query.native.query);
                tables.forEach(t => modelDeps.add(`ref('${t}')`));
              } else if (card.dataset_query?.type === 'query') {
                const sourceTable = card.dataset_query.query?.['source-table'];
                if (sourceTable) {
                  modelDeps.add(`ref('table_${sourceTable}')`);
                }
              }
            }
          }

          const cleanName = String(dash.name)
            .toLowerCase()
            .replace(/[^a-z0-9_]+/g, '_')
            .replace(/^_+|_+$/g, '');

          exposures.push({
            name: `metabase_dashboard_${dash.id}_${cleanName}`,
            label: `[Metabase] ${dash.name}`,
            type: 'dashboard',
            maturity: 'high',
            url: `${metabase_base_url.replace(/\/$/, '')}/dashboard/${dash.id}`,
            description: dash.description || `Metabase Dashboard: ${dash.name} containing ${fullDash.ordered_cards?.length || 0} visual cards.`,
            depends_on: Array.from(modelDeps),
            owner: {
              name: owner_name,
              email: owner_email,
            },
          });
        }
      } catch (err) {
        logger.warn(`Failed to scan Metabase dashboards for exposures: ${err.message}`);
      }
    }

    // 2. Scan Standalone Cards (Saved Questions)
    if (include_cards) {
      try {
        const cards = await this.client.get('/card');
        for (const card of cards) {
          if (card.archived) continue;

          const modelDeps = new Set();
          if (card.dataset_query?.type === 'native' && card.dataset_query.native?.query) {
            const tables = this.extractTablesFromSql(card.dataset_query.native.query);
            tables.forEach(t => modelDeps.add(`ref('${t}')`));
          }

          if (modelDeps.size > 0) {
            const cleanName = String(card.name)
              .toLowerCase()
              .replace(/[^a-z0-9_]+/g, '_')
              .replace(/^_+|_+$/g, '');

            exposures.push({
              name: `metabase_card_${card.id}_${cleanName}`,
              label: `[Question] ${card.name}`,
              type: 'analysis',
              maturity: 'medium',
              url: `${metabase_base_url.replace(/\/$/, '')}/question/${card.id}`,
              description: card.description || `Saved Metabase Question: ${card.name}`,
              depends_on: Array.from(modelDeps),
              owner: {
                name: owner_name,
                email: owner_email,
              },
            });
          }
        }
      } catch (err) {
        logger.warn(`Failed to scan Metabase cards for exposures: ${err.message}`);
      }
    }

    const payload = {
      version: 2,
      exposures,
    };

    const yamlContent = yaml.dump(payload, {
      indent: 2,
      lineWidth: -1,
      noRefs: true,
    });

    const header = `# =========================================================================\n` +
      `# 🔄 AUTO-GENERATED REVERSE LINEAGE EXPOSURES FROM METABASE\n` +
      `# Generated by Metabase AI Assistant on ${new Date().toISOString()}\n` +
      `# Save to: models/exposures/_metabase__exposures.yml in your dbt project\n` +
      `# =========================================================================\n\n`;

    return {
      total_exposures: exposures.length,
      yaml: header + yamlContent,
      exposures,
    };
  }
}
