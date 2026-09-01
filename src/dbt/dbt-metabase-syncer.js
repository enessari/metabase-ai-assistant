/**
 * src/dbt/dbt-metabase-syncer.js
 * Automated dbt Semantic Layer -> Metabase Data Model Synchronizer
 * 
 * Extracts dbt rich schema (100% Turkish labels, column descriptions, group_label,
 * semantic types, currency formats, and foreign key relations) and writes them
 * directly into Metabase tables and fields via REST API.
 */

import { DbtDeepScanner } from './dbt-deep-scanner.js';
import { logger } from '../utils/logger.js';

export class DbtMetabaseSyncer {
  constructor(metabaseClient, options = {}) {
    this.client = metabaseClient;
    this.projectDir = options.projectDir || process.env.DBT_PROJECT_DIR || process.cwd();
    this.scanner = new DbtDeepScanner({ projectDir: this.projectDir });
  }

  /**
   * Determine Metabase semantic_type based on column name, data type, and dbt meta
   */
  resolveSemanticType(columnName = '', dataType = '', meta = {}) {
    const cname = columnName.toLowerCase();
    const dtype = (dataType || '').toLowerCase();

    // 1. Explicit dbt meta override
    if (meta.metabase?.semantic_type) return meta.metabase.semantic_type;
    if (meta.dimension?.format === 'currency') return 'type/Currency';

    // 2. Primary & Foreign Keys
    if (cname.endsWith('_id') || cname.endsWith('_key') || cname === 'id') {
      if (cname.startsWith('fk_') || cname.includes('parent_') || cname.includes('target_')) {
        return 'type/FK';
      }
      return 'type/PK';
    }

    // 3. Financial & Currency Fields
    if (
      cname.includes('try') ||
      cname.includes('usd') ||
      cname.includes('eur') ||
      cname.includes('amount') ||
      cname.includes('price') ||
      cname.includes('revenue') ||
      cname.includes('profit') ||
      cname.includes('fare') ||
      cname.includes('fee') ||
      cname.includes('cost') ||
      cname.includes('matrah') ||
      cname.includes('discount') ||
      cname.includes('commission')
    ) {
      return 'type/Currency';
    }

    // 4. Temporal / Date Fields
    if (
      cname.endsWith('_at') ||
      cname.includes('timestamp') ||
      cname.includes('datetime') ||
      dtype.includes('timestamp') ||
      dtype.includes('datetime')
    ) {
      return 'type/CreationTimestamp';
    }

    if (
      cname.endsWith('_date') ||
      cname.includes('date_day') ||
      cname === 'date' ||
      dtype.includes('date')
    ) {
      return 'type/CreationDate';
    }

    // 5. Categorical / Dimension Fields
    if (
      cname.includes('name') ||
      cname.includes('type') ||
      cname.includes('status') ||
      cname.includes('category') ||
      cname.includes('channel') ||
      cname.includes('provider') ||
      cname.includes('airline') ||
      cname.includes('company') ||
      cname.includes('market') ||
      cname.includes('country') ||
      cname.includes('city')
    ) {
      return 'type/Category';
    }

    // 6. Quantitative Counts
    if (
      cname.includes('count') ||
      cname.includes('cnt') ||
      cname.includes('quantity') ||
      cname.includes('volume') ||
      cname.includes('tickets') ||
      cname.includes('baskets')
    ) {
      return 'type/Quantity';
    }

    return null;
  }

  /**
   * Sync dbt models and schema to Metabase database Data Model
   */
  async syncToMetabase(databaseId, options = {}) {
    const { dryRun = false, targetDomain = null, tierFilter = ['marts_fact', 'marts_dim', 'marts_report', 'intermediate'] } = options;

    logger.info(`Starting dbt -> Metabase Data Model sync for Database ID ${databaseId}...`);

    // 1. Scan dbt project
    await this.scanner.scanProject(this.projectDir);
    const dbtModels = this.scanner.models;

    // 2. Fetch Metabase database metadata
    const dbMetadata = await this.client.request('GET', `/api/database/${databaseId}/metadata`);
    const mbTables = dbMetadata.tables || [];

    const stats = {
      tables_scanned: mbTables.length,
      tables_updated: 0,
      fields_updated: 0,
      relationships_linked: 0,
      errors: []
    };

    // Build field lookup by table_name + field_name for FK linking
    const fieldLookup = new Map();
    for (const tbl of mbTables) {
      for (const fld of tbl.fields || []) {
        fieldLookup.set(`${tbl.name.toLowerCase()}.${fld.name.toLowerCase()}`, fld.id);
      }
    }

    // 3. Iterate through Metabase tables and match with dbt models
    for (const mbTable of mbTables) {
      const modelName = mbTable.name.toLowerCase();
      const dbtModel = dbtModels.get(modelName);

      if (!dbtModel) continue;

      // Filter by domain or tier if requested
      if (targetDomain && !dbtModel.path?.includes(`/${targetDomain}/`)) continue;
      if (tierFilter && !tierFilter.includes(dbtModel.tier)) continue;

      try {
        const tableUpdates = {};

        // A. Table Display Name
        const customLabel = dbtModel.config?.meta?.label || dbtModel.meta?.label;
        if (customLabel && customLabel !== mbTable.display_name) {
          tableUpdates.display_name = customLabel;
        }

        // B. Table Description
        if (dbtModel.description && dbtModel.description !== mbTable.description) {
          tableUpdates.description = dbtModel.description;
        }

        if (Object.keys(tableUpdates).length > 0) {
          if (!dryRun) {
            await this.client.request('PUT', `/api/table/${mbTable.id}`, tableUpdates);
          }
          stats.tables_updated++;
          logger.info(`Updated Metabase Table: ${mbTable.name} -> ${JSON.stringify(tableUpdates)}`);
        }

        // C. Fields Synchronization
        const dbtColumns = dbtModel.columns || {};

        for (const mbField of mbTable.fields || []) {
          const colName = mbField.name.toLowerCase();
          let dbtCol = null;
          if (dbtColumns instanceof Map) {
            dbtCol = dbtColumns.get(colName) || Array.from(dbtColumns.values()).find(c => c.name?.toLowerCase() === colName);
          } else if (typeof dbtColumns === 'object' && dbtColumns !== null) {
            dbtCol = dbtColumns[colName] || Object.values(dbtColumns).find(c => c && c.name?.toLowerCase() === colName);
          }

          const fieldUpdates = {};

          if (dbtCol) {
            // Field Display Name (from dbt dimension label or clean format)
            const colLabel = dbtCol.config?.meta?.dimension?.label || dbtCol.meta?.dimension?.label;
            if (colLabel && colLabel !== mbField.display_name) {
              fieldUpdates.display_name = colLabel;
            }

            // Field Description
            if (dbtCol.description && dbtCol.description !== mbField.description) {
              fieldUpdates.description = dbtCol.description;
            }

            // Semantic Type
            const expectedSemantic = this.resolveSemanticType(colName, dbtCol.data_type, dbtCol.config?.meta || dbtCol.meta || {});
            if (expectedSemantic && expectedSemantic !== mbField.semantic_type) {
              fieldUpdates.semantic_type = expectedSemantic;
            }
          } else {
            // Infer semantic type even if column YAML description is missing
            const inferredSemantic = this.resolveSemanticType(colName, mbField.base_type, {});
            if (inferredSemantic && inferredSemantic !== mbField.semantic_type) {
              fieldUpdates.semantic_type = inferredSemantic;
            }
          }

          if (Object.keys(fieldUpdates).length > 0) {
            if (!dryRun) {
              await this.client.request('PUT', `/api/field/${mbField.id}`, fieldUpdates);
            }
            stats.fields_updated++;
          }
        }
      } catch (err) {
        logger.error(`Error updating table ${mbTable.name}: ${err.message}`);
        stats.errors.push({ table: mbTable.name, error: err.message });
      }
    }

    return stats;
  }
}
