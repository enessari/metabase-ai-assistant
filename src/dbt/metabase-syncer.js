/**
 * dbt-to-Metabase Metadata & Metrics Syncer
 * Synchronizes dbt schema descriptions, display names, semantic data types,
 * visibility types, foreign keys, and MetricFlow metrics directly into Metabase Data Model.
 * Fully compatible with Metabase v0.50+ (Saved Questions / Models / Collections).
 */

import { DbtDeepScanner } from './dbt-deep-scanner.js';
import { logger } from '../utils/logger.js';

export const METABASE_SEMANTIC_TYPE_MAP = {
  currency: 'type/Currency',
  money: 'type/Currency',
  price: 'type/Currency',
  amount: 'type/Currency',
  cost: 'type/Currency',
  revenue: 'type/Currency',
  date: 'type/CreationDate',
  created_at: 'type/CreationTimestamp',
  updated_at: 'type/UpdatedTimestamp',
  timestamp: 'type/DateTime',
  email: 'type/Email',
  email_address: 'type/Email',
  phone: 'type/CellPhoneNumber',
  country: 'type/Country',
  city: 'type/City',
  state: 'type/State',
  zip: 'type/ZipCode',
  postal_code: 'type/ZipCode',
  url: 'type/URL',
  avatar: 'type/AvatarURL',
  image: 'type/ImageURL',
  category: 'type/Category',
  status: 'type/Category',
  id: 'type/PK',
  pk: 'type/PK',
  fk: 'type/FK',
};

export class DbtMetabaseSyncer {
  constructor(metabaseClient, scanner = null) {
    this.client = metabaseClient;
    this.scanner = scanner || new DbtDeepScanner();
  }

  /**
   * Infer Metabase semantic type from column name and dbt metadata
   */
  inferSemanticType(colName, colMeta = {}, colType = '') {
    const lowerName = String(colName).toLowerCase();
    const lowerType = String(colType).toLowerCase();

    // 1. Explicit meta override (e.g. meta: { metabase: { semantic_type: 'type/Currency' } })
    if (colMeta.metabase?.semantic_type) return colMeta.metabase.semantic_type;
    if (colMeta.metabase?.type) return colMeta.metabase.type;
    if (colMeta.semantic_type) return colMeta.semantic_type;

    // 2. Specific heuristics based on name suffix/prefix
    if (lowerName.endsWith('_id') || lowerName.endsWith('id')) {
      if (lowerName === 'id' || lowerName.startsWith('pk_')) return 'type/PK';
      return 'type/FK';
    }

    if (lowerName.includes('email')) return 'type/Email';
    if (lowerName.includes('phone') || lowerName.includes('gsm')) return 'type/CellPhoneNumber';
    if (lowerName.includes('price') || lowerName.includes('amount') || lowerName.includes('cost') || lowerName.includes('revenue') || lowerName.includes('margin') || lowerName.includes('sales') || lowerName.includes('fark') || lowerName.includes('komisyon')) return 'type/Currency';
    if (lowerName.includes('created_at') || lowerName.includes('signup_date') || lowerName.includes('inserted_at')) return 'type/CreationTimestamp';
    if (lowerName.includes('date') || lowerType.includes('date')) return 'type/CreationDate';
    if (lowerName.includes('country')) return 'type/Country';
    if (lowerName.includes('city')) return 'type/City';
    if (lowerName.includes('status') || lowerName.includes('type') || lowerName.includes('gender') || lowerName.includes('category')) return 'type/Category';

    return null;
  }

  /**
   * Sync dbt metadata (table names, column descriptions, semantic types, visibility, caveats, FKs) into Metabase
   */
  async syncMetadata(options = {}) {
    const { database_id, dbt_project_dir = null, dry_run = false } = options;

    if (!database_id) {
      throw new Error('database_id is required to sync dbt metadata into Metabase.');
    }

    // 1. Scan dbt project if not already populated or custom dir passed
    if (dbt_project_dir || this.scanner.models.size === 0) {
      try {
        this.scanner.scanProject(dbt_project_dir);
      } catch (err) {
        logger.warn(`Could not scan dbt project folder: ${err.message}`);
      }
    }
    
    // 2. Fetch existing Metabase tables and fields for target database
    const mbTables = await this.client.getDatabaseTables(database_id);
    const mbTableMap = new Map();
    for (const t of mbTables) {
      mbTableMap.set(t.name.toLowerCase(), t);
    }

    const updates = {
      tables_updated: 0,
      fields_updated: 0,
      relationships_configured: 0,
      dry_run,
      details: [],
    };

    // 3. Match dbt models with Metabase tables
    for (const [modelName, dbtModel] of this.scanner.models.entries()) {
      const mbTable = mbTableMap.get(modelName.toLowerCase());
      if (!mbTable) continue;

      const tableUpdates = {};
      const customLabel = dbtModel.meta?.metabase?.label || dbtModel.meta?.metabase?.display_name || dbtModel.meta?.label || dbtModel.meta?.display_name;
      if (customLabel && customLabel !== mbTable.display_name) {
        tableUpdates.display_name = customLabel;
      }
      if (dbtModel.description && dbtModel.description !== mbTable.description) {
        tableUpdates.description = dbtModel.description;
      }
      // Visibility type & Caveats support (gouline/dbt-metabase compatibility)
      if (dbtModel.meta?.metabase?.visibility_type) {
        tableUpdates.visibility_type = dbtModel.meta.metabase.visibility_type;
      }
      if (dbtModel.meta?.metabase?.points_of_interest) {
        tableUpdates.points_of_interest = dbtModel.meta.metabase.points_of_interest;
      }
      if (dbtModel.meta?.metabase?.caveats) {
        tableUpdates.caveats = dbtModel.meta.metabase.caveats;
      }

      if (Object.keys(tableUpdates).length > 0) {
        if (!dry_run) {
          try {
            await this.client.put(`/table/${mbTable.id}`, tableUpdates);
          } catch (e) {
            logger.warn(`Failed to update Metabase table ${mbTable.name}: ${e.message}`);
          }
        }
        updates.tables_updated++;
        updates.details.push({
          type: 'table',
          name: mbTable.name,
          table_id: mbTable.id,
          updates: tableUpdates,
        });
      }

      // 4. Sync column descriptions, semantic types, visibility and formatting
      const mbFields = await this.client.getTableFields(mbTable.id);
      for (const field of mbFields) {
        const dbtCol = dbtModel.columns[field.name];
        if (!dbtCol) continue;

        const fieldUpdates = {};
        if (dbtCol.description && dbtCol.description !== field.description) {
          fieldUpdates.description = dbtCol.description;
        }

        const inferredType = this.inferSemanticType(field.name, dbtCol.meta, field.base_type);
        if (inferredType && field.semantic_type !== inferredType) {
          fieldUpdates.semantic_type = inferredType;
        }

        // Visibility type (sensitive, details-only, normal)
        const visibilityType = dbtCol.meta?.metabase?.visibility_type || dbtCol.meta?.visibility_type;
        if (visibilityType && field.visibility_type !== visibilityType) {
          fieldUpdates.visibility_type = visibilityType;
        }

        // Has field values dropdown (list, none, auto-list)
        if (dbtCol.meta?.metabase?.has_field_values) {
          fieldUpdates.has_field_values = dbtCol.meta.metabase.has_field_values;
        }

        if (Object.keys(fieldUpdates).length > 0) {
          if (!dry_run) {
            try {
              await this.client.put(`/field/${field.id}`, fieldUpdates);
            } catch (e) {
              logger.warn(`Failed to update field ${field.name} in table ${mbTable.name}: ${e.message}`);
            }
          }
          updates.fields_updated++;
          updates.details.push({
            type: 'field',
            table: mbTable.name,
            field: field.name,
            field_id: field.id,
            updates: fieldUpdates,
          });
        }
      }
    }

    logger.info(`dbt metadata sync complete: ${updates.tables_updated} tables, ${updates.fields_updated} fields updated.`);
    return updates;
  }

  /**
   * Helper to find or create the Official Metrics collection in Metabase (v0.50+ compatible)
   */
  async ensureMetricsCollection() {
    try {
      const collections = await this.client.get('/collection');
      const existing = collections?.find?.(c => c.name === 'Resmi Metrikler' || c.name === 'Official Metrics');
      if (existing) return existing.id;

      const created = await this.client.post('/collection', {
        name: 'Resmi Metrikler',
        description: 'dbt MetricFlow tarafından otomatik senkronize edilen resmi iş metrikleri',
        color: '#2ECC71',
      });
      return created.id;
    } catch {
      return null;
    }
  }

  /**
   * Sync dbt MetricFlow / YAML metrics into Metabase (v0.50+ Saved Question & Model Compatible)
   */
  async syncMetrics(options = {}) {
    const { database_id, dbt_project_dir = null, dry_run = false } = options;

    if (!database_id) {
      throw new Error('database_id is required to sync dbt metrics into Metabase.');
    }

    if (dbt_project_dir || this.scanner.metrics.size === 0) {
      try {
        this.scanner.scanProject(dbt_project_dir);
      } catch (err) {
        logger.warn(`Could not scan dbt project folder: ${err.message}`);
      }
    }
    const mbTables = await this.client.getDatabaseTables(database_id);
    const mbTableMap = new Map();
    for (const t of mbTables) {
      mbTableMap.set(t.name.toLowerCase(), t);
    }

    const collectionId = dry_run ? 1 : await this.ensureMetricsCollection();
    const createdMetrics = [];
    const skippedMetrics = [];

    for (const [metricName, metricDef] of this.scanner.metrics.entries()) {
      let targetModel = metricDef.model;

      // 1. Domain prefix fallback resolution
      if (!targetModel) {
        const mname = metricName.toLowerCase();
        if (mname.startsWith('company_')) targetModel = 'mrt_executive_daily_sales';
        else if (mname.startsWith('flight_')) targetModel = 'mrt_flight_daily_sales';
        else if (mname.startsWith('bus_')) targetModel = 'mrt_bus_daily_sales';
        else if (mname.startsWith('car_')) targetModel = 'mrt_car_daily_sales';
        else if (mname.startsWith('hotel_')) targetModel = 'mrt_hotel_daily_sales';
        else if (mname.startsWith('ferry_')) targetModel = 'mrt_ferry_daily_sales';
      }

      if (!targetModel) {
        skippedMetrics.push({ name: metricName, reason: 'No underlying model defined' });
        continue;
      }

      const mbTable = mbTableMap.get(targetModel.toLowerCase());
      if (!mbTable) {
        skippedMetrics.push({ name: metricName, reason: `Underlying table ${targetModel} not found in Metabase` });
        continue;
      }

      const mbFields = await this.client.getTableFields(mbTable.id);
      const targetFieldName = metricDef.expression || metricName;
      const targetField = mbFields.find(f => f.name.toLowerCase() === targetFieldName.toLowerCase()) || mbFields[0];

      // Custom color support from dbt meta
      const customColor = metricDef.meta?.metabase?.color || metricDef.meta?.color || '#2ECC71';

      // v0.50+ Card Payload (Saved Metric / Scalar Question)
      const metricCardPayload = {
        name: metricDef.label || metricName.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        description: metricDef.description || `dbt Metric: ${metricName} (Model: ${targetModel})`,
        collection_id: collectionId,
        dataset_query: {
          database: database_id,
          type: 'query',
          query: {
            'source-table': mbTable.id,
            aggregation: [metricDef.type === 'count_distinct' ? 'distinct' : (metricDef.type || 'sum'), ['field', targetField ? targetField.id : mbTable.id, null]],
          },
        },
        display: 'scalar',
        visualization_settings: {
          'graph.colors': [customColor],
          'scalar.field': targetField ? targetField.name : 'id',
        },
      };

      if (!dry_run) {
        try {
          const res = await this.client.post('/card', metricCardPayload);
          createdMetrics.push({
            name: metricName,
            label: metricCardPayload.name,
            card_id: res.id,
            table: mbTable.name,
            color: customColor,
          });
        } catch (e) {
          logger.warn(`Failed to create Metabase metric card ${metricName}: ${e.message}`);
          skippedMetrics.push({ name: metricName, reason: e.message });
        }
      } else {
        createdMetrics.push({
          name: metricName,
          label: metricCardPayload.name,
          table: mbTable.name,
          color: customColor,
          payload: metricCardPayload,
        });
      }
    }

    return {
      database_id,
      collection_id: collectionId,
      metrics_created: createdMetrics.length,
      metrics_skipped: skippedMetrics.length,
      created: createdMetrics,
      skipped: skippedMetrics,
      dry_run,
    };
  }
}
