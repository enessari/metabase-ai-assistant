/**
 * src/dbt/dbt-yaml-exporter.js
 * Omni.co Controlled Semantic-to-YAML Exporter & Provenance Engine
 *
 * Translates approved (ACTIVE) SemanticMemory business definitions, KPI calculations,
 * and metric rules into valid dbt schema.yml, semantic_models.yml, and metrics.yml files.
 * Preserves full audit provenance headers, author notes, and soft-deprecation rules.
 * Strictly non-destructive and read-only: generates valid YAML strings in-memory.
 */

import yaml from 'js-yaml';
import { globalSemanticMemory, RULE_STATUS } from '../semantic/semantic-memory.js';

export const EXPORT_FORMATS = {
  ALL: 'all',
  MODELS: 'models',
  SEMANTIC_MODELS: 'semantic_models',
  METRICS: 'metrics',
  SCHEMA_YML: 'schema_yml',
  CONSOLIDATED: 'consolidated',
};

export const METRIC_TYPES = {
  SIMPLE: 'simple',
  RATIO: 'ratio',
  CUMULATIVE: 'cumulative',
  DERIVED: 'derived',
};

export const ENTITY_TYPES = {
  PRIMARY: 'primary',
  FOREIGN: 'foreign',
  UNIQUE: 'unique',
};

export const DIMENSION_TYPES = {
  CATEGORICAL: 'categorical',
  TIME: 'time',
};

/**
 * Sanitize string into valid snake_case dbt identifier
 */
export function sanitizeIdentifier(str) {
  if (!str) return 'unnamed';
  return String(str)
    .trim()
    .toLowerCase()
    .replace(/%/g, '_pct')
    .replace(/#/g, '_num')
    .replace(/&/g, '_and_')
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/^(\d)/, 'm_$1');
}

/**
 * Infer SQL aggregation function from sql_condition or definition
 */
export function inferAggFromSql(sqlExpr = '', def = '') {
  const combined = `${sqlExpr || ''} ${def || ''}`.toLowerCase();
  if (combined.includes('count(distinct') || combined.includes('count_distinct') || combined.includes('distinct count')) {
    return 'count_distinct';
  }
  if (combined.includes('count(') || combined.includes('count of')) {
    return 'count';
  }
  if (combined.includes('avg(') || combined.includes('average(') || combined.includes('mean(') || combined.includes('average of')) {
    return 'average';
  }
  if (combined.includes('min(') || combined.includes('minimum(') || combined.includes('minimum of')) {
    return 'min';
  }
  if (combined.includes('max(') || combined.includes('maximum(') || combined.includes('maximum of')) {
    return 'max';
  }
  if (combined.includes('case when') && (combined.includes('then 1') || combined.includes('then true'))) {
    return 'sum_boolean';
  }
  return 'sum';
}

/**
 * Extract target column name inside SQL aggregate expression
 */
export function extractColumnFromSql(sqlExpr = '') {
  if (!sqlExpr || typeof sqlExpr !== 'string') return null;
  const trimmed = sqlExpr.trim();

  // If it's a simple column name without functions
  if (/^[a-zA-Z0-9_.]+$/.test(trimmed)) {
    return trimmed.replace(/^[a-zA-Z0-9_]+\./, '');
  }

  // If it's wrapped in aggregate function e.g. SUM(column) or COUNT(DISTINCT column)
  const match = trimmed.match(/\((?:distinct\s+)?([a-zA-Z0-9_.]+)\)/i);
  if (match) {
    return match[1].replace(/^[a-zA-Z0-9_]+\./, '');
  }

  // If complex expression without parentheses e.g. amount * 1.2
  return trimmed;
}

/**
 * Infer MetricFlow metric type and parameters from rule definition
 */
export function inferMetricType(rule = {}) {
  if (rule.metric_type && Object.values(METRIC_TYPES).includes(rule.metric_type)) {
    return rule.metric_type;
  }

  const def = (rule.definition || '').toLowerCase();
  const sql = (rule.sql_condition || '').toLowerCase();
  const term = (rule.term || '').toLowerCase();

  // Ratio metric detection
  if (
    rule.numerator ||
    rule.denominator ||
    def.includes('divided by') ||
    def.includes('ratio of') ||
    def.includes('ratio between') ||
    (sql.includes('/') && !sql.startsWith('sum(') && !sql.startsWith('count('))
  ) {
    return METRIC_TYPES.RATIO;
  }

  // Cumulative metric detection
  if (
    rule.window ||
    rule.grain_to_date ||
    def.includes('cumulative') ||
    def.includes('running total') ||
    def.includes('year to date') ||
    def.includes('ytd') ||
    term.includes('cumulative') ||
    term.includes('ytd')
  ) {
    return METRIC_TYPES.CUMULATIVE;
  }

  // Derived metric detection
  if (
    rule.expr ||
    rule.referenced_metrics ||
    (rule.sql_condition && /[-+*/]/.test(rule.sql_condition) && !sql.startsWith('sum(') && !sql.startsWith('count(') && !sql.startsWith('avg('))
  ) {
    return METRIC_TYPES.DERIVED;
  }

  return METRIC_TYPES.SIMPLE;
}

export class DbtYamlExporter {
  constructor(semanticMemory, options = {}) {
    this.semanticMemory = semanticMemory || globalSemanticMemory;
    this.options = options;
    this.warnings = [];
    this.errors = [];
    this.scanner = options.scanner || null;
    this.projectDir = options.projectDir || options.project_dir || null;
    this.manifestPath = options.manifestPath || options.manifest_path || null;
  }

  /**
   * Main export dispatcher (alias: exportSemanticToYaml)
   * @param {object} options
   * @returns {object} Export result with YAML contents, file breakdowns, and audit metadata
   */
  exportToYaml(options = {}) {
    return this.exportSemanticToYaml(options);
  }

  /**
   * Main semantic export method
   * @param {object} options
   * @returns {object}
   */
  exportSemanticToYaml(options = {}) {
    const {
      format = EXPORT_FORMATS.ALL,
      rules: customRules,
      rule_ids,
      ruleIds,
      model_name,
      modelName,
      target_model,
      targetModel,
      category,
      categories,
      status_filter,
      statusFilter = 'ACTIVE',
      include_deprecated,
      includeDeprecated,
      include_semantic_layer = true,
      include_metricflow = true,
      includeMetricFlow = true,
      include_dbt_schema = true,
      includeDbtSchema = true,
      include_provenance_header,
      include_provenance_comments,
      includeProvenanceComments,
      author = 'Metabase AI Assistant',
      rationale = 'Sync approved semantic governance rules to dbt YAML',
      version = 2,
    } = options;

    const effectiveStatusFilter = status_filter || statusFilter;
    const effectiveModelName = target_model || targetModel || model_name || modelName || null;
    const effectiveRuleIds = rule_ids || ruleIds || null;
    const effectiveCategories = categories || (category ? [category] : null);

    const shouldIncludeDeprecated = Boolean(
      include_deprecated !== undefined
        ? include_deprecated
        : (includeDeprecated !== undefined
            ? includeDeprecated
            : (effectiveStatusFilter === 'DEPRECATED' || effectiveStatusFilter === 'ALL'))
    );

    const shouldIncludeSemanticLayer = Boolean(
      include_semantic_layer && include_metricflow && includeMetricFlow
    );

    const shouldIncludeDbtSchema = Boolean(
      include_dbt_schema && includeDbtSchema
    );

    const shouldIncludeProvenance = Boolean(
      include_provenance_header !== undefined
        ? include_provenance_header
        : (include_provenance_comments !== undefined
            ? include_provenance_comments
            : (includeProvenanceComments !== undefined ? includeProvenanceComments : true))
    );

    // 1. Fetch rules from SemanticMemory or custom input
    let allRules = [];
    if (Array.isArray(customRules)) {
      allRules = customRules;
    } else if (this.semanticMemory) {
      if (typeof this.semanticMemory.listRules === 'function') {
        allRules = this.semanticMemory.listRules().rules || [];
      } else if (this.semanticMemory.rules && typeof this.semanticMemory.rules.values === 'function') {
        allRules = Array.from(this.semanticMemory.rules.values());
      }
    }

    // 2. Filter and categorize rules
    const {
      activeRules,
      pendingRules,
      deprecatedRules,
      skippedRules,
      rulesByCategory,
      targetModels,
    } = this.filterRules(allRules, {
      rule_ids: effectiveRuleIds,
      model_name: effectiveModelName,
      categories: effectiveCategories,
      status_filter: effectiveStatusFilter,
    });

    const timestamp = new Date().toISOString();
    const activeCount = activeRules.length;
    const deprecatedCount = deprecatedRules.length;
    const skippedCount = pendingRules.length + skippedRules.length;

    // Rules to export into active YAML blocks
    const exportableRules = effectiveStatusFilter === 'DEPRECATED' ? [] : activeRules;
    const exportableDeprecatedRules = shouldIncludeDeprecated ? deprecatedRules : [];

    // 3. Generate individual YAML blocks
    const schemaYml = shouldIncludeDbtSchema
      ? this.generateSchemaYaml(exportableRules, {
          version,
          model_name: effectiveModelName,
          include_provenance_header: shouldIncludeProvenance,
          author,
          rationale,
          timestamp,
          deprecatedRules: exportableDeprecatedRules,
        })
      : '';

    const semanticModelsYml = shouldIncludeSemanticLayer
      ? this.generateSemanticModelsYaml(exportableRules, {
          version,
          model_name: effectiveModelName,
          include_provenance_header: shouldIncludeProvenance,
          author,
          rationale,
          timestamp,
          deprecatedRules: exportableDeprecatedRules,
        })
      : '';

    const metricsYml = shouldIncludeSemanticLayer
      ? this.generateMetricsYaml(exportableRules, {
          version,
          model_name: effectiveModelName,
          include_provenance_header: shouldIncludeProvenance,
          author,
          rationale,
          timestamp,
          deprecatedRules: exportableDeprecatedRules,
        })
      : '';

    // 4. Assemble consolidated YAML content
    let consolidatedYaml = '';
    const normalizedFormat = String(format).toLowerCase();

    if (normalizedFormat === EXPORT_FORMATS.MODELS || normalizedFormat === EXPORT_FORMATS.SCHEMA_YML) {
      consolidatedYaml = schemaYml;
    } else if (normalizedFormat === EXPORT_FORMATS.SEMANTIC_MODELS) {
      consolidatedYaml = semanticModelsYml;
    } else if (normalizedFormat === EXPORT_FORMATS.METRICS) {
      consolidatedYaml = metricsYml;
    } else if (normalizedFormat === EXPORT_FORMATS.CONSOLIDATED) {
      const parts = [];
      if (schemaYml) parts.push(schemaYml);
      if (semanticModelsYml) parts.push(semanticModelsYml);
      if (metricsYml) parts.push(metricsYml);
      consolidatedYaml = parts.join('\n\n---\n\n');
    } else {
      // Default: ALL
      const parts = [];
      if (schemaYml) parts.push(schemaYml);
      if (semanticModelsYml) parts.push(semanticModelsYml);
      if (metricsYml) parts.push(metricsYml);
      consolidatedYaml = parts.join('\n\n');
    }

    const payload = {
      success: true,
      export_format: format,
      exported_count: effectiveStatusFilter === 'DEPRECATED' && shouldIncludeDeprecated ? deprecatedCount : activeCount,
      skipped_count: skippedCount,
      active_count: activeCount,
      active_rules_count: activeCount,
      deprecated_count: deprecatedCount,
      deprecated_rules_count: deprecatedCount,
      rules_by_category: rulesByCategory,
      target_models: targetModels,
      schema_yaml: schemaYml,
      semantic_models_yaml: semanticModelsYml,
      metrics_yaml: metricsYml,
      yaml_content: consolidatedYaml,
      files: {
        'schema.yml': schemaYml,
        'semantic_models.yml': semanticModelsYml,
        'metrics.yml': metricsYml,
      },
      exported_rules: exportableRules.map(r => ({
        rule_id: r.rule_id,
        term: r.term,
        status: r.status,
        category: r.category,
      })),
      skipped_rules: [
        ...pendingRules.map(r => ({
          rule_id: r.rule_id,
          term: r.term,
          status: r.status,
          reason: 'PENDING_APPROVAL: Rule not explicitly approved.',
        })),
        ...skippedRules.map(r => ({
          rule_id: r.rule_id,
          term: r.term,
          status: r.status,
          reason: r._skipReason || 'Filter mismatch',
        })),
      ],
      deprecated_rules: deprecatedRules.map(r => ({
        rule_id: r.rule_id,
        term: r.term,
        reason: r.deprecation_reason || 'Deprecated without reason',
        deprecated_by: r.deprecated_by || 'unknown',
        deprecated_at: r.deprecated_at || timestamp,
      })),
      _provenance: {
        governance_level: 'EXPLICIT_APPROVAL_REQUIRED_NO_HARD_DELETES',
        exporter: 'DbtYamlExporter',
        timestamp,
        exported_count: effectiveStatusFilter === 'DEPRECATED' && shouldIncludeDeprecated ? deprecatedCount : activeCount,
        skipped_count: skippedCount,
        active_count: activeCount,
        active_rules_count: activeCount,
        deprecated_count: deprecatedCount,
        deprecated_rules_count: deprecatedCount,
        audit_trail_preserved: true,
        active_rules_only: effectiveStatusFilter === 'ACTIVE',
        author,
        rationale,
      },
    };

    return payload;
  }

  /**
   * Filter rules into active, pending, deprecated, and skipped categories
   */
  filterRules(rules = [], options = {}) {
    const { rule_ids, model_name, categories } = options;
    const ruleIdSet = Array.isArray(rule_ids) && rule_ids.length > 0 ? new Set(rule_ids) : null;
    const categoriesSet = Array.isArray(categories) && categories.length > 0 ? new Set(categories) : null;

    const activeRules = [];
    const pendingRules = [];
    const deprecatedRules = [];
    const skippedRules = [];
    const targetModelsSet = new Set();

    const rulesByCategory = {
      metric_definition: 0,
      business_term: 0,
      filter_rule: 0,
      join_preference: 0,
      exclusion_rule: 0,
    };

    for (const rule of rules) {
      // 1. Rule ID Filter
      if (ruleIdSet && !ruleIdSet.has(rule.rule_id)) {
        continue;
      }

      // 2. Category Filter
      if (categoriesSet && !categoriesSet.has(rule.category)) {
        skippedRules.push({
          ...rule,
          _skipReason: `Category mismatch (expected one of: ${Array.from(categoriesSet).join(', ')}, actual: ${rule.category})`,
        });
        continue;
      }

      // 3. Model Name Filter
      if (model_name && rule.dbt_model_hint && rule.dbt_model_hint !== model_name) {
        skippedRules.push({
          ...rule,
          _skipReason: `Model hint mismatch (expected: ${model_name}, actual: ${rule.dbt_model_hint})`,
        });
        continue;
      }

      if (rule.dbt_model_hint) {
        targetModelsSet.add(rule.dbt_model_hint);
      }

      if (rulesByCategory[rule.category] !== undefined) {
        rulesByCategory[rule.category]++;
      }

      // 4. Status Partitioning
      if (rule.status === RULE_STATUS.ACTIVE) {
        activeRules.push(rule);
      } else if (rule.status === RULE_STATUS.PENDING) {
        pendingRules.push(rule);
      } else if (rule.status === RULE_STATUS.DEPRECATED) {
        deprecatedRules.push(rule);
      } else {
        skippedRules.push({ ...rule, _skipReason: `Unknown status: ${rule.status}` });
      }
    }

    return {
      activeRules,
      pendingRules,
      deprecatedRules,
      skippedRules,
      rulesByCategory,
      targetModels: Array.from(targetModelsSet),
    };
  }

  /**
   * Format standard dbt models schema.yml (alias: generateSchemaYaml)
   */
  generateSchemaYaml(modelsOrRules = [], options = {}) {
    return this.formatDbtSchemaYaml(modelsOrRules, options);
  }

  /**
   * Format standard dbt models schema.yml
   */
  formatDbtSchemaYaml(rules = [], options = {}) {
    const {
      version = 2,
      model_name,
      include_provenance_header = true,
      author,
      rationale,
      timestamp = new Date().toISOString(),
      deprecatedRules = [],
    } = options;

    const modelsMap = new Map();

    // If rules are empty, provide a clean default model skeleton if model_name is specified
    if (rules.length === 0 && model_name) {
      modelsMap.set(model_name, {
        name: model_name,
        description: `Semantic model and governance definitions for ${model_name}`,
        meta: {
          metabase: {
            governance_policy: 'ACTIVE_APPROVED_RULES',
            synced_at: timestamp,
          },
          lightdash: {
            tier: 'marts_fact',
          },
        },
        columns: [],
      });
    }

    for (const rule of rules) {
      const targetModel = rule.dbt_model_hint || model_name || 'mart_metrics';
      if (!modelsMap.has(targetModel)) {
        modelsMap.set(targetModel, {
          name: targetModel,
          description: `Semantic models and governance metrics for ${targetModel}`,
          meta: {
            metabase: {
              governance_policy: 'ACTIVE_APPROVED_RULES',
              synced_at: timestamp,
            },
            lightdash: {
              tier: 'marts_fact',
            },
          },
          columns: [],
        });
      }

      const modelEntry = modelsMap.get(targetModel);
      const colName = sanitizeIdentifier(rule.column_name || rule.term);

      let col = modelEntry.columns.find(c => c.name === colName);
      if (!col) {
        col = {
          name: colName,
          description: rule.definition || rule.term,
          meta: {
            metabase: {
              semantic_rule_id: rule.rule_id,
              term: rule.term,
              category: rule.category,
              approved_by: rule.approved_by || 'reviewer',
              approved_at: rule.approved_at || timestamp,
            },
          },
        };

        // Add tests
        if (Array.isArray(rule.tests) && rule.tests.length > 0) {
          col.tests = rule.tests;
        } else {
          const autoTests = [];
          if (rule.unique || (rule.definition && rule.definition.toLowerCase().includes('unique identifier'))) {
            autoTests.push('unique');
          }
          if (
            rule.not_null ||
            (rule.sql_condition && rule.sql_condition.toLowerCase().includes('is not null')) ||
            (rule.sql_condition && rule.sql_condition.toLowerCase().includes('not null'))
          ) {
            autoTests.push('not_null');
          }
          if (rule.relationships) {
            autoTests.push({ relationships: rule.relationships });
          }
          if (autoTests.length > 0) {
            col.tests = autoTests;
          }
        }

        // Add visual formatting metadata
        if (rule.formatting || rule.meta?.metabase?.formatting) {
          col.meta.metabase.formatting = rule.formatting || rule.meta.metabase.formatting;
        }

        modelEntry.columns.push(col);
      }
    }

    const docObj = {
      version,
      models: Array.from(modelsMap.values()),
    };

    let yamlStr = yaml.dump(docObj, {
      indent: 2,
      lineWidth: -1,
      noRefs: true,
      sortKeys: false,
    });

    // Append soft-deprecated comments
    if (deprecatedRules && deprecatedRules.length > 0) {
      yamlStr += '\n# ─────────────────────────────────────────────────────────────────────────────\n';
      yamlStr += '# ⚠️ SOFT-DEPRECATED BUSINESS RULES (Archived — Not Compiled in dbt DAG)\n';
      yamlStr += '# ─────────────────────────────────────────────────────────────────────────────\n';
      for (const depRule of deprecatedRules) {
        yamlStr += this.formatSoftDeprecatedComment(depRule, 'model_column');
      }
    }

    if (include_provenance_header) {
      const header = this.formatAuditHeader({
        title: 'dbt Core Models Schema (schema.yml)',
        author,
        rationale,
        timestamp,
        activeCount: rules.length,
        deprecatedCount: deprecatedRules.length,
      });
      return `${header}\n\n${yamlStr}`;
    }

    return yamlStr;
  }

  /**
   * Format MetricFlow semantic_models.yml (alias: generateSemanticModelsYaml)
   */
  generateSemanticModelsYaml(semanticModelsOrRules = [], options = {}) {
    return this.formatSemanticModelsYaml(semanticModelsOrRules, options);
  }

  /**
   * Format MetricFlow semantic_models.yml
   */
  formatSemanticModelsYaml(rules = [], options = {}) {
    const {
      version = 2,
      model_name,
      include_provenance_header = true,
      author,
      rationale,
      timestamp = new Date().toISOString(),
      deprecatedRules = [],
    } = options;

    const semanticModelsMap = new Map();

    for (const rule of rules) {
      const baseModel = rule.dbt_model_hint || model_name || 'fct_orders';
      const semModelName = `${sanitizeIdentifier(baseModel)}_semantic`;

      if (!semanticModelsMap.has(semModelName)) {
        semanticModelsMap.set(semModelName, {
          name: semModelName,
          description: `Semantic model definition for ${baseModel}`,
          model: `ref('${baseModel}')`,
          defaults: {
            agg_time_dimension: rule.time_dimension || 'order_date',
          },
          entities: [
            {
              name: `${sanitizeIdentifier(baseModel)}_id`,
              type: ENTITY_TYPES.PRIMARY,
            },
          ],
          dimensions: [
            {
              name: rule.time_dimension || 'order_date',
              type: DIMENSION_TYPES.TIME,
              type_params: {
                time_granularity: rule.time_granularity || 'day',
              },
            },
          ],
          measures: [],
        });
      }

      const semModel = semanticModelsMap.get(semModelName);
      const measureName = sanitizeIdentifier(rule.term);
      const aggType = rule.agg || inferAggFromSql(rule.sql_condition, rule.definition);
      const expr = rule.expr || extractColumnFromSql(rule.sql_condition) || measureName;

      semModel.measures.push({
        name: measureName,
        description: rule.definition || rule.term,
        expr: expr,
        agg: aggType,
        agg_time_dimension: rule.time_dimension || 'order_date',
      });
    }

    const docObj = {
      version,
      semantic_models: Array.from(semanticModelsMap.values()),
    };

    let yamlStr = yaml.dump(docObj, {
      indent: 2,
      lineWidth: -1,
      noRefs: true,
      sortKeys: false,
    });

    if (deprecatedRules && deprecatedRules.length > 0) {
      yamlStr += '\n# ─────────────────────────────────────────────────────────────────────────────\n';
      yamlStr += '# ⚠️ SOFT-DEPRECATED SEMANTIC MEASURES (MetricFlow)\n';
      yamlStr += '# ─────────────────────────────────────────────────────────────────────────────\n';
      for (const depRule of deprecatedRules) {
        yamlStr += this.formatSoftDeprecatedComment(depRule, 'semantic_measure');
      }
    }

    if (include_provenance_header) {
      const header = this.formatAuditHeader({
        title: 'dbt Semantic Layer — Semantic Models (semantic_models.yml)',
        author,
        rationale,
        timestamp,
        activeCount: rules.length,
        deprecatedCount: deprecatedRules.length,
      });
      return `${header}\n\n${yamlStr}`;
    }

    return yamlStr;
  }

  /**
   * Format MetricFlow metrics.yml (alias: generateMetricsYaml)
   */
  generateMetricsYaml(metricsOrRules = [], options = {}) {
    return this.formatMetricsYaml(metricsOrRules, options);
  }

  /**
   * Format MetricFlow metrics.yml (simple, ratio, cumulative, derived)
   */
  formatMetricsYaml(rules = [], options = {}) {
    const {
      version = 2,
      include_provenance_header = true,
      author,
      rationale,
      timestamp = new Date().toISOString(),
      deprecatedRules = [],
    } = options;

    const metricEntries = [];

    for (const rule of rules) {
      const metricName = sanitizeIdentifier(rule.term);
      const label = rule.term;
      const description = rule.definition || rule.term;

      let type = inferMetricType(rule);
      let typeParams = {};

      if (rule.type_params) {
        typeParams = { ...rule.type_params };
      } else if (type === METRIC_TYPES.RATIO) {
        typeParams = {
          numerator: rule.numerator || 'total_revenue',
          denominator: rule.denominator || 'order_count',
        };
      } else if (type === METRIC_TYPES.CUMULATIVE) {
        typeParams = {
          measure: rule.measure || metricName,
          window: rule.window || '1 year',
          grain_to_date: rule.grain_to_date || 'year',
        };
      } else if (type === METRIC_TYPES.DERIVED) {
        typeParams = {
          expr: rule.expr || rule.sql_condition || `${metricName}_a / ${metricName}_b`,
          metrics: rule.referenced_metrics || rule.metrics || [{ name: metricName }],
        };
      } else {
        // Default SIMPLE
        type = METRIC_TYPES.SIMPLE;
        typeParams = {
          measure: rule.measure || metricName,
        };
      }

      metricEntries.push({
        name: metricName,
        label: label,
        description: description,
        type: type,
        type_params: typeParams,
        meta: {
          metabase: {
            semantic_rule_id: rule.rule_id,
            approved_by: rule.approved_by || 'reviewer',
            approved_at: rule.approved_at || timestamp,
          },
        },
      });
    }

    const docObj = {
      version,
      metrics: metricEntries,
    };

    let yamlStr = yaml.dump(docObj, {
      indent: 2,
      lineWidth: -1,
      noRefs: true,
      sortKeys: false,
    });

    if (deprecatedRules && deprecatedRules.length > 0) {
      yamlStr += '\n# ─────────────────────────────────────────────────────────────────────────────\n';
      yamlStr += '# ⚠️ SOFT-DEPRECATED METRICFLOW METRICS (metrics.yml)\n';
      yamlStr += '# ─────────────────────────────────────────────────────────────────────────────\n';
      for (const depRule of deprecatedRules) {
        yamlStr += this.formatSoftDeprecatedComment(depRule, 'metric');
      }
    }

    if (include_provenance_header) {
      const header = this.formatAuditHeader({
        title: 'dbt Semantic Layer — MetricFlow Metrics (metrics.yml)',
        author,
        rationale,
        timestamp,
        activeCount: rules.length,
        deprecatedCount: deprecatedRules.length,
      });
      return `${header}\n\n${yamlStr}`;
    }

    return yamlStr;
  }

  /**
   * Combined MetricFlow formatter
   */
  formatMetricFlowYaml(rules = [], options = {}) {
    const semModels = this.generateSemanticModelsYaml(rules, {
      ...options,
      include_provenance_header: false,
    });
    const metrics = this.generateMetricsYaml(rules, {
      ...options,
      include_provenance_header: false,
    });

    const header = options.include_provenance_header !== false
      ? this.formatAuditHeader({
          title: 'dbt MetricFlow Semantic Layer (semantic_models.yml + metrics.yml)',
          author: options.author,
          rationale: options.rationale,
          timestamp: options.timestamp || new Date().toISOString(),
          activeCount: rules.length,
          deprecatedCount: options.deprecatedRules?.length || 0,
        })
      : '';

    return `${header ? header + '\n\n' : ''}${semModels}\n---\n${metrics}`;
  }

  /**
   * Format audit comments for a specific rule
   */
  formatAuditComments(rule, _options = {}) {
    if (!rule) return '';
    const lines = [
      `# Rule ID: ${rule.rule_id || 'unknown'} | Status: ${rule.status || 'ACTIVE'} | Category: ${rule.category || 'business_term'}`,
      `# Approved by: ${rule.approved_by || 'reviewer'} at ${rule.approved_at || 'unknown'}`,
    ];
    if (rule.approval_comment) {
      lines.push(`# Approval Comment: ${rule.approval_comment}`);
    }
    return lines.join('\n');
  }

  /**
   * Generate ASCII audit provenance header block comment
   */
  formatAuditHeader(meta = {}) {
    const lines = [
      '# ==============================================================================',
      `# ${meta.title || 'dbt Semantic BI Schema Definition'}`,
      '# Generated by: Metabase AI Assistant (Omni.co Controlled Semantic Exporter)',
      '# Governance Policy: EXPLICIT_APPROVAL_REQUIRED_NO_HARD_DELETES',
      `# Export Timestamp: ${meta.timestamp || new Date().toISOString()}`,
      `# Active Rules Exported: ${meta.activeCount || 0}`,
      `# Soft-Deprecated Rules: ${meta.deprecatedCount || 0}`,
      `# Author / Stakeholder: ${meta.author || 'Metabase AI Assistant'}`,
      `# Governance Rationale: ${meta.rationale || 'Sync approved SemanticMemory rules to dbt'}`,
      '# ==============================================================================',
    ];
    return lines.join('\n');
  }

  /**
   * Format soft-deprecated rule as a commented YAML block
   */
  formatSoftDeprecatedComment(rule, _context = 'metric') {
    const lines = [
      `# ⚠️ [DEPRECATED] Rule ID: \`${rule.rule_id}\` | Term: "${rule.term}"`,
      `#   Reason: ${rule.deprecation_reason || 'Deprecated without specified reason'}`,
      `#   Deprecated By: ${rule.deprecated_by || 'reviewer'} at ${rule.deprecated_at || 'unknown'}`,
      `#   # - name: ${sanitizeIdentifier(rule.term)}`,
      `#   #   description: "${rule.definition || rule.term} (DEPRECATED)"`,
      `#   #   meta:`,
      `#   #     deprecated: true`,
      `#   #     deprecation_reason: "${rule.deprecation_reason || ''}"`,
      '#',
    ];
    return lines.join('\n');
  }
}
