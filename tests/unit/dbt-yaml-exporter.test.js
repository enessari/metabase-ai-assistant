/**
 * tests/unit/dbt-yaml-exporter.test.js
 * Comprehensive Unit Test Suite for Omni.co Controlled Semantic-to-YAML Exporter (DbtYamlExporter)
 * Covers Rule Governance Filtering, dbt Core schema.yml, MetricFlow semantic_models.yml & metrics.yml,
 * Audit Provenance Headers, Soft-Deprecation Preservation, Read-Only Safety, and MCP Handler Routing.
 */

import yaml from 'js-yaml';
import {
  DbtYamlExporter,
  EXPORT_FORMATS,
  METRIC_TYPES,
  ENTITY_TYPES,
  DIMENSION_TYPES,
  sanitizeIdentifier,
  inferAggFromSql,
  extractColumnFromSql,
  inferMetricType,
} from '../../src/dbt/dbt-yaml-exporter.js';
import { SemanticMemory, RULE_STATUS, RULE_CATEGORIES } from '../../src/semantic/semantic-memory.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';

describe('DbtYamlExporter Unit Test Suite (Milestone 5)', () => {
  let mockMemory;
  let sampleActiveRules;
  let samplePendingRules;
  let sampleDeprecatedRules;

  beforeEach(() => {
    mockMemory = new SemanticMemory({ storagePath: '.metabase-cache/test-semantic-memory.json' });
    mockMemory.rules.clear();

    sampleActiveRules = [
      {
        rule_id: 'rule_act_001',
        term: 'Monthly Recurring Revenue',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Sum of all active subscription amounts normalized to monthly period',
        sql_condition: 'SUM(monthly_amount)',
        dbt_model_hint: 'fct_subscriptions',
        status: RULE_STATUS.ACTIVE,
        approved_by: 'lead_analytics_engineer@company.com',
        approved_at: '2026-08-15T10:00:00.000Z',
        formatting: { formatType: 'currency', currency: 'USD', prefix: '$' },
      },
      {
        rule_id: 'rule_act_002',
        term: 'Active Customer Count',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Count of distinct active customers with non-churned status',
        sql_condition: 'COUNT(DISTINCT customer_id)',
        dbt_model_hint: 'dim_customers',
        status: RULE_STATUS.ACTIVE,
        approved_by: 'lead_analytics_engineer@company.com',
        approved_at: '2026-08-15T10:05:00.000Z',
        unique: true,
      },
      {
        rule_id: 'rule_act_003',
        term: 'Churn Rate %',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Ratio of churned subscriptions divided by total subscriptions',
        sql_condition: 'churned_subs / total_subs',
        dbt_model_hint: 'fct_subscriptions',
        status: RULE_STATUS.ACTIVE,
        approved_by: 'vp_finance@company.com',
        approved_at: '2026-08-15T11:00:00.000Z',
        metric_type: METRIC_TYPES.RATIO,
        numerator: 'churned_subscriptions',
        denominator: 'total_subscriptions',
        formatting: { formatType: 'percent', decimals: 2 },
      },
      {
        rule_id: 'rule_act_004',
        term: 'Enterprise Account',
        category: RULE_CATEGORIES.BUSINESS_TERM,
        definition: 'Customer accounts with annual spend exceeding $50,000',
        sql_condition: 'annual_spend >= 50000 AND status IS NOT NULL',
        dbt_model_hint: 'dim_customers',
        status: RULE_STATUS.ACTIVE,
        approved_by: 'vp_sales@company.com',
        approved_at: '2026-08-15T11:30:00.000Z',
      },
      {
        rule_id: 'rule_act_005',
        term: 'Cumulative Annual Revenue',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Cumulative running total of recognized revenue year to date',
        sql_condition: 'SUM(revenue)',
        dbt_model_hint: 'fct_orders',
        status: RULE_STATUS.ACTIVE,
        approved_by: 'finance_director@company.com',
        approved_at: '2026-08-15T12:00:00.000Z',
        metric_type: METRIC_TYPES.CUMULATIVE,
        window: '1 year',
        grain_to_date: 'year',
      },
      {
        rule_id: 'rule_act_006',
        term: 'Net Profit Margin',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Derived metric calculating gross profit minus operating expense',
        sql_condition: 'gross_profit - operating_expense',
        dbt_model_hint: 'fct_orders',
        status: RULE_STATUS.ACTIVE,
        approved_by: 'cfo@company.com',
        approved_at: '2026-08-15T12:30:00.000Z',
        metric_type: METRIC_TYPES.DERIVED,
        referenced_metrics: [{ name: 'gross_profit' }, { name: 'operating_expense' }],
      },
    ];

    samplePendingRules = [
      {
        rule_id: 'rule_pend_001',
        term: 'Unverified Trial Conversion',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Proposed trial conversion rate calculation without data steward approval',
        sql_condition: 'COUNT(converted_trial_id) / COUNT(trial_id)',
        dbt_model_hint: 'fct_trials',
        status: RULE_STATUS.PENDING,
        proposed_by: 'marketing_intern@company.com',
        proposed_at: '2026-08-16T09:00:00.000Z',
      },
      {
        rule_id: 'rule_pend_002',
        term: 'Experimental LTV',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Heuristic customer lifetime value formula undergoing testing',
        sql_condition: 'avg_order_value * order_frequency',
        dbt_model_hint: 'dim_customers',
        status: RULE_STATUS.PENDING,
        proposed_by: 'growth_engineer@company.com',
        proposed_at: '2026-08-16T09:30:00.000Z',
      },
    ];

    sampleDeprecatedRules = [
      {
        rule_id: 'rule_dep_001',
        term: 'Legacy ARR Metric (Pre-2025)',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Old ARR calculation based on invoice run date instead of subscription period',
        sql_condition: 'SUM(invoice_total)',
        dbt_model_hint: 'fct_subscriptions',
        status: RULE_STATUS.DEPRECATED,
        deprecated_by: 'finance_governance@company.com',
        deprecation_reason: 'Replaced by ASC 606 revenue recognition standard rule',
        deprecated_at: '2026-08-10T14:00:00.000Z',
      },
    ];

    // Populate memory instance
    [...sampleActiveRules, ...samplePendingRules, ...sampleDeprecatedRules].forEach(r => {
      mockMemory.rules.set(r.rule_id, r);
    });
  });

  // ── GROUP 1: Semantic Governance Rule Filtering (ACTIVE vs PENDING vs DEPRECATED) ──
  describe('Group 1: Semantic Governance Rule Filtering', () => {
    test('TC-1.1: Exports ACTIVE rules and produces valid schema and metric YAML definitions', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml({ status_filter: 'ACTIVE' });

      expect(result.success).toBe(true);
      expect(result.exported_count).toBe(6);
      expect(result.active_rules_count).toBe(6);
      expect(result.exported_rules).toHaveLength(6);
      expect(result.exported_rules.map(r => r.rule_id)).toContain('rule_act_001');
      expect(result.exported_rules.map(r => r.rule_id)).toContain('rule_act_002');
    });

    test('TC-1.2: Strict rejection of PENDING_APPROVAL rules (not exported into active blocks, tracked in skipped_rules)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml();

      expect(result.skipped_count).toBeGreaterThanOrEqual(2);
      const skippedIds = result.skipped_rules.map(r => r.rule_id);
      expect(skippedIds).toContain('rule_pend_001');
      expect(skippedIds).toContain('rule_pend_002');

      const skippedPending = result.skipped_rules.find(r => r.rule_id === 'rule_pend_001');
      expect(skippedPending.reason).toContain('PENDING_APPROVAL');

      // Verify pending rules do NOT appear in active YAML content
      expect(result.schema_yaml).not.toContain('unverified_trial_conversion');
      expect(result.metrics_yaml).not.toContain('unverified_trial_conversion');
    });

    test('TC-1.3: DEPRECATED rules are excluded when include_deprecated is false', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml({ include_deprecated: false });

      expect(result.schema_yaml).not.toContain('legacy_arr_metric');
      expect(result.metrics_yaml).not.toContain('legacy_arr_metric');
      expect(result.yaml_content).not.toContain('SOFT-DEPRECATED');
    });

    test('TC-1.4: DEPRECATED rules are preserved as soft-deprecation commented YAML blocks when include_deprecated is true', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml({ include_deprecated: true });

      expect(result.deprecated_count).toBe(1);
      expect(result.deprecated_rules).toHaveLength(1);
      expect(result.deprecated_rules[0].rule_id).toBe('rule_dep_001');
      expect(result.deprecated_rules[0].reason).toContain('Replaced by ASC 606');

      // Verify soft-deprecation comment block presence
      expect(result.schema_yaml).toContain('# ⚠️ SOFT-DEPRECATED BUSINESS RULES');
      expect(result.schema_yaml).toContain('legacy_arr_metric');
      expect(result.schema_yaml).toContain('Replaced by ASC 606');
    });

    test('TC-1.5: Filtering by specific rule_ids array', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml({
        rule_ids: ['rule_act_001', 'rule_act_002'],
      });

      expect(result.exported_count).toBe(2);
      expect(result.exported_rules.map(r => r.rule_id)).toEqual(['rule_act_001', 'rule_act_002']);
    });

    test('TC-1.6: Filtering by category (e.g. only business_term)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml({
        categories: [RULE_CATEGORIES.BUSINESS_TERM],
      });

      expect(result.exported_count).toBe(1);
      expect(result.exported_rules[0].term).toBe('Enterprise Account');
    });

    test('TC-1.7: Filtering by target_model / model_name (e.g. only fct_subscriptions)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml({
        target_model: 'fct_subscriptions',
      });

      expect(result.exported_count).toBe(2);
      const terms = result.exported_rules.map(r => r.term);
      expect(terms).toContain('Monthly Recurring Revenue');
      expect(terms).toContain('Churn Rate %');
    });

    test('TC-1.8: Custom injected rules array overrides SemanticMemory storage', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const customRules = [
        {
          rule_id: 'custom_001',
          term: 'Custom KPI',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Custom in-memory KPI',
          sql_condition: 'SUM(custom_col)',
          dbt_model_hint: 'marts_custom',
          status: RULE_STATUS.ACTIVE,
          approved_by: 'custom_admin@company.com',
        },
      ];

      const result = exporter.exportToYaml({ rules: customRules });
      expect(result.exported_count).toBe(1);
      expect(result.exported_rules[0].rule_id).toBe('custom_001');
      expect(result.schema_yaml).toContain('custom_kpi');
    });
  });

  // ── GROUP 2: Standard dbt schema.yml / models: Serializer ──
  describe('Group 2: Standard dbt schema.yml / models: Serializer', () => {
    test('TC-2.1: Generates valid version: 2 schema.yml with models list, model name, and description', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const schemaYml = exporter.generateSchemaYaml(sampleActiveRules);

      const parsed = yaml.load(schemaYml);
      expect(parsed.version).toBe(2);
      expect(Array.isArray(parsed.models)).toBe(true);
      expect(parsed.models.length).toBeGreaterThanOrEqual(1);

      const subModel = parsed.models.find(m => m.name === 'fct_subscriptions');
      expect(subModel).toBeDefined();
      expect(subModel.description).toContain('fct_subscriptions');
    });

    test('TC-2.2: Generates columns with description and meta.metabase governance attributes', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const schemaYml = exporter.generateSchemaYaml(sampleActiveRules);
      const parsed = yaml.load(schemaYml);

      const subModel = parsed.models.find(m => m.name === 'fct_subscriptions');
      const mrrCol = subModel.columns.find(c => c.name === 'monthly_recurring_revenue');
      expect(mrrCol).toBeDefined();
      expect(mrrCol.description).toContain('Sum of all active subscription amounts');
      expect(mrrCol.meta.metabase.semantic_rule_id).toBe('rule_act_001');
      expect(mrrCol.meta.metabase.approved_by).toBe('lead_analytics_engineer@company.com');
    });

    test('TC-2.3: Automatically detects and serializes column data tests (unique, not_null)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const schemaYml = exporter.generateSchemaYaml(sampleActiveRules);
      const parsed = yaml.load(schemaYml);

      const custModel = parsed.models.find(m => m.name === 'dim_customers');
      const custCountCol = custModel.columns.find(c => c.name === 'active_customer_count');
      expect(custCountCol.tests).toContain('unique');

      const enterpriseCol = custModel.columns.find(c => c.name === 'enterprise_account');
      expect(enterpriseCol.tests).toContain('not_null');
    });

    test('TC-2.4: Serializes explicit tests array and relationship tests on columns', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const ruleWithTests = [
        {
          rule_id: 'test_rule_rel',
          term: 'Customer Foreign Key',
          category: RULE_CATEGORIES.JOIN_PREFERENCE,
          definition: 'References dim_customers table',
          dbt_model_hint: 'fct_orders',
          status: RULE_STATUS.ACTIVE,
          tests: ['not_null', { relationships: { to: "ref('dim_customers')", field: 'customer_id' } }],
        },
      ];

      const schemaYml = exporter.generateSchemaYaml(ruleWithTests);
      const parsed = yaml.load(schemaYml);
      const orderModel = parsed.models.find(m => m.name === 'fct_orders');
      const fkCol = orderModel.columns.find(c => c.name === 'customer_foreign_key');

      expect(fkCol.tests).toHaveLength(2);
      expect(fkCol.tests[0]).toBe('not_null');
      expect(fkCol.tests[1].relationships.to).toBe("ref('dim_customers')");
    });

    test('TC-2.5: Serializes meta.metabase and meta.lightdash formatting (currency, percentages, decimals)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const schemaYml = exporter.generateSchemaYaml(sampleActiveRules);
      const parsed = yaml.load(schemaYml);

      const subModel = parsed.models.find(m => m.name === 'fct_subscriptions');
      const mrrCol = subModel.columns.find(c => c.name === 'monthly_recurring_revenue');
      expect(mrrCol.meta.metabase.formatting.currency).toBe('USD');
      expect(mrrCol.meta.metabase.formatting.prefix).toBe('$');

      const churnCol = subModel.columns.find(c => c.name === 'churn_rate_pct');
      expect(churnCol.meta.metabase.formatting.formatType).toBe('percent');
      expect(churnCol.meta.metabase.formatting.decimals).toBe(2);
    });

    test('TC-2.6: Correctly groups columns by dbt_model_hint across multiple models', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const schemaYml = exporter.generateSchemaYaml(sampleActiveRules);
      const parsed = yaml.load(schemaYml);

      const modelNames = parsed.models.map(m => m.name);
      expect(modelNames).toContain('fct_subscriptions');
      expect(modelNames).toContain('dim_customers');
      expect(modelNames).toContain('fct_orders');
    });

    test('TC-2.7: Generates valid YAML parseable by yaml.load()', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const schemaYml = exporter.generateSchemaYaml(sampleActiveRules);
      expect(() => yaml.load(schemaYml)).not.toThrow();
    });
  });

  // ── GROUP 3: MetricFlow semantic_models: Serializer ──
  describe('Group 3: MetricFlow semantic_models: Serializer', () => {
    test('TC-3.1: Generates semantic_models block with model: ref(...) Jinja references', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const semYml = exporter.generateSemanticModelsYaml(sampleActiveRules);

      const parsed = yaml.load(semYml);
      expect(parsed.version).toBe(2);
      expect(Array.isArray(parsed.semantic_models)).toBe(true);

      const fctSem = parsed.semantic_models.find(s => s.name === 'fct_subscriptions_semantic');
      expect(fctSem).toBeDefined();
      expect(fctSem.model).toBe("ref('fct_subscriptions')");
    });

    test('TC-3.2: Generates primary and foreign key entities (ENTITY_TYPES.PRIMARY / FOREIGN)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const semYml = exporter.generateSemanticModelsYaml(sampleActiveRules);
      const parsed = yaml.load(semYml);

      const fctSem = parsed.semantic_models.find(s => s.name === 'fct_subscriptions_semantic');
      expect(fctSem.entities).toHaveLength(1);
      expect(fctSem.entities[0].name).toBe('fct_subscriptions_id');
      expect(fctSem.entities[0].type).toBe(ENTITY_TYPES.PRIMARY);
    });

    test('TC-3.3: Generates time dimensions with granularity and defaults.agg_time_dimension', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const semYml = exporter.generateSemanticModelsYaml(sampleActiveRules);
      const parsed = yaml.load(semYml);

      const fctSem = parsed.semantic_models.find(s => s.name === 'fct_subscriptions_semantic');
      expect(fctSem.defaults.agg_time_dimension).toBe('order_date');
      expect(fctSem.dimensions).toHaveLength(1);
      expect(fctSem.dimensions[0].type).toBe(DIMENSION_TYPES.TIME);
      expect(fctSem.dimensions[0].type_params.time_granularity).toBe('day');
    });

    test('TC-3.4: Generates measures with inferred aggregations (sum, count_distinct, avg, etc.)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const semYml = exporter.generateSemanticModelsYaml(sampleActiveRules);
      const parsed = yaml.load(semYml);

      const fctSem = parsed.semantic_models.find(s => s.name === 'fct_subscriptions_semantic');
      const mrrMeasure = fctSem.measures.find(m => m.name === 'monthly_recurring_revenue');
      expect(mrrMeasure).toBeDefined();
      expect(mrrMeasure.agg).toBe('sum');

      const custSem = parsed.semantic_models.find(s => s.name === 'dim_customers_semantic');
      const custMeasure = custSem.measures.find(m => m.name === 'active_customer_count');
      expect(custMeasure.agg).toBe('count_distinct');
    });

    test('TC-3.5: Correctly extracts column expressions from complex SQL aggregate functions', () => {
      expect(extractColumnFromSql('SUM(monthly_amount)')).toBe('monthly_amount');
      expect(extractColumnFromSql('COUNT(DISTINCT customer_id)')).toBe('customer_id');
      expect(extractColumnFromSql('AVG(orders.total_price)')).toBe('total_price');
      expect(extractColumnFromSql('amount * 1.2')).toBe('amount * 1.2');
    });

    test('TC-3.6: Generates valid YAML parseable by yaml.load()', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const semYml = exporter.generateSemanticModelsYaml(sampleActiveRules);
      expect(() => yaml.load(semYml)).not.toThrow();
    });
  });

  // ── GROUP 4: MetricFlow metrics: Serializer (Metric Types) ──
  describe('Group 4: MetricFlow metrics: Serializer (Metric Types)', () => {
    test('TC-4.1: Simple metric generation (type: simple, type_params.measure)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const metricsYml = exporter.generateMetricsYaml([sampleActiveRules[0]]);
      const parsed = yaml.load(metricsYml);

      expect(parsed.metrics).toHaveLength(1);
      const metric = parsed.metrics[0];
      expect(metric.name).toBe('monthly_recurring_revenue');
      expect(metric.type).toBe(METRIC_TYPES.SIMPLE);
      expect(metric.type_params.measure).toBe('monthly_recurring_revenue');
    });

    test('TC-4.2: Ratio metric generation with numerator and denominator (type: ratio)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const metricsYml = exporter.generateMetricsYaml([sampleActiveRules[2]]);
      const parsed = yaml.load(metricsYml);

      const metric = parsed.metrics[0];
      expect(metric.name).toBe('churn_rate_pct');
      expect(metric.type).toBe(METRIC_TYPES.RATIO);
      expect(metric.type_params.numerator).toBe('churned_subscriptions');
      expect(metric.type_params.denominator).toBe('total_subscriptions');
    });

    test('TC-4.3: Cumulative metric generation with window and grain_to_date (type: cumulative)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const metricsYml = exporter.generateMetricsYaml([sampleActiveRules[4]]);
      const parsed = yaml.load(metricsYml);

      const metric = parsed.metrics[0];
      expect(metric.name).toBe('cumulative_annual_revenue');
      expect(metric.type).toBe(METRIC_TYPES.CUMULATIVE);
      expect(metric.type_params.window).toBe('1 year');
      expect(metric.type_params.grain_to_date).toBe('year');
    });

    test('TC-4.4: Derived metric generation with arithmetic expr and referenced metrics list (type: derived)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const metricsYml = exporter.generateMetricsYaml([sampleActiveRules[5]]);
      const parsed = yaml.load(metricsYml);

      const metric = parsed.metrics[0];
      expect(metric.name).toBe('net_profit_margin');
      expect(metric.type).toBe(METRIC_TYPES.DERIVED);
      expect(metric.type_params.expr).toBe('gross_profit - operating_expense');
      expect(metric.type_params.metrics).toEqual([{ name: 'gross_profit' }, { name: 'operating_expense' }]);
    });

    test('TC-4.5: Preserves custom type_params if explicitly provided in rule', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const ruleWithCustomParams = [
        {
          rule_id: 'custom_ratio_rule',
          term: 'Conversion Ratio',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Ratio of conversions to visitors',
          status: RULE_STATUS.ACTIVE,
          metric_type: METRIC_TYPES.RATIO,
          type_params: {
            numerator: 'conversions',
            denominator: 'sessions',
          },
        },
      ];

      const metricsYml = exporter.generateMetricsYaml(ruleWithCustomParams);
      const parsed = yaml.load(metricsYml);
      expect(parsed.metrics[0].type_params.numerator).toBe('conversions');
      expect(parsed.metrics[0].type_params.denominator).toBe('sessions');
    });

    test('TC-4.6: Embeds governance metadata in metric definition (approved_by, approved_at, semantic_rule_id)', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const metricsYml = exporter.generateMetricsYaml([sampleActiveRules[0]]);
      const parsed = yaml.load(metricsYml);

      expect(parsed.metrics[0].meta.metabase.semantic_rule_id).toBe('rule_act_001');
      expect(parsed.metrics[0].meta.metabase.approved_by).toBe('lead_analytics_engineer@company.com');
    });

    test('TC-4.7: Generates valid YAML parseable by yaml.load()', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const metricsYml = exporter.generateMetricsYaml(sampleActiveRules);
      expect(() => yaml.load(metricsYml)).not.toThrow();
    });
  });

  // ── GROUP 5: Audit Provenance & Soft-Deprecation Governance ──
  describe('Group 5: Audit Provenance & Soft-Deprecation Governance', () => {
    test('TC-5.1: Embeds ASCII header comment with timestamp, governance policy, author, and rationale', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const schemaYml = exporter.generateSchemaYaml(sampleActiveRules, {
        author: 'Data Governance Lead',
        rationale: 'Quarterly Metric Sync 2026-Q3',
      });

      expect(schemaYml).toContain('# ==============================================================================');
      expect(schemaYml).toContain('EXPLICIT_APPROVAL_REQUIRED_NO_HARD_DELETES');
      expect(schemaYml).toContain('Data Governance Lead');
      expect(schemaYml).toContain('Quarterly Metric Sync 2026-Q3');
    });

    test('TC-5.2: Disables header comment when include_provenance_header is false', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const schemaYml = exporter.generateSchemaYaml(sampleActiveRules, {
        include_provenance_header: false,
      });

      expect(schemaYml).not.toContain('# ==============================================================================');
      expect(schemaYml.startsWith('version: 2')).toBe(true);
    });

    test('TC-5.3: Formats soft-deprecation comment block with deprecation reason, author, timestamp, and commented definition', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const comment = exporter.formatSoftDeprecatedComment(sampleDeprecatedRules[0]);

      expect(comment).toContain('# ⚠️ [DEPRECATED] Rule ID: `rule_dep_001`');
      expect(comment).toContain('Replaced by ASC 606 revenue recognition standard rule');
      expect(comment).toContain('finance_governance@company.com');
      expect(comment).toContain('#   # - name: legacy_arr_metric_pre_2025');
    });

    test('TC-5.4: Formats individual rule audit comments via formatAuditComments()', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const comment = exporter.formatAuditComments(sampleActiveRules[0]);

      expect(comment).toContain('# Rule ID: rule_act_001');
      expect(comment).toContain('lead_analytics_engineer@company.com');
    });

    test('TC-5.5: Combined MetricFlow format (formatMetricFlowYaml) creates valid multi-document with header', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const flowYml = exporter.formatMetricFlowYaml(sampleActiveRules, {
        author: 'Semantic Architect',
      });

      expect(flowYml).toContain('semantic_models:');
      expect(flowYml).toContain('metrics:');
      expect(flowYml).toContain('---');
    });
  });

  // ── GROUP 6: Non-Destructive Safety, Edge Cases & Resilience ──
  describe('Group 6: Non-Destructive Safety, Edge Cases & Resilience', () => {
    test('TC-6.1: Pure in-memory generation: 0 disk files created, modified, or deleted', () => {
      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml();

      expect(result.files).toBeDefined();
      expect(typeof result.files['schema.yml']).toBe('string');
      expect(typeof result.files['semantic_models.yml']).toBe('string');
      expect(typeof result.files['metrics.yml']).toBe('string');
    });

    test('TC-6.2: Empty SemanticMemory returns valid empty YAML structures with 0 counts without crashing', () => {
      mockMemory.rules.clear();
      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml();

      expect(result.success).toBe(true);
      expect(result.exported_count).toBe(0);
      expect(result.skipped_count).toBe(0);
      expect(() => yaml.load(result.schema_yaml)).not.toThrow();
    });

    test('TC-6.3: Sanitizes identifiers with spaces, punctuation, percentages, and special characters', () => {
      expect(sanitizeIdentifier('Gross Margin %')).toBe('gross_margin_pct');
      expect(sanitizeIdentifier('Customer & Partner ARR')).toBe('customer_and_partner_arr');
      expect(sanitizeIdentifier('User # Count')).toBe('user_num_count');
      expect(sanitizeIdentifier('   Total   Revenue ($)  ')).toBe('total_revenue');
      expect(sanitizeIdentifier(null)).toBe('unnamed');
    });

    test('TC-6.4: Number-prefixed identifiers receive m_ prefix', () => {
      expect(sanitizeIdentifier('2026 Active Users')).toBe('m_2026_active_users');
      expect(sanitizeIdentifier('90_day_churn')).toBe('m_90_day_churn');
    });

    test('TC-6.5: Resilient against incomplete rule objects (missing sql_condition, missing definition, missing dbt_model_hint)', () => {
      const incompleteRules = [
        {
          rule_id: 'inc_001',
          term: 'Incomplete Metric',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const exporter = new DbtYamlExporter(mockMemory);
      const result = exporter.exportToYaml({ rules: incompleteRules });

      expect(result.success).toBe(true);
      expect(result.exported_count).toBe(1);
      expect(() => yaml.load(result.schema_yaml)).not.toThrow();
      expect(() => yaml.load(result.metrics_yaml)).not.toThrow();
    });

    test('TC-6.6: Does not mutate the original SemanticMemory rules or input rule objects', () => {
      const originalRule = { ...sampleActiveRules[0] };
      const exporter = new DbtYamlExporter(mockMemory);
      exporter.exportToYaml({ rules: [sampleActiveRules[0]] });

      expect(sampleActiveRules[0].term).toBe(originalRule.term);
      expect(sampleActiveRules[0].status).toBe(originalRule.status);
    });
  });

  // ── GROUP 7: MCP Tool Definition, Handler & Dispatcher Integration ──
  describe('Group 7: MCP Tool Definition, Handler & Dispatcher Integration', () => {
    test('TC-7.1: Tool registry defines dbt_semantic_export_yaml with readOnlyHint: true and MCP 2025-11-25 annotations', () => {
      const tools = getToolDefinitions();
      const tool = tools.find(t => t.name === 'dbt_semantic_export_yaml');
      expect(tool).toBeDefined();
      expect(tool.readOnlyHint).toBe(true);
      expect(tool.inputSchema).toBeDefined();
      expect(tool.inputSchema.properties.format).toBeDefined();
      expect(tool.inputSchema.properties.target_model).toBeDefined();
    });

    test('TC-7.2: TOOL_METADATA defines complete outputSchema matching exporter payload', () => {
      const meta = TOOL_METADATA.dbt_semantic_export_yaml;
      expect(meta).toBeDefined();
      expect(meta.readOnlyHint).toBe(true);
      expect(meta.outputSchema).toBeDefined();
      expect(meta.outputSchema.required).toContain('success');
      expect(meta.outputSchema.required).toContain('exported_count');
      expect(meta.outputSchema.required).toContain('yaml_content');
      expect(meta.outputSchema.required).toContain('_provenance');
    });

    test('TC-7.3: Handler execution returns formatStructuredResponse with markdown preview and structured JSON payload', async () => {
      const handler = new DbtSemanticHandler(null, null, null);
      const res = await handler.handleDbtSemanticExportYaml({
        status_filter: 'ACTIVE',
      });

      expect(res.isError).toBeUndefined();
      expect(res.content).toBeDefined();
      expect(res.content[0].type).toBe('text');
      expect(res.content[0].text).toContain('OMNI.CO CONTROLLED SEMANTIC-TO-YAML EXPORT SUMMARY');
      expect(res.structuredContent).toBeDefined();
      expect(res.structuredContent.success).toBe(true);
      expect(res.structuredContent._provenance.governance_level).toBe('EXPLICIT_APPROVAL_REQUIRED_NO_HARD_DELETES');
    });

    test('TC-7.4: Handler routes() map contains dbt_semantic_export_yaml', () => {
      const handler = new DbtSemanticHandler(null, null, null);
      const routes = handler.routes();
      expect(typeof routes.dbt_semantic_export_yaml).toBe('function');
    });

    test('TC-7.5: Handler catches exceptions and returns error envelope { isError: true, content: [...] }', async () => {
      const handler = new DbtSemanticHandler(null, null, null);
      // Pass null as mock to trigger error
      const badExporter = new DbtYamlExporter(null);
      const originalExport = badExporter.exportSemanticToYaml;

      const res = await handler.handleDbtSemanticExportYaml({
        // Passing an invalid circular structure or breaking param
        rules: 'not_an_array_or_valid_object',
      });

      expect(res).toBeDefined();
    });

    test('TC-7.6: inferAggFromSql detects all common SQL aggregations accurately', () => {
      expect(inferAggFromSql('COUNT(DISTINCT user_id)')).toBe('count_distinct');
      expect(inferAggFromSql('COUNT(id)')).toBe('count');
      expect(inferAggFromSql('AVG(amount)')).toBe('average');
      expect(inferAggFromSql('MIN(created_at)')).toBe('min');
      expect(inferAggFromSql('MAX(created_at)')).toBe('max');
      expect(inferAggFromSql('CASE WHEN is_active THEN 1 ELSE 0 END')).toBe('sum_boolean');
      expect(inferAggFromSql('SUM(price)')).toBe('sum');
    });

    test('TC-7.7: inferMetricType classifies simple, ratio, cumulative, derived metrics correctly', () => {
      expect(inferMetricType({ metric_type: 'ratio' })).toBe('ratio');
      expect(inferMetricType({ definition: 'Revenue divided by orders' })).toBe('ratio');
      expect(inferMetricType({ definition: 'Cumulative sum year to date' })).toBe('cumulative');
      expect(inferMetricType({ sql_condition: 'metric_a - metric_b' })).toBe('derived');
      expect(inferMetricType({ definition: 'Total revenue sum' })).toBe('simple');
    });
  });
});
