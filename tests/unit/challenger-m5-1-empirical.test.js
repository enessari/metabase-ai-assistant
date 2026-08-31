/**
 * tests/unit/challenger-m5-1-empirical.test.js
 * Adversarial Empirical Verification Suite by Challenger M5-1
 * 
 * Focus Areas:
 * 1. YAML Validity & Round-Trip Parsing (js-yaml.load & js-yaml.loadAll across all export formats)
 * 2. String Quoting Edge Cases (colons, Jinja braces, quotes, multiline Markdown, YAML special chars, Unicode/emojis)
 * 3. dbt Test Definitions (relationships with ref, custom severity tags, accepted_values, auto-inferred tests)
 * 4. Boundary Conditions & Resilience (empty rules, 0-column models, null/undefined inputs, malformed rule objects)
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
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';
import { TOOL_METADATA, getToolDefinitions } from '../../src/mcp/tool-registry.js';

describe('Challenger M5-1: Adversarial Schema.yml Syntax, Quotes & Tests Suite', () => {
  let emptyMemory;
  let exporter;

  beforeEach(() => {
    emptyMemory = new SemanticMemory({ storagePath: '.metabase-cache/test-challenger-m5-1.json' });
    emptyMemory.rules.clear();
    exporter = new DbtYamlExporter(emptyMemory);
  });

  // =========================================================================
  // GROUP 1: YAML VALIDITY & ROUND-TRIP PARSING
  // =========================================================================
  describe('Group 1: YAML Validity & Round-Trip Parsing', () => {
    test('TC-ADV-1.1: Round-trips schema.yml with version: 2, models, columns, tests, and meta tags', () => {
      const rules = [
        {
          rule_id: 'rule_adv_001',
          term: 'Total Order Value',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Sum of total order price after tax and discounts',
          sql_condition: 'SUM(order_price)',
          dbt_model_hint: 'fct_orders',
          status: RULE_STATUS.ACTIVE,
          approved_by: 'lead_ae@company.com',
          approved_at: '2026-08-30T12:00:00Z',
          unique: false,
          not_null: true,
          formatting: { formatType: 'currency', currency: 'USD' },
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.MODELS,
        include_provenance_header: true,
      });

      expect(res.success).toBe(true);
      expect(typeof res.yaml_content).toBe('string');

      // Round-trip through js-yaml
      const parsed = yaml.load(res.yaml_content);
      expect(parsed).toBeDefined();
      expect(parsed.version).toBe(2);
      expect(Array.isArray(parsed.models)).toBe(true);
      expect(parsed.models.length).toBe(1);

      const model = parsed.models[0];
      expect(model.name).toBe('fct_orders');
      expect(model.columns.length).toBe(1);
      expect(model.columns[0].name).toBe('total_order_value');
      expect(model.columns[0].tests).toContain('not_null');
      expect(model.columns[0].meta.metabase.formatting.currency).toBe('USD');
    });

    test('TC-ADV-1.2: Round-trips semantic_models.yml with entities, dimensions, measures and Jinja ref', () => {
      const rules = [
        {
          rule_id: 'rule_adv_002',
          term: 'Active Subscriptions',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Count of active subscription records',
          sql_condition: 'COUNT(subscription_id)',
          dbt_model_hint: 'dim_subscriptions',
          status: RULE_STATUS.ACTIVE,
          time_dimension: 'created_at',
          time_granularity: 'month',
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.SEMANTIC_MODELS,
        include_provenance_header: true,
      });

      const parsed = yaml.load(res.yaml_content);
      expect(parsed).toBeDefined();
      expect(parsed.version).toBe(2);
      expect(Array.isArray(parsed.semantic_models)).toBe(true);
      expect(parsed.semantic_models.length).toBe(1);

      const semModel = parsed.semantic_models[0];
      expect(semModel.name).toBe('dim_subscriptions_semantic');
      expect(semModel.model).toBe("ref('dim_subscriptions')");
      expect(semModel.entities[0].type).toBe('primary');
      expect(semModel.dimensions[0].type).toBe('time');
      expect(semModel.dimensions[0].type_params.time_granularity).toBe('month');
      expect(semModel.measures[0].name).toBe('active_subscriptions');
      expect(semModel.measures[0].agg).toBe('count');
    });

    test('TC-ADV-1.3: Round-trips metrics.yml with all 4 metric types (simple, ratio, cumulative, derived)', () => {
      const rules = [
        {
          rule_id: 'm_simple',
          term: 'Gross Margin',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Total gross margin',
          metric_type: METRIC_TYPES.SIMPLE,
          measure: 'margin_amount',
          status: RULE_STATUS.ACTIVE,
        },
        {
          rule_id: 'm_ratio',
          term: 'Return Rate',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Returned items divided by sold items',
          metric_type: METRIC_TYPES.RATIO,
          numerator: 'returned_items',
          denominator: 'sold_items',
          status: RULE_STATUS.ACTIVE,
        },
        {
          rule_id: 'm_cumul',
          term: 'Running Total Signups',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Cumulative user signups across year',
          metric_type: METRIC_TYPES.CUMULATIVE,
          window: '1 year',
          grain_to_date: 'year',
          status: RULE_STATUS.ACTIVE,
        },
        {
          rule_id: 'm_derived',
          term: 'Net Profit Margin Ratio',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Net profit divided by gross revenue',
          metric_type: METRIC_TYPES.DERIVED,
          expr: 'net_profit / gross_revenue',
          referenced_metrics: [{ name: 'net_profit' }, { name: 'gross_revenue' }],
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.METRICS,
        include_provenance_header: true,
      });

      const parsed = yaml.load(res.yaml_content);
      expect(parsed.version).toBe(2);
      expect(parsed.metrics.length).toBe(4);

      const [simple, ratio, cumul, derived] = parsed.metrics;
      expect(simple.type).toBe('simple');
      expect(simple.type_params.measure).toBe('margin_amount');

      expect(ratio.type).toBe('ratio');
      expect(ratio.type_params.numerator).toBe('returned_items');
      expect(ratio.type_params.denominator).toBe('sold_items');

      expect(cumul.type).toBe('cumulative');
      expect(cumul.type_params.window).toBe('1 year');

      expect(derived.type).toBe('derived');
      expect(derived.type_params.expr).toBe('net_profit / gross_revenue');
      expect(derived.type_params.metrics).toEqual([{ name: 'net_profit' }, { name: 'gross_revenue' }]);
    });

    test('TC-ADV-1.4: Multi-document parsing for consolidated format and formatMetricFlowYaml', () => {
      const rules = [
        {
          rule_id: 'rule_combo_1',
          term: 'Total Bookings',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          sql_condition: 'SUM(booking_amount)',
          dbt_model_hint: 'fct_bookings',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      // Consolidated format
      const consolidatedRes = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.CONSOLIDATED,
        include_provenance_header: true,
      });

      const docsConsolidated = [];
      yaml.loadAll(consolidatedRes.yaml_content, doc => {
        if (doc) docsConsolidated.push(doc);
      });
      expect(docsConsolidated.length).toBe(3);
      expect(docsConsolidated[0].models).toBeDefined();
      expect(docsConsolidated[1].semantic_models).toBeDefined();
      expect(docsConsolidated[2].metrics).toBeDefined();

      // formatMetricFlowYaml
      const metricFlowStr = exporter.formatMetricFlowYaml(rules, { include_provenance_header: true });
      const docsMetricFlow = [];
      yaml.loadAll(metricFlowStr, doc => {
        if (doc) docsMetricFlow.push(doc);
      });
      expect(docsMetricFlow.length).toBe(2);
      expect(docsMetricFlow[0].semantic_models).toBeDefined();
      expect(docsMetricFlow[1].metrics).toBeDefined();
    });

    test('TC-ADV-1.5: Soft-deprecated comment blocks do not corrupt YAML parsing and are ignored by parser', () => {
      const activeRules = [
        {
          rule_id: 'rule_active_1',
          term: 'Current KPI',
          definition: 'Approved metric',
          status: RULE_STATUS.ACTIVE,
        },
      ];
      const deprecatedRules = [
        {
          rule_id: 'rule_dep_1',
          term: 'Old Legacy Metric',
          definition: 'Deprecated metric calculation',
          status: RULE_STATUS.DEPRECATED,
          deprecation_reason: 'Migrated to Current KPI in v2',
          deprecated_by: 'lead_architect',
          deprecated_at: '2026-08-20T10:00:00Z',
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules: [...activeRules, ...deprecatedRules],
        format: EXPORT_FORMATS.ALL,
        include_deprecated: true,
        include_provenance_header: true,
      });

      expect(res.yaml_content).toContain('⚠️ SOFT-DEPRECATED BUSINESS RULES');
      expect(res.yaml_content).toContain('⚠️ [DEPRECATED] Rule ID: `rule_dep_1`');

      // The schema.yml snippet must parse cleanly despite comments
      const parsedSchema = yaml.load(res.files['schema.yml']);
      expect(parsedSchema.models[0].columns.length).toBe(1);
      expect(parsedSchema.models[0].columns[0].name).toBe('current_kpi');
    });
  });

  // =========================================================================
  // GROUP 2: STRING QUOTING EDGE CASES & HOSTILE CHARACTERS
  // =========================================================================
  describe('Group 2: String Quoting Edge Cases & Hostile Characters', () => {
    test('TC-ADV-2.1: Handles colons in terms, definitions, and model hints without YAML syntax errors', () => {
      const rules = [
        {
          rule_id: 'rule_colon_1',
          term: 'KPI: Customer Acquisition Cost: Blended',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Ratio of total marketing spend: sales spend vs: customer acquisitions',
          sql_condition: 'total_spend / new_customers',
          dbt_model_hint: 'fct_marketing_costs',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.ALL,
      });

      const parsed = yaml.load(res.files['schema.yml']);
      expect(parsed.models[0].columns[0].description).toBe(
        'Ratio of total marketing spend: sales spend vs: customer acquisitions'
      );
      expect(parsed.models[0].columns[0].meta.metabase.term).toBe(
        'KPI: Customer Acquisition Cost: Blended'
      );
    });

    test('TC-ADV-2.2: Handles Jinja template tags {{ ... }} and JSON strings {"key": "value"} safely', () => {
      const rules = [
        {
          rule_id: 'rule_jinja_1',
          term: 'Dynamic Conversion Metric',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Evaluated using {{ ref("dim_stages") }} with config {"window_days": 30}',
          sql_condition: 'SUM(CASE WHEN stage = {{ var("target_stage") }} THEN 1 ELSE 0 END)',
          dbt_model_hint: 'fct_conversions',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.ALL,
      });

      const parsed = yaml.load(res.files['schema.yml']);
      expect(parsed.models[0].columns[0].description).toBe(
        'Evaluated using {{ ref("dim_stages") }} with config {"window_days": 30}'
      );
    });

    test('TC-ADV-2.3: Handles single quotes, double quotes, unescaped quotes, and backticks', () => {
      const rules = [
        {
          rule_id: 'rule_quotes_1',
          term: `User's "Diamond" Tier & \`Special\` Status`,
          category: RULE_CATEGORIES.BUSINESS_TERM,
          definition: `A user whose status is "active" AND who's marked 'vip' in \`dim_users\``,
          sql_condition: `status = 'active' AND tier = "diamond"`,
          dbt_model_hint: 'dim_users',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.ALL,
      });

      const parsed = yaml.load(res.files['schema.yml']);
      const col = parsed.models[0].columns[0];
      expect(col.description).toBe(`A user whose status is "active" AND who's marked 'vip' in \`dim_users\``);
      expect(col.meta.metabase.term).toBe(`User's "Diamond" Tier & \`Special\` Status`);
    });

    test('TC-ADV-2.4: Handles multi-line Markdown descriptions with headers, bullet points, and code blocks', () => {
      const multilineMarkdown = [
        '### Enterprise KPI Definition',
        '',
        'Calculated according to the following rules:',
        '- Step 1: Filter `status = "active"` and `is_deleted = false`',
        '- Step 2: Sum `amount * tax_rate`',
        '',
        '> **Note**: Strictly for finance stakeholder reports.',
        '```sql',
        'SELECT SUM(revenue) FROM fct_orders WHERE status = 1',
        '```',
      ].join('\n');

      const rules = [
        {
          rule_id: 'rule_multiline_1',
          term: 'Complex MRR Calculation',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: multilineMarkdown,
          sql_condition: 'SUM(revenue)',
          dbt_model_hint: 'fct_revenue',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.ALL,
      });

      const parsed = yaml.load(res.files['schema.yml']);
      expect(parsed.models[0].columns[0].description).toBe(multilineMarkdown);
    });

    test('TC-ADV-2.5: Handles YAML reserved control characters (@, #, %, *, &, [, ], {, }, |, >, !, ?, -)', () => {
      const rules = [
        {
          rule_id: 'rule_yaml_chars_1',
          term: '@special #tag %pct *star &amp [bracket] {brace} |pipe >greater !excl ?quest -dash',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: '@tag: %percent *reference &anchor [array] {map} |literal >folded !custom ?key -item',
          sql_condition: 'column_val',
          dbt_model_hint: 'fct_special_chars',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.ALL,
      });

      const parsed = yaml.load(res.files['schema.yml']);
      expect(parsed.models[0].columns[0].description).toBe(
        '@tag: %percent *reference &anchor [array] {map} |literal >folded !custom ?key -item'
      );
      // Sanitizer converts # to _num and % to _pct
      expect(parsed.models[0].columns[0].name).toBe(
        'special_numtag_pctpct_star_and_amp_bracket_brace_pipe_greater_excl_quest_dash'
      );
    });

    test('TC-ADV-2.6: Handles Unicode, emojis, non-Latin alphabets, and mathematical symbols', () => {
      const rules = [
        {
          rule_id: 'rule_unicode_1',
          term: '📈 Gross Profit Margin (収益 / 総利益)',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: '총 매출에서 비용을 뺀 금액 ≥ 100€ ± 5% — (العربية / Türkçe / Deutsch: Äpfel & Öle)',
          sql_condition: 'SUM(gross_profit)',
          dbt_model_hint: 'fct_global_finance',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.ALL,
      });

      const parsed = yaml.load(res.files['schema.yml']);
      expect(parsed.models[0].columns[0].description).toBe(
        '총 매출에서 비용을 뺀 금액 ≥ 100€ ± 5% — (العربية / Türkçe / Deutsch: Äpfel & Öle)'
      );
      expect(parsed.models[0].columns[0].name).toBe('gross_profit_margin');
    });

    test('TC-ADV-2.7: Handles complex SQL condition expressions with nested functions, casts, and regex', () => {
      const complexSql = `CAST(JSON_EXTRACT_SCALAR(attributes, '$.user.plan') AS STRING) = 'enterprise' AND created_at > '2026-01-01'::timestamp`;
      const rules = [
        {
          rule_id: 'rule_complex_sql_1',
          term: 'Enterprise Filtered Revenue',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Revenue from enterprise plan users',
          sql_condition: `SUM(CASE WHEN ${complexSql} THEN amount ELSE 0 END)`,
          dbt_model_hint: 'fct_plan_revenue',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.ALL,
      });

      const parsed = yaml.load(res.files['schema.yml']);
      expect(parsed.models[0].columns[0].name).toBe('enterprise_filtered_revenue');
      expect(parsed.models[0].columns[0].description).toBe('Revenue from enterprise plan users');

      const semParsed = yaml.load(res.files['semantic_models.yml']);
      expect(semModelMeasure(semParsed, 'enterprise_filtered_revenue')).toBeDefined();
    });
  });

  // =========================================================================
  // GROUP 3: DBT TEST DEFINITIONS & COMPLEX CONFIGURATIONS
  // =========================================================================
  describe('Group 3: dbt Test Definitions & Complex Configurations', () => {
    test('TC-ADV-3.1: Explicit relationship test with to: ref(...) and field', () => {
      const rules = [
        {
          rule_id: 'rule_rel_1',
          term: 'Customer Identifier',
          category: RULE_CATEGORIES.BUSINESS_TERM,
          definition: 'Foreign key to dim_customers',
          dbt_model_hint: 'fct_orders',
          status: RULE_STATUS.ACTIVE,
          relationships: {
            to: "ref('dim_customers')",
            field: 'customer_id',
          },
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.MODELS,
      });

      const parsed = yaml.load(res.yaml_content);
      const col = parsed.models[0].columns[0];
      expect(col.tests).toBeDefined();
      expect(col.tests.length).toBe(1);
      expect(col.tests[0]).toEqual({
        relationships: {
          to: "ref('dim_customers')",
          field: 'customer_id',
        },
      });
    });

    test('TC-ADV-3.2: Complex tests array with severity tags and accepted_values', () => {
      const explicitTests = [
        'not_null',
        {
          unique: {
            config: {
              severity: 'warn',
              error_if: '>10',
              warn_if: '>0',
            },
          },
        },
        {
          accepted_values: {
            values: ['pending', 'processing', 'completed', 'canceled'],
            quote: true,
            config: {
              severity: 'error',
            },
          },
        },
        {
          relationships: {
            to: "ref('stg_payments')",
            field: 'payment_id',
            config: {
              where: "status = 'completed'",
            },
          },
        },
      ];

      const rules = [
        {
          rule_id: 'rule_tests_1',
          term: 'Payment Status Code',
          category: RULE_CATEGORIES.BUSINESS_TERM,
          definition: 'Status code of payment transaction',
          dbt_model_hint: 'fct_payments',
          status: RULE_STATUS.ACTIVE,
          tests: explicitTests,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.MODELS,
      });

      const parsed = yaml.load(res.yaml_content);
      const col = parsed.models[0].columns[0];
      expect(col.tests).toEqual(explicitTests);
    });

    test('TC-ADV-3.3: Automatic detection of unique, not_null, and relationships from rule fields', () => {
      const rules = [
        {
          rule_id: 'rule_auto_1',
          term: 'Order ID',
          definition: 'Unique identifier for orders',
          sql_condition: 'order_id IS NOT NULL',
          dbt_model_hint: 'fct_orders',
          status: RULE_STATUS.ACTIVE,
          unique: true,
          not_null: true,
          relationships: {
            to: "ref('stg_orders')",
            field: 'id',
          },
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.MODELS,
      });

      const parsed = yaml.load(res.yaml_content);
      const col = parsed.models[0].columns[0];
      expect(col.tests).toContain('unique');
      expect(col.tests).toContain('not_null');
      expect(col.tests).toContainEqual({
        relationships: {
          to: "ref('stg_orders')",
          field: 'id',
        },
      });
    });

    test('TC-ADV-3.4: Serializes rich BI formatting metadata (currencies, colors, decimals) and Lightdash tier', () => {
      const rules = [
        {
          rule_id: 'rule_meta_1',
          term: 'Adjusted EBITDA',
          definition: 'Earnings before interest, taxes, depreciation, and amortization',
          sql_condition: 'SUM(ebitda_adjusted)',
          dbt_model_hint: 'fct_financials',
          status: RULE_STATUS.ACTIVE,
          formatting: {
            formatType: 'currency',
            currency: 'EUR',
            prefix: '€',
            decimals: 2,
            negativeFormat: '(€100)',
            colorRules: [
              { condition: '<0', color: '#FF0000' },
              { condition: '>=0', color: '#00FF00' },
            ],
          },
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.MODELS,
      });

      const parsed = yaml.load(res.yaml_content);
      const col = parsed.models[0].columns[0];
      expect(col.meta.metabase.formatting.currency).toBe('EUR');
      expect(col.meta.metabase.formatting.prefix).toBe('€');
      expect(col.meta.metabase.formatting.decimals).toBe(2);
      expect(col.meta.metabase.formatting.colorRules).toHaveLength(2);
      expect(parsed.models[0].meta.lightdash.tier).toBe('marts_fact');
    });
  });

  // =========================================================================
  // GROUP 4: BOUNDARY CONDITIONS, EMPTY INPUTS & RESILIENCE
  // =========================================================================
  describe('Group 4: Boundary Conditions, Empty Inputs & Resilience', () => {
    test('TC-ADV-4.1: Empty rules array without model_name generates valid empty YAML structures without crash', () => {
      const res = exporter.exportSemanticToYaml({
        rules: [],
        format: EXPORT_FORMATS.ALL,
      });

      expect(res.success).toBe(true);
      expect(res.exported_count).toBe(0);
      expect(res.skipped_count).toBe(0);

      // schema.yml is valid version: 2 with empty models array
      const parsedSchema = yaml.load(res.files['schema.yml']);
      expect(parsedSchema.version).toBe(2);
      expect(parsedSchema.models).toEqual([]);

      const parsedSem = yaml.load(res.files['semantic_models.yml']);
      expect(parsedSem.version).toBe(2);
      expect(parsedSem.semantic_models).toEqual([]);

      const parsedMetrics = yaml.load(res.files['metrics.yml']);
      expect(parsedMetrics.version).toBe(2);
      expect(parsedMetrics.metrics).toEqual([]);
    });

    test('TC-ADV-4.2: Empty rules array WITH model_name produces a clean model skeleton with 0 columns', () => {
      const res = exporter.exportSemanticToYaml({
        rules: [],
        model_name: 'dim_empty_skeleton',
        format: EXPORT_FORMATS.MODELS,
      });

      expect(res.success).toBe(true);
      const parsed = yaml.load(res.yaml_content);
      expect(parsed.version).toBe(2);
      expect(parsed.models.length).toBe(1);
      expect(parsed.models[0].name).toBe('dim_empty_skeleton');
      expect(parsed.models[0].columns).toEqual([]);
      expect(parsed.models[0].meta.metabase.governance_policy).toBe('ACTIVE_APPROVED_RULES');
    });

    test('TC-ADV-4.3: Empty semantic memory defaults to 0 exported rules when no rules passed', () => {
      const resEmpty = exporter.exportSemanticToYaml({});
      expect(resEmpty.success).toBe(true);
      expect(resEmpty.exported_count).toBe(0);
    });

    test('TC-ADV-4.4: Incomplete rule objects (missing non-critical fields) serialize safely', () => {
      const sparseRules = [
        { rule_id: 'sparse_1', term: 'Sparse Metric', status: RULE_STATUS.ACTIVE },
        { rule_id: 'sparse_2', term: 'Another Sparse', status: RULE_STATUS.ACTIVE, dbt_model_hint: 'fct_sparse' },
      ];

      const res = exporter.exportSemanticToYaml({
        rules: sparseRules,
        format: EXPORT_FORMATS.ALL,
      });

      expect(res.success).toBe(true);
      expect(res.exported_count).toBe(2);

      const parsedSchema = yaml.load(res.files['schema.yml']);
      expect(parsedSchema).toBeDefined();
      expect(parsedSchema.version).toBe(2);

      const parsedMetrics = yaml.load(res.files['metrics.yml']);
      expect(parsedMetrics.metrics.length).toBe(2);
    });

    test('TC-ADV-4.5: Filtering edge cases (non-existent IDs, categories, models)', () => {
      const rules = [
        { rule_id: 'r1', term: 'Term 1', category: 'metric_definition', dbt_model_hint: 'm1', status: 'ACTIVE' },
        { rule_id: 'r2', term: 'Term 2', category: 'business_term', dbt_model_hint: 'm2', status: 'ACTIVE' },
      ];

      // Non-existent ID filter
      const resId = exporter.exportSemanticToYaml({ rules, rule_ids: ['non_existent'] });
      expect(resId.exported_count).toBe(0);

      // Non-existent category filter
      const resCat = exporter.exportSemanticToYaml({ rules, categories: ['non_existent_cat'] });
      expect(resCat.exported_count).toBe(0);
      expect(resCat.skipped_count).toBe(2);

      // Non-existent model filter
      const resMod = exporter.exportSemanticToYaml({ rules, target_model: 'non_existent_model' });
      expect(resMod.exported_count).toBe(0);
      expect(resMod.skipped_count).toBe(2);
    });

    test('TC-ADV-4.6: Deep immutability: verifies input rule objects are never mutated', () => {
      const originalRule = Object.freeze({
        rule_id: 'rule_frozen_1',
        term: 'Frozen Term',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Frozen definition',
        sql_condition: 'SUM(val)',
        status: RULE_STATUS.ACTIVE,
      });

      expect(() => {
        exporter.exportSemanticToYaml({
          rules: [originalRule],
          format: EXPORT_FORMATS.ALL,
        });
      }).not.toThrow();

      expect(originalRule.term).toBe('Frozen Term');
    });

    test('TC-ADV-4.7: MetricFlow ratio/cumulative/derived defaults when parameters are absent', () => {
      const rules = [
        {
          rule_id: 'r_ratio_no_params',
          term: 'Sparse Ratio',
          metric_type: METRIC_TYPES.RATIO,
          status: RULE_STATUS.ACTIVE,
        },
        {
          rule_id: 'r_cumul_no_params',
          term: 'Sparse Cumulative',
          metric_type: METRIC_TYPES.CUMULATIVE,
          status: RULE_STATUS.ACTIVE,
        },
        {
          rule_id: 'r_derived_no_params',
          term: 'Sparse Derived',
          metric_type: METRIC_TYPES.DERIVED,
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const res = exporter.exportSemanticToYaml({
        rules,
        format: EXPORT_FORMATS.METRICS,
      });

      const parsed = yaml.load(res.yaml_content);
      expect(parsed.metrics.length).toBe(3);
      expect(parsed.metrics[0].type_params.numerator).toBe('total_revenue');
      expect(parsed.metrics[0].type_params.denominator).toBe('order_count');
      expect(parsed.metrics[1].type_params.window).toBe('1 year');
      expect(parsed.metrics[2].type_params.expr).toBe('sparse_derived_a / sparse_derived_b');
    });

    test('TC-ADV-4.8: Identifier sanitizer handles valid standard names and prefixes numbers', () => {
      expect(sanitizeIdentifier(null)).toBe('unnamed');
      expect(sanitizeIdentifier('')).toBe('unnamed');
      expect(sanitizeIdentifier('123abc456')).toBe('m_123abc456');
      expect(sanitizeIdentifier('Order Count')).toBe('order_count');
      expect(sanitizeIdentifier('Order Count %')).toBe('order_count_pct');
      expect(sanitizeIdentifier('Orders & Subscriptions #')).toBe('orders_and_subscriptions_num');
    });
  });
});

// Helper function
function semModelMeasure(parsedSemYaml, measureName) {
  for (const semModel of parsedSemYaml.semantic_models || []) {
    const found = (semModel.measures || []).find(m => m.name === measureName);
    if (found) return found;
  }
  return null;
}
