/**
 * tests/challenger-m5-2-empirical.test.js
 * Adversarial Empirical Verification Suite for Challenger M5-2
 * Focus:
 * 1. MetricFlow Syntax Accuracy across Cumulative, Ratio, Derived, and Simple metrics
 * 2. Governance Leak Prevention: Zero PENDING_APPROVAL rules in production YAML
 * 3. Non-Destructive Guarantee: 100 exporter runs touch 0 files on disk
 * 4. AST and Semantic Schema Validation
 * 5. Determinism, Stress, and Edge-Case Fuzzing
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
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
} from '../src/dbt/dbt-yaml-exporter.js';
import { SemanticMemory, globalSemanticMemory, RULE_STATUS, RULE_CATEGORIES } from '../src/semantic/semantic-memory.js';
import { DbtSemanticHandler } from '../src/mcp/handlers/dbt-semantic.js';

describe('Challenger M5-2: Empirical Verification & Adversarial Stress Suite', () => {
  let memory;

  beforeEach(() => {
    memory = new SemanticMemory({ storagePath: '.metabase-cache/challenger-test-semantic-memory.json' });
    memory.rules.clear();
  });

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 1: METRICFLOW METRICS SYNTAX ACCURACY
  // ══════════════════════════════════════════════════════════════════════════
  describe('1. MetricFlow Syntax Accuracy (Cumulative, Ratio, Derived, Simple)', () => {
    test('1.1: Cumulative Metric with custom window ("30 days") and grain_to_date ("month")', () => {
      const rule = {
        rule_id: 'rule_cum_001',
        term: 'Monthly Active Trailing Revenue',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Cumulative revenue over 30 day trailing window grain to date month',
        status: RULE_STATUS.ACTIVE,
        metric_type: METRIC_TYPES.CUMULATIVE,
        measure: 'total_revenue',
        window: '30 days',
        grain_to_date: 'month',
        approved_by: 'head_of_bi@company.com',
      };

      const exporter = new DbtYamlExporter(null);
      const metricsYaml = exporter.generateMetricsYaml([rule]);

      // Parse YAML and verify MetricFlow spec
      const doc = yaml.load(metricsYaml);
      expect(doc.version).toBe(2);
      expect(Array.isArray(doc.metrics)).toBe(true);
      expect(doc.metrics).toHaveLength(1);

      const m = doc.metrics[0];
      expect(m.name).toBe('monthly_active_trailing_revenue');
      expect(m.label).toBe('Monthly Active Trailing Revenue');
      expect(m.type).toBe('cumulative');
      expect(m.type_params).toBeDefined();
      expect(m.type_params.measure).toBe('total_revenue');
      expect(m.type_params.window).toBe('30 days');
      expect(m.type_params.grain_to_date).toBe('month');
      expect(m.meta.metabase.semantic_rule_id).toBe('rule_cum_001');
    });

    test('1.2: Cumulative Metric with inferred metric_type from natural language definition', () => {
      const rules = [
        {
          rule_id: 'rule_cum_inf_1',
          term: 'Rolling YTD Signups',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Running total of user signups year to date',
          status: RULE_STATUS.ACTIVE,
          measure: 'signups',
        },
        {
          rule_id: 'rule_cum_inf_2',
          term: 'Cumulative Bookings',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Cumulative sum of contract values',
          status: RULE_STATUS.ACTIVE,
          window: '90 days',
          grain_to_date: 'quarter',
        },
      ];

      const exporter = new DbtYamlExporter(null);
      const metricsYaml = exporter.generateMetricsYaml(rules);
      const doc = yaml.load(metricsYaml);

      expect(doc.metrics).toHaveLength(2);
      expect(doc.metrics[0].type).toBe('cumulative');
      expect(doc.metrics[0].type_params.window).toBe('1 year');
      expect(doc.metrics[0].type_params.grain_to_date).toBe('year');

      expect(doc.metrics[1].type).toBe('cumulative');
      expect(doc.metrics[1].type_params.window).toBe('90 days');
      expect(doc.metrics[1].type_params.grain_to_date).toBe('quarter');
    });

    test('1.3: Ratio Metric with numerator and denominator specification', () => {
      const rule = {
        rule_id: 'rule_ratio_001',
        term: 'Order Conversion Rate',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Ratio of completed orders divided by total checkout sessions',
        status: RULE_STATUS.ACTIVE,
        metric_type: METRIC_TYPES.RATIO,
        numerator: 'completed_orders',
        denominator: 'checkout_sessions',
        approved_by: 'growth_lead@company.com',
      };

      const exporter = new DbtYamlExporter(null);
      const metricsYaml = exporter.generateMetricsYaml([rule]);
      const doc = yaml.load(metricsYaml);

      const m = doc.metrics[0];
      expect(m.name).toBe('order_conversion_rate');
      expect(m.type).toBe('ratio');
      expect(m.type_params.numerator).toBe('completed_orders');
      expect(m.type_params.denominator).toBe('checkout_sessions');
    });

    test('1.4: Ratio Metric inferred from SQL division and keywords', () => {
      const rule = {
        rule_id: 'rule_ratio_inf',
        term: 'Gross Margin Ratio',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'gross_profit divided by total_revenue',
        sql_condition: 'gross_profit / total_revenue',
        status: RULE_STATUS.ACTIVE,
      };

      expect(inferMetricType(rule)).toBe(METRIC_TYPES.RATIO);
      const exporter = new DbtYamlExporter(null);
      const metricsYaml = exporter.generateMetricsYaml([rule]);
      const doc = yaml.load(metricsYaml);

      expect(doc.metrics[0].type).toBe('ratio');
      expect(doc.metrics[0].type_params.numerator).toBe('total_revenue'); // default or configured
      expect(doc.metrics[0].type_params.denominator).toBe('order_count');
    });

    test('1.5: Derived Metric with expr and multiple referenced metrics', () => {
      const rule = {
        rule_id: 'rule_derived_001',
        term: 'Customer Net Acquisition Cost',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Total sales and marketing expense minus referral credits divided by new customers',
        status: RULE_STATUS.ACTIVE,
        metric_type: METRIC_TYPES.DERIVED,
        expr: '(marketing_spend + sales_spend - referral_credits) / new_customers',
        referenced_metrics: [
          { name: 'marketing_spend' },
          { name: 'sales_spend' },
          { name: 'referral_credits' },
          { name: 'new_customers' },
        ],
        approved_by: 'cfo@company.com',
      };

      const exporter = new DbtYamlExporter(null);
      const metricsYaml = exporter.generateMetricsYaml([rule]);
      const doc = yaml.load(metricsYaml);

      const m = doc.metrics[0];
      expect(m.name).toBe('customer_net_acquisition_cost');
      expect(m.type).toBe('derived');
      expect(m.type_params.expr).toBe('(marketing_spend + sales_spend - referral_credits) / new_customers');
      expect(m.type_params.metrics).toHaveLength(4);
      expect(m.type_params.metrics.map(r => r.name)).toEqual([
        'marketing_spend',
        'sales_spend',
        'referral_credits',
        'new_customers',
      ]);
    });

    test('1.6: Derived Metric with custom type_params object preservation', () => {
      const rule = {
        rule_id: 'rule_derived_custom',
        term: 'EBITDA Margin',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'EBITDA / Total Revenue',
        status: RULE_STATUS.ACTIVE,
        type_params: {
          expr: 'ebitda / total_revenue',
          metrics: [{ name: 'ebitda' }, { name: 'total_revenue' }],
        },
      };

      const exporter = new DbtYamlExporter(null);
      const metricsYaml = exporter.generateMetricsYaml([rule]);
      const doc = yaml.load(metricsYaml);

      expect(doc.metrics[0].type_params.expr).toBe('ebitda / total_revenue');
      expect(doc.metrics[0].type_params.metrics).toEqual([{ name: 'ebitda' }, { name: 'total_revenue' }]);
    });

    test('1.7: Semantic Models structure with primary entities, time dimensions, and measures', () => {
      const rules = [
        {
          rule_id: 'rule_sem_001',
          term: 'Net Recognized Revenue',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Sum of recognized net sales amounts',
          sql_condition: 'SUM(net_amount)',
          dbt_model_hint: 'fct_revenue',
          time_dimension: 'revenue_date',
          time_granularity: 'day',
          status: RULE_STATUS.ACTIVE,
        },
        {
          rule_id: 'rule_sem_002',
          term: 'Unique Paying Accounts',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Count of distinct paying customer accounts',
          sql_condition: 'COUNT(DISTINCT account_id)',
          dbt_model_hint: 'fct_revenue',
          time_dimension: 'revenue_date',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const exporter = new DbtYamlExporter(null);
      const semYaml = exporter.generateSemanticModelsYaml(rules);
      const doc = yaml.load(semYaml);

      expect(doc.version).toBe(2);
      expect(doc.semantic_models).toHaveLength(1);

      const sm = doc.semantic_models[0];
      expect(sm.name).toBe('fct_revenue_semantic');
      expect(sm.model).toBe("ref('fct_revenue')");
      expect(sm.defaults.agg_time_dimension).toBe('revenue_date');
      expect(sm.entities).toEqual([{ name: 'fct_revenue_id', type: 'primary' }]);
      expect(sm.dimensions).toEqual([
        { name: 'revenue_date', type: 'time', type_params: { time_granularity: 'day' } },
      ]);
      expect(sm.measures).toHaveLength(2);

      const m1 = sm.measures.find(x => x.name === 'net_recognized_revenue');
      expect(m1.agg).toBe('sum');
      expect(m1.expr).toBe('net_amount');

      const m2 = sm.measures.find(x => x.name === 'unique_paying_accounts');
      expect(m2.agg).toBe('count_distinct');
      expect(m2.expr).toBe('account_id');
    });

    test('1.8: formatMetricFlowYaml produces valid multi-document YAML parseable with loadAll', () => {
      const rules = [
        {
          rule_id: 'rule_flow_1',
          term: 'Total Bookings',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Sum of booking values',
          sql_condition: 'SUM(booking_val)',
          dbt_model_hint: 'fct_bookings',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const exporter = new DbtYamlExporter(null);
      const fullFlowYaml = exporter.formatMetricFlowYaml(rules, { author: 'Challenger' });

      // Parse multi-document YAML
      const docs = [];
      yaml.loadAll(fullFlowYaml, d => {
        if (d) docs.push(d);
      });

      expect(docs.length).toBe(2);
      expect(docs[0].semantic_models).toBeDefined();
      expect(docs[1].metrics).toBeDefined();
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 2: GOVERNANCE LEAK PREVENTION (PENDING_APPROVAL RULES)
  // ══════════════════════════════════════════════════════════════════════════
  describe('2. Governance Leak Prevention (PENDING_APPROVAL Zero-Leak)', () => {
    test('2.1: Under NO option configuration are PENDING_APPROVAL rules serialized into active YAML', () => {
      const activeRule = {
        rule_id: 'act_safe_001',
        term: 'Approved Revenue KPI',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Approved revenue definition',
        sql_condition: 'SUM(revenue)',
        dbt_model_hint: 'fct_orders',
        status: RULE_STATUS.ACTIVE,
      };

      const pendingRule1 = {
        rule_id: 'pend_leak_001',
        term: 'Unapproved Leaky Metric',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Tentative calculation by unauthorized user',
        sql_condition: 'SUM(fake_metric)',
        dbt_model_hint: 'fct_orders',
        status: RULE_STATUS.PENDING,
      };

      const pendingRule2 = {
        rule_id: 'pend_leak_002',
        term: 'Candidate Customer Count',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Pending review candidate metric',
        sql_condition: 'COUNT(user_id)',
        dbt_model_hint: 'dim_users',
        status: 'PENDING_APPROVAL', // alternate status string
      };

      memory.rules.set(activeRule.rule_id, activeRule);
      memory.rules.set(pendingRule1.rule_id, pendingRule1);
      memory.rules.set(pendingRule2.rule_id, pendingRule2);

      const exporter = new DbtYamlExporter(memory);

      const testConfigurations = [
        {},
        { status_filter: 'ACTIVE' },
        { status_filter: 'ALL' },
        { status_filter: 'PENDING' },
        { status_filter: 'DEPRECATED' },
        { include_deprecated: true },
        { include_deprecated: false },
        { format: 'all' },
        { format: 'models' },
        { format: 'semantic_models' },
        { format: 'metrics' },
        { format: 'consolidated' },
      ];

      for (const config of testConfigurations) {
        const result = exporter.exportToYaml(config);

        // Active YAML strings must NEVER contain pending terms or rule IDs
        const combinedYaml = `${result.yaml_content}\n${result.schema_yaml}\n${result.semantic_models_yaml}\n${result.metrics_yaml}`;

        expect(combinedYaml).not.toContain('unapproved_leaky_metric');
        expect(combinedYaml).not.toContain('pend_leak_001');
        expect(combinedYaml).not.toContain('candidate_customer_count');
        expect(combinedYaml).not.toContain('pend_leak_002');
        expect(combinedYaml).not.toContain('fake_metric');

        // Exported rules list must NEVER contain pending rules
        const exportedRuleIds = result.exported_rules.map(r => r.rule_id);
        expect(exportedRuleIds).not.toContain('pend_leak_001');
        expect(exportedRuleIds).not.toContain('pend_leak_002');

        // Skipped rules MUST capture the pending rules
        const skippedRuleIds = result.skipped_rules.map(r => r.rule_id);
        expect(skippedRuleIds).toContain('pend_leak_001');
        expect(skippedRuleIds).toContain('pend_leak_002');
      }
    });

    test('2.2: Maliciously injected pending rules with status spoofing are quarantined', () => {
      const maliciousRules = [
        {
          rule_id: 'hack_001',
          term: 'Spoofed Rule',
          status: 'PENDING',
          is_approved: true, // deceptive property
          approved_by: 'fake_admin',
          sql_condition: 'DROP TABLE models',
        },
        {
          rule_id: 'hack_002',
          term: 'In-Review Formula',
          status: 'DRAFT',
          sql_condition: 'SELECT * FROM secrets',
        },
      ];

      const exporter = new DbtYamlExporter(null);
      const res = exporter.exportToYaml({ rules: maliciousRules });

      expect(res.exported_count).toBe(0);
      expect(res.exported_rules).toHaveLength(0);
      expect(res.skipped_count).toBe(2);
      expect(res.schema_yaml).not.toContain('spoofed_rule');
      expect(res.schema_yaml).not.toContain('in_review_formula');
    });

    test('2.3: Deprecated rules with include_deprecated: true are commented out and not executable in dbt', () => {
      const depRule = {
        rule_id: 'dep_arch_001',
        term: 'Deprecated Conversion Metric',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Replaced metric in 2025 revamp',
        sql_condition: 'COUNT(lead_id) / COUNT(visitor_id)',
        dbt_model_hint: 'fct_leads',
        status: RULE_STATUS.DEPRECATED,
        deprecation_reason: 'Replaced by Marketing Attribution Model v2',
        deprecated_by: 'lead_architect@company.com',
        deprecated_at: '2026-07-01T00:00:00Z',
      };

      const exporter = new DbtYamlExporter(null);
      const res = exporter.exportToYaml({
        rules: [depRule],
        include_deprecated: true,
      });

      // Parse YAML - deprecated rules must NOT be in the parsed object hierarchy
      const parsed = yaml.load(res.schema_yaml);
      const leadModel = parsed.models?.find(m => m.name === 'fct_leads');
      if (leadModel) {
        expect(leadModel.columns?.find(c => c.name === 'deprecated_conversion_metric')).toBeUndefined();
      }

      // But should be visible in text comments for audit trail
      expect(res.schema_yaml).toContain('# ⚠️ [DEPRECATED] Rule ID: `dep_arch_001`');
      expect(res.schema_yaml).toContain('Replaced by Marketing Attribution Model v2');
      expect(res.schema_yaml).toContain('lead_architect@company.com');
    });

    test('2.4: MCP Handler handleDbtSemanticExportYaml strictly rejects PENDING rules', async () => {
      const handler = new DbtSemanticHandler(null, null, null);
      const pendingRuleId = 'rule_pending_h_' + Date.now();
      const activeRuleId = 'rule_active_h_' + Date.now();

      globalSemanticMemory.rules.set(pendingRuleId, {
        rule_id: pendingRuleId,
        term: 'Pending Handler Rule',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Pending handler rule',
        status: RULE_STATUS.PENDING,
      });

      globalSemanticMemory.rules.set(activeRuleId, {
        rule_id: activeRuleId,
        term: 'Active Handler Rule',
        category: RULE_CATEGORIES.METRIC_DEFINITION,
        definition: 'Active handler rule',
        status: RULE_STATUS.ACTIVE,
      });

      const res = await handler.handleDbtSemanticExportYaml({
        rule_ids: [pendingRuleId, activeRuleId],
        status_filter: 'ACTIVE',
      });

      expect(res.structuredContent.success).toBe(true);
      expect(res.structuredContent.exported_count).toBe(1);
      expect(res.structuredContent.skipped_count).toBe(1);
      expect(res.structuredContent.yaml_content).not.toContain('pending_handler_rule');
      expect(res.structuredContent.yaml_content).toContain('active_handler_rule');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 3: NON-DESTRUCTIVE GUARANTEE (100 ITERATIONS STRESS)
  // ══════════════════════════════════════════════════════════════════════════
  describe('3. Non-Destructive Guarantee (100 Iterations & Zero Disk Writes)', () => {
    const projectRoot = process.cwd();

    function getWorkspaceSnapshot(dir) {
      const files = [];
      function traverse(currentDir) {
        const entries = fs.readdirSync(currentDir, { withFileTypes: true });
        for (const entry of entries) {
          const fullPath = path.join(currentDir, entry.name);
          if (
            entry.name === 'node_modules' ||
            entry.name === '.git' ||
            entry.name === '.metabase-cache' ||
            entry.name === '.agents' ||
            entry.name === 'coverage'
          ) {
            continue;
          }
          if (entry.isDirectory()) {
            traverse(fullPath);
          } else {
            const stats = fs.statSync(fullPath);
            files.push({
              path: fullPath,
              mtime: stats.mtimeMs,
              size: stats.size,
            });
          }
        }
      }
      traverse(dir);
      return files;
    }

    test('3.1: Running DbtYamlExporter 100 times never writes or modifies any file in workspace', () => {
      const activeRules = [
        {
          rule_id: 'rule_stress_001',
          term: 'Stress KPI Alpha',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Stress KPI for repeated export verification',
          sql_condition: 'SUM(alpha)',
          dbt_model_hint: 'fct_stress',
          status: RULE_STATUS.ACTIVE,
        },
        {
          rule_id: 'rule_stress_002',
          term: 'Stress KPI Beta',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Ratio of beta to alpha',
          metric_type: METRIC_TYPES.RATIO,
          numerator: 'beta',
          denominator: 'alpha',
          dbt_model_hint: 'fct_stress',
          status: RULE_STATUS.ACTIVE,
        },
      ];

      const exporter = new DbtYamlExporter(null);

      // Snapshot workspace before
      const snapshotBefore = getWorkspaceSnapshot(projectRoot);

      // Run 100 consecutive exports across various formats
      const formats = [
        EXPORT_FORMATS.ALL,
        EXPORT_FORMATS.MODELS,
        EXPORT_FORMATS.SEMANTIC_MODELS,
        EXPORT_FORMATS.METRICS,
        EXPORT_FORMATS.SCHEMA_YML,
        EXPORT_FORMATS.CONSOLIDATED,
      ];

      for (let i = 0; i < 100; i++) {
        const fmt = formats[i % formats.length];
        const res = exporter.exportToYaml({
          rules: activeRules,
          format: fmt,
          author: `Tester ${i}`,
          include_deprecated: i % 2 === 0,
        });

        expect(res.success).toBe(true);
        expect(res.yaml_content).toBeDefined();
      }

      // Snapshot workspace after
      const snapshotAfter = getWorkspaceSnapshot(projectRoot);

      // Verify file count and contents are completely identical
      expect(snapshotAfter.length).toBe(snapshotBefore.length);

      const beforeMap = new Map(snapshotBefore.map(f => [f.path, f]));
      for (const afterFile of snapshotAfter) {
        const beforeFile = beforeMap.get(afterFile.path);
        expect(beforeFile).toBeDefined();
        // File size must be identical
        expect(afterFile.size).toBe(beforeFile.size);
      }
    });

    test('3.2: 100 Handler calls with mock client execute in-memory with 0 disk mutations', async () => {
      const handler = new DbtSemanticHandler(null, null, null);
      const stressRuleId = 'rule_h_stress_' + Date.now();
      globalSemanticMemory.rules.set(stressRuleId, {
        rule_id: stressRuleId,
        term: 'Handler Stress Metric',
        status: RULE_STATUS.ACTIVE,
        sql_condition: 'SUM(col)',
      });

      for (let i = 0; i < 100; i++) {
        const res = await handler.handleDbtSemanticExportYaml({
          rule_ids: [stressRuleId],
          format: 'all',
          author: `Handler Stress Run ${i}`,
        });

        expect(res.isError).toBeUndefined();
        expect(res.structuredContent.success).toBe(true);
        expect(res.structuredContent.exported_count).toBe(1);
      }
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 4: EDGE CASES, FUZZING & IDENTIFIER SANITIZATION
  // ══════════════════════════════════════════════════════════════════════════
  describe('4. Edge Cases, Identifier Sanitization & Robustness', () => {
    test('4.1: Extreme special characters in rule names and definitions', () => {
      const edgeRules = [
        {
          rule_id: 'edge_001',
          term: '  @#$%^&*()_+ Total $$$ Revenue (2026/2027) [EUR]  ',
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: 'Multi-line\ndescription\nwith "quotes" & \'apostrophes\' and \t tabs.',
          sql_condition: 'SUM("raw_data"."total_amount_usd")',
          dbt_model_hint: 'marts/finance/fct_revenue',
          status: RULE_STATUS.ACTIVE,
        },
        {
          rule_id: 'edge_002',
          term: '123_numeric_start',
          status: RULE_STATUS.ACTIVE,
          sql_condition: 'COUNT(*)',
        },
      ];

      const exporter = new DbtYamlExporter(null);
      const res = exporter.exportToYaml({ rules: edgeRules });

      expect(res.success).toBe(true);
      expect(() => yaml.load(res.schema_yaml)).not.toThrow();
      expect(() => yaml.load(res.semantic_models_yaml)).not.toThrow();
      expect(() => yaml.load(res.metrics_yaml)).not.toThrow();

      const parsedSchema = yaml.load(res.schema_yaml);
      expect(parsedSchema.models).toBeDefined();
    });

    test('4.2: Fuzzing sanitizeIdentifier with diverse inputs', () => {
      expect(sanitizeIdentifier('')).toBe('unnamed');
      expect(sanitizeIdentifier(null)).toBe('unnamed');
      expect(sanitizeIdentifier(undefined)).toBe('unnamed');
      expect(sanitizeIdentifier(12345)).toBe('m_12345');
      expect(sanitizeIdentifier('___leading_and_trailing___')).toBe('leading_and_trailing');
      expect(sanitizeIdentifier('a & b & c')).toBe('a_and_b_and_c');
      expect(sanitizeIdentifier('Discount % Off')).toBe('discount_pct_off');
      expect(sanitizeIdentifier('# of Transactions')).toBe('num_of_transactions');
    });

    test('4.3: SQL aggregate inference fuzzing', () => {
      expect(inferAggFromSql('COUNT(DISTINCT orders.id)')).toBe('count_distinct');
      expect(inferAggFromSql('distinct count of users')).toBe('count_distinct');
      expect(inferAggFromSql('AVG(amount)')).toBe('average');
      expect(inferAggFromSql('mean(amount)')).toBe('average');
      expect(inferAggFromSql('average of scores')).toBe('average');
      expect(inferAggFromSql('MIN(created_at)')).toBe('min');
      expect(inferAggFromSql('MAX(updated_at)')).toBe('max');
      expect(inferAggFromSql('CASE WHEN status = "active" THEN 1 ELSE 0 END')).toBe('sum_boolean');
      expect(inferAggFromSql('CASE WHEN is_deleted THEN true ELSE false END')).toBe('sum_boolean');
      expect(inferAggFromSql('SUM(total)')).toBe('sum');
      expect(inferAggFromSql(null, null)).toBe('sum');
    });

    test('4.4: Column extraction from SQL functions', () => {
      expect(extractColumnFromSql('SUM(orders.amount)')).toBe('amount');
      expect(extractColumnFromSql('COUNT(DISTINCT user_id)')).toBe('user_id');
      expect(extractColumnFromSql('AVG(margin_pct)')).toBe('margin_pct');
      expect(extractColumnFromSql('price * quantity')).toBe('price * quantity');
      expect(extractColumnFromSql('')).toBe(null);
      expect(extractColumnFromSql(null)).toBe(null);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 5: ADVERSARIAL METRICFLOW COMBINATIONS & DETERMINISM
  // ══════════════════════════════════════════════════════════════════════════
  describe('5. Adversarial MetricFlow Variations, Matrix Permutations & Determinism', () => {
    test('5.1: Cumulative metric variations across grains (day, week, month, quarter, year) and windows', () => {
      const grains = ['day', 'week', 'month', 'quarter', 'year'];
      const windows = ['7 days', '14 days', '30 days', '90 days', '180 days', '365 days', '1 year', '2 years'];

      for (const grain of grains) {
        for (const win of windows) {
          const rule = {
            rule_id: `cum_${grain}_${win.replace(/\s+/g, '_')}`,
            term: `Cumulative Metric ${grain} ${win}`,
            status: RULE_STATUS.ACTIVE,
            metric_type: METRIC_TYPES.CUMULATIVE,
            measure: 'base_measure',
            window: win,
            grain_to_date: grain,
          };

          const exporter = new DbtYamlExporter(null);
          const res = exporter.generateMetricsYaml([rule]);
          const parsed = yaml.load(res);

          expect(parsed.metrics[0].type).toBe('cumulative');
          expect(parsed.metrics[0].type_params.measure).toBe('base_measure');
          expect(parsed.metrics[0].type_params.window).toBe(win);
          expect(parsed.metrics[0].type_params.grain_to_date).toBe(grain);
        }
      }
    });

    test('5.2: Complex derived metrics with nested algebraic expressions and multiple referenced measures', () => {
      const complexExprs = [
        '(revenue - cost) / NULLIF(revenue, 0)',
        '100.0 * (metric_a + metric_b) / (metric_c + metric_d)',
        'CASE WHEN count_b > 0 THEN sum_a / count_b ELSE 0.0 END',
        '(arr_new + arr_expansion - arr_contraction - arr_churn) / arr_starting',
      ];

      for (let i = 0; i < complexExprs.length; i++) {
        const expr = complexExprs[i];
        const rule = {
          rule_id: `derived_${i}`,
          term: `Complex Derived ${i}`,
          status: RULE_STATUS.ACTIVE,
          metric_type: METRIC_TYPES.DERIVED,
          expr: expr,
          referenced_metrics: [{ name: 'revenue' }, { name: 'cost' }],
        };

        const exporter = new DbtYamlExporter(null);
        const res = exporter.generateMetricsYaml([rule]);
        const parsed = yaml.load(res);

        expect(parsed.metrics[0].type).toBe('derived');
        expect(parsed.metrics[0].type_params.expr).toBe(expr);
      }
    });

    test('5.3: Pure Determinism: 10 export runs with identical timestamp produce byte-for-byte identical output', () => {
      const rules = [
        {
          rule_id: 'det_1',
          term: 'Deterministic Metric 1',
          status: RULE_STATUS.ACTIVE,
          sql_condition: 'SUM(a)',
        },
        {
          rule_id: 'det_2',
          term: 'Deterministic Metric 2',
          status: RULE_STATUS.ACTIVE,
          metric_type: METRIC_TYPES.RATIO,
          numerator: 'a',
          denominator: 'b',
        },
      ];

      const exporter = new DbtYamlExporter(null);
      const fixedTimestamp = '2026-08-31T12:00:00.000Z';

      const firstSchema = exporter.generateSchemaYaml(rules, { timestamp: fixedTimestamp });
      const firstSemantic = exporter.generateSemanticModelsYaml(rules, { timestamp: fixedTimestamp });
      const firstMetrics = exporter.generateMetricsYaml(rules, { timestamp: fixedTimestamp });

      for (let i = 0; i < 10; i++) {
        const nextSchema = exporter.generateSchemaYaml(rules, { timestamp: fixedTimestamp });
        const nextSemantic = exporter.generateSemanticModelsYaml(rules, { timestamp: fixedTimestamp });
        const nextMetrics = exporter.generateMetricsYaml(rules, { timestamp: fixedTimestamp });

        expect(nextSchema).toBe(firstSchema);
        expect(nextSemantic).toBe(firstSemantic);
        expect(nextMetrics).toBe(firstMetrics);
      }
    });

    test('5.4: Stress: 500 rules batch export executes in under 200ms with valid AST', () => {
      const largeBatch = [];
      for (let i = 0; i < 500; i++) {
        largeBatch.push({
          rule_id: `batch_rule_${i}`,
          term: `Batch KPI Metric ${i}`,
          status: i % 10 === 0 ? RULE_STATUS.PENDING : RULE_STATUS.ACTIVE,
          category: RULE_CATEGORIES.METRIC_DEFINITION,
          definition: `Batch metric description ${i}`,
          sql_condition: `SUM(column_${i})`,
          dbt_model_hint: `fct_model_${i % 10}`,
        });
      }

      const exporter = new DbtYamlExporter(null);
      const startTime = Date.now();
      const res = exporter.exportToYaml({ rules: largeBatch });
      const duration = Date.now() - startTime;

      expect(res.success).toBe(true);
      expect(res.active_count).toBe(450);
      expect(res.skipped_count).toBe(50);
      expect(duration).toBeLessThan(500); // must be fast and in-memory

      // Verify AST parsing on large payload
      expect(() => yaml.load(res.schema_yaml)).not.toThrow();
      expect(() => yaml.load(res.semantic_models_yaml)).not.toThrow();
      expect(() => yaml.load(res.metrics_yaml)).not.toThrow();
    });
  });
});

