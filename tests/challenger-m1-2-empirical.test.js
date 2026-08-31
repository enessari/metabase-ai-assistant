import fs from 'fs';
import path from 'path';
import os from 'os';
import { DbtDeepScanner, DOC_BLOCK_REGEX, DOC_REF_REGEX, SQL_REF_REGEX, SQL_SOURCE_REGEX } from '../src/dbt/dbt-deep-scanner.js';
import { DBT_TIERS, DbtParser } from '../src/dbt/dbt-parser.js';
import { DbtSemanticHandler } from '../src/mcp/handlers/dbt-semantic.js';
import { getToolDefinitions, TOOL_METADATA } from '../src/mcp/tool-registry.js';

describe('Challenger M1-2: Empirical Correctness & Chaos Verification', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-challenger-m1-2-'));
  });

  afterEach(() => {
    if (tempDir && fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  // =========================================================================
  // Section 1: MetricFlow Semantic Models & 5 Metric Types Adversarial Testing
  // =========================================================================
  describe('Section 1: MetricFlow Semantic Layer & Metric Extraction', () => {
    test('CHAOS-1.1: extracts all 5 metric types (simple, ratio, cumulative, derived, conversion)', () => {
      const metricflowYaml = `
semantic_models:
  - name: transactions_source
    model: ref('fct_transactions')
    description: "Core transactions semantic model"
    entities:
      - name: transaction_id
        type: primary
      - name: customer_id
        type: foreign
      - name: session_id
        type: unique
      - name: device_fingerprint
        type: natural
    dimensions:
      - name: transaction_date
        type: time
        type_params:
          time_granularity: day
          valid_granularity: [day, month, year]
        is_partition: true
      - name: channel
        type: categorical
    measures:
      - name: gross_revenue
        agg: sum
        expr: amount
        agg_time_dimension: transaction_date
      - name: transaction_count
        agg: count
        expr: transaction_id
      - name: daily_active_buyers
        agg: count_distinct
        expr: customer_id
      - name: ending_inventory_balance
        agg: sum
        expr: quantity
        non_additive_dimension:
          name: transaction_date
          window_choice: max
          window_groupings: [channel]

metrics:
  # 1. Simple metric (scalar measure ref)
  - name: total_gross_revenue
    label: "Total Gross Revenue"
    description: "Total revenue before returns"
    type: simple
    type_params:
      measure: gross_revenue
    meta:
      metabase:
        formatting:
          number_style: currency
          currency: USD
          decimals: 2

  # 2. Ratio metric (numerator / denominator with object/string params)
  - name: average_order_value
    label: "Average Order Value (AOV)"
    description: "Gross revenue divided by total transaction count"
    type: ratio
    type_params:
      numerator: gross_revenue
      denominator:
        name: transaction_count
    meta:
      metabase:
        formatting:
          number_style: currency
          currency: USD
          decimals: 2

  # 3. Cumulative metric (rolling window & grain_to_date)
  - name: rolling_30d_revenue
    label: "Rolling 30-Day Gross Revenue"
    type: cumulative
    type_params:
      measure: gross_revenue
      window: 30 days
      grain_to_date: month

  # 4. Derived metric (complex arithmetic expression of other metrics)
  - name: net_operating_margin
    label: "Net Operating Margin"
    type: derived
    type_params:
      expr: "(total_gross_revenue - cogs_total - opex) / total_gross_revenue"
      metrics:
        - name: total_gross_revenue
          alias: gross
        - name: cogs_total
          alias: cogs
        - name: opex
          alias: op
    meta:
      lightdash:
        format: percent
        round: 2

  # 5. Conversion metric (funnel transition between base & conversion events)
  - name: visitor_to_buyer_conversion_rate
    label: "Visitor to Buyer Conversion Rate"
    type: conversion
    type_params:
      conversion_type_params:
        base_measure: session_count
        conversion_measure: transaction_count
        window: 7 days
        entity: customer_id
`;

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const parsed = scanner.parseMetricFlow(metricflowYaml, 'models/semantic/core.yml');

      expect(parsed.semanticModels.length).toBe(1);
      const semModel = parsed.semanticModels[0];
      expect(semModel.entities).toHaveLength(4);
      expect(semModel.entities.map(e => e.type)).toEqual(['primary', 'foreign', 'unique', 'natural']);

      expect(semModel.dimensions).toHaveLength(2);
      expect(semModel.dimensions[0].isPartition).toBe(true);
      expect(semModel.dimensions[0].typeParams.validGranularity).toEqual(['day', 'month', 'year']);

      expect(semModel.measures).toHaveLength(4);
      expect(semModel.measures[3].nonAdditiveDimension.windowChoice).toBe('max');
      expect(semModel.measures[3].nonAdditiveDimension.windowGroupings).toEqual(['channel']);

      // Check all 5 metrics
      expect(parsed.metrics.length).toBe(5);

      // 1. Simple
      const mSimple = scanner.metrics.get('total_gross_revenue');
      expect(mSimple.type).toBe('simple');
      expect(mSimple.typeParams.measure).toBe('gross_revenue');
      expect(mSimple.visualMeta.formatting.formatType).toBe('currency');
      expect(mSimple.visualMeta.formatting.currency).toBe('USD');

      // 2. Ratio
      const mRatio = scanner.metrics.get('average_order_value');
      expect(mRatio.type).toBe('ratio');
      expect(mRatio.typeParams.numerator).toBe('gross_revenue');
      expect(mRatio.typeParams.denominator).toBe('transaction_count');

      // 3. Cumulative
      const mCumulative = scanner.metrics.get('rolling_30d_revenue');
      expect(mCumulative.type).toBe('cumulative');
      expect(mCumulative.typeParams.measure).toBe('gross_revenue');
      expect(mCumulative.typeParams.window).toBe('30 days');
      expect(mCumulative.typeParams.grainToDate).toBe('month');

      // 4. Derived
      const mDerived = scanner.metrics.get('net_operating_margin');
      expect(mDerived.type).toBe('derived');
      expect(mDerived.typeParams.expr).toBe('(total_gross_revenue - cogs_total - opex) / total_gross_revenue');
      expect(mDerived.typeParams.metrics).toEqual([
        { name: 'total_gross_revenue', alias: 'gross' },
        { name: 'cogs_total', alias: 'cogs' },
        { name: 'opex', alias: 'op' },
      ]);
      expect(mDerived.visualMeta.formatting.formatType).toBe('percent');
      expect(mDerived.visualMeta.formatting.decimals).toBe(2);

      // 5. Conversion
      const mConversion = scanner.metrics.get('visitor_to_buyer_conversion_rate');
      expect(mConversion.type).toBe('conversion');
      expect(mConversion.typeParams.conversionTypeParams).toBeDefined();
      expect(mConversion.typeParams.conversionTypeParams.base_measure).toBe('session_count');
      expect(mConversion.typeParams.conversionTypeParams.conversion_measure).toBe('transaction_count');
      expect(mConversion.typeParams.conversionTypeParams.window).toBe('7 days');

      // Auto-synthesized base measure metrics
      expect(scanner.metrics.has('transactions_source_gross_revenue')).toBe(true);
      expect(scanner.metrics.has('transactions_source_ending_inventory_balance')).toBe(true);
    });

    test('CHAOS-1.2: handles malformed, empty, and non-object MetricFlow inputs without exceptions', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });

      expect(scanner.parseMetricFlow(null)).toEqual({ semanticModels: [], metrics: [] });
      expect(scanner.parseMetricFlow('')).toEqual({ semanticModels: [], metrics: [] });
      expect(scanner.parseMetricFlow('   \n  \n')).toEqual({ semanticModels: [], metrics: [] });
      expect(scanner.parseMetricFlow('just a random string')).toEqual({ semanticModels: [], metrics: [] });
      expect(scanner.parseMetricFlow('version: 2\nsome_other_key: [1, 2, 3]')).toEqual({ semanticModels: [], metrics: [] });
    });

    test('CHAOS-1.3: synthesizes multi-table join relationships across multiple primary & foreign entities', async () => {
      const semDir = path.join(tempDir, 'models', 'semantic');
      fs.mkdirSync(semDir, { recursive: true });

      const semYaml = `
semantic_models:
  - name: customers_model
    model: ref('dim_customers')
    entities:
      - name: customer_id
        type: primary
      - name: region_id
        type: foreign

  - name: regions_model
    model: ref('dim_regions')
    entities:
      - name: region_id
        type: primary

  - name: orders_model
    model: ref('fct_orders')
    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign
      - name: product_id
        type: foreign

  - name: products_model
    model: ref('dim_products')
    entities:
      - name: product_id
        type: primary
`;
      fs.writeFileSync(path.join(semDir, 'models.yml'), semYaml);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.relationshipCount).toBe(3);
      const ordersToCustomers = result.relationships.find(r => r.fromModel === 'fct_orders' && r.toModel === 'dim_customers');
      const ordersToProducts = result.relationships.find(r => r.fromModel === 'fct_orders' && r.toModel === 'dim_products');
      const customersToRegions = result.relationships.find(r => r.fromModel === 'dim_customers' && r.toModel === 'dim_regions');

      expect(ordersToCustomers).toBeDefined();
      expect(ordersToCustomers.fromColumn).toBe('customer_id');
      expect(ordersToCustomers.toColumn).toBe('customer_id');

      expect(ordersToProducts).toBeDefined();
      expect(ordersToProducts.fromColumn).toBe('product_id');

      expect(customersToRegions).toBeDefined();
      expect(customersToRegions.fromColumn).toBe('region_id');
    });
  });

  // =========================================================================
  // Section 2: Tier Ranking Accuracy Across Complex Naming & Custom Paths
  // =========================================================================
  describe('Section 2: Architectural Tier Ranking Accuracy', () => {
    const parser = new DbtParser();

    const testCases = [
      // Marts / Facts (100)
      { name: 'fct_daily_sales', path: 'models/marts/finance/fct_daily_sales.sql', meta: {}, tags: [], expectedTier: 'marts_fact', expectedRank: 100 },
      { name: 'fact_user_retention', path: 'models/custom/fact_user_retention.sql', meta: {}, tags: [], expectedTier: 'marts_fact', expectedRank: 100 },
      { name: 'unprefixed_marts_model', path: 'models/marts/core/unprefixed_marts_model.sql', meta: {}, tags: [], expectedTier: 'marts_fact', expectedRank: 100 },
      { name: 'custom_tagged_fact', path: 'models/legacy/custom.sql', meta: {}, tags: ['fact'], expectedTier: 'marts_fact', expectedRank: 100 },

      // Marts / Dims (90)
      { name: 'dim_organizations', path: 'models/marts/dim_organizations.sql', meta: {}, tags: [], expectedTier: 'marts_dim', expectedRank: 90 },
      { name: 'dimension_accounts', path: 'models/custom/dimension_accounts.sql', meta: {}, tags: [], expectedTier: 'marts_dim', expectedRank: 90 },
      { name: 'tagged_dim', path: 'models/warehouse/tagged_dim.sql', meta: {}, tags: ['dim'], expectedTier: 'marts_dim', expectedRank: 90 },

      // Marts / Reports (85)
      { name: 'rpt_executive_summary', path: 'models/marts/reports/rpt_executive_summary.sql', meta: {}, tags: [], expectedTier: 'marts_report', expectedRank: 85 },
      { name: 'agg_monthly_sales', path: 'models/marts/agg_monthly_sales.sql', meta: {}, tags: [], expectedTier: 'marts_report', expectedRank: 85 },
      { name: 'kpi_user_growth', path: 'models/kpi/kpi_user_growth.sql', meta: {}, tags: [], expectedTier: 'marts_report', expectedRank: 85 },
      { name: 'report_churn_rate', path: 'models/report_churn_rate.sql', meta: {}, tags: [], expectedTier: 'marts_report', expectedRank: 85 },
      { name: 'summary_pipeline', path: 'models/marts/summary_pipeline.sql', meta: {}, tags: [], expectedTier: 'marts_report', expectedRank: 85 },
      { name: 'tagged_report', path: 'models/analytics/tagged_report.sql', meta: {}, tags: ['report'], expectedTier: 'marts_report', expectedRank: 85 },

      // Intermediate (50)
      { name: 'int_orders_pivoted', path: 'models/intermediate/finance/int_orders_pivoted.sql', meta: {}, tags: [], expectedTier: 'intermediate', expectedRank: 50 },
      { name: 'intermediate_charges', path: 'models/int/intermediate_charges.sql', meta: {}, tags: [], expectedTier: 'intermediate', expectedRank: 50 },
      { name: 'tagged_int', path: 'models/custom/tagged_int.sql', meta: {}, tags: ['intermediate'], expectedTier: 'intermediate', expectedRank: 50 },

      // Snapshot (25)
      { name: 'snap_inventory', path: 'snapshots/snap_inventory.sql', meta: {}, tags: [], expectedTier: 'snapshot', expectedRank: 25 },
      { name: 'snapshot_customers', path: 'snapshots/scd/snapshot_customers.sql', meta: {}, tags: [], expectedTier: 'snapshot', expectedRank: 25 },
      { name: 'tagged_snap', path: 'models/custom/tagged_snap.sql', meta: {}, tags: ['snapshot'], expectedTier: 'snapshot', expectedRank: 25 },

      // Staging (20)
      { name: 'stg_stripe__charges', path: 'models/staging/stripe/stg_stripe__charges.sql', meta: {}, tags: [], expectedTier: 'staging', expectedRank: 20 },
      { name: 'stage_salesforce_leads', path: 'models/stg/salesforce/stage_salesforce_leads.sql', meta: {}, tags: [], expectedTier: 'staging', expectedRank: 20 },
      { name: 'tagged_stage', path: 'models/custom/tagged_stage.sql', meta: {}, tags: ['staging'], expectedTier: 'staging', expectedRank: 20 },

      // Seed (15)
      { name: 'seed_country_codes', path: 'seeds/seed_country_codes.csv', meta: {}, tags: [], expectedTier: 'seed', expectedRank: 15 },
      { name: 'lookup_currency_rates', path: 'seeds/lookup_currency_rates.csv', meta: {}, tags: [], expectedTier: 'seed', expectedRank: 15 },
      { name: 'ref_zip_codes', path: 'seeds/ref_zip_codes.csv', meta: {}, tags: [], expectedTier: 'seed', expectedRank: 15 },
      { name: 'tagged_seed', path: 'data/custom.csv', meta: {}, tags: ['seed'], expectedTier: 'seed', expectedRank: 15 },

      // Source (15)
      { name: 'src_segment_events', path: 'models/sources/src_segment_events.yml', meta: {}, tags: [], expectedTier: 'source', expectedRank: 15 },
      { name: 'source_raw_logs', path: 'models/sources/source_raw_logs.yml', meta: {}, tags: [], expectedTier: 'source', expectedRank: 15 },
      { name: 'tagged_src', path: 'models/custom/tagged_src.yml', meta: {}, tags: ['source'], expectedTier: 'source', expectedRank: 15 },

      // Raw / Fallback (10)
      { name: 'raw_events_stream', path: 'models/raw/raw_events_stream.sql', meta: {}, tags: [], expectedTier: 'raw', expectedRank: 10 },
      { name: 'base_postgres_users', path: 'models/base_postgres_users.sql', meta: {}, tags: [], expectedTier: 'raw', expectedRank: 10 },
      { name: 'unrecognized_adhoc_model', path: 'models/adhoc/unrecognized_adhoc_model.sql', meta: {}, tags: [], expectedTier: 'raw', expectedRank: 10 },

      // Precedence: explicit meta override beats everything
      { name: 'stg_special_override', path: 'models/staging/stg_special_override.sql', meta: { tier: 'marts_fact' }, tags: ['staging'], expectedTier: 'marts_fact', expectedRank: 100 },
      { name: 'raw_table_override', path: 'models/raw/raw_table.sql', meta: { metabase: { tier: 'marts_dim' } }, tags: [], expectedTier: 'marts_dim', expectedRank: 90 },
      { name: 'int_table_override', path: 'models/intermediate/int_table.sql', meta: { lightdash: { tier: 'marts_report' } }, tags: [], expectedTier: 'marts_report', expectedRank: 85 },
    ];

    test.each(testCases)(
      'CHAOS-2.X: classifies $name in "$path" as $expectedTier (rank $expectedRank)',
      ({ name, path: modelPath, meta, tags, expectedTier, expectedRank }) => {
        const result = parser.classifyTier(name, modelPath, meta, tags);
        expect(result.tier).toBe(expectedTier);
        expect(result.rank).toBe(expectedRank);
      }
    );
  });

  // =========================================================================
  // Section 3: MCP Handler Output Contract & Provenance Verification
  // =========================================================================
  describe('Section 3: MCP Handler Output Contract & Provenance', () => {
    test('CHAOS-3.1: DbtSemanticHandler returns dual-format response conforming strictly to MCP contract', async () => {
      const modelsDir = path.join(tempDir, 'models', 'marts');
      fs.mkdirSync(modelsDir, { recursive: true });
      fs.writeFileSync(path.join(modelsDir, 'fct_revenue.sql'), 'SELECT 1 as amount');

      const handler = new DbtSemanticHandler();
      const res = await handler.handleDbtProjectScanDeep({
        project_dir: tempDir,
      });

      expect(res.isError).toBeUndefined();
      expect(Array.isArray(res.content)).toBe(true);
      expect(res.content[0].type).toBe('text');
      expect(typeof res.content[0].text).toBe('string');
      expect(res.content[0].text).toContain('[dbt DEEP PROJECT SCAN OVERVIEW]');

      const structured = res.structuredContent;
      expect(structured).toBeDefined();
      expect(structured.project_dir).toBe(tempDir);
      expect(structured.model_count).toBe(1);
      expect(structured.models_by_tier).toEqual({
        marts_fact: 1,
        marts_dim: 0,
        marts_report: 0,
        intermediate: 0,
        snapshot: 0,
        staging: 0,
        seed: 0,
        source: 0,
        raw: 0,
      });

      // Strict check of _provenance
      expect(structured._provenance).toBeDefined();
      expect(structured._provenance.governance_level).toBe('READ_ONLY_INSPECTION');
      expect(structured._provenance.scanner).toBe('DbtDeepScanner');
      expect(typeof structured._provenance.timestamp).toBe('string');
      expect(structured._provenance.manifest_loaded).toBe(false);
      expect(structured._provenance.catalog_loaded).toBe(false);
    });

    test('CHAOS-3.2: verifies tool registry metadata compliance and readOnlyHint', () => {
      const tools = getToolDefinitions();
      const scanTool = tools.find(t => t.name === 'dbt_project_scan_deep');

      expect(scanTool).toBeDefined();
      expect(scanTool.annotations.readOnlyHint).toBe(true);
      expect(scanTool.annotations.destructiveHint).toBe(false);

      const meta = TOOL_METADATA.dbt_project_scan_deep;
      expect(meta).toBeDefined();
      expect(meta.outputSchema.properties._provenance).toBeDefined();
      expect(meta.outputSchema.properties.models_by_tier).toBeDefined();
      expect(meta.outputSchema.properties.models).toBeDefined();
    });

    test('CHAOS-3.3: handles selective tier filtering correctly', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(path.join(modelsDir, 'marts'), { recursive: true });
      fs.mkdirSync(path.join(modelsDir, 'staging'), { recursive: true });
      fs.mkdirSync(path.join(modelsDir, 'intermediate'), { recursive: true });

      fs.writeFileSync(path.join(modelsDir, 'marts', 'fct_orders.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'marts', 'dim_users.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'staging', 'stg_orders.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'intermediate', 'int_orders.sql'), 'SELECT 1');

      const handler = new DbtSemanticHandler();

      // Filter single tier via tier_filter
      const res1 = await handler.handleDbtProjectScanDeep({
        project_dir: tempDir,
        tier_filter: 'marts_fact',
      });
      expect(res1.structuredContent.model_count).toBe(1);
      expect(res1.structuredContent.models[0].name).toBe('fct_orders');
      expect(res1.structuredContent.modelsByTier.marts_fact).toBe(1);
      expect(res1.structuredContent.modelsByTier.staging).toBe(1);

      // Filter multiple tiers via filter_tiers
      const res2 = await handler.handleDbtProjectScanDeep({
        project_dir: tempDir,
        filter_tiers: ['marts_fact', 'marts_dim'],
      });
      expect(res2.structuredContent.model_count).toBe(2);
      const names = res2.structuredContent.models.map(m => m.name).sort();
      expect(names).toEqual(['dim_users', 'fct_orders']);
    });
  });

  // =========================================================================
  // Section 4: Complex Synthetic dbt Projects & Extreme Edge Cases
  // =========================================================================
  describe('Section 4: Synthetic Enterprise Project & Chaos Resilience', () => {
    test('CHAOS-4.1: scans a full synthetic dbt enterprise project with all artifacts', async () => {
      // 1. dbt_project.yml
      fs.writeFileSync(
        path.join(tempDir, 'dbt_project.yml'),
        `name: "enterprise_bi_demo"\nversion: "1.0.0"\nprofile: "default"`
      );

      // 2. docs/*.md
      const docsDir = path.join(tempDir, 'docs');
      fs.mkdirSync(docsDir, { recursive: true });
      fs.writeFileSync(
        path.join(docsDir, 'glossary.md'),
        `{% docs order_total_doc %}
Total dollar amount paid by customer including tax and discounts.
{% enddocs %}
{% docs customer_tier_doc %}
Customer VIP tier based on 12-month trailing spend.
{% enddocs %}`
      );

      // 3. models & schema.yml
      const martsDir = path.join(tempDir, 'models', 'marts', 'finance');
      fs.mkdirSync(martsDir, { recursive: true });

      fs.writeFileSync(
        path.join(martsDir, 'fct_orders.sql'),
        `
        {{ config(materialized='table') }}
        SELECT
          o.order_id,
          o.customer_id,
          o.total_amount
        FROM {{ ref('stg_orders') }} o
        JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
        `
      );

      fs.writeFileSync(
        path.join(martsDir, 'fct_orders.yml'),
        `
version: 2
models:
  - name: fct_orders
    description: "Core sales orders fact table"
    columns:
      - name: order_id
        data_type: integer
        tests:
          - unique
          - not_null
      - name: customer_id
        data_type: integer
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: total_amount
        description: "{{ doc('order_total_doc') }}"
        data_type: numeric
        meta:
          metabase:
            formatting:
              number_style: currency
              currency: USD
`
      );

      const dimDir = path.join(tempDir, 'models', 'marts', 'core');
      fs.mkdirSync(dimDir, { recursive: true });
      fs.writeFileSync(path.join(dimDir, 'dim_customers.sql'), 'SELECT 1 as customer_id');

      // 4. Staging
      const stgDir = path.join(tempDir, 'models', 'staging', 'ecommerce');
      fs.mkdirSync(stgDir, { recursive: true });
      fs.writeFileSync(
        path.join(stgDir, 'stg_orders.sql'),
        `SELECT * FROM {{ source('shopify', 'raw_orders') }}`
      );
      fs.writeFileSync(
        path.join(stgDir, 'sources.yml'),
        `
version: 2
sources:
  - name: shopify
    tables:
      - name: raw_orders
        columns:
          - name: id
            data_type: string
`
      );

      // 5. Exposures
      const expDir = path.join(tempDir, 'models', 'exposures');
      fs.mkdirSync(expDir, { recursive: true });
      fs.writeFileSync(
        path.join(expDir, 'dashboard.yml'),
        `
version: 2
exposures:
  - name: ceo_daily_pulse
    type: dashboard
    depends_on:
      - ref('fct_orders')
`
      );

      // 6. Macros
      const macroDir = path.join(tempDir, 'macros');
      fs.mkdirSync(macroDir, { recursive: true });
      fs.writeFileSync(
        path.join(macroDir, 'cents_to_dollars.sql'),
        `{% macro cents_to_dollars(column_name, scale=2) %}
          ROUND({{ column_name }} / 100.0, {{ scale }})
        {% endmacro %}`
      );

      // 7. Catalog
      const targetDir = path.join(tempDir, 'target');
      fs.mkdirSync(targetDir, { recursive: true });
      const catalogData = {
        metadata: { dbt_version: '1.8.2', generated_at: '2026-09-01T00:00:00Z' },
        nodes: {
          'model.enterprise_bi_demo.fct_orders': {
            metadata: { name: 'fct_orders', schema: 'analytics', type: 'table' },
            stats: {
              row_count: { value: 2500000 },
              bytes: { value: 524288000 }, // 500 MB
            },
            columns: {
              order_id: { type: 'INT64', index: 1, null_count: 0, distinct_count: 2500000 },
              total_amount: { type: 'NUMERIC(12,2)', index: 2, null_count: 5 },
            },
          },
        },
        sources: {
          'source.enterprise_bi_demo.shopify.raw_orders': {
            metadata: { name: 'raw_orders' },
            stats: { row_count: { value: 3000000 }, bytes: { value: 629145600 } },
          },
        },
      };
      fs.writeFileSync(path.join(targetDir, 'catalog.json'), JSON.stringify(catalogData));

      // Scan project
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.projectName).toBe('enterprise_bi_demo');
      expect(result.modelCount).toBe(3); // fct_orders, dim_customers, stg_orders
      expect(result.sourceCount).toBe(1);
      expect(result.exposureCount).toBe(1);
      expect(result.summary.macroCount).toBe(1);
      expect(result.macros).toHaveLength(1);
      expect(result.docBlockCount).toBe(2);
      expect(result.catalogLoaded).toBe(true);

      const fctOrders = result.models.find(m => m.name === 'fct_orders');
      expect(fctOrders.tier).toBe('marts_fact');
      expect(fctOrders.tierRank).toBe(100);
      expect(fctOrders.stats.rowCount).toBe(2500000);
      expect(fctOrders.stats.formattedSize).toBe('500.0 MB');
      expect(fctOrders.columns.total_amount.description).toContain('Total dollar amount paid');
      expect(fctOrders.columns.total_amount.physicalDataType).toBe('NUMERIC(12,2)');
      expect(fctOrders.columns.total_amount.visualMeta.formatting.formatType).toBe('currency');

      expect(result.relationships).toHaveLength(1);
      expect(result.relationships[0].fromModel).toBe('fct_orders');
      expect(result.relationships[0].toModel).toBe('dim_customers');
    });

    test('CHAOS-4.2: handles complex circular doc reference chains without stack overflow', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.docBlocks.set('doc_1', 'References {{ doc("doc_2") }}');
      scanner.docBlocks.set('doc_2', 'References {{ doc("doc_3") }}');
      scanner.docBlocks.set('doc_3', 'References {{ doc("doc_1") }}');

      const resolved = scanner.resolveDocReference('Root: {{ doc("doc_1") }}');
      expect(typeof resolved).toBe('string');
      expect(resolved.length).toBeGreaterThan(0);
    });

    test('CHAOS-4.3: parses unquoted Jinja doc references in YAML without failing YAML parser', () => {
      const schemaDir = path.join(tempDir, 'models');
      fs.mkdirSync(schemaDir, { recursive: true });

      const unquotedYaml = `
version: 2
models:
  - name: fct_sales
    description: {{ doc('sales_summary') }}
    columns:
      - name: id
        description: {{ doc('id_col') }}
`;
      fs.writeFileSync(path.join(schemaDir, 'sales.yml'), unquotedYaml);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.docBlocks.set('sales_summary', 'Cleaned sales transactions');
      scanner.docBlocks.set('id_col', 'Primary key');

      scanner.parseYamlFile(path.join(schemaDir, 'sales.yml'));
      expect(scanner.models.has('fct_sales')).toBe(true);
      expect(scanner.models.get('fct_sales').columns.id).toBeDefined();
    });

    test('CHAOS-4.4: handles corrupted catalog.json gracefully with warning', () => {
      const catPath = path.join(tempDir, 'corrupted_catalog.json');
      fs.writeFileSync(catPath, '{ "metadata": "incomplete JSON');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const stats = scanner.parseCatalog(catPath);

      expect(stats.catalogLoaded).toBe(false);
      expect(stats.errors.length).toBeGreaterThan(0);
      expect(scanner.warnings.length).toBeGreaterThan(0);
    });

    test('CHAOS-4.5: parses SQL containing multiline, upper-case, and multiple ref/source calls', () => {
      const sqlContent = `
        SELECT
          a.*,
          b.*,
          c.*
        FROM {{
          ref(
            'stg_orders'
          )
        }} a
        JOIN {{ REF("dim_customers") }} b ON a.cust_id = b.id
        LEFT JOIN {{ source('crm_feed', 'raw_contacts') }} c ON 1=1
        CROSS JOIN {{ ref('seed_countries') }}
      `;

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const deps = scanner.extractSqlDependencies(sqlContent);

      expect(deps).toContain('stg_orders');
      expect(deps).toContain('dim_customers');
      expect(deps).toContain('crm_feed.raw_contacts');
      expect(deps).toContain('seed_countries');
    });
  });
});
