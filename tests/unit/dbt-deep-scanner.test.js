import fs from 'fs';
import path from 'path';
import os from 'os';
import { DbtDeepScanner, DOC_BLOCK_REGEX, DOC_REF_REGEX } from '../../src/dbt/dbt-deep-scanner.js';
import { DBT_TIERS, DbtParser } from '../../src/dbt/dbt-parser.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';

describe('DbtDeepScanner & dbt Semantic Handler Unit Tests', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-scanner-test-'));
  });

  afterEach(() => {
    if (tempDir && fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  // =========================================================================
  // Group 1: Recursive AST Traversal & Model Scanning
  // =========================================================================
  describe('Group 1: Recursive AST Traversal & Model Scanning', () => {
    test('TC-1.1: scans nested dbt models directory and indexes SQL files with dependencies', async () => {
      const modelsDir = path.join(tempDir, 'models', 'marts', 'core');
      fs.mkdirSync(modelsDir, { recursive: true });

      fs.writeFileSync(
        path.join(modelsDir, 'fct_orders.sql'),
        `SELECT * FROM {{ ref('stg_orders') }} JOIN {{ source('raw_feed', 'customers') }} ON 1=1`
      );

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.modelCount).toBe(1);
      const model = result.models.find(m => m.name === 'fct_orders');
      expect(model).toBeDefined();
      expect(model.tier).toBe('marts_fact');
      expect(model.tierRank).toBe(100);
      expect(model.dependsOn).toContain('stg_orders');
      expect(model.dependsOn).toContain('raw_feed.customers');
    });

    test('TC-1.2: parses schema.yml model definitions with columns and descriptions', async () => {
      const stagingDir = path.join(tempDir, 'models', 'staging');
      fs.mkdirSync(stagingDir, { recursive: true });

      const schemaYaml = `
version: 2
models:
  - name: stg_orders
    description: "Cleaned raw orders table"
    columns:
      - name: order_id
        description: "Primary key for orders"
        data_type: integer
      - name: amount
        description: "Transaction amount in USD"
        data_type: numeric
`;
      fs.writeFileSync(path.join(stagingDir, 'schema.yml'), schemaYaml);
      fs.writeFileSync(path.join(stagingDir, 'stg_orders.sql'), 'SELECT 1 as order_id, 100.0 as amount');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.modelCount).toBe(1);
      const model = result.models.find(m => m.name === 'stg_orders');
      expect(model).toBeDefined();
      expect(model.tier).toBe('staging');
      expect(model.tierRank).toBe(20);
      expect(model.columns.order_id.description).toBe('Primary key for orders');
      expect(model.columns.amount.dataType).toBe('numeric');
    });

    test('TC-1.3: parses sources.yml definitions with tables and columns', async () => {
      const srcDir = path.join(tempDir, 'models', 'staging');
      fs.mkdirSync(srcDir, { recursive: true });

      const sourceYaml = `
version: 2
sources:
  - name: stripe
    description: "Stripe payment gateway export"
    tables:
      - name: charges
        description: "Raw stripe charges"
        columns:
          - name: id
            data_type: string
          - name: amount_cents
            data_type: integer
`;
      fs.writeFileSync(path.join(srcDir, 'sources.yml'), sourceYaml);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.sourceCount).toBe(1);
      const source = result.sources.find(s => s.sourceName === 'stripe' && s.tableName === 'charges');
      expect(source).toBeDefined();
      expect(source.tier).toBe('source');
      expect(source.tierRank).toBe(15);
      expect(source.columns.id.dataType).toBe('string');
    });

    test('TC-1.4: parses exposures.yml definitions with owner and depends_on', async () => {
      const expDir = path.join(tempDir, 'models', 'exposures');
      fs.mkdirSync(expDir, { recursive: true });

      const exposureYaml = `
version: 2
exposures:
  - name: executive_revenue_dashboard
    type: dashboard
    description: "Core C-level revenue performance dashboard"
    owner:
      name: "Finance Analytics Team"
      email: "finance@company.com"
    depends_on:
      - ref('fct_orders')
      - ref('dim_customers')
`;
      fs.writeFileSync(path.join(expDir, 'exposures.yml'), exposureYaml);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.exposureCount).toBe(1);
      const exp = result.exposures.find(e => e.name === 'executive_revenue_dashboard');
      expect(exp).toBeDefined();
      expect(exp.type).toBe('dashboard');
      expect(exp.dependsOn).toEqual(['fct_orders', 'dim_customers']);
      expect(exp.owner.name).toBe('Finance Analytics Team');
    });

    test('TC-1.5: extracts relationship tests into graph foreign key linkages', async () => {
      const martsDir = path.join(tempDir, 'models', 'marts');
      fs.mkdirSync(martsDir, { recursive: true });

      const schemaYaml = `
version: 2
models:
  - name: fct_orders
    columns:
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
`;
      fs.writeFileSync(path.join(martsDir, 'fct_orders.yml'), schemaYaml);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.relationshipCount).toBe(1);
      const rel = result.relationships[0];
      expect(rel.fromModel).toBe('fct_orders');
      expect(rel.fromColumn).toBe('customer_id');
      expect(rel.toModel).toBe('dim_customers');
      expect(rel.toColumn).toBe('customer_id');
      expect(rel.source).toBe('dbt_test');
    });

    test('TC-1.6: parses seeds (.csv) and snapshots ({% snapshot %})', async () => {
      const seedsDir = path.join(tempDir, 'seeds');
      const snapsDir = path.join(tempDir, 'snapshots');
      fs.mkdirSync(seedsDir, { recursive: true });
      fs.mkdirSync(snapsDir, { recursive: true });

      fs.writeFileSync(path.join(seedsDir, 'country_codes.csv'), 'code,name\nUS,United States\nTR,Turkey');
      fs.writeFileSync(
        path.join(snapsDir, 'orders_snapshot.sql'),
        `{% snapshot orders_snapshot %}
          {{ config(target_schema='snapshots', unique_key='id', strategy='timestamp', updated_at='updated_at') }}
          select * from {{ source('raw', 'orders') }}
        {% endsnapshot %}`
      );

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.summary.seedCount).toBe(1);
      expect(result.summary.snapshotCount).toBe(1);
      expect(result.seeds[0].name).toBe('country_codes');
      expect(result.seeds[0].tier).toBe('seed');
      expect(result.snapshots[0].name).toBe('orders_snapshot');
      expect(result.snapshots[0].tier).toBe('snapshot');
    });
  });

  // =========================================================================
  // Group 2: Architectural Tier Ranking & Classification
  // =========================================================================
  describe('Group 2: Architectural Tier Ranking & Classification', () => {
    test('TC-2.1: classifies marts_fact (rank 100) for fct_ prefix and /marts/ path', () => {
      const parser = new DbtParser();
      const tier1 = parser.classifyTier('fct_orders', 'models/marts/core/fct_orders.sql');
      const tier2 = parser.classifyTier('fact_daily_revenue', 'models/revenue/fact_daily_revenue.sql');

      expect(tier1.tier).toBe('marts_fact');
      expect(tier1.rank).toBe(100);
      expect(tier2.tier).toBe('marts_fact');
      expect(tier2.rank).toBe(100);
    });

    test('TC-2.2: classifies marts_dim (rank 90) for dim_ prefix and /marts/ path', () => {
      const parser = new DbtParser();
      const tier1 = parser.classifyTier('dim_customers', 'models/marts/dim_customers.sql');
      const tier2 = parser.classifyTier('dimension_organizations', 'models/entities/dimension_organizations.sql');

      expect(tier1.tier).toBe('marts_dim');
      expect(tier1.rank).toBe(90);
      expect(tier2.tier).toBe('marts_dim');
      expect(tier2.rank).toBe(90);
    });

    test('TC-2.3: classifies marts_report (rank 85) for rpt_ / agg_ / kpi_ prefixes', () => {
      const parser = new DbtParser();
      const tier1 = parser.classifyTier('rpt_monthly_kpi', 'models/marts/reports/rpt_monthly_kpi.sql');
      const tier2 = parser.classifyTier('agg_user_churn', 'models/marts/agg_user_churn.sql');
      const tier3 = parser.classifyTier('kpi_active_subscriptions', 'models/kpi_active_subscriptions.sql');

      expect(tier1.tier).toBe('marts_report');
      expect(tier1.rank).toBe(85);
      expect(tier2.tier).toBe('marts_report');
      expect(tier3.tier).toBe('marts_report');
    });

    test('TC-2.4: classifies intermediate (rank 50) for int_ prefix and /intermediate/ path', () => {
      const parser = new DbtParser();
      const tier1 = parser.classifyTier('int_orders_joined', 'models/intermediate/int_orders_joined.sql');
      const tier2 = parser.classifyTier('intermediate_sessions', 'models/int/intermediate_sessions.sql');

      expect(tier1.tier).toBe('intermediate');
      expect(tier1.rank).toBe(50);
      expect(tier2.tier).toBe('intermediate');
    });

    test('TC-2.5: classifies snapshot (rank 25) for snap_ prefix and /snapshots/ path', () => {
      const parser = new DbtParser();
      const tier1 = parser.classifyTier('snap_customer_status', 'snapshots/snap_customer_status.sql');
      const tier2 = parser.classifyTier('snapshot_orders', 'snapshots/snapshot_orders.sql');

      expect(tier1.tier).toBe('snapshot');
      expect(tier1.rank).toBe(25);
      expect(tier2.tier).toBe('snapshot');
    });

    test('TC-2.6: classifies staging (rank 20) for stg_ prefix and /staging/ path', () => {
      const parser = new DbtParser();
      const tier1 = parser.classifyTier('stg_stripe_charges', 'models/staging/stripe/stg_stripe_charges.sql');
      const tier2 = parser.classifyTier('stage_users', 'models/staging/stage_users.sql');

      expect(tier1.tier).toBe('staging');
      expect(tier1.rank).toBe(20);
      expect(tier2.tier).toBe('staging');
    });

    test('TC-2.7: classifies raw (rank 10) for unclassified models', () => {
      const parser = new DbtParser();
      const tier1 = parser.classifyTier('raw_event_stream', 'models/raw_event_stream.sql');
      const tier2 = parser.classifyTier('base_unstructured_data', 'models/base_unstructured_data.sql');
      const tier3 = parser.classifyTier('scratch_query', 'models/scratch_query.sql');

      expect(tier1.tier).toBe('raw');
      expect(tier1.rank).toBe(10);
      expect(tier2.tier).toBe('raw');
      expect(tier3.tier).toBe('raw');
    });

    test('TC-2.8: respects explicit meta.tier override over path and prefix heuristics', () => {
      const parser = new DbtParser();
      const tier = parser.classifyTier(
        'stg_special_view',
        'models/staging/stg_special_view.sql',
        { tier: 'marts_fact' },
        []
      );

      expect(tier.tier).toBe('marts_fact');
      expect(tier.rank).toBe(100);
    });

    test('TC-2.9: respects model tags heuristics', () => {
      const parser = new DbtParser();
      const tier = parser.classifyTier(
        'custom_sales_view',
        'models/analytics/custom_sales_view.sql',
        {},
        ['fact', 'daily']
      );

      expect(tier.tier).toBe('marts_fact');
      expect(tier.rank).toBe(100);
    });

    test('TC-2.10: computes accurate modelsByTier breakdown across all tiers', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(path.join(modelsDir, 'marts'), { recursive: true });
      fs.mkdirSync(path.join(modelsDir, 'intermediate'), { recursive: true });
      fs.mkdirSync(path.join(modelsDir, 'staging'), { recursive: true });

      fs.writeFileSync(path.join(modelsDir, 'marts', 'fct_orders.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'marts', 'dim_users.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'marts', 'rpt_sales.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'intermediate', 'int_orders.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'staging', 'stg_orders.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'raw_logs.sql'), 'SELECT 1');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.modelsByTier.marts_fact).toBe(1);
      expect(result.modelsByTier.marts_dim).toBe(1);
      expect(result.modelsByTier.marts_report).toBe(1);
      expect(result.modelsByTier.intermediate).toBe(1);
      expect(result.modelsByTier.staging).toBe(1);
      expect(result.modelsByTier.raw).toBe(1);
    });
  });

  // =========================================================================
  // Group 3: dbt Docs Block Parsing & Resolution
  // =========================================================================
  describe('Group 3: dbt Docs Block Parsing & Resolution', () => {
    test('TC-3.1: parses {% docs doc_name %} blocks from docs/*.md files', () => {
      const docsDir = path.join(tempDir, 'docs');
      fs.mkdirSync(docsDir, { recursive: true });

      const docContent = `
{% docs order_id_description %}
The unique primary key identifying a customer transaction in the ERP.
{% enddocs %}

{% docs customer_status %}
Active or Churned customer lifecycle state.
{% enddocs %}
`;
      fs.writeFileSync(path.join(docsDir, 'overview.md'), docContent);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const docBlocks = scanner.parseDocBlocks(tempDir);

      expect(docBlocks.size).toBe(2);
      expect(docBlocks.get('order_id_description')).toBe(
        'The unique primary key identifying a customer transaction in the ERP.'
      );
      expect(docBlocks.get('customer_status')).toBe(
        'Active or Churned customer lifecycle state.'
      );
    });

    test('TC-3.2: resolves {{ doc("doc_name") }} and doc("doc_name") in model and column descriptions', async () => {
      const docsDir = path.join(tempDir, 'docs');
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(docsDir, { recursive: true });
      fs.mkdirSync(modelsDir, { recursive: true });

      fs.writeFileSync(
        path.join(docsDir, 'docs.md'),
        `{% docs doc_table %}Core orders table{% enddocs %}\n{% docs doc_col %}Unique order identifier{% enddocs %}`
      );

      const schemaYaml = `
version: 2
models:
  - name: fct_orders
    description: "{{ doc('doc_table') }}"
    columns:
      - name: order_id
        description: "{{ doc('doc_col') }}"
`;
      fs.writeFileSync(path.join(modelsDir, 'schema.yml'), schemaYaml);
      fs.writeFileSync(path.join(modelsDir, 'fct_orders.sql'), 'SELECT 1');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      const model = result.models.find(m => m.name === 'fct_orders');
      expect(model.description).toBe('Core orders table');
      expect(model.columns.order_id.description).toBe('Unique order identifier');
    });

    test('TC-3.3: handles package-scoped doc references {{ doc("my_pkg", "doc_name") }}', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.docBlocks.set('dbt_utils.surrogate_key_doc', 'Generates hashed surrogate key.');
      scanner.docBlocks.set('global_status', 'Universal status code.');

      const text1 = 'Field description: {{ doc("dbt_utils", "surrogate_key_doc") }}';
      const text2 = 'Field description: {{ doc("global_status") }}';

      expect(scanner.resolveDocReference(text1)).toBe('Field description: Generates hashed surrogate key.');
      expect(scanner.resolveDocReference(text2)).toBe('Field description: Universal status code.');
    });

    test('TC-3.4: handles multi-line markdown with code fences and lists in doc blocks', () => {
      const docsDir = path.join(tempDir, 'docs');
      fs.mkdirSync(docsDir, { recursive: true });

      const multiLineDoc = `
{% docs complex_metric_doc %}
### Metric Definition
Calculates gross margin using the formula:
\`\`\`sql
(revenue - cogs) / revenue
\`\`\`
- Minimum value: 0.0
- Target threshold: > 0.45
{% enddocs %}
`;
      fs.writeFileSync(path.join(docsDir, 'metrics.md'), multiLineDoc);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.parseDocBlocks(tempDir);

      const resolved = scanner.resolveDocReference('Overview: {{ doc("complex_metric_doc") }}');
      expect(resolved).toContain('### Metric Definition');
      expect(resolved).toContain('(revenue - cogs) / revenue');
      expect(resolved).toContain('Target threshold: > 0.45');
    });

    test('TC-3.5: bounded recursion prevents infinite loops on circular doc block references', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.docBlocks.set('doc_a', 'Includes {{ doc("doc_b") }}');
      scanner.docBlocks.set('doc_b', 'Includes {{ doc("doc_a") }}');

      // Should not throw stack overflow; terminates safely
      const resolved = scanner.resolveDocReference('{{ doc("doc_a") }}');
      expect(typeof resolved).toBe('string');
      expect(resolved.length).toBeGreaterThan(0);
    });

    test('TC-3.6: gracefully handles missing doc block reference without throwing exceptions', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const text = 'Column is {{ doc("non_existent_doc_key") }}';

      const resolved = scanner.resolveDocReference(text);
      expect(resolved).toBe(text);
    });
  });

  // =========================================================================
  // Group 4: Catalog Stats & MetricFlow Semantic Layer Ingestion
  // =========================================================================
  describe('Group 4: Catalog Stats & MetricFlow Semantic Layer Ingestion', () => {
    test('TC-4.1: ingests table row counts and byte sizes from catalog.json across dialects', () => {
      const catalogPath = path.join(tempDir, 'catalog.json');
      const catalogData = {
        metadata: { dbt_version: '1.8.0', generated_at: '2026-09-01T00:00:00Z' },
        nodes: {
          'model.my_proj.fct_orders': {
            unique_id: 'model.my_proj.fct_orders',
            metadata: { name: 'fct_orders', schema: 'analytics', database: 'prod_db', type: 'table', owner: 'data_team' },
            stats: {
              row_count: { value: '1,500,000' },
              num_bytes: { value: 104857600 },
              partitioning_type: { value: 'DAY' },
              clustering_fields: { value: 'customer_id,status' },
            },
            columns: {
              order_id: { type: 'BIGINT', index: 1, comment: 'Primary key' },
              amount: { type: 'NUMERIC(18,2)', index: 2, comment: 'Total amount' },
            },
          },
        },
        sources: {},
      };

      fs.writeFileSync(catalogPath, JSON.stringify(catalogData));

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const catalog = scanner.parseCatalog(catalogPath);

      expect(catalog.catalogLoaded).toBe(true);
      expect(catalog.totalRows).toBe(1500000);
      expect(catalog.totalBytes).toBe(104857600);
      expect(catalog.formattedTotalBytes).toBe('100.0 MB');
      expect(catalog.tables.fct_orders.isPartitioned).toBe(true);
      expect(catalog.tables.fct_orders.partitionType).toBe('DAY');
      expect(catalog.tables.fct_orders.clusterKeys).toEqual(['customer_id', 'status']);
      expect(catalog.tables.fct_orders.columns.order_id.type).toBe('BIGINT');
    });

    test('TC-4.2: enriches model columns with physical data types & profiling from catalog', async () => {
      const targetDir = path.join(tempDir, 'target');
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(targetDir, { recursive: true });
      fs.mkdirSync(modelsDir, { recursive: true });

      fs.writeFileSync(
        path.join(modelsDir, 'fct_orders.yml'),
        `version: 2\nmodels:\n  - name: fct_orders\n    columns:\n      - name: order_id\n        data_type: unknown`
      );

      const catalogData = {
        metadata: { dbt_version: '1.8.0' },
        nodes: {
          'model.proj.fct_orders': {
            metadata: { name: 'fct_orders' },
            stats: { row_count: { value: 50000 }, bytes: { value: 2048000 } },
            columns: {
              order_id: { type: 'INT64', index: 1, null_count: 0, distinct_count: 50000 },
            },
          },
        },
      };
      fs.writeFileSync(path.join(targetDir, 'catalog.json'), JSON.stringify(catalogData));

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      const model = result.models.find(m => m.name === 'fct_orders');
      expect(model.stats).toBeDefined();
      expect(model.stats.rowCount).toBe(50000);
      expect(model.columns.order_id.dataType).toBe('INT64');
      expect(model.columns.order_id.physicalDataType).toBe('INT64');
      expect(model.columns.order_id.nullCount).toBe(0);
      expect(model.columns.order_id.distinctCount).toBe(50000);
    });

    test('TC-4.3: parses MetricFlow semantic_models with entities, dimensions, and measures', () => {
      const metricflowYaml = `
semantic_models:
  - name: orders_source
    model: ref('fct_orders')
    description: "Semantic model for customer orders"
    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: status
        type: categorical
    measures:
      - name: total_revenue
        description: "Gross total revenue from completed orders"
        agg: sum
        expr: amount
        agg_time_dimension: order_date
`;

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const parsed = scanner.parseMetricFlow(metricflowYaml, 'models/semantic/orders.yml');

      expect(parsed.semanticModels.length).toBe(1);
      const semModel = parsed.semanticModels[0];
      expect(semModel.name).toBe('orders_source');
      expect(semModel.model).toBe('fct_orders');
      expect(semModel.entities).toHaveLength(2);
      expect(semModel.entities[0].type).toBe('primary');
      expect(semModel.dimensions).toHaveLength(2);
      expect(semModel.dimensions[0].type).toBe('time');
      expect(semModel.measures).toHaveLength(1);
      expect(semModel.measures[0].agg).toBe('sum');
      expect(semModel.measures[0].expr).toBe('amount');

      // Auto-created base measure metric
      expect(scanner.metrics.has('orders_source_total_revenue')).toBe(true);
    });

    test('TC-4.4: parses MetricFlow metrics (simple, ratio, cumulative, derived)', () => {
      const metricsYaml = `
metrics:
  - name: simple_orders_count
    label: "Total Orders"
    type: simple
    type_params:
      measure: order_count

  - name: gross_margin_pct
    label: "Gross Margin %"
    type: ratio
    type_params:
      numerator: gross_profit
      denominator: total_revenue

  - name: active_users_30d
    label: "30-Day Active Users"
    type: cumulative
    type_params:
      measure: daily_active_users
      window: 30 days

  - name: net_profit
    label: "Net Profit"
    type: derived
    type_params:
      expr: "revenue - cogs - tax"
      metrics:
        - name: revenue
        - name: cogs
        - name: tax
`;

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const parsed = scanner.parseMetricFlow(metricsYaml, 'models/semantic/metrics.yml');

      expect(parsed.metrics).toHaveLength(4);
      expect(scanner.metrics.get('simple_orders_count').type).toBe('simple');
      expect(scanner.metrics.get('gross_margin_pct').type).toBe('ratio');
      expect(scanner.metrics.get('gross_margin_pct').typeParams.numerator).toBe('gross_profit');
      expect(scanner.metrics.get('active_users_30d').type).toBe('cumulative');
      expect(scanner.metrics.get('active_users_30d').typeParams.window).toBe('30 days');
      expect(scanner.metrics.get('net_profit').type).toBe('derived');
      expect(scanner.metrics.get('net_profit').typeParams.expr).toBe('revenue - cogs - tax');
    });

    test('TC-4.5: normalizes visual metadata from meta.metabase and meta.lightdash', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });

      const rawMetabase = {
        metabase: {
          display_name: 'Revenue (USD)',
          chart_type: 'line',
          color: '#2563EB',
          special_type: 'type/Currency',
          formatting: {
            number_style: 'currency',
            currency: 'USD',
            decimals: 2,
          },
        },
      };

      const rawLightdash = {
        lightdash: {
          label: 'Churn Rate',
          chart_type: 'bar',
          format: 'percent',
          round: 1,
          colors: ['#EF4444', '#10B981'],
          urls: ['https://app.company.com/users/${row.user_id}'],
        },
      };

      const meta1 = scanner.normalizeVisualMetadata(rawMetabase);
      expect(meta1.displayName).toBe('Revenue (USD)');
      expect(meta1.chartType).toBe('line');
      expect(meta1.color).toBe('#2563EB');
      expect(meta1.semanticType).toBe('type/Currency');
      expect(meta1.formatting.formatType).toBe('currency');
      expect(meta1.formatting.currency).toBe('USD');
      expect(meta1.formatting.decimals).toBe(2);

      const meta2 = scanner.normalizeVisualMetadata(rawLightdash);
      expect(meta2.displayName).toBe('Churn Rate');
      expect(meta2.chartType).toBe('bar');
      expect(meta2.semanticType).toBe('type/Percentage');
      expect(meta2.formatting.formatType).toBe('percent');
      expect(meta2.formatting.decimals).toBe(1);
      expect(meta2.colors).toEqual(['#EF4444', '#10B981']);
      expect(meta2.drillUrls).toHaveLength(1);
    });

    test('TC-4.6: synthesizes foreign-to-primary join relationships from MetricFlow entities', async () => {
      const semDir = path.join(tempDir, 'models', 'semantic');
      fs.mkdirSync(semDir, { recursive: true });

      const semYaml = `
semantic_models:
  - name: customers_source
    model: ref('dim_customers')
    entities:
      - name: customer_id
        type: primary

  - name: orders_source
    model: ref('fct_orders')
    entities:
      - name: customer_id
        type: foreign
`;
      fs.writeFileSync(path.join(semDir, 'semantic.yml'), semYaml);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      const entityRel = result.relationships.find(r => r.source === 'metricflow_entity');
      expect(entityRel).toBeDefined();
      expect(entityRel.fromModel).toBe('fct_orders');
      expect(entityRel.fromColumn).toBe('customer_id');
      expect(entityRel.toModel).toBe('dim_customers');
      expect(entityRel.toColumn).toBe('customer_id');
    });
  });

  // =========================================================================
  // Group 5: Error Resilience & MCP Integration
  // =========================================================================
  describe('Group 5: Error Resilience & MCP Integration', () => {
    test('TC-5.1: throws descriptive error when project directory does not exist', async () => {
      const scanner = new DbtDeepScanner({ projectDir: '/non/existent/path/123' });
      await expect(scanner.scanProject('/non/existent/path/123')).rejects.toThrow(
        /dbt project directory not found/
      );
    });

    test('TC-5.2: gracefully ignores malformed YAML files with warnings without halting scan', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });

      fs.writeFileSync(path.join(modelsDir, 'fct_orders.sql'), 'SELECT 1 as id');
      fs.writeFileSync(path.join(modelsDir, 'corrupted_schema.yml'), ':::invalid yaml text {{ unclosed');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.modelCount).toBe(1);
      expect(result.warnings.length).toBeGreaterThan(0);
      expect(result.warnings.some(w => w.includes('corrupted_schema.yml'))).toBe(true);
    });

    test('TC-5.3: handles missing catalog.json gracefully with AST fallback', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });
      fs.writeFileSync(path.join(modelsDir, 'dim_users.sql'), 'SELECT 1 as id');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.summary.catalogStatsLoaded).toBe(false);
      expect(result.summary.totalTableRows).toBe(0);
      expect(result.modelCount).toBe(1);
      expect(result.models[0].name).toBe('dim_users');
    });

    test('TC-5.4: verifies dbt_project_scan_deep is properly registered in tool-registry', () => {
      const tools = getToolDefinitions();
      const deepScanTool = tools.find(t => t.name === 'dbt_project_scan_deep');

      expect(deepScanTool).toBeDefined();
      expect(deepScanTool.title).toBe('Deep dbt Project & Lineage Scanner');
      expect(deepScanTool.annotations.readOnlyHint).toBe(true);
      expect(deepScanTool.inputSchema.properties.project_dir).toBeDefined();
      expect(deepScanTool.inputSchema.properties.catalog_path).toBeDefined();

      const meta = TOOL_METADATA.dbt_project_scan_deep;
      expect(meta).toBeDefined();
      expect(meta.outputSchema).toBeDefined();
      expect(meta.outputSchema.properties.models_by_tier).toBeDefined();
    });

    test('TC-5.5: DbtSemanticHandler.handleDbtProjectScanDeep returns valid structured MCP response', async () => {
      const modelsDir = path.join(tempDir, 'models', 'marts');
      fs.mkdirSync(modelsDir, { recursive: true });
      fs.writeFileSync(path.join(modelsDir, 'fct_orders.sql'), 'SELECT 1 as id');

      const handler = new DbtSemanticHandler();
      const response = await handler.handleDbtProjectScanDeep({ project_dir: tempDir });

      expect(response.isError).toBeUndefined();
      expect(response.content[0].type).toBe('text');
      expect(response.content[0].text).toContain('[dbt DEEP PROJECT SCAN OVERVIEW]');
      expect(response.structuredContent).toBeDefined();
      expect(response.structuredContent.model_count).toBe(1);
      expect(response.structuredContent._provenance.governance_level).toBe('READ_ONLY_INSPECTION');
      expect(response.structuredContent._provenance.scanner).toBe('DbtDeepScanner');
    });

    test('TC-5.6: handleDbtProjectScanDeep filters models by filter_tiers', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(path.join(modelsDir, 'marts'), { recursive: true });
      fs.mkdirSync(path.join(modelsDir, 'staging'), { recursive: true });

      fs.writeFileSync(path.join(modelsDir, 'marts', 'fct_orders.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'staging', 'stg_orders.sql'), 'SELECT 1');

      const handler = new DbtSemanticHandler();
      const response = await handler.handleDbtProjectScanDeep({
        project_dir: tempDir,
        filter_tiers: ['marts_fact'],
      });

      expect(response.structuredContent.model_count).toBe(1);
      expect(response.structuredContent.models[0].name).toBe('fct_orders');
      expect(response.structuredContent.modelsByTier.marts_fact).toBe(1);
      expect(response.structuredContent.modelsByTier.staging).toBe(1);
    });

    test('TC-5.7: handleDbtProjectScanDeep returns error envelope when project path is invalid', async () => {
      const handler = new DbtSemanticHandler();
      const response = await handler.handleDbtProjectScanDeep({
        project_dir: '/invalid/path/that/does/not/exist',
      });

      expect(response.isError).toBe(true);
      expect(response.content[0].text).toContain('❌ dbt Deep Scan Error');
    });
  });
});
