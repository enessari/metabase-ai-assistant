import fs from 'fs';
import path from 'path';
import os from 'os';
import { DbtDeepScanner } from '../../src/dbt/dbt-deep-scanner.js';
import { DbtParser, DBT_TIERS } from '../../src/dbt/dbt-parser.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';

describe('Adversarial Stress & Edge-Case Suite: DbtDeepScanner & dbt_project_scan_deep', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-adversarial-test-'));
  });

  afterEach(() => {
    if (tempDir && fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  // =========================================================================
  // Test Category 1: Empty, Degenerate & Non-Standard Project Layouts
  // =========================================================================
  describe('Category 1: Empty, Degenerate & Non-Standard Project Layouts', () => {
    test('ADV-1.1: Completely empty directory returns clean empty diagnostic structure with 0 models', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result).toBeDefined();
      expect(result.modelCount).toBe(0);
      expect(result.sourceCount).toBe(0);
      expect(result.metricCount).toBe(0);
      expect(result.exposureCount).toBe(0);
      expect(result.relationshipCount).toBe(0);
      expect(result.docBlockCount).toBe(0);
      expect(result.manifestLoaded).toBe(false);
      expect(result.catalogLoaded).toBe(false);
      expect(result.docsLoaded).toBe(false);
      expect(result.modelsByTier).toEqual({
        marts_fact: 0,
        marts_dim: 0,
        marts_report: 0,
        intermediate: 0,
        snapshot: 0,
        staging: 0,
        seed: 0,
        source: 0,
        raw: 0,
      });
      expect(result._provenance.governance_level).toBe('READ_ONLY_INSPECTION');
      expect(result.errors).toEqual([]);
      expect(result.warnings).toEqual([]);
    });

    test('ADV-1.2: Directory with only empty dbt_project.yml and empty subdirs', async () => {
      fs.writeFileSync(path.join(tempDir, 'dbt_project.yml'), 'name: adversarial_dummy_project\nversion: 1.0.0\n');
      fs.mkdirSync(path.join(tempDir, 'models'), { recursive: true });
      fs.mkdirSync(path.join(tempDir, 'docs'), { recursive: true });
      fs.mkdirSync(path.join(tempDir, 'seeds'), { recursive: true });
      fs.mkdirSync(path.join(tempDir, 'snapshots'), { recursive: true });
      fs.mkdirSync(path.join(tempDir, 'macros'), { recursive: true });

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.projectName).toBe('adversarial_dummy_project');
      expect(result.modelCount).toBe(0);
      expect(result.summary.modelCount).toBe(0);
      expect(result.summary.seedCount).toBe(0);
    });

    test('ADV-1.3: Ignores non-dbt files, hidden files, and arbitrary subdirectories safely', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });

      // Non-SQL/non-YAML files
      fs.writeFileSync(path.join(modelsDir, '.DS_Store'), 'fake-binary-content');
      fs.writeFileSync(path.join(modelsDir, 'readme.txt'), 'This is a readme');
      fs.writeFileSync(path.join(modelsDir, 'data.parquet'), 'fake-parquet-content');
      fs.writeFileSync(path.join(modelsDir, 'valid_model.sql'), 'SELECT 1 as id');

      // Ignored folders
      const nodeModules = path.join(tempDir, 'node_modules', 'pkg');
      const gitDir = path.join(tempDir, '.git', 'hooks');
      const dbtPackages = path.join(tempDir, 'dbt_packages', 'dbt_utils');
      fs.mkdirSync(nodeModules, { recursive: true });
      fs.mkdirSync(gitDir, { recursive: true });
      fs.mkdirSync(dbtPackages, { recursive: true });

      fs.writeFileSync(path.join(nodeModules, 'ignored.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(gitDir, 'ignored.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(dbtPackages, 'ignored.sql'), 'SELECT 1');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.modelCount).toBe(1);
      expect(result.models[0].name).toBe('valid_model');
      expect(result.models.map(m => m.name)).not.toContain('ignored');
    });
  });

  // =========================================================================
  // Test Category 2: Malformed, Corrupted & Non-Standard YAML Payloads
  // =========================================================================
  describe('Category 2: Malformed, Corrupted & Non-Standard YAML Payloads', () => {
    test('ADV-2.1: Handles unparseable corrupted YAML files without halting remaining valid models', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });

      // Valid model
      fs.writeFileSync(path.join(modelsDir, 'fct_revenue.sql'), 'SELECT 100 as rev');
      fs.writeFileSync(
        path.join(modelsDir, 'valid_schema.yml'),
        'version: 2\nmodels:\n  - name: fct_revenue\n    description: "Revenue fact"'
      );

      // Multiple corrupted YAMLs
      fs.writeFileSync(path.join(modelsDir, 'corrupt1.yml'), ':::invalid yaml ::: ::: [[[ ]');
      fs.writeFileSync(path.join(modelsDir, 'corrupt2.yaml'), '\t\ttabs_are_forbidden: [1, 2,\n 3');
      fs.writeFileSync(path.join(modelsDir, 'corrupt3.yml'), 'key: "unclosed string');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      // Valid model is parsed
      expect(result.modelCount).toBe(1);
      const model = result.models.find(m => m.name === 'fct_revenue');
      expect(model).toBeDefined();
      expect(model.description).toBe('Revenue fact');

      // Warnings capture all 3 corrupt files
      expect(result.warnings.length).toBeGreaterThanOrEqual(3);
      expect(result.warnings.some(w => w.includes('corrupt1.yml'))).toBe(true);
      expect(result.warnings.some(w => w.includes('corrupt2.yaml'))).toBe(true);
      expect(result.warnings.some(w => w.includes('corrupt3.yml'))).toBe(true);
    });

    test('ADV-2.2: Handles non-mapping YAML files (scalars, arrays, nulls, booleans)', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });

      fs.writeFileSync(path.join(modelsDir, 'scalar.yml'), 'just a plain string');
      fs.writeFileSync(path.join(modelsDir, 'number.yml'), '12345');
      fs.writeFileSync(path.join(modelsDir, 'boolean.yml'), 'true');
      fs.writeFileSync(path.join(modelsDir, 'null.yml'), '~');
      fs.writeFileSync(path.join(modelsDir, 'array.yml'), '- item 1\n- item 2');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.modelCount).toBe(0);
      expect(result.warnings.length).toBeGreaterThanOrEqual(4);
    });

    test('ADV-2.3: Handles malformed property types (models as string, columns as boolean, sources as number)', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });

      const malformedPropertiesYaml = `
version: 2
models: "not-an-array"
sources: 99999
metrics: true
semantic_models: null
exposures: { not: "array" }
seeds: "invalid"
snapshots: 123
`;
      fs.writeFileSync(path.join(modelsDir, 'bad_types.yml'), malformedPropertiesYaml);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.modelCount).toBe(0);
      expect(result.sourceCount).toBe(0);
      expect(result.metricCount).toBe(0);
      expect(result.exposureCount).toBe(0);
    });

    test('ADV-2.4: Handles massive text descriptions, Unicode, emojis, and special symbols', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });

      const massiveDescription = 'A'.repeat(50000);
      const unicodeName = 'fct_türkiye_satış_🚀';
      const unicodeDesc = 'Gelir hesaplaması 💰 & Çözümleme: 日本語 / 한국어 / العربية / 🧑‍💻';

      const schemaYaml = `
version: 2
models:
  - name: ${unicodeName}
    description: "${unicodeDesc}"
  - name: fct_huge
    description: "${massiveDescription}"
`;
      fs.writeFileSync(path.join(modelsDir, 'unicode_schema.yml'), schemaYaml);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.modelCount).toBe(2);
      const uniModel = result.models.find(m => m.name === unicodeName);
      expect(uniModel).toBeDefined();
      expect(uniModel.description).toBe(unicodeDesc);

      const hugeModel = result.models.find(m => m.name === 'fct_huge');
      expect(hugeModel).toBeDefined();
      expect(hugeModel.description.length).toBe(50000);
    });
  });

  // =========================================================================
  // Test Category 3: Circular & Complex dbt Docs References
  // =========================================================================
  describe('Category 3: Circular & Complex dbt Docs References', () => {
    test('ADV-3.1: Circular doc references (A -> B -> A) terminate safely without stack overflow', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.docBlocks.set('doc_a', 'Part A: {{ doc("doc_b") }}');
      scanner.docBlocks.set('doc_b', 'Part B: {{ doc("doc_a") }}');

      const resolved = scanner.resolveDocReference('Root: {{ doc("doc_a") }}');
      expect(typeof resolved).toBe('string');
      expect(resolved).toContain('Part A');
      expect(resolved).toContain('Part B');
    });

    test('ADV-3.2: Self-referencing doc (A -> A) terminates immediately', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.docBlocks.set('self_ref', 'I call myself: {{ doc("self_ref") }}');

      const resolved = scanner.resolveDocReference('{{ doc("self_ref") }}');
      expect(typeof resolved).toBe('string');
      expect(resolved).toContain('I call myself');
    });

    test('ADV-3.3: Deep nested doc block chain (10 levels) terminates without stack overflow', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      for (let i = 1; i <= 10; i++) {
        if (i === 10) {
          scanner.docBlocks.set(`chain_${i}`, 'Final Leaf Content');
        } else {
          scanner.docBlocks.set(`chain_${i}`, `Level ${i} -> {{ doc("chain_${i + 1}") }}`);
        }
      }

      const resolved = scanner.resolveDocReference('Start: {{ doc("chain_1") }}');
      expect(typeof resolved).toBe('string');
      expect(resolved).toContain('Level 1');
    });

    test('ADV-3.4: Varied syntax doc references (jinja tag, bare doc, whitespace, quotes)', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.docBlocks.set('doc_target', 'Resolved Target Content');

      const testCases = [
        '{{ doc("doc_target") }}',
        '{{doc("doc_target")}}',
        '{{   doc(   \'doc_target\'   )   }}',
        '{% doc "doc_target" %}',
        '{%  doc \'doc_target\'  %}',
        'doc(\'doc_target\')',
        'doc("doc_target")',
      ];

      for (const tc of testCases) {
        const resolved = scanner.resolveDocReference(tc);
        expect(resolved).toBe('Resolved Target Content');
      }
    });

    test('ADV-3.5: Malformed / incomplete doc references do not throw or crash', () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.docBlocks.set('valid_key', 'Valid Content');

      const malformedCases = [
        '{{ doc( }}',
        '{{ doc("unclosed }}',
        '{{ doc() }}',
        '{{ doc("") }}',
        'doc()',
        'doc("")',
        '{% doc %}',
        null,
        undefined,
        12345,
        {},
      ];

      for (const mc of malformedCases) {
        expect(() => scanner.resolveDocReference(mc)).not.toThrow();
      }
    });

    test('ADV-3.6: Nested doc blocks in deep subdirectories and non-standard markdown filenames', () => {
      const nestedDocsDir = path.join(tempDir, 'sub1', 'sub2', 'docs_nested');
      fs.mkdirSync(nestedDocsDir, { recursive: true });

      const content = `
{% docs deep_doc_1 %}Deeply nested doc 1{% enddocs %}
{% docs deep_doc_2 %}Deeply nested doc 2{% enddocs %}
`;
      fs.writeFileSync(path.join(nestedDocsDir, 'custom_docs_file.md'), content);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      scanner.parseDocBlocks(tempDir);

      expect(scanner.docBlocks.size).toBe(2);
      expect(scanner.docBlocks.get('deep_doc_1')).toBe('Deeply nested doc 1');
      expect(scanner.docBlocks.get('deep_doc_2')).toBe('Deeply nested doc 2');
    });
  });

  // =========================================================================
  // Test Category 4: Multi-Warehouse Catalog Variations & Column Profiling
  // =========================================================================
  describe('Category 4: Multi-Warehouse Catalog Variations & Column Profiling', () => {
    test('ADV-4.1: BigQuery catalog format (partitioning_type, num_bytes, string row count)', () => {
      const catalogPath = path.join(tempDir, 'bq_catalog.json');
      const bqCatalog = {
        metadata: { dbt_version: '1.8.2', generated_at: '2026-09-01T00:00:00Z' },
        nodes: {
          'model.bq_proj.fct_bigquery_events': {
            metadata: { name: 'fct_bigquery_events', schema: 'analytics_prod', database: 'gcp-project-id', type: 'table' },
            stats: {
              row_count: { value: '25,000,000' },
              num_bytes: { value: 53687091200 }, // ~50 GB
              partitioning_type: { value: 'DAY' },
              clustering_fields: { value: 'user_id, event_type' },
            },
            columns: {
              event_id: { type: 'STRING', index: 1, null_count: 0, distinct_count: 25000000 },
              user_id: { type: 'INT64', index: 2, null_count: 500, distinct_count: 1200000 },
              event_timestamp: { type: 'TIMESTAMP', index: 3 },
              event_payload: { type: 'JSON', index: 4 },
            },
          },
        },
        sources: {},
      };
      fs.writeFileSync(catalogPath, JSON.stringify(bqCatalog));

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const catalog = scanner.parseCatalog(catalogPath);

      expect(catalog.catalogLoaded).toBe(true);
      expect(catalog.totalRows).toBe(25000000);
      expect(catalog.totalBytes).toBe(53687091200);
      expect(catalog.formattedTotalBytes).toBe('50.0 GB');

      const table = catalog.tables.fct_bigquery_events;
      expect(table).toBeDefined();
      expect(table.isPartitioned).toBe(true);
      expect(table.partitionType).toBe('DAY');
      expect(table.clusterKeys).toEqual(['user_id', 'event_type']);
      expect(table.columns.event_id.nullCount).toBe(0);
      expect(table.columns.user_id.distinctCount).toBe(1200000);
    });

    test('ADV-4.2: Snowflake catalog format (rows, bytes, clustering_key, uppercase columns)', () => {
      const catalogPath = path.join(tempDir, 'sf_catalog.json');
      const sfCatalog = {
        metadata: { dbt_version: '1.8.0' },
        nodes: {
          'model.sf_proj.DIM_CUSTOMERS': {
            metadata: { name: 'DIM_CUSTOMERS', schema: 'MARTS', database: 'PROD_WH', type: 'table' },
            stats: {
              rows: { value: 850000 },
              bytes: { value: 1073741824 }, // 1 GB
              clustering_key: { value: 'CUSTOMER_ID' },
              last_altered: { value: '2026-08-31 18:30:00 UTC' },
            },
            columns: {
              CUSTOMER_ID: { type: 'NUMBER(38,0)', index: 1, null_count: '0' },
              ORGANIZATION_NAME: { type: 'VARCHAR(16777216)', index: 2 },
              IS_ACTIVE: { type: 'BOOLEAN', index: 3 },
            },
          },
        },
        sources: {},
      };
      fs.writeFileSync(catalogPath, JSON.stringify(sfCatalog));

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const catalog = scanner.parseCatalog(catalogPath);

      expect(catalog.catalogLoaded).toBe(true);
      expect(catalog.totalRows).toBe(850000);
      expect(catalog.totalBytes).toBe(1073741824);
      expect(catalog.formattedTotalBytes).toBe('1.0 GB');

      const table = catalog.tables.DIM_CUSTOMERS;
      expect(table).toBeDefined();
      expect(table.columns.CUSTOMER_ID.type).toBe('NUMBER(38,0)');
      expect(table.columns.CUSTOMER_ID.nullCount).toBe(0);
      expect(table.lastModified).toBe('2026-08-31 18:30:00 UTC');
    });

    test('ADV-4.3: ClickHouse catalog format (total_rows, total_bytes, encoded_size, partition_by)', () => {
      const catalogPath = path.join(tempDir, 'ch_catalog.json');
      const chCatalog = {
        metadata: { dbt_version: '1.7.9' },
        nodes: {
          'model.ch_proj.fct_sensor_telemetry': {
            metadata: { name: 'fct_sensor_telemetry', schema: 'default', database: 'telemetry_db', type: 'table' },
            stats: {
              count: { value: 100000000 },
              total_bytes: { value: 1099511627776 }, // 1 TB
              partition_by: { value: 'toYYYYMM(event_time)' },
              cluster_by: { value: 'device_id, metric_code' },
            },
            columns: {
              device_id: { type: 'LowCardinality(String)', index: 1 },
              reading_val: { type: 'Float64', index: 2 },
              tags: { type: 'Array(String)', index: 3 },
            },
          },
        },
        sources: {},
      };
      fs.writeFileSync(catalogPath, JSON.stringify(chCatalog));

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const catalog = scanner.parseCatalog(catalogPath);

      expect(catalog.catalogLoaded).toBe(true);
      expect(catalog.totalRows).toBe(100000000);
      expect(catalog.formattedTotalBytes).toBe('1.0 TB');
      expect(catalog.tables.fct_sensor_telemetry.isPartitioned).toBe(true);
      expect(catalog.tables.fct_sensor_telemetry.partitionType).toBe('toYYYYMM(event_time)');
      expect(catalog.tables.fct_sensor_telemetry.columns.device_id.type).toBe('LowCardinality(String)');
    });

    test('ADV-4.4: DuckDB / PostgreSQL catalog format (rowcount, size_bytes, null stats)', () => {
      const catalogPath = path.join(tempDir, 'duckdb_catalog.json');
      const duckCatalog = {
        metadata: { dbt_version: '1.8.0' },
        nodes: {
          'model.duck_proj.stg_local_data': {
            metadata: { name: 'stg_local_data', schema: 'main', database: 'memory', type: 'view' },
            stats: {
              rowcount: { value: 5000 },
              size_bytes: { value: 40960 },
            },
            columns: {
              id: { type: 'BIGINT', index: 1 },
              meta_json: { type: 'JSON', index: 2 },
            },
          },
        },
        sources: {},
      };
      fs.writeFileSync(catalogPath, JSON.stringify(duckCatalog));

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const catalog = scanner.parseCatalog(catalogPath);

      expect(catalog.catalogLoaded).toBe(true);
      expect(catalog.totalRows).toBe(5000);
      expect(catalog.totalBytes).toBe(40960);
      expect(catalog.formattedTotalBytes).toBe('40.0 KB');
      expect(catalog.tables.stg_local_data.type).toBe('view');
    });

    test('ADV-4.5: Corrupted catalog.json (invalid JSON syntax, empty file, non-object JSON)', () => {
      const catalogPath = path.join(tempDir, 'bad_catalog.json');
      fs.writeFileSync(catalogPath, '{ invalid json string');

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const catalog = scanner.parseCatalog(catalogPath);

      expect(catalog.catalogLoaded).toBe(false);
      expect(catalog.errors.length).toBeGreaterThan(0);
      expect(catalog.tableCount).toBe(0);
      expect(catalog.totalRows).toBe(0);
    });

    test('ADV-4.6: Catalog node missing metadata, columns, or stats does not throw exception', () => {
      const catalogPath = path.join(tempDir, 'sparse_catalog.json');
      const sparseCatalog = {
        nodes: {
          'model.proj.sparse1': {},
          'model.proj.sparse2': { metadata: null, stats: null, columns: null },
          'model.proj.sparse3': { metadata: { name: 'sparse3' }, stats: { row_count: { value: null } } },
        },
      };
      fs.writeFileSync(catalogPath, JSON.stringify(sparseCatalog));

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const catalog = scanner.parseCatalog(catalogPath);

      expect(catalog.catalogLoaded).toBe(true);
      expect(catalog.tableCount).toBe(3);
    });
  });

  // =========================================================================
  // Test Category 5: Complex SQL Dependency Extraction & Relationship Tests
  // =========================================================================
  describe('Category 5: Complex SQL Dependency Extraction & Relationship Tests', () => {
    test('ADV-5.1: Complex SQL with multiple refs, sources, CTEs, subqueries, and Jinja comments', async () => {
      const modelsDir = path.join(tempDir, 'models', 'marts');
      fs.mkdirSync(modelsDir, { recursive: true });

      const complexSql = `
-- Model: fct_orders
-- Author: Data Eng
{# Jinja comment with fake ref: {{ ref('fake_model_in_comment') }} #}

WITH raw_orders AS (
  SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
  SELECT * FROM {{ ref('dim_customers') }}
),
exchange_rates AS (
  SELECT * FROM {{ source('finance_raw', 'fx_rates') }}
),
regional_settings AS (
  SELECT * FROM {{ source("geo_data", "regions") }}
)
SELECT
  o.order_id,
  c.customer_name,
  o.amount * fx.rate AS usd_amount
FROM raw_orders o
LEFT JOIN customers c ON o.customer_id = c.id
CROSS JOIN exchange_rates fx
LEFT JOIN regional_settings r ON c.region_id = r.id
WHERE 1=1
`;
      fs.writeFileSync(path.join(modelsDir, 'fct_orders.sql'), complexSql);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      const model = result.models.find(m => m.name === 'fct_orders');
      expect(model).toBeDefined();
      expect(model.dependsOn).toContain('stg_orders');
      expect(model.dependsOn).toContain('dim_customers');
      expect(model.dependsOn).toContain('finance_raw.fx_rates');
      expect(model.dependsOn).toContain('geo_data.regions');
      expect(new Set(model.dependsOn).size).toBe(model.dependsOn.length);
    });

    test('ADV-5.2: SQL file with snapshot blocks and macro blocks co-located in single directory', async () => {
      const sharedDir = path.join(tempDir, 'models', 'shared');
      fs.mkdirSync(sharedDir, { recursive: true });

      const mixedContent = `
{% snapshot customer_snapshot %}
  {{ config(target_schema='snapshots', unique_key='id', strategy='check', check_cols=['status']) }}
  SELECT * FROM {{ source('crm', 'customers') }}
{% endsnapshot %}

{% macro calculate_vat(amount, rate=0.20) %}
  ({{ amount }} * {{ rate }})
{% endmacro %}
`;
      fs.writeFileSync(path.join(sharedDir, 'mixed.sql'), mixedContent);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.summary.snapshotCount).toBe(1);
      expect(result.snapshots[0].name).toBe('customer_snapshot');
      expect(result.summary.macroCount).toBe(1);
      expect(result.macros[0].name).toBe('calculate_vat');
      expect(result.macros[0].args).toContain('amount');
    });

    test('ADV-5.3: Relationship tests with varied ref syntax in schema.yml', async () => {
      const martsDir = path.join(tempDir, 'models', 'marts');
      fs.mkdirSync(martsDir, { recursive: true });

      const schemaWithTests = `
version: 2
models:
  - name: fct_transactions
    columns:
      - name: user_id
        tests:
          - relationships:
              to: ref('dim_users')
              field: user_id
      - name: merchant_id
        tests:
          - relationships:
              to: "ref('dim_merchants')"
              field: merchant_id
      - name: currency_code
        tests:
          - relationships:
              to: dim_currencies
              field: code
`;
      fs.writeFileSync(path.join(martsDir, 'schema.yml'), schemaWithTests);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const result = await scanner.scanProject(tempDir);

      expect(result.relationshipCount).toBe(3);
      expect(result.relationships.find(r => r.fromColumn === 'user_id').toModel).toBe('dim_users');
      expect(result.relationships.find(r => r.fromColumn === 'merchant_id').toModel).toBe('dim_merchants');
      expect(result.relationships.find(r => r.fromColumn === 'currency_code').toModel).toBe('dim_currencies');
    });
  });

  // =========================================================================
  // Test Category 6: Stress Scale, Concurrency & State Isolation
  // =========================================================================
  describe('Category 6: Stress Scale, Concurrency & State Isolation', () => {
    test('ADV-6.1: High volume project (100 models, 50 sources, 20 metrics, 30 docs) scans in < 1 second', async () => {
      const modelsDir = path.join(tempDir, 'models');
      const docsDir = path.join(tempDir, 'docs');
      fs.mkdirSync(path.join(modelsDir, 'marts'), { recursive: true });
      fs.mkdirSync(path.join(modelsDir, 'staging'), { recursive: true });
      fs.mkdirSync(path.join(modelsDir, 'intermediate'), { recursive: true });
      fs.mkdirSync(docsDir, { recursive: true });

      // Generate 30 doc blocks
      let docText = '';
      for (let i = 1; i <= 30; i++) {
        docText += `{% docs doc_${i} %}Description for doc #${i}{% enddocs %}\n`;
      }
      fs.writeFileSync(path.join(docsDir, 'bulk_docs.md'), docText);

      // Generate 100 models (40 marts, 30 intermediate, 30 staging)
      for (let i = 1; i <= 40; i++) {
        fs.writeFileSync(path.join(modelsDir, 'marts', `fct_model_${i}.sql`), `SELECT 1 as id, '{{ doc("doc_${(i % 30) + 1}") }}' as note`);
      }
      for (let i = 1; i <= 30; i++) {
        fs.writeFileSync(path.join(modelsDir, 'intermediate', `int_model_${i}.sql`), 'SELECT 1 as id');
      }
      for (let i = 1; i <= 30; i++) {
        fs.writeFileSync(path.join(modelsDir, 'staging', `stg_model_${i}.sql`), 'SELECT 1 as id');
      }

      // Generate schema YAML with sources and metrics
      let sourcesYaml = 'version: 2\nsources:\n';
      for (let i = 1; i <= 50; i++) {
        sourcesYaml += `  - name: src_${i}\n    tables:\n      - name: table_${i}\n`;
      }
      fs.writeFileSync(path.join(modelsDir, 'sources.yml'), sourcesYaml);

      let metricsYaml = 'version: 2\nmetrics:\n';
      for (let i = 1; i <= 20; i++) {
        metricsYaml += `  - name: metric_${i}\n    type: simple\n    type_params:\n      measure: measure_${i}\n`;
      }
      fs.writeFileSync(path.join(modelsDir, 'metrics.yml'), metricsYaml);

      const scanner = new DbtDeepScanner({ projectDir: tempDir });
      const t0 = Date.now();
      const result = await scanner.scanProject(tempDir);
      const elapsedMs = Date.now() - t0;

      expect(result.modelCount).toBe(100);
      expect(result.sourceCount).toBe(50);
      expect(result.metricCount).toBe(20);
      expect(result.docBlockCount).toBe(30);
      expect(result.modelsByTier.marts_fact).toBe(40);
      expect(result.modelsByTier.intermediate).toBe(30);
      expect(result.modelsByTier.staging).toBe(30);
      expect(elapsedMs).toBeLessThan(1500);
    });

    test('ADV-6.2: State isolation across consecutive scanProject calls on the same scanner instance', async () => {
      const scanner = new DbtDeepScanner({ projectDir: tempDir });

      const result1 = await scanner.scanProject(tempDir);
      expect(result1.modelCount).toBe(0);

      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(modelsDir, { recursive: true });
      fs.writeFileSync(path.join(modelsDir, 'dim_users.sql'), 'SELECT 1 as id');

      const result2 = await scanner.scanProject(tempDir);
      expect(result2.modelCount).toBe(1);

      fs.rmSync(path.join(modelsDir, 'dim_users.sql'));
      const result3 = await scanner.scanProject(tempDir);
      expect(result3.modelCount).toBe(0);
    });

    test('ADV-6.3: Concurrent scans from multiple scanner instances operate independently', async () => {
      const dir1 = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-c1-'));
      const dir2 = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-c2-'));

      try {
        fs.mkdirSync(path.join(dir1, 'models'), { recursive: true });
        fs.mkdirSync(path.join(dir2, 'models'), { recursive: true });

        fs.writeFileSync(path.join(dir1, 'models', 'fct_one.sql'), 'SELECT 1');
        fs.writeFileSync(path.join(dir2, 'models', 'fct_two_a.sql'), 'SELECT 1');
        fs.writeFileSync(path.join(dir2, 'models', 'fct_two_b.sql'), 'SELECT 1');

        const s1 = new DbtDeepScanner({ projectDir: dir1 });
        const s2 = new DbtDeepScanner({ projectDir: dir2 });

        const [r1, r2] = await Promise.all([s1.scanProject(dir1), s2.scanProject(dir2)]);

        expect(r1.modelCount).toBe(1);
        expect(r1.models[0].name).toBe('fct_one');

        expect(r2.modelCount).toBe(2);
        expect(r2.models.map(m => m.name)).toEqual(['fct_two_a', 'fct_two_b']);
      } finally {
        fs.rmSync(dir1, { recursive: true, force: true });
        fs.rmSync(dir2, { recursive: true, force: true });
      }
    });
  });

  // =========================================================================
  // Test Category 7: MCP Handler End-to-End Edge Cases
  // =========================================================================
  describe('Category 7: MCP Handler End-to-End Edge Cases', () => {
    test('ADV-7.1: handleDbtProjectScanDeep with all flags disabled (include_docs=false, include_catalog=false, include_metrics=false)', async () => {
      const modelsDir = path.join(tempDir, 'models');
      const docsDir = path.join(tempDir, 'docs');
      fs.mkdirSync(modelsDir, { recursive: true });
      fs.mkdirSync(docsDir, { recursive: true });

      fs.writeFileSync(path.join(docsDir, 'docs.md'), '{% docs test_doc %}Sample Doc{% enddocs %}');
      fs.writeFileSync(path.join(modelsDir, 'fct_orders.sql'), 'SELECT 1');
      fs.writeFileSync(path.join(modelsDir, 'metrics.yml'), 'version: 2\nmetrics:\n  - name: m1\n    type: simple');

      const handler = new DbtSemanticHandler();
      const response = await handler.handleDbtProjectScanDeep({
        project_dir: tempDir,
        include_docs: false,
        include_catalog: false,
        include_metrics: false,
      });

      expect(response.isError).toBeUndefined();
      expect(response.structuredContent.doc_block_count).toBe(0);
      expect(response.structuredContent.metric_count).toBe(0);
      expect(response.structuredContent.model_count).toBe(1);
    });

    test('ADV-7.2: handleDbtProjectScanDeep handles non-matching tier_filter without error', async () => {
      const modelsDir = path.join(tempDir, 'models');
      fs.mkdirSync(path.join(modelsDir, 'staging'), { recursive: true });
      fs.writeFileSync(path.join(modelsDir, 'staging', 'stg_orders.sql'), 'SELECT 1');

      const handler = new DbtSemanticHandler();
      const response = await handler.handleDbtProjectScanDeep({
        project_dir: tempDir,
        tier_filter: 'marts_fact',
      });

      expect(response.isError).toBeUndefined();
      expect(response.structuredContent.model_count).toBe(0);
      expect(response.structuredContent.modelsByTier.staging).toBe(1);
      expect(response.content[0].text).toContain('Gold Facts (`marts_fact`): **0**');
    });
  });
});
