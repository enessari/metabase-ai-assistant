/**
 * tests/m3-challenger-empirical.test.js
 * Adversarial Empirical Stress-Test Suite for Milestone 3: Cube.js Pre-Aggregation Advisor
 * Challenger: M3-1 (Multi-Dialect DDL Syntax & Edge-Case Challenger)
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import {
  DbtPreaggAdvisor,
  SUPPORTED_DIALECTS,
  SUPPORTED_GRAINS,
  ADDITIVITY_TYPES,
  normalizeDialect,
  normalizeTimeGrain,
  formatBytes,
} from '../src/dbt/preagg-advisor.js';
import { DbtSemanticHandler } from '../src/mcp/handlers/dbt-semantic.js';
import { getToolDefinitions, TOOL_METADATA } from '../src/mcp/tool-registry.js';

describe('Adversarial Challenger M3-1: Multi-Dialect DDL & Edge-Case Verification', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'challenger-m3-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  // =========================================================================
  // 1. Multi-Dialect DDL Syntax & Structural Invariant Verification
  // =========================================================================
  describe('1. Multi-Dialect DDL Syntax & Structural Invariant Verification', () => {
    const advisor = new DbtPreaggAdvisor();

    test('1.1 PostgreSQL: generates syntactically valid DDL with unique index for REFRESH CONCURRENTLY', () => {
      const result = advisor.generateRollupDDL({
        dialect: 'postgres',
        model: 'fct_orders',
        targetSchema: 'analytics_preagg',
        timeGrain: 'week',
        timeDimension: 'order_timestamp',
        dimensions: ['status', 'country_code'],
        measures: [
          { name: 'revenue', agg: 'sum', column: 'amount' },
          { name: 'order_count', agg: 'count', column: '*' },
          { name: 'avg_price', agg: 'avg', column: 'price' },
        ],
      });

      expect(result.ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_preagg.fct_orders_preagg_week AS');
      expect(result.ddl).toContain("DATE_TRUNC('week', order_timestamp) AS order_timestamp_week");
      expect(result.ddl).toContain('status,\n  country_code');
      expect(result.ddl).toContain('SUM(amount) AS sum_revenue');
      expect(result.ddl).toContain('COUNT(*) AS count_order_count');
      expect(result.ddl).toContain('SUM(price) AS sum_avg_price, COUNT(price) AS count_avg_price');
      expect(result.ddl).toContain('GROUP BY 1, 2, 3;');

      // Verify PostgreSQL Concurrent Refresh Prerequisite
      expect(result.index_ddl).toHaveLength(2);
      expect(result.index_ddl[0]).toBe(
        'CREATE UNIQUE INDEX IF NOT EXISTS uidx_fct_orders_preagg_week ON analytics_preagg.fct_orders_preagg_week (order_timestamp_week, status, country_code);'
      );
      expect(result.index_ddl[1]).toBe(
        'CREATE INDEX IF NOT EXISTS idx_fct_orders_preagg_week_time ON analytics_preagg.fct_orders_preagg_week (order_timestamp_week);'
      );
      expect(result.refresh_command).toBe('REFRESH MATERIALIZED VIEW CONCURRENTLY analytics_preagg.fct_orders_preagg_week;');
      expect(result.refresh_strategy).toBe('concurrent');
    });

    test('1.2 Google BigQuery: verifies backtick escaping, OPTIONS, PARTITION BY, and 4-column CLUSTER BY cap', () => {
      const result = advisor.generateRollupDDL({
        dialect: 'bigquery',
        model: 'fct_web_events',
        targetSchema: 'marts_preagg',
        timeGrain: 'day',
        timeDimension: 'event_date',
        dimensions: ['browser', 'os', 'country', 'device', 'extra_dim1', 'extra_dim2'],
        refreshIntervalMinutes: 30,
      });

      expect(result.ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS `marts_preagg.fct_web_events_preagg_day`');
      expect(result.ddl).toContain('OPTIONS (\n  enable_refresh = true,\n  refresh_interval_minutes = 30\n)');
      expect(result.ddl).toContain('PARTITION BY DATE(event_date_day)');
      // Strict BigQuery limit: max 4 cluster columns
      expect(result.ddl).toContain('CLUSTER BY browser, os, country, device');
      expect(result.ddl).not.toContain('CLUSTER BY browser, os, country, device, extra_dim1');
      expect(result.ddl).toContain('FROM `fct_web_events`');
      expect(result.refresh_command).toBe("CALL BQ.REFRESH_MATERIALIZED_VIEW('marts_preagg.fct_web_events_preagg_day');");
      expect(result.refresh_strategy).toBe('auto');
    });

    test('1.3 Snowflake: verifies CREATE OR REPLACE MATERIALIZED VIEW with CLUSTER BY syntax', () => {
      const result = advisor.generateRollupDDL({
        dialect: 'snowflake',
        model: 'fct_subscriptions',
        targetSchema: 'preagg_sf',
        timeGrain: 'month',
        timeDimension: 'start_date',
        dimensions: ['plan_tier', 'billing_frequency', 'region'],
      });

      expect(result.ddl).toContain('CREATE OR REPLACE MATERIALIZED VIEW preagg_sf.fct_subscriptions_preagg_month');
      expect(result.ddl).toContain('CLUSTER BY (start_date_month, plan_tier, billing_frequency, region)');
      expect(result.ddl).toContain("DATE_TRUNC('month', start_date) AS start_date_month");
      expect(result.ddl).toContain('GROUP BY 1, 2, 3, 4;');
      expect(result.refresh_strategy).toBe('auto');
    });

    test('1.4 ClickHouse: verifies SummingMergeTree table with PRIMARY KEY and Materialized View TO trigger', () => {
      const result = advisor.generateRollupDDL({
        dialect: 'clickhouse',
        model: 'fct_transactions',
        targetSchema: 'analytics_ch',
        timeGrain: 'day',
        timeDimension: 'tx_time',
        dimensions: ['merchant_id', 'currency', 'status'],
        measures: [
          { name: 'amount', agg: 'sum', column: 'amount' },
          { name: 'fee', agg: 'sum', column: 'fee' },
          { name: 'tx_count', agg: 'count', column: '*' },
          { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
        ],
      });

      // Target Table DDL
      expect(result.ddl).toContain('CREATE TABLE IF NOT EXISTS analytics_ch.fct_transactions_preagg_day (');
      expect(result.ddl).toContain('tx_time_day Date');
      expect(result.ddl).toContain('merchant_id LowCardinality(String)');
      expect(result.ddl).toContain('sum_amount Float64');
      expect(result.ddl).toContain('sum_fee Float64');
      expect(result.ddl).toContain('count_tx_count UInt64');
      expect(result.ddl).toContain('hll_unique_users AggregateFunction(uniqCombined, String)');
      expect(result.ddl).toContain('ENGINE = SummingMergeTree((sum_amount, sum_fee, count_tx_count))');
      expect(result.ddl).toContain('PRIMARY KEY (tx_time_day, merchant_id, currency, status)');
      expect(result.ddl).toContain('ORDER BY (tx_time_day, merchant_id, currency, status)');
      expect(result.ddl).toContain('SETTINGS index_granularity = 8192;');

      // Materialized View Trigger DDL
      expect(result.ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_ch.fct_transactions_preagg_day_mv');
      expect(result.ddl).toContain('TO analytics_ch.fct_transactions_preagg_day');
      expect(result.ddl).toContain('toStartOfDay(tx_time) AS tx_time_day');
      expect(result.ddl).toContain('uniqCombinedState(user_id) AS hll_unique_users');
      expect(result.ddl).toContain('GROUP BY tx_time_day, merchant_id, currency, status;');
      expect(result.refresh_strategy).toBe('realtime_mv');
    });

    test('1.5 DuckDB: verifies CREATE OR REPLACE TABLE and secondary index creation', () => {
      const result = advisor.generateRollupDDL({
        dialect: 'duckdb',
        model: 'fct_logs',
        targetSchema: 'preagg_duck',
        timeGrain: 'hour',
        timeDimension: 'log_time',
        dimensions: ['level', 'service'],
      });

      expect(result.ddl).toContain('CREATE OR REPLACE TABLE preagg_duck.fct_logs_preagg_hour AS');
      expect(result.ddl).toContain("DATE_TRUNC('hour', log_time) AS log_time_hour");
      expect(result.ddl).toContain('GROUP BY 1, 2, 3;');
      expect(result.index_ddl).toHaveLength(1);
      expect(result.index_ddl[0]).toBe('CREATE INDEX IF NOT EXISTS idx_fct_logs_preagg_hour ON preagg_duck.fct_logs_preagg_hour (log_time_hour, level, service);');
      expect(result.refresh_strategy).toBe('batch_replace');
    });

    test('1.6 MySQL: verifies InnoDB summary table, backtick escaping, PRIMARY KEY, and ON DUPLICATE KEY UPDATE', () => {
      const result = advisor.generateRollupDDL({
        dialect: 'mysql',
        model: 'fct_orders',
        targetSchema: 'analytics',
        timeGrain: 'month',
        timeDimension: 'order_date',
        dimensions: ['store_id', 'status'],
        measures: [
          { name: 'amount', agg: 'sum', column: 'amount' },
          { name: 'orders', agg: 'count', column: '*' },
          { name: 'avg_price', agg: 'avg', column: 'price' },
        ],
      });

      expect(result.ddl).toContain('CREATE TABLE IF NOT EXISTS `analytics`.`fct_orders_preagg_month` (');
      expect(result.ddl).toContain('`order_date_month` DATE NOT NULL');
      expect(result.ddl).toContain('`store_id` VARCHAR(128) NOT NULL');
      expect(result.ddl).toContain('PRIMARY KEY (`order_date_month`, `store_id`, `status`)');
      expect(result.ddl).toContain('INDEX `idx_time` (`order_date_month`)');
      expect(result.ddl).toContain('ENGINE = InnoDB;');

      // Scheduled Upsert Command
      expect(result.refresh_command).toContain('INSERT INTO `analytics`.`fct_orders_preagg_month`');
      expect(result.refresh_command).toContain("DATE_FORMAT(order_date, '%Y-%m-01') AS order_date_month");
      expect(result.refresh_command).toContain('ON DUPLICATE KEY UPDATE');
      expect(result.refresh_command).toContain('`sum_amount` = VALUES(`sum_amount`)');
      expect(result.refresh_command).toContain('`count_orders` = VALUES(`count_orders`)');
      expect(result.refresh_command).toContain('`sum_avg_price` = VALUES(`sum_avg_price`)');
      expect(result.refresh_command).toContain('`count_avg_price` = VALUES(`count_avg_price`)');
      expect(result.refresh_strategy).toBe('scheduled_upsert');
    });

    test('1.7 Amazon Redshift: verifies AUTO REFRESH YES, DISTKEY, and SORTKEY syntax', () => {
      const result = advisor.generateRollupDDL({
        dialect: 'redshift',
        model: 'fct_clicks',
        targetSchema: 'preagg_rs',
        timeGrain: 'quarter',
        timeDimension: 'click_time',
        dimensions: ['campaign_id', 'ad_group_id'],
      });

      expect(result.ddl).toContain('CREATE MATERIALIZED VIEW preagg_rs.fct_clicks_preagg_quarter');
      expect(result.ddl).toContain('AUTO REFRESH YES');
      expect(result.ddl).toContain('DISTSTYLE KEY');
      expect(result.ddl).toContain('DISTKEY(campaign_id)');
      expect(result.ddl).toContain('SORTKEY(click_time_quarter, campaign_id, ad_group_id)');
      expect(result.ddl).toContain("DATE_TRUNC('quarter', click_time) AS click_time_quarter");
      expect(result.refresh_command).toBe('REFRESH MATERIALIZED VIEW preagg_rs.fct_clicks_preagg_quarter;');
      expect(result.refresh_strategy).toBe('auto');
    });

    test('1.8 Exhaustive 42-Combination Date Truncation Engine Check across all 7 Dialects and 6 Grains', () => {
      const allDialects = ['postgres', 'bigquery', 'snowflake', 'clickhouse', 'duckdb', 'redshift', 'mysql'];
      const allGrains = ['hour', 'day', 'week', 'month', 'quarter', 'year'];

      for (const d of allDialects) {
        for (const g of allGrains) {
          const sql = advisor.getDateTruncSql(d, 'ts_col', g);
          expect(typeof sql).toBe('string');
          expect(sql.length).toBeGreaterThan(0);
          expect(sql).toContain('ts_col');
        }
      }
    });
  });

  // =========================================================================
  // 2. Edge Cases: Reserved Keywords, Extreme Time Grains & Unknown Dialects
  // =========================================================================
  describe('2. Edge Cases: Reserved Keywords, Extreme Grains & Unknown Dialects', () => {
    const advisor = new DbtPreaggAdvisor();

    test('2.1 Handles SQL reserved keywords in column names and model names safely', () => {
      const reservedResult = advisor.generateRollupDDL({
        dialect: 'mysql',
        model: 'order',
        targetSchema: 'group',
        timeGrain: 'day',
        timeDimension: 'date',
        dimensions: ['select', 'where', 'from', 'table'],
        measures: [
          { name: 'count', agg: 'count', column: '*' },
          { name: 'sum', agg: 'sum', column: 'amount' },
        ],
      });

      expect(reservedResult.ddl).toContain('CREATE TABLE IF NOT EXISTS `group`.`order_preagg_day` (');
      expect(reservedResult.ddl).toContain('`date_day` DATE NOT NULL');
      expect(reservedResult.ddl).toContain('`select` VARCHAR(128) NOT NULL');
      expect(reservedResult.ddl).toContain('`where` VARCHAR(128) NOT NULL');
      expect(reservedResult.ddl).toContain('PRIMARY KEY (`date_day`, `select`, `where`, `from`, `table`)');
      expect(reservedResult.refresh_command).toContain('FROM `order`');
    });

    test('2.2 Unknown and invalid time grains normalize safely to day fallback', () => {
      expect(normalizeTimeGrain('millisecond')).toBe('day');
      expect(normalizeTimeGrain('nanosecond')).toBe('day');
      expect(normalizeTimeGrain('century')).toBe('day');
      expect(normalizeTimeGrain('invalid_grain_xyz')).toBe('day');
      expect(normalizeTimeGrain('')).toBe('day');
      expect(normalizeTimeGrain(null)).toBe('day');
      expect(normalizeTimeGrain(undefined)).toBe('day');
      expect(normalizeTimeGrain(12345)).toBe('day');
    });

    test('2.3 Unknown and invalid SQL dialects normalize safely to postgres fallback', () => {
      expect(normalizeDialect('oracle')).toBe('postgres');
      expect(normalizeDialect('sqlite')).toBe('postgres');
      expect(normalizeDialect('db2')).toBe('postgres');
      expect(normalizeDialect('cockroachdb')).toBe('postgres');
      expect(normalizeDialect('unknown_db')).toBe('postgres');
      expect(normalizeDialect('')).toBe('postgres');
      expect(normalizeDialect(null)).toBe('postgres');
      expect(normalizeDialect(undefined)).toBe('postgres');
      expect(normalizeDialect(12345)).toBe('postgres');
    });

    test('2.4 Dialect aliases resolve accurately across upper/lower/mixed casing', () => {
      expect(normalizeDialect('POSTGRESQL')).toBe('postgres');
      expect(normalizeDialect('PGSQL')).toBe('postgres');
      expect(normalizeDialect('BQ')).toBe('bigquery');
      expect(normalizeDialect('SnowFlake')).toBe('snowflake');
      expect(normalizeDialect('CH')).toBe('clickhouse');
      expect(normalizeDialect('Duck')).toBe('duckdb');
      expect(normalizeDialect('RS')).toBe('redshift');
      expect(normalizeDialect('MariaDB')).toBe('mysql');
    });

    test('2.5 Missing or null time dimension auto-detects or falls back to order_date', () => {
      const ddlWithoutTimeDim = advisor.generateRollupDDL({
        model: 'fct_orders',
        timeDimension: null,
        dimensions: ['status'],
      });

      expect(ddlWithoutTimeDim.ddl).toContain("DATE_TRUNC('day', order_date) AS order_date_day");
    });
  });

  // =========================================================================
  // 3. Degenerate Models & Empty Scan Results
  // =========================================================================
  describe('3. Degenerate Models & Empty Scan Results', () => {
    test('3.1 Generates sensible recommendations when scanResult is completely empty', () => {
      const advisor = new DbtPreaggAdvisor({});
      const recs = advisor.advisePreaggregations();

      expect(recs.length).toBeGreaterThan(0);
      expect(recs[0].model).toBe('fct_orders');
      expect(recs[0].time_dimension).toBe('order_date');
      expect(recs[0].dimensions).toEqual(['status', 'category']);
      expect(recs[0].speedup_estimate.is_heuristic_estimate).toBe(true);
    });

    test('3.2 Handles model with 0 metrics and empty columns object gracefully', () => {
      const scanResult = {
        models: [
          {
            name: 'fct_empty',
            tier: 'marts_fact',
            columns: {},
          },
        ],
      };

      const advisor = new DbtPreaggAdvisor(scanResult);
      const recs = advisor.advisePreaggregations({ model_name: 'fct_empty' });

      expect(recs.length).toBeGreaterThan(0);
      expect(recs[0].model).toBe('fct_empty');
      expect(recs[0].measures.length).toBeGreaterThan(0);
      expect(recs[0].ddl).toContain('CREATE MATERIALIZED VIEW');
    });

    test('3.3 Handles pure temporal rollup with 0 categorical dimensions (time-only aggregation)', () => {
      const advisor = new DbtPreaggAdvisor();
      const result = advisor.generateRollupDDL({
        model: 'fct_metrics',
        timeDimension: 'created_at',
        timeGrain: 'day',
        dimensions: [],
        measures: [{ name: 'total_events', agg: 'count', column: '*' }],
      });

      expect(result.ddl).toContain("DATE_TRUNC('day', created_at) AS created_at_day");
      expect(result.ddl).toContain('COUNT(*) AS count_total_events');
      expect(result.ddl).toContain('GROUP BY 1;');
      expect(result.index_ddl[0]).toContain('(created_at_day)');
    });

    test('3.4 Default parameter initialization when constructor options are omitted', () => {
      const advisorDefault = new DbtPreaggAdvisor();
      expect(advisorDefault.defaultDialect).toBe('postgres');
      expect(advisorDefault.defaultSchema).toBe('preagg');

      const recs = advisorDefault.advisePreaggregations({});
      expect(recs.length).toBeGreaterThan(0);
    });

    test('3.5 Multiple model batch advising across disparate marts models', () => {
      const scanResult = {
        models: [
          {
            name: 'fct_orders',
            tier: 'marts_fact',
            columns: { order_date: {}, status: {}, amount: {} },
          },
          {
            name: 'fct_subscriptions',
            tier: 'marts_fact',
            columns: { start_date: {}, plan: {}, mrr: {} },
          },
        ],
      };

      const advisor = new DbtPreaggAdvisor(scanResult);
      const recs = advisor.advisePreaggregations({
        model_names: ['fct_orders', 'fct_subscriptions'],
        time_grain: 'month',
      });

      expect(recs).toHaveLength(2);
      expect(recs[0].model).toBe('fct_orders');
      expect(recs[1].model).toBe('fct_subscriptions');
    });
  });

  // =========================================================================
  // 4. Mathematical Additivity & HyperLogLog Verification Across All 7 Dialects
  // =========================================================================
  describe('4. Mathematical Additivity & HyperLogLog Verification Across All 7 Dialects', () => {
    const advisor = new DbtPreaggAdvisor();

    test('4.1 HyperLogLog distinct count sketch generation across all 7 dialects', () => {
      const dialects = ['bigquery', 'snowflake', 'clickhouse', 'duckdb', 'redshift', 'postgres', 'mysql'];
      const expectedExpressions = {
        bigquery: 'HLL_COUNT.INIT(user_id, 14) AS hll_unique_visitors',
        snowflake: 'HLL_ACCUMULATE(user_id) AS hll_unique_visitors',
        clickhouse: 'uniqCombinedState(user_id) AS hll_unique_visitors',
        duckdb: 'approx_count_distinct(user_id) AS approx_unique_visitors',
        redshift: 'hyperloglog(user_id) AS hll_unique_visitors',
        postgres: 'hll_add_agg(hll_hash_text(user_id::text)) AS hll_unique_visitors',
        mysql: 'COUNT(DISTINCT user_id) AS count_distinct_unique_visitors',
      };

      for (const d of dialects) {
        const classified = advisor.classifyAdditivity(
          { name: 'unique_visitors', agg: 'count_distinct', column: 'user_id' },
          { dialect: d, includeHll: true }
        );
        expect(classified.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
        expect(classified.sql_expression).toBe(expectedExpressions[d]);
      }
    });

    test('4.2 Non-additive average decomposition protects against division by zero', () => {
      const classified = advisor.classifyAdditivity({
        name: 'avg_order_value',
        agg: 'avg',
        column: 'order_total',
      });

      expect(classified.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(classified.sql_expression).toBe('SUM(order_total) AS sum_avg_order_value, COUNT(order_total) AS count_avg_order_value');
      expect(classified.rollup_expression).toBe('SUM(sum_avg_order_value) / NULLIF(SUM(count_avg_order_value), 0)');
    });

    test('4.3 Compound ratio metric decomposes into base additive measures with NULLIF denominator', () => {
      const classified = advisor.classifyAdditivity({
        name: 'conversion_rate',
        agg: 'ratio',
        formula: 'converted_users / total_visitors',
      });

      expect(classified.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(classified.sql_expression).toBe('SUM(converted_users) AS sum_conversion_rate_num, SUM(total_visitors) AS sum_conversion_rate_den');
      expect(classified.rollup_expression).toBe('SUM(sum_conversion_rate_num) / NULLIF(SUM(sum_conversion_rate_den), 0)');
      expect(classified.columns).toHaveLength(2);
    });

    test('4.4 Semi-additive snapshot metric preserves snapshot date and warning recommendation', () => {
      const classified = advisor.classifyAdditivity({
        name: 'daily_bank_balance',
        agg: 'balance',
        column: 'closing_balance',
        non_additive_dimension: { name: 'snapshot_date', window_choice: 'max' },
      });

      expect(classified.additivity).toBe(ADDITIVITY_TYPES.SEMI_ADDITIVE);
      expect(classified.non_additive_dimension.name).toBe('snapshot_date');
      expect(classified.recommendation).toContain('Preserve snapshot time grain');
    });

    test('4.5 Additive boolean sum metric generates CASE WHEN statement', () => {
      const classified = advisor.classifyAdditivity({
        name: 'active_accounts',
        agg: 'sum_boolean',
        column: 'is_active',
      });

      expect(classified.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);
      expect(classified.sql_expression).toBe('SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS sum_active_accounts');
      expect(classified.rollup_expression).toBe('SUM(sum_active_accounts)');
    });
  });

  // =========================================================================
  // 5. Performance Speedup Estimator & Scan Reduction Boundaries
  // =========================================================================
  describe('5. Performance Speedup Estimator & Scan Reduction Boundaries', () => {
    const advisor = new DbtPreaggAdvisor();

    test('5.1 Speedup estimator survives 0, negative, and astronomical row counts', () => {
      const zeroEst = advisor.estimateSpeedup(0, 'day', ['status']);
      expect(zeroEst.raw_rows).toBe(1000000); // defaults safely
      expect(zeroEst.speedup_factor).toBeGreaterThan(1);

      const hugeEst = advisor.estimateSpeedup(1000000000, 'month', ['status']);
      expect(hugeEst.raw_rows).toBe(1000000000);
      expect(hugeEst.speedup_factor).toBeGreaterThan(100);
      expect(hugeEst.scan_reduction_pct).toBeCloseTo(99.9, 0);
    });

    test('5.2 High-cardinality combinatorial explosion is capped at raw row count', () => {
      const dims = [
        { name: 'user_id', cardinality: 100000 },
        { name: 'session_id', cardinality: 50000 },
      ];
      const estimate = advisor.estimateSpeedup(10000, 'day', dims);

      expect(estimate.preagg_rows).toBeLessThanOrEqual(10000);
      expect(estimate.speedup_factor).toBe(1.0);
    });

    test('5.3 Time grain non-temporal grain (none) estimates single aggregate row', () => {
      const estimate = advisor.estimateSpeedup(1000000, 'none', []);
      expect(estimate.preagg_rows).toBe(1);
      expect(estimate.speedup_factor).toBe(10000); // capped at maximum 10,000x
    });
  });

  // =========================================================================
  // 6. MCP Handler End-to-End Stress Test
  // =========================================================================
  describe('6. MCP Handler End-to-End Stress Test', () => {
    test('6.1 handleDbtSemanticPreaggAdvisor produces valid MCP payload and governance provenance for multi-grain request', async () => {
      // Set up dbt project
      fs.writeFileSync(path.join(tempDir, 'dbt_project.yml'), 'name: analytics_pkg\nversion: 2.0.0\n');
      const targetDir = path.join(tempDir, 'target');
      fs.mkdirSync(targetDir, { recursive: true });

      const manifest = {
        metadata: { dbt_version: '1.8.0' },
        nodes: {
          'model.analytics.fct_mrr': {
            name: 'fct_mrr',
            resource_type: 'model',
            package_name: 'analytics',
            columns: {
              activity_date: { name: 'activity_date', data_type: 'date' },
              plan_type: { name: 'plan_type', data_type: 'varchar' },
              mrr_amount: { name: 'mrr_amount', data_type: 'numeric' },
            },
          },
        },
        sources: {},
        metrics: {
          'metric.analytics.total_mrr': {
            name: 'total_mrr',
            type: 'simple',
            model: 'fct_mrr',
            agg: 'sum',
            column: 'mrr_amount',
          },
        },
        semantic_models: {},
        exposures: {},
      };
      fs.writeFileSync(path.join(targetDir, 'manifest.json'), JSON.stringify(manifest));

      const handler = new DbtSemanticHandler(null, null, null);
      const response = await handler.handleDbtSemanticPreaggAdvisor({
        project_dir: tempDir,
        model_name: 'fct_mrr',
        dialect: 'snowflake',
        time_grains: ['day', 'week', 'month'],
        target_schema: 'marts_rollup',
      });

      expect(response.isError).toBeFalsy();
      expect(response.content[0].text).toContain('SNOWFLAKE');
      expect(response.structuredContent).toBeDefined();
      expect(response.structuredContent.recommendations).toHaveLength(3);
      expect(response.structuredContent.recommendations[0].time_grain).toBe('day');
      expect(response.structuredContent.recommendations[1].time_grain).toBe('week');
      expect(response.structuredContent.recommendations[2].time_grain).toBe('month');
      expect(response.structuredContent.ddl_summary.materialized_views).toHaveLength(3);
      expect(response.structuredContent._provenance.governance_level).toBe('READ_ONLY_ADVISORY');
      expect(response.structuredContent._provenance.advisor).toBe('DbtPreaggAdvisor');
    });
  });
});
