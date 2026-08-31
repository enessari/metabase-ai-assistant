/**
 * tests/unit/preagg-advisor.test.js
 * Unit Test Suite for DbtPreaggAdvisor, Metric Additivity Classifier, Multi-Dialect DDL Compiler,
 * and dbt_semantic_preagg_advisor MCP Tool.
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
} from '../../src/dbt/preagg-advisor.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';

describe('DbtPreaggAdvisor & dbt_semantic_preagg_advisor Unit Tests', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbt-preagg-test-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 1: Metric & Measure Additivity Classification
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 1: Metric & Measure Additivity Classification', () => {
    const advisor = new DbtPreaggAdvisor();

    test('TC-1.1: Additive measures: classifies sum, count, min, max, sum_boolean as ADDITIVE', () => {
      const sumResult = advisor.classifyAdditivity({ name: 'revenue', agg: 'sum', column: 'amount' });
      expect(sumResult.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);
      expect(sumResult.sql_expression).toBe('SUM(amount) AS sum_revenue');
      expect(sumResult.rollup_expression).toBe('SUM(sum_revenue)');

      const countResult = advisor.classifyAdditivity({ name: 'orders', agg: 'count', column: '*' });
      expect(countResult.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);
      expect(countResult.sql_expression).toBe('COUNT(*) AS count_orders');
      expect(countResult.rollup_expression).toBe('SUM(count_orders)');

      const minResult = advisor.classifyAdditivity({ name: 'min_price', agg: 'min', column: 'price' });
      expect(minResult.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);
      expect(minResult.rollup_expression).toBe('MIN(min_min_price)');

      const maxResult = advisor.classifyAdditivity({ name: 'max_price', agg: 'max', column: 'price' });
      expect(maxResult.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);
      expect(maxResult.rollup_expression).toBe('MAX(max_max_price)');

      const boolResult = advisor.classifyAdditivity({ name: 'active_cnt', agg: 'sum_boolean', column: 'is_active' });
      expect(boolResult.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);
      expect(boolResult.sql_expression).toContain('CASE WHEN is_active THEN 1 ELSE 0 END');
    });

    test('TC-1.2: Semi-additive measures: classifies snapshot / balance metrics with non_additive_dimension', () => {
      const snapshotResult = advisor.classifyAdditivity({
        name: 'inventory_level',
        agg: 'sum',
        column: 'quantity_on_hand',
        non_additive_dimension: {
          name: 'snapshot_date',
          window_choice: 'max',
          window_groupings: ['warehouse_id'],
        },
      });

      expect(snapshotResult.additivity).toBe(ADDITIVITY_TYPES.SEMI_ADDITIVE);
      expect(snapshotResult.non_additive_dimension).toBeDefined();
      expect(snapshotResult.non_additive_dimension.name).toBe('snapshot_date');
      expect(snapshotResult.decomposition).toContain('non-additive across time dimension');
    });

    test('TC-1.3: Non-additive avg/average/mean: decomposes into constituent SUM and COUNT columns', () => {
      const avgResult = advisor.classifyAdditivity({ name: 'avg_order_val', agg: 'avg', column: 'amount' });

      expect(avgResult.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(avgResult.sql_expression).toContain('SUM(amount) AS sum_avg_order_val');
      expect(avgResult.sql_expression).toContain('COUNT(amount) AS count_avg_order_val');
      expect(avgResult.rollup_expression).toBe('SUM(sum_avg_order_val) / NULLIF(SUM(count_avg_order_val), 0)');
      expect(avgResult.columns).toHaveLength(2);
      expect(avgResult.columns[0].type).toBe('sum');
      expect(avgResult.columns[1].type).toBe('count');
    });

    test('TC-1.4: Non-additive count_distinct: generates HyperLogLog sketch columns across dialects', () => {
      const bqHll = advisor.classifyAdditivity(
        { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
        { dialect: 'bigquery', includeHll: true }
      );
      expect(bqHll.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(bqHll.hll_supported).toBe(true);
      expect(bqHll.sql_expression).toBe('HLL_COUNT.INIT(user_id, 14) AS hll_unique_users');
      expect(bqHll.rollup_expression).toBe('HLL_COUNT.MERGE(hll_unique_users)');

      const sfHll = advisor.classifyAdditivity(
        { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
        { dialect: 'snowflake', includeHll: true }
      );
      expect(sfHll.sql_expression).toBe('HLL_ACCUMULATE(user_id) AS hll_unique_users');
      expect(sfHll.rollup_expression).toBe('HLL_ESTIMATE(HLL_COMBINE(hll_unique_users))');

      const chHll = advisor.classifyAdditivity(
        { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
        { dialect: 'clickhouse', includeHll: true }
      );
      expect(chHll.sql_expression).toBe('uniqCombinedState(user_id) AS hll_unique_users');
      expect(chHll.rollup_expression).toBe('uniqCombinedMerge(hll_unique_users)');

      const duckHll = advisor.classifyAdditivity(
        { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
        { dialect: 'duckdb', includeHll: true }
      );
      expect(duckHll.sql_expression).toBe('approx_count_distinct(user_id) AS approx_unique_users');

      const rsHll = advisor.classifyAdditivity(
        { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
        { dialect: 'redshift', includeHll: true }
      );
      expect(rsHll.sql_expression).toBe('hyperloglog(user_id) AS hll_unique_users');
    });

    test('TC-1.5: Non-additive count_distinct with includeHll: false falls back to distinct key retention', () => {
      const distinctResult = advisor.classifyAdditivity(
        { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
        { dialect: 'bigquery', includeHll: false }
      );

      expect(distinctResult.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(distinctResult.hll_supported).toBe(false);
      expect(distinctResult.decomposition).toContain('Two-tier exact distinct count');
    });

    test('TC-1.6: Non-additive compound ratio / derived metrics decompose into base additive sums', () => {
      const ratioResult = advisor.classifyAdditivity({
        name: 'profit_margin',
        agg: 'ratio',
        formula: 'gross_profit / revenue',
      });

      expect(ratioResult.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(ratioResult.sql_expression).toContain('SUM(gross_profit) AS sum_profit_margin_num');
      expect(ratioResult.sql_expression).toContain('SUM(revenue) AS sum_profit_margin_den');
      expect(ratioResult.rollup_expression).toBe('SUM(sum_profit_margin_num) / NULLIF(SUM(sum_profit_margin_den), 0)');
    });

    test('TC-1.7: Classifies string inputs and custom object aliases gracefully', () => {
      const stringResult = advisor.classifyAdditivity('sum', { name: 'sales', column: 'sales_amount' });
      expect(stringResult.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);
      expect(stringResult.sql_expression).toBe('SUM(sales_amount) AS sum_sales');

      const unknownResult = advisor.classifyAdditivity({ name: 'custom_metric', agg: 'unknown_aggregation' });
      expect(unknownResult.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE); // treated as standard sum fallback
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 2: Multi-Dialect Date Truncation Engine
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 2: Multi-Dialect Date Truncation Engine', () => {
    const advisor = new DbtPreaggAdvisor();

    test('TC-2.1: PostgreSQL date truncation across all 6 grains', () => {
      expect(advisor.getDateTruncSql('postgres', 'order_date', 'hour')).toBe("DATE_TRUNC('hour', order_date)");
      expect(advisor.getDateTruncSql('postgres', 'order_date', 'day')).toBe("DATE_TRUNC('day', order_date)");
      expect(advisor.getDateTruncSql('postgres', 'order_date', 'week')).toBe("DATE_TRUNC('week', order_date)");
      expect(advisor.getDateTruncSql('postgres', 'order_date', 'month')).toBe("DATE_TRUNC('month', order_date)");
      expect(advisor.getDateTruncSql('postgres', 'order_date', 'quarter')).toBe("DATE_TRUNC('quarter', order_date)");
      expect(advisor.getDateTruncSql('postgres', 'order_date', 'year')).toBe("DATE_TRUNC('year', order_date)");
    });

    test('TC-2.2: Google BigQuery date & timestamp truncation', () => {
      expect(advisor.getDateTruncSql('bigquery', 'order_date', 'hour')).toBe('TIMESTAMP_TRUNC(order_date, HOUR)');
      expect(advisor.getDateTruncSql('bigquery', 'order_date', 'day')).toBe('DATE_TRUNC(order_date, DAY)');
      expect(advisor.getDateTruncSql('bigquery', 'order_date', 'week')).toBe('DATE_TRUNC(order_date, WEEK)');
      expect(advisor.getDateTruncSql('bigquery', 'order_date', 'month')).toBe('DATE_TRUNC(order_date, MONTH)');
      expect(advisor.getDateTruncSql('bigquery', 'order_date', 'quarter')).toBe('DATE_TRUNC(order_date, QUARTER)');
      expect(advisor.getDateTruncSql('bigquery', 'order_date', 'year')).toBe('DATE_TRUNC(order_date, YEAR)');
    });

    test('TC-2.3: Snowflake date truncation syntax', () => {
      expect(advisor.getDateTruncSql('snowflake', 'created_at', 'month')).toBe("DATE_TRUNC('month', created_at)");
      expect(advisor.getDateTruncSql('snowflake', 'created_at', 'day')).toBe("DATE_TRUNC('day', created_at)");
    });

    test('TC-2.4: ClickHouse start-of-period functions', () => {
      expect(advisor.getDateTruncSql('clickhouse', 'event_time', 'hour')).toBe('toStartOfHour(event_time)');
      expect(advisor.getDateTruncSql('clickhouse', 'event_time', 'day')).toBe('toStartOfDay(event_time)');
      expect(advisor.getDateTruncSql('clickhouse', 'event_time', 'week')).toBe('toStartOfWeek(event_time)');
      expect(advisor.getDateTruncSql('clickhouse', 'event_time', 'month')).toBe('toStartOfMonth(event_time)');
      expect(advisor.getDateTruncSql('clickhouse', 'event_time', 'quarter')).toBe('toStartOfQuarter(event_time)');
      expect(advisor.getDateTruncSql('clickhouse', 'event_time', 'year')).toBe('toStartOfYear(event_time)');
    });

    test('TC-2.5: DuckDB date truncation syntax', () => {
      expect(advisor.getDateTruncSql('duckdb', 'order_date', 'day')).toBe("DATE_TRUNC('day', order_date)");
      expect(advisor.getDateTruncSql('duckdb', 'order_date', 'month')).toBe("DATE_TRUNC('month', order_date)");
    });

    test('TC-2.6: MySQL date and period formatting functions', () => {
      expect(advisor.getDateTruncSql('mysql', 'order_date', 'hour')).toBe("DATE_FORMAT(order_date, '%Y-%m-%d %H:00:00')");
      expect(advisor.getDateTruncSql('mysql', 'order_date', 'day')).toBe('DATE(order_date)');
      expect(advisor.getDateTruncSql('mysql', 'order_date', 'month')).toBe("DATE_FORMAT(order_date, '%Y-%m-01')");
      expect(advisor.getDateTruncSql('mysql', 'order_date', 'year')).toBe("DATE_FORMAT(order_date, '%Y-01-01')");
    });

    test('TC-2.7: Amazon Redshift date truncation syntax', () => {
      expect(advisor.getDateTruncSql('redshift', 'order_date', 'quarter')).toBe("DATE_TRUNC('quarter', order_date)");
    });

    test('TC-2.8: Normalizes dialect names and grain aliases', () => {
      expect(normalizeDialect('PostgreSQL')).toBe('postgres');
      expect(normalizeDialect('bq')).toBe('bigquery');
      expect(normalizeDialect('SF')).toBe('snowflake');
      expect(normalizeDialect('ch')).toBe('clickhouse');
      expect(normalizeDialect('DUCK')).toBe('duckdb');
      expect(normalizeDialect('mariadb')).toBe('mysql');
      expect(normalizeTimeGrain('daily')).toBe('day');
      expect(normalizeTimeGrain('monthly')).toBe('month');
      expect(normalizeTimeGrain('weekly')).toBe('week');
      expect(normalizeTimeGrain('yearly')).toBe('year');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 3: Multi-Dialect Materialized View & Rollup DDL Generation
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 3: Multi-Dialect Materialized View & Rollup DDL Generation', () => {
    const advisor = new DbtPreaggAdvisor();

    test('TC-3.1: PostgreSQL DDL compiles materialized view, unique index, and concurrent refresh', () => {
      const result = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'postgres',
        targetSchema: 'analytics_preagg',
        timeGrain: 'month',
        timeDimension: 'order_date',
        dimensions: ['status', 'customer_country'],
        measures: [
          { name: 'total_amount', agg: 'sum', column: 'amount' },
          { name: 'order_count', agg: 'count', column: '*' },
        ],
      });

      expect(result.ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_preagg.fct_orders_preagg_month AS');
      expect(result.ddl).toContain("DATE_TRUNC('month', order_date) AS order_date_month");
      expect(result.ddl).toContain('status,\n  customer_country');
      expect(result.ddl).toContain('GROUP BY 1, 2, 3;');
      expect(result.index_ddl).toHaveLength(2);
      expect(result.index_ddl[0]).toContain('CREATE UNIQUE INDEX IF NOT EXISTS uidx_fct_orders_preagg_month');
      expect(result.index_ddl[1]).toContain('CREATE INDEX IF NOT EXISTS idx_fct_orders_preagg_month_time');
      expect(result.refresh_command).toBe('REFRESH MATERIALIZED VIEW CONCURRENTLY analytics_preagg.fct_orders_preagg_month;');
    });

    test('TC-3.2: Google BigQuery DDL compiles OPTIONS, PARTITION BY, and CLUSTER BY', () => {
      const result = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'bigquery',
        targetSchema: 'analytics_preagg',
        timeGrain: 'day',
        timeDimension: 'order_date',
        dimensions: ['status', 'region'],
        refreshIntervalMinutes: 60,
      });

      expect(result.ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS `analytics_preagg.fct_orders_preagg_day`');
      expect(result.ddl).toContain('enable_refresh = true');
      expect(result.ddl).toContain('refresh_interval_minutes = 60');
      expect(result.ddl).toContain('PARTITION BY DATE(order_date_day)');
      expect(result.ddl).toContain('CLUSTER BY status, region');
      expect(result.refresh_command).toContain('CALL BQ.REFRESH_MATERIALIZED_VIEW');
    });

    test('TC-3.3: BigQuery cluster column limit: enforces maximum 4 clustering columns', () => {
      const result = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'bigquery',
        dimensions: ['dim1', 'dim2', 'dim3', 'dim4', 'dim5', 'dim6'],
      });

      expect(result.ddl).toContain('CLUSTER BY dim1, dim2, dim3, dim4');
      expect(result.ddl).not.toContain('CLUSTER BY dim1, dim2, dim3, dim4, dim5');
    });

    test('TC-3.4: Snowflake DDL compiles materialized view with CLUSTER BY', () => {
      const result = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'snowflake',
        targetSchema: 'preagg',
        timeGrain: 'month',
        timeDimension: 'order_date',
        dimensions: ['status', 'category'],
      });

      expect(result.ddl).toContain('CREATE OR REPLACE MATERIALIZED VIEW preagg.fct_orders_preagg_month');
      expect(result.ddl).toContain('CLUSTER BY (order_date_month, status, category)');
      expect(result.refresh_strategy).toBe('auto');
    });

    test('TC-3.5: ClickHouse DDL compiles SummingMergeTree Table + Materialized View', () => {
      const result = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'clickhouse',
        targetSchema: 'analytics',
        timeGrain: 'day',
        timeDimension: 'order_date',
        dimensions: ['status', 'country'],
        measures: [
          { name: 'amount', agg: 'sum', column: 'amount' },
          { name: 'orders', agg: 'count', column: '*' },
        ],
      });

      expect(result.ddl).toContain('CREATE TABLE IF NOT EXISTS analytics.fct_orders_preagg_day');
      expect(result.ddl).toContain('ENGINE = SummingMergeTree((sum_amount, count_orders))');
      expect(result.ddl).toContain('PRIMARY KEY (order_date_day, status, country)');
      expect(result.ddl).toContain('CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.fct_orders_preagg_day_mv');
      expect(result.ddl).toContain('TO analytics.fct_orders_preagg_day');
    });

    test('TC-3.6: DuckDB DDL compiles table and index creation', () => {
      const result = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'duckdb',
        targetSchema: 'preagg',
        dimensions: ['status'],
      });

      expect(result.ddl).toContain('CREATE OR REPLACE TABLE preagg.fct_orders_preagg_day AS');
      expect(result.index_ddl).toHaveLength(1);
      expect(result.index_ddl[0]).toContain('CREATE INDEX IF NOT EXISTS idx_fct_orders_preagg_day');
    });

    test('TC-3.7: MySQL DDL compiles InnoDB table and scheduled upsert refresh', () => {
      const result = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'mysql',
        targetSchema: 'preagg',
        dimensions: ['status', 'category'],
      });

      expect(result.ddl).toContain('CREATE TABLE IF NOT EXISTS `preagg`.`fct_orders_preagg_day` (');
      expect(result.ddl).toContain('PRIMARY KEY (`order_date_day`, `status`, `category`)');
      expect(result.refresh_command).toContain('INSERT INTO `preagg`.`fct_orders_preagg_day`');
      expect(result.refresh_command).toContain('ON DUPLICATE KEY UPDATE');
    });

    test('TC-3.8: Amazon Redshift DDL compiles AUTO REFRESH YES, DISTKEY, and SORTKEY', () => {
      const result = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'redshift',
        targetSchema: 'preagg',
        timeGrain: 'month',
        dimensions: ['status', 'customer_id'],
      });

      expect(result.ddl).toContain('CREATE MATERIALIZED VIEW preagg.fct_orders_preagg_month');
      expect(result.ddl).toContain('AUTO REFRESH YES');
      expect(result.ddl).toContain('DISTKEY(status)');
      expect(result.ddl).toContain('SORTKEY(order_date_month, status, customer_id)');
      expect(result.refresh_command).toBe('REFRESH MATERIALIZED VIEW preagg.fct_orders_preagg_month;');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 4: Query Acceleration, Scan Reduction & Performance Heuristics
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 4: Query Acceleration, Scan Reduction & Performance Heuristics', () => {
    const advisor = new DbtPreaggAdvisor();

    test('TC-4.1: Speedup estimator calculates row reduction, speedup factor, and data scan reduction', () => {
      const estimate = advisor.estimateSpeedup(1000000, 'day', ['status', 'category']);

      expect(estimate.raw_rows).toBe(1000000);
      expect(estimate.preagg_rows).toBeLessThan(100000);
      expect(estimate.speedup_factor).toBeGreaterThan(10);
      expect(estimate.scan_reduction_pct).toBeGreaterThan(90);
      expect(estimate.speedup_label).toMatch(/\d+x/);
      expect(estimate.bytes_saved_est).toContain('MB');
    });

    test('TC-4.2: Speedup estimator handles monthly, yearly, and hourly time grains', () => {
      const hourlyEst = advisor.estimateSpeedup(1000000, 'hour', ['status']);
      const monthlyEst = advisor.estimateSpeedup(1000000, 'month', ['status']);
      const yearlyEst = advisor.estimateSpeedup(1000000, 'year', ['status']);

      expect(monthlyEst.speedup_factor).toBeGreaterThan(hourlyEst.speedup_factor);
      expect(yearlyEst.speedup_factor).toBeGreaterThan(monthlyEst.speedup_factor);
    });

    test('TC-4.3: Speedup estimator incorporates explicit dimension cardinalities', () => {
      const lowCardEst = advisor.estimateSpeedup(1000000, 'day', [{ name: 'gender', cardinality: 2 }]);
      const highCardEst = advisor.estimateSpeedup(1000000, 'day', [{ name: 'user_uuid', cardinality: 50000 }]);

      expect(lowCardEst.preagg_rows).toBeLessThan(highCardEst.preagg_rows);
      expect(lowCardEst.speedup_factor).toBeGreaterThan(highCardEst.speedup_factor);
    });

    test('TC-4.4: Speedup estimator caps pre-aggregation rows at raw row count', () => {
      const hugeDims = [
        { name: 'dim1', cardinality: 1000 },
        { name: 'dim2', cardinality: 1000 },
        { name: 'dim3', cardinality: 1000 },
      ];
      const estimate = advisor.estimateSpeedup(50000, 'day', hugeDims);

      expect(estimate.preagg_rows).toBeLessThanOrEqual(50000);
      expect(estimate.speedup_factor).toBe(1.0);
    });

    test('TC-4.5: formatBytes formats byte units accurately', () => {
      expect(formatBytes(0)).toBe('0 B');
      expect(formatBytes(1024)).toBe('1.0 KB');
      expect(formatBytes(1048576 * 50)).toBe('50.0 MB');
      expect(formatBytes(1073741824 * 2.5)).toBe('2.5 GB');
    });

    test('TC-4.6: Generates query acceleration rewrite patterns and routing rules', () => {
      const queryRewriter = advisor.generateAcceleratedQuery(
        'fct_orders',
        'preagg',
        'fct_orders_preagg_day',
        'order_date',
        'day',
        ['status'],
        [
          { name: 'total_amount', agg: 'sum', rollup_expression: 'SUM(sum_total_amount)' },
          { name: 'avg_price', agg: 'avg', rollup_expression: 'SUM(sum_avg_price) / NULLIF(SUM(count_avg_price), 0)' },
        ],
        'postgres'
      );

      expect(queryRewriter.original_query_pattern).toContain('FROM fct_orders');
      expect(queryRewriter.accelerated_query_pattern).toContain('FROM preagg.fct_orders_preagg_day');
      expect(queryRewriter.accelerated_query_pattern).toContain('SUM(sum_total_amount) AS total_amount');
      expect(queryRewriter.accelerated_query_pattern).toContain('SUM(sum_avg_price) / NULLIF(SUM(count_avg_price), 0) AS avg_price');
      expect(queryRewriter.routing_rule).toContain('automatically routed to pre-aggregation "preagg.fct_orders_preagg_day"');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 5: Autonomous Model Discovery & MetricFlow Integration
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 5: Autonomous Model Discovery & MetricFlow Integration', () => {
    test('TC-5.1: Automatically discovers fact models and column schemas from scanResult', () => {
      const scanResult = {
        projectDir: '/test/dbt',
        manifestLoaded: true,
        catalogLoaded: true,
        models: [
          {
            name: 'fct_orders',
            tier: 'marts_fact',
            columns: {
              order_date: { type: 'date' },
              status: { type: 'varchar' },
              customer_country: { type: 'varchar' },
              order_amount: { type: 'numeric' },
            },
          },
          {
            name: 'stg_orders',
            tier: 'staging',
            columns: { id: { type: 'integer' } },
          },
        ],
        catalogStats: {
          tables: {
            'model.test.fct_orders': {
              name: 'fct_orders',
              rowCount: 500000,
              bytes: 64000000,
            },
          },
        },
      };

      const advisor = new DbtPreaggAdvisor(scanResult);
      const recommendations = advisor.advisePreaggregations({
        dialect: 'postgres',
        time_grain: 'month',
      });

      expect(recommendations.length).toBeGreaterThan(0);
      expect(recommendations[0].model).toBe('fct_orders');
      expect(recommendations[0].time_dimension).toBe('order_date');
      expect(recommendations[0].time_grain).toBe('month');
      expect(recommendations[0].dimensions).toContain('status');
      expect(recommendations[0].speedup_estimate.raw_rows).toBe(500000);
    });

    test('TC-5.2: Generates multi-tier rollups when time_grains array is provided', () => {
      const scanResult = {
        models: [
          {
            name: 'fct_sales',
            tier: 'marts_fact',
            columns: {
              transaction_date: { type: 'date' },
              channel: { type: 'varchar' },
              revenue: { type: 'numeric' },
            },
          },
        ],
      };

      const advisor = new DbtPreaggAdvisor(scanResult);
      const recommendations = advisor.advisePreaggregations({
        model_name: 'fct_sales',
        time_grains: ['day', 'month'],
      });

      expect(recommendations).toHaveLength(2);
      expect(recommendations[0].time_grain).toBe('day');
      expect(recommendations[1].time_grain).toBe('month');
      expect(recommendations[0].name).toBe('fct_sales_preagg_day');
      expect(recommendations[1].name).toBe('fct_sales_preagg_month');
    });

    test('TC-5.3: Ingests MetricFlow metrics from scanResult.metrics', () => {
      const scanResult = {
        models: [{ name: 'fct_orders', columns: { order_date: {}, amount: {} } }],
        metrics: [
          { name: 'total_revenue', type: 'simple', model: 'fct_orders', agg: 'sum', column: 'amount' },
          { name: 'avg_order_value', type: 'simple', model: 'fct_orders', agg: 'avg', column: 'amount' },
        ],
      };

      const advisor = new DbtPreaggAdvisor(scanResult);
      const analysis = advisor.analyzeMetricAdditivity('fct_orders');

      expect(analysis).toHaveLength(2);
      expect(analysis[0].metric_name).toBe('total_revenue');
      expect(analysis[0].additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);
      expect(analysis[1].metric_name).toBe('avg_order_value');
      expect(analysis[1].additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
    });

    test('TC-5.4: Fallback heuristic estimation when catalogStats is absent', () => {
      const scanResult = {
        catalogLoaded: false,
        models: [{ name: 'fct_orders', columns: { order_date: {} } }],
      };

      const advisor = new DbtPreaggAdvisor(scanResult);
      const recommendations = advisor.advisePreaggregations({ model_name: 'fct_orders' });

      expect(recommendations[0].speedup_estimate.raw_rows).toBe(1000000);
      expect(recommendations[0].speedup_estimate.is_heuristic_estimate).toBe(true);
    });

    test('TC-5.5: Filters out models when estimated speedup is below minSpeedupFactor', () => {
      const advisor = new DbtPreaggAdvisor();
      const recommendations = advisor.advisePreaggregations({
        model_name: 'fct_orders',
        dimensions: ['dim1', 'dim2', 'dim3', 'dim4'], // high cardinality combo
        min_speedup_factor: 10000.0, // impossibly high threshold
      });

      expect(recommendations).toHaveLength(0);
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // GROUP 6: MCP Tool Registry & Handler End-to-End
  // ══════════════════════════════════════════════════════════════════════════
  describe('Group 6: MCP Tool Registry & Handler End-to-End', () => {
    test('TC-6.1: Tool registry registers dbt_semantic_preagg_advisor with readOnlyHint: true and valid inputSchema', () => {
      const tools = getToolDefinitions();
      const tool = tools.find(t => t.name === 'dbt_semantic_preagg_advisor');

      expect(tool).toBeDefined();
      expect(tool.readOnlyHint).toBe(true);
      expect(tool.inputSchema).toBeDefined();
      expect(tool.inputSchema.properties.dialect).toBeDefined();
      expect(tool.inputSchema.properties.time_grain).toBeDefined();
      expect(tool.inputSchema.properties.target_schema).toBeDefined();
    });

    test('TC-6.2: TOOL_METADATA defines dbt_semantic_preagg_advisor with MCP 2025-11-25 outputSchema', () => {
      const meta = TOOL_METADATA.dbt_semantic_preagg_advisor;

      expect(meta).toBeDefined();
      expect(meta.title).toBe('Cube.js Pre-Aggregation & Rollup Advisor');
      expect(meta.outputSchema).toBeDefined();
      expect(meta.outputSchema.properties.recommendations).toBeDefined();
      expect(meta.outputSchema.properties._provenance).toBeDefined();
    });

    test('TC-6.3: handleDbtSemanticPreaggAdvisor returns formatted markdown and structuredContent payload', async () => {
      // Set up minimal dbt project structure
      const dbtProjectYaml = 'name: test_shop\nversion: 1.0.0\n';
      fs.writeFileSync(path.join(tempDir, 'dbt_project.yml'), dbtProjectYaml);

      const targetDir = path.join(tempDir, 'target');
      fs.mkdirSync(targetDir, { recursive: true });

      const manifest = {
        metadata: { dbt_version: '1.7.0' },
        nodes: {
          'model.test.fct_orders': {
            name: 'fct_orders',
            resource_type: 'model',
            package_name: 'test',
            original_file_path: 'models/marts/fct_orders.sql',
            columns: {
              order_date: { name: 'order_date', data_type: 'date' },
              status: { name: 'status', data_type: 'varchar' },
              customer_country: { name: 'customer_country', data_type: 'varchar' },
              order_amount: { name: 'order_amount', data_type: 'numeric' },
            },
          },
        },
        sources: {},
        metrics: {},
        semantic_models: {},
        exposures: {},
      };
      fs.writeFileSync(path.join(targetDir, 'manifest.json'), JSON.stringify(manifest));

      const handler = new DbtSemanticHandler(null, null, null);
      const response = await handler.handleDbtSemanticPreaggAdvisor({
        project_dir: tempDir,
        model_name: 'fct_orders',
        dialect: 'bigquery',
        time_grain: 'month',
      });

      expect(response.isError).toBeFalsy();
      expect(response.content).toHaveLength(1);
      expect(response.content[0].text).toContain('CUBE.JS PRE-AGGREGATION & ROLLUP ADVISOR');
      expect(response.content[0].text).toContain('BIGQUERY');
      expect(response.structuredContent).toBeDefined();
      expect(response.structuredContent.model_name).toBe('fct_orders');
      expect(response.structuredContent.dialect).toBe('bigquery');
      expect(response.structuredContent.recommendations).toHaveLength(1);
      expect(response.structuredContent.recommendations[0].ddl).toContain('CREATE MATERIALIZED VIEW');
    });

    test('TC-6.4: Provenance metadata contains READ_ONLY_ADVISORY governance stamp', async () => {
      fs.writeFileSync(path.join(tempDir, 'dbt_project.yml'), 'name: test_pkg\n');
      const handler = new DbtSemanticHandler(null, null, null);

      const response = await handler.handleDbtSemanticPreaggAdvisor({
        project_dir: tempDir,
        dialect: 'snowflake',
      });

      expect(response.structuredContent._provenance).toBeDefined();
      expect(response.structuredContent._provenance.governance_level).toBe('READ_ONLY_ADVISORY');
      expect(response.structuredContent._provenance.advisor).toBe('DbtPreaggAdvisor');
      expect(response.structuredContent._provenance.dialect).toBe('snowflake');
      expect(response.structuredContent._provenance.timestamp).toBeDefined();
    });

    test('TC-6.5: Handler handles missing project directory gracefully with error response', async () => {
      const handler = new DbtSemanticHandler(null, null, null);
      const response = await handler.handleDbtSemanticPreaggAdvisor({
        project_dir: '/invalid/directory/path/that/does/not/exist',
      });

      expect(response.isError).toBe(true);
      expect(response.content[0].text).toContain('Pre-Aggregation Advisor Error');
    });

    test('TC-6.6: DbtSemanticHandler.routes() maps dbt_semantic_preagg_advisor', () => {
      const handler = new DbtSemanticHandler(null, null, null);
      const routes = handler.routes();

      expect(routes.dbt_semantic_preagg_advisor).toBeDefined();
      expect(typeof routes.dbt_semantic_preagg_advisor).toBe('function');
    });
  });
});
