/**
 * tests/unit/challenger-m3-preagg.test.js
 * Adversarial & Boundary Stress Tests for DbtPreaggAdvisor
 */

import {
  DbtPreaggAdvisor,
  normalizeDialect,
  normalizeTimeGrain,
  formatBytes,
  ADDITIVITY_TYPES,
} from '../../src/dbt/preagg-advisor.js';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';

describe('Adversarial & Boundary Stress Tests for DbtPreaggAdvisor', () => {
  const advisor = new DbtPreaggAdvisor();

  describe('Adversarial Dialect & Time Grain Normalization', () => {
    test('Handles malformed, unusual casing, and unknown dialects safely', () => {
      expect(normalizeDialect(null)).toBe('postgres');
      expect(normalizeDialect(undefined)).toBe('postgres');
      expect(normalizeDialect('')).toBe('postgres');
      expect(normalizeDialect(123)).toBe('postgres');
      expect(normalizeDialect('  BIGQUERY  ')).toBe('bigquery');
      expect(normalizeDialect('unknown_db_engine')).toBe('postgres');
      expect(normalizeDialect('mariadb')).toBe('mysql');
      expect(normalizeDialect('sf')).toBe('snowflake');
      expect(normalizeDialect('ch')).toBe('clickhouse');
      expect(normalizeDialect('duck')).toBe('duckdb');
      expect(normalizeDialect('rs')).toBe('redshift');
    });

    test('Handles malformed, unusual casing, and unknown time grains safely', () => {
      expect(normalizeTimeGrain(null)).toBe('day');
      expect(normalizeTimeGrain(undefined)).toBe('day');
      expect(normalizeTimeGrain('')).toBe('day');
      expect(normalizeTimeGrain(456)).toBe('day');
      expect(normalizeTimeGrain('  MONTHLY  ')).toBe('month');
      expect(normalizeTimeGrain('annually')).toBe('year');
      expect(normalizeTimeGrain('unknown_grain')).toBe('day');
    });
  });

  describe('Adversarial Additivity Classification', () => {
    test('Handles complex derived metric expressions', () => {
      const metric = {
        name: 'roas',
        agg: 'derived',
        expr: 'total_revenue / ad_spend',
      };
      const classified = advisor.classifyAdditivity(metric);
      expect(classified.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(classified.sql_expression).toContain('SUM(total_revenue) AS sum_roas_num');
      expect(classified.sql_expression).toContain('SUM(ad_spend) AS sum_roas_den');
      expect(classified.rollup_expression).toBe('SUM(sum_roas_num) / NULLIF(SUM(sum_roas_den), 0)');
    });

    test('Handles missing column and null object safely', () => {
      const result1 = advisor.classifyAdditivity(null);
      expect(result1.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);

      const result2 = advisor.classifyAdditivity({});
      expect(result2.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);
      expect(result2.name).toBe('measure');
    });

    test('Handles cumulative snapshots without non_additive_dimension explicitly provided', () => {
      const snap = {
        name: 'daily_closing_balance',
        agg: 'cumulative_snapshot',
        column: 'balance_amount',
      };
      const result = advisor.classifyAdditivity(snap);
      expect(result.additivity).toBe(ADDITIVITY_TYPES.SEMI_ADDITIVE);
      expect(result.non_additive_dimension).toBeDefined();
      expect(result.non_additive_dimension.name).toBe('snapshot_date');
    });

    test('PostgreSQL HLL compilation produces valid hll_add_agg / hll_cardinality expressions', () => {
      const pgHll = advisor.classifyAdditivity(
        { name: 'distinct_sessions', agg: 'count_distinct', column: 'session_id' },
        { dialect: 'postgres', includeHll: true }
      );
      expect(pgHll.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(pgHll.hll_supported).toBe(true);
      expect(pgHll.sql_expression).toBe('hll_add_agg(hll_hash_text(session_id::text)) AS hll_distinct_sessions');
      expect(pgHll.rollup_expression).toBe('hll_cardinality(hll_union_agg(hll_distinct_sessions))');
    });
  });

  describe('Adversarial Speedup Estimation & Edge Cases', () => {
    test('Handles zero and negative raw row counts gracefully', () => {
      const zeroEst = advisor.estimateSpeedup(0, 'day', ['status']);
      expect(zeroEst.raw_rows).toBe(1000000); // defaults to 1M

      const negEst = advisor.estimateSpeedup(-500, 'day', ['status']);
      expect(negEst.raw_rows).toBe(1000000);
    });

    test('Handles empty dimensions and non-temporal grains', () => {
      const est = advisor.estimateSpeedup(1000000, 'none', []);
      expect(est.preagg_rows).toBe(1);
      expect(est.speedup_factor).toBe(10000); // capped at 10000
    });

    test('Handles dimension names matching various heuristic patterns', () => {
      const dayDims = [
        'order_status',
        'user_device',
      ];
      const dayEst = advisor.estimateSpeedup(1000000, 'day', dayDims);
      expect(dayEst.speedup_factor).toBeGreaterThan(10);
      expect(dayEst.scan_reduction_pct).toBeGreaterThan(80);

      const monthDims = [
        'order_status',
        'customer_region',
        'user_device',
      ];
      const monthEst = advisor.estimateSpeedup(1000000, 'month', monthDims);
      expect(monthEst.speedup_factor).toBeGreaterThan(10);
      expect(monthEst.scan_reduction_pct).toBeGreaterThan(80);

      // Slicing with high cardinality unique identifier correctly caps speedup at 1.0x
      const highCardDims = ['order_status', 'customer_id'];
      const highCardEst = advisor.estimateSpeedup(1000000, 'day', highCardDims);
      expect(highCardEst.speedup_factor).toBe(1.0);
    });
  });

  describe('Adversarial Multi-Dialect DDL Generation', () => {
    test('Generates DDL with complex custom schemas and measures', () => {
      const ddlRes = advisor.generateRollupDDL({
        model: 'marts_fact_orders',
        sourceTable: 'raw_db.raw_schema.orders',
        targetSchema: 'custom_mvs',
        dialect: 'bigquery',
        timeDimension: 'created_timestamp',
        timeGrain: 'hour',
        dimensions: ['store_id', 'tier', 'device_category'],
        measures: [
          { name: 'revenue', agg: 'sum', column: 'amount' },
          { name: 'avg_price', agg: 'avg', column: 'price' },
          { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
        ],
      });

      expect(ddlRes.ddl).toContain('TIMESTAMP_TRUNC(created_timestamp, HOUR) AS created_timestamp_hour');
      expect(ddlRes.ddl).toContain('HLL_COUNT.INIT(user_id, 14) AS hll_unique_users');
      expect(ddlRes.ddl).toContain('PARTITION BY DATE(created_timestamp_hour)');
      expect(ddlRes.ddl).toContain('CLUSTER BY store_id, tier, device_category');
      expect(ddlRes.ddl).toContain('FROM `raw_db.raw_schema.orders`');
    });

    test('Generates ClickHouse DDL with HLL AggregateFunction and SummingMergeTree', () => {
      const ddlRes = advisor.generateRollupDDL({
        model: 'fct_events',
        dialect: 'clickhouse',
        targetSchema: 'mvs',
        timeGrain: 'day',
        timeDimension: 'event_time',
        dimensions: ['country_code'],
        measures: [
          { name: 'event_count', agg: 'count', column: '*' },
          { name: 'amount', agg: 'sum', column: 'amount' },
          { name: 'unique_visitors', agg: 'count_distinct', column: 'visitor_id' },
        ],
      });

      expect(ddlRes.ddl).toContain('CREATE TABLE IF NOT EXISTS mvs.fct_events_preagg_day');
      expect(ddlRes.ddl).toContain('toStartOfDay(event_time) AS event_time_day');
      expect(ddlRes.ddl).toContain('hll_unique_visitors AggregateFunction(uniqCombined, String)');
      expect(ddlRes.ddl).toContain('ENGINE = SummingMergeTree((count_event_count, sum_amount))');
    });
  });

  describe('Adversarial MCP Handler Edge Cases', () => {
    test('Handler executes successfully with null optional arguments', async () => {
      const handler = new DbtSemanticHandler(null, null, null);
      const res = await handler.handleDbtSemanticPreaggAdvisor({});
      expect(res.isError).toBeFalsy();
      expect(res.structuredContent).toBeDefined();
      expect(res.structuredContent.recommendations).toBeDefined();
    });
  });
});
