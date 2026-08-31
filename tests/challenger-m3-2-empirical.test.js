/**
 * tests/challenger-m3-2-empirical.test.js
 * Adversarial & Empirical Stress Test Suite for Challenger M3-2
 * Focus: Non-Additive Metrics, Metric Decomposition, HLL Sketches vs 2-Tier Rollups,
 * and Scale Acceleration Stability across Extreme Cardinalities & Row Counts.
 */

import {
  DbtPreaggAdvisor,
  SUPPORTED_DIALECTS,
  SUPPORTED_GRAINS,
  ADDITIVITY_TYPES,
  normalizeDialect,
  normalizeTimeGrain,
  formatBytes,
} from '../src/dbt/preagg-advisor.js';

describe('Challenger M3-2: Non-Additive Metrics & Scale Acceleration Empirical Suite', () => {
  const advisor = new DbtPreaggAdvisor();

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 1: NON-ADDITIVE DISTINCT COUNT HANDLING (HLL vs 2-Tier Rollups)
  // ══════════════════════════════════════════════════════════════════════════
  describe('1. Non-Additive Distinct Count & HyperLogLog Dialect Parity', () => {
    test('1.1: HLL sketch generation across all 7 supported SQL dialects', () => {
      const dialects = [
        { dialect: 'postgres', expectedSql: 'hll_add_agg(hll_hash_text(user_id::text)) AS hll_unique_users', expectedRollup: 'hll_cardinality(hll_union_agg(hll_unique_users))' },
        { dialect: 'bigquery', expectedSql: 'HLL_COUNT.INIT(user_id, 14) AS hll_unique_users', expectedRollup: 'HLL_COUNT.MERGE(hll_unique_users)' },
        { dialect: 'snowflake', expectedSql: 'HLL_ACCUMULATE(user_id) AS hll_unique_users', expectedRollup: 'HLL_ESTIMATE(HLL_COMBINE(hll_unique_users))' },
        { dialect: 'clickhouse', expectedSql: 'uniqCombinedState(user_id) AS hll_unique_users', expectedRollup: 'uniqCombinedMerge(hll_unique_users)' },
        { dialect: 'duckdb', expectedSql: 'approx_count_distinct(user_id) AS approx_unique_users', expectedRollup: 'approx_count_distinct(user_id)' },
        { dialect: 'redshift', expectedSql: 'hyperloglog(user_id) AS hll_unique_users', expectedRollup: 'hyperloglog_count(hll_unique_users)' },
        { dialect: 'mysql', expectedSql: 'COUNT(DISTINCT user_id) AS count_distinct_unique_users', expectedRollup: 'COUNT(DISTINCT user_id)' },
      ];

      for (const d of dialects) {
        const res = advisor.classifyAdditivity(
          { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
          { dialect: d.dialect, includeHll: true }
        );

        expect(res.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
        expect(res.sql_expression).toBe(d.expectedSql);
        expect(res.rollup_expression).toBe(d.expectedRollup);
        expect(res.hll_supported).toBe(true);
        expect(res.decomposition).toBeDefined();
        expect(typeof res.decomposition).toBe('string');
      }
    });

    test('1.2: Distinct count aliases (distinct_count, unique, approx_count_distinct)', () => {
      const aliases = ['count_distinct', 'distinct_count', 'unique', 'approx_count_distinct'];
      for (const aggAlias of aliases) {
        const res = advisor.classifyAdditivity(
          { name: 'active_sessions', agg: aggAlias, column: 'session_id' },
          { dialect: 'bigquery', includeHll: true }
        );
        expect(res.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
        expect(res.sql_expression).toBe('HLL_COUNT.INIT(session_id, 14) AS hll_active_sessions');
        expect(res.rollup_expression).toBe('HLL_COUNT.MERGE(hll_active_sessions)');
      }
    });

    test('1.3: 2-Tier Rollup retention when includeHll is disabled (include_hll: false)', () => {
      for (const d of SUPPORTED_DIALECTS) {
        const res = advisor.classifyAdditivity(
          { name: 'unique_visitors', agg: 'count_distinct', column: 'visitor_id' },
          { dialect: d, includeHll: false }
        );

        expect(res.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
        expect(res.hll_supported).toBe(false);
        expect(res.sql_expression).toBe('COUNT(DISTINCT visitor_id) AS distinct_unique_visitors');
        expect(res.rollup_expression).toBe('COUNT(DISTINCT visitor_id)');
        expect(res.decomposition).toContain('Two-tier exact distinct count');
      }
    });

    test('1.4: ClickHouse SummingMergeTree table DDL compiles AggregateFunction for HLL', () => {
      const ddlResult = advisor.generateRollupDDL({
        model: 'fct_events',
        dialect: 'clickhouse',
        targetSchema: 'analytics',
        timeGrain: 'day',
        timeDimension: 'event_time',
        dimensions: ['country'],
        measures: [
          { name: 'event_count', agg: 'count', column: '*' },
          { name: 'unique_users', agg: 'count_distinct', column: 'user_id' },
        ],
      });

      expect(ddlResult.ddl).toContain('hll_unique_users AggregateFunction(uniqCombined, String)');
      expect(ddlResult.ddl).toContain('uniqCombinedState(user_id) AS hll_unique_users');
      expect(ddlResult.ddl).toContain('ENGINE = SummingMergeTree((count_event_count))');
    });

    test('1.5: Complex expressions in count_distinct (e.g. COALESCE, CONCAT)', () => {
      const res = advisor.classifyAdditivity(
        { name: 'distinct_composite', agg: 'count_distinct', column: "CONCAT(tenant_id, '-', user_id)" },
        { dialect: 'snowflake', includeHll: true }
      );

      expect(res.sql_expression).toBe("HLL_ACCUMULATE(CONCAT(tenant_id, '-', user_id)) AS hll_distinct_composite");
      expect(res.rollup_expression).toBe('HLL_ESTIMATE(HLL_COMBINE(hll_distinct_composite))');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 2: AVERAGE DECOMPOSITION & QUERY RECOMBINATION
  // ══════════════════════════════════════════════════════════════════════════
  describe('2. Average / Mean Metric Decomposition & Recombination Rewrites', () => {
    test('2.1: Correctly decomposes avg, average, mean into SUM and COUNT components', () => {
      const avgVariants = ['avg', 'average', 'mean', 'AVG', 'Average', 'MEAN'];
      for (const variant of avgVariants) {
        const res = advisor.classifyAdditivity({ name: 'order_value', agg: variant, column: 'amount' });
        expect(res.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
        expect(res.sql_expression).toBe('SUM(amount) AS sum_order_value, COUNT(amount) AS count_order_value');
        expect(res.rollup_expression).toBe('SUM(sum_order_value) / NULLIF(SUM(count_order_value), 0)');
        expect(res.columns).toHaveLength(2);
        expect(res.columns[0]).toEqual({
          name: 'sum_order_value',
          sql_expression: 'SUM(amount)',
          rollup_expression: 'SUM(sum_order_value)',
          type: 'sum',
        });
        expect(res.columns[1]).toEqual({
          name: 'count_order_value',
          sql_expression: 'COUNT(amount)',
          rollup_expression: 'SUM(count_order_value)',
          type: 'count',
        });
      }
    });

    test('2.2: Generates NULLIF division-by-zero protection in rollup expressions', () => {
      const res = advisor.classifyAdditivity({ name: 'latency_ms', agg: 'avg', column: 'duration' });
      expect(res.rollup_expression).toContain('NULLIF(SUM(count_latency_ms), 0)');
    });

    test('2.3: Accelerated query generation reconstructs avg via SUM/COUNT ratio', () => {
      const classifiedAvg = advisor.classifyAdditivity({ name: 'avg_price', agg: 'avg', column: 'price' });
      const queryRewriter = advisor.generateAcceleratedQuery(
        'fct_sales',
        'preagg',
        'fct_sales_preagg_month',
        'sale_date',
        'month',
        ['category'],
        [classifiedAvg],
        'postgres'
      );

      expect(queryRewriter.original_query_pattern).toContain('FROM fct_sales');
      expect(queryRewriter.accelerated_query_pattern).toContain('FROM preagg.fct_sales_preagg_month');
      expect(queryRewriter.accelerated_query_pattern).toContain('SUM(sum_avg_price) / NULLIF(SUM(count_avg_price), 0) AS avg_price');
    });

    test('2.4: Materialized view DDL for PostgreSQL and BigQuery includes both sum and count columns for avg', () => {
      const pgDdl = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'postgres',
        dimensions: ['status'],
        measures: [{ name: 'basket_size', agg: 'avg', column: 'item_count' }],
      });
      expect(pgDdl.ddl).toContain('SUM(item_count) AS sum_basket_size, COUNT(item_count) AS count_basket_size');

      const bqDdl = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'bigquery',
        dimensions: ['status'],
        measures: [{ name: 'basket_size', agg: 'avg', column: 'item_count' }],
      });
      expect(bqDdl.ddl).toContain('SUM(item_count) AS sum_basket_size, COUNT(item_count) AS count_basket_size');
    });

    test('2.5: ClickHouse DDL creates Float64 sum column and UInt64 count column for avg', () => {
      const chDdl = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'clickhouse',
        dimensions: ['status'],
        measures: [{ name: 'basket_size', agg: 'avg', column: 'item_count' }],
      });
      expect(chDdl.ddl).toContain('sum_basket_size Float64');
      expect(chDdl.ddl).toContain('count_basket_size UInt64');
    });

    test('2.6: MySQL DDL creates DECIMAL and BIGINT columns with ON DUPLICATE KEY UPDATE for avg', () => {
      const mysqlDdl = advisor.generateRollupDDL({
        model: 'fct_orders',
        dialect: 'mysql',
        dimensions: ['status'],
        measures: [{ name: 'basket_size', agg: 'avg', column: 'item_count' }],
      });
      expect(mysqlDdl.ddl).toContain('`sum_basket_size` DECIMAL(18,4)');
      expect(mysqlDdl.ddl).toContain('`count_basket_size` BIGINT');
      expect(mysqlDdl.refresh_command).toContain('`sum_basket_size` = VALUES(`sum_basket_size`)');
      expect(mysqlDdl.refresh_command).toContain('`count_basket_size` = VALUES(`count_basket_size`)');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 3: RATIO & DERIVED METRIC DECOMPOSITION
  // ══════════════════════════════════════════════════════════════════════════
  describe('3. Ratio & Derived Metric Decomposition into Base Measures', () => {
    test('3.1: Decomposes ratio metric with formula "profit / revenue" into base sums', () => {
      const res = advisor.classifyAdditivity({
        name: 'profit_margin',
        agg: 'ratio',
        formula: 'net_profit / gross_revenue',
      });

      expect(res.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(res.sql_expression).toBe('SUM(net_profit) AS sum_profit_margin_num, SUM(gross_revenue) AS sum_profit_margin_den');
      expect(res.rollup_expression).toBe('SUM(sum_profit_margin_num) / NULLIF(SUM(sum_profit_margin_den), 0)');
      expect(res.columns).toHaveLength(2);
      expect(res.columns[0].name).toBe('sum_profit_margin_num');
      expect(res.columns[1].name).toBe('sum_profit_margin_den');
    });

    test('3.2: Handles ratio with varied whitespace and expr property', () => {
      const res = advisor.classifyAdditivity({
        name: 'ctr',
        type: 'derived',
        expr: ' clicks   /   impressions ',
      });

      expect(res.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(res.sql_expression).toBe('SUM(clicks) AS sum_ctr_num, SUM(impressions) AS sum_ctr_den');
      expect(res.rollup_expression).toBe('SUM(sum_ctr_num) / NULLIF(SUM(sum_ctr_den), 0)');
    });

    test('3.3: Gracefully handles ratio metric when formula is absent', () => {
      const res = advisor.classifyAdditivity({
        name: 'conversion_rate',
        agg: 'ratio',
      });

      expect(res.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(res.sql_expression).toBe('SUM(numerator_conversion_rate) AS sum_conversion_rate_num, SUM(denominator_conversion_rate) AS sum_conversion_rate_den');
      expect(res.rollup_expression).toBe('SUM(sum_conversion_rate_num) / NULLIF(SUM(sum_conversion_rate_den), 0)');
    });

    test('3.4: Accelerated query generation reassembles ratio metric cleanly', () => {
      const classifiedRatio = advisor.classifyAdditivity({
        name: 'margin_rate',
        agg: 'ratio',
        formula: 'margin / total_sales',
      });

      const queryRewriter = advisor.generateAcceleratedQuery(
        'fct_finance',
        'preagg',
        'fct_finance_preagg_day',
        'trans_date',
        'day',
        ['dept_id'],
        [classifiedRatio],
        'postgres'
      );

      expect(queryRewriter.accelerated_query_pattern).toContain('SUM(sum_margin_rate_num) / NULLIF(SUM(sum_margin_rate_den), 0) AS margin_rate');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 4: SCALE ACCELERATION ON SMALL (<1k) VS MASSIVE (>100M) TABLES
  // ══════════════════════════════════════════════════════════════════════════
  describe('4. Scale Acceleration Factor Stability (Small <1K vs Massive >100M Tables)', () => {
    test('4.1: Tiny tables (<1,000 rows): 0 rows, 1 row, 10 rows, 100 rows, 500 rows', () => {
      const testCases = [
        { rows: 0, grain: 'day', dims: ['status'] },
        { rows: 1, grain: 'day', dims: ['status'] },
        { rows: 10, grain: 'day', dims: ['status'] },
        { rows: 100, grain: 'day', dims: ['status', 'category'] },
        { rows: 500, grain: 'day', dims: ['status', 'country'] },
        { rows: 999, grain: 'month', dims: ['status'] },
      ];

      for (const tc of testCases) {
        const est = advisor.estimateSpeedup(tc.rows, tc.grain, tc.dims);

        // Assert no NaN or undefined
        expect(isNaN(est.raw_rows)).toBe(false);
        expect(isNaN(est.preagg_rows)).toBe(false);
        expect(isNaN(est.speedup_factor)).toBe(false);
        expect(isNaN(est.scan_reduction_pct)).toBe(false);

        // Assert non-negative and finite
        expect(est.preagg_rows).toBeGreaterThanOrEqual(1);
        expect(est.speedup_factor).toBeGreaterThanOrEqual(1.0);
        expect(est.scan_reduction_pct).toBeGreaterThanOrEqual(0.0);
        expect(est.scan_reduction_pct).toBeLessThanOrEqual(99.9);
        expect(est.speedup_label).toMatch(/^\d+x$/);
        expect(est.bytes_saved_est).toBeDefined();

        // Rollup row count should never exceed raw rows (unless raw rows was 0, where fallback 1M applies)
        if (tc.rows > 0) {
          expect(est.preagg_rows).toBeLessThanOrEqual(tc.rows);
        }
      }
    });

    test('4.2: Massive tables (>100M to 10B rows): 100M, 500M, 1B, 10B rows', () => {
      const testCases = [
        { rows: 100_000_000, grain: 'day', dims: ['status', 'category'] },
        { rows: 500_000_000, grain: 'day', dims: ['status', 'country'] },
        { rows: 1_000_000_000, grain: 'month', dims: ['status'] },
        { rows: 10_000_000_000, grain: 'year', dims: ['status'] },
      ];

      for (const tc of testCases) {
        const est = advisor.estimateSpeedup(tc.rows, tc.grain, tc.dims);

        // Assert numbers are finite, not NaN, not Infinity
        expect(Number.isFinite(est.raw_rows)).toBe(true);
        expect(Number.isFinite(est.preagg_rows)).toBe(true);
        expect(Number.isFinite(est.speedup_factor)).toBe(true);
        expect(Number.isFinite(est.scan_reduction_pct)).toBe(true);

        // Assert strong speedup and reduction
        expect(est.speedup_factor).toBeGreaterThan(10);
        expect(est.scan_reduction_pct).toBeGreaterThanOrEqual(90.0);
        expect(est.scan_reduction_pct).toBeLessThanOrEqual(99.9);
        expect(est.preagg_rows).toBeLessThan(est.raw_rows);
      }
    });

    test('4.3: Edge-case inputs: negative numbers, strings, null, undefined, NaN', () => {
      const edgeCases = [
        { rows: -500, grain: 'day', dims: [] },
        { rows: '50000', grain: 'day', dims: [] },
        { rows: 'not_a_number', grain: 'day', dims: [] },
        { rows: null, grain: 'day', dims: [] },
        { rows: undefined, grain: 'day', dims: [] },
        { rows: NaN, grain: 'day', dims: [] },
      ];

      for (const tc of edgeCases) {
        const est = advisor.estimateSpeedup(tc.rows, tc.grain, tc.dims);

        expect(Number.isFinite(est.raw_rows)).toBe(true);
        expect(Number.isFinite(est.preagg_rows)).toBe(true);
        expect(Number.isFinite(est.speedup_factor)).toBe(true);
        expect(Number.isFinite(est.scan_reduction_pct)).toBe(true);
        expect(est.speedup_factor).toBeGreaterThanOrEqual(1.0);
        expect(est.scan_reduction_pct).toBeGreaterThanOrEqual(0.0);
        expect(est.scan_reduction_pct).toBeLessThanOrEqual(99.9);
      }
    });

    test('4.4: Cardinality multiplication overflow and extreme dimensions', () => {
      // Dimensions with huge cardinalities exceeding table size
      const extremeDims = [
        { name: 'user_uuid', cardinality: 50_000_000 },
        { name: 'device_id', cardinality: 20_000_000 },
        { name: 'ip_address', cardinality: 10_000_000 },
      ];

      const est = advisor.estimateSpeedup(1_000_000, 'day', extremeDims);

      // Preagg rows must be capped at raw rows
      expect(est.preagg_rows).toBe(1_000_000);
      expect(est.speedup_factor).toBe(1.0);
      expect(est.speedup_label).toBe('1x');
      expect(est.scan_reduction_pct).toBeLessThanOrEqual(50.0); // limited or 0 reduction
    });

    test('4.5: Zero-dimension edge case (Grand Total Rollups / Single Row Preaggs)', () => {
      const est = advisor.estimateSpeedup(5_000_000, 'month', []); // 0 dimensions
      expect(est.preagg_rows).toBeLessThanOrEqual(36); // 36 months in 3-yr horizon
      expect(est.speedup_factor).toBeGreaterThan(100);
      expect(est.scan_reduction_pct).toBeGreaterThan(95);
    });

    test('4.6: formatBytes robustness across entire range (0 to Petabytes, invalid values)', () => {
      expect(formatBytes(0)).toBe('0 B');
      expect(formatBytes(-100)).toBe('0 B');
      expect(formatBytes(null)).toBe('0 B');
      expect(formatBytes(undefined)).toBe('0 B');
      expect(formatBytes(NaN)).toBe('0 B');

      expect(formatBytes(500)).toBe('500 B');
      expect(formatBytes(1024)).toBe('1.0 KB');
      expect(formatBytes(1048576)).toBe('1.0 MB');
      expect(formatBytes(1073741824)).toBe('1.0 GB');
      expect(formatBytes(1099511627776)).toBe('1.0 TB');
      expect(formatBytes(1125899906842624)).toBe('1.0 PB');
      expect(formatBytes(1125899906842624 * 50)).toBe('50.0 PB');
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // SECTION 5: ADVANCED ADVISORY & REASONING STRESS TESTS
  // ══════════════════════════════════════════════════════════════════════════
  describe('5. Advanced Advisory & Mixed Metric Scenarios', () => {
    test('5.1: Mixed rollup with Additive, Non-Additive Avg, HLL Distinct Count, and Ratio Metrics', () => {
      const mixedMeasures = [
        { name: 'total_revenue', agg: 'sum', column: 'revenue' },
        { name: 'order_count', agg: 'count', column: '*' },
        { name: 'avg_order_value', agg: 'avg', column: 'revenue' },
        { name: 'unique_customers', agg: 'count_distinct', column: 'customer_id' },
        { name: 'margin_pct', agg: 'ratio', formula: 'profit / revenue' },
      ];

      const scanResult = {
        models: [
          {
            name: 'fct_orders',
            tier: 'marts_fact',
            columns: {
              order_date: { type: 'date' },
              status: { type: 'varchar' },
              revenue: { type: 'numeric' },
              profit: { type: 'numeric' },
              customer_id: { type: 'integer' },
            },
          },
        ],
        catalogLoaded: true,
        catalogStats: {
          tables: {
            'model.test.fct_orders': { name: 'fct_orders', rowCount: 10_000_000 },
          },
        },
      };

      const customAdvisor = new DbtPreaggAdvisor(scanResult);
      const recs = customAdvisor.advisePreaggregations({
        model_name: 'fct_orders',
        dialect: 'bigquery',
        time_grain: 'day',
        dimensions: ['status'],
        metrics: mixedMeasures,
      });

      expect(recs).toHaveLength(1);
      const rec = recs[0];
      expect(rec.measures).toHaveLength(5);

      // Verify each measure classification in recommendations
      const sumM = rec.measures.find(m => m.name === 'total_revenue');
      expect(sumM.additivity).toBe(ADDITIVITY_TYPES.ADDITIVE);

      const avgM = rec.measures.find(m => m.name === 'avg_order_value');
      expect(avgM.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(avgM.sql_expression).toContain('SUM(revenue)');
      expect(avgM.sql_expression).toContain('COUNT(revenue)');

      const hllM = rec.measures.find(m => m.name === 'unique_customers');
      expect(hllM.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(hllM.sql_expression).toContain('HLL_COUNT.INIT');

      const ratioM = rec.measures.find(m => m.name === 'margin_pct');
      expect(ratioM.additivity).toBe(ADDITIVITY_TYPES.NON_ADDITIVE);
      expect(ratioM.sql_expression).toContain('SUM(profit)');
      expect(ratioM.sql_expression).toContain('SUM(revenue)');

      // Verify generated DDL contains all expressions
      expect(rec.ddl).toContain('CREATE MATERIALIZED VIEW');
      expect(rec.ddl).toContain('SUM(revenue) AS sum_total_revenue');
      expect(rec.ddl).toContain('COUNT(*) AS count_order_count');
      expect(rec.ddl).toContain('SUM(revenue) AS sum_avg_order_value, COUNT(revenue) AS count_avg_order_value');
      expect(rec.ddl).toContain('HLL_COUNT.INIT(customer_id, 14) AS hll_unique_customers');
      expect(rec.ddl).toContain('SUM(profit) AS sum_margin_pct_num, SUM(revenue) AS sum_margin_pct_den');

      // Verify query acceleration rewrite contains all matching recombined measures
      expect(rec.query_acceleration.accelerated_query_pattern).toContain('SUM(sum_total_revenue) AS total_revenue');
      expect(rec.query_acceleration.accelerated_query_pattern).toContain('SUM(count_order_count) AS order_count');
      expect(rec.query_acceleration.accelerated_query_pattern).toContain('SUM(sum_avg_order_value) / NULLIF(SUM(count_avg_order_value), 0) AS avg_order_value');
      expect(rec.query_acceleration.accelerated_query_pattern).toContain('HLL_COUNT.MERGE(hll_unique_customers) AS unique_customers');
      expect(rec.query_acceleration.accelerated_query_pattern).toContain('SUM(sum_margin_pct_num) / NULLIF(SUM(sum_margin_pct_den), 0) AS margin_pct');
    });
  });
});
