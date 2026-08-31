/**
 * src/dbt/preagg-advisor.js
 * Cube.js-Style Pre-Aggregation & Rollup Materialized View Advisor
 *
 * Formulates database-specific pre-aggregation recommendations, rollups, and
 * Materialized View DDLs across 7 dialects: PostgreSQL, BigQuery, Snowflake,
 * ClickHouse, DuckDB, MySQL, and Redshift.
 *
 * Classifies metrics and measures by additivity:
 * - ADDITIVE: sum, count, min, max, sum_boolean
 * - SEMI_ADDITIVE: periodic snapshots, balances (preserves snapshot grain / window grouping)
 * - NON_ADDITIVE: count_distinct (HLL sketches or 2-tier retention), avg (sum+count decomposition), ratio/derived
 */

export const SUPPORTED_DIALECTS = [
  'postgres',
  'bigquery',
  'snowflake',
  'clickhouse',
  'duckdb',
  'redshift',
  'mysql',
];

export const SUPPORTED_GRAINS = ['hour', 'day', 'week', 'month', 'quarter', 'year'];

export const ADDITIVITY_TYPES = Object.freeze({
  ADDITIVE: 'additive',
  SEMI_ADDITIVE: 'semi_additive',
  NON_ADDITIVE: 'non_additive',
});

/**
 * Normalize SQL dialect name and handle aliases
 * @param {string} dialect
 * @returns {string}
 */
export function normalizeDialect(dialect) {
  if (!dialect || typeof dialect !== 'string') return 'postgres';
  const clean = dialect.toLowerCase().trim();
  const aliasMap = {
    postgresql: 'postgres',
    pgsql: 'postgres',
    postgres: 'postgres',
    bigquery: 'bigquery',
    bq: 'bigquery',
    snowflake: 'snowflake',
    sf: 'snowflake',
    clickhouse: 'clickhouse',
    ch: 'clickhouse',
    duckdb: 'duckdb',
    duck: 'duckdb',
    redshift: 'redshift',
    rs: 'redshift',
    mysql: 'mysql',
    mariadb: 'mysql',
  };
  return aliasMap[clean] || (SUPPORTED_DIALECTS.includes(clean) ? clean : 'postgres');
}

/**
 * Normalize time grain string and handle aliases
 * @param {string} grain
 * @returns {string}
 */
export function normalizeTimeGrain(grain) {
  if (!grain || typeof grain !== 'string') return 'day';
  const clean = grain.toLowerCase().trim();
  const grainMap = {
    hour: 'hour',
    hourly: 'hour',
    day: 'day',
    daily: 'day',
    week: 'week',
    weekly: 'week',
    month: 'month',
    monthly: 'month',
    quarter: 'quarter',
    quarterly: 'quarter',
    year: 'year',
    yearly: 'year',
    annual: 'year',
    annually: 'year',
  };
  return grainMap[clean] || (SUPPORTED_GRAINS.includes(clean) ? clean : 'day');
}

/**
 * Format raw byte counts to human readable strings (KB, MB, GB, TB)
 * @param {number} bytes
 * @returns {string}
 */
export function formatBytes(bytes) {
  if (!bytes || bytes <= 0 || isNaN(bytes)) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const val = (bytes / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1);
  return `${val} ${units[i]}`;
}

export class DbtPreaggAdvisor {
  /**
   * @param {object} [scanResult=null] - Output from DbtDeepScanner.scanProject
   * @param {object} [options={}] - Default configuration options
   */
  constructor(scanResult = null, options = {}) {
    this.scanResult = scanResult || {};
    this.options = options;
    this.defaultDialect = normalizeDialect(options.dialect || 'postgres');
    this.defaultSchema = options.targetSchema || options.target_schema || 'preagg';
    this.includeHll = options.includeHll !== undefined ? Boolean(options.includeHll) : (options.include_hll !== undefined ? Boolean(options.include_hll) : true);
    this.minSpeedupFactor = Number(options.minSpeedupFactor || options.min_speedup_factor || 2.0);
  }

  /**
   * Classify measure / metric additivity and generate decomposition & rollup rules
   * @param {string|object} measureOrAgg
   * @param {object} [context={}]
   * @returns {object}
   */
  classifyAdditivity(measureOrAgg, context = {}) {
    let name = 'metric';
    let agg = 'sum';
    let column = null;
    let expr = null;
    let nonAdditiveDim = context.nonAdditiveDimension || context.non_additive_dimension || null;
    let isSnapshot = Boolean(context.isSnapshot || context.is_snapshot);
    let dialect = normalizeDialect(context.dialect || this.defaultDialect);
    let includeHll = context.includeHll !== undefined ? Boolean(context.includeHll) : (context.include_hll !== undefined ? Boolean(context.include_hll) : this.includeHll);

    if (typeof measureOrAgg === 'string') {
      agg = measureOrAgg.toLowerCase().trim();
      name = context.name || `${agg}_measure`;
      column = context.column || context.expr || name;
    } else if (typeof measureOrAgg === 'object' && measureOrAgg !== null) {
      name = measureOrAgg.name || measureOrAgg.metric_name || context.name || 'measure';
      agg = (measureOrAgg.agg || measureOrAgg.type || measureOrAgg.agg_type || measureOrAgg.aggregation || 'sum').toLowerCase().trim();
      column = measureOrAgg.column || measureOrAgg.expr || measureOrAgg.field || name;
      expr = measureOrAgg.expr || measureOrAgg.formula || null;
      if (measureOrAgg.non_additive_dimension) {
        nonAdditiveDim = measureOrAgg.non_additive_dimension;
        isSnapshot = true;
      }
      if (measureOrAgg.is_snapshot || measureOrAgg.snapshot) {
        isSnapshot = true;
      }
    }

    const colExpr = expr || column || name;

    // 1. Semi-Additive Checks (Snapshots, balances with non-additive dimensions)
    if (isSnapshot || nonAdditiveDim || agg === 'snapshot' || agg === 'balance' || agg === 'cumulative_snapshot') {
      return {
        name,
        agg: agg === 'snapshot' || agg === 'balance' ? 'sum' : agg,
        column: colExpr,
        additivity: ADDITIVITY_TYPES.SEMI_ADDITIVE,
        sql_expression: `SUM(${colExpr}) AS sum_${name}`,
        rollup_expression: `SUM(sum_${name})`,
        decomposition: 'Additive across categorical dimensions; non-additive across time dimension. Snapshot timestamp must be preserved in GROUP BY or filtered via MAX/LAST_VALUE window grouping.',
        hll_supported: false,
        non_additive_dimension: nonAdditiveDim || { name: 'snapshot_date', window_choice: 'max' },
        recommendation: 'Preserve snapshot time grain; avoid unconstrained time-range summation without snapshot window partition.',
      };
    }

    // 2. Fully Additive Measures (sum, count, min, max, sum_boolean, count_boolean)
    if (['sum', 'count', 'min', 'max', 'sum_boolean', 'count_boolean'].includes(agg)) {
      let sqlExpr = '';
      let rollupExpr = '';

      switch (agg) {
        case 'sum':
          sqlExpr = `SUM(${colExpr}) AS sum_${name}`;
          rollupExpr = `SUM(sum_${name})`;
          break;
        case 'count':
        case 'count_boolean':
          if (colExpr === '*' || colExpr === '1') {
            sqlExpr = `COUNT(*) AS count_${name}`;
          } else {
            sqlExpr = `COUNT(${colExpr}) AS count_${name}`;
          }
          rollupExpr = `SUM(count_${name})`; // Important: rollup over count is SUM
          break;
        case 'min':
          sqlExpr = `MIN(${colExpr}) AS min_${name}`;
          rollupExpr = `MIN(min_${name})`;
          break;
        case 'max':
          sqlExpr = `MAX(${colExpr}) AS max_${name}`;
          rollupExpr = `MAX(max_${name})`;
          break;
        case 'sum_boolean':
          sqlExpr = `SUM(CASE WHEN ${colExpr} THEN 1 ELSE 0 END) AS sum_${name}`;
          rollupExpr = `SUM(sum_${name})`;
          break;
      }

      return {
        name,
        agg,
        column: colExpr,
        additivity: ADDITIVITY_TYPES.ADDITIVE,
        sql_expression: sqlExpr,
        rollup_expression: rollupExpr,
        decomposition: null,
        hll_supported: false,
        non_additive_dimension: null,
        recommendation: 'Fully additive across all dimensions and time grains. Direct SUM/MIN/MAX rollups supported.',
      };
    }

    // 3. Non-Additive: Average / Mean Decomposition (avg -> sum + count)
    if (['avg', 'average', 'mean'].includes(agg)) {
      return {
        name,
        agg: 'avg',
        column: colExpr,
        additivity: ADDITIVITY_TYPES.NON_ADDITIVE,
        sql_expression: `SUM(${colExpr}) AS sum_${name}, COUNT(${colExpr}) AS count_${name}`,
        rollup_expression: `SUM(sum_${name}) / NULLIF(SUM(count_${name}), 0)`,
        decomposition: `Algebraically decomposed into constituent additive components: SUM(${colExpr}) AS sum_${name} and COUNT(${colExpr}) AS count_${name}. Reconstructed at query time via: SUM(sum_${name}) / NULLIF(SUM(count_${name}), 0).`,
        hll_supported: false,
        non_additive_dimension: null,
        columns: [
          { name: `sum_${name}`, sql_expression: `SUM(${colExpr})`, rollup_expression: `SUM(sum_${name})`, type: 'sum' },
          { name: `count_${name}`, sql_expression: `COUNT(${colExpr})`, rollup_expression: `SUM(count_${name})`, type: 'count' },
        ],
        recommendation: 'Materialize SUM and COUNT in rollup; compute ratio at query time to preserve mathematical precision.',
      };
    }

    // 4. Non-Additive: Distinct Count & HyperLogLog (count_distinct, unique)
    if (['count_distinct', 'distinct_count', 'unique', 'approx_count_distinct'].includes(agg)) {
      if (includeHll) {
        let hllSql = '';
        let hllRollup = '';

        switch (dialect) {
          case 'bigquery':
            hllSql = `HLL_COUNT.INIT(${colExpr}, 14) AS hll_${name}`;
            hllRollup = `HLL_COUNT.MERGE(hll_${name})`;
            break;
          case 'snowflake':
            hllSql = `HLL_ACCUMULATE(${colExpr}) AS hll_${name}`;
            hllRollup = `HLL_ESTIMATE(HLL_COMBINE(hll_${name}))`;
            break;
          case 'clickhouse':
            hllSql = `uniqCombinedState(${colExpr}) AS hll_${name}`;
            hllRollup = `uniqCombinedMerge(hll_${name})`;
            break;
          case 'duckdb':
            hllSql = `approx_count_distinct(${colExpr}) AS approx_${name}`;
            hllRollup = `approx_count_distinct(${colExpr})`;
            break;
          case 'redshift':
            hllSql = `hyperloglog(${colExpr}) AS hll_${name}`;
            hllRollup = `hyperloglog_count(hll_${name})`;
            break;
          case 'postgres':
            hllSql = `hll_add_agg(hll_hash_text(${colExpr}::text)) AS hll_${name}`;
            hllRollup = `hll_cardinality(hll_union_agg(hll_${name}))`;
            break;
          case 'mysql':
          default:
            hllSql = `COUNT(DISTINCT ${colExpr}) AS count_distinct_${name}`;
            hllRollup = `COUNT(DISTINCT ${colExpr})`;
            break;
        }

        return {
          name,
          agg: 'count_distinct',
          column: colExpr,
          additivity: ADDITIVITY_TYPES.NON_ADDITIVE,
          sql_expression: hllSql,
          rollup_expression: hllRollup,
          decomposition: `Approximated via HyperLogLog (HLL) sketch state for dialect "${dialect}" with bounded error (±1-2%).`,
          hll_supported: true,
          non_additive_dimension: null,
          recommendation: 'Materialize HLL sketch state in pre-aggregation; merge sketch states at query time.',
        };
      } else {
        return {
          name,
          agg: 'count_distinct',
          column: colExpr,
          additivity: ADDITIVITY_TYPES.NON_ADDITIVE,
          sql_expression: `COUNT(DISTINCT ${colExpr}) AS distinct_${name}`,
          rollup_expression: `COUNT(DISTINCT ${colExpr})`,
          decomposition: 'Two-tier exact distinct count: retains distinct entity key in intermediate preaggregation grain.',
          hll_supported: false,
          non_additive_dimension: null,
          recommendation: 'Include distinct entity key in intermediate pre-aggregation grouping dimensions.',
        };
      }
    }

    // 5. Non-Additive: Ratio & Derived Compound Metrics (e.g. margin_pct = profit / revenue)
    if (['ratio', 'derived', 'derived_metric', 'cumulative'].includes(agg) || (expr && (expr.includes('/') || expr.includes('*')))) {
      const numMatch = expr ? expr.match(/([a-zA-Z0-9_]+)\s*\/\s*([a-zA-Z0-9_]+)/) : null;
      const numCol = numMatch ? numMatch[1] : `numerator_${name}`;
      const denCol = numMatch ? numMatch[2] : `denominator_${name}`;

      return {
        name,
        agg: 'ratio',
        column: colExpr,
        additivity: ADDITIVITY_TYPES.NON_ADDITIVE,
        sql_expression: `SUM(${numCol}) AS sum_${name}_num, SUM(${denCol}) AS sum_${name}_den`,
        rollup_expression: `SUM(sum_${name}_num) / NULLIF(SUM(sum_${name}_den), 0)`,
        decomposition: `Compound ratio decomposed into underlying base additive measures: SUM(${numCol}) and SUM(${denCol}). Recomputed at query time.`,
        hll_supported: false,
        non_additive_dimension: null,
        columns: [
          { name: `sum_${name}_num`, sql_expression: `SUM(${numCol})`, rollup_expression: `SUM(sum_${name}_num)` },
          { name: `sum_${name}_den`, sql_expression: `SUM(${denCol})`, rollup_expression: `SUM(sum_${name}_den)` },
        ],
        recommendation: 'Materialize underlying additive base measures in rollup; compute derived formula at query time.',
      };
    }

    // Fallback: Generic measure as SUM
    return {
      name,
      agg: 'sum',
      column: colExpr,
      additivity: ADDITIVITY_TYPES.ADDITIVE,
      sql_expression: `SUM(${colExpr}) AS sum_${name}`,
      rollup_expression: `SUM(sum_${name})`,
      decomposition: null,
      hll_supported: false,
      non_additive_dimension: null,
      recommendation: 'Treated as standard additive measure.',
    };
  }

  /**
   * Get exact SQL Date Truncation expression for any supported dialect and time grain
   * @param {string} dialect
   * @param {string} timeColumn
   * @param {string} timeGrain
   * @returns {string}
   */
  getDateTruncSql(dialect, timeColumn, timeGrain) {
    const d = normalizeDialect(dialect);
    const g = normalizeTimeGrain(timeGrain);
    const col = timeColumn || 'order_date';

    switch (d) {
      case 'postgres':
      case 'redshift':
      case 'duckdb':
        return `DATE_TRUNC('${g}', ${col})`;

      case 'bigquery':
        if (g === 'hour') {
          return `TIMESTAMP_TRUNC(${col}, HOUR)`;
        }
        return `DATE_TRUNC(${col}, ${g.toUpperCase()})`;

      case 'snowflake':
        return `DATE_TRUNC('${g}', ${col})`;

      case 'clickhouse':
        switch (g) {
          case 'hour': return `toStartOfHour(${col})`;
          case 'day': return `toStartOfDay(${col})`;
          case 'week': return `toStartOfWeek(${col})`;
          case 'month': return `toStartOfMonth(${col})`;
          case 'quarter': return `toStartOfQuarter(${col})`;
          case 'year': return `toStartOfYear(${col})`;
          default: return `toStartOfDay(${col})`;
        }

      case 'mysql':
        switch (g) {
          case 'hour': return `DATE_FORMAT(${col}, '%Y-%m-%d %H:00:00')`;
          case 'day': return `DATE(${col})`;
          case 'week': return `STR_TO_DATE(CONCAT(YEARWEEK(${col}, 3), ' Monday'), '%X%V %W')`;
          case 'month': return `DATE_FORMAT(${col}, '%Y-%m-01')`;
          case 'quarter': return `MAKEDATE(YEAR(${col}), 1) + INTERVAL QUARTER(${col})*3-3 MONTH`;
          case 'year': return `DATE_FORMAT(${col}, '%Y-01-01')`;
          default: return `DATE(${col})`;
        }

      default:
        return `DATE_TRUNC('${g}', ${col})`;
    }
  }

  /**
   * Estimate query speedup factor and byte scan reduction percentage
   * @param {number} rawRowCount
   * @param {string} timeGrain
   * @param {Array<string|object>} [dimensions=[]]
   * @param {object} [options={}]
   * @returns {object}
   */
  estimateSpeedup(rawRowCount, timeGrain, dimensions = [], options = {}) {
    const rawRows = Number(rawRowCount) > 0 ? Number(rawRowCount) : 1000000;
    const grain = timeGrain ? normalizeTimeGrain(timeGrain) : 'day';
    const isNonTemporal = grain === 'none' || timeGrain === 'none';

    // 1. Time Slices for a 3-Year Horizon
    let timeSlices = 1095; // default day (365 * 3)
    if (!isNonTemporal) {
      switch (grain) {
        case 'hour': timeSlices = 26280; break;
        case 'day': timeSlices = 1095; break;
        case 'week': timeSlices = 156; break;
        case 'month': timeSlices = 36; break;
        case 'quarter': timeSlices = 12; break;
        case 'year': timeSlices = 3; break;
        default: timeSlices = 1095; break;
      }
    } else {
      timeSlices = 1;
    }

    // 2. Compute Product of Dimension Cardinalities
    let dimProduct = 1;
    const dims = Array.isArray(dimensions) ? dimensions : [];
    
    for (const d of dims) {
      let card = 20; // default unknown dim heuristic
      let dimName = '';
      if (typeof d === 'string') {
        dimName = d.toLowerCase();
      } else if (typeof d === 'object' && d !== null) {
        dimName = (d.name || '').toLowerCase();
        if (d.cardinality && Number(d.cardinality) > 0) {
          card = Number(d.cardinality);
        }
      }

      if (!d.cardinality) {
        if (['status', 'type', 'tier', 'plan', 'category', 'gender', 'is_active', 'is_deleted'].some(k => dimName.includes(k))) {
          card = 6;
        } else if (['country', 'state', 'region', 'city', 'department'].some(k => dimName.includes(k))) {
          card = 30;
        } else if (['device', 'browser', 'os', 'channel', 'source'].some(k => dimName.includes(k))) {
          card = 10;
        } else if (['customer_id', 'user_id', 'account_id', 'order_id', 'uuid'].some(k => dimName.includes(k))) {
          card = Math.min(10000, Math.round(rawRows * 0.1));
        }
      }

      dimProduct *= card;
    }

    // 3. Rollup Row Count Estimation
    const combinatorialRows = timeSlices * dimProduct;
    // Apply realistic sparsity factor (0.8) and cap at raw row count
    const preaggRows = Math.max(1, Math.min(Math.round(combinatorialRows * 0.8), rawRows));

    // 4. Byte Scan Estimation
    const rawBytesPerRow = Number(options.rawBytesPerRow) || 128;
    const rawBytes = options.rawBytes && Number(options.rawBytes) > 0 ? Number(options.rawBytes) : rawRows * rawBytesPerRow;

    const measureCount = Number(options.measureCount) || 4;
    const preaggBytesPerRow = (1 + dims.length + measureCount) * 12;
    const preaggBytes = preaggRows * preaggBytesPerRow;

    // 5. Scan Reduction % & Speedup Factor
    const scanReductionPct = Number(Math.min(99.9, Math.max(0, (1 - (preaggBytes / rawBytes)) * 100)).toFixed(1));
    const speedupRatio = rawRows / preaggRows;
    const speedupFactor = Number(Math.min(10000, Math.max(1.0, speedupRatio)).toFixed(1));
    const speedupLabel = `${Math.round(speedupFactor)}x`;
    const bytesSaved = Math.max(0, rawBytes - preaggBytes);

    return {
      raw_rows: rawRows,
      preagg_rows: preaggRows,
      speedup_factor: speedupFactor,
      speedup_label: speedupLabel,
      scan_reduction_pct: scanReductionPct,
      bytes_saved_est: formatBytes(bytesSaved),
      is_heuristic_estimate: !Boolean(options.catalogStatsLoaded),
    };
  }

  /**
   * Multi-Dialect DDL Compiler
   * Generates syntactically valid Materialized View / Summary Table DDL, Index DDLs, and Refresh commands
   * @param {object} spec
   * @returns {object}
   */
  generateRollupDDL(spec = {}) {
    const dialect = normalizeDialect(spec.dialect || this.defaultDialect);
    const targetSchema = spec.targetSchema || spec.target_schema || this.defaultSchema;
    const modelName = spec.model || spec.modelName || spec.model_name || 'fct_orders';
    const sourceTable = spec.sourceTable || spec.source_table || (spec.schema ? `${spec.schema}.${modelName}` : modelName);
    const timeGrain = spec.timeGrain ? normalizeTimeGrain(spec.timeGrain) : 'day';
    const timeDimension = spec.timeDimension || spec.time_dimension || 'order_date';
    const dimensions = Array.isArray(spec.dimensions) ? spec.dimensions.map(d => typeof d === 'string' ? d : d.name) : [];
    const preaggName = spec.name || spec.preaggName || spec.preagg_name || `${modelName}_preagg_${timeGrain}`;
    const includeIndexes = spec.includeIndexes !== undefined ? Boolean(spec.includeIndexes) : true;
    const refreshInterval = Number(spec.refreshIntervalMinutes || spec.refresh_interval_minutes || 60);

    // Build Time Truncation Column
    const timeTruncSql = this.getDateTruncSql(dialect, timeDimension, timeGrain);
    const timeAlias = `${timeDimension}_${timeGrain}`;

    // Classify & Compile Measures
    const rawMeasures = Array.isArray(spec.measures) && spec.measures.length > 0
      ? spec.measures
      : [
          { name: 'total_amount', agg: 'sum', column: 'amount' },
          { name: 'order_count', agg: 'count', column: '*' },
          { name: 'min_amount', agg: 'min', column: 'amount' },
          { name: 'max_amount', agg: 'max', column: 'amount' },
        ];

    const classifiedMeasures = rawMeasures.map(m => this.classifyAdditivity(m, { dialect, includeHll: this.includeHll }));

    // Select Columns list
    const selectColumns = [`${timeTruncSql} AS ${timeAlias}`];
    dimensions.forEach(dim => selectColumns.push(dim));
    classifiedMeasures.forEach(m => selectColumns.push(m.sql_expression));

    const selectColumnsSql = selectColumns.join(',\n  ');
    const groupByCount = 1 + dimensions.length;
    const groupBy1toN = Array.from({ length: groupByCount }, (_, i) => i + 1).join(', ');

    let ddl = '';
    let indexDdl = [];
    let refreshCommand = '';
    let refreshStrategy = spec.refreshStrategy || 'auto';
    let refreshSchedule = `Every ${refreshInterval} minutes`;

    switch (dialect) {
      case 'postgres': {
        ddl = [
          `CREATE MATERIALIZED VIEW IF NOT EXISTS ${targetSchema}.${preaggName} AS`,
          `SELECT`,
          `  ${selectColumnsSql}`,
          `FROM ${sourceTable}`,
          `GROUP BY ${groupBy1toN};`,
        ].join('\n');

        if (includeIndexes) {
          const uidxCols = [timeAlias, ...dimensions].join(', ');
          indexDdl.push(`CREATE UNIQUE INDEX IF NOT EXISTS uidx_${preaggName} ON ${targetSchema}.${preaggName} (${uidxCols});`);
          indexDdl.push(`CREATE INDEX IF NOT EXISTS idx_${preaggName}_time ON ${targetSchema}.${preaggName} (${timeAlias});`);
        }

        refreshCommand = `REFRESH MATERIALIZED VIEW CONCURRENTLY ${targetSchema}.${preaggName};`;
        refreshStrategy = 'concurrent';
        break;
      }

      case 'bigquery': {
        // BigQuery limits CLUSTER BY to max 4 columns
        const clusterCols = dimensions.slice(0, 4);
        const clusterClause = clusterCols.length > 0 ? `\nCLUSTER BY ${clusterCols.join(', ')}` : '';
        const partitionClause = `\nPARTITION BY DATE(${timeAlias})`;

        ddl = [
          `CREATE MATERIALIZED VIEW IF NOT EXISTS \`${targetSchema}.${preaggName}\``,
          `OPTIONS (`,
          `  enable_refresh = true,`,
          `  refresh_interval_minutes = ${refreshInterval}`,
          `)` + partitionClause + clusterClause,
          `AS`,
          `SELECT`,
          `  ${selectColumnsSql}`,
          `FROM \`${sourceTable}\``,
          `GROUP BY ${groupBy1toN};`,
        ].join('\n');

        refreshCommand = `CALL BQ.REFRESH_MATERIALIZED_VIEW('${targetSchema}.${preaggName}');`;
        refreshStrategy = 'auto';
        break;
      }

      case 'snowflake': {
        const clusterCols = [timeAlias, ...dimensions.slice(0, 3)];
        const clusterClause = clusterCols.length > 0 ? `\nCLUSTER BY (${clusterCols.join(', ')})` : '';

        ddl = [
          `CREATE OR REPLACE MATERIALIZED VIEW ${targetSchema}.${preaggName}` + clusterClause,
          `AS`,
          `SELECT`,
          `  ${selectColumnsSql}`,
          `FROM ${sourceTable}`,
          `GROUP BY ${groupBy1toN};`,
        ].join('\n');

        refreshCommand = `-- Snowflake maintains materialized views automatically via serverless compute`;
        refreshStrategy = 'auto';
        break;
      }

      case 'clickhouse': {
        // Generate ClickHouse SummingMergeTree Table + Materialized View
        const orderCols = [timeAlias, ...dimensions];
        const sumMeasures = classifiedMeasures
          .filter(m => m.agg === 'sum' || m.agg === 'count')
          .map(m => m.name.startsWith('sum_') || m.name.startsWith('count_') ? m.name : `${m.agg}_${m.name}`);

        const chColumnDefs = [`  ${timeAlias} Date`];
        dimensions.forEach(dim => chColumnDefs.push(`  ${dim} LowCardinality(String)`));
        classifiedMeasures.forEach(m => {
          if (m.agg === 'sum' || m.agg === 'avg') {
            chColumnDefs.push(`  sum_${m.name} Float64`);
            if (m.agg === 'avg') chColumnDefs.push(`  count_${m.name} UInt64`);
          } else if (m.agg === 'count') {
            chColumnDefs.push(`  count_${m.name} UInt64`);
          } else if (m.agg === 'min' || m.agg === 'max') {
            chColumnDefs.push(`  ${m.agg}_${m.name} Float64`);
          } else if (m.hll_supported) {
            chColumnDefs.push(`  hll_${m.name} AggregateFunction(uniqCombined, String)`);
          } else {
            chColumnDefs.push(`  ${m.name} Float64`);
          }
        });

        const engineClause = sumMeasures.length > 0
          ? `ENGINE = SummingMergeTree((${sumMeasures.join(', ')}))`
          : `ENGINE = SummingMergeTree()`;

        const tableDdl = [
          `CREATE TABLE IF NOT EXISTS ${targetSchema}.${preaggName} (`,
          chColumnDefs.join(',\n'),
          `) ${engineClause}`,
          `PRIMARY KEY (${orderCols.join(', ')})`,
          `ORDER BY (${orderCols.join(', ')})`,
          `SETTINGS index_granularity = 8192;`,
        ].join('\n');

        const mvDdl = [
          `CREATE MATERIALIZED VIEW IF NOT EXISTS ${targetSchema}.${preaggName}_mv`,
          `TO ${targetSchema}.${preaggName}`,
          `AS SELECT`,
          `  ${selectColumnsSql}`,
          `FROM ${sourceTable}`,
          `GROUP BY ${orderCols.join(', ')};`,
        ].join('\n');

        ddl = `${tableDdl}\n\n${mvDdl}`;
        refreshCommand = `-- Real-time continuous ingestion via ClickHouse Materialized View trigger`;
        refreshStrategy = 'realtime_mv';
        break;
      }

      case 'duckdb': {
        ddl = [
          `CREATE OR REPLACE TABLE ${targetSchema}.${preaggName} AS`,
          `SELECT`,
          `  ${selectColumnsSql}`,
          `FROM ${sourceTable}`,
          `GROUP BY ${groupBy1toN};`,
        ].join('\n');

        if (includeIndexes) {
          indexDdl.push(`CREATE INDEX IF NOT EXISTS idx_${preaggName} ON ${targetSchema}.${preaggName} (${[timeAlias, ...dimensions].join(', ')});`);
        }

        refreshCommand = `CREATE OR REPLACE TABLE ${targetSchema}.${preaggName} AS SELECT ${selectColumnsSql} FROM ${sourceTable} GROUP BY ${groupBy1toN};`;
        refreshStrategy = 'batch_replace';
        break;
      }

      case 'mysql': {
        const pkCols = [`\`${timeAlias}\``, ...dimensions.map(d => `\`${d}\``)];
        const mysqlColDefs = [`  \`${timeAlias}\` DATE NOT NULL`];
        dimensions.forEach(dim => mysqlColDefs.push(`  \`${dim}\` VARCHAR(128) NOT NULL`));
        classifiedMeasures.forEach(m => {
          if (m.agg === 'sum' || m.agg === 'avg') {
            mysqlColDefs.push(`  \`sum_${m.name}\` DECIMAL(18,4)`);
            if (m.agg === 'avg') mysqlColDefs.push(`  \`count_${m.name}\` BIGINT`);
          } else if (m.agg === 'count') {
            mysqlColDefs.push(`  \`count_${m.name}\` BIGINT`);
          } else if (m.agg === 'min' || m.agg === 'max') {
            mysqlColDefs.push(`  \`${m.agg}_${m.name}\` DECIMAL(18,4)`);
          } else {
            mysqlColDefs.push(`  \`${m.name}\` DECIMAL(18,4)`);
          }
        });

        const onUpdates = [];
        classifiedMeasures.forEach(m => {
          if (m.agg === 'sum' || m.agg === 'avg') {
            onUpdates.push(`  \`sum_${m.name}\` = VALUES(\`sum_${m.name}\`)`);
            if (m.agg === 'avg') onUpdates.push(`  \`count_${m.name}\` = VALUES(\`count_${m.name}\`)`);
          } else if (m.agg === 'count') {
            onUpdates.push(`  \`count_${m.name}\` = VALUES(\`count_${m.name}\`)`);
          } else if (m.agg === 'min' || m.agg === 'max') {
            onUpdates.push(`  \`${m.agg}_${m.name}\` = VALUES(\`${m.agg}_${m.name}\`)`);
          }
        });

        ddl = [
          `CREATE TABLE IF NOT EXISTS \`${targetSchema}\`.\`${preaggName}\` (`,
          mysqlColDefs.join(',\n'),
          `,`,
          `  PRIMARY KEY (${pkCols.join(', ')}),`,
          `  INDEX \`idx_time\` (\`${timeAlias}\`)`,
          `) ENGINE = InnoDB;`,
        ].join('\n');

        refreshCommand = [
          `INSERT INTO \`${targetSchema}\`.\`${preaggName}\``,
          `SELECT`,
          `  ${selectColumnsSql}`,
          `FROM \`${sourceTable}\``,
          `GROUP BY ${groupBy1toN}`,
          `ON DUPLICATE KEY UPDATE`,
          onUpdates.join(',\n') + `;`,
        ].join('\n');

        refreshStrategy = 'scheduled_upsert';
        break;
      }

      case 'redshift': {
        const distKey = dimensions.length > 0 ? dimensions[0] : timeAlias;
        const sortKeys = [timeAlias, ...dimensions.slice(0, 2)];

        ddl = [
          `CREATE MATERIALIZED VIEW ${targetSchema}.${preaggName}`,
          `AUTO REFRESH YES`,
          `DISTSTYLE KEY`,
          `DISTKEY(${distKey})`,
          `SORTKEY(${sortKeys.join(', ')})`,
          `AS`,
          `SELECT`,
          `  ${selectColumnsSql}`,
          `FROM ${sourceTable}`,
          `GROUP BY ${groupBy1toN};`,
        ].join('\n');

        refreshCommand = `REFRESH MATERIALIZED VIEW ${targetSchema}.${preaggName};`;
        refreshStrategy = 'auto';
        break;
      }
    }

    return {
      ddl,
      index_ddl: indexDdl,
      refresh_strategy: refreshStrategy,
      refresh_schedule: refreshSchedule,
      refresh_command: refreshCommand,
    };
  }

  /**
   * Generate accelerated SQL query targeting the pre-aggregation object
   * @param {string} modelName
   * @param {string} targetSchema
   * @param {string} preaggName
   * @param {string} timeDimension
   * @param {string} timeGrain
   * @param {Array<string>} dimensions
   * @param {Array<object>} measures
   * @param {string} dialect
   * @returns {object}
   */
  generateAcceleratedQuery(modelName, targetSchema, preaggName, timeDimension, timeGrain, dimensions = [], measures = [], dialect = 'postgres') {
    const timeAlias = `${timeDimension}_${timeGrain}`;
    const preaggTable = `${targetSchema}.${preaggName}`;
    const timeTruncSql = this.getDateTruncSql(dialect, timeDimension, timeGrain);

    const rawSelectCols = [`${timeTruncSql} AS ${timeAlias}`, ...dimensions];
    measures.forEach(m => {
      if (m.agg === 'sum') rawSelectCols.push(`SUM(${m.column || 'amount'}) AS ${m.name}`);
      else if (m.agg === 'count') rawSelectCols.push(`COUNT(*) AS ${m.name}`);
      else if (m.agg === 'avg') rawSelectCols.push(`AVG(${m.column || 'amount'}) AS ${m.name}`);
    });

    const origQuery = [
      `SELECT`,
      `  ${rawSelectCols.join(',\n  ')}`,
      `FROM ${modelName}`,
      `WHERE ${timeDimension} >= CURRENT_DATE - INTERVAL '90 day'`,
      `GROUP BY ${Array.from({ length: 1 + dimensions.length }, (_, i) => i + 1).join(', ')};`,
    ].join('\n');

    const accelSelectCols = [timeAlias, ...dimensions];
    measures.forEach(m => {
      if (m.rollup_expression) {
        accelSelectCols.push(`${m.rollup_expression} AS ${m.name}`);
      } else if (m.agg === 'sum') {
        accelSelectCols.push(`SUM(sum_${m.name}) AS ${m.name}`);
      } else if (m.agg === 'count') {
        accelSelectCols.push(`SUM(count_${m.name}) AS ${m.name}`);
      } else if (m.agg === 'avg') {
        accelSelectCols.push(`SUM(sum_${m.name}) / NULLIF(SUM(count_${m.name}), 0) AS ${m.name}`);
      } else {
        accelSelectCols.push(`SUM(${m.name}) AS ${m.name}`);
      }
    });

    const accelQuery = [
      `SELECT`,
      `  ${accelSelectCols.join(',\n  ')}`,
      `FROM ${preaggTable}`,
      `WHERE ${timeAlias} >= CURRENT_DATE - INTERVAL '90 day'`,
      `GROUP BY ${Array.from({ length: 1 + dimensions.length }, (_, i) => i + 1).join(', ')};`,
    ].join('\n');

    const routingRule = `Queries filtering on "${timeDimension}" and grouping by [${dimensions.join(', ')}] will be automatically routed to pre-aggregation "${preaggTable}", avoiding full scans of raw table "${modelName}".`;

    return {
      original_query_pattern: origQuery,
      accelerated_query_pattern: accelQuery,
      routing_rule: routingRule,
    };
  }

  /**
   * Analyze metric additivity for a model or metric list
   * @param {string} modelName
   * @param {Array<string>} [metricsList=null]
   * @returns {Array<object>}
   */
  analyzeMetricAdditivity(modelName, metricsList = null) {
    const results = [];
    const model = this.findModel(modelName);

    // 1. Check MetricFlow Metrics
    if (this.scanResult.metrics && Array.isArray(this.scanResult.metrics)) {
      for (const met of this.scanResult.metrics) {
        if (!metricsList || metricsList.includes(met.name)) {
          if (!modelName || met.model === modelName || met.modelName === modelName) {
            const classified = this.classifyAdditivity(met, { dialect: this.defaultDialect, includeHll: this.includeHll });
            results.push({
              metric_name: met.name,
              measure_type: met.type || met.agg || 'derived',
              additivity: classified.additivity,
              decomposition: classified.decomposition || 'Direct rollup aggregation',
              hll_supported: Boolean(classified.hll_supported),
              recommendation: classified.recommendation,
            });
          }
        }
      }
    }

    // 2. Check Model Columns & synthesized measures if results are empty
    if (results.length === 0 && model) {
      const columns = model.columns ? Object.keys(model.columns) : [];
      for (const col of columns) {
        if (col.includes('amount') || col.includes('revenue') || col.includes('price') || col.includes('total') || col.includes('count')) {
          const classified = this.classifyAdditivity({ name: col, agg: 'sum', column: col }, { dialect: this.defaultDialect });
          results.push({
            metric_name: col,
            measure_type: 'sum',
            additivity: classified.additivity,
            decomposition: classified.decomposition || 'Direct rollup aggregation',
            hll_supported: false,
            recommendation: classified.recommendation,
          });
        }
      }
    }

    return results;
  }

  /**
   * Helper: Find model by name in scanResult
   * @param {string} modelName
   * @returns {object|null}
   */
  findModel(modelName) {
    if (!modelName || !this.scanResult.models) return null;
    const list = Array.isArray(this.scanResult.models) ? this.scanResult.models : Object.values(this.scanResult.models);
    return list.find(m => m.name === modelName || m.uniqueId === modelName || m.alias === modelName) || null;
  }

  /**
   * Helper: Detect primary time dimension from model
   * @param {object} model
   * @returns {string}
   */
  detectTimeDimension(model) {
    if (!model) return 'order_date';
    const columns = model.columns ? Object.keys(model.columns) : [];
    const timeCandidates = [
      'order_date',
      'created_at',
      'inserted_at',
      'updated_at',
      'event_time',
      'event_timestamp',
      'activity_date',
      'transaction_date',
      'snapshot_date',
      'date',
      'timestamp',
    ];

    for (const cand of timeCandidates) {
      if (columns.includes(cand)) return cand;
    }

    const partial = columns.find(c => c.endsWith('_date') || c.endsWith('_at') || c.endsWith('_timestamp') || c.endsWith('_time'));
    return partial || 'order_date';
  }

  /**
   * Helper: Detect dimension columns from model
   * @param {object} model
   * @param {string} timeDim
   * @returns {Array<string>}
   */
  detectDimensions(model, timeDim) {
    if (!model || !model.columns) return ['status', 'category'];
    const columns = Object.keys(model.columns);
    const exclude = new Set([
      timeDim,
      'id',
      'uuid',
      'description',
      'notes',
      'comment',
      'raw_json',
      'payload',
    ]);

    const dims = [];
    for (const col of columns) {
      if (exclude.has(col)) continue;
      const lower = col.toLowerCase();
      if (
        lower.endsWith('_id') ||
        lower.endsWith('_key') ||
        lower.endsWith('_code') ||
        lower.includes('status') ||
        lower.includes('type') ||
        lower.includes('country') ||
        lower.includes('region') ||
        lower.includes('state') ||
        lower.includes('category') ||
        lower.includes('tier') ||
        lower.includes('channel') ||
        lower.includes('gender') ||
        lower.startsWith('is_') ||
        lower.startsWith('has_')
      ) {
        dims.push(col);
      }
    }

    return dims.slice(0, 4);
  }

  /**
   * Helper: Detect measures from model
   * @param {object} model
   * @returns {Array<object>}
   */
  detectMeasures(model) {
    if (!model || !model.columns) {
      return [
        { name: 'total_amount', agg: 'sum', column: 'amount' },
        { name: 'order_count', agg: 'count', column: '*' },
        { name: 'avg_amount', agg: 'avg', column: 'amount' },
      ];
    }

    const measures = [];
    const columns = Object.keys(model.columns);

    for (const col of columns) {
      const lower = col.toLowerCase();
      if (
        lower.includes('amount') ||
        lower.includes('revenue') ||
        lower.includes('price') ||
        lower.includes('cost') ||
        lower.includes('total') ||
        lower.includes('sales') ||
        lower.includes('margin') ||
        lower.includes('profit')
      ) {
        measures.push({ name: `total_${col}`, agg: 'sum', column: col });
        measures.push({ name: `avg_${col}`, agg: 'avg', column: col });
      }
    }

    measures.push({ name: 'row_count', agg: 'count', column: '*' });
    return measures.slice(0, 6);
  }

  /**
   * Helper: Get row count from catalog statistics
   * @param {string} modelName
   * @returns {number}
   */
  getModelRowCount(modelName) {
    if (!this.scanResult.catalogStats || !this.scanResult.catalogStats.tables) {
      return 1000000;
    }

    const tables = this.scanResult.catalogStats.tables;
    for (const [key, table] of Object.entries(tables)) {
      if (table.name === modelName || key.endsWith(`.${modelName}`) || table.table === modelName) {
        if (Number(table.rowCount) > 0) return Number(table.rowCount);
      }
    }

    return 1000000;
  }

  /**
   * Main recommendation generator
   * Formulates complete Pre-Aggregation and Rollup Materialized View DDL recommendations
   * @param {object} [options={}]
   * @returns {Array<object>}
   */
  advisePreaggregations(options = {}) {
    const dialect = normalizeDialect(options.dialect || this.defaultDialect);
    const targetSchema = options.targetSchema || options.target_schema || this.defaultSchema;
    const singleGrain = options.timeGrain || options.time_grain || 'day';
    const grains = Array.isArray(options.timeGrains || options.time_grains) && (options.timeGrains || options.time_grains).length > 0
      ? (options.timeGrains || options.time_grains).map(normalizeTimeGrain)
      : [normalizeTimeGrain(singleGrain)];
    const minSpeedup = Number(options.minSpeedupFactor || options.min_speedup_factor || this.minSpeedupFactor);
    const includeIndexes = options.includeIndexes !== undefined ? Boolean(options.includeIndexes) : (options.include_indexes !== undefined ? Boolean(options.include_indexes) : true);
    const refreshStrategy = options.refreshStrategy || options.refresh_strategy || 'auto';
    const refreshInterval = Number(options.refreshIntervalMinutes || options.refresh_interval_minutes || 60);
    const maxRecommendations = Number(options.maxRecommendations || options.max_recommendations || 5);

    // 1. Identify Target Models
    let targetModelNames = [];
    if (options.modelName || options.model_name) {
      targetModelNames.push(options.modelName || options.model_name);
    }
    if (Array.isArray(options.modelNames || options.model_names)) {
      for (const m of (options.modelNames || options.model_names)) {
        if (m && !targetModelNames.includes(m)) targetModelNames.push(m);
      }
    }

    // Auto-discover top fact models if none explicitly provided
    if (targetModelNames.length === 0) {
      const modelsList = Array.isArray(this.scanResult.models)
        ? this.scanResult.models
        : (this.scanResult.models ? Object.values(this.scanResult.models) : []);

      const factModels = modelsList.filter(m => m.tier === 'marts_fact' || (m.name && (m.name.startsWith('fct_') || m.name.startsWith('fact_'))));
      if (factModels.length > 0) {
        targetModelNames = factModels.slice(0, 3).map(m => m.name);
      } else if (modelsList.length > 0) {
        targetModelNames = modelsList.slice(0, 3).map(m => m.name);
      } else {
        targetModelNames = ['fct_orders'];
      }
    }

    const recommendations = [];
    let recIdx = 1;

    for (const modelName of targetModelNames) {
      const model = this.findModel(modelName);
      const rawRowCount = this.getModelRowCount(modelName);

      // Determine Time Dimension
      const timeDimension = options.timeDimension || options.time_dimension || this.detectTimeDimension(model);

      // Determine Dimensions
      const dimensions = Array.isArray(options.dimensions) && options.dimensions.length > 0
        ? options.dimensions
        : this.detectDimensions(model, timeDimension);

      // Determine Measures
      let measures = [];
      if (Array.isArray(options.metrics) && options.metrics.length > 0) {
        measures = options.metrics.map(m => {
          if (typeof m === 'string') return { name: m, agg: 'sum', column: m };
          return m;
        });
      } else {
        measures = this.detectMeasures(model);
      }

      // Generate recommendation for each requested time grain
      for (const grain of grains) {
        const speedupEstimate = this.estimateSpeedup(rawRowCount, grain, dimensions, {
          catalogStatsLoaded: Boolean(this.scanResult.catalogLoaded),
          measureCount: measures.length,
        });

        // Filter by minimum speedup factor threshold
        if (speedupEstimate.speedup_factor < minSpeedup) {
          continue;
        }

        const preaggName = `${modelName}_preagg_${grain}`;
        const rollupSpec = {
          model: modelName,
          name: preaggName,
          targetSchema,
          dialect,
          timeGrain: grain,
          timeDimension,
          dimensions,
          measures,
          includeIndexes,
          refreshStrategy,
          refreshIntervalMinutes: refreshInterval,
        };

        const ddlResult = this.generateRollupDDL(rollupSpec);
        const classifiedMeasures = measures.map(m => this.classifyAdditivity(m, { dialect, includeHll: this.includeHll }));
        const acceleratedQuery = this.generateAcceleratedQuery(
          modelName,
          targetSchema,
          preaggName,
          timeDimension,
          grain,
          dimensions,
          classifiedMeasures,
          dialect
        );

        recommendations.push({
          id: `preagg_${modelName}_${grain}_${recIdx++}`,
          name: preaggName,
          type: 'rollup',
          model: modelName,
          time_dimension: timeDimension,
          time_grain: grain,
          dimensions,
          measures: classifiedMeasures,
          ddl: ddlResult.ddl,
          index_ddl: ddlResult.index_ddl,
          refresh_strategy: ddlResult.refresh_strategy,
          refresh_schedule: ddlResult.refresh_schedule,
          refresh_command: ddlResult.refresh_command,
          speedup_estimate: speedupEstimate,
          query_acceleration: acceleratedQuery,
        });

        if (recommendations.length >= maxRecommendations) {
          break;
        }
      }

      if (recommendations.length >= maxRecommendations) {
        break;
      }
    }

    return recommendations;
  }
}
