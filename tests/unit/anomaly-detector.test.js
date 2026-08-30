import {
  calculateMean,
  calculateStdDev,
  calculateQuartiles,
  calculateMAD,
  renderSparkline,
  inferColumns,
  normalizeDataset,
  runZScoreDetection,
  runIQRDetection,
  runRollingBandDetection,
  runSeasonalDetection,
  runDeltaDetection,
  analyzeDimensionalRootCause,
  detectAnomalies,
} from '../../src/analytics/anomaly-detector.js';

describe('Proactive KPI Anomaly & Outlier Detector Unit Tests (R4)', () => {
  describe('Statistical Helpers', () => {
    test('calculateMean computes arithmetic mean accurately', () => {
      expect(calculateMean([10, 20, 30, 40, 50])).toBe(30);
      expect(calculateMean([])).toBe(0);
      expect(calculateMean([100])).toBe(100);
    });

    test('calculateStdDev computes sample standard deviation', () => {
      const vals = [10, 12, 23, 23, 16, 23, 21, 16];
      const std = calculateStdDev(vals);
      expect(std).toBeCloseTo(5.237, 2);
      expect(calculateStdDev([5])).toBe(0);
    });

    test('calculateQuartiles computes median, Q1, Q3, and IQR', () => {
      const vals = [6, 7, 15, 36, 39, 40, 41, 42, 43, 47, 49];
      const { median, q1, q3, iqr } = calculateQuartiles(vals);

      expect(median).toBe(40);
      expect(q1).toBe(15);
      expect(q3).toBe(43);
      expect(iqr).toBe(28);
    });

    test('calculateMAD computes median absolute deviation', () => {
      const vals = [1, 1, 2, 2, 4, 6, 9];
      const mad = calculateMAD(vals);
      expect(mad).toBe(1);
    });
  });

  describe('renderSparkline', () => {
    test('renders Unicode sparkline matching series profile', () => {
      const series = [10, 20, 50, 80, 100];
      const sparkline = renderSparkline(series);

      expect(sparkline.length).toBe(5);
      expect(sparkline).toBe('  ▄▆█');
    });

    test('handles flat series and empty array', () => {
      expect(renderSparkline([50, 50, 50])).toBe('▄▄▄');
      expect(renderSparkline([])).toBe('');
    });
  });

  describe('inferColumns and normalizeDataset', () => {
    test('auto-infers time, metric, and dimension columns from raw records', () => {
      const records = [
        { order_date: '2026-08-01', revenue: 15000, region: 'North America', id: 101 },
        { order_date: '2026-08-02', revenue: 16200, region: 'EMEA', id: 102 },
      ];
      const inferred = inferColumns(records);

      expect(inferred.timeColumn).toBe('order_date');
      expect(inferred.metricColumn).toBe('revenue');
      expect(inferred.dimensionColumn).toBe('region');
    });

    test('normalizes Metabase dataset format { data: { rows, cols } }', () => {
      const mbData = {
        data: {
          cols: [{ name: 'dt' }, { name: 'sales' }],
          rows: [
            ['2026-08-01', 100],
            ['2026-08-02', 150],
          ],
        },
      };
      const normalized = normalizeDataset(mbData);

      expect(normalized.length).toBe(2);
      expect(normalized[0]).toEqual({ dt: '2026-08-01', sales: 100 });
      expect(normalized[1]).toEqual({ dt: '2026-08-02', sales: 150 });
    });

    test('normalizes pure array of numbers into timestamped records', () => {
      const numbers = [100, 110, 105, 500, 95];
      const normalized = normalizeDataset(numbers);

      expect(normalized.length).toBe(5);
      expect(normalized[0].value).toBe(100);
      expect(normalized[3].value).toBe(500);
      expect(normalized[0].timestamp).toBeDefined();
    });
  });

  describe('Individual Statistical Algorithms', () => {
    test('Algorithm 1: Z-Score / MAD detects high sigma outlier spike', () => {
      const series = [100, 102, 98, 101, 99, 103, 100, 102, 500, 101, 98];
      const results = runZScoreDetection(series, 'medium');

      expect(results[8].isAnomaly).toBe(true);
      expect(results[8].zScore).toBeGreaterThan(2.5);
      expect(results[0].isAnomaly).toBe(false);
    });

    test('Algorithm 2: IQR Tukey Fences detects bottom outlier drop', () => {
      const series = [100, 105, 102, 98, 110, 104, 101, 99, 10, 103, 102];
      const results = runIQRDetection(series, 'medium');

      expect(results[8].isAnomaly).toBe(true);
      expect(results[8].lowerBound).toBeGreaterThan(10);
      expect(results[0].isAnomaly).toBe(false);
    });

    test('Algorithm 3: Rolling Moving Average & Bollinger Bands detects transient shock in trending series', () => {
      // Linear upward trend with sudden local plunge
      const series = [10, 12, 14, 16, 18, 20, 22, 5, 26, 28, 30];
      const results = runRollingBandDetection(series, 'medium');

      // Index 7 (value: 5) should breach rolling lower band
      expect(results[7].isAnomaly).toBe(true);
      expect(results[7].expected).toBeGreaterThan(15);
      expect(results[7].lowerBand).toBeGreaterThan(5);
    });

    test('Algorithm 4: Seasonal Decomposition detects day-of-week anomaly', () => {
      // 3-week daily cycle where weekend values are consistently ~50 and weekday ~100
      const series = [
        100, 100, 100, 100, 100, 50, 50, // W1
        100, 100, 100, 100, 100, 50, 50, // W2
        100, 100, 100, 100, 100, 150, 50, // W3 (day 19 is 150 on a Saturday where 50 is expected)
      ];
      const results = runSeasonalDetection(series, 'medium');

      expect(results[19].isAnomaly).toBe(true);
      expect(results[19].residual).toBeGreaterThan(40);
    });

    test('Algorithm 5: Percentage Delta detects DoD surge and drop-to-zero', () => {
      const series = [100, 105, 250, 240, 0, 100];
      const results = runDeltaDetection(series, 'medium');

      // Surge from 105 to 250 (+138%)
      expect(results[2].isAnomaly).toBe(true);
      expect(results[2].deltaPct).toBeCloseTo(138.1, 1);

      // Sudden zero drop (from 240 to 0)
      expect(results[4].isAnomaly).toBe(true);
      expect(results[4].isZeroDrop).toBe(true);
    });
  });

  describe('Dimensional Root Cause Drilldown', () => {
    test('isolates top contributor category driving total anomaly delta', () => {
      const data = [
        // Normal baseline
        { date: '2026-08-20', revenue: 5000, region: 'US' },
        { date: '2026-08-20', revenue: 3000, region: 'EU' },
        { date: '2026-08-20', revenue: 2000, region: 'APAC' },
        // Anomaly day: Total revenue crashes because US plummeted
        { date: '2026-08-21', revenue: 500, region: 'US' },
        { date: '2026-08-21', revenue: 2900, region: 'EU' },
        { date: '2026-08-21', revenue: 1950, region: 'APAC' },
      ];

      const rootCause = analyzeDimensionalRootCause(data, 'date', 'revenue', 'region', '2026-08-21');

      expect(rootCause).toBeDefined();
      expect(rootCause.dimension).toBe('region');
      expect(rootCause.top_contributor).toBe('US');
      expect(rootCause.contribution_pct).toBeGreaterThan(80);
      expect(rootCause.breakdown.length).toBe(3);
    });
  });

  describe('detectAnomalies Full Ensemble Workflow & Provenance', () => {
    test('runs ensemble detection with severity classification and structured provenance', () => {
      const data = [
        { date: '2026-08-01', orders: 100 },
        { date: '2026-08-02', orders: 105 },
        { date: '2026-08-03', orders: 98 },
        { date: '2026-08-04', orders: 102 },
        { date: '2026-08-05', orders: 500 }, // Critical spike
        { date: '2026-08-06', orders: 99 },
        { date: '2026-08-07', orders: 101 },
        { date: '2026-08-08', orders: 0 }, // Critical drop to zero
        { date: '2026-08-09', orders: 104 },
      ];

      const result = detectAnomalies({
        data,
        method: 'auto',
        sensitivity: 'medium',
      });

      expect(result.metric_name).toBe('orders');
      expect(result.time_column).toBe('date');
      expect(result.total_points_analyzed).toBe(9);
      expect(result.anomalies.length).toBeGreaterThanOrEqual(2);

      const spike = result.anomalies.find(a => a.timestamp === '2026-08-05');
      expect(spike).toBeDefined();
      expect(spike.actual_value).toBe(500);
      expect(spike.severity).toBe('CRITICAL');
      expect(spike.type).toBe('spike');

      const drop = result.anomalies.find(a => a.timestamp === '2026-08-08');
      expect(drop).toBeDefined();
      expect(drop.actual_value).toBe(0);
      expect(drop.severity).toBe('CRITICAL');
      expect(drop.type).toBe('drop');

      expect(result.sparkline).toBeDefined();
      expect(result.sparkline.length).toBe(9);

      expect(result._provenance).toEqual({
        ai_generated: true,
        tool: 'ai_analytics_detect_anomalies',
        review_required: false,
        timestamp: expect.any(String),
      });
    });

    test('respects direction filtering (only spikes or only drops)', () => {
      const data = [
        { date: '2026-08-01', val: 100 },
        { date: '2026-08-02', val: 102 },
        { date: '2026-08-03', val: 400 }, // spike
        { date: '2026-08-04', val: 100 },
        { date: '2026-08-05', val: 10 },  // drop
        { date: '2026-08-06', val: 99 },
      ];

      const spikesOnly = detectAnomalies({ data, direction: 'spike' });
      expect(spikesOnly.anomalies.every(a => a.type === 'spike')).toBe(true);

      const dropsOnly = detectAnomalies({ data, direction: 'drop' });
      expect(dropsOnly.anomalies.every(a => a.type === 'drop')).toBe(true);
    });

    test('handles empty dataset gracefully', () => {
      const result = detectAnomalies({ data: [] });
      expect(result.total_points_analyzed).toBe(0);
      expect(result.anomalies_detected_count).toBe(0);
      expect(result.anomalies).toEqual([]);
    });
  });
});
