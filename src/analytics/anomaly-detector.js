/**
 * Statistical calculations and anomaly detection algorithms
 */

/**
 * Calculates mean of numeric array
 * @param {Array<number>} values 
 * @returns {number}
 */
export function calculateMean(values) {
  if (!values || values.length === 0) return 0;
  const sum = values.reduce((acc, val) => acc + (Number(val) || 0), 0);
  return sum / values.length;
}

/**
 * Calculates standard deviation
 * @param {Array<number>} values 
 * @param {number} [mean]
 * @returns {number}
 */
export function calculateStdDev(values, mean = null) {
  if (!values || values.length <= 1) return 0;
  const m = mean !== null ? mean : calculateMean(values);
  const variance = values.reduce((acc, val) => acc + Math.pow((Number(val) || 0) - m, 2), 0) / (values.length - 1);
  return Math.sqrt(variance);
}

/**
 * Calculates median and quartiles
 * @param {Array<number>} values 
 * @returns {{ median: number, q1: number, q3: number, iqr: number }}
 */
export function calculateQuartiles(values) {
  if (!values || values.length === 0) {
    return { median: 0, q1: 0, q3: 0, iqr: 0 };
  }
  const sorted = [...values].map(Number).sort((a, b) => a - b);
  const n = sorted.length;

  const getMedianOfSlice = (arr) => {
    if (arr.length === 0) return 0;
    const mid = Math.floor(arr.length / 2);
    if (arr.length % 2 === 0) {
      return (arr[mid - 1] + arr[mid]) / 2;
    }
    return arr[mid];
  };

  const median = getMedianOfSlice(sorted);
  const mid = Math.floor(n / 2);
  const lowerHalf = sorted.slice(0, mid);
  const upperHalf = n % 2 === 0 ? sorted.slice(mid) : sorted.slice(mid + 1);

  const q1 = lowerHalf.length > 0 ? getMedianOfSlice(lowerHalf) : median;
  const q3 = upperHalf.length > 0 ? getMedianOfSlice(upperHalf) : median;
  const iqr = q3 - q1;

  return { median, q1, q3, iqr };
}

/**
 * Calculates Median Absolute Deviation (MAD)
 * @param {Array<number>} values 
 * @param {number} [med]
 * @returns {number}
 */
export function calculateMAD(values, med = null) {
  if (!values || values.length === 0) return 0;
  const m = med !== null ? med : calculateQuartiles(values).median;
  const absDevs = values.map(v => Math.abs((Number(v) || 0) - m));
  return calculateQuartiles(absDevs).median;
}

/**
 * Generates Unicode Sparkline for a numeric series
 * @param {Array<number>} values 
 * @returns {string}
 */
export function renderSparkline(values) {
  if (!values || values.length === 0) return '';
  const numValues = values.map(v => (typeof v === 'number' ? v : Number(v) || 0));
  const min = Math.min(...numValues);
  const max = Math.max(...numValues);
  const span = max - min;

  const ticks = [' ', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
  if (span === 0) {
    return ticks[3].repeat(numValues.length);
  }

  return numValues
    .map(v => {
      const normalized = (v - min) / span;
      const index = Math.min(ticks.length - 1, Math.max(0, Math.floor(normalized * (ticks.length - 1))));
      return ticks[index];
    })
    .join('');
}

/**
 * Auto-infers time, metric, and dimension columns from tabular records
 * @param {Array<object>} rows 
 * @param {Array<string>} [providedCols]
 * @returns {{ timeColumn: string, metricColumn: string, dimensionColumn: string }}
 */
export function inferColumns(rows, providedCols = null) {
  if (!rows || rows.length === 0) {
    return { timeColumn: null, metricColumn: null, dimensionColumn: null };
  }

  const sample = rows[0];
  const colKeys = providedCols || Object.keys(sample);

  let detectedTime = null;
  let detectedMetric = null;
  let detectedDimension = null;

  // 1. Detect Time Column
  const timeKeywords = /date|time|created|timestamp|day|month|year|period|bucket|dt/i;
  for (const col of colKeys) {
    if (timeKeywords.test(col)) {
      detectedTime = col;
      break;
    }
  }
  if (!detectedTime) {
    // Check if values parse as valid dates
    for (const col of colKeys) {
      const val = sample[col];
      if (typeof val === 'string' && !isNaN(Date.parse(val)) && isNaN(Number(val))) {
        detectedTime = col;
        break;
      }
    }
  }

  // 2. Detect Metric Column (numeric, prefer non-id)
  const metricKeywords = /amount|revenue|total|sales|count|value|metric|price|cost|qty|quantity|sum|score|volume/i;
  for (const col of colKeys) {
    if (col === detectedTime) continue;
    if (metricKeywords.test(col)) {
      const val = Number(sample[col]);
      if (!isNaN(val)) {
        detectedMetric = col;
        break;
      }
    }
  }
  if (!detectedMetric) {
    for (const col of colKeys) {
      if (col === detectedTime) continue;
      if (!/id$|_id$/i.test(col)) {
        const val = Number(sample[col]);
        if (!isNaN(val) && typeof sample[col] !== 'boolean') {
          detectedMetric = col;
          break;
        }
      }
    }
  }
  if (!detectedMetric) {
    for (const col of colKeys) {
      if (col === detectedTime) continue;
      const val = Number(sample[col]);
      if (!isNaN(val)) {
        detectedMetric = col;
        break;
      }
    }
  }

  // 3. Detect Dimension Column (low cardinality string/categorical)
  const dimKeywords = /category|region|channel|status|segment|country|source|type|plan|group|store|branch/i;
  for (const col of colKeys) {
    if (col === detectedTime || col === detectedMetric) continue;
    if (dimKeywords.test(col)) {
      detectedDimension = col;
      break;
    }
  }
  if (!detectedDimension) {
    for (const col of colKeys) {
      if (col === detectedTime || col === detectedMetric) continue;
      if (typeof sample[col] === 'string' && !sample[col].startsWith('http')) {
        detectedDimension = col;
        break;
      }
    }
  }

  return {
    timeColumn: detectedTime || colKeys[0],
    metricColumn: detectedMetric || colKeys[1] || colKeys[0],
    dimensionColumn: detectedDimension || null,
  };
}

/**
 * Normalizes input raw data (arrays, objects, Metabase dataset) into flat record objects
 * @param {any} inputData 
 * @returns {Array<object>}
 */
export function normalizeDataset(inputData) {
  if (!inputData) return [];

  // Metabase dataset format: { data: { rows: [...], cols: [...] } }
  if (inputData.data && Array.isArray(inputData.data.rows) && Array.isArray(inputData.data.cols)) {
    const colNames = inputData.data.cols.map(c => (typeof c === 'string' ? c : c.name || c.display_name));
    return inputData.data.rows.map(row => {
      const obj = {};
      colNames.forEach((col, idx) => {
        obj[col] = Array.isArray(row) ? row[idx] : row[col];
      });
      return obj;
    });
  }

  // Generic { rows: [...], columns: [...] }
  if (Array.isArray(inputData.rows) && Array.isArray(inputData.columns)) {
    const colNames = inputData.columns.map(c => (typeof c === 'string' ? c : c.name || c));
    return inputData.rows.map(row => {
      const obj = {};
      colNames.forEach((col, idx) => {
        obj[col] = Array.isArray(row) ? row[idx] : row[col];
      });
      return obj;
    });
  }

  // Pure array of numbers: [100, 105, 90, 500, ...]
  if (Array.isArray(inputData) && inputData.length > 0 && typeof inputData[0] === 'number') {
    return inputData.map((val, idx) => {
      const date = new Date(Date.UTC(2026, 0, 1 + idx));
      return {
        timestamp: date.toISOString().split('T')[0],
        value: val,
      };
    });
  }

  // Already array of objects
  if (Array.isArray(inputData)) {
    return inputData;
  }

  return [];
}

/**
 * Algorithm 1: Z-Score & Modified Z-Score (MAD)
 */
export function runZScoreDetection(values, sensitivity = 'medium') {
  const n = values.length;
  if (n < 3) return values.map(() => ({ isAnomaly: false, score: 0, zScore: 0, modZScore: 0 }));

  const mean = calculateMean(values);
  const std = calculateStdDev(values, mean);
  const { median } = calculateQuartiles(values);
  const mad = calculateMAD(values, median);

  // Thresholds based on sensitivity
  let zThresh = 2.5;
  let modZThresh = 3.0;
  if (sensitivity === 'high') {
    zThresh = 2.0;
    modZThresh = 2.5;
  } else if (sensitivity === 'low') {
    zThresh = 3.2;
    modZThresh = 4.0;
  }

  return values.map(v => {
    const val = Number(v) || 0;
    const z = std > 0 ? (val - mean) / std : 0;
    const modZ = mad > 0 ? (0.6745 * (val - median)) / mad : (std > 0 ? z : 0);

    const isZAnomaly = Math.abs(z) >= zThresh;
    const isModZAnomaly = Math.abs(modZ) >= modZThresh;
    const isAnomaly = isZAnomaly || isModZAnomaly;

    const normScore = Math.min(1.0, Math.max(Math.abs(z) / (zThresh * 1.5), Math.abs(modZ) / (modZThresh * 1.5)));

    return {
      isAnomaly,
      score: isAnomaly ? normScore : 0,
      zScore: Math.round(z * 100) / 100,
      modZScore: Math.round(modZ * 100) / 100,
      expected: Math.round(mean * 100) / 100,
    };
  });
}

/**
 * Algorithm 2: IQR (Tukey's Fences)
 */
export function runIQRDetection(values, sensitivity = 'medium') {
  const n = values.length;
  if (n < 4) return values.map(() => ({ isAnomaly: false, score: 0, lowerBound: 0, upperBound: 0 }));

  const { q1, q3, iqr, median } = calculateQuartiles(values);

  let k = 1.5;
  if (sensitivity === 'high') k = 1.0;
  if (sensitivity === 'low') k = 2.5;

  const lowerBound = q1 - k * iqr;
  const upperBound = q3 + k * iqr;

  return values.map(v => {
    const val = Number(v) || 0;
    const isBelow = val < lowerBound;
    const isAbove = val > upperBound;
    const isAnomaly = isBelow || isAbove;

    let score = 0;
    if (isAnomaly) {
      const dev = isBelow ? lowerBound - val : val - upperBound;
      score = Math.min(1.0, 0.5 + (dev / (iqr || 1)) * 0.3);
    }

    return {
      isAnomaly,
      score: isAnomaly ? score : 0,
      lowerBound: Math.round(lowerBound * 100) / 100,
      upperBound: Math.round(upperBound * 100) / 100,
      expected: Math.round(median * 100) / 100,
    };
  });
}

/**
 * Algorithm 3: Rolling Moving Average with Dynamic Bollinger-style Bands
 */
export function runRollingBandDetection(values, sensitivity = 'medium') {
  const n = values.length;
  const windowSize = Math.max(3, Math.min(7, Math.floor(n / 4)));

  let k = 2.3;
  if (sensitivity === 'high') k = 1.8;
  if (sensitivity === 'low') k = 3.0;

  return values.map((v, idx) => {
    const val = Number(v) || 0;
    if (idx < 2) {
      return { isAnomaly: false, score: 0, expected: val, lowerBand: val, upperBand: val };
    }

    const startIdx = Math.max(0, idx - windowSize);
    const windowSlice = values.slice(startIdx, idx);
    const rMean = calculateMean(windowSlice);
    const rStd = calculateStdDev(windowSlice, rMean) || (rMean * 0.05);

    const lowerBand = rMean - k * rStd;
    const upperBand = rMean + k * rStd;

    const isBelow = val < lowerBand;
    const isAbove = val > upperBand;
    const isAnomaly = isBelow || isAbove;

    let score = 0;
    if (isAnomaly) {
      const dev = Math.abs(val - rMean);
      score = Math.min(1.0, 0.4 + (dev / (k * rStd || 1)) * 0.3);
    }

    return {
      isAnomaly,
      score: isAnomaly ? score : 0,
      expected: Math.round(rMean * 100) / 100,
      lowerBand: Math.round(lowerBand * 100) / 100,
      upperBand: Math.round(upperBand * 100) / 100,
    };
  });
}

/**
 * Algorithm 4: Seasonal Decomposition (STL-style)
 */
export function runSeasonalDetection(values, sensitivity = 'medium') {
  const n = values.length;
  let period = 7; // default weekly for daily data
  if (n < 14) period = Math.max(2, Math.floor(n / 2));
  if (n < 6) {
    return values.map(() => ({ isAnomaly: false, score: 0, residual: 0 }));
  }

  const globalMean = calculateMean(values);

  // Compute seasonal indices
  const seasonalSums = Array(period).fill(0);
  const seasonalCounts = Array(period).fill(0);

  values.forEach((v, idx) => {
    const p = idx % period;
    seasonalSums[p] += Number(v) || 0;
    seasonalCounts[p] += 1;
  });

  const seasonalIndices = seasonalSums.map((sum, p) => {
    const avg = seasonalCounts[p] > 0 ? sum / seasonalCounts[p] : globalMean;
    return avg - globalMean;
  });

  // Calculate de-seasonalized series and residuals
  const deseasonalized = values.map((v, idx) => (Number(v) || 0) - seasonalIndices[idx % period]);

  // Trend estimation via moving average on deseasonalized
  const halfWindow = Math.floor(period / 2);
  const residuals = deseasonalized.map((dVal, idx) => {
    const start = Math.max(0, idx - halfWindow);
    const end = Math.min(n, idx + halfWindow + 1);
    const slice = deseasonalized.slice(start, end);
    const trend = calculateMean(slice);
    return dVal - trend;
  });

  const resMean = calculateMean(residuals);
  const resStd = calculateStdDev(residuals, resMean) || (globalMean * 0.05);

  let k = 2.5;
  if (sensitivity === 'high') k = 2.0;
  if (sensitivity === 'low') k = 3.2;

  return values.map((v, idx) => {
    const res = residuals[idx];
    const isAnomaly = Math.abs(res) >= k * resStd;
    const score = isAnomaly ? Math.min(1.0, 0.4 + (Math.abs(res) / (k * resStd || 1)) * 0.3) : 0;

    return {
      isAnomaly,
      score,
      residual: Math.round(res * 100) / 100,
      expected: Math.round(((Number(v) || 0) - res) * 100) / 100,
    };
  });
}

/**
 * Algorithm 5: Percentage Delta (Period-over-Period)
 */
export function runDeltaDetection(values, sensitivity = 'medium') {
  let threshPct = 50;
  if (sensitivity === 'high') threshPct = 30;
  if (sensitivity === 'low') threshPct = 75;

  return values.map((v, idx) => {
    const val = Number(v) || 0;
    if (idx === 0) return { isAnomaly: false, score: 0, deltaPct: 0 };

    const prev = Number(values[idx - 1]) || 0;
    let deltaPct = 0;
    if (prev !== 0) {
      deltaPct = ((val - prev) / Math.abs(prev)) * 100;
    } else if (val !== 0) {
      deltaPct = 100;
    }

    const isZeroDrop = val === 0 && prev > 0;
    const isThreshBreach = Math.abs(deltaPct) >= threshPct;
    const isAnomaly = isZeroDrop || isThreshBreach;

    let score = 0;
    if (isAnomaly) {
      if (isZeroDrop) score = 0.95;
      else score = Math.min(1.0, 0.4 + (Math.abs(deltaPct) / (threshPct * 2)) * 0.4);
    }

    return {
      isAnomaly,
      score,
      deltaPct: Math.round(deltaPct * 10) / 10,
      isZeroDrop,
    };
  });
}

/**
 * Analyzes dimensional root cause for an anomaly timestamp
 * @param {Array<object>} data 
 * @param {string} timeColumn 
 * @param {string} metricColumn 
 * @param {string} dimensionColumn 
 * @param {string} anomalyTimestamp 
 * @returns {object}
 */
export function analyzeDimensionalRootCause(data, timeColumn, metricColumn, dimensionColumn, anomalyTimestamp) {
  if (!data || !dimensionColumn || !anomalyTimestamp) {
    return null;
  }

  // Filter rows for the anomaly timestamp
  const targetRows = data.filter(r => String(r[timeColumn]) === String(anomalyTimestamp));
  // Filter prior baseline rows
  const baselineRows = data.filter(r => String(r[timeColumn]) !== String(anomalyTimestamp));

  if (targetRows.length === 0) return null;

  // Aggregate metric by dimension for target timestamp
  const targetDimTotals = {};
  targetRows.forEach(r => {
    const dimVal = String(r[dimensionColumn] || 'Unknown');
    targetDimTotals[dimVal] = (targetDimTotals[dimVal] || 0) + (Number(r[metricColumn]) || 0);
  });

  // Calculate baseline averages per dimension
  const baselineDimSums = {};
  const baselineDimCounts = {};
  baselineRows.forEach(r => {
    const dimVal = String(r[dimensionColumn] || 'Unknown');
    baselineDimSums[dimVal] = (baselineDimSums[dimVal] || 0) + (Number(r[metricColumn]) || 0);
    baselineDimCounts[dimVal] = (baselineDimCounts[dimVal] || 0) + 1;
  });

  const dimDeltas = [];
  let totalDeltaSum = 0;

  const allDimensions = new Set([...Object.keys(targetDimTotals), ...Object.keys(baselineDimSums)]);

  allDimensions.forEach(dim => {
    const actual = targetDimTotals[dim] || 0;
    const baseCount = baselineDimCounts[dim] || 1;
    const baseAvg = (baselineDimSums[dim] || 0) / baseCount;
    const delta = actual - baseAvg;

    totalDeltaSum += Math.abs(delta);
    dimDeltas.push({
      dimension_value: dim,
      actual_value: Math.round(actual * 100) / 100,
      baseline_value: Math.round(baseAvg * 100) / 100,
      delta: Math.round(delta * 100) / 100,
      abs_delta: Math.abs(delta),
    });
  });

  dimDeltas.sort((a, b) => b.abs_delta - a.abs_delta);

  const breakdown = dimDeltas.map(d => ({
    dimension_value: d.dimension_value,
    actual: d.actual_value,
    baseline: d.baseline_value,
    delta: d.delta,
    contribution_pct: totalDeltaSum > 0 ? Math.round((d.abs_delta / totalDeltaSum) * 1000) / 10 : 0,
  }));

  const top = breakdown[0] || null;

  return {
    dimension: dimensionColumn,
    top_contributor: top ? top.dimension_value : 'N/A',
    contribution_pct: top ? top.contribution_pct : 0,
    breakdown: breakdown.slice(0, 5),
  };
}

/**
 * Main Anomaly Detection Engine: Runs multi-algorithm ensemble on time-series records
 * @param {object} params
 * @param {Array<object>|object} params.data - Tabular rows, Metabase dataset, or numeric array
 * @param {string} [params.timeColumn]
 * @param {string} [params.metricColumn]
 * @param {string} [params.dimensionColumn]
 * @param {string} [params.method='auto'] - 'auto', 'z_score', 'modified_z_score', 'iqr', 'moving_average', 'seasonal_decomposition', 'percentage_delta'
 * @param {string} [params.sensitivity='medium'] - 'low', 'medium', 'high'
 * @param {string} [params.direction='both'] - 'both', 'spike', 'drop'
 * @param {number} [params.maxAnomalies=20]
 * @returns {object}
 */
export function detectAnomalies({
  data,
  timeColumn,
  metricColumn,
  dimensionColumn,
  method = 'auto',
  sensitivity = 'medium',
  direction = 'both',
  maxAnomalies = 20,
}) {
  const records = normalizeDataset(data);

  if (!records || records.length === 0) {
    return {
      metric_name: metricColumn || 'value',
      time_column: timeColumn || 'timestamp',
      total_points_analyzed: 0,
      anomalies_detected_count: 0,
      method_used: method,
      sensitivity,
      baseline_summary: { mean: 0, median: 0, std_dev: 0, min: 0, max: 0, trend: 'no_data' },
      anomalies: [],
      summary: { total_points: 0, anomaly_count: 0, critical_count: 0, warning_count: 0 },
      dimensional_breakdown: null,
      sparkline: '',
      _provenance: {
        ai_generated: true,
        tool: 'ai_analytics_detect_anomalies',
        review_required: false,
        timestamp: new Date().toISOString(),
      },
    };
  }

  // Auto-infer columns if not passed
  const inferred = inferColumns(records);
  const finalTimeCol = timeColumn || inferred.timeColumn || 'timestamp';
  const finalMetricCol = metricColumn || inferred.metricColumn || 'value';
  const finalDimCol = dimensionColumn || inferred.dimensionColumn || null;

  // Sort records chronologically
  const sortedRecords = [...records].sort((a, b) => {
    const tA = String(a[finalTimeCol] || '');
    const tB = String(b[finalTimeCol] || '');
    return tA.localeCompare(tB);
  });

  const numericValues = sortedRecords.map(r => {
    const v = Number(r[finalMetricCol]);
    return isNaN(v) ? 0 : v;
  });

  const mean = calculateMean(numericValues);
  const std = calculateStdDev(numericValues, mean);
  const { median, q1, q3, iqr } = calculateQuartiles(numericValues);
  const min = Math.min(...numericValues);
  const max = Math.max(...numericValues);

  // Trend detection
  let trend = 'stable';
  if (numericValues.length >= 4) {
    const firstHalf = numericValues.slice(0, Math.floor(numericValues.length / 2));
    const secondHalf = numericValues.slice(Math.floor(numericValues.length / 2));
    const mean1 = calculateMean(firstHalf);
    const mean2 = calculateMean(secondHalf);
    if (mean1 > 0) {
      const changePct = ((mean2 - mean1) / mean1) * 100;
      if (changePct > 5) trend = `upward (+${changePct.toFixed(1)}%)`;
      else if (changePct < -5) trend = `downward (${changePct.toFixed(1)}%)`;
    }
  }

  // Execute Individual Algorithms
  const zResults = runZScoreDetection(numericValues, sensitivity);
  const iqrResults = runIQRDetection(numericValues, sensitivity);
  const rollResults = runRollingBandDetection(numericValues, sensitivity);
  const seasonResults = runSeasonalDetection(numericValues, sensitivity);
  const deltaResults = runDeltaDetection(numericValues, sensitivity);

  const sparkline = renderSparkline(numericValues);

  const anomalies = [];

  for (let i = 0; i < sortedRecords.length; i++) {
    const val = numericValues[i];
    const timeVal = sortedRecords[i][finalTimeCol];

    const z = zResults[i];
    const iq = iqrResults[i];
    const roll = rollResults[i];
    const seas = seasonResults[i];
    const del = deltaResults[i];

    const methodsFlagged = [];
    let compositeScore = 0;

    if (method === 'z_score' || method === 'modified_z_score') {
      if (z.isAnomaly) {
        methodsFlagged.push('z_score');
        compositeScore = z.score;
      }
    } else if (method === 'iqr') {
      if (iq.isAnomaly) {
        methodsFlagged.push('iqr');
        compositeScore = iq.score;
      }
    } else if (method === 'moving_average') {
      if (roll.isAnomaly) {
        methodsFlagged.push('moving_average');
        compositeScore = roll.score;
      }
    } else if (method === 'seasonal_decomposition') {
      if (seas.isAnomaly) {
        methodsFlagged.push('seasonal_decomposition');
        compositeScore = seas.score;
      }
    } else if (method === 'percentage_delta') {
      if (del.isAnomaly) {
        methodsFlagged.push('percentage_delta');
        compositeScore = del.score;
      }
    } else {
      // 'auto' / ensemble voting
      if (z.isAnomaly) { methodsFlagged.push('z_score'); compositeScore += z.score * 0.25; }
      if (iq.isAnomaly) { methodsFlagged.push('iqr'); compositeScore += iq.score * 0.20; }
      if (roll.isAnomaly) { methodsFlagged.push('moving_average'); compositeScore += roll.score * 0.25; }
      if (seas.isAnomaly) { methodsFlagged.push('seasonal_decomposition'); compositeScore += seas.score * 0.15; }
      if (del.isAnomaly) { methodsFlagged.push('percentage_delta'); compositeScore += del.score * 0.15; }

      // Boost if multiple methods agree
      if (methodsFlagged.length >= 3) compositeScore = Math.min(1.0, compositeScore * 1.3);
    }

    compositeScore = Math.min(1.0, Math.round(compositeScore * 100) / 100);

    // Filter by threshold
    let thresh = 0.45;
    if (sensitivity === 'high') thresh = 0.35;
    if (sensitivity === 'low') thresh = 0.65;

    const isFlagged = (method === 'auto' ? compositeScore >= thresh : methodsFlagged.length > 0);

    if (isFlagged) {
      const expected = roll.expected || z.expected || median;
      const absDev = val - expected;
      const pctDev = expected !== 0 ? Math.round((absDev / Math.abs(expected)) * 10000) / 100 : 100;
      const anomalyType = val >= expected ? 'spike' : 'drop';

      // Direction filtering
      if (direction === 'spike' && anomalyType !== 'spike') continue;
      if (direction === 'drop' && anomalyType !== 'drop') continue;

      // Severity assignment
      let severity = 'INFO';
      if (compositeScore >= 0.80 || Math.abs(pctDev) >= 100 || del.isZeroDrop) {
        severity = 'CRITICAL';
      } else if (compositeScore >= 0.55 || Math.abs(pctDev) >= 40) {
        severity = 'WARNING';
      }

      // Dimensional root cause drilldown
      let rootCause = null;
      if (finalDimCol) {
        rootCause = analyzeDimensionalRootCause(sortedRecords, finalTimeCol, finalMetricCol, finalDimCol, timeVal);
      }

      const insight = `${anomalyType === 'spike' ? 'Surge' : 'Drop'} of ${pctDev > 0 ? '+' : ''}${pctDev}% relative to baseline (${expected.toLocaleString()})`;

      anomalies.push({
        timestamp: timeVal,
        actual_value: Math.round(val * 100) / 100,
        expected_value: Math.round(expected * 100) / 100,
        lower_bound: roll.lowerBand || Math.round((q1 - 1.5 * iqr) * 100) / 100,
        upper_bound: roll.upperBand || Math.round((q3 + 1.5 * iqr) * 100) / 100,
        absolute_deviation: Math.round(absDev * 100) / 100,
        percentage_deviation: pctDev,
        severity,
        type: anomalyType,
        anomaly_score: compositeScore,
        methods_flagged: methodsFlagged.length > 0 ? methodsFlagged : ['ensemble'],
        insight,
        root_cause: rootCause,
      });
    }
  }

  // Sort anomalies by score descending (or timestamp) and slice to maxAnomalies
  anomalies.sort((a, b) => b.anomaly_score - a.anomaly_score);
  const finalAnomalies = anomalies.slice(0, maxAnomalies);

  const criticalCount = finalAnomalies.filter(a => a.severity === 'CRITICAL').length;
  const warningCount = finalAnomalies.filter(a => a.severity === 'WARNING').length;

  return {
    metric_name: finalMetricCol,
    time_column: finalTimeCol,
    dimension_column: finalDimCol,
    total_points_analyzed: sortedRecords.length,
    anomalies_detected_count: finalAnomalies.length,
    method_used: method,
    sensitivity,
    baseline_summary: {
      mean: Math.round(mean * 100) / 100,
      median: Math.round(median * 100) / 100,
      std_dev: Math.round(std * 100) / 100,
      min: Math.round(min * 100) / 100,
      max: Math.round(max * 100) / 100,
      trend,
    },
    anomalies: finalAnomalies,
    summary: {
      total_points: sortedRecords.length,
      anomaly_count: finalAnomalies.length,
      critical_count: criticalCount,
      warning_count: warningCount,
    },
    sparkline,
    _provenance: {
      ai_generated: true,
      tool: 'ai_analytics_detect_anomalies',
      review_required: false,
      timestamp: new Date().toISOString(),
    },
  };
}
