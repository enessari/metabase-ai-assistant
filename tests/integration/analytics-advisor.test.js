import { jest } from '@jest/globals';
import { AnalyticsHandler } from '../../src/mcp/handlers/analytics.js';
import { getToolDefinitions, TOOL_METADATA } from '../../src/mcp/tool-registry.js';

describe('Analytics Advisory & Anomaly Detection Integration Tests (M4)', () => {
  let mockMetabaseClient;
  let analyticsHandler;

  beforeEach(() => {
    mockMetabaseClient = {
      getDatabase: jest.fn().mockResolvedValue({
        id: 1,
        name: 'Production PostgreSQL',
        engine: 'postgres',
      }),
      executeNativeQuery: jest.fn().mockImplementation(async (dbId, sql) => {
        // If EXPLAIN query
        if (sql.toUpperCase().startsWith('EXPLAIN')) {
          return [
            {
              'QUERY PLAN': 'Seq Scan on orders (cost=0.00..12500.00 rows=50000 width=64)\n  Filter: (status = \'completed\')\nExecution Time: 320.50 ms',
            },
          ];
        }
        // If dataset query
        return {
          data: {
            cols: [{ name: 'day' }, { name: 'revenue' }, { name: 'region' }],
            rows: [
              ['2026-08-01', 10000, 'US'],
              ['2026-08-02', 10500, 'US'],
              ['2026-08-03', 10200, 'US'],
              ['2026-08-04', 9800, 'US'],
              ['2026-08-05', 45000, 'US'], // Outlier spike
              ['2026-08-06', 10100, 'US'],
              ['2026-08-07', 9900, 'US'],
            ],
          },
        };
      }),
      getQuestion: jest.fn().mockResolvedValue({
        id: 42,
        name: 'Daily Revenue Trends',
        database_id: 1,
        dataset_query: {
          type: 'native',
          native: {
            query: 'SELECT date, amount FROM transactions WHERE status = \'settled\' ORDER BY date DESC;',
          },
        },
      }),
      runQuery: jest.fn().mockResolvedValue({
        data: {
          cols: [{ name: 'date' }, { name: 'amount' }],
          rows: [
            ['2026-08-01', 500],
            ['2026-08-02', 520],
            ['2026-08-03', 510],
            ['2026-08-04', 505],
            ['2026-08-05', 0], // Drop to zero
            ['2026-08-06', 515],
          ],
        },
      }),
    };

    analyticsHandler = new AnalyticsHandler(mockMetabaseClient);
  });

  describe('handleQueryIndexAdvisor Integration', () => {
    test('analyzes SQL query, executes EXPLAIN, and returns structured recommendations with warning banner', async () => {
      const result = await analyticsHandler.handleQueryIndexAdvisor({
        sql: 'SELECT id, customer_id, total_amount FROM orders WHERE status = \'completed\' AND created_at >= \'2026-01-01\' ORDER BY total_amount DESC;',
        database_id: 1,
        run_explain: true,
      });

      expect(result.content).toBeDefined();
      expect(result.content[0].type).toBe('text');
      expect(result.content[0].text).toContain('⚠️ **[AI-GENERATED CONTENT — REVIEW BEFORE EXECUTING]**');
      expect(result.content[0].text).toContain('AI Query Index & Materialized View Advisory');
      expect(result.content[0].text).toContain('CREATE INDEX CONCURRENTLY IF NOT EXISTS claude_ai_idx_orders_');

      expect(result.structuredContent).toBeDefined();
      expect(result.structuredContent.dialect).toBe('postgres');
      expect(result.structuredContent.database_id).toBe(1);
      expect(result.structuredContent.index_recommendations.length).toBeGreaterThan(0);

      const primaryIdx = result.structuredContent.index_recommendations[0];
      expect(primaryIdx.table).toBe('orders');
      expect(primaryIdx.columns).toEqual(expect.arrayContaining(['status', 'created_at', 'total_amount']));
      expect(primaryIdx.priority).toBe('HIGH');

      expect(result.structuredContent._provenance).toEqual({
        ai_generated: true,
        tool: 'ai_query_index_advisor',
        review_required: true,
        timestamp: expect.any(String),
        dialect: 'postgres',
      });
    });

    test('extracts SQL from Metabase card_id when SQL is not directly supplied', async () => {
      const result = await analyticsHandler.handleQueryIndexAdvisor({
        card_id: 42,
      });

      expect(mockMetabaseClient.getQuestion).toHaveBeenCalledWith(42);
      expect(result.structuredContent).toBeDefined();
      expect(result.structuredContent.sql).toContain('transactions');
      expect(result.structuredContent.query_analysis.tables).toContain('transactions');
    });

    test('returns structured error if input is invalid', async () => {
      const result = await analyticsHandler.handleQueryIndexAdvisor({
        sql: '',
      });

      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain('Query index advisory failed');
    });
  });

  describe('handleDetectAnomalies Integration', () => {
    test('executes native SQL against database and detects KPI anomalies with sparkline and baseline', async () => {
      const result = await analyticsHandler.handleDetectAnomalies({
        sql: 'SELECT day, revenue, region FROM sales_summary;',
        database_id: 1,
        method: 'auto',
        sensitivity: 'medium',
      });

      expect(mockMetabaseClient.executeNativeQuery).toHaveBeenCalledWith(1, 'SELECT day, revenue, region FROM sales_summary;');

      expect(result.content[0].text).toContain('Proactive KPI Anomaly & Outlier Detection Report');
      expect(result.content[0].text).toContain('Trend Sparkline:');
      expect(result.content[0].text).toContain('CRITICAL');

      expect(result.structuredContent).toBeDefined();
      expect(result.structuredContent.total_points_analyzed).toBe(7);
      expect(result.structuredContent.anomalies_detected_count).toBeGreaterThanOrEqual(1);

      const anomaly = result.structuredContent.anomalies[0];
      expect(anomaly.timestamp).toBe('2026-08-05');
      expect(anomaly.actual_value).toBe(45000);
      expect(anomaly.severity).toBe('CRITICAL');

      expect(result.structuredContent._provenance).toEqual({
        ai_generated: true,
        tool: 'ai_analytics_detect_anomalies',
        review_required: false,
        timestamp: expect.any(String),
      });
    });

    test('runs anomaly detection from Metabase card_id', async () => {
      const result = await analyticsHandler.handleDetectAnomalies({
        card_id: 42,
      });

      expect(mockMetabaseClient.runQuery).toHaveBeenCalledWith({ type: 'card', card_id: 42 });
      expect(result.structuredContent).toBeDefined();
      expect(result.structuredContent.total_points_analyzed).toBe(6);

      const zeroDrop = result.structuredContent.anomalies.find(a => a.actual_value === 0);
      expect(zeroDrop).toBeDefined();
      expect(zeroDrop.type).toBe('drop');
    });

    test('accepts raw data array directly', async () => {
      const rawData = [
        { date: '2026-08-01', visitors: 1000 },
        { date: '2026-08-02', visitors: 1050 },
        { date: '2026-08-03', visitors: 990 },
        { date: '2026-08-04', visitors: 1020 },
        { date: '2026-08-05', visitors: 8000 }, // Huge spike
        { date: '2026-08-06', visitors: 1010 },
      ];

      const result = await analyticsHandler.handleDetectAnomalies({
        data: rawData,
        sensitivity: 'high',
      });

      expect(result.structuredContent).toBeDefined();
      expect(result.structuredContent.metric_name).toBe('visitors');
      expect(result.structuredContent.anomalies.length).toBeGreaterThanOrEqual(1);
    });

    test('returns structured error when no dataset can be resolved', async () => {
      const result = await analyticsHandler.handleDetectAnomalies({});

      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain('No valid dataset could be retrieved');
    });
  });

  describe('Tool Registry Validation for M4 Tools', () => {
    test('registers ai_query_index_advisor and ai_analytics_detect_anomalies with annotations', () => {
      const tools = getToolDefinitions();

      const advisorTool = tools.find(t => t.name === 'ai_query_index_advisor');
      expect(advisorTool).toBeDefined();
      expect(advisorTool.annotations.readOnlyHint).toBe(true);
      expect(advisorTool.outputSchema).toBeDefined();
      expect(advisorTool.outputSchema.properties._provenance).toBeDefined();

      const anomalyTool = tools.find(t => t.name === 'ai_analytics_detect_anomalies');
      expect(anomalyTool).toBeDefined();
      expect(anomalyTool.annotations.readOnlyHint).toBe(true);
      expect(anomalyTool.outputSchema).toBeDefined();
      expect(anomalyTool.outputSchema.properties._provenance).toBeDefined();
    });

    test('routes map correctly in AnalyticsHandler', () => {
      const routes = analyticsHandler.routes();
      expect(routes['ai_query_index_advisor']).toBeDefined();
      expect(routes['ai_analytics_detect_anomalies']).toBeDefined();
    });
  });
});
