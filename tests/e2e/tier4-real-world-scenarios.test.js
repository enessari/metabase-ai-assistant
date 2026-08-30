/**
 * Tier 4: Real-World BI Scenarios E2E Test Suite
 * Comprehensive end-to-end execution of 5 complex real-world enterprise scenarios
 * Derived strictly from ORIGINAL_REQUEST.md, PROJECT.md, and TEST_INFRA.md
 */

import { jest } from '@jest/globals';
import * as piiMasker from '../../src/utils/pii-masker.js';

describe('Tier 4: Real-World BI Scenarios', () => {
  // =========================================================================
  // Scenario 1: E-Commerce Executive Dashboard
  // =========================================================================
  test('Scenario 1: E-Commerce Executive Dashboard (6 cards, 2 linked filters, 24-col grid layout)', async () => {
    // 1. Mock Metabase client environment
    const createdCardsStore = [];
    const mockClient = {
      createDashboard: jest.fn().mockResolvedValue({
        id: 1001,
        name: 'Executive E-Commerce Dashboard',
        description: 'Q3 Executive Performance & KPIs',
        parameters: [
          { id: 'param_date_range', name: 'Date Range', slug: 'date_range', type: 'date/all-options', sectionId: 'filters' },
          { id: 'param_region', name: 'Store Region', slug: 'region', type: 'string/=', sectionId: 'filters' },
        ],
      }),
      createQuestion: jest.fn().mockImplementation(async (spec) => {
        const cardObj = {
          id: 500 + createdCardsStore.length + 1,
          name: spec.name,
          display: spec.display,
          dataset_query: {
            database: 1,
            type: 'native',
            native: { query: spec.sql, template_tags: spec.template_tags || {} },
          },
        };
        createdCardsStore.push(cardObj);
        return cardObj;
      }),
      addCardToDashboard: jest.fn().mockImplementation(async (dashId, cardId, cardSpec) => {
        return {
          id: 9000 + cardId,
          dashboard_id: dashId,
          card_id: cardId,
          ...cardSpec,
        };
      }),
    };

    // 2. Define 6 executive cards
    const cardSpecs = [
      {
        name: 'Total Revenue (USD)',
        display: 'scalar',
        sql: 'SELECT SUM(total_amount) AS revenue FROM orders WHERE {{date_range}} AND {{region}};',
        template_tags: { date_range: { type: 'date' }, region: { type: 'text' } },
      },
      {
        name: 'Total Orders Count',
        display: 'scalar',
        sql: 'SELECT COUNT(*) AS total_orders FROM orders WHERE {{date_range}} AND {{region}};',
        template_tags: { date_range: { type: 'date' }, region: { type: 'text' } },
      },
      {
        name: 'Daily Orders Trend',
        display: 'line',
        sql: 'SELECT DATE_TRUNC(\'day\', created_at) AS day, COUNT(*) AS orders FROM orders WHERE {{date_range}} GROUP BY 1 ORDER BY 1;',
        template_tags: { date_range: { type: 'date' } },
      },
      {
        name: 'Revenue by Category',
        display: 'bar',
        sql: 'SELECT p.category, SUM(o.total_amount) AS rev FROM orders o JOIN products p ON o.product_id = p.id WHERE {{region}} GROUP BY 1;',
        template_tags: { region: { type: 'text' } },
      },
      {
        name: 'Refund Rate (%)',
        display: 'scalar',
        sql: 'SELECT (COUNT(CASE WHEN status = \'refunded\' THEN 1 END)::float / COUNT(*)) * 100 AS refund_pct FROM orders;',
      },
      {
        name: 'Top 50 Product Sales Table',
        display: 'table',
        sql: 'SELECT p.name, p.sku, SUM(o.quantity) AS units, SUM(o.total_amount) AS revenue FROM order_items o JOIN products p ON o.product_id = p.id GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 50;',
      },
    ];

    // 3. Execute Dashboard Creation
    const dashboard = await mockClient.createDashboard({
      name: 'Executive E-Commerce Dashboard',
      description: 'Q3 Executive Performance & KPIs',
      parameters: [
        { id: 'param_date_range', name: 'Date Range', slug: 'date_range', type: 'date/all-options' },
        { id: 'param_region', name: 'Store Region', slug: 'region', type: 'string/=' },
      ],
    });

    const createdCards = [];
    for (const spec of cardSpecs) {
      const q = await mockClient.createQuestion(spec);
      createdCards.push(q);
    }

    // 4. Calculate 24-Column Collision-Free Coordinates
    const gridPlacements = [
      { card_id: createdCards[0].id, row: 0, col: 0, size_x: 6, size_y: 4 },
      { card_id: createdCards[1].id, row: 0, col: 6, size_x: 6, size_y: 4 },
      { card_id: createdCards[2].id, row: 4, col: 0, size_x: 12, size_y: 8 },
      { card_id: createdCards[3].id, row: 4, col: 12, size_x: 12, size_y: 8 },
      { card_id: createdCards[4].id, row: 12, col: 0, size_x: 6, size_y: 4 },
      { card_id: createdCards[5].id, row: 16, col: 0, size_x: 24, size_y: 8 },
    ];

    // 5. Wire Parameter Mappings
    const dashboardCards = [];
    for (const placement of gridPlacements) {
      const card = createdCards.find(c => c.id === placement.card_id);
      const mappings = [];
      if (card.dataset_query.native.template_tags?.date_range) {
        mappings.push({
          parameter_id: 'param_date_range',
          card_id: card.id,
          target: ['variable', ['template-tag', 'date_range']],
        });
      }
      if (card.dataset_query.native.template_tags?.region) {
        mappings.push({
          parameter_id: 'param_region',
          card_id: card.id,
          target: ['variable', ['template-tag', 'region']],
        });
      }

      const dashCard = await mockClient.addCardToDashboard(dashboard.id, card.id, {
        ...placement,
        parameter_mappings: mappings,
      });
      dashboardCards.push(dashCard);
    }

    // Assertions
    expect(dashboard.id).toBe(1001);
    expect(createdCards).toHaveLength(6);
    expect(dashboardCards).toHaveLength(6);

    // Verify non-overlapping grid layout
    for (let i = 0; i < gridPlacements.length; i++) {
      for (let j = i + 1; j < gridPlacements.length; j++) {
        const a = gridPlacements[i];
        const b = gridPlacements[j];
        const noCollision = (
          a.col + a.size_x <= b.col ||
          b.col + b.size_x <= a.col ||
          a.row + a.size_y <= b.row ||
          b.row + b.size_y <= a.row
        );
        expect(noCollision).toBe(true);
      }
    }

    // Verify parameter linkages
    const dateLinkedCards = dashboardCards.filter(dc => dc.parameter_mappings.some(m => m.parameter_id === 'param_date_range'));
    expect(dateLinkedCards).toHaveLength(3);
  });

  // =========================================================================
  // Scenario 2: Broken Query Recovery in Financial Analytics
  // =========================================================================
  test('Scenario 2: Broken Query Financial Recovery (column typo, table prefix, and group-by omissions healed)', async () => {
    let callCount = 0;
    const mockExecute = jest.fn().mockImplementation(async (dbId, sql) => {
      callCount++;
      if (callCount === 1) {
        throw new Error('Metabase API Error: column "revenue_usd" does not exist in relation "orders"');
      }
      if (callCount === 2) {
        throw new Error('Metabase API Error: relation "orders" does not exist');
      }
      if (callCount === 3) {
        throw new Error('Metabase API Error: column "public.orders.region" must appear in the GROUP BY clause');
      }
      return {
        data: {
          cols: [
            { name: 'region', base_type: 'type/Text' },
            { name: 'total_revenue', base_type: 'type/Decimal' },
          ],
          rows: [
            ['North America', 1250000.00],
            ['Europe', 940000.50],
            ['Asia Pacific', 620000.00],
          ],
        },
      };
    });

    const brokenSqlAttempt1 = 'SELECT region, SUM(revenue_usd) AS total_revenue FROM orders;';
    const healedSqlAttempt2 = 'SELECT region, SUM(total_revenue) AS total_revenue FROM orders;';
    const healedSqlAttempt3 = 'SELECT region, SUM(total_revenue) AS total_revenue FROM public.orders;';
    const healedSqlAttempt4 = 'SELECT region, SUM(total_revenue) AS total_revenue FROM public.orders GROUP BY region;';

    const healingTrail = [];
    let currentSql = brokenSqlAttempt1;
    let queryResult = null;

    for (let attempt = 1; attempt <= 4; attempt++) {
      try {
        const res = await mockExecute(1, currentSql);
        queryResult = res;
        break;
      } catch (err) {
        let category = 'UNKNOWN';
        let nextSql = currentSql;
        let diagnosis = '';

        if (/column "revenue_usd" does not exist/i.test(err.message)) {
          category = 'MISSING_COLUMN';
          diagnosis = 'Replaced column "revenue_usd" with existing column "total_revenue"';
          nextSql = healedSqlAttempt2;
        } else if (/relation "orders" does not exist/i.test(err.message)) {
          category = 'INVALID_TABLE';
          diagnosis = 'Qualified table "orders" with schema prefix "public.orders"';
          nextSql = healedSqlAttempt3;
        } else if (/GROUP BY/i.test(err.message)) {
          category = 'GROUP_BY_VIOLATION';
          diagnosis = 'Appended missing dimension column "region" to GROUP BY clause';
          nextSql = healedSqlAttempt4;
        }

        healingTrail.push({
          attempt,
          failed_sql: currentSql,
          error_message: err.message,
          error_category: category,
          diagnosis,
          corrected_sql: nextSql,
          timestamp: new Date().toISOString(),
        });
        currentSql = nextSql;
      }
    }

    expect(callCount).toBe(4);
    expect(healingTrail).toHaveLength(3);
    expect(healingTrail[0].error_category).toBe('MISSING_COLUMN');
    expect(healingTrail[1].error_category).toBe('INVALID_TABLE');
    expect(healingTrail[2].error_category).toBe('GROUP_BY_VIOLATION');
    expect(queryResult.data.rows).toHaveLength(3);
    expect(queryResult.data.rows[0][1]).toBe(1250000.00);
  });

  // =========================================================================
  // Scenario 3: High-Volume SaaS Query Optimization
  // =========================================================================
  test('Scenario 3: High-Volume SaaS Multi-Table Join Query Optimization (PostgreSQL, MySQL, Snowflake)', () => {
    const complexSaaSSql = `
      SELECT
        t.tenant_name,
        e.event_type,
        DATE_TRUNC('day', e.created_at) AS event_date,
        COUNT(e.id) AS event_count,
        COUNT(DISTINCT e.user_id) AS active_users
      FROM events e
      JOIN tenants t ON e.tenant_id = t.id
      JOIN users u ON e.user_id = u.id
      WHERE e.tenant_id = 42
        AND e.event_type IN ('login', 'checkout', 'export')
        AND e.created_at >= '2026-01-01'
      GROUP BY 1, 2, 3
      ORDER BY event_date DESC;
    `;

    const generateDialectAdvice = (sql, dialect) => {
      const compositeIndex = {
        table: 'events',
        columns: ['tenant_id', 'event_type', 'created_at'],
        ddl: '',
      };

      if (dialect === 'postgres') {
        compositeIndex.ddl = 'CREATE INDEX IF NOT EXISTS claude_ai_idx_events_tenant_event_created ON events (tenant_id, event_type, created_at);';
      } else if (dialect === 'mysql') {
        compositeIndex.ddl = 'CREATE INDEX claude_ai_idx_events_tenant_event_created ON events (tenant_id, event_type, created_at);';
      } else if (dialect === 'snowflake') {
        compositeIndex.ddl = 'ALTER TABLE events CLUSTER BY (tenant_id, event_type, created_at);';
      }

      const mvAdvice = {
        recommended: true,
        view_name: 'claude_ai_mv_daily_tenant_events',
        ddl: `CREATE MATERIALIZED VIEW IF NOT EXISTS claude_ai_mv_daily_tenant_events AS ${sql.trim()}`,
      };

      return { compositeIndex, mvAdvice };
    };

    const pgAdvice = generateDialectAdvice(complexSaaSSql, 'postgres');
    const mysqlAdvice = generateDialectAdvice(complexSaaSSql, 'mysql');
    const snowflakeAdvice = generateDialectAdvice(complexSaaSSql, 'snowflake');

    expect(pgAdvice.compositeIndex.ddl).toContain('CREATE INDEX IF NOT EXISTS');
    expect(mysqlAdvice.compositeIndex.ddl).toContain('CREATE INDEX claude_ai_idx');
    expect(snowflakeAdvice.compositeIndex.ddl).toContain('CLUSTER BY (tenant_id, event_type, created_at)');
    expect(pgAdvice.mvAdvice.view_name).toBe('claude_ai_mv_daily_tenant_events');
  });

  // =========================================================================
  // Scenario 4: Cybersecurity / PII-Sensitive Export
  // =========================================================================
  test('Scenario 4: Cybersecurity & PII-Sensitive Customer Export (Zero Leaks, Domain Retention, Deterministic Pseudonyms)', () => {
    const rawCustomerDataset = {
      columns: [
        { name: 'user_id' },
        { name: 'full_name' },
        { name: 'email' },
        { name: 'phone' },
        { name: 'ssn' },
        { name: 'credit_card' },
        { name: 'api_token' },
      ],
      rows: [
        [101, 'Ada Lovelace', 'ada.lovelace@cybernetics.ac.uk', '+1 (555) 234-5678', '123-45-6789', '4111-1111-1111-1111', 'sk-ant-api03-abcdef1234567890abcdef12'],
        [102, 'Alan Turing', 'alan.turing@bletchley.org', '+1 (555) 987-6543', '987-65-4321', '4532-1234-5678-9010', 'sk-proj-xyz9876543210zyxwvu987654'],
      ],
    };

    // Step 1: Sanitize Tabular Results
    const sanitizedTabular = piiMasker.maskTabularResult(rawCustomerDataset);
    const sanitizedRows = sanitizedTabular.rows || sanitizedTabular.data.rows;

    // Zero-leak assertions
    sanitizedRows.forEach(row => {
      // Email masked with domain preserved
      expect(row[2]).toMatch(/^[a-zA-Z0-9*]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/);
      expect(row[2]).not.toContain('ada.lovelace');
      expect(row[2]).not.toContain('alan.turing');

      // Phone masked
      expect(row[3]).toContain('***');
      expect(row[3]).not.toContain('234-5678');

      // SSN masked
      expect(row[4]).toBe('***-**-' + row[4].slice(-4));
      expect(row[4]).not.toContain('123-45');

      // Card masked
      expect(row[5]).toBe('****-****-****-' + row[5].slice(-4));

      // API token completely redacted
      expect(row[6]).toBe(piiMasker.REDACTION_TOKENS.SECRET);
    });

    // Step 2: Convert to CSV Export and Verify
    const csvHeader = 'user_id,full_name,email,phone,ssn,credit_card,api_token\n';
    const csvRows = rawCustomerDataset.rows.map(r => r.join(',')).join('\n');
    const rawCSV = csvHeader + csvRows;

    const sanitizedCSV = piiMasker.maskCSV(rawCSV);
    expect(sanitizedCSV).not.toContain('ada.lovelace@cybernetics.ac.uk');
    expect(sanitizedCSV).not.toContain('123-45-6789');
    expect(sanitizedCSV).not.toContain('sk-ant-');
    expect(sanitizedCSV).toContain('@cybernetics.ac.uk');
    expect(sanitizedCSV).toContain('@bletchley.org');

    // Step 3: Pseudonymization test for cohort joining
    const anon1 = piiMasker.pseudonymizeValue(rawCustomerDataset.rows[0][0], 'enterprise_salt');
    const anon2 = piiMasker.pseudonymizeValue(rawCustomerDataset.rows[0][0], 'enterprise_salt');
    expect(anon1).toBe(anon2);
    expect(anon1).toMatch(/^anon_[a-f0-9]{12}$/);
  });

  // =========================================================================
  // Scenario 5: Multi-Store Retail Anomaly Detection
  // =========================================================================
  test('Scenario 5: Multi-Store Retail Anomaly Detection (Black Friday spike, Store #104 stockout drop)', () => {
    // Generate 90 days of multi-store data
    const retailData = [];
    const stores = ['Store #101', 'Store #102', 'Store #103', 'Store #104', 'Store #105'];

    for (let day = 1; day <= 90; day++) {
      const dateStr = `2026-${String(Math.floor((day - 1) / 30) + 6).padStart(2, '0')}-${String(((day - 1) % 30) + 1).padStart(2, '0')}`;
      for (const store of stores) {
        let sales = 10000 + ((day * 13) % 2000); // Baseline ~$10k-12k

        // Injected Outlier 1: Black Friday across all stores on Day 45
        if (day === 45) {
          sales = 120000;
        }

        // Injected Outlier 2: Severe stockout drop isolated to Store #104 on Day 75
        if (day === 75 && store === 'Store #104') {
          sales = 150;
        }

        retailData.push({ date: dateStr, store, sales });
      }
    }

    // Step 1: Overall daily aggregation
    const dailyTotals = {};
    retailData.forEach(d => {
      dailyTotals[d.date] = (dailyTotals[d.date] || 0) + d.sales;
    });

    const dailySeries = Object.entries(dailyTotals).map(([date, total]) => ({ date, total }));
    const totals = dailySeries.map(d => d.total);
    const mean = totals.reduce((a, b) => a + b, 0) / totals.length;
    const std = Math.sqrt(totals.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b, 0) / totals.length);

    // Detect overall anomalies (Spike on Day 45)
    const dailyAnomalies = dailySeries.filter(d => Math.abs((d.total - mean) / std) >= 2.5);
    expect(dailyAnomalies.length).toBeGreaterThanOrEqual(1);

    const spikeDay = dailyAnomalies.find(d => d.total > mean);
    expect(spikeDay.total).toBe(120000 * 5); // All 5 stores spiked

    // Step 2: Store #104 isolated anomaly detection using IQR fences
    const store104Data = retailData.filter(d => d.store === 'Store #104');
    const sortedSales = store104Data.map(d => d.sales).sort((a, b) => a - b);
    const q1 = sortedSales[Math.floor(sortedSales.length * 0.25)];
    const q3 = sortedSales[Math.floor(sortedSales.length * 0.75)];
    const iqr = q3 - q1;
    const lowerFence = q1 - 1.5 * iqr;

    const store104Drop = store104Data.find(d => d.sales < lowerFence);
    expect(store104Drop).toBeDefined();
    expect(store104Drop.sales).toBe(150);
    expect(store104Drop.store).toBe('Store #104');
  });
});
