import { DbtParser, DBT_TIERS } from '../../src/dbt/dbt-parser.js';
import fs from 'fs';
import path from 'path';

describe('dbt Parser & Tier Classifier Unit Tests', () => {
  let parser;

  beforeEach(() => {
    parser = new DbtParser();
  });

  test('correctly classifies model names into architectural tiers', () => {
    expect(parser.classifyTier('fct_orders', 'models/marts/fct_orders.sql').tier).toBe('marts_fact');
    expect(parser.classifyTier('fact_sales', 'models/marts/fact_sales.sql').tier).toBe('marts_fact');
    expect(parser.classifyTier('dim_customers', 'models/marts/dim_customers.sql').tier).toBe('marts_dim');
    expect(parser.classifyTier('dimension_users', 'models/marts/dimension_users.sql').tier).toBe('marts_dim');
    expect(parser.classifyTier('rpt_monthly_kpi', 'models/marts/rpt_monthly_kpi.sql').tier).toBe('marts_report');
    expect(parser.classifyTier('int_orders_joined', 'models/intermediate/int_orders_joined.sql').tier).toBe('intermediate');
    expect(parser.classifyTier('stg_stripe_payments', 'models/staging/stg_stripe_payments.sql').tier).toBe('staging');
    expect(parser.classifyTier('raw_logs', 'models/raw_logs.sql').tier).toBe('raw');
  });

  test('prioritizes gold marts (fct/dim) over bronze staging for queries', () => {
    // Mock models in parser
    parser.models.set('stg_orders', {
      name: 'stg_orders',
      tier: 'staging',
      tierRank: 20,
      tierDescription: 'Bronze / Staged Raw Cleaned Data',
      description: 'Cleaned raw orders',
      columns: { id: {}, amount: {} },
      tags: ['orders'],
    });

    parser.models.set('fct_orders', {
      name: 'fct_orders',
      tier: 'marts_fact',
      tierRank: 100,
      tierDescription: 'Gold / Facts (Business Transactions)',
      description: 'Core revenue transactions',
      columns: { order_id: {}, customer_id: {}, amount: {}, status: {} },
      tags: ['orders', 'finance'],
    });

    const recommendations = parser.prioritizeSources(['orders', 'revenue']);
    expect(recommendations.length).toBe(2);
    expect(recommendations[0].name).toBe('fct_orders');
    expect(recommendations[0].recommendationScore).toBeGreaterThan(recommendations[1].recommendationScore);
  });
});
