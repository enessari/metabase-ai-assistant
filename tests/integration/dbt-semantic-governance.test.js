import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';
import { globalSemanticMemory } from '../../src/semantic/semantic-memory.js';

describe('dbt & Semantic Governance End-to-End Integration Tests', () => {
  let handler;

  beforeEach(() => {
    handler = new DbtSemanticHandler(null, null, null);
  });

  test('dbt_prioritize_sources returns structured recommendations with warnings', async () => {
    // Populate parser with sample mock models
    handler.dbtParser.models.set('stg_payments', {
      name: 'stg_payments',
      tier: 'staging',
      tierRank: 20,
      tierDescription: 'Bronze / Staged Raw Cleaned Data',
      description: 'Staging payments table',
      columns: {},
      tags: [],
    });

    handler.dbtParser.models.set('fct_payments', {
      name: 'fct_payments',
      tier: 'marts_fact',
      tierRank: 100,
      tierDescription: 'Gold / Facts (Business Transactions)',
      description: 'Cleaned transaction fact table',
      columns: {},
      tags: [],
    });

    const res = await handler.handleDbtPrioritizeSources({
      query_intent: 'Analyze total revenue and payments',
    });

    expect(res.isError).toBeFalsy();
    expect(res.structuredContent.top_recommendations[0].model_name).toBe('fct_payments');
    expect(res.structuredContent.governance_note).toContain('Always prefer Marts');
  });

  test('full governance lifecycle: propose -> list (pending) -> approve -> list (active) -> deprecate -> list (deprecated) -> restore', async () => {
    // 1. Propose
    const propRes = await handler.handleSemanticMemoryPropose({
      term: 'Gross Margin %',
      definition: '(Total Revenue - COGS) / Total Revenue * 100',
      category: 'metric_definition',
      comment: 'Proposed by CFO',
      author: 'cfo@company.com',
      dbt_model_hint: 'fct_monthly_financials',
    });

    expect(propRes.isError).toBeFalsy();
    const ruleId = propRes.structuredContent.rule.rule_id;
    expect(propRes.content[0].text).toContain('PENDING');

    // 2. Approve
    const appRes = await handler.handleSemanticMemoryApprove({
      rule_id: ruleId,
      comment: 'Approved by Head of Data',
      author: 'head_data@company.com',
    });
    expect(appRes.structuredContent.rule.status).toBe('ACTIVE');

    // 3. Deprecate with mandatory reason
    const depRes = await handler.handleSemanticMemoryDeprecate({
      rule_id: ruleId,
      reason: 'Migrated to Net Margin in 2026 standard chart of accounts',
      author: 'compliance@company.com',
    });
    expect(depRes.structuredContent.rule.status).toBe('DEPRECATED');

    // 4. Restore
    const restRes = await handler.handleSemanticMemoryRestore({
      rule_id: ruleId,
      comment: 'Restored for historical comparative analysis',
      author: 'analyst@company.com',
    });
    expect(restRes.structuredContent.rule.status).toBe('ACTIVE');

    // 5. List
    const listRes = await handler.handleSemanticMemoryList({});
    expect(listRes.structuredContent.rules.some(r => r.rule_id === ruleId)).toBe(true);
  });
});
