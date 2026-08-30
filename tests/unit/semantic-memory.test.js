import { SemanticMemory, RULE_STATUS, RULE_CATEGORIES } from '../../src/semantic/semantic-memory.js';
import fs from 'fs';
import path from 'path';

describe('Governance-First Semantic Memory Unit Tests', () => {
  const testStoragePath = path.resolve('.metabase-cache/test-semantic-memory.json');
  let memory;

  beforeEach(() => {
    if (fs.existsSync(testStoragePath)) {
      fs.unlinkSync(testStoragePath);
    }
    memory = new SemanticMemory({ storagePath: testStoragePath });
  });

  afterEach(() => {
    if (fs.existsSync(testStoragePath)) {
      fs.unlinkSync(testStoragePath);
    }
  });

  test('proposes a rule in PENDING_APPROVAL status (no silent active save)', () => {
    const res = memory.proposeRule({
      term: 'Active User',
      definition: 'Logged in within last 30 days and account verified',
      category: RULE_CATEGORIES.BUSINESS_TERM,
      comment: 'Requested by Growth Team',
      author: 'alice@company.com',
    });

    expect(res.rule.status).toBe(RULE_STATUS.PENDING);
    expect(res.warning).toContain('PENDING');
    expect(memory.rules.get(res.rule.rule_id).status).toBe(RULE_STATUS.PENDING);

    // Pending rule must NOT be in active query context
    const context = memory.getActiveContextForQuery(['active', 'user']);
    expect(context).toBe('');
  });

  test('explicitly approves a rule to make it ACTIVE', () => {
    const proposed = memory.proposeRule({
      term: 'MRR',
      definition: 'SUM(subscription_amount) where status = active',
      category: RULE_CATEGORIES.METRIC_DEFINITION,
    });

    const approved = memory.approveRule(proposed.rule.rule_id, {
      author: 'lead_steward@company.com',
      comment: 'Verified against financial accounting standards',
    });

    expect(approved.rule.status).toBe(RULE_STATUS.ACTIVE);
    expect(approved.rule.audit_history.length).toBe(2);

    // Now it appears in active context
    const context = memory.getActiveContextForQuery(['mrr']);
    expect(context).toContain('MRR');
    expect(context).toContain('GOVERNANCE_APPROVED_BUSINESS_RULES');
  });

  test('soft-deprecates (archives) a rule without hard-deleting and requires reason', () => {
    const proposed = memory.proposeRule({ term: 'Old Churn', definition: '60 days inactive' });
    memory.approveRule(proposed.rule.rule_id, { comment: 'Initial approval' });

    // Fails without reason
    expect(() => {
      memory.deprecateRule(proposed.rule.rule_id, { reason: '' });
    }).toThrow('mandatory');

    // Soft-deprecates with valid reason
    const deprecated = memory.deprecateRule(proposed.rule.rule_id, {
      author: 'lead_steward@company.com',
      reason: 'Replaced by 2026 Q3 updated Churn methodology',
    });

    expect(deprecated.rule.status).toBe(RULE_STATUS.DEPRECATED);
    expect(deprecated.rule.deprecation_reason).toBe('Replaced by 2026 Q3 updated Churn methodology');

    // Rule STILL EXISTS in memory / registry (not hard-deleted)
    expect(memory.rules.has(proposed.rule.rule_id)).toBe(true);

    // But does NOT appear in active query context
    const context = memory.getActiveContextForQuery(['churn']);
    expect(context).toBe('');
  });

  test('restores a deprecated rule back to ACTIVE', () => {
    const proposed = memory.proposeRule({ term: 'Trial User', definition: 'Free plan user' });
    memory.approveRule(proposed.rule.rule_id, { comment: 'Approve' });
    memory.deprecateRule(proposed.rule.rule_id, { reason: 'Deprecated temporarily' });

    const restored = memory.restoreRule(proposed.rule.rule_id, {
      author: 'admin',
      comment: 'Restoring per Q4 roadmap decision',
    });

    expect(restored.rule.status).toBe(RULE_STATUS.ACTIVE);
    expect(memory.rules.get(proposed.rule.rule_id).status).toBe(RULE_STATUS.ACTIVE);
  });
});
