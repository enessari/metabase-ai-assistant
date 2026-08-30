/**
 * Governance-First Semantic Memory Engine
 * Implements strict proposal, explicit approval, soft-deprecation (no hard-deletes),
 * and comprehensive audit trails for business definitions and dbt metric rules.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { logger } from '../utils/logger.js';

export const RULE_STATUS = {
  PENDING: 'PENDING_APPROVAL',
  ACTIVE: 'ACTIVE',
  DEPRECATED: 'DEPRECATED',
};

export const RULE_CATEGORIES = {
  METRIC_DEFINITION: 'metric_definition',
  BUSINESS_TERM: 'business_term',
  FILTER_RULE: 'filter_rule',
  JOIN_PREFERENCE: 'join_preference',
  EXCLUSION_RULE: 'exclusion_rule',
};

export class SemanticMemory {
  constructor(options = {}) {
    this.storagePath = options.storagePath || path.resolve('.metabase-cache/semantic-memory.json');
    this.rules = new Map();
    this.load();
  }

  /**
   * Load stored rules from disk
   */
  load() {
    try {
      if (fs.existsSync(this.storagePath)) {
        const raw = JSON.parse(fs.readFileSync(this.storagePath, 'utf8'));
        this.rules.clear();
        if (Array.isArray(raw.rules)) {
          raw.rules.forEach(rule => this.rules.set(rule.rule_id, rule));
        }
        logger.info(`Loaded ${this.rules.size} semantic governance rules.`);
      } else {
        this.save(); // Initialize empty file
      }
    } catch (err) {
      logger.warn(`Failed to load semantic memory from ${this.storagePath}: ${err.message}. Starting fresh.`);
      this.rules.clear();
    }
  }

  /**
   * Save rules to disk safely
   */
  save() {
    try {
      const dir = path.dirname(this.storagePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      const payload = {
        version: '1.0.0',
        governance_policy: 'EXPLICIT_APPROVAL_REQUIRED_NO_HARD_DELETES',
        updated_at: new Date().toISOString(),
        total_rules: this.rules.size,
        rules: Array.from(this.rules.values()),
      };

      fs.writeFileSync(this.storagePath, JSON.stringify(payload, null, 2), 'utf8');
    } catch (err) {
      logger.error(`Failed to save semantic memory to ${this.storagePath}: ${err.message}`);
    }
  }

  /**
   * Propose a new semantic rule (Status: PENDING_APPROVAL)
   * ⚠️ Does NOT activate the rule until explicit user approval.
   */
  proposeRule(params = {}) {
    const {
      term,
      definition,
      category = RULE_CATEGORIES.BUSINESS_TERM,
      comment = '',
      author = 'user',
      sql_condition = '',
      dbt_model_hint = '',
    } = params;

    if (!term || !definition) {
      throw new Error('Semantic rule proposal requires both "term" and "definition".');
    }

    const ruleId = `rule_${crypto.randomBytes(4).toString('hex')}`;
    const rule = {
      rule_id: ruleId,
      origin: 'metabase_assistant',
      status: RULE_STATUS.PENDING,
      term: String(term).trim(),
      category: category,
      definition: String(definition).trim(),
      sql_condition: sql_condition ? String(sql_condition).trim() : null,
      dbt_model_hint: dbt_model_hint ? String(dbt_model_hint).trim() : null,
      proposed_by: author,
      proposed_comment: comment,
      proposed_at: new Date().toISOString(),
      audit_history: [
        {
          action: 'PROPOSED',
          author,
          comment,
          timestamp: new Date().toISOString(),
        },
      ],
    };

    this.rules.set(ruleId, rule);
    this.save();

    logger.info(`Semantic rule proposal created: ${ruleId} (${rule.term}) [STATUS: PENDING_APPROVAL]`);
    return {
      warning: '⚠️ [GOVERNANCE] Rule created in PENDING status. Explicit approval via semantic_memory_approve is required before this rule affects queries.',
      rule,
    };
  }

  /**
   * Explicitly approve a proposed rule (Status: ACTIVE)
   */
  approveRule(ruleId, params = {}) {
    const { author = 'reviewer', comment = 'Approved by user' } = params;
    const rule = this.rules.get(ruleId);

    if (!rule) {
      throw new Error(`Semantic rule ${ruleId} not found.`);
    }

    if (rule.status === RULE_STATUS.ACTIVE) {
      return { message: `Rule ${ruleId} is already ACTIVE.`, rule };
    }

    rule.status = RULE_STATUS.ACTIVE;
    rule.approved_by = author;
    rule.approval_comment = comment;
    rule.approved_at = new Date().toISOString();
    rule.audit_history.push({
      action: 'APPROVED',
      author,
      comment,
      timestamp: new Date().toISOString(),
    });

    this.save();
    logger.info(`Semantic rule approved and activated: ${ruleId} (${rule.term})`);
    return {
      notice: '✅ [APPROVED] Rule is now ACTIVE and will be incorporated into BI SQL generation and dbt context.',
      rule,
    };
  }

  /**
   * Soft-deprecate / archive a rule (Status: DEPRECATED)
   * ⚠️ NO HARD DELETION — Rule is commented out with mandatory reason.
   */
  deprecateRule(ruleId, params = {}) {
    const { author = 'reviewer', reason } = params;

    if (!reason || reason.trim().length < 5) {
      throw new Error('A valid deprecation reason (at least 5 characters) is mandatory to deprecate a business rule.');
    }

    const rule = this.rules.get(ruleId);
    if (!rule) {
      throw new Error(`Semantic rule ${ruleId} not found.`);
    }

    if (rule.origin !== 'metabase_assistant') {
      throw new Error(`Cannot modify upstream core rule ${ruleId}. Only rules created via Metabase Assistant can be deprecated.`);
    }

    rule.status = RULE_STATUS.DEPRECATED;
    rule.deprecated_by = author;
    rule.deprecation_reason = reason.trim();
    rule.deprecated_at = new Date().toISOString();
    rule.audit_history.push({
      action: 'DEPRECATED',
      author,
      reason: reason.trim(),
      timestamp: new Date().toISOString(),
    });

    this.save();
    logger.info(`Semantic rule deprecated (soft-archive): ${ruleId} (${rule.term}) - Reason: ${reason}`);
    return {
      notice: '⚠️ [DEPRECATED] Rule has been safely archived (soft-deleted). It will no longer apply to queries but can be restored at any time.',
      rule,
    };
  }

  /**
   * Restore a previously deprecated rule
   */
  restoreRule(ruleId, params = {}) {
    const { author = 'reviewer', comment = 'Restored by user' } = params;
    const rule = this.rules.get(ruleId);

    if (!rule) {
      throw new Error(`Semantic rule ${ruleId} not found.`);
    }

    rule.status = RULE_STATUS.ACTIVE;
    rule.restored_by = author;
    rule.restore_comment = comment;
    rule.restored_at = new Date().toISOString();
    rule.audit_history.push({
      action: 'RESTORED',
      author,
      comment,
      timestamp: new Date().toISOString(),
    });

    this.save();
    logger.info(`Semantic rule restored to ACTIVE: ${ruleId} (${rule.term})`);
    return {
      notice: '✅ [RESTORED] Rule is back in ACTIVE status.',
      rule,
    };
  }

  /**
   * List rules with optional status or category filtering
   */
  listRules(filter = {}) {
    const { status, category } = filter;
    let list = Array.from(this.rules.values());

    if (status) {
      list = list.filter(r => r.status === status);
    }
    if (category) {
      list = list.filter(r => r.category === category);
    }

    return {
      total_count: list.length,
      rules: list,
    };
  }

  /**
   * Get all active rules formatted for LLM Prompt Context
   */
  getActiveContextForQuery(keywords = []) {
    const activeRules = Array.from(this.rules.values()).filter(r => r.status === RULE_STATUS.ACTIVE);
    if (activeRules.length === 0) return '';

    const lines = [
      '### [GOVERNANCE_APPROVED_BUSINESS_RULES]',
      'The following business terms and metrics have been explicitly verified and approved by data stewards:',
    ];

    activeRules.forEach((rule, idx) => {
      lines.push(`${idx + 1}. **${rule.term}** (${rule.category}): ${rule.definition}`);
      if (rule.sql_condition) {
        lines.push(`   - SQL Expression: \`${rule.sql_condition}\``);
      }
      if (rule.dbt_model_hint) {
        lines.push(`   - Recommended dbt Mart: \`${rule.dbt_model_hint}\``);
      }
    });

    lines.push('---');
    return lines.join('\n');
  }
}

// Global Singleton
export const globalSemanticMemory = new SemanticMemory();
