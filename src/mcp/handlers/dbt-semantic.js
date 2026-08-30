/**
 * dbt & Semantic Layer MCP Handler
 * Provides dbt architectural inspection, source prioritization (marts > staging),
 * and governance-first semantic memory workflows with explicit approvals and soft-deprecation.
 */

import { BaseHandler } from './base.js';
import { DbtParser } from '../../dbt/dbt-parser.js';
import { globalSemanticMemory, RULE_CATEGORIES, RULE_STATUS } from '../../semantic/semantic-memory.js';
import { logger } from '../../utils/logger.js';
import { formatStructuredResponse } from '../../utils/structured-response.js';

export class DbtSemanticHandler extends BaseHandler {
  constructor(metabaseClient, assistant, metadataClient) {
    super(metabaseClient, assistant, metadataClient);
    this.dbtParser = new DbtParser();
  }

  /**
   * Inspect dbt project models and architectural tiers
   */
  async handleDbtInspectModels(args = {}) {
    const { manifest_path } = args;

    try {
      if (manifest_path) {
        this.dbtParser.parseManifest(manifest_path);
      }

      const models = this.dbtParser.getModelsList();
      const payload = {
        total_models: models.length,
        models_by_tier: {
          marts_fact: models.filter(m => m.tier === 'marts_fact').length,
          marts_dim: models.filter(m => m.tier === 'marts_dim').length,
          marts_report: models.filter(m => m.tier === 'marts_report').length,
          intermediate: models.filter(m => m.tier === 'intermediate').length,
          staging: models.filter(m => m.tier === 'staging').length,
          raw_or_other: models.filter(m => m.tier === 'raw').length,
        },
        models,
        _provenance: {
          governance_level: 'READ_ONLY_INSPECTION',
          source: manifest_path || 'cached_manifest',
          timestamp: new Date().toISOString(),
        },
      };

      return formatStructuredResponse(
        `ℹ️ [dbt ARCHITECTURAL OVERVIEW]\nTotal Models: ${models.length}\nMarts (Facts/Dims): ${payload.models_by_tier.marts_fact + payload.models_by_tier.marts_dim}\n\n` +
        JSON.stringify(payload, null, 2),
        payload
      );
    } catch (error) {
      logger.error(`Error in handleDbtInspectModels: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ dbt Inspection Error: ${error.message}` }],
      };
    }
  }

  /**
   * Prioritize best source models for a business question
   */
  async handleDbtPrioritizeSources(args = {}) {
    const { query_intent, keywords = [] } = args;

    const allKeywords = [...(Array.isArray(keywords) ? keywords : [keywords])];
    if (query_intent) {
      allKeywords.push(...query_intent.split(/\s+/));
    }

    const prioritized = this.dbtParser.prioritizeSources(allKeywords);
    const topRecommendations = prioritized.slice(0, 10);

    const payload = {
      query_intent: query_intent || '',
      top_recommendations: topRecommendations.map(m => ({
        model_name: m.name,
        tier: m.tier,
        tier_description: m.tierDescription,
        recommendation_score: m.recommendationScore,
        schema: m.schema,
        description: m.description,
      })),
      governance_note: '⚠️ [SOURCE PRIORITY RULE] Always prefer Marts (fct_ / dim_) models over Staging (stg_) for reporting and dashboards to prevent uncleaned/duplicated metric calculation.',
      _provenance: {
        timestamp: new Date().toISOString(),
      },
    };

    return formatStructuredResponse(
      `📊 [dbt SOURCE RESOLUTION RECOMMENDATIONS]\n` +
      topRecommendations.map((r, i) => `${i + 1}. **${r.name}** [${r.tierDescription}] (Score: ${r.recommendationScore})`).join('\n') +
      `\n\n${payload.governance_note}`,
      payload
    );
  }

  /**
   * Propose a semantic rule (Status: PENDING_APPROVAL)
   */
  async handleSemanticMemoryPropose(args = {}) {
    const { term, definition, category, comment, author, sql_condition, dbt_model_hint } = args;

    try {
      const result = globalSemanticMemory.proposeRule({
        term,
        definition,
        category,
        comment,
        author,
        sql_condition,
        dbt_model_hint,
      });

      return formatStructuredResponse(
        `${result.warning}\n\nProposed Rule ID: \`${result.rule.rule_id}\`\nTerm: **${result.rule.term}**\nDefinition: ${result.rule.definition}\nStatus: **${result.rule.status}**\n\nTo activate, run \`semantic_memory_approve\` with rule_id "${result.rule.rule_id}".`,
        result
      );
    } catch (error) {
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ Proposal Error: ${error.message}` }],
      };
    }
  }

  /**
   * Explicitly approve a semantic rule (Status: ACTIVE)
   */
  async handleSemanticMemoryApprove(args = {}) {
    const { rule_id, comment, author } = args;

    try {
      const result = globalSemanticMemory.approveRule(rule_id, { comment, author });
      return formatStructuredResponse(
        `${result.notice}\n\nRule ID: \`${result.rule.rule_id}\`\nTerm: **${result.rule.term}**\nStatus: **ACTIVE**`,
        result
      );
    } catch (error) {
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ Approval Error: ${error.message}` }],
      };
    }
  }

  /**
   * Soft-deprecate / archive a rule (Status: DEPRECATED)
   */
  async handleSemanticMemoryDeprecate(args = {}) {
    const { rule_id, reason, author } = args;

    try {
      const result = globalSemanticMemory.deprecateRule(rule_id, { reason, author });
      return formatStructuredResponse(
        `${result.notice}\n\nRule ID: \`${result.rule.rule_id}\`\nTerm: **${result.rule.term}**\nStatus: **DEPRECATED**\nReason: "${reason}"`,
        result
      );
    } catch (error) {
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ Deprecation Error: ${error.message}` }],
      };
    }
  }

  /**
   * Restore a previously deprecated rule
   */
  async handleSemanticMemoryRestore(args = {}) {
    const { rule_id, comment, author } = args;

    try {
      const result = globalSemanticMemory.restoreRule(rule_id, { comment, author });
      return formatStructuredResponse(
        `${result.notice}\n\nRule ID: \`${result.rule.rule_id}\`\nTerm: **${result.rule.term}**\nStatus: **ACTIVE**`,
        result
      );
    } catch (error) {
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ Restore Error: ${error.message}` }],
      };
    }
  }

  /**
   * List semantic rules with governance audit history
   */
  async handleSemanticMemoryList(args = {}) {
    const { status, category } = args;
    const result = globalSemanticMemory.listRules({ status, category });

    const payload = {
      governance_policy: 'EXPLICIT_APPROVAL_NO_HARD_DELETES',
      total_count: result.total_count,
      active_rules: result.rules.filter(r => r.status === 'ACTIVE').length,
      pending_rules: result.rules.filter(r => r.status === 'PENDING_APPROVAL').length,
      deprecated_rules: result.rules.filter(r => r.status === 'DEPRECATED').length,
      rules: result.rules,
      _provenance: {
        timestamp: new Date().toISOString(),
      },
    };

    return formatStructuredResponse(
      `📚 [SEMANTIC BUSINESS RULES REGISTRY]\n` +
      `Active: ${payload.active_rules} | Pending Approval: ${payload.pending_rules} | Deprecated: ${payload.deprecated_rules}\n\n` +
      result.rules.map(r => `• [${r.status}] **${r.term}** (${r.category}): ${r.definition} (ID: \`${r.rule_id}\`)`).join('\n'),
      payload
    );
  }
}
