/**
 * dbt Manifest, Catalog & Semantic Layer Parser
 * Classifies models into architectural tiers (marts/facts, marts/dims, intermediate, staging)
 * and resolves optimal upstream sources for BI analysis.
 */

import fs from 'fs';
import path from 'path';
import { logger } from '../utils/logger.js';

export const DBT_TIERS = {
  MART_FACT: { tier: 'marts_fact', rank: 100, prefix: ['fct_', 'fact_'], description: 'Gold / Facts (Business Transactions)' },
  MART_DIM: { tier: 'marts_dim', rank: 90, prefix: ['dim_', 'dimension_'], description: 'Gold / Dimensions (Entities & Attributes)' },
  MART_REPORT: { tier: 'marts_report', rank: 85, prefix: ['rpt_', 'agg_', 'kpi_'], description: 'Gold / Pre-aggregated Metrics' },
  INTERMEDIATE: { tier: 'intermediate', rank: 50, prefix: ['int_'], description: 'Silver / Intermediate Transformations' },
  STAGING: { tier: 'staging', rank: 20, prefix: ['stg_'], description: 'Bronze / Staged Raw Cleaned Data' },
  RAW: { tier: 'raw', rank: 10, prefix: ['raw_', 'source_'], description: 'Raw Source Tables (Lowest Priority for BI)' },
};

export class DbtParser {
  constructor(options = {}) {
    this.projectDir = options.projectDir || process.cwd();
    this.models = new Map();
    this.metrics = new Map();
    this.sources = new Map();
  }

  /**
   * Classify model into layer tier based on path or prefix
   */
  classifyTier(modelName, modelPath = '') {
    const lowerName = (modelName || '').toLowerCase();
    const lowerPath = (modelPath || '').toLowerCase();

    if (lowerPath.includes('/marts/core/') || lowerPath.includes('/marts/')) {
      if (lowerName.startsWith('fct_') || lowerName.startsWith('fact_')) return DBT_TIERS.MART_FACT;
      if (lowerName.startsWith('dim_') || lowerName.startsWith('dimension_')) return DBT_TIERS.MART_DIM;
      if (lowerName.startsWith('rpt_') || lowerName.startsWith('agg_')) return DBT_TIERS.MART_REPORT;
      return DBT_TIERS.MART_FACT; // Default marts to high priority
    }

    if (lowerPath.includes('/intermediate/') || lowerName.startsWith('int_')) {
      return DBT_TIERS.INTERMEDIATE;
    }

    if (lowerPath.includes('/staging/') || lowerName.startsWith('stg_')) {
      return DBT_TIERS.STAGING;
    }

    // Name prefix heuristics fallback
    for (const tierConfig of Object.values(DBT_TIERS)) {
      if (tierConfig.prefix.some(p => lowerName.startsWith(p))) {
        return tierConfig;
      }
    }

    return DBT_TIERS.RAW;
  }

  /**
   * Parse dbt manifest.json file
   */
  parseManifest(manifestPath) {
    if (!fs.existsSync(manifestPath)) {
      throw new Error(`dbt manifest not found at: ${manifestPath}`);
    }

    const content = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    this.models.clear();
    this.metrics.clear();

    // 1. Parse Nodes (Models)
    if (content.nodes) {
      for (const [uniqueId, node] of Object.entries(content.nodes)) {
        if (node.resource_type === 'model') {
          const tier = this.classifyTier(node.name, node.original_file_path || node.path);
          this.models.set(node.name, {
            uniqueId,
            name: node.name,
            database: node.database,
            schema: node.schema,
            alias: node.alias || node.name,
            description: node.description || '',
            tier: tier.tier,
            tierRank: tier.rank,
            tierDescription: tier.description,
            path: node.original_file_path || node.path,
            columns: node.columns || {},
            tags: node.tags || [],
            dependsOn: node.depends_on?.nodes || [],
          });
        }
      }
    }

    // 2. Parse Metrics / Semantic Models (MetricFlow / dbt v1.6+)
    if (content.metrics) {
      for (const [uniqueId, metric] of Object.entries(content.metrics)) {
        this.metrics.set(metric.name, {
          uniqueId,
          name: metric.name,
          description: metric.description || '',
          label: metric.label || metric.name,
          type: metric.type,
          model: metric.model,
          calculationMethod: metric.calculation_method || metric.type,
          expression: metric.expression,
          timestamp: metric.timestamp,
          timeGrains: metric.time_grains || [],
          dimensions: metric.dimensions || [],
        });
      }
    }

    logger.info(`Parsed ${this.models.size} dbt models and ${this.metrics.size} semantic metrics from manifest.`);
    return {
      modelCount: this.models.size,
      metricCount: this.metrics.size,
    };
  }

  /**
   * Prioritize best source models for a given query/question
   */
  prioritizeSources(keywords = []) {
    const normalizedKeywords = (Array.isArray(keywords) ? keywords : [keywords])
      .map(k => String(k).toLowerCase());

    const scoredModels = [];

    for (const model of this.models.values()) {
      let score = model.tierRank; // Base score from architectural tier

      // Match keyword in model name, description, tags, or columns
      const searchTarget = `${model.name} ${model.description} ${model.tags.join(' ')} ${Object.keys(model.columns).join(' ')}`.toLowerCase();

      for (const kw of normalizedKeywords) {
        if (searchTarget.includes(kw)) {
          score += 50; // Keyword match bonus
          if (model.name.toLowerCase().includes(kw)) {
            score += 30; // Direct name match bonus
          }
        }
      }

      scoredModels.push({
        ...model,
        recommendationScore: score,
        isRecommended: score >= 90,
      });
    }

    // Sort descending by recommendation score
    return scoredModels.sort((a, b) => b.recommendationScore - a.recommendationScore);
  }

  /**
   * Get all parsed models summary
   */
  getModelsList() {
    return Array.from(this.models.values()).map(m => ({
      name: m.name,
      tier: m.tier,
      tierDescription: m.tierDescription,
      schema: m.schema,
      description: m.description,
      columnCount: Object.keys(m.columns).length,
      tags: m.tags,
    }));
  }
}
