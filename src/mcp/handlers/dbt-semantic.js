/**
 * dbt & Semantic Layer MCP Handler
 * Provides dbt architectural inspection, source prioritization (marts > staging),
 * and governance-first semantic memory workflows with explicit approvals and soft-deprecation.
 */

import { BaseHandler } from './base.js';
import { DbtParser } from '../../dbt/dbt-parser.js';
import { DbtDeepScanner } from '../../dbt/dbt-deep-scanner.js';
import { DbtLineageGraph } from '../../dbt/lineage-joins.js';
import { DbtPreaggAdvisor } from '../../dbt/preagg-advisor.js';
import { DbtDashboardBuilder } from '../../dbt/dbt-dashboard-builder.js';
import { DbtYamlExporter } from '../../dbt/dbt-yaml-exporter.js';
import { DbtMetabaseSyncer } from '../../dbt/metabase-syncer.js';
import { MetabaseReverseLineage } from '../../dbt/metabase-reverse-lineage.js';
import { DbtSmartCardBuilder } from '../../dbt/dbt-smart-card-builder.js';
import { globalSemanticMemory, RULE_CATEGORIES, RULE_STATUS } from '../../semantic/semantic-memory.js';
import { logger } from '../../utils/logger.js';
import { formatStructuredResponse } from '../../utils/structured-response.js';
import { isReadOnlyMode } from './database.js';

export class DbtSemanticHandler extends BaseHandler {
  constructor(metabaseClient, assistant, metadataClient) {
    super(metabaseClient, assistant, metadataClient);
    this.dbtParser = new DbtParser();
    this.deepScanner = new DbtDeepScanner();
    this.syncer = new DbtMetabaseSyncer(metabaseClient, this.deepScanner);
    this.reverseLineage = new MetabaseReverseLineage(metabaseClient);
    this.smartCardBuilder = new DbtSmartCardBuilder(metabaseClient, assistant);
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

  /**
   * Deep scan dbt project repository, extract tiers, docs, catalog stats, MetricFlow, and relationships
   */
  async handleDbtProjectScanDeep(args = {}) {
    const {
      project_dir = process.env.DBT_PROJECT_DIR || process.cwd(),
      manifest_path,
      catalog_path,
      include_docs = true,
      include_catalog = true,
      include_metrics = true,
      tier_filter,
      filter_tiers,
    } = args;

    try {
      const scanner = new DbtDeepScanner({ projectDir: project_dir });
      const scanResult = await scanner.scanProject(project_dir, {
        manifestPath: manifest_path,
        catalogPath: catalog_path,
        includeDocs: include_docs,
        includeCatalog: include_catalog,
        includeMetrics: include_metrics,
      });

      let models = scanResult.models;
      const targetTiers = filter_tiers || (tier_filter && tier_filter !== 'all' ? [tier_filter] : null);
      if (Array.isArray(targetTiers) && targetTiers.length > 0) {
        const tierSet = new Set(targetTiers);
        models = models.filter(m => tierSet.has(m.tier));
      }

      const payload = {
        project_dir: scanResult.projectDir,
        model_count: models.length,
        source_count: scanResult.sourceCount || 0,
        metric_count: scanResult.metricCount || 0,
        exposure_count: scanResult.exposureCount || 0,
        relationship_count: scanResult.relationshipCount || 0,
        doc_block_count: scanResult.docBlockCount || 0,
        catalog_table_count: scanResult.catalogTableCount || 0,
        models_by_tier: scanResult.modelsByTier,
        modelsByTier: scanResult.modelsByTier,
        models,
        sources: scanResult.sources || [],
        metrics: scanResult.metrics || [],
        exposures: scanResult.exposures || [],
        relationships: scanResult.relationships || [],
        doc_blocks: scanResult.docBlocks || {},
        catalog_stats: scanResult.catalogStats || {},
        _provenance: {
          governance_level: 'READ_ONLY_INSPECTION',
          scanner: 'DbtDeepScanner',
          timestamp: new Date().toISOString(),
          manifest_loaded: Boolean(scanResult.manifestLoaded),
          catalog_loaded: Boolean(scanResult.catalogLoaded),
          docs_loaded: Boolean(scanResult.docsLoaded),
        },
      };

      const markdownText = [
        `🔍 [dbt DEEP PROJECT SCAN OVERVIEW]`,
        `📁 Project Root: \`${payload.project_dir}\``,
        `📊 Total Models: **${payload.model_count}** | Sources: **${payload.source_count}** | Metrics: **${payload.metric_count}** | Exposures: **${payload.exposure_count}** | Join Tests: **${payload.relationship_count}**`,
        `📚 Doc Blocks: **${payload.doc_block_count}** | Catalog Profiling Tables: **${payload.catalog_table_count}**`,
        ``,
        `🏛️ Architectural Tier Breakdown:`,
        `  • Gold Facts (\`marts_fact\`): **${payload.models_by_tier.marts_fact || 0}**`,
        `  • Gold Dimensions (\`marts_dim\`): **${payload.models_by_tier.marts_dim || 0}**`,
        `  • Gold Reports/KPIs (\`marts_report\`): **${payload.models_by_tier.marts_report || 0}**`,
        `  • Silver Intermediate (\`intermediate\`): **${payload.models_by_tier.intermediate || 0}**`,
        `  • Bronze Staging (\`staging\`): **${payload.models_by_tier.staging || 0}**`,
        `  • Raw/Other (\`raw\`): **${payload.models_by_tier.raw || 0}**`,
        ``,
        `🏆 Top Recommended Models (Gold Marts):`,
        ...models
          .filter(m => m.tierRank >= 85)
          .slice(0, 8)
          .map((m, i) => `  ${i + 1}. **${m.name}** [${m.tierDescription}] — ${Object.keys(m.columns || {}).length} columns${m.description ? `: "${m.description.slice(0, 80)}${m.description.length > 80 ? '...' : ''}"` : ''}`),
      ].join('\n');

      return formatStructuredResponse(markdownText, payload);
    } catch (error) {
      logger.error(`Error in handleDbtProjectScanDeep: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ dbt Deep Scan Error: ${error.message}` }],
      };
    }
  }

  /**
   * Resolve multi-hop lineage DAG and semantic join paths
   */
  async handleDbtLineageJoinsGraph(args = {}) {
    const {
      project_dir = process.env.DBT_PROJECT_DIR || process.cwd(),
      manifest_path,
      source_model,
      target_model,
      target_models,
      max_hops = 5,
      join_type = 'LEFT',
      include_sql = true,
      direction = 'both',
      confidence_threshold = 0.5,
      base_alias,
    } = args;

    try {
      const scanner = new DbtDeepScanner({ projectDir: project_dir });
      const scanResult = await scanner.scanProject(project_dir, { manifestPath: manifest_path });
      const graph = new DbtLineageGraph(scanResult);

      const targets = [];
      if (target_model) targets.push(target_model);
      if (Array.isArray(target_models)) {
        for (const t of target_models) {
          if (t && !targets.includes(t)) targets.push(t);
        }
      }

      const joinPaths = [];
      if (source_model && targets.length > 0) {
        for (const target of targets) {
          const pathResult = graph.findJoinPath(source_model, target, {
            maxHops: max_hops,
            confidenceThreshold: confidence_threshold,
            joinType: join_type,
            baseAlias: base_alias,
          });
          if (pathResult) {
            joinPaths.push(pathResult);
          }
        }
      }

      let lineage = null;
      if (source_model) {
        lineage = {
          model: source_model,
          upstream: (direction === 'upstream' || direction === 'both') ? graph.getAllUpstream(source_model, { includeNodeInfo: true }) : [],
          downstream: (direction === 'downstream' || direction === 'both') ? graph.getAllDownstream(source_model, { includeNodeInfo: true }) : [],
          direct_parents: graph.getDirectParents(source_model),
          direct_children: graph.getDirectChildren(source_model),
        };
      }

      let sqlSnippet = '';
      if (include_sql && source_model && joinPaths.length > 0) {
        if (joinPaths.length === 1) {
          sqlSnippet = joinPaths[0].sqlJoinClause || graph.generateJoinSql(joinPaths[0].edges, base_alias, { joinType: join_type });
        } else {
          sqlSnippet = graph.generateMultiJoinSql(source_model, joinPaths, {
            joinType: join_type,
            baseAlias: base_alias,
          });
        }
      }

      const payload = {
        project_dir: scanResult.projectDir,
        node_count: graph.getNodeCount(),
        edge_count: graph.getEdgeCount(),
        has_cycles: graph.hasCycles(),
        cycle_nodes: graph.getCycleNodes(),
        topological_order: graph.getTopologicalOrder(),
        source_model: source_model || null,
        target_model: target_model || null,
        join_paths: joinPaths,
        lineage,
        sql_snippet: sqlSnippet,
        _provenance: {
          governance_level: 'READ_ONLY_INSPECTION',
          resolver: 'DbtLineageGraph',
          timestamp: new Date().toISOString(),
          manifest_loaded: Boolean(scanResult.manifestLoaded),
          has_cycles: Boolean(graph.hasCycles()),
        },
      };

      const markdownLines = [
        `🔗 [dbt MULTI-HOP LINEAGE & SEMANTIC JOIN GRAPH]`,
        `📁 Project Root: \`${payload.project_dir}\``,
        `📊 Graph Statistics: **${payload.node_count}** nodes | **${payload.edge_count}** edges | Cycles: **${payload.has_cycles ? '⚠️ DETECTED' : '✅ NONE (Valid DAG)'}**`,
      ];

      if (source_model && joinPaths.length > 0) {
        markdownLines.push(``, `🛣️ Resolved Semantic Join Paths from **${source_model}**:`);
        for (const jp of joinPaths) {
          if (!jp.found) {
            markdownLines.push(`  • **${jp.source_model}** ➔ **${jp.target_model}**: ❌ ${jp.message || 'No join path found'}`);
            continue;
          }
          const arrowChain = (jp.path || []).join(' ──► ');
          markdownLines.push(`  • **${jp.source_model}** ➔ **${jp.target_model}** (${jp.hops} hops, confidence: ${((jp.confidenceScore || jp.confidence || 0) * 100).toFixed(0)}%)`);
          markdownLines.push(`    Route: \`${arrowChain}\``);
          if (Array.isArray(jp.edges)) {
            for (const edge of jp.edges) {
              markdownLines.push(`    - \`${edge.fromModel || edge.from_model}.${edge.fromColumn || edge.from_column}\` = \`${edge.toModel || edge.to_model}.${edge.toColumn || edge.to_column}\` [${edge.source || 'relationship_test'}]`);
            }
          }
        }
      }

      if (lineage) {
        markdownLines.push(``, `🌳 Model Lineage: **${source_model}**`);
        if (lineage.upstream.length > 0) {
          markdownLines.push(`  🔼 Upstream Parents (${lineage.upstream.length}): ${lineage.upstream.map(u => typeof u === 'object' ? `${u.name} (d=${u.depth})` : u).join(', ')}`);
        }
        if (lineage.downstream.length > 0) {
          markdownLines.push(`  🔽 Downstream Dependents (${lineage.downstream.length}): ${lineage.downstream.map(d => typeof d === 'object' ? `${d.name} (d=${d.depth})` : d).join(', ')}`);
        }
      }

      if (sqlSnippet) {
        markdownLines.push(``, `📝 Generated ANSI SQL:`, '```sql', sqlSnippet, '```');
      }

      return formatStructuredResponse(markdownLines.join('\n'), payload);
    } catch (error) {
      logger.error(`Error in handleDbtLineageJoinsGraph: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ dbt Lineage & Joins Error: ${error.message}` }],
      };
    }
  }

  /**
   * Formulate Cube.js-style Pre-Aggregation and Rollup Materialized View DDL recommendations
   */
  async handleDbtSemanticPreaggAdvisor(args = {}) {
    const {
      project_dir = process.env.DBT_PROJECT_DIR || process.cwd(),
      manifest_path,
      catalog_path,
      model_name,
      model_names,
      metrics,
      dimensions,
      time_dimension,
      time_grain = 'day',
      time_grains,
      dialect = 'postgres',
      target_schema = 'preagg',
      refresh_strategy = 'auto',
      refresh_interval_minutes = 60,
      include_hll = true,
      min_speedup_factor = 2.0,
      include_indexes = true,
    } = args;

    try {
      // 1. Scan project & load metadata
      const scanner = new DbtDeepScanner({ projectDir: project_dir });
      const scanResult = await scanner.scanProject(project_dir, {
        manifestPath: manifest_path,
        catalogPath: catalog_path,
        includeCatalog: true,
        includeMetrics: true,
      });

      // 2. Initialize Pre-Aggregation Advisor
      const advisor = new DbtPreaggAdvisor(scanResult, {
        dialect,
        targetSchema: target_schema,
        includeHll: include_hll,
        minSpeedupFactor: min_speedup_factor,
      });

      // 3. Generate recommendations
      const recommendations = advisor.advisePreaggregations({
        modelName: model_name,
        modelNames: model_names,
        metrics,
        dimensions,
        timeDimension: time_dimension,
        timeGrain: time_grain,
        timeGrains: time_grains,
        dialect,
        targetSchema: target_schema,
        refreshStrategy: refresh_strategy,
        refreshIntervalMinutes: refresh_interval_minutes,
        includeHll: include_hll,
        minSpeedupFactor: min_speedup_factor,
        includeIndexes: include_indexes,
      });

      // 4. Metric Additivity Analysis
      const targetModel = model_name || (recommendations[0] ? recommendations[0].model : 'default');
      const additivityAnalysis = advisor.analyzeMetricAdditivity(targetModel, metrics);

      // 5. Construct DDL Summary collections
      const ddlSummary = {
        materialized_views: recommendations.map(r => r.ddl).filter(Boolean),
        indexes: recommendations.flatMap(r => r.index_ddl || []),
        refresh_commands: recommendations.map(r => r.refresh_command).filter(Boolean),
      };

      const payload = {
        project_dir: scanResult.projectDir,
        model_name: targetModel,
        dialect,
        time_grain,
        target_schema,
        recommendation_count: recommendations.length,
        recommendations,
        metric_additivity_analysis: additivityAnalysis,
        ddl_summary: ddlSummary,
        _provenance: {
          governance_level: 'READ_ONLY_ADVISORY',
          advisor: 'DbtPreaggAdvisor',
          dialect,
          timestamp: new Date().toISOString(),
          models_analyzed: model_names?.length || (model_name ? 1 : recommendations.length),
          catalog_loaded: Boolean(scanResult.catalogLoaded),
        },
      };

      // 6. Build Rich Markdown Presentation
      const markdownLines = [
        `⚡ [CUBE.JS PRE-AGGREGATION & ROLLUP ADVISOR]`,
        `📁 Project Root: \`${payload.project_dir}\``,
        `🎯 Target Model: **${payload.model_name}** | Dialect: **${dialect.toUpperCase()}** | Schema: \`${target_schema}\``,
        `📊 Recommendations Generated: **${recommendations.length}**`,
        ``,
      ];

      if (recommendations.length === 0) {
        markdownLines.push(`ℹ️ No pre-aggregations met the minimum speedup threshold of **${min_speedup_factor}x**.`);
      } else {
        recommendations.forEach((rec, idx) => {
          markdownLines.push(
            `### ${idx + 1}. Rollup: \`${rec.name}\` (${rec.time_grain.toUpperCase()} Grain)`,
            `• **Type**: \`${rec.type}\` | **Model**: \`${rec.model}\``,
            `• **Dimensions**: ${rec.dimensions.length > 0 ? rec.dimensions.map(d => `\`${d}\``).join(', ') : '_None (Time only)_'}`,
            `• **Measures**: ${rec.measures.map(m => `\`${m.name}\` (${m.agg}, ${m.additivity})`).join(', ')}`,
            `• 🚀 **Performance Speedup**: **${rec.speedup_estimate.speedup_label}** (${rec.speedup_estimate.speedup_factor}x faster)`,
            `• 📉 **Scan Reduction**: **${rec.speedup_estimate.scan_reduction_pct}%** (${rec.speedup_estimate.raw_rows.toLocaleString()} raw ➔ ${rec.speedup_estimate.preagg_rows.toLocaleString()} preagg rows)`,
            ``,
            `**Materialized View DDL:**`,
            '```sql',
            rec.ddl,
            '```',
            ``
          );

          if (rec.index_ddl && rec.index_ddl.length > 0) {
            markdownLines.push(
              `**Index & Clustering Optimization:**`,
              '```sql',
              rec.index_ddl.join('\n'),
              '```',
              ``
            );
          }

          if (rec.refresh_command) {
            markdownLines.push(
              `**Refresh Strategy (${rec.refresh_strategy}):**`,
              '```sql',
              rec.refresh_command,
              '```',
              ``
            );
          }

          if (rec.query_acceleration?.accelerated_query_pattern) {
            markdownLines.push(
              `**Query Acceleration Rewrite:**`,
              '```sql',
              rec.query_acceleration.accelerated_query_pattern,
              '```',
              ``
            );
          }
        });
      }

      if (additivityAnalysis.length > 0) {
        markdownLines.push(
          `### 📐 Metric Additivity Analysis`,
          ...additivityAnalysis.map(
            m => `• **${m.metric_name}** [${m.additivity.toUpperCase()}]: ${m.decomposition || m.recommendation}`
          )
        );
      }

      return formatStructuredResponse(markdownLines.join('\n'), payload);
    } catch (error) {
      logger.error(`Error in handleDbtSemanticPreaggAdvisor: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ Pre-Aggregation Advisor Error: ${error.message}` }],
      };
    }
  }

  /**
   * Build complete Metabase dashboard from dbt project YAML definitions (Lightdash-style Code-as-BI)
   */
  async handleDbtBuildDashboardFromYaml(args = {}) {
    const isDryRun = Boolean(args.dry_run !== undefined ? args.dry_run : args.dryRun);

    if (isReadOnlyMode() && !isDryRun) {
      logger.warn('Read-only mode: Blocked dbt_build_dashboard_from_yaml operation');
      return {
        isError: true,
        content: [
          {
            type: 'text',
            text: `🔒 **Read-Only Mode Active**\n\n⛔ **Operation Blocked:** \`dbt_build_dashboard_from_yaml\`\n\n` +
              `This MCP server is running in read-only mode for security.\n` +
              `Write operations (dashboard creation, card creation) are not allowed.\n\n` +
              `To enable write operations, set \`METABASE_READ_ONLY_MODE=false\` in your environment.`,
          },
        ],
      };
    }

    try {
      const builder = new DbtDashboardBuilder(this.metabaseClient, {
        assistant: this.aiAssistant,
      });

      const result = await builder.buildDashboardFromYaml(args);

      // Build rich markdown presentation
      let markdownText = `🎨 **[LIGHTDASH CODE-AS-BI DASHBOARD BUILT SUCCESSFULLY]**\n\n`;
      markdownText += `📊 **Dashboard:** ${result.name} (ID: \`${result.dashboard_id}\`)\n`;
      markdownText += `🔗 **URL:** ${result.url}\n`;
      markdownText += `🧩 **Total Cards Created:** **${result.card_count}** (${(result.cards || []).filter(c => c.is_model).length} Metabase Models v50+, ${(result.cards || []).filter(c => !c.is_model).length} Questions)\n`;
      markdownText += `🎛️ **Interactive Filters:** **${result.filter_count}**\n`;
      markdownText += `🏛️ **Theme:** \`${result.theme || 'executive'}\`\n\n`;

      markdownText += `### 📐 24-Column Executive Grid Layout:\n`;
      markdownText += `| # | Card / Model Name | Type | Display | Position (Row, Col) | Size (WxH) | Linked Filters |\n`;
      markdownText += `|---|---|---|---|---|---|---|\n`;
      (result.cards || []).forEach((card, idx) => {
        const filterNames = (card.parameter_mappings || [])
          .map(m => {
            const f = (result.filters || []).find(flt => flt.id === m.parameter_id);
            return f ? f.name : m.parameter_id;
          }).filter(Boolean).join(', ') || '_None_';
        markdownText += `| ${idx + 1} | **${card.name.replace(/\|/g, '\\|')}** | \`${card.type || (card.is_model ? 'model' : 'question')}\` | \`${card.display}\` | (${card.position.row}, ${card.position.col}) | ${card.position.size_x}x${card.position.size_y} | ${filterNames} |\n`;
      });

      if (Array.isArray(result.filters) && result.filters.length > 0) {
        markdownText += `\n### 🎛️ Configured Global Filters:\n`;
        for (const f of result.filters) {
          markdownText += `- **${f.name}** (\`${f.slug}\`, type: \`${f.type}\`${f.default !== null && f.default !== undefined ? `, default: \`${JSON.stringify(f.default)}\`` : ''})\n`;
        }
      }

      markdownText += `\n🤖 _Generated with Lightdash-style Code-as-BI YAML specifications and Metabase 24-column collision-free grid engine._`;

      return formatStructuredResponse(markdownText, result);
    } catch (error) {
      logger.error(`Error in handleDbtBuildDashboardFromYaml: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ dbt Dashboard Builder Error: ${error.message}` }],
      };
    }
  }

  /**
   * Export approved SemanticMemory business rules into dbt schema.yml, semantic_models.yml, and metrics.yml
   */
  async handleDbtSemanticExportYaml(args = {}) {
    const {
      target_model,
      targetModel,
      model_name,
      modelName,
      status_filter = 'ACTIVE',
      statusFilter,
      categories,
      category,
      rule_ids,
      ruleIds,
      include_deprecated,
      includeDeprecated,
      include_metricflow = true,
      includeMetricFlow,
      include_semantic_layer = true,
      include_dbt_schema = true,
      includeDbtSchema,
      include_provenance_header,
      include_provenance_comments,
      includeProvenanceComments,
      author = 'Metabase AI Assistant',
      rationale = 'Sync approved semantic governance rules to dbt YAML',
      format = 'all',
      project_dir = process.env.DBT_PROJECT_DIR || process.cwd(),
      manifest_path,
    } = args;

    try {
      const exporter = new DbtYamlExporter(globalSemanticMemory, {
        projectDir: project_dir,
        manifestPath: manifest_path,
      });

      const result = exporter.exportSemanticToYaml({
        format,
        target_model: target_model || targetModel || model_name || modelName,
        status_filter: status_filter || statusFilter || 'ACTIVE',
        categories: categories || (category ? [category] : undefined),
        rule_ids: rule_ids || ruleIds,
        include_deprecated: include_deprecated !== undefined ? include_deprecated : includeDeprecated,
        include_semantic_layer: include_semantic_layer && (include_metricflow !== undefined ? include_metricflow : (includeMetricFlow !== undefined ? includeMetricFlow : true)),
        include_dbt_schema: include_dbt_schema !== undefined ? include_dbt_schema : (includeDbtSchema !== undefined ? includeDbtSchema : true),
        include_provenance_header: include_provenance_header !== undefined ? include_provenance_header : (include_provenance_comments !== undefined ? include_provenance_comments : (includeProvenanceComments !== undefined ? includeProvenanceComments : true)),
        author,
        rationale,
      });

      const markdownLines = [
        `🌉 **[OMNI.CO CONTROLLED SEMANTIC-TO-YAML EXPORT SUMMARY]**`,
        ``,
        `📊 **Export Statistics:**`,
        `  • Exported Rules: **${result.exported_count}** (Active: **${result.active_rules_count || 0}**, Deprecated: **${result.deprecated_rules_count || 0}**)`,
        `  • Skipped Rules: **${result.skipped_count}** (Pending / Unapproved / Filtered)`,
        `  • Target Models: **${(result.target_models || []).length > 0 ? result.target_models.join(', ') : 'All Models / Generic'}**`,
        ``,
        `🏷️ **Breakdown by Category:**`,
        `  • Metrics (\`metric_definition\`): **${result.rules_by_category?.metric_definition || 0}**`,
        `  • Business Terms (\`business_term\`): **${result.rules_by_category?.business_term || 0}**`,
        `  • Filter Rules (\`filter_rule\`): **${result.rules_by_category?.filter_rule || 0}**`,
        `  • Exclusion Rules (\`exclusion_rule\`): **${result.rules_by_category?.exclusion_rule || 0}**`,
        `  • Join Preferences (\`join_preference\`): **${result.rules_by_category?.join_preference || 0}**`,
        ``,
      ];

      if (result.schema_yaml) {
        markdownLines.push(
          `### 📄 dbt Core \`schema.yml\`:`,
          '```yaml',
          result.schema_yaml,
          '```',
          ''
        );
      }

      if (result.semantic_models_yaml) {
        markdownLines.push(
          `### 📄 dbt MetricFlow \`semantic_models.yml\`:`,
          '```yaml',
          result.semantic_models_yaml,
          '```',
          ''
        );
      }

      if (result.metrics_yaml) {
        markdownLines.push(
          `### 📄 dbt MetricFlow \`metrics.yml\`:`,
          '```yaml',
          result.metrics_yaml,
          '```',
          ''
        );
      }

      markdownLines.push(
        `🛡️ **[GOVERNANCE & SAFETY NOTICE]**`,
        `This export is strictly non-destructive. No files on disk were modified.`,
        `To incorporate these definitions into your dbt repository, review the snippets above and commit them via standard pull request.`
      );

      return formatStructuredResponse(markdownLines.join('\n'), result);
    } catch (error) {
      logger.error(`Error in handleDbtSemanticExportYaml: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ dbt Semantic YAML Export Error: ${error.message}` }],
      };
    }
  }

  /**
   * 1. Sync dbt metadata (table names, column descriptions, semantic types, FKs) into Metabase
   */
  async handleDbtSyncMetadataToMetabase(args = {}) {
    const { database_id, dbt_project_dir, dry_run = false } = args;

    try {
      if (!database_id) {
        throw new Error('database_id parameter is required.');
      }

      if (isReadOnlyMode() && !dry_run) {
        throw new Error('Metabase is running in READ-ONLY mode. Metadata synchronization requires write mode or dry_run: true.');
      }

      const result = await this.syncer.syncMetadata({
        database_id,
        dbt_project_dir,
        dry_run,
      });

      const text = `🔄 **[dbt METADATA TO METABASE SYNC COMPLETE]**\n\n` +
        `• **Tables Synchronized**: ${result.tables_updated}\n` +
        `• **Fields & Semantic Types Synchronized**: ${result.fields_updated}\n` +
        `• **Mode**: ${dry_run ? 'Dry-Run (Simulated)' : 'Live Applied'}\n\n` +
        (result.details.length > 0 ? `### Sync Details:\n` + result.details.slice(0, 10).map(d => `- **${d.name || d.field}** (${d.type}): ${JSON.stringify(d.updates)}`).join('\n') : `All fields and tables are already up to date.`);

      return formatStructuredResponse(text, result);
    } catch (error) {
      logger.error(`Error in handleDbtSyncMetadataToMetabase: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ Metadata Sync Error: ${error.message}` }],
      };
    }
  }

  /**
   * 2. Sync dbt MetricFlow / YAML metrics into official Metabase Metrics (/api/metric)
   */
  async handleDbtSyncMetricsToMetabase(args = {}) {
    const { database_id, dbt_project_dir, dry_run = false } = args;

    try {
      if (!database_id) {
        throw new Error('database_id parameter is required.');
      }

      if (isReadOnlyMode() && !dry_run) {
        throw new Error('Metabase is running in READ-ONLY mode. Metrics synchronization requires write mode or dry_run: true.');
      }

      const result = await this.syncer.syncMetrics({
        database_id,
        dbt_project_dir,
        dry_run,
      });

      const text = `📊 **[dbt METRICS TO METABASE SYNC COMPLETE]**\n\n` +
        `• **Metrics Created**: ${result.metrics_created}\n` +
        `• **Metrics Skipped**: ${result.metrics_skipped}\n` +
        `• **Mode**: ${dry_run ? 'Dry-Run (Simulated)' : 'Live Applied'}\n\n` +
        (result.created.length > 0 ? `### Created Metabase Metrics:\n` + result.created.map(m => `- **${m.label || m.name}** (Table: ${m.table})`).join('\n') : `No new metrics to sync.`);

      return formatStructuredResponse(text, result);
    } catch (error) {
      logger.error(`Error in handleDbtSyncMetricsToMetabase: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ Metrics Sync Error: ${error.message}` }],
      };
    }
  }

  /**
   * 3. Reverse Lineage: Generate dbt exposures YAML from all Metabase Dashboards and Cards
   */
  async handleDbtGenerateExposuresFromMetabase(args = {}) {
    const { metabase_base_url, include_cards = true, include_dashboards = true, owner_name, owner_email } = args;

    try {
      const result = await this.reverseLineage.generateExposures({
        metabase_base_url,
        include_cards,
        include_dashboards,
        owner_name,
        owner_email,
      });

      return formatStructuredResponse(
        `🔁 **[METABASE TO dbt REVERSE LINEAGE EXPOSURES]**\n\n` +
        `Discovered **${result.total_exposures} exposures** from your Metabase instance.\n` +
        `Copy the generated YAML below into \`models/exposures/_metabase__exposures.yml\`:\n\n\`\`\`yaml\n${result.yaml}\n\`\`\``,
        result
      );
    } catch (error) {
      logger.error(`Error in handleDbtGenerateExposuresFromMetabase: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ Reverse Lineage Error: ${error.message}` }],
      };
    }
  }

  /**
   * 4. Smart Card Creator: Generate verified Metabase Card using dbt semantic rules
   */
  async handleDbtSmartCreateCard(args = {}) {
    const { question, database_id, collection_id, custom_name, description, dry_run = false } = args;

    try {
      if (!question || !database_id) {
        throw new Error('Both question and database_id are required.');
      }

      if (isReadOnlyMode() && !dry_run) {
        throw new Error('Metabase is running in READ-ONLY mode. Creating cards requires write mode or dry_run: true.');
      }

      const result = await this.smartCardBuilder.createSmartCard({
        question,
        database_id,
        collection_id,
        custom_name,
        description,
        dry_run,
      });

      const text = `🤖 **[dbt-VERIFIED QUESTION CARD CREATED]**\n\n` +
        `• **Card Name**: **${result.name}**\n` +
        `• **Display Chart Type**: \`${result.display}\`\n` +
        `• **Semantic Rules Applied**: ${result.semantic_rules_applied ? '✅ Yes' : 'ℹ️ Standard'}\n` +
        `• **Healing Applied**: ${result.healing_applied ? '⚡ Auto-Healed' : '✅ First-Pass Valid'}\n` +
        `• **Sample Rows**: ${result.sample_rows_count}\n\n` +
        `\`\`\`sql\n${result.sql}\n\`\`\``;

      return formatStructuredResponse(text, result);
    } catch (error) {
      logger.error(`Error in handleDbtSmartCreateCard: ${error.message}`);
      return {
        isError: true,
        content: [{ type: 'text', text: `❌ Smart Card Creation Error: ${error.message}` }],
      };
    }
  }

  routes() {
    return {
      dbt_inspect_models: this.handleDbtInspectModels.bind(this),
      dbt_prioritize_sources: this.handleDbtPrioritizeSources.bind(this),
      dbt_project_scan_deep: this.handleDbtProjectScanDeep.bind(this),
      dbt_lineage_joins_graph: this.handleDbtLineageJoinsGraph.bind(this),
      dbt_semantic_preagg_advisor: this.handleDbtSemanticPreaggAdvisor.bind(this),
      dbt_build_dashboard_from_yaml: this.handleDbtBuildDashboardFromYaml.bind(this),
      dbt_semantic_export_yaml: this.handleDbtSemanticExportYaml.bind(this),
      dbt_sync_metadata_to_metabase: this.handleDbtSyncMetadataToMetabase.bind(this),
      dbt_sync_metrics_to_metabase: this.handleDbtSyncMetricsToMetabase.bind(this),
      dbt_generate_exposures_from_metabase: this.handleDbtGenerateExposuresFromMetabase.bind(this),
      dbt_smart_create_card: this.handleDbtSmartCreateCard.bind(this),
      semantic_memory_propose: this.handleSemanticMemoryPropose.bind(this),
      semantic_memory_approve: this.handleSemanticMemoryApprove.bind(this),
      semantic_memory_deprecate: this.handleSemanticMemoryDeprecate.bind(this),
      semantic_memory_restore: this.handleSemanticMemoryRestore.bind(this),
      semantic_memory_list: this.handleSemanticMemoryList.bind(this),
    };
  }
}
