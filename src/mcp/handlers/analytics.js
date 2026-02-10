import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';

export class AnalyticsHandler {
  constructor(metabaseClient, metadataClient, activityLogger) {
    this.metabaseClient = metabaseClient;
    this.metadataClient = metadataClient || null;
    this.activityLogger = activityLogger || null;
  }

  setMetadataClient(client) {
    this.metadataClient = client;
  }

  routes() {
    return {
      'mb_meta_query_performance': (args) => this.handleMetadataQueryPerformance(args),
      'mb_meta_content_usage': (args) => this.handleMetadataContentUsage(args),
      'mb_meta_user_activity': (args) => this.handleMetadataUserActivity(args),
      'mb_meta_database_usage': (args) => this.handleMetadataDatabaseUsage(args),
      'mb_meta_dashboard_complexity': (args) => this.handleMetadataDashboardComplexity(args),
      'mb_meta_info': (args) => this.handleMetadataInfo(args),
      'mb_meta_table_dependencies': (args) => this.handleMetadataTableDependencies(args),
      'mb_meta_impact_analysis': (args) => this.handleMetadataImpactAnalysis(args),
      'mb_meta_optimization_recommendations': (args) => this.handleMetadataOptimizationRecommendations(args),
      'mb_meta_error_patterns': (args) => this.handleMetadataErrorPatterns(args),
      'mb_meta_export_workspace': (args) => this.handleMetadataExportWorkspace(args),
      'mb_meta_import_preview': (args) => this.handleMetadataImportPreview(args),
      'mb_meta_compare_environments': (args) => this.handleMetadataCompareEnvironments(args),
      'mb_meta_auto_cleanup': (args) => this.handleMetadataAutoCleanup(args),
    };
  }

  async handleMetadataQueryPerformance(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.**\n\nTo use metadata analytics:\n1. Set `MB_METADATA_ENABLED=true` in your .env file\n2. Configure metadata database connection (MB_METADATA_*)\n3. Restart the MCP server'
        }]
      };
    }

    try {
      const days = args.days || 7;
      const includeSlowQueries = args.include_slow_queries !== false;
      const slowThreshold = args.slow_threshold_ms || 10000;

      // Get overall stats
      const stats = await this.metadataClient.getQueryPerformanceStats(days);

      let output = `📊 **Query Performance Analysis** (Last ${days} Days)\n\n`;
      output += `**Overall Statistics:**\n`;
      output += `• Total Queries: ${stats.total_queries?.toLocaleString() || 0}\n`;
      output += `• Unique Users: ${stats.unique_users || 0}\n`;
      output += `• Average Runtime: ${stats.avg_runtime_ms || 0}ms\n`;
      output += `• Median Runtime: ${stats.median_runtime_ms || 0}ms\n`;
      output += `• 95th Percentile: ${stats.p95_runtime_ms || 0}ms\n`;
      output += `• Max Runtime: ${stats.max_runtime_ms || 0}ms\n`;
      output += `• Cache Hit Rate: ${stats.cache_hit_rate || 0}%\n`;
      output += `• Errors: ${stats.errors || 0} (${((stats.errors / stats.total_queries) * 100).toFixed(2)}%)\n\n`;

      // Get slow queries if requested
      if (includeSlowQueries) {
        const slowQueries = await this.metadataClient.getSlowQueries(slowThreshold, 10);

        if (slowQueries.length > 0) {
          output += `🐌 **Slowest Questions** (>${slowThreshold}ms):\n\n`;
          slowQueries.slice(0, 10).forEach((q, i) => {
            output += `${i + 1}. **${q.question_name || 'Ad-hoc Query'}** (ID: ${q.card_id || 'N/A'})\n`;
            output += `   • Avg Runtime: ${q.avg_runtime_ms}ms\n`;
            output += `   • Max Runtime: ${q.max_runtime_ms}ms\n`;
            output += `   • Executions: ${q.execution_count}\n`;
            output += `   • Database: ${q.database_name}\n`;
            if (q.error_count > 0) {
              output += `   • ⚠️ Errors: ${q.error_count}\n`;
            }
            output += `\n`;
          });
        }
      }

      // Get performance by context
      const contextPerf = await this.metadataClient.getQueryPerformanceByContext(days);
      if (contextPerf.length > 0) {
        output += `📈 **Performance by Context:**\n\n`;
        contextPerf.forEach(c => {
          output += `• **${c.context || 'unknown'}**: ${c.query_count} queries, avg ${c.avg_runtime_ms}ms, error rate ${c.error_rate}%\n`;
        });
      }

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata query performance analysis failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Query performance analysis failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataContentUsage(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const days = args.days || 30;
      const unusedThreshold = args.unused_threshold_days || 90;
      const limit = args.limit || 20;

      let output = `📚 **Content Usage Analysis** (Last ${days} Days)\n\n`;

      // Get popular questions
      const popularQuestions = await this.metadataClient.getPopularQuestions(days, limit);
      if (popularQuestions.length > 0) {
        output += `🌟 **Most Popular Questions:**\n\n`;
        popularQuestions.slice(0, 10).forEach((q, i) => {
          output += `${i + 1}. **${q.name}** (ID: ${q.id})\n`;
          output += `   • Executions: ${q.execution_count}\n`;
          output += `   • Avg Runtime: ${q.avg_runtime_ms}ms\n`;
          output += `   • Collection: ${q.collection_name || 'Root'}\n`;
          output += `   • Type: ${q.display}\n\n`;
        });
      }

      // Get popular dashboards
      const popularDashboards = await this.metadataClient.getPopularDashboards(days, limit);
      if (popularDashboards.length > 0) {
        output += `📊 **Most Popular Dashboards:**\n\n`;
        popularDashboards.slice(0, 10).forEach((d, i) => {
          output += `${i + 1}. **${d.name}** (ID: ${d.id})\n`;
          output += `   • Views: ${d.view_count}\n`;
          output += `   • Cards: ${d.card_count}\n`;
          output += `   • Avg Load Time: ${d.avg_load_time_ms}ms\n`;
          output += `   • Collection: ${d.collection_name || 'Root'}\n\n`;
        });
      }

      // Get unused content
      const unused = await this.metadataClient.getUnusedContent(unusedThreshold);
      output += `🗑️ **Cleanup Recommendations** (Unused >${unusedThreshold} days):\n\n`;
      output += `• Unused Questions: ${unused.unused_questions.length}\n`;
      output += `• Unused Dashboards: ${unused.unused_dashboards.length}\n\n`;

      if (unused.unused_questions.length > 0) {
        output += `**Sample Unused Questions:**\n`;
        unused.unused_questions.slice(0, 5).forEach((q, i) => {
          const lastUsed = q.last_used ? new Date(q.last_used).toLocaleDateString() : 'Never';
          output += `${i + 1}. ${q.name} (ID: ${q.id}) - Last used: ${lastUsed}\n`;
        });
        output += `\n`;
      }

      // Get orphaned cards
      const orphaned = await this.metadataClient.getOrphanedCards();
      output += `📌 **Orphaned Cards** (Not in any dashboard):\n`;
      output += `• Total: ${orphaned.length}\n`;
      if (orphaned.length > 0) {
        output += `• Top used orphaned cards:\n`;
        orphaned.slice(0, 5).forEach((c, i) => {
          output += `  ${i + 1}. ${c.name} (ID: ${c.id}) - ${c.execution_count} uses\n`;
        });
      }

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata content usage analysis failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Content usage analysis failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataUserActivity(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const days = args.days || 30;
      const inactiveThreshold = args.inactive_threshold_days || 90;
      const includeLoginHistory = args.include_login_history !== false;

      let output = `👥 **User Activity Analysis** (Last ${days} Days)\n\n`;

      // Get user activity stats
      const userStats = await this.metadataClient.getUserActivityStats(days);

      // Active users
      const activeUsers = userStats.filter(u => u.query_count > 0);
      const inactiveUsers = userStats.filter(u => u.query_count === 0);

      output += `**Overview:**\n`;
      output += `• Total Active Users: ${activeUsers.length}\n`;
      output += `• Inactive Users (last ${days}d): ${inactiveUsers.length}\n`;
      output += `• Total Queries: ${activeUsers.reduce((sum, u) => sum + u.query_count, 0)}\n\n`;

      // Top users
      output += `🏆 **Most Active Users:**\n\n`;
      activeUsers.slice(0, 10).forEach((u, i) => {
        output += `${i + 1}. ${u.email}${u.is_superuser ? ' (Admin)' : ''}\n`;
        output += `   • Queries: ${u.query_count}\n`;
        output += `   • Questions Used: ${u.unique_questions_used}\n`;
        output += `   • Dashboards Viewed: ${u.unique_dashboards_viewed}\n`;
        output += `   • Avg Query Time: ${u.avg_query_time_ms}ms\n\n`;
      });

      // Inactive users
      const longInactive = await this.metadataClient.getInactiveUsers(inactiveThreshold);
      if (longInactive.length > 0) {
        output += `⚠️ **Long-Inactive Users** (>${inactiveThreshold} days):\n`;
        output += `• Count: ${longInactive.length}\n`;
        output += `• **Recommendation:** Consider license optimization\n\n`;

        longInactive.slice(0, 5).forEach((u, i) => {
          const lastLogin = u.last_login ? new Date(u.last_login).toLocaleDateString() : 'Never';
          output += `${i + 1}. ${u.email} - Last login: ${lastLogin} (${Math.round(u.days_inactive)} days ago)\n`;
        });
        output += `\n`;
      }

      // Login timeline
      if (includeLoginHistory) {
        const loginTimeline = await this.metadataClient.getLoginTimeline(days);
        if (loginTimeline.length > 0) {
          output += `📅 **Recent Login Activity:**\n\n`;
          loginTimeline.slice(0, 7).forEach(t => {
            const date = new Date(t.login_date).toLocaleDateString();
            output += `• ${date}: ${t.login_count} logins, ${t.unique_users} unique users\n`;
          });
        }
      }

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata user activity analysis failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **User activity analysis failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataDatabaseUsage(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const days = args.days || 30;
      let output = `🗃️ **Database Usage Analysis** (Last ${days} Days)\n\n`;

      // Get database usage
      const dbUsage = await this.metadataClient.getDatabaseUsageStats(days);

      output += `**Connected Databases:**\n\n`;
      dbUsage.forEach((db, i) => {
        output += `${i + 1}. **${db.name}** (${db.engine})\n`;
        output += `   • Queries: ${db.query_count}\n`;
        output += `   • Avg Runtime: ${db.avg_runtime_ms}ms\n`;
        output += `   • Errors: ${db.error_count}\n`;
        output += `   • Unique Users: ${db.unique_users}\n\n`;
      });

      // Get table usage if database_id provided
      if (args.database_id) {
        const tableUsage = await this.metadataClient.getTableUsageStats(args.database_id, days);
        if (tableUsage.length > 0) {
          output += `\n📊 **Table Usage** (Database ID: ${args.database_id}):\n\n`;
          tableUsage.slice(0, 20).forEach((t, i) => {
            output += `${i + 1}. ${t.schema}.${t.table_name}\n`;
            output += `   • Questions Using: ${t.question_count}\n\n`;
          });
        }
      }

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata database usage analysis failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Database usage analysis failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataDashboardComplexity(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const complexity = await this.metadataClient.getDashboardComplexityAnalysis();

      let output = `📊 **Dashboard Complexity Analysis**\n\n`;
      output += `Analyzing dashboards with 10+ cards...\n\n`;

      if (complexity.length === 0) {
        output += `✅ No overly complex dashboards found!\n`;
      } else {
        output += `🔍 **Complex Dashboards:**\n\n`;
        complexity.forEach((d, i) => {
          output += `${i + 1}. **${d.name}** (ID: ${d.id})\n`;
          output += `   • Cards: ${d.card_count}\n`;
          output += `   • Avg Load Time: ${d.avg_load_time_ms}ms\n`;
          output += `   • Max Load Time: ${d.max_load_time_ms}ms\n`;
          output += `   • Views (30d): ${d.view_count_30d}\n`;

          if (d.avg_load_time_ms > 5000) {
            output += `   • ⚠️ **Slow dashboard** - Consider optimization\n`;
          }
          if (d.card_count > 15) {
            output += `   • ⚠️ **High card count** - Consider splitting\n`;
          }
          output += `\n`;
        });

        output += `\n💡 **Optimization Tips:**\n`;
        output += `• Cache frequently accessed data\n`;
        output += `• Split large dashboards into focused views\n`;
        output += `• Optimize slow queries\n`;
        output += `• Remove unused cards\n`;
      }

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata dashboard complexity analysis failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Dashboard complexity analysis failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataInfo(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const info = await this.metadataClient.getDatabaseInfo();

      let output = `ℹ️ **Metabase Metadata Overview**\n\n`;
      output += `**Content Statistics:**\n`;
      output += `• Active Users: ${info.active_users}\n`;
      output += `• Active Questions: ${info.active_questions}\n`;
      output += `• Active Dashboards: ${info.active_dashboards}\n`;
      output += `• Connected Databases: ${info.connected_databases}\n`;
      output += `• Queries (Last 7d): ${info.queries_last_7d}\n\n`;

      output += `**Connection Info:**\n`;
      output += `• Database: ${this.metadataClient.config.database}\n`;
      output += `• Engine: ${this.metadataClient.config.engine}\n`;
      output += `• Status: ✅ Connected\n\n`;

      output += `💡 Use other metadata tools for detailed analysis:\n`;
      output += `• \`mb_meta_query_performance\` - Query performance stats\n`;
      output += `• \`mb_meta_content_usage\` - Popular & unused content\n`;
      output += `• \`mb_meta_user_activity\` - User engagement\n`;
      output += `• \`mb_meta_database_usage\` - Database usage patterns\n`;

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata info failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Metadata info failed:** ${error.message}`
        }]
      };
    }
  }

  // ============================================
  // PHASE 2: ADVANCED ANALYTICS HANDLERS
  // ============================================

  async handleMetadataTableDependencies(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const dependencies = await this.metadataClient.getTableDependencies(
        args.database_id,
        args.table_name,
        args.schema_name
      );

      if (!dependencies.table_found) {
        return {
          content: [{ type: 'text', text: `❌ ${dependencies.message}` }]
        };
      }

      const table = dependencies.table;
      const summary = dependencies.impact_summary;

      let output = `🔗 **Table Dependency Analysis**\n\n`;
      output += `**Table:** ${table.schema ? table.schema + '.' : ''}${table.name}\n`;
      output += `**Display Name:** ${table.display_name || table.name}\n\n`;

      output += `**Impact Summary:**\n`;
      output += `• Questions Affected: ${summary.questions_affected}\n`;
      output += `• Dashboards Affected: ${summary.dashboards_affected}\n`;
      output += `• Fields: ${summary.fields_count}\n`;
      output += `• Total Executions (30d): ${summary.total_executions_30d}\n\n`;

      if (dependencies.questions.length > 0) {
        output += `📊 **Dependent Questions** (Top 10):\n\n`;
        dependencies.questions.slice(0, 10).forEach((q, i) => {
          output += `${i + 1}. **${q.name}** (ID: ${q.id})\n`;
          output += `   • Type: ${q.display}\n`;
          output += `   • Executions (30d): ${q.execution_count_30d}\n`;
          output += `   • Collection: ${q.collection_name || 'Root'}\n`;
          output += `   • Creator: ${q.creator}\n\n`;
        });

        if (dependencies.questions.length > 10) {
          output += `_...and ${dependencies.questions.length - 10} more questions_\n\n`;
        }
      } else {
        output += `✅ **No questions depend on this table**\n\n`;
      }

      if (dependencies.dashboards.length > 0) {
        output += `📈 **Affected Dashboards** (Top 10):\n\n`;
        dependencies.dashboards.slice(0, 10).forEach((d, i) => {
          output += `${i + 1}. **${d.name}** (ID: ${d.id})\n`;
          output += `   • Total Cards: ${d.total_cards}\n`;
          output += `   • Views (30d): ${d.view_count_30d}\n`;
          output += `   • Collection: ${d.collection_name || 'Root'}\n\n`;
        });

        if (dependencies.dashboards.length > 10) {
          output += `_...and ${dependencies.dashboards.length - 10} more dashboards_\n\n`;
        }
      }

      if (dependencies.fields.length > 0) {
        output += `📋 **Table Fields** (${dependencies.fields.length} total):\n`;
        dependencies.fields.slice(0, 15).forEach(f => {
          output += `• ${f.name} (${f.base_type})${f.semantic_type ? ' - ' + f.semantic_type : ''}\n`;
        });
        if (dependencies.fields.length > 15) {
          output += `_...and ${dependencies.fields.length - 15} more fields_\n`;
        }
      }

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata table dependencies analysis failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Table dependencies analysis failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataImpactAnalysis(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const impact = await this.metadataClient.analyzeTableRemovalImpact(
        args.database_id,
        args.table_name,
        args.schema_name
      );

      if (!impact.table_found) {
        return {
          content: [{ type: 'text', text: `❌ ${impact.message}` }]
        };
      }

      const table = impact.table;
      const analysis = impact.impact_analysis;
      const breaking = analysis.breaking_changes;

      let output = `⚠️ **Table Removal Impact Analysis**\n\n`;
      output += `**Table:** ${table.schema ? table.schema + '.' : ''}${table.name}\n`;
      output += `**Severity:** ${analysis.severity === 'HIGH' ? '🔴 HIGH' : analysis.severity === 'MEDIUM' ? '🟡 MEDIUM' : '🟢 LOW'}\n\n`;

      output += `**Breaking Changes:**\n`;
      output += `• Questions Will Break: ${breaking.questions_will_break}\n`;
      output += `• Dashboards Will Break: ${breaking.dashboards_will_break}\n`;
      output += `• Critical Questions: ${breaking.critical_questions} (>10 executions/month)\n`;
      output += `• Critical Dashboards: ${breaking.critical_dashboards} (>5 views/month)\n`;
      output += `• Unused Questions: ${breaking.unused_questions}\n\n`;

      output += `**Recommendations:**\n`;
      analysis.recommendations.forEach(rec => {
        output += `${rec}\n`;
      });
      output += `\n`;

      if (breaking.critical_questions > 0 && impact.questions.length > 0) {
        output += `🔥 **Critical Questions** (highly used):\n\n`;
        const criticalQuestions = impact.questions.filter(q => parseInt(q.execution_count_30d) > 10);
        criticalQuestions.slice(0, 5).forEach((q, i) => {
          output += `${i + 1}. **${q.name}** (ID: ${q.id})\n`;
          output += `   • Executions: ${q.execution_count_30d}\n`;
          output += `   • Collection: ${q.collection_name || 'Root'}\n\n`;
        });
      }

      if (breaking.critical_dashboards > 0 && impact.dashboards.length > 0) {
        output += `🔥 **Critical Dashboards** (actively viewed):\n\n`;
        const criticalDashboards = impact.dashboards.filter(d => parseInt(d.view_count_30d) > 5);
        criticalDashboards.slice(0, 5).forEach((d, i) => {
          output += `${i + 1}. **${d.name}** (ID: ${d.id})\n`;
          output += `   • Views: ${d.view_count_30d}\n`;
          output += `   • Cards: ${d.total_cards}\n\n`;
        });
      }

      output += `\n💡 **Next Steps:**\n`;
      if (analysis.severity === 'HIGH') {
        output += `1. Review and migrate critical questions to alternative tables\n`;
        output += `2. Update dashboard queries with new data sources\n`;
        output += `3. Archive unused questions before removal\n`;
        output += `4. Communicate changes to affected users\n`;
        output += `5. Plan rollback strategy if needed\n`;
      } else if (analysis.severity === 'MEDIUM') {
        output += `1. Archive or update affected questions\n`;
        output += `2. Notify owners of affected dashboards\n`;
        output += `3. Consider archiving table instead of deletion\n`;
      } else {
        output += `1. Verify table is truly unused\n`;
        output += `2. Archive table for 30 days before permanent deletion\n`;
        output += `3. Monitor for any unexpected dependencies\n`;
      }

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata impact analysis failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Impact analysis failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataOptimizationRecommendations(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const days = args.days || 30;
      const includeMatView = args.include_matview_candidates !== false;
      const includeCache = args.include_cache_recommendations !== false;

      let output = `⚡ **Optimization Recommendations** (Last ${days} Days)\n\n`;

      // Index recommendations
      const indexRecs = await this.metadataClient.getIndexRecommendations(args.database_id, days);

      if (indexRecs.length > 0) {
        output += `📊 **Index Recommendations:**\n\n`;
        indexRecs.slice(0, 10).forEach((rec, i) => {
          const priorityIcon = rec.priority === 'HIGH' ? '🔴' : rec.priority === 'MEDIUM' ? '🟡' : '🟢';
          output += `${i + 1}. ${priorityIcon} **${rec.schema}.${rec.table}** (Priority: ${rec.priority})\n`;
          output += `   • Query Count: ${rec.query_count}\n`;
          output += `   • Avg Runtime: ${rec.avg_runtime_ms}ms\n`;
          output += `   • Max Runtime: ${rec.max_runtime_ms}ms\n`;
          output += `   • ${rec.recommendation}\n\n`;
        });
      } else {
        output += `✅ **No urgent index recommendations** - Query performance is acceptable\n\n`;
      }

      // Materialized view candidates
      if (includeMatView) {
        const matviewCandidates = await this.metadataClient.getMaterializedViewCandidates(days, 5);

        if (matviewCandidates.length > 0) {
          output += `🔄 **Materialized View Candidates:**\n\n`;
          matviewCandidates.slice(0, 5).forEach((c, i) => {
            const priorityIcon = c.priority === 'HIGH' ? '🔴' : c.priority === 'MEDIUM' ? '🟡' : '🟢';
            output += `${i + 1}. ${priorityIcon} **${c.question_name || 'Ad-hoc Query'}** (Priority: ${c.priority})\n`;
            output += `   • Database: ${c.database_name}\n`;
            output += `   • Executions: ${c.execution_count}\n`;
            output += `   • Avg Runtime: ${c.avg_runtime_ms}ms\n`;
            output += `   • Potential Time Saved: ${(c.total_time_saved_potential / (1000 * 60)).toFixed(2)} minutes\n`;
            output += `   • ${c.recommendation}\n\n`;
          });
        } else {
          output += `✅ **No materialized view candidates** - No repeated heavy queries detected\n\n`;
        }
      }

      // Cache optimization
      if (includeCache) {
        const cacheRecs = await this.metadataClient.getCacheOptimizationRecommendations(7);

        if (cacheRecs.length > 0) {
          output += `💾 **Cache Optimization Recommendations:**\n\n`;

          const highPriorityCache = cacheRecs.filter(c => c.cache_hit_rate < 30 && c.execution_count > 10);
          if (highPriorityCache.length > 0) {
            output += `🔴 **High Priority** (Low cache hit rate):\n`;
            highPriorityCache.slice(0, 5).forEach(c => {
              output += `• **${c.question_name}** (ID: ${c.card_id})\n`;
              output += `  Current TTL: ${c.current_cache_ttl || 'None'}, Suggested: ${c.suggested_cache_ttl}s\n`;
              output += `  Hit Rate: ${c.cache_hit_rate}%, Executions: ${c.execution_count}\n`;
              output += `  ${c.recommendation}\n\n`;
            });
          }

          const noCacheQuestions = cacheRecs.filter(c => !c.current_cache_ttl && c.execution_count > 15);
          if (noCacheQuestions.length > 0) {
            output += `🟡 **Enable Caching** (Frequently accessed, no cache):\n`;
            noCacheQuestions.slice(0, 5).forEach(c => {
              output += `• **${c.question_name}** - ${c.execution_count} executions, no caching configured\n`;
            });
            output += `\n`;
          }
        }
      }

      output += `\n💡 **Implementation Guide:**\n`;
      output += `• **Indexes**: Use \`create_index_direct\` tool to add recommended indexes\n`;
      output += `• **Materialized Views**: Use \`create_materialized_view_direct\` for PostgreSQL\n`;
      output += `• **Caching**: Update question cache_ttl via \`mb_question_update\` tool\n`;

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata optimization recommendations failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Optimization recommendations failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataErrorPatterns(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const days = args.days || 30;
      const includeRecurring = args.include_recurring_questions !== false;
      const includeTimeline = args.include_timeline !== false;

      let output = `🚨 **Error Pattern Analysis** (Last ${days} Days)\n\n`;

      // Error patterns
      const patterns = await this.metadataClient.getErrorPatterns(days);

      if (patterns.length > 0) {
        output += `**Top Error Patterns:**\n\n`;
        patterns.slice(0, 10).forEach((p, i) => {
          const severityIcon = p.severity === 'HIGH' ? '🔴' : p.severity === 'MEDIUM' ? '🟡' : '🟢';
          output += `${i + 1}. ${severityIcon} **${p.category}** (${p.severity} Severity)\n`;
          output += `   • Occurrences: ${p.occurrence_count}\n`;
          output += `   • Affected Questions: ${p.affected_questions}\n`;
          output += `   • Affected Users: ${p.affected_users}\n`;
          output += `   • Database: ${p.primary_database} (${p.database_engine})\n`;
          output += `   • Error: ${p.error_pattern.substring(0, 80)}...\n`;
          output += `   • **Resolution:** ${p.resolution_suggestion}\n\n`;
        });
      } else {
        output += `✅ **No recurring error patterns detected**\n\n`;
      }

      // Recurring error questions
      if (includeRecurring) {
        const recurringQuestions = await this.metadataClient.getRecurringErrorQuestions(days, 3);

        if (recurringQuestions.length > 0) {
          output += `\n🔥 **Questions with Recurring Errors:**\n\n`;
          recurringQuestions.slice(0, 10).forEach((q, i) => {
            const severityIcon = q.severity === 'CRITICAL' ? '🔴' : q.severity === 'HIGH' ? '🟡' : '🟢';
            output += `${i + 1}. ${severityIcon} **${q.question_name}** (ID: ${q.card_id}) - ${q.severity}\n`;
            output += `   • Error Rate: ${q.error_rate}% (${q.error_count}/${q.total_executions})\n`;
            output += `   • Collection: ${q.collection_name || 'Root'}\n`;
            output += `   • Last Error: ${new Date(q.last_error_time).toLocaleDateString()}\n`;
            output += `   • ${q.recommendation}\n\n`;
          });
        }
      }

      // Error timeline
      if (includeTimeline) {
        const timeline = await this.metadataClient.getErrorTimeline(days);

        if (timeline.length > 0) {
          output += `\n📅 **Error Timeline** (Last 7 days):\n\n`;
          timeline.slice(0, 7).forEach(t => {
            const date = new Date(t.error_date).toLocaleDateString();
            output += `• ${date}: ${t.error_count} errors (${t.error_rate}% error rate), ${t.affected_questions} questions\n`;
          });
        }
      }

      output += `\n💡 **Recommended Actions:**\n`;
      const highSeverity = patterns.filter(p => p.severity === 'HIGH');
      const criticalQuestions = includeRecurring ?
        (await this.metadataClient.getRecurringErrorQuestions(days, 3)).filter(q => q.severity === 'CRITICAL') : [];

      if (criticalQuestions.length > 0) {
        output += `1. **URGENT**: Fix or archive ${criticalQuestions.length} critical questions (>50% error rate)\n`;
      }
      if (highSeverity.length > 0) {
        output += `2. Address ${highSeverity.length} high-severity error patterns\n`;
      }
      output += `3. Review database permissions and connectivity\n`;
      output += `4. Optimize timeout-prone queries\n`;
      output += `5. Update questions with schema changes\n`;

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata error patterns analysis failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Error patterns analysis failed:** ${error.message}`
        }]
      };
    }
  }

  // ============================================================================
  // Phase 3: Export/Import & Migration Handlers
  // ============================================================================

  async handleMetadataExportWorkspace(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const options = {
        include_collections: args.include_collections !== false,
        include_questions: args.include_questions !== false,
        include_dashboards: args.include_dashboards !== false,
        collection_id: args.collection_id,
        created_after: args.created_after,
        created_before: args.created_before
      };

      let output = `📤 **Workspace Export** (READ-ONLY Operation)\n\n`;

      const result = await this.metadataClient.exportWorkspace(options);

      output += `**Export Summary:**\n`;
      output += `• **Collections**: ${result.collections?.length || 0}\n`;
      output += `• **Questions**: ${result.questions?.length || 0}\n`;
      output += `• **Dashboards**: ${result.dashboards?.length || 0}\n`;
      output += `• **Total Items**: ${result.metadata.total_items}\n`;
      output += `• **Export Date**: ${result.metadata.exported_at}\n\n`;

      if (result.collections?.length > 0) {
        output += `**Exported Collections:**\n`;
        result.collections.slice(0, 10).forEach(c => {
          output += `• **${c.name}** (ID: ${c.id}) - ${c.description || 'No description'}\n`;
        });
        if (result.collections.length > 10) {
          output += `... and ${result.collections.length - 10} more collections\n`;
        }
        output += `\n`;
      }

      if (result.questions?.length > 0) {
        output += `**Exported Questions:**\n`;
        result.questions.slice(0, 10).forEach(q => {
          output += `• **${q.name}** (ID: ${q.id})\n`;
        });
        if (result.questions.length > 10) {
          output += `... and ${result.questions.length - 10} more questions\n`;
        }
        output += `\n`;
      }

      if (result.dashboards?.length > 0) {
        output += `**Exported Dashboards:**\n`;
        result.dashboards.slice(0, 10).forEach(d => {
          output += `• **${d.name}** (ID: ${d.id}) - ${d.description || 'No description'}\n`;
        });
        if (result.dashboards.length > 10) {
          output += `... and ${result.dashboards.length - 10} more dashboards\n`;
        }
        output += `\n`;
      }

      output += `\n📋 **Export Data (JSON):**\n`;
      output += `\`\`\`json\n${JSON.stringify(result, null, 2)}\`\`\`\n`;

      output += `\n💡 **Next Steps:**\n`;
      output += `• Save this JSON to a file for backup or migration\n`;
      output += `• Use \`mb_meta_import_preview\` to analyze import impact before importing\n`;
      output += `• Use \`mb_meta_compare_environments\` to compare with other environments\n`;

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata export workspace failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Export workspace failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataImportPreview(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    if (!args.workspace_json) {
      return {
        content: [{
          type: 'text',
          text: '❌ **Missing required parameter:** workspace_json'
        }]
      };
    }

    try {
      const workspace = typeof args.workspace_json === 'string'
        ? JSON.parse(args.workspace_json)
        : args.workspace_json;

      let output = `🔍 **Import Impact Preview** (DRY-RUN - No Changes Made)\n\n`;

      const impact = await this.metadataClient.previewImportImpact(workspace);

      output += `**Import Summary:**\n`;
      output += `• **Collections to Import**: ${impact.summary.collections_to_import}\n`;
      output += `• **Questions to Import**: ${impact.summary.questions_to_import}\n`;
      output += `• **Dashboards to Import**: ${impact.summary.dashboards_to_import}\n`;
      output += `• **Name Conflicts**: ${impact.summary.name_conflicts}\n`;
      output += `• **Overall Risk**: ${impact.summary.overall_risk}\n\n`;

      if (impact.conflicts.length > 0) {
        output += `⚠️ **Conflicts Detected (${impact.conflicts.length}):**\n`;
        impact.conflicts.forEach(c => {
          const icon = c.severity === 'HIGH' ? '🔴' : c.severity === 'MEDIUM' ? '🟡' : '🟢';
          output += `${icon} **${c.type}**: ${c.item_name}\n`;
          output += `  - ${c.message}\n`;
          output += `  - Recommendation: ${c.recommendation}\n`;
        });
        output += `\n`;
      }

      if (impact.warnings.length > 0) {
        output += `⚠️ **Warnings (${impact.warnings.length}):**\n`;
        impact.warnings.forEach(w => {
          output += `• ${w}\n`;
        });
        output += `\n`;
      }

      output += `\n📊 **Detailed Analysis:**\n\n`;

      if (impact.new_items.collections.length > 0) {
        output += `**New Collections (${impact.new_items.collections.length}):**\n`;
        impact.new_items.collections.slice(0, 5).forEach(c => {
          output += `✅ ${c}\n`;
        });
        if (impact.new_items.collections.length > 5) {
          output += `... and ${impact.new_items.collections.length - 5} more\n`;
        }
        output += `\n`;
      }

      if (impact.new_items.questions.length > 0) {
        output += `**New Questions (${impact.new_items.questions.length}):**\n`;
        impact.new_items.questions.slice(0, 5).forEach(q => {
          output += `✅ ${q}\n`;
        });
        if (impact.new_items.questions.length > 5) {
          output += `... and ${impact.new_items.questions.length - 5} more\n`;
        }
        output += `\n`;
      }

      if (impact.new_items.dashboards.length > 0) {
        output += `**New Dashboards (${impact.new_items.dashboards.length}):**\n`;
        impact.new_items.dashboards.slice(0, 5).forEach(d => {
          output += `✅ ${d}\n`;
        });
        if (impact.new_items.dashboards.length > 5) {
          output += `... and ${impact.new_items.dashboards.length - 5} more\n`;
        }
        output += `\n`;
      }

      output += `\n💡 **Recommendations:**\n`;
      if (impact.recommendations.length > 0) {
        impact.recommendations.forEach(r => {
          output += `• ${r}\n`;
        });
      } else {
        output += `✅ No issues detected - safe to import\n`;
      }

      output += `\n🔒 **Next Steps:**\n`;
      if (impact.summary.overall_risk === 'HIGH') {
        output += `⚠️ **HIGH RISK** - Review conflicts carefully before proceeding\n`;
      } else if (impact.summary.overall_risk === 'MEDIUM') {
        output += `⚠️ **MEDIUM RISK** - Address warnings before import\n`;
      } else {
        output += `✅ **LOW RISK** - Safe to proceed with import\n`;
      }
      output += `• Resolve name conflicts by renaming items in the workspace JSON\n`;
      output += `• Create backup before actual import\n`;
      output += `• Use \`mb_meta_export_workspace\` to backup current state first\n`;

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata import preview failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Import preview failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataCompareEnvironments(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    if (!args.target_workspace_json) {
      return {
        content: [{
          type: 'text',
          text: '❌ **Missing required parameter:** target_workspace_json'
        }]
      };
    }

    try {
      const targetWorkspace = typeof args.target_workspace_json === 'string'
        ? JSON.parse(args.target_workspace_json)
        : args.target_workspace_json;

      let output = `🔄 **Environment Comparison** (READ-ONLY Operation)\n\n`;

      const comparison = await this.metadataClient.compareEnvironments(targetWorkspace);

      output += `**Comparison Summary:**\n`;
      output += `• **Current Environment**: ${comparison.metadata.source_name || 'Current'}\n`;
      output += `• **Target Environment**: ${comparison.metadata.target_name || 'Target'}\n`;
      output += `• **Collections Missing in Target**: ${comparison.summary.collections_missing_in_target}\n`;
      output += `• **Questions Missing in Target**: ${comparison.summary.questions_missing_in_target}\n`;
      output += `• **Dashboards Missing in Target**: ${comparison.summary.dashboards_missing_in_target}\n`;
      output += `• **Collections Missing in Source**: ${comparison.summary.collections_missing_in_source}\n`;
      output += `• **Questions Missing in Source**: ${comparison.summary.questions_missing_in_source}\n`;
      output += `• **Dashboards Missing in Source**: ${comparison.summary.dashboards_missing_in_source}\n`;
      output += `• **Different Items**: ${comparison.summary.different_items}\n`;
      output += `• **Drift Level**: ${comparison.summary.drift_level}\n\n`;

      if (comparison.missing_in_target.collections.length > 0) {
        output += `📤 **Collections in Source but NOT in Target (${comparison.missing_in_target.collections.length}):**\n`;
        comparison.missing_in_target.collections.slice(0, 5).forEach(c => {
          output += `• **${c.name}** (ID: ${c.id})\n`;
        });
        if (comparison.missing_in_target.collections.length > 5) {
          output += `... and ${comparison.missing_in_target.collections.length - 5} more\n`;
        }
        output += `\n`;
      }

      if (comparison.missing_in_target.questions.length > 0) {
        output += `📤 **Questions in Source but NOT in Target (${comparison.missing_in_target.questions.length}):**\n`;
        comparison.missing_in_target.questions.slice(0, 5).forEach(q => {
          output += `• **${q.name}** (ID: ${q.id})\n`;
        });
        if (comparison.missing_in_target.questions.length > 5) {
          output += `... and ${comparison.missing_in_target.questions.length - 5} more\n`;
        }
        output += `\n`;
      }

      if (comparison.missing_in_target.dashboards.length > 0) {
        output += `📤 **Dashboards in Source but NOT in Target (${comparison.missing_in_target.dashboards.length}):**\n`;
        comparison.missing_in_target.dashboards.slice(0, 5).forEach(d => {
          output += `• **${d.name}** (ID: ${d.id})\n`;
        });
        if (comparison.missing_in_target.dashboards.length > 5) {
          output += `... and ${comparison.missing_in_target.dashboards.length - 5} more\n`;
        }
        output += `\n`;
      }

      if (comparison.missing_in_source.collections.length > 0) {
        output += `📥 **Collections in Target but NOT in Source (${comparison.missing_in_source.collections.length}):**\n`;
        comparison.missing_in_source.collections.slice(0, 5).forEach(c => {
          output += `• **${c.name}** (ID: ${c.id})\n`;
        });
        if (comparison.missing_in_source.collections.length > 5) {
          output += `... and ${comparison.missing_in_source.collections.length - 5} more\n`;
        }
        output += `\n`;
      }

      if (comparison.different.length > 0) {
        output += `⚠️ **Items with Differences (${comparison.different.length}):**\n`;
        comparison.different.slice(0, 5).forEach(d => {
          output += `• **${d.name}** (${d.type})\n`;
          output += `  - Differences: ${d.differences.join(', ')}\n`;
        });
        if (comparison.different.length > 5) {
          output += `... and ${comparison.different.length - 5} more\n`;
        }
        output += `\n`;
      }

      output += `\n📊 **Drift Analysis:**\n`;
      if (comparison.summary.drift_level === 'HIGH') {
        output += `🔴 **HIGH DRIFT** - Environments are significantly different\n`;
        output += `• Consider syncing environments to maintain consistency\n`;
      } else if (comparison.summary.drift_level === 'MEDIUM') {
        output += `🟡 **MEDIUM DRIFT** - Some differences detected\n`;
        output += `• Review differences and sync if needed\n`;
      } else {
        output += `🟢 **LOW DRIFT** - Environments are mostly in sync\n`;
        output += `• Minor differences only\n`;
      }

      output += `\n💡 **Recommendations:**\n`;
      if (comparison.recommendations.length > 0) {
        comparison.recommendations.forEach(r => {
          output += `• ${r}\n`;
        });
      }

      output += `\n🔒 **Next Steps:**\n`;
      output += `• Export missing items from source: \`mb_meta_export_workspace\`\n`;
      output += `• Preview import to target: \`mb_meta_import_preview\`\n`;
      output += `• Regular comparison helps maintain environment consistency\n`;
      output += `• Recommended: Dev → Staging → Production promotion workflow\n`;

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata environment comparison failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Environment comparison failed:** ${error.message}`
        }]
      };
    }
  }

  async handleMetadataAutoCleanup(args) {
    if (!this.metadataClient) {
      return {
        content: [{
          type: 'text',
          text: '⚠️ **Metadata client not enabled.** Set MB_METADATA_ENABLED=true to use this feature.'
        }]
      };
    }

    try {
      const options = {
        dry_run: args.dry_run !== false,  // Default: true
        approved: args.approved === true,  // Default: false
        unused_days: args.unused_days || 180,
        orphaned_cards: args.orphaned_cards !== false,
        empty_collections: args.empty_collections !== false,
        broken_questions: args.broken_questions !== false,
        backup_recommended: args.backup_recommended !== false
      };

      const isDryRun = options.dry_run;
      const isApproved = options.approved;

      let output = `🧹 **Auto-Cleanup Analysis**\n\n`;

      if (isDryRun) {
        output += `🔒 **MODE**: DRY-RUN (Preview Only - No Changes Made)\n\n`;
      } else if (!isApproved) {
        output += `🚫 **BLOCKED**: Execution requires approved: true\n\n`;
      } else {
        output += `⚠️ **MODE**: EXECUTION (Changes Will Be Made)\n\n`;
      }

      const cleanup = await this.metadataClient.autoCleanup(options);

      output += `**Cleanup Summary:**\n`;
      output += `• **Unused Questions**: ${cleanup.summary.unused_questions}\n`;
      output += `• **Orphaned Cards**: ${cleanup.summary.orphaned_cards}\n`;
      output += `• **Empty Collections**: ${cleanup.summary.empty_collections}\n`;
      output += `• **Broken Questions**: ${cleanup.summary.broken_questions}\n`;
      output += `• **Total Items to Clean**: ${cleanup.summary.total_items}\n\n`;

      if (cleanup.blocked) {
        output += `🚫 **EXECUTION BLOCKED:**\n`;
        output += `• This is a destructive operation\n`;
        output += `• Set dry_run: false AND approved: true to execute\n`;
        output += `• Review all items carefully before approving\n\n`;
      }

      if (cleanup.items_to_cleanup.unused_questions.length > 0) {
        output += `📊 **Unused Questions (${cleanup.items_to_cleanup.unused_questions.length}):**\n`;
        output += `(Not viewed in ${options.unused_days} days)\n`;
        cleanup.items_to_cleanup.unused_questions.slice(0, 10).forEach(q => {
          output += `• **${q.name}** (ID: ${q.id})\n`;
          output += `  - Last viewed: ${q.last_viewed || 'Never'}\n`;
          output += `  - Created: ${q.created_at}\n`;
        });
        if (cleanup.items_to_cleanup.unused_questions.length > 10) {
          output += `... and ${cleanup.items_to_cleanup.unused_questions.length - 10} more\n`;
        }
        output += `\n`;
      }

      if (cleanup.items_to_cleanup.orphaned_cards.length > 0) {
        output += `🔗 **Orphaned Cards (${cleanup.items_to_cleanup.orphaned_cards.length}):**\n`;
        output += `(Not in any dashboard or collection)\n`;
        cleanup.items_to_cleanup.orphaned_cards.slice(0, 10).forEach(c => {
          output += `• **${c.name}** (ID: ${c.id})\n`;
        });
        if (cleanup.items_to_cleanup.orphaned_cards.length > 10) {
          output += `... and ${cleanup.items_to_cleanup.orphaned_cards.length - 10} more\n`;
        }
        output += `\n`;
      }

      if (cleanup.items_to_cleanup.empty_collections.length > 0) {
        output += `📁 **Empty Collections (${cleanup.items_to_cleanup.empty_collections.length}):**\n`;
        cleanup.items_to_cleanup.empty_collections.slice(0, 10).forEach(c => {
          output += `• **${c.name}** (ID: ${c.id})\n`;
        });
        if (cleanup.items_to_cleanup.empty_collections.length > 10) {
          output += `... and ${cleanup.items_to_cleanup.empty_collections.length - 10} more\n`;
        }
        output += `\n`;
      }

      if (cleanup.items_to_cleanup.broken_questions.length > 0) {
        output += `❌ **Broken Questions (${cleanup.items_to_cleanup.broken_questions.length}):**\n`;
        output += `(High error rate: >50%)\n`;
        cleanup.items_to_cleanup.broken_questions.slice(0, 10).forEach(q => {
          output += `• **${q.name}** (ID: ${q.id})\n`;
          output += `  - Error rate: ${q.error_rate}%\n`;
          output += `  - Last error: ${q.last_error}\n`;
        });
        if (cleanup.items_to_cleanup.broken_questions.length > 10) {
          output += `... and ${cleanup.items_to_cleanup.broken_questions.length - 10} more\n`;
        }
        output += `\n`;
      }

      if (cleanup.warnings.length > 0) {
        output += `⚠️ **Warnings:**\n`;
        cleanup.warnings.forEach(w => {
          output += `• ${w}\n`;
        });
        output += `\n`;
      }

      output += `\n🔒 **Safety Checks:**\n`;
      cleanup.safety_checks.forEach(check => {
        const icon = check.status === 'passed' ? '✅' : '⚠️';
        output += `${icon} **${check.check}**: ${check.message}\n`;
      });

      output += `\n💡 **Recommendations:**\n`;
      if (cleanup.recommendations.length > 0) {
        cleanup.recommendations.forEach(r => {
          output += `• ${r}\n`;
        });
      }

      output += `\n📋 **Next Steps:**\n`;
      if (isDryRun) {
        output += `1. **Review** all items to be cleaned carefully\n`;
        output += `2. **Backup** your workspace: \`mb_meta_export_workspace\`\n`;
        output += `3. **Execute** cleanup with: dry_run: false, approved: true\n`;
      } else if (cleanup.blocked) {
        output += `1. Set approved: true to execute cleanup\n`;
        output += `2. Backup recommended before execution\n`;
      } else {
        output += `✅ Cleanup executed successfully\n`;
        output += `• Archive or permanently delete archived items via Metabase UI\n`;
        output += `• Monitor for any unintended impacts\n`;
      }

      return {
        content: [{ type: 'text', text: output }]
      };
    } catch (error) {
      logger.error('Metadata auto-cleanup failed:', error);
      return {
        content: [{
          type: 'text',
          text: `❌ **Auto-cleanup failed:** ${error.message}`
        }]
      };
    }
  }


  // === ACTIVITY LOGGING HANDLERS ===

  async handleInitializeActivityLog(args) {
    try {
      if (!this.activityLogger) {
        this.activityLogger = new ActivityLogger(this.metabaseClient, {
          logTableName: 'claude_ai_activity_log',
          schema: args.schema || 'public'
        });
      }

      await this.activityLogger.initialize(args.database_id);

      return {
        content: [
          {
            type: 'text',
            text: `✅ **Activity Logging Initialized!**\\n\\n` +
              `📊 **Configuration:**\\n` +
              `• Database ID: ${args.database_id}\\n` +
              `• Schema: ${args.schema || 'public'}\\n` +
              `• Log Table: \`claude_ai_activity_log\`\\n` +
              `• Session ID: \`${this.activityLogger.sessionId}\`\\n\\n` +
              `🎯 **What Gets Tracked:**\\n` +
              `• SQL query executions and performance\\n` +
              `• Table/View/Index creation operations\\n` +
              `• Metabase dashboard and question creation\\n` +
              `• Error patterns and debugging info\\n` +
              `• Execution times and resource usage\\n\\n` +
              `📈 **Available Analytics:**\\n` +
              `• Session summaries and insights\\n` +
              `• Database usage patterns\\n` +
              `• Performance optimization suggestions\\n` +
              `• Error analysis and troubleshooting\\n\\n` +
              `💡 **Next Steps:** All your operations are now being tracked for analytics!`,
          },
        ],
      };

    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `❌ **Activity Logging Initialization Failed!**\\n\\n` +
              `🚫 **Error:** ${error.message}\\n\\n` +
              `🔧 **Troubleshooting:**\\n` +
              `• Ensure you have CREATE permissions on the schema\\n` +
              `• Verify database connection is working\\n` +
              `• Check that the database supports the required SQL features`,
          },
        ],
      };
    }
  }


  async handleGetSessionSummary(args) {
    if (!this.activityLogger) {
      return {
        content: [
          {
            type: 'text',
            text: `⚠️ **Activity logging not initialized.** Run \`activity_log_init\` first.`,
          },
        ],
      };
    }

    try {
      const summary = await this.activityLogger.getSessionSummary(args.session_id);

      if (!summary) {
        return {
          content: [
            {
              type: 'text',
              text: `📊 **No session data found.**\\n\\nSession ID: ${args.session_id || 'current session'}\\n\\nTry running some operations first to generate activity data.`,
            },
          ],
        };
      }

      const [sessionId, sessionStart, sessionEnd, totalOps, successOps, failedOps,
        dbsUsed, opTypes, totalExecTime, avgExecTime, totalRowsReturned,
        totalRowsAffected, ddlOps, queryOps, metabaseOps] = summary;

      const duration = new Date(sessionEnd) - new Date(sessionStart);
      const durationMin = Math.round(duration / 60000);
      const successRate = ((successOps / totalOps) * 100).toFixed(1);

      return {
        content: [
          {
            type: 'text',
            text: `📊 **Session Summary**\\n\\n` +
              `🔢 **Session:** \`${sessionId}\`\\n` +
              `⏰ **Duration:** ${durationMin} minutes\\n` +
              `✅ **Success Rate:** ${successRate}% (${successOps}/${totalOps} operations)\\n\\n` +
              `📈 **Operations Breakdown:**\\n` +
              `• Total Operations: ${totalOps}\\n` +
              `• SQL Queries: ${queryOps}\\n` +
              `• DDL Operations: ${ddlOps}\\n` +
              `• Metabase Operations: ${metabaseOps}\\n` +
              `• Failed Operations: ${failedOps}\\n\\n` +
              `⚡ **Performance:**\\n` +
              `• Total Execution Time: ${totalExecTime}ms\\n` +
              `• Average Execution Time: ${Math.round(avgExecTime)}ms\\n` +
              `• Data Processed: ${totalRowsReturned} rows returned\\n\\n` +
              `🎯 **Scope:**\\n` +
              `• Databases Used: ${dbsUsed}\\n` +
              `• Operation Types: ${opTypes}`,
          },
        ],
      };

    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `❌ **Failed to get session summary:** ${error.message}`,
          },
        ],
      };
    }
  }


  async handleGetOperationStats(args) {
    if (!this.activityLogger) {
      return {
        content: [
          {
            type: 'text',
            text: `⚠️ **Activity logging not initialized.** Run \`activity_log_init\` first.`,
          },
        ],
      };
    }

    try {
      const stats = await this.activityLogger.getOperationStats(args.days || 7);

      if (stats.length === 0) {
        return {
          content: [
            {
              type: 'text',
              text: `📊 **No operation data found** for the last ${args.days || 7} days.`,
            },
          ],
        };
      }

      let output = `📊 **Operation Statistics** (Last ${args.days || 7} Days)\\n\\n`;

      stats.slice(0, 10).forEach((stat, index) => {
        const [opType, opCategory, opCount, successCount, errorCount, avgTime] = stat;
        const successRate = ((successCount / opCount) * 100).toFixed(1);

        output += `${index + 1}. **${opType}** (${opCategory})\\n`;
        output += `   • Executions: ${opCount} (${successRate}% success)\\n`;
        output += `   • Avg Time: ${Math.round(avgTime)}ms\\n\\n`;
      });

      return {
        content: [
          {
            type: 'text',
            text: output,
          },
        ],
      };

    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `❌ **Failed to get operation stats:** ${error.message}`,
          },
        ],
      };
    }
  }


  async handleGetDatabaseUsage(args) {
    if (!this.activityLogger) {
      return {
        content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.** Run \`activity_log_init\` first.` }],
      };
    }

    try {
      const usage = await this.activityLogger.getDatabaseUsageStats(args.days || 30);

      if (usage.length === 0) {
        return {
          content: [{ type: 'text', text: `📊 **No database usage data found** for the last ${args.days || 30} days.` }],
        };
      }

      let output = `🗃️ **Database Usage** (Last ${args.days || 30} Days)\\n\\n`;

      usage.slice(0, 5).forEach((db, index) => {
        const [dbId, dbName, totalOps, uniqueSessions] = db;
        output += `${index + 1}. **${dbName || `DB ${dbId}`}**: ${totalOps} ops, ${uniqueSessions} sessions\\n`;
      });

      return { content: [{ type: 'text', text: output }] };

    } catch (error) {
      return { content: [{ type: 'text', text: `❌ **Failed to get database usage:** ${error.message}` }] };
    }
  }


  async handleGetErrorAnalysis(args) {
    if (!this.activityLogger) {
      return { content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.**` }] };
    }

    try {
      const errors = await this.activityLogger.getErrorAnalysis(args.days || 7);

      if (errors.length === 0) {
        return { content: [{ type: 'text', text: `✅ **No errors found** in the last ${args.days || 7} days! 🎉` }] };
      }

      let output = `🚨 **Error Analysis** (Last ${args.days || 7} Days)\\n\\n`;

      errors.slice(0, 5).forEach((error, index) => {
        const [opType, errorMsg, errorCount] = error;
        output += `${index + 1}. **${opType}**: ${errorCount} errors\\n`;
        output += `   ${errorMsg.substring(0, 80)}...\\n\\n`;
      });

      return { content: [{ type: 'text', text: output }] };

    } catch (error) {
      return { content: [{ type: 'text', text: `❌ **Error analysis failed:** ${error.message}` }] };
    }
  }


  async handleGetPerformanceInsights(args) {
    if (!this.activityLogger) {
      return { content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.**` }] };
    }

    try {
      const insights = await this.activityLogger.getPerformanceInsights(args.days || 7);

      if (insights.length === 0) {
        return { content: [{ type: 'text', text: `📊 **No performance data found.**` }] };
      }

      let output = `⚡ **Performance Insights** (Last ${args.days || 7} Days)\\n\\n`;

      insights.slice(0, 5).forEach((insight, index) => {
        const [opType, execCount, , , avgTime, , p95Time, slowOps] = insight;

        output += `${index + 1}. **${opType}**\\n`;
        output += `   • ${execCount} executions, avg ${Math.round(avgTime)}ms\\n`;
        output += `   • 95th percentile: ${Math.round(p95Time)}ms\\n`;
        output += `   • Slow operations: ${slowOps}\\n\\n`;
      });

      return { content: [{ type: 'text', text: output }] };

    } catch (error) {
      return { content: [{ type: 'text', text: `❌ **Performance insights failed:** ${error.message}` }] };
    }
  }


  async handleGetActivityTimeline(args) {
    if (!this.activityLogger) {
      return { content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.**` }] };
    }

    try {
      const timeline = await this.activityLogger.getActivityTimeline(args.days || 7, args.limit || 20);

      if (timeline.length === 0) {
        return { content: [{ type: 'text', text: `📊 **No recent activity found.**` }] };
      }

      let output = `📅 **Recent Activity**\\n\\n`;

      timeline.forEach((activity, index) => {
        const [timestamp, , opType, , , status] = activity;
        const statusIcon = status === 'success' ? '✅' : '❌';
        output += `${index + 1}. ${statusIcon} ${opType} - ${timestamp}\\n`;
      });

      return { content: [{ type: 'text', text: output }] };

    } catch (error) {
      return { content: [{ type: 'text', text: `❌ **Timeline failed:** ${error.message}` }] };
    }
  }


  async handleCleanupActivityLogs(args) {
    if (!this.activityLogger) {
      return { content: [{ type: 'text', text: `⚠️ **Activity logging not initialized.**` }] };
    }

    try {
      const retentionDays = args.retention_days || 90;
      const isDryRun = args.dry_run !== false;

      if (isDryRun) {
        return {
          content: [{
            type: 'text',
            text: `🔍 **Cleanup Preview**: Would delete logs older than ${retentionDays} days. Set \`dry_run: false\` to execute.`
          }],
        };
      }

      const deletedCount = await this.activityLogger.cleanupOldLogs();

      return {
        content: [{
          type: 'text',
          text: `✅ **Cleanup completed!** Deleted ${deletedCount} old log entries.`
        }],
      };

    } catch (error) {
      return { content: [{ type: 'text', text: `❌ **Cleanup failed:** ${error.message}` }] };
    }
  }
}
