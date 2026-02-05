import { McpError, ErrorCode, McpError as McpErrorSdk } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';
import { config } from '../../utils/config.js';

/**
 * Handler for Advanced Metadata Operations
 * Uses direct SQL access to Metabase internal database
 */
export class MetadataHandler {
    constructor(metabaseClient) {
        this.metabaseClient = metabaseClient;
    }

    /**
     * Helper to get Internal DB ID with fallback
     */
    async getInternalDbId(providedId) {
        // 1. Use provided ID if any
        if (providedId) return providedId;

        // 2. Use Env Var
        if (config.METABASE_INTERNAL_DB_ID) return config.METABASE_INTERNAL_DB_ID;

        // 3. Throw error (Auto-detect should be explicit tool call)
        throw new Error('Internal Database ID not configured. Use `meta_find_internal_db` to find it, or set METABASE_INTERNAL_DB_ID env var.');
    }

    /**
     * Auto-detect Metabase Internal DB
     */
    async handleFindInternalDb() {
        try {
            const databases = await this.metabaseClient.getDatabases();
            const candidates = [];

            for (const db of databases) {
                try {
                    // Try to query a core table
                    await this.metabaseClient.executeNativeQuery(db.id, 'SELECT count(*) FROM report_card');
                    candidates.push({ id: db.id, name: db.name, engine: db.engine });
                } catch (e) {
                    // Ignore failures, not the internal DB
                }
            }

            if (candidates.length === 0) {
                return {
                    content: [{ type: 'text', text: '❌ Could not find Internal Database. Please add Metabase Application DB as a source.' }]
                };
            }

            let output = `✅ **Internal Database Candidates Found:**\n\n`;
            candidates.forEach(c => {
                output += `- **${c.name}** (ID: ${c.id}, Engine: ${c.engine})\n`;
            });
            output += `\n💡 Recommended: Set \`METABASE_INTERNAL_DB_ID=${candidates[0].id}\` in .env`;

            return { content: [{ type: 'text', text: output }] };

        } catch (error) {
            return { content: [{ type: 'text', text: `❌ Discovery failed: ${error.message}` }] };
        }
    }

    /**
     * Audit Logs - Query Performance & Usage
     */
    async handleAuditLogs(args) {
        const dbId = await this.getInternalDbId(args.internal_db_id);
        const days = args.days || 30;
        const limit = args.limit || 50;

        const sql = `
      SELECT 
        c.name as card_name,
        c.id as card_id,
        u.email as runner_email,
        qe.started_at,
        qe.running_time as duration_ms,
        qe.native as is_native,
        qe.error
      FROM query_execution qe
      LEFT JOIN report_card c ON qe.card_id = c.id
      LEFT JOIN core_user u ON qe.executor_id = u.id
      WHERE qe.started_at > NOW() - INTERVAL '${days} days'
      ORDER BY qe.running_time DESC
      LIMIT ${limit}
    `;

        // Re-use logic from sql_execute via client
        const result = await this.metabaseClient.executeNativeQuery(dbId, sql);
        const rows = result.data.rows || [];

        let output = `📊 **Audit Logs (Last ${days} days)**\n`;
        output += `Found ${rows.length} execution records (Top Slowest)\n\n`;

        // Helper to format table
        if (rows.length > 0) {
            output += `| Card | Runner | Duration (ms) | Date | Error |\n`;
            output += `|---|---|---|---|---|\n`;
            rows.forEach(row => {
                const [card, cardId, runner, date, duration, isNative, error] = row;
                const name = card ? `${card} (${cardId})` : (isNative ? 'Ad-hoc SQL' : 'Unknown');
                const errIcon = error ? '❌' : '✅';
                output += `| ${name} | ${runner || 'System'} | ${duration} | ${new Date(date).toLocaleDateString()} | ${errIcon} |\n`;
            });
        }

        return { content: [{ type: 'text', text: output }] };
    }

    /**
     * Lineage - Dependency Graph
     */
    async handleLineage(args) {
        const dbId = await this.getInternalDbId(args.internal_db_id);
        const term = args.search_term;

        // 1. Find Questions using this table/field string in their query
        const sqlQuestions = `
      SELECT id, name, collection_id 
      FROM report_card 
      WHERE dataset_query LIKE '%${term}%' 
      AND archived = false
      LIMIT 50
    `;

        const resultQ = await this.metabaseClient.executeNativeQuery(dbId, sqlQuestions);
        const questions = resultQ.data.rows || [];

        // 2. Find Dashboards using these questions (if any questions found)
        let dashboards = [];
        if (questions.length > 0) {
            const qIds = questions.map(r => r[0]).join(',');
            const sqlDash = `
        SELECT DISTINCT d.id, d.name 
        FROM report_dashboard d
        JOIN report_dashboardcard dc ON d.id = dc.dashboard_id
        WHERE dc.card_id IN (${qIds})
        AND d.archived = false
      `;
            const resultD = await this.metabaseClient.executeNativeQuery(dbId, sqlDash);
            dashboards = resultD.data.rows || [];
        }

        let output = `🔗 **Lineage Analysis for:** \`${term}\`\n\n`;

        if (questions.length === 0) {
            output += `No direct dependencies found in active questions.\n`;
        } else {
            output += `**📉 Dependent Questions (${questions.length}):**\n`;
            questions.forEach(q => output += `- [${q[0]}] ${q[1]}\n`);

            output += `\n**📊 Impacted Dashboards (${dashboards.length}):**\n`;
            dashboards.forEach(d => output += `- [${d[0]}] ${d[1]}\n`);
        }

        return { content: [{ type: 'text', text: output }] };
    }

    /**
     * Advanced Search
     */
    async handleAdvancedSearch(args) {
        const dbId = await this.getInternalDbId(args.internal_db_id);
        const query = args.query;

        const sql = `
      SELECT id, name, 'Question' as type, description 
      FROM report_card 
      WHERE name ILIKE '%${query}%' 
         OR description ILIKE '%${query}%' 
         OR dataset_query ILIKE '%${query}%'
      UNION ALL
      SELECT id, name, 'Dashboard' as type, description
      FROM report_dashboard
      WHERE name ILIKE '%${query}%'
         OR description ILIKE '%${query}%'
      LIMIT 20
    `;

        const result = await this.metabaseClient.executeNativeQuery(dbId, sql);
        const rows = result.data.rows || [];

        let output = `🔍 **Advanced Search Results for:** \`${query}\`\n\n`;
        if (rows.length === 0) {
            output += "No results found.";
        } else {
            rows.forEach(row => {
                const [id, name, type, desc] = row;
                output += `**[${type}] ${name}** (ID: ${id})\n`;
                if (desc) output += `> ${desc}\n`;
                output += `\n`;
            });
        }

        return { content: [{ type: 'text', text: output }] };
    }
}
