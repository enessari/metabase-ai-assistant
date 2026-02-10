import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';
import { config } from '../../utils/config.js';
import { sanitizeNumber, sanitizeString, sanitizeJson } from '../../utils/sql-sanitizer.js';

/**
 * Handler for Direct SQL Dashboard Operations
 * Bypasses API limitations by directly interacting with Internal DB
 */
export class DashboardDirectHandler {
    constructor(metabaseClient, metadataHandler) {
        this.metabaseClient = metabaseClient;
        this.metadataHandler = metadataHandler;
    }

    /**
     * Add Cards to Dashboard via SQL (Batch/Loop Insert)
     * Resolves positioning issues and timeouts found in API
     */
    async handleAddCardSql(args) {
        const { dashboard_id, cards } = args;

        let internalDbId;
        try {
            internalDbId = await this.metadataHandler.getInternalDbId();
        } catch (e) {
            internalDbId = 6;
            logger.warn(`Could not determine Internal DB ID, using default: ${internalDbId}`);
        }

        const safeDashboardId = sanitizeNumber(dashboard_id);
        const results = [];
        const errors = [];

        logger.info(`Adding ${cards.length} cards to dashboard ${safeDashboardId} via Direct SQL (DB: ${internalDbId})`);

        for (const card of cards) {
            try {
                const safeCardId = sanitizeNumber(card.card_id);
                const row = sanitizeNumber(card.row !== undefined ? card.row : 0);
                const col = sanitizeNumber(card.col !== undefined ? card.col : 0);
                const sizeX = sanitizeNumber(card.size_x || 4);
                const sizeY = sanitizeNumber(card.size_y || 4);
                const vizSettings = sanitizeJson(card.visualization_settings || {});
                const paramMappings = sanitizeJson(card.parameter_mappings || []);

                const sql = `
            INSERT INTO report_dashboardcard 
            (card_id, dashboard_id, row, col, size_x, size_y, visualization_settings, parameter_mappings, created_at, updated_at)
            VALUES 
            (
                ${safeCardId}, 
                ${safeDashboardId}, 
                ${row}, 
                ${col}, 
                ${sizeX}, 
                ${sizeY}, 
                '${vizSettings}', 
                '${paramMappings}', 
                NOW(), 
                NOW()
            )
        `;

                await this.metabaseClient.executeNativeQuery(internalDbId, sql, { enforcePrefix: false });
                results.push(`✅ Card ${safeCardId} -> (${row}, ${col}) [${sizeX}x${sizeY}]`);

            } catch (error) {
                const msg = `❌ Failed Card ${card.card_id}: ${error.message}`;
                logger.error(msg);
                errors.push(msg);
            }
        }

        let output = `🏗️ **Direct SQL Card Addition Results**\n\n`;
        output += `Target Dashboard: ${safeDashboardId}\n`;
        output += `Success: ${results.length} / ${cards.length}\n\n`;

        if (results.length > 0) {
            output += `**Successfully Added:**\n${results.join('\n')}\n\n`;
        }

        if (errors.length > 0) {
            output += `**Errors:**\n${errors.join('\n')}\n`;
        }

        output += `\n💡 *Note: You may need to refresh the dashboard execution cache or reload the page to see changes immediately.*`;

        return { content: [{ type: 'text', text: output }] };
    }

    /**
     * Batch Update Dashboard Layout via SQL
     * Direct UPDATE to report_dashboardcard
     */
    async handleUpdateLayoutSql(args) {
        const { dashboard_id, updates } = args;

        let internalDbId;
        try {
            internalDbId = await this.metadataHandler.getInternalDbId();
        } catch (e) {
            internalDbId = 6;
        }

        const safeDashboardId = sanitizeNumber(dashboard_id);
        const results = [];
        const errors = [];

        logger.info(`Updating layout for ${updates.length} cards on dashboard ${safeDashboardId} (DB: ${internalDbId})`);

        for (const update of updates) {
            try {
                if (!update.card_id) continue;

                const safeCardId = sanitizeNumber(update.card_id);
                const setParts = [];
                if (update.row !== undefined) setParts.push(`row = ${sanitizeNumber(update.row)}`);
                if (update.col !== undefined) setParts.push(`col = ${sanitizeNumber(update.col)}`);
                if (update.size_x !== undefined) setParts.push(`size_x = ${sanitizeNumber(update.size_x)}`);
                if (update.size_y !== undefined) setParts.push(`size_y = ${sanitizeNumber(update.size_y)}`);

                setParts.push(`updated_at = NOW()`);

                const sql = `
            UPDATE report_dashboardcard
            SET ${setParts.join(', ')}
            WHERE dashboard_id = ${safeDashboardId} AND card_id = ${safeCardId}
        `;

                await this.metabaseClient.executeNativeQuery(internalDbId, sql, { enforcePrefix: false });
                results.push(`✅ Card ${safeCardId}`);

            } catch (error) {
                errors.push(`❌ Card ${update.card_id}: ${error.message}`);
            }
        }

        return {
            content: [{
                type: 'text',
                text: `🏗️ **Layout Update Results**\nDashboard: ${safeDashboardId}\nSuccess: ${results.length}\nErrors: ${errors.length}\n\n${errors.join('\n')}`
            }]
        };
    }

    /**
     * Create Parametric Native SQL Question
     * Constructs the complex dataset_query JSON and inserts directly
     */
    async handleCreateParametricQuestionSql(args) {
        const { name, description, database_id, query_sql, parameters, collection_id } = args;

        let internalDbId;
        try {
            internalDbId = await this.metadataHandler.getInternalDbId();
        } catch (e) {
            internalDbId = 6;
        }

        // Construct Template Tags
        const templateTags = {};
        if (parameters && Array.isArray(parameters)) {
            for (const param of parameters) {
                const tagId = `tag_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
                templateTags[param.name] = {
                    "id": tagId,
                    "name": param.name,
                    "display-name": param.display_name || param.name,
                    "type": param.type || "text",
                    "default": param.default || null,
                    "required": param.required || false
                };
                if (param.widget_type) {
                    templateTags[param.name]["widget-type"] = param.widget_type;
                }
            }
        }

        const datasetQuery = {
            "type": "native",
            "native": {
                "query": query_sql,
                "template-tags": templateTags
            },
            "database": database_id
        };

        const safeName = sanitizeString(name);
        const safeDesc = sanitizeString(description || '');
        const safeQueryJson = sanitizeJson(datasetQuery);
        const safeDbId = sanitizeNumber(database_id);
        const collectionVal = collection_id ? sanitizeNumber(collection_id) : 'NULL';
        const creatorId = 1;

        const sql = `
            INSERT INTO report_card 
            (name, description, display, dataset_query, visualization_settings, 
             creator_id, database_id, query_type, created_at, updated_at, 
             collection_id, type, parameters, archived)
            VALUES 
            (
                '${safeName}',
                '${safeDesc}',
                'table',
                '${safeQueryJson}',
                '{}',
                ${creatorId},
                ${safeDbId},
                'native',
                NOW(),
                NOW(),
                ${collectionVal},
                'question',
                '[]',
                false
            )
        `;

        try {
            await this.metabaseClient.executeNativeQuery(internalDbId, sql, { enforcePrefix: false });
            return {
                content: [{
                    type: 'text',
                    text: `✅ **Parametric Question Created**\n\nName: ${name}\nDB Source: ${database_id}\nParameters: ${Object.keys(templateTags).length}\n\n*Note: Use 'meta_advanced_search' to find the new Card ID.*`
                }]
            };
        } catch (error) {
            logger.error(`Failed to create parametric question: ${error.message}`);
            throw new McpError(ErrorCode.InternalError, `Failed to create question via SQL: ${error.message}`);
        }
    }

    /**
     * Link Dashboard Filter to Card Parameter
     * Updates parameter_mappings JSONB
     */
    async handleLinkDashboardFilter(args) {
        const { dashboard_id, card_id, mappings } = args;

        let internalDbId;
        try {
            internalDbId = await this.metadataHandler.getInternalDbId();
        } catch (e) {
            internalDbId = 6;
        }

        const safeDashboardId = sanitizeNumber(dashboard_id);
        const safeCardId = sanitizeNumber(card_id);

        const mappingArray = mappings.map(m => {
            const mapObj = {
                "parameter_id": m.parameter_id,
                "card_id": safeCardId,
                "target": null
            };

            if (m.target_type === 'variable') {
                mapObj.target = ["variable", ["template-tag", m.target_value]];
            } else if (m.target_type === 'dimension') {
                if (Array.isArray(m.target_value)) {
                    mapObj.target = m.target_value;
                } else {
                    mapObj.target = ["dimension", ["field", m.target_value, null]];
                }
            }
            return mapObj;
        });

        const jsonMappings = sanitizeJson(mappingArray);

        const sql = `
            UPDATE report_dashboardcard 
            SET parameter_mappings = '${jsonMappings}',
                updated_at = NOW()
            WHERE dashboard_id = ${safeDashboardId} AND card_id = ${safeCardId}
        `;

        try {
            await this.metabaseClient.executeNativeQuery(internalDbId, sql, { enforcePrefix: false });
            return {
                content: [{
                    type: 'text',
                    text: `✅ **Filter Linked**\nDashboard: ${safeDashboardId}\nCard: ${safeCardId}\nMappings Applied: ${mappings.length}`
                }]
            };
        } catch (error) {
            logger.error(`Failed to link filters: ${error.message}`);
            throw new McpError(ErrorCode.InternalError, `Link Filter SQL Failed: ${error.message}`);
        }
    }
}
