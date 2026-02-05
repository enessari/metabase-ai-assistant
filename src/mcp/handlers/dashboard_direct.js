import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';
import { config } from '../../utils/config.js';

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

        // 1. Get Internal DB ID
        // Use MetadataHandler's helper or fallback to 6
        let internalDbId;
        try {
            internalDbId = await this.metadataHandler.getInternalDbId();
        } catch (e) {
            internalDbId = 6; // Fallback from user guide
            logger.warn(`Could not determine Internal DB ID, using default: ${internalDbId}`);
        }

        const results = [];
        const errors = [];

        logger.info(`Adding ${cards.length} cards to dashboard ${dashboard_id} via Direct SQL (DB: ${internalDbId})`);

        // 2. Loop through cards (Single Row Insert Constraint)
        // "Metabase/Driver limitations often cause batch inserts to fail silently or partially"
        for (const card of cards) {
            try {
                // Validation / Defaults
                const row = card.row !== undefined ? card.row : 0;
                const col = card.col !== undefined ? card.col : 0;
                const sizeX = card.size_x || 4;
                const sizeY = card.size_y || 4;
                const vizSettings = card.visualization_settings ? JSON.stringify(card.visualization_settings) : '{}';
                const paramMappings = card.parameter_mappings ? JSON.stringify(card.parameter_mappings) : '[]';

                // 3. Construct SQL
                const sql = `
            INSERT INTO report_dashboardcard 
            (card_id, dashboard_id, row, col, size_x, size_y, visualization_settings, parameter_mappings, created_at, updated_at)
            VALUES 
            (
                ${card.card_id}, 
                ${dashboard_id}, 
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

                // 4. Execute
                await this.metabaseClient.executeNativeQuery(internalDbId, sql, { enforcePrefix: false });

                results.push(`✅ Card ${card.card_id} -> (${row}, ${col}) [${sizeX}x${sizeY}]`);

            } catch (error) {
                const msg = `❌ Failed Card ${card.card_id}: ${error.message}`;
                logger.error(msg);
                errors.push(msg);
            }
        }

        // 5. Construct Response
        let output = `🏗️ **Direct SQL Card Addition Results**\n\n`;
        output += `Target Dashboard: ${dashboard_id}\n`;
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

        // Default to DB 6 if no other info
        let internalDbId;
        try {
            internalDbId = await this.metadataHandler.getInternalDbId();
        } catch (e) {
            internalDbId = 6;
        }

        const results = [];
        const errors = [];

        logger.info(`Updating layout for ${updates.length} cards on dashboard ${dashboard_id} (DB: ${internalDbId})`);

        for (const update of updates) {
            try {
                if (!update.card_id) continue;

                const setParts = [];
                if (update.row !== undefined) setParts.push(`row = ${update.row}`);
                if (update.col !== undefined) setParts.push(`col = ${update.col}`);
                if (update.size_x !== undefined) setParts.push(`size_x = ${update.size_x}`);
                if (update.size_y !== undefined) setParts.push(`size_y = ${update.size_y}`);

                setParts.push(`updated_at = NOW()`);

                const sql = `
            UPDATE report_dashboardcard
            SET ${setParts.join(', ')}
            WHERE dashboard_id = ${dashboard_id} AND card_id = ${update.card_id}
        `;

                await this.metabaseClient.executeNativeQuery(internalDbId, sql, { enforcePrefix: false });
                results.push(`✅ Card ${update.card_id}`);

            } catch (error) {
                errors.push(`❌ Card ${update.card_id}: ${error.message}`);
            }
        }

        return {
            content: [{
                type: 'text',
                text: `🏗️ **Layout Update Results**\nDashboard: ${dashboard_id}\nSuccess: ${results.length}\nErrors: ${errors.length}\n\n${errors.join('\n')}`
            }]
        };
    }

    /**
     * Create Parametric Native SQL Question
     * Constructs the complex dataset_query JSON and inserts directly
     */
    async handleCreateParametricQuestionSql(args) {
        const { name, description, database_id, query_sql, parameters, collection_id } = args;

        // 1. Get Internal DB ID
        let internalDbId;
        try {
            internalDbId = await this.metadataHandler.getInternalDbId();
        } catch (e) {
            internalDbId = 6;
        }

        // 2. Construct Template Tags
        const templateTags = {};
        if (parameters && Array.isArray(parameters)) {
            for (const param of parameters) {
                // Generate a UUID-like key or just use the name
                const tagId = `tag_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
                templateTags[param.name] = {
                    "id": tagId,
                    "name": param.name,
                    "display-name": param.display_name || param.name,
                    "type": param.type || "text", // text, number, date, dimension
                    "default": param.default || null,
                    "required": param.required || false
                };

                // Add widget type if specified (e.g. category filter)
                if (param.widget_type) {
                    templateTags[param.name]["widget-type"] = param.widget_type;
                }
            }
        }

        // 3. Construct Dataset Query
        const datasetQuery = {
            "type": "native",
            "native": {
                "query": query_sql,
                "template-tags": templateTags
            },
            "database": database_id
        };

        const display = "table"; // Default display type
        const vizSettings = "{}"; // Default empty viz settings

        // 4. Construct SQL Insert
        // Note: collection_id can be NULL
        const collectionVal = collection_id ? collection_id : 'NULL';

        // creator_id fallback (usually 1 for admin)
        const creatorId = 1;

        // Escaping for SQL string literals (basic)
        const safeName = name.replace(/'/g, "''");
        const safeDesc = (description || '').replace(/'/g, "''");
        const safeQueryJson = JSON.stringify(datasetQuery).replace(/'/g, "''");

        const sql = `
            INSERT INTO report_card 
            (name, description, display, dataset_query, visualization_settings, 
             creator_id, database_id, query_type, created_at, updated_at, 
             collection_id, type, parameters, archived)
            VALUES 
            (
                '${safeName}',
                '${safeDesc}',
                '${display}',
                '${safeQueryJson}',
                '${vizSettings}',
                ${creatorId},
                ${database_id},
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

        // 1. Get Internal DB ID
        let internalDbId;
        try {
            internalDbId = await this.metadataHandler.getInternalDbId();
        } catch (e) {
            internalDbId = 6;
        }

        // 2. Construct Mappings JSON
        const mappingArray = mappings.map(m => {
            const mapObj = {
                "parameter_id": m.parameter_id,
                "card_id": card_id,
                "target": m.target_value // already formatted based on type? 
                // Wait, implementation plan said type enum.
                // We need to construct the target array structure based on type.
            };

            if (m.target_type === 'variable') {
                mapObj.target = ["variable", ["template-tag", m.target_value]];
            } else if (m.target_type === 'dimension') {
                // Dimension format: ["dimension", ["field", id, null]] or similar
                // For now, assume user passes the raw dimension array or string if they know it.
                // OR simplify: require target_value to be the full target structure if complex.
                // Let's stick to variable support as primary goal.
                if (Array.isArray(m.target_value)) {
                    mapObj.target = m.target_value;
                } else {
                    mapObj.target = ["dimension", ["field", m.target_value, null]];
                }
            }
            return mapObj;
        });

        const jsonMappings = JSON.stringify(mappingArray);

        // 3. Execute Update
        const sql = `
            UPDATE report_dashboardcard 
            SET parameter_mappings = '${jsonMappings}',
                updated_at = NOW()
            WHERE dashboard_id = ${dashboard_id} AND card_id = ${card_id}
        `;

        try {
            await this.metabaseClient.executeNativeQuery(internalDbId, sql, { enforcePrefix: false });
            return {
                content: [{
                    type: 'text',
                    text: `✅ **Filter Linked**\nDashboard: ${dashboard_id}\nCard: ${card_id}\nMappings Applied: ${mappings.length}`
                }]
            };
        } catch (error) {
            logger.error(`Failed to link filters: ${error.message}`);
            throw new McpError(ErrorCode.InternalError, `Link Filter SQL Failed: ${error.message}`);
        }
    }
}
