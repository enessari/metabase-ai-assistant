/**
 * src/dbt/dbt-dashboard-architect.js
 * dbt-Aware Autonomous Metabase Dashboard & Card Architect
 *
 * Automatically inspects a dbt Mart model, generates canonical business cards
 * (Scorecards for Gross Sales & Net Profit, Daily Trend Line Charts, Channel Breakdown Bar Charts),
 * creates a Metabase Dashboard, and arranges cards on the 24-column grid.
 */

import { logger } from '../utils/logger.js';

export class DbtDashboardArchitect {
  constructor(metabaseClient, options = {}) {
    this.client = metabaseClient;
  }

  /**
   * Architect and build an Executive Dashboard in Metabase for a dbt model
   */
  async buildExecutiveDashboard(databaseId, options = {}) {
    const {
      dashboardName = "👑 Şirket Geneli Satış ve Kârlılık Paneli (dbt Marts)",
      description = "dbt mrt_executive_daily_sales modeli üzerinden otomatik oluşturulan kurumsal yönetim gösterge panosu.",
      collectionId = null
    } = options;

    logger.info(`Architecting Metabase Dashboard: '${dashboardName}'...`);

    // 1. Create Dashboard
    const dashboardPayload = {
      name: dashboardName,
      description: description,
      collection_id: collectionId,
      parameters: []
    };

    const dashboard = await this.client.request('POST', '/api/dashboard', dashboardPayload);
    const dashboardId = dashboard.id;
    logger.info(`Created Dashboard ID: ${dashboardId}`);

    // 2. Define Card Definitions tailored to mrt_executive_daily_sales
    const cardTemplates = [
      {
        name: "💰 Toplam Brüt Ciro (Son 30 Gün)",
        display: "scalar",
        query: `SELECT SUM(gross_sales_amount_try) AS toplam_ciro
FROM \`turna-dwh.marts_executive.mrt_executive_daily_sales\`
WHERE booking_date >= DATE_SUB(CURRENT_DATE('Europe/Istanbul'), INTERVAL 30 DAY)`,
        grid: { col: 0, row: 0, size_x: 6, size_y: 3 }
      },
      {
        name: "📈 Toplam Net Kâr (Son 30 Gün)",
        display: "scalar",
        query: `SELECT SUM(net_profit_try) AS toplam_net_kar
FROM \`turna-dwh.marts_executive.mrt_executive_daily_sales\`
WHERE booking_date >= DATE_SUB(CURRENT_DATE('Europe/Istanbul'), INTERVAL 30 DAY)`,
        grid: { col: 6, row: 0, size_x: 6, size_y: 3 }
      },
      {
        name: "🎫 Toplam Net Bilet Adedi (Son 30 Gün)",
        display: "scalar",
        query: `SELECT SUM(net_ticket_count) AS net_bilet_adedi
FROM \`turna-dwh.marts_executive.mrt_executive_daily_sales\`
WHERE booking_date >= DATE_SUB(CURRENT_DATE('Europe/Istanbul'), INTERVAL 30 DAY)`,
        grid: { col: 12, row: 0, size_x: 6, size_y: 3 }
      },
      {
        name: "🛡️ Toplam DFF Marjı (Son 30 Gün)",
        display: "scalar",
        query: `SELECT SUM(dff_amount_try) AS toplam_dff
FROM \`turna-dwh.marts_executive.mrt_executive_daily_sales\`
WHERE booking_date >= DATE_SUB(CURRENT_DATE('Europe/Istanbul'), INTERVAL 30 DAY)`,
        grid: { col: 18, row: 0, size_x: 6, size_y: 3 }
      },
      {
        name: "📅 Günlük Ciro & Net Kâr Trendi (TRY)",
        display: "line",
        query: `SELECT booking_date,
       SUM(gross_sales_amount_try) AS brüt_ciro,
       SUM(net_profit_try) AS net_kar
FROM \`turna-dwh.marts_executive.mrt_executive_daily_sales\`
WHERE booking_date >= DATE_SUB(CURRENT_DATE('Europe/Istanbul'), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY 1 ASC`,
        grid: { col: 0, row: 3, size_x: 12, size_y: 6 }
      },
      {
        name: "🛍️ Ürün Dikeyine Göre Ciro Dağılımı",
        display: "bar",
        query: `SELECT product_type AS urun_turu,
       SUM(gross_sales_amount_try) AS toplam_ciro
FROM \`turna-dwh.marts_executive.mrt_executive_daily_sales\`
WHERE booking_date >= DATE_SUB(CURRENT_DATE('Europe/Istanbul'), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY 2 DESC`,
        grid: { col: 12, row: 3, size_x: 6, size_y: 6 }
      },
      {
        name: "👥 Satış Kanallarına Göre Bilet Hacmi",
        display: "bar",
        query: `SELECT channel_name AS satis_kanali,
       SUM(sale_ticket_count) AS bilet_adedi
FROM \`turna-dwh.marts_executive.mrt_executive_daily_sales\`
WHERE booking_date >= DATE_SUB(CURRENT_DATE('Europe/Istanbul'), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY 2 DESC`,
        grid: { col: 18, row: 3, size_x: 6, size_y: 6 }
      }
    ];

    const createdCards = [];
    const dashboardCards = [];

    // 3. Create Cards and add to Dashboard
    for (const tpl of cardTemplates) {
      const cardPayload = {
        name: tpl.name,
        display: tpl.display,
        dataset_query: {
          database: databaseId,
          type: "native",
          native: {
            query: tpl.query
          }
        },
        visualization_settings: {}
      };

      const card = await this.client.request('POST', '/api/card', cardPayload);
      createdCards.push(card);

      dashboardCards.push({
        id: -dashboardCards.length - 1, // temporary card_id for batch
        card_id: card.id,
        row: tpl.grid.row,
        col: tpl.grid.col,
        size_x: tpl.grid.size_x,
        size_y: tpl.grid.size_y,
        parameter_mappings: [],
        visualization_settings: {}
      });
    }

    // 4. Save Cards onto Dashboard
    await this.client.request('PUT', `/api/dashboard/${dashboardId}/cards`, {
      cards: dashboardCards
    });

    logger.info(`Successfully added ${createdCards.length} cards to Dashboard ${dashboardId}!`);

    return {
      dashboard_id: dashboardId,
      dashboard_url: `${this.client.baseURL}/dashboard/${dashboardId}`,
      dashboard_name: dashboardName,
      cards_created: createdCards.length,
      cards: createdCards.map(c => ({ id: c.id, name: c.name, display: c.display }))
    };
  }
}
