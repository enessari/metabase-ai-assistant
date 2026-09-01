import { jest } from '@jest/globals';
import { DbtSemanticHandler } from '../../src/mcp/handlers/dbt-semantic.js';

describe('dbt Reverse Lineage, Sync & Smart Card MCP Integration Tests', () => {
  let mockClient;
  let handler;

  beforeEach(() => {
    mockClient = {
      getDatabaseTables: jest.fn().mockResolvedValue([
        { id: 1, name: 'fct_orders', display_name: 'Fct Orders', description: '' }
      ]),
      getTableFields: jest.fn().mockResolvedValue([
        { id: 101, name: 'order_id', base_type: 'type/Integer', semantic_type: null, description: '' },
        { id: 102, name: 'gross_amount', base_type: 'type/Decimal', semantic_type: null, description: '' }
      ]),
      put: jest.fn().mockResolvedValue({ status: 'ok' }),
      post: jest.fn().mockResolvedValue({ id: 123, name: 'Smart Card' }),
      get: jest.fn().mockImplementation((endpoint) => {
        if (endpoint === '/dashboard') return Promise.resolve([{ id: 10, name: 'Revenue Board', archived: false }]);
        if (endpoint === '/dashboard/10') {
          return Promise.resolve({
            id: 10,
            name: 'Revenue Board',
            ordered_cards: [
              {
                card: {
                  id: 20,
                  name: 'Card 1',
                  dataset_query: { type: 'native', native: { query: 'SELECT * FROM fct_orders;' } }
                }
              }
            ]
          });
        }
        if (endpoint === '/card') return Promise.resolve([]);
        return Promise.resolve([]);
      }),
      executeAndHeal: jest.fn().mockResolvedValue({
        final_sql: 'SELECT * FROM fct_orders LIMIT 10;',
        data: { columns: [{ name: 'day' }, { name: 'sales' }], rows: [['2026-08-01', 5000]] }
      })
    };

    handler = new DbtSemanticHandler(mockClient);
  });

  test('dbt_generate_exposures_from_metabase tool executes and returns formatted YAML', async () => {
    const res = await handler.handleDbtGenerateExposuresFromMetabase({
      metabase_base_url: 'https://metabase.corp.com'
    });

    expect(res.isError).toBeFalsy();
    expect(res.structuredContent.total_exposures).toBe(1);
    expect(res.structuredContent.yaml).toContain('metabase_dashboard_10_revenue_board');
    expect(res.structuredContent.yaml).toContain("ref('fct_orders')");
  });

  test('dbt_sync_metadata_to_metabase tool executes with dry_run simulation', async () => {
    handler.deepScanner.models.set('fct_orders', {
      name: 'fct_orders',
      description: 'Fact table of customer orders',
      meta: { metabase: { label: '🏢 Siparişler ve Ciro' } },
      columns: {
        gross_amount: { description: 'Brüt satış tutarı', meta: { metabase: { semantic_type: 'type/Currency' } } }
      }
    });

    const res = await handler.handleDbtSyncMetadataToMetabase({
      database_id: 1,
      dry_run: true
    });

    expect(res.isError).toBeFalsy();
    expect(res.structuredContent.tables_updated).toBe(1);
    expect(res.structuredContent.fields_updated).toBeGreaterThanOrEqual(1);
    expect(res.structuredContent.dry_run).toBe(true);
  });
});
