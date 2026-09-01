import { jest } from '@jest/globals';
import { DbtMetabaseSyncer, METABASE_SEMANTIC_TYPE_MAP } from '../../src/dbt/metabase-syncer.js';
import { MetabaseReverseLineage } from '../../src/dbt/metabase-reverse-lineage.js';
import { DbtSmartCardBuilder } from '../../src/dbt/dbt-smart-card-builder.js';

describe('dbt Metabase Advanced Syncer, Reverse Lineage & Smart Card Unit Tests', () => {
  let mockClient;
  let syncer;
  let reverseLineage;
  let smartCardBuilder;

  beforeEach(() => {
    mockClient = {
      getDatabaseTables: jest.fn().mockResolvedValue([
        { id: 10, name: 'fct_daily_sales', display_name: 'Fct Daily Sales', description: '' },
        { id: 11, name: 'dim_customers', display_name: 'Dim Customers', description: '' }
      ]),
      getTableFields: jest.fn().mockResolvedValue([
        { id: 101, name: 'order_id', base_type: 'type/Integer', semantic_type: null, description: '' },
        { id: 102, name: 'total_amount', base_type: 'type/Decimal', semantic_type: null, description: '' },
        { id: 103, name: 'customer_email', base_type: 'type/Text', semantic_type: null, description: '' },
        { id: 104, name: 'created_at', base_type: 'type/DateTime', semantic_type: null, description: '' }
      ]),
      put: jest.fn().mockResolvedValue({ status: 'ok' }),
      post: jest.fn().mockResolvedValue({ id: 999, name: 'mock_created' }),
      get: jest.fn().mockImplementation((endpoint) => {
        if (endpoint === '/dashboard') {
          return Promise.resolve([{ id: 1, name: 'Executive Sales Dashboard', archived: false }]);
        }
        if (endpoint === '/dashboard/1') {
          return Promise.resolve({
            id: 1,
            name: 'Executive Sales Dashboard',
            ordered_cards: [
              {
                card: {
                  id: 50,
                  name: 'Daily Revenue Trend',
                  dataset_query: {
                    type: 'native',
                    native: { query: 'SELECT * FROM fct_daily_sales JOIN dim_customers ON fct_daily_sales.cust_id = dim_customers.id;' }
                  }
                }
              }
            ]
          });
        }
        if (endpoint === '/card') {
          return Promise.resolve([
            {
              id: 60,
              name: 'Customer Count Analysis',
              dataset_query: {
                type: 'native',
                native: { query: 'SELECT count(*) FROM dim_customers;' }
              }
            }
          ]);
        }
        return Promise.resolve([]);
      }),
      executeAndHeal: jest.fn().mockResolvedValue({
        final_sql: 'SELECT * FROM fct_daily_sales WHERE status = \'completed\' LIMIT 10;',
        data: { columns: [{ name: 'day' }, { name: 'sales' }], rows: [['2026-08-01', 5000]] }
      })
    };

    syncer = new DbtMetabaseSyncer(mockClient);
    reverseLineage = new MetabaseReverseLineage(mockClient);
    smartCardBuilder = new DbtSmartCardBuilder(mockClient);
  });

  test('inferSemanticType accurately infers Metabase semantic types', () => {
    expect(syncer.inferSemanticType('user_email')).toBe('type/Email');
    expect(syncer.inferSemanticType('net_revenue')).toBe('type/Currency');
    expect(syncer.inferSemanticType('customer_id')).toBe('type/FK');
    expect(syncer.inferSemanticType('id')).toBe('type/PK');
    expect(syncer.inferSemanticType('order_created_at')).toBe('type/CreationTimestamp');
    expect(syncer.inferSemanticType('country_code')).toBe('type/Country');
  });

  test('syncMetadata synchronizes table labels and field semantic types into Metabase', async () => {
    // Populate scanner with mock model
    syncer.scanner.models.set('fct_daily_sales', {
      name: 'fct_daily_sales',
      description: 'Core company daily sales revenue',
      meta: { metabase: { label: '🏢 Şirket Geneli Günlük Satış' } },
      columns: {
        total_amount: { description: 'Net sales in TRY', meta: { metabase: { semantic_type: 'type/Currency' } } },
        customer_email: { description: 'Customer login email', meta: {} }
      }
    });

    const res = await syncer.syncMetadata({ database_id: 1, dry_run: false });
    expect(res.tables_updated).toBe(1);
    expect(res.fields_updated).toBeGreaterThanOrEqual(1);
    expect(mockClient.put).toHaveBeenCalledWith('/table/10', expect.objectContaining({
      display_name: '🏢 Şirket Geneli Günlük Satış',
      description: 'Core company daily sales revenue'
    }));
  });

  test('generateExposures generates clean dbt exposure YAML with reverse dependencies', async () => {
    const res = await reverseLineage.generateExposures({ metabase_base_url: 'https://metabase.company.com' });
    expect(res.total_exposures).toBe(2);
    expect(res.yaml).toContain('metabase_dashboard_1_executive_sales_dashboard');
    expect(res.yaml).toContain("ref('fct_daily_sales')");
    expect(res.yaml).toContain("ref('dim_customers')");
  });

  test('inferDisplayType selects correct visual chart format', () => {
    expect(smartCardBuilder.inferDisplayType([{ name: 'total' }], [[100]])).toBe('scalar');
    expect(smartCardBuilder.inferDisplayType([{ name: 'date' }, { name: 'sales' }], [['2026-08-01', 500]])).toBe('line');
    expect(smartCardBuilder.inferDisplayType([{ name: 'category' }, { name: 'count' }], [['electronics', 50]])).toBe('bar');
  });
});
