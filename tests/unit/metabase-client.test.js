import { MetabaseClient } from '../../src/metabase/client.js';
import { jest } from '@jest/globals';

describe('Unit Test: MetabaseClient', () => {
  test('authenticates via API key without session endpoint call', async () => {
    const client = new MetabaseClient({
      url: 'http://localhost:3000',
      apiKey: 'mb_test_api_key_123',
    });

    const result = await client.authenticate();
    expect(result).toBe(true);
    expect(client.client.defaults.headers['x-api-key']).toBe('mb_test_api_key_123');
  });

  test('authenticates via username/password and sets X-Metabase-Session header', async () => {
    const client = new MetabaseClient({
      url: 'http://localhost:3000',
      username: 'admin@metabase.local',
      password: 'password123',
    });

    client.client.post = jest.fn().mockResolvedValueOnce({
      data: { id: 'session_token_xyz_456' },
    });

    const result = await client.authenticate();
    expect(result).toBe(true);
    expect(client.sessionToken).toBe('session_token_xyz_456');
    expect(client.client.defaults.headers['X-Metabase-Session']).toBe('session_token_xyz_456');
  });

  test('handles 401 invalid credentials gracefully', async () => {
    const client = new MetabaseClient({
      url: 'http://localhost:3000',
      username: 'wrong@user.com',
      password: 'badpassword',
    });

    client.client.post = jest.fn().mockRejectedValueOnce({
      response: { status: 401, data: { message: 'Invalid credentials' } },
      message: 'Request failed with status code 401',
    });

    await expect(client.authenticate()).rejects.toThrow('Invalid username or password: Invalid credentials');
  });

  describe('Metabase Version Discovery & Modern API Compatibility', () => {
    test('getServerVersion discovers version tag and enterprise status from /api/session/properties', async () => {
      const client = new MetabaseClient({
        url: 'http://localhost:3000',
        apiKey: 'test_key',
      });
      client.sessionToken = 'mock_token';
      client.client.get = jest.fn().mockResolvedValueOnce({
        data: {
          version: { tag: 'v0.52.4', date: '2026-06-01', branch: 'release-x.52.x' },
          'has-premium-features?': true,
        },
      });

      const version = await client.getServerVersion();
      expect(version.tag).toBe('v0.52.4');
      expect(version.is_enterprise).toBe(true);
      expect(version.branch).toBe('release-x.52.x');
    });

    test('getCollectionTree calls /api/collection/tree endpoint', async () => {
      const client = new MetabaseClient({
        url: 'http://localhost:3000',
        apiKey: 'test_key',
      });
      client.sessionToken = 'mock_token';
      client.client.request = jest.fn().mockResolvedValueOnce({
        data: [{ id: 'root', name: 'Our analytics', children: [{ id: 1, name: 'Marketing' }] }],
      });

      const tree = await client.getCollectionTree();
      expect(tree).toBeDefined();
      expect(tree[0].name).toBe('Our analytics');
      expect(tree[0].children[0].name).toBe('Marketing');
    });

    test('getCollectionItems supports root and nested collection items query', async () => {
      const client = new MetabaseClient({
        url: 'http://localhost:3000',
        apiKey: 'test_key',
      });
      client.sessionToken = 'mock_token';
      client.client.request = jest.fn().mockResolvedValueOnce({
        data: { total: 5, data: [{ id: 42, model: 'card', name: 'Weekly MRR' }] },
      });

      const items = await client.getCollectionItems(1, { models: ['card', 'dashboard'] });
      expect(items.total).toBe(5);
      expect(items.data[0].name).toBe('Weekly MRR');
    });
  });
});
