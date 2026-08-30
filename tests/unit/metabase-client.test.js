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
});
