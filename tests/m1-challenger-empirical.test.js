import { CollectionsHandler } from '../src/mcp/handlers/collections.js';
import { UsersHandler } from '../src/mcp/handlers/users.js';
import { ActionsHandler } from '../src/mcp/handlers/actions.js';
import { jest } from '@jest/globals';

describe('Empirical Verification: Milestone 1 Handler Implementations', () => {
  let mockClient;

  beforeEach(() => {
    mockClient = {
      request: jest.fn(),
    };
  });

  // =========================================================================
  // 1. CollectionsHandler.prototype.handleCollectionCopy REST API verification
  // =========================================================================
  describe('CollectionsHandler.prototype.handleCollectionCopy', () => {
    test('successfully copies a collection containing cards, dashboards, and ignores non-card/dashboard items', async () => {
      const sourceCollectionId = 10;
      const destinationParentId = 20;
      const newCollectionName = 'Q3 Marketing Reports (Cloned)';

      // Mock sequence of Metabase REST API calls:
      mockClient.request.mockImplementation((method, endpoint, body) => {
        // 1. Fetch source collection
        if (method === 'GET' && endpoint === `/api/collection/${sourceCollectionId}`) {
          return Promise.resolve({
            id: sourceCollectionId,
            name: 'Q3 Marketing Reports',
            description: 'Original description',
            parent_id: 1,
          });
        }
        // 2. Create target collection
        if (method === 'POST' && endpoint === '/api/collection') {
          return Promise.resolve({
            id: 999,
            name: body.name,
            description: body.description,
            parent_id: body.parent_id,
          });
        }
        // 3. Fetch items in source collection
        if (method === 'GET' && endpoint === `/api/collection/${sourceCollectionId}/items`) {
          return Promise.resolve([
            { id: 101, model: 'card' },
            { id: 201, model: 'dashboard' },
            { id: 301, model: 'pulse' }, // other model should be safely skipped
          ]);
        }
        // 4. Fetch source card details
        if (method === 'GET' && endpoint === '/api/card/101') {
          return Promise.resolve({
            id: 101,
            name: 'Conversion Rate by Channel',
            description: 'Card description',
            display: 'line',
            dataset_query: { type: 'query', query: { 'source-table': 5 } },
            visualization_settings: { 'graph.x': 'date' },
            collection_id: sourceCollectionId,
          });
        }
        // 5. Create new card in destination collection
        if (method === 'POST' && endpoint === '/api/card') {
          return Promise.resolve({
            id: 102,
            name: body.name,
            collection_id: body.collection_id,
          });
        }
        // 6. Fetch source dashboard details
        if (method === 'GET' && endpoint === '/api/dashboard/201') {
          return Promise.resolve({
            id: 201,
            name: 'Campaign Overview Dashboard',
            description: 'Dash description',
            collection_id: sourceCollectionId,
          });
        }
        // 7. Create new dashboard in destination collection
        if (method === 'POST' && endpoint === '/api/dashboard') {
          return Promise.resolve({
            id: 202,
            name: body.name,
            collection_id: body.collection_id,
          });
        }
        return Promise.reject(new Error(`Unexpected request: ${method} ${endpoint}`));
      });

      const handler = new CollectionsHandler(mockClient);
      const result = await handler.handleCollectionCopy({
        collection_id: sourceCollectionId,
        destination_id: destinationParentId,
        new_name: newCollectionName,
      });

      expect(result).toBeDefined();
      expect(result.content).toBeDefined();
      expect(result.content[0].type).toBe('text');
      expect(result.content[0].text).toContain('✅ Collection copied:');
      expect(result.content[0].text).toContain('New Collection ID: 999');
      expect(result.content[0].text).toContain(`Name: ${newCollectionName}`);
      expect(result.content[0].text).toContain('Cards copied: 1');
      expect(result.content[0].text).toContain('Dashboards copied: 1');

      // Verify POST /api/collection arguments
      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/collection', {
        name: newCollectionName,
        description: 'Original description',
        parent_id: destinationParentId,
      });

      // Verify POST /api/card arguments
      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/card', {
        name: 'Copy of Conversion Rate by Channel',
        description: 'Card description',
        display: 'line',
        dataset_query: { type: 'query', query: { 'source-table': 5 } },
        visualization_settings: { 'graph.x': 'date' },
        collection_id: 999,
      });

      // Verify POST /api/dashboard arguments
      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/dashboard', {
        name: 'Copy of Campaign Overview Dashboard',
        description: 'Dash description',
        collection_id: 999,
      });
    });

    test('handles default new_name and parent_id fallback when omitted', async () => {
      mockClient.request.mockImplementation((method, endpoint, body) => {
        if (method === 'GET' && endpoint === '/api/collection/5') {
          return Promise.resolve({
            id: 5,
            name: 'Finance',
            description: 'Financial records',
            parent_id: 2,
          });
        }
        if (method === 'POST' && endpoint === '/api/collection') {
          return Promise.resolve({ id: 50, name: body.name, parent_id: body.parent_id });
        }
        if (method === 'GET' && endpoint === '/api/collection/5/items') {
          return Promise.resolve({ data: [] }); // Wrapped in { data: [] }
        }
        return Promise.reject(new Error(`Unexpected: ${method} ${endpoint}`));
      });

      const handler = new CollectionsHandler(mockClient);
      const result = await handler.handleCollectionCopy({
        collection_id: 5,
      });

      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/collection', {
        name: 'Copy of Finance',
        description: 'Financial records',
        parent_id: 2,
      });
      expect(result.content[0].text).toContain('Cards copied: 0');
      expect(result.content[0].text).toContain('Dashboards copied: 0');
    });

    test('gracefully handles and returns error on REST API failure during source collection fetch', async () => {
      mockClient.request.mockRejectedValueOnce(new Error('Collection 404 Not Found'));

      const handler = new CollectionsHandler(mockClient);
      const result = await handler.handleCollectionCopy({ collection_id: 404 });

      expect(result.content[0].text).toBe('❌ Collection copy error: Collection 404 Not Found');
    });

    test('gracefully handles and returns error on REST API failure during card fetch/creation', async () => {
      mockClient.request.mockImplementation((method, endpoint) => {
        if (method === 'GET' && endpoint === '/api/collection/1') {
          return Promise.resolve({ id: 1, name: 'Root Coll', parent_id: null });
        }
        if (method === 'POST' && endpoint === '/api/collection') {
          return Promise.resolve({ id: 2, name: 'Copy of Root Coll' });
        }
        if (method === 'GET' && endpoint === '/api/collection/1/items') {
          return Promise.resolve([{ id: 10, model: 'card' }]);
        }
        if (method === 'GET' && endpoint === '/api/card/10') {
          return Promise.reject(new Error('Card 10 access denied'));
        }
        return Promise.reject(new Error('Unexpected'));
      });

      const handler = new CollectionsHandler(mockClient);
      const result = await handler.handleCollectionCopy({ collection_id: 1 });

      expect(result.content[0].text).toBe('❌ Collection copy error: Card 10 access denied');
    });
  });

  // =========================================================================
  // 2. UsersHandler Tool Routing & Error Handling Verification
  // =========================================================================
  describe('UsersHandler routing and error handling', () => {
    test('routes() exposes all 10 user and permission group tools', () => {
      const handler = new UsersHandler(mockClient);
      const routes = handler.routes();
      const expectedTools = [
        'mb_user_list',
        'mb_user_get',
        'mb_user_create',
        'mb_user_update',
        'mb_user_disable',
        'mb_permission_group_list',
        'mb_permission_group_create',
        'mb_permission_group_delete',
        'mb_permission_group_add_user',
        'mb_permission_group_remove_user',
      ];

      expect(Object.keys(routes).sort()).toEqual(expectedTools.sort());
      for (const toolName of expectedTools) {
        expect(typeof routes[toolName]).toBe('function');
      }
    });

    test('mb_user_list handles status filtering, group filtering, and null names correctly', async () => {
      const mockUsers = [
        { id: 1, email: 'admin@test.com', first_name: 'Admin', last_name: 'User', is_active: true, is_superuser: true, group_ids: [1, 2] },
        { id: 2, email: 'inactive@test.com', first_name: null, last_name: null, is_active: false, is_superuser: false, group_ids: [1] },
        { id: 3, email: 'member@test.com', first_name: 'Member', last_name: 'User', is_active: true, is_superuser: false, group_ids: [2] },
      ];

      mockClient.request.mockResolvedValue({ data: mockUsers });

      const handler = new UsersHandler(mockClient);
      const routes = handler.routes();

      // 1. All users
      const allResult = await routes['mb_user_list']({});
      expect(allResult.structuredContent.count).toBe(3);
      expect(allResult.structuredContent.users[1].first_name).toBeNull();
      expect(allResult.structuredContent.users[1].last_name).toBeNull();

      // 2. Active users filter
      const activeResult = await routes['mb_user_list']({ status: 'active' });
      expect(activeResult.structuredContent.count).toBe(2);
      expect(activeResult.structuredContent.users.map(u => u.id)).toEqual([1, 3]);

      // 3. Inactive users filter
      const inactiveResult = await routes['mb_user_list']({ status: 'inactive' });
      expect(inactiveResult.structuredContent.count).toBe(1);
      expect(inactiveResult.structuredContent.users[0].id).toBe(2);

      // 4. Group filter
      const groupResult = await routes['mb_user_list']({ group_id: 2 });
      expect(groupResult.structuredContent.count).toBe(2);
      expect(groupResult.structuredContent.users.map(u => u.id)).toEqual([1, 3]);
    });

    test('mb_user_list handles REST API errors without uncaught exception', async () => {
      mockClient.request.mockRejectedValueOnce(new Error('Unauthorized'));
      const handler = new UsersHandler(mockClient);
      const result = await handler.routes()['mb_user_list']({});
      expect(result.content[0].text).toContain('❌ User list error: Unauthorized');
    });

    test('mb_user_get formats user info on success and catches errors', async () => {
      mockClient.request.mockResolvedValueOnce({
        id: 7,
        first_name: 'John',
        last_name: 'Doe',
        email: 'john@example.com',
        is_active: true,
        is_superuser: false,
        group_ids: [10],
        last_login: '2026-08-30T12:00:00Z',
        date_joined: '2025-01-01T00:00:00Z',
      });

      const handler = new UsersHandler(mockClient);
      const routes = handler.routes();

      const successResult = await routes['mb_user_get']({ user_id: 7 });
      expect(successResult.content[0].text).toContain('ID: 7');
      expect(successResult.content[0].text).toContain('Name: John Doe');
      expect(successResult.content[0].text).toContain('Email: john@example.com');

      // Error case
      mockClient.request.mockRejectedValueOnce(new Error('User not found'));
      const errorResult = await routes['mb_user_get']({ user_id: 999 });
      expect(errorResult.content[0].text).toContain('❌ User get error: User not found');
    });

    test('mb_user_create, mb_user_update, mb_user_disable execute and handle errors', async () => {
      const handler = new UsersHandler(mockClient);
      const routes = handler.routes();

      // Create
      mockClient.request.mockResolvedValueOnce({ id: 8, email: 'new@example.com', first_name: 'New', last_name: 'User' });
      const createRes = await routes['mb_user_create']({ email: 'new@example.com', first_name: 'New', last_name: 'User', password: 'secretPassword1' });
      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/user', {
        email: 'new@example.com',
        first_name: 'New',
        last_name: 'User',
        password: 'secretPassword1',
      });
      expect(createRes.content[0].text).toContain('✅ User created successfully:');

      // Update
      mockClient.request.mockResolvedValueOnce({ id: 8, first_name: 'Updated' });
      const updateRes = await routes['mb_user_update']({ user_id: 8, first_name: 'Updated' });
      expect(mockClient.request).toHaveBeenCalledWith('PUT', '/api/user/8', { first_name: 'Updated' });
      expect(updateRes.content[0].text).toContain('✅ User 8 updated successfully');

      // Disable
      mockClient.request.mockResolvedValueOnce({});
      const disableRes = await routes['mb_user_disable']({ user_id: 8 });
      expect(mockClient.request).toHaveBeenCalledWith('DELETE', '/api/user/8');
      expect(disableRes.content[0].text).toContain('✅ User 8 has been disabled');

      // Error handling on create failure
      mockClient.request.mockRejectedValueOnce(new Error('Email already in use'));
      const errorCreate = await routes['mb_user_create']({ email: 'duplicate@example.com' });
      expect(errorCreate.content[0].text).toContain('❌ User create error: Email already in use');
    });

    test('permission group routes execute and handle membership removal correctly', async () => {
      const handler = new UsersHandler(mockClient);
      const routes = handler.routes();

      // Group List
      mockClient.request.mockResolvedValueOnce([{ id: 1, name: 'Administrators', member_count: 3 }]);
      const listRes = await routes['mb_permission_group_list']({});
      expect(listRes.content[0].text).toContain('[1] Administrators (3 members)');

      // Group Create
      mockClient.request.mockResolvedValueOnce({ id: 2, name: 'Analytics' });
      const createRes = await routes['mb_permission_group_create']({ name: 'Analytics' });
      expect(createRes.content[0].text).toContain('✅ Permission group created:\n  ID: 2\n  Name: Analytics');

      // Group Delete
      mockClient.request.mockResolvedValueOnce({});
      const deleteRes = await routes['mb_permission_group_delete']({ group_id: 2 });
      expect(deleteRes.content[0].text).toContain('✅ Permission group 2 deleted');

      // Group Add User
      mockClient.request.mockResolvedValueOnce({});
      const addRes = await routes['mb_permission_group_add_user']({ group_id: 1, user_id: 5 });
      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/permissions/membership', { group_id: 1, user_id: 5 });
      expect(addRes.content[0].text).toContain('✅ User 5 added to group 1');

      // Group Remove User (user present)
      mockClient.request
        .mockResolvedValueOnce({ members: [{ user_id: 5, membership_id: 105 }] })
        .mockResolvedValueOnce({});
      const removeRes = await routes['mb_permission_group_remove_user']({ group_id: 1, user_id: 5 });
      expect(mockClient.request).toHaveBeenCalledWith('DELETE', '/api/permissions/membership/105');
      expect(removeRes.content[0].text).toContain('✅ User 5 removed from group 1');

      // Group Remove User (user not found in group)
      mockClient.request.mockResolvedValueOnce({ members: [{ user_id: 99, membership_id: 199 }] });
      const notInGroupRes = await routes['mb_permission_group_remove_user']({ group_id: 1, user_id: 5 });
      expect(notInGroupRes.content[0].text).toContain('❌ User 5 is not in group 1');
    });
  });

  // =========================================================================
  // 3. ActionsHandler Tool Routing & Error Handling Verification
  // =========================================================================
  describe('ActionsHandler routing and error handling', () => {
    test('routes() exposes all 6 action, alert, and pulse tools', () => {
      const handler = new ActionsHandler(mockClient);
      const routes = handler.routes();
      const expectedTools = [
        'mb_action_create',
        'mb_action_list',
        'mb_action_execute',
        'mb_alert_create',
        'mb_alert_list',
        'mb_pulse_create',
      ];

      expect(Object.keys(routes).sort()).toEqual(expectedTools.sort());
      for (const toolName of expectedTools) {
        expect(typeof routes[toolName]).toBe('function');
      }
    });

    test('mb_action_create creates action and catches errors', async () => {
      const handler = new ActionsHandler(mockClient);
      const routes = handler.routes();

      mockClient.request.mockResolvedValueOnce({ id: 42, name: 'Update Customer Status', type: 'query' });
      const res = await routes['mb_action_create']({
        name: 'Update Customer Status',
        model_id: 10,
        database_id: 1,
        dataset_query: { type: 'native', native: { query: 'UPDATE customers SET status = ?' } },
        parameters: [{ id: 'status', type: 'text' }],
      });

      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/action', {
        name: 'Update Customer Status',
        description: '',
        model_id: 10,
        type: 'query',
        database_id: 1,
        dataset_query: { type: 'native', native: { query: 'UPDATE customers SET status = ?' } },
        parameters: [{ id: 'status', type: 'text' }],
        visualization_settings: {},
      });
      expect(res.content[0].text).toContain('✅ **Action Created!**');
      expect(res.content[0].text).toContain('🆔 Action ID: 42');

      // Error case
      mockClient.request.mockRejectedValueOnce(new Error('Invalid action definition'));
      const errRes = await routes['mb_action_create']({ name: 'Bad Action' });
      expect(errRes.content[0].text).toContain('❌ Action creation failed: Invalid action definition');
    });

    test('mb_action_list lists actions and handles empty results or errors', async () => {
      const handler = new ActionsHandler(mockClient);
      const routes = handler.routes();

      // With actions
      mockClient.request.mockResolvedValueOnce([
        { id: 1, name: 'Refund Order', type: 'query' },
        { id: 2, name: 'Send Email', type: 'http' },
      ]);
      const listRes = await routes['mb_action_list']({ model_id: 15 });
      expect(mockClient.request).toHaveBeenCalledWith('GET', '/api/action?model-id=15');
      expect(listRes.content[0].text).toContain('1. **Refund Order** (ID: 1)');
      expect(listRes.content[0].text).toContain('2. **Send Email** (ID: 2)');

      // Empty
      mockClient.request.mockResolvedValueOnce([]);
      const emptyRes = await routes['mb_action_list']({ model_id: 99 });
      expect(emptyRes.content[0].text).toContain('No actions found for this model.');

      // Error
      mockClient.request.mockRejectedValueOnce(new Error('Model not accessible'));
      const errRes = await routes['mb_action_list']({ model_id: -1 });
      expect(errRes.content[0].text).toContain('❌ Action list failed: Model not accessible');
    });

    test('mb_action_execute executes action with parameters and handles errors', async () => {
      const handler = new ActionsHandler(mockClient);
      const routes = handler.routes();

      mockClient.request.mockResolvedValueOnce({ rows_affected: 1, success: true });
      const execRes = await routes['mb_action_execute']({
        action_id: 42,
        parameters: { customer_id: 100, new_status: 'vip' },
      });
      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/action/42/execute', {
        parameters: { customer_id: 100, new_status: 'vip' },
      });
      expect(execRes.content[0].text).toContain('✅ **Action Executed!**');
      expect(execRes.content[0].text).toContain('📊 Result: {"rows_affected":1,"success":true}');

      // Error
      mockClient.request.mockRejectedValueOnce(new Error('Action execution timed out'));
      const errRes = await routes['mb_action_execute']({ action_id: 42, parameters: {} });
      expect(errRes.content[0].text).toContain('❌ Action execution failed: Action execution timed out');
    });

    test('mb_alert_create and mb_alert_list execute properly', async () => {
      const handler = new ActionsHandler(mockClient);
      const routes = handler.routes();

      // Create alert
      mockClient.request.mockResolvedValueOnce({ id: 501 });
      const createRes = await routes['mb_alert_create']({ card_id: 33, alert_condition: 'goal' });
      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/alert', {
        card: { id: 33 },
        alert_condition: 'goal',
        alert_first_only: false,
        alert_above_goal: undefined,
        channels: [{
          channel_type: 'email',
          enabled: true,
          recipients: [],
          schedule_type: 'hourly',
        }],
      });
      expect(createRes.content[0].text).toContain('✅ **Alert Created!**');
      expect(createRes.content[0].text).toContain('🆔 Alert ID: 501');

      // List alert by question
      mockClient.request.mockResolvedValueOnce([
        { id: 501, card: { id: 33, name: 'Daily Revenue' }, alert_condition: 'goal' },
      ]);
      const listQuestionRes = await routes['mb_alert_list']({ card_id: 33 });
      expect(mockClient.request).toHaveBeenCalledWith('GET', '/api/alert/question/33');
      expect(listQuestionRes.content[0].text).toContain('Alert ID: 501');
      expect(listQuestionRes.content[0].text).toContain('Card: Daily Revenue');

      // List all alerts
      mockClient.request.mockResolvedValueOnce([]);
      const listAllRes = await routes['mb_alert_list']({});
      expect(mockClient.request).toHaveBeenCalledWith('GET', '/api/alert');
      expect(listAllRes.content[0].text).toContain('No alerts found.');
    });

    test('mb_pulse_create executes properly and handles errors', async () => {
      const handler = new ActionsHandler(mockClient);
      const routes = handler.routes();

      mockClient.request.mockResolvedValueOnce({ id: 777, name: 'Weekly Digest' });
      const res = await routes['mb_pulse_create']({
        name: 'Weekly Digest',
        cards: [{ id: 1 }, { id: 2 }],
        channels: [{ channel_type: 'slack', details: { channel: '#reports' } }],
        collection_id: 5,
      });

      expect(mockClient.request).toHaveBeenCalledWith('POST', '/api/pulse', {
        name: 'Weekly Digest',
        cards: [{ id: 1 }, { id: 2 }],
        channels: [{ channel_type: 'slack', details: { channel: '#reports' } }],
        skip_if_empty: true,
        collection_id: 5,
      });
      expect(res.content[0].text).toContain('✅ **Scheduled Report (Pulse) Created!**');
      expect(res.content[0].text).toContain('🆔 Pulse ID: 777');

      // Error
      mockClient.request.mockRejectedValueOnce(new Error('Invalid pulse configuration'));
      const errRes = await routes['mb_pulse_create']({ name: 'Bad Pulse', cards: [], channels: [] });
      expect(errRes.content[0].text).toContain('❌ Pulse creation failed: Invalid pulse configuration');
    });
  });
});
