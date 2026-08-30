import { CollectionsHandler } from '../src/mcp/handlers/collections.js';
import { UsersHandler } from '../src/mcp/handlers/users.js';
import { ActionsHandler } from '../src/mcp/handlers/actions.js';
import { BaseHandler } from '../src/mcp/handlers/base.js';

let passed = 0;
let failed = 0;

function assert(condition, message) {
  if (!condition) {
    console.error(`❌ ASSERTION FAILED: ${message}`);
    failed++;
    throw new Error(message);
  } else {
    passed++;
  }
}

async function runEmpiricalHarness() {
  console.log('=== STARTING EMPIRICAL CHALLENGER STRESS HARNESS ===\n');

  // -------------------------------------------------------------
  // Test 1: BaseHandler inheritance & ensureInitialized contract
  // -------------------------------------------------------------
  console.log('Test 1: BaseHandler inheritance & lifecycle guarantees...');
  const base = new BaseHandler({ metabaseClient: { foo: 'bar' } });
  assert(base.metabaseClient.foo === 'bar', 'BaseHandler client injection');
  assert(typeof base.ensureInitialized === 'function', 'BaseHandler ensureInitialized is function');
  await base.ensureInitialized();
  assert(typeof base.routes === 'function', 'BaseHandler routes is function');

  const coll = new CollectionsHandler({});
  assert(coll instanceof BaseHandler, 'CollectionsHandler extends BaseHandler');
  await coll.ensureInitialized();

  const users = new UsersHandler({});
  assert(users instanceof BaseHandler, 'UsersHandler extends BaseHandler');
  await users.ensureInitialized();

  const actions = new ActionsHandler({});
  assert(actions instanceof BaseHandler, 'ActionsHandler extends BaseHandler');
  await actions.ensureInitialized();
  console.log('  -> PASSED: All handlers inherit BaseHandler and have safe ensureInitialized.');

  // -------------------------------------------------------------
  // Test 2: CollectionsHandler.handleCollectionCopy Deep Stress Test
  // -------------------------------------------------------------
  console.log('\nTest 2: CollectionsHandler.prototype.handleCollectionCopy REST logic...');
  const recordedCalls = [];
  const mockMetabaseClient = {
    request: async (method, endpoint, body) => {
      recordedCalls.push({ method, endpoint, body });
      if (endpoint === '/api/collection/100') {
        return { id: 100, name: 'Sales Pipeline', description: '2026 Q3 Pipeline', parent_id: 10 };
      }
      if (method === 'POST' && endpoint === '/api/collection') {
        return { id: 200, name: body.name, description: body.description, parent_id: body.parent_id };
      }
      if (endpoint === '/api/collection/100/items') {
        return {
          data: [
            { id: 1001, model: 'card' },
            { id: 1002, model: 'card' },
            { id: 2001, model: 'dashboard' },
            { id: 3001, model: 'snippet' }, // should be ignored
            { id: 4001, model: 'dataset' }, // should be ignored
          ]
        };
      }
      if (endpoint === '/api/card/1001') {
        return { id: 1001, name: 'Deals Closed', description: null, display: 'table', dataset_query: { type: 'native', native: { query: 'SELECT * FROM deals' } }, visualization_settings: {} };
      }
      if (endpoint === '/api/card/1002') {
        return { id: 1002, name: 'Deals Lost', description: 'Lost deals', display: 'bar', dataset_query: { type: 'query', query: {} }, visualization_settings: { 'graph.y': 'amount' } };
      }
      if (method === 'POST' && endpoint === '/api/card') {
        return { id: Math.floor(Math.random() * 1000) + 5000, name: body.name, collection_id: body.collection_id };
      }
      if (endpoint === '/api/dashboard/2001') {
        return { id: 2001, name: 'Executive Sales Dashboard', description: 'Dash desc', collection_id: 100 };
      }
      if (method === 'POST' && endpoint === '/api/dashboard') {
        return { id: 7001, name: body.name, collection_id: body.collection_id };
      }
      throw new Error(`Unhandled mock request: ${method} ${endpoint}`);
    }
  };

  const collHandler = new CollectionsHandler(mockMetabaseClient);
  const copyResult = await collHandler.handleCollectionCopy({
    collection_id: 100,
    destination_id: 50,
    new_name: 'Sales Pipeline (Archived Copy)',
  });

  assert(copyResult.content && copyResult.content[0].type === 'text', 'Result content structure valid');
  assert(copyResult.content[0].text.includes('New Collection ID: 200'), 'Includes new collection ID');
  assert(copyResult.content[0].text.includes('Name: Sales Pipeline (Archived Copy)'), 'Includes copied name');
  assert(copyResult.content[0].text.includes('Cards copied: 2'), 'Copied exactly 2 cards');
  assert(copyResult.content[0].text.includes('Dashboards copied: 1'), 'Copied exactly 1 dashboard');

  // Verify created collection payload
  const createColCall = recordedCalls.find(c => c.method === 'POST' && c.endpoint === '/api/collection');
  assert(createColCall && createColCall.body.parent_id === 50, 'Target collection created under parent_id 50');
  assert(createColCall.body.name === 'Sales Pipeline (Archived Copy)', 'Target collection named correctly');

  // Verify cards created with correct collection_id
  const createCardCalls = recordedCalls.filter(c => c.method === 'POST' && c.endpoint === '/api/card');
  assert(createCardCalls.length === 2, 'Exactly 2 card POST calls made');
  assert(createCardCalls[0].body.collection_id === 200, 'Card 1 placed in new collection');
  assert(createCardCalls[0].body.name === 'Copy of Deals Closed', 'Card 1 named "Copy of Deals Closed"');
  assert(createCardCalls[1].body.collection_id === 200, 'Card 2 placed in new collection');
  assert(createCardCalls[1].body.name === 'Copy of Deals Lost', 'Card 2 named "Copy of Deals Lost"');

  // Verify dashboard created with correct collection_id
  const createDashCalls = recordedCalls.filter(c => c.method === 'POST' && c.endpoint === '/api/dashboard');
  assert(createDashCalls.length === 1, 'Exactly 1 dashboard POST call made');
  assert(createDashCalls[0].body.collection_id === 200, 'Dashboard placed in new collection');
  assert(createDashCalls[0].body.name === 'Copy of Executive Sales Dashboard', 'Dashboard named correctly');

  console.log('  -> PASSED: handleCollectionCopy REST API logic works flawlessly.');

  // -------------------------------------------------------------
  // Test 3: UsersHandler Complete Tool Routing & Error Handling
  // -------------------------------------------------------------
  console.log('\nTest 3: UsersHandler routing and error boundaries...');
  const userClient = {
    request: async (method, endpoint, body) => {
      if (endpoint === '/api/user' && method === 'GET') {
        return [
          { id: 1, email: 'usr1@example.com', first_name: 'John', last_name: 'Doe', is_active: true, is_superuser: true, group_ids: [1, 2] },
          { id: 2, email: 'usr2@example.com', first_name: null, last_name: null, is_active: false, is_superuser: false, group_ids: [1] }
        ];
      }
      if (endpoint === '/api/user/1' && method === 'GET') {
        return { id: 1, email: 'usr1@example.com', first_name: 'John', last_name: 'Doe', is_active: true, is_superuser: true, group_ids: [1, 2], date_joined: '2026-01-01' };
      }
      if (endpoint === '/api/user' && method === 'POST') {
        return { id: 3, ...body };
      }
      if (endpoint === '/api/user/3' && method === 'PUT') {
        return { id: 3, ...body };
      }
      if (endpoint === '/api/user/3' && method === 'DELETE') {
        return {};
      }
      if (endpoint === '/api/permissions/group' && method === 'GET') {
        return [{ id: 1, name: 'All Users', member_count: 5 }];
      }
      if (endpoint === '/api/permissions/group' && method === 'POST') {
        return { id: 2, name: body.name };
      }
      if (endpoint === '/api/permissions/group/2' && method === 'DELETE') {
        return {};
      }
      if (endpoint === '/api/permissions/membership' && method === 'POST') {
        return { id: 99, ...body };
      }
      if (endpoint === '/api/permissions/group/1' && method === 'GET') {
        return { members: [{ user_id: 1, membership_id: 77 }] };
      }
      if (endpoint === '/api/permissions/membership/77' && method === 'DELETE') {
        return {};
      }
      throw new Error(`Not found: ${method} ${endpoint}`);
    }
  };

  const usersHandler = new UsersHandler(userClient);
  const uRoutes = usersHandler.routes();
  const requiredUserTools = [
    'mb_user_list', 'mb_user_get', 'mb_user_create', 'mb_user_update', 'mb_user_disable',
    'mb_permission_group_list', 'mb_permission_group_create', 'mb_permission_group_delete',
    'mb_permission_group_add_user', 'mb_permission_group_remove_user'
  ];
  for (const t of requiredUserTools) {
    assert(typeof uRoutes[t] === 'function', `UsersHandler routes.${t} is function`);
  }

  // Test mb_user_list
  const uListRes = await uRoutes['mb_user_list']({});
  assert(uListRes.structuredContent.count === 2, 'User list returned 2 users');
  assert(uListRes.structuredContent.users[1].first_name === null, 'Nullable first_name mapped to null');
  assert(uListRes.structuredContent.users[1].last_name === null, 'Nullable last_name mapped to null');

  // Test mb_user_get
  const uGetRes = await uRoutes['mb_user_get']({ user_id: 1 });
  assert(uGetRes.content[0].text.includes('usr1@example.com'), 'User get returned user email');

  // Test mb_user_create
  const uCreateRes = await uRoutes['mb_user_create']({ email: 'usr3@example.com', first_name: 'Sam', last_name: 'Alt' });
  assert(uCreateRes.content[0].text.includes('User created successfully'), 'User create returned success');

  // Test mb_user_update
  const uUpdateRes = await uRoutes['mb_user_update']({ user_id: 3, first_name: 'Samuel' });
  assert(uUpdateRes.content[0].text.includes('User 3 updated successfully'), 'User update returned success');

  // Test mb_user_disable
  const uDisableRes = await uRoutes['mb_user_disable']({ user_id: 3 });
  assert(uDisableRes.content[0].text.includes('User 3 has been disabled'), 'User disable returned success');

  // Test permission groups
  const gListRes = await uRoutes['mb_permission_group_list']({});
  assert(gListRes.content[0].text.includes('All Users'), 'Group list returned group name');

  const gCreateRes = await uRoutes['mb_permission_group_create']({ name: 'Managers' });
  assert(gCreateRes.content[0].text.includes('Permission group created'), 'Group create returned success');

  const gDeleteRes = await uRoutes['mb_permission_group_delete']({ group_id: 2 });
  assert(gDeleteRes.content[0].text.includes('Permission group 2 deleted'), 'Group delete returned success');

  const gAddUserRes = await uRoutes['mb_permission_group_add_user']({ group_id: 1, user_id: 3 });
  assert(gAddUserRes.content[0].text.includes('User 3 added to group 1'), 'Add user to group success');

  const gRemoveUserRes = await uRoutes['mb_permission_group_remove_user']({ group_id: 1, user_id: 1 });
  assert(gRemoveUserRes.content[0].text.includes('User 1 removed from group 1'), 'Remove user from group success');

  // Test error handling when user not found in group
  const gRemoveUserNotFound = await uRoutes['mb_permission_group_remove_user']({ group_id: 1, user_id: 999 });
  assert(gRemoveUserNotFound.content[0].text.includes('User 999 is not in group 1'), 'Handles user not in group error gracefully');

  // Test error handling when client throws
  const failingUserClient = { request: async () => { throw new Error('Metabase 500 DB error'); } };
  const failingUsersHandler = new UsersHandler(failingUserClient);
  const failList = await failingUsersHandler.routes()['mb_user_list']({});
  assert(failList.content[0].text.includes('❌ User list error: Metabase 500 DB error'), 'Catches and formats user list error');
  const failGet = await failingUsersHandler.routes()['mb_user_get']({ user_id: 1 });
  assert(failGet.content[0].text.includes('❌ User get error: Metabase 500 DB error'), 'Catches and formats user get error');

  console.log('  -> PASSED: All 10 UsersHandler routes execute and handle errors cleanly.');

  // -------------------------------------------------------------
  // Test 4: ActionsHandler Complete Tool Routing & Error Handling
  // -------------------------------------------------------------
  console.log('\nTest 4: ActionsHandler routing and error boundaries...');
  const actionClient = {
    request: async (method, endpoint, body) => {
      if (endpoint === '/api/action' && method === 'POST') {
        return { id: 101, name: body.name, type: body.type };
      }
      if (endpoint === '/api/action?model-id=10' && method === 'GET') {
        return [{ id: 101, name: 'Archive Customer', type: 'query' }];
      }
      if (endpoint === '/api/action/101/execute' && method === 'POST') {
        return { rows_affected: 1, ok: true };
      }
      if (endpoint === '/api/alert' && method === 'POST') {
        return { id: 201 };
      }
      if (endpoint === '/api/alert/question/5' && method === 'GET') {
        return [{ id: 201, card: { id: 5, name: 'Daily Signups' }, alert_condition: 'rows' }];
      }
      if (endpoint === '/api/alert' && method === 'GET') {
        return [];
      }
      if (endpoint === '/api/pulse' && method === 'POST') {
        return { id: 301, name: body.name };
      }
      throw new Error(`Not found: ${method} ${endpoint}`);
    }
  };

  const actionsHandler = new ActionsHandler(actionClient);
  const aRoutes = actionsHandler.routes();
  const requiredActionTools = [
    'mb_action_create', 'mb_action_list', 'mb_action_execute',
    'mb_alert_create', 'mb_alert_list', 'mb_pulse_create'
  ];
  for (const t of requiredActionTools) {
    assert(typeof aRoutes[t] === 'function', `ActionsHandler routes.${t} is function`);
  }

  // Test mb_action_create
  const aCreateRes = await aRoutes['mb_action_create']({ name: 'Archive Customer', model_id: 10 });
  assert(aCreateRes.content[0].text.includes('Action Created!'), 'Action create success');

  // Test mb_action_list
  const aListRes = await aRoutes['mb_action_list']({ model_id: 10 });
  assert(aListRes.content[0].text.includes('Archive Customer'), 'Action list contains item');

  // Test mb_action_execute
  const aExecRes = await aRoutes['mb_action_execute']({ action_id: 101, parameters: { id: 5 } });
  assert(aExecRes.content[0].text.includes('Action Executed!'), 'Action execute success');

  // Test mb_alert_create
  const alertCreateRes = await aRoutes['mb_alert_create']({ card_id: 5 });
  assert(alertCreateRes.content[0].text.includes('Alert Created!'), 'Alert create success');

  // Test mb_alert_list with card_id
  const alertListCardRes = await aRoutes['mb_alert_list']({ card_id: 5 });
  assert(alertListCardRes.content[0].text.includes('Daily Signups'), 'Alert list contains card');

  // Test mb_alert_list without card_id (all)
  const alertListAllRes = await aRoutes['mb_alert_list']({});
  assert(alertListAllRes.content[0].text.includes('No alerts found'), 'Empty alert list handled');

  // Test mb_pulse_create
  const pulseCreateRes = await aRoutes['mb_pulse_create']({ name: 'Daily Digest', cards: [{ id: 1 }], channels: [{ channel_type: 'email' }] });
  assert(pulseCreateRes.content[0].text.includes('Scheduled Report (Pulse) Created!'), 'Pulse create success');

  // Test error handling
  const failingActionClient = { request: async () => { throw new Error('Internal Metabase Crash'); } };
  const failingActionsHandler = new ActionsHandler(failingActionClient);
  const failActCreate = await failingActionsHandler.routes()['mb_action_create']({ name: 'X' });
  assert(failActCreate.content[0].text.includes('❌ Action creation failed: Internal Metabase Crash'), 'Action create error caught');
  const failActList = await failingActionsHandler.routes()['mb_action_list']({ model_id: 1 });
  assert(failActList.content[0].text.includes('❌ Action list failed: Internal Metabase Crash'), 'Action list error caught');
  const failActExec = await failingActionsHandler.routes()['mb_action_execute']({ action_id: 1, parameters: {} });
  assert(failActExec.content[0].text.includes('❌ Action execution failed: Internal Metabase Crash'), 'Action exec error caught');
  const failAlertCreate = await failingActionsHandler.routes()['mb_alert_create']({ card_id: 1 });
  assert(failAlertCreate.content[0].text.includes('❌ Alert creation failed: Internal Metabase Crash'), 'Alert create error caught');
  const failAlertList = await failingActionsHandler.routes()['mb_alert_list']({});
  assert(failAlertList.content[0].text.includes('❌ Alert list failed: Internal Metabase Crash'), 'Alert list error caught');
  const failPulseCreate = await failingActionsHandler.routes()['mb_pulse_create']({ name: 'P', cards: [], channels: [] });
  assert(failPulseCreate.content[0].text.includes('❌ Pulse creation failed: Internal Metabase Crash'), 'Pulse create error caught');

  console.log('  -> PASSED: All 6 ActionsHandler routes execute and handle errors cleanly.');

  console.log(`\n======================================================`);
  console.log(`SUMMARY: ${passed} assertions passed, ${failed} failed.`);
  console.log(`VERDICT: 100% EMPIRICAL SUCCESS`);
  console.log(`======================================================\n`);
}

runEmpiricalHarness().catch(err => {
  console.error('Fatal harness error:', err);
  process.exit(1);
});
