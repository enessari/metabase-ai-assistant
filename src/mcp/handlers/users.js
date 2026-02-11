import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';

export class UsersHandler {
  constructor(metabaseClient) {
    this.metabaseClient = metabaseClient;
  }

  routes() {
    return {
      'mb_user_list': (args) => this.handleUserList(args),
      'mb_user_get': (args) => this.handleUserGet(args),
      'mb_user_create': (args) => this.handleUserCreate(args),
      'mb_user_update': (args) => this.handleUserUpdate(args),
      'mb_user_disable': (args) => this.handleUserDisable(args),
      'mb_permission_group_list': (args) => this.handlePermissionGroupList(args),
      'mb_permission_group_create': (args) => this.handlePermissionGroupCreate(args),
      'mb_permission_group_delete': (args) => this.handlePermissionGroupDelete(args),
      'mb_permission_group_add_user': (args) => this.handlePermissionGroupAddUser(args),
      'mb_permission_group_remove_user': (args) => this.handlePermissionGroupRemoveUser(args),
    };
  }

  async handleUserList(args) {
    await this.ensureInitialized();
    const { status = 'all', group_id } = args;

    try {
      const response = await this.metabaseClient.request('GET', '/api/user');
      let users = response.data || response;

      // Filter by status
      if (status === 'active') {
        users = users.filter(u => u.is_active);
      } else if (status === 'inactive') {
        users = users.filter(u => !u.is_active);
      }

      // Filter by group
      if (group_id) {
        users = users.filter(u => u.group_ids && u.group_ids.includes(group_id));
      }

      return {
        content: [{
          type: 'text',
          text: `Found ${users.length} users:\n${users.map(u =>
            `  - [${u.id}] ${u.first_name} ${u.last_name} (${u.email}) - ${u.is_active ? 'Active' : 'Inactive'}${u.is_superuser ? ' [Admin]' : ''}`
          ).join('\n')}`
        }],
        structuredContent: {
          users: users.map(u => ({
            id: u.id, email: u.email,
            first_name: u.first_name, last_name: u.last_name,
            is_active: u.is_active,
          })),
          count: users.length,
        },
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ User list error: ${error.message}` }] };
    }
  }

  async handleUserGet(args) {
    await this.ensureInitialized();
    const { user_id } = args;

    try {
      const user = await this.metabaseClient.request('GET', `/api/user/${user_id}`);

      return {
        content: [{
          type: 'text',
          text: `User Details:\n` +
            `  ID: ${user.id}\n` +
            `  Name: ${user.first_name} ${user.last_name}\n` +
            `  Email: ${user.email}\n` +
            `  Active: ${user.is_active}\n` +
            `  Superuser: ${user.is_superuser}\n` +
            `  Groups: ${(user.group_ids || []).join(', ')}\n` +
            `  Last Login: ${user.last_login || 'Never'}\n` +
            `  Created: ${user.date_joined}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ User get error: ${error.message}` }] };
    }
  }

  async handleUserCreate(args) {
    await this.ensureInitialized();
    const { email, first_name, last_name, password, group_ids } = args;

    try {
      const userData = {
        email,
        first_name,
        last_name,
        ...(password && { password }),
        ...(group_ids && { group_ids })
      };

      const user = await this.metabaseClient.request('POST', '/api/user', userData);

      return {
        content: [{
          type: 'text',
          text: `✅ User created successfully:\n` +
            `  ID: ${user.id}\n` +
            `  Name: ${user.first_name} ${user.last_name}\n` +
            `  Email: ${user.email}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ User create error: ${error.message}` }] };
    }
  }

  async handleUserUpdate(args) {
    await this.ensureInitialized();
    const { user_id, ...updates } = args;

    try {
      const user = await this.metabaseClient.request('PUT', `/api/user/${user_id}`, updates);

      return {
        content: [{
          type: 'text',
          text: `✅ User ${user_id} updated successfully`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ User update error: ${error.message}` }] };
    }
  }

  async handleUserDisable(args) {
    await this.ensureInitialized();
    const { user_id } = args;

    try {
      await this.metabaseClient.request('DELETE', `/api/user/${user_id}`);

      return {
        content: [{
          type: 'text',
          text: `✅ User ${user_id} has been disabled`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ User disable error: ${error.message}` }] };
    }
  }

  // ==================== PERMISSION GROUP HANDLERS ====================

  async handlePermissionGroupList(args) {
    await this.ensureInitialized();

    try {
      const groups = await this.metabaseClient.request('GET', '/api/permissions/group');

      return {
        content: [{
          type: 'text',
          text: `Found ${groups.length} permission groups:\n${groups.map(g =>
            `  - [${g.id}] ${g.name} (${g.member_count || 0} members)`
          ).join('\n')}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Permission group list error: ${error.message}` }] };
    }
  }

  async handlePermissionGroupCreate(args) {
    await this.ensureInitialized();
    const { name } = args;

    try {
      const group = await this.metabaseClient.request('POST', '/api/permissions/group', { name });

      return {
        content: [{
          type: 'text',
          text: `✅ Permission group created:\n  ID: ${group.id}\n  Name: ${group.name}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Permission group create error: ${error.message}` }] };
    }
  }

  async handlePermissionGroupDelete(args) {
    await this.ensureInitialized();
    const { group_id } = args;

    try {
      await this.metabaseClient.request('DELETE', `/api/permissions/group/${group_id}`);

      return {
        content: [{
          type: 'text',
          text: `✅ Permission group ${group_id} deleted`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Permission group delete error: ${error.message}` }] };
    }
  }

  async handlePermissionGroupAddUser(args) {
    await this.ensureInitialized();
    const { group_id, user_id } = args;

    try {
      await this.metabaseClient.request('POST', '/api/permissions/membership', {
        group_id,
        user_id
      });

      return {
        content: [{
          type: 'text',
          text: `✅ User ${user_id} added to group ${group_id}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Add user to group error: ${error.message}` }] };
    }
  }

  async handlePermissionGroupRemoveUser(args) {
    await this.ensureInitialized();
    const { group_id, user_id } = args;

    try {
      // First get the membership ID
      const memberships = await this.metabaseClient.request('GET', `/api/permissions/group/${group_id}`);
      const membership = memberships.members?.find(m => m.user_id === user_id);

      if (!membership) {
        return { content: [{ type: 'text', text: `❌ User ${user_id} is not in group ${group_id}` }] };
      }

      await this.metabaseClient.request('DELETE', `/api/permissions/membership/${membership.membership_id}`);

      return {
        content: [{
          type: 'text',
          text: `✅ User ${user_id} removed from group ${group_id}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Remove user from group error: ${error.message}` }] };
    }
  }
}
