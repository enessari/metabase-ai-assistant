import { BaseHandler } from './base.js';

export class CollectionsHandler extends BaseHandler {
  constructor(contextOrClient) {
    super(contextOrClient);
  }

  routes() {
    return {
      'mb_collection_create': (args) => this.handleCollectionCreate(args),
      'mb_collection_list': (args) => this.handleCollectionList(args),
      'mb_collection_move': (args) => this.handleCollectionMove(args),
      'mb_collection_copy': (args) => this.handleCollectionCopy(args),
      'mb_collection_permissions_get': (args) => this.handleCollectionPermissionsGet(args),
      'mb_collection_permissions_update': (args) => this.handleCollectionPermissionsUpdate(args),
    };
  }

  async handleCollectionCreate(args) {
    try {
      const collectionData = {
        name: args.name,
        description: args.description || '',
        parent_id: args.parent_id || null,
        color: args.color || '#509EE3'
      };

      const collection = await this.metabaseClient.request('POST', '/api/collection', collectionData);

      return {
        content: [{
          type: 'text',
          text: `✅ **Collection Created!**\n\n` +
            `🆔 Collection ID: ${collection.id}\n` +
            `📁 Name: ${collection.name}\n` +
            `📝 Description: ${collection.description || 'None'}\n` +
            `🎨 Color: ${collection.color}\n` +
            `📂 Parent: ${args.parent_id || 'Root'}`
        }]
      };

    } catch (error) {
      // Better error messages for common issues
      let userMessage = error.message;

      if (error.message.includes('already exists') || error.response?.status === 409) {
        userMessage = `Collection already exists with this name: "${args.name}"`;
      } else if (error.message.includes('permission') || error.response?.status === 403) {
        userMessage = `Permission denied. Contact admin for collection creation access.`;
      } else if (error.message.includes('parent') || (error.message.includes('not found') && args.parent_id)) {
        userMessage = `Parent collection not found: ID ${args.parent_id}`;
      }

      return {
        content: [{ type: 'text', text: `❌ Collection creation failed: ${userMessage}` }]
      };
    }
  }

  async handleCollectionList(args = {}) {
    try {
      const parentId = args?.parent_id;

      if (parentId !== undefined && parentId !== null && parentId !== '') {
        const endpoint = `/api/collection/${parentId}/items`;
        const response = await this.metabaseClient.request('GET', endpoint);
        const rawItems = Array.isArray(response) ? response : (response.data || []);

        let output = `📂 **Collection Items (Parent: ${parentId})**\n\n`;
        if (rawItems.length === 0) {
          output += 'No items found in this collection.\n';
        } else {
          rawItems.slice(0, 50).forEach((item, i) => {
            const modelTag = item.model ? `[${item.model}] ` : '';
            output += `${i + 1}. ${modelTag}**${item.name}** (ID: ${item.id})\n`;
            if (item.description) output += `   ${item.description.substring(0, 50)}...\n`;
          });
        }
        output += `\n📊 Total Items: ${rawItems.length}`;

        return {
          content: [{ type: 'text', text: output }],
          structuredContent: {
            parent_id: parentId,
            items: rawItems.map(item => ({
              id: item.id,
              name: item.name,
              model: item.model || 'unknown',
              description: item.description ?? null,
            })),
            collections: rawItems.filter(item => item.model === 'collection').map(c => ({ id: c.id, name: c.name })),
            count: rawItems.length,
          },
        };
      }

      const collections = await this.metabaseClient.request('GET', '/api/collection');
      const collList = Array.isArray(collections) ? collections : (collections.data || []);
      let output = `📂 **Collections**\n\n`;

      const rootCollections = collList.filter(c => !c.personal_owner_id);
      rootCollections.slice(0, 20).forEach((col, i) => {
        output += `${i + 1}. **${col.name}** (ID: ${col.id})\n`;
        if (col.description) output += `   ${col.description.substring(0, 50)}...\n`;
      });

      output += `\n📊 Total Collections: ${collList.length}`;

      return {
        content: [{ type: 'text', text: output }],
        structuredContent: {
          collections: rootCollections.map(c => ({ id: c.id, name: c.name })),
          count: collList.length,
        },
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Collection list failed: ${error.message}` }]
      };
    }
  }

  async handleCollectionMove(args) {
    try {
      let endpoint;
      const updateData = { collection_id: args.target_collection_id };

      switch (args.item_type) {
        case 'card':
          endpoint = `/api/card/${args.item_id}`;
          break;
        case 'dashboard':
          endpoint = `/api/dashboard/${args.item_id}`;
          break;
        case 'collection':
          endpoint = `/api/collection/${args.item_id}`;
          updateData.parent_id = args.target_collection_id;
          delete updateData.collection_id;
          break;
        default:
          throw new Error(`Unknown item type: ${args.item_type}`);
      }

      await this.metabaseClient.request('PUT', endpoint, updateData);

      return {
        content: [{
          type: 'text',
          text: `✅ **Item Moved!**\n\n` +
            `📦 Type: ${args.item_type}\n` +
            `🆔 Item ID: ${args.item_id}\n` +
            `📂 Target Collection: ${args.target_collection_id || 'Root'}`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Move failed: ${error.message}` }]
      };
    }
  }

  async handleCollectionPermissionsGet(args) {
    const { collection_id } = args;

    try {
      const graph = await this.metabaseClient.request('GET', '/api/collection/graph');
      const collectionPerms = graph.groups;

      const permissions = [];
      for (const [groupId, perms] of Object.entries(collectionPerms)) {
        const collPerm = perms[collection_id];
        if (collPerm) {
          permissions.push({ group_id: groupId, permission: collPerm });
        }
      }

      return {
        content: [{
          type: 'text',
          text: `Collection ${collection_id} permissions:\n${permissions.map(p =>
            `  - Group ${p.group_id}: ${p.permission}`
          ).join('\n') || '  No specific permissions set'}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Collection permissions get error: ${error.message}` }] };
    }
  }

  async handleCollectionPermissionsUpdate(args) {
    const { collection_id, group_id, permission } = args;

    try {
      // Get current graph
      const graph = await this.metabaseClient.request('GET', '/api/collection/graph');

      // Update the permission
      if (!graph.groups[group_id]) {
        graph.groups[group_id] = {};
      }
      graph.groups[group_id][collection_id] = permission;

      // Save the updated graph
      await this.metabaseClient.request('PUT', '/api/collection/graph', graph);

      return {
        content: [{
          type: 'text',
          text: `✅ Collection ${collection_id} permission updated: Group ${group_id} = ${permission}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Collection permissions update error: ${error.message}` }] };
    }
  }

  async handleCollectionCopy(args) {
    const { collection_id, destination_id, new_name } = args;

    try {
      // Get source collection
      const sourceCollection = await this.metabaseClient.request('GET', `/api/collection/${collection_id}`);

      // Create new collection
      const newCollection = await this.metabaseClient.request('POST', '/api/collection', {
        name: new_name || `Copy of ${sourceCollection.name}`,
        description: sourceCollection.description,
        parent_id: destination_id || sourceCollection.parent_id
      });

      // Get items in source collection
      const items = await this.metabaseClient.request('GET', `/api/collection/${collection_id}/items`);
      const allItems = items.data || items;

      let copiedCards = 0;
      let copiedDashboards = 0;

      // Copy each item
      for (const item of allItems) {
        if (item.model === 'card') {
          const sourceCard = await this.metabaseClient.request('GET', `/api/card/${item.id}`);
          await this.metabaseClient.request('POST', '/api/card', {
            name: `Copy of ${sourceCard.name}`,
            description: sourceCard.description,
            display: sourceCard.display,
            dataset_query: sourceCard.dataset_query,
            visualization_settings: sourceCard.visualization_settings,
            collection_id: newCollection.id
          });
          copiedCards++;
        } else if (item.model === 'dashboard') {
          const sourceDash = await this.metabaseClient.request('GET', `/api/dashboard/${item.id}`);
          await this.metabaseClient.request('POST', '/api/dashboard', {
            name: `Copy of ${sourceDash.name}`,
            description: sourceDash.description,
            collection_id: newCollection.id
          });
          copiedDashboards++;
        }
      }

      return {
        content: [{
          type: 'text',
          text: `✅ Collection copied:\n  New Collection ID: ${newCollection.id}\n  Name: ${newCollection.name}\n  Cards copied: ${copiedCards}\n  Dashboards copied: ${copiedDashboards}`
        }]
      };
    } catch (error) {
      return { content: [{ type: 'text', text: `❌ Collection copy error: ${error.message}` }] };
    }
  }
}
