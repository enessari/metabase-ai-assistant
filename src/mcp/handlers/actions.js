import { BaseHandler } from './base.js';

export class ActionsHandler extends BaseHandler {
    constructor(contextOrClient) {
        super(contextOrClient);
    }

    routes() {
        return {
            'mb_action_create': (args) => this.handleActionCreate(args),
            'mb_action_list': (args) => this.handleActionList(args),
            'mb_action_execute': (args) => this.handleActionExecute(args),
            'mb_alert_create': (args) => this.handleAlertCreate(args),
            'mb_alert_list': (args) => this.handleAlertList(args),
            'mb_pulse_create': (args) => this.handlePulseCreate(args),
        };
    }

  async handleActionCreate(args) {
    try {
      const actionData = {
        name: args.name,
        description: args.description || '',
        model_id: args.model_id,
        type: args.type || 'query',
        database_id: args.database_id,
        dataset_query: args.dataset_query,
        parameters: args.parameters || [],
        visualization_settings: args.visualization_settings || {}
      };

      const action = await this.metabaseClient.request('POST', '/api/action', actionData);

      return {
        content: [{
          type: 'text',
          text: `✅ **Action Created!**\n\n` +
            `🆔 Action ID: ${action.id}\n` +
            `📋 Name: ${action.name}\n` +
            `⚙️ Type: ${action.type}\n` +
            `📊 Model ID: ${args.model_id}\n` +
            `🔧 Parameters: ${(args.parameters || []).length}`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Action creation failed: ${error.message}` }]
      };
    }
  }

  async handleActionList(args) {
    try {
      const actions = await this.metabaseClient.request('GET', `/api/action?model-id=${args.model_id}`);

      let output = `📋 **Actions for Model ${args.model_id}**\n\n`;

      if (actions.length === 0) {
        output += 'No actions found for this model.';
      } else {
        actions.forEach((action, i) => {
          output += `${i + 1}. **${action.name}** (ID: ${action.id})\n`;
          output += `   Type: ${action.type}\n`;
        });
      }

      return {
        content: [{ type: 'text', text: output }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Action list failed: ${error.message}` }]
      };
    }
  }

  async handleActionExecute(args) {
    try {
      const result = await this.metabaseClient.request('POST', `/api/action/${args.action_id}/execute`, {
        parameters: args.parameters
      });

      return {
        content: [{
          type: 'text',
          text: `✅ **Action Executed!**\n\n` +
            `🆔 Action ID: ${args.action_id}\n` +
            `📋 Parameters: ${JSON.stringify(args.parameters)}\n` +
            `📊 Result: ${JSON.stringify(result)}`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Action execution failed: ${error.message}` }]
      };
    }
  }

  // === ALERTS & NOTIFICATIONS HANDLERS ===

  async handleAlertCreate(args) {
    try {
      const alertData = {
        card: { id: args.card_id },
        alert_condition: args.alert_condition || 'rows',
        alert_first_only: args.alert_first_only || false,
        alert_above_goal: args.alert_above_goal,
        channels: args.channels || [{
          channel_type: 'email',
          enabled: true,
          recipients: [],
          schedule_type: 'hourly'
        }]
      };

      const alert = await this.metabaseClient.request('POST', '/api/alert', alertData);

      return {
        content: [{
          type: 'text',
          text: `✅ **Alert Created!**\n\n` +
            `🆔 Alert ID: ${alert.id}\n` +
            `🔔 Card ID: ${args.card_id}\n` +
            `⚙️ Condition: ${args.alert_condition || 'rows'}\n` +
            `📧 Channels: ${(args.channels || []).length}`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Alert creation failed: ${error.message}` }]
      };
    }
  }

  async handleAlertList(args) {
    try {
      let endpoint = '/api/alert';
      if (args.card_id) {
        endpoint = `/api/alert/question/${args.card_id}`;
      }

      const alerts = await this.metabaseClient.request('GET', endpoint);

      let output = `🔔 **Alerts**\n\n`;

      if (alerts.length === 0) {
        output += 'No alerts found.';
      } else {
        alerts.forEach((alert, i) => {
          output += `${i + 1}. Alert ID: ${alert.id}\n`;
          output += `   Card: ${alert.card?.name || alert.card?.id}\n`;
          output += `   Condition: ${alert.alert_condition}\n\n`;
        });
      }

      return {
        content: [{ type: 'text', text: output }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Alert list failed: ${error.message}` }]
      };
    }
  }

  async handlePulseCreate(args) {
    try {
      const pulseData = {
        name: args.name,
        cards: args.cards,
        channels: args.channels,
        skip_if_empty: args.skip_if_empty !== false,
        collection_id: args.collection_id
      };

      const pulse = await this.metabaseClient.request('POST', '/api/pulse', pulseData);

      return {
        content: [{
          type: 'text',
          text: `✅ **Scheduled Report (Pulse) Created!**\n\n` +
            `🆔 Pulse ID: ${pulse.id}\n` +
            `📋 Name: ${pulse.name}\n` +
            `📊 Cards: ${args.cards.length}\n` +
            `📧 Channels: ${args.channels.length}`
        }]
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Pulse creation failed: ${error.message}` }]
      };
    }
  }
}
