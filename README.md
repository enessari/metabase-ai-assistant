# Metabase AI Assistant

[![Apache 2.0 License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Node.js](https://img.shields.io/badge/Node.js-18+-brightgreen.svg)](https://nodejs.org/)
[![MCP Compatible](https://img.shields.io/badge/MCP-Compatible-blue.svg)](https://modelcontextprotocol.io/)

**The most comprehensive MCP server for Metabase with 107 tools for AI-powered business intelligence.**

An AI-powered Model Context Protocol (MCP) server that connects to Metabase and PostgreSQL databases. Generate SQL queries from natural language, create dashboards, manage users, and automate BI workflows with LLM integration.

> Developed by **Abdullah Enes SARI** for **ONMARTECH LLC**

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
- [MCP Integration](#mcp-integration)
- [Available Tools](#available-tools)
- [Usage Examples](#usage-examples)
- [Security](#security)
- [Deployment](#deployment)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

---

## Features

### MCP Server Integration
- Native Model Context Protocol (MCP) server implementation
- Compatible with AI assistants that support MCP
- 107 tools for comprehensive Metabase control
- Direct PostgreSQL database connections
- Hybrid connection management (API + Direct DB)

### AI-Powered Capabilities
- Natural language to SQL query generation
- SQL query optimization with performance suggestions
- Query explanation in plain English
- Auto-describe database schemas
- Relationship detection between tables

### Database Operations (25 tools)
- List databases, schemas, and tables
- Execute SQL queries with timeout control
- Create views, materialized views, and indexes
- VACUUM, ANALYZE, and EXPLAIN operations
- Table statistics and index usage analysis
- Safe DDL with prefix protection

### Question and Card Management (12 tools)
- Create, update, delete, and archive questions
- Parametric questions with dynamic filters
- Get card data in JSON format
- Copy and clone cards between collections

### Dashboard Management (14 tools)
- Create and manage dashboards
- Add/remove cards with positioning
- Dashboard filters and parameters
- Executive dashboard templates
- Layout optimization

### User and Permission Management (10 tools)
- User CRUD operations
- Permission group management
- Collection permissions
- Role-based access control

### Additional Features
- Metric and segment creation
- Alert and pulse management
- Bookmark management
- Global search across Metabase
- Activity logging and analytics
- Metabase documentation search

---

## Installation

### Requirements

- Node.js 18 or higher
- Metabase instance (v0.48+)
- PostgreSQL database (for direct connections)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/onmartech/metabase-ai-assistant.git
cd metabase-ai-assistant

# Install dependencies
npm install

# Create environment file
cp .env.example .env

# Edit .env with your credentials
# Then start the MCP server
npm run mcp
```

---

## Configuration

Create a `.env` file with the following variables:

```env
# Metabase Connection
METABASE_URL=http://your-metabase-instance.com
METABASE_USERNAME=your_username
METABASE_PASSWORD=your_password
METABASE_API_KEY=your_api_key

# AI Provider (optional, for AI features)
ANTHROPIC_API_KEY=your_anthropic_key
OPENAI_API_KEY=your_openai_key

# Application Settings
LOG_LEVEL=info
NODE_ENV=production
```

**Security Note**: Never commit the `.env` file to version control.

---

## MCP Integration

### Configuration for AI Assistants

Add the following to your MCP client configuration:

```json
{
  "mcpServers": {
    "metabase": {
      "command": "node",
      "args": ["/path/to/metabase-ai-assistant/src/mcp/server.js"],
      "env": {
        "METABASE_URL": "http://your-metabase-instance.com",
        "METABASE_USERNAME": "your_username",
        "METABASE_PASSWORD": "your_password",
        "METABASE_API_KEY": "your_api_key"
      }
    }
  }
}
```

### Testing the MCP Server

```bash
# Test server startup
npm run mcp

# Test with MCP protocol
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | node src/mcp/server.js
```

---

## Available Tools

### Database Operations

| Tool | Description |
|------|-------------|
| `db_list` | List all databases in Metabase |
| `db_schemas` | Get schema names for a database |
| `db_tables` | Get tables with field counts |
| `db_test_speed` | Test database response time |
| `sql_execute` | Execute SQL queries |
| `db_connection_info` | Get connection details (admin) |
| `db_table_create` | Create tables (with prefix) |
| `db_view_create` | Create views |
| `db_matview_create` | Create materialized views |
| `db_index_create` | Create indexes |
| `db_schema_explore` | Fast schema exploration |
| `db_schema_analyze` | Deep schema analysis |
| `db_relationships_detect` | Detect foreign keys |
| `db_sync_schema` | Trigger schema sync |

### AI Features

| Tool | Description |
|------|-------------|
| `ai_sql_generate` | Generate SQL from natural language |
| `ai_sql_optimize` | Optimize query performance |
| `ai_sql_explain` | Explain SQL in plain English |
| `ai_relationships_suggest` | Suggest table relationships |
| `mb_auto_describe` | Auto-generate descriptions |

### Question/Card Operations

| Tool | Description |
|------|-------------|
| `mb_question_create` | Create new question |
| `mb_questions` | List questions |
| `mb_question_create_parametric` | Create parametric question |
| `mb_card_get` | Get card details |
| `mb_card_update` | Update card |
| `mb_card_delete` | Delete card |
| `mb_card_archive` | Archive card |
| `mb_card_data` | Get card data |
| `mb_card_copy` | Copy card |
| `mb_card_clone` | Clone card |

### Dashboard Operations

| Tool | Description |
|------|-------------|
| `mb_dashboard_create` | Create dashboard |
| `mb_dashboards` | List dashboards |
| `mb_dashboard_get` | Get dashboard details |
| `mb_dashboard_update` | Update dashboard |
| `mb_dashboard_delete` | Delete dashboard |
| `mb_dashboard_add_card` | Add card to dashboard |
| `mb_dashboard_add_filter` | Add dashboard filter |
| `mb_dashboard_layout_optimize` | Optimize layout |
| `mb_dashboard_template_executive` | Create executive dashboard |
| `mb_dashboard_copy` | Copy dashboard |

### User Management

| Tool | Description |
|------|-------------|
| `mb_user_list` | List all users |
| `mb_user_get` | Get user details |
| `mb_user_create` | Create user |
| `mb_user_update` | Update user |
| `mb_user_disable` | Disable user |
| `mb_permission_group_list` | List permission groups |
| `mb_permission_group_create` | Create group |
| `mb_permission_group_delete` | Delete group |

### Search and Utilities

| Tool | Description |
|------|-------------|
| `mb_search` | Global search |
| `mb_bookmark_create` | Create bookmark |
| `mb_bookmark_list` | List bookmarks |
| `mb_cache_invalidate` | Invalidate cache |
| `web_search_metabase_docs` | Search documentation |

---

## Usage Examples

### Generate SQL from Natural Language

```javascript
// Ask: "Show me total sales by category for last 30 days"
// Tool: ai_sql_generate
{
  "description": "Show total sales by category for last 30 days",
  "database_id": 1
}
```

### Create Executive Dashboard

```javascript
// Tool: mb_dashboard_template_executive
{
  "name": "Sales Performance",
  "database_id": 1,
  "business_domain": "ecommerce",
  "time_period": "last_30_days"
}
```

### Search Across Metabase

```javascript
// Tool: mb_search
{
  "query": "revenue",
  "type": "card"
}
```

---

## Security

### Credential Protection
- All credentials stored in environment variables
- No hardcoded secrets in source code
- `.env` file excluded from version control

### Database Safety
- AI-created objects use `claude_ai_` prefix
- DDL operations require explicit approval
- Dry-run mode enabled by default

### Audit and Compliance
- Activity logging for all operations
- Error analysis and performance insights
- Supports GDPR and SOC 2 requirements

See [SECURITY.md](SECURITY.md) for full security policy.

---

## Deployment

### PM2 (Recommended)

```bash
npm install -g pm2
npm run pm2:start
pm2 save
pm2 startup
```

### Docker

```bash
npm run docker:run
```

### Systemd (Linux)

```bash
sudo cp metabase-ai-mcp.service /etc/systemd/system/
sudo systemctl enable metabase-ai-mcp
sudo systemctl start metabase-ai-mcp
```

---

## API Reference

### MetabaseClient

```javascript
import { MetabaseClient } from './src/metabase/client.js';

const client = new MetabaseClient({
  url: 'http://your-metabase.com',
  apiKey: 'your_api_key'
});

// Get databases
const databases = await client.getDatabases();

// Execute query
const result = await client.executeNativeQuery(databaseId, sql);

// Create question
const question = await client.createQuestion(questionData);
```

### MetabaseAIAssistant

```javascript
import { MetabaseAIAssistant } from './src/ai/assistant.js';

const assistant = new MetabaseAIAssistant({
  metabaseClient: client,
  aiProvider: 'anthropic',
  anthropicApiKey: 'your_key'
});

// Generate SQL
const sql = await assistant.generateSQL(description, schema);

// Optimize query
const optimized = await assistant.optimizeQuery(sql);
```

---

## Project Structure

```
metabase-ai-assistant/
├── src/
│   ├── mcp/
│   │   └── server.js           # MCP Server (107 tools)
│   ├── metabase/
│   │   └── client.js           # Metabase API client
│   ├── database/
│   │   ├── direct-client.js    # Direct PostgreSQL client
│   │   └── connection-manager.js
│   ├── ai/
│   │   └── assistant.js        # AI helper functions
│   ├── cli/
│   │   └── interactive.js      # Interactive CLI
│   └── utils/
│       ├── logger.js           # Logging utilities
│       ├── activity-logger.js  # Activity tracking
│       └── parametric-questions.js
├── docs/
│   └── COMPETITOR_ANALYSIS.md
├── .env.example
├── package.json
├── LICENSE
├── SECURITY.md
├── PRIVACY.md
└── README.md
```

---

## Contributing

Contributions are welcome. Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit changes (`git commit -m 'Add new feature'`)
4. Push to branch (`git push origin feature/new-feature`)
5. Open a Pull Request

### Guidelines

- Write tests for new features
- Follow existing code style
- Update documentation as needed
- Use conventional commit messages

---

## Troubleshooting

### Connection Issues

- Verify Metabase URL is accessible
- Check API key validity
- Confirm network connectivity

### MCP Issues

- Ensure Node.js 18+ is installed
- Verify environment variables are set
- Test server directly: `node src/mcp/server.js`

### Query Errors

- Validate SQL syntax
- Check table and column names
- Verify database permissions

---

## Comparison with Alternatives

This implementation provides the most comprehensive Metabase MCP integration available:

| Feature | This Project | Others |
|---------|-------------|--------|
| Total Tools | 107 | 20-30 |
| User Management | Yes | Limited |
| Direct DDL | Yes | No |
| AI SQL Generation | Yes | No |
| Dashboard Templates | Yes | No |
| Activity Logging | Yes | No |
| Parametric Questions | Yes | No |

---

## Keywords

Metabase, MCP, Model Context Protocol, AI, Business Intelligence, SQL, PostgreSQL, Dashboard, Analytics, LLM, Natural Language SQL, Query Builder, Data Visualization, BI Tools, Database Management, API Integration, Automation

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

Copyright 2024-2026 ONMARTECH LLC

---

## Author

**Abdullah Enes SARI**
- Company: ONMARTECH LLC
- GitHub: [@onmartech](https://github.com/onmartech)

---

## Acknowledgments

- Metabase Team for the excellent BI platform
- MCP Protocol contributors
- Open source community

---

## Support

- GitHub Issues: [Report bugs](https://github.com/onmartech/metabase-ai-assistant/issues)
- Documentation: [Wiki](https://github.com/onmartech/metabase-ai-assistant/wiki)
- Commercial Support: contact@onmartech.com
