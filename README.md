<div align="center">

# 🚀 Metabase AI Assistant

### **The Most Powerful MCP Server for Metabase**

**134 Tools** • **MCP SDK v1.26.0** • **AI-Powered SQL** • **Structured Output** • **Enterprise Security**

[![npm version](https://img.shields.io/npm/v/metabase-ai-assistant.svg?style=for-the-badge&logo=npm)](https://www.npmjs.com/package/metabase-ai-assistant)
[![npm downloads](https://img.shields.io/npm/dm/metabase-ai-assistant.svg?style=for-the-badge&logo=npm)](https://www.npmjs.com/package/metabase-ai-assistant)
[![GitHub stars](https://img.shields.io/github/stars/enessari/metabase-ai-assistant?style=for-the-badge&logo=github)](https://github.com/enessari/metabase-ai-assistant/stargazers)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

[![MCP Compatible](https://img.shields.io/badge/MCP-Compatible-blueviolet.svg?style=flat-square&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZmlsbD0id2hpdGUiIGQ9Ik0xMiAyTDIgN2wxMCA1IDEwLTV6Ii8+PC9zdmc+)](https://modelcontextprotocol.io/)
[![Claude](https://img.shields.io/badge/Claude-Ready-orange.svg?style=flat-square)](https://claude.ai)
[![Cursor](https://img.shields.io/badge/Cursor-Ready-green.svg?style=flat-square)](https://cursor.sh)
[![Node.js](https://img.shields.io/badge/Node.js-18+-brightgreen.svg?style=flat-square&logo=node.js)](https://nodejs.org/)
[![MCP Badge](https://lobehub.com/badge/mcp/onmartech-metabase-ai-assistant)](https://lobehub.com/mcp/onmartech-metabase-ai-assistant)
---

**Turn your AI assistant into a Metabase power user.**  
Generate SQL from natural language, create dashboards, manage users, and automate BI workflows.

[**📦 Install Now**](#-quick-start) • [**📖 Documentation**](#-available-tools) • [**🎯 Features**](#-why-this-project) • [**⭐ Star Us**](https://github.com/enessari/metabase-ai-assistant)

</div>

---

## ⭐ Why This Project?

> **"I analyzed every Metabase MCP server on the market. This one has 4x more tools and features than any competitor."**

| Feature | **This Project** | Other MCP Servers |
|---------|:----------------:|:-----------------:|
| **Total Tools** | **134** ✅ | 6-30 |
| **AI SQL Generation** | ✅ | ❌ |
| **AI SQL Optimization** | ✅ | ❌ |
| **Dashboard Templates** | ✅ | ❌ |
| **User Management** | ✅ | ❌ |
| **Workspace Export/Import** | ✅ | ❌ |
| **Read-Only Security Mode** | ✅ | ✅ |
| **Response Caching** | ✅ | ✅ |
| **Activity Logging** | ✅ | ❌ |
| **Metadata Analytics** | ✅ | ❌ |
| **Parametric Questions** | ✅ | ❌ |
| **Environment Comparison** | ✅ | ❌ |
| **Structured Output (JSON)** | ✅ | ❌ |
| **Tool Annotations** | ✅ | ❌ |

---

## 🚀 Quick Start

### One-Line Install

```bash
npx metabase-ai-assistant
```

### Add to Claude Desktop / Cursor

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://your-metabase.com",
        "METABASE_API_KEY": "mb_your_api_key"
      }
    }
  }
}
```

That's it! Your AI assistant now has full Metabase superpowers. 🦸

---

## 🎯 What Can You Do?

### 💬 Natural Language → SQL

```
You: "Show me total revenue by product category for the last 30 days"
AI: Uses ai_sql_generate → Runs query → Returns formatted results
```

### 📊 Instant Dashboard Creation

```
You: "Create an executive dashboard for our e-commerce sales"
AI: Uses mb_dashboard_template_executive → Creates fully configured dashboard
```

### 🔍 Deep Database Exploration

```
You: "What tables are related to 'orders' and show their relationships"
AI: Uses db_relationships_detect → Returns complete ER diagram info
```

### 🛡️ Enterprise-Grade Security

```
You: "DROP TABLE users" 
AI: 🔒 Blocked - Read-only mode active
```

---

## 🔧 Complete Tool List (134)

> 🆕 All tools include MCP annotations and `title`. 16 priority tools support `outputSchema` + `structuredContent` for typed JSON responses.

<details>
<summary><b>📊 Database Operations (25 tools)</b></summary>

| Tool | Description |
|------|-------------|
| `db_list` | List all databases |
| `db_schemas` | Get schemas in a database |
| `db_tables` | Get tables with fields |
| `sql_execute` | Execute SQL queries |
| `db_table_create` | Create tables (AI-prefixed) |
| `db_view_create` | Create views |
| `db_matview_create` | Create materialized views |
| `db_index_create` | Create indexes |
| `db_vacuum_analyze` | VACUUM and ANALYZE |
| `db_query_explain` | EXPLAIN query plans |
| `db_table_stats` | Table statistics |
| `db_index_usage` | Index usage analysis |
| `db_schema_explore` | Fast schema exploration |
| `db_schema_analyze` | Deep schema analysis |
| `db_relationships_detect` | Detect foreign keys |
| ...and more |

</details>

<details>
<summary><b>🤖 AI-Powered Features (5 tools)</b></summary>

| Tool | Description |
|------|-------------|
| `ai_sql_generate` | Natural language → SQL |
| `ai_sql_optimize` | Query optimization suggestions |
| `ai_sql_explain` | Explain SQL in plain English |
| `ai_relationships_suggest` | Suggest table relationships |
| `mb_auto_describe` | Auto-generate descriptions |

</details>

<details>
<summary><b>📋 Question/Card Management (12 tools)</b></summary>

| Tool | Description |
|------|-------------|
| `mb_question_create` | Create new questions |
| `mb_questions` | List all questions |
| `mb_question_create_parametric` | Parametric questions |
| `mb_card_get` | Get card details |
| `mb_card_update` | Update cards |
| `mb_card_delete` | Delete cards |
| `mb_card_archive` | Archive cards |
| `mb_card_data` | Get card data as JSON |
| `mb_card_copy` | Copy cards |
| `mb_card_clone` | Clone cards |
| ...and more |

</details>

<details>
<summary><b>📈 Dashboard Management (14 tools)</b></summary>

| Tool | Description |
|------|-------------|
| `mb_dashboard_create` | Create dashboards |
| `mb_dashboards` | List all dashboards |
| `mb_dashboard_get` | Get dashboard details |
| `mb_dashboard_update` | Update dashboards |
| `mb_dashboard_delete` | Delete dashboards |
| `mb_dashboard_add_card` | Add cards to dashboard |
| `mb_dashboard_add_filter` | Add filters |
| `mb_dashboard_layout_optimize` | Optimize layout |
| `mb_dashboard_template_executive` | Executive templates |
| ...and more |

</details>

<details>
<summary><b>👥 User & Permission Management (10 tools)</b></summary>

| Tool | Description |
|------|-------------|
| `mb_user_list` | List users |
| `mb_user_get` | Get user details |
| `mb_user_create` | Create users |
| `mb_user_update` | Update users |
| `mb_user_disable` | Disable users |
| `mb_permission_group_list` | List groups |
| `mb_permission_group_create` | Create groups |
| ...and more |

</details>

<details>
<summary><b>📊 Metadata Analytics (14 tools)</b></summary>

| Tool | Description |
|------|-------------|
| `mb_meta_overview` | Instance health check |
| `mb_meta_query_performance` | Query analytics |
| `mb_meta_content_usage` | Content usage stats |
| `mb_meta_user_activity` | User activity |
| `mb_meta_table_dependencies` | Table dependencies |
| `mb_meta_impact_analysis` | Breaking change analysis |
| `mb_meta_optimization_recommendations` | Index suggestions |
| `mb_meta_export_workspace` | Backup to JSON |
| `mb_meta_import_preview` | Import dry-run |
| `mb_meta_compare_environments` | Dev vs Prod diff |
| `mb_meta_auto_cleanup` | Safe cleanup |
| ...and more |

</details>

---

## 🛡️ Security Features

| Feature | Description |
|---------|-------------|
| **🔒 Read-Only Mode** | Blocks INSERT, UPDATE, DELETE, DROP (default: enabled) |
| **🏷️ AI Prefix** | All AI-created objects use `claude_ai_` prefix |
| **✅ Explicit Approval** | Destructive operations require confirmation |
| **📝 Activity Logging** | Full audit trail of all operations |
| **🔐 Env Validation** | Zod-validated environment variables |
| **💾 Auto-Backup** | Prompts for backup before destructive ops |

```bash
# Enable/disable read-only mode
METABASE_READ_ONLY_MODE=true  # Default: blocks write ops
METABASE_READ_ONLY_MODE=false # Allow write operations
```

---

## ⚙️ Configuration

Create a `.env` file:

```bash
# Required
METABASE_URL=https://your-metabase.com
METABASE_API_KEY=mb_your_api_key

# Or use username/password
# METABASE_USERNAME=admin@example.com
# METABASE_PASSWORD=your_password

# Security (defaults to true)
METABASE_READ_ONLY_MODE=true

# AI Features (optional)
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...

# Performance (optional)
CACHE_TTL_MS=600000  # 10 minutes
```

---

## 📦 Installation Options

### npm (Recommended)

```bash
npm install -g metabase-ai-assistant
```

### Docker

```bash
docker run -e METABASE_URL=... -e METABASE_API_KEY=... ghcr.io/enessari/metabase-ai-assistant
```

### From Source

```bash
git clone https://github.com/enessari/metabase-ai-assistant.git
cd metabase-ai-assistant
npm install
npm run mcp
```

---

## 🏗️ Architecture

```
metabase-ai-assistant/
├── src/
│   ├── mcp/
│   │   ├── server.js              # MCP Server entry point
│   │   ├── tool-registry.js       # 134 tool definitions + annotations + outputSchema
│   │   ├── tool-router.js         # Dynamic routing with read-only gate
│   │   └── handlers/              # 15 modular handler files
│   ├── utils/
│   │   ├── structured-response.js # Structured output (MCP 2025-06-18)
│   │   ├── cache.js               # TTL-based caching
│   │   ├── config.js              # Zod validation
│   │   └── response-optimizer.js  # Compact response formatting
│   └── metabase/
│       └── client.js              # Metabase API client
```

---

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Fork, clone, install
git clone https://github.com/YOUR_USERNAME/metabase-ai-assistant.git
npm install

# Create feature branch
git checkout -b feature/amazing-feature

# Test and submit PR
npm test
git push origin feature/amazing-feature
```

---

## 📚 Resources

- [📖 Full Documentation](https://github.com/enessari/metabase-ai-assistant/wiki)
- [🐛 Report Issues](https://github.com/enessari/metabase-ai-assistant/issues)
- [💬 Discussions](https://github.com/enessari/metabase-ai-assistant/discussions)
- [📦 npm Package](https://www.npmjs.com/package/metabase-ai-assistant)

---

## 📄 License

Apache License 2.0 - see [LICENSE](LICENSE)

---

<div align="center">

### ⭐ Star this repo if it helps you!

**Built with ❤️ by [Abdullah Enes SARI](https://github.com/enessari) @ [ONMARTECH LLC](https://onmartech.com)**

[![Star History](https://img.shields.io/github/stars/enessari/metabase-ai-assistant?style=social)](https://github.com/enessari/metabase-ai-assistant/stargazers)

---

**Keywords:** Metabase MCP Server, Model Context Protocol, AI SQL Generation, Business Intelligence, Claude AI, Cursor AI, Natural Language SQL, Dashboard Automation, PostgreSQL, Data Analytics, LLM Tools

</div>
