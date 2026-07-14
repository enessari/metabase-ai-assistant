# Metabase AI Assistant

MCP server for Metabase: SQL execution, questions, dashboards, metadata, and AI-assisted queries. Compatible with Claude, Cursor, and other MCP clients.

## Rama `fix/read-only-enforcement-and-card-query` (vs `main`)

Estamos optimizando este MCP para **apurarlo** — menos latencia, respuestas más livianas y menos round-trips. Los cambios de esta rama son el primer paso; el trabajo de performance sigue en curso.

| Área | Antes (`main`) | Ahora (esta rama) |
|------|----------------|-------------------|
| **`mb_card_get` — `structuredContent`** | Metadatos básicos de la card | Incluye **`dataset_query`** (SQL nativo o MBQL) sin llamadas extra |
| **`mb_card_get` — `outputSchema`** | `description` y `collection_id` solo como string/number | Tipos **nullable** alineados con Metabase |
| **`mb_card_get` — schema JSON** | Propiedades fijas | `additionalProperties: true` |
| **Handler `cards.js`** | `description \|\| null`, `collection_id \|\| null` | **`?? null`** para distinguir vacío de `null` real |

**Por qué importa:** en modo read-only, un agente puede leer el SQL/MBQL de una pregunta existente desde `mb_card_get` sin herramientas de escritura ni parsear texto plano.

Archivos: `src/mcp/handlers/cards.js`, `src/mcp/tool-registry.js`

---

## Quick Start

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

That's it. Your AI assistant can use Metabase through MCP.

---

## Examples

### Natural language to SQL

```
You: "Show me total revenue by product category for the last 30 days"
AI: Uses ai_sql_generate → Runs query → Returns formatted results
```

### Dashboard creation

```
You: "Create an executive dashboard for our e-commerce sales"
AI: Uses mb_dashboard_template_executive → Creates fully configured dashboard
```

### Database exploration

```
You: "What tables are related to 'orders' and show their relationships"
AI: Uses db_relationships_detect → Returns complete ER diagram info
```

### Read-only mode

```
You: "DROP TABLE users"
AI: Blocked — read-only mode active
```

---

## Tool list (134)

134 tools with MCP annotations. 16 priority tools support `outputSchema` + `structuredContent`.

<details>
<summary><b>Database operations (25 tools)</b></summary>

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
<summary><b>AI features (5 tools)</b></summary>

| Tool | Description |
|------|-------------|
| `ai_sql_generate` | Natural language → SQL |
| `ai_sql_optimize` | Query optimization suggestions |
| `ai_sql_explain` | Explain SQL in plain English |
| `ai_relationships_suggest` | Suggest table relationships |
| `mb_auto_describe` | Auto-generate descriptions |

</details>

<details>
<summary><b>Question/card management (12 tools)</b></summary>

| Tool | Description |
|------|-------------|
| `mb_question_create` | Create new questions |
| `mb_questions` | List all questions |
| `mb_question_create_parametric` | Parametric questions |
| `mb_card_get` | Get card details (includes `dataset_query` on this branch) |
| `mb_card_update` | Update cards |
| `mb_card_delete` | Delete cards |
| `mb_card_archive` | Archive cards |
| `mb_card_data` | Get card data as JSON |
| `mb_card_copy` | Copy cards |
| `mb_card_clone` | Clone cards |
| ...and more |

</details>

<details>
<summary><b>Dashboard management (14 tools)</b></summary>

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
<summary><b>User and permission management (10 tools)</b></summary>

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
<summary><b>Metadata analytics (14 tools)</b></summary>

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

## Security

| Feature | Description |
|---------|-------------|
| Read-only mode | Blocks INSERT, UPDATE, DELETE, DROP (default: enabled) |
| AI prefix | AI-created objects use `claude_ai_` prefix |
| Explicit approval | Destructive operations require confirmation |
| Activity logging | Audit trail of operations |
| Env validation | Zod-validated environment variables |

```bash
# Enable/disable read-only mode
METABASE_READ_ONLY_MODE=true  # Default: blocks write ops
METABASE_READ_ONLY_MODE=false # Allow write operations
```

---

## Configuration

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

## Installation

### npm (Recommended)

```bash
npm install -g metabase-ai-assistant
```

### Docker

```bash
docker run -e METABASE_URL=... -e METABASE_API_KEY=... metabase-ai-assistant
```

### From Source

```bash
git clone <repo-url>
cd metabase-ai-assistant
npm install
npm run mcp
```

---

## Architecture

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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## Resources

- [MCP integration guide](README_MCP.md)
- [npm package](https://www.npmjs.com/package/metabase-ai-assistant)

---

## License

Apache License 2.0 — see [LICENSE](LICENSE)
