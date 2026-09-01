# Metabase AI Assistant — Model Context Protocol (MCP) Server
 
[![npm version](https://img.shields.io/npm/v/metabase-ai-assistant.svg?style=flat-square)](https://www.npmjs.com/package/metabase-ai-assistant)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square)](https://opensource.org/licenses/Apache-2.0)
[![Node.js](https://img.shields.io/badge/Node.js-18%2B-brightgreen.svg?style=flat-square)](https://nodejs.org/)
[![MCP SDK](https://img.shields.io/badge/MCP%20SDK-v1.26.0-purple.svg?style=flat-square)](https://modelcontextprotocol.io/)

Metabase AI Assistant is an enterprise-grade Model Context Protocol (MCP) server that connects Large Language Models (LLMs), AI coding assistants, and automated data workflows directly to your Metabase Business Intelligence instance.

Featuring **152 dedicated tools**, native **dbt Metadata & Metrics Auto-Syncer**, **Metabase to dbt Reverse Lineage Exposures**, **dbt-Smart Question Creator**, **Lightdash Code-as-BI YAML-to-Dashboard** generation, **Cube.js-style Pre-aggregations & Multi-Hop Lineage Joins**, **Omni.co Controlled Semantic-to-YAML bridge**, autonomous self-healing SQL execution, full-scale dashboard architecting, proactive anomaly detection, query index advisory, zero-leak PII masking, and strict security guardrails. Works seamlessly with Claude, Cursor, ChatGPT, Gemini, and **Google Antigravity**.

---

## 🌍 Language Versions / Dil Seçenekleri / 语言版本 / النسخ اللغوية

- 🇬🇧 **[English (Main Documentation)](README.md)**
- 🇹🇷 **[Türkçe Dokümantasyon](README_TR.md)**
- 🇨🇳 **[中文文档 (Chinese)](README_ZH.md)**
- 🇸🇦 **[التوثيق باللغة العربية (Arabic)](README_AR.md)**

---

## Table of Contents

- [Core Architectural Highlights](#core-architectural-highlights)
- [Next-Gen Autonomous Features (v5.3)](#next-gen-autonomous-features-v53)
- [Metabase Version Compatibility](#metabase-version-compatibility)
- [Quick Start & Installation](#quick-start--installation)
- [Client Configuration & Desktop Setup](#client-configuration--desktop-setup)
  - [Claude Desktop (One-Click DXT & JSON)](#1-claude-desktop)
  - [Cursor IDE, Windsurf & VS Code](#2-cursor-ide-windsurf--vs-code)
  - [ChatGPT Custom GPTs & Actions](#3-chatgpt-custom-gpts--actions)
  - [Google Gemini & AI Studio](#4-google-gemini--google-ai-studio)
  - [Google Antigravity SDK & MCP](#5-google-antigravity-sdk--mcp)
- [Tool Categories Overview (152 Tools)](#tool-categories-overview-152-tools)
- [Testing & Quality Assurance](#testing--quality-assurance)
- [Project Roadmap & Upcoming Features](ROADMAP.md)
- [License](#license)

---

## Core Architectural Highlights

Metabase AI Assistant transforms standard AI interfaces (Claude Desktop, Cursor, VS Code, ChatGPT, Gemini, automated agent frameworks) into full-fledged Metabase power users:

1. **dbt Deep Scanning & MetricFlow Integration (`dbt_project_scan_deep`)**: 9-tier architectural classification, `doc('...')` resolution, and `catalog.json` table/column profiling.
2. **Cube.js Multi-Hop Lineage Joins (`dbt_lineage_joins_graph`)**: Resolves shortest join paths via Dijkstra Min-Heap algorithms with 3-color DAG cycle detection.
3. **Cube.js Pre-Aggregation & Rollup Advisor (`dbt_semantic_preagg_advisor`)**: Generates multi-dialect Materialized View DDLs (Postgres, BigQuery, Snowflake, ClickHouse, DuckDB, Redshift, MySQL) with HyperLogLog distinct counts.
4. **Lightdash Code-as-BI Dashboard Builder (`dbt_build_dashboard_from_yaml`)**: Translates `meta.metabase` and `meta.lightdash` formatting options into collision-free 24-column Metabase Dashboards.
5. **Omni.co Controlled Semantic-to-YAML Exporter (`dbt_semantic_export_yaml`)**: Serializes approved business rules into clean dbt `schema.yml` / `semantic_models.yml` code blocks.
6. **Autonomous Self-Healing SQL Engine (`ai_sql_execute_and_heal`)**: 3-iteration automated error-recovery loop for resilient querying.
7. **Zero-Leak Enterprise PII Masker**: Real-time sanitization of emails, phone numbers, national IDs, credit cards, IP addresses, and tokens.

---

## Next-Gen Autonomous Features (v5.1)

### 1. dbt Architectural Hierarchy & Source Prioritization
$$\mathbf{Gold\;Marts\;(fct\_,\;dim\_,\;rpt\_)} \;\gg\; \mathbf{Silver\;(int\_)} \;\gg\; \mathbf{Bronze\;Staging\;(stg\_)}$$
- `dbt_inspect_models`: Parses dbt `manifest.json` and MetricFlow semantic models.
- `dbt_prioritize_sources`: Dynamically routes natural language questions to pre-aggregated, tested dimensional and fact tables.

### 2. Governance-First Semantic Memory (No Silent Learning, No Hard-Deletes)
- `semantic_memory_propose`: Proposes a business rule in `PENDING_APPROVAL` status.
- `semantic_memory_approve`: Explicitly activates the rule with required data steward comments.
- `semantic_memory_deprecate`: Safely soft-archives rules with mandatory audit reasons (`DEPRECATED`).
- `semantic_memory_restore`: Instantly restores archived rules.
- `semantic_memory_list`: Lists all rules with complete audit history and timestamps.

### 3. Autonomous Self-Healing SQL Engine (`ai_sql_execute_and_heal`)
- Catches syntax errors, Levenshtein-distance column misspellings, missing `GROUP BY` clauses, and dialect quirks across Postgres, MySQL, BigQuery, Snowflake, and SQLite.
- Preserves fix history in `_provenance.healing_trail`.

---

## Metabase Version Compatibility

Metabase AI Assistant provides backward and forward compatibility across all major Metabase architectures:

| Metabase Version Range | Compatibility Level | Key Features Supported |
|---|:---:|---|
| **Metabase v0.55 – v0.61+** *(Current)* | **Full Support** | Modern MBQL 5 format (`stages`, `lib/type`), `/api/upload/csv`, updated collection permissions, multi-tab dashboards |
| **Metabase v0.50 – v0.54** | **Full Support** | Collection tree hierarchies (`/api/collection/tree`), Model cards, API Key auth (`x-api-key`), sequential parametric queries |
| **Metabase v0.43 – v0.49** | **Full Support** | Session token authentication (`X-Metabase-Session`), legacy MBQL query pipelines, database introspection |
| **Metabase Open Source & Enterprise** | **Full Support** | Automatic feature detection (whitelabeling, audit logs, granular data permissions) |

---

## Quick Start & Installation

### Global Execution via NPX

```bash
npx metabase-ai-assistant
```

### Manual Installation via NPM

```bash
npm install -g metabase-ai-assistant
```

---

## Client Configuration & Desktop Setup

### 1. Claude Desktop

#### Option A: One-Click Extension (DXT / MCPB)
1. Open **Claude Desktop Settings** -> **Developer / Extensions** -> **Install Local Extension**.
2. Select this repository folder.
3. Or install via Smithery CLI:
   ```bash
   npx -y @smithery/cli install metabase-ai-assistant --client claude
   ```

#### Option B: Manual JSON Configuration
Add the server definition to `claude_desktop_config.json`:
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://your-metabase-instance.com",
        "METABASE_API_KEY": "mb_your_api_key_here",
        "METABASE_READ_ONLY_MODE": "true"
      }
    }
  }
}
```

### 2. Cursor IDE, Windsurf & VS Code

Add to `.cursor/mcp.json` or VS Code MCP settings:

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://your-metabase-instance.com",
        "METABASE_API_KEY": "mb_your_api_key_here",
        "METABASE_READ_ONLY_MODE": "true"
      }
    }
  }
}
```

### 3. ChatGPT Custom GPTs & Actions

Expose Metabase AI Assistant as an OpenAPI Action for ChatGPT Plus / Team / Enterprise:
1. Start the Remote SSE/HTTP server: `npm run start:sse`
2. In ChatGPT, create a **Custom GPT** -> **Actions** -> **Import from URL**: `https://your-domain.com/tools/openapi.json`
3. Detailed setup guide: [docs/integrations/CHATGPT_ACTIONS_GUIDE.md](docs/integrations/CHATGPT_ACTIONS_GUIDE.md)

### 4. Google Gemini & Google AI Studio

Pass tool definitions to Gemini Function Calling SDKs (`@google/genai` or `google-generativeai`):
- Detailed setup guide: [docs/integrations/GOOGLE_GEMINI_GUIDE.md](docs/integrations/GOOGLE_GEMINI_GUIDE.md)

### 5. Cloudflare Workers (Serverless Edge)

Deploy directly to Cloudflare's edge network for free:
```bash
cd deploy/cloudflare
npx wrangler deploy
```

---

## Tool Categories Overview (143 Tools)

The 143 MCP tools are categorized into 10 operational domains:

1. **dbt & Semantic Layer (6 tools)**: Model hierarchy inspection, lineage resolution, source prioritization, governance-first business memory (propose, approve, soft-deprecate, restore).
2. **Autonomous AI BI Operations (4 tools)**: Self-healing SQL engine, end-to-end dashboard architect, query index advisor, proactive anomaly detector.
3. **SQL & Query Execution (14 tools)**: Direct SQL queries, async execution jobs, query status tracking, pagination, and speed benchmarks.
4. **AI Query Intelligence (6 tools)**: Natural language to SQL, query performance optimizer, query explainer, automated table description.
5. **Cards & Visualizations (34 tools)**: Question creation, query execution, parametric filtering, card cloning, visualization settings.
6. **Dashboards & Layouts (22 tools)**: Dashboard creation, grid placement, filter linking, tab management, executive templates.
7. **Collections & Organization (8 tools)**: Collection tree traversal, hierarchical moves, permission graphs, item listing.
8. **Schema & Data Modeling (18 tools)**: Schema retrieval, foreign key inference, data profiling, table definitions.
9. **User & Permission Administration (12 tools)**: User invitations, group assignments, membership controls, status toggling.
10. **Actions & Documentation (19 tools)**: Metabase actions execution, pulses, alerts, webhooks, metrics, segment definitions, workspace migration.

---

## Testing & Quality Assurance

Backed by an automated multi-tier test suite covering unit logic, integration workflows, and security fuzzing:

```bash
# Run complete test suite (32 suites, 583 tests)
npm test

# Run unit tests
npm run test:unit

# Run integration workflows
npm run test:integration

# Run security & PII zero-leak fuzzing tests
npm run test:security
```

---

## License

Licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for details.

Developed and maintained by **Abdullah Enes SARI** ([ONMARTECH LLC](https://github.com/enessari)).
