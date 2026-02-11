# Metabase AI Assistant - MCP Integration Guide

This document explains how to integrate Metabase AI Assistant with MCP-compatible AI clients.

**Author**: Abdullah Enes SARI
**Company**: ONMARTECH LLC

---

## Quick Start

### 1. Test the MCP Server

```bash
cd /path/to/metabase-ai-assistant
npm run mcp
```

### 2. MCP Client Configuration

Add the following to your MCP client configuration file:

```json
{
  "mcpServers": {
    "metabase-ai-assistant": {
      "command": "node",
      "args": ["/path/to/metabase-ai-assistant/src/mcp/server.js"],
      "env": {
        "METABASE_URL": "http://localhost:3000",
        "METABASE_USERNAME": "your_username",
        "METABASE_PASSWORD": "your_password",
        "METABASE_API_KEY": "your_api_key",
        "ANTHROPIC_API_KEY": "your_anthropic_key",
        "OPENAI_API_KEY": "your_openai_key"
      }
    }
  }
}
```

### 3. Restart Your MCP Client

After updating the configuration file, restart your MCP client to load the new server.

---

## Available Tools (134 Total)

> 🆕 **MCP SDK v1.26.0** — All tools include annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`) and human-readable `title`. 16 priority tools support `outputSchema` + `structuredContent` for typed JSON responses.

### Database Operations
- **db_list**: List all databases in Metabase
- **db_schemas**: Get schema names for a database
- **db_tables**: Get tables with metadata
- **db_connection_info**: Get database connection details (admin required)

### SQL Operations
- **sql_execute**: Execute native SQL query
- **sql_submit**: Asynchronously create a long-running SQL query job
- **sql_status**: Check the status of a SQL job
- **sql_cancel**: Cancel a running SQL job
- **db_table_profile**: Profile a table (smart dim/ref detection)
- **ai_sql_generate**: Generate SQL from natural language description
- **ai_sql_optimize**: Optimize SQL query for performance
- **ai_sql_explain**: Explain what a SQL query does

### Question/Chart Operations
- **mb_question_create**: Create new question/chart in Metabase
- **mb_questions**: List existing questions
- **mb_card_get**: Get card details
- **mb_card_update**: Update card properties
- **mb_card_delete**: Delete card
- **mb_card_data**: Get card result data

### Dashboard Operations
- **mb_dashboard_create**: Create new dashboard
- **mb_dashboards**: List existing dashboards
- **mb_dashboard_add_card**: Add card to dashboard
- **mb_dashboard_template_executive**: Create executive dashboard template

### Direct Database Operations

**Security Feature**: All objects are created with `claude_ai_` prefix and only prefixed objects can be deleted.

#### DDL Operations
- **db_table_create**: Create table directly in database
- **db_view_create**: Create view directly in database
- **db_matview_create**: Create materialized view (PostgreSQL)
- **db_index_create**: Create index directly in database

#### DDL Reading
- **db_table_ddl**: Get table CREATE statement
- **db_view_ddl**: Get view CREATE statement

#### Object Management
- **db_ai_list**: List all AI-created objects
- **db_ai_drop**: Safely delete AI objects

#### Security Controls
- **Prefix Protection**: Only `claude_ai_` prefixed objects
- **Approval System**: `approved: true` required
- **Dry Run**: Default `dry_run: true`
- **Operation Whitelist**: Only safe operations allowed
- **No System Modifications**: System tables/views protected

---

## Usage Examples

### Basic Queries
```
"Show tables in BIDB database"
"List last 30 days sales data"
"Show top selling products"
```

### AI-Powered SQL Generation
```
"Create a query showing monthly revenue trend"
"Write SQL for customer segmentation"
"Generate top 10 customers list"
```

### Dashboard Creation
```
"Create a sales performance dashboard"
"Build an executive summary report"
```

### Query Analysis
```
"Optimize this SQL query: SELECT * FROM ..."
"Explain what this query does: SELECT ..."
```

### Direct Database Operations
```
"Create a customer_analysis table in BIDB database"
"Create a sales_summary view"
"Create a performance_metrics materialized view"
"Add an index on customer_id column"
"List AI-created objects"
"Delete claude_ai_test_table"
```

### Safe DDL Workflow
```
1. "Dry run create test_table" (preview)
2. "Create test_table with approval" (execute)
3. "List AI objects" (verify)
4. "Show DDL for test_table" (validate)
```

---

## Troubleshooting

### Connection Issues

1. **MCP Server Not Running?**
   ```bash
   npm run mcp
   ```

2. **Missing Environment Variables?**
   Check your `.env` file or configuration `env` section.

3. **Metabase Access Problem?**
   ```bash
   npm start test
   ```

### Common Errors

- **"Tool not found"**: Restart your MCP client
- **"Authentication failed"**: Check Metabase credentials
- **"AI assistant not configured"**: Add API keys

---

## Features Summary

### Currently Available
- **MCP SDK v1.26.0** (Spec 2025-11-25 compliant)
- **Structured Output** (`outputSchema` + `structuredContent` for 16 tools)
- **Tool Annotations** (`readOnlyHint`, `destructiveHint`, `idempotentHint`)
- **Dynamic Tool List** (`listChanged` capability)
- **Async Query Management** (for long-running queries)
- **Smart Response Optimization** (no truncation for DDL/definitions)
- **Table Profiling** (dim/ref table detection)
- Metabase API integration
- SQL query execution
- Question/Dashboard creation
- AI-powered SQL generation
- Query optimization
- Query explanation
- Direct DDL operations
- User and permission management
- Global search
- Activity logging

### Under Development
- Batch operations
- Export/Import features
- Real-time updates
- Advanced visualizations
- Custom metrics
- Automated reports

---

## API Reference

### db_list
```json
// Input: None
// Output: List of databases
```

### sql_execute
```json
{
  "database_id": 1,
  "sql": "SELECT * FROM table_name LIMIT 10",
  "full_results": false // Optional: Set true to disable truncation (useful for DDL)
}
```

### sql_submit (Async Query)
```json
{
  "database_id": 1,
  "sql": "SELECT SLEEP(120)",
  "timeout_seconds": 300
}
```

### sql_status
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### db_table_profile
```json
{
  "database_id": 1,
  "schema": "public",
  "table": "dim_customers",
  "sample_rows": 3
}
```

### ai_sql_generate
```json
{
  "description": "Show last 30 days sales data",
  "database_id": 1
}
```

### mb_question_create
```json
{
  "name": "Question Name",
  "description": "Description",
  "database_id": 1,
  "sql": "SELECT ...",
  "collection_id": 1
}
```

---

## Useful Links

- [MCP Specification](https://modelcontextprotocol.io/)
- [Metabase API Documentation](https://www.metabase.com/docs/latest/api-documentation.html)

---

Copyright 2024-2026 ONMARTECH LLC
Developed by Abdullah Enes SARI
