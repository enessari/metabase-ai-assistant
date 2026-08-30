# 🤖 ChatGPT Custom GPTs & Actions Setup Guide

You can connect **Metabase AI Assistant** directly to ChatGPT (GPT-4o / GPT-4.5) using **Custom GPTs and Actions**, allowing any team member to query Metabase from within the standard ChatGPT interface.

---

## 🚀 Quick Setup (3 Minutes)

### Step 1: Start the Remote HTTP/SSE Server

Run the built-in HTTP server (or deploy to Cloudflare/Render/Railway):

```bash
npm run start:sse
```

This starts the server on port `3000` (or `process.env.PORT`) and exposes:
- **OpenAPI Schema**: `http://your-domain.com/tools/openapi.json`
- **Tool Execution Webhook**: `http://your-domain.com/tools/{toolName}`

---

### Step 2: Create a Custom GPT in ChatGPT

1. Go to [ChatGPT](https://chat.openai.com) -> **Explore GPTs** -> **Create a GPT**.
2. Name your GPT: **Metabase BI Copilot**.
3. In the **Instructions** box, paste:

```markdown
You are an expert Metabase Business Intelligence assistant.
You help users explore databases, generate SQL queries, inspect table relationships, manage dashboards, and analyze analytics data.

Security Instructions:
- Always review SQL queries before executing.
- In read-only mode, only execute SELECT queries.
- When generating SQL, strictly use table and column names returned by schema discovery tools.
```

---

### Step 3: Add Action via OpenAPI Schema

1. Scroll down to **Actions** -> Click **Create new action**.
2. Click **Import from URL** and enter:
   `https://your-server-domain.com/tools/openapi.json`
   *(Or paste the schema directly from `tools/openapi.json`)*
3. Set **Authentication**:
   - Authentication Type: **API Key**
   - Header Name: `x-api-key`
   - Value: Your `METABASE_API_KEY`
4. Click **Save** -> Publish to **Only me** or **Anyone in my workspace**.

---

## 🎯 Example Prompts in ChatGPT

- *"What databases and tables are available in our Metabase instance?"*
- *"Show me top 10 customers by revenue last month from the orders table."*
- *"Create a new dashboard named 'Q3 Sales Performance'."*
- *"Explain how the users and transactions tables are linked."*
