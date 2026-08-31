# 🗺️ Metabase AI Assistant — Project Roadmap & Upcoming Features

This document tracks upcoming features, strategic enhancements, and community growth initiatives for `metabase-ai-assistant`.

---

## 🎯 High-Priority Backlog (İlk Fırsatta Yapılacaklar)

### 1. 🔑 Metabase v60+ Native OAuth 2.0 Authentication
- **Objective**: Add native Metabase OAuth 2.0 PKCE flow as an alternative to API Key / Session Token authentication.
- **Why**: Metabase v60 introduced built-in OAuth support. Adding this will provide seamless zero-config login for Metabase Cloud and enterprise SSO users.
- **Implementation**:
  - `src/metabase/oauth-client.js`: OAuth authorization URL generation, token exchange, and refresh token handler.
  - CLI command: `npx metabase-ai-assistant --login-oauth`

---

### 2. 📊 Inline Chart & Visual Graph Renderer (`ai_chart_render`)
- **Objective**: Generate and return inline PNG/SVG charts (bar, line, pie, funnel, combo) directly in AI chat windows (Claude Desktop, Cursor, ChatGPT).
- **Why**: Currently, the assistant returns markdown tables, structured JSON, and Metabase web links. Inline SVG/PNG rendering enables instant visual insights without switching to a browser.
- **Implementation**:
  - Lightweight chart rendering engine using pure SVG / canvas.
  - New Tool: `ai_chart_render` (Accepts SQL/Card data + chart type, returns base64 image and SVG markup).

---

### 3. 📤 Direct CSV / Excel File Upload Tool (`mb_upload_csv`)
- **Objective**: Allow users and AI agents to upload CSV and Excel spreadsheets directly into Metabase database tables via the Metabase v50+ `/api/upload/csv` endpoint.
- **Why**: Enables rapid ad-hoc data analysis, spreadsheet modeling, and AI-driven data onboarding.
- **Implementation**:
  - New Tool: `mb_upload_csv` (Parameters: `file_path` or `base64_data`, `table_name`, `db_id`).

---

### 4. 🌐 Community Traction & Ecosystem Growth
- **Objective**: Maximize visibility and adoption across the global Data Engineering and AI developer communities.
- **Action Items**:
  - [ ] Submit to official **Anthropic MCP Registry**.
  - [ ] Update listing on **Smithery.ai** (`npx @smithery/cli`).
  - [ ] Publish technical deep-dive article on Medium / Dev.to: *"How we built a dbt-aware, self-healing Metabase MCP server"*.
  - [ ] Share in Reddit communities: `r/metabase`, `r/dataengineering`, `r/ClaudeAI`, `r/LocalLLaMA`.
  - [ ] Product Hunt and Hacker News (Show HN) launch.

---

## 📅 Version History

- **v5.1.0 (Current)**: Native dbt Semantic Layer awareness, Governance-First business memory (propose/approve, soft-deprecation), 143 MCP tools, multilingual docs (EN, TR, ZH, AR), GEO (`llms.txt`, `llms-full.txt`, `CITATION.cff`).
- **v5.0.0**: Autonomous self-healing SQL engine, end-to-end dashboard architect, query index advisor, proactive anomaly detector, zero-leak enterprise PII masker.
- **v4.0.0**: MCP SDK v1.26.0 migration, modular handlers, structured output schemas (`outputSchema`), read-only gatekeeper.

---

*Maintained by [ONMARTECH LLC](https://github.com/enessari) — Abdullah Enes SARI.*
