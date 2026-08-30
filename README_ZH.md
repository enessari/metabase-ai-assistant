# Metabase AI Assistant — 模型上下文协议 (MCP) 服务端

[![npm version](https://img.shields.io/npm/v/metabase-ai-assistant.svg?style=flat-square)](https://www.npmjs.com/package/metabase-ai-assistant)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square)](https://opensource.org/licenses/Apache-2.0)
[![Node.js](https://img.shields.io/badge/Node.js-18%2B-brightgreen.svg?style=flat-square)](https://nodejs.org/)
[![MCP SDK](https://img.shields.io/badge/MCP%20SDK-v1.26.0-purple.svg?style=flat-square)](https://modelcontextprotocol.io/)

Metabase AI Assistant 是一个企业级模型上下文协议 (MCP) 服务端，可将大型语言模型 (LLM)、AI 编程助手以及自动化数据工作流无缝连接至您的 Metabase 商业智能 (BI) 实例。

拥有 **143 个专用工具**、原生 **dbt 语义层 (Semantic Layer)** 架构感知、**治理优先的语义记忆 (Governance-First Memory)**、自主自愈 SQL 引擎、全自动仪表板构建、主动异常检测、索引优化建议以及零泄露 PII 脱敏防护。

---

## 🌍 语言版本 / Other Languages

- 🇬🇧 **[English (主文档)](README.md)**
- 🇹🇷 **[Türkçe Dokümantasyon (土耳其语)](README_TR.md)**
- 🇨🇳 **[中文文档 (Chinese)](README_ZH.md)**
- 🇸🇦 **[التوثيق باللغة العربية (阿拉伯语)](README_AR.md)**

---

## 核心架构特性

1. **dbt 语义层与模型层级感知**：优先选择经过清洗与验证的金牌集市 (`fct_`, `dim_`, `rpt_`) 模型，而非原始 staging 表，确保指标统计精准无误。
2. **治理优先的语义记忆引擎**：通过**显式提案与审批工作流**学习企业专属业务规则。严格执行**软归档（禁止硬删除）**，并记录完备的审计说明。
3. **自主自愈 SQL 引擎 (`ai_sql_execute_and_heal`)**：自动捕获数据库语法及模式错误，通过 3 轮自动修复循环完成 SQL 纠错与执行。
4. **全端自主仪表板架构师 (`ai_dashboard_build_full`)**：仅凭一句自然语言需求，即可自动创建 6–8 个指标卡片、完成 24 列无碰撞排版并绑定全局筛选器。
5. **AI 查询索引与物化视图顾问 (`ai_query_index_advisor`)**：深度分析 `EXPLAIN` 查询计划，为 DBA 提供高价值的复合索引与物化视图创建语句。
6. **主动 KPI 异常与离群值检测 (`ai_analytics_detect_anomalies`)**：集成 Z-Score、Tukey IQR 与布林带多模型算法，检测指标异常并生成归因假设。
7. **企业级零泄露 PII 隐私脱敏**：在数据离开进入 LLM 上下文之前，实时自动屏蔽电子邮件、电话号码、身份证件、信用卡及密钥信息。

---

## 快速安装与配置

### 通过 NPX 快速启动

```bash
npx metabase-ai-assistant
```

### Claude Desktop 客户端配置

在 `claude_desktop_config.json` 中添加配置：

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://your-metabase.com",
        "METABASE_API_KEY": "mb_your_api_key_here",
        "METABASE_READ_ONLY_MODE": "true"
      }
    }
  }
}
```

### Cursor IDE / VS Code 配置

在 `.cursor/mcp.json` 中添加相同配置即可直接在 AI 对话框中调用 Metabase 智能工具。

---

## 自动化测试与质量保证

```bash
# 运行完整测试套件 (32 个测试套件，583 个测试 100% 通过)
npm test

# 运行安全性与 PII 脱敏测试
npm run test:security
```

---

## 开源协议

本项目基于 **Apache License 2.0** 协议开源。详见 [LICENSE](LICENSE) 文件。
