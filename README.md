# Metabase AI Assistant — Model Context Protocol (MCP) Server

[![npm version](https://img.shields.io/npm/v/metabase-ai-assistant.svg?style=flat-square)](https://www.npmjs.com/package/metabase-ai-assistant)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square)](https://opensource.org/licenses/Apache-2.0)
[![Node.js](https://img.shields.io/badge/Node.js-18%2B-brightgreen.svg?style=flat-square)](https://nodejs.org/)
[![MCP SDK](https://img.shields.io/badge/MCP%20SDK-v1.26.0-purple.svg?style=flat-square)](https://modelcontextprotocol.io/)

Metabase AI Assistant is an enterprise-grade Model Context Protocol (MCP) server that connects Large Language Models (LLMs), AI coding assistants, and automated data workflows directly to your Metabase Business Intelligence instance.

Featuring **133 dedicated tools**, strict security guardrails, AI provenance tracking, and deep version compatibility across Metabase releases, this server enables natural language analytics, automated dashboard operations, schema exploration, and administrative orchestration.

---

## Table of Contents / İçindekiler / جدول المحتويات / 目录

- [English Documentation](#-english-documentation)
  - [Core Capabilities](#core-capabilities)
  - [Architecture & Security](#architecture--security)
  - [Metabase Version Compatibility](#metabase-version-compatibility)
  - [Quick Start & Installation](#quick-start--installation)
  - [Client Configuration](#client-configuration)
  - [Tool Categories Overview](#tool-categories-overview)
- [Türkçe Dokümantasyon](#-türkçe-dokümantasyon)
  - [Genel Bakış](#genel-bakış)
  - [Temel Özellikler](#temel-özellikler)
  - [Kurulum ve Entegrasyon](#kurulum-ve-entegrasyon)
- [التوثيق باللغة العربية](#-التوثيق-باللغة-العربية)
  - [نظرة عامة](#نظرة-عامة)
  - [الميزات الرئيسية](#الميزات-الرئيسية)
  - [التثبيت والإعداد](#التثبيت-والإعداد)
- [中文文档](#-中文文档)
  - [项目概述](#项目概述)
  - [核心功能](#核心功能)
  - [快速安装与配置](#快速安装与配置)
- [Testing & Quality Assurance](#-testing--quality-assurance)
- [License](#-license)

---

# 🇬🇧 English Documentation

## Core Capabilities

Metabase AI Assistant transforms standard AI interfaces (Claude Desktop, Cursor, VS Code, automated agent frameworks) into full-fledged Metabase power users:

1. **Natural Language to SQL Generation**: Generate syntactically valid and optimized queries based on live database schemas, table metadata, and relational constraints.
2. **Dashboard & Card Lifecycle Management**: Create, clone, modify, archive, and parameterize cards/questions and dashboards programmatically.
3. **Intelligent Grid & Layout Automation**: Position and resize cards automatically on the Metabase dashboard grid, configure series settings, and link dashboard filters to card variables.
4. **Full Schema & Relationship Introspection**: Inspect tables, fields, foreign keys, cardinality, and data types without issuing manual DDL queries.
5. **Role & Permission Management**: Manage users, permission groups, collection access graphs, and enterprise governance structures.
6. **Performance & Query Analytics**: Test database latency, inspect slow queries, monitor execution logs, and analyze Metabase internal database metrics.

---

## Architecture & Security

Designed with an emphasis on production stability and enterprise safety:

- **Modular Handler Design**: Core server logic is decomposed into dedicated handlers (`SqlHandler`, `CardsHandler`, `CollectionsHandler`, `UsersHandler`, `SchemaHandler`, `AnalyticsHandler`, `ActionsHandler`).
- **AI Provenance Envelope**: Every generative SQL tool (`ai_sql_generate`, `ai_sql_optimize`, `ai_sql_explain`) returns a structured `_provenance` metadata envelope (`ai_generated: true`, `tool`, `review_required: true`, `timestamp`) and explicit review warning banners to prevent blind down-stream execution.
- **Prompt Injection Boundaries**: Untrusted metadata (table comments, user-submitted descriptions) is wrapped in strict `[UNTRUSTED_METADATA]` delimiters with delimiter neutralization.
- **Read-Only Enforcement by Default**: Safe by design with `METABASE_READ_ONLY_MODE=true` by default. All 61 write/mutation tools and regex DML/DDL executions are blocked unless explicitly configured.
- **Strict SQL Sanitization**: Parameter validation, identifier sanitization, and quote escaping via dedicated sanitizer modules.

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

No permanent installation is required to start using the MCP server:

```bash
npx metabase-ai-assistant
```

### Manual Installation via NPM

```bash
npm install -g metabase-ai-assistant
```

---

## Client Configuration

### 1. Claude Desktop (`claude_desktop_config.json`)

On macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`  
On Windows: `%APPDATA%\Claude\claude_desktop_config.json`

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

### 2. Cursor IDE (`.cursor/mcp.json`)

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://your-metabase-instance.com",
        "METABASE_API_KEY": "mb_your_api_key_here"
      }
    }
  }
}
```

### 3. Username / Password Authentication (Alternative)

If API keys are not enabled on your Metabase instance, provide standard credentials:

```json
{
  "env": {
    "METABASE_URL": "https://your-metabase-instance.com",
    "METABASE_USERNAME": "admin@company.com",
    "METABASE_PASSWORD": "your_secure_password"
  }
}
```

---

## Tool Categories Overview

The 133 MCP tools are categorized into 9 operational domains:

1. **SQL & Query Execution (14 tools)**: Direct SQL queries, async execution jobs, query status tracking, pagination, and speed benchmarks.
2. **AI Query Intelligence (6 tools)**: Natural language to SQL, query performance optimizer, query explainer, automated table description.
3. **Cards & Visualizations (34 tools)**: Question creation, query execution, parametric filtering, card cloning, visualization settings.
4. **Dashboards & Layouts (22 tools)**: Dashboard creation, grid placement, filter linking, tab management, executive templates.
5. **Collections & Organization (8 tools)**: Collection tree traversal, hierarchical moves, permission graphs, item listing.
6. **Schema & Data Modeling (18 tools)**: Schema retrieval, foreign key inference, data profiling, table definitions.
7. **User & Permission Administration (12 tools)**: User invitations, group assignments, membership controls, status toggling.
8. **Actions & Automation (8 tools)**: Metabase actions execution, pulse creation, alert triggers, webhook notifications.
9. **Documentation & Metadata (11 tools)**: Metric creation, segment definitions, workspace export/import migration.

---

# 🇹🇷 Türkçe Dokümantasyon

## Genel Bakış

Metabase AI Assistant, Yapay Zeka modellerini (LLM), Claude Desktop, Cursor ve kodlama asistanlarını Metabase İş Zekası (BI) platformunuza doğrudan bağlayan kurumsal standartta bir **Model Context Protocol (MCP)** sunucusudur.

Bünyesinde barındırdığı **133 operasyonel araç**, sıkı güvenlik protokolleri, yapay zeka kaynak doğrulama (provenance) zarfı ve geniş sürüm uyumluluğu sayesinde veri analitiği süreçlerini otomatikleştirir.

## Temel Özellikler

- **Doğal Dilden SQL Üretimi**: Şema ve tablo ilişkilerini analiz ederek optimize SQL sorguları hazırlar.
- **Dashboard ve Soru Yönetimi**: Kartları ve dashboard'ları API üzerinden oluşturur, düzenler, parametrelendirir ve görselleştirir.
- **Otomatik Izgara (Grid) Yerleşimi**: Dashboard kartlarının boyut ve koordinatlarını akıllı şekilde konumlandırır, dashboard filtrelerini kart değişkenlerine bağlar.
- **Koleksiyon Hiyerarşisi**: Metabase koleksiyon ağaçlarını listeler, öğeleri taşır ve yetkilendirmeleri yönetir.
- **Kurumsal Güvenlik Standartları**: Varsayılan salt-okunur (read-only) modu, prompt injection izolasyonu (`[UNTRUSTED_METADATA]`) ve SQL sanitizasyonu ile veri güvenliğini garanti eder.

## Kurulum ve Entegrasyon

Claude Desktop veya Cursor konfigürasyon dosyanıza (`claude_desktop_config.json` veya `.cursor/mcp.json`) aşağıdaki tanımı eklemeniz yeterlidir:

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://metabase-adresiniz.com",
        "METABASE_API_KEY": "mb_api_anahtariniz",
        "METABASE_READ_ONLY_MODE": "true"
      }
    }
  }
}
```

---

# 🇸🇦 التوثيق باللغة العربية

## نظرة عامة

يعد **Metabase AI Assistant** خادم بروتوكول سياق النموذج (MCP) مصممًا على مستوى المؤسسات لربط نماذج الذكاء الاصطناعي التوليدي، ومساعدات البرمجة مثل Claude Desktop وCursor، مباشرة بمنصة ذكاء الأعمال Metabase.

يحتوي الخادم على **133 أداة متخصصة** تتيح للمؤسسات تحليل البيانات، وتوليد استعلامات SQL، وأتمتة لوحات المعلومات (Dashboards)، وإدارة المستخدمين وفق أعلى معايير الأمان المتبعة.

## الميزات الرئيسية

1. **تحويل اللغة الطبيعية إلى استعلامات SQL**: إنشاء استعلامات قواعد بيانات دقيقة ومحسّنة بالاعتماد على المخطط الفعلي للبيانات.
2. **إدارة لوحات المعلومات والبطاقات**: إنشاء البطاقات ولوحات المعلومات، وتحديث التخطيط، وربط الفلاتر برمجياً عبر واجهة برمجة التطبيقات (API).
3. **أمان المؤسسات والحماية من الحقن**: عزل البيانات غير الموثوقة لمنع هجمات Prompt Injection، مع تفعيل وضع القراءة فقط (Read-Only Mode) افتراضياً.
4. **تتبع مصدر مخرجات الذكاء الاصطناعي**: إضافة حزمة بيانات وصفية (`_provenance`) تضمن مراجعة استعلامات SQL قبل تنفيذها على قواعد البيانات.
5. **توافق شامل مع إصدارات Metabase**: يدعم جميع إصدارات Metabase من v0.43 حتى الإصدارات الحديثة v0.61+.

## التثبيت والإعداد

يمكن تشغيل الخادم مباشرة دون الحاجة لتثبيت مسبق عبر أمر `npx`:

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://your-metabase-instance.com",
        "METABASE_API_KEY": "mb_api_key_here",
        "METABASE_READ_ONLY_MODE": "true"
      }
    }
  }
}
```

---

# 🇨🇳 中文文档

## 项目概述

**Metabase AI Assistant** 是一款企业级模型上下文协议（MCP, Model Context Protocol）服务器，旨在将大语言模型（LLM）、Claude Desktop、Cursor 以及 AI 编程助手直接与 Metabase 商业智能平台无缝连接。

项目内置 **133 个专业工具**，提供严密的权限控制、AI 生成溯源（Provenance Envelope）以及跨版本 Metabase 深度兼容能力，助力企业实现自动化数据分析与 BI 运维。

## 核心功能

1. **自然语言转 SQL (Text-to-SQL)**：结合实时数据库元数据与外键关系，自动生成高效、合规的 SQL 查询语句。
2. **仪表盘与卡片全生命周期管理**：支持自动化创建、更新、克隆仪表盘及图表卡片，并自动映射全局过滤参数。
3. **企业级安全防护体系**：
   - 默认启用只读保护模式（Read-Only Mode），防止非授权数据变更；
   - 隔离提示词注入风险（Prompt Injection），使用 `[UNTRUSTED_METADATA]` 边界标签封装不可信元数据；
   - 输出标准化 `_provenance` 结构体，确保 AI 生成代码在执行前经过安全审计。
4. **全版本兼容架构**：全面支持 Metabase v0.43 至最新 v0.61+ 版本（涵盖 MBQL 5 格式、API Key 认证及目录树层级）。

## 快速安装与配置

在 Claude Desktop 或 Cursor 的 MCP 配置文件中添加如下配置即可：

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

---

## 🧪 Testing & Quality Assurance

The codebase is backed by a multi-tiered automated test suite covering unit logic, integration workflows, and security fuzzing:

```bash
# Run complete test suite (12 suites, 126 tests)
npm test

# Run unit tests
npm run test:unit

# Run end-to-end integration workflows
npm run test:integration

# Run security, prompt injection & SQL fuzzing tests
npm run test:security

# Generate test coverage report
npm run test:coverage

# Run ESLint validation
npm run lint
```

---

## 📄 License

This project is licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for complete details.

Developed and maintained by **Abdullah Enes SARI** ([ONMARTECH LLC](https://github.com/enessari)).
