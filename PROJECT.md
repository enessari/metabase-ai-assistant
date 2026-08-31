# Project: Metabase AI Assistant — dbt Semantic BI Co-Pilot

## Architecture

`metabase-ai-assistant` is a Node.js ESM-native Model Context Protocol (MCP) server providing autonomous Business Intelligence operations. This expansion enhances the server into a dbt-native Semantic BI Co-Pilot combining the paradigms of:
1. **Lightdash**: Code-as-BI YAML-driven visualization, metric definitions, and dashboard generation.
2. **Cube.js**: Multi-hop semantic lineage joins and pre-aggregation / rollup materialized view advisory.
3. **Omni.co**: Controlled hybrid modeling and governance bridge syncing approved `SemanticMemory` rules back to dbt `schema.yml` and MetricFlow files.

```
                               ┌──────────────────────────────────────────────────────────┐
                               │           dbt Project Workspace & Compilations           │
                               │  (models/, schema.yml, docs/, manifest, catalog.json)    │
                               └────────────────────────────┬─────────────────────────────┘
                                                            │
                                        [M1: DbtDeepScanner / dbt_project_scan_deep]
                                                            │
                     ┌──────────────────────────────────────┼──────────────────────────────────────┐
                     │                                      │                                      │
                     ▼                                      ▼                                      ▼
      [M2: DbtLineageGraph]                 [M3: DbtPreaggAdvisor]               [M4: DbtDashboardBuilder]
   (dbt_lineage_joins_graph)             (dbt_semantic_preagg_advisor)          (dbt_build_dashboard_from_yaml)
   - Dependency DAG & tests               - Additive/Non-additive metrics        - Lightdash / Metabase YAML
   - Multi-Hop join path routing          - Time-grain Rollup DDL                - Metabase Model Cards (v50+)
   - ANSI SQL Join generator              - PG/BQ/Snowflake/ClickHouse/DuckDB    - 24-Col Collision-Free Grid
                     │                                      │                    - Parametric Filter Bindings
                     │                                      │                              │
                     └──────────────────────────────────────┼──────────────────────────────┘
                                                            │
                                                            ▼
                                                [M5: DbtYamlExporter]
                                             (dbt_semantic_export_yaml)
                                             - SemanticMemory ACTIVE rules
                                             - dbt schema.yml & MetricFlow
                                             - Audit provenance comments
                                             - Soft-deprecation handling
```

---

## Feature Inventory

| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | Recursive dbt File & AST Scanner | Deep scan models, sources, exposures, seeds, macros, and schema YAMLs | M1 | Survey R1 |
| 2 | dbt Docs & `doc('...')` Resolver | Parse `docs/*.md` blocks and resolve documentation references in models/columns | M1 | Lead Directive 1 |
| 3 | dbt Catalog Stats & Profiler | Ingest `catalog.json` table row counts, byte sizes, and column data profiling | M1 | Lead Directive 1 |
| 4 | MetricFlow & Semantic Layer Ingestion | Parse dbt Semantic Layer entities, dimensions, measures, and metrics | M1 | Lead Directive 2 |
| 5 | Architectural Tier Categorization | Classify models into `marts_fact`, `marts_dim`, `marts_report`, `intermediate`, `staging`, `source` | M1 | Survey R1 |
| 6 | Visual Metadata Ingestion | Parse `meta.metabase` and `meta.lightdash` formatting, colors, chart types | M1 | Lead Directive 3 |
| 7 | Tool `dbt_project_scan_deep` | Expose deep scanner tool with readOnlyHint and MCP 2025-11-25 envelope | M1 | Survey R1 |
| 8 | Model Dependency DAG Builder | Construct directed dependency graph across all dbt nodes, sources, and seeds | M2 | Survey R2 |
| 9 | Semantic Relationship Test Parser | Extract explicit relationship tests, foreign keys, and MetricFlow entity linkages | M2 | Survey R2 |
| 10 | Multi-Hop Shortest Join Path Finder | BFS / Dijkstra path resolution between distant models with confidence scoring | M2 | Survey R2 |
| 11 | ANSI SQL Join Generator | Generate clean multi-hop `LEFT JOIN` clauses with alias resolution | M2 | Survey R2 |
| 12 | Tool `dbt_lineage_joins_graph` | Expose lineage and join graph resolver tool via MCP | M2 | Survey R2 |
| 13 | Metric & Measure Additivity Analyzer | Classify additive, semi-additive, and non-additive metrics / measures | M3 | Survey R3 |
| 14 | Time-Grain Rollup Engine | Formulate rollups across `day`, `week`, `month`, `quarter`, `year` | M3 | Survey R3 |
| 15 | Multi-Dialect Materialized View DDL | Generate valid DDL for PostgreSQL, BigQuery, Snowflake, ClickHouse, DuckDB/MySQL | M3 | Survey R3 |
| 16 | Query Acceleration & Scan Estimator | Calculate 10x-100x query speedup index and byte scan reduction percentage | M3 | Survey R3 |
| 17 | Tool `dbt_semantic_preagg_advisor` | Expose Cube.js-style pre-aggregation advisor tool via MCP | M3 | Survey R3 |
| 18 | Lightdash & Exposure YAML Parser | Parse `exposures.yml` and dbt YAML metric card definitions | M4 | Survey R4 |
| 19 | Metabase Question & Model Generator | Construct native Metabase Questions and Metabase Model cards (v50+) | M4 | Lead Directive 4 |
| 20 | 24-Column Collision-Free Grid Placer | Compute responsive 24-column coordinates ensuring 0 card overlaps | M4 | Lead Directive 4 |
| 21 | Template Tag Filter Binder | Bind global interactive dashboard filters to SQL variables `{{variable}}` | M4 | Lead Directive 4 |
| 22 | Tool `dbt_build_dashboard_from_yaml` | Expose dashboard builder tool via MCP (registered in WRITE_TOOLS) | M4 | Survey R4 |
| 23 | Semantic Governance Rule Filter | Filter `SemanticMemory` for `ACTIVE` rules; exclude pending/unapproved rules | M5 | Survey R5 |
| 24 | dbt Core & MetricFlow YAML Serializer | Format clean `schema.yml`, `semantic_models.yml`, and `metrics.yml` blocks | M5 | Survey R5 |
| 25 | Audit Provenance & Soft-Deprecation | Embed stakeholder comments and format deprecated rules with reason comments | M5 | Survey R5 |
| 26 | Tool `dbt_semantic_export_yaml` | Expose semantic-to-YAML export tool via MCP | M5 | Survey R5 |
| 27 | E2E Regression & Scale Verification | Pass 100% of test suites with >=595 passing tests, 0 regressions, and Tier 5 audit | M6 | Survey Spec |

---

## Milestones

| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Deep dbt Scanner & Metadata Profiler | `src/dbt/dbt-deep-scanner.js`, `src/mcp/handlers/dbt-semantic.js`, `src/mcp/tool-registry.js` (R1, Features 1-7) | None | DONE |
| M2 | Lineage DAG & Multi-Hop Join Resolver | `src/dbt/lineage-joins.js`, `src/mcp/handlers/dbt-semantic.js`, `src/mcp/tool-registry.js` (R2, Features 8-12) | M1 | DONE |
| M3 | Cube.js Pre-Aggregation Advisor | `src/dbt/preagg-advisor.js`, `src/mcp/handlers/dbt-semantic.js`, `src/mcp/tool-registry.js` (R3, Features 13-17) | M1 | DONE |
| M4 | Lightdash Code-as-BI Dashboard Builder | `src/dbt/dbt-dashboard-builder.js`, `src/mcp/handlers/dbt-semantic.js`, `src/mcp/tool-registry.js`, `src/mcp/tool-router.js` (R4, Features 18-22) | M1 | DONE |
| M5 | Omni.co Semantic-to-YAML Exporter | `src/dbt/dbt-yaml-exporter.js`, `src/mcp/handlers/dbt-semantic.js`, `src/mcp/tool-registry.js` (R5, Features 23-26) | M1 | DONE |
| M6 | Final E2E Suite & Adversarial Hardening | E2E test pass (Tiers 1-4, >=595 passing tests) + Tier 5 Adversarial Coverage Hardening | M1, M2, M3, M4, M5 | DONE |

---

## Interface Contracts

### 1. `DbtDeepScanner` (`src/dbt/dbt-deep-scanner.js`)
```javascript
export class DbtDeepScanner {
  constructor(options = {})
  async scanProject(projectDir, options = {}) // returns ProjectScanResult
  parseDocBlocks(projectDir) // returns Map<string, string>
  parseCatalog(catalogPath) // returns CatalogStats
  parseMetricFlow(content) // returns SemanticModels & Metrics
  resolveDocReference(docRef, docMap) // returns resolved string
}
```

### 2. `DbtLineageGraph` (`src/dbt/lineage-joins.js`)
```javascript
export class DbtLineageGraph {
  constructor(scanResult)
  buildGraph(models, relationships)
  findJoinPath(sourceModel, targetModel, options = {}) // returns JoinPathResult
  getAllUpstream(modelName) // returns string[]
  getAllDownstream(modelName) // returns string[]
  generateJoinSql(path, baseAlias) // returns SQL string
}
```

### 3. `DbtPreaggAdvisor` (`src/dbt/preagg-advisor.js`)
```javascript
export class DbtPreaggAdvisor {
  constructor(scanResult)
  advisePreaggregations(options = {}) // returns PreaggRecommendation[]
  generateRollupDDL(options = {}) // returns { ddl, index_ddl, refresh_strategy }
  estimateSpeedup(rawRowCount, timeGrain, dimensions) // returns { speedup, dataScanReduction }
}
```

### 4. `DbtDashboardBuilder` (`src/dbt/dbt-dashboard-builder.js`)
```javascript
export class DbtDashboardBuilder {
  constructor(client, options = {})
  async buildDashboardFromYaml(options = {}) // returns DashboardBuildResult
  calculateGridCoordinates(cards, options = {}) // returns PlacedCard[]
  generateFilterMappings(cards, filters) // returns ParameterMapping[]
}
```

### 5. `DbtYamlExporter` (`src/dbt/dbt-yaml-exporter.js`)
```javascript
export class DbtYamlExporter {
  constructor(semanticMemory, options = {})
  exportToYaml(options = {}) // returns { yaml_content, exported_count, skipped_count }
  formatDbtSchemaYaml(rules, options = {}) // returns YAML string
  formatMetricFlowYaml(rules, options = {}) // returns YAML string
}
```

---

## Code Layout

- **Source Code**:
  - `src/dbt/dbt-deep-scanner.js` (M1: Scanner, Doc blocks, Catalog profiling, MetricFlow)
  - `src/dbt/lineage-joins.js` (M2: DAG, Relationship tests, Dijkstra/BFS Joins)
  - `src/dbt/preagg-advisor.js` (M3: Additivity, Multi-dialect Rollup DDLs)
  - `src/dbt/dbt-dashboard-builder.js` (M4: Lightdash Code-as-BI, Metabase Models v50+, 24-col grid)
  - `src/dbt/dbt-yaml-exporter.js` (M5: SemanticMemory-to-YAML export, governance comments)
  - `src/mcp/tool-registry.js` (All 5 MCP tool definitions + MCP 2025-11-25 hints)
  - `src/mcp/handlers/dbt-semantic.js` (Handler methods with structured envelope)
  - `src/mcp/tool-router.js` (`WRITE_TOOLS` registration for `dbt_build_dashboard_from_yaml`)
  - `src/mcp/server.js` (Tool dispatcher integration)
- **Test Code**:
  - `tests/unit/dbt-deep-scanner.test.js`
  - `tests/unit/lineage-joins.test.js`
  - `tests/unit/preagg-advisor.test.js`
  - `tests/unit/dbt-dashboard-builder.test.js`
  - `tests/unit/dbt-yaml-exporter.test.js`
  - `tests/integration/dbt-code-as-bi-workflow.test.js`
