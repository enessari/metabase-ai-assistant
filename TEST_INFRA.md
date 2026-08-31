# E2E Test Infra: metabase-ai-assistant (dbt Semantic BI)

## Test Philosophy
- Opaque-box, requirement-driven. Derives test assertions directly from `ORIGINAL_REQUEST.md`, dbt specifications, Metabase API contracts, and MCP 2025-11-25 protocols.
- Methodology: Category-Partition + Boundary Value Analysis (BVA) + Pairwise Combinatorial Testing + Real-World Workload Testing.

---

## Feature Inventory & Test Coverage Matrix

| # | Feature | Source (Requirement) | Tier 1 (Feature) | Tier 2 (Boundary) | Tier 3 (Pairwise) | Tier 4 (E2E Scenarios) |
|---|---------|----------------------|:----------------:|:-----------------:|:-----------------:|:----------------------:|
| 1 | Recursive dbt File & AST Scanner | ORIGINAL_REQUEST §R1 | 5 | 5 | ✓ | ✓ |
| 2 | dbt Docs & `doc('...')` Resolver | Lead Directive 1 | 5 | 5 | ✓ | ✓ |
| 3 | dbt Catalog Stats & Profiler | Lead Directive 1 | 5 | 5 | ✓ | ✓ |
| 4 | MetricFlow & Semantic Layer Ingestion | Lead Directive 2 | 5 | 5 | ✓ | ✓ |
| 5 | Architectural Tier Categorization | ORIGINAL_REQUEST §R1 | 5 | 5 | ✓ | ✓ |
| 6 | Visual Metadata Ingestion | Lead Directive 3 | 5 | 5 | ✓ | ✓ |
| 7 | Tool `dbt_project_scan_deep` | ORIGINAL_REQUEST §R1 | 5 | 5 | ✓ | ✓ |
| 8 | Model Dependency DAG Builder | ORIGINAL_REQUEST §R2 | 5 | 5 | ✓ | ✓ |
| 9 | Semantic Relationship Test Parser | ORIGINAL_REQUEST §R2 | 5 | 5 | ✓ | ✓ |
| 10 | Multi-Hop Shortest Join Path Finder | ORIGINAL_REQUEST §R2 | 5 | 5 | ✓ | ✓ |
| 11 | ANSI SQL Join Generator | ORIGINAL_REQUEST §R2 | 5 | 5 | ✓ | ✓ |
| 12 | Tool `dbt_lineage_joins_graph` | ORIGINAL_REQUEST §R2 | 5 | 5 | ✓ | ✓ |
| 13 | Metric & Measure Additivity Analyzer | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| 14 | Time-Grain Rollup Engine | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| 15 | Multi-Dialect Materialized View DDL | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| 16 | Query Acceleration & Scan Estimator | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| 17 | Tool `dbt_semantic_preagg_advisor` | ORIGINAL_REQUEST §R3 | 5 | 5 | ✓ | ✓ |
| 18 | Lightdash & Exposure YAML Parser | ORIGINAL_REQUEST §R4 | 5 | 5 | ✓ | ✓ |
| 19 | Metabase Question & Model Generator | Lead Directive 4 | 5 | 5 | ✓ | ✓ |
| 20 | 24-Column Collision-Free Grid Placer | Lead Directive 4 | 5 | 5 | ✓ | ✓ |
| 21 | Template Tag Filter Binder | Lead Directive 4 | 5 | 5 | ✓ | ✓ |
| 22 | Tool `dbt_build_dashboard_from_yaml` | ORIGINAL_REQUEST §R4 | 5 | 5 | ✓ | ✓ |
| 23 | Semantic Governance Rule Filter | ORIGINAL_REQUEST §R5 | 5 | 5 | ✓ | ✓ |
| 24 | dbt Core & MetricFlow YAML Serializer | ORIGINAL_REQUEST §R5 | 5 | 5 | ✓ | ✓ |
| 25 | Audit Provenance & Soft-Deprecation | ORIGINAL_REQUEST §R5 | 5 | 5 | ✓ | ✓ |
| 26 | Tool `dbt_semantic_export_yaml` | ORIGINAL_REQUEST §R5 | 5 | 5 | ✓ | ✓ |

---

## Test Architecture

- **Test Runner**: Jest `^29.7.0` running with `--experimental-vm-modules` for ESM.
- **Invocation Command**: `npm test` (or `node --experimental-vm-modules node_modules/jest/bin/jest.js --forceExit`)
- **Unit Test Runner**: `npm run test:unit <test_path>`
- **Integration Test Runner**: `npm run test:integration <test_path>`
- **Pass/Fail Semantics**: All test suites must exit with 0 errors, 0 failed tests, and 0 memory leaks.
- **Directory Layout**:
  - `tests/unit/dbt-deep-scanner.test.js` (R1 Unit Suite)
  - `tests/unit/lineage-joins.test.js` (R2 Unit Suite)
  - `tests/unit/preagg-advisor.test.js` (R3 Unit Suite)
  - `tests/unit/dbt-dashboard-builder.test.js` (R4 Unit Suite)
  - `tests/unit/dbt-yaml-exporter.test.js` (R5 Unit Suite)
  - `tests/integration/dbt-code-as-bi-workflow.test.js` (E2E Integration Suite)

---

## Real-World Application Scenarios (Tier 4)

| # | Scenario | Features Exercised | Complexity |
|---|----------|--------------------|------------|
| 1 | Jaffle Shop / Ecommerce Complete Lifecycle (Scan -> Lineage -> Preagg -> Lightdash Dashboard -> Omni Export) | F1-F26 (All) | High |
| 2 | Multi-Hop Join Assembly (`fct_order_items -> fct_orders -> dim_customers -> dim_regions`) | F8, F9, F10, F11, F12 | High |
| 3 | Cube.js Multi-Dialect Pre-Aggregation Rollup Advisory (PG / BQ / Snowflake / ClickHouse) | F13, F14, F15, F16, F17 | Medium |
| 4 | Lightdash Exposure-to-Metabase Dashboard Build (4+ cards, 24-col grid, `{{order_date}}` filter) | F18, F19, F20, F21, F22 | High |
| 5 | Omni.co SemanticMemory Controlled YAML Export with Soft-Deprecation Governance | F23, F24, F25, F26 | Medium |

---

## Coverage Thresholds & Baseline

- **Current Baseline**: 32 test suites passed, 583 tests passed, 0 failures.
- **Target Requirement**: >= 595 total passing tests with 0 regressions.
- **Tier 1 (Feature Coverage)**: >= 5 tests per major feature.
- **Tier 2 (Boundary & Corner Cases)**: >= 5 tests per major feature.
- **Tier 3 (Pairwise & Combinations)**: Full coverage of cross-module data flow.
- **Tier 4 (Real-World Application Scenarios)**: >= 5 end-to-end workload scenarios.
