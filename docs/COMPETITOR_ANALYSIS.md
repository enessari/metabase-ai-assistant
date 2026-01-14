# Metabase MCP Server - Competitor Analysis and Feature Comparison

## Current Tool Count: 107 tools

**Author**: Abdullah Enes SARI
**Company**: ONMARTECH LLC
**Last Updated**: January 2026

---

## Overview

This document provides a comprehensive analysis of Metabase MCP server implementations, comparing features across different projects and identifying unique capabilities.

---

## Tools Added in Version 2.0 (32 tools)

### User Management
- `mb_user_list` - List all users with filtering
- `mb_user_get` - Get user details by ID
- `mb_user_create` - Create new Metabase user
- `mb_user_update` - Update user information
- `mb_user_disable` - Disable/deactivate user

### Permission Groups
- `mb_permission_group_list` - List all permission groups
- `mb_permission_group_create` - Create permission group
- `mb_permission_group_delete` - Delete permission group
- `mb_permission_group_add_user` - Add user to group
- `mb_permission_group_remove_user` - Remove user from group

### Collection Permissions
- `mb_collection_permissions_get` - Get collection permissions graph
- `mb_collection_permissions_update` - Update collection permissions

### Card CRUD Operations
- `mb_card_get` - Get card/question details
- `mb_card_update` - Update card properties
- `mb_card_delete` - Delete card permanently
- `mb_card_archive` - Archive card (soft delete)
- `mb_card_data` - Get card result data

### Dashboard CRUD Operations
- `mb_dashboard_get` - Get dashboard details
- `mb_dashboard_update` - Update dashboard properties
- `mb_dashboard_delete` - Delete dashboard
- `mb_dashboard_card_update` - Update card position/size
- `mb_dashboard_card_remove` - Remove card from dashboard

### Copy/Clone Operations
- `mb_card_copy` - Copy card to new location
- `mb_card_clone` - Clone card with modifications
- `mb_dashboard_copy` - Deep copy dashboard with cards
- `mb_collection_copy` - Copy entire collection

### Search and Bookmarks
- `mb_search` - Global search across Metabase
- `mb_bookmark_create` - Create bookmark
- `mb_bookmark_list` - List user bookmarks
- `mb_bookmark_delete` - Remove bookmark

### Segments
- `mb_segment_create` - Create reusable segment filter
- `mb_segment_list` - List all segments

### Database Operations
- `db_sync_schema` - Trigger database schema sync
- `mb_cache_invalidate` - Invalidate query cache

---

## Competitor Repositories Analyzed

### 1. imlewc/metabase-server (TypeScript)
- **Language**: TypeScript
- **Architecture**: MCP SDK with resource templates
- **Tool Count**: Approximately 30 tools
- **Unique Features**:
  - Permission groups management
  - User management operations
  - Collection permissions graph
  - User-group membership management
  - Resource Templates (URI patterns)
  - Dashboard card update/remove

### 2. sazboxai/MCP_MetaBase (Python)
- **Language**: Python
- **Tool Count**: Approximately 15 tools
- **Unique Features**:
  - Database relationship visualization
  - Web interface for testing
  - Docker deployment support
  - Encrypted API key storage

### 3. vvaezian/metabase_api_python (Python)
- **Language**: Python (API wrapper, not MCP)
- **Unique Features**:
  - Clone card with table retargeting
  - Copy operations (card, collection, dashboard, pulse)
  - Segment creation
  - Archive operations
  - Field metadata updates
  - Card data export (JSON/CSV)
  - Global search

### 4. Metabase Official API
- **Endpoints**: 50+ API categories
- **Core APIs**: action, activity, alert, api-key, bookmark, cache, card, channel, collection, dashboard, database, dataset, email, embed, field, geojson, llm, metabot, model-index, native-query-snippet, notify, permissions, persist, public, pulse, revision, search, segment, stale, table, task, tiles, timeline, timeline-event, user, util
- **Enterprise APIs**: advanced-config-logs, advanced-permissions, audit-app-user, content-verification-review, query-reference-validation, sandbox, sso, scim, serialization

---

## Feature Comparison Matrix

| Category | Feature | Ours | imlewc | sazboxai | vvaezian | Metabase API |
|----------|---------|------|--------|----------|----------|--------------|
| **Database** | List databases | Yes | Yes | Yes | Yes | Yes |
| | Get schemas | Yes | Yes | Yes | Yes | Yes |
| | Get tables | Yes | Yes | Yes | Yes | Yes |
| | Execute SQL | Yes | Yes | Yes | Yes | Yes |
| | Create view | Yes | No | No | No | No |
| | Create materialized view | Yes | No | No | No | No |
| | Create index | Yes | No | No | No | No |
| | VACUUM/ANALYZE | Yes | No | No | No | No |
| | Query EXPLAIN | Yes | No | No | No | No |
| | Table stats | Yes | No | No | No | No |
| | Index usage | Yes | No | No | No | No |
| | Test speed | Yes | No | No | No | No |
| | Relationship detection | Yes | No | No | No | No |
| | Relationship visualization | No | No | Yes | No | No |
| **Questions/Cards** | Create question | Yes | Yes | Yes | Yes | Yes |
| | List questions | Yes | Yes | Yes | Yes | Yes |
| | Parametric questions | Yes | No | No | No | Yes |
| | Clone/Copy card | Yes | No | No | Yes | No |
| | Get card data | Yes | Yes | No | Yes | Yes |
| | Update card | Yes | Yes | No | Yes | Yes |
| | Delete card | Yes | Yes | No | Yes | Yes |
| | Archive card | Yes | No | No | Yes | Yes |
| **Dashboards** | Create dashboard | Yes | Yes | Yes | Yes | Yes |
| | List dashboards | Yes | Yes | Yes | Yes | Yes |
| | Add card | Yes | Yes | Yes | Yes | Yes |
| | Update cards | Yes | Yes | No | No | Yes |
| | Remove card | Yes | Yes | No | No | Yes |
| | Dashboard filters | Yes | Yes | No | No | Yes |
| | Layout optimize | Yes | No | No | No | No |
| | Executive template | Yes | No | No | No | No |
| | Copy dashboard | Yes | No | No | Yes | No |
| **Collections** | Create collection | Yes | Yes | No | No | Yes |
| | List collections | Yes | Yes | No | Yes | Yes |
| | Move item | Yes | No | No | No | Yes |
| | Get permissions | Yes | Yes | No | No | Yes |
| | Update permissions | Yes | Yes | No | No | Yes |
| | Copy collection | Yes | No | No | Yes | No |
| **Metrics** | Create metric | Yes | No | No | No | Yes |
| **Segments** | Create segment | Yes | No | No | Yes | Yes |
| **Alerts** | Create alert | Yes | No | No | No | Yes |
| | List alerts | Yes | No | No | No | Yes |
| **Pulses** | Create pulse | Yes | No | No | No | Yes |
| | Copy pulse | No | No | No | Yes | No |
| **Fields** | Get metadata | Yes | No | No | No | Yes |
| | Get values | Yes | No | No | No | Yes |
| | Update column | No | No | No | Yes | Yes |
| **Tables** | Get metadata | Yes | No | No | No | Yes |
| **Embedding** | Generate URL | Yes | No | No | No | Yes |
| | Get settings | Yes | No | No | No | Yes |
| **Visualization** | Set settings | Yes | No | No | No | Yes |
| | Get recommendations | Yes | No | No | No | No |
| **Users** | List users | Yes | Yes | No | No | Yes |
| | Create user | Yes | Yes | No | No | Yes |
| | Update user | Yes | Yes | No | No | Yes |
| | Disable user | Yes | Yes | No | No | Yes |
| | Get user | Yes | Yes | No | No | Yes |
| **Permission Groups** | List groups | Yes | Yes | No | No | Yes |
| | Create group | Yes | Yes | No | No | Yes |
| | Delete group | Yes | Yes | No | No | Yes |
| | Add user | Yes | Yes | No | No | Yes |
| | Remove user | Yes | Yes | No | No | Yes |
| **Search** | Global search | Yes | No | No | Yes | Yes |
| **AI Features** | SQL generate | Yes | No | No | No | No |
| | SQL optimize | Yes | No | No | No | No |
| | SQL explain | Yes | No | No | No | No |
| | Auto describe | Yes | No | No | No | No |
| | Relationship suggest | Yes | No | No | No | No |
| **Activity/Logging** | Init logging | Yes | No | No | No | No |
| | Session summary | Yes | No | No | No | No |
| | Operation stats | Yes | No | No | No | No |
| | Error analysis | Yes | No | No | No | No |
| | Performance insights | Yes | No | No | No | No |
| **Definitions** | Business terms | Yes | No | No | No | No |
| | Metric definitions | Yes | No | No | No | No |
| | Templates | Yes | No | No | No | No |
| **Resource Templates** | URI patterns | No | Yes | No | No | N/A |

---

## Unique Strengths of This Implementation

1. **Database Maintenance Tools** - VACUUM, ANALYZE, EXPLAIN, statistics
2. **Direct DDL Execution** - Views, Materialized Views, Indexes with safety controls
3. **AI-Powered Features** - SQL generation, optimization, explanation
4. **Business Definitions** - Term dictionary, metric definitions, templates
5. **Activity Logging** - Session tracking, error analysis, performance insights
6. **Parametric Questions** - Advanced filter templates
7. **Dashboard Templates** - Executive dashboard auto-generation
8. **Layout Optimization** - Automatic dashboard card arrangement
9. **Comprehensive User Management** - Full CRUD with permission groups
10. **Copy/Clone Operations** - Deep copy for cards, dashboards, collections

---

## Implementation Summary

| Priority | Tool Count | Status |
|----------|------------|--------|
| Phase 1 (User/Permission) | 17 tools | Completed |
| Phase 2 (CRUD Operations) | 10 tools | Completed |
| Phase 3 (Copy/Search) | 5 tools | Completed |
| **Total New in v2.0** | **32 tools** | **Completed** |
| **Grand Total** | **107 tools** | **Production Ready** |

---

## Future Enhancements (Phase 4)

- Native Query Snippets management
- Timeline and Timeline Events
- Pulse copy functionality
- Field value updates
- Relationship visualization diagrams
- Advanced embedding options

---

## Conclusion

With 107 tools, this implementation provides the most comprehensive Metabase MCP integration available. It combines the best features from all analyzed competitors while adding unique AI-powered capabilities and database maintenance tools.

---

Copyright 2024-2026 ONMARTECH LLC
Developed by Abdullah Enes SARI
