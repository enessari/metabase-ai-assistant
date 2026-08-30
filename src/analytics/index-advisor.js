import { logger } from '../utils/logger.js';

/**
 * Normalizes SQL dialect name
 * @param {string} dialect 
 * @returns {string}
 */
export function normalizeDialect(dialect) {
  if (!dialect) return 'postgres';
  const d = String(dialect).toLowerCase().trim();
  if (d.includes('postgres') || d.includes('pg')) return 'postgres';
  if (d.includes('mysql') || d.includes('mariadb')) return 'mysql';
  if (d.includes('sqlite') || d.includes('h2')) return 'sqlite';
  if (d.includes('bigquery') || d.includes('bq')) return 'bigquery';
  if (d.includes('snowflake')) return 'snowflake';
  if (d.includes('redshift')) return 'redshift';
  if (d.includes('clickhouse')) return 'clickhouse';
  if (d.includes('sqlserver') || d.includes('mssql')) return 'sqlserver';
  if (d.includes('oracle')) return 'oracle';
  return 'postgres';
}

/**
 * Cleans identifier (removes quotes, backticks, brackets)
 * @param {string} str 
 * @returns {string}
 */
function cleanIdentifier(str) {
  if (!str) return '';
  return str.replace(/[`"'[\]]/g, '').trim();
}

/**
 * Extracts AST-like structured components from a SQL query
 * @param {string} sql 
 * @returns {object}
 */
export function extractQueryAST(sql) {
  if (!sql || typeof sql !== 'string') {
    throw new Error('SQL query must be a non-empty string');
  }

  // Remove comments and semicolons
  const cleanSql = sql
    .replace(/--.*$/gm, '')
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/;/g, ' ')
    .trim();

  const tables = [];
  const tableAliasMap = {}; // alias -> table_name
  const filterPredicates = [];
  const joinConditions = [];
  const groupByColumns = [];
  const orderByColumns = [];
  const aggregations = [];
  const selectColumns = [];

  // 1. Extract Aggregations from SELECT
  const aggRegex = /\b(COUNT\s*\(\s*(?:DISTINCT\s+)?[^)]+\)|SUM\s*\([^)]+\)|AVG\s*\([^)]+\)|MIN\s*\([^)]+\)|MAX\s*\([^)]+\)|ARRAY_AGG\s*\([^)]+\)|STRING_AGG\s*\([^)]+\))/gi;
  let aggMatch;
  while ((aggMatch = aggRegex.exec(cleanSql)) !== null) {
    aggregations.push(aggMatch[1].trim());
  }

  // 2. Extract SELECT projection columns (first level)
  const selectMatch = cleanSql.match(/SELECT\s+(DISTINCT\s+)?([\s\S]+?)\s+FROM\b/i);
  if (selectMatch && selectMatch[2]) {
    const rawCols = selectMatch[2].split(/,(?![^(]*\))/);
    for (const rawCol of rawCols) {
      const trimmed = rawCol.trim();
      const asMatch = trimmed.match(/^(.*?)\s+AS\s+([\w`"'-]+)$/i) || trimmed.match(/^(.*?)\s+([\w`"'-]+)$/);
      const expr = asMatch ? asMatch[1].trim() : trimmed;
      const alias = asMatch ? asMatch[2].trim() : expr;
      const cleaned = cleanIdentifier(alias);
      if (cleaned && !cleaned.includes('(') && cleaned !== '*') {
        selectColumns.push(cleaned);
      }
    }
  }

  // 3. Extract Tables & Aliases from FROM and JOINs
  // Match FROM clause
  const fromMatch = cleanSql.match(/FROM\s+([\s\S]+?)(?=\b(?:WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET|UNION|WINDOW|FETCH)\b|$)/i);
  if (fromMatch && fromMatch[1]) {
    const fromBody = fromMatch[1].trim();

    // Match all JOINs and main table
    // E.g. FROM table1 t1 JOIN table2 t2 ON ... LEFT JOIN table3 t3 ON ...
    const joinSplitRegex = /\b(?:(?:LEFT|RIGHT|FULL|INNER|CROSS|OUTER|NATURAL)\s+)?JOIN\b/i;
    const parts = fromBody.split(joinSplitRegex);

    // First part is the base table (or comma-separated tables)
    const baseTables = parts[0].split(/,(?![^(]*\))/);
    for (const bt of baseTables) {
      const trimmed = bt.trim();
      if (!trimmed || trimmed.startsWith('(')) continue; // ignore subqueries for base name
      const tokens = trimmed.split(/\s+/).filter(Boolean);
      if (tokens.length >= 1) {
        const tblName = cleanIdentifier(tokens[0]);
        let alias = tblName;
        if (tokens.length >= 2) {
          if (tokens[1].toLowerCase() === 'as' && tokens.length >= 3) {
            alias = cleanIdentifier(tokens[2]);
          } else {
            alias = cleanIdentifier(tokens[1]);
          }
        }
        if (tblName && !tables.includes(tblName)) {
          tables.push(tblName);
        }
        tableAliasMap[alias] = tblName;
        tableAliasMap[tblName] = tblName;
      }
    }

    // Process JOIN sections
    const joinMatches = [
      ...cleanSql.matchAll(/\b(?:(?:LEFT|RIGHT|FULL|INNER|CROSS|OUTER|NATURAL)\s+)?JOIN\s+([^\s,()]+)(?:\s+(?:AS\s+)?([^\s,()]+))?\s+ON\s+([\s\S]+?)(?=\b(?:(?:LEFT|RIGHT|FULL|INNER|CROSS|OUTER|NATURAL)\s+)?JOIN\b|\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|$)/gi)
    ];

    for (const jm of joinMatches) {
      const tblName = cleanIdentifier(jm[1]);
      let alias = jm[2] ? cleanIdentifier(jm[2]) : tblName;
      if (alias.toLowerCase() === 'on') alias = tblName;
      const onCondition = jm[3].trim();

      if (tblName && !tables.includes(tblName)) {
        tables.push(tblName);
      }
      tableAliasMap[alias] = tblName;
      tableAliasMap[tblName] = tblName;

      // Extract Join conditions (e.g., a.id = b.user_id)
      const onParts = onCondition.split(/\bAND\b|\bOR\b/i);
      for (const cond of onParts) {
        const condTrim = cond.trim();
        const eqMatch = condTrim.match(/([\w`"'.]+)\s*(=|!=|<>|<=|>=|<|>)\s*([\w`"'.]+)/);
        if (eqMatch) {
          const leftRaw = cleanIdentifier(eqMatch[1]);
          const op = eqMatch[2];
          const rightRaw = cleanIdentifier(eqMatch[3]);

          const leftSplit = leftRaw.split('.');
          const rightSplit = rightRaw.split('.');

          const leftTable = leftSplit.length > 1 ? (tableAliasMap[leftSplit[0]] || leftSplit[0]) : (tables[0] || 'unknown');
          const leftCol = leftSplit.length > 1 ? leftSplit[1] : leftSplit[0];

          const rightTable = rightSplit.length > 1 ? (tableAliasMap[rightSplit[0]] || rightSplit[0]) : (tblName || 'unknown');
          const rightCol = rightSplit.length > 1 ? rightSplit[1] : rightSplit[0];

          joinConditions.push({
            raw: condTrim,
            left: { table: leftTable, column: leftCol, raw: leftRaw },
            right: { table: rightTable, column: rightCol, raw: rightRaw },
            operator: op,
          });
        }
      }
    }
  }

  // 4. Extract WHERE Filters
  const whereMatch = cleanSql.match(/\bWHERE\s+([\s\S]+?)(?=\b(?:GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET|UNION|WINDOW|FETCH)\b|$)/i);
  if (whereMatch && whereMatch[1]) {
    const whereBody = whereMatch[1].trim();

    // Protect BETWEEN ... AND ... from being split
    const maskedWhere = whereBody.replace(/\bBETWEEN\s+('[^']+'|\d+|[\w`"'.()]+)\s+AND\s+('[^']+'|\d+|[\w`"'.()]+)/gi, 'BETWEEN $1 __BETWEEN_AND__ $2');

    // Split by AND (and respect top-level clauses)
    const clauses = maskedWhere.split(/\bAND\b/i);

    for (const rawClause of clauses) {
      const c = rawClause.replace(/__BETWEEN_AND__/g, 'AND').trim();
      if (!c) continue;

      // Check IN predicate: col IN (...)
      const inMatch = c.match(/([\w`"'.]+)\s+(NOT\s+)?IN\s*\(([^)]+)\)/i);
      if (inMatch) {
        const colRaw = cleanIdentifier(inMatch[1]);
        const colSplit = colRaw.split('.');
        const tbl = colSplit.length > 1 ? (tableAliasMap[colSplit[0]] || colSplit[0]) : (tables[0] || 'unknown');
        const col = colSplit.length > 1 ? colSplit[1] : colSplit[0];
        filterPredicates.push({
          raw: c,
          table: tbl,
          column: col,
          operator: inMatch[2] ? 'NOT IN' : 'IN',
          type: 'in',
          values: inMatch[3].split(',').map(v => v.trim().replace(/^['"]|['"]$/g, '')),
        });
        continue;
      }

      // Check BETWEEN predicate: col BETWEEN val1 AND val2
      const betweenMatch = c.match(/([\w`"'.]+)\s+(NOT\s+)?BETWEEN\s+(.+?)\s+AND\s+(.+)/i);
      if (betweenMatch) {
        const colRaw = cleanIdentifier(betweenMatch[1]);
        const colSplit = colRaw.split('.');
        const tbl = colSplit.length > 1 ? (tableAliasMap[colSplit[0]] || colSplit[0]) : (tables[0] || 'unknown');
        const col = colSplit.length > 1 ? colSplit[1] : colSplit[0];
        filterPredicates.push({
          raw: c,
          table: tbl,
          column: col,
          operator: betweenMatch[2] ? 'NOT BETWEEN' : 'BETWEEN',
          type: 'range',
        });
        continue;
      }

      // Check IS NULL / IS NOT NULL
      const isNullMatch = c.match(/([\w`"'.]+)\s+IS\s+(NOT\s+)?NULL/i);
      if (isNullMatch) {
        const colRaw = cleanIdentifier(isNullMatch[1]);
        const colSplit = colRaw.split('.');
        const tbl = colSplit.length > 1 ? (tableAliasMap[colSplit[0]] || colSplit[0]) : (tables[0] || 'unknown');
        const col = colSplit.length > 1 ? colSplit[1] : colSplit[0];
        filterPredicates.push({
          raw: c,
          table: tbl,
          column: col,
          operator: isNullMatch[2] ? 'IS NOT NULL' : 'IS NULL',
          type: isNullMatch[2] ? 'is_not_null' : 'equality',
        });
        continue;
      }

      // Check LIKE / ILIKE
      const likeMatch = c.match(/([\w`"'.]+)\s+(NOT\s+)?(I?LIKE)\s+['"]([^'"]+)['"]/i);
      if (likeMatch) {
        const colRaw = cleanIdentifier(likeMatch[1]);
        const colSplit = colRaw.split('.');
        const tbl = colSplit.length > 1 ? (tableAliasMap[colSplit[0]] || colSplit[0]) : (tables[0] || 'unknown');
        const col = colSplit.length > 1 ? colSplit[1] : colSplit[0];
        const val = likeMatch[4];
        const isPrefix = val.endsWith('%') && !val.startsWith('%');
        filterPredicates.push({
          raw: c,
          table: tbl,
          column: col,
          operator: (likeMatch[2] || '') + likeMatch[3],
          type: isPrefix ? 'range' : 'like_wildcard',
          value: val,
          isPrefix,
        });
        continue;
      }

      // Check Standard Comparisons (=, !=, <>, <, <=, >, >=)
      const compMatch = c.match(/([\w`"'.]+)\s*(=|!=|<>|<=|>=|<|>)\s*(.+)/);
      if (compMatch) {
        const colRaw = cleanIdentifier(compMatch[1]);
        const op = compMatch[2];
        const val = compMatch[3].trim().replace(/^['"]|['"]$/g, '');
        const colSplit = colRaw.split('.');
        const tbl = colSplit.length > 1 ? (tableAliasMap[colSplit[0]] || colSplit[0]) : (tables[0] || 'unknown');
        const col = colSplit.length > 1 ? colSplit[1] : colSplit[0];

        const isEquality = op === '=';
        filterPredicates.push({
          raw: c,
          table: tbl,
          column: col,
          operator: op,
          value: val,
          type: isEquality ? 'equality' : 'range',
        });
      }
    }
  }

  // 5. Extract GROUP BY Columns
  const groupByMatch = cleanSql.match(/\bGROUP\s+BY\s+([\s\S]+?)(?=\b(?:HAVING|ORDER\s+BY|LIMIT|OFFSET|UNION|WINDOW|FETCH)\b|$)/i);
  if (groupByMatch && groupByMatch[1]) {
    const rawGroups = groupByMatch[1].split(/,(?![^(]*\))/);
    for (const g of rawGroups) {
      const trimmed = g.trim();
      const cleaned = cleanIdentifier(trimmed);
      const colSplit = cleaned.split('.');
      const tbl = colSplit.length > 1 ? (tableAliasMap[colSplit[0]] || colSplit[0]) : (tables[0] || 'unknown');
      const col = colSplit.length > 1 ? colSplit[1] : colSplit[0];
      groupByColumns.push({
        raw: trimmed,
        table: tbl,
        column: col,
      });
    }
  }

  // 6. Extract ORDER BY Columns
  const orderByMatch = cleanSql.match(/\bORDER\s+BY\s+([\s\S]+?)(?=\b(?:LIMIT|OFFSET|UNION|WINDOW|FETCH)\b|$)/i);
  if (orderByMatch && orderByMatch[1]) {
    const rawOrders = orderByMatch[1].split(/,(?![^(]*\))/);
    for (const o of rawOrders) {
      const trimmed = o.trim();
      if (!trimmed) continue;
      const dirMatch = trimmed.match(/^([\w`"'.]+)(?:\s+(ASC|DESC))?(?:\s+NULLS\s+(FIRST|LAST))?$/i);
      const expr = dirMatch ? dirMatch[1].trim() : trimmed.replace(/\s+(ASC|DESC).*$/i, '').trim();
      const direction = (dirMatch && dirMatch[2]) ? dirMatch[2].toUpperCase() : (/\bDESC\b/i.test(trimmed) ? 'DESC' : 'ASC');
      const cleaned = cleanIdentifier(expr);
      const colSplit = cleaned.split('.');
      const tbl = colSplit.length > 1 ? (tableAliasMap[colSplit[0]] || colSplit[0]) : (tables[0] || 'unknown');
      const col = colSplit.length > 1 ? colSplit[1] : colSplit[0];

      orderByColumns.push({
        raw: `${col}${direction === 'DESC' ? ' DESC' : ''}`,
        table: tbl,
        column: col,
        direction,
      });
    }
  }

  return {
    tables: tables.length > 0 ? tables : ['unknown_table'],
    tableAliasMap,
    filterPredicates,
    joinConditions,
    groupByColumns,
    orderByColumns,
    aggregations,
    selectColumns,
    hasAggregations: aggregations.length > 0 || groupByColumns.length > 0,
  };
}

/**
 * Parses dialect-specific EXPLAIN output to identify scans and bottlenecks
 * @param {string|object} explainResult 
 * @param {string} dialect 
 * @returns {object}
 */
export function parseExplainPlan(explainResult, dialect = 'postgres') {
  const normDialect = normalizeDialect(dialect);
  const scansDetected = [];
  let totalCost = null;
  let executionTimeMs = null;
  const bottlenecks = [];

  if (!explainResult) {
    return {
      explain_mode: 'heuristic_fallback',
      scans_detected: [],
      total_cost: null,
      execution_time_ms: null,
      bottlenecks: ['No EXPLAIN output available; using deterministic AST analysis.'],
    };
  }

  let textContent = '';
  if (typeof explainResult === 'string') {
    textContent = explainResult;
  } else if (Array.isArray(explainResult)) {
    // Array of explain rows or JSON plan
    if (explainResult.length > 0 && explainResult[0]['QUERY PLAN']) {
      textContent = explainResult.map(r => r['QUERY PLAN']).join('\n');
    } else {
      textContent = JSON.stringify(explainResult, null, 2);
    }
  } else if (typeof explainResult === 'object') {
    textContent = JSON.stringify(explainResult, null, 2);
  }

  // 1. PostgreSQL Parsing
  if (normDialect === 'postgres' || normDialect === 'redshift') {
    // Look for Seq Scan
    const seqScanMatches = [...textContent.matchAll(/Seq Scan on ([\w`"'-]+)/gi)];
    for (const sm of seqScanMatches) {
      scansDetected.push({
        table: cleanIdentifier(sm[1]),
        scan_type: 'Seq Scan',
        impact: 'HIGH',
        reason: `Full Sequential Scan detected on table '${cleanIdentifier(sm[1])}'. Missing index on filter predicates.`,
      });
      bottlenecks.push(`Sequential table scan on ${cleanIdentifier(sm[1])}`);
    }

    // Look for Bitmap Heap Scan without Index Only Scan
    if (textContent.includes('Bitmap Heap Scan')) {
      bottlenecks.push('Bitmap Heap Scan indicates non-covering index; candidate for composite or covering index.');
    }

    // Look for Sort Method external merge Disk
    if (/Sort Method:.*external/i.test(textContent)) {
      bottlenecks.push('Sort spilled to disk (external merge disk). Adding index on ORDER BY columns will eliminate memory spill.');
    }

    // Look for Cost
    const costMatch = textContent.match(/cost=[\d.]+\.\.([\d.]+)/);
    if (costMatch) {
      totalCost = parseFloat(costMatch[1]);
    }

    // Look for Execution Time
    const timeMatch = textContent.match(/Execution Time:\s*([\d.]+)\s*ms/i);
    if (timeMatch) {
      executionTimeMs = parseFloat(timeMatch[1]);
    }
  }

  // 2. MySQL Parsing
  else if (normDialect === 'mysql') {
    if (/"type":\s*"ALL"/i.test(textContent) || /\btype:\s*ALL\b/i.test(textContent) || /\bALL\b/i.test(textContent)) {
      const tableMatch = textContent.match(/"table_name":\s*"([^"]+)"/i) || textContent.match(/table:\s*([\w`"'-]+)/i);
      const tbl = tableMatch ? cleanIdentifier(tableMatch[1]) : 'target_table';
      scansDetected.push({
        table: tbl,
        scan_type: 'ALL (Table Scan)',
        impact: 'HIGH',
        reason: `MySQL type 'ALL' full table scan on '${tbl}'.`,
      });
      bottlenecks.push(`Full table scan (type: ALL) on ${tbl}`);
    }

    if (/Using filesort/i.test(textContent)) {
      bottlenecks.push('Using filesort: Sorting requires temporary buffer. Index on ORDER BY/GROUP BY recommended.');
    }
    if (/Using temporary/i.test(textContent)) {
      bottlenecks.push('Using temporary: Query requires temporary table for aggregation/sorting.');
    }
  }

  // 3. SQLite Parsing
  else if (normDialect === 'sqlite') {
    const scanMatches = [...textContent.matchAll(/SCAN\s+(?:TABLE\s+)?([\w`"'-]+)/gi)];
    for (const sm of scanMatches) {
      scansDetected.push({
        table: cleanIdentifier(sm[1]),
        scan_type: 'SCAN TABLE',
        impact: 'HIGH',
        reason: `SQLite full scan on table '${cleanIdentifier(sm[1])}'.`,
      });
      bottlenecks.push(`SCAN TABLE on ${cleanIdentifier(sm[1])}`);
    }

    if (/USE TEMP B-TREE/i.test(textContent)) {
      bottlenecks.push('USE TEMP B-TREE for ORDER BY / GROUP BY. Index recommended to eliminate temp b-tree.');
    }
  }

  return {
    explain_mode: scansDetected.length > 0 ? 'explain_plan_parsed' : 'heuristic_fallback',
    scans_detected: scansDetected,
    total_cost: totalCost,
    execution_time_ms: executionTimeMs,
    bottlenecks: bottlenecks.length > 0 ? bottlenecks : ['Standard AST clause scan evaluation.'],
  };
}

/**
 * Generates dialect-appropriate composite B-Tree indexes and covering/partial indexes
 * Rule: (Equality -> Range/IN -> Sort/Group)
 * @param {object} ast 
 * @param {object} explainPlan 
 * @param {string} dialect 
 * @returns {Array<object>}
 */
export function generateIndexRecommendations(ast, explainPlan = {}, dialect = 'postgres') {
  const normDialect = normalizeDialect(dialect);
  const recommendations = [];
  const generatedIndexKeys = new Set();

  for (const table of ast.tables) {
    if (!table || table === 'unknown_table') continue;

    // 1. Gather Equality Columns for this table
    const equalityCols = [];
    for (const f of ast.filterPredicates) {
      if (f.table === table && (f.type === 'equality' || f.type === 'is_not_null')) {
        if (!equalityCols.includes(f.column)) {
          equalityCols.push(f.column);
        }
      }
    }

    // 2. Gather Range and IN columns for this table
    const rangeCols = [];
    for (const f of ast.filterPredicates) {
      if (f.table === table && (f.type === 'range' || f.type === 'in')) {
        if (!equalityCols.includes(f.column) && !rangeCols.includes(f.column)) {
          rangeCols.push(f.column);
        }
      }
    }

    // 3. Gather Join Foreign Key columns for this table
    const joinCols = [];
    for (const j of ast.joinConditions) {
      if (j.left.table === table && !equalityCols.includes(j.left.column) && !rangeCols.includes(j.left.column)) {
        if (!joinCols.includes(j.left.column)) joinCols.push(j.left.column);
      }
      if (j.right.table === table && !equalityCols.includes(j.right.column) && !rangeCols.includes(j.right.column)) {
        if (!joinCols.includes(j.right.column)) joinCols.push(j.right.column);
      }
    }

    // 4. Gather Sort and Group columns for this table
    const sortGroupCols = [];
    for (const g of ast.groupByColumns) {
      if (g.table === table && !equalityCols.includes(g.column) && !rangeCols.includes(g.column) && !joinCols.includes(g.column)) {
        if (!sortGroupCols.includes(g.column)) sortGroupCols.push(g.column);
      }
    }
    for (const o of ast.orderByColumns) {
      if (o.table === table && !equalityCols.includes(o.column) && !rangeCols.includes(o.column) && !joinCols.includes(o.column) && !sortGroupCols.includes(o.column)) {
        if (!sortGroupCols.includes(o.column)) sortGroupCols.push(o.column);
      }
    }

    // Composite Index Columns according to (Equality -> Range -> Sort/Group)
    const compositeCols = [
      ...equalityCols,
      ...joinCols,
      ...rangeCols,
      ...sortGroupCols,
    ];

    if (compositeCols.length > 0) {
      const colHash = compositeCols.map(c => c.replace(/\W+/g, '_')).join('_').substring(0, 32);
      const indexName = `claude_ai_idx_${table}_${colHash}`;
      const indexKey = `${table}:${compositeCols.join(',')}`;

      if (!generatedIndexKeys.has(indexKey)) {
        generatedIndexKeys.add(indexKey);

        const colsStr = compositeCols.join(', ');
        let ddl = '';
        let priority = 'MEDIUM';
        let estimatedSpeedup = '5x - 15x';

        // Check if full table scan was detected for this table
        const scan = (explainPlan.scans_detected || []).find(s => s.table === table);
        if (scan || equalityCols.length > 0) {
          priority = 'HIGH';
          estimatedSpeedup = '10x - 50x';
        }

        // Check for covering index opportunity (PostgreSQL, SQLServer)
        const unindexedSelectCols = ast.selectColumns.filter(c => !compositeCols.includes(c) && c !== '*').slice(0, 3);
        const hasCoveringOpportunity = (normDialect === 'postgres' || normDialect === 'sqlserver') && unindexedSelectCols.length > 0 && unindexedSelectCols.length <= 3;

        // Dialect-specific DDL generation
        switch (normDialect) {
          case 'postgres':
            if (hasCoveringOpportunity) {
              ddl = `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${indexName} ON ${table} (${colsStr}) INCLUDE (${unindexedSelectCols.join(', ')});`;
            } else {
              ddl = `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${indexName} ON ${table} (${colsStr});`;
            }
            break;
          case 'mysql':
            ddl = `CREATE INDEX ${indexName} ON ${table} (${colsStr});`;
            break;
          case 'sqlite':
            ddl = `CREATE INDEX IF NOT EXISTS ${indexName} ON ${table} (${colsStr});`;
            break;
          case 'bigquery':
            // BigQuery clustering recommendation
            ddl = `-- BigQuery Table Optimization:\n-- Recreate or cluster table with:\n-- CREATE OR REPLACE TABLE \`${table}\` CLUSTER BY ${colsStr} AS SELECT * FROM \`${table}\`;`;
            break;
          case 'snowflake':
            ddl = `ALTER TABLE ${table} CLUSTER BY (${colsStr});`;
            break;
          case 'redshift':
            ddl = `ALTER TABLE ${table} ALTER SORTKEY (${colsStr});`;
            break;
          case 'clickhouse':
            ddl = `ALTER TABLE ${table} ADD INDEX ${indexName} (${colsStr}) TYPE minmax GRANULARITY 1;`;
            break;
          case 'sqlserver':
            if (hasCoveringOpportunity) {
              ddl = `CREATE NONCLUSTERED INDEX ${indexName} ON ${table} (${colsStr}) INCLUDE (${unindexedSelectCols.join(', ')});`;
            } else {
              ddl = `CREATE NONCLUSTERED INDEX ${indexName} ON ${table} (${colsStr});`;
            }
            break;
          default:
            ddl = `CREATE INDEX ${indexName} ON ${table} (${colsStr});`;
        }

        let rationale = `Composite index following (Equality -> Range -> Sort/Group) rule on table '${table}'. `;
        if (equalityCols.length > 0) rationale += `Accelerates exact matches on (${equalityCols.join(', ')}). `;
        if (joinCols.length > 0) rationale += `Accelerates join lookups on (${joinCols.join(', ')}). `;
        if (rangeCols.length > 0) rationale += `Prunes range/IN scans on (${rangeCols.join(', ')}). `;
        if (sortGroupCols.length > 0) rationale += `Avoids filesort/temp table on (${sortGroupCols.join(', ')}). `;
        if (hasCoveringOpportunity) rationale += `Includes covering columns (${unindexedSelectCols.join(', ')}) for index-only scans.`;

        recommendations.push({
          table,
          index_name: indexName,
          columns: compositeCols,
          index_type: normDialect === 'snowflake' || normDialect === 'bigquery' ? 'clustering' : 'btree',
          ddl,
          priority,
          estimated_speedup: estimatedSpeedup,
          rationale: rationale.trim(),
        });
      }
    }

    // 5. Check for Partial Index opportunities (e.g. status = 'active' or deleted_at IS NULL)
    for (const f of ast.filterPredicates) {
      if (f.table === table && f.type === 'equality' && f.value && typeof f.value === 'string' && ['active', 'pending', 'open', 'completed', 'true', 'false', '0', '1'].includes(f.value.toLowerCase())) {
        const partialColHash = (f.column + '_' + f.value).replace(/\W+/g, '_');
        const partialIdxName = `claude_ai_idx_${table}_partial_${partialColHash}`;
        const partialKey = `${table}:partial:${f.column}:${f.value}`;

        if (!generatedIndexKeys.has(partialKey) && (normDialect === 'postgres' || normDialect === 'sqlite')) {
          generatedIndexKeys.add(partialKey);
          const otherCols = compositeCols.filter(c => c !== f.column);
          const targetCols = otherCols.length > 0 ? otherCols.join(', ') : f.column;
          const partialDDL = `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${partialIdxName} ON ${table} (${targetCols}) WHERE ${f.column} = '${f.value}';`;

          recommendations.push({
            table,
            index_name: partialIdxName,
            columns: otherCols.length > 0 ? otherCols : [f.column],
            index_type: 'partial_btree',
            ddl: partialDDL,
            priority: 'MEDIUM',
            estimated_speedup: '5x - 20x',
            rationale: `Partial index targeting static subset where '${f.column}' = '${f.value}', significantly reducing index size and write overhead.`,
          });
        }
      }
    }

    // 6. Check for Independent Join Foreign Keys
    for (const j of ast.joinConditions) {
      const fkCandidate = j.left.table === table ? j.left.column : (j.right.table === table ? j.right.column : null);
      if (fkCandidate && !compositeCols.includes(fkCandidate)) {
        const fkIdxName = `claude_ai_idx_${table}_fk_${fkCandidate}`;
        const fkKey = `${table}:fk:${fkCandidate}`;

        if (!generatedIndexKeys.has(fkKey)) {
          generatedIndexKeys.add(fkKey);
          let fkDDL = '';
          switch (normDialect) {
            case 'postgres': fkDDL = `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${fkIdxName} ON ${table} (${fkCandidate});`; break;
            case 'sqlite': fkDDL = `CREATE INDEX IF NOT EXISTS ${fkIdxName} ON ${table} (${fkCandidate});`; break;
            default: fkDDL = `CREATE INDEX ${fkIdxName} ON ${table} (${fkCandidate});`;
          }

          recommendations.push({
            table,
            index_name: fkIdxName,
            columns: [fkCandidate],
            index_type: 'btree',
            ddl: fkDDL,
            priority: 'MEDIUM',
            estimated_speedup: '3x - 10x',
            rationale: `Index on foreign key column '${fkCandidate}' to eliminate nested loop join scans on table '${table}'.`,
          });
        }
      }
    }
  }

  return recommendations;
}

/**
 * Generates Materialized View recommendations for complex aggregations or heavy multi-table joins
 * @param {object} ast 
 * @param {string} sql 
 * @param {string} dialect 
 * @param {object} explainPlan 
 * @returns {Array<object>}
 */
export function generateMaterializedViewRecommendations(ast, sql, dialect = 'postgres', explainPlan = {}) {
  const normDialect = normalizeDialect(dialect);
  const recommendations = [];

  // Evaluate Materialized View eligibility:
  // 1. Has aggregations (SUM, COUNT, AVG, etc.) + GROUP BY
  // 2. Multi-table joins (> 1 table) with aggregation
  // 3. Or heavy explain cost / scan
  const isAggCandidate = ast.hasAggregations && ast.groupByColumns.length > 0;
  const isMultiJoin = ast.tables.length > 1;
  const isHeavyScan = (explainPlan.scans_detected || []).length > 1 || (explainPlan.total_cost && explainPlan.total_cost > 1000);

  if (isAggCandidate || (isMultiJoin && isHeavyScan)) {
    const baseTable = ast.tables[0] || 'dataset';
    const viewName = `claude_ai_mv_${baseTable}_summary`;
    const cleanSql = sql.trim().replace(/;$/, '');

    let ddl = '';
    let refreshStrategy = '';
    let priority = isAggCandidate && isMultiJoin ? 'HIGH' : 'MEDIUM';

    switch (normDialect) {
      case 'postgres':
        ddl = `CREATE MATERIALIZED VIEW IF NOT EXISTS ${viewName} AS\n${cleanSql};`;
        if (ast.groupByColumns.length > 0) {
          const pkCols = ast.groupByColumns.map(g => g.column).join(', ');
          ddl += `\nCREATE UNIQUE INDEX IF NOT EXISTS idx_${viewName}_unique ON ${viewName} (${pkCols});`;
        }
        refreshStrategy = `REFRESH MATERIALIZED VIEW CONCURRENTLY ${viewName}; -- Schedule via cron or trigger on data load`;
        break;

      case 'bigquery':
        ddl = `CREATE MATERIALIZED VIEW IF NOT EXISTS \`${viewName}\`\nOPTIONS (enable_refresh = true, refresh_interval_minutes = 60)\nAS\n${cleanSql};`;
        refreshStrategy = `Automatic incremental background refresh managed by BigQuery (every 60 minutes or upon underlying partition commit).`;
        break;

      case 'snowflake':
        ddl = `CREATE MATERIALIZED VIEW IF NOT EXISTS ${viewName} AS\n${cleanSql};`;
        refreshStrategy = `Snowflake serverless background maintenance automatically keeps the materialized view up to date.`;
        break;

      case 'clickhouse':
        ddl = `CREATE MATERIALIZED VIEW IF NOT EXISTS ${viewName}\nENGINE = SummingMergeTree()\nORDER BY (${ast.groupByColumns.map(g => g.column).join(', ') || 'tuple()'})\nAS\n${cleanSql};`;
        refreshStrategy = `Real-time automatic continuous aggregation on table insert stream.`;
        break;

      case 'sqlite':
      case 'mysql':
        // MySQL 8 / SQLite do not natively have standard REFRESH MATERIALIZED VIEW syntax without scheduled tables
        ddl = `CREATE VIEW IF NOT EXISTS ${viewName} AS\n${cleanSql};`;
        refreshStrategy = `Standard View created. For materialized caching, replicate to summary table using a scheduled event (e.g. hourly cron).`;
        break;

      default:
        ddl = `CREATE MATERIALIZED VIEW IF NOT EXISTS ${viewName} AS\n${cleanSql};`;
        refreshStrategy = `Periodic refresh recommended after batch data ingestion.`;
    }

    recommendations.push({
      view_name: viewName,
      ddl,
      refresh_strategy: refreshStrategy,
      priority,
      estimated_speedup: '20x - 100x',
      rationale: `Precomputes aggregated metrics and multi-table joins (${ast.tables.join(', ')}) to eliminate repetitive CPU and I/O overhead on live dashboard queries.`,
    });
  }

  return recommendations;
}

/**
 * Main Advisor entry point: Advises query indexes and materialized views
 * @param {object} params
 * @param {number} [params.databaseId]
 * @param {string} params.sql
 * @param {number} [params.cardId]
 * @param {string|object} [params.explainResult]
 * @param {object} [params.catalog]
 * @param {string} [params.dialect]
 * @param {object} [params.client]
 * @param {object} [params.metabaseClient]
 * @param {boolean} [params.runExplain]
 * @param {boolean} [params.workloadAnalysis]
 * @returns {Promise<object>}
 */
export async function adviseQueryIndexes({
  databaseId,
  sql,
  cardId,
  explainResult,
  _catalog,
  dialect = 'postgres',
  client,
  metabaseClient,
  runExplain = true,
  _workloadAnalysis = false,
}) {
  let targetSql = sql;
  let targetDbId = databaseId;
  const targetDialect = normalizeDialect(dialect);

  // 1. If cardId provided and no SQL, fetch card definition
  if (cardId && !targetSql && metabaseClient) {
    try {
      const card = await metabaseClient.getQuestion(cardId);
      if (card && card.dataset_query) {
        if (card.dataset_query.native && card.dataset_query.native.query) {
          targetSql = card.dataset_query.native.query;
        }
        if (!targetDbId && card.database_id) {
          targetDbId = card.database_id;
        }
      }
    } catch (err) {
      logger.warn(`Failed to fetch card ${cardId} in index advisor:`, err.message);
    }
  }

  if (!targetSql || typeof targetSql !== 'string' || !targetSql.trim()) {
    throw new Error('Valid SQL query or card_id containing SQL must be provided.');
  }

  // 2. Parse Query AST
  const ast = extractQueryAST(targetSql);

  // 3. Run EXPLAIN if requested and client/metabaseClient available
  let parsedExplain = null;
  if (explainResult) {
    parsedExplain = parseExplainPlan(explainResult, targetDialect);
  } else if (runExplain && (client || metabaseClient) && targetDbId) {
    try {
      const explainSql = targetDialect === 'postgres'
        ? `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${targetSql.replace(/;$/, '')};`
        : (targetDialect === 'mysql'
          ? `EXPLAIN FORMAT=JSON ${targetSql.replace(/;$/, '')};`
          : `EXPLAIN ${targetSql.replace(/;$/, '')};`);

      let rawResult = null;
      if (client && typeof client.executeNativeQuery === 'function') {
        rawResult = await client.executeNativeQuery(targetDbId, explainSql);
      } else if (metabaseClient && typeof metabaseClient.executeNativeQuery === 'function') {
        rawResult = await metabaseClient.executeNativeQuery(targetDbId, explainSql);
      }

      if (rawResult) {
        parsedExplain = parseExplainPlan(rawResult, targetDialect);
      }
    } catch (explainErr) {
      logger.info(`EXPLAIN execution fallback to AST heuristics: ${explainErr.message}`);
      parsedExplain = parseExplainPlan(null, targetDialect);
    }
  } else {
    parsedExplain = parseExplainPlan(null, targetDialect);
  }

  // 4. Generate Index Recommendations
  const indexRecommendations = generateIndexRecommendations(ast, parsedExplain, targetDialect);

  // 5. Generate Materialized View Recommendations
  const matViewRecommendations = generateMaterializedViewRecommendations(ast, targetSql, targetDialect, parsedExplain);

  // 6. Calculate Estimated Overall Impact
  let overallImpact = 'Low impact (query already relatively lightweight or well-structured)';
  if (indexRecommendations.length > 0 || matViewRecommendations.length > 0) {
    const hasHighPriority = indexRecommendations.some(r => r.priority === 'HIGH') || matViewRecommendations.some(r => r.priority === 'HIGH');
    if (hasHighPriority) {
      overallImpact = 'High impact: Expected 10x - 50x reduction in query latency and elimination of table scans.';
    } else {
      overallImpact = 'Moderate impact: Expected 3x - 10x reduction in query latency and improved cache locality.';
    }
  }

  // 7. Assemble Structured Result
  return {
    sql: targetSql,
    database_id: targetDbId || null,
    dialect: targetDialect,
    query_analysis: {
      tables: ast.tables,
      filter_columns: ast.filterPredicates.map(f => `${f.table}.${f.column}`),
      join_conditions: ast.joinConditions.map(j => j.raw),
      group_by_columns: ast.groupByColumns.map(g => `${g.table}.${g.column}`),
      order_by_columns: ast.orderByColumns.map(o => `${o.table}.${o.column} ${o.direction}`),
      has_aggregations: ast.hasAggregations,
      scans_detected: parsedExplain.scans_detected || [],
      bottlenecks: parsedExplain.bottlenecks || [],
      explain_mode: parsedExplain.explain_mode,
    },
    index_recommendations: indexRecommendations,
    materialized_view_recommendations: matViewRecommendations,
    estimated_impact: overallImpact,
    _provenance: {
      ai_generated: true,
      tool: 'ai_query_index_advisor',
      review_required: true,
      timestamp: new Date().toISOString(),
      dialect: targetDialect,
    },
  };
}
