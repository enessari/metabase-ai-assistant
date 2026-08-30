import { logger } from '../utils/logger.js';
import { maskRow, isPiiMaskingEnabled } from '../utils/pii-masker.js';
import { detectWriteOperation, isReadOnlyMode } from '../mcp/handlers/database.js';
import { wrapUntrustedMetadata } from '../utils/prompt-sanitizer.js';

/**
 * SQL Error Categories Enum
 */
export const ERROR_CATEGORIES = {
  SYNTAX_ERROR: 'SYNTAX_ERROR',
  MISSING_COLUMN: 'MISSING_COLUMN',
  INVALID_TABLE: 'INVALID_TABLE',
  TYPE_MISMATCH: 'TYPE_MISMATCH',
  AMBIGUOUS_COLUMN: 'AMBIGUOUS_COLUMN',
  GROUP_BY_VIOLATION: 'GROUP_BY_VIOLATION',
  DIVIDE_BY_ZERO: 'DIVIDE_BY_ZERO',
  UNKNOWN: 'UNKNOWN',
};

/**
 * Common SQL Keyword Typos Dictionary
 */
export const SYNTAX_TYPO_MAP = {
  FORM: 'FROM',
  WHER: 'WHERE',
  SELEC: 'SELECT',
  SELCT: 'SELECT',
  'GROP BY': 'GROUP BY',
  GROUPBY: 'GROUP BY',
  'OERDER BY': 'ORDER BY',
  'ODER BY': 'ORDER BY',
  HAVNG: 'HAVING',
  LIMT: 'LIMIT',
  LIMITT: 'LIMIT',
  INNERJOIN: 'INNER JOIN',
  LEFTJOIN: 'LEFT JOIN',
  RIGHTJOIN: 'RIGHT JOIN',
  DISTINCTT: 'DISTINCT',
  DISTINT: 'DISTINCT',
  COALESCEE: 'COALESCE',
  COUNTD: 'COUNT',
};

/**
 * Common Column Name Synonym & Typo Map
 */
export const COLUMN_SYNONYM_MAP = {
  user_mail: 'email',
  user_email: 'email',
  mail: 'email',
  email_address: 'email',
  created_time: 'created_at',
  created_date: 'created_at',
  creation_date: 'created_at',
  creation_time: 'created_at',
  updated_time: 'updated_at',
  updated_date: 'updated_at',
  modification_date: 'updated_at',
  amount: 'total_amount',
  order_amount: 'total_amount',
  price: 'total_amount',
  cost: 'total_amount',
  cust_id: 'customer_id',
  user_identifier: 'user_id',
  phone_number: 'phone',
  tel: 'phone',
  zip: 'zip_code',
  zipcode: 'zip_code',
  postal_code: 'zip_code',
  address1: 'address',
};

/**
 * Common Table Name Singular/Plural Map
 */
export const TABLE_PLURAL_MAP = {
  order: 'orders',
  user: 'users',
  product: 'products',
  customer: 'customers',
  account: 'accounts',
  item: 'items',
  card: 'cards',
  dashboard: 'dashboards',
  collection: 'collections',
  metric: 'metrics',
  log: 'logs',
  transaction: 'transactions',
  category: 'categories',
};

/**
 * Calculates Levenshtein Distance between two strings.
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
export function levenshteinDistance(a, b) {
  if (!a || !b) return (a || b || '').length;
  const s1 = String(a).toLowerCase();
  const s2 = String(b).toLowerCase();
  const al = s1.length;
  const bl = s2.length;

  const matrix = Array.from({ length: al + 1 }, () => new Array(bl + 1).fill(0));
  for (let i = 0; i <= al; i++) matrix[i][0] = i;
  for (let j = 0; j <= bl; j++) matrix[0][j] = j;

  for (let i = 1; i <= al; i++) {
    for (let j = 1; j <= bl; j++) {
      const cost = s1[i - 1] === s2[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost
      );
    }
  }

  return matrix[al][bl];
}

/**
 * Normalized string similarity score (0.0 to 1.0)
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
export function stringSimilarity(a, b) {
  if (!a && !b) return 1.0;
  if (!a || !b) return 0.0;
  const s1 = String(a).toLowerCase().replace(/[\s_-]/g, '');
  const s2 = String(b).toLowerCase().replace(/[\s_-]/g, '');
  if (s1 === s2) return 1.0;
  const maxLen = Math.max(s1.length, s2.length);
  if (maxLen === 0) return 1.0;
  const dist = levenshteinDistance(s1, s2);
  return Math.max(0, 1.0 - (dist / maxLen));
}

/**
 * Finds best matching string from a list of candidate strings or objects.
 * @param {string} target
 * @param {Array<string|object>} candidates
 * @param {number} threshold
 * @returns {string|null}
 */
export function findBestMatch(target, candidates, threshold = 0.45) {
  if (!target || !Array.isArray(candidates) || candidates.length === 0) return null;
  let bestName = null;
  let highestScore = 0;

  for (const candidate of candidates) {
    const candidateName = typeof candidate === 'object' && candidate !== null
      ? (candidate.name || candidate.display_name || candidate.name_field || '')
      : String(candidate);

    if (!candidateName) continue;

    const score = stringSimilarity(target, candidateName);
    if (score > highestScore && score >= threshold) {
      highestScore = score;
      bestName = candidateName;
    }
  }

  return bestName;
}

/**
 * Classify SQL Error into a standardized category.
 * @param {string|Error} error
 * @param {string} sql
 * @returns {string} Error category from ERROR_CATEGORIES
 */
export function classifySQLError(error, _sql = '') {
  if (!error) return ERROR_CATEGORIES.UNKNOWN;
  const msg = typeof error === 'string' ? error : (error.message || String(error));
  const lowerMsg = msg.toLowerCase();

  // 1. Divide by zero
  if (
    lowerMsg.includes('division by zero') ||
    lowerMsg.includes('division by 0') ||
    lowerMsg.includes('divide by zero') ||
    lowerMsg.includes('division_by_zero')
  ) {
    return ERROR_CATEGORIES.DIVIDE_BY_ZERO;
  }

  // 2. Ambiguous column
  if (
    lowerMsg.includes('is ambiguous') ||
    lowerMsg.includes('ambiguous column') ||
    /column reference "([^"]+)" is ambiguous/i.test(msg) ||
    /Column '([^']+)' in [^ ]+ is ambiguous/i.test(msg)
  ) {
    return ERROR_CATEGORIES.AMBIGUOUS_COLUMN;
  }

  // 3. Missing column
  if (
    /column "([^"]+)" does not exist/i.test(msg) ||
    /unknown column '([^']+)'/i.test(msg) ||
    /column not found:? ([^\s,]+)/i.test(msg) ||
    /invalid identifier '?([^'\s,]+)'?/i.test(msg) ||
    /no such column: ([^\s,]+)/i.test(msg) ||
    /cannot find column '?([^'\s,]+)'?/i.test(msg) ||
    /attribute "([^"]+)" not found/i.test(msg) ||
    /unrecognized name: ([^\s,]+)/i.test(msg) ||
    (lowerMsg.includes('column') && (lowerMsg.includes('does not exist') || lowerMsg.includes('not found')))
  ) {
    return ERROR_CATEGORIES.MISSING_COLUMN;
  }

  // 4. Invalid table
  if (
    /relation "([^"]+)" does not exist/i.test(msg) ||
    /table '([^']+)' doesn't exist/i.test(msg) ||
    /no such table: ([^\s,]+)/i.test(msg) ||
    /table not found:? ([^\s,]+)/i.test(msg) ||
    /table or view `?([^`]+)`? not found/i.test(msg) ||
    /cannot find table/i.test(msg) ||
    /object '([^']+)' does not exist/i.test(msg) ||
    (lowerMsg.includes('table') && (lowerMsg.includes('does not exist') || lowerMsg.includes("doesn't exist") || lowerMsg.includes('not found')))
  ) {
    return ERROR_CATEGORIES.INVALID_TABLE;
  }

  // 5. Group By violation
  if (
    lowerMsg.includes('group by') ||
    lowerMsg.includes('must appear in the group by clause') ||
    lowerMsg.includes('is not in group by clause') ||
    lowerMsg.includes('not functionally dependent on columns in group by') ||
    lowerMsg.includes('expression not in group by') ||
    lowerMsg.includes('aggregate_without_group_by')
  ) {
    return ERROR_CATEGORIES.GROUP_BY_VIOLATION;
  }

  // 6. Type mismatch
  if (
    lowerMsg.includes('operator does not exist') ||
    lowerMsg.includes('cannot cast type') ||
    lowerMsg.includes('not comparable') ||
    lowerMsg.includes('types are not comparable') ||
    lowerMsg.includes('conversion failed') ||
    lowerMsg.includes('type mismatch') ||
    lowerMsg.includes('incompatible types') ||
    lowerMsg.includes('data type mismatch') ||
    lowerMsg.includes('cannot compare')
  ) {
    return ERROR_CATEGORIES.TYPE_MISMATCH;
  }

  // 7. Syntax error
  if (
    lowerMsg.includes('syntax error') ||
    lowerMsg.includes('unexpected token') ||
    lowerMsg.includes('mismatched parenthesis') ||
    lowerMsg.includes('parse_error') ||
    lowerMsg.includes('parse error') ||
    lowerMsg.includes('you have an error in your sql syntax') ||
    lowerMsg.includes('unexpected end of') ||
    lowerMsg.includes('unrecognized token') ||
    lowerMsg.includes('misquoted string') ||
    /near ".*": syntax error/i.test(msg)
  ) {
    return ERROR_CATEGORIES.SYNTAX_ERROR;
  }

  return ERROR_CATEGORIES.UNKNOWN;
}

/**
 * Extracts target identifiers (column, table, etc.) from error message.
 * @param {string} errorMessage
 * @param {string} category
 * @returns {object} { columnName, tableName }
 */
export function extractErrorIdentifiers(errorMessage, category) {
  if (!errorMessage) return {};
  const res = {};

  if (category === ERROR_CATEGORIES.MISSING_COLUMN || category === ERROR_CATEGORIES.AMBIGUOUS_COLUMN) {
    const colMatch =
      errorMessage.match(/column(?:\s+reference)?\s+"([^"]+)"/i) ||
      errorMessage.match(/column(?:\s+reference)?\s+'([^']+)'/i) ||
      errorMessage.match(/Unknown column '([^']+)'/i) ||
      errorMessage.match(/column not found:?\s*([^\s,]+)/i) ||
      errorMessage.match(/invalid identifier '?([^'\s,]+)'?/i) ||
      errorMessage.match(/no such column:\s*([^\s,]+)/i) ||
      errorMessage.match(/ambiguous column name:?\s*([^\s,]+)/i) ||
      errorMessage.match(/unrecognized name:\s*([^\s,]+)/i) ||
      errorMessage.match(/attribute "([^"]+)"/i);
    if (colMatch) {
      res.columnName = colMatch[1];
    }
  }

  if (category === ERROR_CATEGORIES.INVALID_TABLE) {
    const tableMatch =
      errorMessage.match(/relation "([^"]+)"/i) ||
      errorMessage.match(/Table '([^']+)'/i) ||
      errorMessage.match(/no such table:\s*([^\s,]+)/i) ||
      errorMessage.match(/table not found:?\s*([^\s,]+)/i) ||
      errorMessage.match(/Object '([^']+)'/i) ||
      errorMessage.match(/table or view `?([^`]+)`?/i);
    if (tableMatch) {
      res.tableName = tableMatch[1];
    }
  }

  if (category === ERROR_CATEGORIES.GROUP_BY_VIOLATION) {
    const groupMatch =
      errorMessage.match(/column "([^"]+)" must appear in the GROUP BY/i) ||
      errorMessage.match(/expression '([^']+)' must appear in the GROUP BY/i) ||
      errorMessage.match(/column '([^']+)' is not in GROUP BY/i);
    if (groupMatch) {
      res.columnName = groupMatch[1];
    }
  }

  return res;
}

/**
 * Extracts all table references from SQL query.
 * @param {string} sql
 * @returns {Array<string>}
 */
export function extractTablesFromSQL(sql) {
  if (!sql) return [];
  const tables = [];
  const regex = /\b(?:FROM|JOIN)\s+([a-zA-Z0-9_."`]+)/gi;
  let match;
  while ((match = regex.exec(sql)) !== null) {
    const raw = match[1].replace(/[`"]/g, '');
    const parts = raw.split('.');
    const tableName = parts[parts.length - 1];
    if (tableName && !tables.includes(tableName)) {
      tables.push(tableName);
    }
  }
  return tables;
}

/**
 * Extracts selected columns and aggregate presence from SQL.
 * @param {string} sql
 * @returns {object} { selectColumns, hasAggregates, nonAggregatedColumns }
 */
export function analyzeSelectClause(sql) {
  if (!sql) return { selectColumns: [], hasAggregates: false, nonAggregatedColumns: [] };
  const selectMatch = sql.match(/\bSELECT\s+([\s\S]*?)\bFROM\b/i);
  if (!selectMatch) return { selectColumns: [], hasAggregates: false, nonAggregatedColumns: [] };

  const selectBody = selectMatch[1].trim();
  const aggRegex = /\b(COUNT|SUM|AVG|MIN|MAX|ARRAY_AGG|STRING_AGG)\s*\(/i;
  const hasAggregates = aggRegex.test(selectBody);

  // Split columns by comma (ignoring commas inside parentheses)
  const columns = [];
  let depth = 0;
  let current = '';

  for (let i = 0; i < selectBody.length; i++) {
    const char = selectBody[i];
    if (char === '(') depth++;
    else if (char === ')') depth = Math.max(0, depth - 1);

    if (char === ',' && depth === 0) {
      if (current.trim()) columns.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  if (current.trim()) columns.push(current.trim());

  const nonAggregatedColumns = [];
  for (const col of columns) {
    if (!aggRegex.test(col)) {
      // Extract alias or base column name
      const asMatch = col.match(/\bAS\s+([a-zA-Z0-9_"`]+)$/i) || col.match(/\s+([a-zA-Z0-9_"`]+)$/);
      let colName = col;
      if (asMatch && !col.includes('(')) {
        colName = col.split(/\bAS\b/i)[0].trim();
      }
      if (colName && !colName.includes('*') && !nonAggregatedColumns.includes(colName)) {
        nonAggregatedColumns.push(colName);
      }
    }
  }

  return { selectColumns: columns, hasAggregates, nonAggregatedColumns };
}

/**
 * Deterministically repairs SQL based on classified error and schema context.
 * @param {object} options
 * @returns {Promise<object>} { repairedSql, diagnosis, ruleApplied }
 */
export async function repairSQLError({
  sql,
  errorCategory,
  errorMessage = '',
  catalog = null,
  dialect: _dialect = 'generic',
  assistant = null,
  explanation = '',
}) {
  if (!sql || typeof sql !== 'string') {
    return { repairedSql: sql, diagnosis: 'No SQL provided to repair', ruleApplied: false };
  }

  let repairedSql = sql;
  let diagnosis = '';
  let ruleApplied = false;

  const identifiers = extractErrorIdentifiers(errorMessage, errorCategory);

  // ==================== 1. SYNTAX ERROR REPAIR ====================
  if (errorCategory === ERROR_CATEGORIES.SYNTAX_ERROR) {
    let modified = repairedSql;

    // A. Replace single-token syntax typos
    for (const [typo, fix] of Object.entries(SYNTAX_TYPO_MAP)) {
      const typoRegex = new RegExp(`\\b${typo}\\b`, 'gi');
      if (typoRegex.test(modified)) {
        modified = modified.replace(typoRegex, fix);
        diagnosis += `Fixed typo '${typo}' -> '${fix}'. `;
        ruleApplied = true;
      }
    }

    // B. Fix trailing commas before FROM, WHERE, GROUP BY, ORDER BY, LIMIT, or closing paren
    const trailingCommaRegexes = [
      { regex: /,\s*(\bFROM\b)/gi, replace: ' $1', name: 'before FROM' },
      { regex: /,\s*(\bWHERE\b)/gi, replace: ' $1', name: 'before WHERE' },
      { regex: /,\s*(\bGROUP\s+BY\b)/gi, replace: ' $1', name: 'before GROUP BY' },
      { regex: /,\s*(\bORDER\s+BY\b)/gi, replace: ' $1', name: 'before ORDER BY' },
      { regex: /,\s*(\bLIMIT\b)/gi, replace: ' $1', name: 'before LIMIT' },
      { regex: /,\s*(\))/g, replace: '$1', name: 'before closing parenthesis' },
    ];

    for (const { regex, replace, name } of trailingCommaRegexes) {
      if (regex.test(modified)) {
        modified = modified.replace(regex, replace);
        diagnosis += `Removed dangling comma ${name}. `;
        ruleApplied = true;
      }
    }

    // C. Fix unmatched parentheses
    const openParens = (modified.match(/\(/g) || []).length;
    const closeParens = (modified.match(/\)/g) || []).length;
    if (openParens > closeParens) {
      const diff = openParens - closeParens;
      const endsWithSemicolon = modified.trim().endsWith(';');
      const clean = endsWithSemicolon ? modified.trim().slice(0, -1) : modified.trim();
      modified = clean + ')'.repeat(diff) + (endsWithSemicolon ? ';' : '');
      diagnosis += `Appended ${diff} missing closing parenthesis. `;
      ruleApplied = true;
    }

    repairedSql = modified;
  }

  // ==================== 2. MISSING COLUMN REPAIR ====================
  if (errorCategory === ERROR_CATEGORIES.MISSING_COLUMN || (errorCategory === ERROR_CATEGORIES.UNKNOWN && identifiers.columnName)) {
    let targetCol = identifiers.columnName;

    // If target column wasn't in error, inspect SQL against synonyms
    let replacementCol = null;

    if (targetCol) {
      // Check deterministic synonym mapping
      const normalizedTarget = targetCol.toLowerCase();
      if (COLUMN_SYNONYM_MAP[normalizedTarget]) {
        replacementCol = COLUMN_SYNONYM_MAP[normalizedTarget];
      }

      // Check catalog schema if provided
      if (!replacementCol && catalog) {
        const candidateFields = [];
        const tables = Array.isArray(catalog) ? catalog : (catalog.tables || []);
        for (const t of tables) {
          if (t.fields && Array.isArray(t.fields)) {
            for (const f of t.fields) candidateFields.push(f);
          }
          if (t.columns && Array.isArray(t.columns)) {
            for (const c of t.columns) candidateFields.push(c);
          }
        }

        if (candidateFields.length > 0) {
          const match = findBestMatch(targetCol, candidateFields, 0.4);
          if (match && match.toLowerCase() !== targetCol.toLowerCase()) {
            replacementCol = match;
          }
        }
      }

      if (replacementCol) {
        const colRegex = new RegExp(`\\b${targetCol}\\b`, 'gi');
        if (colRegex.test(repairedSql)) {
          repairedSql = repairedSql.replace(colRegex, replacementCol);
          diagnosis += `Replaced missing column '${targetCol}' with '${replacementCol}'. `;
          ruleApplied = true;
        }
      }
    } else {
      // Check SQL for any known synonyms directly
      for (const [synonym, standard] of Object.entries(COLUMN_SYNONYM_MAP)) {
        const synRegex = new RegExp(`\\b${synonym}\\b`, 'gi');
        if (synRegex.test(repairedSql)) {
          repairedSql = repairedSql.replace(synRegex, standard);
          diagnosis += `Substituted column synonym '${synonym}' -> '${standard}'. `;
          ruleApplied = true;
        }
      }
    }
  }

  // ==================== 3. INVALID TABLE REPAIR ====================
  if (errorCategory === ERROR_CATEGORIES.INVALID_TABLE || (errorCategory === ERROR_CATEGORIES.UNKNOWN && identifiers.tableName)) {
    let targetTable = identifiers.tableName;
    let replacementTable = null;

    if (targetTable) {
      const normalizedTable = targetTable.toLowerCase();

      // Check pluralization / singular map
      if (TABLE_PLURAL_MAP[normalizedTable]) {
        replacementTable = TABLE_PLURAL_MAP[normalizedTable];
      }

      // Check catalog
      if (!replacementTable && catalog) {
        const tables = Array.isArray(catalog) ? catalog : (catalog.tables || []);
        const match = findBestMatch(targetTable, tables, 0.4);
        if (match && match.toLowerCase() !== targetTable.toLowerCase()) {
          replacementTable = match;
        }
      }

      // Check schema prefix qualification
      if (!replacementTable && catalog) {
        const tables = Array.isArray(catalog) ? catalog : (catalog.tables || []);
        const matchedTable = tables.find(t => {
          const name = typeof t === 'object' ? (t.name || '') : String(t);
          return name.toLowerCase() === normalizedTable;
        });
        if (matchedTable && matchedTable.schema && matchedTable.schema !== 'public' && !targetTable.includes('.')) {
          replacementTable = `${matchedTable.schema}.${matchedTable.name}`;
        }
      }

      if (replacementTable) {
        const tableRegex = new RegExp(`(\\bFROM|JOIN)\\s+["\`]?${targetTable}["\`]?`, 'gi');
        if (tableRegex.test(repairedSql)) {
          repairedSql = repairedSql.replace(tableRegex, `$1 ${replacementTable}`);
          diagnosis += `Replaced invalid table '${targetTable}' with '${replacementTable}'. `;
          ruleApplied = true;
        }
      }
    } else {
      // Check SQL for any known table singulars
      for (const [singular, plural] of Object.entries(TABLE_PLURAL_MAP)) {
        const tableRegex = new RegExp(`(\\bFROM|JOIN)\\s+["\`]?${singular}["\`]?\\b`, 'gi');
        if (tableRegex.test(repairedSql)) {
          repairedSql = repairedSql.replace(tableRegex, `$1 ${plural}`);
          diagnosis += `Substituted table name '${singular}' -> '${plural}'. `;
          ruleApplied = true;
        }
      }
    }
  }

  // ==================== 4. GROUP BY VIOLATION REPAIR ====================
  if (errorCategory === ERROR_CATEGORIES.GROUP_BY_VIOLATION) {
    const { nonAggregatedColumns } = analyzeSelectClause(repairedSql);
    const targetCol = identifiers.columnName;

    const columnsToAdd = [];
    if (targetCol && !columnsToAdd.includes(targetCol)) {
      columnsToAdd.push(targetCol);
    }
    for (const col of nonAggregatedColumns) {
      if (!columnsToAdd.includes(col)) {
        columnsToAdd.push(col);
      }
    }

    if (columnsToAdd.length > 0) {
      const groupByMatch = repairedSql.match(/\bGROUP\s+BY\s+([\s\S]*?)(?=\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|;|$)/i);

      if (groupByMatch) {
        const existingGroupColumns = groupByMatch[1].split(',').map(c => c.trim().toLowerCase());
        const missing = columnsToAdd.filter(c => !existingGroupColumns.includes(c.toLowerCase()));
        if (missing.length > 0) {
          const newGroupClause = `GROUP BY ${groupByMatch[1].trim()}, ${missing.join(', ')}`;
          repairedSql = repairedSql.replace(groupByMatch[0], newGroupClause);
          diagnosis += `Added missing columns [${missing.join(', ')}] to GROUP BY clause. `;
          ruleApplied = true;
        }
      } else {
        // Append GROUP BY clause before ORDER BY, HAVING, LIMIT or end
        const groupClause = `GROUP BY ${columnsToAdd.join(', ')}`;
        const insertionMatch = repairedSql.match(/\b(HAVING|ORDER\s+BY|LIMIT)\b/i);

        if (insertionMatch) {
          const insertIdx = repairedSql.indexOf(insertionMatch[0]);
          repairedSql = repairedSql.slice(0, insertIdx) + `${groupClause} ` + repairedSql.slice(insertIdx);
        } else {
          const endsWithSemicolon = repairedSql.trim().endsWith(';');
          const clean = endsWithSemicolon ? repairedSql.trim().slice(0, -1) : repairedSql.trim();
          repairedSql = `${clean} ${groupClause}${endsWithSemicolon ? ';' : ''}`;
        }
        diagnosis += `Added missing GROUP BY clause for [${columnsToAdd.join(', ')}]. `;
        ruleApplied = true;
      }
    }
  }

  // ==================== 5. DIVIDE BY ZERO REPAIR ====================
  if (errorCategory === ERROR_CATEGORIES.DIVIDE_BY_ZERO) {
    // Wrap division expressions with NULLIF(divisor, 0)
    const divisionRegex = /\/\s*([a-zA-Z0-9_.]+|\([^\)]+\))(?!\s*NULLIF)/gi;
    if (divisionRegex.test(repairedSql)) {
      repairedSql = repairedSql.replace(divisionRegex, (match, divisor) => {
        if (divisor.toLowerCase().includes('nullif')) return match;
        return `/ NULLIF(${divisor}, 0)`;
      });
      diagnosis += 'Wrapped division denominator in NULLIF(..., 0) to prevent division-by-zero. ';
      ruleApplied = true;
    }
  }

  // ==================== 6. AMBIGUOUS COLUMN REPAIR ====================
  if (errorCategory === ERROR_CATEGORIES.AMBIGUOUS_COLUMN && identifiers.columnName) {
    const targetCol = identifiers.columnName;
    const tables = extractTablesFromSQL(repairedSql);
    if (tables.length > 0) {
      const primaryTable = tables[0];
      // Check if primary table has an alias
      const aliasMatch = repairedSql.match(new RegExp(`\\bFROM\\s+["\`]?${primaryTable}["\`]?\\s+(?:AS\\s+)?([a-zA-Z0-9_]+)`, 'i'));
      const prefix = aliasMatch ? aliasMatch[1] : primaryTable;

      const colRegex = new RegExp(`(?<![a-zA-Z0-9_.])\\b${targetCol}\\b(?![a-zA-Z0-9_])`, 'g');
      if (colRegex.test(repairedSql)) {
        repairedSql = repairedSql.replace(colRegex, `${prefix}.${targetCol}`);
        diagnosis += `Qualified ambiguous column '${targetCol}' with table prefix '${prefix}'. `;
        ruleApplied = true;
      }
    }
  }

  // ==================== 7. LLM-ASSISTED FALLBACK ====================
  // If rule-based repair did not modify the query or if category is UNKNOWN, invoke AI assistant
  if ((!ruleApplied || repairedSql === sql || errorCategory === ERROR_CATEGORIES.UNKNOWN) && assistant) {
    try {
      diagnosis += 'Invoking AI assistant fallback repair. ';
      const aiRepaired = await executeLLMRepair({
        assistant,
        failedSql: sql,
        errorMessage,
        errorCategory,
        catalog,
        explanation,
      });

      if (aiRepaired && aiRepaired.trim() !== sql.trim()) {
        repairedSql = aiRepaired.trim();
        diagnosis += 'AI assistant generated corrected SQL. ';
        ruleApplied = true;
      }
    } catch (llmErr) {
      logger.warn('LLM repair fallback encountered error:', llmErr.message);
      diagnosis += `LLM fallback failed: ${llmErr.message}. `;
    }
  }

  return {
    repairedSql,
    diagnosis: diagnosis.trim() || 'No automatic repair available for this error pattern',
    ruleApplied,
  };
}

/**
 * Invokes LLM Assistant with targeted SQL repair prompt.
 * @param {object} options
 * @returns {Promise<string>}
 */
export async function executeLLMRepair({ assistant, failedSql, errorMessage, errorCategory, catalog, explanation }) {
  if (!assistant) return failedSql;

  const prompt = `
You are an expert SQL engineer. Fix the following failed SQL query so that it executes successfully.

[USER_INPUT]
Failed SQL:
${failedSql}

Error Category: ${errorCategory}
Error Message:
${errorMessage}
${explanation ? `Query Intent: ${explanation}` : ''}
[/USER_INPUT]

${catalog ? `Available Database Schema Metadata:\n${wrapUntrustedMetadata(catalog)}` : ''}

Instructions:
1. Return ONLY the valid, corrected read-only SQL query without explanation or markdown blocks (or within \`\`\`sql ... \`\`\`).
2. Fix all syntax errors, column name typos, table references, group by expressions, and type casts.
3. Ensure the corrected query performs ONLY read-only operations (SELECT).
`;

  let response = '';

  if (typeof assistant.getAIResponse === 'function') {
    response = await assistant.getAIResponse(prompt);
  } else if (typeof assistant.generateSQL === 'function') {
    response = await assistant.generateSQL(`Fix error: ${errorMessage} in query: ${failedSql}`, catalog || []);
  } else if (typeof assistant === 'function') {
    response = await assistant(prompt);
  }

  if (typeof response !== 'string') {
    return failedSql;
  }

  // Extract SQL from markdown code fences if present
  const codeBlockMatch = response.match(/```(?:sql)?\s*([\s\S]*?)```/i);
  if (codeBlockMatch) {
    return codeBlockMatch[1].trim();
  }

  return response.trim();
}

/**
 * Executes a SQL query with autonomous self-healing retries up to maxAttempts.
 * @param {object} options
 * @returns {Promise<object>} Result structure with healing trail & provenance
 */
export async function executeAndHealSQL({
  databaseId,
  sql,
  maxAttempts = 3,
  client,
  assistant = null,
  catalog = null,
  dialect = 'generic',
  maskPii = true,
  maskOptions = {},
  explanation = '',
}) {
  if (!sql || typeof sql !== 'string') {
    throw new Error('SQL query string is required');
  }

  const effectiveMaxAttempts = Math.max(1, Math.min(Number(maxAttempts) || 3, 5));
  let currentSql = sql;
  let attemptsUsed = 0;
  const healingTrail = [];
  let lastError = null;
  let success = false;
  let resultData = null;
  let executionStartTime = Date.now();
  let totalExecutionTimeMs = 0;

  // Pre-execution Read-Only Mode Security Check
  if (isReadOnlyMode()) {
    const blockedOp = detectWriteOperation(currentSql);
    if (blockedOp) {
      throw new Error(`Read-only mode: Blocked write operation '${blockedOp}' in SQL query`);
    }
  }

  // Schema catalog cache
  let activeCatalog = catalog;

  for (let attempt = 1; attempt <= effectiveMaxAttempts; attempt++) {
    attemptsUsed = attempt;
    const attemptStartTime = Date.now();

    // Check read-only security on candidate SQL
    if (isReadOnlyMode()) {
      const blockedOp = detectWriteOperation(currentSql);
      if (blockedOp) {
        healingTrail.push({
          attempt,
          failed_sql: currentSql,
          error_message: `Security violation: Blocked ${blockedOp} operation`,
          error_category: 'SECURITY_VIOLATION',
          diagnosis: 'Query rejected due to write operation in read-only mode',
          corrected_sql: null,
          timestamp: new Date().toISOString(),
        });
        lastError = `Security violation: Blocked ${blockedOp} operation in read-only mode`;
        break;
      }
    }

    try {
      let queryResult;
      if (client && typeof client.executeNativeQuery === 'function') {
        queryResult = await client.executeNativeQuery(databaseId, currentSql);
      } else if (typeof client === 'function') {
        queryResult = await client(databaseId, currentSql);
      } else {
        throw new Error('Database client does not provide executeNativeQuery method');
      }

      totalExecutionTimeMs = Date.now() - executionStartTime;
      success = true;

      // Extract rows and cols
      const rawRows = queryResult.data?.rows || queryResult.rows || [];
      const rawCols = queryResult.data?.cols || queryResult.columns || [];
      const columnNames = rawCols.map(col =>
        typeof col === 'object' && col !== null ? (col.name || col.display_name || '') : String(col)
      );

      // PII Masking
      const maskingEnabled = isPiiMaskingEnabled({ mask_pii: maskPii });
      const rows = maskingEnabled ? rawRows.map(row => maskRow(row, columnNames, maskOptions)) : rawRows;

      resultData = {
        columns: rawCols.map(c => ({
          name: typeof c === 'object' && c !== null ? (c.name || c.display_name || '') : String(c),
          base_type: typeof c === 'object' && c !== null ? (c.base_type || c.type || 'unknown') : 'unknown',
        })),
        rows,
        row_count: rows.length,
        execution_time_ms: Date.now() - attemptStartTime,
      };

      break; // Query succeeded, exit retry loop
    } catch (err) {
      lastError = err.message || String(err);
      const errorCategory = classifySQLError(lastError, currentSql);

      logger.warn(`SQL Execution Attempt ${attempt}/${effectiveMaxAttempts} failed [${errorCategory}]: ${lastError}`);

      // If last attempt, record failure and exit
      if (attempt >= effectiveMaxAttempts) {
        healingTrail.push({
          attempt,
          failed_sql: currentSql,
          error_message: lastError,
          error_category: errorCategory,
          diagnosis: `Max attempts (${effectiveMaxAttempts}) reached without resolution.`,
          corrected_sql: null,
          timestamp: new Date().toISOString(),
        });
        break;
      }

      // Lazily introspect database schema if catalog is not yet populated
      if (!activeCatalog && client && typeof client.getDatabaseTables === 'function') {
        try {
          activeCatalog = await client.getDatabaseTables(databaseId);
        } catch (catErr) {
          logger.debug('Catalog introspection failed:', catErr.message);
          activeCatalog = null;
        }
      }

      // Repair SQL
      const repairResult = await repairSQLError({
        sql: currentSql,
        errorCategory,
        errorMessage: lastError,
        catalog: activeCatalog,
        dialect,
        assistant,
        explanation,
      });

      const correctedSql = repairResult.repairedSql;

      healingTrail.push({
        attempt,
        failed_sql: currentSql,
        error_message: lastError,
        error_category: errorCategory,
        diagnosis: repairResult.diagnosis,
        corrected_sql: correctedSql,
        timestamp: new Date().toISOString(),
      });

      // Update current SQL for next attempt
      currentSql = correctedSql;
    }
  }

  totalExecutionTimeMs = Date.now() - executionStartTime;

  const _provenance = {
    ai_generated: healingTrail.length > 0,
    tool: 'ai_sql_execute_and_heal',
    review_required: false,
    timestamp: new Date().toISOString(),
    provider: assistant?.aiProvider || 'heuristic_rule_engine',
    model: assistant?.model || 'sql-healing-v1',
    generation_parameters: {
      database_id: databaseId,
      max_attempts: effectiveMaxAttempts,
      attempts_used: attemptsUsed,
      healed: healingTrail.length > 0 && success,
      enforce_read_only: true,
    },
    healing_trail: healingTrail,
  };

  return {
    success,
    data: resultData,
    original_sql: sql,
    final_sql: currentSql,
    attempts_used: attemptsUsed,
    healed: healingTrail.length > 0 && success,
    healing_trail: healingTrail,
    error: success ? null : lastError,
    execution_time_ms: totalExecutionTimeMs,
    _provenance,
  };
}

/**
 * Class wrapping self-healing SQL engine
 */
export class SqlHealingEngine {
  constructor(config = {}) {
    this.metabaseClient = config.metabaseClient || null;
    this.aiAssistant = config.aiAssistant || null;
    this.defaultMaxAttempts = config.maxAttempts || 3;
    this.dialect = config.dialect || 'generic';
  }

  async executeAndHeal({ databaseId, sql, maxAttempts, maskPii = true, explanation = '' }) {
    return await executeAndHealSQL({
      databaseId,
      sql,
      maxAttempts: maxAttempts || this.defaultMaxAttempts,
      client: this.metabaseClient,
      assistant: this.aiAssistant,
      dialect: this.dialect,
      maskPii,
      explanation,
    });
  }

  classifyError(errorMessage, sql = '') {
    return classifySQLError(errorMessage, sql);
  }

  async repairError(options) {
    return await repairSQLError({
      ...options,
      assistant: options.assistant || this.aiAssistant,
      dialect: options.dialect || this.dialect,
    });
  }
}
