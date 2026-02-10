/**
 * SQL Sanitizer Utility
 * Prevents SQL injection in direct SQL queries
 * Used by handlers that build raw SQL strings
 */

/**
 * Sanitize a string value for safe SQL interpolation
 * Escapes single quotes and backslashes
 * @param {any} val - Value to sanitize
 * @returns {string} Safe SQL string (without surrounding quotes)
 */
export function sanitizeString(val) {
    if (val === null || val === undefined) return 'NULL';
    return String(val).replace(/\\/g, '\\\\').replace(/'/g, "''");
}

/**
 * Sanitize and validate a numeric value
 * @param {any} val - Value to validate as number
 * @returns {number} Validated number
 * @throws {Error} If value is not a valid number
 */
export function sanitizeNumber(val) {
    if (val === null || val === undefined) return 0;
    const num = Number(val);
    if (!Number.isFinite(num)) {
        throw new Error(`Invalid numeric value: ${val}`);
    }
    return num;
}

/**
 * Sanitize an identifier (table name, column name)
 * Only allows alphanumeric characters, underscores, dots, and hyphens
 * @param {string} val - Identifier to sanitize
 * @returns {string} Safe identifier
 * @throws {Error} If identifier contains invalid characters
 */
export function sanitizeIdentifier(val) {
    if (!val || typeof val !== 'string') {
        throw new Error('Identifier must be a non-empty string');
    }
    if (!/^[a-zA-Z0-9_.\\-]+$/.test(val)) {
        throw new Error(`Invalid identifier: ${val}. Only alphanumeric, underscore, dot, and hyphen allowed.`);
    }
    return val;
}

/**
 * Sanitize a LIKE pattern to prevent pattern injection
 * Escapes %, _, and single quotes
 * @param {string} val - Pattern to sanitize
 * @returns {string} Safe LIKE pattern (without surrounding quotes or %)
 */
export function sanitizeLikePattern(val) {
    if (val === null || val === undefined) return '';
    return String(val)
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "''")
        .replace(/%/g, '\\%')
        .replace(/_/g, '\\_');
}

/**
 * Sanitize a JSON string for SQL insertion
 * Validates JSON structure and escapes for SQL
 * @param {object|string} val - JSON object or string
 * @returns {string} Safe JSON string for SQL
 */
export function sanitizeJson(val) {
    if (val === null || val === undefined) return '{}';
    const str = typeof val === 'string' ? val : JSON.stringify(val);
    // Validate it's actual JSON
    try {
        JSON.parse(str);
    } catch (e) {
        throw new Error(`Invalid JSON value: ${str.substring(0, 50)}`);
    }
    return str.replace(/'/g, "''");
}

/**
 * Sanitize an interval string (e.g., "30 days")
 * Only allows number + known unit
 * @param {number} value - Numeric value
 * @param {string} unit - Time unit (days, hours, minutes)
 * @returns {string} Safe interval string
 */
export function sanitizeInterval(value, unit = 'days') {
    const num = sanitizeNumber(value);
    const allowedUnits = ['days', 'hours', 'minutes', 'seconds', 'weeks', 'months'];
    if (!allowedUnits.includes(unit.toLowerCase())) {
        throw new Error(`Invalid interval unit: ${unit}`);
    }
    return `${num} ${unit.toLowerCase()}`;
}
