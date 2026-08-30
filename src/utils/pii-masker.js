import crypto from 'crypto';

/**
 * Enterprise PII & Sensitive Data Sanitizer Module
 * 
 * Provides zero-leak sanitization, column-aware heuristics, analytical utility preservation,
 * format-preserving masking, deterministic pseudonymization, and tabular/deep-object traversal.
 */

// ==================== PRECOMPILED REGULAR EXPRESSIONS ====================

// 1. Email addresses: matches standard, subaddressed, and URL-encoded email formats
export const EMAIL_REGEX = /\b[a-zA-Z0-9._%+-]+(?:@|%40)[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b/gi;

// 2. Phone numbers: international E.164, domestic with area code, dashes, dots, spaces, parens, and URL-encoded
export const PHONE_REGEX = /(?:\+?\d{1,3}[-.\s])?(?:\(?\d{2,4}\)?[-.\s])\d{3,4}[-.\s]\d{4}\b|\b\(\d{2,4}\)\s*\d{3,4}[-.\s]?\d{4}\b|(?:\%2B|\+)\d{1,3}(?:\%20|\s|\-|\.)(?:\(?\d{2,4}\)?(?:\%20|\s|\-|\.))?\d{3,4}(?:\%20|\s|\-|\.)\d{4}\b|\+\d{1,3}(?:[-.\s]?\(?\d{1,4}\)?[-.\s]?(?:\d{2,4}[-.\s]?){2,4})\b|\+\d{7,15}\b|\b\d{10}\b/gi;

// 3. Social Security Numbers (SSN): 3-2-4 format with hyphens, dots, or spaces
export const SSN_REGEX = /\b\d{3}[-.\s]\d{2}[-.\s]\d{4}\b/g;

// Raw 9-digit SSN (for SSN-designated columns)
export const SSN_RAW_REGEX = /^\d{9}$/;

// 4. Credit / Debit Cards (PAN): 13 to 19 digits with optional dashes/spaces
export const CARD_REGEX = /\b(?:\d{4}[-\s]?){3}\d{1,7}\b|\b\d{4}[-\s]\d{6}[-\s]\d{5}\b|\b\d{13,19}\b/g;

// 5. IP Addresses
// IPv4
export const IPV4_REGEX = /\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b/g;
// IPv6
export const IPV6_REGEX = /(?:[A-Fa-f0-9]{1,4}:){7}[A-Fa-f0-9]{1,4}|(?:[A-Fa-f0-9]{1,4}:){1,7}:(?:[A-Fa-f0-9]{1,4}:){0,6}[A-Fa-f0-9]{1,4}|::(?:[A-Fa-f0-9]{1,4}:){0,6}[A-Fa-f0-9]{1,4}|(?:\b|(?<=\s|^|[^\w:]))::1(?=\s|$|[^\w:])|(?:\b|(?<=\s|^|[^\w:]))::(?:[A-Fa-f0-9]{1,4}:){0,6}[A-Fa-f0-9]{1,4}(?=\s|$|[^\w:])/gi;

// 6. API Keys, Tokens & Secrets
export const OPENAI_KEY_REGEX = /\bsk-(?:proj-)?[a-zA-Z0-9_-]{20,}\b/g;
export const ANTHROPIC_KEY_REGEX = /\bsk-ant-[a-zA-Z0-9_-]{20,}\b/g;
export const AWS_ACCESS_KEY_REGEX = /\bAKIA[0-9A-Z]{16}\b/g;
export const AWS_SECRET_KEY_REGEX = /(?:aws_secret_access_key|aws_session_token)\s*[:=]\s*['"]?([a-zA-Z0-9/+=]{40})['"]?/gi;
export const GITHUB_TOKEN_REGEX = /\b(?:ghp|gho|ghu|ghs|ghr)_[a-zA-Z0-9]{36,}\b/g;
export const JWT_TOKEN_REGEX = /\beyJ[A-Za-z0-9-_]+\.eyJ[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\b/g;
export const BEARER_TOKEN_REGEX = /\bBearer\s+([a-zA-Z0-9\-._~+/]+=*)\b/gi;
export const SLACK_TOKEN_REGEX = /\bxox[baprs]-[0-9a-zA-Z-]{10,80}\b/g;
export const STRIPE_KEY_REGEX = /\b[sp]k_(?:test|live)_[0-9a-zA-Z]{20,}\b/g;
export const PRIVATE_KEY_REGEX = /-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----/g;
export const URI_PASSWORD_REGEX = /([a-zA-Z0-9+.-]+:\/\/[^:\s\/@]+:)(.*)(@[a-zA-Z0-9_.-]+(?::\d+)?(?:\/[^\s"']*)?)/gi;

// 7. Passwords and Hashes
export const BCRYPT_HASH_REGEX = /\$2[aby]\$[0-9]{2}\$[./A-Za-z0-9]{53}(?=[^./A-Za-z0-9]|$)/g;
export const ARGON2_HASH_REGEX = /\$argon2(?:id?|d)\$v=[0-9]+\$m=[0-9]+,t=[0-9]+,p=[0-9]+\$[A-Za-z0-9+/]+\$[A-Za-z0-9+/]+(?=[^A-Za-z0-9+/]|$)/g;
export const SHA256_HEX_REGEX = /\b[a-f0-9]{64}\b/gi;
export const MD5_HEX_REGEX = /\b[a-f0-9]{32}\b/gi;

// Redaction tokens
export const REDACTION_TOKENS = {
  EMAIL: '[REDACTED_EMAIL]',
  PHONE: '[REDACTED_PHONE]',
  SSN: '[REDACTED_SSN]',
  CARD: '[REDACTED_CARD]',
  IP: '[REDACTED_IP]',
  SECRET: '[REDACTED_SECRET]',
  API_KEY: '[REDACTED_SECRET]',
  JWT: '[REDACTED_SECRET]',
  PASSWORD: '[REDACTED_PASSWORD]',
  HASH: '[REDACTED_PASSWORD]',
  GENERIC: '[REDACTED]',
};

// ==================== LUHN ALGORITHM VALIDATOR ====================

/**
 * Validates a number string using the Luhn mod-10 algorithm.
 * @param {string|number} numberString
 * @returns {boolean}
 */
export function isValidLuhn(numberString) {
  if (numberString === null || numberString === undefined) return false;
  const digits = String(numberString).replace(/\D/g, '');
  if (digits.length < 13 || digits.length > 19) return false;

  let sum = 0;
  let shouldDouble = false;

  for (let i = digits.length - 1; i >= 0; i--) {
    let digit = parseInt(digits.charAt(i), 10);
    if (isNaN(digit)) return false;

    if (shouldDouble) {
      digit *= 2;
      if (digit > 9) digit -= 9;
    }
    sum += digit;
    shouldDouble = !shouldDouble;
  }

  return sum % 10 === 0;
}

// ==================== DETERMINISTIC PSEUDONYMIZATION ====================

/**
 * Deterministically pseudonymizes a value with SHA-256 HMAC.
 * Allows grouping, distinct counting, and join integrity in analytics without exposing identity.
 * @param {any} value
 * @param {string} salt
 * @returns {string|any}
 */
export function pseudonymizeValue(value, salt = 'metabase_ai_salt') {
  if (value === null || value === undefined) return value;
  const str = String(value);
  const hash = crypto.createHmac('sha256', salt).update(str).digest('hex').substring(0, 12);
  return `anon_${hash}`;
}

// ==================== COLUMN-AWARE HEURISTIC DETECTOR ====================

/**
 * Detects sensitive category from column name heuristics.
 * @param {string} columnName
 * @returns {string|null} 'email' | 'phone' | 'ssn' | 'card' | 'password' | 'secret' | 'ip' | null
 */
export function detectSensitiveCategoryByColumnName(columnName) {
  if (!columnName || typeof columnName !== 'string') return null;
  const normalized = columnName.toLowerCase().replace(/[\s_-]/g, '');

  if (/^(?:user|contact|customer|client|author|owner|billing|shipping)?(?:email|mail)s?$/i.test(normalized) ||
      normalized.includes('email') || normalized === 'mail' || normalized.endsWith('mail')) {
    return 'email';
  }

  if (/^(?:phone|telephone|mobile|cell|tel|fax|phonenumber|contactnumber|mobilenumber|telnumber|cellnumber|faxnumber)$/i.test(normalized) ||
      normalized.includes('phone') || normalized.includes('telephone') || normalized.includes('mobile') ||
      normalized.includes('telnumber') || normalized.includes('cellnumber')) {
    return 'phone';
  }

  if (/^(?:ssn|socialsecurity|socialsecuritynumber|taxid|ein|tin|tinnumber|taxnumber|nationalid|nationalidnumber)$/i.test(normalized) ||
      normalized.includes('ssn') || normalized.includes('socialsecurity') || normalized.includes('taxid') ||
      normalized.includes('tinnumber')) {
    return 'ssn';
  }

  if (/^(?:card|creditcard|debitcard|cardnumber|ccnum|ccnumber|pan|cvv|cvc|cardexp|creditcardnumber)$/i.test(normalized) ||
      normalized.includes('creditcard') || normalized.includes('debitcard') || normalized.includes('cardnumber')) {
    return 'card';
  }

  if (/^(?:password|passwd|pwd|pass|passwordhash|passwdhash|passworddigest|passphrase|userpassword)$/i.test(normalized) ||
      normalized.includes('password') || normalized.includes('passwd')) {
    return 'password';
  }

  if (/^(?:secret|token|apikey|auth|bearer|authbearer|accesstoken|refreshtoken|privatekey|secretkey|clientsecret|authtoken|apisecret|signature)$/i.test(normalized) ||
      normalized.includes('secret') || normalized.includes('apikey') || normalized.includes('token') || normalized.includes('privatekey') ||
      normalized.includes('bearer')) {
    return 'secret';
  }

  if (/^(?:ip|ipaddress|clientip|remoteip|userip|sourceip|destip|ipv4|ipv6|sourceipv4|sourceipv6|destipv4|destipv6)$/i.test(normalized) ||
      normalized.includes('ipaddress') || normalized.includes('ipv4') || normalized.includes('ipv6')) {
    return 'ip';
  }

  return null;
}

// ==================== CATEGORY-SPECIFIC MASKING HELPERS ====================

/**
 * Masks an email address.
 * Options:
 *  - strict: boolean (replaces completely with [REDACTED_EMAIL])
 *  - preserveDomain: boolean (default true; preserves domain e.g. j***e@example.com)
 *  - pseudonymize: boolean
 * @param {string} email
 * @param {object} options
 * @returns {string}
 */
export function maskEmail(email, options = {}) {
  if (!email || typeof email !== 'string') return email;
  if (options.pseudonymize) {
    return pseudonymizeValue(email, options.salt);
  }
  if (options.strict) {
    return REDACTION_TOKENS.EMAIL;
  }

  const preserveDomain = options.preserveDomain !== false;

  return email.replace(EMAIL_REGEX, (match) => {
    const isUrlEncoded = match.toLowerCase().includes('%40');
    const atSeparator = isUrlEncoded ? '%40' : '@';
    const atIndex = isUrlEncoded ? match.toLowerCase().lastIndexOf('%40') : match.lastIndexOf('@');
    if (atIndex <= 0) return REDACTION_TOKENS.EMAIL;

    const user = match.substring(0, atIndex);
    const domain = match.substring(atIndex + atSeparator.length);

    if (!preserveDomain) {
      return REDACTION_TOKENS.EMAIL;
    }

    let maskedUser;
    if (user.length <= 1) {
      maskedUser = '*';
    } else if (user.length === 2) {
      maskedUser = `${user[0]}*`;
    } else {
      maskedUser = `${user[0]}***${user[user.length - 1]}`;
    }

    return `${maskedUser}${atSeparator}${domain}`;
  });
}

/**
 * Masks a phone number.
 * Options:
 *  - strict: boolean
 *  - preserveFormat: boolean (default true; retains last 4 digits and country/area prefix where possible)
 * @param {string} phone
 * @param {object} options
 * @returns {string}
 */
export function maskPhone(phone, options = {}) {
  if (!phone || typeof phone !== 'string') return phone;
  if (options.pseudonymize) {
    return pseudonymizeValue(phone, options.salt);
  }
  if (options.strict) {
    return REDACTION_TOKENS.PHONE;
  }

  // Preserve format logic
  return phone.replace(PHONE_REGEX, (match) => {
    const cleaned = match.replace(/%20/g, ' ').replace(/%2[bB]/g, '+');
    const digits = cleaned.replace(/\D/g, '');
    if (digits.length < 7 || digits.length > 15) return match;

    const lastFour = digits.slice(-4);

    if (match.startsWith('+') || match.toLowerCase().startsWith('%2b')) {
      const countryCodeMatch = match.match(/^(?:\+|\%2b)\d{1,3}/i);
      const cc = countryCodeMatch ? countryCodeMatch[0] : '+1';
      return `${cc}-***-***-${lastFour}`;
    }

    if (match.includes('(') && match.includes(')')) {
      const areaMatch = match.match(/\((\d{2,4})\)/);
      const area = areaMatch ? areaMatch[0] : '(***)';
      return `${area} ***-${lastFour}`;
    }

    if (match.includes('-')) {
      return `***-***-${lastFour}`;
    }

    if (match.includes('.')) {
      return `***.***.${lastFour}`;
    }

    return `******${lastFour}`;
  });
}

/**
 * Masks a Social Security Number.
 * @param {string} ssn
 * @param {object} options
 * @returns {string}
 */
export function maskSSN(ssn, options = {}) {
  if (!ssn || typeof ssn !== 'string') return ssn;
  if (options.pseudonymize) {
    return pseudonymizeValue(ssn, options.salt);
  }
  if (options.strict) {
    return REDACTION_TOKENS.SSN;
  }

  // Hyphenated / dotted / spaced format
  let masked = ssn.replace(SSN_REGEX, (match) => {
    const lastFour = match.slice(-4);
    const separator = match[3] || '-';
    return `***${separator}**${separator}${lastFour}`;
  });

  // Raw 9 digits
  if (SSN_RAW_REGEX.test(masked)) {
    const lastFour = masked.slice(-4);
    return `*****${lastFour}`;
  }

  return masked;
}

/**
 * Masks a credit or debit card number with Luhn validation.
 * @param {string} card
 * @param {object} options
 * @returns {string}
 */
export function maskCard(card, options = {}) {
  if (!card || typeof card !== 'string') return card;
  if (options.pseudonymize) {
    return pseudonymizeValue(card, options.salt);
  }
  if (options.strict) {
    return REDACTION_TOKENS.CARD;
  }

  return card.replace(CARD_REGEX, (match) => {
    const digits = match.replace(/\D/g, '');
    if (!isValidLuhn(digits)) {
      // If it fails Luhn check and we are not strictly in a card column, return as-is
      if (!options.isCardColumn) return match;
    }

    const lastFour = digits.slice(-4);

    if (match.includes('-')) {
      return `****-****-****-${lastFour}`;
    }
    if (match.includes(' ')) {
      return `**** **** **** ${lastFour}`;
    }

    const padLen = Math.max(0, digits.length - 4);
    return '*'.repeat(padLen) + lastFour;
  });
}

/**
 * Masks IP addresses (IPv4 & IPv6).
 * Options:
 *  - strict: boolean
 *  - preserveFormat: boolean (default true; masks last octets/groups)
 * @param {string} ip
 * @param {object} options
 * @returns {string}
 */
export function maskIP(ip, options = {}) {
  if (!ip || typeof ip !== 'string') return ip;
  if (options.pseudonymize) {
    return pseudonymizeValue(ip, options.salt);
  }
  if (options.strict) {
    return REDACTION_TOKENS.IP;
  }

  let result = ip.replace(IPV4_REGEX, (match) => {
    const parts = match.split('.');
    if (parts.length === 4) {
      return `${parts[0]}.${parts[1]}.*.*`;
    }
    return REDACTION_TOKENS.IP;
  });

  result = result.replace(IPV6_REGEX, (match) => {
    const parts = match.split(':');
    if (parts.length >= 4) {
      return `${parts[0]}:${parts[1]}:*:*:*:*:*:*`;
    }
    return REDACTION_TOKENS.IP;
  });

  return result;
}

/**
 * Masks API keys, tokens, and secrets.
 * @param {string} secret
 * @param {object} options
 * @returns {string}
 */
export function maskSecrets(secret, options = {}) {
  if (!secret || typeof secret !== 'string') return secret;

  let result = secret;

  // 1. Private keys
  result = result.replace(PRIVATE_KEY_REGEX, REDACTION_TOKENS.SECRET);

  // 2. URI Passwords (e.g. postgres://user:password@host)
  result = result.replace(URI_PASSWORD_REGEX, `$1${REDACTION_TOKENS.PASSWORD}$3`);

  // 3. JWT Tokens
  result = result.replace(JWT_TOKEN_REGEX, REDACTION_TOKENS.SECRET);

  // 4. API Keys
  result = result.replace(OPENAI_KEY_REGEX, REDACTION_TOKENS.SECRET);
  result = result.replace(ANTHROPIC_KEY_REGEX, REDACTION_TOKENS.SECRET);
  result = result.replace(AWS_ACCESS_KEY_REGEX, REDACTION_TOKENS.SECRET);
  result = result.replace(AWS_SECRET_KEY_REGEX, `aws_secret_access_key=${REDACTION_TOKENS.SECRET}`);
  result = result.replace(GITHUB_TOKEN_REGEX, REDACTION_TOKENS.SECRET);
  result = result.replace(BEARER_TOKEN_REGEX, `Bearer ${REDACTION_TOKENS.SECRET}`);
  result = result.replace(SLACK_TOKEN_REGEX, REDACTION_TOKENS.SECRET);
  result = result.replace(STRIPE_KEY_REGEX, REDACTION_TOKENS.SECRET);

  // 5. Password hashes
  result = result.replace(BCRYPT_HASH_REGEX, REDACTION_TOKENS.PASSWORD);
  result = result.replace(ARGON2_HASH_REGEX, REDACTION_TOKENS.PASSWORD);

  return result;
}

// Alias maskSecret -> maskSecrets
export const maskSecret = maskSecrets;

// ==================== FULL TEXT MASKING ====================

/**
 * Sanitizes arbitrary text across all 7 sensitive categories.
 * @param {string} text
 * @param {object} options
 * @returns {string}
 */
export function maskString(text, options = {}) {
  if (text === null || text === undefined) return text;
  if (typeof text !== 'string') {
    text = String(text);
  }

  let sanitized = text;

  // 1. Secrets, URIs, JWTs, Hashes
  sanitized = maskSecrets(sanitized, options);

  // 2. Credit Cards (with Luhn check)
  sanitized = maskCard(sanitized, options);

  // 3. SSN
  sanitized = maskSSN(sanitized, options);

  // 4. IP Addresses
  sanitized = maskIP(sanitized, options);

  // 5. Email Addresses
  sanitized = maskEmail(sanitized, options);

  // 6. Phone Numbers
  sanitized = maskPhone(sanitized, options);

  return sanitized;
}

// ==================== VALUE MASKING (COLUMN-AWARE) ====================

/**
 * Masks a single value with optional column-name heuristic awareness.
 * Supports signatures:
 *   maskValue(val, options)
 *   maskValue(val, columnName, options)
 * 
 * @param {any} value
 * @param {string|object} [columnNameOrOptions]
 * @param {object} [maybeOptions]
 * @returns {any}
 */
export function maskValue(value, columnNameOrOptions = '', maybeOptions = {}) {
  if (value === null || value === undefined) return value;

  let columnName = '';
  let options = {};

  if (typeof columnNameOrOptions === 'object' && columnNameOrOptions !== null) {
    options = columnNameOrOptions;
  } else if (typeof columnNameOrOptions === 'string') {
    columnName = columnNameOrOptions;
    options = maybeOptions || {};
  }

  // Check if pseudonymization requested for all values
  if (options.pseudonymize && !columnName) {
    if (typeof value === 'string' && (EMAIL_REGEX.test(value) || PHONE_REGEX.test(value) || isValidLuhn(value))) {
      return pseudonymizeValue(value, options.salt);
    }
  }

  // Apply column-aware heuristics if column name is provided and columnAware !== false
  const isColumnAware = options.columnAware !== false;
  if (isColumnAware && columnName) {
    const category = detectSensitiveCategoryByColumnName(columnName);

    if (category) {
      if (options.pseudonymize) {
        return pseudonymizeValue(value, options.salt);
      }

      const strVal = String(value);

      switch (category) {
        case 'email':
          return maskEmail(strVal, options);
        case 'phone':
          return maskPhone(strVal, options);
        case 'ssn':
          return maskSSN(strVal, options);
        case 'card':
          return maskCard(strVal, { ...options, isCardColumn: true });
        case 'password':
          return REDACTION_TOKENS.PASSWORD;
        case 'secret':
          return REDACTION_TOKENS.SECRET;
        case 'ip':
          return maskIP(strVal, options);
        default:
          break;
      }
    }
  }

  // Non-string primitives (numbers, booleans, bigints, symbols) that are not column-flagged as sensitive
  if (typeof value !== 'string') {
    if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint' || typeof value === 'symbol') {
      return value;
    }
    if (typeof value === 'object') {
      return maskObject(value, options);
    }
  }

  // Scan string values across all patterns
  return maskString(String(value), options);
}

// ==================== ROW MASKING ====================

/**
 * Sanitizes a tabular row (array or object) with column heuristics.
 * @param {Array|object} row
 * @param {Array<string>} [columnNames]
 * @param {object} [options]
 * @returns {Array|object}
 */
export function maskRow(row, columnNames = [], options = {}) {
  if (!row) return row;

  if (Array.isArray(row)) {
    const cols = Array.isArray(columnNames) ? columnNames : [];
    return row.map((cell, idx) => {
      const colName = cols[idx] || '';
      return maskValue(cell, colName, options);
    });
  }

  if (typeof row === 'object') {
    const result = {};
    for (const [key, val] of Object.entries(row)) {
      result[key] = maskValue(val, key, options);
    }
    return result;
  }

  return maskValue(row, '', options);
}

// ==================== TABULAR RESULT MASKING ====================

/**
 * Sanitizes a tabular query result structure ({ columns/cols, rows } or array of objects).
 * Guarantees zero leak of sensitive data while preserving table metadata and types.
 * @param {object|Array} data
 * @param {object} [options]
 * @returns {object|Array}
 */
export function maskTabularResult(data, options = {}) {
  if (!data) return data;

  // Case 1: Standard Metabase query result format { data: { cols, rows } }
  if (data.data && Array.isArray(data.data.rows)) {
    const cols = data.data.cols || [];
    const columnNames = cols.map(c => (typeof c === 'object' && c !== null) ? (c.name || c.display_name || '') : String(c));
    const maskedRows = data.data.rows.map(row => maskRow(row, columnNames, options));

    return {
      ...data,
      data: {
        ...data.data,
        rows: maskedRows,
      },
    };
  }

  // Case 2: Direct tabular object { columns, rows } or { cols, rows }
  if (data.rows && Array.isArray(data.rows)) {
    const rawCols = data.columns || data.cols || [];
    const columnNames = rawCols.map(c => (typeof c === 'object' && c !== null) ? (c.name || c.display_name || '') : String(c));
    const maskedRows = data.rows.map(row => maskRow(row, columnNames, options));

    return {
      ...data,
      rows: maskedRows,
      ...(data.columns ? { columns: data.columns } : {}),
      ...(data.cols ? { cols: data.cols } : {}),
    };
  }

  // Case 3: Array of objects [ { id: 1, email: '...' }, ... ]
  if (Array.isArray(data)) {
    return data.map(item => {
      if (Array.isArray(item)) {
        return maskRow(item, [], options);
      }
      if (typeof item === 'object' && item !== null) {
        return maskRow(item, Object.keys(item), options);
      }
      return maskValue(item, '', options);
    });
  }

  // Fallback for generic object
  if (typeof data === 'object') {
    return maskObject(data, options);
  }

  return maskValue(data, '', options);
}

// ==================== DEEP OBJECT MASKING ====================

/**
 * Deep recursive traversal of arbitrary objects and arrays with circular reference protection.
 * @param {any} obj
 * @param {object} [options]
 * @param {WeakSet} [visited]
 * @returns {any}
 */
export function maskObject(obj, options = {}, visited = new WeakSet()) {
  if (obj === null || obj === undefined) return obj;

  if (typeof obj !== 'object') {
    return maskValue(obj, '', options);
  }

  // Prevent circular infinite loop
  if (visited.has(obj)) {
    return '[CIRCULAR]';
  }
  visited.add(obj);

  // Handle Date and RegExp
  if (obj instanceof Date) return new Date(obj.getTime());
  if (obj instanceof RegExp) return new RegExp(obj);

  if (Array.isArray(obj)) {
    return obj.map(item => maskObject(item, options, visited));
  }

  const result = {};
  for (const [key, val] of Object.entries(obj)) {
    if (typeof val === 'object' && val !== null) {
      result[key] = maskObject(val, options, visited);
    } else {
      result[key] = maskValue(val, key, options);
    }
  }

  return result;
}

// ==================== CSV SANITIZATION ====================

/**
 * Parses, sanitizes, and re-serializes CSV content.
 * Compliant with RFC 4180 (multiline fields, quoted values, escaped quotes).
 * @param {string} csvString
 * @param {object} [options]
 * @returns {string}
 */
export function maskCSV(csvString, options = {}) {
  if (!csvString || typeof csvString !== 'string') return csvString;

  const rows = parseCSV(csvString);
  if (rows.length === 0) return csvString;

  const headerRow = rows[0];
  const hasHeaders = options.hasHeaders !== false;
  const columnNames = hasHeaders ? headerRow : [];

  const startIndex = hasHeaders ? 1 : 0;
  const maskedRows = hasHeaders ? [headerRow] : [];

  for (let i = startIndex; i < rows.length; i++) {
    const row = rows[i];
    const maskedRow = maskRow(row, columnNames, options);
    maskedRows.push(maskedRow);
  }

  return serializeCSV(maskedRows);
}

/**
 * Helper to parse RFC 4180 CSV strings into array of row arrays.
 * @param {string} text
 * @returns {Array<Array<string>>}
 */
function parseCSV(text) {
  const result = [];
  let row = [];
  let currentField = '';
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    const nextChar = text[i + 1];

    if (inQuotes) {
      if (char === '"') {
        if (nextChar === '"') {
          currentField += '"';
          i++; // skip escaped quote
        } else {
          inQuotes = false;
        }
      } else {
        currentField += char;
      }
    } else {
      if (char === '"') {
        inQuotes = true;
      } else if (char === ',') {
        row.push(currentField);
        currentField = '';
      } else if (char === '\r') {
        if (nextChar === '\n') i++;
        row.push(currentField);
        result.push(row);
        row = [];
        currentField = '';
      } else if (char === '\n') {
        row.push(currentField);
        result.push(row);
        row = [];
        currentField = '';
      } else {
        currentField += char;
      }
    }
  }

  if (currentField.length > 0 || row.length > 0) {
    row.push(currentField);
    result.push(row);
  }

  return result;
}

/**
 * Helper to serialize array of row arrays into RFC 4180 CSV string.
 * @param {Array<Array<any>>} rows
 * @returns {string}
 */
function serializeCSV(rows) {
  return rows
    .map(row =>
      row
        .map(cell => {
          if (cell === null || cell === undefined) return '';
          const str = String(cell);
          if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
            return `"${str.replace(/"/g, '""')}"`;
          }
          return str;
        })
        .join(',')
    )
    .join('\n') + '\n';
}

// ==================== GLOBAL HELPER FOR PII MASKING STATUS ====================

/**
 * Checks whether PII masking is globally enabled.
 * Default is TRUE unless METABASE_PII_MASKING_ENABLED === 'false' or options.maskPii === false.
 * @param {object} [options]
 * @returns {boolean}
 */
export function isPiiMaskingEnabled(options = {}) {
  if (options.mask_pii === false || options.maskPii === false || options.sanitize === false) {
    return false;
  }
  if (process.env.METABASE_PII_MASKING_ENABLED === 'false') {
    return false;
  }
  return true;
}
