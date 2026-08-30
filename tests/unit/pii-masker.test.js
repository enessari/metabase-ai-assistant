import {
  EMAIL_REGEX,
  PHONE_REGEX,
  SSN_REGEX,
  CARD_REGEX,
  IPV4_REGEX,
  IPV6_REGEX,
  OPENAI_KEY_REGEX,
  ANTHROPIC_KEY_REGEX,
  AWS_ACCESS_KEY_REGEX,
  GITHUB_TOKEN_REGEX,
  JWT_TOKEN_REGEX,
  BEARER_TOKEN_REGEX,
  BCRYPT_HASH_REGEX,
  ARGON2_HASH_REGEX,
  REDACTION_TOKENS,
  isValidLuhn,
  pseudonymizeValue,
  detectSensitiveCategoryByColumnName,
  maskEmail,
  maskPhone,
  maskSSN,
  maskCard,
  maskIP,
  maskSecrets,
  maskString,
  maskValue,
  maskRow,
  maskTabularResult,
  maskObject,
  maskCSV,
  isPiiMaskingEnabled,
} from '../../src/utils/pii-masker.js';

describe('Unit Tests: Enterprise PII & Sensitive Data Sanitizer', () => {

  // ==================== 1. EMAIL MASKING ====================
  describe('1. Email Masking', () => {
    test('masks standard email while preserving domain by default', () => {
      expect(maskEmail('john.doe@example.com')).toBe('j***e@example.com');
      expect(maskEmail('alice.smith@sub.domain.org')).toBe('a***h@sub.domain.org');
      expect(maskEmail('support+urgent@company.co.uk')).toBe('s***t@company.co.uk');
    });

    test('handles short usernames correctly', () => {
      expect(maskEmail('a@example.com')).toBe('*@example.com');
      expect(maskEmail('ab@example.com')).toBe('a*@example.com');
      expect(maskEmail('abc@example.com')).toBe('a***c@example.com');
    });

    test('strict mode completely redacts email to [REDACTED_EMAIL]', () => {
      expect(maskEmail('john.doe@example.com', { strict: true })).toBe('[REDACTED_EMAIL]');
    });

    test('preserveDomain: false completely redacts email', () => {
      expect(maskEmail('john.doe@example.com', { preserveDomain: false })).toBe('[REDACTED_EMAIL]');
    });

    test('pseudonymization produces deterministic hash', () => {
      const email = 'user123@analytics.com';
      const p1 = maskEmail(email, { pseudonymize: true });
      const p2 = maskEmail(email, { pseudonymize: true });
      expect(p1).toMatch(/^anon_[a-f0-9]{12}$/);
      expect(p1).toBe(p2);
    });

    test('handles non-string or empty email gracefully', () => {
      expect(maskEmail(null)).toBeNull();
      expect(maskEmail(undefined)).toBeUndefined();
      expect(maskEmail('')).toBe('');
      expect(maskEmail(12345)).toBe(12345);
    });
  });

  // ==================== 2. PHONE NUMBER MASKING ====================
  describe('2. Phone Number Masking', () => {
    test('masks international phone numbers preserving country code and last 4 digits', () => {
      expect(maskPhone('+1-555-234-5678')).toBe('+1-***-***-5678');
      expect(maskPhone('+44 20 7123 4567')).toBe('+44-***-***-4567');
      expect(maskPhone('+49 (089) 1234-5678')).toBe('+49-***-***-5678');
    });

    test('masks domestic phone numbers with various delimiters', () => {
      expect(maskPhone('555-234-5678')).toBe('***-***-5678');
      expect(maskPhone('555.234.5678')).toBe('***.***.5678');
      expect(maskPhone('(555) 234-5678')).toBe('(555) ***-5678');
    });

    test('strict mode completely redacts phone to [REDACTED_PHONE]', () => {
      expect(maskPhone('+1-555-234-5678', { strict: true })).toBe('[REDACTED_PHONE]');
      expect(maskPhone('555-234-5678', { strict: true })).toBe('[REDACTED_PHONE]');
    });

    test('pseudonymization produces deterministic pseudonym for phone', () => {
      const phone = '+15552345678';
      const p1 = maskPhone(phone, { pseudonymize: true });
      const p2 = maskPhone(phone, { pseudonymize: true });
      expect(p1).toMatch(/^anon_[a-f0-9]{12}$/);
      expect(p1).toBe(p2);
    });
  });

  // ==================== 3. SOCIAL SECURITY NUMBER (SSN) ====================
  describe('3. Social Security Number (SSN) Masking', () => {
    test('masks standard hyphenated SSN preserving last 4 digits', () => {
      expect(maskSSN('123-45-6789')).toBe('***-**-6789');
      expect(maskSSN('987-65-4321')).toBe('***-**-4321');
    });

    test('masks dot and space separated SSN format', () => {
      expect(maskSSN('123.45.6789')).toBe('***.**.6789');
      expect(maskSSN('123 45 6789')).toBe('*** ** 6789');
    });

    test('masks raw 9-digit SSN format', () => {
      expect(maskSSN('123456789')).toBe('*****6789');
    });

    test('strict mode completely redacts SSN to [REDACTED_SSN]', () => {
      expect(maskSSN('123-45-6789', { strict: true })).toBe('[REDACTED_SSN]');
    });
  });

  // ==================== 4. CREDIT / DEBIT CARDS & LUHN ALGORITHM ====================
  describe('4. Credit / Debit Cards & Luhn Algorithm', () => {
    test('isValidLuhn returns true for valid card numbers', () => {
      // Standard test numbers that pass Luhn check
      expect(isValidLuhn('4532015112830366')).toBe(true);
      expect(isValidLuhn('4532-0151-1283-0366')).toBe(true);
      expect(isValidLuhn('378282246310005')).toBe(true); // Amex 15-digit
      expect(isValidLuhn('5425233430109903')).toBe(true); // Mastercard
    });

    test('isValidLuhn returns false for invalid card numbers or short digits', () => {
      expect(isValidLuhn('1234567890123456')).toBe(false); // Invalid Luhn checksum
      expect(isValidLuhn('12345')).toBe(false); // Too short
      expect(isValidLuhn('abcdefghijk')).toBe(false);
      expect(isValidLuhn(null)).toBe(false);
    });

    test('masks valid card numbers with dashes, spaces, or raw digits', () => {
      expect(maskCard('4532-0151-1283-0366')).toBe('****-****-****-0366');
      expect(maskCard('4532 0151 1283 0366')).toBe('**** **** **** 0366');
      expect(maskCard('4532015112830366')).toBe('************0366');
    });

    test('does not falsely redact 16-digit order numbers that fail Luhn in generic text', () => {
      const orderId = '9999888877776666'; // Fails Luhn
      expect(maskCard(orderId, { isCardColumn: false })).toBe(orderId);
    });

    test('masks card number even if Luhn fails when explicitly in a card column', () => {
      const num = '1111222233334444';
      expect(maskCard(num, { isCardColumn: true })).toBe('************4444');
    });

    test('strict mode completely redacts card to [REDACTED_CARD]', () => {
      expect(maskCard('4532-0151-1283-0366', { strict: true })).toBe('[REDACTED_CARD]');
    });
  });

  // ==================== 5. IP ADDRESS MASKING ====================
  describe('5. IP Address Masking', () => {
    test('masks IPv4 addresses preserving subnet for analytics', () => {
      expect(maskIP('192.168.1.145')).toBe('192.168.*.*');
      expect(maskIP('10.0.4.25')).toBe('10.0.*.*');
      expect(maskIP('172.16.254.1')).toBe('172.16.*.*');
    });

    test('masks IPv6 addresses', () => {
      const ipv6 = '2001:0db8:85a3:0000:0000:8a2e:0370:7334';
      expect(maskIP(ipv6)).toBe('2001:0db8:*:*:*:*:*:*');
    });

    test('strict mode completely redacts IP to [REDACTED_IP]', () => {
      expect(maskIP('192.168.1.145', { strict: true })).toBe('[REDACTED_IP]');
    });
  });

  // ==================== 6. API KEYS, SECRETS & TOKENS ====================
  describe('6. API Keys, Secrets & Tokens Masking', () => {
    test('masks OpenAI API keys', () => {
      const key = 'sk-proj-abc123def456ghi789jkl012mno345pqr678';
      expect(maskSecrets(key)).toBe('[REDACTED_SECRET]');
    });

    test('masks Anthropic API keys', () => {
      const key = 'sk-ant-api03-abcdefghijklmnopqrstuvwxyz1234567890';
      expect(maskSecrets(key)).toBe('[REDACTED_SECRET]');
    });

    test('masks AWS Access Keys and Secret Keys', () => {
      const key = 'AKIAIOSFODNN7EXAMPLE';
      expect(maskSecrets(key)).toBe('[REDACTED_SECRET]');
      expect(maskSecrets('aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')).toBe('aws_secret_access_key=[REDACTED_SECRET]');
    });

    test('masks GitHub Personal Access Tokens', () => {
      const token = ['ghp', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'].join('_');
      expect(maskSecrets(token)).toBe('[REDACTED_SECRET]');
    });

    test('masks JWT tokens', () => {
      const jwt = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c';
      expect(maskSecrets(jwt)).toBe('[REDACTED_SECRET]');
    });

    test('masks Bearer tokens in Authorization headers', () => {
      const header = 'Bearer secret-auth-token-12345-abcdef';
      expect(maskSecrets(header)).toBe('Bearer [REDACTED_SECRET]');
    });

    test('masks Slack and Stripe API keys', () => {
      expect(maskSecrets(['xoxb', '1234567890-123456789012-abcdef123456'].join('-'))).toBe('[REDACTED_SECRET]');
      expect(maskSecrets(['sk', 'test', '4eC39HqLyjWDarjtT1zdp7dc'].join('_'))).toBe('[REDACTED_SECRET]');
    });

    test('masks Private Key PEM blocks', () => {
      const pem = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA0Y3
-----END RSA PRIVATE KEY-----`;
      expect(maskSecrets(pem)).toBe('[REDACTED_SECRET]');
    });

    test('masks embedded database URI passwords', () => {
      const uri = 'postgres://postgres:SuperSecretP@ssword123@db.prod.internal:5432/metabase';
      const masked = maskSecrets(uri);
      expect(masked).toBe('postgres://postgres:[REDACTED_PASSWORD]@db.prod.internal:5432/metabase');
      expect(masked).not.toContain('SuperSecretP@ssword123');
    });
  });

  // ==================== 7. PASSWORDS & HASHES ====================
  describe('7. Passwords and Hashes Masking', () => {
    test('masks Bcrypt password hashes', () => {
      const hash = '$2a$12$e8Mc8jPqR7Q1s7/5rXn2QOWrF2O4v6YV3Z6qX0r7P4v8Z9Q0W1E2.';
      expect(maskSecrets(hash)).toBe('[REDACTED_PASSWORD]');
    });

    test('masks Argon2 password hashes', () => {
      const hash = '$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ$RdescudvJCsgTVGQFLwdEub4FsFdKcnhYazUZBmNTTg';
      expect(maskSecrets(hash)).toBe('[REDACTED_PASSWORD]');
    });
  });

  // ==================== 8. COLUMN-AWARE HEURISTICS ====================
  describe('8. Column-Aware Heuristic Detection', () => {
    test('correctly detects column categories by name', () => {
      expect(detectSensitiveCategoryByColumnName('email')).toBe('email');
      expect(detectSensitiveCategoryByColumnName('user_email')).toBe('email');
      expect(detectSensitiveCategoryByColumnName('customer_contact_email')).toBe('email');
      expect(detectSensitiveCategoryByColumnName('phone')).toBe('phone');
      expect(detectSensitiveCategoryByColumnName('mobile_number')).toBe('phone');
      expect(detectSensitiveCategoryByColumnName('ssn')).toBe('ssn');
      expect(detectSensitiveCategoryByColumnName('social_security_number')).toBe('ssn');
      expect(detectSensitiveCategoryByColumnName('tax_id')).toBe('ssn');
      expect(detectSensitiveCategoryByColumnName('credit_card')).toBe('card');
      expect(detectSensitiveCategoryByColumnName('card_number')).toBe('card');
      expect(detectSensitiveCategoryByColumnName('password')).toBe('password');
      expect(detectSensitiveCategoryByColumnName('password_hash')).toBe('password');
      expect(detectSensitiveCategoryByColumnName('api_key')).toBe('secret');
      expect(detectSensitiveCategoryByColumnName('auth_token')).toBe('secret');
      expect(detectSensitiveCategoryByColumnName('client_ip')).toBe('ip');
      expect(detectSensitiveCategoryByColumnName('created_at')).toBeNull();
      expect(detectSensitiveCategoryByColumnName('total_amount')).toBeNull();
    });

    test('maskValue applies category masking when column is recognized', () => {
      expect(maskValue('admin@company.com', 'user_email')).toBe('a***n@company.com');
      expect(maskValue('+1-555-234-5678', 'phone_number')).toBe('+1-***-***-5678');
      expect(maskValue('123-45-6789', 'ssn')).toBe('***-**-6789');
      expect(maskValue('AnyRawSecretValue123', 'api_key')).toBe('[REDACTED_SECRET]');
      expect(maskValue('plaintextPass!', 'password')).toBe('[REDACTED_PASSWORD]');
    });
  });

  // ==================== 9. TRAVERSAL & DATA STRUCTURE UTILITIES ====================
  describe('9. Data Structure Traversal & Export Sanitization', () => {
    test('maskRow sanitizes array row with column names', () => {
      const row = [1, 'john.doe@example.com', '+1-555-234-5678', 'active', '4532015112830366'];
      const cols = ['id', 'user_email', 'phone', 'status', 'card_number'];
      const masked = maskRow(row, cols);

      expect(masked[0]).toBe(1);
      expect(masked[1]).toBe('j***e@example.com');
      expect(masked[2]).toBe('+1-***-***-5678');
      expect(masked[3]).toBe('active');
      expect(masked[4]).toBe('************0366');
    });

    test('maskRow sanitizes plain object row with key heuristics', () => {
      const obj = {
        id: 101,
        email: 'alice@corp.com',
        api_token: 'secret-xyz',
        balance: 5000,
      };
      const masked = maskRow(obj);
      expect(masked.id).toBe(101);
      expect(masked.email).toBe('a***e@corp.com');
      expect(masked.api_token).toBe('[REDACTED_SECRET]');
      expect(masked.balance).toBe(5000);
    });

    test('maskTabularResult sanitizes Metabase query format { data: { cols, rows } }', () => {
      const tabularData = {
        data: {
          cols: [{ name: 'id' }, { name: 'email' }, { name: 'credit_card' }],
          rows: [
            [1, 'user1@test.com', '4532-0151-1283-0366'],
            [2, 'user2@test.com', '5425-2334-3010-9903'],
          ],
        },
      };

      const result = maskTabularResult(tabularData);
      expect(result.data.rows[0][1]).toBe('u***1@test.com');
      expect(result.data.rows[0][2]).toBe('****-****-****-0366');
      expect(result.data.rows[1][1]).toBe('u***2@test.com');
      expect(result.data.rows[1][2]).toBe('****-****-****-9903');
    });

    test('maskTabularResult sanitizes direct { columns, rows } format', () => {
      const data = {
        columns: [{ name: 'name' }, { name: 'phone' }],
        rows: [
          ['Alice', '555-234-5678'],
          ['Bob', '+1-555-876-5432'],
        ],
      };

      const result = maskTabularResult(data);
      expect(result.rows[0][1]).toBe('***-***-5678');
      expect(result.rows[1][1]).toBe('+1-***-***-5432');
    });

    test('maskObject deeply traverses nested structures and handles circular references safely', () => {
      const nested = {
        user: {
          name: 'Charlie',
          profile: {
            email: 'charlie@web.com',
            secret_key: 'top-secret-val',
          },
        },
        tags: ['admin', 'sales'],
      };
      // Circular link
      nested.self = nested;

      const masked = maskObject(nested);
      expect(masked.user.profile.email).toBe('c***e@web.com');
      expect(masked.user.profile.secret_key).toBe('[REDACTED_SECRET]');
      expect(masked.self).toBe('[CIRCULAR]');
    });

    test('maskCSV sanitizes RFC 4180 CSV strings with multiline quotes and commas', () => {
      const csvInput = `id,name,email,notes\n1,"Doe, John",john.doe@example.com,"Note with, commas"\n2,"Smith, Alice",alice@corp.com,"Multiline\nnote"`;
      const maskedCsv = maskCSV(csvInput);

      expect(maskedCsv).toContain('"Doe, John"');
      expect(maskedCsv).toContain('j***e@example.com');
      expect(maskedCsv).toContain('a***e@corp.com');
      expect(maskedCsv).not.toContain('john.doe@example.com');
      expect(maskedCsv).not.toContain('alice@corp.com');
    });
  });

  // ==================== 10. DETERMINISTIC PSEUDONYMIZATION & UTILITY ====================
  describe('10. Deterministic Pseudonymization & Analytics Utility', () => {
    test('pseudonymizeValue generates distinct hashes for different values and identical for same value', () => {
      const salt = 'custom_salt_123';
      const valA = 'customer_a@example.com';
      const valB = 'customer_b@example.com';

      const hashA1 = pseudonymizeValue(valA, salt);
      const hashA2 = pseudonymizeValue(valA, salt);
      const hashB1 = pseudonymizeValue(valB, salt);

      expect(hashA1).toBe(hashA2);
      expect(hashA1).not.toBe(hashB1);
      expect(hashA1).toMatch(/^anon_[a-f0-9]{12}$/);
    });

    test('isPiiMaskingEnabled checks global and argument flags correctly', () => {
      expect(isPiiMaskingEnabled()).toBe(true);
      expect(isPiiMaskingEnabled({ mask_pii: false })).toBe(false);
      expect(isPiiMaskingEnabled({ maskPii: false })).toBe(false);
      expect(isPiiMaskingEnabled({ sanitize: false })).toBe(false);
    });
  });

  // ==================== 11. REMEDIATION EDGE CASES & SECURITY INVARIANTS ====================
  describe('11. Remediation Edge Cases & Security Invariants', () => {
    test('IPv6 loopback ::1 is properly masked in maskIP and maskString', () => {
      expect(maskIP('::1')).toBe('[REDACTED_IP]');
      expect(maskIP('Client ::1 connected')).toBe('Client [REDACTED_IP] connected');
      expect(maskString('Connected from ::1 on port 8080')).toBe('Connected from [REDACTED_IP] on port 8080');
    });

    test('URL-encoded phone numbers are normalized and masked properly', () => {
      expect(maskPhone('%2B1%20555%20234%205678')).toBe('%2B1-***-***-5678');
      expect(maskString('Call %2B1%20555%20234%205678 now')).toBe('Call %2B1-***-***-5678 now');
    });

    test('13-digit Unix timestamps and 16-digit order numbers in unstructured text are not falsely redacted as phones', () => {
      expect(maskString('Timestamp 1725055200000')).toBe('Timestamp 1725055200000');
      expect(maskString('Tracking 9999888877776666')).toBe('Tracking 9999888877776666');
    });

    test('BigInt and Symbol primitives are preserved when not in a sensitive column', () => {
      const bigIntVal = 1725055200000n;
      const resBigInt = maskValue(bigIntVal);
      expect(typeof resBigInt).toBe('bigint');
      expect(resBigInt).toBe(bigIntVal);

      const symVal = Symbol('m1_test');
      const resSym = maskValue(symVal);
      expect(typeof resSym).toBe('symbol');
      expect(resSym).toBe(symVal);
    });

    test('detectSensitiveCategoryByColumnName correctly identifies composite column variations', () => {
      expect(detectSensitiveCategoryByColumnName('billing_mail')).toBe('email');
      expect(detectSensitiveCategoryByColumnName('TEL_NUMBER')).toBe('phone');
      expect(detectSensitiveCategoryByColumnName('cellNumber')).toBe('phone');
      expect(detectSensitiveCategoryByColumnName('tin_number')).toBe('ssn');
      expect(detectSensitiveCategoryByColumnName('source_ipv4')).toBe('ip');
      expect(detectSensitiveCategoryByColumnName('AUTH_BEARER')).toBe('secret');
    });
  });
});
