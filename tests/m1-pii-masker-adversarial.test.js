import {
  EMAIL_REGEX,
  PHONE_REGEX,
  SSN_REGEX,
  SSN_RAW_REGEX,
  CARD_REGEX,
  IPV4_REGEX,
  IPV6_REGEX,
  OPENAI_KEY_REGEX,
  ANTHROPIC_KEY_REGEX,
  AWS_ACCESS_KEY_REGEX,
  AWS_SECRET_KEY_REGEX,
  GITHUB_TOKEN_REGEX,
  JWT_TOKEN_REGEX,
  BEARER_TOKEN_REGEX,
  SLACK_TOKEN_REGEX,
  STRIPE_KEY_REGEX,
  PRIVATE_KEY_REGEX,
  URI_PASSWORD_REGEX,
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
  maskSecret,
  maskString,
  maskValue,
  maskRow,
  maskTabularResult,
  maskObject,
  maskCSV,
  isPiiMaskingEnabled,
} from '../src/utils/pii-masker.js';

describe('Adversarial Challenger Suite: Enterprise PII Sanitizer (M1)', () => {

  // =========================================================================
  // VECTOR 1: EMAIL ADVERSARIAL & EDGE-CASE CHALLENGES
  // =========================================================================
  describe('Vector 1: Email Adversarial Tests', () => {
    test('1.1 Sub-addressing, multi-dot domains, and URL-encoded @ symbols', () => {
      expect(maskEmail('dev+alert-critical.prod@sub.domain.corp.co.uk')).toBe('d***d@sub.domain.corp.co.uk');
      expect(maskEmail('user%40example.com')).toBe('u***r%40example.com');
      expect(maskEmail('sales%40emea.company.org')).toBe('s***s%40emea.company.org');
    });

    test('1.2 Username length boundaries (1, 2, 3, 50 chars)', () => {
      expect(maskEmail('x@test.com')).toBe('*@test.com');
      expect(maskEmail('xy@test.com')).toBe('x*@test.com');
      expect(maskEmail('xyz@test.com')).toBe('x***z@test.com');
      const longUser = 'a'.repeat(50);
      const maskedLong = maskEmail(`${longUser}@test.com`);
      expect(maskedLong).toBe('a***a@test.com');
    });

    test('1.3 Malformed email strings do not crash or corrupt text', () => {
      const nonEmails = [
        'plainaddress',
        '@missinguser.com',
        'missingdomain@.com',
        'user@domain..com',
        'user@@domain.com',
        'user@domain',
        'https://example.com/api/v1/users',
      ];
      for (const str of nonEmails) {
        expect(() => maskEmail(str)).not.toThrow();
        if (!str.includes('@') && !str.includes('%40')) {
          expect(maskEmail(str)).toBe(str);
        }
      }
    });

    test('1.4 Strict mode vs preserveDomain: false', () => {
      expect(maskEmail('ceo@enterprise.com', { strict: true })).toBe('[REDACTED_EMAIL]');
      expect(maskEmail('ceo@enterprise.com', { preserveDomain: false })).toBe('[REDACTED_EMAIL]');
      expect(maskEmail('ceo@enterprise.com', { preserveDomain: true })).toBe('c***o@enterprise.com');
    });

    test('1.5 Pseudonymization deterministic HMAC guarantees', () => {
      const emailA = 'alice@example.com';
      const emailB = 'bob@example.com';
      const salt1 = 'test_salt_1';
      const salt2 = 'test_salt_2';

      const pseudoA1 = maskEmail(emailA, { pseudonymize: true, salt: salt1 });
      const pseudoA1_dup = maskEmail(emailA, { pseudonymize: true, salt: salt1 });
      const pseudoA2 = maskEmail(emailA, { pseudonymize: true, salt: salt2 });
      const pseudoB1 = maskEmail(emailB, { pseudonymize: true, salt: salt1 });

      expect(pseudoA1).toBe(pseudoA1_dup);
      expect(pseudoA1).not.toBe(pseudoA2);
      expect(pseudoA1).not.toBe(pseudoB1);
      expect(pseudoA1).toMatch(/^anon_[a-f0-9]{12}$/);
    });
  });

  // =========================================================================
  // VECTOR 2: PHONE ADVERSARIAL & EDGE-CASE CHALLENGES
  // =========================================================================
  describe('Vector 2: Phone Adversarial Tests', () => {
    test('2.1 International numbers across varied country codes and delimiters', () => {
      expect(maskPhone('+1 (800) 555-1234')).toBe('+1-***-***-1234');
      expect(maskPhone('+44 20 7123 4567')).toBe('+44-***-***-4567');
      expect(maskPhone('+49 (089) 1234-5678')).toBe('+49-***-***-5678');
      expect(maskPhone('+90-532-123-4567')).toBe('+90-***-***-4567');
    });

    test('2.2 Domestic and area code variations', () => {
      expect(maskPhone('555-234-5678')).toBe('***-***-5678');
      expect(maskPhone('555.234.5678')).toBe('***.***.5678');
      expect(maskPhone('(555) 234-5678')).toBe('(555) ***-5678');
      expect(maskPhone('555 234 5678')).toBe('******5678');
      expect(maskPhone('5552345678')).toBe('******5678');
    });

    test('2.3 URL-encoded phone number masking', () => {
      const urlEncodedPhone = '%2B1%20555%20234%205678';
      const masked = maskPhone(urlEncodedPhone);
      expect(masked).toBe('%2B1-***-***-5678');
    });

    test('2.4 Short digit sequences (< 7 digits) are not falsely redacted in maskPhone', () => {
      expect(maskPhone('123-456')).toBe('123-456');
      expect(maskPhone('HTTP 404')).toBe('HTTP 404');
      expect(maskPhone('Order #12345')).toBe('Order #12345');
    });

    test('2.5 Strict and pseudonymize modes for phone', () => {
      expect(maskPhone('+1-555-234-5678', { strict: true })).toBe('[REDACTED_PHONE]');
      const pseudo = maskPhone('+1-555-234-5678', { pseudonymize: true });
      expect(pseudo).toMatch(/^anon_[a-f0-9]{12}$/);
    });
  });

  // =========================================================================
  // VECTOR 3: CREDIT CARD & LUHN ALGORITHM CHALLENGES
  // =========================================================================
  describe('Vector 3: Credit Card & Luhn Algorithm Tests', () => {
    test('3.1 Valid Luhn verification for major card issuers', () => {
      expect(isValidLuhn('4532015112830366')).toBe(true);
      expect(isValidLuhn('5425233430109903')).toBe(true);
      expect(isValidLuhn('378282246310005')).toBe(true);
      expect(isValidLuhn('6011000990139424')).toBe(true);
      expect(isValidLuhn('4532-0151-1283-0366')).toBe(true);
      expect(isValidLuhn('4532 0151 1283 0366')).toBe(true);
    });

    test('3.2 Invalid Luhn detection and boundary inputs', () => {
      expect(isValidLuhn('4532015112830367')).toBe(false);
      expect(isValidLuhn('4000123456789010')).toBe(false);
      expect(isValidLuhn('123456789012')).toBe(false);
      expect(isValidLuhn('12345678901234567890')).toBe(false);
      expect(isValidLuhn(null)).toBe(false);
      expect(isValidLuhn(undefined)).toBe(false);
      expect(isValidLuhn('')).toBe(false);
      expect(isValidLuhn('abc-def-ghi-jkl')).toBe(false);
    });

    test('3.3 Card masking preserves formatting and masks prefix', () => {
      expect(maskCard('4532-0151-1283-0366')).toBe('****-****-****-0366');
      expect(maskCard('4532 0151 1283 0366')).toBe('**** **** **** 0366');
      expect(maskCard('4532015112830366')).toBe('************0366');
      expect(maskCard('378282246310005')).toBe('***********0005');
    });

    test('3.4 Explicit card column forces masking even if Luhn fails (malformed card in DB)', () => {
      const corruptCard = '4000123456789010';
      expect(maskCard(corruptCard, { isCardColumn: true })).toBe('************9010');
      expect(maskValue(corruptCard, 'credit_card_number')).toBe('************9010');
    });
  });

  // =========================================================================
  // VECTOR 4: IP ADDRESS ADVERSARIAL CHALLENGES (IPv4 & IPv6)
  // =========================================================================
  describe('Vector 4: IP Address Adversarial Tests', () => {
    test('4.1 IPv4 addresses are masked preserving subnet', () => {
      expect(maskIP('192.168.1.100')).toBe('192.168.*.*');
      expect(maskIP('10.0.0.1')).toBe('10.0.*.*');
      expect(maskIP('127.0.0.1')).toBe('127.0.*.*');
      expect(maskIP('255.255.255.255')).toBe('255.255.*.*');
      expect(maskIP('0.0.0.0')).toBe('0.0.*.*');
    });

    test('4.2 Non-IP decimal versions are NOT falsely matched as IPv4', () => {
      expect(maskIP('300.1.2.3')).toBe('300.1.2.3');
      expect(maskIP('version 1.2.3')).toBe('version 1.2.3');
    });

    test('4.3 IPv6 addresses masking & ::1 loopback sanitization', () => {
      const fullIpv6 = '2001:0db8:85a3:0000:0000:8a2e:0370:7334';
      expect(maskIP(fullIpv6)).toBe('2001:0db8:*:*:*:*:*:*');
      
      const compressedIpv6 = '2001:db8::1';
      expect(maskIP(compressedIpv6)).toBe('2001:db8:*:*:*:*:*:*');

      // Verify ::1 loopback is masked
      const loopbackIpv6 = '::1';
      expect(maskIP(loopbackIpv6)).toBe('[REDACTED_IP]');
    });

    test('4.4 Strict mode for IP', () => {
      expect(maskIP('192.168.1.100', { strict: true })).toBe('[REDACTED_IP]');
      expect(maskIP('2001:0db8:85a3:0000:0000:8a2e:0370:7334', { strict: true })).toBe('[REDACTED_IP]');
    });
  });

  // =========================================================================
  // VECTOR 5: SECRETS, API KEYS, JWT, PEM, URI PASSWORDS & HASHES
  // =========================================================================
  describe('Vector 5: Secrets, API Keys, Tokens and Hashes Tests', () => {
    test('5.1 AI Provider Keys (OpenAI, Anthropic)', () => {
      expect(maskSecrets('sk-1234567890abcdefghijklmnopqrstuvwxyz')).toBe('[REDACTED_SECRET]');
      expect(maskSecrets('sk-proj-1234567890abcdefghijklmnopqrstuvwxyz_ABCD')).toBe('[REDACTED_SECRET]');
      expect(maskSecrets('sk-ant-api03-abcdefghijklmnopqrstuvwxyz1234567890')).toBe('[REDACTED_SECRET]');
    });

    test('5.2 Cloud and Git Provider Tokens (AWS, GitHub, Slack, Stripe)', () => {
      expect(maskSecrets('AKIAIOSFODNN7EXAMPLE')).toBe('[REDACTED_SECRET]');
      expect(maskSecrets('aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"')).toBe('aws_secret_access_key=[REDACTED_SECRET]');
      expect(maskSecrets(['ghp', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'].join('_'))).toBe('[REDACTED_SECRET]');
      expect(maskSecrets(['gho', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'].join('_'))).toBe('[REDACTED_SECRET]');
      expect(maskSecrets(['xoxb', '123456789012-1234567890123-abcdef1234567890'].join('-'))).toBe('[REDACTED_SECRET]');
      expect(maskSecrets(['sk', 'live', '51AbcDefGhiJklMnoPqrStuVwxYz1234567890'].join('_'))).toBe('[REDACTED_SECRET]');
      expect(maskSecrets(['pk', 'test', '51AbcDefGhiJklMnoPqrStuVwxYz1234567890'].join('_'))).toBe('[REDACTED_SECRET]');
    });

    test('5.3 JWT Tokens and Bearer Auth headers', () => {
      const jwt = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkFsaWNlIn0.g_5X6Y_Z7W8A9B0C1D2E3F4G5H6I7J8K9L0M1N2O3P4';
      expect(maskSecrets(jwt)).toBe('[REDACTED_SECRET]');
      expect(maskSecrets(`Authorization: Bearer ${jwt}`)).toBe('Authorization: Bearer [REDACTED_SECRET]');
      expect(maskSecrets('Bearer custom-session-token-9876543210')).toBe('Bearer [REDACTED_SECRET]');
    });

    test('5.4 Private Key PEM blocks across multiple key types', () => {
      const rsaPem = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA0Y3ABC
-----END RSA PRIVATE KEY-----`;
      expect(maskSecrets(rsaPem)).toBe('[REDACTED_SECRET]');

      const ecPem = `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIz...
-----END EC PRIVATE KEY-----`;
      expect(maskSecrets(ecPem)).toBe('[REDACTED_SECRET]');

      const openSshPem = `-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAA...
-----END OPENSSH PRIVATE KEY-----`;
      expect(maskSecrets(openSshPem)).toBe('[REDACTED_SECRET]');
    });

    test('5.5 Database Connection URI embedded passwords', () => {
      const pgUri = 'postgres://postgres:SuperSecretP@ssword123@db.prod.internal:5432/metabase';
      expect(maskSecrets(pgUri)).toBe('postgres://postgres:[REDACTED_PASSWORD]@db.prod.internal:5432/metabase');

      const mysqlUri = 'mysql://app_user:Complex%20P%40ss!@10.0.0.5:3306/production_db?charset=utf8';
      expect(maskSecrets(mysqlUri)).toBe('mysql://app_user:[REDACTED_PASSWORD]@10.0.0.5:3306/production_db?charset=utf8');

      const mongoUri = 'mongodb://root:SecretAdminPass@mongo-cluster.local:27017/admin';
      expect(maskSecrets(mongoUri)).toBe('mongodb://root:[REDACTED_PASSWORD]@mongo-cluster.local:27017/admin');
    });

    test('5.6 Password hashes (Bcrypt, Argon2)', () => {
      const bcrypt = '$2y$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy';
      expect(maskSecrets(bcrypt)).toBe('[REDACTED_PASSWORD]');

      const argon2 = '$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ$RdescudvJCsgTVGQFLwdEub4FsFdKcnhYazUZBmNTTg';
      expect(maskSecrets(argon2)).toBe('[REDACTED_PASSWORD]');
    });
  });

  // =========================================================================
  // VECTOR 6: JAVASCRIPT PRIMITIVES, SYMBOLS, BIGINT, NULL/UNDEFINED, CIRCULAR
  // =========================================================================
  describe('Vector 6: Primitives, Symbols, BigInt, and Circular Structure Tests', () => {
    test('6.1 Primitives: null, undefined, boolean, number, NaN, Infinity', () => {
      expect(maskValue(null)).toBeNull();
      expect(maskValue(undefined)).toBeUndefined();
      expect(maskValue(true)).toBe(true);
      expect(maskValue(false)).toBe(false);
      expect(maskValue(0)).toBe(0);
      expect(maskValue(42.5)).toBe(42.5);
      expect(maskValue(NaN)).toBeNaN();
      expect(maskValue(Infinity)).toBe(Infinity);
      expect(maskValue(-Infinity)).toBe(-Infinity);
    });

    test('6.2 Date and RegExp instances in maskObject are cloned without destruction', () => {
      const d = new Date('2026-08-31T01:00:00Z');
      const r = /^test-[0-9]+$/gi;
      const obj = { createdAt: d, pattern: r, email: 'admin@corp.com' };

      const masked = maskObject(obj);
      expect(masked.createdAt instanceof Date).toBe(true);
      expect(masked.createdAt.toISOString()).toBe(d.toISOString());
      expect(masked.pattern instanceof RegExp).toBe(true);
      expect(masked.email).toBe('a***n@corp.com');
    });

    test('6.3 Circular structures in objects and arrays are safely handled with [CIRCULAR]', () => {
      const nodeA = { name: 'Node A', email: 'a@corp.com' };
      const nodeB = { name: 'Node B', email: 'b@corp.com', neighbor: nodeA };
      nodeA.neighbor = nodeB;

      const masked = maskObject(nodeA);
      expect(masked.email).toBe('*@corp.com');
      expect(masked.neighbor.email).toBe('*@corp.com');
      expect(masked.neighbor.neighbor).toBe('[CIRCULAR]');
    });

    test('6.4 Deeply nested hierarchy (50 levels) without call stack exhaustion', () => {
      let root = { level: 0, email: 'root@example.com' };
      let curr = root;
      for (let i = 1; i <= 50; i++) {
        curr.child = { level: i, email: `user${i}@example.com` };
        curr = curr.child;
      }

      const masked = maskObject(root);
      expect(masked.email).toBe('r***t@example.com');
      let verify = masked;
      for (let i = 1; i <= 50; i++) {
        verify = verify.child;
        expect(verify.level).toBe(i);
        expect(verify.email).toContain('@example.com');
        expect(verify.email).not.toBe(`user${i}@example.com`);
      }
    });

    test('6.5 Objects with null prototype Object.create(null)', () => {
      const bareObj = Object.create(null);
      bareObj.email = 'bare@domain.com';
      bareObj.secret = 'my-secret-key-12345';
      const masked = maskObject(bareObj);
      expect(masked.email).toBe('b***e@domain.com');
    });

    test('6.6 Frozen objects are safely sanitized into new objects without runtime error', () => {
      const frozen = Object.freeze({
        user: Object.freeze({ email: 'frozen@corp.com', phone: '555-234-5678' }),
      });
      expect(() => maskObject(frozen)).not.toThrow();
      const masked = maskObject(frozen);
      expect(masked.user.email).toBe('f***n@corp.com');
      expect(masked.user.phone).toBe('***-***-5678');
    });
  });

  // =========================================================================
  // VECTOR 7: TABULAR DATA STRUCTURES & COLUMN HEURISTICS
  // =========================================================================
  describe('Vector 7: Tabular Data Structures & Column Heuristics Tests', () => {
    test('7.1 Column name case-insensitivity and normalization', () => {
      const testCases = [
        ['USER_EMAIL', 'test@test.com', 't***t@test.com'],
        ['CustomerEmailAddress', 'cust@test.com', 'c***t@test.com'],
        ['PHONE_NUM', '+1-555-123-4567', '+1-***-***-4567'],
        ['Cellular_Mobile', '555-123-4567', '***-***-4567'],
        ['SSN_NUMBER', '123-45-6789', '***-**-6789'],
        ['TAX_ID', '123456789', '*****6789'],
        ['CreditCardNumber', '4532-0151-1283-0366', '****-****-****-0366'],
        ['PASSWORD_HASH', '$2a$12$e8Mc8jPqR7Q1s7/5rXn2QOWrF2O4v6YV3Z6qX0r7P4v8Z9Q0W1E2.', '[REDACTED_PASSWORD]'],
        ['API_SECRET_TOKEN', 'custom-api-token-999', '[REDACTED_SECRET]'],
        ['CLIENT_IP_ADDRESS', '192.168.1.50', '192.168.*.*'],
      ];

      for (const [col, rawVal, expected] of testCases) {
        expect(maskValue(rawVal, col)).toBe(expected);
      }
    });

    test('7.2 Metabase format { data: { cols, rows } } full sanitization', () => {
      const metabaseOutput = {
        data: {
          cols: [
            { name: 'id', display_name: 'User ID' },
            { name: 'email', display_name: 'Email Address' },
            { name: 'phone', display_name: 'Phone' },
            { name: 'credit_card', display_name: 'Card' },
            { name: 'amount', display_name: 'Total Spent' },
          ],
          rows: [
            [1, 'john@doe.com', '555-123-4567', '4532-0151-1283-0366', 1500.50],
            [2, 'alice@smith.org', '+1-555-987-6543', '5425-2334-3010-9903', 240.00],
          ],
        },
      };

      const sanitized = maskTabularResult(metabaseOutput);
      expect(sanitized.data.rows[0][0]).toBe(1);
      expect(sanitized.data.rows[0][1]).toBe('j***n@doe.com');
      expect(sanitized.data.rows[0][2]).toBe('***-***-4567');
      expect(sanitized.data.rows[0][3]).toBe('****-****-****-0366');
      expect(sanitized.data.rows[0][4]).toBe(1500.50);

      expect(sanitized.data.rows[1][1]).toBe('a***e@smith.org');
      expect(sanitized.data.rows[1][2]).toBe('+1-***-***-6543');
    });

    test('7.3 Array of objects tabular format', () => {
      const rows = [
        { id: 101, user_email: 'ceo@tech.io', client_ip: '10.0.1.20', status: 'active' },
        { id: 102, user_email: 'cto@tech.io', client_ip: '10.0.1.21', status: 'active' },
      ];

      const sanitized = maskTabularResult(rows);
      expect(sanitized[0].user_email).toBe('c***o@tech.io');
      expect(sanitized[0].client_ip).toBe('10.0.*.*');
      expect(sanitized[0].status).toBe('active');
    });
  });

  // =========================================================================
  // VECTOR 8: CSV SANITIZATION & RFC 4180 CORNER CASES
  // =========================================================================
  describe('Vector 8: CSV RFC 4180 Sanitization Tests', () => {
    test('8.1 Quoted commas, escaped quotes, and newlines in CSV cells', () => {
      const csv = `id,name,email,comment\n1,"Doe, John",john.doe@example.com,"He said ""Confidential"" and left"\n2,"Smith, Jane",jane@corp.com,"Line 1\nLine 2 with phone 555-234-5678"`;
      const sanitized = maskCSV(csv);

      expect(sanitized).toContain('"Doe, John"');
      expect(sanitized).toContain('j***e@example.com');
      expect(sanitized).toContain('""Confidential""');
      expect(sanitized).toContain('"Smith, Jane"');
      expect(sanitized).toContain('j***e@corp.com');
      expect(sanitized).toContain('***-***-5678');
      expect(sanitized).not.toContain('john.doe@example.com');
    });

    test('8.2 CSV with Windows CRLF line endings', () => {
      const csv = `id,email\r\n1,alpha@corp.com\r\n2,beta@corp.com\r\n`;
      const sanitized = maskCSV(csv);
      expect(sanitized).toContain('a***a@corp.com');
      expect(sanitized).toContain('b***a@corp.com');
    });

    test('8.3 CSV with hasHeaders: false', () => {
      const csv = `john@example.com,555-234-5678\nalice@example.com,555-876-5432`;
      const sanitized = maskCSV(csv, { hasHeaders: false });
      expect(sanitized).toContain('j***n@example.com,***-***-5678');
      expect(sanitized).toContain('a***e@example.com,***-***-5432');
    });

    test('8.4 Large CSV buffer stress test (1,000 rows with mixed PII)', () => {
      const rows = ['id,name,email,phone,card,note'];
      for (let i = 1; i <= 1000; i++) {
        rows.push(`${i},"User, #${i}",user${i}@enterprise.com,+1-555-123-4567,4532-0151-1283-0366,"Safe note #${i}"`);
      }
      const rawCsv = rows.join('\n');

      const startTime = performance.now();
      const maskedCsv = maskCSV(rawCsv);
      const durationMs = performance.now() - startTime;

      expect(maskedCsv).toContain('u***1@enterprise.com');
      expect(maskedCsv).toContain('+1-***-***-4567');
      expect(maskedCsv).toContain('****-****-****-0366');
      expect(maskedCsv).not.toContain('user1@enterprise.com');
      expect(durationMs).toBeLessThan(1000);
    });
  });

  // =========================================================================
  // VECTOR 9: THROUGHPUT & STRESS HARNESS
  // =========================================================================
  describe('Vector 9: High-Stress Throughput & Scale Tests', () => {
    test('9.1 Throughput exceeds 100,000 operations per second', () => {
      const sampleValues = [
        'john.doe@example.com',
        '+1-555-234-5678',
        '4532-0151-1283-0366',
        '192.168.1.100',
        'sk-proj-abcdefghijklmnopqrstuvwxyz123456',
        'Plain business text with no sensitive data at all',
        12345,
        true,
      ];

      const startTime = performance.now();
      const ITERATIONS = 10000;
      for (let i = 0; i < ITERATIONS; i++) {
        maskValue(sampleValues[i % sampleValues.length]);
      }
      const elapsedMs = performance.now() - startTime;
      const opsPerSec = Math.round(ITERATIONS / (elapsedMs / 1000));
      expect(opsPerSec).toBeGreaterThan(100000);
    });
  });
});
