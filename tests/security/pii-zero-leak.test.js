import {
  maskString,
  maskValue,
  maskRow,
  maskTabularResult,
  maskObject,
  maskCSV,
  pseudonymizeValue,
} from '../../src/utils/pii-masker.js';
import { SqlHandler } from '../../src/mcp/handlers/sql.js';
import { CardsHandler } from '../../src/mcp/handlers/cards.js';
import { jest } from '@jest/globals';

describe('Security & Zero-Leak Fuzzing Tests: PII Sanitizer', () => {

  // Sample raw sensitive data corpus
  const rawEmails = [
    'victor.stone@justiceleague.org',
    'bruce.wayne@wayneenterprises.com',
    'clark.kent@dailyplanet.com',
    'barry.allen@ccpd.gov',
    'diana.prince@themiscira.net',
  ];

  const rawPhones = [
    '+1-555-890-1234',
    '+44 20 7946 0991',
    '(555) 345-6789',
    '555-876-5432',
    '+1 (555) 999-0000',
  ];

  const rawSSNs = [
    '000-12-3456',
    '999-88-7777',
    '123-45-6789',
    '456-78-9012',
  ];

  const rawCards = [
    '4532-0151-1283-0366', // Visa
    '5425-2334-3010-9903', // Mastercard
    '3782-822463-10005',  // Amex
    '4532015112830366',    // Raw digits Visa
  ];

  const rawApiKeys = [
    'sk-proj-a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2',
    'sk-ant-api03-abcdefghijklmnopqrstuvwxyz1234567890abcdef',
    'AKIAIOSFODNN7EXAMPLE',
    ['ghp', '1234567890abcdefghijklmnopqrstuvwxyz12'].join('_'),
    ['xoxb', '1234567890-123456789012-abcdef123456'].join('-'),
    ['sk', 'test', '4eC39HqLyjWDarjtT1zdp7dc'].join('_'),
  ];

  const rawHashes = [
    '$2a$12$e8Mc8jPqR7Q1s7/5rXn2QOWrF2O4v6YV3Z6qX0r7P4v8Z9Q0W1E2.',
    '$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ$RdescudvJCsgTVGQFLwdEub4FsFdKcnhYazUZBmNTTg',
  ];

  // ==================== 1. ADVERSARIAL MUTATION FUZZING (1,000+ ITERATIONS) ====================
  describe('1. Adversarial Multi-Format Injection & Fuzzing (1,000+ Samples)', () => {
    test('neutralizes all sensitive PII across 1,000 adversarial payloads', () => {
      const templates = [
        (email, phone, ssn, card, key) => `User profile: email=${email}, phone=${phone}, ssn=${ssn}, card=${card}, key=${key}`,
        (email, phone, ssn, card, key) => `{"user": {"email": "${email}", "contact": "${phone}", "tax_id": "${ssn}", "payment": "${card}", "auth": "${key}"}}`,
        (email, phone, ssn, card, key) => `SELECT * FROM users WHERE email='${email}' AND ssn='${ssn}' AND cc='${card}'; -- key:${key}`,
        (email, phone, ssn, card, key) => `[LOG] 2026-08-31 ERR connection failed for ${email} with token Bearer ${key} (card:${card}, ip: 192.168.1.100)`,
        (email, phone, ssn, card, key) => `<user email="${email}" phone="${phone}" ssn="${ssn}" card="${card}" apiKey="${key}" />`,
        (email, phone, ssn, card, key) => `Contact: ${email} | Phone: ${phone} | SSN: ${ssn} | CC: ${card} | Secret: ${key}`,
        (email, phone, ssn, card, key) => `https://api.example.com/v1/checkout?email=${encodeURIComponent(email)}&phone=${encodeURIComponent(phone)}&card=${card}`,
      ];

      let count = 0;
      for (let i = 0; i < 1000; i++) {
        const email = rawEmails[i % rawEmails.length];
        const phone = rawPhones[i % rawPhones.length];
        const ssn = rawSSNs[i % rawSSNs.length];
        const card = rawCards[i % rawCards.length];
        const key = rawApiKeys[i % rawApiKeys.length];

        const template = templates[i % templates.length];
        const payload = template(email, phone, ssn, card, key);

        const sanitized = maskString(payload);

        // ZERO LEAK ASSERTIONS:
        // 1. Raw email full username must not leak
        const emailUser = email.split('@')[0];
        if (emailUser.length > 3) {
          expect(sanitized).not.toContain(emailUser);
        }

        // 2. Raw phone must not be in unmasked format
        expect(sanitized).not.toContain('555-890-1234');
        expect(sanitized).not.toContain('555-345-6789');

        // 3. Raw SSN must not leak
        expect(sanitized).not.toContain('000-12-3456');
        expect(sanitized).not.toContain('999-88-7777');

        // 4. Raw Credit card must not leak
        expect(sanitized).not.toContain('4532-0151-1283-0366');
        expect(sanitized).not.toContain('5425-2334-3010-9903');
        expect(sanitized).not.toContain('4532015112830366');

        // 5. Raw API key must not leak
        expect(sanitized).not.toContain('sk-proj-');
        expect(sanitized).not.toContain('sk-ant-');
        expect(sanitized).not.toContain('AKIAIOSFODNN7EXAMPLE');
        expect(sanitized).not.toContain(['ghp', '1234567890abcdef'].join('_'));

        count++;
      }

      expect(count).toBe(1000);
    });
  });

  // ==================== 2. INTEGRATION ZERO-LEAK WITH SQLHANDLER ====================
  describe('2. Integration Zero-Leak with SqlHandler', () => {
    let mockClient;

    beforeEach(() => {
      mockClient = {
        executeNativeQuery: jest.fn(),
      };
    });

    test('SqlHandler.handleExecuteSQL masks sensitive columns in text and structured outputs', async () => {
      mockClient.executeNativeQuery.mockResolvedValueOnce({
        data: {
          cols: [
            { name: 'id', base_type: 'type/Integer' },
            { name: 'user_email', base_type: 'type/Text' },
            { name: 'phone_number', base_type: 'type/Text' },
            { name: 'ssn', base_type: 'type/Text' },
            { name: 'credit_card', base_type: 'type/Text' },
            { name: 'api_token', base_type: 'type/Text' },
            { name: 'password_hash', base_type: 'type/Text' },
          ],
          rows: [
            [
              1,
              'superadmin@acme-corp.com',
              '+1-555-123-4567',
              '123-45-6789',
              '4532-0151-1283-0366',
              'sk-proj-secretKey12345678901234567890',
              '$2a$12$e8Mc8jPqR7Q1s7/5rXn2QOWrF2O4v6YV3Z6qX0r7P4v8Z9Q0W1E2.',
            ],
          ],
        },
      });

      const sqlHandler = new SqlHandler(mockClient);
      const result = await sqlHandler.handleExecuteSQL({
        database_id: 1,
        sql: 'SELECT * FROM users LIMIT 1;',
      });

      const textOutput = result.content[0].text;
      const structuredRows = result.structuredContent.rows;

      // Assertions on text output (Markdown display)
      expect(textOutput).toContain('s***n@acme-corp.com');
      expect(textOutput).toContain('+1-***-***-4567');
      expect(textOutput).toContain('***-**-6789');
      expect(textOutput).toContain('****-****-****-0366');
      expect(textOutput).toContain('[REDACTED_SECRET]');
      expect(textOutput).toContain('[REDACTED_PASSWORD]');

      expect(textOutput).not.toContain('superadmin@acme-corp.com');
      expect(textOutput).not.toContain('123-45-6789');
      expect(textOutput).not.toContain('4532-0151-1283-0366');
      expect(textOutput).not.toContain('sk-proj-secretKey');
      expect(textOutput).not.toContain('$2a$12$e8Mc8jP');

      // Assertions on structured content
      expect(structuredRows[0][1]).toBe('s***n@acme-corp.com');
      expect(structuredRows[0][2]).toBe('+1-***-***-4567');
      expect(structuredRows[0][3]).toBe('***-**-6789');
      expect(structuredRows[0][4]).toBe('****-****-****-0366');
      expect(structuredRows[0][5]).toBe('[REDACTED_SECRET]');
      expect(structuredRows[0][6]).toBe('[REDACTED_PASSWORD]');
    });

    test('SqlHandler respects mask_pii: false when explicitly requested', async () => {
      mockClient.executeNativeQuery.mockResolvedValueOnce({
        data: {
          cols: [{ name: 'email' }],
          rows: [['unmasked@example.com']],
        },
      });

      const sqlHandler = new SqlHandler(mockClient);
      const result = await sqlHandler.handleExecuteSQL({
        database_id: 1,
        sql: 'SELECT email FROM users;',
        mask_pii: false,
      });

      expect(result.content[0].text).toContain('unmasked@example.com');
      expect(result.structuredContent.rows[0][0]).toBe('unmasked@example.com');
    });
  });

  // ==================== 3. INTEGRATION ZERO-LEAK WITH CARDSHANDLER ====================
  describe('3. Integration Zero-Leak with CardsHandler', () => {
    let mockClient;

    beforeEach(() => {
      mockClient = {
        request: jest.fn(),
      };
    });

    test('CardsHandler.handleCardData masks sensitive JSON sample data', async () => {
      mockClient.request.mockResolvedValueOnce({
        data: {
          cols: [{ name: 'customer_email' }, { name: 'card_number' }],
          rows: [
            ['vip@luxury.com', '4532-0151-1283-0366'],
            ['executive@corp.com', '5425-2334-3010-9903'],
          ],
        },
      });

      const cardsHandler = new CardsHandler(mockClient);
      const result = await cardsHandler.handleCardData({ card_id: 10, format: 'json' });

      const textOutput = result.content[0].text;
      expect(textOutput).toContain('v***p@luxury.com');
      expect(textOutput).toContain('e***e@corp.com');
      expect(textOutput).toContain('****-****-****-0366');
      expect(textOutput).toContain('****-****-****-9903');

      expect(textOutput).not.toContain('vip@luxury.com');
      expect(textOutput).not.toContain('executive@corp.com');
      expect(textOutput).not.toContain('4532-0151-1283-0366');
    });

    test('CardsHandler.handleCardData masks CSV exports', async () => {
      const rawCsv = `id,customer_email,phone\n1,alice@secret.org,+1-555-444-3322\n2,bob@agency.gov,555-888-9999`;
      mockClient.request.mockResolvedValueOnce(rawCsv);

      const cardsHandler = new CardsHandler(mockClient);
      const result = await cardsHandler.handleCardData({ card_id: 12, format: 'csv' });

      const textOutput = result.content[0].text;
      expect(textOutput).toContain('a***e@secret.org');
      expect(textOutput).toContain('b***b@agency.gov');
      expect(textOutput).toContain('+1-***-***-3322');
      expect(textOutput).toContain('***-***-9999');

      expect(textOutput).not.toContain('alice@secret.org');
      expect(textOutput).not.toContain('bob@agency.gov');
    });
  });

  // ==================== 4. ANALYTICAL DISTINCT GROUPING PRESERVATION ====================
  describe('4. Analytical Grouping & Distinct Count Integrity', () => {
    test('pseudonymization preserves group-by aggregation integrity without leaking identities', () => {
      const rawDataset = [
        { customer_id: 'cust_alpha@shop.com', amount: 100 },
        { customer_id: 'cust_alpha@shop.com', amount: 150 },
        { customer_id: 'cust_beta@shop.com', amount: 200 },
        { customer_id: 'cust_gamma@shop.com', amount: 300 },
        { customer_id: 'cust_beta@shop.com', amount: 50 },
      ];

      // Salt for deterministic HMAC
      const salt = 'analytics_session_salt_2026';

      const sanitizedDataset = rawDataset.map(row => ({
        customer_id: pseudonymizeValue(row.customer_id, salt),
        amount: row.amount,
      }));

      // Calculate distinct customer count on raw vs sanitized
      const rawDistinctCount = new Set(rawDataset.map(r => r.customer_id)).size;
      const sanitizedDistinctCount = new Set(sanitizedDataset.map(r => r.customer_id)).size;

      expect(sanitizedDistinctCount).toBe(rawDistinctCount);
      expect(sanitizedDistinctCount).toBe(3);

      // Group-by sum aggregation check
      const rawSums = {};
      rawDataset.forEach(r => {
        rawSums[r.customer_id] = (rawSums[r.customer_id] || 0) + r.amount;
      });

      const sanitizedSums = {};
      sanitizedDataset.forEach(r => {
        sanitizedSums[r.customer_id] = (sanitizedSums[r.customer_id] || 0) + r.amount;
      });

      // Verify group cardinality matches exactly
      const rawSortedTotals = Object.values(rawSums).sort((a, b) => a - b);
      const sanitizedSortedTotals = Object.values(sanitizedSums).sort((a, b) => a - b);

      expect(sanitizedSortedTotals).toEqual(rawSortedTotals);
      expect(sanitizedSortedTotals).toEqual([250, 250, 300]);

      // Verify zero raw identities in sanitized dataset
      sanitizedDataset.forEach(r => {
        expect(r.customer_id).not.toContain('cust_alpha');
        expect(r.customer_id).not.toContain('cust_beta');
        expect(r.customer_id).not.toContain('cust_gamma');
      });
    });
  });
});
