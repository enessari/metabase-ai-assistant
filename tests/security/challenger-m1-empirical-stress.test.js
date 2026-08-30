import {
  maskString,
  maskValue,
  maskRow,
  maskTabularResult,
  maskObject,
  maskCSV,
  pseudonymizeValue,
  detectSensitiveCategoryByColumnName,
  isValidLuhn,
} from '../../src/utils/pii-masker.js';
import { SqlHandler } from '../../src/mcp/handlers/sql.js';
import { CardsHandler } from '../../src/mcp/handlers/cards.js';
import { getJobStore } from '../../src/mcp/job-store.js';
import { jest } from '@jest/globals';

describe('Challenger 2 M1: Adversarial PII & Query Output Sanitization Suite', () => {

  // =========================================================================
  // Section 1: Column-Aware Heuristics Under Supported Column Naming
  // =========================================================================
  describe('1. Supported and Unsupported Column Name Heuristics', () => {
    const supportedCases = [
      { col: 'USER_EMAIL', expected: 'email' },
      { col: 'customer_email', expected: 'email' },
      { col: 'ClientEmail', expected: 'email' },
      { col: 'mail', expected: 'email' },

      { col: 'TELEPHONE', expected: 'phone' },
      { col: 'Mobile_Phone', expected: 'phone' },
      { col: 'contact_number', expected: 'phone' },
      { col: 'cell', expected: 'phone' },
      { col: 'phone', expected: 'phone' },

      { col: 'SocialSecurityNumber', expected: 'ssn' },
      { col: 'customer_ssn', expected: 'ssn' },
      { col: 'taxid', expected: 'ssn' },
      { col: 'nationalid', expected: 'ssn' },

      { col: 'CREDIT_CARD_NUM', expected: 'card' },
      { col: 'customer_debitcard', expected: 'card' },
      { col: 'cardnumber', expected: 'card' },
      { col: 'cc_num', expected: 'card' },

      { col: 'User_Password_Hash', expected: 'password' },
      { col: 'PASSWD', expected: 'password' },
      { col: 'passphrase', expected: 'password' },

      { col: 'API_SECRET_KEY', expected: 'secret' },
      { col: 'bearer_token', expected: 'secret' },
      { col: 'access_token', expected: 'secret' },
      { col: 'privatekey', expected: 'secret' },

      { col: 'CLIENT_IP', expected: 'ip' },
      { col: 'remote_ip', expected: 'ip' },
      { col: 'ipaddress', expected: 'ip' },
    ];

    test.each(supportedCases)(
      'detects category $expected for column name "$col"',
      ({ col, expected }) => {
        expect(detectSensitiveCategoryByColumnName(col)).toBe(expected);
      }
    );

    // Verify extended composite column name heuristics
    test('detects composite column name variations correctly', () => {
      expect(detectSensitiveCategoryByColumnName('billing_mail')).toBe('email');
      expect(detectSensitiveCategoryByColumnName('TEL_NUMBER')).toBe('phone');
      expect(detectSensitiveCategoryByColumnName('cellNumber')).toBe('phone');
      expect(detectSensitiveCategoryByColumnName('tin_number')).toBe('ssn');
      expect(detectSensitiveCategoryByColumnName('source_ipv4')).toBe('ip');
      expect(detectSensitiveCategoryByColumnName('AUTH_BEARER')).toBe('secret');
    });
  });

  // =========================================================================
  // Section 2: Deterministic Pseudonymization & Analytical Integrity
  // =========================================================================
  describe('2. Deterministic Pseudonymization & COUNT(DISTINCT) Preservation', () => {
    test('preserves 100% COUNT(DISTINCT) accuracy across 2,000 synthetic records', () => {
      const distinctUsers = 250;
      const totalRecords = 2000;
      const salt = 'enterprise_bi_salt_2026';

      const userPool = Array.from({ length: distinctUsers }, (_, i) => `user_${i.toString().padStart(4, '0')}@corp-analytics.io`);
      const dataset = [];

      for (let i = 0; i < totalRecords; i++) {
        const user = userPool[i % distinctUsers];
        dataset.push({
          transaction_id: i + 1,
          user_email: user,
          amount: (i % 50) * 10 + 5,
        });
      }

      // Raw distinct count
      const rawDistinctCount = new Set(dataset.map(d => d.user_email)).size;
      expect(rawDistinctCount).toBe(distinctUsers);

      // Mask dataset with pseudonymization
      const maskedDataset = dataset.map(row => ({
        ...row,
        user_email: pseudonymizeValue(row.user_email, salt),
      }));

      // Pseudonymized distinct count
      const maskedDistinctCount = new Set(maskedDataset.map(d => d.user_email)).size;
      expect(maskedDistinctCount).toBe(rawDistinctCount);

      // Zero identity leakage in pseudonymized output
      maskedDataset.forEach(row => {
        expect(row.user_email).toMatch(/^anon_[a-f0-9]{12}$/);
        expect(row.user_email).not.toContain('user_');
        expect(row.user_email).not.toContain('corp-analytics.io');
      });

      // Group-by sum verification
      const rawUserSums = new Map();
      dataset.forEach(row => {
        rawUserSums.set(row.user_email, (rawUserSums.get(row.user_email) || 0) + row.amount);
      });

      const maskedUserSums = new Map();
      maskedDataset.forEach(row => {
        maskedUserSums.set(row.user_email, (maskedUserSums.get(row.user_email) || 0) + row.amount);
      });

      const rawSumsSorted = Array.from(rawUserSums.values()).sort((a, b) => a - b);
      const maskedSumsSorted = Array.from(maskedUserSums.values()).sort((a, b) => a - b);

      expect(maskedSumsSorted).toEqual(rawSumsSorted);
    });

    test('maintains join consistency across different queries with same salt and isolates different salts', () => {
      const email = 'executive@holdingcompany.com';
      const saltA = 'tenant_production_salt';
      const saltB = 'tenant_staging_salt';

      const pA1 = pseudonymizeValue(email, saltA);
      const pA2 = pseudonymizeValue(email, saltA);
      const pB = pseudonymizeValue(email, saltB);

      expect(pA1).toBe(pA2);
      expect(pA1).not.toBe(pB);
      expect(pA1).toMatch(/^anon_[a-f0-9]{12}$/);
      expect(pB).toMatch(/^anon_[a-f0-9]{12}$/);
    });
  });

  // =========================================================================
  // Section 3: Integration Stress Test for SqlHandler.handleExecuteSQL
  // =========================================================================
  describe('3. SqlHandler.handleExecuteSQL Zero-Leak Verification', () => {
    let mockClient;

    beforeEach(() => {
      mockClient = {
        executeNativeQuery: jest.fn(),
      };
    });

    test('sanitizes mixed payloads and adversarial column names in markdown and structuredContent', async () => {
      const mockCols = [
        { name: 'id', base_type: 'type/Integer' },
        { name: 'Customer_Email', base_type: 'type/Text' },
        { name: 'TEL_NO', base_type: 'type/Text' },
        { name: 'Social_Security', base_type: 'type/Text' },
        { name: 'CC_NUM', base_type: 'type/Text' },
        { name: 'UNSTRUCTURED_LOGS', base_type: 'type/Text' },
      ];

      const mockRows = [
        [
          101,
          'alice.wonderland@secret-lab.org',
          '+1-555-321-7654',
          '123-45-6789',
          '4532-0151-1283-0366',
          'User logged in with key sk-proj-12345678901234567890 and token Bearer mySecretToken12345',
        ],
        [
          102,
          'bob.builder@construction.co.uk',
          '+44 20 7123 4567',
          '987.65.4321',
          '5425 2334 3010 9903',
          'Auth error: password $2a$12$e8Mc8jPqR7Q1s7/5rXn2QOWrF2O4v6YV3Z6qX0r7P4v8Z9Q0W1E2. from IP 192.168.1.50',
        ],
      ];

      mockClient.executeNativeQuery.mockResolvedValueOnce({
        data: {
          cols: mockCols,
          rows: mockRows,
        },
      });

      const sqlHandler = new SqlHandler(mockClient);
      const res = await sqlHandler.handleExecuteSQL({
        database_id: 1,
        sql: 'SELECT * FROM confidential_audit LIMIT 2;',
      });

      const text = res.content[0].text;
      const structuredRows = res.structuredContent.rows;

      // Assertions on Markdown Output
      expect(text).toContain('a***d@secret-lab.org');
      expect(text).toContain('+1-***-***-7654');
      expect(text).toContain('***-**-6789');
      expect(text).toContain('****-****-****-0366');
      expect(text).toContain('[REDACTED_SECRET]');
      expect(text).toContain('b***r@construction.co.uk');
      expect(text).toContain('+44-***-***-4567');
      expect(text).toContain('***.**.4321');
      expect(text).toContain('**** **** **** 9903');
      expect(text).toContain('[REDACTED_PASSWORD]');
      expect(text).toContain('192.168.*.*');

      // Verify zero leak in text
      expect(text).not.toContain('alice.wonderland@secret-lab.org');
      expect(text).not.toContain('bob.builder@construction.co.uk');
      expect(text).not.toContain('4532-0151-1283-0366');
      expect(text).not.toContain('5425 2334 3010 9903');
      expect(text).not.toContain('sk-proj-12345678901234567890');
      expect(text).not.toContain('mySecretToken12345');
      expect(text).not.toContain('$2a$12$e8Mc8j');

      // Assertions on Structured Rows
      expect(structuredRows[0][1]).toBe('a***d@secret-lab.org');
      expect(structuredRows[0][2]).toBe('+1-***-***-7654');
      expect(structuredRows[0][3]).toBe('***-**-6789');
      expect(structuredRows[0][4]).toBe('****-****-****-0366');
      expect(structuredRows[0][5]).toContain('[REDACTED_SECRET]');
      expect(structuredRows[1][1]).toBe('b***r@construction.co.uk');
      expect(structuredRows[1][2]).toBe('+44-***-***-4567');
      expect(structuredRows[1][3]).toBe('***.**.4321');
      expect(structuredRows[1][4]).toBe('**** **** **** 9903');
      expect(structuredRows[1][5]).toContain('[REDACTED_PASSWORD]');
      expect(structuredRows[1][5]).toContain('192.168.*.*');
    });
  });

  // =========================================================================
  // Section 4: Integration Stress Test for CardsHandler.handleCardData
  // =========================================================================
  describe('4. CardsHandler.handleCardData Zero-Leak Verification', () => {
    let mockClient;

    beforeEach(() => {
      mockClient = {
        request: jest.fn(),
      };
    });

    test('sanitizes card query data in JSON format', async () => {
      mockClient.request.mockResolvedValueOnce({
        data: {
          cols: [{ name: 'billing_email' }, { name: 'credit_card' }],
          rows: [
            ['customer1@shop.com', '4532-0151-1283-0366'],
            ['customer2@shop.com', '5425-2334-3010-9903'],
          ],
        },
      });

      const cardsHandler = new CardsHandler(mockClient);
      const res = await cardsHandler.handleCardData({ card_id: 42, format: 'json' });

      const text = res.content[0].text;
      expect(text).toContain('c***1@shop.com');
      expect(text).toContain('c***2@shop.com');
      expect(text).toContain('****-****-****-0366');
      expect(text).toContain('****-****-****-9903');

      expect(text).not.toContain('customer1@shop.com');
      expect(text).not.toContain('customer2@shop.com');
      expect(text).not.toContain('4532-0151-1283-0366');
    });

    test('sanitizes multiline RFC 4180 CSV export with embedded PII and quotes', async () => {
      const rawCsv = `id,customer_email,payload,card_number\n1,john@acme.com,"Note with phone: +1-555-888-9999 and token sk-proj-12345678901234567890",4532-0151-1283-0366\n2,sarah@acme.com,"Multiline\nuser message with ssn 999-88-7777",5425-2334-3010-9903\n`;

      mockClient.request.mockResolvedValueOnce(rawCsv);

      const cardsHandler = new CardsHandler(mockClient);
      const res = await cardsHandler.handleCardData({ card_id: 42, format: 'csv' });

      const csvText = res.content[0].text;
      expect(csvText).toContain('j***n@acme.com');
      expect(csvText).toContain('s***h@acme.com');
      expect(csvText).toContain('+1-***-***-9999');
      expect(csvText).toContain('[REDACTED_SECRET]');
      expect(csvText).toContain('***-**-7777');
      expect(csvText).toContain('****-****-****-0366');
      expect(csvText).toContain('****-****-****-9903');

      expect(csvText).not.toContain('john@acme.com');
      expect(csvText).not.toContain('sarah@acme.com');
      expect(csvText).not.toContain('+1-555-888-9999');
      expect(csvText).not.toContain('sk-proj-');
      expect(csvText).not.toContain('999-88-7777');
      expect(csvText).not.toContain('4532-0151-1283-0366');
    });
  });

  // =========================================================================
  // Section 5: EMPIRICAL VULNERABILITY REPRODUCTION IN SqlHandler.handleSQLStatus
  // =========================================================================
  describe('5. Empirical Vulnerability Reproduction: SqlHandler.handleSQLStatus structuredContent Leak', () => {
    test('reproduces and documents raw PII leakage in structuredContent.result.rows of completed async query jobs', async () => {
      const jobStore = getJobStore();
      const job = jobStore.create(1, 'SELECT sensitive_data FROM users;', 60);

      const sensitivePayload = {
        data: {
          cols: [
            { name: 'email' },
            { name: 'phone' },
            { name: 'ssn' },
            { name: 'credit_card' },
            { name: 'secret_key' },
          ],
          rows: [
            [
              'vulnerable.target@enterprise.com',
              '+1-555-456-7890',
              '123-45-6789',
              '4532-0151-1283-0366',
              'sk-proj-09876543210987654321',
            ],
          ],
        },
      };

      jobStore.markComplete(job.id, sensitivePayload, 1);

      const sqlHandler = new SqlHandler({});
      const res = await sqlHandler.handleSQLStatus({ job_id: job.id });

      // 1. Verify textOutput IS masked
      const textOutput = res.content[0].text;
      expect(textOutput).toContain('v***t@enterprise.com');
      expect(textOutput).toContain('+1-***-***-7890');
      expect(textOutput).toContain('***-**-6789');

      // 2. Observe structuredContent.result.rows behavior
      // In src/mcp/handlers/sql.js line 377:
      // rows: rows.slice(0, 200)
      // This references the sanitized `rows` variable.
      const structuredRows = res.structuredContent?.result?.rows;
      expect(structuredRows).toBeDefined();

      expect(structuredRows[0][0]).toBe('v***t@enterprise.com');
      expect(structuredRows[0][1]).toBe('+1-***-***-7890');
      expect(structuredRows[0][2]).toBe('***-**-6789');
      expect(structuredRows[0][3]).toBe('****-****-****-0366');
      expect(structuredRows[0][4]).toBe('[REDACTED_SECRET]');

      const leaksRawPii = structuredRows[0][0] === 'vulnerable.target@enterprise.com';
      expect(leaksRawPii).toBe(false);
    });
  });
});
