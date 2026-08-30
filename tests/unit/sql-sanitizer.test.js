import {
  sanitizeString,
  sanitizeNumber,
  sanitizeIdentifier,
  sanitizeLikePattern,
  sanitizeJson,
  sanitizeInterval
} from '../../src/utils/sql-sanitizer.js';

describe('Unit Test: SQL Sanitizer Utility', () => {
  describe('sanitizeString', () => {
    test('handles null and undefined', () => {
      expect(sanitizeString(null)).toBe('NULL');
      expect(sanitizeString(undefined)).toBe('NULL');
    });

    test('escapes single quotes and backslashes', () => {
      expect(sanitizeString("O'Reilly")).toBe("O''Reilly");
      expect(sanitizeString("C:\\path\\to\\file")).toBe("C:\\\\path\\\\to\\\\file");
      expect(sanitizeString("'; DROP TABLE users; --")).toBe("''; DROP TABLE users; --");
    });
  });

  describe('sanitizeNumber', () => {
    test('validates valid numbers and numeric strings', () => {
      expect(sanitizeNumber(42)).toBe(42);
      expect(sanitizeNumber('123.45')).toBe(123.45);
      expect(sanitizeNumber(0)).toBe(0);
      expect(sanitizeNumber(null)).toBe(0);
    });

    test('throws on NaN or invalid inputs', () => {
      expect(() => sanitizeNumber('invalid_num')).toThrow('Invalid numeric value');
      expect(() => sanitizeNumber({})).toThrow('Invalid numeric value');
    });
  });

  describe('sanitizeIdentifier', () => {
    test('allows alphanumeric, underscores, dots, hyphens', () => {
      expect(sanitizeIdentifier('users')).toBe('users');
      expect(sanitizeIdentifier('public.user_profiles')).toBe('public.user_profiles');
      expect(sanitizeIdentifier('table-name_123')).toBe('table-name_123');
    });

    test('rejects spaces, quotes, and malicious SQL injection in identifiers', () => {
      expect(() => sanitizeIdentifier('users; DROP TABLE')).toThrow('Invalid identifier');
      expect(() => sanitizeIdentifier('users"')).toThrow('Invalid identifier');
      expect(() => sanitizeIdentifier('')).toThrow('Identifier must be a non-empty string');
    });
  });

  describe('sanitizeLikePattern', () => {
    test('escapes %, _, single quotes and backslashes', () => {
      expect(sanitizeLikePattern('100%_guaranteed')).toBe('100\\%\\_guaranteed');
      expect(sanitizeLikePattern("user's_data%")).toBe("user''s\\_data\\%");
    });
  });

  describe('sanitizeJson', () => {
    test('serializes objects and escapes single quotes', () => {
      expect(sanitizeJson({ name: "O'Connor" })).toBe('{"name":"O\'\'Connor"}');
    });

    test('throws on invalid JSON strings', () => {
      expect(() => sanitizeJson('{invalid_json')).toThrow('Invalid JSON value');
    });
  });

  describe('sanitizeInterval', () => {
    test('constructs valid intervals', () => {
      expect(sanitizeInterval(30, 'days')).toBe('30 days');
      expect(sanitizeInterval('12', 'HOURS')).toBe('12 hours');
    });

    test('rejects unauthorized units', () => {
      expect(() => sanitizeInterval(5, 'years; DROP TABLE')).toThrow('Invalid interval unit');
    });
  });
});
