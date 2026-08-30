import {
  isValidLuhn,
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
} from '../src/utils/pii-masker.js';

console.log('--- START DEEP EMPIRICAL EXPLORATION ---');

// 1. Check Phone masking with URL encoded phone
console.log('1. URL Encoded Phone:');
const p1 = maskPhone('%2B1%20555%20234%205678');
console.log('maskPhone("%2B1%20555%20234%205678") =>', JSON.stringify(p1));

// 2. Check BigInt handling in maskValue
console.log('\n2. BigInt Handling:');
const bigInt = 1234567890123456n;
const maskedBigInt = maskValue(bigInt);
console.log('maskValue(1234567890123456n) =>', typeof maskedBigInt, maskedBigInt);

// 3. Check Symbol handling in maskValue
console.log('\n3. Symbol Handling:');
const sym = Symbol('apiKey');
try {
  const maskedSym = maskValue(sym);
  console.log('maskValue(Symbol("apiKey")) =>', typeof maskedSym, maskedSym);
} catch (e) {
  console.log('maskValue(Symbol) threw:', e.message);
}

// 4. Check Order IDs and timestamps matched by PHONE_REGEX
console.log('\n4. Non-phone digits in maskString:');
const orderId = '1234567890123456';
console.log('maskString("Order: 1234567890123456") =>', maskString(`Order: ${orderId}`));
console.log('maskString("Timestamp: 1725055200000") =>', maskString('Timestamp: 1725055200000'));

// 5. Check URL password regex with complex URIs
console.log('\n5. URI Password regex:');
const uri1 = 'postgres://user:pass%40word@localhost:5432/db';
console.log('maskSecrets(uri1) =>', maskSecrets(uri1));
const uri2 = 'https://api.example.com/v1/users?token=secret123';
console.log('maskSecrets(uri2) =>', maskSecrets(uri2));

// 6. Check CSV RFC 4180 with complex quotes and newlines
console.log('\n6. CSV RFC 4180 parser:');
const csvComplex = `id,name,email,description
1,"Smith, ""The Boss"" John",john.boss@corp.com,"Line 1
Line 2 with comma, and ""quotes"""
2,Jane,jane@domain.com,Simple note`;

const maskedCsv = maskCSV(csvComplex);
console.log('Masked CSV result:\n' + maskedCsv);

// 7. Check Object mutation / immutability
console.log('\n7. Object Immutability:');
const originalObj = { user: { email: 'original@corp.com' } };
const maskedObj = maskObject(originalObj);
console.log('Original email before/after:', originalObj.user.email);
console.log('Masked email:', maskedObj.user.email);
console.log('Is new object created?', originalObj !== maskedObj && originalObj.user !== maskedObj.user);

// 8. Check Column heuristics coverage
console.log('\n8. Column Heuristics:');
const colNames = [
  'email', 'user_email', 'contact_email', 'mail', 'EmailAddress',
  'phone', 'phone_number', 'mobile', 'tel', 'cell', 'telephone',
  'ssn', 'social_security', 'tax_id', 'ein', 'tin', 'national_id',
  'credit_card', 'card_number', 'pan', 'cvv', 'cvc',
  'password', 'password_hash', 'passwd', 'pwd',
  'api_key', 'auth_token', 'secret', 'bearer_token', 'private_key',
  'ip', 'ip_address', 'client_ip', 'remote_ip', 'ipv4', 'ipv6',
  'id', 'created_at', 'updated_at', 'amount', 'status'
];
for (const col of colNames) {
  console.log(`Column '${col}' =>`, detectSensitiveCategoryByColumnName(col));
}

// 9. Throughput benchmark (10,000 values)
console.log('\n9. Throughput Benchmark:');
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

const t0 = performance.now();
const ITERATIONS = 10000;
for (let i = 0; i < ITERATIONS; i++) {
  const val = sampleValues[i % sampleValues.length];
  maskValue(val);
}
const t1 = performance.now();
console.log(`Processed ${ITERATIONS} values in ${(t1 - t0).toFixed(2)}ms (${Math.round(ITERATIONS / ((t1 - t0) / 1000))} ops/sec)`);

console.log('--- END DEEP EMPIRICAL EXPLORATION ---');
