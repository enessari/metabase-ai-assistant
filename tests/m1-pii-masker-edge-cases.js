import {
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
} from '../src/utils/pii-masker.js';

console.log('--- ADDITIONAL EDGE CASE PROBING ---');

// 1. Display name with email
console.log('1. Email in Angle Brackets:');
const emailWithDisplay = '"John Doe" <john.doe@example.com>';
console.log('maskString:', maskString(emailWithDisplay));

// 2. Tabular edge cases
console.log('\n2. Tabular Result with Nulls/Empty:');
console.log('maskTabularResult(null) =>', maskTabularResult(null));
console.log('maskTabularResult({}) =>', maskTabularResult({}));
console.log('maskTabularResult({ data: null }) =>', maskTabularResult({ data: null }));
console.log('maskTabularResult({ data: { cols: [], rows: [] } }) =>', maskTabularResult({ data: { cols: [], rows: [] } }));

// 3. Tabular with mismatched cols/rows length
const mismatchedTabular = {
  data: {
    cols: [{ name: 'id' }, { name: 'email' }],
    rows: [
      [1], // missing email
      [2, 'user@test.com', 'extra_value', 'another_extra'], // extra columns
    ],
  },
};
console.log('\n3. Mismatched Tabular columns and rows:');
console.log(JSON.stringify(maskTabularResult(mismatchedTabular), null, 2));

// 4. CSV Edge cases
console.log('\n4. CSV Edge cases:');
console.log('maskCSV(null) =>', maskCSV(null));
console.log('maskCSV("") =>', JSON.stringify(maskCSV("")));
console.log('maskCSV("a,b,c") =>', JSON.stringify(maskCSV("a,b,c")));

// 5. Functions, Errors, Symbols inside nested objects
console.log('\n5. Special Object Properties:');
const specialObj = {
  fn: () => {},
  err: new Error('test error'),
  sym: Symbol('mySym'),
  undef: undefined,
  nil: null,
  nested: {
    email: 'test@domain.com',
  },
};
console.log('maskObject(specialObj) =>', maskObject(specialObj));

console.log('--- END ADDITIONAL EDGE CASE PROBING ---');
