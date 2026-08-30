/**
 * Prompt Sanitizer Utility
 * Neutralizes prompt injection vectors in untrusted metadata and user inputs
 * Used by AI assistant and generative handlers for boundary isolation
 */

/**
 * Sanitize prompt metadata by neutralizing delimiter tags, system instruction tags,
 * and dangerous control characters.
 * @param {any} val - Value (string or object/primitive) to sanitize
 * @returns {string} Sanitized string
 */
export function sanitizePromptMetadata(val) {
  if (val === null || val === undefined) return '';
  const str = typeof val === 'string' ? val : JSON.stringify(val, null, 2);
  return str
    .replace(/\[\/?UNTRUSTED_METADATA\]/gi, match =>
      match.toLowerCase().includes('/') ? '[/SAFE_METADATA]' : '[SAFE_METADATA]'
    )
    .replace(/\[\/?USER_INPUT\]/gi, match =>
      match.toLowerCase().includes('/') ? '[/SAFE_USER_INPUT]' : '[SAFE_USER_INPUT]'
    )
    .replace(/<\/?system_instructions>/gi, match =>
      match.startsWith('</') ? '&lt;/system_instructions&gt;' : '&lt;system_instructions&gt;'
    )
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '');
}

/**
 * Wrap sanitized untrusted metadata inside boundary delimiter tags.
 * @param {any} metadata - Untrusted schema, table/column comments, or database metadata
 * @returns {string} Delimited metadata block
 */
export function wrapUntrustedMetadata(metadata) {
  return `[UNTRUSTED_METADATA]\n${sanitizePromptMetadata(metadata)}\n[/UNTRUSTED_METADATA]`;
}

/**
 * Wrap sanitized user input inside boundary delimiter tags.
 * @param {any} input - Natural language request or user prompt
 * @returns {string} Delimited user input block
 */
export function wrapUserInput(input) {
  return `[USER_INPUT]\n${sanitizePromptMetadata(input)}\n[/USER_INPUT]`;
}

export default {
  sanitizePromptMetadata,
  wrapUntrustedMetadata,
  wrapUserInput,
};
