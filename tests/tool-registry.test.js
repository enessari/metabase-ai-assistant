import { getToolDefinitions, TOOL_METADATA } from '../src/mcp/tool-registry.js';
import { WRITE_TOOLS } from '../src/mcp/tool-router.js';

describe('Tool Registry Validation & Deduplication Tests', () => {
  const tools = getToolDefinitions();

  test('all registered tools have unique names with zero duplicates', () => {
    const names = tools.map(t => t.name);
    const seen = new Set();
    const duplicates = [];

    for (const name of names) {
      if (seen.has(name)) {
        duplicates.push(name);
      }
      seen.add(name);
    }

    expect(duplicates).toEqual([]);
    expect(tools.length).toBe(seen.size);
  });

  test('each tool definition has valid structure', () => {
    for (const tool of tools) {
      expect(tool.name).toBeDefined();
      expect(typeof tool.name).toBe('string');
      expect(tool.name.length).toBeGreaterThan(0);

      expect(tool.description).toBeDefined();
      expect(typeof tool.description).toBe('string');
      expect(tool.description.length).toBeGreaterThan(0);

      expect(tool.inputSchema).toBeDefined();
      expect(tool.inputSchema.type).toBe('object');
      expect(tool.inputSchema.properties).toBeDefined();

      expect(tool.annotations).toBeDefined();
      expect(typeof tool.annotations.readOnlyHint).toBe('boolean');
    }
  });

  test('read-only annotation aligns with write tools metadata', () => {
    for (const tool of tools) {
      const meta = TOOL_METADATA[tool.name];
      if (meta && meta.write === true) {
        expect(tool.annotations.readOnlyHint).toBe(false);
      } else {
        expect(tool.annotations.readOnlyHint).toBe(true);
      }
    }
  });

  test('generative AI tools define _provenance envelope in outputSchema', () => {
    const generativeTools = [
      'ai_sql_generate',
      'ai_sql_optimize',
      'ai_sql_explain',
      'ai_relationships_suggest',
      'mb_auto_describe',
    ];

    for (const name of generativeTools) {
      const tool = tools.find(t => t.name === name);
      expect(tool).toBeDefined();
      expect(tool.outputSchema).toBeDefined();
      expect(tool.outputSchema.properties._provenance).toBeDefined();
    }
  });
});
