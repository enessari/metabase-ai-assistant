/**
 * Structured Response Utility
 * MCP Spec 2025-06-18 compliant structured output support.
 * 
 * When a tool defines an outputSchema, the response MUST include
 * `structuredContent` matching that schema. The `content` field
 * (text) is kept for backward compatibility with older clients.
 */

/**
 * Create an MCP tool response with both text content and structured content.
 * @param {object} data - The structured data to return (must match outputSchema)
 * @param {function} textFormatter - Function that converts data to human-readable text
 * @returns {{ content: Array, structuredContent: object }}
 */
export function structuredResult(data, textFormatter) {
    return {
        content: [{ type: 'text', text: textFormatter(data) }],
        structuredContent: data,
    };
}

/**
 * Create a structured error response.
 * Error responses should NOT include structuredContent per MCP spec.
 * @param {string} message - Human-readable error message
 * @returns {{ content: Array, isError: boolean }}
 */
export function structuredError(message) {
    return {
        content: [{ type: 'text', text: message }],
        isError: true,
    };
}

/**
 * Wrap an existing handler to add structuredContent from parsed text.
 * Useful for handlers that already return well-structured data internally
 * but format it as text before returning.
 * 
 * @param {function} handler - Original handler function
 * @param {function} dataExtractor - Function to extract structured data from args + raw result
 * @returns {function} Wrapped handler
 */
export function withStructuredOutput(handler, dataExtractor) {
    return async function (...args) {
        const result = await handler.apply(this, args);

        // Don't add structuredContent to error responses
        if (result.isError) return result;

        try {
            const structuredData = await dataExtractor(args, result);
            if (structuredData) {
                result.structuredContent = structuredData;
            }
        } catch (e) {
            // If extraction fails, return the original text-only response
            // This ensures backward compatibility
        }

        return result;
    };
}
