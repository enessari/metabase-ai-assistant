/**
 * Cloudflare Worker Adapter for Metabase AI Assistant
 * Provides serverless HTTP endpoints for ChatGPT Actions, Gemini Webhooks, and Remote MCP
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const metabaseUrl = env.METABASE_URL || 'https://your-metabase.com';
    const metabaseApiKey = env.METABASE_API_KEY || '';

    // Handle CORS
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-API-Key',
        },
      });
    }

    // Health check
    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response(
        JSON.stringify({
          status: 'ok',
          service: 'Metabase AI Assistant Serverless Edge',
          version: '4.2.1',
          platform: 'Cloudflare Workers',
        }),
        {
          headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        }
      );
    }

    // Query forwarding to Metabase API
    if (url.pathname.startsWith('/api/')) {
      const targetUrl = `${metabaseUrl}${url.pathname}${url.search}`;
      const headers = new Headers(request.headers);
      if (metabaseApiKey) {
        headers.set('x-api-key', metabaseApiKey);
      }

      const response = await fetch(targetUrl, {
        method: request.method,
        headers: headers,
        body: request.method !== 'GET' ? request.body : undefined,
      });

      const responseHeaders = new Headers(response.headers);
      responseHeaders.set('Access-Control-Allow-Origin', '*');

      return new Response(response.body, {
        status: response.status,
        headers: responseHeaders,
      });
    }

    return new Response(JSON.stringify({ error: 'Endpoint not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  },
};
