#!/usr/bin/env node

import { spawn } from 'child_process';

const serverPath = '/Users/aes/my-ai-projects/metabase-ai-assistant/src/mcp/server.js';

const env = {
  ...process.env,
  METABASE_URL: process.env.METABASE_URL || 'http://localhost:3000',
  METABASE_USERNAME: process.env.METABASE_USERNAME || 'your_username',
  METABASE_PASSWORD: process.env.METABASE_PASSWORD || 'your_password',
  METABASE_API_KEY: process.env.METABASE_API_KEY || 'your_api_key',
  ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY || 'your_anthropic_key',
  OPENAI_API_KEY: process.env.OPENAI_API_KEY || 'your_openai_key'
};

console.log('Starting MCP server test...');
console.log('Server path:', serverPath);

const server = spawn('node', [serverPath], {
  env,
  stdio: ['pipe', 'pipe', 'pipe']
});

server.stdout.on('data', (data) => {
  console.log('STDOUT:', data.toString());
});

server.stderr.on('data', (data) => {
  console.error('STDERR:', data.toString());
});

server.on('error', (error) => {
  console.error('Failed to start server:', error);
});

server.on('exit', (code, signal) => {
  console.log(`Server exited with code ${code} and signal ${signal}`);
});

// Send initialize request after a short delay
setTimeout(() => {
  const initRequest = JSON.stringify({
    jsonrpc: '2.0',
    method: 'initialize',
    params: {
      protocolVersion: '2024-11-05',
      capabilities: {},
      clientInfo: {
        name: 'test-client',
        version: '1.0.0'
      }
    },
    id: 1
  }) + '\n';
  
  console.log('Sending initialize request...');
  server.stdin.write(initRequest);
}, 1000);

// Keep the process running
process.stdin.resume();