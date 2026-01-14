#!/usr/bin/env node

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

console.log('Starting MCP server debug...\n');

const env = {
  ...process.env,
  METABASE_URL: process.env.METABASE_URL || 'http://localhost:3000',
  METABASE_USERNAME: process.env.METABASE_USERNAME || 'your_username',
  METABASE_PASSWORD: process.env.METABASE_PASSWORD || 'your_password',
  METABASE_API_KEY: process.env.METABASE_API_KEY || 'your_api_key',
  NODE_ENV: 'production',
  LOG_LEVEL: 'error'
};

const serverPath = join(__dirname, 'src', 'mcp', 'server.js');
console.log('Server path:', serverPath);
console.log('Environment:', Object.keys(env).filter(k => k.startsWith('METABASE')).join(', '));
console.log('\n---Starting server---\n');

const proc = spawn('node', [serverPath], {
  env,
  stdio: ['pipe', 'pipe', 'inherit']
});

// Listen for stdout (this is where MCP communication happens)
proc.stdout.on('data', (data) => {
  const text = data.toString();
  console.log('MCP Response:', text);
});

// Handle errors
proc.on('error', (err) => {
  console.error('Failed to start server:', err);
  process.exit(1);
});

proc.on('close', (code) => {
  console.log(`Server exited with code ${code}`);
  process.exit(code);
});

// Send test initialization after a delay
setTimeout(() => {
  const initMessage = {
    jsonrpc: '2.0',
    method: 'initialize',
    params: {
      protocolVersion: '2024-11-05',
      capabilities: {},
      clientInfo: {
        name: 'debug-client',
        version: '1.0.0'
      }
    },
    id: 1
  };
  
  console.log('\nSending initialization request...');
  proc.stdin.write(JSON.stringify(initMessage) + '\n');
}, 500);

// Send list tools request after initialization
setTimeout(() => {
  const listToolsMessage = {
    jsonrpc: '2.0',
    method: 'tools/list',
    params: {},
    id: 2
  };
  
  console.log('\nSending tools/list request...');
  proc.stdin.write(JSON.stringify(listToolsMessage) + '\n');
}, 1500);

// Keep process alive for a bit
setTimeout(() => {
  console.log('\n---Debug session ending---');
  proc.kill();
}, 5000);