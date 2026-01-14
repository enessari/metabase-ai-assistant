export default {
  apps: [{
    name: 'metabase-ai-mcp',
    script: 'src/mcp/server.js',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    env: {
      NODE_ENV: 'production',
      LOG_LEVEL: 'info'
    },
    env_production: {
      NODE_ENV: 'production',
      LOG_LEVEL: 'error'
    },
    error_file: './logs/mcp-error.log',
    out_file: './logs/mcp-out.log',
    log_file: './logs/mcp-combined.log',
    time: true,
    merge_logs: true,
    max_restarts: 10,
    min_uptime: '10s',
    restart_delay: 4000
  }]
};