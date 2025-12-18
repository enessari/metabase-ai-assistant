# Heroku Deployment Guide

## Quick Deploy
[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/enessari/metabase-ai-assistant)

## Manual Deployment

### 1. Create Heroku App
```bash
heroku create metabase-ai-mcp-[your-name]
```

### 2. Set Environment Variables
```bash
heroku config:set NODE_ENV=production
heroku config:set LOG_LEVEL=info
heroku config:set METABASE_URL=your_metabase_url
heroku config:set METABASE_USERNAME=your_username
heroku config:set METABASE_PASSWORD=your_password
heroku config:set METABASE_API_KEY=your_api_key
heroku config:set ANTHROPIC_API_KEY=your_anthropic_key
```

### 3. Deploy
```bash
git push heroku main
```

### 4. Scale
```bash
heroku ps:scale worker=1
```

## Monitoring
```bash
heroku logs --tail
heroku ps
```