# Privacy Policy

**Effective Date**: January 2024
**Last Updated**: January 2026

## Overview

This Privacy Policy describes how ONMARTECH LLC ("we", "our", "us") handles information when you use the Metabase AI Assistant software ("the Software").

## Data Collection

### What We Do NOT Collect

The Metabase AI Assistant is a self-hosted, open-source tool. We do not:

- Collect or store your database credentials
- Access your Metabase instance
- Store your SQL queries or results
- Track your usage patterns
- Collect personal information
- Send telemetry or analytics data

### What Remains on Your Systems

All data processed by the Software remains within your infrastructure:

- Database connection credentials (stored in your `.env` file)
- Query results and database content
- Metabase configurations
- Log files and error reports

## Data Processing

### Local Processing

The Software operates entirely within your local or self-hosted environment:

1. **MCP Server**: Runs on your machine or server
2. **Database Connections**: Direct connections to your databases
3. **API Calls**: Communication only with your Metabase instance

### Third-Party Services

When AI features are enabled, queries may be sent to:

- **OpenAI API**: For SQL generation and optimization (if configured)
- **Anthropic API**: For AI-powered features (if configured)

These services are subject to their respective privacy policies:
- OpenAI: https://openai.com/privacy
- Anthropic: https://www.anthropic.com/privacy

## Data Security

### Credential Protection

- Credentials are stored only in your local `.env` file
- The `.env` file is excluded from version control by default
- No credentials are logged or transmitted in plain text

### Database Operations

- AI-created objects are prefixed with `claude_ai_` for identification
- DDL operations require explicit approval
- Audit logs are stored locally

## User Rights

As all data remains on your systems, you maintain full control over:

- **Access**: Full access to all stored data
- **Deletion**: Delete any data at any time
- **Export**: Export data in any format you choose
- **Modification**: Modify or update any stored information

## Children's Privacy

The Software is not intended for use by individuals under 18 years of age.

## Changes to This Policy

We may update this Privacy Policy periodically. Changes will be documented in the repository with updated "Last Updated" dates.

## Open Source Transparency

This is an open-source project. You can:

- Review the complete source code
- Verify data handling practices
- Audit security implementations
- Fork and modify for your needs

Repository: https://github.com/onmartech/metabase-ai-assistant

## Contact Information

For privacy-related inquiries:

- **Email**: privacy@onmartech.com
- **Company**: ONMARTECH LLC
- **GitHub Issues**: https://github.com/onmartech/metabase-ai-assistant/issues

## Legal Basis

This Software is provided under the Apache License 2.0. By using the Software, you acknowledge that:

1. You are responsible for your own data security
2. You comply with applicable data protection regulations
3. You have appropriate authorization to access connected databases
4. You understand that AI features may process data through third-party APIs

---

Copyright 2024-2026 ONMARTECH LLC. All rights reserved.
