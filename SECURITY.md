# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 2.x.x   | Yes                |
| 1.x.x   | No                 |

## Reporting a Vulnerability

We take security seriously at ONMARTECH LLC. If you discover a security vulnerability within the Metabase AI Assistant, please follow the responsible disclosure process below.

### How to Report

1. **Email**: Send a detailed report to security@onmartech.com
2. **Subject Line**: Use "[SECURITY] Metabase AI Assistant - Brief Description"
3. **Include**:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact assessment
   - Any suggested fixes (optional)

### Response Timeline

- **Initial Response**: Within 48 hours
- **Status Update**: Within 7 days
- **Resolution Target**: Within 30 days for critical issues

### What to Expect

- Acknowledgment of your report
- Regular updates on progress
- Credit in security advisories (if desired)
- No legal action for responsible disclosure

## Security Best Practices

### Environment Configuration

```bash
# Never commit .env files
# Use .env.example as a template
cp .env.example .env
```

### Credential Management

- Store all credentials in environment variables
- Never hardcode API keys or passwords in source code
- Rotate credentials periodically
- Use API keys instead of username/password when possible

### Database Security

- All AI-created database objects use the `claude_ai_` prefix
- DDL operations require explicit approval (`approved: true`)
- Dry-run mode is enabled by default for destructive operations
- Only AI-prefixed objects can be modified or deleted by the assistant

### Network Security

- Use HTTPS for all Metabase connections in production
- Configure firewall rules to restrict database access
- Use VPN or private networks for sensitive deployments

### MCP Server Security

- Environment variables are isolated per session
- No credentials are logged or transmitted in plain text
- All inputs are validated before execution
- Error messages are sanitized to prevent information leakage

## Secure Deployment Checklist

- [ ] Environment variables configured (not hardcoded)
- [ ] `.env` file excluded from version control
- [ ] HTTPS enabled for Metabase connection
- [ ] Database user has minimum required permissions
- [ ] API keys rotated from development values
- [ ] Audit logging enabled
- [ ] Rate limiting configured
- [ ] Firewall rules in place

## Known Security Considerations

### SQL Injection Prevention

The assistant uses parameterized queries and input validation to prevent SQL injection attacks. However, when using AI-generated SQL, always review queries before execution in production environments.

### API Key Security

- API keys provide full access to Metabase instance
- Store keys securely using secret management solutions
- Avoid sharing keys across environments

### Third-Party Dependencies

We regularly update dependencies to address known vulnerabilities. Run `npm audit` to check for issues.

## Compliance

This project is designed to support compliance with:
- GDPR (General Data Protection Regulation)
- SOC 2 Type II requirements
- HIPAA (when properly configured)

For specific compliance requirements, please contact ONMARTECH LLC.

## Contact

- Security Issues: security@onmartech.com
- General Inquiries: info@onmartech.com
- GitHub: https://github.com/onmartech/metabase-ai-assistant

---

Copyright 2024-2026 ONMARTECH LLC. All rights reserved.
