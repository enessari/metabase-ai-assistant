# Contributing to Metabase AI Assistant

Thank you for your interest in contributing to Metabase AI Assistant! This document provides guidelines for contributing to the project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Commit Messages](#commit-messages)
- [Reporting Issues](#reporting-issues)

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Set up the development environment
4. Create a feature branch
5. Make your changes
6. Submit a pull request

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/metabase-ai-assistant.git
cd metabase-ai-assistant

# Install dependencies
npm install

# Copy environment template
cp .env.example .env

# Edit .env with your test credentials
# Run the MCP server
npm run mcp
```

## How to Contribute

### Types of Contributions

- **Bug Fixes**: Fix issues and improve stability
- **New Features**: Add new MCP tools or capabilities
- **Documentation**: Improve README, add examples, fix typos
- **Tests**: Add or improve test coverage
- **Performance**: Optimize existing functionality

### Before You Start

1. Check existing issues and pull requests
2. Open an issue to discuss major changes
3. Ensure your contribution aligns with project goals

## Pull Request Process

1. **Create a Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Changes**
   - Write clean, readable code
   - Add tests if applicable
   - Update documentation

3. **Test Your Changes**
   ```bash
   npm test
   npm run lint
   ```

4. **Commit Your Changes**
   ```bash
   git commit -m "feat: add new feature description"
   ```

5. **Push and Create PR**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **PR Review**
   - Address reviewer feedback
   - Keep PR focused and small
   - Update PR description as needed

## Coding Standards

### JavaScript/Node.js

- Use ES Modules (import/export)
- Use async/await for asynchronous code
- Follow existing code style
- Add JSDoc comments for public functions

### File Structure

```
src/
├── mcp/           # MCP server and tools
├── metabase/      # Metabase API client
├── database/      # Direct database connections
├── ai/            # AI assistant functionality
├── utils/         # Utility functions
└── cli/           # CLI interface
```

### Naming Conventions

- **Files**: kebab-case (e.g., `activity-logger.js`)
- **Functions**: camelCase (e.g., `getDatabases`)
- **Classes**: PascalCase (e.g., `MetabaseClient`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `MAX_RETRIES`)

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): description

[optional body]

[optional footer]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

### Examples

```
feat(mcp): add user management tools
fix(database): handle connection timeout properly
docs(readme): update installation instructions
```

## Reporting Issues

### Bug Reports

Include:
- Clear description of the bug
- Steps to reproduce
- Expected vs actual behavior
- Environment details (Node.js version, OS)
- Error messages or logs

### Feature Requests

Include:
- Clear description of the feature
- Use case and motivation
- Proposed implementation (optional)

## Questions?

- Open a GitHub Discussion for questions
- Check existing issues and documentation
- Contact: contribute@onmartech.com

---

Copyright 2024-2026 ONMARTECH LLC
Developed by Abdullah Enes SARI
