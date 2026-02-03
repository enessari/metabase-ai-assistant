---
description: MCP Server ve Backend projeleri için engineering excellence prensipleri
---

# Engineering Excellence - MCP & Backend Prensipleri

Bu döküman, `jerichosequitin/metabase-mcp` ve diğer başarılı projelerden öğrenilen best practices'leri içerir.

## 🏗️ Kod Yapısı

### Modüler Handler Yapısı
Büyük dosyaları (1000+ satır) modüllere ayır:

```
src/
├── mcp/
│   ├── server.js           # Ana giriş noktası
│   └── handlers/           # İşlem modülleri
│       ├── index.js        # Merkezi export
│       ├── database.js     # DB işlemleri
│       ├── dashboard.js    # Dashboard işlemleri
│       ├── questions.js    # Question işlemleri
│       └── ai.js           # AI özellikleri
├── utils/
│   ├── cache.js            # Caching
│   ├── config.js           # Env validation
│   ├── logger.js           # Logging
│   └── response-optimizer.js
```

### Handler Context Pattern
```javascript
// Her handler'a context geç
function getHandlerContext() {
  return {
    metabaseClient: this.metabaseClient,
    aiAssistant: this.aiAssistant,
    activityLogger: this.activityLogger,
    cache: this.cache,
  };
}

// Handler'da kullan
export async function handleGetDatabases(context) {
  const { metabaseClient, cache } = context;
  // ...
}
```

---

## 🔒 Güvenlik

### Read-Only Mode (Varsayılan Açık)
```javascript
// Environment variable kontrolü
export function isReadOnlyMode() {
  return process.env.METABASE_READ_ONLY_MODE !== 'false';
}

// SQL write pattern detection
export function detectWriteOperation(sql) {
  const writePattern = /\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|EXEC|EXECUTE)\b/i;
  const match = sql.match(writePattern);
  return match ? match[0].toUpperCase() : null;
}

// Kullanım
if (isReadOnlyMode()) {
  const blocked = detectWriteOperation(sql);
  if (blocked) {
    return { error: `🔒 Blocked: ${blocked}` };
  }
}
```

### Zod Environment Validation
```javascript
import { z } from 'zod';

const envSchema = z.object({
  METABASE_URL: z.string().url(),
  METABASE_API_KEY: z.string().optional(),
  METABASE_READ_ONLY_MODE: z
    .string()
    .default('true')
    .transform(val => val.toLowerCase() === 'true'),
  CACHE_TTL_MS: z
    .string()
    .default('600000')
    .transform(val => parseInt(val, 10)),
});

// Validate on startup
const config = envSchema.parse(process.env);
```

### AI Object Prefix
AI tarafından oluşturulan objelere prefix ekle:
```javascript
const AI_PREFIX = 'claude_ai_';
const tableName = `${AI_PREFIX}${userInput}`;
```

---

## ⚡ Performans

### TTL-Based Caching
```javascript
class CacheManager {
  constructor({ ttl = 600000 }) { // 10 dakika default
    this.ttl = ttl;
    this.cache = new Map();
  }

  async getOrSet(key, fetchFn) {
    const cached = this.get(key);
    if (cached) return { data: cached, source: 'cache' };
    
    const data = await fetchFn();
    this.set(key, data);
    return { data, source: 'api' };
  }

  get(key) {
    const item = this.cache.get(key);
    if (!item) return null;
    if (Date.now() - item.timestamp > this.ttl) {
      this.cache.delete(key);
      return null;
    }
    return item.data;
  }

  set(key, data) {
    this.cache.set(key, { data, timestamp: Date.now() });
  }
}
```

### Cache Key Generators
```javascript
export const CacheKeys = {
  databases: () => 'databases',
  database: (id) => `database:${id}`,
  databaseSchemas: (id) => `database:${id}:schemas`,
  table: (id) => `table:${id}`,
};
```

---

## 📊 Response Optimization

### Token-Efficient Responses
```javascript
// Minimal format - sadece ID ve isim
const minimalDatabase = (db) => ({
  id: db.id,
  name: db.name,
  engine: db.engine,
});

// Format seviyeleri
const ResponseFormat = {
  FULL: 'full',      // Tüm detaylar
  COMPACT: 'compact', // Önemli alanlar
  MINIMAL: 'minimal', // ID + name
};

// Token estimation
function estimateTokens(text) {
  return Math.ceil(text.length / 4);
}
```

---

## 📦 Package.json SEO

```json
{
  "name": "project-name",
  "version": "3.3.0",
  "description": "Açıklayıcı, anahtar kelime içeren description",
  "keywords": [
    "mcp", "mcp-server", "model-context-protocol",
    "ai", "llm", "claude", "cursor", "openai",
    "your-domain-keywords"
  ]
}
```

---

## 📄 README Best Practices

### SEO Odaklı Yapı
1. **Centered banner** + for-the-badge style badges
2. **"Why This Project"** - Karşılaştırma tablosu
3. **Quick Start** - One-liner install
4. **Collapsible sections** - `<details>` ile tool listeleri
5. **Keywords** - Footer'da SEO kelimeleri

### Badges
```markdown
[![npm version](https://img.shields.io/npm/v/package?style=for-the-badge)](link)
[![GitHub stars](https://img.shields.io/github/stars/user/repo?style=for-the-badge)](link)
```

---

## 🚀 Release Workflow

### 1. Version Bump
```bash
npm version patch|minor|major
```

### 2. Git Tag
```bash
git tag -a v3.3.0 -m "Release notes..."
git push origin v3.3.0
```

### 3. GitHub Release
```bash
gh release create v3.3.0 --title "v3.3.0" --notes "Changelog..."
```

### 4. npm Publish
```bash
npm publish --access public
```

---

## 🔧 Kullanılacak Araçlar

| Araç | Amaç |
|------|------|
| **Zod** | Environment validation |
| **Winston** | Logging |
| **dotenv** | Environment variables |
| **gh CLI** | GitHub automation |

---

## ✅ Checklist - Yeni Proje

- [ ] Modüler yapı (`handlers/`, `utils/`)
- [ ] Read-only mode (default: true)
- [ ] Zod env validation
- [ ] TTL caching
- [ ] Response optimization
- [ ] Activity logging
- [ ] SEO README
- [ ] GitHub topics ve description
- [ ] npm keywords
- [ ] GitHub release + tag
