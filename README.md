# Metabase AI Assistant 🤖

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)
[![Node.js](https://img.shields.io/badge/Node.js-18+-brightgreen.svg)](https://nodejs.org/)
[![MCP Compatible](https://img.shields.io/badge/MCP-Compatible-blue.svg)](https://modelcontextprotocol.io/)
[![GitHub stars](https://img.shields.io/github/stars/enessari/metabase-ai-assistant.svg?style=social&label=Star)](https://github.com/enessari/metabase-ai-assistant/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/enessari/metabase-ai-assistant.svg?style=social&label=Fork)](https://github.com/enessari/metabase-ai-assistant/forks)

AI-powered assistant that connects to **Metabase** and **PostgreSQL databases** directly via **Model Context Protocol (MCP)** for **Claude Desktop** and **Claude Code**. Creates models, SQL queries, metrics, and dashboards using both Metabase API and direct database connections.

> 🚀 **MCP Server for Claude Desktop & Claude Code** - Metabase + Direct DB Access  
> ⭐ **If you find this project useful, please give it a star!** ⭐

## 🚀 Features

### 🔌 MCP Integration (Claude Desktop & Claude Code)
- **Model Context Protocol**: Native integration with Claude Desktop and Claude Code
- **Direct Database Access**: Direct PostgreSQL database connections
- **Metabase API Integration**: Full integration with Metabase instances
- **Schema Discovery**: Automatic database schema discovery and analysis
- **Relationship Detection**: Table relationship detection and suggestions

### 🤖 AI-Powered Features
- **Natural Language SQL**: Generate SQL queries from natural language descriptions
- **Smart Model Building**: AI-assisted Metabase model creation
- **Intelligent Dashboards**: Automatic dashboard layout and widget suggestions
- **Query Optimization**: SQL query performance optimization
- **Data Insights**: Data analysis and pattern detection

### 🛠️ Developer Tools
- **DDL Operations**: Safe table/view/index creation (prefix-protected)
- **Batch Operations**: Bulk data processing operations
- **Connection Management**: Hybrid connection management (API + Direct)
- **Security Controls**: AI object prefix control and approval workflows
- **Performance Monitoring**: Operation timing and timeout controls

## 📋 Requirements

### 🖥️ System
- **Node.js 18+**
- **Claude Desktop** (for MCP support) OR **Claude Code**
- **PostgreSQL Database** (for direct connections)

### 🔗 Services
- **Metabase instance** (v0.48+)
- **Anthropic API** (included in Claude Desktop/Code)

## 🔧 Installation

```bash
# Clone the repository
git clone https://github.com/onmartech/metabase-ai-assistant.git
cd metabase-ai-assistant

# Install dependencies
npm install

# Create environment file
cp .env.example .env
```

## ⚙️ Configuration

Edit the `.env` file:

```env
# Metabase Configuration
METABASE_URL=http://your-metabase-instance.com
METABASE_USERNAME=your_username
METABASE_PASSWORD=your_password
METABASE_API_KEY=your_metabase_api_key

# AI Provider (at least one required)
ANTHROPIC_API_KEY=your_anthropic_key
# or
OPENAI_API_KEY=your_openai_key

# Application Settings
LOG_LEVEL=info
```

⚠️ **Security Warning**: Never commit the `.env` file to version control. This file is already included in `.gitignore`.

## 🔌 Claude Desktop & Claude Code Integration (MCP)

This project integrates with Claude Desktop and Claude Code via Model Context Protocol (MCP):

### For Claude Desktop:

1. **Edit Claude Desktop Config**: `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "metabase-ai-assistant": {
      "command": "node",
      "args": ["/path/to/your/metabase-ai-assistant/src/mcp/server.js"],
      "env": {
        "METABASE_URL": "http://your-metabase-instance.com",
        "METABASE_USERNAME": "your_username",
        "METABASE_PASSWORD": "your_password",
        "ANTHROPIC_API_KEY": "your_anthropic_key"
      }
    }
  }
}
```

2. **Restart Claude Desktop** and MCP tools will be available.

### For Claude Code:

Claude Code can use this MCP server directly via global installation:

#### Step 1: Global Installation
```bash
# Install the MCP server globally
npm link

# Verify installation
which metabase-ai-mcp
npm list -g | grep metabase-ai-assistant
```

#### Step 2: Environment Setup
Ensure your `.env` file is properly configured with your Metabase credentials:

```env
METABASE_URL=http://your-metabase-instance.com
METABASE_USERNAME=your_username
METABASE_PASSWORD=your_password
METABASE_API_KEY=your_api_key
ANTHROPIC_API_KEY=your_anthropic_key
```

#### Step 3: Test MCP Server
```bash
# Test the MCP server directly
node src/mcp/server.js

# Test with environment variables
export METABASE_URL="http://your-instance.com"
export METABASE_USERNAME="your_username"
export METABASE_PASSWORD="your_password"
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | node src/mcp/server.js
```

#### Step 4: Verify Integration
In Claude Code, ask: **"What MCP tools do you have available?"**

You should see **27 Metabase AI Assistant tools** available:

**📊 Database Tools:**
- `db_list` - List all Metabase databases
- `db_schemas` - Get schema information  
- `db_tables` - List tables with details
- `sql_execute` - Run SQL queries

**🎯 Metabase Tools:**
- `mb_question_create` - Create questions/charts
- `mb_dashboard_create` - Create dashboards
- `mb_dashboard_template_executive` - Auto-generate executive dashboards
- `mb_question_create_parametric` - Create parametric questions

**🔍 AI-Powered Tools:**
- `ai_sql_generate` - Generate SQL from natural language
- `ai_sql_optimize` - Optimize SQL performance
- `ai_sql_explain` - Explain SQL queries

**📚 Documentation Tools:**
- `web_explore_metabase_docs` - Crawl Metabase documentation
- `web_search_metabase_docs` - Search documentation

The server provides comprehensive Metabase and PostgreSQL integration with **27 tools** for:
- Database schema exploration and analysis
- Natural language SQL query generation and optimization  
- Executive dashboard templates and parametric questions
- Direct DDL operations with security controls
- Metabase documentation crawling and search
- Table relationship detection and mapping

## 🎯 Usage

### Interactive CLI

```bash
npm start
```

### Programmatic Usage

```javascript
import { MetabaseClient } from './src/metabase/client.js';
import { MetabaseAIAssistant } from './src/ai/assistant.js';

// Client oluştur
const client = new MetabaseClient({
  url: 'http://your-metabase.com',
  username: 'user',
  password: 'pass'
});

// AI Assistant başlat
const assistant = new MetabaseAIAssistant({
  metabaseClient: client,
  aiProvider: 'anthropic',
  anthropicApiKey: 'your-key'
});

// Model oluştur
const model = await assistant.createModel(
  'Müşteri segmentasyon modeli',
  databaseId
);

// SQL sorgusu üret
const sql = await assistant.generateSQL(
  'Son 30 günün satış toplamı',
  schema
);
```

## 📚 Örnek Senaryolar

### 1. E-Ticaret Dashboard'u

```javascript
// Satış modeli oluştur
await assistant.createModel(
  'Günlük satış özeti - ürün, kategori, tutar',
  databaseId
);

// Metrikler tanımla
await assistant.createMetric(
  'Ortalama sepet değeri',
  tableId
);

// Dashboard oluştur
await assistant.createDashboard(
  'E-Ticaret Yönetici Paneli',
  questions
);
```

### 2. Müşteri Analizi

```javascript
// Müşteri segmentasyon sorgusu
const sql = await assistant.generateSQL(
  'RFM analizi ile müşteri segmentleri',
  schema
);

// Churn prediction modeli
await assistant.createModel(
  'Müşteri kayıp tahmin modeli',
  databaseId
);
```

### 3. Finansal Raporlama

```javascript
// Gelir-gider analizi
await assistant.createQuestion(
  'Aylık kar-zarar tablosu',
  databaseId
);

// Bütçe karşılaştırma dashboard'u
await assistant.createDashboard(
  'Bütçe vs Gerçekleşen',
  budgetQuestions
);
```

## 🛠️ CLI Komutları

Interaktif CLI'da kullanılabilir komutlar:

- **📊 Create Model**: AI ile model oluştur
- **❓ Create Question**: SQL sorgusu oluştur
- **📈 Create Metric**: Metrik tanımla
- **📋 Create Dashboard**: Dashboard hazırla
- **🔍 Explore Schema**: Veritabanı şemasını incele
- **🚀 Execute SQL**: SQL sorgusu çalıştır
- **🔧 Optimize Query**: Sorgu optimize et
- **💡 AI Query Builder**: Doğal dilde sorgu oluştur

## 📂 Proje Yapısı

```
metabase-ai-assistant/
├── src/
│   ├── mcp/
│   │   └── server.js        # MCP Server (Claude Desktop entegrasyonu)
│   ├── metabase/
│   │   └── client.js        # Metabase API client
│   ├── database/
│   │   ├── direct-client.js     # Direct PostgreSQL client
│   │   └── connection-manager.js # Hybrid connection manager
│   ├── ai/
│   │   └── assistant.js     # AI helper functions
│   ├── cli/
│   │   └── interactive.js   # Interactive CLI (standalone)
│   ├── utils/
│   │   └── logger.js        # Logging utilities
│   └── index.js             # Main entry point (CLI mode)
├── tests/                    # Test files
├── .env.example             # Environment template
├── package.json
└── README.md
```

## 🔍 API Referansı

### MetabaseClient

```javascript
// Veritabanları
getDatabases()
getDatabase(id)
getDatabaseSchemas(databaseId)
getDatabaseTables(databaseId)

// Modeller
getModels()
createModel(modelData)

// Sorgular
getQuestions(collectionId)
createQuestion(questionData)
executeNativeQuery(databaseId, sql)

// Metrikler
getMetrics()
createMetric(metricData)

// Dashboard'lar
getDashboards()
createDashboard(dashboardData)
addCardToDashboard(dashboardId, cardId, options)
```

### MetabaseAIAssistant

```javascript
// AI İşlemleri
analyzeRequest(userRequest)
generateSQL(description, schema)
suggestVisualization(data, questionType)
optimizeQuery(sql)
explainQuery(sql)

// Oluşturma İşlemleri
createModel(description, databaseId)
createQuestion(description, databaseId, collectionId)
createMetric(description, tableId)
createDashboard(description, questions)
```

## 🧪 Test

```bash
# Tüm testleri çalıştır
npm test

# Bağlantı testi
npm run test:connection

# Coverage raporu
npm run test:coverage
```

## 🔒 Security

### Data Security
- **Environment Variables**: All sensitive data (API keys, passwords) stored in `.env` file
- **Git Ignore**: `.env` file excluded from version control
- **SQL Injection Protection**: Parameterized queries and input validation
- **Rate Limiting**: API request rate limiting applied
- **Audit Logging**: All database operations logged for security monitoring
- **No Hardcoded Credentials**: Security-first approach prevents credential exposure

### Database Security
- **AI Object Prefix**: All AI-created objects marked with `claude_ai_` prefix for safety
- **Schema Isolation**: Operations limited to specified schemas only
- **Read-Only Mode**: Default read-only permissions with explicit approval for modifications
- **DDL Approval System**: Database changes require explicit confirmation
- **Prefix Validation**: Only AI-prefixed objects can be modified or deleted

### MCP Security
- **Secure Transport**: MCP communication over secure channels
- **Environment Isolation**: Credentials passed via environment variables
- **Tool Validation**: All tool inputs validated before execution
- **Error Handling**: Sensitive information filtered from error messages

### Production Deployment
- Use environment-specific configuration files
- Prefer SSL/TLS connections for all database communications
- Grant minimum required permissions to database users
- Protect API endpoints with authentication and authorization
- Regularly rotate API keys and database passwords
- Monitor and log all tool usage for security auditing

## 🐛 Troubleshooting

### Connection Errors
- Verify Metabase URL is accessible
- Ensure API key and credentials are valid
- Check network connectivity and firewall settings
- Confirm environment variables are properly set

### MCP Integration Issues
- Ensure `npm link` was run successfully
- Verify MCP server binary is in PATH: `which metabase-ai-mcp`
- Check environment variables are exported: `echo $METABASE_URL`
- Test MCP server directly: `node src/mcp/server.js`
- Restart Claude Code after global installation

### Query Errors
- Validate SQL syntax and formatting
- Verify table and column names exist
- Check database permissions and schema access
- Ensure proper schema selection for operations

### Security Warnings
- Never commit `.env` files to version control
- Avoid hardcoding credentials in source code
- Use prefix validation for AI-created objects
- Monitor database operations for security compliance

## 🚀 Production Deployment

### Option 1: PM2 Process Manager (Recommended)
```bash
# Install PM2 globally
npm install -g pm2

# Start MCP server with PM2
npm run pm2:start

# Monitor and manage
npm run pm2:logs
npm run pm2:restart
npm run pm2:stop

# Auto-restart on system reboot
pm2 startup
pm2 save
```

### Option 2: Docker Container
```bash
# Build and run with Docker Compose
npm run docker:run

# Monitor logs
npm run docker:logs

# Stop containers
npm run docker:stop
```

### Option 3: Cloud Deployment
- **Railway**: One-click deploy with `railway.json`
- **Heroku**: Deploy with Heroku CLI (see `deploy/heroku-deploy.md`)
- **DigitalOcean**: App Platform with Docker
- **AWS**: ECS Fargate or EC2 with systemd service

### Option 4: Systemd Service (Linux)
```bash
# Copy service file
sudo cp metabase-ai-mcp.service /etc/systemd/system/

# Enable and start service
sudo systemctl enable metabase-ai-mcp
sudo systemctl start metabase-ai-mcp

# Monitor service
sudo systemctl status metabase-ai-mcp
sudo journalctl -u metabase-ai-mcp -f
```

### Production Scripts
```bash
npm run mcp:prod          # Production mode
npm run test:connection   # Health check
npm run lint             # Code quality check
```

## 📈 Roadmap

- [ ] Natural Language Processing geliştirmeleri
- [ ] Görsel sorgu builder
- [ ] Otomatik dashboard öneri sistemi
- [ ] Multi-database desteği
- [ ] Real-time data streaming
- [ ] Advanced ML modelleri

## 🤝 Katkıda Bulunma

Bu projeyi beğendiyseniz ve geliştirmesine katkıda bulunmak istiyorsanız:

### ⭐ Projeyi Destekleyin
- **GitHub'da Star Verin**: Projeyi faydalı bulduysanız ⭐ star verin
- **Follow Edin**: Güncellemelerden haberdar olmak için [@onmartech](https://github.com/onmartech) hesabını takip edin
- **Share Edin**: Sosyal medyada paylaşın ve arkadaşlarınıza önerin

### 🔧 Geliştirmeye Katılın
1. **Fork** yapın
2. **Feature branch** oluşturun (`git checkout -b feature/yeni-ozellik`)
3. **Değişikliklerinizi** commit yapın (`git commit -m 'feat: Yeni özellik eklendi'`)
4. **Push** yapın (`git push origin feature/yeni-ozellik`)
5. **Pull Request** açın

### 💡 Katkı Fikirleri
- Yeni AI modeli entegrasyonları
- Dashboard template'leri
- Metabase connector'ları
- Dokümantasyon iyileştirmeleri
- Bug fixes ve performans optimizasyonları

### 📋 Katkı Kuralları
- Kod değişikliklerinde test yazın
- Commit mesajlarında [Conventional Commits](https://conventionalcommits.org/) kullanın
- ESLint ve Prettier ayarlarına uyun
- Değişikliklerinizi dokümante edin

## 📄 Lisans

MIT License - Detaylar için [LICENSE](LICENSE) dosyasına bakın.
Copyright (c) 2024 ONMARTECH LLC

## 👥 Destek ve İletişim

### 🐛 Bug Reports & Feature Requests
- **GitHub Issues**: [Issues sayfası](https://github.com/onmartech/metabase-ai-assistant/issues)
- **Bug Template**: Issue açarken template'leri kullanın
- **Feature Requests**: Hangi özelliği istediğinizi detaylandırın

### 💬 Topluluk
- **GitHub Discussions**: Soru-cevap ve fikirler için
- **Documentation**: Wiki sayfalarına katkı yapın
- **Examples**: Örnek kullanım case'leri paylaşın

### 🚀 Ticari Destek
ONMARTECH LLC tarafından profesyonel destek ve customization hizmetleri mevcuttur.

## 🏆 Katkıda Bulunanlar

Bu projeyi mümkün kılan herkese teşekkürler:

- **ONMARTECH LLC** - Proje geliştirme ve bakım
- **Metabase Team** - Harika platform
- **Open Source Community** - Sürekli ilham ve geri bildirim

### 🌟 Hall of Fame
Önemli katkılarda bulunan geliştiriciler burada listelenecektir.

**Bu projeyi faydalı bulduysanız ⭐ star vermeyi ve 🔄 share etmeyi unutmayın!**
