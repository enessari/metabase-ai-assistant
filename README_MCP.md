# Metabase AI Assistant - Claude Desktop Entegrasyonu

Bu doküman Metabase AI Assistant'ı Claude Desktop ile nasıl kullanacağınızı açıklar.

## 🚀 Kurulum

### 1. MCP Server'ı Test Et

```bash
cd /Users/aes/my-ai-projects/metabase-ai-assistant
npm run mcp
```

### 2. Claude Desktop Konfigürasyonu

Claude Desktop'ınızdaki `claude_desktop_config.json` dosyasına şunu ekleyin:

```json
{
  "mcpServers": {
    "metabase-ai-assistant": {
      "command": "node",
      "args": ["/Users/<UPDATE FOLDER NAME>/metabase-ai-assistant/src/mcp/server.js"],
      "env": {
        "METABASE_URL": "http://xx.xx.xx.xx:3000",
        "METABASE_USERNAME": "abc@example.com", 
        "METABASE_PASSWORD": "abc123def.!",
        "ANTHROPIC_API_KEY": "",
        "OPENAI_API_KEY": ""
      }
    }
  }
}
```

### 3. Claude Desktop'ı Yeniden Başlat

Konfigürasyon dosyasını güncelledikten sonra Claude Desktop'ı kapatıp yeniden açın.

## 🛠️ Kullanılabilir Araçlar

### Veritabanı İşlemleri

- **get_databases**: Metabase'deki tüm veritabanlarını listele
- **get_database_tables**: Belirli bir veritabanının tablolarını ve metadata'sını getir
- **get_db_connection_info**: Veritabanı bağlantı bilgilerini al (admin gerekli)

### SQL İşlemleri

- **execute_sql**: Veritabanında native SQL sorgusu çalıştır
- **generate_sql**: Doğal dilden SQL sorgusu oluştur (AI destekli)
- **optimize_query**: SQL sorgusunu performans için optimize et (AI destekli)
- **explain_query**: SQL sorgusunun ne yaptığını açıkla (AI destekli)

### Question/Chart İşlemleri

- **create_question**: Metabase'de yeni soru/grafik oluştur
- **get_questions**: Mevcut soruları listele

### Dashboard İşlemleri

- **create_dashboard**: Yeni dashboard oluştur
- **get_dashboards**: Mevcut dashboard'ları listele

### 🆕 Direct Database Operations (Yeni!)

⚠️ **Güvenlik Özelliği**: Tüm objeler `claude_ai_` prefix'i ile oluşturulur ve sadece bu prefix'li objeler silinebilir.

#### DDL Operations
- **create_table_direct**: Doğrudan veritabanında tablo oluştur
- **create_view_direct**: Doğrudan veritabanında view oluştur  
- **create_materialized_view_direct**: Materialized view oluştur (PostgreSQL)
- **create_index_direct**: Doğrudan veritabanında index oluştur

#### DDL Reading
- **get_table_ddl**: Tablonun CREATE statement'ını al
- **get_view_ddl**: View'ın CREATE statement'ını al

#### Object Management
- **list_ai_objects**: AI tarafından oluşturulan tüm objeleri listele
- **drop_ai_object**: AI objelerini güvenli şekilde sil

#### Güvenlik Kontrolleri
- ✅ **Prefix Protection**: Sadece `claude_ai_` ile başlayan objeler
- ✅ **Approval System**: `approved: true` zorunlu
- ✅ **Dry Run**: Varsayılan olarak `dry_run: true` 
- ✅ **Operation Whitelist**: Sadece güvenli operasyonlar
- ✅ **No System Modifications**: Sistem tabloları/view'ları korunur

## 💬 Örnek Kullanım

Claude Desktop'da şu komutları deneyebilirsiniz:

### Temel Sorgular
```
"BIDB veritabanındaki tabloları göster"
"Son 30 günün satış verilerini listele"
"En çok satan ürünleri göster"
```

### AI Destekli SQL Üretimi
```
"Aylık gelir trendini gösteren bir sorgu oluştur"
"Müşteri segmentasyonu için SQL yaz"
"Top 10 müşteri listesi oluştur"
```

### Dashboard Oluşturma
```
"Satış performansı için dashboard oluştur"
"Yönetici özet raporu hazırla"
```

### Sorgu Analizi
```
"Bu SQL sorgusunu optimize et: SELECT * FROM ..."
"Bu sorgunun ne yaptığını açıkla: SELECT ..."
```

### 🆕 Direct Database Operations
```
"BIDB veritabanında müşteri_analizi adında tablo oluştur"
"sales_summary view'ını oluştur"
"performance_metrics materialized view'ı yap"
"customer_id sütununa index ekle"
"AI tarafından oluşturulan objeleri listele"
"claude_ai_test_table tablosunu sil"
```

### Güvenli DDL Workflow
```
1. "Dry run olarak test_table oluştur" (önizleme)
2. "Onaylanmış şekilde test_table oluştur" (gerçek işlem)
3. "AI objelerini listele" (kontrol)
4. "test_table tablosunun DDL'ini göster" (doğrulama)
```

## 🔧 Troubleshooting

### Bağlantı Sorunları

1. **MCP Server Çalışmıyor mu?**
   ```bash
   npm run mcp
   ```
   
2. **Environment Variables Eksik mi?**
   `.env` dosyasını kontrol edin veya konfigürasyonda `env` alanını doldurun.

3. **Metabase Erişim Sorunu?**
   ```bash
   npm start test
   ```

### Common Errors

- **"Tool not found"**: Claude Desktop'ı yeniden başlatın
- **"Authentication failed"**: Metabase credentials'ları kontrol edin
- **"AI assistant not configured"**: API anahtarlarını ekleyin

## 🎯 Özellikler

### ✅ Şu An Kullanılabilir
- Metabase API entegrasyonu
- SQL sorgu çalıştırma
- Question/Dashboard oluşturma
- AI destekli SQL üretimi
- Sorgu optimizasyonu
- Sorgu açıklama

### 🚧 Geliştirilecek
- Batch operations
- Export/Import özellikleri
- Real-time updates
- Advanced visualizations
- Custom metrics
- Automated reports

## 📖 API Referansı

Her araç için detaylı parametreler:

### get_databases
```json
// Input: Yok
// Output: Veritabanı listesi
```

### execute_sql
```json
{
  "database_id": 1,
  "sql": "SELECT * FROM table_name LIMIT 10"
}
```

### generate_sql
```json
{
  "description": "Son 30 günün satış verilerini göster",
  "database_id": 1
}
```

### create_question
```json
{
  "name": "Soru Adı",
  "description": "Açıklama",
  "database_id": 1,
  "sql": "SELECT ...",
  "collection_id": 1
}
```

## 🔗 Yararlı Linkler

- [MCP Specification](https://github.com/modelcontextprotocol/specification)
- [Claude Desktop MCP Guide](https://claude.ai/docs/mcp)
- [Metabase API Documentation](https://www.metabase.com/docs/latest/api-documentation.html)
