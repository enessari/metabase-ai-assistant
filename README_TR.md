# Metabase AI Assistant — Model Context Protocol (MCP) Sunucusu

[![npm version](https://img.shields.io/npm/v/metabase-ai-assistant.svg?style=flat-square)](https://www.npmjs.com/package/metabase-ai-assistant)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square)](https://opensource.org/licenses/Apache-2.0)
[![Node.js](https://img.shields.io/badge/Node.js-18%2B-brightgreen.svg?style=flat-square)](https://nodejs.org/)
[![MCP SDK](https://img.shields.io/badge/MCP%20SDK-v1.26.0-purple.svg?style=flat-square)](https://modelcontextprotocol.io/)

Metabase AI Assistant, Büyük Dil Modellerini (LLM), AI kodlama asistanlarını ve veri iş akışlarını Metabase İş Zekası (BI) örneğinize doğrudan bağlayan kurumsal düzeyde bir Model Context Protocol (MCP) sunucusudur.

Bünyesinde barındırdığı **143 özel araç**, yerel **dbt Semantik Katman** entegrasyonu, **Governance-First Semantik Bellek** (açık onay mekanizması, yoruma alma/arşivleme), otonom kendi kendini onaran SQL motoru, uçtan uca dashboard mimarı, proaktif anomali tespiti, sorgu indeks danışmanı ve sıfır sızıntılı PII maskeleme katmanıyla şirketinizin en güçlü veri asistanıdır.

---

## 🌍 Dil Seçenekleri / Other Languages

- 🇬🇧 **[English (Ana Dokümantasyon)](README.md)**
- 🇹🇷 **[Türkçe Dokümantasyon](README_TR.md)**
- 🇨🇳 **[中文文档 (Çince)](README_ZH.md)**
- 🇸🇦 **[التوثيق باللغة العربية (Arapça)](README_AR.md)**

---

## 📋 İçindekiler

- [Temel Mimari Özellikler](#temel-mimari-özellikler)
- [Yeni Nesil Otonom Yetenekler (v5.1)](#yeni-nesil-otonom-yetenekler-v51)
- [Metabase Sürüm Uyumluluğu](#metabase-sürüm-uyumluluğu)
- [Hızlı Başlangıç ve Kurulum](#hızlı-başlangıç-ve-kurulum)
- [Masaüstü ve İstemci Yapılandırması](#masaüstü-ve-i̇stemci-yapılandırması)
  - [Claude Desktop Entegrasyonu](#1-claude-desktop)
  - [Cursor IDE, Windsurf ve VS Code](#2-cursor-ide-windsurf-ve-vs-code)
  - [ChatGPT Custom GPTs ve Actions](#3-chatgpt-custom-gpts-ve-actions)
  - [Google Gemini ve AI Studio](#4-google-gemini-ve-ai-studio)
  - [Cloudflare Workers Dağıtımı](#5-cloudflare-workers-sunucusuz-dağıtım)
- [Araç Kategorileri (143 Araç)](#araç-kategorileri-143-araç)
- [Araç Kategorileri (152 Araç)](#araç-kategorileri-152-araç)
- [Test ve Kalite Güvencesi](#test-ve-kalite-güvencesi)
- [Geliştirme Yol Haritası (Roadmap)](ROADMAP.md)
- [Lisans](#lisans)

---

## Temel Mimari Özellikler

Metabase AI Assistant; Claude Desktop, Cursor, VS Code, ChatGPT ve Gemini gibi popüler AI istemcilerini uzman bir Metabase kullanıcısına dönüştürür:

1. **dbt Semantik Katman & Model Önceliği Bilinci**: Analiz yaparken ham/staging tablolar yerine temizlenmiş ve test edilmiş Gold Mart (`fct_`, `dim_`, `rpt_`) modellerini önceliklendirir.
2. **Denetlenebilir Semantik Bellek (Governance-First)**: Şirkete özel iş kurallarını **iki aşamalı onay mekanizmasıyla** öğrenir. Fiziksel silme yapmaz; kuralları gerekçesiyle **yoruma alır/arşivler (soft-deprecation)**.
3. **Otonom Kendi Kendini Onaran SQL Motoru (`ai_sql_execute_and_heal`)**: Sözdizimi veya şema hatalarında tablo yapısını inceleyerek sorguyu 3 döngüde otomatik düzeltir ve icra eder.
4. **Uçtan Uca Otonom Dashboard Mimarı (`ai_dashboard_build_full`)**: Tek bir doğal dil istemiyle 6-8 metrik kartı oluşturur, 24 kolonlu ızgaraya yerleştirir ve filtreleri bağlar.
5. **AI İndeks & Materialized View Danışmanı (`ai_query_index_advisor`)**: `EXPLAIN` analiz planlarını inceleyerek DBA'ler için optimize composite index ve Materialized View önerir.
6. **Proaktif KPI Anomali Tespiti (`ai_analytics_detect_anomalies`)**: Z-Score, Tukey IQR ve Bollinger Bantları ile metrik sapmalarını ve olası kök nedenlerini raporlar.
7. **Kurumsal Sıfır Sızıntı PII Maskeleme**: E-posta, telefon, TC/SSN ve kredi kartı gibi hassas verileri AI modellerine gönderilmeden önce anında maskeler.

---

## 🚀 Öne Çıkan Süper Yetenekler (v5.3 — 152 Araç)

1. 🔄 **`dbt_sync_metadata_to_metabase` (Otomatik Veri Modeli Eşitleyici)**: dbt tablo etiketlerini, zengin Türkçe alan açıklamalarını, semantik tipleri (`type/Currency`, `type/CreationDate`, `type/Category`, `type/FK`) ve yabancı anahtar (FK) ilişkilerini tek tıkla Metabase Data Model'e yazar.
2. 📊 **`dbt_sync_metrics_to_metabase` (Metabase Metrics Oluşturucu)**: dbt MetricFlow'daki tüm metrikleri Metabase resmi `/api/metric` nesneleri olarak kaydeder.
3. 🔁 **`dbt_generate_exposures_from_metabase` (Tersine Lineage / Reverse Lineage)**: Metabase panolarını ve sorularını tarayarak dbt Lineage ve dbt Docs için `models/exposures/_metabase__exposures.yml` dosyasını otomatik üretir.
4. 🤖 **`dbt_smart_create_card` (dbt Kurallarıyla Akıllı Soru / Kart Üretici)**: Şirkete özel iş kurallarını (`SemanticMemory`) sorguya enjekte eder, kendi kendini onaran (self-healing) SQL çalıştırır ve Metabase'de hazır bir Card oluşturur.
5. 🔍 **`dbt_project_scan_deep` (Derin dbt Proje & Şema Tarayıcısı)**: 9 katmanlı model hiyerarşisi (`marts_fact` > `staging`), `doc('...')` ve `catalog.json` profil çıkarma.
6. 🧬 **`dbt_lineage_joins_graph` (Cube.js Multi-Hop Joins)**: Dijkstra Min-Heap ile en kısa join rotaları ve döngüsüz DAG.
7. ⚡ **`dbt_semantic_preagg_advisor` (Cube.js Pre-Aggregation)**: 7 SQL lehçesinde Materialized View ve HLL distinct count öneri motoru.
8. 📊 **`dbt_build_dashboard_from_yaml` (Lightdash Code-as-BI)**: dbt YAML'dan Metabase'de çakışmasız 24 kolonlu yönetici panoları kurma.
9. 🔄 **`dbt_semantic_export_yaml` (Omni.co Semantik Köprü)**: Onaylanmış iş kurallarını dbt `schema.yml` formatında dışa aktarma.

---

## Yeni Nesil Otonom Yetenekler (v5.1)

### 1. dbt Katman Hiyerarşisi & Kaynak Çözümleme
$$\mathbf{Gold\;Marts\;(fct\_,\;dim\_,\;rpt\_)} \;\gg\; \mathbf{Silver\;(int\_)} \;\gg\; \mathbf{Bronze\;Staging\;(stg\_)}$$
- `dbt_inspect_models`: dbt `manifest.json` ve MetricFlow modellerini okur.
- `dbt_prioritize_sources`: Sorulan soru için en optimize Mart (`fct_`/`dim_`) tablosunu çözer.

### 2. Governance-First Semantik Bellek (Sıfır Sessiz Öğrenme, Fiziksel Silme Yok)
- `semantic_memory_propose`: Kuralı `PENDING_APPROVAL` durumunda taslak olarak sunar.
- `semantic_memory_approve`: Veri yöneticisinin açık onayıyla kuralı `ACTIVE` yapar.
- `semantic_memory_deprecate`: Kuralı silmeden gerekçesiyle arşive/yoruma alır (`DEPRECATED`).
- `semantic_memory_restore`: Arşivdeki kuralı anında geri yükler.
- `semantic_memory_list`: Kayıtlı tüm kuralları ve denetim geçmişini listeler.

---

## Metabase Sürüm Uyumluluğu

| Metabase Sürüm Aralığı | Uyumluluk Durumu | Desteklenen Başlıca Özellikler |
|---|:---:|---|
| **Metabase v0.55 – v0.61+** *(Güncel)* | **Tam Destek** | Modern MBQL 5 formatı, `/api/upload/csv`, güncel koleksiyon yetkileri, sekmeli dashboardlar |
| **Metabase v0.50 – v0.54** | **Tam Destek** | Koleksiyon ağacı (`/api/collection/tree`), Model kartları, API Key auth (`x-api-key`), parametrik sorgular |
| **Metabase v0.43 – v0.49** | **Tam Destek** | Oturum jetonu doğrulaması (`X-Metabase-Session`), klasik MBQL sorgu boru hatları |
| **Metabase Open Source & Enterprise** | **Tam Destek** | Otomatik özellik algılama (denetim kayıtları, granüler izinler, whitelabeling) |

---

## Hızlı Başlangıç ve Kurulum

### NPX ile Doğrudan Çalıştırma

```bash
npx metabase-ai-assistant
```

### NPM ile Global Kurulum

```bash
npm install -g metabase-ai-assistant
```

---

## Masaüstü ve İstemci Yapılandırması

### 1. Claude Desktop

`claude_desktop_config.json` dosyanıza şu bloğu ekleyin:
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://metabase.sirketiniz.com",
        "METABASE_API_KEY": "mb_api_anahtariniz",
        "METABASE_READ_ONLY_MODE": "true"
      }
    }
  }
}
```

### 2. Cursor IDE, Windsurf ve VS Code

`.cursor/mcp.json` veya VS Code MCP yapılandırmasına ekleyin:

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://metabase.sirketiniz.com",
        "METABASE_API_KEY": "mb_api_anahtariniz",
        "METABASE_READ_ONLY_MODE": "true"
      }
    }
  }
}
```

### 3. ChatGPT Custom GPTs ve Actions

1. Yerel SSE sunucusunu başlatın: `npm run start:sse`
2. ChatGPT Custom GPT ayarlarından `https://domaininiz.com/tools/openapi.json` adresini içe aktarın.
3. Detaylı rehber: [docs/integrations/CHATGPT_ACTIONS_GUIDE.md](docs/integrations/CHATGPT_ACTIONS_GUIDE.md)

### 4. Google Gemini ve AI Studio

- Detaylı entegrasyon rehberi: [docs/integrations/GOOGLE_GEMINI_GUIDE.md](docs/integrations/GOOGLE_GEMINI_GUIDE.md)

### 5. Cloudflare Workers (Sunucusuz Dağıtım)

```bash
cd deploy/cloudflare
npx wrangler deploy
```

---

## Araç Kategorileri (143 Araç)

1. **dbt ve Semantik Katman (6 araç)**: Model hiyerarşisi inceleme, soy ağacı analizi, kaynak önceliklendirme, onaylı semantik bellek.
2. **Otonom Yapay Zeka İşlemleri (4 araç)**: Kendi kendini onaran SQL, dashboard mimarı, indeks danışmanı, anomali tespiti.
3. **SQL ve Sorgu İcrası (14 araç)**: Doğrudan SQL, asenkron işler, sorgu durumu, hız testleri.
4. **AI Sorgu Analitiği (6 araç)**: Doğal dilden SQL üretimi, optimizasyon, sorgu açıklaması.
5. **Kartlar ve Görselleştirmeler (34 araç)**: Soru oluşturma, parametrik filtreler, kart kopyalama.
6. **Dashboardlar ve Yerleşim (22 araç)**: Pano oluşturma, ızgara dizilimi, filtre bağlama, şablonlar.
7. **Koleksiyonlar (8 araç)**: Klasör ağacı, taşıma, yetki grafiği.
8. **Şema ve Veri Modelleme (18 araç)**: Şema keşfi, sanal ilişkiler, profil çıkarma.
9. **Kullanıcı ve İzin Yönetimi (12 araç)**: Kullanıcı daveti, grup atama, üyelik kontrolü.
10. **Aksiyonlar ve Dokümantasyon (19 araç)**: Metabase aksiyonları, uyarılar, metrikler, çalışma alanı yedekleme/taşıma.

---

## Test ve Kalite Güvencesi

```bash
# Tüm test paketlerini koştur (32 suite, 583 test - %100 Başarı)
npm test

# Sadece Güvenlik ve PII testleri
npm run test:security
```

---

## Lisans

Bu proje **Apache License 2.0** ile lisanslanmıştır. Detaylar için [LICENSE](LICENSE) dosyasına bakabilirsiniz.

Geliştirici: **Abdullah Enes SARI** ([ONMARTECH LLC](https://github.com/enessari)).
