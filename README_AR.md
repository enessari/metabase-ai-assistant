# المساعد الذكي لميتالبيس (Metabase AI Assistant) — خادم بروتوكول سياق النموذج (MCP)

[![npm version](https://img.shields.io/npm/v/metabase-ai-assistant.svg?style=flat-square)](https://www.npmjs.com/package/metabase-ai-assistant)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square)](https://opensource.org/licenses/Apache-2.0)
[![Node.js](https://img.shields.io/badge/Node.js-18%2B-brightgreen.svg?style=flat-square)](https://nodejs.org/)
[![MCP SDK](https://img.shields.io/badge/MCP%20SDK-v1.26.0-purple.svg?style=flat-square)](https://modelcontextprotocol.io/)

يعد Metabase AI Assistant خادمًا متقدمًا وفق معيار بروتوكول سياق النموذج (MCP) المخصص للمؤسسات، حيث يربط نماذج الذكاء الاصطناعي التوليدي الكبيرة (LLMs) ومساعدات البرمجة مباشرة بنظام ذكاء الأعمال Metabase.

يتميز بـ **143 أداة متخصصة**، مع دعم أصيل لطبقة المعاني والبيانات **dbt Semantic Layer**، وذاكرة دلالية خاضعة للحوكمة الصارمة (**Governance-First Memory**)، ومحرك تصحيح ذاتي لاستعلامات SQL، وبناء تلقائي للوحات التحكم، ورصد استباقي للشذوذ الإحصائي، وإخفاء تلقائي للبيانات الحساسة والشخصية (PII Masking).

---

## 🌍 اللغات المتاحة / Other Languages

- 🇬🇧 **[English (الوثيقة الرئيسية)](README.md)**
- 🇹🇷 **[Türkçe Dokümantasyon (التركية)](README_TR.md)**
- 🇨🇳 **[中文文档 (الصينية)](README_ZH.md)**
- 🇸🇦 **[التوثيق باللغة العربية (Arabic)](README_AR.md)**

---

## أبرز الميزات الهندسية

1. **الوعي المعماري بطبقات dbt**: يعطي الأولوية لجداول الحقائق والأبعاد المعتمدة (`fct_`, `dim_`, `rpt_`) على حساب الجداول الأولية غير المعالجة.
2. **محرك الذاكرة الدلالية الخاضع للحوكمة**: يتعلم قواعد العمل الخاصة بالمؤسسة عبر **مسار تدقيق وموافقة صريحة**، مع اعتماد الأرشفة الآمنة والتعطيل المؤقت دون أي حذف نهائي للبيانات.
3. **محرك الاستعلامات ذاتي الشفاء (`ai_sql_execute_and_heal`)**: يعالج أخطاء بناء الجمل وأسماء الأعمدة تلقائيًا ويعيد تنفيذ الاستعلام بنجاح.
4. **مهندس لوحات التحكم المستقل (`ai_dashboard_build_full`)**: ينشئ من 6 إلى 8 بطاقات مؤشرات أداء رئيسية ويوزعها عبر شبكة متجاوبة من 24 عمودًا ويربط الفلاتر تلقائيًا.
5. **مستشار فهارس وقواعد البيانات (`ai_query_index_advisor`)**: يحلل خطط الاستعلام ويوصي بفهارس مركبة لتحسين الأداء.
6. **كاشف الشذوذ الإحصائي الاستباقي (`ai_analytics_detect_anomalies`)**: يستخدم نماذج Z-Score و Bollinger Bands لرصد التغيرات المفاجئة في المؤشرات وتحديد أسبابها.
7. **نظام حجب البيانات الحساسة (PII Masking)**: يقوم بتشفير وحجب البريد الإلكتروني والهواتف وأرقام البطاقات قبل إرسال البيانات لسياق الذكاء الاصطناعي.

---

## التثبيت السريع

### التشغيل عبر NPX

```bash
npx metabase-ai-assistant
```

### إعداد تطبيق Claude Desktop

أضف الإعدادات التالية إلى ملف `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["-y", "metabase-ai-assistant"],
      "env": {
        "METABASE_URL": "https://your-metabase.com",
        "METABASE_API_KEY": "mb_your_api_key_here",
        "METABASE_READ_ONLY_MODE": "true"
      }
    }
  }
}
```

---

## الاختبارات وضمان الجودة

```bash
# تشغيل جميع الاختبارات المؤتمتة (32 حزمة، 583 اختبار بنجاح 100%)
npm test

# تشغيل اختبارات الأمان وحجب البيانات الحساسة
npm run test:security
```

---

## الترخيص

البرنامج مرخص بموجب **Apache License 2.0**. راجع ملف [LICENSE](LICENSE) للمزيد من التفاصيل.
