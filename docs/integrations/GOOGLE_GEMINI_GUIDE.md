# ♊ Google Gemini & Google AI Studio Integration Guide

Connect **Metabase AI Assistant** to **Google Gemini (Gemini 1.5 Pro / 2.0 Flash)** using native Function Calling.

---

## 🚀 Option 1: Direct Python / Node.js SDK with Gemini Function Calling

You can supply Metabase MCP tool definitions directly to Google Gemini's `@google/genai` or `google-generativeai` SDKs.

### Python Example with Google GenAI SDK

```python
import os
import requests
from google import genai
from google.genai import types

# 1. Fetch available tools from Metabase Assistant
tools_response = requests.get("http://localhost:3000/tools/openapi.json").json()

# 2. Initialize Gemini Client
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

# 3. Chat with Function Calling
response = client.models.generate_content(
    model="gemini-2.0-flash",
    contents="Show me the list of database tables in Metabase database ID 1",
    config=types.GenerateContentConfig(
        tools=[{
            "function_declarations": [
                {
                    "name": "db_tables",
                    "description": "Get all tables in a Metabase database",
                    "parameters": {
                        "type": "OBJECT",
                        "properties": {
                            "database_id": {"type": "NUMBER", "description": "Database ID"}
                        },
                        "required": ["database_id"]
                    }
                }
            ]
        }]
    )
)

print(response.text)
```

---

## 🚀 Option 2: Google AI Studio

1. Open [Google AI Studio](https://aistudio.google.com).
2. Under **Model Parameters**, enable **Function Calling (Tools)**.
3. Import your OpenAPI tool schema from `http://your-server-domain.com/tools/openapi.json`.
4. Gemini will automatically call the appropriate tool when you ask natural language database questions!
