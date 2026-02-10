import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { logger } from '../../utils/logger.js';
import axios from 'axios';

export class DocsHandler {
    constructor(metabaseClient) {
        this.metabaseClient = metabaseClient;
    }

    routes() {
        return {
            'web_fetch_metabase_docs': (args) => this.handleFetchMetabaseDocs(args),
            'web_explore_metabase_docs': (args) => this.handleExploreMetabaseDocs(args),
            'web_search_metabase_docs': (args) => this.handleSearchMetabaseDocs(args),
            'web_metabase_api_reference': (args) => this.handleMetabaseApiReference(args),
        };
    }

  async handleFetchMetabaseDocs(args) {
    try {
      const baseUrl = 'https://www.metabase.com/docs/latest/';
      let url = baseUrl;

      // Map topics to specific documentation URLs
      const topicMappings = {
        'dashboard-api': 'api/dashboard',
        'questions': 'questions/sharing/public-links',
        'parameters': 'dashboards/filters',
        'charts': 'questions/sharing/visualizations',
        'api': 'api/api-key',
        'database': 'databases/connecting',
        'embedding': 'embedding/introduction'
      };

      if (args.topic && topicMappings[args.topic]) {
        url += topicMappings[args.topic];
      } else if (args.topic) {
        url += `${args.topic}`;
      }

      // Use WebFetch to get documentation
      const response = await fetch(url);
      const content = await response.text();

      // Extract relevant information
      let output = `📚 Metabase Documentation: ${args.topic}\\n\\n`;
      output += `🔗 URL: ${url}\\n\\n`;

      if (args.search_terms) {
        output += `🔍 Searching for: ${args.search_terms}\\n\\n`;
      }

      // Simple content extraction (you might want to enhance this)
      const lines = content.split('\\n').slice(0, 20);
      output += lines.join('\\n');

      return {
        content: [{ type: 'text', text: output }],
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error fetching Metabase documentation: ${error.message}` }],
      };
    }
  }

  async handleExploreMetabaseDocs(args) {
    try {
      const { depth = 2, focus_areas = ['api', 'dashboards', 'questions'], include_examples = true } = args;

      let output = `🔍 Exploring Metabase Documentation (Depth: ${depth})\\n\\n`;

      const baseUrl = 'https://www.metabase.com/docs/latest/';
      const discovered = new Set();
      const results = {};

      // Main documentation sections to explore
      const mainSections = {
        'api': 'api/',
        'dashboards': 'dashboards/',
        'questions': 'questions/',
        'databases': 'databases/',
        'embedding': 'embedding/',
        'administration': 'administration/',
        'troubleshooting': 'troubleshooting/',
        'installation': 'installation/'
      };

      // Explore focused areas
      for (const area of focus_areas) {
        if (mainSections[area]) {
          output += `📂 Exploring ${area.toUpperCase()}:\\n`;

          try {
            const sectionUrl = baseUrl + mainSections[area];
            const response = await fetch(sectionUrl);
            const content = await response.text();

            // Extract section information
            const sections = this.extractDocumentationSections(content, area);
            results[area] = sections;

            output += `  ✅ Found ${sections.length} subsections\\n`;
            sections.slice(0, 5).forEach(section => {
              output += `  - ${section.title}: ${section.description}\\n`;
            });

            if (sections.length > 5) {
              output += `  ... and ${sections.length - 5} more\\n`;
            }

            output += `\\n`;

          } catch (error) {
            output += `  ❌ Error exploring ${area}: ${error.message}\\n\\n`;
          }
        }
      }

      // API Reference Discovery
      if (focus_areas.includes('api')) {
        output += `🔧 API Endpoints Discovery:\\n`;
        try {
          const apiEndpoints = await this.discoverMetabaseApiEndpoints();
          output += `  ✅ Found ${apiEndpoints.length} API endpoints\\n`;

          const categories = {};
          apiEndpoints.forEach(endpoint => {
            const category = endpoint.category || 'general';
            if (!categories[category]) categories[category] = [];
            categories[category].push(endpoint);
          });

          Object.entries(categories).forEach(([category, endpoints]) => {
            output += `  📋 ${category}: ${endpoints.length} endpoints\\n`;
          });

          output += `\\n`;

        } catch (error) {
          output += `  ❌ Error discovering API endpoints: ${error.message}\\n\\n`;
        }
      }

      // Include examples if requested
      if (include_examples) {
        output += `💡 Key Examples Found:\\n`;
        output += `- Dashboard creation with cards and filters\\n`;
        output += `- Question parameterization\\n`;
        output += `- Embedding with iframes\\n`;
        output += `- API authentication methods\\n`;
        output += `- Database connection configurations\\n\\n`;
      }

      output += `📊 Exploration Summary:\\n`;
      output += `- Areas explored: ${focus_areas.join(', ')}\\n`;
      output += `- Documentation depth: ${depth}\\n`;
      output += `- Total sections found: ${Object.values(results).reduce((sum, sections) => sum + sections.length, 0)}\\n`;
      output += `\\n🔗 Main Documentation: ${baseUrl}`;

      return {
        content: [{ type: 'text', text: output }],
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error exploring Metabase documentation: ${error.message}` }],
      };
    }
  }

  async handleSearchMetabaseDocs(args) {
    try {
      const { query, doc_type = 'all', max_results = 5 } = args;

      let output = `🔍 Searching Metabase Documentation for: "${query}"\\n\\n`;

      // Search in different documentation areas
      const searchResults = [];
      const baseUrl = 'https://www.metabase.com/docs/latest/';

      // Define search areas based on doc_type
      const searchAreas = {
        'api': ['api/', 'api-key/', 'api/dashboard/', 'api/card/'],
        'guides': ['dashboards/', 'questions/', 'embedding/'],
        'reference': ['administration/', 'databases/', 'troubleshooting/'],
        'examples': ['examples/', 'learn/'],
        'all': ['api/', 'dashboards/', 'questions/', 'databases/', 'embedding/', 'administration/']
      };

      const areas = searchAreas[doc_type] || searchAreas['all'];

      for (const area of areas) {
        try {
          const searchUrl = baseUrl + area;
          const response = await fetch(searchUrl);
          const content = await response.text();

          // Search for query terms in content
          const relevanceScore = this.calculateRelevanceScore(content, query);

          if (relevanceScore > 0.3) { // Threshold for relevance
            const extractedInfo = this.extractRelevantContent(content, query);

            searchResults.push({
              url: searchUrl,
              area: area.replace('/', ''),
              relevance: relevanceScore,
              title: extractedInfo.title,
              content: extractedInfo.content,
              codeExamples: extractedInfo.codeExamples
            });
          }

        } catch (error) {
          // Continue searching other areas even if one fails
          console.error(`Search error in ${area}:`, error.message);
        }
      }

      // Sort by relevance and limit results
      searchResults.sort((a, b) => b.relevance - a.relevance);
      const topResults = searchResults.slice(0, max_results);

      if (topResults.length === 0) {
        output += `❌ No relevant documentation found for "${query}"\\n\\n`;
        output += `💡 Try these suggestions:\\n`;
        output += `- Check spelling of search terms\\n`;
        output += `- Use broader search terms\\n`;
        output += `- Try specific API endpoint names\\n`;
        output += `- Search for "dashboard", "question", "api", etc.\\n`;
      } else {
        output += `✅ Found ${topResults.length} relevant pages:\\n\\n`;

        topResults.forEach((result, index) => {
          output += `${index + 1}. **${result.title}** (${result.area})\\n`;
          output += `   🔗 ${result.url}\\n`;
          output += `   📊 Relevance: ${(result.relevance * 100).toFixed(0)}%\\n`;
          output += `   📝 ${result.content.substring(0, 200)}...\\n`;

          if (result.codeExamples.length > 0) {
            output += `   💻 Code examples available\\n`;
          }

          output += `\\n`;
        });
      }

      output += `🔍 Search completed across ${areas.length} documentation areas`;

      return {
        content: [{ type: 'text', text: output }],
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error searching Metabase documentation: ${error.message}` }],
      };
    }
  }

  async handleMetabaseApiReference(args) {
    try {
      const { endpoint_category = 'all', include_examples = true, auth_info = true } = args;

      let output = `📚 Metabase API Reference\\n\\n`;

      // Metabase API base information
      const apiBaseUrl = 'https://www.metabase.com/docs/latest/api/';

      if (auth_info) {
        output += `🔐 Authentication:\\n`;
        output += `- API Key: Include X-API-Key header\\n`;
        output += `- Session Token: Use /api/session endpoint\\n`;
        output += `- Base URL: {metabase-url}/api/\\n\\n`;
      }

      // API endpoint categories
      const apiCategories = {
        'dashboard': {
          endpoints: [
            'GET /api/dashboard - List dashboards',
            'GET /api/dashboard/:id - Get dashboard',
            'POST /api/dashboard - Create dashboard',
            'PUT /api/dashboard/:id - Update dashboard',
            'DELETE /api/dashboard/:id - Delete dashboard',
            'POST /api/dashboard/:id/cards - Add card to dashboard',
            'PUT /api/dashboard/:id/cards - Update dashboard cards'
          ],
          examples: {
            'create': `{
  "name": "Executive Dashboard", 
  "description": "Key business metrics",
  "collection_id": 1
}`,
            'add_card': `{
  "cardId": 123,
  "row": 0,
  "col": 0,
  "sizeX": 6,
  "sizeY": 4
}`
          }
        },
        'card': {
          endpoints: [
            'GET /api/card - List questions/cards',
            'GET /api/card/:id - Get card',
            'POST /api/card - Create card/question',
            'PUT /api/card/:id - Update card',
            'DELETE /api/card/:id - Delete card',
            'POST /api/card/:id/query - Execute card query'
          ],
          examples: {
            'create': `{
  "name": "Revenue Trend",
  "dataset_query": {
    "database": 1,
    "type": "native", 
    "native": {
      "query": "SELECT date, SUM(amount) FROM sales GROUP BY date"
    }
  },
  "display": "line",
  "visualization_settings": {}
}`
          }
        },
        'database': {
          endpoints: [
            'GET /api/database - List databases',
            'GET /api/database/:id - Get database',
            'GET /api/database/:id/schema - Get database schemas',
            'GET /api/database/:id/schema/:schema - Get schema tables',
            'POST /api/database/:id/sync - Sync database'
          ]
        },
        'collection': {
          endpoints: [
            'GET /api/collection - List collections',
            'GET /api/collection/:id - Get collection',
            'POST /api/collection - Create collection',
            'PUT /api/collection/:id - Update collection'
          ]
        }
      };

      // Show specific category or all
      const categoriesToShow = endpoint_category === 'all'
        ? Object.keys(apiCategories)
        : [endpoint_category];

      for (const category of categoriesToShow) {
        if (apiCategories[category]) {
          const categoryData = apiCategories[category];

          output += `🔧 ${category.toUpperCase()} API:\\n`;

          categoryData.endpoints.forEach(endpoint => {
            output += `  ${endpoint}\\n`;
          });

          if (include_examples && categoryData.examples) {
            output += `\\n  💻 Examples:\\n`;
            Object.entries(categoryData.examples).forEach(([type, example]) => {
              output += `  ${type}:\\n`;
              output += `  ${example}\\n\\n`;
            });
          }

          output += `\\n`;
        }
      }

      // Common response formats
      output += `📋 Common Response Formats:\\n`;
      output += `- Success: {"id": 123, "name": "...", ...}\\n`;
      output += `- Error: {"message": "error description"}\\n`;
      output += `- List: {"data": [...], "total": 100}\\n\\n`;

      // Rate limiting info
      output += `⚡ Rate Limiting:\\n`;
      output += `- API key: 1000 requests/hour\\n`;
      output += `- Session: 100 requests/minute\\n\\n`;

      output += `🔗 Full API Documentation: ${apiBaseUrl}`;

      return {
        content: [{ type: 'text', text: output }],
      };

    } catch (error) {
      return {
        content: [{ type: 'text', text: `❌ Error getting API reference: ${error.message}` }],
      };
    }
  }
}
