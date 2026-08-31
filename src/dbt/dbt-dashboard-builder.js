/**
 * src/dbt/dbt-dashboard-builder.js
 * Lightdash Code-as-BI Dashboard Builder & 24-Column Grid Architect
 *
 * Translates dbt YAML metric definitions, exposures.yml, and meta.metabase / meta.lightdash
 * visual properties into native Metabase Dashboards, Questions, and Model cards (v50+).
 */

import fs from 'fs';
import path from 'path';
import yaml from 'js-yaml';
import { logger } from '../utils/logger.js';
import { DbtDeepScanner } from './dbt-deep-scanner.js';
import {
  calculate24ColGridPositions,
  validateNoCollisions,
  generateFilterMappings,
  GRID_WIDTH,
  CARD_ARCHETYPES,
  DISPLAY_DIMENSIONS,
  getCardDimensionsAndArchetype,
  getDefaultVisualizationSettings,
} from '../analytics/dashboard-architect.js';
import { isPiiMaskingEnabled, maskString } from '../utils/pii-masker.js';

export const THEME_PALETTES = {
  executive: ['#2563EB', '#3B82F6', '#60A5FA', '#93C5FD', '#1D4ED8', '#1E40AF', '#047857', '#F59E0B'],
  modern_emerald: ['#059669', '#10B981', '#34D399', '#6EE7B7', '#047857', '#065F46', '#2563EB', '#D97706'],
  indigo_violet: ['#6366F1', '#8B5CF6', '#A78BFA', '#C4B5FD', '#4F46E5', '#4338CA', '#06B6D4', '#EC4899'],
  amber_warm: ['#D97706', '#F59E0B', '#FBBF24', '#FDE68A', '#B45309', '#92400E', '#2563EB', '#10B981'],
  slate_minimal: ['#475569', '#64748B', '#94A3B8', '#CBD5E1', '#334155', '#1E293B', '#3B82F6', '#10B981'],
  financial: ['#1E3A8A', '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#6366F1', '#8B5CF6', '#14B8A6'],
  operational: ['#0F766E', '#14B8A6', '#2DD4BF', '#065F46', '#2563EB', '#D97706', '#DC2626', '#475569'],
  marketing: ['#EC4899', '#F43F5E', '#8B5CF6', '#6366F1', '#3B82F6', '#10B981', '#F59E0B', '#F97316'],
  dark: ['#60A5FA', '#34D399', '#FBBF24', '#F87171', '#A78BFA', '#38BDF8', '#4ADE80', '#FB923C'],
  custom: ['#2563EB', '#3B82F6', '#60A5FA', '#93C5FD', '#1D4ED8', '#1E40AF', '#047857', '#F59E0B'],
};

export class DbtDashboardBuilder {
  /**
   * @param {object} client Metabase API client instance (or mock)
   * @param {object} options Options including scanner, assistant, theme, projectDir
   */
  constructor(client, options = {}) {
    this.client = client;
    this.scanner = options.scanner || new DbtDeepScanner({ projectDir: options.projectDir });
    this.theme = options.theme || 'executive';
    this.assistant = options.assistant || null;
  }

  /**
   * Parse exposure YAML string or file path
   * @param {string|object} yamlContentOrFile
   * @returns {object} Normalized dashboard/exposure spec
   */
  parseExposureYaml(yamlContentOrFile) {
    if (!yamlContentOrFile) {
      throw new Error('YAML content or file path must be provided');
    }

    let rawYaml = yamlContentOrFile;
    if (typeof yamlContentOrFile === 'string' && fs.existsSync(yamlContentOrFile)) {
      try {
        rawYaml = fs.readFileSync(yamlContentOrFile, 'utf8');
      } catch (err) {
        throw new Error(`Failed to read YAML file at ${yamlContentOrFile}: ${err.message}`);
      }
    }

    return this.parseYamlDashboardSpec(rawYaml);
  }

  /**
   * Parse raw YAML string or object into normalized dashboard specification
   * @param {string|object} rawYaml
   * @returns {object} Normalized specification
   */
  parseYamlDashboardSpec(rawYaml) {
    let content;
    if (typeof rawYaml === 'string') {
      try {
        // Sanitize Jinja template tags (doc, ref, source, etc.) for valid YAML parsing
        const sanitized = rawYaml
          .replace(/:\s*(\{\{[\s\S]*?\}\})/g, (match, p1) => {
            const escaped = p1.replace(/"/g, "'");
            return `: "${escaped}"`;
          })
          .replace(/:\s*(\{%[\s\S]*?%\})/g, (match, p1) => {
            const escaped = p1.replace(/"/g, "'");
            return `: "${escaped}"`;
          });
        content = yaml.load(sanitized);
      } catch (err) {
        throw new Error(`Failed to parse YAML dashboard spec: ${err.message}`);
      }
    } else if (typeof rawYaml === 'object' && rawYaml !== null) {
      content = rawYaml;
    } else {
      throw new Error('YAML content must be a string or object');
    }

    if (!content || typeof content !== 'object') {
      throw new Error('Parsed YAML content is empty or not an object');
    }

    // Support multiple top-level keys (exposures, dashboard, models, metrics)
    if (Array.isArray(content.exposures) && content.exposures.length > 0) {
      return this.normalizeExposureSpec(content.exposures[0]);
    }
    if (content.dashboard) {
      return this.normalizeDashboardSpec(content.dashboard);
    }
    if (content.name && (content.cards || content.tiles || content.metrics || content.type === 'dashboard')) {
      return this.normalizeDashboardSpec(content);
    }
    if (Array.isArray(content.models) && content.models.length > 0) {
      return this.normalizeModelDashboardSpec(content.models[0]);
    }
    if (Array.isArray(content.metrics) && content.metrics.length > 0) {
      return this.normalizeMetricsDashboardSpec(content.metrics);
    }

    return this.normalizeDashboardSpec(content);
  }

  /**
   * Normalize an exposure definition from exposures.yml
   */
  normalizeExposureSpec(exp) {
    const meta = exp.meta || {};
    const mbMeta = meta.metabase || {};
    const ldMeta = meta.lightdash || {};

    const rawCards = mbMeta.cards || mbMeta.tiles || ldMeta.tiles || meta.tiles || meta.cards || exp.cards || exp.tiles || [];

    return {
      name: exp.label || exp.name || 'dbt Exposure Dashboard',
      slug: exp.name || 'exposure_dashboard',
      description: exp.description || '',
      owner: exp.owner || {},
      type: exp.type || 'dashboard',
      maturity: exp.maturity || 'high',
      depends_on: Array.isArray(exp.depends_on) ? exp.depends_on : [],
      theme: mbMeta.theme || ldMeta.theme || meta.theme || this.theme,
      filters: mbMeta.filters || ldMeta.filters || meta.filters || exp.filters || [],
      cards: rawCards,
      meta,
    };
  }

  /**
   * Normalize a direct dashboard definition
   */
  normalizeDashboardSpec(dash) {
    const meta = dash.meta || {};
    const mb = meta.metabase || {};
    const ld = meta.lightdash || {};

    const rawCards = dash.cards || dash.tiles || mb.cards || mb.tiles || ld.tiles || [];

    return {
      name: dash.label || dash.name || dash.dashboard_name || 'dbt Semantic Dashboard',
      slug: dash.name || 'dbt_dashboard',
      description: dash.description || dash.dashboard_description || '',
      theme: dash.theme || mb.theme || ld.theme || this.theme,
      filters: dash.filters || mb.filters || ld.filters || [],
      cards: rawCards,
      meta,
    };
  }

  /**
   * Normalize a model definition into an automatic dashboard specification
   */
  normalizeModelDashboardSpec(modelDef) {
    const meta = modelDef.meta || {};
    const mb = meta.metabase || {};
    const ld = meta.lightdash || {};

    return {
      name: mb.dashboard_name || ld.dashboard_name || `${modelDef.name} Executive Dashboard`,
      slug: modelDef.name,
      description: modelDef.description || `Autonomous executive dashboard for dbt model ${modelDef.name}`,
      theme: mb.theme || ld.theme || this.theme,
      model: modelDef,
      resolvedModel: modelDef,
      filters: mb.filters || ld.filters || [
        { name: 'Date Range', slug: 'date_range', type: 'date/all-options', target_variable: 'date_range' },
      ],
      cards: mb.cards || ld.cards || [],
      meta,
    };
  }

  /**
   * Normalize a list of metrics into a dashboard specification
   */
  normalizeMetricsDashboardSpec(metricsList) {
    return {
      name: 'dbt Metrics Executive Dashboard',
      slug: 'metrics_dashboard',
      description: 'Autonomous executive dashboard covering dbt metrics & KPIs',
      theme: this.theme,
      metrics: metricsList,
      filters: [
        { name: 'Date Range', slug: 'date_range', type: 'date/all-options', target_variable: 'date_range' },
      ],
      cards: [],
      meta: {},
    };
  }

  /**
   * Calculate 24-column grid coordinates ensuring zero collisions
   * @param {Array<object>} cards
   * @param {object} options
   * @returns {Array<{ row: number, col: number, size_x: number, size_y: number }>}
   */
  calculateGridCoordinates(cards, options = {}) {
    return calculate24ColGridPositions(cards, options);
  }

  /**
   * Generate parameter mappings connecting dashboard filters to card template tags
   * @param {Array<object>} cards
   * @param {Array<object>} filters
   * @returns {Array<Array<object>>}
   */
  generateFilterMappings(cards, filters = []) {
    return generateFilterMappings(cards, filters);
  }

  /**
   * Build Metabase dataset_query template-tags definition for SQL query
   * @param {string} sql
   * @param {Array<object>} filters
   * @returns {object}
   */
  buildTemplateTags(sql, filters = []) {
    const templateTags = {};
    if (!sql || typeof sql !== 'string') return templateTags;

    const matches = sql.match(/\{\{([a-zA-Z0-9_]+)\}\}/g) || [];
    for (const match of matches) {
      const tag = match.replace(/[{}]/g, '');
      const matchedFilter = filters.find(
        f => f.slug === tag || f.target_variable === tag || (f.name && f.name.toLowerCase().replace(/\s+/g, '_') === tag)
      );

      let tagType = 'text';
      if (matchedFilter) {
        if (matchedFilter.type && matchedFilter.type.startsWith('date')) {
          tagType = 'date';
        } else if (matchedFilter.type === 'number/=') {
          tagType = 'number';
        } else {
          tagType = 'dimension';
        }
      } else if (tag.includes('date') || tag.includes('time')) {
        tagType = 'date';
      }

      templateTags[tag] = {
        id: tag,
        name: tag,
        'display-name': tag.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        type: tagType,
      };
    }

    return templateTags;
  }

  /**
   * Format visualization settings with color palettes, formatting, labels, etc.
   * @param {object} card
   * @param {string} themeName
   * @param {object} visualMeta
   * @returns {object}
   */
  formatVisualSettings(card, themeName = 'executive', visualMeta = {}) {
    const display = (card.display || 'table').toLowerCase();
    const defaults = getDefaultVisualizationSettings(display);
    const palette = THEME_PALETTES[themeName] || THEME_PALETTES.executive;

    const settings = {
      ...defaults,
      ...(card.visualization_settings || {}),
      ...(card.visual_settings || {}),
    };

    // Color theme
    if (!settings['graph.colors']) {
      settings['graph.colors'] = palette;
    }

    // Line / Area chart timeseries settings
    if (['line', 'area'].includes(display)) {
      settings['graph.show_values'] = settings['graph.show_values'] !== undefined ? settings['graph.show_values'] : true;
      settings['graph.x_axis.scale'] = settings['graph.x_axis.scale'] || 'timeseries';
    }

    // Bar / Row chart settings
    if (['bar', 'row'].includes(display)) {
      settings['graph.show_values'] = settings['graph.show_values'] !== undefined ? settings['graph.show_values'] : true;
    }

    // Pie / Donut settings
    if (['pie', 'donut'].includes(display)) {
      settings['pie.show_legend'] = settings['pie.show_legend'] !== undefined ? settings['pie.show_legend'] : true;
      settings['pie.percent_visibility'] = settings['pie.percent_visibility'] || 'inside';
    }

    // Apply meta.metabase / meta.lightdash formatting if present
    const formatting = visualMeta.formatting || {};
    if (formatting.currency || formatting.formatType === 'currency') {
      settings['column_settings'] = settings['column_settings'] || {};
      const prefix = formatting.prefix || (formatting.currency === 'EUR' ? '€' : '$');
      settings['scalar.currency'] = formatting.currency || 'USD';
      settings['scalar.prefix'] = prefix;
    } else if (formatting.formatType === 'percent') {
      settings['scalar.suffix'] = '%';
      if (formatting.decimals !== null && formatting.decimals !== undefined) {
        settings['scalar.decimals'] = formatting.decimals;
      }
    }

    if (formatting.decimals !== null && formatting.decimals !== undefined) {
      settings['scalar.decimals'] = formatting.decimals;
    }

    if (visualMeta.color) {
      settings['graph.colors'] = [visualMeta.color, ...palette.filter(c => c !== visualMeta.color)];
    }

    return settings;
  }

  /**
   * Resolves and enriches cards from specification, ensuring minimum 4 cards with executive hierarchy.
   * Executive Visual Hierarchy:
   * 1. Top Tier (Row 0): KPI Numbers / Gauges (size 6x4)
   * 2. Mid Tier (Row 4): Trend Line / Area Charts (size 12x8)
   * 3. Breakdown Tier (Row 12): Categorical Breakdown Bar / Pie / Row (size 12x6 or 8x6)
   * 4. Detail Tier (Row 18): Detail Tables / Metabase Model Cards (size 24x8)
   */
  async resolveAndEnrichCards(spec, databaseId, themeName, options = {}) {
    const enrichedCards = [];
    const includeModels = options.include_models !== false && options.includeModels !== false;
    const specifiedCards = spec.cards || [];

    // 1. Ingest explicitly defined cards from spec
    for (let i = 0; i < specifiedCards.length; i++) {
      const rawCard = specifiedCards[i];
      const display = (rawCard.display || rawCard.type || 'table').toLowerCase();
      const dims = getCardDimensionsAndArchetype(display);

      const cardName = rawCard.name || rawCard.label || rawCard.title || `Card ${i + 1}`;
      const cardSql = rawCard.sql || rawCard.query || (spec.resolvedModel ? `SELECT * FROM ${spec.resolvedModel.name} LIMIT 100` : `SELECT 1 AS val`);
      const visualSettings = this.formatVisualSettings(rawCard, themeName, rawCard.meta || {});

      enrichedCards.push({
        name: cardName,
        display: dims.archetype === CARD_ARCHETYPES.TABLE && rawCard.type === 'model' ? 'table' : display,
        type: rawCard.type || (rawCard.is_model ? 'model' : 'question'),
        is_model: Boolean(rawCard.is_model || rawCard.type === 'model'),
        sql: cardSql,
        description: rawCard.description || `${cardName} for ${spec.name}`,
        row: rawCard.row,
        col: rawCard.col,
        size_x: rawCard.size_x || rawCard.sizeX || dims.size_x,
        size_y: rawCard.size_y || rawCard.sizeY || dims.size_y,
        visualization_settings: visualSettings,
        parameter_name: rawCard.parameter_name || rawCard.target_variable || null,
        target_variable: rawCard.target_variable || null,
      });
    }

    // 2. If fewer than 4 cards, synthesize complementary executive cards from model / metrics
    if (enrichedCards.length < 4) {
      const model = spec.resolvedModel || (spec.model ? spec.model : null);
      const modelName = model ? model.name : (spec.slug || 'marts_orders');
      const columns = (model && model.columns) ? Object.values(model.columns) : [];

      // Detect date, numeric, and categorical columns
      const dateCol = columns.find(c => {
        const t = (c.dataType || c.type || '').toLowerCase();
        const n = c.name.toLowerCase();
        return t.includes('date') || t.includes('time') || n.includes('date') || n.includes('created') || n.includes('timestamp');
      })?.name || 'order_date';

      const numCol = columns.find(c => {
        const t = (c.dataType || c.type || '').toLowerCase();
        const n = c.name.toLowerCase();
        return (t.includes('num') || t.includes('int') || t.includes('float') || t.includes('dec') || t.includes('money') || t.includes('amount') || t.includes('price')) && !n.endsWith('_id');
      })?.name || 'amount';

      const catCol = columns.find(c => {
        const t = (c.dataType || c.type || '').toLowerCase();
        const n = c.name.toLowerCase();
        return (t.includes('char') || t.includes('text') || t.includes('str') || n.includes('status') || n.includes('type') || n.includes('category') || n.includes('region')) && !n.endsWith('_id');
      })?.name || 'status';

      const hasKpi = enrichedCards.some(c => ['scalar', 'number', 'gauge', 'smartscalar'].includes(c.display));
      const hasTrend = enrichedCards.some(c => ['line', 'area', 'combo'].includes(c.display));
      const hasBreakdown = enrichedCards.some(c => ['bar', 'row', 'pie', 'donut'].includes(c.display));
      const hasTableOrModel = enrichedCards.some(c => ['table', 'pivot'].includes(c.display) || c.is_model);

      // Synthesize Tier 1: KPI
      if (!hasKpi && enrichedCards.length < 4) {
        enrichedCards.unshift({
          name: `Total ${numCol.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}`,
          display: 'scalar',
          type: 'question',
          is_model: false,
          sql: `SELECT SUM(${numCol}) AS total_${numCol}, COUNT(*) AS total_count FROM ${modelName} WHERE 1=1 [[AND {{date_range}}]]`,
          description: `High-level executive KPI for ${modelName}`,
          size_x: 6,
          size_y: 4,
          visualization_settings: this.formatVisualSettings({ display: 'scalar' }, themeName),
          target_variable: 'date_range',
        });
      }

      // Synthesize Tier 2: Trend Line
      if (!hasTrend && enrichedCards.length < 4) {
        enrichedCards.push({
          name: `${numCol.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())} Trend Over Time`,
          display: 'line',
          type: 'question',
          is_model: false,
          sql: `SELECT DATE_TRUNC('month', ${dateCol}) AS date_month, SUM(${numCol}) AS monthly_sum, COUNT(*) AS count FROM ${modelName} WHERE 1=1 [[AND {{date_range}}]] GROUP BY 1 ORDER BY 1 ASC`,
          description: `Temporal trend analysis for ${modelName}`,
          size_x: 12,
          size_y: 8,
          visualization_settings: this.formatVisualSettings({ display: 'line' }, themeName),
          target_variable: 'date_range',
        });
      }

      // Synthesize Tier 3: Dimensional Breakdown
      if (!hasBreakdown && enrichedCards.length < 4) {
        enrichedCards.push({
          name: `Breakdown by ${catCol.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}`,
          display: 'bar',
          type: 'question',
          is_model: false,
          sql: `SELECT ${catCol}, COUNT(*) AS record_count, SUM(${numCol}) AS total_${numCol} FROM ${modelName} WHERE 1=1 [[AND {{date_range}}]] GROUP BY 1 ORDER BY 2 DESC LIMIT 10`,
          description: `Categorical breakdown by ${catCol} for ${modelName}`,
          size_x: 12,
          size_y: 6,
          visualization_settings: this.formatVisualSettings({ display: 'bar' }, themeName),
          target_variable: 'date_range',
        });
      }

      // Synthesize Tier 4: Detail Table or Metabase Model Card
      if (!hasTableOrModel && enrichedCards.length < 4) {
        enrichedCards.push({
          name: `${modelName} Granular Detail Log`,
          display: 'table',
          type: includeModels ? 'model' : 'question',
          is_model: Boolean(includeModels),
          sql: `SELECT * FROM ${modelName} WHERE 1=1 [[AND {{date_range}}]] LIMIT 100`,
          description: `Curated tabular record records for ${modelName}`,
          size_x: 24,
          size_y: 8,
          visualization_settings: this.formatVisualSettings({ display: 'table' }, themeName),
          target_variable: 'date_range',
        });
      }

      // If still less than 4 (e.g. initial had 0 or 1), add second KPI
      while (enrichedCards.length < 4) {
        const idx = enrichedCards.length + 1;
        enrichedCards.push({
          name: `KPI Metric #${idx}`,
          display: 'scalar',
          type: 'question',
          is_model: false,
          sql: `SELECT COUNT(*) AS total_records FROM ${modelName} WHERE 1=1 [[AND {{date_range}}]]`,
          description: `Executive metric #${idx} for ${modelName}`,
          size_x: 6,
          size_y: 4,
          visualization_settings: this.formatVisualSettings({ display: 'scalar' }, themeName),
          target_variable: 'date_range',
        });
      }
    }

    return enrichedCards;
  }

  /**
   * Main Build Method: Builds complete dashboard from YAML / Exposures / Models
   * @param {object} options
   * @returns {Promise<object>} DashboardBuildResult
   */
  async buildDashboardFromYaml(options = {}) {
    const {
      yaml_content,
      yamlContent,
      yaml_path,
      yamlPath,
      exposure_name,
      exposureName,
      model_name,
      modelName,
      metrics,
      models,
      project_dir = process.env.DBT_PROJECT_DIR || process.cwd(),
      projectDir,
      manifest_path,
      manifestPath,
      catalog_path,
      catalogPath,
      database_id = 1,
      databaseId,
      collection_id = null,
      collectionId,
      dashboard_name,
      dashboardName,
      dashboard_description,
      dashboardDescription,
      theme,
      create_models = true,
      createModels,
      include_models = true,
      includeModels,
      dry_run = false,
      dryRun,
      mask_pii = true,
      maskPii,
      filters: customFilters,
      cards: customCards,
    } = options;

    const dbId = Number(databaseId !== undefined ? databaseId : database_id);
    const collId = collectionId !== undefined ? collectionId : (collection_id !== undefined ? collection_id : null);
    const isDryRun = Boolean(dryRun !== undefined ? dryRun : dry_run);
    const shouldCreateModels = Boolean(
      (includeModels !== undefined ? includeModels : include_models) &&
      (createModels !== undefined ? createModels : create_models)
    );
    const shouldMaskPii = Boolean(maskPii !== undefined ? maskPii : mask_pii);
    const targetTheme = theme || this.theme;
    const targetProjectDir = projectDir || project_dir;

    // 1. Resolve Spec from input sources
    let spec = null;
    const rawYaml = yaml_content !== undefined ? yaml_content : (yamlContent !== undefined ? yamlContent : (yaml_path || yamlPath));

    if (rawYaml) {
      if (typeof rawYaml === 'string' && (rawYaml.endsWith('.yml') || rawYaml.endsWith('.yaml') || fs.existsSync(rawYaml))) {
        spec = this.parseExposureYaml(rawYaml);
      } else {
        spec = this.parseYamlDashboardSpec(rawYaml);
      }
    } else if (exposure_name || exposureName) {
      const targetExpName = exposure_name || exposureName;
      const scanResult = await this.scanner.scanProject(targetProjectDir, {
        manifestPath: manifestPath || manifest_path,
        catalogPath: catalogPath || catalog_path,
      });
      const exposure = scanResult.exposures.find(e => e.name === targetExpName);
      if (!exposure) {
        throw new Error(`Exposure "${targetExpName}" not found in dbt project.`);
      }
      spec = this.normalizeExposureSpec(exposure);
      spec.scanResult = scanResult;

      // If exposure has depends_on refs, link model
      if (Array.isArray(exposure.depends_on) && exposure.depends_on.length > 0) {
        const firstRef = exposure.depends_on.find(r => typeof r === 'string' && (r.startsWith('ref(') || !r.includes('.')));
        const refName = firstRef ? firstRef.replace(/^ref\(['"]?|['"]?\)$/g, '') : null;
        if (refName) {
          const matchedModel = scanResult.models.find(m => m.name === refName);
          if (matchedModel) {
            spec.resolvedModel = matchedModel;
          }
        }
      }
    } else if (model_name || modelName || (Array.isArray(models) && models.length > 0)) {
      const targetModelName = model_name || modelName || models[0];
      const scanResult = await this.scanner.scanProject(targetProjectDir, {
        manifestPath: manifestPath || manifest_path,
        catalogPath: catalogPath || catalog_path,
      });
      const model = scanResult.models.find(m => m.name === targetModelName);
      if (!model) {
        throw new Error(`Model "${targetModelName}" not found in dbt project.`);
      }
      spec = this.normalizeModelDashboardSpec(model);
      spec.scanResult = scanResult;
      spec.resolvedModel = model;
    } else if (Array.isArray(metrics) && metrics.length > 0) {
      spec = this.normalizeMetricsDashboardSpec(metrics);
    } else {
      // Auto-discover top marts model from project if project dir exists
      try {
        const scanResult = await this.scanner.scanProject(targetProjectDir, {
          manifestPath: manifestPath || manifest_path,
          catalogPath: catalogPath || catalog_path,
        });
        const topModel = scanResult.models.find(m => m.tier === 'marts_fact') ||
          scanResult.models.find(m => m.tier === 'marts_dim') ||
          scanResult.models[0];

        if (topModel) {
          spec = this.normalizeModelDashboardSpec(topModel);
          spec.scanResult = scanResult;
          spec.resolvedModel = topModel;
        } else {
          throw new Error('No models found in dbt project to build dashboard from.');
        }
      } catch (err) {
        throw new Error(`Either yaml_content, exposure_name, or model_name must be provided: ${err.message}`);
      }
    }

    // Override names/descriptions if explicitly provided in args
    if (dashboard_name || dashboardName) {
      spec.name = dashboard_name || dashboardName;
    }
    if (dashboard_description || dashboardDescription) {
      spec.description = dashboard_description || dashboardDescription;
    }
    if (customFilters && Array.isArray(customFilters) && customFilters.length > 0) {
      spec.filters = customFilters;
    }
    if (customCards && Array.isArray(customCards) && customCards.length > 0) {
      spec.cards = [...(spec.cards || []), ...customCards];
    }

    // 2. Synthesize & enrich cards (ensuring >= 4 cards with executive visual hierarchy)
    const cards = await this.resolveAndEnrichCards(spec, dbId, targetTheme, {
      include_models: shouldCreateModels,
      includeModels: shouldCreateModels,
    });

    if (cards.length < 4) {
      throw new Error(`Autonomous dashboard build requires at least 4 card definitions (resolved ${cards.length}).`);
    }

    // 3. Compute 24-column collision-free grid positions
    const positions = this.calculateGridCoordinates(cards);
    validateNoCollisions(positions);

    // 4. Generate Filter Mappings
    const filterMappings = this.generateFilterMappings(cards, spec.filters || []);

    // 5. If Dry Run or no client, assemble and return simulated plan
    if (isDryRun || !this.client) {
      return this.assembleDashboardResult({
        dashboardId: 9999,
        name: spec.name,
        description: spec.description,
        theme: targetTheme,
        dbId,
        collId,
        cards,
        positions,
        filterMappings,
        filters: spec.filters || [],
        modelsCreated: [],
        dryRun: true,
      });
    }

    // 6. Execute Live Metabase Mutations
    // 6a. Optionally create Metabase Model (v50+) for mart models
    const modelsCreated = [];
    if (shouldCreateModels && spec.resolvedModel) {
      try {
        if (typeof this.client.createModel === 'function') {
          const modelCard = await this.client.createModel({
            name: `${spec.resolvedModel.name} (dbt Model)`,
            description: spec.resolvedModel.description || `Curated Metabase Model for dbt mart ${spec.resolvedModel.name}`,
            database_id: dbId,
            collection_id: collId,
            dataset_query: {
              database: dbId,
              type: 'native',
              native: { query: `SELECT * FROM ${spec.resolvedModel.name}` },
            },
          });
          modelsCreated.push(modelCard);
        }
      } catch (modelErr) {
        logger.warn(`Could not create Metabase Model card: ${modelErr.message}`);
      }
    }

    // 6b. Create Questions / Cards in Metabase
    const createdCards = [];
    for (let i = 0; i < cards.length; i++) {
      const c = cards[i];
      const templateTags = this.buildTemplateTags(c.sql, spec.filters || []);
      const questionPayload = {
        name: c.name,
        description: c.description || `${c.name} for ${spec.name}`,
        database_id: dbId,
        collection_id: collId,
        display: c.display || 'table',
        visualization_settings: c.visualization_settings || getDefaultVisualizationSettings(c.display),
        dataset_query: {
          database: dbId,
          type: 'native',
          native: {
            query: c.sql,
            'template-tags': templateTags,
          },
        },
      };

      let created;
      if (c.is_model && typeof this.client.createModel === 'function') {
        try {
          created = await this.client.createModel(questionPayload);
        } catch (mErr) {
          created = await this.client.createQuestion(questionPayload);
        }
      } else {
        created = await this.client.createQuestion(questionPayload);
      }

      createdCards.push({
        ...created,
        id: (created && created.id) ? created.id : (1000 + i),
        name: c.name,
        display: c.display,
        type: c.type || (c.is_model ? 'model' : 'question'),
        is_model: c.is_model,
        sql: c.sql,
        visualization_settings: questionPayload.visualization_settings,
      });
    }

    // 6c. Create Dashboard Entity in Metabase
    const dashboardPayload = {
      name: spec.name,
      description: spec.description || `Code-as-BI dbt Dashboard with ${cards.length} cards`,
      collection_id: collId,
      parameters: (spec.filters || []).map((f, idx) => ({
        id: f.id || `param_${f.slug || idx}`,
        name: f.name || `Filter ${idx + 1}`,
        slug: f.slug || `filter_${idx + 1}`,
        type: f.type || 'category',
        default: f.default !== undefined ? f.default : null,
      })),
    };

    const createdDash = await this.client.createDashboard(dashboardPayload);
    const dashId = (createdDash && createdDash.id) ? createdDash.id : 100;

    // 6d. Attach Cards to Dashboard with Positions and Filter Parameter Mappings
    const attachedCards = [];
    for (let i = 0; i < createdCards.length; i++) {
      const card = createdCards[i];
      const pos = positions[i];
      const mappings = (filterMappings[i] || []).map(m => ({ ...m, card_id: card.id }));

      if (typeof this.client.addCardToDashboard === 'function') {
        try {
          await this.client.addCardToDashboard(dashId, card.id, {
            row: pos.row,
            col: pos.col,
            size_x: pos.size_x,
            size_y: pos.size_y,
            sizeX: pos.size_x,
            sizeY: pos.size_y,
            parameter_mappings: mappings,
            visualization_settings: card.visualization_settings,
          });
        } catch (addErr) {
          logger.warn(`Could not attach card ${card.id} to dashboard ${dashId}: ${addErr.message}`);
        }
      }

      attachedCards.push({
        card_id: card.id,
        name: card.name,
        display: card.display,
        type: card.type,
        is_model: card.is_model,
        position: pos,
        parameter_mappings: mappings,
        sql: card.sql,
        visualization_settings: card.visualization_settings,
      });
    }

    return this.assembleDashboardResult({
      dashboardId: dashId,
      name: spec.name,
      description: spec.description,
      theme: targetTheme,
      dbId,
      collId,
      cards: attachedCards,
      positions,
      filterMappings,
      filters: spec.filters || [],
      modelsCreated,
      dryRun: false,
    });
  }

  /**
   * Assemble standard structured result envelope
   */
  assembleDashboardResult({
    dashboardId,
    name,
    description,
    theme,
    dbId,
    collId,
    cards,
    positions,
    filterMappings,
    filters,
    modelsCreated = [],
    dryRun = false,
  }) {
    const formattedCards = cards.map((c, i) => {
      const pos = positions[i] || { row: 0, col: 0, size_x: 6, size_y: 4 };
      const mappings = (filterMappings[i] || []).map(m => ({
        parameter_id: m.parameter_id,
        target: m.target,
        card_id: c.card_id || c.id || (1000 + i),
      }));

      return {
        card_id: c.card_id || c.id || (1000 + i),
        name: c.name,
        display: c.display || 'table',
        type: c.type || (c.is_model ? 'model' : 'question'),
        is_model: Boolean(c.is_model || c.type === 'model'),
        position: {
          row: pos.row,
          col: pos.col,
          size_x: pos.size_x,
          size_y: pos.size_y,
        },
        parameter_mappings: mappings,
        visualization_settings: c.visualization_settings || {},
        sql: c.sql || '',
      };
    });

    const kpiCount = formattedCards.filter(c => ['scalar', 'number', 'gauge', 'smartscalar'].includes(c.display)).length;
    const trendCount = formattedCards.filter(c => ['line', 'bar', 'area', 'combo', 'waterfall'].includes(c.display)).length;
    const breakdownCount = formattedCards.filter(c => ['pie', 'donut', 'row', 'funnel', 'scatter'].includes(c.display)).length;
    const tableCount = formattedCards.filter(c => ['table', 'pivot', 'detail'].includes(c.display) && !c.is_model).length;
    const modelCount = formattedCards.filter(c => c.is_model || c.type === 'model').length;

    const maxRow = Math.max(...positions.map(p => p.row + p.size_y), 0);

    return {
      dashboard_id: dashboardId,
      name,
      description: description || `Code-as-BI dbt Dashboard with ${cards.length} cards`,
      url: `http://localhost:3000/dashboard/${dashboardId}`,
      card_count: formattedCards.length,
      filter_count: filters.length,
      theme,
      cards: formattedCards,
      filters: filters.map((f, idx) => ({
        id: f.id || `param_${f.slug || idx}`,
        name: f.name || `Filter ${idx + 1}`,
        slug: f.slug || `filter_${idx + 1}`,
        type: f.type || 'category',
        default: f.default !== undefined ? f.default : null,
      })),
      models_created: modelsCreated,
      grid_summary: {
        grid_width: GRID_WIDTH,
        total_rows: maxRow,
        kpi_count: kpiCount,
        trend_count: trendCount,
        breakdown_count: breakdownCount,
        table_count: tableCount,
        model_count: modelCount,
      },
      _provenance: {
        governance_level: 'PRODUCTION_CODE_AS_BI',
        builder: 'DbtDashboardBuilder',
        timestamp: new Date().toISOString(),
        theme,
        card_count: formattedCards.length,
        filter_count: filters.length,
        dry_run: dryRun,
      },
    };
  }
}
