/**
 * src/dbt/dbt-deep-scanner.js
 * Deep dbt Project Scanner & Metadata Profiler
 *
 * Recursively inspects dbt projects (models, sources, seeds, snapshots, macros, exposures, schema.yml, docs/*.md),
 * target/manifest.json, target/catalog.json, MetricFlow semantic models, and Metabase/Lightdash visual metadata.
 */

import fs from 'fs';
import path from 'path';
import yaml from 'js-yaml';
import { DBT_TIERS, DbtParser } from './dbt-parser.js';
import { logger } from '../utils/logger.js';

export const DOC_BLOCK_REGEX = /{%[-]?\s*docs?\s+([a-zA-Z0-9_.-]+)\s*[-]?%}([\s\S]*?){%[-]?\s*enddocs?\s*[-]?%}/gi;
export const DOC_REF_REGEX = /\{\{\s*doc\(\s*(?:['"]([a-zA-Z0-9_.-]+)['"]\s*,\s*)?['"]([a-zA-Z0-9_.-]+)['"]\s*\)\s*\}\}/gi;
export const JINJA_TAG_DOC_REF_REGEX = /\{%\s*doc\s*(?:['"]([a-zA-Z0-9_.-]+)['"]\s*,\s*)?['"]([a-zA-Z0-9_.-]+)['"]\s*%\}/gi;
export const BARE_DOC_REF_REGEX = /(?<![{%]\s*)\bdoc\(\s*(?:['"]([a-zA-Z0-9_.-]+)['"]\s*,\s*)?['"]([a-zA-Z0-9_.-]+)['"]\s*\)(?!\s*[%}])/gi;
export const SQL_REF_REGEX = /\{\{\s*ref\(\s*(?:['"]([a-zA-Z0-9_.-]+)['"]\s*,\s*)?['"]([a-zA-Z0-9_.-]+)['"]\s*\)\s*\}\}/gi;
export const SQL_SOURCE_REGEX = /\{\{\s*source\(\s*['"]([a-zA-Z0-9_.-]+)['"]\s*,\s*['"]([a-zA-Z0-9_.-]+)['"]\s*\)\s*\}\}/gi;
export const MACRO_BLOCK_REGEX = /{%[-]?\s*macro\s+([a-zA-Z0-9_.-]+)\s*\((.*?)\)\s*[-]?%}([\s\S]*?){%[-]?\s*endmacro\s*[-]?%}/gi;
export const SNAPSHOT_BLOCK_REGEX = /{%[-]?\s*snapshot\s+([a-zA-Z0-9_.-]+)\s*[-]?%}([\s\S]*?){%[-]?\s*endsnapshot\s*[-]?%}/gi;

export class DbtDeepScanner {
  constructor(options = {}) {
    this.projectDir = options.projectDir || process.cwd();
    this.parser = new DbtParser({ projectDir: this.projectDir });
    this.models = new Map();
    this.sources = new Map();
    this.semanticModels = new Map();
    this.metrics = new Map();
    this.exposures = new Map();
    this.seeds = new Map();
    this.snapshots = new Map();
    this.macros = new Map();
    this.docBlocks = new Map();
    this.relationships = [];
    this.catalogStats = null;
    this.warnings = [];
    this.errors = [];
  }

  /**
   * Reset all internal scanner state
   */
  clearState() {
    this.models.clear();
    this.sources.clear();
    this.semanticModels.clear();
    this.metrics.clear();
    this.exposures.clear();
    this.seeds.clear();
    this.snapshots.clear();
    this.macros.clear();
    this.docBlocks.clear();
    this.relationships = [];
    this.catalogStats = null;
    this.warnings = [];
    this.errors = [];
  }

  /**
   * Main entry point: Deep scan dbt project folder
   */
  async scanProject(customDir = null, options = {}) {
    const targetDir = customDir || this.projectDir;
    if (!fs.existsSync(targetDir)) {
      throw new Error(`dbt project directory not found: ${targetDir}`);
    }

    const startTime = Date.now();
    this.clearState();

    // 1. Ingest Doc Blocks first (docs/*.md and project *.md)
    if (options.includeDocs !== false) {
      this.parseDocBlocks(targetDir);
    }

    // 2. Parse compiled manifest if available
    const manifestPath = options.manifestPath || path.join(targetDir, 'target', 'manifest.json');
    let manifestLoaded = false;
    if (fs.existsSync(manifestPath)) {
      try {
        this.parser.parseManifest(manifestPath);
        for (const [name, model] of this.parser.models.entries()) {
          const tier = this.parser.classifyTier(model.name, model.path, model.meta || {}, model.tags || []);
          this.models.set(name, {
            ...model,
            tier: tier.tier,
            tierRank: tier.rank,
            tierDescription: tier.description,
            visualMeta: this.normalizeVisualMetadata(model.meta || {}),
          });
        }
        for (const [name, metric] of this.parser.metrics.entries()) {
          this.metrics.set(name, {
            ...metric,
            visualMeta: this.normalizeVisualMetadata(metric.meta || {}),
          });
        }
        manifestLoaded = true;
        logger.info(`Loaded manifest with ${this.models.size} models and ${this.metrics.size} metrics.`);
      } catch (err) {
        this.warnings.push(`Failed to parse target/manifest.json: ${err.message}. Falling back to recursive AST scan.`);
        logger.warn(this.warnings[this.warnings.length - 1]);
      }
    }

    // 3. Ingest physical catalog if available
    const catalogPath = options.catalogPath || path.join(targetDir, 'target', 'catalog.json');
    if (options.includeCatalog !== false) {
      this.catalogStats = this.parseCatalog(catalogPath);
    }

    // 4. Recursive Directory AST Scan (.yml, .yaml, .sql, .csv)
    this.scanDirectoryRecursive(targetDir);

    // 5. Ingest MetricFlow metrics if options enable
    if (options.includeMetrics === false) {
      this.semanticModels.clear();
      this.metrics.clear();
    }

    // 6. Enrich models and sources with catalog stats & physical types
    if (this.catalogStats && this.catalogStats.catalogLoaded) {
      this.enrichWithCatalogStats(this.catalogStats);
    }

    // 7. Synthesize relationships from MetricFlow entities & dbt tests
    this.synthesizeEntityRelationships();

    // 8. Resolve doc('...') references across all entities
    this.resolveAllDocReferences();

    const duration = Date.now() - startTime;
    return this.buildProjectScanResult(targetDir, duration, manifestLoaded);
  }

  /**
   * Recursively scan directory for .yml/.yaml, .sql, .csv, and .md files
   */
  scanDirectoryRecursive(dir) {
    if (!fs.existsSync(dir)) return;
    const entries = fs.readdirSync(dir, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);

      if (entry.isDirectory()) {
        if (!['node_modules', '.git', 'dbt_packages', 'target', '.metabase-cache', 'venv', '.venv'].includes(entry.name)) {
          this.scanDirectoryRecursive(fullPath);
        }
      } else if (entry.isFile()) {
        const ext = path.extname(entry.name).toLowerCase();
        if (ext === '.yml' || ext === '.yaml') {
          this.parseYamlFile(fullPath);
        } else if (ext === '.sql') {
          this.parseSqlFile(fullPath);
        } else if (ext === '.csv') {
          this.parseSeedFile(fullPath);
        }
      }
    }
  }

  /**
   * Parse dbt YAML file content safely
   */
  parseYamlFile(filePath) {
    try {
      const raw = fs.readFileSync(filePath, 'utf8');
      let content;
      try {
        const sanitized = raw.replace(/:\s*(\{\{\s*(?:doc|ref|source)\([^}]+?\)\s*\}\})/gi, ': "$1"');
        content = yaml.load(sanitized);
      } catch (yamlErr) {
        this.warnings.push(`Failed to parse YAML file ${filePath}: ${yamlErr.message}`);
        return;
      }

      if (!content || typeof content !== 'object') {
        this.warnings.push(`YAML file ${filePath} does not contain a valid mapping or list`);
        return;
      }

      // Parse models
      if (Array.isArray(content.models)) {
        for (const modelDef of content.models) {
          this.indexModelDef(modelDef, filePath);
        }
      }

      // Parse sources
      if (Array.isArray(content.sources)) {
        for (const sourceDef of content.sources) {
          this.indexSourceDef(sourceDef, filePath);
        }
      }

      // Parse metrics
      if (Array.isArray(content.metrics)) {
        for (const metricDef of content.metrics) {
          this.indexMetricDef(metricDef, filePath);
        }
      }

      // Parse semantic models (dbt MetricFlow)
      if (Array.isArray(content.semantic_models)) {
        for (const semDef of content.semantic_models) {
          this.indexSemanticModelDef(semDef, filePath);
        }
      }

      // Parse exposures (Dashboards & Reports)
      if (Array.isArray(content.exposures)) {
        for (const expDef of content.exposures) {
          this.indexExposureDef(expDef, filePath);
        }
      }

      // Parse seeds YAML metadata
      if (Array.isArray(content.seeds)) {
        for (const seedDef of content.seeds) {
          this.indexSeedYamlDef(seedDef, filePath);
        }
      }

      // Parse snapshots YAML metadata
      if (Array.isArray(content.snapshots)) {
        for (const snapDef of content.snapshots) {
          this.indexSnapshotYamlDef(snapDef, filePath);
        }
      }
    } catch (err) {
      this.warnings.push(`Failed to read/parse file ${filePath}: ${err.message}`);
    }
  }

  /**
   * Parse SQL file for models, snapshots, or macros
   */
  parseSqlFile(filePath) {
    try {
      const content = fs.readFileSync(filePath, 'utf8');
      const normalizedPath = filePath.replace(/\\/g, '/').toLowerCase();

      // 1. Check for snapshot block
      const snapRegex = new RegExp(SNAPSHOT_BLOCK_REGEX);
      let snapMatch;
      while ((snapMatch = snapRegex.exec(content)) !== null) {
        const snapName = snapMatch[1];
        this.snapshots.set(snapName, {
          name: snapName,
          filePath,
          sql: snapMatch[2].trim(),
          tier: 'snapshot',
          tierRank: DBT_TIERS.SNAPSHOT.rank,
        });
      }

      // 2. Check for macro blocks
      const macroRegex = new RegExp(MACRO_BLOCK_REGEX);
      let macroMatch;
      while ((macroMatch = macroRegex.exec(content)) !== null) {
        const macroName = macroMatch[1];
        const macroArgs = macroMatch[2] ? macroMatch[2].split(',').map(a => a.trim()).filter(Boolean) : [];
        this.macros.set(macroName, {
          name: macroName,
          args: macroArgs,
          filePath,
          body: macroMatch[3].trim(),
        });
      }

      // 3. If in models/ or snapshots/ or seeds/ and not a pure macro file
      if (normalizedPath.includes('/models/') || (!normalizedPath.includes('/macros/') && !this.snapshots.has(path.basename(filePath, '.sql')))) {
        const modelName = path.basename(filePath, '.sql');
        if (!this.models.has(modelName)) {
          const tier = this.parser.classifyTier(modelName, filePath);
          const dependsOn = this.extractSqlDependencies(content);

          this.models.set(modelName, {
            name: modelName,
            description: '',
            tier: tier.tier,
            tierRank: tier.rank,
            tierDescription: tier.description,
            meta: {},
            visualMeta: this.normalizeVisualMetadata({}),
            columns: {},
            filePath,
            tags: [],
            dependsOn,
          });
        } else {
          // Model already registered via YAML, update dependsOn if empty
          const existing = this.models.get(modelName);
          if (!existing.dependsOn || existing.dependsOn.length === 0) {
            existing.dependsOn = this.extractSqlDependencies(content);
          }
        }
      }
    } catch (err) {
      this.warnings.push(`Failed to parse SQL file ${filePath}: ${err.message}`);
    }
  }

  /**
   * Extract ref() and source() dependencies from SQL text
   */
  extractSqlDependencies(sqlContent) {
    const dependencies = [];
    let match;

    const refRegex = new RegExp(SQL_REF_REGEX);
    while ((match = refRegex.exec(sqlContent)) !== null) {
      const refName = match[2] || match[1];
      if (refName && !dependencies.includes(refName)) {
        dependencies.push(refName);
      }
    }

    const sourceRegex = new RegExp(SQL_SOURCE_REGEX);
    while ((match = sourceRegex.exec(sqlContent)) !== null) {
      const sourceKey = `${match[1]}.${match[2]}`;
      if (!dependencies.includes(sourceKey)) {
        dependencies.push(sourceKey);
      }
    }

    return dependencies;
  }

  /**
   * Parse static CSV seeds
   */
  parseSeedFile(filePath) {
    const seedName = path.basename(filePath, '.csv');
    if (!this.seeds.has(seedName)) {
      this.seeds.set(seedName, {
        name: seedName,
        filePath,
        tier: 'seed',
        tierRank: DBT_TIERS.SEED.rank,
      });
    }
  }

  /**
   * Index model from YAML definition
   */
  indexModelDef(modelDef, filePath) {
    const name = modelDef.name;
    if (!name) return;

    const tags = Array.isArray(modelDef.tags) ? modelDef.tags : [];
    const meta = modelDef.meta || {};
    const tier = this.parser.classifyTier(name, filePath, meta, tags);
    const columns = {};

    if (Array.isArray(modelDef.columns)) {
      for (const col of modelDef.columns) {
        const colMeta = col.meta || {};
        columns[col.name] = {
          name: col.name,
          description: col.description || '',
          dataType: col.data_type || col.type || 'unknown',
          meta: colMeta,
          visualMeta: this.normalizeVisualMetadata(colMeta),
          tests: col.tests || [],
        };

        // Extract relationship / foreign key tests
        if (Array.isArray(col.tests)) {
          for (const test of col.tests) {
            if (typeof test === 'object' && test.relationships) {
              const rel = test.relationships;
              const toRefMatch = String(rel.to).match(/ref\(['"](.+?)['"]\)/);
              const toTable = toRefMatch ? toRefMatch[1] : String(rel.to).replace(/['"]/g, '');

              this.relationships.push({
                fromModel: name,
                fromColumn: col.name,
                toModel: toTable,
                toColumn: rel.field,
                source: 'dbt_test',
                sourceFile: filePath,
              });
            }
          }
        }
      }
    }

    const existing = this.models.get(name);
    this.models.set(name, {
      name,
      description: modelDef.description || existing?.description || '',
      tier: tier.tier,
      tierRank: tier.rank,
      tierDescription: tier.description,
      meta,
      visualMeta: this.normalizeVisualMetadata(meta),
      columns: { ...(existing?.columns || {}), ...columns },
      filePath,
      tests: modelDef.tests || [],
      tags,
      dependsOn: existing?.dependsOn || [],
    });
  }

  /**
   * Index source from YAML definition
   */
  indexSourceDef(sourceDef, filePath) {
    const sourceName = sourceDef.name;
    if (!sourceName || !Array.isArray(sourceDef.tables)) return;

    for (const table of sourceDef.tables) {
      const key = `${sourceName}.${table.name}`;
      const columns = {};

      if (Array.isArray(table.columns)) {
        for (const col of table.columns) {
          const colMeta = col.meta || {};
          columns[col.name] = {
            name: col.name,
            description: col.description || '',
            dataType: col.data_type || col.type || 'unknown',
            meta: colMeta,
            visualMeta: this.normalizeVisualMetadata(colMeta),
          };
        }
      }

      this.sources.set(key, {
        sourceName,
        tableName: table.name,
        description: table.description || sourceDef.description || '',
        columns,
        meta: table.meta || {},
        visualMeta: this.normalizeVisualMetadata(table.meta || {}),
        filePath,
        tier: 'source',
        tierRank: DBT_TIERS.SOURCE.rank,
      });
    }
  }

  /**
   * Index metric from YAML definition
   */
  indexMetricDef(metricDef, filePath) {
    const name = metricDef.name;
    if (!name) return;

    const metricType = metricDef.type || 'simple';
    const typeParams = metricDef.type_params || {};

    let modelRef = metricDef.model ? String(metricDef.model).replace(/ref\(['"](.+?)['"]\)/, '$1') : null;
    if (!modelRef && typeParams.measure) {
      const measureName = typeof typeParams.measure === 'string' ? typeParams.measure : typeParams.measure?.name;
      for (const sm of this.semanticModels.values()) {
        if (sm.measures?.some(m => m.name === measureName)) {
          modelRef = sm.model;
          break;
        }
      }
    }

    const normalizedTypeParams = {
      measure: typeof typeParams.measure === 'string' ? typeParams.measure : (typeParams.measure?.name || null),
      numerator: typeof typeParams.numerator === 'string' ? typeParams.numerator : (typeParams.numerator?.name || null),
      denominator: typeof typeParams.denominator === 'string' ? typeParams.denominator : (typeParams.denominator?.name || null),
      expr: typeParams.expr || null,
      metrics: Array.isArray(typeParams.metrics)
        ? typeParams.metrics.map(m => typeof m === 'string' ? { name: m, alias: m } : { name: m.name, alias: m.alias || m.name })
        : [],
      window: typeParams.window || null,
      grainToDate: typeParams.grain_to_date || null,
      conversionTypeParams: typeParams.conversion_type_params || null,
    };

    const metricObj = {
      name,
      label: metricDef.label || name,
      description: metricDef.description || '',
      type: metricType,
      model: modelRef,
      typeParams: normalizedTypeParams,
      calculationMethod: metricDef.calculation_method || metricType,
      expression: metricDef.expression || typeParams.expr || metricDef.sql || '',
      filter: metricDef.filter || null,
      timestamp: metricDef.timestamp || null,
      timeGrains: metricDef.time_grains || ['day', 'week', 'month', 'quarter', 'year'],
      dimensions: metricDef.dimensions || [],
      meta: metricDef.meta || {},
      visualMeta: this.normalizeVisualMetadata(metricDef.meta || {}),
      filePath,
    };

    this.metrics.set(name, metricObj);
  }

  /**
   * Index semantic model (MetricFlow) from YAML definition
   */
  indexSemanticModelDef(semDef, filePath) {
    const name = semDef.name;
    if (!name) return;

    const modelRef = semDef.model ? String(semDef.model).replace(/ref\(['"](.+?)['"]\)/, '$1') : name;

    const entities = Array.isArray(semDef.entities) ? semDef.entities.map(e => ({
      name: e.name,
      type: e.type || 'foreign', // primary, foreign, unique, natural
      expr: e.expr || e.name,
      description: e.description || '',
    })) : [];

    const dimensions = Array.isArray(semDef.dimensions) ? semDef.dimensions.map(d => ({
      name: d.name,
      type: d.type || 'categorical', // categorical, time
      typeParams: {
        timeGranularity: d.type_params?.time_granularity || (d.type === 'time' ? 'day' : null),
        validGranularity: d.type_params?.valid_granularity || [],
      },
      expr: d.expr || d.name,
      description: d.description || '',
      isPartition: Boolean(d.is_partition),
    })) : [];

    const measures = Array.isArray(semDef.measures) ? semDef.measures.map(m => ({
      name: m.name,
      description: m.description || '',
      agg: m.agg || 'sum',
      expr: m.expr || m.name,
      aggTimeDimension: m.agg_time_dimension || semDef.defaults?.agg_time_dimension || null,
      aggParams: m.agg_params || {},
      nonAdditiveDimension: m.non_additive_dimension ? {
        name: m.non_additive_dimension.name,
        windowChoice: m.non_additive_dimension.window_choice || 'max',
        windowGroupings: m.non_additive_dimension.window_groupings || [],
      } : null,
      meta: m.meta || {},
      visualMeta: this.normalizeVisualMetadata(m.meta || {}),
    })) : [];

    const semModelObj = {
      name,
      description: semDef.description || '',
      model: modelRef,
      defaults: semDef.defaults || {},
      nodeRelation: semDef.node_relation || {},
      entities,
      dimensions,
      measures,
      meta: semDef.meta || {},
      visualMeta: this.normalizeVisualMetadata(semDef.meta || {}),
      filePath,
    };

    this.semanticModels.set(name, semModelObj);

    // Also expose base measures as simple metrics
    for (const m of measures) {
      const metricKey = `${name}_${m.name}`;
      if (!this.metrics.has(metricKey)) {
        this.metrics.set(metricKey, {
          name: metricKey,
          label: m.description || m.name,
          description: m.description || '',
          type: 'simple',
          model: modelRef,
          typeParams: { measure: m.name },
          calculationMethod: m.agg,
          expression: m.expr,
          timestamp: m.aggTimeDimension,
          timeGrains: ['day', 'week', 'month', 'quarter', 'year'],
          dimensions: dimensions.map(d => d.name),
          meta: m.meta,
          visualMeta: m.visualMeta,
          filePath,
        });
      }
    }
  }

  /**
   * Index exposure (dashboard / notebook / ML model)
   */
  indexExposureDef(expDef, filePath) {
    const name = expDef.name;
    if (!name) return;

    this.exposures.set(name, {
      name,
      label: expDef.label || name,
      type: expDef.type || 'dashboard',
      description: expDef.description || '',
      owner: expDef.owner || {},
      dependsOn: Array.isArray(expDef.depends_on)
        ? expDef.depends_on.map(d => String(d).replace(/ref\(['"](.+?)['"]\)/, '$1'))
        : [],
      meta: expDef.meta || {},
      visualMeta: this.normalizeVisualMetadata(expDef.meta || {}),
      filePath,
    });
  }

  /**
   * Index seed YAML definition
   */
  indexSeedYamlDef(seedDef, filePath) {
    const name = seedDef.name;
    if (!name) return;

    const existing = this.seeds.get(name) || {};
    this.seeds.set(name, {
      ...existing,
      name,
      description: seedDef.description || existing.description || '',
      meta: seedDef.meta || existing.meta || {},
      columns: seedDef.columns || existing.columns || {},
      filePath,
      tier: 'seed',
      tierRank: DBT_TIERS.SEED.rank,
    });
  }

  /**
   * Index snapshot YAML definition
   */
  indexSnapshotYamlDef(snapDef, filePath) {
    const name = snapDef.name;
    if (!name) return;

    const existing = this.snapshots.get(name) || {};
    this.snapshots.set(name, {
      ...existing,
      name,
      description: snapDef.description || existing.description || '',
      meta: snapDef.meta || existing.meta || {},
      columns: snapDef.columns || existing.columns || {},
      filePath,
      tier: 'snapshot',
      tierRank: DBT_TIERS.SNAPSHOT.rank,
    });
  }

  /**
   * Parse dbt Doc markdown blocks ({% docs name %} ... {% enddocs %})
   */
  parseDocBlocks(projectDir) {
    try {
      const scanDocs = (dir) => {
        if (!fs.existsSync(dir)) return;
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
          const fullPath = path.join(dir, entry.name);
          if (entry.isDirectory()) {
            if (!['node_modules', '.git', 'target', 'dbt_packages', '.metabase-cache', 'venv', '.venv'].includes(entry.name)) {
              scanDocs(fullPath);
            }
          } else if (entry.isFile() && path.extname(entry.name).toLowerCase() === '.md') {
            try {
              const content = fs.readFileSync(fullPath, 'utf8');
              const docRegex = new RegExp(DOC_BLOCK_REGEX);
              let match;
              while ((match = docRegex.exec(content)) !== null) {
                const docName = match[1].trim();
                const docBody = match[2].trim();
                this.docBlocks.set(docName, docBody);
              }
            } catch (readErr) {
              this.warnings.push(`Failed to read doc file ${fullPath}: ${readErr.message}`);
            }
          }
        }
      };

      scanDocs(projectDir);
      logger.info(`Loaded ${this.docBlocks.size} dbt doc blocks.`);
      return this.docBlocks;
    } catch (err) {
      this.warnings.push(`Doc block scan failed: ${err.message}`);
      return this.docBlocks;
    }
  }

  /**
   * Resolve doc('...') references in text strings with circular reference bounds
   */
  resolveDocReference(text, docMap = null, depth = 0) {
    if (!text || typeof text !== 'string') return text || '';
    if (depth > 5) return text;
    const map = docMap || this.docBlocks;
    if (!map || map.size === 0) return text;

    let resolved = text;

    // 1. Jinja tag format: {{ doc('name') }} or {{ doc("pkg", "name") }}
    resolved = resolved.replace(DOC_REF_REGEX, (match, pkg, docName) => {
      const targetName = docName || pkg;
      const scopedKey = pkg && docName ? `${pkg}.${docName}` : null;
      if (scopedKey && map.has(scopedKey)) return map.get(scopedKey);
      if (map.has(targetName)) return map.get(targetName);
      return match;
    });

    // 2. Jinja {% doc 'name' %}
    resolved = resolved.replace(JINJA_TAG_DOC_REF_REGEX, (match, pkg, docName) => {
      const targetName = docName || pkg;
      const scopedKey = pkg && docName ? `${pkg}.${docName}` : null;
      if (scopedKey && map.has(scopedKey)) return map.get(scopedKey);
      if (map.has(targetName)) return map.get(targetName);
      return match;
    });

    // 3. Bare doc('name')
    resolved = resolved.replace(BARE_DOC_REF_REGEX, (match, pkg, docName) => {
      const targetName = docName || pkg;
      if (map.has(targetName)) return map.get(targetName);
      return match;
    });

    if (resolved !== text && (DOC_REF_REGEX.test(resolved) || JINJA_TAG_DOC_REF_REGEX.test(resolved) || BARE_DOC_REF_REGEX.test(resolved))) {
      return this.resolveDocReference(resolved, map, depth + 1);
    }

    return resolved;
  }

  /**
   * Parse target/catalog.json table stats and physical column profiling across dialects
   */
  parseCatalog(catalogPath) {
    if (!fs.existsSync(catalogPath)) {
      return {
        catalogLoaded: false,
        catalogPath,
        message: 'target/catalog.json not found. Run "dbt docs generate" to generate table stats and physical column profiles.',
        tables: {},
        sources: {},
        tableCount: 0,
        sourceCount: 0,
        totalRows: 0,
        totalBytes: 0,
        formattedTotalBytes: '0 B',
        errors: [],
      };
    }

    try {
      const raw = fs.readFileSync(catalogPath, 'utf8');
      const catalogData = JSON.parse(raw);

      const tables = {};
      const sources = {};
      let totalRows = 0;
      let totalBytes = 0;

      const extractStats = (node) => {
        const stats = node.stats || {};

        // Row count parsing with multi-dialect fallbacks (BigQuery, Snowflake, Redshift, Databricks, Postgres)
        const rowCountRaw = stats.row_count?.value ?? stats.rows?.value ?? stats.num_rows?.value ?? stats.rowcount?.value ?? stats.count?.value;
        const rowCount = this.parseNumericStat(rowCountRaw);

        // Byte size parsing with multi-dialect fallbacks
        const bytesRaw = stats.num_bytes?.value ?? stats.bytes?.value ?? stats.size_bytes?.value ?? stats.total_size?.value ?? stats.total_bytes?.value ?? stats.size?.value ?? stats.encoded_size?.value;
        const bytes = this.parseNumericStat(bytesRaw);

        if (rowCount !== null) totalRows += rowCount;
        if (bytes !== null) totalBytes += bytes;

        // Partitioning
        const partitionType = stats.partitioning_type?.value ?? stats.partition_type?.value ?? stats.partition_by?.value ?? null;
        const isPartitioned = Boolean(partitionType);

        // Clustering
        const clusterRaw = stats.clustering_fields?.value ?? stats.clustering_key?.value ?? stats.cluster_by?.value ?? null;
        const clusterKeys = clusterRaw ? String(clusterRaw).replace(/^LINEAR\((.*)\)$/i, '$1').split(',').map(s => s.trim()).filter(Boolean) : [];

        // Last modified
        const lastModified = stats.last_modified?.value ?? stats.last_altered?.value ?? null;

        // Physical columns & profiling
        const columns = {};
        if (node.columns) {
          for (const [colName, colDef] of Object.entries(node.columns)) {
            columns[colName] = {
              name: colName,
              type: colDef.type || 'unknown',
              index: colDef.index || 0,
              comment: colDef.comment || '',
              nullCount: colDef.null_count !== undefined ? Number(colDef.null_count) : null,
              distinctCount: colDef.distinct_count !== undefined ? Number(colDef.distinct_count) : null,
              stats: colDef.stats || {},
            };
          }
        }

        return {
          uniqueId: node.unique_id,
          name: node.metadata?.name || '',
          schema: node.metadata?.schema || '',
          database: node.metadata?.database || '',
          type: node.metadata?.type || 'table',
          owner: node.metadata?.owner || null,
          comment: node.metadata?.comment || '',
          rowCount,
          bytes,
          formattedSize: this.formatBytes(bytes),
          isPartitioned,
          partitionType,
          clusterKeys,
          lastModified,
          columns,
          rawStats: stats,
        };
      };

      if (catalogData.nodes) {
        for (const [id, node] of Object.entries(catalogData.nodes)) {
          const tableName = node.metadata?.name || id.split('.').pop();
          tables[tableName] = extractStats(node);
        }
      }

      if (catalogData.sources) {
        for (const [id, node] of Object.entries(catalogData.sources)) {
          const sourceName = node.metadata?.name || id.split('.').pop();
          sources[sourceName] = extractStats(node);
        }
      }

      return {
        catalogLoaded: true,
        catalogPath,
        generatedAt: catalogData.metadata?.generated_at || null,
        dbtVersion: catalogData.metadata?.dbt_version || null,
        tableCount: Object.keys(tables).length,
        sourceCount: Object.keys(sources).length,
        totalRows,
        totalBytes,
        formattedTotalBytes: this.formatBytes(totalBytes),
        tables,
        sources,
        errors: catalogData.errors || [],
      };
    } catch (err) {
      this.warnings.push(`Error parsing catalog.json: ${err.message}`);
      return {
        catalogLoaded: false,
        catalogPath,
        error: err.message,
        tables: {},
        sources: {},
        tableCount: 0,
        sourceCount: 0,
        totalRows: 0,
        totalBytes: 0,
        formattedTotalBytes: '0 B',
        errors: [err.message],
      };
    }
  }

  /**
   * Parse MetricFlow semantic models and metrics
   */
  parseMetricFlow(content, filePath = '') {
    const parsed = typeof content === 'string' ? yaml.load(content) : content;
    const result = {
      semanticModels: [],
      metrics: [],
    };
    if (!parsed || typeof parsed !== 'object') return result;

    if (Array.isArray(parsed.semantic_models)) {
      for (const sm of parsed.semantic_models) {
        this.indexSemanticModelDef(sm, filePath);
        const stored = this.semanticModels.get(sm.name);
        if (stored) result.semanticModels.push(stored);
      }
    }

    if (Array.isArray(parsed.metrics)) {
      for (const m of parsed.metrics) {
        this.indexMetricDef(m, filePath);
        const stored = this.metrics.get(m.name);
        if (stored) result.metrics.push(stored);
      }
    }

    return result;
  }

  /**
   * Normalize visual metadata across meta.metabase and meta.lightdash
   */
  normalizeVisualMetadata(rawMeta = {}) {
    const mb = rawMeta.metabase || {};
    const ld = rawMeta.lightdash || {};

    const displayName = mb.display_name || mb.friendly_name || ld.label || null;
    const chartType = mb.chart_type || ld.chart_type || null;
    const color = mb.color || ld.color || null;
    const palette = Array.isArray(ld.colors) ? ld.colors : (Array.isArray(mb.colors) ? mb.colors : (color ? [color] : []));

    let semanticType = mb.special_type || mb.semantic_type || null;
    if (!semanticType && ld.format) {
      const fmt = String(ld.format).toLowerCase();
      if (['usd', 'eur', 'gbp', 'currency'].includes(fmt)) semanticType = 'type/Currency';
      else if (['percent', 'percentage'].includes(fmt)) semanticType = 'type/Percentage';
      else if (['id', 'pk'].includes(fmt)) semanticType = 'type/PK';
      else if (['fk'].includes(fmt)) semanticType = 'type/FK';
    }

    const mbFmt = mb.formatting || {};
    let formatType = 'string';
    let currency = mbFmt.currency || (['usd', 'eur', 'gbp'].includes(String(ld.format || '').toLowerCase()) ? String(ld.format).toUpperCase() : null);
    let decimals = mbFmt.decimals !== undefined ? mbFmt.decimals : (ld.round !== undefined ? ld.round : null);

    if (currency || mbFmt.number_style === 'currency' || semanticType === 'type/Currency') {
      formatType = 'currency';
    } else if (mbFmt.number_style === 'percent' || ld.format === 'percent' || semanticType === 'type/Percentage') {
      formatType = 'percent';
    } else if (mbFmt.number_style === 'decimal' || ld.format === 'number') {
      formatType = 'number';
    }

    const formatting = {
      formatType,
      currency,
      decimals,
      prefix: mbFmt.prefix || (currency === 'USD' ? '$' : currency === 'EUR' ? '€' : ''),
      suffix: mbFmt.suffix || (formatType === 'percent' ? '%' : ''),
      compact: mbFmt.compact || ld.compact || false,
      dateFormat: mbFmt.date_format || null,
    };

    return {
      displayName,
      chartType,
      color,
      palette,
      colors: palette,
      semanticType,
      formatting,
      hidden: mb.visibility === 'hidden' || ld.hidden === true,
      drillUrls: Array.isArray(ld.urls) ? ld.urls : [],
      urls: Array.isArray(ld.urls) ? ld.urls : [],
      joins: Array.isArray(ld.joins) ? ld.joins : [],
      rawMetabase: mb,
      rawLightdash: ld,
    };
  }

  /**
   * Enrich models & sources with catalog table stats and physical column types
   */
  enrichWithCatalogStats(catalog) {
    if (!catalog || !catalog.tables) return;

    for (const [modelName, model] of this.models.entries()) {
      const tableStat = catalog.tables[modelName] || catalog.tables[model.alias];
      if (tableStat) {
        model.stats = {
          rowCount: tableStat.rowCount,
          bytes: tableStat.bytes,
          formattedSize: tableStat.formattedSize,
          isPartitioned: tableStat.isPartitioned,
          partitionType: tableStat.partitionType,
          clusterKeys: tableStat.clusterKeys,
          lastModified: tableStat.lastModified,
          tableType: tableStat.type,
          owner: tableStat.owner,
          columns: tableStat.columns,
        };

        // Enrich physical column data types & profiles
        if (model.columns && tableStat.columns) {
          for (const [colName, col] of Object.entries(model.columns)) {
            if (tableStat.columns[colName]) {
              const catCol = tableStat.columns[colName];
              col.physicalDataType = catCol.type;
              if (col.dataType === 'unknown') {
                col.dataType = catCol.type;
              }
              if (catCol.nullCount !== null) col.nullCount = catCol.nullCount;
              if (catCol.distinctCount !== null) col.distinctCount = catCol.distinctCount;
            }
          }
        }
      }
    }

    // Enrich sources
    if (catalog.sources) {
      for (const [sourceKey, source] of this.sources.entries()) {
        const sourceStat = catalog.sources[sourceKey] || catalog.sources[source.tableName];
        if (sourceStat) {
          source.stats = {
            rowCount: sourceStat.rowCount,
            bytes: sourceStat.bytes,
            formattedSize: sourceStat.formattedSize,
            isPartitioned: sourceStat.isPartitioned,
            partitionType: sourceStat.partitionType,
            clusterKeys: sourceStat.clusterKeys,
            lastModified: sourceStat.lastModified,
            tableType: sourceStat.type,
            owner: sourceStat.owner,
          };
        }
      }
    }
  }

  /**
   * Synthesize join relationships from MetricFlow entities (foreign -> primary)
   */
  synthesizeEntityRelationships() {
    const primaryEntities = new Map(); // entityName -> { modelName, fieldName }

    for (const sm of this.semanticModels.values()) {
      if (!Array.isArray(sm.entities)) continue;
      for (const entity of sm.entities) {
        if (entity.type === 'primary') {
          primaryEntities.set(entity.name, {
            modelName: sm.model,
            fieldName: entity.expr || entity.name,
          });
        }
      }
    }

    for (const sm of this.semanticModels.values()) {
      if (!Array.isArray(sm.entities)) continue;
      for (const entity of sm.entities) {
        if (entity.type === 'foreign' && primaryEntities.has(entity.name)) {
          const target = primaryEntities.get(entity.name);
          if (target.modelName !== sm.model) {
            this.relationships.push({
              fromModel: sm.model,
              fromColumn: entity.expr || entity.name,
              toModel: target.modelName,
              toColumn: target.fieldName,
              source: 'metricflow_entity',
              sourceFile: sm.filePath,
            });
          }
        }
      }
    }
  }

  /**
   * Resolve doc('...') references across all model/column descriptions and metrics
   */
  resolveAllDocReferences() {
    for (const model of this.models.values()) {
      if (model.description) {
        model.description = this.resolveDocReference(model.description);
      }
      if (model.columns) {
        for (const col of Object.values(model.columns)) {
          if (col.description) {
            col.description = this.resolveDocReference(col.description);
          }
        }
      }
    }

    for (const source of this.sources.values()) {
      if (source.description) {
        source.description = this.resolveDocReference(source.description);
      }
      if (source.columns) {
        for (const col of Object.values(source.columns)) {
          if (col.description) {
            col.description = this.resolveDocReference(col.description);
          }
        }
      }
    }

    for (const metric of this.metrics.values()) {
      if (metric.description) {
        metric.description = this.resolveDocReference(metric.description);
      }
    }

    for (const semModel of this.semanticModels.values()) {
      if (semModel.description) {
        semModel.description = this.resolveDocReference(semModel.description);
      }
    }

    for (const exp of this.exposures.values()) {
      if (exp.description) {
        exp.description = this.resolveDocReference(exp.description);
      }
    }
  }

  /**
   * Helper: Parse comma-separated numeric strings and numbers
   */
  parseNumericStat(val) {
    if (val === null || val === undefined) return null;
    if (typeof val === 'number') return isNaN(val) ? null : val;
    if (typeof val === 'string') {
      const cleaned = val.replace(/,/g, '').trim();
      const num = Number(cleaned);
      return isNaN(num) ? null : num;
    }
    return null;
  }

  /**
   * Helper: Format bytes to human-readable string
   */
  formatBytes(bytes) {
    if (!bytes || isNaN(bytes) || bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    const formatted = (bytes / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1);
    return `${formatted} ${units[i] || 'PB'}`;
  }

  /**
   * Helper: Extract project name from dbt_project.yml
   */
  getProjectName(projectDir) {
    try {
      const pyml = path.join(projectDir, 'dbt_project.yml');
      if (fs.existsSync(pyml)) {
        const conf = yaml.load(fs.readFileSync(pyml, 'utf8'));
        return conf?.name || path.basename(projectDir);
      }
    } catch (_) {}
    return path.basename(projectDir);
  }

  /**
   * Get counts of models across all 9 tiers
   */
  getModelsByTierSummary() {
    const summary = {
      marts_fact: 0,
      marts_dim: 0,
      marts_report: 0,
      intermediate: 0,
      snapshot: 0,
      staging: 0,
      seed: 0,
      source: 0,
      raw: 0,
    };

    for (const m of this.models.values()) {
      if (summary[m.tier] !== undefined) {
        summary[m.tier]++;
      } else {
        summary.raw++;
      }
    }
    return summary;
  }

  /**
   * Build complete ProjectScanResult envelope
   */
  buildProjectScanResult(targetDir, durationMs = 0, manifestLoaded = false) {
    const modelsByTier = this.getModelsByTierSummary();
    const modelsList = Array.from(this.models.values());
    const sourcesList = Array.from(this.sources.values());
    const semanticModelsList = Array.from(this.semanticModels.values());
    const metricsList = Array.from(this.metrics.values());
    const exposuresList = Array.from(this.exposures.values());
    const seedsList = Array.from(this.seeds.values());
    const snapshotsList = Array.from(this.snapshots.values());
    const macrosList = Array.from(this.macros.values());
    const docBlocksObj = Object.fromEntries(this.docBlocks);

    const summary = {
      modelCount: this.models.size,
      sourceCount: this.sources.size,
      exposureCount: this.exposures.size,
      semanticModelCount: this.semanticModels.size,
      metricCount: this.metrics.size,
      docBlockCount: this.docBlocks.size,
      seedCount: this.seeds.size,
      snapshotCount: this.snapshots.size,
      macroCount: this.macros.size,
      relationshipCount: this.relationships.length,
      catalogTableCount: this.catalogStats?.tableCount || 0,
      manifestLoaded: Boolean(manifestLoaded),
      catalogStatsLoaded: Boolean(this.catalogStats?.catalogLoaded),
      totalTableRows: this.catalogStats?.totalRows || 0,
      totalTableBytes: this.catalogStats?.totalBytes || 0,
      formattedTotalBytes: this.catalogStats?.formattedTotalBytes || '0 B',
      modelsByTier,
    };

    return {
      projectDir: targetDir,
      projectName: this.getProjectName(targetDir),
      dbtVersion: this.catalogStats?.dbtVersion || null,
      scanTimestamp: new Date().toISOString(),
      scanDurationMs: durationMs,
      manifestLoaded: Boolean(manifestLoaded),
      catalogLoaded: Boolean(this.catalogStats?.catalogLoaded),
      docsLoaded: this.docBlocks.size > 0,
      summary,
      modelCount: this.models.size,
      sourceCount: this.sources.size,
      metricCount: this.metrics.size,
      exposureCount: this.exposures.size,
      relationshipCount: this.relationships.length,
      docBlockCount: this.docBlocks.size,
      catalogTableCount: this.catalogStats?.tableCount || 0,
      modelsByTier,
      models: modelsList,
      modelsMap: Object.fromEntries(this.models),
      sources: sourcesList,
      sourcesMap: Object.fromEntries(this.sources),
      semanticModels: semanticModelsList,
      semanticModelsMap: Object.fromEntries(this.semanticModels),
      metrics: metricsList,
      metricsMap: Object.fromEntries(this.metrics),
      exposures: exposuresList,
      exposuresMap: Object.fromEntries(this.exposures),
      seeds: seedsList,
      snapshots: snapshotsList,
      macros: macrosList,
      docBlocks: docBlocksObj,
      relationships: this.relationships,
      catalog: this.catalogStats,
      catalogStats: this.catalogStats,
      warnings: this.warnings,
      errors: this.errors,
      _provenance: {
        governance_level: 'READ_ONLY_INSPECTION',
        scanner: 'DbtDeepScanner',
        timestamp: new Date().toISOString(),
        manifest_loaded: Boolean(manifestLoaded),
        catalog_loaded: Boolean(this.catalogStats?.catalogLoaded),
        docs_loaded: this.docBlocks.size > 0,
      },
    };
  }
}
