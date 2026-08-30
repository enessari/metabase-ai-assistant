import { logger } from '../utils/logger.js';
import { isPiiMaskingEnabled } from '../utils/pii-masker.js';

/**
 * 24-Column Grid Width Constant
 */
export const GRID_WIDTH = 24;

/**
 * Card Archetypes Enum
 */
export const CARD_ARCHETYPES = {
  KPI: 'KPI',
  TREND: 'TREND',
  BREAKDOWN: 'BREAKDOWN',
  TABLE: 'TABLE',
  OTHER: 'OTHER',
};

/**
 * Default Display Type Dimensions on 24-Column Grid
 */
export const DISPLAY_DIMENSIONS = {
  // KPI / Summary cards (4 cards per 24-col row)
  scalar: { size_x: 6, size_y: 4, archetype: CARD_ARCHETYPES.KPI },
  number: { size_x: 6, size_y: 4, archetype: CARD_ARCHETYPES.KPI },
  gauge: { size_x: 6, size_y: 4, archetype: CARD_ARCHETYPES.KPI },
  smartscalar: { size_x: 6, size_y: 4, archetype: CARD_ARCHETYPES.KPI },

  // Trend / Time-series / Primary charts (2 cards per 24-col row)
  line: { size_x: 12, size_y: 8, archetype: CARD_ARCHETYPES.TREND },
  bar: { size_x: 12, size_y: 8, archetype: CARD_ARCHETYPES.TREND },
  area: { size_x: 12, size_y: 8, archetype: CARD_ARCHETYPES.TREND },
  combo: { size_x: 12, size_y: 8, archetype: CARD_ARCHETYPES.TREND },
  waterfall: { size_x: 12, size_y: 8, archetype: CARD_ARCHETYPES.TREND },

  // Categorical Breakdown / Secondary charts (2 cards per 24-col row, or 3 cards at 8 cols)
  pie: { size_x: 12, size_y: 6, archetype: CARD_ARCHETYPES.BREAKDOWN },
  donut: { size_x: 12, size_y: 6, archetype: CARD_ARCHETYPES.BREAKDOWN },
  row: { size_x: 12, size_y: 6, archetype: CARD_ARCHETYPES.BREAKDOWN },
  funnel: { size_x: 12, size_y: 6, archetype: CARD_ARCHETYPES.BREAKDOWN },
  progress: { size_x: 12, size_y: 6, archetype: CARD_ARCHETYPES.BREAKDOWN },
  scatter: { size_x: 12, size_y: 6, archetype: CARD_ARCHETYPES.BREAKDOWN },

  // Full-width detail tables
  table: { size_x: 24, size_y: 8, archetype: CARD_ARCHETYPES.TABLE },
  pivot: { size_x: 24, size_y: 8, archetype: CARD_ARCHETYPES.TABLE },
  detail: { size_x: 24, size_y: 8, archetype: CARD_ARCHETYPES.TABLE },
};

/**
 * Returns archetype and default dimensions for a given display type.
 * @param {string} display
 * @returns {{ size_x: number, size_y: number, archetype: string }}
 */
export function getCardDimensionsAndArchetype(display) {
  const normalized = String(display || 'table').toLowerCase().trim();
  if (DISPLAY_DIMENSIONS[normalized]) {
    return { ...DISPLAY_DIMENSIONS[normalized] };
  }
  return { size_x: 12, size_y: 6, archetype: CARD_ARCHETYPES.OTHER };
}

/**
 * Generates default visualization settings based on display type
 * @param {string} display
 * @returns {object}
 */
export function getDefaultVisualizationSettings(display) {
  const normalized = String(display || 'table').toLowerCase().trim();
  switch (normalized) {
    case 'scalar':
    case 'number':
    case 'gauge':
      return { 'scalar.decimals': 0 };
    case 'line':
    case 'area':
      return { 'graph.show_values': true, 'graph.x_axis.scale': 'timeseries' };
    case 'bar':
      return { 'graph.show_values': true };
    case 'pie':
    case 'donut':
      return { 'pie.show_legend': true, 'pie.percent_visibility': 'inside' };
    case 'row':
      return { 'graph.show_values': true };
    case 'table':
    case 'pivot':
    default:
      return {};
  }
}

/**
 * Validates that all positioned cards fit within 24 columns and do not overlap.
 * @param {Array<{ row: number, col: number, size_x: number, size_y: number }>} positionedCards
 * @returns {boolean} Returns true if valid, throws an Error if invalid.
 */
export function validateNoCollisions(positionedCards) {
  if (!Array.isArray(positionedCards)) {
    throw new Error('positionedCards must be an array');
  }

  for (let i = 0; i < positionedCards.length; i++) {
    const a = positionedCards[i];
    const sizeX = a.size_x !== undefined ? a.size_x : a.sizeX;
    const sizeY = a.size_y !== undefined ? a.size_y : a.sizeY;

    if (a.row === undefined || a.row < 0) {
      throw new Error(`Card at index ${i} has invalid row: ${a.row}`);
    }
    if (a.col === undefined || a.col < 0) {
      throw new Error(`Card at index ${i} has invalid col: ${a.col}`);
    }
    if (!sizeX || sizeX < 1 || sizeX > GRID_WIDTH) {
      throw new Error(`Card at index ${i} has invalid size_x: ${sizeX} (must be between 1 and 24)`);
    }
    if (!sizeY || sizeY < 1) {
      throw new Error(`Card at index ${i} has invalid size_y: ${sizeY} (must be at least 1)`);
    }
    if (a.col + sizeX > GRID_WIDTH) {
      throw new Error(`Card at index ${i} exceeds 24-column grid boundary: col (${a.col}) + size_x (${sizeX}) = ${a.col + sizeX} > 24`);
    }

    // Pairwise overlap check
    for (let j = i + 1; j < positionedCards.length; j++) {
      const b = positionedCards[j];
      const bSizeX = b.size_x !== undefined ? b.size_x : b.sizeX;
      const bSizeY = b.size_y !== undefined ? b.size_y : b.sizeY;

      const overlapX = a.col < b.col + bSizeX && a.col + sizeX > b.col;
      const overlapY = a.row < b.row + bSizeY && a.row + sizeY > b.row;

      if (overlapX && overlapY) {
        throw new Error(
          `Grid collision detected between card ${i} [row:${a.row}, col:${a.col}, ${sizeX}x${sizeY}] ` +
          `and card ${j} [row:${b.row}, col:${b.col}, ${bSizeX}x${bSizeY}]`
        );
      }
    }
  }

  return true;
}

/**
 * 2D Bin-Packing Layout Calculator on Metabase's 24-Column Grid.
 * Computes non-overlapping coordinates (row, col, size_x, size_y) for all cards.
 * 
 * Layout Strategy:
 * - Row 0: KPI summary cards (scalar/number/gauge): col: 0, 6, 12, 18, size_x: 6, size_y: 4
 * - Row 4: Primary trend / time-series charts (line/bar/area): col: 0, 12, size_x: 12, size_y: 8
 * - Row 12: Secondary / breakdown charts (pie/donut/row): col: 0, 12, size_x: 12, size_y: 6
 * - Row 18: Full-width detail tables: col: 0, size_x: 24, size_y: 8
 * 
 * @param {Array<object>} cards
 * @param {object} options
 * @returns {Array<{ row: number, col: number, size_x: number, size_y: number }>}
 */
export function calculate24ColGridPositions(cards, options = {}) {
  if (!Array.isArray(cards) || cards.length === 0) {
    return [];
  }

  // Check if all cards have explicit valid positions provided
  const allExplicit = cards.every(c =>
    c.row !== undefined &&
    c.col !== undefined &&
    (c.size_x !== undefined || c.sizeX !== undefined) &&
    (c.size_y !== undefined || c.sizeY !== undefined)
  );

  if (allExplicit) {
    const explicitPositions = cards.map(c => ({
      row: Number(c.row),
      col: Number(c.col),
      size_x: Number(c.size_x !== undefined ? c.size_x : c.sizeX),
      size_y: Number(c.size_y !== undefined ? c.size_y : c.sizeY),
    }));

    try {
      validateNoCollisions(explicitPositions);
      return explicitPositions;
    } catch (e) {
      logger.warn('Explicit card positions had collisions or bounds errors, falling back to autonomous layout calculation:', e.message);
    }
  }

  // Determine card archetype and dimensions for each card
  const cardsWithMeta = cards.map((c, index) => {
    const { size_x: defX, size_y: defY, archetype } = getCardDimensionsAndArchetype(c.display);
    const size_x = Number(c.size_x || c.sizeX || defX);
    const size_y = Number(c.size_y || c.sizeY || defY);
    return {
      index,
      original: c,
      display: c.display || 'table',
      archetype,
      size_x: Math.min(Math.max(1, size_x), GRID_WIDTH),
      size_y: Math.max(1, size_y),
    };
  });

  // Check if cards are organized by archetype section (KPI -> TREND -> BREAKDOWN -> TABLE)
  const isSectional = options.forceSequential !== true;

  if (isSectional) {
    // Separate by archetype while retaining original indices
    const kpis = cardsWithMeta.filter(c => c.archetype === CARD_ARCHETYPES.KPI);
    const trends = cardsWithMeta.filter(c => c.archetype === CARD_ARCHETYPES.TREND);
    const breakdowns = cardsWithMeta.filter(c => c.archetype === CARD_ARCHETYPES.BREAKDOWN);
    const tables = cardsWithMeta.filter(c => c.archetype === CARD_ARCHETYPES.TABLE);
    const others = cardsWithMeta.filter(c => c.archetype === CARD_ARCHETYPES.OTHER);

    const positions = new Array(cards.length);
    let currentRow = 0;

    // 1. KPI Section (Row 0+)
    if (kpis.length > 0) {
      const kpiRowHeight = 4;
      const kpiWidth = 6;
      kpis.forEach((kpi, i) => {
        const rowOffset = Math.floor(i / 4) * kpiRowHeight;
        const col = (i % 4) * kpiWidth;
        positions[kpi.index] = {
          row: currentRow + rowOffset,
          col: col,
          size_x: kpiWidth,
          size_y: kpiRowHeight,
        };
      });
      const totalKpiRows = Math.ceil(kpis.length / 4);
      currentRow += totalKpiRows * kpiRowHeight;
    }

    // 2. Trend Section (Row 4+)
    if (trends.length > 0) {
      const trendRowHeight = 8;
      const trendWidth = 12;
      trends.forEach((trend, i) => {
        const rowOffset = Math.floor(i / 2) * trendRowHeight;
        const col = (i % 2) * trendWidth;
        positions[trend.index] = {
          row: currentRow + rowOffset,
          col: col,
          size_x: trendWidth,
          size_y: trendRowHeight,
        };
      });
      const totalTrendRows = Math.ceil(trends.length / 2);
      currentRow += totalTrendRows * trendRowHeight;
    }

    // 3. Breakdown Section (Row 12+)
    if (breakdowns.length > 0) {
      const breakdownRowHeight = 6;
      // If 3 breakdown cards, place 3 side-by-side (width 8), otherwise width 12
      const isTriRow = breakdowns.length % 3 === 0 && breakdowns.length <= 6;
      const breakdownWidth = isTriRow ? 8 : 12;
      const cardsPerRow = isTriRow ? 3 : 2;

      breakdowns.forEach((bd, i) => {
        const rowOffset = Math.floor(i / cardsPerRow) * breakdownRowHeight;
        const col = (i % cardsPerRow) * breakdownWidth;
        positions[bd.index] = {
          row: currentRow + rowOffset,
          col: col,
          size_x: breakdownWidth,
          size_y: breakdownRowHeight,
        };
      });
      const totalBreakdownRows = Math.ceil(breakdowns.length / cardsPerRow);
      currentRow += totalBreakdownRows * breakdownRowHeight;
    }

    // 4. Detail Table Section (Row 18+)
    if (tables.length > 0) {
      const tableRowHeight = 8;
      const tableWidth = 24;
      tables.forEach((tbl, i) => {
        positions[tbl.index] = {
          row: currentRow + (i * tableRowHeight),
          col: 0,
          size_x: tableWidth,
          size_y: tableRowHeight,
        };
      });
      currentRow += tables.length * tableRowHeight;
    }

    // 5. Other / Unclassified cards
    if (others.length > 0) {
      const otherRowHeight = 6;
      const otherWidth = 12;
      others.forEach((oth, i) => {
        const rowOffset = Math.floor(i / 2) * otherRowHeight;
        const col = (i % 2) * otherWidth;
        positions[oth.index] = {
          row: currentRow + rowOffset,
          col: col,
          size_x: otherWidth,
          size_y: otherRowHeight,
        };
      });
      currentRow += Math.ceil(others.length / 2) * otherRowHeight;
    }

    // Validate that all positions were assigned and are collision-free
    const allAssigned = positions.every(p => p !== undefined && p.row !== undefined);
    if (allAssigned) {
      try {
        validateNoCollisions(positions);
        return positions;
      } catch (e) {
        logger.warn('Sectional layout validation failed, falling back to 2D bin-packing:', e.message);
      }
    }
  }

  // Sequential 2D Bin-Packing Fallback Engine
  const grid = new Map(); // key: `${row},${col}` -> true
  const positions = [];

  function isCellFree(r, c) {
    return !grid.has(`${r},${c}`);
  }

  function canPlace(r, c, width, height) {
    if (c + width > GRID_WIDTH) return false;
    for (let row = r; row < r + height; row++) {
      for (let col = c; col < c + width; col++) {
        if (!isCellFree(row, col)) return false;
      }
    }
    return true;
  }

  function occupy(r, c, width, height) {
    for (let row = r; row < r + height; row++) {
      for (let col = c; col < c + width; col++) {
        grid.set(`${row},${col}`, true);
      }
    }
  }

  for (const card of cardsWithMeta) {
    const width = card.size_x;
    const height = card.size_y;
    let placed = false;
    let r = 0;

    while (!placed) {
      for (let c = 0; c <= GRID_WIDTH - width; c++) {
        if (canPlace(r, c, width, height)) {
          occupy(r, c, width, height);
          positions.push({
            row: r,
            col: c,
            size_x: width,
            size_y: height,
          });
          placed = true;
          break;
        }
      }
      if (!placed) {
        r++;
      }
    }
  }

  validateNoCollisions(positions);
  return positions;
}

/**
 * Generates Metabase parameter mappings connecting dashboard filter parameters to cards.
 * 
 * Supports:
 * - Native SQL template tags: ["variable", ["template-tag", variable_name]]
 * - Field dimensions: ["dimension", ["field", field_id, null]]
 * - Auto-detection from {{template_tag}} markers in card SQL
 * 
 * @param {Array<object>} cards
 * @param {Array<object>} filters
 * @returns {Array<Array<object>>} An array where index i is the parameter_mappings array for card i
 */
export function generateFilterMappings(cards, filters = []) {
  if (!Array.isArray(cards)) return [];
  if (!Array.isArray(filters) || filters.length === 0) {
    return cards.map(() => []);
  }

  // Normalize filters
  const normalizedFilters = filters.map(f => {
    const name = f.name || 'Filter';
    const slug = f.slug || name.toLowerCase().replace(/\s+/g, '_');
    const id = f.id || `param_${slug}`;
    const targetVar = f.target_variable || f.targetVariable || slug;
    return {
      id,
      name,
      slug,
      type: f.type || 'category',
      target_variable: targetVar,
      field_id: f.field_id || f.fieldId || f.target_field_id || null,
    };
  });

  return cards.map(card => {
    const mappings = [];
    const sql = card.sql || '';

    // 1. Extract template tags from card SQL: {{variable_name}}
    const tagMatches = (sql.match(/\{\{([a-zA-Z0-9_]+)\}\}/g) || []).map(t => t.replace(/[{}]/g, ''));

    // 2. Check for explicit card parameters / parameter_name
    const cardParamName = card.parameter_name || card.target_variable || null;

    for (const filter of normalizedFilters) {
      let matchedTag = null;

      // Direct template tag match in SQL
      if (tagMatches.includes(filter.target_variable)) {
        matchedTag = filter.target_variable;
      } else if (tagMatches.includes(filter.slug)) {
        matchedTag = filter.slug;
      } else if (tagMatches.some(t => t.toLowerCase() === filter.name.toLowerCase().replace(/\s+/g, '_'))) {
        matchedTag = tagMatches.find(t => t.toLowerCase() === filter.name.toLowerCase().replace(/\s+/g, '_'));
      }

      // Explicit parameter name match
      if (!matchedTag && cardParamName) {
        if (
          cardParamName === filter.target_variable ||
          cardParamName === filter.slug ||
          cardParamName.toLowerCase() === filter.name.toLowerCase()
        ) {
          matchedTag = cardParamName;
        }
      }

      if (matchedTag) {
        mappings.push({
          parameter_id: filter.id,
          target: ['variable', ['template-tag', matchedTag]],
        });
      } else if (filter.field_id) {
        // Field dimension mapping
        mappings.push({
          parameter_id: filter.id,
          target: ['dimension', ['field', filter.field_id, null]],
        });
      }
    }

    return mappings;
  });
}

/**
 * End-to-End Autonomous Dashboard Architect (`buildFullDashboard`)
 * Creates questions/cards, dashboard entity, computes 24-col grid layout, links filters, and returns complete structured metadata.
 * 
 * @param {object} options
 * @returns {Promise<object>} Complete dashboard response descriptor with _provenance
 */
export async function buildFullDashboard({
  name,
  description = '',
  databaseId,
  database_id,
  collectionId,
  collection_id,
  theme = 'executive',
  cards,
  filters = [],
  client,
  assistant = null,
  maskPii = true,
}) {
  const dbId = Number(databaseId !== undefined ? databaseId : database_id);
  const collId = collectionId !== undefined ? collectionId : (collection_id !== undefined ? collection_id : null);

  // 1. Validation Pre-Flight
  if (!name || typeof name !== 'string' || !name.trim()) {
    throw new Error('Dashboard name is required');
  }
  if (!dbId || isNaN(dbId)) {
    throw new Error('Valid database_id is required');
  }
  if (!Array.isArray(cards) || cards.length < 4) {
    throw new Error(
      `Autonomous dashboard build requires at least 4 card definitions (received ${Array.isArray(cards) ? cards.length : 0})`
    );
  }

  for (let i = 0; i < cards.length; i++) {
    const c = cards[i];
    if (!c || typeof c !== 'object') {
      throw new Error(`Card specification at index ${i} is invalid`);
    }
    if (!c.name || typeof c.name !== 'string') {
      throw new Error(`Card at index ${i} is missing a valid 'name'`);
    }
    if (!c.sql || typeof c.sql !== 'string') {
      throw new Error(`Card at index ${i} ('${c.name}') is missing a valid 'sql' query`);
    }
  }

  if (!client) {
    throw new Error('Metabase client instance is required to build dashboard');
  }

  logger.info(`Building autonomous dashboard "${name}" with ${cards.length} cards and ${filters.length} filters`);

  // 2. Compute Non-Overlapping 24-Column Grid Positions
  const positions = calculate24ColGridPositions(cards);

  // 3. Prepare Normalized Filter Definitions
  const normalizedFilters = filters.map((f, i) => {
    const filterName = f.name || `Filter ${i + 1}`;
    const slug = f.slug || filterName.toLowerCase().replace(/\s+/g, '_');
    const id = f.id || `param_${slug}_${Math.random().toString(36).substr(2, 6)}`;
    const defaultValue = f.default_value !== undefined ? f.default_value : (f.default !== undefined ? f.default : null);

    return {
      id,
      name: filterName,
      slug,
      type: f.type || 'category',
      sectionId: 'filters',
      default: defaultValue,
      target_variable: f.target_variable || slug,
      field_id: f.field_id || f.fieldId || null,
    };
  });

  // 4. Generate Filter Mappings for Cards
  const filterMappingsPerCard = generateFilterMappings(cards, normalizedFilters);

  // 5. Create Questions / Cards in Metabase
  const createdCards = [];
  for (let i = 0; i < cards.length; i++) {
    const cardSpec = cards[i];
    const display = cardSpec.display || 'table';

    // Parse template tags from SQL for parametric questions
    const tagMatches = (cardSpec.sql.match(/\{\{([a-zA-Z0-9_]+)\}\}/g) || []).map(t => t.replace(/[{}]/g, ''));
    const templateTags = {};

    for (const tag of tagMatches) {
      const matchingFilter = normalizedFilters.find(f => f.slug === tag || f.target_variable === tag);
      templateTags[tag] = {
        id: tag,
        name: tag,
        'display-name': matchingFilter?.name || tag.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
        type: matchingFilter?.type === 'date/all-options' ? 'date' : (matchingFilter?.type || 'text'),
        required: false,
        default: matchingFilter?.default ?? null,
      };
    }

    const questionPayload = {
      name: cardSpec.name,
      description: cardSpec.description || `${cardSpec.name} question for ${name}`,
      database_id: dbId,
      collection_id: collId,
      display: display,
      visualization_settings: cardSpec.visualization_settings || getDefaultVisualizationSettings(display),
      dataset_query: {
        database: dbId,
        type: 'native',
        native: {
          query: cardSpec.sql,
          'template-tags': templateTags,
        },
      },
    };

    let createdCard;
    if (typeof client.createQuestion === 'function') {
      createdCard = await client.createQuestion(questionPayload);
    } else if (typeof client.createSQLQuestion === 'function' && Object.keys(templateTags).length === 0) {
      createdCard = await client.createSQLQuestion(
        cardSpec.name,
        cardSpec.description || '',
        dbId,
        cardSpec.sql,
        collId
      );
    } else if (typeof client.request === 'function') {
      createdCard = await client.request('POST', '/api/card', questionPayload);
    } else {
      throw new Error('Metabase client does not provide card creation methods');
    }

    const cardId = createdCard.id || createdCard.card_id || (i + 100);
    createdCards.push({
      ...createdCard,
      id: cardId,
      name: cardSpec.name,
      display: display,
    });
  }

  // 6. Create Dashboard Entity in Metabase
  const dashboardPayload = {
    name: name,
    description: description || `Autonomous ${theme} dashboard with ${cards.length} cards and ${normalizedFilters.length} interactive filters`,
    collection_id: collId,
    parameters: normalizedFilters.map(f => ({
      id: f.id,
      name: f.name,
      slug: f.slug,
      type: f.type,
      sectionId: 'filters',
      default: f.default,
    })),
  };

  let createdDashboard;
  if (typeof client.createDashboard === 'function') {
    createdDashboard = await client.createDashboard(dashboardPayload);
  } else if (typeof client.request === 'function') {
    createdDashboard = await client.request('POST', '/api/dashboard', dashboardPayload);
  } else {
    throw new Error('Metabase client does not provide dashboard creation methods');
  }

  const dashboardId = createdDashboard.id || 1;

  // Update dashboard parameters if needed
  if (normalizedFilters.length > 0 && typeof client.updateDashboard === 'function') {
    try {
      await client.updateDashboard(dashboardId, {
        parameters: dashboardPayload.parameters,
      });
    } catch (paramErr) {
      logger.debug('Dashboard parameter update notice:', paramErr.message);
    }
  }

  // 7. Attach Cards to Dashboard with Grid Positions and Filter Parameter Mappings
  const attachedCards = [];
  for (let i = 0; i < createdCards.length; i++) {
    const card = createdCards[i];
    const pos = positions[i];
    const mappings = filterMappingsPerCard[i] || [];

    const attachOptions = {
      row: pos.row,
      col: pos.col,
      size_x: pos.size_x,
      size_y: pos.size_y,
      sizeX: pos.size_x,
      sizeY: pos.size_y,
      parameter_mappings: mappings.map(m => ({
        ...m,
        card_id: card.id,
      })),
      visualization_settings: cards[i].visualization_settings || getDefaultVisualizationSettings(card.display),
    };

    let attachResult = null;
    if (typeof client.addCardToDashboard === 'function') {
      try {
        attachResult = await client.addCardToDashboard(dashboardId, card.id, attachOptions);
      } catch (attachErr) {
        logger.warn(`Could not attach card ${card.id} via addCardToDashboard: ${attachErr.message}`);
      }
    } else if (typeof client.request === 'function') {
      try {
        attachResult = await client.request('POST', `/api/dashboard/${dashboardId}/cards`, {
          cardId: card.id,
          row: pos.row,
          col: pos.col,
          size_x: pos.size_x,
          size_y: pos.size_y,
          parameter_mappings: attachOptions.parameter_mappings,
        });
      } catch (attachErr) {
        logger.warn(`Could not attach card ${card.id} via REST endpoint: ${attachErr.message}`);
      }
    }

    attachedCards.push({
      card_id: card.id,
      name: card.name,
      display: card.display || cards[i].display || 'table',
      position: pos,
      parameter_mappings: attachOptions.parameter_mappings,
      sql: cards[i].sql,
    });
  }

  const metabaseBaseUrl = process.env.METABASE_URL || 'http://localhost:3000';
  const dashboardUrl = `${metabaseBaseUrl}/dashboard/${dashboardId}`;

  // 8. Assemble Provenance Envelope
  const _provenance = {
    ai_generated: true,
    tool: 'ai_dashboard_build_full',
    review_required: false,
    timestamp: new Date().toISOString(),
    provider: assistant?.aiProvider || 'heuristic_layout_engine',
    model: assistant?.model || 'dashboard-architect-v1',
    generation_parameters: {
      database_id: dbId,
      collection_id: collId,
      theme: theme,
      card_count: cards.length,
      filter_count: normalizedFilters.length,
      grid_system: '24-column',
      mask_pii: maskPii,
    },
  };

  return {
    dashboard_id: dashboardId,
    name: name,
    description: description || createdDashboard.description || null,
    url: dashboardUrl,
    card_count: attachedCards.length,
    filter_count: normalizedFilters.length,
    cards: attachedCards,
    filters: normalizedFilters.map(f => ({
      id: f.id,
      name: f.name,
      slug: f.slug,
      type: f.type,
      default: f.default,
    })),
    _provenance,
  };
}

/**
 * Dashboard Architect Engine Class
 */
export class DashboardArchitect {
  constructor(config = {}) {
    this.metabaseClient = config.metabaseClient || null;
    this.aiAssistant = config.aiAssistant || null;
  }

  calculateLayout(cards, options) {
    return calculate24ColGridPositions(cards, options);
  }

  generateMappings(cards, filters) {
    return generateFilterMappings(cards, filters);
  }

  async buildDashboard(options) {
    return await buildFullDashboard({
      ...options,
      client: options.client || this.metabaseClient,
      assistant: options.assistant || this.aiAssistant,
    });
  }
}
