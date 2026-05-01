/*******************************************************
 * 06_fetch_objects.gs
 * Получение метаданных объектов BigQuery
 *******************************************************/


/**
 * Возвращает список табличных объектов проекта.
 * Источник:
 * region-<region>.INFORMATION_SCHEMA.TABLES
 *
 * Выход:
 * [
 *   {
 *     project_id,
 *     dataset_name,
 *     table_name,
 *     table_type,
 *     managed_table_type,
 *     is_insertable_into,
 *     is_fine_grained_mutations_enabled,
 *     is_typed,
 *     creation_time,
 *     ddl,
 *     default_collation_name
 *   }
 * ]
 */
function fetchTablesAndViews_() {
  const sql = buildFetchTablesAndViewsSql_();

  const rows = runQuery_(sql, {
    useLegacySql: false,
    location: CONFIG.core.region
  });

  return rows.map(function (row) {
    return normalizeTableObjectRow_(row);
  });
}


/**
 * Возвращает только materialized views.
 * Безопасно: если поле/тип недоступен, просто вернет пустой массив по фильтру.
 */
function fetchMaterializedViews_() {
  return fetchTablesAndViews_().filter(function (row) {
    return String(row.table_type || '').toUpperCase() === CONFIG.objectTypes.materializedView;
  });
}


/**
 * Возвращает только external tables.
 */
function fetchExternalTables_() {
  return fetchTablesAndViews_().filter(function (row) {
    return String(row.table_type || '').toUpperCase() === CONFIG.objectTypes.external;
  });
}


/**
 * Возвращает только snapshots.
 */
function fetchSnapshots_() {
  return fetchTablesAndViews_().filter(function (row) {
    return String(row.table_type || '').toUpperCase() === CONFIG.objectTypes.snapshot;
  });
}


/**
 * Возвращает только clones.
 */
function fetchClones_() {
  return fetchTablesAndViews_().filter(function (row) {
    return String(row.table_type || '').toUpperCase() === CONFIG.objectTypes.clone;
  });
}


/**
 * Возвращает колонки всех объектов проекта.
 * Источник:
 * region-<region>.INFORMATION_SCHEMA.COLUMNS
 */
function fetchColumns_() {
  const sql = buildFetchColumnsSql_();

  const rows = runQuery_(sql, {
    useLegacySql: false,
    location: CONFIG.core.region
  });

  return rows.map(function (row) {
    return normalizeColumnRow_(row);
  });
}


/**
 * Возвращает options таблиц/объектов.
 * Источник:
 * region-<region>.INFORMATION_SCHEMA.TABLE_OPTIONS
 */
function fetchTableOptions_() {
  const sql = buildFetchTableOptionsSql_();

  const rows = runQuery_(sql, {
    useLegacySql: false,
    location: CONFIG.core.region
  });

  return rows.map(function (row) {
    return normalizeTableOptionRow_(row);
  });
}


/**
 * Возвращает definitions для VIEW.
 * Источник:
 * region-<region>.INFORMATION_SCHEMA.VIEWS
 */
function fetchViews_() {
  const sql = buildFetchViewsSql_();

  const rows = runQuery_(sql, {
    useLegacySql: false,
    location: CONFIG.core.region
  });

  return rows.map(function (row) {
    return normalizeViewRow_(row);
  });
}


/**
 * Возвращает routines проекта:
 * functions / procedures / table functions.
 * Источник:
 * region-<region>.INFORMATION_SCHEMA.ROUTINES
 */
function fetchRoutines_() {
  const sql = buildFetchRoutinesSql_();

  const rows = runQuery_(sql, {
    useLegacySql: false,
    location: CONFIG.core.region
  });

  return rows.map(function (row) {
    return normalizeRoutineRow_(row);
  });
}


/**
 * Возвращает storage statistics.
 *
 * Предпочтительно используем INFORMATION_SCHEMA.TABLE_STORAGE.
 * Если по какой-то причине запрос не проходит,
 * делаем fallback на legacy __TABLES__ по датасетам.
 */
function fetchStorageStats_() {
  try {
    const sql = buildFetchTableStorageSql_();

    const rows = runQuery_(sql, {
      useLegacySql: false,
      location: CONFIG.core.region
    });

    return rows.map(function (row) {
      return normalizeStorageRow_(row);
    });
  } catch (error) {
    logWarn_('TABLE_STORAGE недоступен, переключаемся на fallback __TABLES__', error);
    return fetchStorageStatsFromLegacyTables_();
  }
}


/**
 * Fallback storage statistics через dataset.__TABLES__
 */
function fetchStorageStatsFromLegacyTables_() {
  const datasets = fetchDatasets_();
  const datasetNames = extractDatasetNames_(datasets);

  if (!datasetNames.length) {
    return [];
  }

  const sqlParts = datasetNames.map(function (datasetName) {
    return `
      SELECT
        '${CONFIG.core.projectId}' AS project_id,
        '${datasetName}' AS dataset_name,
        table_id AS table_name,
        row_count,
        size_bytes,
        TIMESTAMP_MILLIS(creation_time) AS storage_creation_time,
        TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time,
        '' AS active_logical_bytes,
        '' AS long_term_logical_bytes,
        '' AS active_physical_bytes,
        '' AS long_term_physical_bytes
      FROM \`${CONFIG.core.projectId}.${datasetName}.__TABLES__\`
    `;
  });

  const sql = sqlParts.join('\nUNION ALL\n');

  const rows = runQuery_(sql, {
    useLegacySql: false,
    location: CONFIG.core.region
  });

  return rows.map(function (row) {
    const normalized = normalizeStorageRow_(row);
    normalized.storage_source = 'LEGACY___TABLES__';
    return normalized;
  });
}


/**
 * Строит SQL для TABLES
 */
function buildFetchTablesAndViewsSql_() {
  return `
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset_name,
      table_name,
      table_type,
      managed_table_type,
      is_insertable_into,
      is_fine_grained_mutations_enabled,
      is_typed,
      creation_time,
      ddl,
      default_collation_name
    FROM \`${CONFIG.core.projectId}.region-${CONFIG.core.region}.INFORMATION_SCHEMA.TABLES\`
    ORDER BY table_schema, table_type, table_name
  `;
}


/**
 * Строит SQL для COLUMNS
 */
function buildFetchColumnsSql_() {
  return `
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset_name,
      table_name,
      ordinal_position,
      column_name,
      data_type,
      is_nullable,
      is_hidden,
      is_system_defined,
      is_partitioning_column,
      clustering_ordinal_position,
      collation_name,
      column_default,
      rounding_mode
    FROM \`${CONFIG.core.projectId}.region-${CONFIG.core.region}.INFORMATION_SCHEMA.COLUMNS\`
    ORDER BY table_schema, table_name, ordinal_position
  `;
}


/**
 * Строит SQL для TABLE_OPTIONS
 */
function buildFetchTableOptionsSql_() {
  return `
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset_name,
      table_name,
      option_name,
      option_type,
      option_value
    FROM \`${CONFIG.core.projectId}.region-${CONFIG.core.region}.INFORMATION_SCHEMA.TABLE_OPTIONS\`
    ORDER BY table_schema, table_name, option_name
  `;
}


/**
 * Строит SQL для VIEWS
 */
function buildFetchViewsSql_() {
  return `
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset_name,
      table_name,
      view_definition,
      check_option,
      use_standard_sql
    FROM \`${CONFIG.core.projectId}.region-${CONFIG.core.region}.INFORMATION_SCHEMA.VIEWS\`
    ORDER BY table_schema, table_name
  `;
}


/**
 * Строит SQL для ROUTINES
 */
function buildFetchRoutinesSql_() {
  return `
    SELECT
      specific_catalog AS project_id,
      specific_schema AS dataset_name,
      specific_name AS routine_name,
      routine_catalog,
      routine_schema,
      routine_name AS routine_display_name,
      routine_type,
      data_type,
      routine_body,
      external_language,
      is_deterministic,
      security_type,
      created,
      last_altered,
      ddl
    FROM \`${CONFIG.core.projectId}.region-${CONFIG.core.region}.INFORMATION_SCHEMA.ROUTINES\`
    ORDER BY specific_schema, routine_name
  `;
}


/**
 * Строит SQL для TABLE_STORAGE
 */
function buildFetchTableStorageSql_() {
  return `
    SELECT
      table_catalog AS project_id,
      table_schema AS dataset_name,
      table_name,
      row_count,
      total_logical_bytes AS size_bytes,
      creation_time AS storage_creation_time,
      last_modified_time,
      active_logical_bytes,
      long_term_logical_bytes,
      active_physical_bytes,
      long_term_physical_bytes
    FROM \`${CONFIG.core.projectId}.region-${CONFIG.core.region}.INFORMATION_SCHEMA.TABLE_STORAGE\`
    ORDER BY table_schema, table_name
  `;
}


/**
 * Нормализация строки объекта TABLES
 */
function normalizeTableObjectRow_(row) {
  return {
    project_id: normalizeStringSafe_(row.project_id),
    dataset_name: normalizeStringSafe_(row.dataset_name),
    table_name: normalizeStringSafe_(row.table_name),
    table_type: normalizeStringSafe_(row.table_type),
    managed_table_type: normalizeStringSafe_(row.managed_table_type),
    is_insertable_into: normalizeStringSafe_(row.is_insertable_into),
    is_fine_grained_mutations_enabled: normalizeStringSafe_(row.is_fine_grained_mutations_enabled),
    is_typed: normalizeStringSafe_(row.is_typed),
    creation_time: normalizeDateTimeSafe_(row.creation_time),
    ddl: normalizeStringSafe_(row.ddl),
    default_collation_name: normalizeStringSafe_(row.default_collation_name)
  };
}


/**
 * Нормализация строки колонки
 */
function normalizeColumnRow_(row) {
  return {
    project_id: normalizeStringSafe_(row.project_id),
    dataset_name: normalizeStringSafe_(row.dataset_name),
    table_name: normalizeStringSafe_(row.table_name),
    ordinal_position: normalizeNumberSafe_(row.ordinal_position),
    column_name: normalizeStringSafe_(row.column_name),
    data_type: normalizeStringSafe_(row.data_type),
    is_nullable: normalizeStringSafe_(row.is_nullable),
    is_hidden: normalizeStringSafe_(row.is_hidden),
    is_system_defined: normalizeStringSafe_(row.is_system_defined),
    is_partitioning_column: normalizeStringSafe_(row.is_partitioning_column),
    clustering_ordinal_position: normalizeNumberSafe_(row.clustering_ordinal_position),
    collation_name: normalizeStringSafe_(row.collation_name),
    column_default: normalizeUnknownValueSafe_(row.column_default),
    rounding_mode: normalizeStringSafe_(row.rounding_mode)
  };
}


/**
 * Нормализация строки option
 */
function normalizeTableOptionRow_(row) {
  return {
    project_id: normalizeStringSafe_(row.project_id),
    dataset_name: normalizeStringSafe_(row.dataset_name),
    table_name: normalizeStringSafe_(row.table_name),
    option_name: normalizeStringSafe_(row.option_name),
    option_type: normalizeStringSafe_(row.option_type),
    option_value: normalizeUnknownValueSafe_(row.option_value)
  };
}


/**
 * Нормализация VIEW row
 */
function normalizeViewRow_(row) {
  return {
    project_id: normalizeStringSafe_(row.project_id),
    dataset_name: normalizeStringSafe_(row.dataset_name),
    table_name: normalizeStringSafe_(row.table_name),
    view_definition: normalizeStringSafe_(row.view_definition),
    check_option: normalizeStringSafe_(row.check_option),
    use_standard_sql: normalizeStringSafe_(row.use_standard_sql)
  };
}


/**
 * Нормализация ROUTINE row
 */
function normalizeRoutineRow_(row) {
  return {
    project_id: normalizeStringSafe_(row.project_id),
    dataset_name: normalizeStringSafe_(row.dataset_name),
    routine_name: normalizeStringSafe_(row.routine_name),
    routine_catalog: normalizeStringSafe_(row.routine_catalog),
    routine_schema: normalizeStringSafe_(row.routine_schema),
    routine_display_name: normalizeStringSafe_(row.routine_display_name),
    routine_type: normalizeStringSafe_(row.routine_type),
    data_type: normalizeStringSafe_(row.data_type),
    routine_body: normalizeStringSafe_(row.routine_body),
    external_language: normalizeStringSafe_(row.external_language),
    is_deterministic: normalizeStringSafe_(row.is_deterministic),
    security_type: normalizeStringSafe_(row.security_type),
    created: normalizeDateTimeSafe_(row.created),
    last_altered: normalizeDateTimeSafe_(row.last_altered),
    ddl: normalizeStringSafe_(row.ddl)
  };
}


/**
 * Нормализация storage row
 */
function normalizeStorageRow_(row) {
  return {
    project_id: normalizeStringSafe_(row.project_id),
    dataset_name: normalizeStringSafe_(row.dataset_name),
    table_name: normalizeStringSafe_(row.table_name),
    row_count: normalizeNumberSafe_(row.row_count),
    size_bytes: normalizeNumberSafe_(row.size_bytes),
    storage_creation_time: normalizeDateTimeSafe_(row.storage_creation_time),
    last_modified_time: normalizeDateTimeSafe_(row.last_modified_time),
    active_logical_bytes: normalizeNumberSafe_(row.active_logical_bytes),
    long_term_logical_bytes: normalizeNumberSafe_(row.long_term_logical_bytes),
    active_physical_bytes: normalizeNumberSafe_(row.active_physical_bytes),
    long_term_physical_bytes: normalizeNumberSafe_(row.long_term_physical_bytes),
    storage_source: 'INFORMATION_SCHEMA.TABLE_STORAGE'
  };
}


/**
 * Map объектов по ключу project.dataset.table
 */
function buildObjectsMap_(objects) {
  const items = Array.isArray(objects) ? objects : [];
  const map = {};

  items.forEach(function (row) {
    const key = buildObjectKey_(row.project_id, row.dataset_name, row.table_name);
    map[key] = row;
  });

  return map;
}


/**
 * Map storage по ключу project.dataset.table
 */
function buildStorageMapFromRows_(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const map = {};

  items.forEach(function (row) {
    const key = buildObjectKey_(row.project_id, row.dataset_name, row.table_name);
    map[key] = row;
  });

  return map;
}


/**
 * Группировка колонок по объекту.
 * {
 *   "project.dataset.table": [ ...columns ]
 * }
 */
function buildColumnsGroupedMap_(columns) {
  const items = Array.isArray(columns) ? columns : [];
  const map = {};

  items.forEach(function (row) {
    const key = buildObjectKey_(row.project_id, row.dataset_name, row.table_name);

    if (!map[key]) {
      map[key] = [];
    }

    map[key].push(row);
  });

  Object.keys(map).forEach(function (key) {
    map[key].sort(function (a, b) {
      return Number(a.ordinal_position || 0) - Number(b.ordinal_position || 0);
    });
  });

  return map;
}


/**
 * Группировка options по объекту.
 * {
 *   "project.dataset.table": [ ...options ]
 * }
 */
function buildTableOptionsGroupedMap_(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const map = {};

  items.forEach(function (row) {
    const key = buildObjectKey_(row.project_id, row.dataset_name, row.table_name);

    if (!map[key]) {
      map[key] = [];
    }

    map[key].push(row);
  });

  return map;
}


/**
 * Map view definitions по объекту
 */
function buildViewsMap_(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const map = {};

  items.forEach(function (row) {
    const key = buildObjectKey_(row.project_id, row.dataset_name, row.table_name);
    map[key] = row;
  });

  return map;
}


/**
 * Map routines по ключу project.dataset.routine
 */
function buildRoutinesMap_(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const map = {};

  items.forEach(function (row) {
    const key = buildObjectKey_(row.project_id, row.dataset_name, row.routine_name);
    map[key] = row;
  });

  return map;
}


/**
 * Возвращает summary по объектам
 */
function buildObjectsFetchSummary_(objects) {
  const items = Array.isArray(objects) ? objects : [];

  const summary = {
    total_count: items.length,
    base_table_count: 0,
    view_count: 0,
    materialized_view_count: 0,
    external_count: 0,
    snapshot_count: 0,
    clone_count: 0,
    other_count: 0
  };

  items.forEach(function (row) {
    const type = String(row.table_type || '').toUpperCase();

    if (type === CONFIG.objectTypes.baseTable) summary.base_table_count += 1;
    else if (type === CONFIG.objectTypes.view) summary.view_count += 1;
    else if (type === CONFIG.objectTypes.materializedView) summary.materialized_view_count += 1;
    else if (type === CONFIG.objectTypes.external) summary.external_count += 1;
    else if (type === CONFIG.objectTypes.snapshot) summary.snapshot_count += 1;
    else if (type === CONFIG.objectTypes.clone) summary.clone_count += 1;
    else summary.other_count += 1;
  });

  return summary;
}


/**
 * Безопасная нормализация чисел
 */
function normalizeNumberSafe_(value) {
  if (value === null || value === undefined || value === '') {
    return '';
  }

  const n = Number(value);
  return isNaN(n) ? '' : n;
}


/**
 * Нормализация сложного/неизвестного значения
 */
function normalizeUnknownValueSafe_(value) {
  if (value === null || value === undefined) {
    return '';
  }

  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch (error) {
      return String(value);
    }
  }

  return value;
}
