/*******************************************************
 * 05_fetch_datasets.gs
 * Получение метаданных датасетов BigQuery
 *******************************************************/


/**
 * Возвращает список датасетов проекта.
 * Источник:
 * region-<region>.INFORMATION_SCHEMA.SCHEMATA
 *
 * Выход:
 * [
 *   {
 *     project_id,
 *     dataset_name,
 *     location,
 *     creation_time,
 *     last_modified_time,
 *     default_collation_name,
 *     dataset_ddl
 *   }
 * ]
 */
function fetchDatasets_() {
  const sql = buildFetchDatasetsSql_();

  const rows = runQuery_(sql, {
    useLegacySql: false,
    location: CONFIG.core.region
  });

  return rows.map(function (row) {
    return normalizeDatasetRow_(row);
  });
}


/**
 * Возвращает map датасетов по ключу project.dataset
 *
 * {
 *   "acs-dwh.analytics_demand": { ...datasetRow }
 * }
 */
function fetchDatasetsMap_() {
  const datasets = fetchDatasets_();
  const map = {};

  datasets.forEach(function (datasetRow) {
    const key = buildDatasetKey_(datasetRow.project_id, datasetRow.dataset_name);
    map[key] = datasetRow;
  });

  return map;
}


/**
 * Возвращает краткую статистику по датасетам.
 */
function buildDatasetsSummary_(datasets) {
  const items = Array.isArray(datasets) ? datasets : [];

  const summary = {
    datasets_count: items.length,
    datasets_with_location_count: 0,
    datasets_without_location_count: 0,
    datasets_with_ddl_count: 0
  };

  items.forEach(function (item) {
    if (String(item.location || '').trim()) {
      summary.datasets_with_location_count += 1;
    } else {
      summary.datasets_without_location_count += 1;
    }

    if (String(item.dataset_ddl || '').trim()) {
      summary.datasets_with_ddl_count += 1;
    }
  });

  return summary;
}


/**
 * Строит SQL для получения списка датасетов.
 */
function buildFetchDatasetsSql_() {
  return `
    SELECT
      catalog_name AS project_id,
      schema_name AS dataset_name,
      location,
      creation_time,
      last_modified_time,
      default_collation_name,
      ddl
    FROM \`${CONFIG.core.projectId}.region-${CONFIG.core.region}.INFORMATION_SCHEMA.SCHEMATA\`
    ORDER BY schema_name
  `;
}


/**
 * Нормализует одну строку датасета.
 */
function normalizeDatasetRow_(row) {
  return {
    project_id: normalizeStringSafe_(row.project_id),
    dataset_name: normalizeStringSafe_(row.dataset_name),
    location: normalizeStringSafe_(row.location),
    creation_time: normalizeDateTimeSafe_(row.creation_time),
    last_modified_time: normalizeDateTimeSafe_(row.last_modified_time),
    default_collation_name: normalizeStringSafe_(row.default_collation_name),
    dataset_ddl: normalizeStringSafe_(row.ddl)
  };
}


/**
 * Возвращает ключ датасета project.dataset
 */
function buildDatasetKey_(projectId, datasetName) {
  return [
    String(projectId || '').trim(),
    String(datasetName || '').trim()
  ].join('.');
}


/**
 * Возвращает только имена датасетов.
 */
function extractDatasetNames_(datasets) {
  const items = Array.isArray(datasets) ? datasets : [];

  return items
    .map(function (item) {
      return String(item.dataset_name || '').trim();
    })
    .filter(function (name) {
      return !!name;
    });
}


/**
 * Фильтрация датасетов по префиксу.
 */
function filterDatasetsByPrefix_(datasets, prefix) {
  const items = Array.isArray(datasets) ? datasets : [];
  const safePrefix = String(prefix || '').trim().toLowerCase();

  if (!safePrefix) {
    return items.slice();
  }

  return items.filter(function (item) {
    return String(item.dataset_name || '')
      .toLowerCase()
      .indexOf(safePrefix) === 0;
  });
}


/**
 * Фильтрация датасетов по include/exclude правилам.
 *
 * options = {
 *   includePrefixes: ['analytics_', 'salesdrive_'],
 *   excludeNames: ['tmp_dataset']
 * }
 */
function filterDatasetsByRules_(datasets, options) {
  const items = Array.isArray(datasets) ? datasets : [];
  const opts = options || {};

  const includePrefixes = Array.isArray(opts.includePrefixes)
    ? opts.includePrefixes.map(function (v) { return String(v || '').toLowerCase(); })
    : [];

  const excludeNames = Array.isArray(opts.excludeNames)
    ? opts.excludeNames.map(function (v) { return String(v || '').toLowerCase(); })
    : [];

  return items.filter(function (item) {
    const datasetName = String(item.dataset_name || '').toLowerCase();

    const included = !includePrefixes.length || includePrefixes.some(function (prefix) {
      return datasetName.indexOf(prefix) === 0;
    });

    const excluded = excludeNames.indexOf(datasetName) !== -1;

    return included && !excluded;
  });
}


/**
 * Сортировка датасетов по имени.
 */
function sortDatasetsByName_(datasets) {
  const items = Array.isArray(datasets) ? datasets.slice() : [];

  items.sort(function (a, b) {
    return String(a.dataset_name || '').localeCompare(String(b.dataset_name || ''));
  });

  return items;
}


/**
 * Безопасная нормализация строки.
 */
function normalizeStringSafe_(value) {
  if (value === null || value === undefined) return '';
  return String(value).trim();
}


/**
 * Безопасная нормализация даты/времени.
 * Оставляем как строку, пригодную для записи в Sheets.
 */
function normalizeDateTimeSafe_(value) {
  if (value === null || value === undefined || value === '') {
    return '';
  }

  if (value instanceof Date) {
    return formatProjectDateTime_(value);
  }

  const parsed = new Date(value);
  if (!isNaN(parsed.getTime())) {
    return formatProjectDateTime_(parsed);
  }

  return String(value);
}
