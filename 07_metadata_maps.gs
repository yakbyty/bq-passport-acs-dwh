/*******************************************************
 * 07_metadata_maps.gs
 * Сборка metadata maps / grouped maps для enrichment слоя
 *******************************************************/


/**
 * Главная функция сборки всех metadata maps.
 *
 * input = {
 *   datasets,
 *   objects,
 *   columns,
 *   tableOptions,
 *   views,
 *   storageStats,
 *   materializedViews,
 *   routines,
 *   manualMetadata,
 *   manualDependencies,
 *   qualityRules,
 *   objectSlaMap,
 *   objectOwnersMap
 * }
 */
function buildMetadataMaps_(input) {
  const payload = input || {};

  const datasets = asArraySafe_(payload.datasets);
  const objects = asArraySafe_(payload.objects);
  const columns = asArraySafe_(payload.columns);
  const tableOptions = asArraySafe_(payload.tableOptions);
  const views = asArraySafe_(payload.views);
  const storageStats = asArraySafe_(payload.storageStats);
  const materializedViews = asArraySafe_(payload.materializedViews);
  const routines = asArraySafe_(payload.routines);
  const manualDependencies = asArraySafe_(payload.manualDependencies);
  const qualityRules = asArraySafe_(payload.qualityRules);

  const manualMetadata = asObjectSafe_(payload.manualMetadata);
  const objectSlaMap = asObjectSafe_(payload.objectSlaMap);
  const objectOwnersMap = asObjectSafe_(payload.objectOwnersMap);

  const datasetsMap = buildDatasetsMap_(datasets);
  const objectsMap = buildObjectsMap_(objects);

  const columnsMap = buildColumnsGroupedMap_(columns);
  const tableOptionsGroupedMap = buildTableOptionsGroupedMap_(tableOptions);
  const tableOptionsFlatMap = buildTableOptionsFlatMap_(tableOptions);
  const viewsMap = buildViewsMap_(views);
  const storageMap = buildStorageMapFromRows_(storageStats);
  const materializedViewsMap = buildMaterializedViewsMap_(materializedViews);
  const routinesMap = buildRoutinesMap_(routines);

  const manualDependenciesGroupedMap = buildManualDependenciesGroupedMap_(manualDependencies);
  const qualityRulesGroupedMap = buildQualityRulesGroupedMap_(qualityRules);

  const columnsStatsMap = buildColumnsStatsMap_(columnsMap);
  const objectFlagsMap = buildObjectFlagsMap_(objectsMap, viewsMap, storageMap, tableOptionsFlatMap);

  return {
    datasetsMap: datasetsMap,
    objectsMap: objectsMap,

    columnsMap: columnsMap,
    columnsStatsMap: columnsStatsMap,

    tableOptionsGroupedMap: tableOptionsGroupedMap,
    tableOptionsFlatMap: tableOptionsFlatMap,

    viewsMap: viewsMap,
    storageMap: storageMap,
    materializedViewsMap: materializedViewsMap,
    routinesMap: routinesMap,

    manualMetadataMap: manualMetadata,
    manualDependenciesGroupedMap: manualDependenciesGroupedMap,
    qualityRulesGroupedMap: qualityRulesGroupedMap,
    objectSlaMap: objectSlaMap,
    objectOwnersMap: objectOwnersMap,

    objectFlagsMap: objectFlagsMap
  };
}


/**
 * Map датасетов по ключу project.dataset
 */
function buildDatasetsMap_(datasets) {
  const items = asArraySafe_(datasets);
  const map = {};

  items.forEach(function (row) {
    const key = buildDatasetKey_(
      row.project_id,
      row.dataset_name
    );
    map[key] = row;
  });

  return map;
}


/**
 * Плоский map table options:
 * {
 *   "project.dataset.table": {
 *      description: "...",
 *      partitioning_type: "...",
 *      partitioning_field: "...",
 *      clustering_fields: "...",
 *      labels: "...",
 *      expiration_timestamp: "..."
 *   }
 * }
 */
function buildTableOptionsFlatMap_(rows) {
  const items = asArraySafe_(rows);
  const map = {};

  items.forEach(function (row) {
    const key = buildObjectKey_(
      row.project_id,
      row.dataset_name,
      row.table_name
    );

    if (!map[key]) {
      map[key] = {};
    }

    const optionName = String(row.option_name || '').trim();
    if (!optionName) {
      return;
    }

    map[key][optionName] = row.option_value !== undefined
      ? row.option_value
      : '';
  });

  return map;
}


/**
 * Map materialized views по ключу project.dataset.table
 */
function buildMaterializedViewsMap_(rows) {
  const items = asArraySafe_(rows);
  const map = {};

  items.forEach(function (row) {
    const key = buildObjectKey_(
      row.project_id,
      row.dataset_name,
      row.table_name
    );
    map[key] = row;
  });

  return map;
}


/**
 * Группировка manual dependencies по ключу объекта.
 * {
 *   "project.dataset.table": [ ...deps ]
 * }
 *
 * Поддерживает старый формат dataset.table и новый project.dataset.table.
 */
function buildManualDependenciesGroupedMap_(rows) {
  const items = asArraySafe_(rows);
  const map = {};

  items.forEach(function (row) {
    const key = resolveManualObjectKey_(row);
    if (!key) {
      return;
    }

    if (!map[key]) {
      map[key] = [];
    }

    map[key].push({
      object_key: key,
      dataset_name: normalizePlainString_(row.dataset_name),
      table_name: normalizePlainString_(row.table_name),
      depends_on: normalizePlainString_(row.depends_on),
      dependency_type: normalizePlainString_(row.dependency_type),
      refresh_mechanism: normalizePlainString_(row.refresh_mechanism),
      comment: normalizePlainString_(row.comment)
    });
  });

  return map;
}


/**
 * Группировка quality rules по ключу объекта.
 * {
 *   "project.dataset.table": [ ...rules ]
 * }
 *
 * Поддерживает старый формат dataset.table и новый project.dataset.table.
 */
function buildQualityRulesGroupedMap_(rows) {
  const items = asArraySafe_(rows);
  const map = {};

  items.forEach(function (row) {
    const key = resolveManualObjectKey_(row);
    if (!key) {
      return;
    }

    if (!map[key]) {
      map[key] = [];
    }

    map[key].push({
      object_key: key,
      dataset_name: normalizePlainString_(row.dataset_name),
      table_name: normalizePlainString_(row.table_name),
      check_name: normalizePlainString_(row.check_name),
      check_sql: row.check_sql !== undefined ? row.check_sql : '',
      severity: normalizePlainString_(row.severity),
      is_enabled: row.is_enabled
    });
  });

  return map;
}


/**
 * Статистика колонок по каждому объекту.
 * {
 *   "project.dataset.table": {
 *      columns_count,
 *      has_partition_column,
 *      has_date_candidate,
 *      has_timestamp_candidate,
 *      column_names,
 *      columns_list
 *   }
 * }
 */
function buildColumnsStatsMap_(columnsMap) {
  const map = {};
  const keys = Object.keys(asObjectSafe_(columnsMap));

  keys.forEach(function (objectKey) {
    const cols = asArraySafe_(columnsMap[objectKey]);

    const columnNames = cols.map(function (col) {
      return String(col.column_name || '').trim();
    });

    const columnsList = cols.map(function (col) {
      return String(col.column_name || '').trim() +
        ' (' + String(col.data_type || '').trim() + ')';
    });

    const hasPartitionColumn = cols.some(function (col) {
      return String(col.is_partitioning_column || '').toUpperCase() === 'YES';
    });

    const hasDateCandidate = cols.some(function (col) {
      const t = String(col.data_type || '').toUpperCase();
      return t === 'DATE' || t === 'DATETIME' || t === 'TIMESTAMP';
    });

    const hasTimestampCandidate = cols.some(function (col) {
      const t = String(col.data_type || '').toUpperCase();
      return t === 'TIMESTAMP' || t === 'DATETIME';
    });

    map[objectKey] = {
      columns_count: cols.length,
      has_partition_column: hasPartitionColumn,
      has_date_candidate: hasDateCandidate,
      has_timestamp_candidate: hasTimestampCandidate,
      column_names: columnNames,
      columns_list: columnsList.join(' | ')
    };
  });

  return map;
}


/**
 * Признаки объекта, собранные из нескольких слоев.
 * Это не бизнес-оценка, а технические флаги.
 *
 * {
 *   "project.dataset.table": {
 *      has_view_definition,
 *      has_storage_stats,
 *      has_options,
 *      has_description,
 *      has_partitioning,
 *      has_clustering
 *   }
 * }
 */
function buildObjectFlagsMap_(objectsMap, viewsMap, storageMap, tableOptionsFlatMap) {
  const objectMapSafe = asObjectSafe_(objectsMap);
  const viewMapSafe = asObjectSafe_(viewsMap);
  const storageMapSafe = asObjectSafe_(storageMap);
  const optionsMapSafe = asObjectSafe_(tableOptionsFlatMap);

  const keys = Object.keys(objectMapSafe);
  const map = {};

  keys.forEach(function (objectKey) {
    const options = optionsMapSafe[objectKey] || {};
    const description = normalizeUnknownToString_(options.description);
    const partitioningType = normalizeUnknownToString_(options.partitioning_type);
    const partitioningField = normalizeUnknownToString_(options.partitioning_field);
    const clusteringFields = normalizeUnknownToString_(options.clustering_fields);

    map[objectKey] = {
      has_view_definition: !!viewMapSafe[objectKey],
      has_storage_stats: !!storageMapSafe[objectKey],
      has_options: !!optionsMapSafe[objectKey],
      has_description: !!description,
      has_partitioning: !!partitioningType || !!partitioningField,
      has_clustering: !!clusteringFields
    };
  });

  return map;
}


/**
 * Возвращает object key для manual row.
 * Поддерживает 2 формата:
 * 1. row.object_key = project.dataset.table
 * 2. dataset_name + table_name
 */
function resolveManualObjectKey_(row) {
  const explicitKey = normalizePlainString_(row && row.object_key);
  if (explicitKey) {
    return explicitKey;
  }

  const datasetName = normalizePlainString_(row && row.dataset_name);
  const tableName = normalizePlainString_(row && row.table_name);

  if (!datasetName || !tableName) {
    return '';
  }

  return buildObjectKey_(CONFIG.core.projectId, datasetName, tableName);
}


/**
 * Возвращает количество элементов в каждой map-структуре.
 * Полезно для диагностики.
 */
function buildMetadataMapsSummary_(maps) {
  const safeMaps = asObjectSafe_(maps);

  return {
    datasetsMap_count: getObjectSizeSafe_(safeMaps.datasetsMap),
    objectsMap_count: getObjectSizeSafe_(safeMaps.objectsMap),
    columnsMap_count: getObjectSizeSafe_(safeMaps.columnsMap),
    columnsStatsMap_count: getObjectSizeSafe_(safeMaps.columnsStatsMap),
    tableOptionsGroupedMap_count: getObjectSizeSafe_(safeMaps.tableOptionsGroupedMap),
    tableOptionsFlatMap_count: getObjectSizeSafe_(safeMaps.tableOptionsFlatMap),
    viewsMap_count: getObjectSizeSafe_(safeMaps.viewsMap),
    storageMap_count: getObjectSizeSafe_(safeMaps.storageMap),
    materializedViewsMap_count: getObjectSizeSafe_(safeMaps.materializedViewsMap),
    routinesMap_count: getObjectSizeSafe_(safeMaps.routinesMap),
    manualMetadataMap_count: getObjectSizeSafe_(safeMaps.manualMetadataMap),
    manualDependenciesGroupedMap_count: getObjectSizeSafe_(safeMaps.manualDependenciesGroupedMap),
    qualityRulesGroupedMap_count: getObjectSizeSafe_(safeMaps.qualityRulesGroupedMap),
    objectSlaMap_count: getObjectSizeSafe_(safeMaps.objectSlaMap),
    objectOwnersMap_count: getObjectSizeSafe_(safeMaps.objectOwnersMap),
    objectFlagsMap_count: getObjectSizeSafe_(safeMaps.objectFlagsMap)
  };
}


/**
 * Безопасное приведение к массиву
 */
function asArraySafe_(value) {
  return Array.isArray(value) ? value : [];
}


/**
 * Безопасное приведение к plain object
 */
function asObjectSafe_(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value;
}


/**
 * Безопасная нормализация обычной строки
 */
function normalizePlainString_(value) {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value).trim();
}


/**
 * Безопасная строковая нормализация сложного значения
 */
function normalizeUnknownToString_(value) {
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

  return String(value).trim();
}


/**
 * Размер объекта
 */
function getObjectSizeSafe_(obj) {
  return Object.keys(asObjectSafe_(obj)).length;
}
