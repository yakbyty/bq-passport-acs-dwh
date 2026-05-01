/*******************************************************
 * 08_enrichment.gs
 * Обогащение объектных метаданных:
 * layer, source, purpose, freshness params, keys, risks
 *******************************************************/


/**
 * Главная функция enrichment.
 *
 * objects: массив строк из fetchTablesAndViews_()
 * metadataMaps: результат buildMetadataMaps_()
 *
 * Возвращает enriched objects для листа "Объекты"
 */
function enrichObjects_(objects, metadataMaps) {
  const items = Array.isArray(objects) ? objects : [];
  const maps = metadataMaps || {};

  return items.map(function (obj) {
    return enrichSingleObject_(obj, maps);
  });
}


/**
 * Обогащение одного объекта
 */
function enrichSingleObject_(obj, metadataMaps) {
  const object = obj || {};
  const maps = metadataMaps || {};

  const objectKey = buildObjectKey_(
    object.project_id,
    object.dataset_name,
    object.table_name
  );

  const columns = getColumnsForObject_(maps, objectKey);
  const columnsStats = getColumnsStatsForObject_(maps, objectKey);
  const optionsFlat = getTableOptionsFlatForObject_(maps, objectKey);
  const viewRow = getViewForObject_(maps, objectKey);
  const storageRow = getStorageForObject_(maps, objectKey);
  const manualMeta = getManualMetadataForObject_(maps, objectKey);
  const objectSla = getObjectSlaForObject_(maps, objectKey);
  const objectOwner = getObjectOwnerForObject_(maps, objectKey);
  const objectFlags = getObjectFlagsForObject_(maps, objectKey);

  const dateCandidate = detectDateFieldCandidate_(columns);

  const description = firstNonEmpty_([
    normalizeUnknownToString_(optionsFlat.description),
    normalizePlainString_(manualMeta.description),
    ''
  ]);

  const partitioningType = normalizeUnknownToString_(optionsFlat.partitioning_type);
  const partitioningField = normalizeUnknownToString_(optionsFlat.partitioning_field);
  const clusteringFields = normalizeUnknownToString_(optionsFlat.clustering_fields);

  const rowCount = normalizeNumberSafe_(storageRow.row_count);
  const sizeBytes = normalizeNumberSafe_(storageRow.size_bytes);

  const actualLastUpdate = firstNonEmpty_([
    normalizePlainString_(storageRow.last_modified_time),
    normalizePlainString_(object.last_modified_time),
    normalizePlainString_(object.creation_time)
  ]);

  const layer = inferLayer_(object, columns, optionsFlat, viewRow);
  const source = inferSource_(object, columns, optionsFlat, viewRow);
  const purpose = inferPurpose_(object, columns, optionsFlat, viewRow);
  const updateType = inferUpdateType_(object, layer, source, purpose);
  const refreshMechanism = inferRefreshMechanism_(object, layer, source, purpose, manualMeta);
  const expectedRefreshFrequency = inferExpectedRefreshFrequency_(object, manualMeta, objectSla);
  const freshnessThresholdHours = inferFreshnessThresholdHours_(object, manualMeta, objectSla);

  const businessCriticality = inferBusinessCriticality_(object, purpose, manualMeta, objectSla);
  const managementPriority = inferManagementPriority_(object, purpose, manualMeta, objectSla);

  const primaryKeyCandidate = firstNonEmpty_([
    normalizePlainString_(manualMeta.primary_key_candidate),
    inferPrimaryKeyCandidate_(columns, dateCandidate)
  ]);

  const joinKeys = firstNonEmpty_([
    normalizePlainString_(manualMeta.join_keys),
    inferJoinKeys_(columns)
  ]);

  const grain = firstNonEmpty_([
    normalizePlainString_(manualMeta.grain),
    inferGrain_(object, columns, purpose, dateCandidate)
  ]);

  const businessDefinition = firstNonEmpty_([
    normalizePlainString_(manualMeta.business_definition),
    inferBusinessDefinition_(object, purpose)
  ]);

  const allowedUse = firstNonEmpty_([
    normalizePlainString_(manualMeta.allowed_use),
    CONFIG.defaults.allowedUse
  ]);

  const notRecommendedUse = firstNonEmpty_([
    normalizePlainString_(manualMeta.not_recommended_use),
    CONFIG.defaults.notRecommendedUse
  ]);

  const incidentIfBroken = firstNonEmpty_([
    normalizePlainString_(manualMeta.incident_if_broken),
    CONFIG.defaults.incidentIfBroken
  ]);

  const riskFlag = inferRiskFlag_({
    object: object,
    description: description,
    actualLastUpdate: actualLastUpdate,
    sizeBytes: sizeBytes,
    columnsCount: columnsStats.columns_count
  });

  const scanRisk = inferScanRisk_({
    object: object,
    sizeBytes: sizeBytes,
    partitioningType: partitioningType,
    partitioningField: partitioningField,
    clusteringFields: clusteringFields
  });

  return {
    object_key: objectKey,

    project_id: normalizePlainString_(object.project_id),
    dataset_name: normalizePlainString_(object.dataset_name),
    table_name: normalizePlainString_(object.table_name),
    table_type: normalizePlainString_(object.table_type),
    managed_table_type: normalizePlainString_(object.managed_table_type),

    description: description,
    creation_time: normalizePlainString_(object.creation_time),
    last_modified_time: normalizePlainString_(storageRow.last_modified_time || object.last_modified_time),

    row_count: rowCount,
    size_bytes: sizeBytes,
    size_mb: sizeBytes !== '' ? roundBytesToMb_(sizeBytes) : '',
    size_gb: sizeBytes !== '' ? roundBytesToGb_(sizeBytes) : '',

    partitioning_type: partitioningType,
    partitioning_field: partitioningField,
    clustering_fields: clusteringFields,

    columns_count: columnsStats.columns_count,
    columns_list: columnsStats.columns_list,

    layer: layer,
    source: source,
    purpose: purpose,
    update_type: updateType,
    refresh_mechanism: refreshMechanism,
    expected_refresh_frequency: expectedRefreshFrequency,
    freshness_threshold_hours: freshnessThresholdHours,
    actual_last_update: actualLastUpdate,
    freshness_status: '',
    freshness_comment: '',

    business_criticality: businessCriticality,
    management_priority: managementPriority,

    grain: grain,
    primary_key_candidate: primaryKeyCandidate,
    join_keys: joinKeys,
    business_definition: businessDefinition,
    allowed_use: allowedUse,
    not_recommended_use: notRecommendedUse,
    incident_if_broken: incidentIfBroken,

    quality_status: '',
    quality_issues_count: 0,

    scan_risk: scanRisk,
    risk_flag: riskFlag,

    date_field_candidate: dateCandidate ? normalizePlainString_(dateCandidate.column_name) : '',
    date_field_type: dateCandidate ? normalizePlainString_(dateCandidate.data_type) : '',
    period_min: '',
    period_max: '',
    period_row_count_checked: '',

    owner: firstNonEmpty_([
      normalizePlainString_(objectOwner.owner),
      normalizePlainString_(manualMeta.owner),
      CONFIG.defaults.owner
    ]),

    sla_tier: firstNonEmpty_([
      normalizePlainString_(objectSla.sla_tier),
      normalizePlainString_(manualMeta.sla_tier),
      CONFIG.defaults.slaTier
    ]),

    has_view_definition: !!objectFlags.has_view_definition,
    has_storage_stats: !!objectFlags.has_storage_stats,
    has_options: !!objectFlags.has_options,
    has_description: !!objectFlags.has_description,
    has_partitioning: !!objectFlags.has_partitioning,
    has_clustering: !!objectFlags.has_clustering,

    ddl: normalizePlainString_(object.ddl)
  };
}


/**
 * Получить колонки объекта
 */
function getColumnsForObject_(metadataMaps, objectKey) {
  const map = asObjectSafe_(metadataMaps.columnsMap);
  return asArraySafe_(map[objectKey]);
}


/**
 * Получить stats колонок объекта
 */
function getColumnsStatsForObject_(metadataMaps, objectKey) {
  const map = asObjectSafe_(metadataMaps.columnsStatsMap);
  return map[objectKey] || {
    columns_count: 0,
    has_partition_column: false,
    has_date_candidate: false,
    has_timestamp_candidate: false,
    column_names: [],
    columns_list: ''
  };
}


/**
 * Получить flat options объекта
 */
function getTableOptionsFlatForObject_(metadataMaps, objectKey) {
  const map = asObjectSafe_(metadataMaps.tableOptionsFlatMap);
  return asObjectSafe_(map[objectKey]);
}


/**
 * Получить view row объекта
 */
function getViewForObject_(metadataMaps, objectKey) {
  const map = asObjectSafe_(metadataMaps.viewsMap);
  return map[objectKey] || {};
}


/**
 * Получить storage row объекта
 */
function getStorageForObject_(metadataMaps, objectKey) {
  const map = asObjectSafe_(metadataMaps.storageMap);
  return map[objectKey] || {};
}


/**
 * Получить manual metadata объекта
 */
function getManualMetadataForObject_(metadataMaps, objectKey) {
  const map = asObjectSafe_(metadataMaps.manualMetadataMap);

  if (map[objectKey]) {
    return asObjectSafe_(map[objectKey]);
  }

  const shortKey = extractShortObjectKey_(objectKey);
  if (shortKey && map[shortKey]) {
    return asObjectSafe_(map[shortKey]);
  }

  return {};
}


/**
 * Получить SLA объекта
 */
function getObjectSlaForObject_(metadataMaps, objectKey) {
  const map = asObjectSafe_(metadataMaps.objectSlaMap);

  if (map[objectKey]) {
    return asObjectSafe_(map[objectKey]);
  }

  const shortKey = extractShortObjectKey_(objectKey);
  if (shortKey && map[shortKey]) {
    return asObjectSafe_(map[shortKey]);
  }

  return {};
}


/**
 * Получить owner объекта
 */
function getObjectOwnerForObject_(metadataMaps, objectKey) {
  const map = asObjectSafe_(metadataMaps.objectOwnersMap);

  if (map[objectKey]) {
    return asObjectSafe_(map[objectKey]);
  }

  const shortKey = extractShortObjectKey_(objectKey);
  if (shortKey && map[shortKey]) {
    return asObjectSafe_(map[shortKey]);
  }

  return {};
}


/**
 * Получить flags объекта
 */
function getObjectFlagsForObject_(metadataMaps, objectKey) {
  const map = asObjectSafe_(metadataMaps.objectFlagsMap);
  return map[objectKey] || {
    has_view_definition: false,
    has_storage_stats: false,
    has_options: false,
    has_description: false,
    has_partitioning: false,
    has_clustering: false
  };
}


/**
 * Layer inference
 */
function inferLayer_(object, columns, optionsFlat, viewRow) {
  const datasetName = normalizePlainString_(object.dataset_name).toLowerCase();
  const tableName = normalizePlainString_(object.table_name).toLowerCase();
  const tableType = normalizePlainString_(object.table_type).toUpperCase();

  if (datasetName.indexOf('analytics_') === 0) {
    return CONFIG.layers.rawGa4;
  }

  if (datasetName.indexOf('raw') !== -1 || tableName.indexOf('raw_') === 0) {
    return CONFIG.layers.raw;
  }

  if (tableName.indexOf('dim_') === 0) {
    return CONFIG.layers.dim;
  }

  if (tableName.indexOf('stg_') === 0 || tableName.indexOf('stage_') === 0) {
    return CONFIG.layers.staging;
  }

  if (tableName.indexOf('mart_') === 0) {
    return CONFIG.layers.mart;
  }

  if (tableType === CONFIG.objectTypes.view || normalizePlainString_(viewRow.view_definition)) {
    return CONFIG.layers.execView;
  }

  if (tableName.indexOf('forecast') !== -1) {
    return CONFIG.layers.forecast;
  }

  if (datasetName.indexOf('dwh') !== -1) {
    return CONFIG.layers.analytics;
  }

  return CONFIG.layers.unknown;
}


/**
 * Source inference
 */
function inferSource_(object, columns, optionsFlat, viewRow) {
  const datasetName = normalizePlainString_(object.dataset_name).toLowerCase();
  const tableName = normalizePlainString_(object.table_name).toLowerCase();

  if (datasetName.indexOf('analytics_') === 0) {
    return CONFIG.sources.ga4;
  }

  if (
    datasetName.indexOf('google_ads') !== -1 ||
    tableName.indexOf('ads_') === 0 ||
    tableName.indexOf('p_ads_') === 0
  ) {
    return CONFIG.sources.googleAds;
  }

  if (datasetName.indexOf('salesdrive') !== -1) {
    return CONFIG.sources.salesDrive;
  }

  if (datasetName.indexOf('search_console') !== -1) {
    return CONFIG.sources.searchConsole;
  }

  if (datasetName.indexOf('manual') !== -1) {
    return CONFIG.sources.manual;
  }

  return CONFIG.sources.unknown;
}


/**
 * Purpose inference
 */
function inferPurpose_(object, columns, optionsFlat, viewRow) {
  const tableName = normalizePlainString_(object.table_name).toLowerCase();
  const datasetName = normalizePlainString_(object.dataset_name).toLowerCase();

  if (tableName.indexOf('demand') !== -1) return CONFIG.purposes.demand;
  if (tableName.indexOf('order') !== -1) return CONFIG.purposes.orders;
  if (tableName.indexOf('sales') !== -1) return CONFIG.purposes.sales;
  if (tableName.indexOf('forecast') !== -1) return CONFIG.purposes.forecast;
  if (tableName.indexOf('abc') !== -1) return CONFIG.purposes.abcAnalysis;
  if (tableName.indexOf('signal') !== -1) return CONFIG.purposes.actionSignals;
  if (tableName.indexOf('priority') !== -1) return CONFIG.purposes.priority;
  if (tableName.indexOf('executive') !== -1) return CONFIG.purposes.executiveControl;
  if (tableName.indexOf('sku_master') !== -1 || tableName.indexOf('dim_') === 0) {
    return CONFIG.purposes.masterData;
  }
  if (datasetName.indexOf('google_ads') !== -1 || tableName.indexOf('ads_') === 0) {
    return CONFIG.purposes.adsRaw;
  }

  return CONFIG.purposes.other;
}


/**
 * Update type inference
 */
function inferUpdateType_(object, layer, source, purpose) {
  const tableType = normalizePlainString_(object.table_type).toUpperCase();
  const datasetName = normalizePlainString_(object.dataset_name).toLowerCase();
  const tableName = normalizePlainString_(object.table_name).toLowerCase();

  if (tableType === CONFIG.objectTypes.view) {
    return CONFIG.updateTypes.liveView;
  }

  if (datasetName.indexOf('analytics_') === 0) {
    return CONFIG.updateTypes.autoGa4Export;
  }

  if (
    datasetName.indexOf('google_ads') !== -1 ||
    tableName.indexOf('ads_') === 0
  ) {
    return CONFIG.updateTypes.autoConnectorOrTransfer;
  }

  if (
    tableName.indexOf('mart_') === 0 ||
    tableName.indexOf('dim_') === 0 ||
    tableName.indexOf('stg_') === 0
  ) {
    return CONFIG.updateTypes.scriptOrScheduledQuery;
  }

  return CONFIG.updateTypes.unknown;
}


/**
 * Refresh mechanism inference
 */
function inferRefreshMechanism_(object, layer, source, purpose, manualMeta) {
  const manual = normalizePlainString_(manualMeta.refresh_mechanism);
  if (manual) {
    return manual;
  }

  const tableType = normalizePlainString_(object.table_type).toUpperCase();
  const datasetName = normalizePlainString_(object.dataset_name).toLowerCase();
  const tableName = normalizePlainString_(object.table_name).toLowerCase();

  if (tableType === CONFIG.objectTypes.view) {
    return CONFIG.refreshMechanisms.liveView;
  }

  if (datasetName.indexOf('analytics_') === 0) {
    return CONFIG.refreshMechanisms.ga4Export;
  }

  if (
    datasetName.indexOf('google_ads') !== -1 ||
    tableName.indexOf('ads_') === 0
  ) {
    return CONFIG.refreshMechanisms.transferOrConnector;
  }

  if (
    tableName.indexOf('mart_') === 0 ||
    tableName.indexOf('dim_') === 0 ||
    tableName.indexOf('stg_') === 0
  ) {
    return CONFIG.refreshMechanisms.scriptOrScheduledQuery;
  }

  return CONFIG.refreshMechanisms.unknown;
}


/**
 * Expected refresh frequency
 */
function inferExpectedRefreshFrequency_(object, manualMeta, objectSla) {
  return firstNonEmpty_([
    normalizePlainString_(objectSla.expected_refresh_frequency),
    normalizePlainString_(manualMeta.expected_refresh_frequency),
    CONFIG.thresholds.defaultExpectedRefreshFrequency
  ]);
}


/**
 * Freshness threshold hours
 */
function inferFreshnessThresholdHours_(object, manualMeta, objectSla) {
  const value = firstNonEmpty_([
    objectSla.freshness_threshold_hours,
    manualMeta.freshness_threshold_hours,
    CONFIG.thresholds.defaultFreshnessThresholdHours
  ]);

  const normalized = normalizeNumberSafe_(value);
  return normalized === '' ? CONFIG.thresholds.defaultFreshnessThresholdHours : normalized;
}


/**
 * Business criticality
 */
function inferBusinessCriticality_(object, purpose, manualMeta, objectSla) {
  const manual = firstNonEmpty_([
    normalizePlainString_(objectSla.business_criticality),
    normalizePlainString_(manualMeta.business_criticality)
  ]);

  if (manual) {
    return manual;
  }

  if (purpose === CONFIG.purposes.executiveControl) {
    return 'critical';
  }

  if (
    purpose === CONFIG.purposes.sales ||
    purpose === CONFIG.purposes.orders ||
    purpose === CONFIG.purposes.demand
  ) {
    return 'high';
  }

  return CONFIG.defaults.businessCriticality;
}


/**
 * Management priority
 */
function inferManagementPriority_(object, purpose, manualMeta, objectSla) {
  const manual = firstNonEmpty_([
    normalizePlainString_(objectSla.management_priority),
    normalizePlainString_(manualMeta.management_priority)
  ]);

  if (manual) {
    return manual;
  }

  if (purpose === CONFIG.purposes.executiveControl) {
    return 'high';
  }

  if (purpose === CONFIG.purposes.priority || purpose === CONFIG.purposes.actionSignals) {
    return 'high';
  }

  return CONFIG.defaults.managementPriority;
}


/**
 * Grain inference
 */
function inferGrain_(object, columns, purpose, dateCandidate) {
  const columnNames = columns.map(function (c) {
    return normalizePlainString_(c.column_name).toLowerCase();
  });

  const hasSku = columnNames.indexOf('sku') !== -1;
  const hasPromId = columnNames.indexOf('prom_id') !== -1;
  const hasCategory = columnNames.indexOf('category') !== -1;
  const hasOrderId = columnNames.indexOf('order_id') !== -1;

  if (hasOrderId) {
    return '1 row = 1 order';
  }

  if (dateCandidate && hasSku) {
    return '1 row = 1 date x 1 sku';
  }

  if (dateCandidate && hasPromId) {
    return '1 row = 1 date x 1 prom_id';
  }

  if (dateCandidate && hasCategory) {
    return '1 row = 1 date x 1 category';
  }

  if (dateCandidate) {
    return '1 row = 1 date-level record';
  }

  return '';
}


/**
 * Business definition inference
 */
function inferBusinessDefinition_(object, purpose) {
  const tableName = normalizePlainString_(object.table_name);

  if (purpose === CONFIG.purposes.executiveControl) {
    return 'Управленческий контрольный объект для принятия решений';
  }

  if (purpose === CONFIG.purposes.demand) {
    return 'Объект с данными о спросе и пользовательском интересе';
  }

  if (purpose === CONFIG.purposes.orders) {
    return 'Объект с данными по заказам';
  }

  if (purpose === CONFIG.purposes.sales) {
    return 'Объект с данными по продажам';
  }

  if (purpose === CONFIG.purposes.masterData) {
    return 'Справочник или мастер-данные для нормализации и джойнов';
  }

  return tableName ? ('Бизнес-объект: ' + tableName) : '';
}


/**
 * Risk flag inference
 */
function inferRiskFlag_(input) {
  const payload = input || {};
  const object = payload.object || {};
  const description = normalizePlainString_(payload.description);
  const actualLastUpdate = normalizePlainString_(payload.actualLastUpdate);
  const sizeBytes = normalizeNumberSafe_(payload.sizeBytes);
  const columnsCount = normalizeNumberSafe_(payload.columnsCount);

  if (!columnsCount) {
    return CONFIG.riskFlags.noColumnsMetadata;
  }

  if (!description) {
    return CONFIG.riskFlags.noDescription;
  }

  if (
    normalizePlainString_(object.table_type).toUpperCase() === CONFIG.objectTypes.baseTable &&
    sizeBytes === 0
  ) {
    return CONFIG.riskFlags.empty;
  }

  if (
    sizeBytes !== '' &&
    sizeBytes > CONFIG.thresholds.veryHeavyObjectSizeGbThreshold * 1024 * 1024 * 1024
  ) {
    return CONFIG.riskFlags.tooBig;
  }

  if (actualLastUpdate) {
    const dt = new Date(actualLastUpdate);
    if (!isNaN(dt.getTime())) {
      const now = new Date();
      const diffDays = (now.getTime() - dt.getTime()) / (1000 * 60 * 60 * 24);

      if (diffDays > 3) {
        return CONFIG.riskFlags.oldData;
      }
    }
  }

  return CONFIG.riskFlags.ok;
}


/**
 * Scan risk inference
 */
function inferScanRisk_(input) {
  const payload = input || {};
  const object = payload.object || {};
  const sizeBytes = normalizeNumberSafe_(payload.sizeBytes);
  const partitioningType = normalizePlainString_(payload.partitioningType);
  const partitioningField = normalizePlainString_(payload.partitioningField);
  const clusteringFields = normalizePlainString_(payload.clusteringFields);
  const tableType = normalizePlainString_(object.table_type).toUpperCase();

  const sizeGb = sizeBytes === '' ? 0 : sizeBytes / 1024 / 1024 / 1024;

  if (tableType === CONFIG.objectTypes.view) {
    return sizeGb >= CONFIG.thresholds.veryHeavyObjectSizeGbThreshold
      ? CONFIG.scanRisk.high
      : CONFIG.scanRisk.low;
  }

  if (
    sizeGb >= CONFIG.thresholds.veryHeavyObjectSizeGbThreshold &&
    !partitioningType &&
    !partitioningField
  ) {
    return CONFIG.scanRisk.veryHigh;
  }

  if (
    sizeGb >= CONFIG.thresholds.heavyObjectSizeGbThreshold &&
    !partitioningType &&
    !partitioningField
  ) {
    return CONFIG.scanRisk.high;
  }

  if (
    sizeGb >= CONFIG.thresholds.heavyObjectSizeGbThreshold &&
    (partitioningType || partitioningField) &&
    !clusteringFields
  ) {
    return CONFIG.scanRisk.medium;
  }

  return CONFIG.scanRisk.low;
}


/**
 * Candidate key inference
 */
function inferPrimaryKeyCandidate_(columns, dateCandidate) {
  const cols = Array.isArray(columns) ? columns : [];
  const colNames = cols.map(function (c) {
    return normalizePlainString_(c.column_name).toLowerCase();
  });

  for (var i = 0; i < CONFIG.primaryKeyPreferredCombos.length; i++) {
    var combo = CONFIG.primaryKeyPreferredCombos[i];
    var ok = combo.every(function (c) {
      return colNames.indexOf(c) !== -1;
    });

    if (ok) {
      return combo.join(', ');
    }
  }

  if (dateCandidate) {
    const dateCol = normalizePlainString_(dateCandidate.column_name).toLowerCase();

    var dimensionCandidate = ['sku', 'item_id', 'prom_id', 'category', 'channel', 'source_medium']
      .find(function (name) {
        return colNames.indexOf(name) !== -1;
      });

    if (dimensionCandidate) {
      return dateCol + ', ' + dimensionCandidate;
    }
  }

  return '';
}


/**
 * Join keys inference
 */
function inferJoinKeys_(columns) {
  const cols = Array.isArray(columns) ? columns : [];
  const colNames = cols.map(function (c) {
    return normalizePlainString_(c.column_name).toLowerCase();
  });

  const result = CONFIG.joinKeyCandidates.filter(function (candidate) {
    return colNames.indexOf(candidate) !== -1;
  });

  return result.join(', ');
}


/**
 * Date field candidate detection
 */
function detectDateFieldCandidate_(columns) {
  const cols = Array.isArray(columns) ? columns : [];
  if (!cols.length) {
    return null;
  }

  const exactPriority = cols.find(function (col) {
    const dataType = normalizePlainString_(col.data_type).toUpperCase();
    const columnName = normalizePlainString_(col.column_name).toLowerCase();

    return (
      (dataType === 'DATE' || dataType === 'TIMESTAMP' || dataType === 'DATETIME') &&
      ['date', 'event_date', 'order_date', 'created_at', 'created_date', 'updated_at', 'day', 'dt']
        .indexOf(columnName) !== -1
    );
  });

  if (exactPriority) {
    return exactPriority;
  }

  for (var i = 0; i < CONFIG.dateFieldPriorityNames.length; i++) {
    var marker = CONFIG.dateFieldPriorityNames[i];

    var found = cols.find(function (col) {
      const dataType = normalizePlainString_(col.data_type).toUpperCase();
      const columnName = normalizePlainString_(col.column_name).toLowerCase();

      return (
        (dataType === 'DATE' || dataType === 'TIMESTAMP' || dataType === 'DATETIME') &&
        columnName.indexOf(marker) !== -1
      );
    });

    if (found) {
      return found;
    }
  }

  return cols.find(function (col) {
    const dataType = normalizePlainString_(col.data_type).toUpperCase();
    return dataType === 'DATE' || dataType === 'TIMESTAMP' || dataType === 'DATETIME';
  }) || null;
}


/**
 * Извлечь short key dataset.table из project.dataset.table
 */
function extractShortObjectKey_(objectKey) {
  const parts = String(objectKey || '').split('.');
  if (parts.length < 3) {
    return '';
  }
  return parts[1] + '.' + parts[2];
}


/**
 * Первый непустой элемент
 */
function firstNonEmpty_(values) {
  const items = Array.isArray(values) ? values : [];

  for (var i = 0; i < items.length; i++) {
    const value = items[i];

    if (value === null || value === undefined) {
      continue;
    }

    if (typeof value === 'number') {
      return value;
    }

    const asString = String(value).trim();
    if (asString !== '') {
      return value;
    }
  }

  return '';
}


/**
 * Округление bytes -> MB
 */
function roundBytesToMb_(bytes) {
  return roundNumberSafe_(Number(bytes) / 1024 / 1024, 2);
}


/**
 * Округление bytes -> GB
 */
function roundBytesToGb_(bytes) {
  return roundNumberSafe_(Number(bytes) / 1024 / 1024 / 1024, 2);
}


/**
 * Безопасное округление
 */
function roundNumberSafe_(value, digits) {
  const n = Number(value);
  if (isNaN(n)) {
    return '';
  }

  const p = Math.pow(10, Number(digits || 0));
  return Math.round(n * p) / p;
}
