/*******************************************************
 * 17_summary.gs
 * Summary layer:
 * - сводка проекта
 * - ключевые объекты
 * - executive status
 * - агрегаты по слоям, источникам, рискам
 *******************************************************/


/**
 * Главная функция построения сводки.
 *
 * Совместима с вызовом из 01_main.gs:
 * buildSummaryRows_(datasets, objects, columns, dependencyRows, dateCoverageRows, qualityRows, keyObjectsRows)
 */
function buildSummaryRows_(
  datasets,
  enrichedObjects,
  columns,
  dependencyRows,
  dateCoverageRows,
  qualityRows,
  keyObjectsRows
) {
  const ds = Array.isArray(datasets) ? datasets : [];
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const cols = Array.isArray(columns) ? columns : [];
  const deps = Array.isArray(dependencyRows) ? dependencyRows : [];
  const coverage = Array.isArray(dateCoverageRows) ? dateCoverageRows : [];
  const quality = Array.isArray(qualityRows) ? qualityRows : [];
  const keyObjects = Array.isArray(keyObjectsRows) ? keyObjectsRows : [];

  const rows = [];

  addSummaryRow_(rows, 'project_id', CONFIG.core.projectId);
  addSummaryRow_(rows, 'region', CONFIG.core.region);
  addSummaryRow_(rows, 'updated_at', getNowProjectTz_());

  addSummaryRow_(rows, 'datasets_count', ds.length);
  addSummaryRow_(rows, 'objects_count', objects.length);
  addSummaryRow_(rows, 'columns_count', cols.length);
  addSummaryRow_(rows, 'dependency_rows_count', deps.length);
  addSummaryRow_(rows, 'date_coverage_rows_count', coverage.length);
  addSummaryRow_(rows, 'quality_rows_count', quality.length);
  addSummaryRow_(rows, 'key_objects_count', keyObjects.length);

  const typeCounts = countObjectsByField_(objects, 'table_type');
  addObjectCountsToSummary_(rows, 'object_type', typeCounts);

  addSummaryRow_(rows, 'base_tables_count', typeCounts[CONFIG.objectTypes.baseTable] || 0);
  addSummaryRow_(rows, 'views_count', typeCounts[CONFIG.objectTypes.view] || 0);
  addSummaryRow_(rows, 'materialized_views_count', typeCounts[CONFIG.objectTypes.materializedView] || 0);
  addSummaryRow_(rows, 'external_tables_count', typeCounts[CONFIG.objectTypes.external] || 0);

  addObjectCountsToSummary_(rows, 'layer', countObjectsByField_(objects, 'layer'));
  addObjectCountsToSummary_(rows, 'source', countObjectsByField_(objects, 'source'));
  addObjectCountsToSummary_(rows, 'purpose', countObjectsByField_(objects, 'purpose'));
  addObjectCountsToSummary_(rows, 'update_type', countObjectsByField_(objects, 'update_type'));
  addObjectCountsToSummary_(rows, 'refresh_mechanism', countObjectsByField_(objects, 'refresh_mechanism'));
  addObjectCountsToSummary_(rows, 'business_criticality', countObjectsByField_(objects, 'business_criticality'));
  addObjectCountsToSummary_(rows, 'management_priority', countObjectsByField_(objects, 'management_priority'));
  addObjectCountsToSummary_(rows, 'freshness', countObjectsByField_(objects, 'freshness_status'));
  addObjectCountsToSummary_(rows, 'quality', countObjectsByField_(objects, 'quality_status'));
  addObjectCountsToSummary_(rows, 'risk', countObjectsByField_(objects, 'risk_flag'));
  addObjectCountsToSummary_(rows, 'scan_risk', countObjectsByField_(objects, 'scan_risk'));

  addSummaryRow_(rows, 'objects_without_description_count', countObjectsWhere_(objects, function (o) {
    return !normalizePlainString_(o.description);
  }));

  addSummaryRow_(rows, 'objects_without_owner_count', countObjectsWhere_(objects, function (o) {
    return !normalizePlainString_(o.owner);
  }));

  addSummaryRow_(rows, 'objects_without_primary_key_candidate_count', countObjectsWhere_(objects, function (o) {
    return !normalizePlainString_(o.primary_key_candidate);
  }));

  addSummaryRow_(rows, 'objects_without_date_candidate_count', countObjectsWhere_(objects, function (o) {
    return !normalizePlainString_(o.date_field_candidate);
  }));

  addSummaryRow_(rows, 'stale_objects_count', countObjectsWhere_(objects, function (o) {
    return normalizePlainString_(o.freshness_status) === CONFIG.statuses.freshness.stale;
  }));

  addSummaryRow_(rows, 'late_objects_count', countObjectsWhere_(objects, function (o) {
    return normalizePlainString_(o.freshness_status) === CONFIG.statuses.freshness.late;
  }));

  addSummaryRow_(rows, 'quality_fail_objects_count', countObjectsWhere_(objects, function (o) {
    return normalizePlainString_(o.quality_status) === CONFIG.statuses.quality.fail;
  }));

  addSummaryRow_(rows, 'quality_warn_objects_count', countObjectsWhere_(objects, function (o) {
    return normalizePlainString_(o.quality_status) === CONFIG.statuses.quality.warn;
  }));

  addSummaryRow_(rows, 'heavy_objects_count', countObjectsWhere_(objects, function (o) {
    return isHeavyOrRiskyObject_(o);
  }));

  addSummaryRow_(rows, 'total_size_gb', roundNumberSafe_(sumObjectsNumericField_(objects, 'size_gb'), 2));

  return rows;
}


/**
 * Строит ключевые объекты.
 */
function buildKeyObjectsRows_(enrichedObjects) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];

  return objects
    .filter(function (obj) {
      return isKeyObject_(obj);
    })
    .map(function (obj) {
      return buildKeyObjectRow_(obj);
    })
    .sort(function (a, b) {
      return calcKeyObjectScore_(b) - calcKeyObjectScore_(a);
    });
}


/**
 * Является ли объект ключевым.
 */
function isKeyObject_(obj) {
  const criticality = normalizePlainString_(obj.business_criticality).toLowerCase();
  const priority = normalizePlainString_(obj.management_priority).toLowerCase();
  const purpose = normalizePlainString_(obj.purpose);
  const quality = normalizePlainString_(obj.quality_status);
  const freshness = normalizePlainString_(obj.freshness_status);

  if (criticality === 'critical' || criticality === 'high') return true;
  if (priority === 'high') return true;
  if (purpose === CONFIG.purposes.executiveControl) return true;
  if (quality === CONFIG.statuses.quality.fail) return true;
  if (freshness === CONFIG.statuses.freshness.stale) return true;

  return false;
}


/**
 * Строка ключевого объекта.
 */
function buildKeyObjectRow_(obj) {
  return {
    object_key: normalizePlainString_(obj.object_key),
    dataset_name: normalizePlainString_(obj.dataset_name),
    table_name: normalizePlainString_(obj.table_name),
    table_type: normalizePlainString_(obj.table_type),
    source: normalizePlainString_(obj.source),
    purpose: normalizePlainString_(obj.purpose),
    freshness_status: normalizePlainString_(obj.freshness_status),
    quality_status: normalizePlainString_(obj.quality_status),
    business_criticality: normalizePlainString_(obj.business_criticality),
    management_priority: normalizePlainString_(obj.management_priority),
    owner: normalizePlainString_(obj.owner),
    sla_tier: normalizePlainString_(obj.sla_tier),
    grain: normalizePlainString_(obj.grain),
    primary_key_candidate: normalizePlainString_(obj.primary_key_candidate),
    join_keys: normalizePlainString_(obj.join_keys),
    business_definition: normalizePlainString_(obj.business_definition),
    incident_if_broken: normalizePlainString_(obj.incident_if_broken),
    risk_flag: normalizePlainString_(obj.risk_flag),
    scan_risk: normalizePlainString_(obj.scan_risk),
    key_score: calcKeyObjectScore_(obj)
  };
}


/**
 * Score ключевого объекта.
 */
function calcKeyObjectScore_(obj) {
  let score = 0;

  const criticality = normalizePlainString_(obj.business_criticality).toLowerCase();
  const priority = normalizePlainString_(obj.management_priority).toLowerCase();
  const purpose = normalizePlainString_(obj.purpose);
  const quality = normalizePlainString_(obj.quality_status);
  const freshness = normalizePlainString_(obj.freshness_status);
  const risk = normalizePlainString_(obj.risk_flag);
  const scanRisk = normalizePlainString_(obj.scan_risk);

  if (criticality === 'critical') score += 100;
  if (criticality === 'high') score += 80;
  if (priority === 'high') score += 50;
  if (purpose === CONFIG.purposes.executiveControl) score += 40;
  if (quality === CONFIG.statuses.quality.fail) score += 30;
  if (quality === CONFIG.statuses.quality.warn) score += 10;
  if (freshness === CONFIG.statuses.freshness.stale) score += 25;
  if (freshness === CONFIG.statuses.freshness.late) score += 10;
  if (risk === CONFIG.riskFlags.tooBig) score += 20;
  if (risk === CONFIG.riskFlags.noColumnsMetadata) score += 20;
  if (scanRisk === CONFIG.scanRisk.veryHigh) score += 20;
  if (scanRisk === CONFIG.scanRisk.high) score += 10;

  return score;
}


/**
 * Executive status rows.
 * Это отдельная короткая управленческая панель.
 */
function buildExecutiveStatusRows_(datasets, enrichedObjects, qualityRows) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const quality = Array.isArray(qualityRows) ? qualityRows : [];

  const rows = [];

  rows.push(buildExecutiveStatusRow_(
    'DATA_PLATFORM_STRUCTURE',
    objects.length > 0 ? 'OK' : 'FAIL',
    'Объекты проекта обнаружены: ' + objects.length
  ));

  const staleCount = countObjectsWhere_(objects, function (o) {
    return normalizePlainString_(o.freshness_status) === CONFIG.statuses.freshness.stale;
  });

  rows.push(buildExecutiveStatusRow_(
    'FRESHNESS',
    staleCount === 0 ? 'OK' : 'WARN',
    'STALE objects: ' + staleCount
  ));

  const failCount = countObjectsWhere_(objects, function (o) {
    return normalizePlainString_(o.quality_status) === CONFIG.statuses.quality.fail;
  });

  rows.push(buildExecutiveStatusRow_(
    'QUALITY',
    failCount === 0 ? 'OK' : 'WARN',
    'Quality FAIL objects: ' + failCount
  ));

  const noOwnerCount = countObjectsWhere_(objects, function (o) {
    return !normalizePlainString_(o.owner);
  });

  rows.push(buildExecutiveStatusRow_(
    'OWNERSHIP',
    noOwnerCount === 0 ? 'OK' : 'WARN',
    'Objects without owner: ' + noOwnerCount
  ));

  const heavyCount = countObjectsWhere_(objects, function (o) {
    return isHeavyOrRiskyObject_(o);
  });

  rows.push(buildExecutiveStatusRow_(
    'COST_RISK',
    heavyCount === 0 ? 'OK' : 'WARN',
    'Heavy/risky objects: ' + heavyCount
  ));

  rows.push(buildExecutiveStatusRow_(
    'QUALITY_CHECKS',
    quality.length > 0 ? 'OK' : 'WARN',
    'Quality checks rows: ' + quality.length
  ));

  return rows;
}


/**
 * Строка executive status.
 */
function buildExecutiveStatusRow_(area, status, comment) {
  return {
    area: normalizePlainString_(area),
    status: normalizePlainString_(status),
    comment: normalizePlainString_(comment),
    checked_at: getNowProjectTz_()
  };
}


/**
 * Добавляет строку summary.
 */
function addSummaryRow_(rows, metric, value) {
  rows.push({
    metric: normalizePlainString_(metric),
    value: value === null || value === undefined ? '' : value
  });
}


/**
 * Добавляет count-map в summary.
 */
function addObjectCountsToSummary_(rows, prefix, countsMap) {
  const counts = countsMap || {};

  Object.keys(counts).sort().forEach(function (key) {
    const safeKey = normalizePlainString_(key) || 'EMPTY';
    addSummaryRow_(rows, prefix + '_' + safeKey, counts[key]);
  });
}


/**
 * Count by field.
 */
function countObjectsByField_(objects, fieldName) {
  const items = Array.isArray(objects) ? objects : [];
  const map = {};

  items.forEach(function (obj) {
    const key = normalizePlainString_(obj[fieldName]) || 'EMPTY';
    map[key] = (map[key] || 0) + 1;
  });

  return map;
}


/**
 * Count where.
 */
function countObjectsWhere_(objects, predicateFn) {
  const items = Array.isArray(objects) ? objects : [];
  let count = 0;

  items.forEach(function (obj) {
    if (predicateFn(obj)) {
      count += 1;
    }
  });

  return count;
}


/**
 * Sum numeric field.
 */
function sumObjectsNumericField_(objects, fieldName) {
  const items = Array.isArray(objects) ? objects : [];
  let total = 0;

  items.forEach(function (obj) {
    const value = normalizeNumberSafe_(obj[fieldName]);
    if (value !== '') {
      total += Number(value);
    }
  });

  return total;
}


/**
 * Заголовки key objects.
 */
function getKeyObjectsHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'table_type',
    'source',
    'purpose',
    'freshness_status',
    'quality_status',
    'business_criticality',
    'management_priority',
    'owner',
    'sla_tier',
    'grain',
    'primary_key_candidate',
    'join_keys',
    'business_definition',
    'incident_if_broken',
    'risk_flag',
    'scan_risk',
    'key_score'
  ];
}


/**
 * Заголовки summary.
 */
function getSummaryHeaders_() {
  return [
    'metric',
    'value'
  ];
}


/**
 * Заголовки executive status.
 */
function getExecutiveStatusHeaders_() {
  return [
    'area',
    'status',
    'comment',
    'checked_at'
  ];
}
