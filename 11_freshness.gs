/*******************************************************
 * 11_freshness.gs
 * Freshness layer:
 * - расчет свежести по metadata timestamp
 * - расчет lag
 * - построение строк freshness
 * - история freshness
 *******************************************************/


/**
 * Применяет freshness к enriched objects in-place.
 * Возвращает тот же массив объектов.
 */
function applyFreshnessToObjects_(enrichedObjects) {
  const items = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const now = new Date();

  items.forEach(function (obj) {
    const result = calculateFreshnessForObject_(obj, now);

    obj.freshness_status = result.freshness_status;
    obj.freshness_comment = result.freshness_comment;
    obj.freshness_lag_hours = result.freshness_lag_hours;
    obj.freshness_reference_type = result.freshness_reference_type;
    obj.freshness_reference_value = result.freshness_reference_value;
  });

  return items;
}


/**
 * Главный расчет freshness для одного объекта.
 */
function calculateFreshnessForObject_(obj, now) {
  const object = obj || {};
  const currentTime = now instanceof Date ? now : new Date();

  const actualLastUpdate = normalizePlainString_(object.actual_last_update);
  const tableType = normalizePlainString_(object.table_type).toUpperCase();
  const expectedRefreshFrequency = normalizePlainString_(object.expected_refresh_frequency);
  const thresholdHours = normalizeFreshnessThresholdHours_(object.freshness_threshold_hours);

  if (!actualLastUpdate) {
    return {
      freshness_status: CONFIG.statuses.freshness.unknown,
      freshness_comment: 'Нет actual_last_update',
      freshness_lag_hours: '',
      freshness_reference_type: 'METADATA_TIMESTAMP',
      freshness_reference_value: ''
    };
  }

  const parsedDate = parseDateSafe_(actualLastUpdate);
  if (!parsedDate) {
    return {
      freshness_status: CONFIG.statuses.freshness.unknown,
      freshness_comment: 'Некорректный формат actual_last_update',
      freshness_lag_hours: '',
      freshness_reference_type: 'METADATA_TIMESTAMP',
      freshness_reference_value: actualLastUpdate
    };
  }

  const lagHours = calculateLagHours_(parsedDate, currentTime);
  const status = calculateFreshnessStatusByLag_(lagHours, thresholdHours);
  const comment = buildFreshnessComment_({
    lagHours: lagHours,
    thresholdHours: thresholdHours,
    tableType: tableType,
    expectedRefreshFrequency: expectedRefreshFrequency,
    hasDateCoverage: !!normalizePlainString_(object.period_max)
  });

  return {
    freshness_status: status,
    freshness_comment: comment,
    freshness_lag_hours: lagHours,
    freshness_reference_type: 'METADATA_TIMESTAMP',
    freshness_reference_value: formatProjectDateTime_(parsedDate)
  };
}


/**
 * Вычисляет freshness status по lag.
 */
function calculateFreshnessStatusByLag_(lagHours, thresholdHours) {
  const lag = normalizeNumberSafe_(lagHours);
  const threshold = normalizeFreshnessThresholdHours_(thresholdHours);

  if (lag === '') {
    return CONFIG.statuses.freshness.unknown;
  }

  const warningBoundary = threshold * CONFIG.thresholds.freshnessWarningMultiplier;
  const criticalBoundary = threshold * CONFIG.thresholds.freshnessCriticalMultiplier;

  if (lag <= warningBoundary) {
    return CONFIG.statuses.freshness.fresh;
  }

  if (lag <= criticalBoundary) {
    return CONFIG.statuses.freshness.late;
  }

  return CONFIG.statuses.freshness.stale;
}


/**
 * Формирует комментарий freshness.
 */
function buildFreshnessComment_(input) {
  const payload = input || {};
  const lagHours = normalizeNumberSafe_(payload.lagHours);
  const thresholdHours = normalizeFreshnessThresholdHours_(payload.thresholdHours);
  const tableType = normalizePlainString_(payload.tableType).toUpperCase();
  const expectedRefreshFrequency = normalizePlainString_(payload.expectedRefreshFrequency);
  const hasDateCoverage = !!payload.hasDateCoverage;

  if (lagHours === '') {
    return 'Не удалось вычислить lag';
  }

  let baseComment = 'Обновление ' + roundNumberSafe_(lagHours, 1) + ' ч назад';
  baseComment += '; SLA ' + thresholdHours + ' ч';

  if (expectedRefreshFrequency) {
    baseComment += '; expected=' + expectedRefreshFrequency;
  }

  if (tableType === CONFIG.objectTypes.view) {
    baseComment += '; для VIEW это косвенный индикатор';
  }

  if (!hasDateCoverage) {
    baseComment += '; нет business-date coverage';
  }

  return baseComment;
}


/**
 * Строит отдельные строки freshness.
 * Можно писать в отдельный лист или history.
 */
function buildFreshnessRows_(enrichedObjects) {
  const items = Array.isArray(enrichedObjects) ? enrichedObjects : [];

  return items.map(function (obj) {
    return {
      object_key: normalizePlainString_(obj.object_key),
      dataset_name: normalizePlainString_(obj.dataset_name),
      table_name: normalizePlainString_(obj.table_name),
      table_type: normalizePlainString_(obj.table_type),
      refresh_mechanism: normalizePlainString_(obj.refresh_mechanism),
      expected_refresh_frequency: normalizePlainString_(obj.expected_refresh_frequency),
      freshness_threshold_hours: normalizeNumberSafe_(obj.freshness_threshold_hours),
      actual_last_update: normalizePlainString_(obj.actual_last_update),
      freshness_lag_hours: normalizeNumberSafe_(obj.freshness_lag_hours),
      freshness_status: normalizePlainString_(obj.freshness_status),
      freshness_comment: normalizePlainString_(obj.freshness_comment),
      period_max: normalizePlainString_(obj.period_max),
      checked_at: getNowProjectTz_()
    };
  });
}


/**
 * Строит history rows freshness.
 * История — append-only слой.
 */
function appendFreshnessHistoryRows_(enrichedObjects) {
  if (!CONFIG.features.enableFreshnessHistory) {
    return [];
  }

  const rows = buildFreshnessRows_(enrichedObjects);

  return rows.map(function (row) {
    return {
      run_id: '',
      checked_at: row.checked_at,
      object_key: row.object_key,
      dataset_name: row.dataset_name,
      table_name: row.table_name,
      table_type: row.table_type,
      refresh_mechanism: row.refresh_mechanism,
      expected_refresh_frequency: row.expected_refresh_frequency,
      freshness_threshold_hours: row.freshness_threshold_hours,
      actual_last_update: row.actual_last_update,
      freshness_lag_hours: row.freshness_lag_hours,
      freshness_status: row.freshness_status,
      freshness_comment: row.freshness_comment,
      period_max: row.period_max
    };
  });
}


/**
 * Вспомогательный расчет lag в часах.
 */
function calculateLagHours_(fromDate, toDate) {
  if (!(fromDate instanceof Date) || !(toDate instanceof Date)) {
    return '';
  }

  const diffMs = toDate.getTime() - fromDate.getTime();
  return roundNumberSafe_(diffMs / (1000 * 60 * 60), 2);
}


/**
 * Нормализация порога freshness
 */
function normalizeFreshnessThresholdHours_(value) {
  const normalized = normalizeNumberSafe_(value);
  return normalized === ''
    ? CONFIG.thresholds.defaultFreshnessThresholdHours
    : normalized;
}


/**
 * Безопасный parse даты
 */
function parseDateSafe_(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  if (value instanceof Date) {
    return isNaN(value.getTime()) ? null : value;
  }

  const parsed = new Date(value);
  return isNaN(parsed.getTime()) ? null : parsed;
}
