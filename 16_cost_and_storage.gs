/*******************************************************
 * 16_cost_and_storage.gs
 * Storage / cost audit:
 * - тяжелые объекты
 * - scan risk
 * - optimization hints
 * - storage audit rows
 *******************************************************/


/**
 * Строит список тяжелых объектов.
 */
function buildHeavyObjectsRows_(enrichedObjects) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];

  return objects
    .filter(function (obj) {
      return isHeavyOrRiskyObject_(obj);
    })
    .map(function (obj) {
      return buildHeavyObjectRow_(obj);
    })
    .sort(function (a, b) {
      return Number(b.size_gb || 0) - Number(a.size_gb || 0);
    })
    .slice(0, CONFIG.limits.maxHeavyObjectsPerRun);
}


/**
 * Строит storage audit rows по всем объектам.
 */
function buildStorageAuditRows_(enrichedObjects) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];

  return objects.map(function (obj) {
    return {
      object_key: normalizePlainString_(obj.object_key),
      dataset_name: normalizePlainString_(obj.dataset_name),
      table_name: normalizePlainString_(obj.table_name),
      table_type: normalizePlainString_(obj.table_type),
      row_count: normalizeNumberSafe_(obj.row_count),
      size_bytes: normalizeNumberSafe_(obj.size_bytes),
      size_mb: normalizeNumberSafe_(obj.size_mb),
      size_gb: normalizeNumberSafe_(obj.size_gb),
      partitioning_type: normalizePlainString_(obj.partitioning_type),
      partitioning_field: normalizePlainString_(obj.partitioning_field),
      clustering_fields: normalizePlainString_(obj.clustering_fields),
      scan_risk: normalizePlainString_(obj.scan_risk),
      risk_flag: normalizePlainString_(obj.risk_flag),
      optimization_comment: buildOptimizationComment_(obj),
      checked_at: getNowProjectTz_()
    };
  });
}


/**
 * Проверяет, тяжелый ли объект или рискованный.
 */
function isHeavyOrRiskyObject_(obj) {
  const sizeGb = normalizeNumberSafe_(obj.size_gb);
  const scanRisk = normalizePlainString_(obj.scan_risk);
  const riskFlag = normalizePlainString_(obj.risk_flag);

  if (sizeGb !== '' && sizeGb >= CONFIG.thresholds.heavyObjectSizeGbThreshold) {
    return true;
  }

  if (
    scanRisk === CONFIG.scanRisk.high ||
    scanRisk === CONFIG.scanRisk.veryHigh
  ) {
    return true;
  }

  if (
    riskFlag === CONFIG.riskFlags.tooBig ||
    riskFlag === CONFIG.riskFlags.empty
  ) {
    return true;
  }

  return false;
}


/**
 * Строка тяжелого объекта.
 */
function buildHeavyObjectRow_(obj) {
  return {
    object_key: normalizePlainString_(obj.object_key),
    dataset_name: normalizePlainString_(obj.dataset_name),
    table_name: normalizePlainString_(obj.table_name),
    table_type: normalizePlainString_(obj.table_type),
    row_count: normalizeNumberSafe_(obj.row_count),
    size_gb: normalizeNumberSafe_(obj.size_gb),
    partitioning_type: normalizePlainString_(obj.partitioning_type),
    partitioning_field: normalizePlainString_(obj.partitioning_field),
    clustering_fields: normalizePlainString_(obj.clustering_fields),
    scan_risk: normalizePlainString_(obj.scan_risk),
    risk_flag: normalizePlainString_(obj.risk_flag),
    optimization_comment: buildOptimizationComment_(obj)
  };
}


/**
 * Формирует комментарий оптимизации.
 */
function buildOptimizationComment_(obj) {
  const comments = [];

  const tableType = normalizePlainString_(obj.table_type).toUpperCase();
  const sizeGb = normalizeNumberSafe_(obj.size_gb);
  const rowCount = normalizeNumberSafe_(obj.row_count);
  const partitioningType = normalizePlainString_(obj.partitioning_type);
  const partitioningField = normalizePlainString_(obj.partitioning_field);
  const clusteringFields = normalizePlainString_(obj.clustering_fields);
  const scanRisk = normalizePlainString_(obj.scan_risk);
  const dateFieldCandidate = normalizePlainString_(obj.date_field_candidate);
  const purpose = normalizePlainString_(obj.purpose);
  const layer = normalizePlainString_(obj.layer);

  if (tableType === CONFIG.objectTypes.view) {
    comments.push('VIEW: оценить стоимость базовых таблиц через lineage и частые запросы');
    return comments.join('; ');
  }

  if (sizeGb !== '' && sizeGb >= CONFIG.thresholds.veryHeavyObjectSizeGbThreshold) {
    comments.push('Очень крупный объект');
  } else if (sizeGb !== '' && sizeGb >= CONFIG.thresholds.heavyObjectSizeGbThreshold) {
    comments.push('Крупный объект');
  }

  if (!partitioningType && !partitioningField && dateFieldCandidate) {
    comments.push('Рассмотреть partition by ' + dateFieldCandidate);
  }

  if (!partitioningType && !partitioningField && !dateFieldCandidate) {
    comments.push('Нет partitioning и не найден date field candidate');
  }

  if (!clusteringFields) {
    const suggestedCluster = suggestClusteringFields_(obj);
    if (suggestedCluster) {
      comments.push('Рассмотреть clustering by ' + suggestedCluster);
    } else {
      comments.push('Нет clustering');
    }
  }

  if (scanRisk === CONFIG.scanRisk.veryHigh) {
    comments.push('Очень высокий риск дорогих full scan');
  } else if (scanRisk === CONFIG.scanRisk.high) {
    comments.push('Высокий риск дорогих scan');
  } else if (scanRisk === CONFIG.scanRisk.medium) {
    comments.push('Средний риск scan');
  }

  if (rowCount === 0) {
    comments.push('Пустой объект: проверить необходимость хранения');
  }

  if (purpose === CONFIG.purposes.executiveControl && scanRisk !== CONFIG.scanRisk.low) {
    comments.push('Управленческая витрина должна быть дешевой и стабильной для чтения');
  }

  if (layer === CONFIG.layers.raw && sizeGb !== '' && sizeGb >= CONFIG.thresholds.heavyObjectSizeGbThreshold) {
    comments.push('Raw-слой: проверить retention и партиционирование');
  }

  if (!comments.length) {
    comments.push('Явных storage/cost рисков не найдено');
  }

  return comments.join('; ');
}


/**
 * Предлагает clustering fields по доступным join/date полям.
 */
function suggestClusteringFields_(obj) {
  const joinKeys = normalizePlainString_(obj.join_keys);
  const primaryKey = normalizePlainString_(obj.primary_key_candidate);

  const candidates = [];

  splitCommaList_(joinKeys).forEach(function (key) {
    candidates.push(key);
  });

  splitCommaList_(primaryKey).forEach(function (key) {
    candidates.push(key);
  });

  const preferred = [
    'sku',
    'item_id',
    'prom_id',
    'category',
    'source_medium',
    'channel',
    'order_id'
  ];

  const normalizedCandidates = candidates.map(function (v) {
    return normalizePlainString_(v).toLowerCase();
  });

  const result = preferred.filter(function (field) {
    return normalizedCandidates.indexOf(field) !== -1;
  });

  return dedupePlainArray_(result).slice(0, 4).join(', ');
}


/**
 * Строит partition audit rows.
 * Пока работает по enriched metadata, без INFORMATION_SCHEMA.PARTITIONS.
 */
function buildPartitionAuditRows_(enrichedObjects) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];

  return objects.map(function (obj) {
    const hasPartitioning = !!normalizePlainString_(obj.partitioning_type) ||
      !!normalizePlainString_(obj.partitioning_field);

    return {
      object_key: normalizePlainString_(obj.object_key),
      dataset_name: normalizePlainString_(obj.dataset_name),
      table_name: normalizePlainString_(obj.table_name),
      table_type: normalizePlainString_(obj.table_type),
      has_partitioning: hasPartitioning,
      partitioning_type: normalizePlainString_(obj.partitioning_type),
      partitioning_field: normalizePlainString_(obj.partitioning_field),
      date_field_candidate: normalizePlainString_(obj.date_field_candidate),
      size_gb: normalizeNumberSafe_(obj.size_gb),
      scan_risk: normalizePlainString_(obj.scan_risk),
      recommendation: buildPartitionRecommendation_(obj),
      checked_at: getNowProjectTz_()
    };
  });
}


/**
 * Рекомендация по partition.
 */
function buildPartitionRecommendation_(obj) {
  const tableType = normalizePlainString_(obj.table_type).toUpperCase();
  const sizeGb = normalizeNumberSafe_(obj.size_gb);
  const partitioningType = normalizePlainString_(obj.partitioning_type);
  const partitioningField = normalizePlainString_(obj.partitioning_field);
  const dateFieldCandidate = normalizePlainString_(obj.date_field_candidate);

  if (tableType !== CONFIG.objectTypes.baseTable) {
    return 'Не base table: partition recommendation не применяется напрямую';
  }

  if (partitioningType || partitioningField) {
    return 'Partitioning уже задан';
  }

  if (sizeGb === '' || sizeGb < CONFIG.thresholds.heavyObjectSizeGbThreshold) {
    return 'Размер ниже порога heavy object; partition не является приоритетом';
  }

  if (dateFieldCandidate) {
    return 'Рассмотреть partition by ' + dateFieldCandidate;
  }

  return 'Крупная таблица без partitioning и без явного date field; требуется ручной анализ модели данных';
}


/**
 * Заголовки листа storage audit.
 */
function getStorageAuditHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'table_type',
    'row_count',
    'size_bytes',
    'size_mb',
    'size_gb',
    'partitioning_type',
    'partitioning_field',
    'clustering_fields',
    'scan_risk',
    'risk_flag',
    'optimization_comment',
    'checked_at'
  ];
}


/**
 * Заголовки листа partition audit.
 */
function getPartitionAuditHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'table_type',
    'has_partitioning',
    'partitioning_type',
    'partitioning_field',
    'date_field_candidate',
    'size_gb',
    'scan_risk',
    'recommendation',
    'checked_at'
  ];
}


/**
 * Split comma list.
 */
function splitCommaList_(value) {
  return String(value || '')
    .split(',')
    .map(function (v) {
      return normalizePlainString_(v);
    })
    .filter(function (v) {
      return !!v;
    });
}


/**
 * Dedupe plain array.
 */
function dedupePlainArray_(items) {
  const arr = Array.isArray(items) ? items : [];
  const seen = {};
  const result = [];

  arr.forEach(function (item) {
    const value = normalizePlainString_(item);
    if (!value || seen[value]) {
      return;
    }

    seen[value] = true;
    result.push(value);
  });

  return result;
}
