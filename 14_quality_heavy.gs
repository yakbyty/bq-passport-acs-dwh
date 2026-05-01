/*******************************************************
 * 14_quality_heavy.gs
 * Тяжелые проверки качества:
 * - duplicate checks by candidate key
 * - null checks by candidate key
 * - custom SQL checks
 *******************************************************/


/**
 * Строит heavy quality checks.
 *
 * Важно:
 * - запускать только в FULL-контуре
 * - не смешивать с быстрыми metadata checks
 */
function buildHeavyQualityChecks_(enrichedObjects, qualityRules) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const rules = Array.isArray(qualityRules) ? qualityRules : [];

  const rows = [];

  if (CONFIG.features.enableHeavyChecks) {
    Array.prototype.push.apply(rows, runDuplicateChecks_(objects));
    Array.prototype.push.apply(rows, runNullKeyChecks_(objects));
  }

  if (CONFIG.features.enableCustomSqlChecks) {
    Array.prototype.push.apply(rows, runCustomSqlChecks_(objects, rules));
  }

  return rows;
}


/**
 * Проверка дублей по primary_key_candidate.
 */
function runDuplicateChecks_(enrichedObjects) {
  const objects = selectObjectsForHeavyKeyChecks_(
    enrichedObjects,
    CONFIG.limits.maxDuplicateChecksPerRun
  );

  const rows = [];

  objects.forEach(function (obj) {
    rows.push(runSingleDuplicateCheckSafe_(obj));
  });

  return rows;
}


/**
 * Проверка NULL в primary_key_candidate.
 */
function runNullKeyChecks_(enrichedObjects) {
  const objects = selectObjectsForHeavyKeyChecks_(
    enrichedObjects,
    CONFIG.limits.maxNullKeyChecksPerRun
  );

  const rows = [];

  objects.forEach(function (obj) {
    rows.push(runSingleNullKeyCheckSafe_(obj));
  });

  return rows;
}


/**
 * Ручные SQL-проверки.
 */
function runCustomSqlChecks_(enrichedObjects, qualityRules) {
  const objectMap = buildEnrichedObjectsMap_(enrichedObjects);
  const rules = Array.isArray(qualityRules) ? qualityRules : [];

  const enabledRules = rules
    .filter(function (rule) {
      return rule.is_enabled === true;
    })
    .slice(0, CONFIG.limits.maxCustomSqlChecksPerRun);

  const rows = [];

  enabledRules.forEach(function (rule) {
    const objectKey = normalizePlainString_(rule.object_key) || resolveManualObjectKey_(rule);
    const obj = objectMap[objectKey] || objectMap[extractShortObjectKey_(objectKey)] || {
      object_key: objectKey,
      dataset_name: normalizePlainString_(rule.dataset_name),
      table_name: normalizePlainString_(rule.table_name)
    };

    rows.push(runSingleCustomSqlCheckSafe_(obj, rule));
  });

  return rows;
}


/**
 * Безопасная проверка дублей одного объекта.
 */
function runSingleDuplicateCheckSafe_(obj) {
  try {
    const sql = buildDuplicateCheckQuery_(
      obj.project_id || CONFIG.core.projectId,
      obj.dataset_name,
      obj.table_name,
      obj.primary_key_candidate
    );

    const result = runQuery_(sql, {
      useLegacySql: false,
      location: CONFIG.core.region,
      skipIfTooExpensive: false
    });

    const duplicateRows = result.length
      ? normalizeNumberSafe_(result[0].duplicate_rows)
      : 0;

    return buildQualityRow_(obj, {
      check_group: 'AUTO_DUPLICATES',
      check_name: 'duplicate_by_candidate_key',
      check_result: String(duplicateRows),
      severity: 'critical',
      status: duplicateRows === 0
        ? CONFIG.statuses.quality.pass
        : CONFIG.statuses.quality.fail,
      details: 'Проверен candidate key: ' + normalizePlainString_(obj.primary_key_candidate)
    });
  } catch (error) {
    return buildQualityRow_(obj, {
      check_group: 'AUTO_DUPLICATES',
      check_name: 'duplicate_by_candidate_key',
      check_result: '',
      severity: 'warning',
      status: CONFIG.statuses.quality.error,
      details: extractErrorMessage_(error)
    });
  }
}


/**
 * Безопасная проверка NULL в ключе одного объекта.
 */
function runSingleNullKeyCheckSafe_(obj) {
  try {
    const sql = buildNullKeyCheckQuery_(
      obj.project_id || CONFIG.core.projectId,
      obj.dataset_name,
      obj.table_name,
      obj.primary_key_candidate
    );

    const result = runQuery_(sql, {
      useLegacySql: false,
      location: CONFIG.core.region,
      skipIfTooExpensive: false
    });

    const nullRows = result.length
      ? normalizeNumberSafe_(result[0].null_key_rows)
      : 0;

    return buildQualityRow_(obj, {
      check_group: 'AUTO_NULL_KEYS',
      check_name: 'null_in_candidate_key',
      check_result: String(nullRows),
      severity: 'critical',
      status: nullRows === 0
        ? CONFIG.statuses.quality.pass
        : CONFIG.statuses.quality.fail,
      details: 'Проверен candidate key: ' + normalizePlainString_(obj.primary_key_candidate)
    });
  } catch (error) {
    return buildQualityRow_(obj, {
      check_group: 'AUTO_NULL_KEYS',
      check_name: 'null_in_candidate_key',
      check_result: '',
      severity: 'warning',
      status: CONFIG.statuses.quality.error,
      details: extractErrorMessage_(error)
    });
  }
}


/**
 * Безопасная ручная SQL-проверка.
 *
 * Ожидаемая логика:
 * - если результат содержит status/check_status = FAIL/WARN/PASS — используем его
 * - иначе считаем PASS
 */
function runSingleCustomSqlCheckSafe_(obj, rule) {
  try {
    const sql = String(rule.check_sql || '').trim();

    if (!sql) {
      throw new Error('Пустой check_sql');
    }

    const result = runQuery_(sql, {
      useLegacySql: false,
      location: CONFIG.core.region,
      skipIfTooExpensive: false
    });

    const firstRow = result[0] || {};
    const detectedStatus = detectRuleStatusFromQueryResult_(firstRow);

    return buildQualityRow_(obj, {
      check_group: 'MANUAL_SQL',
      check_name: normalizePlainString_(rule.check_name),
      check_result: stringifyRuleResult_(firstRow),
      severity: normalizePlainString_(rule.severity) || 'warning',
      status: detectedStatus,
      details: ''
    });
  } catch (error) {
    return buildQualityRow_(obj, {
      check_group: 'MANUAL_SQL',
      check_name: normalizePlainString_(rule.check_name),
      check_result: '',
      severity: normalizePlainString_(rule.severity) || 'warning',
      status: CONFIG.statuses.quality.error,
      details: extractErrorMessage_(error)
    });
  }
}


/**
 * Выбирает объекты для тяжелых проверок ключей.
 */
function selectObjectsForHeavyKeyChecks_(enrichedObjects, limit) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const safeLimit = Number(limit || 50);

  return objects
    .filter(function (obj) {
      const tableType = normalizePlainString_(obj.table_type).toUpperCase();
      const key = normalizePlainString_(obj.primary_key_candidate);

      return tableType === CONFIG.objectTypes.baseTable && !!key;
    })
    .sort(compareObjectsForHeavyChecks_)
    .slice(0, safeLimit);
}


/**
 * Приоритет для heavy checks:
 * critical/high сначала, потом stale/fail, потом размер меньше.
 */
function compareObjectsForHeavyChecks_(a, b) {
  const scoreA = calcHeavyCheckPriorityScore_(a);
  const scoreB = calcHeavyCheckPriorityScore_(b);

  if (scoreA !== scoreB) {
    return scoreB - scoreA;
  }

  const sizeA = normalizeNumberSafe_(a.size_gb);
  const sizeB = normalizeNumberSafe_(b.size_gb);

  return Number(sizeA || 0) - Number(sizeB || 0);
}


/**
 * Score приоритета heavy checks.
 */
function calcHeavyCheckPriorityScore_(obj) {
  let score = 0;

  const criticality = normalizePlainString_(obj.business_criticality).toLowerCase();
  const priority = normalizePlainString_(obj.management_priority).toLowerCase();
  const freshness = normalizePlainString_(obj.freshness_status);
  const quality = normalizePlainString_(obj.quality_status);
  const purpose = normalizePlainString_(obj.purpose);

  if (criticality === 'critical') score += 100;
  if (criticality === 'high') score += 80;
  if (priority === 'high') score += 50;
  if (purpose === CONFIG.purposes.executiveControl) score += 40;
  if (freshness === CONFIG.statuses.freshness.stale) score += 20;
  if (quality === CONFIG.statuses.quality.fail) score += 20;

  return score;
}


/**
 * SQL проверки дублей.
 */
function buildDuplicateCheckQuery_(projectId, datasetName, tableName, primaryKeyCandidate) {
  const fq = buildFullyQualifiedTableName_(projectId, datasetName, tableName);
  const keys = parsePrimaryKeyCandidate_(primaryKeyCandidate);

  if (!keys.length) {
    throw new Error('Не удалось разобрать primary_key_candidate');
  }

  const keySql = keys.map(function (key) {
    return '`' + key + '`';
  }).join(', ');

  return `
    SELECT
      COUNT(1) AS duplicate_rows
    FROM (
      SELECT
        ${keySql},
        COUNT(1) AS cnt
      FROM ${fq}
      GROUP BY ${keySql}
      HAVING COUNT(1) > 1
    )
  `;
}


/**
 * SQL проверки NULL в ключе.
 */
function buildNullKeyCheckQuery_(projectId, datasetName, tableName, primaryKeyCandidate) {
  const fq = buildFullyQualifiedTableName_(projectId, datasetName, tableName);
  const keys = parsePrimaryKeyCandidate_(primaryKeyCandidate);

  if (!keys.length) {
    throw new Error('Не удалось разобрать primary_key_candidate');
  }

  const nullConditions = keys.map(function (key) {
    return '`' + key + '` IS NULL';
  }).join(' OR ');

  return `
    SELECT
      COUNT(1) AS null_key_rows
    FROM ${fq}
    WHERE ${nullConditions}
  `;
}


/**
 * Парсит primary key candidate.
 */
function parsePrimaryKeyCandidate_(primaryKeyCandidate) {
  return String(primaryKeyCandidate || '')
    .split(',')
    .map(function (s) {
      return normalizePlainString_(s);
    })
    .filter(function (s) {
      return !!s;
    });
}


/**
 * Fully qualified table name в backticks.
 */
function buildFullyQualifiedTableName_(projectId, datasetName, tableName) {
  const project = normalizePlainString_(projectId) || CONFIG.core.projectId;
  const dataset = normalizePlainString_(datasetName);
  const table = normalizePlainString_(tableName);

  if (!dataset || !table) {
    throw new Error('Не заполнен datasetName/tableName для fully qualified table');
  }

  return '`' + project + '.' + dataset + '.' + table + '`';
}


/**
 * Определяет статус manual SQL rule по первой строке результата.
 */
function detectRuleStatusFromQueryResult_(firstRow) {
  const row = firstRow || {};

  const explicitStatus =
    row.status ||
    row.check_status ||
    row.result_status ||
    row.quality_status;

  if (explicitStatus !== undefined && explicitStatus !== null) {
    const normalized = String(explicitStatus).trim().toUpperCase();

    if (normalized === 'FAIL' || normalized === 'ERROR') {
      return CONFIG.statuses.quality.fail;
    }

    if (normalized === 'WARN' || normalized === 'WARNING') {
      return CONFIG.statuses.quality.warn;
    }

    if (normalized === 'PASS' || normalized === 'OK' || normalized === 'SUCCESS') {
      return CONFIG.statuses.quality.pass;
    }
  }

  const values = Object.keys(row).map(function (key) {
    return String(row[key]).trim().toUpperCase();
  });

  if (values.indexOf('FAIL') !== -1 || values.indexOf('ERROR') !== -1) {
    return CONFIG.statuses.quality.fail;
  }

  if (values.indexOf('WARN') !== -1 || values.indexOf('WARNING') !== -1) {
    return CONFIG.statuses.quality.warn;
  }

  return CONFIG.statuses.quality.pass;
}


/**
 * Строковое представление результата manual rule.
 */
function stringifyRuleResult_(firstRow) {
  const row = firstRow || {};
  const keys = Object.keys(row);

  if (!keys.length) {
    return '';
  }

  return keys.map(function (key) {
    return key + '=' + normalizeUnknownToString_(row[key]);
  }).join('; ');
}


/**
 * История heavy quality.
 * Пока только формирует rows, запись делает writer-слой.
 */
function buildQualityHistoryRows_(qualityRows, runId) {
  if (!CONFIG.features.enableQualityHistory) {
    return [];
  }

  const rows = Array.isArray(qualityRows) ? qualityRows : [];

  return rows.map(function (row) {
    return {
      run_id: normalizePlainString_(runId),
      checked_at: normalizePlainString_(row.checked_at) || getNowProjectTz_(),
      object_key: normalizePlainString_(row.object_key),
      dataset_name: normalizePlainString_(row.dataset_name),
      table_name: normalizePlainString_(row.table_name),
      check_group: normalizePlainString_(row.check_group),
      check_name: normalizePlainString_(row.check_name),
      check_result: normalizeUnknownToString_(row.check_result),
      severity: normalizePlainString_(row.severity),
      status: normalizePlainString_(row.status),
      details: normalizeUnknownToString_(row.details)
    };
  });
}
