/*******************************************************
 * 13_quality_core.gs
 * Быстрые проверки качества без тяжелого сканирования таблиц
 *******************************************************/


/**
 * Строит базовые quality checks.
 *
 * Не выполняет тяжелые SQL по данным.
 * Только checks по уже собранным metadata/enriched objects.
 */
function buildBasicQualityChecks_(enrichedObjects, columnsMap, dateCoverageRows) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const safeColumnsMap = asObjectSafe_(columnsMap);
  const coverageMap = buildDateCoverageMap_(dateCoverageRows);

  const rows = [];

  objects.forEach(function (obj) {
    Array.prototype.push.apply(rows, buildBasicQualityChecksForObject_(obj, safeColumnsMap, coverageMap));
  });

  return rows;
}


/**
 * Базовые проверки одного объекта.
 */
function buildBasicQualityChecksForObject_(obj, columnsMap, coverageMap) {
  const object = obj || {};
  const objectKey = normalizePlainString_(object.object_key);
  const columns = asArraySafe_(columnsMap[objectKey]);
  const coverage = coverageMap[objectKey] || {};

  const rows = [];

  rows.push(checkObjectHasColumns_(object, columns));
  rows.push(checkRowCountPresent_(object));
  rows.push(checkDescriptionPresent_(object));
  rows.push(checkDateFieldCandidate_(object));
  rows.push(checkFreshnessStatus_(object));
  rows.push(checkPrimaryKeyCandidatePresent_(object));
  rows.push(checkJoinKeysPresent_(object));
  rows.push(checkOwnerPresent_(object));
  rows.push(checkSlaPresent_(object));
  rows.push(checkRiskFlag_(object));
  rows.push(checkScanRisk_(object));
  rows.push(checkDateCoverageStatus_(object, coverage));

  return rows;
}


/**
 * Проверка: у объекта есть metadata columns.
 */
function checkObjectHasColumns_(obj, columns) {
  const cols = Array.isArray(columns) ? columns : [];
  const pass = cols.length > 0;

  return buildQualityRow_(obj, {
    check_group: 'AUTO_STRUCTURE',
    check_name: 'columns_metadata_present',
    check_result: String(cols.length),
    severity: 'critical',
    status: pass ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.fail,
    details: pass ? '' : 'Не найдены метаданные колонок'
  });
}


/**
 * Проверка: row_count присутствует.
 */
function checkRowCountPresent_(obj) {
  const tableType = normalizePlainString_(obj.table_type).toUpperCase();
  const rowCount = normalizeNumberSafe_(obj.row_count);

  if (tableType === CONFIG.objectTypes.view) {
    return buildQualityRow_(obj, {
      check_group: 'AUTO_BASIC',
      check_name: 'row_count_present',
      check_result: '',
      severity: 'info',
      status: CONFIG.statuses.quality.pass,
      details: 'Для VIEW row_count не является обязательным'
    });
  }

  const pass = rowCount !== '' && rowCount > 0;

  return buildQualityRow_(obj, {
    check_group: 'AUTO_BASIC',
    check_name: 'row_count_present',
    check_result: rowCount === '' ? '' : String(rowCount),
    severity: 'critical',
    status: pass ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.fail,
    details: pass ? '' : 'Таблица пустая или row_count отсутствует'
  });
}


/**
 * Проверка: описание объекта заполнено.
 */
function checkDescriptionPresent_(obj) {
  const description = normalizePlainString_(obj.description);
  const pass = !!description;

  return buildQualityRow_(obj, {
    check_group: 'AUTO_GOVERNANCE',
    check_name: 'description_present',
    check_result: pass ? 'present' : '',
    severity: 'warning',
    status: pass ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.warn,
    details: pass ? '' : 'Нет описания объекта'
  });
}


/**
 * Проверка: найден date field candidate.
 */
function checkDateFieldCandidate_(obj) {
  const dateField = normalizePlainString_(obj.date_field_candidate);
  const tableType = normalizePlainString_(obj.table_type).toUpperCase();

  if (tableType === CONFIG.objectTypes.view) {
    return buildQualityRow_(obj, {
      check_group: 'AUTO_STRUCTURE',
      check_name: 'date_field_candidate_exists',
      check_result: dateField,
      severity: 'info',
      status: dateField ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.warn,
      details: dateField ? '' : 'Для VIEW date field candidate не найден'
    });
  }

  return buildQualityRow_(obj, {
    check_group: 'AUTO_STRUCTURE',
    check_name: 'date_field_candidate_exists',
    check_result: dateField,
    severity: 'warning',
    status: dateField ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.warn,
    details: dateField ? '' : 'Не найдено поле DATE/TIMESTAMP/DATETIME-кандидат'
  });
}


/**
 * Проверка: freshness статус.
 */
function checkFreshnessStatus_(obj) {
  const status = normalizePlainString_(obj.freshness_status);
  const comment = normalizePlainString_(obj.freshness_comment);

  let qualityStatus = CONFIG.statuses.quality.fail;

  if (status === CONFIG.statuses.freshness.fresh) {
    qualityStatus = CONFIG.statuses.quality.pass;
  } else if (status === CONFIG.statuses.freshness.late) {
    qualityStatus = CONFIG.statuses.quality.warn;
  } else if (status === CONFIG.statuses.freshness.unknown) {
    qualityStatus = CONFIG.statuses.quality.warn;
  }

  return buildQualityRow_(obj, {
    check_group: 'AUTO_FRESHNESS',
    check_name: 'freshness_status',
    check_result: status,
    severity: 'critical',
    status: qualityStatus,
    details: comment
  });
}


/**
 * Проверка: primary key candidate.
 */
function checkPrimaryKeyCandidatePresent_(obj) {
  const key = normalizePlainString_(obj.primary_key_candidate);
  const pass = !!key;

  return buildQualityRow_(obj, {
    check_group: 'AUTO_MODEL',
    check_name: 'primary_key_candidate_present',
    check_result: key,
    severity: 'warning',
    status: pass ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.warn,
    details: pass ? '' : 'Не задан и не найден candidate key'
  });
}


/**
 * Проверка: join keys.
 */
function checkJoinKeysPresent_(obj) {
  const keys = normalizePlainString_(obj.join_keys);
  const pass = !!keys;

  return buildQualityRow_(obj, {
    check_group: 'AUTO_MODEL',
    check_name: 'join_keys_present',
    check_result: keys,
    severity: 'warning',
    status: pass ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.warn,
    details: pass ? '' : 'Не найдены join keys'
  });
}


/**
 * Проверка: owner.
 */
function checkOwnerPresent_(obj) {
  const owner = normalizePlainString_(obj.owner);
  const pass = !!owner;

  return buildQualityRow_(obj, {
    check_group: 'AUTO_GOVERNANCE',
    check_name: 'owner_present',
    check_result: owner,
    severity: 'warning',
    status: pass ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.warn,
    details: pass ? '' : 'Не указан владелец объекта'
  });
}


/**
 * Проверка: SLA.
 */
function checkSlaPresent_(obj) {
  const threshold = normalizeNumberSafe_(obj.freshness_threshold_hours);
  const expected = normalizePlainString_(obj.expected_refresh_frequency);

  const pass = threshold !== '' && !!expected;

  return buildQualityRow_(obj, {
    check_group: 'AUTO_GOVERNANCE',
    check_name: 'sla_present',
    check_result: 'expected=' + expected + '; threshold_hours=' + threshold,
    severity: 'warning',
    status: pass ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.warn,
    details: pass ? '' : 'Не полностью задан SLA свежести'
  });
}


/**
 * Проверка: risk flag.
 */
function checkRiskFlag_(obj) {
  const riskFlag = normalizePlainString_(obj.risk_flag);
  const pass = riskFlag === CONFIG.riskFlags.ok;

  return buildQualityRow_(obj, {
    check_group: 'AUTO_RISK',
    check_name: 'risk_flag',
    check_result: riskFlag,
    severity: riskFlag === CONFIG.riskFlags.tooBig ? 'critical' : 'warning',
    status: pass ? CONFIG.statuses.quality.pass : CONFIG.statuses.quality.warn,
    details: pass ? '' : 'Есть технический/метаданный риск: ' + riskFlag
  });
}


/**
 * Проверка: scan risk.
 */
function checkScanRisk_(obj) {
  const scanRisk = normalizePlainString_(obj.scan_risk);

  let status = CONFIG.statuses.quality.pass;
  let severity = 'info';

  if (scanRisk === CONFIG.scanRisk.medium) {
    status = CONFIG.statuses.quality.warn;
    severity = 'warning';
  }

  if (scanRisk === CONFIG.scanRisk.high || scanRisk === CONFIG.scanRisk.veryHigh) {
    status = CONFIG.statuses.quality.warn;
    severity = 'critical';
  }

  return buildQualityRow_(obj, {
    check_group: 'AUTO_COST',
    check_name: 'scan_risk',
    check_result: scanRisk,
    severity: severity,
    status: status,
    details: status === CONFIG.statuses.quality.pass ? '' : 'Повышенный риск дорогого чтения'
  });
}


/**
 * Проверка: date coverage.
 */
function checkDateCoverageStatus_(obj, coverage) {
  const dateField = normalizePlainString_(obj.date_field_candidate);

  if (!dateField) {
    return buildQualityRow_(obj, {
      check_group: 'AUTO_DATE_COVERAGE',
      check_name: 'date_coverage_status',
      check_result: '',
      severity: 'info',
      status: CONFIG.statuses.quality.pass,
      details: 'Date coverage не требуется: date field candidate отсутствует'
    });
  }

  const coverageStatus = normalizePlainString_(coverage.status);

  if (!coverageStatus) {
    return buildQualityRow_(obj, {
      check_group: 'AUTO_DATE_COVERAGE',
      check_name: 'date_coverage_status',
      check_result: '',
      severity: 'warning',
      status: CONFIG.statuses.quality.warn,
      details: 'Date coverage еще не проверялся'
    });
  }

  let status = CONFIG.statuses.quality.pass;
  if (coverageStatus === CONFIG.statuses.generic.error) {
    status = CONFIG.statuses.quality.fail;
  } else if (
    coverageStatus === CONFIG.statuses.generic.emptyResult ||
    coverageStatus === CONFIG.statuses.generic.skippedByLimit
  ) {
    status = CONFIG.statuses.quality.warn;
  }

  return buildQualityRow_(obj, {
    check_group: 'AUTO_DATE_COVERAGE',
    check_name: 'date_coverage_status',
    check_result: coverageStatus,
    severity: 'warning',
    status: status,
    details: normalizePlainString_(coverage.comment)
  });
}


/**
 * Применяет quality flags к объектам.
 */
function applyQualityFlagsToObjects_(enrichedObjects, qualityRows) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const rows = Array.isArray(qualityRows) ? qualityRows : [];

  const grouped = {};

  rows.forEach(function (row) {
    const key = normalizePlainString_(row.object_key) ||
      buildObjectKey_(CONFIG.core.projectId, row.dataset_name, row.table_name);

    if (!grouped[key]) {
      grouped[key] = [];
    }

    grouped[key].push(row);
  });

  objects.forEach(function (obj) {
    const key = normalizePlainString_(obj.object_key);
    const objectRows = grouped[key] || [];

    const failCount = objectRows.filter(function (row) {
      return row.status === CONFIG.statuses.quality.fail ||
        row.status === CONFIG.statuses.quality.error;
    }).length;

    const warnCount = objectRows.filter(function (row) {
      return row.status === CONFIG.statuses.quality.warn;
    }).length;

    obj.quality_issues_count = failCount + warnCount;

    if (failCount > 0) {
      obj.quality_status = CONFIG.statuses.quality.fail;
    } else if (warnCount > 0) {
      obj.quality_status = CONFIG.statuses.quality.warn;
    } else {
      obj.quality_status = CONFIG.statuses.quality.pass;
    }
  });

  return objects;
}


/**
 * Строит строку quality check.
 */
function buildQualityRow_(obj, check) {
  const object = obj || {};
  const payload = check || {};

  return {
    object_key: normalizePlainString_(object.object_key),
    dataset_name: normalizePlainString_(object.dataset_name),
    table_name: normalizePlainString_(object.table_name),
    check_group: normalizePlainString_(payload.check_group),
    check_name: normalizePlainString_(payload.check_name),
    check_result: normalizeUnknownToString_(payload.check_result),
    severity: normalizePlainString_(payload.severity),
    status: normalizePlainString_(payload.status),
    details: normalizeUnknownToString_(payload.details),
    checked_at: getNowProjectTz_()
  };
}


/**
 * Map coverage по object_key.
 */
function buildDateCoverageMap_(dateCoverageRows) {
  const rows = Array.isArray(dateCoverageRows) ? dateCoverageRows : [];
  const map = {};

  rows.forEach(function (row) {
    const key = normalizePlainString_(row.object_key) ||
      buildObjectKey_(CONFIG.core.projectId, row.dataset_name, row.table_name);

    if (!key) {
      return;
    }

    map[key] = row;
  });

  return map;
}


/**
 * Backward-compatible функция, которую вызывает 01_main.gs.
 *
 * В FAST она даст только core checks.
 * Heavy checks добавим в 14_quality_heavy.gs.
 */
function buildQualityChecksRows_(enrichedObjects, columnsMap, dateCoverageRows, qualityRules) {
  return buildBasicQualityChecks_(enrichedObjects, columnsMap, dateCoverageRows);
}
