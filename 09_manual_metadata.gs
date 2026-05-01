/*******************************************************
 * 09_manual_metadata.gs
 * Чтение и валидация ручных справочников
 *******************************************************/


/**
 * Создает обязательные ручные листы, если их нет.
 */
function ensureCoreSheetsExist_() {
  ensureManualSheet_(
    CONFIG.sheets.manualObjectMetadata,
    getManualObjectMetadataHeaders_()
  );

  ensureManualSheet_(
    CONFIG.sheets.manualDependencies,
    getManualDependenciesHeaders_()
  );

  ensureManualSheet_(
    CONFIG.sheets.manualQualityRules,
    getManualQualityRulesHeaders_()
  );

  ensureManualSheet_(
    CONFIG.sheets.manualObjectSla,
    getManualObjectSlaHeaders_()
  );

  ensureManualSheet_(
    CONFIG.sheets.manualObjectOwners,
    getManualObjectOwnersHeaders_()
  );

  ensureLoggingSheetsExist_();
}


/**
 * Читает ручные метаданные объектов.
 *
 * Поддерживает:
 * - object_key
 * - либо dataset_name + table_name
 *
 * Возвращает map:
 * {
 *   "project.dataset.table": {...},
 *   "dataset.table": {...}   // для обратной совместимости
 * }
 */
function readObjectsMetadataMap_() {
  const sheet = getOrCreateSheet_(CONFIG.sheets.manualObjectMetadata);
  const rows = readSheetObjects_(sheet);

  const map = {};

  rows.forEach(function (row) {
    const objectKey = resolveManualObjectKey_(row);
    const datasetName = normalizePlainString_(row.dataset_name);
    const tableName = normalizePlainString_(row.table_name);

    if (!objectKey || !datasetName || !tableName) {
      return;
    }

    const normalizedRow = {
      object_key: objectKey,
      project_id: extractProjectIdFromObjectKey_(objectKey) || CONFIG.core.projectId,
      dataset_name: datasetName,
      table_name: tableName,

      refresh_mechanism: normalizePlainString_(row.refresh_mechanism),
      expected_refresh_frequency: normalizePlainString_(row.expected_refresh_frequency),
      freshness_threshold_hours: normalizeNumberSafe_(row.freshness_threshold_hours),

      business_criticality: normalizePlainString_(row.business_criticality),
      management_priority: normalizePlainString_(row.management_priority),

      grain: normalizePlainString_(row.grain),
      primary_key_candidate: normalizePlainString_(row.primary_key_candidate),
      join_keys: normalizePlainString_(row.join_keys),

      business_definition: normalizePlainString_(row.business_definition),
      allowed_use: normalizePlainString_(row.allowed_use),
      not_recommended_use: normalizePlainString_(row.not_recommended_use),
      incident_if_broken: normalizePlainString_(row.incident_if_broken),

      owner: normalizePlainString_(row.owner),
      sla_tier: normalizePlainString_(row.sla_tier),
      description: normalizePlainString_(row.description)
    };

    map[objectKey] = normalizedRow;
    map[datasetName + '.' + tableName] = normalizedRow;
  });

  return map;
}


/**
 * Читает ручные зависимости.
 *
 * Возвращает массив строк.
 */
function readManualDependencies_() {
  const sheet = getOrCreateSheet_(CONFIG.sheets.manualDependencies);
  const rows = readSheetObjects_(sheet);

  return rows
    .map(function (row) {
      const objectKey = resolveManualObjectKey_(row);

      return {
        object_key: objectKey,
        dataset_name: normalizePlainString_(row.dataset_name),
        table_name: normalizePlainString_(row.table_name),
        depends_on: normalizePlainString_(row.depends_on),
        dependency_type: normalizePlainString_(row.dependency_type) || CONFIG.lineage.dependencyTypeManual,
        refresh_mechanism: normalizePlainString_(row.refresh_mechanism),
        comment: normalizePlainString_(row.comment)
      };
    })
    .filter(function (row) {
      return !!row.object_key && !!row.depends_on;
    });
}


/**
 * Читает ручные quality rules.
 *
 * Возвращает массив строк.
 */
function readQualityRules_() {
  const sheet = getOrCreateSheet_(CONFIG.sheets.manualQualityRules);
  const rows = readSheetObjects_(sheet);

  return rows
    .map(function (row) {
      const objectKey = resolveManualObjectKey_(row);

      return {
        object_key: objectKey,
        dataset_name: normalizePlainString_(row.dataset_name),
        table_name: normalizePlainString_(row.table_name),
        check_name: normalizePlainString_(row.check_name),
        check_sql: row.check_sql !== undefined ? String(row.check_sql) : '',
        severity: normalizePlainString_(row.severity) || 'warning',
        is_enabled: normalizeBooleanManualSafe_(row.is_enabled)
      };
    })
    .filter(function (row) {
      return !!row.object_key && !!row.check_name;
    });
}


/**
 * Читает SLA map.
 *
 * Возвращает map:
 * {
 *   "project.dataset.table": {...},
 *   "dataset.table": {...}
 * }
 */
function readObjectSlaMap_() {
  const sheet = getOrCreateSheet_(CONFIG.sheets.manualObjectSla);
  const rows = readSheetObjects_(sheet);

  const map = {};

  rows.forEach(function (row) {
    const objectKey = resolveManualObjectKey_(row);
    const datasetName = normalizePlainString_(row.dataset_name);
    const tableName = normalizePlainString_(row.table_name);

    if (!objectKey || !datasetName || !tableName) {
      return;
    }

    const normalizedRow = {
      object_key: objectKey,
      dataset_name: datasetName,
      table_name: tableName,
      expected_refresh_frequency: normalizePlainString_(row.expected_refresh_frequency),
      freshness_threshold_hours: normalizeNumberSafe_(row.freshness_threshold_hours),
      business_criticality: normalizePlainString_(row.business_criticality),
      management_priority: normalizePlainString_(row.management_priority),
      sla_tier: normalizePlainString_(row.sla_tier)
    };

    map[objectKey] = normalizedRow;
    map[datasetName + '.' + tableName] = normalizedRow;
  });

  return map;
}


/**
 * Читает owners map.
 *
 * Возвращает map:
 * {
 *   "project.dataset.table": {...},
 *   "dataset.table": {...}
 * }
 */
function readOwnersMap_() {
  const sheet = getOrCreateSheet_(CONFIG.sheets.manualObjectOwners);
  const rows = readSheetObjects_(sheet);

  const map = {};

  rows.forEach(function (row) {
    const objectKey = resolveManualObjectKey_(row);
    const datasetName = normalizePlainString_(row.dataset_name);
    const tableName = normalizePlainString_(row.table_name);

    if (!objectKey || !datasetName || !tableName) {
      return;
    }

    const normalizedRow = {
      object_key: objectKey,
      dataset_name: datasetName,
      table_name: tableName,
      owner: normalizePlainString_(row.owner),
      owner_email: normalizePlainString_(row.owner_email),
      team: normalizePlainString_(row.team),
      comment: normalizePlainString_(row.comment)
    };

    map[objectKey] = normalizedRow;
    map[datasetName + '.' + tableName] = normalizedRow;
  });

  return map;
}


/**
 * Полная валидация ручных справочников.
 *
 * knownObjectKeys:
 * массив ключей project.dataset.table из реальных объектов
 *
 * Возвращает:
 * {
 *   validationRows: [...],
 *   errorsCount,
 *   warningsCount
 * }
 */
function validateManualMetadata_(knownObjectKeys) {
  const knownKeysMap = buildKnownKeysMap_(knownObjectKeys);

  const validationRows = []
    .concat(validateManualObjectMetadataSheet_(knownKeysMap))
    .concat(validateManualDependenciesSheet_(knownKeysMap))
    .concat(validateManualQualityRulesSheet_(knownKeysMap))
    .concat(validateManualObjectSlaSheet_(knownKeysMap))
    .concat(validateManualObjectOwnersSheet_(knownKeysMap));

  const errorsCount = validationRows.filter(function (row) {
    return row.status === 'ERROR';
  }).length;

  const warningsCount = validationRows.filter(function (row) {
    return row.status === 'WARN';
  }).length;

  return {
    validationRows: validationRows,
    errorsCount: errorsCount,
    warningsCount: warningsCount
  };
}


/**
 * Валидация листа ручных метаданных объектов.
 */
function validateManualObjectMetadataSheet_(knownKeysMap) {
  const rows = readSheetObjects_(getOrCreateSheet_(CONFIG.sheets.manualObjectMetadata));
  const results = [];
  const seen = {};

  rows.forEach(function (row, idx) {
    const rowNumber = idx + 2;
    const objectKey = resolveManualObjectKey_(row);
    const duplicateKey = objectKey ? ('META|' + objectKey) : '';

    if (!objectKey) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectMetadata,
        rowNumber,
        'object_key',
        'ERROR',
        'Не заполнен object_key и/или dataset_name + table_name'
      ));
      return;
    }

    if (seen[duplicateKey]) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectMetadata,
        rowNumber,
        objectKey,
        'WARN',
        'Дублирующая строка для объекта'
      ));
    }
    seen[duplicateKey] = true;

    if (!knownKeysMap[objectKey]) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectMetadata,
        rowNumber,
        objectKey,
        'WARN',
        'Объект не найден среди реальных объектов BigQuery'
      ));
    }

    const threshold = normalizeNumberSafe_(row.freshness_threshold_hours);
    if (row.freshness_threshold_hours !== '' && threshold === '') {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectMetadata,
        rowNumber,
        objectKey,
        'ERROR',
        'freshness_threshold_hours не является числом'
      ));
    }
  });

  return results;
}


/**
 * Валидация листа зависимостей.
 */
function validateManualDependenciesSheet_(knownKeysMap) {
  const rows = readSheetObjects_(getOrCreateSheet_(CONFIG.sheets.manualDependencies));
  const results = [];

  rows.forEach(function (row, idx) {
    const rowNumber = idx + 2;
    const objectKey = resolveManualObjectKey_(row);
    const dependsOn = normalizePlainString_(row.depends_on);

    if (!objectKey) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualDependencies,
        rowNumber,
        'object_key',
        'ERROR',
        'Не заполнен object_key и/или dataset_name + table_name'
      ));
      return;
    }

    if (!dependsOn) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualDependencies,
        rowNumber,
        objectKey,
        'ERROR',
        'Не заполнено depends_on'
      ));
      return;
    }

    if (!knownKeysMap[objectKey]) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualDependencies,
        rowNumber,
        objectKey,
        'WARN',
        'Объект-источник зависимости не найден среди реальных объектов'
      ));
    }
  });

  return results;
}


/**
 * Валидация листа quality rules.
 */
function validateManualQualityRulesSheet_(knownKeysMap) {
  const rows = readSheetObjects_(getOrCreateSheet_(CONFIG.sheets.manualQualityRules));
  const results = [];
  const allowedSeverity = {
    critical: true,
    warning: true,
    info: true
  };

  rows.forEach(function (row, idx) {
    const rowNumber = idx + 2;
    const objectKey = resolveManualObjectKey_(row);
    const checkName = normalizePlainString_(row.check_name);
    const checkSql = row.check_sql !== undefined ? String(row.check_sql).trim() : '';
    const severity = normalizePlainString_(row.severity).toLowerCase();

    if (!objectKey) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualQualityRules,
        rowNumber,
        'object_key',
        'ERROR',
        'Не заполнен object_key и/или dataset_name + table_name'
      ));
      return;
    }

    if (!knownKeysMap[objectKey]) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualQualityRules,
        rowNumber,
        objectKey,
        'WARN',
        'Объект для quality rule не найден среди реальных объектов'
      ));
    }

    if (!checkName) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualQualityRules,
        rowNumber,
        objectKey,
        'ERROR',
        'Не заполнен check_name'
      ));
    }

    if (!checkSql) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualQualityRules,
        rowNumber,
        objectKey,
        'ERROR',
        'Пустой check_sql'
      ));
    }

    if (severity && !allowedSeverity[severity]) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualQualityRules,
        rowNumber,
        objectKey,
        'WARN',
        'severity должен быть critical / warning / info'
      ));
    }
  });

  return results;
}


/**
 * Валидация SLA листа.
 */
function validateManualObjectSlaSheet_(knownKeysMap) {
  const rows = readSheetObjects_(getOrCreateSheet_(CONFIG.sheets.manualObjectSla));
  const results = [];

  rows.forEach(function (row, idx) {
    const rowNumber = idx + 2;
    const objectKey = resolveManualObjectKey_(row);
    const threshold = normalizeNumberSafe_(row.freshness_threshold_hours);

    if (!objectKey) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectSla,
        rowNumber,
        'object_key',
        'ERROR',
        'Не заполнен object_key и/или dataset_name + table_name'
      ));
      return;
    }

    if (!knownKeysMap[objectKey]) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectSla,
        rowNumber,
        objectKey,
        'WARN',
        'Объект SLA не найден среди реальных объектов'
      ));
    }

    if (row.freshness_threshold_hours !== '' && threshold === '') {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectSla,
        rowNumber,
        objectKey,
        'ERROR',
        'freshness_threshold_hours не является числом'
      ));
    }
  });

  return results;
}


/**
 * Валидация owners листа.
 */
function validateManualObjectOwnersSheet_(knownKeysMap) {
  const rows = readSheetObjects_(getOrCreateSheet_(CONFIG.sheets.manualObjectOwners));
  const results = [];

  rows.forEach(function (row, idx) {
    const rowNumber = idx + 2;
    const objectKey = resolveManualObjectKey_(row);
    const owner = normalizePlainString_(row.owner);

    if (!objectKey) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectOwners,
        rowNumber,
        'object_key',
        'ERROR',
        'Не заполнен object_key и/или dataset_name + table_name'
      ));
      return;
    }

    if (!knownKeysMap[objectKey]) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectOwners,
        rowNumber,
        objectKey,
        'WARN',
        'Объект owner map не найден среди реальных объектов'
      ));
    }

    if (!owner) {
      results.push(buildValidationRow_(
        CONFIG.sheets.manualObjectOwners,
        rowNumber,
        objectKey,
        'WARN',
        'Не заполнен owner'
      ));
    }
  });

  return results;
}


/**
 * Создает/проверяет ручной лист
 */
function ensureManualSheet_(sheetName, headers) {
  const sheet = getOrCreateSheet_(sheetName);
  ensureSheetHeaders_(sheet, headers);
  return sheet;
}


/**
 * Чтение листа в массив объектов
 */
function readSheetObjects_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow < 2 || lastColumn < 1) {
    return [];
  }

  const values = sheet.getRange(1, 1, lastRow, lastColumn).getValues();
  const headers = values[0].map(function (h) {
    return String(h || '').trim();
  });

  return values.slice(1).map(function (row) {
    const obj = {};
    headers.forEach(function (header, idx) {
      obj[header] = row[idx];
    });
    return obj;
  });
}


/**
 * Заголовки листа Метаданные объектов
 */
function getManualObjectMetadataHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'refresh_mechanism',
    'expected_refresh_frequency',
    'freshness_threshold_hours',
    'business_criticality',
    'management_priority',
    'grain',
    'primary_key_candidate',
    'join_keys',
    'business_definition',
    'allowed_use',
    'not_recommended_use',
    'incident_if_broken',
    'owner',
    'sla_tier',
    'description'
  ];
}


/**
 * Заголовки листа Зависимости справочник
 */
function getManualDependenciesHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'depends_on',
    'dependency_type',
    'refresh_mechanism',
    'comment'
  ];
}


/**
 * Заголовки листа Правила качества
 */
function getManualQualityRulesHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'check_name',
    'check_sql',
    'severity',
    'is_enabled'
  ];
}


/**
 * Заголовки листа SLA объектов
 */
function getManualObjectSlaHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'expected_refresh_frequency',
    'freshness_threshold_hours',
    'business_criticality',
    'management_priority',
    'sla_tier'
  ];
}


/**
 * Заголовки листа Владельцы объектов
 */
function getManualObjectOwnersHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'owner',
    'owner_email',
    'team',
    'comment'
  ];
}


/**
 * Построить строку результата валидации
 */
function buildValidationRow_(sheetName, rowNumber, objectKey, status, message) {
  return {
    sheet_name: sheetName,
    row_number: rowNumber,
    object_key: objectKey,
    status: status,
    message: message
  };
}


/**
 * Map известных ключей
 */
function buildKnownKeysMap_(knownObjectKeys) {
  const map = {};

  asArraySafe_(knownObjectKeys).forEach(function (key) {
    const normalizedKey = normalizePlainString_(key);
    if (!normalizedKey) {
      return;
    }

    map[normalizedKey] = true;

    const shortKey = extractShortObjectKey_(normalizedKey);
    if (shortKey) {
      map[shortKey] = true;
    }
  });

  return map;
}


/**
 * Извлечь project_id из object_key
 */
function extractProjectIdFromObjectKey_(objectKey) {
  const parts = String(objectKey || '').split('.');
  return parts.length >= 3 ? parts[0] : '';
}


/**
 * Boolean-нормализация для ручных листов
 */
function normalizeBooleanManualSafe_(value) {
  const normalized = String(value === null || value === undefined ? '' : value)
    .trim()
    .toLowerCase();

  return (
    normalized === 'true' ||
    normalized === '1' ||
    normalized === 'yes' ||
    normalized === 'y' ||
    normalized === 'да'
  );
}
