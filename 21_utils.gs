/*******************************************************
 * 21_utils.gs
 * Общие утилиты проекта
 *******************************************************/


/**
 * Безопасная нормализация строки.
 */
function normalizePlainString_(value) {
  if (value === null || value === undefined) {
    return '';
  }

  return String(value).trim();
}


/**
 * Безопасная нормализация строки.
 * Alias для совместимости с ранними файлами.
 */
function normalizeStringSafe_(value) {
  return normalizePlainString_(value);
}


/**
 * Безопасная нормализация числа.
 */
function normalizeNumberSafe_(value) {
  if (value === null || value === undefined || value === '') {
    return '';
  }

  const n = Number(value);
  return isNaN(n) ? '' : n;
}


/**
 * Безопасная нормализация boolean.
 */
function normalizeBooleanSafe_(value) {
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


/**
 * Alias для ручных листов.
 */
function normalizeBooleanManualSafe_(value) {
  return normalizeBooleanSafe_(value);
}


/**
 * Нормализация сложного значения в строку.
 */
function normalizeUnknownToString_(value) {
  if (value === null || value === undefined) {
    return '';
  }

  if (value instanceof Date) {
    return formatProjectDateTime_(value);
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
 * Alias для старого имени.
 */
function normalizeUnknownValueSafe_(value) {
  return normalizeUnknownToString_(value);
}


/**
 * Безопасная нормализация даты/времени.
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


/**
 * Формат даты/времени проекта.
 */
function formatProjectDateTime_(dateObj) {
  const d = dateObj instanceof Date ? dateObj : new Date(dateObj);

  if (isNaN(d.getTime())) {
    return '';
  }

  return Utilities.formatDate(
    d,
    CONFIG.core.timezone,
    'yyyy-MM-dd HH:mm:ss'
  );
}


/**
 * Текущее время в timezone проекта.
 */
function getNowProjectTz_() {
  return formatProjectDateTime_(new Date());
}


/**
 * Безопасное округление.
 */
function roundNumberSafe_(value, digits) {
  const n = Number(value);
  if (isNaN(n)) {
    return '';
  }

  const p = Math.pow(10, Number(digits || 0));
  return Math.round(n * p) / p;
}


/**
 * Alias для старого имени.
 */
function round_(value, digits) {
  return roundNumberSafe_(value, digits);
}


/**
 * Приведение к массиву.
 */
function asArraySafe_(value) {
  return Array.isArray(value) ? value : [];
}


/**
 * Приведение к plain object.
 */
function asObjectSafe_(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }

  return value;
}


/**
 * Первый непустой элемент.
 */
function firstNonEmpty_(values) {
  const items = asArraySafe_(values);

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
 * Безопасное получение поля объекта.
 */
function safeGet_(obj, key, fallback) {
  if (!obj || typeof obj !== 'object') {
    return fallback;
  }

  return obj[key] !== undefined ? obj[key] : fallback;
}


/**
 * Безопасное получение вложенного пути.
 */
function safeGetPath_(obj, path, fallback) {
  if (!obj || !path || !path.length) {
    return fallback;
  }

  let current = obj;

  for (var i = 0; i < path.length; i++) {
    if (current === null || current === undefined) {
      return fallback;
    }

    current = current[path[i]];
  }

  return current === undefined ? fallback : current;
}


/**
 * Строгое сравнение массивов.
 */
function arraysEqualStrict_(a, b) {
  if (!a || !b) return false;
  if (a.length !== b.length) return false;

  for (var i = 0; i < a.length; i++) {
    if (String(a[i] || '') !== String(b[i] || '')) {
      return false;
    }
  }

  return true;
}


/**
 * Размер объекта.
 */
function getObjectSizeSafe_(obj) {
  return Object.keys(asObjectSafe_(obj)).length;
}


/**
 * Удаление дублей массива строк.
 */
function dedupePlainArray_(items) {
  const arr = asArraySafe_(items);
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
 * Count by field.
 */
function countBy_(rows, field) {
  const items = asArraySafe_(rows);
  const map = {};

  items.forEach(function (row) {
    const key = normalizePlainString_(row[field]) || 'EMPTY';
    map[key] = (map[key] || 0) + 1;
  });

  return map;
}


/**
 * Dedupe rows by fields.
 */
function dedupeRowsByFields_(rows, fields) {
  const items = asArraySafe_(rows);
  const keys = asArraySafe_(fields);
  const seen = {};
  const result = [];

  items.forEach(function (row) {
    const key = keys.map(function (field) {
      return normalizePlainString_(row[field]);
    }).join('|');

    if (seen[key]) {
      return;
    }

    seen[key] = true;
    result.push(row);
  });

  return result;
}


/**
 * Header index.
 */
function buildHeaderIndex_(headers) {
  const idx = {};

  asArraySafe_(headers).forEach(function (header, i) {
    idx[normalizePlainString_(header)] = i;
  });

  return idx;
}


/**
 * Get cell by header index.
 */
function getCellByIndex_(row, idx, headerName) {
  const index = idx ? idx[headerName] : undefined;
  return index === undefined ? '' : row[index];
}


/**
 * Build object key.
 * Формат: project.dataset.object
 */
function buildObjectKey_(projectId, datasetName, objectName) {
  return [
    normalizePlainString_(projectId),
    normalizePlainString_(datasetName),
    normalizePlainString_(objectName)
  ].join('.');
}


/**
 * Build dataset key.
 * Формат: project.dataset
 */
function buildDatasetKey_(projectId, datasetName) {
  return [
    normalizePlainString_(projectId),
    normalizePlainString_(datasetName)
  ].join('.');
}


/**
 * Извлекает short key dataset.table из project.dataset.table.
 */
function extractShortObjectKey_(objectKey) {
  const parts = String(objectKey || '').split('.');
  if (parts.length < 3) {
    return '';
  }

  return parts[1] + '.' + parts[2];
}


/**
 * Извлекает project_id из object_key.
 */
function extractProjectIdFromObjectKey_(objectKey) {
  const parts = String(objectKey || '').split('.');
  return parts.length >= 3 ? parts[0] : '';
}


/**
 * Resolve manual object key.
 * Поддерживает:
 * - row.object_key
 * - row.dataset_name + row.table_name
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
 * Parse primary key candidate.
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
 * Проверка режима запуска.
 */
function assertExecutionMode_(mode) {
  const normalized = String(mode || '').toUpperCase().trim();
  const allowed = ['FAST', 'FULL'];

  if (allowed.indexOf(normalized) === -1) {
    throw new Error(
      'Недопустимый режим запуска: ' + mode + '. Разрешено только FAST или FULL.'
    );
  }

  return normalized;
}


/**
 * Санитизация части ID.
 */
function sanitizeIdPart_(value) {
  return String(value || '')
    .trim()
    .replace(/\s+/g, '_')
    .replace(/[^a-zA-Z0-9_]/g, '')
    .slice(0, 40) || 'entry';
}


/**
 * Error message.
 */
function extractErrorMessage_(error) {
  if (!error) return 'Unknown error';

  if (typeof error === 'string') {
    return error;
  }

  if (error && error.message) {
    return String(error.message);
  }

  try {
    return JSON.stringify(error);
  } catch (jsonError) {
    return String(error);
  }
}


/**
 * Error stack.
 */
function extractErrorStack_(error) {
  if (!error) return '';

  if (error && error.stack) {
    return String(error.stack);
  }

  return '';
}


/**
 * Date parse safe.
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


/**
 * Lag hours.
 */
function calculateLagHours_(fromDate, toDate) {
  if (!(fromDate instanceof Date) || !(toDate instanceof Date)) {
    return '';
  }

  const diffMs = toDate.getTime() - fromDate.getTime();
  return roundNumberSafe_(diffMs / (1000 * 60 * 60), 2);
}


/**
 * Bytes -> MB.
 */
function roundBytesToMb_(bytes) {
  return roundNumberSafe_(Number(bytes) / 1024 / 1024, 2);
}


/**
 * Bytes -> GB.
 */
function roundBytesToGb_(bytes) {
  return roundNumberSafe_(Number(bytes) / 1024 / 1024 / 1024, 2);
}


/**
 * Build known keys map.
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
