/*******************************************************
 * 12_coverage.gs
 * Date coverage layer:
 * - min/max date по таблицам
 * - batch rotation
 * - применение coverage к enriched objects
 *******************************************************/


/**
 * Получает date coverage по batch-части таблиц.
 *
 * enrichedObjects: enriched objects
 * columnsMap: map project.dataset.table -> columns[]
 */
function fetchDateCoverage_(enrichedObjects, columnsMap) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const safeColumnsMap = asObjectSafe_(columnsMap);

  const candidates = buildDateCoverageCandidates_(objects, safeColumnsMap);

  if (!candidates.length) {
    return [];
  }

  const selectedBatch = selectDateCoverageBatch_(candidates);

  const rows = [];
  selectedBatch.forEach(function (candidate) {
    const resultRow = fetchSingleDateCoverageSafe_(candidate);
    rows.push(resultRow);
  });

  const skippedRows = buildSkippedCoverageRows_(candidates, selectedBatch);
  Array.prototype.push.apply(rows, skippedRows);

  saveCoverageCursor_(candidates, selectedBatch);

  return rows;
}


/**
 * Применяет coverage к объектам in-place.
 */
function applyDateCoverageToObjects_(enrichedObjects, dateCoverageRows) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const coverageRows = Array.isArray(dateCoverageRows) ? dateCoverageRows : [];

  const map = {};
  coverageRows.forEach(function (row) {
    const key = normalizePlainString_(row.object_key) ||
      buildObjectKey_(CONFIG.core.projectId, row.dataset_name, row.table_name);

    if (row.status === CONFIG.statuses.generic.skippedByLimit) {
      return;
    }

    map[key] = row;
  });

  objects.forEach(function (obj) {
    const key = normalizePlainString_(obj.object_key);
    const item = map[key];

    if (!item) {
      return;
    }

    obj.period_min = normalizePlainString_(item.min_date);
    obj.period_max = normalizePlainString_(item.max_date);
    obj.period_row_count_checked = normalizeNumberSafe_(item.checked_rows);
  });

  return objects;
}


/**
 * Формирует кандидатов на date coverage.
 */
function buildDateCoverageCandidates_(enrichedObjects, columnsMap) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const map = asObjectSafe_(columnsMap);
  const result = [];

  objects.forEach(function (obj) {
    const tableType = normalizePlainString_(obj.table_type).toUpperCase();

    if (tableType !== CONFIG.objectTypes.baseTable) {
      return;
    }

    const objectKey = normalizePlainString_(obj.object_key);
    const cols = asArraySafe_(map[objectKey]);
    const dateCandidate = detectDateFieldCandidate_(cols);

    if (!dateCandidate) {
      return;
    }

    result.push({
      object_key: objectKey,
      project_id: normalizePlainString_(obj.project_id) || CONFIG.core.projectId,
      dataset_name: normalizePlainString_(obj.dataset_name),
      table_name: normalizePlainString_(obj.table_name),
      date_field: normalizePlainString_(dateCandidate.column_name),
      date_field_type: normalizePlainString_(dateCandidate.data_type),
      table_type: tableType
    });
  });

  return result.sort(function (a, b) {
    return a.object_key.localeCompare(b.object_key);
  });
}


/**
 * Выбирает batch для текущего запуска.
 * Использует cursor в ScriptProperties.
 */
function selectDateCoverageBatch_(candidates) {
  const items = Array.isArray(candidates) ? candidates : [];
  const limit = Number(CONFIG.limits.maxDateCoverageTablesPerRun || 150);

  if (items.length <= limit) {
    return items.slice();
  }

  const cursor = getCoverageCursor_();
  const startIndex = cursor >= 0 && cursor < items.length ? cursor : 0;

  const batch = [];

  for (var i = 0; i < limit; i++) {
    const idx = (startIndex + i) % items.length;
    batch.push(items[idx]);
  }

  return batch;
}


/**
 * Проверяет один объект и не роняет весь запуск.
 */
function fetchSingleDateCoverageSafe_(candidate) {
  try {
    const sql = buildDateCoverageQuery_(
      candidate.project_id,
      candidate.dataset_name,
      candidate.table_name,
      candidate.date_field,
      candidate.date_field_type
    );

    const result = runQuery_(sql, {
      useLegacySql: false,
      location: CONFIG.core.region,
      skipIfTooExpensive: false
    });

    if (!result.length) {
      return buildDateCoverageRow_(candidate, {
        min_date: '',
        max_date: '',
        checked_rows: '',
        status: CONFIG.statuses.generic.emptyResult,
        comment: ''
      });
    }

    return buildDateCoverageRow_(candidate, {
      min_date: normalizePlainString_(result[0].min_date),
      max_date: normalizePlainString_(result[0].max_date),
      checked_rows: normalizeNumberSafe_(result[0].checked_rows),
      status: CONFIG.statuses.generic.ok,
      comment: ''
    });
  } catch (error) {
    return buildDateCoverageRow_(candidate, {
      min_date: '',
      max_date: '',
      checked_rows: '',
      status: CONFIG.statuses.generic.error,
      comment: extractErrorMessage_(error)
    });
  }
}


/**
 * SQL min/max по date field.
 */
function buildDateCoverageQuery_(projectId, datasetName, tableName, columnName, dataType) {
  const fq = '`' + projectId + '.' + datasetName + '.' + tableName + '`';
  const col = '`' + columnName + '`';
  const type = normalizePlainString_(dataType).toUpperCase();

  let expr = col;

  if (type === 'TIMESTAMP' || type === 'DATETIME') {
    expr = 'DATE(' + col + ')';
  }

  return `
    SELECT
      CAST(MIN(${expr}) AS STRING) AS min_date,
      CAST(MAX(${expr}) AS STRING) AS max_date,
      COUNT(1) AS checked_rows
    FROM ${fq}
    WHERE ${col} IS NOT NULL
  `;
}


/**
 * Строит строку coverage.
 */
function buildDateCoverageRow_(candidate, values) {
  const val = values || {};

  return {
    object_key: normalizePlainString_(candidate.object_key),
    dataset_name: normalizePlainString_(candidate.dataset_name),
    table_name: normalizePlainString_(candidate.table_name),
    date_field: normalizePlainString_(candidate.date_field),
    date_field_type: normalizePlainString_(candidate.date_field_type),
    min_date: normalizePlainString_(val.min_date),
    max_date: normalizePlainString_(val.max_date),
    checked_rows: normalizeNumberSafe_(val.checked_rows),
    status: normalizePlainString_(val.status),
    comment: normalizePlainString_(val.comment),
    checked_at: getNowProjectTz_()
  };
}


/**
 * Для таблиц вне batch делает строки SKIPPED_BY_LIMIT.
 * Это нужно, чтобы было видно: таблица не забыта, а не проверялась в этом проходе.
 */
function buildSkippedCoverageRows_(allCandidates, selectedBatch) {
  const all = Array.isArray(allCandidates) ? allCandidates : [];
  const selected = Array.isArray(selectedBatch) ? selectedBatch : [];

  const selectedMap = {};
  selected.forEach(function (candidate) {
    selectedMap[candidate.object_key] = true;
  });

  return all
    .filter(function (candidate) {
      return !selectedMap[candidate.object_key];
    })
    .map(function (candidate) {
      return buildDateCoverageRow_(candidate, {
        min_date: '',
        max_date: '',
        checked_rows: '',
        status: CONFIG.statuses.generic.skippedByLimit,
        comment: 'Не входило в текущий batch; будет проверено в одном из следующих запусков'
      });
    });
}


/**
 * Сохраняет cursor для следующего запуска.
 */
function saveCoverageCursor_(allCandidates, selectedBatch) {
  const all = Array.isArray(allCandidates) ? allCandidates : [];
  const selected = Array.isArray(selectedBatch) ? selectedBatch : [];

  if (!all.length || !selected.length) {
    return;
  }

  const lastSelectedKey = selected[selected.length - 1].object_key;
  const lastIndex = all.findIndex(function (candidate) {
    return candidate.object_key === lastSelectedKey;
  });

  const nextIndex = lastIndex === -1
    ? 0
    : (lastIndex + 1) % all.length;

  PropertiesService
    .getScriptProperties()
    .setProperty(getCoverageCursorPropertyKey_(), String(nextIndex));
}


/**
 * Читает cursor.
 */
function getCoverageCursor_() {
  const value = PropertiesService
    .getScriptProperties()
    .getProperty(getCoverageCursorPropertyKey_());

  const n = Number(value);
  return isNaN(n) ? 0 : n;
}


/**
 * Property key для cursor.
 */
function getCoverageCursorPropertyKey_() {
  return [
    'BQPASSPORT',
    CONFIG.core.projectId,
    CONFIG.core.region,
    'DATE_COVERAGE_CURSOR'
  ].join('_');
}


/**
 * Сброс cursor.
 * Можно запускать вручную, если нужно начать coverage с начала.
 */
function resetDateCoverageCursor() {
  PropertiesService
    .getScriptProperties()
    .deleteProperty(getCoverageCursorPropertyKey_());
}
