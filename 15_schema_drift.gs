/*******************************************************
 * 15_schema_drift.gs
 * Schema drift:
 * - snapshot текущей схемы
 * - сравнение с предыдущим snapshot
 * - detection added/removed/changed columns
 *******************************************************/


/**
 * Главная функция schema drift.
 *
 * metadataBundle ожидает:
 * - columns
 * - enrichedObjects
 *
 * previousSchemaRows можно передать позже из writer/storage layer.
 * Сейчас функция безопасна: если предыдущей истории нет,
 * вернет только snapshot без изменений.
 */
function buildSchemaDriftRows_(metadataBundle, previousSchemaRows) {
  if (!CONFIG.features.enableSchemaDrift) {
    return {
      schemaHistoryRows: [],
      schemaChangesRows: []
    };
  }

  const currentSnapshotRows = buildCurrentSchemaSnapshotRows_(metadataBundle);
  const previousRows = Array.isArray(previousSchemaRows) ? previousSchemaRows : [];

  const schemaChangesRows = compareSchemaSnapshots_(
    previousRows,
    currentSnapshotRows
  );

  return {
    schemaHistoryRows: currentSnapshotRows,
    schemaChangesRows: schemaChangesRows
  };
}


/**
 * Совместимая функция для 01_main.gs.
 * Пока previous history не читаем, поэтому первый этап —
 * только текущий snapshot.
 */
function buildSchemaDriftBundle_(ctx, metadataBundle) {
  return safeExecuteStep_(ctx, 'buildSchemaDriftBundle_', function () {
    const result = buildSchemaDriftRows_(metadataBundle, []);

    return {
      schemaHistoryRows: result.schemaHistoryRows,
      schemaChangesRows: result.schemaChangesRows
    };
  });
}


/**
 * Строит snapshot текущей схемы.
 */
function buildCurrentSchemaSnapshotRows_(metadataBundle) {
  const bundle = metadataBundle || {};
  const columns = Array.isArray(bundle.columns) ? bundle.columns : [];
  const objectsMap = buildEnrichedObjectsMap_(bundle.enrichedObjects || []);
  const snapshotAt = getNowProjectTz_();

  return columns.map(function (col) {
    const objectKey = buildObjectKey_(
      col.project_id || CONFIG.core.projectId,
      col.dataset_name,
      col.table_name
    );

    const obj = objectsMap[objectKey] || {};

    return {
      snapshot_at: snapshotAt,
      object_key: objectKey,
      project_id: normalizePlainString_(col.project_id) || CONFIG.core.projectId,
      dataset_name: normalizePlainString_(col.dataset_name),
      table_name: normalizePlainString_(col.table_name),
      table_type: normalizePlainString_(obj.table_type),
      ordinal_position: normalizeNumberSafe_(col.ordinal_position),
      column_name: normalizePlainString_(col.column_name),
      data_type: normalizePlainString_(col.data_type),
      is_nullable: normalizePlainString_(col.is_nullable),
      is_hidden: normalizePlainString_(col.is_hidden),
      is_system_defined: normalizePlainString_(col.is_system_defined),
      is_partitioning_column: normalizePlainString_(col.is_partitioning_column),
      clustering_ordinal_position: normalizeNumberSafe_(col.clustering_ordinal_position),
      collation_name: normalizePlainString_(col.collation_name),
      column_default: normalizeUnknownToString_(col.column_default),
      rounding_mode: normalizePlainString_(col.rounding_mode),
      column_signature: buildColumnSignature_(col)
    };
  });
}


/**
 * Сравнивает предыдущий snapshot с текущим.
 *
 * Возвращает строки изменений:
 * - COLUMN_ADDED
 * - COLUMN_REMOVED
 * - COLUMN_TYPE_CHANGED
 * - COLUMN_NULLABILITY_CHANGED
 * - COLUMN_POSITION_CHANGED
 * - PARTITIONING_FLAG_CHANGED
 * - CLUSTERING_POSITION_CHANGED
 * - COLUMN_DEFAULT_CHANGED
 */
function compareSchemaSnapshots_(previousRows, currentRows) {
  const prev = Array.isArray(previousRows) ? previousRows : [];
  const curr = Array.isArray(currentRows) ? currentRows : [];

  if (!prev.length || !curr.length) {
    return [];
  }

  const prevLatest = selectLatestSchemaSnapshot_(prev);
  const currLatest = selectLatestSchemaSnapshot_(curr);

  const prevMap = buildSchemaColumnMap_(prevLatest);
  const currMap = buildSchemaColumnMap_(currLatest);

  const changes = [];

  Array.prototype.push.apply(changes, detectAddedColumns_(prevMap, currMap));
  Array.prototype.push.apply(changes, detectRemovedColumns_(prevMap, currMap));
  Array.prototype.push.apply(changes, detectChangedColumns_(prevMap, currMap));

  return changes.sort(function (a, b) {
    return [
      a.object_key,
      a.column_name,
      a.change_type
    ].join('|').localeCompare([
      b.object_key,
      b.column_name,
      b.change_type
    ].join('|'));
  });
}


/**
 * Выбирает последний snapshot из массива history rows.
 */
function selectLatestSchemaSnapshot_(rows) {
  const items = Array.isArray(rows) ? rows : [];

  if (!items.length) {
    return [];
  }

  let latestSnapshotAt = '';

  items.forEach(function (row) {
    const snapshotAt = normalizePlainString_(row.snapshot_at);
    if (snapshotAt > latestSnapshotAt) {
      latestSnapshotAt = snapshotAt;
    }
  });

  return items.filter(function (row) {
    return normalizePlainString_(row.snapshot_at) === latestSnapshotAt;
  });
}


/**
 * Map колонок:
 * object_key.column_name -> row
 */
function buildSchemaColumnMap_(schemaRows) {
  const rows = Array.isArray(schemaRows) ? schemaRows : [];
  const map = {};

  rows.forEach(function (row) {
    const key = buildSchemaColumnKey_(row.object_key, row.column_name);
    if (!key) {
      return;
    }

    map[key] = row;
  });

  return map;
}


/**
 * Определяет добавленные колонки.
 */
function detectAddedColumns_(prevMap, currMap) {
  const changes = [];

  Object.keys(currMap || {}).forEach(function (key) {
    if (prevMap[key]) {
      return;
    }

    const row = currMap[key];

    changes.push(buildSchemaChangeRow_(row, {
      change_type: 'COLUMN_ADDED',
      old_value: '',
      new_value: buildColumnSignature_(row),
      severity: inferSchemaChangeSeverity_('COLUMN_ADDED', row),
      comment: 'Колонка появилась в текущей схеме'
    }));
  });

  return changes;
}


/**
 * Определяет удаленные колонки.
 */
function detectRemovedColumns_(prevMap, currMap) {
  const changes = [];

  Object.keys(prevMap || {}).forEach(function (key) {
    if (currMap[key]) {
      return;
    }

    const row = prevMap[key];

    changes.push(buildSchemaChangeRow_(row, {
      change_type: 'COLUMN_REMOVED',
      old_value: buildColumnSignature_(row),
      new_value: '',
      severity: inferSchemaChangeSeverity_('COLUMN_REMOVED', row),
      comment: 'Колонка отсутствует в текущей схеме'
    }));
  });

  return changes;
}


/**
 * Определяет изменения существующих колонок.
 */
function detectChangedColumns_(prevMap, currMap) {
  const changes = [];

  Object.keys(currMap || {}).forEach(function (key) {
    const prev = prevMap[key];
    const curr = currMap[key];

    if (!prev || !curr) {
      return;
    }

    Array.prototype.push.apply(changes, compareColumnField_(
      prev,
      curr,
      'data_type',
      'COLUMN_TYPE_CHANGED',
      'Изменился тип данных колонки'
    ));

    Array.prototype.push.apply(changes, compareColumnField_(
      prev,
      curr,
      'is_nullable',
      'COLUMN_NULLABILITY_CHANGED',
      'Изменилась nullable-настройка колонки'
    ));

    Array.prototype.push.apply(changes, compareColumnField_(
      prev,
      curr,
      'ordinal_position',
      'COLUMN_POSITION_CHANGED',
      'Изменилась позиция колонки'
    ));

    Array.prototype.push.apply(changes, compareColumnField_(
      prev,
      curr,
      'is_partitioning_column',
      'PARTITIONING_FLAG_CHANGED',
      'Изменился признак partitioning column'
    ));

    Array.prototype.push.apply(changes, compareColumnField_(
      prev,
      curr,
      'clustering_ordinal_position',
      'CLUSTERING_POSITION_CHANGED',
      'Изменилась позиция в clustering'
    ));

    Array.prototype.push.apply(changes, compareColumnField_(
      prev,
      curr,
      'column_default',
      'COLUMN_DEFAULT_CHANGED',
      'Изменилось значение default'
    ));
  });

  return changes;
}


/**
 * Сравнение одного поля колонки.
 */
function compareColumnField_(prev, curr, fieldName, changeType, comment) {
  const oldValue = normalizeUnknownToString_(prev[fieldName]);
  const newValue = normalizeUnknownToString_(curr[fieldName]);

  if (oldValue === newValue) {
    return [];
  }

  return [
    buildSchemaChangeRow_(curr, {
      change_type: changeType,
      field_name: fieldName,
      old_value: oldValue,
      new_value: newValue,
      severity: inferSchemaChangeSeverity_(changeType, curr),
      comment: comment
    })
  ];
}


/**
 * Строит строку изменения схемы.
 */
function buildSchemaChangeRow_(schemaRow, change) {
  const payload = change || {};

  return {
    detected_at: getNowProjectTz_(),
    object_key: normalizePlainString_(schemaRow.object_key),
    dataset_name: normalizePlainString_(schemaRow.dataset_name),
    table_name: normalizePlainString_(schemaRow.table_name),
    table_type: normalizePlainString_(schemaRow.table_type),
    column_name: normalizePlainString_(schemaRow.column_name),
    change_type: normalizePlainString_(payload.change_type),
    field_name: normalizePlainString_(payload.field_name),
    old_value: normalizeUnknownToString_(payload.old_value),
    new_value: normalizeUnknownToString_(payload.new_value),
    severity: normalizePlainString_(payload.severity),
    comment: normalizePlainString_(payload.comment)
  };
}


/**
 * Severity для изменений схемы.
 */
function inferSchemaChangeSeverity_(changeType, row) {
  const type = normalizePlainString_(changeType);

  if (type === 'COLUMN_REMOVED') {
    return 'critical';
  }

  if (type === 'COLUMN_TYPE_CHANGED') {
    return 'critical';
  }

  if (type === 'COLUMN_NULLABILITY_CHANGED') {
    return 'warning';
  }

  if (type === 'PARTITIONING_FLAG_CHANGED') {
    return 'warning';
  }

  if (type === 'CLUSTERING_POSITION_CHANGED') {
    return 'warning';
  }

  if (type === 'COLUMN_ADDED') {
    return 'info';
  }

  return 'info';
}


/**
 * Строит сигнатуру колонки.
 */
function buildColumnSignature_(col) {
  return [
    'name=' + normalizePlainString_(col.column_name),
    'type=' + normalizePlainString_(col.data_type),
    'nullable=' + normalizePlainString_(col.is_nullable),
    'position=' + normalizeUnknownToString_(col.ordinal_position),
    'partition=' + normalizePlainString_(col.is_partitioning_column),
    'cluster_pos=' + normalizeUnknownToString_(col.clustering_ordinal_position),
    'default=' + normalizeUnknownToString_(col.column_default)
  ].join('; ');
}


/**
 * Ключ колонки в схеме.
 */
function buildSchemaColumnKey_(objectKey, columnName) {
  const objKey = normalizePlainString_(objectKey);
  const colName = normalizePlainString_(columnName).toLowerCase();

  if (!objKey || !colName) {
    return '';
  }

  return objKey + '.' + colName;
}


/**
 * Заголовки для листа История схем.
 */
function getSchemaHistoryHeaders_() {
  return [
    'snapshot_at',
    'object_key',
    'project_id',
    'dataset_name',
    'table_name',
    'table_type',
    'ordinal_position',
    'column_name',
    'data_type',
    'is_nullable',
    'is_hidden',
    'is_system_defined',
    'is_partitioning_column',
    'clustering_ordinal_position',
    'collation_name',
    'column_default',
    'rounding_mode',
    'column_signature'
  ];
}


/**
 * Заголовки для листа Изменения схем.
 */
function getSchemaChangesHeaders_() {
  return [
    'detected_at',
    'object_key',
    'dataset_name',
    'table_name',
    'table_type',
    'column_name',
    'change_type',
    'field_name',
    'old_value',
    'new_value',
    'severity',
    'comment'
  ];
}
