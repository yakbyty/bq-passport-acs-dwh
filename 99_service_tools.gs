/*******************************************************
 * 99_service_tools.gs
 * Служебные функции обслуживания проекта
 *******************************************************/


/**
 * Первичная инициализация проекта:
 * - создает ручные листы
 * - создает лог-листы
 * - создает справочник
 * - создает оглавление
 */
function serviceBootstrapProject() {
  ensureCoreSheetsExist_();
  ensureLoggingSheetsExist_();
  writeDictionarySheet_();

  const emptyBundle = {
    datasets: [],
    enrichedObjects: [],
    columns: [],
    dependencyRows: []
  };

  buildContentsSheet_(emptyBundle, {
    keyObjectsRows: [],
    heavyObjectsRows: []
  });

  SpreadsheetApp.getUi().alert('Bootstrap завершен');
}


/**
 * Проверка доступа к BigQuery.
 */
function serviceTestBigQueryAccess() {
  const sql = `
    SELECT
      CURRENT_TIMESTAMP() AS checked_at,
      '${CONFIG.core.projectId}' AS project_id,
      '${CONFIG.core.region}' AS region
  `;

  const result = runQuery_(sql, {
    useLegacySql: false,
    location: CONFIG.core.region
  });

  Logger.log('BigQuery test OK');
  Logger.log(JSON.stringify(result, null, 2));

  return result;
}


/**
 * Проверка, видит ли скрипт датасеты проекта.
 */
function serviceTestFetchDatasets() {
  const datasets = fetchDatasets_();

  Logger.log('Datasets count: ' + datasets.length);
  Logger.log(JSON.stringify(datasets.slice(0, 10), null, 2));

  return datasets;
}


/**
 * Проверка, видит ли скрипт объекты проекта.
 */
function serviceTestFetchObjects() {
  const objects = fetchTablesAndViews_();

  Logger.log('Objects count: ' + objects.length);

  const preview = objects.slice(0, 10).map(function (o) {
    return {
      object_key: buildObjectKey_(o.project_id, o.dataset_name, o.table_name),
      dataset_name: o.dataset_name,
      table_name: o.table_name,
      table_type: o.table_type,
      creation_time: o.creation_time
    };
  });

  Logger.log(JSON.stringify(preview, null, 2));

  return objects;
}

/**
 * Сброс cursor date coverage.
 */
function serviceResetCoverageCursor() {
  resetDateCoverageCursor();
  SpreadsheetApp.getUi().alert('Coverage cursor сброшен');
}


/**
 * Удалить и пересоздать триггеры.
 */
function serviceRecreateTriggers() {
  deleteProjectTriggers();
  createProjectTriggers();
  SpreadsheetApp.getUi().alert('Триггеры пересозданы');
}


/**
 * Диагностика конфига.
 */
function servicePrintConfig() {
  Logger.log(JSON.stringify(CONFIG, null, 2));
  SpreadsheetApp.getUi().alert('CONFIG выведен в Logs');
}


/**
 * Проверка обязательных функций.
 * Показывает, какие функции еще не найдены.
 */
function serviceCheckRequiredFunctions() {
  const requiredFunctions = [
    'refreshProjectPassportFast',
    'refreshProjectPassportFull',
    'runManagedExecution_',
    'safeExecuteStep_',
    'runQuery_',
    'fetchDatasets_',
    'fetchTablesAndViews_',
    'fetchColumns_',
    'fetchTableOptions_',
    'fetchViews_',
    'fetchStorageStats_',
    'buildMetadataMaps_',
    'enrichObjects_',
    'readObjectsMetadataMap_',
    'readManualDependencies_',
    'readQualityRules_',
    'buildLineageRows_',
    'buildDependencyRows_',
    'buildTreeRows_',
    'applyFreshnessToObjects_',
    'fetchDateCoverage_',
    'buildQualityChecksRows_',
    'buildHeavyObjectsRows_',
    'buildSummaryRows_',
    'writeMetadataSnapshotSheets_',
    'formatPassportSheets_',
    'buildContentsSheet_',
    'writeDictionarySheet_'
  ];

  const missing = [];

  requiredFunctions.forEach(function (fnName) {
    try {
      eval(fnName);
    } catch (e) {
      missing.push(fnName);
    }
  });

  if (missing.length) {
    Logger.log('Missing functions:\n' + missing.join('\n'));
    SpreadsheetApp.getUi().alert(
      'Не найдены функции: ' + missing.length + '. Проверь журнал выполнения.'
    );
    return;
  }

  SpreadsheetApp.getUi().alert('Все обязательные функции найдены');
}


/**
 * Тестовый быстрый запуск без триггера.
 */
function serviceRunFast() {
  refreshProjectPassportFast();
}


/**
 * Тестовый полный запуск без триггера.
 */
function serviceRunFull() {
  refreshProjectPassportFull();
}

function serviceTestFetchColumns() {
  const columns = fetchColumns_();

  Logger.log('Columns count: ' + columns.length);

  const preview = columns.slice(0, 10).map(function (c) {
    return {
      object_key: buildObjectKey_(c.project_id, c.dataset_name, c.table_name),
      column_name: c.column_name,
      data_type: c.data_type,
      is_nullable: c.is_nullable
    };
  });

  Logger.log(JSON.stringify(preview, null, 2));

  return columns;
}

function serviceTestFetchTableOptions() {
  const options = fetchTableOptions_();

  Logger.log('Table options count: ' + options.length);

  const preview = options.slice(0, 10).map(function (o) {
    return {
      object_key: buildObjectKey_(o.project_id, o.dataset_name, o.table_name),
      option_name: o.option_name,
      option_type: o.option_type,
      option_value: o.option_value
    };
  });

  Logger.log(JSON.stringify(preview, null, 2));

  return options;
}

function serviceTestFetchViews() {
  const views = fetchViews_();

  Logger.log('Views count: ' + views.length);

  const preview = views.slice(0, 10).map(function (v) {
    return {
      object_key: buildObjectKey_(v.project_id, v.dataset_name, v.table_name),
      use_standard_sql: v.use_standard_sql
    };
  });

  Logger.log(JSON.stringify(preview, null, 2));

  return views;
}

function serviceTestFetchStorageStats() {
  const storage = fetchStorageStats_();

  Logger.log('Storage rows count: ' + storage.length);

  const preview = storage.slice(0, 10).map(function (s) {
    return {
      object_key: buildObjectKey_(s.project_id, s.dataset_name, s.table_name),
      row_count: s.row_count,
      size_bytes: s.size_bytes,
      last_modified_time: s.last_modified_time,
      storage_source: s.storage_source
    };
  });

  Logger.log(JSON.stringify(preview, null, 2));

  return storage;
}
