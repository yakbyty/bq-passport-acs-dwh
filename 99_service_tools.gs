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

  SpreadsheetApp.getUi().alert('Найдено датасетов: ' + datasets.length);
}


/**
 * Проверка, видит ли скрипт объекты проекта.
 */
function serviceTestFetchObjects() {
  const objects = fetchTablesAndViews_();

  Logger.log('Objects count: ' + objects.length);
  Logger.log(JSON.stringify(objects.slice(0, 10), null, 2));

  SpreadsheetApp.getUi().alert('Найдено объектов: ' + objects.length);
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
