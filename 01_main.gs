/*******************************************************
 * 01_main.gs
 * Точки входа и orchestration для паспорта BigQuery
 *******************************************************/


/**
 * Быстрый ежедневный запуск.
 * Обновляет только быстрый контур:
 * - datasets
 * - objects
 * - columns
 * - options
 * - views
 * - storage
 * - enrich
 * - lineage
 * - freshness light
 * - summary
 * - sheets
 */
function refreshProjectPassportFast() {
  return runProjectPassport_('FAST');
}


/**
 * Полный запуск.
 * Обновляет быстрый контур + тяжелые проверки:
 * - date coverage
 * - heavy quality checks
 * - schema drift
 * - storage audit
 * - histories
 */
function refreshProjectPassportFull() {
  return runProjectPassport_('FULL');
}


/**
 * Только снимок структуры проекта.
 * Без quality, coverage и тяжелых операций.
 */
function refreshMetadataSnapshot() {
  const mode = 'FAST';

  return runManagedExecution_(mode, 'refreshMetadataSnapshot', function (ctx) {
    const metadataBundle = collectMetadataSnapshot_(ctx);
    persistMetadataSnapshot_(ctx, metadataBundle);
    rebuildNavigationAndSummary_(ctx, metadataBundle);

    return {
      status: 'SUCCESS',
      metadataBundle: metadataBundle
    };
  });
}


/**
 * Только freshness + coverage.
 * Удобно для отдельного триггера или ручного обслуживания.
 */
function refreshFreshnessAndCoverage() {
  const mode = 'FULL';

  return runManagedExecution_(mode, 'refreshFreshnessAndCoverage', function (ctx) {
    const metadataBundle = collectMetadataSnapshot_(ctx);
    const enrichedObjects = metadataBundle.enrichedObjects;

    const freshnessRows = applyFreshnessLayer_(ctx, enrichedObjects);
    const coverageRows = applyCoverageLayer_(ctx, metadataBundle);

    writeFreshnessAndCoverageOutputs_(ctx, {
      enrichedObjects: enrichedObjects,
      freshnessRows: freshnessRows,
      coverageRows: coverageRows
    });

    rebuildNavigationAndSummary_(ctx, metadataBundle);

    return {
      status: 'SUCCESS',
      freshnessRowsCount: freshnessRows.length,
      coverageRowsCount: coverageRows.length
    };
  });
}


/**
 * Только quality-проверки.
 * Можно запускать отдельно, не пересобирая весь паспорт.
 */
function refreshQualityChecks() {
  const mode = 'FULL';

  return runManagedExecution_(mode, 'refreshQualityChecks', function (ctx) {
    const metadataBundle = collectMetadataSnapshot_(ctx);
    const qualityBundle = buildQualityBundle_(ctx, metadataBundle);

    writeQualityOutputs_(ctx, qualityBundle);

    return {
      status: 'SUCCESS',
      qualityRowsCount: (qualityBundle.qualityRows || []).length
    };
  });
}


/**
 * Только rebuild summary + contents + dictionary.
 * Полезно после ручных правок листов-справочников.
 */
function rebuildContentsAndSummary() {
  const mode = 'FAST';

  return runManagedExecution_(mode, 'rebuildContentsAndSummary', function (ctx) {
    const metadataBundle = collectMetadataSnapshot_(ctx);

    rebuildNavigationAndSummary_(ctx, metadataBundle);

    return {
      status: 'SUCCESS'
    };
  });
}


/**
 * Главная orchestration-функция.
 */
function runProjectPassport_(mode) {
  const normalizedMode = assertExecutionMode_(mode);

  return runManagedExecution_(normalizedMode, 'runProjectPassport_', function (ctx) {
    const metadataBundle = collectMetadataSnapshot_(ctx);

    persistMetadataSnapshot_(ctx, metadataBundle);

    if (normalizedMode === 'FAST') {
      runFastPostProcessing_(ctx, metadataBundle);
    }

    if (normalizedMode === 'FULL') {
      runFastPostProcessing_(ctx, metadataBundle);
      runFullPostProcessing_(ctx, metadataBundle);
    }

    rebuildNavigationAndSummary_(ctx, metadataBundle);

    return buildRunResult_(ctx, metadataBundle, normalizedMode);
  });
}


/**
 * Создает триггеры проекта согласно CONFIG.triggers
 */
function createProjectTriggers() {
  deleteProjectTriggers();

  if (CONFIG.triggers.enableDailyFastTrigger) {
    ScriptApp.newTrigger('refreshProjectPassportFast')
      .timeBased()
      .everyDays(1)
      .atHour(CONFIG.triggers.dailyFastHour)
      .inTimezone(CONFIG.core.timezone)
      .create();
  }

  if (CONFIG.triggers.enableDailyFullTrigger) {
    ScriptApp.newTrigger('refreshProjectPassportFull')
      .timeBased()
      .everyDays(1)
      .atHour(CONFIG.triggers.dailyFullHour)
      .inTimezone(CONFIG.core.timezone)
      .create();
  }
}


/**
 * Удаляет все триггеры этого проекта,
 * относящиеся к обновлению паспорта.
 */
function deleteProjectTriggers() {
  const handlersToDelete = {
    refreshProjectPassportFast: true,
    refreshProjectPassportFull: true,
    refreshMetadataSnapshot: true,
    refreshFreshnessAndCoverage: true,
    refreshQualityChecks: true,
    rebuildContentsAndSummary: true
  };

  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(function (trigger) {
    const handler = trigger.getHandlerFunction();
    if (handlersToDelete[handler]) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}


/**
 * Быстрый контур постобработки.
 */
function runFastPostProcessing_(ctx, metadataBundle) {
  safeExecuteStep_(ctx, 'runFastPostProcessing_', function () {
    const enrichedObjects = metadataBundle.enrichedObjects;

    const freshnessRows = applyFreshnessLayer_(ctx, enrichedObjects);
    const keyObjectsRows = buildKeyObjectsLayer_(ctx, enrichedObjects);
    const heavyObjectsRows = buildHeavyObjectsLayer_(ctx, enrichedObjects);
    const summaryRows = buildSummaryLayer_(ctx, metadataBundle, {
      freshnessRows: freshnessRows,
      keyObjectsRows: keyObjectsRows,
      heavyObjectsRows: heavyObjectsRows,
      qualityRows: []
    });

    writeFastOutputs_(ctx, {
      metadataBundle: metadataBundle,
      freshnessRows: freshnessRows,
      keyObjectsRows: keyObjectsRows,
      heavyObjectsRows: heavyObjectsRows,
      summaryRows: summaryRows
    });
  });
}


/**
 * Полный контур постобработки.
 */
function runFullPostProcessing_(ctx, metadataBundle) {
  safeExecuteStep_(ctx, 'runFullPostProcessing_', function () {
    const coverageRows = applyCoverageLayer_(ctx, metadataBundle);
    const qualityBundle = buildQualityBundle_(ctx, metadataBundle);
    const schemaDriftBundle = buildSchemaDriftBundle_(ctx, metadataBundle);
    const historiesBundle = buildHistoriesBundle_(ctx, metadataBundle, {
      coverageRows: coverageRows,
      qualityBundle: qualityBundle,
      schemaDriftBundle: schemaDriftBundle
    });

    writeFullOutputs_(ctx, {
      metadataBundle: metadataBundle,
      coverageRows: coverageRows,
      qualityBundle: qualityBundle,
      schemaDriftBundle: schemaDriftBundle,
      historiesBundle: historiesBundle
    });
  });
}


/**
 * Собирает снимок метаданных проекта.
 * Здесь только получение и enrichment.
 */
function collectMetadataSnapshot_(ctx) {
  return safeExecuteStep_(ctx, 'collectMetadataSnapshot_', function () {
    ensureCoreSheetsExist_();

    const datasets = fetchDatasets_();
    const objects = fetchTablesAndViews_();
    const columns = fetchColumns_();
    const tableOptions = fetchTableOptions_();
    const views = fetchViews_();
    const storageStats = fetchStorageStats_();

    const materializedViews = CONFIG.features.enableMaterializedViewsScan
      ? fetchMaterializedViewsSafe_()
      : [];

    const routines = CONFIG.features.enableRoutinesScan
      ? fetchRoutinesSafe_()
      : [];

    const manualMetadata = readObjectsMetadataMap_();
    const manualDependencies = readManualDependencies_();
    const qualityRules = readQualityRules_();
    const objectSlaMap = readObjectSlaMapSafe_();
    const objectOwnersMap = readOwnersMapSafe_();

    const metadataMaps = buildMetadataMaps_({
      datasets: datasets,
      objects: objects,
      columns: columns,
      tableOptions: tableOptions,
      views: views,
      storageStats: storageStats,
      materializedViews: materializedViews,
      routines: routines,
      manualMetadata: manualMetadata,
      manualDependencies: manualDependencies,
      qualityRules: qualityRules,
      objectSlaMap: objectSlaMap,
      objectOwnersMap: objectOwnersMap
    });

    const enrichedObjects = enrichObjects_(objects, metadataMaps);
    const lineageRows = buildLineageRows_(views);
    const dependencyRows = buildDependencyRows_(enrichedObjects, lineageRows, manualDependencies);
    const treeRows = buildTreeRows_(enrichedObjects, dependencyRows);

    return {
      datasets: datasets,
      objects: objects,
      columns: columns,
      tableOptions: tableOptions,
      views: views,
      storageStats: storageStats,
      materializedViews: materializedViews,
      routines: routines,

      manualMetadata: manualMetadata,
      manualDependencies: manualDependencies,
      qualityRules: qualityRules,
      objectSlaMap: objectSlaMap,
      objectOwnersMap: objectOwnersMap,

      metadataMaps: metadataMaps,
      enrichedObjects: enrichedObjects,
      lineageRows: lineageRows,
      dependencyRows: dependencyRows,
      treeRows: treeRows
    };
  });
}


/**
 * Сохраняет базовый снимок метаданных в листы.
 * Только структура, без тяжелых проверок.
 */
function persistMetadataSnapshot_(ctx, metadataBundle) {
  return safeExecuteStep_(ctx, 'persistMetadataSnapshot_', function () {
    writeMetadataSnapshotSheets_(ctx, metadataBundle);
  });
}


/**
 * Пересобирает summary / contents / dictionary / formatting.
 */
function rebuildNavigationAndSummary_(ctx, metadataBundle) {
  return safeExecuteStep_(ctx, 'rebuildNavigationAndSummary_', function () {
    const enrichedObjects = metadataBundle.enrichedObjects;
    const keyObjectsRows = buildKeyObjectsLayer_(ctx, enrichedObjects);
    const heavyObjectsRows = buildHeavyObjectsLayer_(ctx, enrichedObjects);
    const summaryRows = buildSummaryLayer_(ctx, metadataBundle, {
      freshnessRows: [],
      keyObjectsRows: keyObjectsRows,
      heavyObjectsRows: heavyObjectsRows,
      qualityRows: []
    });

    writeSummarySheet_(summaryRows);
    writeDictionarySheet_();
    buildContentsSheet_(metadataBundle, {
      summaryRows: summaryRows,
      keyObjectsRows: keyObjectsRows,
      heavyObjectsRows: heavyObjectsRows
    });
    formatPassportSheets_();
  });
}


/**
 * Строит bundle quality-данных.
 */
function buildQualityBundle_(ctx, metadataBundle) {
  return safeExecuteStep_(ctx, 'buildQualityBundle_', function () {
    const enrichedObjects = metadataBundle.enrichedObjects;
    const qualityRules = metadataBundle.qualityRules || [];
    const metadataMaps = metadataBundle.metadataMaps || {};

    const qualityRows = buildQualityChecksRows_(
      enrichedObjects,
      metadataMaps.columnsMap || {},
      [],
      qualityRules
    );

    applyQualityFlagsToObjects_(enrichedObjects, qualityRows);

    return {
      qualityRows: qualityRows,
      enrichedObjects: enrichedObjects
    };
  });
}


function buildSchemaDriftBundle_(ctx, metadataBundle) {
  return safeExecuteStep_(ctx, 'buildSchemaDriftBundle_', function () {
    if (!CONFIG.features.enableSchemaDrift) {
      return {
        schemaHistoryRows: [],
        schemaChangesRows: []
      };
    }

    return {
      schemaHistoryRows: [],
      schemaChangesRows: []
    };
  });
}


/**
 * Строит bundle history-слоев.
 * Пока безопасный каркас.
 */
function buildHistoriesBundle_(ctx, metadataBundle, inputs) {
  return safeExecuteStep_(ctx, 'buildHistoriesBundle_', function () {
    return {
      freshnessHistoryRows: [],
      qualityHistoryRows: [],
      schemaHistoryRows: (inputs.schemaDriftBundle || {}).schemaHistoryRows || [],
      schemaChangesRows: (inputs.schemaDriftBundle || {}).schemaChangesRows || []
    };
  });
}


/**
 * Применяет freshness-слой.
 * Возвращает строки freshness, если позже понадобится
 * отдельный лист/история.
 */
function applyFreshnessLayer_(ctx, enrichedObjects) {
  return safeExecuteStep_(ctx, 'applyFreshnessLayer_', function () {
    applyFreshnessToObjects_(enrichedObjects);
    return buildFreshnessRowsSafe_(enrichedObjects);
  });
}


/**
 * Применяет coverage-слой.
 */
function applyCoverageLayer_(ctx, metadataBundle) {
  return safeExecuteStep_(ctx, 'applyCoverageLayer_', function () {
    const enrichedObjects = metadataBundle.enrichedObjects;
    const metadataMaps = metadataBundle.metadataMaps || {};

    const coverageRows = fetchDateCoverage_(
      enrichedObjects,
      metadataMaps.columnsMap || {}
    );

    applyDateCoverageToObjects_(enrichedObjects, coverageRows);

    return coverageRows;
  });
}


/**
 * Строит слой ключевых объектов.
 */
function buildKeyObjectsLayer_(ctx, enrichedObjects) {
  return safeExecuteStep_(ctx, 'buildKeyObjectsLayer_', function () {
    return buildKeyObjectsRows_(enrichedObjects);
  });
}


/**
 * Строит слой тяжелых объектов.
 */
function buildHeavyObjectsLayer_(ctx, enrichedObjects) {
  return safeExecuteStep_(ctx, 'buildHeavyObjectsLayer_', function () {
    return buildHeavyObjectsRows_(enrichedObjects);
  });
}


/**
 * Строит summary-слой.
 */
function buildSummaryLayer_(ctx, metadataBundle, extras) {
  return safeExecuteStep_(ctx, 'buildSummaryLayer_', function () {
    return buildSummaryRows_(
      metadataBundle.datasets || [],
      metadataBundle.enrichedObjects || [],
      metadataBundle.columns || [],
      metadataBundle.dependencyRows || [],
      extras.coverageRows || [],
      extras.qualityRows || [],
      extras.keyObjectsRows || []
    );
  });
}


/**
 * Возвращает итог run result.
 */
function buildRunResult_(ctx, metadataBundle, mode) {
  return {
    runId: ctx.runId,
    mode: mode,
    status: 'SUCCESS',
    datasetsCount: (metadataBundle.datasets || []).length,
    objectsCount: (metadataBundle.enrichedObjects || []).length,
    columnsCount: (metadataBundle.columns || []).length,
    dependencyRowsCount: (metadataBundle.dependencyRows || []).length
  };
}


/**
 * Безопасный fetch materialized views.
 * Пока не ломает запуск, даже если функция еще не реализована.
 */
function fetchMaterializedViewsSafe_() {
  if (typeof fetchMaterializedViews_ === 'function') {
    return fetchMaterializedViews_();
  }
  return [];
}


/**
 * Безопасный fetch routines.
 */
function fetchRoutinesSafe_() {
  if (typeof fetchRoutines_ === 'function') {
    return fetchRoutines_();
  }
  return [];
}


/**
 * Безопасное чтение SLA.
 */
function readObjectSlaMapSafe_() {
  if (typeof readObjectSlaMap_ === 'function') {
    return readObjectSlaMap_();
  }
  return {};
}


/**
 * Безопасное чтение owners.
 */
function readOwnersMapSafe_() {
  if (typeof readOwnersMap_ === 'function') {
    return readOwnersMap_();
  }
  return {};
}


/**
 * Безопасная сборка freshness rows.
 */
function buildFreshnessRowsSafe_(enrichedObjects) {
  if (typeof buildFreshnessRows_ === 'function') {
    return buildFreshnessRows_(enrichedObjects);
  }
  return [];
}
