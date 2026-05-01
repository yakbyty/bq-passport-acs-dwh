/*******************************************************
 * 18_sheet_writer.gs
 * Запись данных паспорта в Google Sheets
 *******************************************************/


/**
 * Записывает базовый metadata snapshot.
 */
function writeMetadataSnapshotSheets_(ctx, metadataBundle) {
  const bundle = metadataBundle || {};

  writeSheet_(
    CONFIG.sheets.datasets,
    getDatasetsHeaders_(),
    bundle.datasets || [],
    ctx
  );

  writeSheet_(
    CONFIG.sheets.objects,
    getObjectsHeaders_(),
    bundle.enrichedObjects || [],
    ctx
  );

  writeSheet_(
    CONFIG.sheets.columns,
    getColumnsHeaders_(),
    bundle.columns || [],
    ctx
  );

  writeSheet_(
    CONFIG.sheets.viewsLineage,
    getViewsLineageHeaders_(),
    bundle.lineageRows || [],
    ctx
  );

  writeSheet_(
    CONFIG.sheets.objectDependencies,
    getObjectDependenciesHeaders_(),
    bundle.dependencyRows || [],
    ctx
  );

  writeSheet_(
    CONFIG.sheets.tree,
    getTreeHeaders_(),
    bundle.treeRows || [],
    ctx
  );
}


/**
 * Записывает быстрые outputs.
 */
function writeFastOutputs_(ctx, payload) {
  const data = payload || {};

  if (data.metadataBundle && data.metadataBundle.enrichedObjects) {
    writeSheet_(
      CONFIG.sheets.objects,
      getObjectsHeaders_(),
      data.metadataBundle.enrichedObjects,
      ctx
    );
  }

  writeSheet_(
    CONFIG.sheets.keyObjects,
    getKeyObjectsHeaders_(),
    data.keyObjectsRows || [],
    ctx
  );

  writeSheet_(
    CONFIG.sheets.heavyObjects,
    getHeavyObjectsHeaders_(),
    data.heavyObjectsRows || [],
    ctx
  );

  writeSheet_(
    CONFIG.sheets.summary,
    getSummaryHeaders_(),
    data.summaryRows || [],
    ctx
  );

  if (CONFIG.features.enableFreshnessHistory && data.freshnessRows && data.freshnessRows.length) {
    appendSheetRows_(
      CONFIG.sheets.freshnessHistory,
      getFreshnessHistoryHeaders_(),
      buildFreshnessHistoryRowsForRun_(ctx, data.freshnessRows),
      ctx
    );
  }
}


/**
 * Записывает full outputs.
 */
function writeFullOutputs_(ctx, payload) {
  const data = payload || {};
  const qualityBundle = data.qualityBundle || {};
  const schemaDriftBundle = data.schemaDriftBundle || {};
  const historiesBundle = data.historiesBundle || {};

  writeSheet_(
    CONFIG.sheets.dateCoverage,
    getDateCoverageHeaders_(),
    data.coverageRows || [],
    ctx
  );

  writeSheet_(
    CONFIG.sheets.qualityChecks,
    getQualityChecksHeaders_(),
    qualityBundle.qualityRows || [],
    ctx
  );

  if (qualityBundle.enrichedObjects) {
    writeSheet_(
      CONFIG.sheets.objects,
      getObjectsHeaders_(),
      qualityBundle.enrichedObjects,
      ctx
    );
  }

  if (CONFIG.features.enableQualityHistory && qualityBundle.qualityRows) {
    appendSheetRows_(
      CONFIG.sheets.qualityHistory,
      getQualityHistoryHeaders_(),
      buildQualityHistoryRows_(qualityBundle.qualityRows, ctx.runId),
      ctx
    );
  }

  if (CONFIG.features.enableSchemaDrift) {
    appendSheetRows_(
      CONFIG.sheets.schemaHistory,
      getSchemaHistoryHeaders_(),
      schemaDriftBundle.schemaHistoryRows || [],
      ctx
    );

    appendSheetRows_(
      CONFIG.sheets.schemaChanges,
      getSchemaChangesHeaders_(),
      schemaDriftBundle.schemaChangesRows || [],
      ctx
    );
  }

  if (historiesBundle.freshnessHistoryRows && historiesBundle.freshnessHistoryRows.length) {
    appendSheetRows_(
      CONFIG.sheets.freshnessHistory,
      getFreshnessHistoryHeaders_(),
      historiesBundle.freshnessHistoryRows,
      ctx
    );
  }
}


/**
 * Записывает freshness + coverage outputs.
 */
function writeFreshnessAndCoverageOutputs_(ctx, payload) {
  const data = payload || {};

  writeSheet_(
    CONFIG.sheets.dateCoverage,
    getDateCoverageHeaders_(),
    data.coverageRows || [],
    ctx
  );

  writeSheet_(
    CONFIG.sheets.objects,
    getObjectsHeaders_(),
    data.enrichedObjects || [],
    ctx
  );

  if (CONFIG.features.enableFreshnessHistory && data.freshnessRows) {
    appendSheetRows_(
      CONFIG.sheets.freshnessHistory,
      getFreshnessHistoryHeaders_(),
      buildFreshnessHistoryRowsForRun_(ctx, data.freshnessRows),
      ctx
    );
  }
}


/**
 * Записывает quality outputs.
 */
function writeQualityOutputs_(ctx, qualityBundle) {
  const bundle = qualityBundle || {};

  writeSheet_(
    CONFIG.sheets.qualityChecks,
    getQualityChecksHeaders_(),
    bundle.qualityRows || [],
    ctx
  );

  if (bundle.enrichedObjects) {
    writeSheet_(
      CONFIG.sheets.objects,
      getObjectsHeaders_(),
      bundle.enrichedObjects,
      ctx
    );
  }

  if (CONFIG.features.enableQualityHistory && bundle.qualityRows) {
    appendSheetRows_(
      CONFIG.sheets.qualityHistory,
      getQualityHistoryHeaders_(),
      buildQualityHistoryRows_(bundle.qualityRows, ctx.runId),
      ctx
    );
  }
}


/**
 * Записывает summary.
 */
function writeSummarySheet_(summaryRows) {
  writeSheet_(
    CONFIG.sheets.summary,
    getSummaryHeaders_(),
    summaryRows || null,
    null
  );
}


/**
 * Универсальная перезапись листа.
 */
function writeSheet_(sheetName, headers, rows, ctx) {
  const sheet = getOrCreateSheet_(sheetName);
  const safeHeaders = Array.isArray(headers) ? headers : [];
  const safeRows = Array.isArray(rows) ? rows : [];

  if (!safeHeaders.length) {
    throw new Error('Пустые headers для листа: ' + sheetName);
  }

  ensureSheetHeaders_(sheet, safeHeaders);
  clearSheetDataBelowHeader_(sheet);

  const values = buildSheetValues_(safeHeaders, safeRows);

  if (values.length) {
    sheet.getRange(2, 1, values.length, safeHeaders.length).setValues(values);
  }

  applyBasicSheetFormatting_(sheet, safeHeaders.length, values.length + 1);

  if (ctx) {
    registerWrittenRows_(ctx, values.length);
  }
}


/**
 * Append rows в конец листа.
 */
function appendSheetRows_(sheetName, headers, rows, ctx) {
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) return;

  const sheet = getOrCreateSheet_(sheetName);
  const safeHeaders = Array.isArray(headers) ? headers : [];

  ensureSheetHeaders_(sheet, safeHeaders);

  const values = buildSheetValues_(safeHeaders, safeRows);
  const startRow = sheet.getLastRow() + 1;

  sheet.getRange(startRow, 1, values.length, safeHeaders.length).setValues(values);

  if (ctx) {
    registerWrittenRows_(ctx, values.length);
  }
}


/**
 * Строит values для записи по headers.
 */
function buildSheetValues_(headers, rows) {
  const safeRows = Array.isArray(rows) ? rows : [];

  return safeRows.map(function (row) {
    return headers.map(function (header) {
      return normalizeSheetCellValue_(row && row[header] !== undefined ? row[header] : '');
    });
  });
}


/**
 * Очищает данные ниже заголовка.
 */
function clearSheetDataBelowHeader_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow <= 1 || lastColumn < 1) {
    return;
  }

  sheet.getRange(2, 1, lastRow - 1, lastColumn).clearContent();
}


/**
 * Базовое форматирование листа.
 */
function applyBasicSheetFormatting_(sheet, cols, rows) {
  sheet.setFrozenRows(1);

  if (sheet.getFilter()) {
    sheet.getFilter().remove();
  }

  if (rows >= 1 && cols >= 1) {
    sheet.getRange(1, 1, rows, cols).createFilter();
  }

  const resizeCols = Math.min(cols, CONFIG.limits.maxSheetsAutoResizeColumns);
  if (resizeCols > 0) {
    sheet.autoResizeColumns(1, resizeCols);
  }
}


/**
 * Нормализация значения для Google Sheets.
 */
function normalizeSheetCellValue_(value) {
  if (value === null || value === undefined) return '';

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

  return value;
}


/**
 * Freshness history rows с run_id.
 */
function buildFreshnessHistoryRowsForRun_(ctx, freshnessRows) {
  const rows = Array.isArray(freshnessRows) ? freshnessRows : [];

  return rows.map(function (row) {
    return {
      run_id: ctx ? ctx.runId : '',
      checked_at: normalizePlainString_(row.checked_at) || getNowProjectTz_(),
      object_key: normalizePlainString_(row.object_key),
      dataset_name: normalizePlainString_(row.dataset_name),
      table_name: normalizePlainString_(row.table_name),
      table_type: normalizePlainString_(row.table_type),
      refresh_mechanism: normalizePlainString_(row.refresh_mechanism),
      expected_refresh_frequency: normalizePlainString_(row.expected_refresh_frequency),
      freshness_threshold_hours: normalizeNumberSafe_(row.freshness_threshold_hours),
      actual_last_update: normalizePlainString_(row.actual_last_update),
      freshness_lag_hours: normalizeNumberSafe_(row.freshness_lag_hours),
      freshness_status: normalizePlainString_(row.freshness_status),
      freshness_comment: normalizePlainString_(row.freshness_comment),
      period_max: normalizePlainString_(row.period_max)
    };
  });
}


/*******************************************************
 * HEADERS
 *******************************************************/

function getDatasetsHeaders_() {
  return [
    'project_id',
    'dataset_name',
    'location',
    'creation_time',
    'last_modified_time',
    'default_collation_name',
    'dataset_ddl'
  ];
}


function getObjectsHeaders_() {
  return [
    'object_key',
    'project_id',
    'dataset_name',
    'table_name',
    'table_type',
    'managed_table_type',
    'description',
    'creation_time',
    'last_modified_time',
    'row_count',
    'size_bytes',
    'size_mb',
    'size_gb',
    'partitioning_type',
    'partitioning_field',
    'clustering_fields',
    'columns_count',
    'columns_list',
    'layer',
    'source',
    'purpose',
    'update_type',
    'refresh_mechanism',
    'expected_refresh_frequency',
    'freshness_threshold_hours',
    'actual_last_update',
    'freshness_status',
    'freshness_comment',
    'freshness_lag_hours',
    'freshness_reference_type',
    'freshness_reference_value',
    'business_criticality',
    'management_priority',
    'grain',
    'primary_key_candidate',
    'join_keys',
    'business_definition',
    'allowed_use',
    'not_recommended_use',
    'incident_if_broken',
    'quality_status',
    'quality_issues_count',
    'scan_risk',
    'risk_flag',
    'date_field_candidate',
    'date_field_type',
    'period_min',
    'period_max',
    'period_row_count_checked',
    'owner',
    'sla_tier',
    'has_view_definition',
    'has_storage_stats',
    'has_options',
    'has_description',
    'has_partitioning',
    'has_clustering',
    'ddl'
  ];
}


function getColumnsHeaders_() {
  return [
    'project_id',
    'dataset_name',
    'table_name',
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
    'rounding_mode'
  ];
}


function getViewsLineageHeaders_() {
  return [
    'object_key',
    'object_type',
    'dataset_name',
    'table_name',
    'depends_on',
    'dependency_type',
    'dependency_source',
    'lineage_confidence',
    'refresh_mechanism',
    'dependency_comment'
  ];
}


function getObjectDependenciesHeaders_() {
  return [
    'object_key',
    'object_type',
    'dataset_name',
    'table_name',
    'depends_on',
    'dependency_type',
    'dependency_source',
    'lineage_confidence',
    'refresh_mechanism',
    'dependency_comment'
  ];
}


function getTreeHeaders_() {
  return [
    'object_key',
    'layer_group',
    'dataset_name',
    'table_name',
    'table_type',
    'source',
    'purpose',
    'update_type',
    'refresh_mechanism',
    'freshness_status',
    'quality_status',
    'business_criticality',
    'management_priority',
    'risk_flag',
    'depends_on_count',
    'depends_on_list',
    'dependency_sources',
    'lineage_confidence'
  ];
}


function getDateCoverageHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'date_field',
    'date_field_type',
    'min_date',
    'max_date',
    'checked_rows',
    'status',
    'comment',
    'checked_at'
  ];
}


function getQualityChecksHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'check_group',
    'check_name',
    'check_result',
    'severity',
    'status',
    'details',
    'checked_at'
  ];
}


function getHeavyObjectsHeaders_() {
  return [
    'object_key',
    'dataset_name',
    'table_name',
    'table_type',
    'row_count',
    'size_gb',
    'partitioning_type',
    'partitioning_field',
    'clustering_fields',
    'scan_risk',
    'risk_flag',
    'optimization_comment'
  ];
}


function getFreshnessHistoryHeaders_() {
  return [
    'run_id',
    'checked_at',
    'object_key',
    'dataset_name',
    'table_name',
    'table_type',
    'refresh_mechanism',
    'expected_refresh_frequency',
    'freshness_threshold_hours',
    'actual_last_update',
    'freshness_lag_hours',
    'freshness_status',
    'freshness_comment',
    'period_max'
  ];
}


function getQualityHistoryHeaders_() {
  return [
    'run_id',
    'checked_at',
    'object_key',
    'dataset_name',
    'table_name',
    'check_group',
    'check_name',
    'check_result',
    'severity',
    'status',
    'details'
  ];
}
