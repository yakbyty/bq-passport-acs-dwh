/*******************************************************
 * 19_sheet_formatting.gs
 * Форматирование листов паспорта
 *******************************************************/


/**
 * Главное форматирование всех паспортных листов.
 */
function formatPassportSheets_() {
  formatSheetIfExists_(CONFIG.sheets.objects, function (sheet) {
    applyStatusColumnColoring_(sheet, 'freshness_status', getFreshnessColorMap_());
    applyStatusColumnColoring_(sheet, 'quality_status', getQualityColorMap_());
    applyStatusColumnColoring_(sheet, 'risk_flag', getRiskFlagColorMap_());
    applyStatusColumnColoring_(sheet, 'scan_risk', getScanRiskColorMap_());
    freezeAndResize_(sheet);
  });

  formatSheetIfExists_(CONFIG.sheets.keyObjects, function (sheet) {
    applyStatusColumnColoring_(sheet, 'freshness_status', getFreshnessColorMap_());
    applyStatusColumnColoring_(sheet, 'quality_status', getQualityColorMap_());
    applyStatusColumnColoring_(sheet, 'risk_flag', getRiskFlagColorMap_());
    applyStatusColumnColoring_(sheet, 'scan_risk', getScanRiskColorMap_());
    freezeAndResize_(sheet);
  });

  formatSheetIfExists_(CONFIG.sheets.qualityChecks, function (sheet) {
    applyStatusColumnColoring_(sheet, 'status', getQualityColorMap_());
    applySeverityColoring_(sheet, 'severity');
    freezeAndResize_(sheet);
  });

  formatSheetIfExists_(CONFIG.sheets.dateCoverage, function (sheet) {
    applyStatusColumnColoring_(sheet, 'status', getGenericStatusColorMap_());
    freezeAndResize_(sheet);
  });

  formatSheetIfExists_(CONFIG.sheets.heavyObjects, function (sheet) {
    applyStatusColumnColoring_(sheet, 'scan_risk', getScanRiskColorMap_());
    applyStatusColumnColoring_(sheet, 'risk_flag', getRiskFlagColorMap_());
    freezeAndResize_(sheet);
  });

  formatSheetIfExists_(CONFIG.sheets.tree, function (sheet) {
    applyStatusColumnColoring_(sheet, 'freshness_status', getFreshnessColorMap_());
    applyStatusColumnColoring_(sheet, 'quality_status', getQualityColorMap_());
    applyStatusColumnColoring_(sheet, 'risk_flag', getRiskFlagColorMap_());
    freezeAndResize_(sheet);
  });

  formatSheetIfExists_(CONFIG.sheets.schemaChanges, function (sheet) {
    applySeverityColoring_(sheet, 'severity');
    freezeAndResize_(sheet);
  });

  formatSheetIfExists_(CONFIG.sheets.errorLog, function (sheet) {
    freezeAndResize_(sheet);
  });

  formatSheetIfExists_(CONFIG.sheets.executionSteps, function (sheet) {
    applyStatusColumnColoring_(sheet, 'status', getExecutionStatusColorMap_());
    freezeAndResize_(sheet);
  });

  formatSheetIfExists_(CONFIG.sheets.runLog, function (sheet) {
    applyStatusColumnColoring_(sheet, 'status', getExecutionStatusColorMap_());
    freezeAndResize_(sheet);
  });
}


/**
 * Форматирует лист, если он существует.
 */
function formatSheetIfExists_(sheetName, formatterFn) {
  const ss = getSpreadsheet_();
  const sheet = ss.getSheetByName(sheetName);

  if (!sheet) {
    return;
  }

  formatterFn(sheet);
}


/**
 * Подсветка колонки по map значений.
 */
function applyStatusColumnColoring_(sheet, headerName, colorMap) {
  const rangeInfo = getColumnRangeByHeader_(sheet, headerName);
  if (!rangeInfo) {
    return;
  }

  const values = rangeInfo.range.getValues();

  values.forEach(function (row, idx) {
    const value = String(row[0] || '').trim();
    const color = colorMap[value];

    if (color) {
      rangeInfo.range.getCell(idx + 1, 1).setBackground(color);
    }
  });
}


/**
 * Подсветка severity.
 */
function applySeverityColoring_(sheet, headerName) {
  const map = {
    critical: '#f4cccc',
    warning: '#fff2cc',
    info: '#d9ead3',
    CRITICAL: '#f4cccc',
    WARNING: '#fff2cc',
    INFO: '#d9ead3'
  };

  applyStatusColumnColoring_(sheet, headerName, map);
}


/**
 * Freeze + resize.
 */
function freezeAndResize_(sheet) {
  if (sheet.getFrozenRows() !== 1) {
    sheet.setFrozenRows(1);
  }

  const lastColumn = sheet.getLastColumn();
  if (lastColumn > 0) {
    const resizeCols = Math.min(lastColumn, CONFIG.limits.maxSheetsAutoResizeColumns);
    sheet.autoResizeColumns(1, resizeCols);
  }
}


/**
 * Получает range колонки по заголовку.
 */
function getColumnRangeByHeader_(sheet, headerName) {
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow < 2 || lastColumn < 1) {
    return null;
  }

  const headers = sheet
    .getRange(1, 1, 1, lastColumn)
    .getValues()[0]
    .map(function (h) {
      return String(h || '').trim();
    });

  const colIdx = headers.indexOf(headerName);
  if (colIdx === -1) {
    return null;
  }

  return {
    columnIndex: colIdx + 1,
    range: sheet.getRange(2, colIdx + 1, lastRow - 1, 1)
  };
}


/**
 * Цвета freshness.
 */
function getFreshnessColorMap_() {
  return {
    FRESH: '#d9ead3',
    LATE: '#fff2cc',
    STALE: '#f4cccc',
    UNKNOWN: '#d9d2e9'
  };
}


/**
 * Цвета quality.
 */
function getQualityColorMap_() {
  return {
    PASS: '#d9ead3',
    WARN: '#fff2cc',
    FAIL: '#f4cccc',
    ERROR: '#ea9999'
  };
}


/**
 * Цвета risk_flag.
 */
function getRiskFlagColorMap_() {
  return {
    OK: '#d9ead3',
    NO_DESCRIPTION: '#fff2cc',
    OLD_DATA: '#f4cccc',
    EMPTY: '#fce5cd',
    TOO_BIG: '#ead1dc',
    NO_COLUMNS_METADATA: '#d0e0e3'
  };
}


/**
 * Цвета scan_risk.
 */
function getScanRiskColorMap_() {
  return {
    LOW: '#d9ead3',
    MEDIUM: '#fff2cc',
    HIGH: '#f4cccc',
    VERY_HIGH: '#ea9999'
  };
}


/**
 * Цвета generic statuses.
 */
function getGenericStatusColorMap_() {
  return {
    OK: '#d9ead3',
    PASS: '#d9ead3',
    WARN: '#fff2cc',
    FAIL: '#f4cccc',
    ERROR: '#ea9999',
    EMPTY_RESULT: '#fff2cc',
    SKIPPED: '#d9d2e9',
    SKIPPED_BY_LIMIT: '#d9d2e9',
    UNKNOWN: '#d9d2e9'
  };
}


/**
 * Цвета run / step statuses.
 */
function getExecutionStatusColorMap_() {
  return {
    RUNNING: '#cfe2f3',
    SUCCESS: '#d9ead3',
    FAILED: '#f4cccc',
    ERROR: '#ea9999',
    SKIPPED: '#d9d2e9'
  };
}
