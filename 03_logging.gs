/*******************************************************
 * 03_logging.gs
 * Логирование запусков, ошибок и этапов выполнения
 *******************************************************/


/**
 * INFO log
 */
function logInfo_(message) {
  Logger.log('[INFO] ' + String(message || ''));
}


/**
 * WARN log
 */
function logWarn_(message, error) {
  Logger.log(
    '[WARN] ' +
    String(message || '') +
    (error ? ' | ' + extractErrorMessage_(error) : '')
  );
}


/**
 * ERROR log
 */
function logError_(message, error) {
  Logger.log(
    '[ERROR] ' +
    String(message || '') +
    (error ? ' | ' + extractErrorMessage_(error) : '') +
    (error && error.stack ? '\n' + error.stack : '')
  );
}


/**
 * Добавляет запись в журнал запусков.
 */
function appendRunLog_(rowObj) {
  const sheet = ensureLogSheet_(
    CONFIG.sheets.runLog,
    CONFIG.headers.runLog
  );

  appendObjectRowToSheet_(sheet, CONFIG.headers.runLog, rowObj);
}


/**
 * Добавляет запись в журнал ошибок.
 */
function appendErrorLog_(rowObj) {
  const sheet = ensureLogSheet_(
    CONFIG.sheets.errorLog,
    CONFIG.headers.errorLog
  );

  appendObjectRowToSheet_(sheet, CONFIG.headers.errorLog, rowObj);
}


/**
 * Добавляет запись в журнал этапов.
 */
function appendStepMetric_(rowObj) {
  const sheet = ensureLogSheet_(
    CONFIG.sheets.executionSteps,
    CONFIG.headers.executionSteps
  );

  appendObjectRowToSheet_(sheet, CONFIG.headers.executionSteps, rowObj);
}


/**
 * Создает обязательные лог-листы, если их нет.
 * Можно вызывать из main/bootstrap.
 */
function ensureLoggingSheetsExist_() {
  ensureLogSheet_(CONFIG.sheets.runLog, CONFIG.headers.runLog);
  ensureLogSheet_(CONFIG.sheets.errorLog, CONFIG.headers.errorLog);
  ensureLogSheet_(CONFIG.sheets.executionSteps, CONFIG.headers.executionSteps);
}


/**
 * Возвращает Spreadsheet по CONFIG.core.spreadsheetId
 */
function getSpreadsheet_() {
  return SpreadsheetApp.openById(CONFIG.core.spreadsheetId);
}


/**
 * Возвращает лист по имени или создает его.
 */
function getOrCreateSheet_(sheetName) {
  const ss = getSpreadsheet_();
  let sheet = ss.getSheetByName(sheetName);

  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
  }

  return sheet;
}


/**
 * Проверяет наличие лог-листа и правильных заголовков.
 */
function ensureLogSheet_(sheetName, headers) {
  const sheet = getOrCreateSheet_(sheetName);
  ensureSheetHeaders_(sheet, headers);
  return sheet;
}


/**
 * Устанавливает заголовки, если лист пустой
 * или если заголовки не совпадают.
 */
function ensureSheetHeaders_(sheet, headers) {
  const expectedHeaders = headers.slice();
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  let actualHeaders = [];
  if (lastRow >= 1 && lastColumn > 0) {
    actualHeaders = sheet
      .getRange(1, 1, 1, Math.max(lastColumn, expectedHeaders.length))
      .getValues()[0]
      .map(function (v) { return String(v || ''); });
  }

  const headersMatch = arraysEqualStrict_(
    actualHeaders.slice(0, expectedHeaders.length),
    expectedHeaders
  );

  if (!headersMatch) {
    sheet.clearContents();
    sheet.clearFormats();

    sheet.getRange(1, 1, 1, expectedHeaders.length)
      .setValues([expectedHeaders]);

    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, expectedHeaders.length).setFontWeight('bold');

    if (expectedHeaders.length <= CONFIG.limits.maxSheetsAutoResizeColumns) {
      sheet.autoResizeColumns(1, expectedHeaders.length);
    }
  } else if (sheet.getFrozenRows() !== 1) {
    sheet.setFrozenRows(1);
  }
}


/**
 * Добавляет одну строку-объект в конец листа в порядке headers.
 */
function appendObjectRowToSheet_(sheet, headers, rowObj) {
  const row = headers.map(function (header) {
    const value = rowObj && rowObj[header] !== undefined
      ? rowObj[header]
      : '';
    return normalizeLogCellValue_(value);
  });

  const targetRow = sheet.getLastRow() + 1;
  sheet.getRange(targetRow, 1, 1, headers.length).setValues([row]);
}


/**
 * Добавляет массив объектных строк в конец листа.
 * Полезно для future batch logging.
 */
function appendObjectRowsToSheet_(sheet, headers, rowObjects) {
  if (!rowObjects || !rowObjects.length) return;

  const values = rowObjects.map(function (rowObj) {
    return headers.map(function (header) {
      const value = rowObj && rowObj[header] !== undefined
        ? rowObj[header]
        : '';
      return normalizeLogCellValue_(value);
    });
  });

  const startRow = sheet.getLastRow() + 1;
  sheet.getRange(startRow, 1, values.length, headers.length).setValues(values);
}


/**
 * Очищает только данные логов, оставляя структуру листа.
 * Использовать осторожно.
 */
function clearLogData_(sheetName) {
  const sheet = getOrCreateSheet_(sheetName);
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow <= 1 || lastColumn === 0) return;

  sheet.getRange(2, 1, lastRow - 1, lastColumn).clearContent();
}


/**
 * Возвращает последние N строк из лог-листа.
 * Удобно для диагностики.
 */
function readLastLogRows_(sheetName, limit) {
  const sheet = getOrCreateSheet_(sheetName);
  const lastRow = sheet.getLastRow();
  const lastColumn = sheet.getLastColumn();

  if (lastRow < 2 || lastColumn === 0) return [];

  const safeLimit = Math.max(1, Number(limit || 10));
  const startRow = Math.max(2, lastRow - safeLimit + 1);
  const numRows = lastRow - startRow + 1;

  return sheet.getRange(startRow, 1, numRows, lastColumn).getValues();
}


/**
 * Инициализация всех системных лог-листов.
 * Можно запускать вручную один раз.
 */
function bootstrapLogging_() {
  ensureLoggingSheetsExist_();
}


/**
 * Нормализация значения ячейки лога.
 */
function normalizeLogCellValue_(value) {
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
