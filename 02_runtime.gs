/*******************************************************
 * 02_runtime.gs
 * Управление выполнением, lock, run context, step execution
 *******************************************************/


/**
 * Главная managed-обертка для любого режима запуска.
 * Гарантирует:
 * - lock
 * - единый run context
 * - started / success / failed
 * - обработку ошибок
 * - release lock
 */
function runManagedExecution_(mode, entryPointName, executorFn) {
  const normalizedMode = assertExecutionMode_(mode);
  const ctx = buildRunContext_(normalizedMode, entryPointName);
  let lock = null;

  try {
    lock = acquireExecutionLock_(ctx);

    markRunStarted_(ctx);

    const result = executorFn(ctx);

    markRunSuccess_(ctx, result || {});

    return result || {
      runId: ctx.runId,
      mode: ctx.mode,
      status: 'SUCCESS'
    };
  } catch (error) {
    markRunFailed_(ctx, error);

    throw error;
  } finally {
    releaseExecutionLock_(lock, ctx);
  }
}


/**
 * Создает execution context.
 */
function buildRunContext_(mode, entryPointName) {
  const now = new Date();

  return {
    runId: buildRunId_(mode, entryPointName, now),
    mode: mode,
    entryPointName: String(entryPointName || '').trim() || 'unknown_entry',
    startedAt: now,
    startedAtStr: formatProjectDateTime_(now),
    finishedAt: null,
    finishedAtStr: '',
    status: 'RUNNING',

    triggerType: detectTriggerType_(),
    actorEmail: getActorEmailSafe_(),

    stepStats: [],
    counters: {
      queriesExecuted: 0,
      rowsWritten: 0,
      errorsCount: 0,
      warningsCount: 0
    },

    diagnostics: {
      lastStepName: '',
      lastStepStartedAt: '',
      lastStepFinishedAt: ''
    }
  };
}


/**
 * Ставит lock на выполнение.
 */
function acquireExecutionLock_(ctx) {
  const lock = LockService.getScriptLock();
  const success = lock.tryLock(CONFIG.execution.lockTimeoutMs);

  if (!success) {
    const error = new Error(
      'Не удалось получить execution lock за ' +
      CONFIG.execution.lockTimeoutMs + ' ms'
    );

    error.code = 'LOCK_TIMEOUT';
    error.runId = ctx.runId;

    throw error;
  }

  return lock;
}


/**
 * Освобождает lock.
 */
function releaseExecutionLock_(lock, ctx) {
  try {
    if (lock) {
      lock.releaseLock();
    }
  } catch (error) {
    logWarnRuntime_(
      'Ошибка при release lock. runId=' + safeGet_(ctx, 'runId', ''),
      error
    );
  }
}


/**
 * Фиксирует старт выполнения.
 */
function markRunStarted_(ctx) {
  ctx.status = 'RUNNING';

  appendRunLogSafe_({
    run_id: ctx.runId,
    mode: ctx.mode,
    started_at: ctx.startedAtStr,
    finished_at: '',
    status: 'RUNNING',
    datasets_count: '',
    objects_count: '',
    columns_count: '',
    dependency_rows_count: '',
    date_coverage_rows_count: '',
    quality_rows_count: '',
    heavy_objects_rows_count: '',
    key_objects_rows_count: '',
    error_message: ''
  });

  logInfoRuntime_(
    'RUN STARTED | runId=' + ctx.runId +
    ' | mode=' + ctx.mode +
    ' | entry=' + ctx.entryPointName
  );
}


/**
 * Фиксирует успешное завершение выполнения.
 */
function markRunSuccess_(ctx, result) {
  ctx.finishedAt = new Date();
  ctx.finishedAtStr = formatProjectDateTime_(ctx.finishedAt);
  ctx.status = 'SUCCESS';

  appendRunLogSafe_({
    run_id: ctx.runId,
    mode: ctx.mode,
    started_at: ctx.startedAtStr,
    finished_at: ctx.finishedAtStr,
    status: 'SUCCESS',
    datasets_count: safeGet_(result, 'datasetsCount', ''),
    objects_count: safeGet_(result, 'objectsCount', ''),
    columns_count: safeGet_(result, 'columnsCount', ''),
    dependency_rows_count: safeGet_(result, 'dependencyRowsCount', ''),
    date_coverage_rows_count: safeGet_(result, 'dateCoverageRowsCount', ''),
    quality_rows_count: safeGet_(result, 'qualityRowsCount', ''),
    heavy_objects_rows_count: safeGet_(result, 'heavyObjectsRowsCount', ''),
    key_objects_rows_count: safeGet_(result, 'keyObjectsRowsCount', ''),
    error_message: ''
  });

  logInfoRuntime_(
    'RUN SUCCESS | runId=' + ctx.runId +
    ' | mode=' + ctx.mode +
    ' | durationMs=' + getRunDurationMs_(ctx)
  );
}


/**
 * Фиксирует аварийное завершение выполнения.
 */
function markRunFailed_(ctx, error) {
  ctx.finishedAt = new Date();
  ctx.finishedAtStr = formatProjectDateTime_(ctx.finishedAt);
  ctx.status = 'FAILED';
  ctx.counters.errorsCount += 1;

  const message = extractErrorMessage_(error);
  const stack = extractErrorStack_(error);

  appendRunLogSafe_({
    run_id: ctx.runId,
    mode: ctx.mode,
    started_at: ctx.startedAtStr,
    finished_at: ctx.finishedAtStr,
    status: 'FAILED',
    datasets_count: '',
    objects_count: '',
    columns_count: '',
    dependency_rows_count: '',
    date_coverage_rows_count: '',
    quality_rows_count: '',
    heavy_objects_rows_count: '',
    key_objects_rows_count: '',
    error_message: message
  });

  appendErrorLogSafe_({
    run_id: ctx.runId,
    mode: ctx.mode,
    step_name: safeGet_(ctx.diagnostics, 'lastStepName', ''),
    error_message: message,
    stack: stack,
    logged_at: formatProjectDateTime_(new Date())
  });

  logErrorRuntime_(
    'RUN FAILED | runId=' + ctx.runId +
    ' | mode=' + ctx.mode +
    ' | step=' + safeGet_(ctx.diagnostics, 'lastStepName', ''),
    error
  );
}


/**
 * Выполняет отдельный этап с замером длительности и логами.
 * Возвращает результат executorFn().
 */
function safeExecuteStep_(ctx, stepName, executorFn) {
  const startedAt = new Date();
  const startedAtStr = formatProjectDateTime_(startedAt);

  ctx.diagnostics.lastStepName = stepName;
  ctx.diagnostics.lastStepStartedAt = startedAtStr;

  appendStepMetricSafe_({
    run_id: ctx.runId,
    mode: ctx.mode,
    step_name: stepName,
    status: 'RUNNING',
    started_at: startedAtStr,
    finished_at: '',
    duration_ms: '',
    details: ''
  });

  try {
    const result = executorFn();

    const finishedAt = new Date();
    const durationMs = finishedAt.getTime() - startedAt.getTime();
    const finishedAtStr = formatProjectDateTime_(finishedAt);

    ctx.diagnostics.lastStepFinishedAt = finishedAtStr;
    ctx.stepStats.push({
      stepName: stepName,
      status: 'SUCCESS',
      startedAt: startedAtStr,
      finishedAt: finishedAtStr,
      durationMs: durationMs
    });

    appendStepMetricSafe_({
      run_id: ctx.runId,
      mode: ctx.mode,
      step_name: stepName,
      status: 'SUCCESS',
      started_at: startedAtStr,
      finished_at: finishedAtStr,
      duration_ms: durationMs,
      details: ''
    });

    return result;
  } catch (error) {
    const finishedAt = new Date();
    const durationMs = finishedAt.getTime() - startedAt.getTime();
    const finishedAtStr = formatProjectDateTime_(finishedAt);
    const message = extractErrorMessage_(error);

    ctx.counters.errorsCount += 1;
    ctx.diagnostics.lastStepFinishedAt = finishedAtStr;
    ctx.stepStats.push({
      stepName: stepName,
      status: 'FAILED',
      startedAt: startedAtStr,
      finishedAt: finishedAtStr,
      durationMs: durationMs,
      errorMessage: message
    });

    appendStepMetricSafe_({
      run_id: ctx.runId,
      mode: ctx.mode,
      step_name: stepName,
      status: 'FAILED',
      started_at: startedAtStr,
      finished_at: finishedAtStr,
      duration_ms: durationMs,
      details: message
    });

    appendErrorLogSafe_({
      run_id: ctx.runId,
      mode: ctx.mode,
      step_name: stepName,
      error_message: message,
      stack: extractErrorStack_(error),
      logged_at: formatProjectDateTime_(new Date())
    });

    throw error;
  }
}


/**
 * Помечает выполнение query.
 * Вызывать из bq client.
 */
function registerExecutedQuery_(ctx, count) {
  if (!ctx || !ctx.counters) return;
  ctx.counters.queriesExecuted += Number(count || 1);
}


/**
 * Помечает запись строк.
 * Вызывать из writer layer.
 */
function registerWrittenRows_(ctx, rowsCount) {
  if (!ctx || !ctx.counters) return;
  ctx.counters.rowsWritten += Number(rowsCount || 0);
}


/**
 * Помечает warning.
 */
function registerWarning_(ctx) {
  if (!ctx || !ctx.counters) return;
  ctx.counters.warningsCount += 1;
}


/**
 * Формирует уникальный runId.
 */
function buildRunId_(mode, entryPointName, dateObj) {
  const timestamp = Utilities.formatDate(
    dateObj,
    CONFIG.core.timezone,
    'yyyyMMdd_HHmmss'
  );

  const suffix = Utilities.getUuid().slice(0, 8);

  return [
    'BQPASSPORT',
    String(mode || '').toUpperCase(),
    sanitizeIdPart_(entryPointName),
    timestamp,
    suffix
  ].join('_');
}


/**
 * Возвращает длительность run в миллисекундах.
 */
function getRunDurationMs_(ctx) {
  if (!ctx || !ctx.startedAt || !ctx.finishedAt) return '';
  return ctx.finishedAt.getTime() - ctx.startedAt.getTime();
}


/**
 * Возвращает тип запуска.
 */
function detectTriggerType_() {
  // В Apps Script надежно отличить все сценарии не всегда просто.
  // Пока фиксируем универсально.
  return 'SCRIPT_EXECUTION';
}


/**
 * Безопасно получает email актера, если доступно.
 */
function getActorEmailSafe_() {
  try {
    const email = Session.getActiveUser().getEmail();
    return String(email || '').trim();
  } catch (error) {
    return '';
  }
}


/**
 * Формат даты/времени в timezone проекта.
 */
function formatProjectDateTime_(dateObj) {
  return Utilities.formatDate(
    dateObj,
    CONFIG.core.timezone,
    'yyyy-MM-dd HH:mm:ss'
  );
}


/**
 * Санитизация части идентификатора.
 */
function sanitizeIdPart_(value) {
  return String(value || '')
    .trim()
    .replace(/\s+/g, '_')
    .replace(/[^a-zA-Z0-9_]/g, '')
    .slice(0, 40) || 'entry';
}


/**
 * Извлекает текст ошибки.
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
 * Извлекает stack ошибки.
 */
function extractErrorStack_(error) {
  if (!error) return '';

  if (error && error.stack) {
    return String(error.stack);
  }

  return '';
}


/**
 * Безопасное получение поля объекта.
 */
function safeGet_(obj, key, fallback) {
  if (!obj || typeof obj !== 'object') return fallback;
  return obj[key] !== undefined ? obj[key] : fallback;
}


/**
 * Fallback info log.
 * Позже его перехватит 03_logging.gs
 */
function logInfoRuntime_(message) {
  if (typeof logInfo_ === 'function') {
    logInfo_(message);
    return;
  }
  Logger.log('[INFO] ' + message);
}


/**
 * Fallback warn log.
 */
function logWarnRuntime_(message, error) {
  if (typeof logWarn_ === 'function') {
    logWarn_(message, error);
    return;
  }
  Logger.log('[WARN] ' + message + (error ? ' | ' + extractErrorMessage_(error) : ''));
}


/**
 * Fallback error log.
 */
function logErrorRuntime_(message, error) {
  if (typeof logError_ === 'function') {
    logError_(message, error);
    return;
  }
  Logger.log('[ERROR] ' + message + (error ? ' | ' + extractErrorMessage_(error) : ''));
}


/**
 * Безопасная запись run log.
 * Позже будет реальная реализация в logging layer.
 */
function appendRunLogSafe_(rowObj) {
  if (typeof appendRunLog_ === 'function') {
    appendRunLog_(rowObj);
    return;
  }
  Logger.log('[RUN_LOG] ' + JSON.stringify(rowObj));
}


/**
 * Безопасная запись error log.
 */
function appendErrorLogSafe_(rowObj) {
  if (typeof appendErrorLog_ === 'function') {
    appendErrorLog_(rowObj);
    return;
  }
  Logger.log('[ERROR_LOG] ' + JSON.stringify(rowObj));
}


/**
 * Безопасная запись step metric.
 */
function appendStepMetricSafe_(rowObj) {
  if (typeof appendStepMetric_ === 'function') {
    appendStepMetric_(rowObj);
    return;
  }
  Logger.log('[STEP_LOG] ' + JSON.stringify(rowObj));
}
