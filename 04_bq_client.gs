/*******************************************************
 * 04_bq_client.gs
 * Единый BigQuery client layer для Apps Script
 *******************************************************/


/**
 * Универсальный запуск query.
 * Возвращает массив объектов-строк.
 *
 * options:
 * - ctx
 * - useLegacySql (default false)
 * - timeoutMs
 * - maxRetries
 * - dryRun
 * - skipIfTooExpensive
 * - maxBytesBilledEstimate
 * - location
 */
function runQuery_(sql, options) {
  const opts = normalizeQueryOptions_(options);
  const trimmedSql = String(sql || '').trim();

  if (!trimmedSql) {
    throw new Error('Пустой SQL в runQuery_()');
  }

  if (opts.dryRun) {
    return runDryQuery_(trimmedSql, opts);
  }

  if (opts.skipIfTooExpensive && CONFIG.features.enableDryRunForHeavyQueries) {
    const dryRunResult = runDryQuery_(trimmedSql, {
      ctx: opts.ctx,
      useLegacySql: opts.useLegacySql,
      timeoutMs: opts.timeoutMs,
      maxRetries: opts.maxRetries,
      location: opts.location
    });

    const estimatedBytes = Number(dryRunResult.totalBytesProcessed || 0);
    const maxAllowedBytes = Number(opts.maxBytesBilledEstimate || 0);

    if (maxAllowedBytes > 0 && estimatedBytes > maxAllowedBytes) {
      const error = new Error(
        'Query skipped as too expensive. estimatedBytes=' +
        estimatedBytes +
        ', limit=' +
        maxAllowedBytes
      );
      error.code = 'QUERY_SKIPPED_TOO_EXPENSIVE';
      error.estimatedBytes = estimatedBytes;
      throw error;
    }
  }

  return executeQueryWithRetries_(trimmedSql, opts);
}


/**
 * Dry-run query.
 * Возвращает объект с оценкой:
 * - totalBytesProcessed
 * - jobId
 * - location
 * - statementType (если доступно)
 */
function runDryQuery_(sql, options) {
  const opts = normalizeQueryOptions_(options);
  const request = {
    query: sql,
    useLegacySql: !!opts.useLegacySql,
    dryRun: true,
    location: opts.location
  };

  return executeBigQueryRequestWithRetries_(function () {
    const response = BigQuery.Jobs.query(request, CONFIG.core.projectId);

    registerExecutedQuerySafe_(opts.ctx, 1);

    return {
      dryRun: true,
      jobId: safeGetPath_(response, ['jobReference', 'jobId'], ''),
      location: safeGetPath_(response, ['jobReference', 'location'], opts.location),
      totalBytesProcessed: Number(response.totalBytesProcessed || 0),
      totalBytesBilled: Number(response.totalBytesBilled || 0),
      cacheHit: !!response.cacheHit,
      statementType: response.statementType || ''
    };
  }, opts);
}


/**
 * Оценивает bytes processed через dry-run.
 * Возвращает число.
 */
function estimateBytes_(sql, options) {
  const result = runDryQuery_(sql, options);
  return Number(result.totalBytesProcessed || 0);
}


/**
 * Нужно ли пропускать запрос как слишком дорогой.
 */
function shouldSkipExpensiveQuery_(sql, maxBytesBilledEstimate, options) {
  const estimatedBytes = estimateBytes_(sql, options);
  return Number(maxBytesBilledEstimate || 0) > 0 &&
    estimatedBytes > Number(maxBytesBilledEstimate || 0);
}


/**
 * Выполняет query с retries.
 */
function executeQueryWithRetries_(sql, opts) {
  return executeBigQueryRequestWithRetries_(function () {
    const queryRequest = {
      query: sql,
      useLegacySql: !!opts.useLegacySql,
      location: opts.location
    };

    const queryResponse = BigQuery.Jobs.query(
      queryRequest,
      CONFIG.core.projectId
    );

    registerExecutedQuerySafe_(opts.ctx, 1);

    const jobId = safeGetPath_(queryResponse, ['jobReference', 'jobId'], '');
    const jobLocation = safeGetPath_(queryResponse, ['jobReference', 'location'], opts.location);

    if (!jobId) {
      throw new Error('BigQuery не вернул jobId');
    }

    const completedResult = waitForJob_(
      CONFIG.core.projectId,
      jobId,
      {
        timeoutMs: opts.timeoutMs,
        pollIntervalMs: CONFIG.execution.queryPollIntervalMs,
        location: jobLocation
      }
    );

    const rows = parseQueryResults_(completedResult);

    let pageToken = completedResult.pageToken || '';
    while (pageToken) {
      const nextPage = BigQuery.Jobs.getQueryResults(
        CONFIG.core.projectId,
        jobId,
        {
          pageToken: pageToken,
          location: jobLocation
        }
      );

      const nextRows = parseQueryResults_(nextPage);
      Array.prototype.push.apply(rows, nextRows);

      pageToken = nextPage.pageToken || '';
    }

    return rows;
  }, opts);
}


/**
 * Универсальная retry-обертка для BigQuery вызовов.
 */
function executeBigQueryRequestWithRetries_(executorFn, opts) {
  var lastError = null;
  var maxRetries = Number(opts.maxRetries || CONFIG.execution.maxRetries);

  for (var attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return executorFn();
    } catch (error) {
      lastError = error;

      var shouldRetry = isRetryableBigQueryError_(error);
      var isLastAttempt = attempt === maxRetries;

      logWarn_(
        'BigQuery request failed. attempt=' + attempt +
        ', maxRetries=' + maxRetries +
        ', retryable=' + shouldRetry,
        error
      );

      if (!shouldRetry || isLastAttempt) {
        throw error;
      }

      var sleepMs = buildRetrySleepMs_(attempt);
      Utilities.sleep(sleepMs);
    }
  }

  throw lastError || new Error('Unknown BigQuery execution error');
}


/**
 * Ожидает завершения job.
 * Возвращает финальный getQueryResults response.
 */
function waitForJob_(projectId, jobId, options) {
  const opts = options || {};
  const timeoutMs = Number(opts.timeoutMs || CONFIG.execution.maxQueryWaitMs);
  const pollIntervalMs = Number(opts.pollIntervalMs || CONFIG.execution.queryPollIntervalMs);
  const location = opts.location || CONFIG.core.region;
  const startedAt = new Date().getTime();

  let response = BigQuery.Jobs.getQueryResults(projectId, jobId, {
    location: location
  });

  while (!response.jobComplete) {
    const now = new Date().getTime();
    const elapsedMs = now - startedAt;

    if (elapsedMs > timeoutMs) {
      const error = new Error(
        'Timeout waiting for BigQuery job. jobId=' + jobId +
        ', timeoutMs=' + timeoutMs
      );
      error.code = 'BQ_JOB_TIMEOUT';
      error.jobId = jobId;
      throw error;
    }

    Utilities.sleep(pollIntervalMs);

    response = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      location: location
    });
  }

  if (response.errors && response.errors.length) {
    const firstError = response.errors[0];
    const error = new Error(
      'BigQuery job completed with errors: ' +
      JSON.stringify(firstError)
    );
    error.code = 'BQ_JOB_RESULT_ERROR';
    error.jobId = jobId;
    error.jobErrors = response.errors;
    throw error;
  }

  return response;
}


/**
 * Парсит BigQuery results в массив объектных строк.
 */
function parseQueryResults_(queryResults) {
  const schemaFields = safeGetPath_(queryResults, ['schema', 'fields'], []);
  const rows = queryResults.rows || [];

  return rows.map(function (row) {
    const obj = {};

    schemaFields.forEach(function (field, idx) {
      const cell = row.f && row.f[idx] ? row.f[idx].v : null;
      obj[field.name] = parseBigQueryFieldValue_(cell, field);
    });

    return obj;
  });
}


/**
 * Парсит одно поле из BigQuery result row.
 */
function parseBigQueryFieldValue_(value, field) {
  if (value === null || value === undefined) {
    return '';
  }

  const fieldType = String(field.type || '').toUpperCase();
  const fieldMode = String(field.mode || '').toUpperCase();

  if (fieldMode === 'REPEATED') {
    if (!Array.isArray(value)) return [];
    return value.map(function (item) {
      return parseBigQueryFieldValue_(item.v, {
        type: fieldType,
        mode: 'NULLABLE',
        fields: field.fields || []
      });
    });
  }

  if (fieldType === 'RECORD' || fieldType === 'STRUCT') {
    const nestedFields = field.fields || [];
    const nestedObj = {};

    if (value && value.f && nestedFields.length) {
      nestedFields.forEach(function (nestedField, idx) {
        const nestedValue = value.f[idx] ? value.f[idx].v : null;
        nestedObj[nestedField.name] = parseBigQueryFieldValue_(nestedValue, nestedField);
      });
      return nestedObj;
    }

    return {};
  }

  if (fieldType === 'INTEGER' || fieldType === 'INT64') {
    const n = Number(value);
    return isNaN(n) ? String(value) : n;
  }

  if (fieldType === 'FLOAT' || fieldType === 'FLOAT64' || fieldType === 'NUMERIC' || fieldType === 'BIGNUMERIC') {
    const f = Number(value);
    return isNaN(f) ? String(value) : f;
  }

  if (fieldType === 'BOOLEAN' || fieldType === 'BOOL') {
    return String(value).toLowerCase() === 'true';
  }

  return value;
}


/**
 * Нормализация query options.
 */
function normalizeQueryOptions_(options) {
  const opts = options || {};

  return {
    ctx: opts.ctx || null,
    useLegacySql: !!opts.useLegacySql,
    timeoutMs: Number(opts.timeoutMs || CONFIG.execution.maxQueryWaitMs),
    maxRetries: Number(opts.maxRetries || CONFIG.execution.maxRetries),
    dryRun: !!opts.dryRun,
    skipIfTooExpensive: !!opts.skipIfTooExpensive,
    maxBytesBilledEstimate: Number(opts.maxBytesBilledEstimate || 0),
    location: opts.location || CONFIG.core.region
  };
}


/**
 * Можно ли retry-ить ошибку BigQuery.
 */
function isRetryableBigQueryError_(error) {
  const message = extractErrorMessage_(error).toLowerCase();

  const retryableMarkers = [
    'internal error',
    'backend error',
    'rate limit',
    'quota exceeded',
    'service unavailable',
    'timed out',
    'timeout',
    'connection reset',
    'temporary'
  ];

  return retryableMarkers.some(function (marker) {
    return message.indexOf(marker) !== -1;
  });
}


/**
 * Backoff sleep
 */
function buildRetrySleepMs_(attempt) {
  const base = Number(CONFIG.execution.retrySleepBaseMs || 2000);
  const jitter = Math.floor(Math.random() * 500);
  return Math.min(base * Math.pow(2, attempt - 1) + jitter, 30000);
}


/**
 * Безопасная регистрация query в runtime counters.
 */
function registerExecutedQuerySafe_(ctx, count) {
  if (typeof registerExecutedQuery_ === 'function') {
    registerExecutedQuery_(ctx, count || 1);
  }
}


/**
 * Безопасный доступ к вложенному пути объекта.
 */
function safeGetPath_(obj, path, fallback) {
  if (!obj || !path || !path.length) return fallback;

  let current = obj;
  for (var i = 0; i < path.length; i++) {
    if (current === null || current === undefined) {
      return fallback;
    }
    current = current[path[i]];
  }

  return current === undefined ? fallback : current;
}
