/*******************************************************
 * 00_config.gs
 * Единая конфигурация проекта паспорта BigQuery -> Google Sheets
 *******************************************************/

const CONFIG = Object.freeze({
  core: Object.freeze({
    projectId: 'acs-dwh',
    region: 'eu',
    spreadsheetId: '10l1V9hRW5P-QKM11yv5EJ9ry8hf3bLM_D8K3XHXYaWU',
    timezone: 'Europe/Kyiv',
    locale: 'ru'
  }),

  execution: Object.freeze({
    defaultMode: 'FAST',           // FAST | FULL
    lockTimeoutMs: 30 * 1000,
    queryPollIntervalMs: 1500,
    maxQueryWaitMs: 10 * 60 * 1000,
    maxRetries: 3,
    retrySleepBaseMs: 2000,
    writeBatchSize: 5000
  }),

  triggers: Object.freeze({
    dailyFastHour: 7,
    dailyFullHour: 8,
    enableDailyFastTrigger: true,
    enableDailyFullTrigger: false
  }),

  limits: Object.freeze({
    maxDateCoverageTablesPerRun: 150,
    maxDuplicateChecksPerRun: 50,
    maxNullKeyChecksPerRun: 50,
    maxCustomSqlChecksPerRun: 50,
    maxHeavyObjectsPerRun: 200,
    maxSheetsAutoResizeColumns: 40
  }),

  thresholds: Object.freeze({
    defaultExpectedRefreshFrequency: 'DAILY',
    defaultFreshnessThresholdHours: 36,

    freshnessWarningMultiplier: 1,
    freshnessCriticalMultiplier: 2,

    rowCountAnomalyRatioWarning: 0.5,
    rowCountAnomalyRatioCritical: 0.2,

    heavyObjectSizeGbThreshold: 1,
    veryHeavyObjectSizeGbThreshold: 10
  }),

  features: Object.freeze({
    enableHeavyChecks: true,
    enableCustomSqlChecks: true,
    enableSchemaDrift: true,
    enableFreshnessHistory: true,
    enableQualityHistory: true,
    enableStorageAudit: true,
    enablePartitionsAudit: false,
    enableRoutinesScan: true,
    enableMaterializedViewsScan: true,
    enableExternalTablesScan: true,
    enableDryRunForHeavyQueries: true
  }),

  defaults: Object.freeze({
    businessCriticality: 'normal',
    managementPriority: 'medium',
    refreshMechanism: 'UNKNOWN',
    allowedUse: '',
    notRecommendedUse: '',
    incidentIfBroken: '',
    owner: '',
    slaTier: 'standard'
  }),

  objectTypes: Object.freeze({
    baseTable: 'BASE TABLE',
    view: 'VIEW',
    materializedView: 'MATERIALIZED VIEW',
    external: 'EXTERNAL',
    snapshot: 'SNAPSHOT',
    clone: 'CLONE'
  }),

  statuses: Object.freeze({
    freshness: Object.freeze({
      fresh: 'FRESH',
      late: 'LATE',
      stale: 'STALE',
      unknown: 'UNKNOWN'
    }),

    quality: Object.freeze({
      pass: 'PASS',
      warn: 'WARN',
      fail: 'FAIL',
      error: 'ERROR'
    }),

    generic: Object.freeze({
      ok: 'OK',
      skipped: 'SKIPPED',
      skippedByLimit: 'SKIPPED_BY_LIMIT',
      emptyResult: 'EMPTY_RESULT',
      unknown: 'UNKNOWN',
      error: 'ERROR'
    })
  }),

  riskFlags: Object.freeze({
    ok: 'OK',
    noDescription: 'NO_DESCRIPTION',
    oldData: 'OLD_DATA',
    empty: 'EMPTY',
    tooBig: 'TOO_BIG',
    noColumnsMetadata: 'NO_COLUMNS_METADATA'
  }),

  scanRisk: Object.freeze({
    low: 'LOW',
    medium: 'MEDIUM',
    high: 'HIGH',
    veryHigh: 'VERY_HIGH'
  }),

  layers: Object.freeze({
    rawGa4: 'raw_ga4',
    raw: 'raw',
    dim: 'dim',
    staging: 'staging',
    mart: 'mart',
    execView: 'exec_view',
    forecast: 'forecast',
    analytics: 'analytics',
    unknown: 'unknown'
  }),

  sources: Object.freeze({
    ga4: 'GA4',
    googleAds: 'Google Ads',
    salesDrive: 'SalesDrive',
    searchConsole: 'Search Console',
    manual: 'Manual',
    unknown: 'Unknown'
  }),

  purposes: Object.freeze({
    demand: 'demand',
    orders: 'orders',
    sales: 'sales',
    forecast: 'forecast',
    abcAnalysis: 'abc_analysis',
    actionSignals: 'action_signals',
    priority: 'priority',
    executiveControl: 'executive_control',
    masterData: 'master_data',
    adsRaw: 'ads_raw',
    other: 'other'
  }),

  updateTypes: Object.freeze({
    liveView: 'LIVE_VIEW',
    autoGa4Export: 'AUTO_GA4_EXPORT',
    autoConnectorOrTransfer: 'AUTO_CONNECTOR_OR_TRANSFER',
    scriptOrScheduledQuery: 'SCRIPT_OR_SCHEDULED_QUERY',
    unknown: 'UNKNOWN'
  }),

  refreshMechanisms: Object.freeze({
    liveView: 'LIVE_VIEW',
    ga4Export: 'GA4_BIGQUERY_EXPORT',
    transferOrConnector: 'TRANSFER_OR_CONNECTOR',
    scriptOrScheduledQuery: 'SCRIPT_OR_SCHEDULED_QUERY',
    manual: 'MANUAL',
    unknown: 'UNKNOWN'
  }),

  lineage: Object.freeze({
    dependencySourceAutoSqlParse: 'AUTO_SQL_PARSE',
    dependencySourceManual: 'MANUAL',
    dependencyTypeAutoViewLineage: 'AUTO_VIEW_LINEAGE',
    dependencyTypeManual: 'MANUAL',
    confidenceHigh: 'HIGH',
    confidenceMedium: 'MEDIUM',
    confidenceLow: 'LOW'
  }),

  sheets: Object.freeze({
    contents: 'Оглавление',
    summary: 'Сводка',
    datasets: 'Датасеты',
    objects: 'Объекты',
    columns: 'Поля',
    viewsLineage: 'Связи VIEW',
    objectDependencies: 'Зависимости объектов',
    tree: 'Дерево',
    dateCoverage: 'Период данных',
    qualityChecks: 'Проверки качества',
    heavyObjects: 'Тяжелые объекты',
    keyObjects: 'Ключевые объекты',
    dictionary: 'Справочник',

    manualObjectMetadata: 'Метаданные объектов',
    manualDependencies: 'Зависимости справочник',
    manualQualityRules: 'Правила качества',
    manualObjectSla: 'SLA объектов',
    manualObjectOwners: 'Владельцы объектов',

    runLog: 'Журнал обновлений',
    errorLog: 'Ошибки',
    executionSteps: 'Этапы выполнения',

    freshnessHistory: 'История freshness',
    qualityHistory: 'История качества',
    schemaHistory: 'История схем',
    schemaChanges: 'Изменения схем'
  }),

  headers: Object.freeze({
    runLog: Object.freeze([
      'run_id',
      'mode',
      'started_at',
      'finished_at',
      'status',
      'datasets_count',
      'objects_count',
      'columns_count',
      'dependency_rows_count',
      'date_coverage_rows_count',
      'quality_rows_count',
      'heavy_objects_rows_count',
      'key_objects_rows_count',
      'error_message'
    ]),

    errorLog: Object.freeze([
      'run_id',
      'mode',
      'step_name',
      'error_message',
      'stack',
      'logged_at'
    ]),

    executionSteps: Object.freeze([
      'run_id',
      'mode',
      'step_name',
      'status',
      'started_at',
      'finished_at',
      'duration_ms',
      'details'
    ])
  }),

  dateFieldPriorityNames: Object.freeze([
    'date',
    'event_date',
    'order_date',
    'created_at',
    'created_date',
    'updated_at',
    'day',
    'dt',
    '_date',
    '_at'
  ]),

  primaryKeyPreferredCombos: Object.freeze([
    ['dt', 'sku'],
    ['date', 'sku'],
    ['event_date', 'item_id'],
    ['event_date', 'sku'],
    ['date', 'item_id'],
    ['order_id'],
    ['id']
  ]),

  joinKeyCandidates: Object.freeze([
    'sku',
    'item_id',
    'prom_id',
    'category',
    'date',
    'dt',
    'event_date',
    'order_id',
    'user_pseudo_id'
  ])
});


/**
 * Возвращает fully qualified object key
 * Формат: project.dataset.object
 */
function buildObjectKey_(projectId, datasetName, objectName) {
  return [projectId, datasetName, objectName]
    .map(v => String(v || '').trim())
    .join('.');
}


/**
 * Возвращает текущий timestamp в строке формата проекта
 */
function getNowProjectTz_() {
  return Utilities.formatDate(
    new Date(),
    CONFIG.core.timezone,
    'yyyy-MM-dd HH:mm:ss'
  );
}


/**
 * Служебная проверка режима запуска
 */
function assertExecutionMode_(mode) {
  const normalized = String(mode || '').toUpperCase().trim();
  const allowed = ['FAST', 'FULL'];

  if (!allowed.includes(normalized)) {
    throw new Error(
      'Недопустимый режим запуска: ' + mode + '. Разрешено только FAST или FULL.'
    );
  }

  return normalized;
}
