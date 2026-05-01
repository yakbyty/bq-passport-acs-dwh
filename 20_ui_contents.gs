/*******************************************************
 * 20_ui_contents.gs
 * Оглавление, справочник, навигация, меню
 *******************************************************/


/**
 * Меню при открытии таблицы.
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('BigQuery паспорт')

    // Основные действия
    .addItem('Обновить быстро', 'refreshProjectPassportFast')
    .addItem('Обновить полностью', 'refreshProjectPassportFull')

    .addSeparator()

    // Частичные обновления
    .addItem('Только структура', 'refreshMetadataSnapshot')
    .addItem('Только freshness + coverage', 'refreshFreshnessAndCoverage')
    .addItem('Только quality checks', 'refreshQualityChecks')

    .addSeparator()

    // Управление
    .addItem('Перестроить оглавление и сводку', 'rebuildContentsAndSummary')
    .addItem('Создать триггеры', 'createProjectTriggers')
    .addItem('Удалить триггеры', 'deleteProjectTriggers')

    .addSeparator()

    // Технические
    .addItem('Сбросить cursor coverage', 'resetDateCoverageCursor')

    .addSeparator()

    // SERVICE
    .addItem('SERVICE: bootstrap проекта', 'serviceBootstrapProject')
    .addItem('SERVICE: test BigQuery access', 'serviceTestBigQueryAccess')
    .addItem('SERVICE: test datasets', 'serviceTestFetchDatasets')
    .addItem('SERVICE: test objects', 'serviceTestFetchObjects')
    .addItem('SERVICE: check required functions', 'serviceCheckRequiredFunctions')
    .addItem('SERVICE: print config', 'servicePrintConfig')

    .addToUi();
}

function writeDictionarySheet_() {
  const rows = buildDictionaryRows_();

  writeSheet_(
    CONFIG.sheets.dictionary,
    ['field', 'value', 'description'],
    rows,
    null
  );
}
