/*******************************************************
 * 10_lineage.gs
 * Построение зависимостей и lineage объектов
 *******************************************************/


/**
 * Строит auto-lineage для VIEW на основе view_definition.
 *
 * views:
 *   массив строк из fetchViews_()
 *
 * Возвращает:
 * [
 *   {
 *     object_key,
 *     object_type,
 *     dataset_name,
 *     table_name,
 *     depends_on,
 *     dependency_type,
 *     dependency_source,
 *     lineage_confidence,
 *     refresh_mechanism,
 *     dependency_comment
 *   }
 * ]
 */
function buildLineageRows_(views) {
  const items = Array.isArray(views) ? views : [];
  const rows = [];

  items.forEach(function (viewRow) {
    const datasetName = normalizePlainString_(viewRow.dataset_name);
    const tableName = normalizePlainString_(viewRow.table_name);
    const projectId = normalizePlainString_(viewRow.project_id) || CONFIG.core.projectId;
    const objectKey = buildObjectKey_(projectId, datasetName, tableName);
    const sql = String(viewRow.view_definition || '');

    const extraction = extractDependenciesFromViewSql_(sql, projectId);
    const dependencies = extraction.dependencies || [];

    if (!dependencies.length) {
      rows.push({
        object_key: objectKey,
        object_type: CONFIG.objectTypes.view,
        dataset_name: datasetName,
        table_name: tableName,
        depends_on: '',
        dependency_type: CONFIG.lineage.dependencyTypeAutoViewLineage,
        dependency_source: CONFIG.lineage.dependencySourceAutoSqlParse,
        lineage_confidence: CONFIG.lineage.confidenceLow,
        refresh_mechanism: CONFIG.refreshMechanisms.liveView,
        dependency_comment: extraction.comment || 'Зависимости не извлечены из SQL'
      });
      return;
    }

    dependencies.forEach(function (dep) {
      rows.push({
        object_key: objectKey,
        object_type: CONFIG.objectTypes.view,
        dataset_name: datasetName,
        table_name: tableName,
        depends_on: dep.depends_on,
        dependency_type: CONFIG.lineage.dependencyTypeAutoViewLineage,
        dependency_source: CONFIG.lineage.dependencySourceAutoSqlParse,
        lineage_confidence: dep.lineage_confidence,
        refresh_mechanism: CONFIG.refreshMechanisms.liveView,
        dependency_comment: dep.dependency_comment || ''
      });
    });
  });

  return dedupeLineageRows_(rows);
}


/**
 * Строит финальные dependency rows:
 * - auto lineage
 * - manual dependencies
 */
function buildDependencyRows_(enrichedObjects, lineageRows, manualDependencies) {
  const objectMap = buildEnrichedObjectsMap_(enrichedObjects);
  const autoRows = Array.isArray(lineageRows) ? lineageRows.slice() : [];
  const manualRows = buildManualDependencyRows_(manualDependencies, objectMap);

  const merged = autoRows.concat(manualRows);

  return dedupeLineageRows_(merged).sort(compareDependencyRows_);
}


/**
 * Строит дерево объектов с агрегированными зависимостями.
 */
function buildTreeRows_(enrichedObjects, dependencyRows) {
  const objects = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const deps = Array.isArray(dependencyRows) ? dependencyRows : [];

  const depMap = buildDependencyListMap_(deps);

  return objects.map(function (obj) {
    const objectKey = normalizePlainString_(obj.object_key);
    const depList = depMap[objectKey] || [];
    const depNames = depList.map(function (item) {
      return item.depends_on;
    });

    return {
      object_key: objectKey,
      layer_group: normalizePlainString_(obj.layer),
      dataset_name: normalizePlainString_(obj.dataset_name),
      table_name: normalizePlainString_(obj.table_name),
      table_type: normalizePlainString_(obj.table_type),
      source: normalizePlainString_(obj.source),
      purpose: normalizePlainString_(obj.purpose),
      update_type: normalizePlainString_(obj.update_type),
      refresh_mechanism: normalizePlainString_(obj.refresh_mechanism),
      freshness_status: normalizePlainString_(obj.freshness_status),
      quality_status: normalizePlainString_(obj.quality_status),
      business_criticality: normalizePlainString_(obj.business_criticality),
      management_priority: normalizePlainString_(obj.management_priority),
      risk_flag: normalizePlainString_(obj.risk_flag),
      depends_on_count: depNames.length,
      depends_on_list: depNames.join(' | '),
      dependency_sources: extractUniqueValues_(depList, 'dependency_source').join(' | '),
      lineage_confidence: inferAggregateLineageConfidence_(depList)
    };
  }).sort(function (a, b) {
    return [
      a.layer_group,
      a.dataset_name,
      a.table_name
    ].join('|').localeCompare([
      b.layer_group,
      b.dataset_name,
      b.table_name
    ].join('|'));
  });
}


/**
 * Извлекает зависимости из SQL VIEW.
 *
 * Возвращает:
 * {
 *   dependencies: [
 *     {
 *       depends_on,
 *       lineage_confidence,
 *       dependency_comment
 *     }
 *   ],
 *   comment
 * }
 */
function extractDependenciesFromViewSql_(sql, defaultProjectId) {
  const text = String(sql || '');
  const projectId = normalizePlainString_(defaultProjectId) || CONFIG.core.projectId;

  if (!text.trim()) {
    return {
      dependencies: [],
      comment: 'Пустой view_definition'
    };
  }

  const dependencies = [];
  const seen = {};

  /**
   * 1. Наиболее надежный путь — fully qualified references в backticks:
   * `project.dataset.table`
   * `dataset.table`
   */
  const backtickRegex = /`([^`]+)`/g;
  let match;

  while ((match = backtickRegex.exec(text)) !== null) {
    const rawRef = normalizePlainString_(match[1]);
    const normalizedRef = normalizeDependencyName_(rawRef, projectId);

    if (!normalizedRef) {
      continue;
    }

    if (!seen[normalizedRef]) {
      dependencies.push({
        depends_on: normalizedRef,
        lineage_confidence: rawRef.split('.').length === 3
          ? CONFIG.lineage.confidenceHigh
          : CONFIG.lineage.confidenceMedium,
        dependency_comment: 'Извлечено из backtick reference'
      });
      seen[normalizedRef] = true;
    }
  }

  /**
   * 2. Менее надежный путь — FROM / JOIN без backticks.
   * Поддерживаем dataset.table и project.dataset.table
   */
  const sqlWithoutStrings = stripSqlStrings_(text);

  const fromJoinRegex = /\b(?:FROM|JOIN)\s+([a-zA-Z0-9_\-]+\.[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)/gi;
  let fromJoinMatch;

  while ((fromJoinMatch = fromJoinRegex.exec(sqlWithoutStrings)) !== null) {
    const rawRef2 = normalizePlainString_(fromJoinMatch[1]);
    const normalizedRef2 = normalizeDependencyName_(rawRef2, projectId);

    if (!normalizedRef2) {
      continue;
    }

    if (!seen[normalizedRef2]) {
      dependencies.push({
        depends_on: normalizedRef2,
        lineage_confidence: rawRef2.split('.').length === 3
          ? CONFIG.lineage.confidenceMedium
          : CONFIG.lineage.confidenceLow,
        dependency_comment: 'Извлечено из FROM/JOIN без backticks'
      });
      seen[normalizedRef2] = true;
    }
  }

  return {
    dependencies: dependencies,
    comment: dependencies.length
      ? ''
      : 'SQL parse не нашел зависимостей'
  };
}


/**
 * Нормализует ссылку зависимости в формат project.dataset.object
 *
 * Поддерживает:
 * - project.dataset.table
 * - dataset.table
 */
function normalizeDependencyName_(rawRef, defaultProjectId) {
  const ref = normalizePlainString_(rawRef);
  if (!ref) {
    return '';
  }

  const cleaned = ref.replace(/^`+|`+$/g, '');
  const parts = cleaned.split('.').map(function (p) {
    return normalizePlainString_(p);
  }).filter(function (p) {
    return !!p;
  });

  if (parts.length === 3) {
    return buildObjectKey_(parts[0], parts[1], parts[2]);
  }

  if (parts.length === 2) {
    return buildObjectKey_(defaultProjectId || CONFIG.core.projectId, parts[0], parts[1]);
  }

  return '';
}


/**
 * Строит manual dependency rows в той же структуре, что и auto lineage.
 */
function buildManualDependencyRows_(manualDependencies, objectMap) {
  const items = Array.isArray(manualDependencies) ? manualDependencies : [];
  const map = objectMap || {};
  const rows = [];

  items.forEach(function (row) {
    const objectKey = resolveManualObjectKey_(row);
    const dependsOn = normalizeDependencyName_(row.depends_on, CONFIG.core.projectId);

    if (!objectKey) {
      return;
    }

    const obj = map[objectKey] || map[extractShortObjectKey_(objectKey)] || {};

    rows.push({
      object_key: objectKey,
      object_type: normalizePlainString_(obj.table_type) || 'UNKNOWN',
      dataset_name: normalizePlainString_(row.dataset_name) || normalizePlainString_(obj.dataset_name),
      table_name: normalizePlainString_(row.table_name) || normalizePlainString_(obj.table_name),
      depends_on: dependsOn || normalizePlainString_(row.depends_on),
      dependency_type: normalizePlainString_(row.dependency_type) || CONFIG.lineage.dependencyTypeManual,
      dependency_source: CONFIG.lineage.dependencySourceManual,
      lineage_confidence: CONFIG.lineage.confidenceHigh,
      refresh_mechanism: normalizePlainString_(row.refresh_mechanism) || '',
      dependency_comment: normalizePlainString_(row.comment)
    });
  });

  return rows;
}


/**
 * Строит map обогащенных объектов
 */
function buildEnrichedObjectsMap_(enrichedObjects) {
  const items = Array.isArray(enrichedObjects) ? enrichedObjects : [];
  const map = {};

  items.forEach(function (obj) {
    const fullKey = normalizePlainString_(obj.object_key);
    if (fullKey) {
      map[fullKey] = obj;
      const shortKey = extractShortObjectKey_(fullKey);
      if (shortKey) {
        map[shortKey] = obj;
      }
    }
  });

  return map;
}


/**
 * Группирует dependency rows по object_key
 */
function buildDependencyListMap_(dependencyRows) {
  const items = Array.isArray(dependencyRows) ? dependencyRows : [];
  const map = {};

  items.forEach(function (row) {
    const objectKey = normalizePlainString_(row.object_key);
    if (!objectKey) {
      return;
    }

    if (!map[objectKey]) {
      map[objectKey] = [];
    }

    map[objectKey].push(row);
  });

  return map;
}


/**
 * Агрегированная оценка confidence по списку зависимостей объекта.
 */
function inferAggregateLineageConfidence_(depList) {
  const items = Array.isArray(depList) ? depList : [];
  if (!items.length) {
    return '';
  }

  const scores = items.map(function (row) {
    const c = normalizePlainString_(row.lineage_confidence).toUpperCase();
    if (c === CONFIG.lineage.confidenceHigh) return 3;
    if (c === CONFIG.lineage.confidenceMedium) return 2;
    if (c === CONFIG.lineage.confidenceLow) return 1;
    return 0;
  });

  const minScore = Math.min.apply(null, scores);

  if (minScore >= 3) return CONFIG.lineage.confidenceHigh;
  if (minScore >= 2) return CONFIG.lineage.confidenceMedium;
  return CONFIG.lineage.confidenceLow;
}


/**
 * Удаляет дубли dependency rows
 */
function dedupeLineageRows_(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const seen = {};
  const result = [];

  items.forEach(function (row) {
    const key = [
      normalizePlainString_(row.object_key),
      normalizePlainString_(row.depends_on),
      normalizePlainString_(row.dependency_type),
      normalizePlainString_(row.dependency_source)
    ].join('|');

    if (seen[key]) {
      return;
    }

    seen[key] = true;
    result.push(row);
  });

  return result;
}


/**
 * Сравнение dependency rows для сортировки
 */
function compareDependencyRows_(a, b) {
  const left = [
    normalizePlainString_(a.dataset_name),
    normalizePlainString_(a.table_name),
    normalizePlainString_(a.depends_on),
    normalizePlainString_(a.dependency_source)
  ].join('|');

  const right = [
    normalizePlainString_(b.dataset_name),
    normalizePlainString_(b.table_name),
    normalizePlainString_(b.depends_on),
    normalizePlainString_(b.dependency_source)
  ].join('|');

  return left.localeCompare(right);
}


/**
 * Удаляет строковые литералы из SQL,
 * чтобы не ловить ложные совпадения в FROM/JOIN regex.
 */
function stripSqlStrings_(sql) {
  return String(sql || '')
    .replace(/'([^'\\]|\\.|'')*'/g, ' ')
    .replace(/"([^"\\]|\\.)*"/g, ' ');
}


/**
 * Уникальные значения поля в массиве объектов
 */
function extractUniqueValues_(rows, fieldName) {
  const items = Array.isArray(rows) ? rows : [];
  const seen = {};
  const result = [];

  items.forEach(function (row) {
    const value = normalizePlainString_(row[fieldName]);
    if (!value || seen[value]) {
      return;
    }
    seen[value] = true;
    result.push(value);
  });

  return result;
}
