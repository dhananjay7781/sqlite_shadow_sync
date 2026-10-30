import '../core/sql_executor.dart';

/// Metadata describing a column in a user table obtained via SQLite introspection.
class TableColumnInfo {
  /// The column ID/index in the table.
  final int cid;

  /// The column name.
  final String name;

  /// The SQLite data type affinity (e.g. `TEXT`, `INTEGER`, `REAL`).
  final String type;

  /// Whether the column is marked `NOT NULL`.
  final bool notNull;

  /// The default value expression if defined.
  final Object? defaultValue;

  /// Whether this column is a primary key or part of a composite primary key.
  final bool isPrimaryKey;

  /// Creates a [TableColumnInfo] descriptor.
  const TableColumnInfo({
    required this.cid,
    required this.name,
    required this.type,
    required this.notNull,
    this.defaultValue,
    required this.isPrimaryKey,
  });

  @override
  String toString() =>
      'TableColumnInfo($name, type: $type, pk: $isPrimaryKey, notNull: $notNull)';
}

/// Dynamic SQLite trigger generator and user schema introspector.
///
/// Introspects registered user tables via `PRAGMA table_info` and synthesizes
/// `AFTER INSERT`, `AFTER UPDATE`, and `AFTER DELETE` triggers.
///
/// Every generated trigger is loop-safe, suppressed during remote pulls via
/// `WHEN (SELECT is_pulling FROM _sync_node_state WHERE id = 1) = 0`.
class TriggerGenerator {
  /// Private constructor to prevent instantiation.
  const TriggerGenerator._();

  /// Prefix applied to generated trigger names.
  static const String triggerPrefix = '_sync_trg_';

  /// Introspects the column schema of [tableName] via `PRAGMA table_info`.
  static Future<List<TableColumnInfo>> introspectTable(
    SyncSqlExecutor db,
    String tableName,
  ) async {
    final rows = await db.query('PRAGMA table_info("$tableName");');
    if (rows.isEmpty) {
      throw ArgumentError(
        'TriggerGenerator: Table "$tableName" does not exist or has no columns.',
      );
    }

    return rows.map((r) {
      final pkVal = r['pk'];
      final isPk =
          pkVal != null &&
          (pkVal is num ? pkVal > 0 : int.tryParse(pkVal.toString())! > 0);
      final notNullVal = r['notnull'];
      final isNotNull =
          notNullVal != null &&
          (notNullVal is num
              ? notNullVal > 0
              : int.tryParse(notNullVal.toString())! > 0);

      return TableColumnInfo(
        cid: (r['cid'] as num?)?.toInt() ?? 0,
        name: r['name'] as String,
        type: (r['type'] as String? ?? 'TEXT').toUpperCase(),
        notNull: isNotNull,
        defaultValue: r['dflt_value'],
        isPrimaryKey: isPk,
      );
    }).toList();
  }

  /// Synthesizes the SQLite trigger DDL statements for [tableName].
  ///
  /// Returns a list of 3 SQL statements: `[insertTrigger, updateTrigger, deleteTrigger]`.
  static Future<List<String>> generateTriggers(
    SyncSqlExecutor db,
    String tableName,
  ) async {
    final columns = await introspectTable(db, tableName);
    final pkCols = columns.where((c) => c.isPrimaryKey).toList();

    // Expression to derive the unique row_id as a string.
    final String rowIdExprNew;
    final String rowIdExprOld;

    if (pkCols.length == 1) {
      final pkName = pkCols.first.name;
      rowIdExprNew = 'CAST(NEW."$pkName" AS TEXT)';
      rowIdExprOld = 'CAST(OLD."$pkName" AS TEXT)';
    } else if (pkCols.length > 1) {
      rowIdExprNew = pkCols
          .map((c) => 'CAST(NEW."${c.name}" AS TEXT)')
          .join(" || '/' || ");
      rowIdExprOld = pkCols
          .map((c) => 'CAST(OLD."${c.name}" AS TEXT)')
          .join(" || '/' || ");
    } else {
      // Fallback for tables without explicit primary keys.
      rowIdExprNew = 'CAST(NEW.rowid AS TEXT)';
      rowIdExprOld = 'CAST(OLD.rowid AS TEXT)';
    }

    final insertTriggerName = '$triggerPrefix${tableName}_ins';
    final updateTriggerName = '$triggerPrefix${tableName}_upd';
    final deleteTriggerName = '$triggerPrefix${tableName}_del';

    // 1. AFTER INSERT Trigger
    final insertBuffer = StringBuffer();
    insertBuffer.writeln('CREATE TRIGGER IF NOT EXISTS "$insertTriggerName"');
    insertBuffer.writeln('AFTER INSERT ON "$tableName"');
    insertBuffer.writeln('FOR EACH ROW');
    insertBuffer.writeln(
      'WHEN (SELECT is_pulling FROM _sync_node_state WHERE id = 1) = 0',
    );
    insertBuffer.writeln('BEGIN');

    // Remove any historical tombstone if re-inserting
    insertBuffer.writeln(
      '  DELETE FROM _sync_tombstones WHERE table_name = \'$tableName\' AND row_id = $rowIdExprNew;',
    );

    for (final col in columns) {
      insertBuffer.writeln('''
  INSERT INTO _sync_outbox (table_name, row_id, column_name, value, hlc, is_deleted)
  VALUES ('$tableName', $rowIdExprNew, '${col.name}', CAST(NEW."${col.name}" AS TEXT),
          strftime('%Y-%m-%dT%H:%M:%fZ', 'now') || ':0000:' || (SELECT node_id FROM _sync_node_state WHERE id = 1), 0)
  ON CONFLICT(table_name, row_id, column_name) DO UPDATE SET
    value = EXCLUDED.value,
    hlc = EXCLUDED.hlc,
    is_deleted = 0;

  INSERT INTO _sync_column_clock (table_name, row_id, column_name, hlc)
  VALUES ('$tableName', $rowIdExprNew, '${col.name}',
          strftime('%Y-%m-%dT%H:%M:%fZ', 'now') || ':0000:' || (SELECT node_id FROM _sync_node_state WHERE id = 1))
  ON CONFLICT(table_name, row_id, column_name) DO UPDATE SET
    hlc = EXCLUDED.hlc;
''');
    }
    insertBuffer.writeln('END;');

    // 2. AFTER UPDATE Trigger
    final updateBuffer = StringBuffer();
    updateBuffer.writeln('CREATE TRIGGER IF NOT EXISTS "$updateTriggerName"');
    updateBuffer.writeln('AFTER UPDATE ON "$tableName"');
    updateBuffer.writeln('FOR EACH ROW');
    updateBuffer.writeln(
      'WHEN (SELECT is_pulling FROM _sync_node_state WHERE id = 1) = 0',
    );
    updateBuffer.writeln('BEGIN');

    // Only non-PK columns need to be captured for column updates
    final updateCols = columns.where((c) => !c.isPrimaryKey).toList();
    final targetCols = updateCols.isEmpty ? columns : updateCols;

    for (final col in targetCols) {
      updateBuffer.writeln('''
  INSERT INTO _sync_outbox (table_name, row_id, column_name, value, hlc, is_deleted)
  SELECT '$tableName', $rowIdExprNew, '${col.name}', CAST(NEW."${col.name}" AS TEXT),
         strftime('%Y-%m-%dT%H:%M:%fZ', 'now') || ':0000:' || (SELECT node_id FROM _sync_node_state WHERE id = 1), 0
  WHERE OLD."${col.name}" IS NOT NEW."${col.name}"
  ON CONFLICT(table_name, row_id, column_name) DO UPDATE SET
    value = EXCLUDED.value,
    hlc = EXCLUDED.hlc,
    is_deleted = 0;

  INSERT INTO _sync_column_clock (table_name, row_id, column_name, hlc)
  SELECT '$tableName', $rowIdExprNew, '${col.name}',
         strftime('%Y-%m-%dT%H:%M:%fZ', 'now') || ':0000:' || (SELECT node_id FROM _sync_node_state WHERE id = 1)
  WHERE OLD."${col.name}" IS NOT NEW."${col.name}"
  ON CONFLICT(table_name, row_id, column_name) DO UPDATE SET
    hlc = EXCLUDED.hlc;
''');
    }
    updateBuffer.writeln('END;');

    // 3. AFTER DELETE Trigger
    final deleteBuffer = StringBuffer();
    deleteBuffer.writeln('CREATE TRIGGER IF NOT EXISTS "$deleteTriggerName"');
    deleteBuffer.writeln('AFTER DELETE ON "$tableName"');
    deleteBuffer.writeln('FOR EACH ROW');
    deleteBuffer.writeln(
      'WHEN (SELECT is_pulling FROM _sync_node_state WHERE id = 1) = 0',
    );
    deleteBuffer.writeln('BEGIN');
    deleteBuffer.writeln('''
  INSERT INTO _sync_tombstones (table_name, row_id, hlc, deleted_at)
  VALUES ('$tableName', $rowIdExprOld,
          strftime('%Y-%m-%dT%H:%M:%fZ', 'now') || ':0000:' || (SELECT node_id FROM _sync_node_state WHERE id = 1),
          strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  ON CONFLICT(table_name, row_id) DO UPDATE SET
    hlc = EXCLUDED.hlc,
    deleted_at = EXCLUDED.deleted_at;

  INSERT INTO _sync_outbox (table_name, row_id, column_name, value, hlc, is_deleted)
  VALUES ('$tableName', $rowIdExprOld, '*', NULL,
          strftime('%Y-%m-%dT%H:%M:%fZ', 'now') || ':0000:' || (SELECT node_id FROM _sync_node_state WHERE id = 1), 1)
  ON CONFLICT(table_name, row_id, column_name) DO UPDATE SET
    value = NULL,
    hlc = EXCLUDED.hlc,
    is_deleted = 1;

  DELETE FROM _sync_outbox
  WHERE table_name = '$tableName'
    AND row_id = $rowIdExprOld
    AND column_name != '*';
''');
    deleteBuffer.writeln('END;');

    return [
      insertBuffer.toString(),
      updateBuffer.toString(),
      deleteBuffer.toString(),
    ];
  }

  /// Installs sync capture triggers on all specified [tableNames].
  static Future<void> attachTriggers(
    SyncSqlExecutor db,
    List<String> tableNames,
  ) async {
    for (final table in tableNames) {
      final statements = await generateTriggers(db, table);
      for (final sql in statements) {
        await db.execute(sql);
      }
    }
  }

  /// Drops all sync capture triggers previously installed on [tableNames].
  static Future<void> detachTriggers(
    SyncSqlExecutor db,
    List<String> tableNames,
  ) async {
    for (final table in tableNames) {
      await db.execute('DROP TRIGGER IF EXISTS "$triggerPrefix${table}_ins";');
      await db.execute('DROP TRIGGER IF EXISTS "$triggerPrefix${table}_upd";');
      await db.execute('DROP TRIGGER IF EXISTS "$triggerPrefix${table}_del";');
    }
  }
}
