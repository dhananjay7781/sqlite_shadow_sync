import '../core/hlc.dart';
import '../core/sql_executor.dart';
import '../core/sync_models.dart';

/// Manages the DDL and lifecycle of SQLite shadow tables used for metadata and outbox queueing.
///
/// These shadow tables isolate sync state completely from user tables, ensuring zero
/// schema pollution and preserving user migrations.
class ShadowTables {
  /// Private constructor to prevent instantiation.
  const ShadowTables._();

  /// Table storing node identity and synchronization suppression flags.
  static const String tableNodeState = '_sync_node_state';

  /// Table storing column-level Hybrid Logical Clocks for Last-Write-Wins resolution.
  static const String tableColumnClock = '_sync_column_clock';

  /// Table queueing local mutations awaiting push to the remote sync transport.
  static const String tableOutbox = '_sync_outbox';

  /// Table tracking row tombstones for deletions.
  static const String tableTombstones = '_sync_tombstones';

  /// DDL statement creating the `_sync_node_state` table.
  static const String ddlNodeState = '''
CREATE TABLE IF NOT EXISTS $tableNodeState (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  node_id TEXT NOT NULL,
  is_pulling INTEGER NOT NULL DEFAULT 0,
  last_pulled_hlc TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
''';

  /// DDL statement creating the `_sync_column_clock` table.
  static const String ddlColumnClock = '''
CREATE TABLE IF NOT EXISTS $tableColumnClock (
  table_name TEXT NOT NULL,
  row_id TEXT NOT NULL,
  column_name TEXT NOT NULL,
  hlc TEXT NOT NULL,
  PRIMARY KEY (table_name, row_id, column_name)
);
''';

  /// DDL statement creating the `_sync_outbox` table.
  static const String ddlOutbox = '''
CREATE TABLE IF NOT EXISTS $tableOutbox (
  table_name TEXT NOT NULL,
  row_id TEXT NOT NULL,
  column_name TEXT NOT NULL,
  value TEXT,
  hlc TEXT NOT NULL,
  is_deleted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (table_name, row_id, column_name)
);
''';

  /// DDL statement creating the `_sync_tombstones` table.
  static const String ddlTombstones = '''
CREATE TABLE IF NOT EXISTS $tableTombstones (
  table_name TEXT NOT NULL,
  row_id TEXT NOT NULL,
  hlc TEXT NOT NULL,
  deleted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  PRIMARY KEY (table_name, row_id)
);
''';

  /// DDL statements for auxiliary performance indexes.
  static const List<String> ddlIndexes = [
    'CREATE INDEX IF NOT EXISTS idx_sync_outbox_tbl_row ON $tableOutbox (table_name, row_id);',
    'CREATE INDEX IF NOT EXISTS idx_sync_col_clock_hlc ON $tableColumnClock (hlc);',
    'CREATE INDEX IF NOT EXISTS idx_sync_tombstones_hlc ON $tableTombstones (hlc);',
  ];

  /// Creates all required shadow tables and indexes if they do not already exist.
  static Future<void> createSchema(SyncSqlExecutor db) async {
    await db.execute(ddlNodeState);
    await db.execute(ddlColumnClock);
    await db.execute(ddlOutbox);
    await db.execute(ddlTombstones);
    for (final indexDdl in ddlIndexes) {
      await db.execute(indexDdl);
    }
  }

  /// Initializes the singleton node state row with the specified [nodeId].
  ///
  /// If the node state row already exists, this is a no-op unless overridden.
  static Future<void> initNodeState(SyncSqlExecutor db, String nodeId) async {
    await db.execute(
      '''
INSERT INTO $tableNodeState (id, node_id, is_pulling, last_pulled_hlc)
VALUES (1, ?, 0, NULL)
ON CONFLICT (id) DO NOTHING;
''',
      [nodeId],
    );
  }

  /// Retrieves the configured node identifier of the local database.
  ///
  /// Throws [StateError] if node state has not yet been initialized via [initNodeState].
  static Future<String> getNodeId(SyncSqlExecutor db) async {
    final rows = await db.query(
      'SELECT node_id FROM $tableNodeState WHERE id = 1;',
    );
    if (rows.isEmpty) {
      throw StateError(
        'ShadowTables: Node state not initialized. Call initNodeState() first.',
      );
    }
    return rows.first['node_id'] as String;
  }

  /// Whether the database is currently in trigger-suppressed remote pull mode.
  static Future<bool> isPulling(SyncSqlExecutor db) async {
    final rows = await db.query(
      'SELECT is_pulling FROM $tableNodeState WHERE id = 1;',
    );
    if (rows.isEmpty) return false;
    final val = rows.first['is_pulling'];
    return val == 1 || val == true;
  }

  /// Updates the trigger suppression flag [isPulling].
  ///
  /// When `true`, all triggers dynamically bypass outbox insertion.
  static Future<void> setPulling(SyncSqlExecutor db, bool isPulling) async {
    await db.execute(
      'UPDATE $tableNodeState SET is_pulling = ? WHERE id = 1;',
      [isPulling ? 1 : 0],
    );
  }

  /// Retrieves the latest remote [Hlc] watermark processed by the local node.
  static Future<Hlc?> getLastPulledHlc(SyncSqlExecutor db) async {
    final rows = await db.query(
      'SELECT last_pulled_hlc FROM $tableNodeState WHERE id = 1;',
    );
    if (rows.isEmpty) return null;
    final val = rows.first['last_pulled_hlc'] as String?;
    return Hlc.tryParse(val);
  }

  /// Persists the latest remote [Hlc] watermark [hlc].
  static Future<void> setLastPulledHlc(SyncSqlExecutor db, Hlc hlc) async {
    await db.execute(
      'UPDATE $tableNodeState SET last_pulled_hlc = ? WHERE id = 1;',
      [hlc.toString()],
    );
  }

  /// Fetches pending local mutations from the outbox up to [limit].
  static Future<List<ColumnDelta>> getOutboxDeltas(
    SyncSqlExecutor db, {
    int? limit,
  }) async {
    final sql = limit != null
        ? 'SELECT table_name, row_id, column_name, value, hlc, is_deleted FROM $tableOutbox ORDER BY hlc ASC LIMIT $limit;'
        : 'SELECT table_name, row_id, column_name, value, hlc, is_deleted FROM $tableOutbox ORDER BY hlc ASC;';

    final rows = await db.query(sql);
    return rows.map((r) {
      final isDel = r['is_deleted'];
      return ColumnDelta(
        tableName: r['table_name'] as String,
        rowId: (r['row_id'] as Object).toString(),
        columnName: r['column_name'] as String,
        value: r['value'],
        hlc: Hlc.parse(r['hlc'] as String),
        isDeleted: isDel == 1 || isDel == true,
      );
    }).toList();
  }

  /// Removes acknowledged [deltas] from the outbox.
  ///
  /// Uses causal matching (`hlc = delta.hlc`) so newer offline modifications
  /// made while the batch was in flight are safely retained.
  static Future<void> clearOutboxDeltas(
    SyncSqlExecutor db,
    List<ColumnDelta> deltas,
  ) async {
    for (final delta in deltas) {
      await db.execute(
        'DELETE FROM $tableOutbox WHERE table_name = ? AND row_id = ? AND column_name = ? AND hlc = ?;',
        [
          delta.tableName,
          delta.rowId,
          delta.columnName,
          delta.hlc.toString(),
        ],
      );
    }
  }

  /// Returns the current count of pending outbox deltas.
  static Future<int> getOutboxCount(SyncSqlExecutor db) async {
    final rows = await db.query('SELECT COUNT(*) AS c FROM $tableOutbox;');
    if (rows.isEmpty) return 0;
    final c = rows.first['c'];
    if (c is int) return c;
    if (c is num) return c.toInt();
    return int.tryParse(c.toString()) ?? 0;
  }
}
