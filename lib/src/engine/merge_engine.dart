import '../core/hlc.dart';
import '../core/sql_executor.dart';
import '../core/sync_models.dart';
import '../schema/shadow_tables.dart';
import '../schema/trigger_generator.dart';

/// The result of executing a merge pass of remote deltas into the local database.
class MergeResult {
  /// The total number of column deltas successfully applied to user tables.
  final int appliedCount;

  /// The list of concurrent conflicts detected during the merge pass.
  final List<SyncConflict> conflicts;

  /// The highest remote [Hlc] observed and processed in the batch.
  final Hlc? maxRemoteHlc;

  /// Creates a [MergeResult].
  const MergeResult({
    required this.appliedCount,
    required this.conflicts,
    this.maxRemoteHlc,
  });

  @override
  String toString() =>
      'MergeResult(applied: $appliedCount, conflicts: ${conflicts.length}, maxHlc: $maxRemoteHlc)';
}

/// Column-level Last-Write-Wins (LWW) conflict resolution and merge engine.
///
/// Features:
/// * **Trigger Suppression:** Sets `is_pulling = 1` during application of remote deltas,
///   guaranteeing incoming server mutations never re-fire local outbox triggers.
/// * **Column-Level Granularity:** Multiple offline clients editing different columns of
///   the same row converge cleanly without clobbering each other's changes.
/// * **Tombstone Semantics:** Deletions are tracked via `_sync_tombstones` and causally
///   compared against incoming mutations.
class MergeEngine {
  /// The database executor used to perform queries and mutations.
  final SyncSqlExecutor db;

  // Cache of primary key column names per table.
  final Map<String, String> _pkCache = {};

  /// Creates a [MergeEngine] for [db].
  MergeEngine(this.db);

  /// Resolves the primary key column name for [tableName], caching the result.
  ///
  /// Returns `null` if the table does not exist in the local SQLite schema.
  Future<String?> _getPrimaryKeyColumn(String tableName) async {
    if (_pkCache.containsKey(tableName)) {
      return _pkCache[tableName];
    }

    final tableExists = await db.query(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?;",
      [tableName],
    );
    if (tableExists.isEmpty) {
      return null;
    }

    final columns = await TriggerGenerator.introspectTable(db, tableName);
    final pks = columns.where((c) => c.isPrimaryKey).toList();
    final pkName = pks.isNotEmpty ? pks.first.name : 'id';
    _pkCache[tableName] = pkName;
    return pkName;
  }

  /// Merges a [batch] of remote deltas into the local database.
  ///
  /// Executes inside an exclusive transaction with trigger suppression enabled.
  Future<MergeResult> mergeBatch(SyncBatch batch) async {
    return mergeDeltas(batch.deltas);
  }

  /// Merges a list of remote [deltas] into the local database.
  ///
  /// Executes inside an exclusive transaction with trigger suppression enabled.
  Future<MergeResult> mergeDeltas(List<ColumnDelta> deltas) async {
    if (deltas.isEmpty) {
      return const MergeResult(appliedCount: 0, conflicts: []);
    }

    // Sort incoming deltas causally by HLC so causal dependency order is preserved.
    final sorted = List<ColumnDelta>.from(deltas)
      ..sort((a, b) => a.hlc.compareTo(b.hlc));

    return db.transaction((txn) async {
      // 1. Activate trigger suppression
      await ShadowTables.setPulling(txn, true);

      int appliedCount = 0;
      final conflicts = <SyncConflict>[];
      Hlc? maxRemoteHlc;

      try {
        // Group deltas by (tableName, rowId) to handle composite row creation cleanly
        final rowGroups = <String, List<ColumnDelta>>{};
        for (final delta in sorted) {
          if (maxRemoteHlc == null || delta.hlc > maxRemoteHlc) {
            maxRemoteHlc = delta.hlc;
          }
          final key = '${delta.tableName}:::${delta.rowId}';
          rowGroups.putIfAbsent(key, () => []).add(delta);
        }

        for (final group in rowGroups.values) {
          final outcome = await _mergeRowGroup(txn, group);
          appliedCount += outcome.applied;
          conflicts.addAll(outcome.conflicts);
        }

        // Update watermark
        if (maxRemoteHlc != null) {
          final currentWatermark = await ShadowTables.getLastPulledHlc(txn);
          if (currentWatermark == null || maxRemoteHlc > currentWatermark) {
            await ShadowTables.setLastPulledHlc(txn, maxRemoteHlc);
          }
        }
      } finally {
        // 2. Guarantee trigger suppression is restored
        await ShadowTables.setPulling(txn, false);
      }

      return MergeResult(
        appliedCount: appliedCount,
        conflicts: conflicts,
        maxRemoteHlc: maxRemoteHlc,
      );
    });
  }

  Future<({int applied, List<SyncConflict> conflicts})> _mergeRowGroup(
    SyncSqlExecutor txn,
    List<ColumnDelta> deltas,
  ) async {
    int applied = 0;
    final conflicts = <SyncConflict>[];

    final first = deltas.first;
    final tableName = first.tableName;
    final rowId = first.rowId;
    final pkCol = await _getPrimaryKeyColumn(tableName);
    if (pkCol == null) {
      // Table does not exist locally; safely ignore delta.
      return (applied: 0, conflicts: conflicts);
    }

    // Check tombstone table
    final tombstoneRows = await txn.query(
      'SELECT hlc FROM ${ShadowTables.tableTombstones} WHERE table_name = ? AND row_id = ?;',
      [tableName, rowId],
    );
    final Hlc? tombstoneHlc = tombstoneRows.isNotEmpty
        ? Hlc.tryParse(tombstoneRows.first['hlc'] as String?)
        : null;

    // Check if whole-row deletion exists in this group
    final deletionDelta = deltas
        .where((d) => d.isDeleted || d.columnName == '*')
        .lastOrNull;

    if (deletionDelta != null) {
      // Check if any local column was updated strictly after this deletion
      final clockRows = await txn.query(
        'SELECT MAX(hlc) AS max_hlc FROM ${ShadowTables.tableColumnClock} WHERE table_name = ? AND row_id = ?;',
        [tableName, rowId],
      );
      final maxLocalClock = clockRows.isNotEmpty
          ? Hlc.tryParse(clockRows.first['max_hlc'] as String?)
          : null;

      if (maxLocalClock != null && maxLocalClock > deletionDelta.hlc) {
        // Local edit occurred after remote deletion => local edit resurrects row
        conflicts.add(
          SyncConflict(
            tableName: tableName,
            rowId: rowId,
            columnName: '*',
            localValue: 'ROW_EXISTS',
            localHlc: maxLocalClock,
            remoteValue: 'DELETED',
            remoteHlc: deletionDelta.hlc,
            resolution: ConflictResolution.localWins,
          ),
        );
      } else {
        // Remote deletion wins
        await txn.execute('DELETE FROM "$tableName" WHERE "$pkCol" = ?;', [
          rowId,
        ]);

        await txn.execute(
          '''
INSERT INTO ${ShadowTables.tableTombstones} (table_name, row_id, hlc, deleted_at)
VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
ON CONFLICT (table_name, row_id) DO UPDATE SET hlc = EXCLUDED.hlc, deleted_at = EXCLUDED.deleted_at;
''',
          [tableName, rowId, deletionDelta.hlc.toString()],
        );

        applied++;
      }
      return (applied: applied, conflicts: conflicts);
    }

    // Process column deltas (mutations)
    // Check if row was deleted locally after these edits
    if (tombstoneHlc != null) {
      final maxGroupHlc = deltas
          .map((d) => d.hlc)
          .reduce((a, b) => a > b ? a : b);
      if (tombstoneHlc >= maxGroupHlc) {
        for (final delta in deltas) {
          conflicts.add(
            SyncConflict(
              tableName: tableName,
              rowId: rowId,
              columnName: delta.columnName,
              localValue: 'DELETED',
              localHlc: tombstoneHlc,
              remoteValue: delta.value,
              remoteHlc: delta.hlc,
              resolution: ConflictResolution.localWins,
            ),
          );
        }
        return (applied: 0, conflicts: conflicts);
      } else {
        // Remote edits are newer than local tombstone => resurrect row
        await txn.execute(
          'DELETE FROM ${ShadowTables.tableTombstones} WHERE table_name = ? AND row_id = ?;',
          [tableName, rowId],
        );
      }
    }

    // Check if row currently exists in the user table
    final existsRows = await txn.query(
      'SELECT "$pkCol" FROM "$tableName" WHERE "$pkCol" = ?;',
      [rowId],
    );
    final rowExists = existsRows.isNotEmpty;

    // Filter winning column deltas
    final winningDeltas = <ColumnDelta>[];
    for (final delta in deltas) {
      if (delta.columnName == '*' || delta.columnName == pkCol) continue;

      final localClockRows = await txn.query(
        'SELECT hlc FROM ${ShadowTables.tableColumnClock} WHERE table_name = ? AND row_id = ? AND column_name = ?;',
        [tableName, rowId, delta.columnName],
      );
      final Hlc? localHlc = localClockRows.isNotEmpty
          ? Hlc.tryParse(localClockRows.first['hlc'] as String?)
          : null;

      final localOutboxRows = await txn.query(
        'SELECT 1 FROM ${ShadowTables.tableOutbox} WHERE table_name = ? AND row_id = ? AND column_name = ?;',
        [tableName, rowId, delta.columnName],
      );
      final hasLocalOfflineEdit = localOutboxRows.isNotEmpty;

      if (localHlc != null && localHlc >= delta.hlc) {
        // Local wins
        conflicts.add(
          SyncConflict(
            tableName: tableName,
            rowId: rowId,
            columnName: delta.columnName,
            localValue: null,
            localHlc: localHlc,
            remoteValue: delta.value,
            remoteHlc: delta.hlc,
            resolution: ConflictResolution.localWins,
          ),
        );
      } else {
        // Remote wins
        winningDeltas.add(delta);
        if (hasLocalOfflineEdit) {
          conflicts.add(
            SyncConflict(
              tableName: tableName,
              rowId: rowId,
              columnName: delta.columnName,
              localValue: null,
              localHlc: localHlc,
              remoteValue: delta.value,
              remoteHlc: delta.hlc,
              resolution: ConflictResolution.remoteWins,
            ),
          );
        }
      }
    }

    if (winningDeltas.isEmpty) {
      return (applied: 0, conflicts: conflicts);
    }

    if (!rowExists) {
      // Insert new row with primary key and all winning columns in a single INSERT
      final colNames = winningDeltas.map((d) => d.columnName).toList();
      final allCols = [pkCol, ...colNames];
      final placeholders = List.filled(allCols.length, '?').join(', ');
      final colSql = allCols.map((c) => '"$c"').join(', ');
      final values = [rowId, ...winningDeltas.map((d) => d.value)];

      await txn.execute(
        'INSERT INTO "$tableName" ($colSql) VALUES ($placeholders);',
        values,
      );

      for (final delta in winningDeltas) {
        await txn.execute(
          '''
INSERT INTO ${ShadowTables.tableColumnClock} (table_name, row_id, column_name, hlc)
VALUES (?, ?, ?, ?)
ON CONFLICT (table_name, row_id, column_name) DO UPDATE SET hlc = EXCLUDED.hlc;
''',
          [tableName, rowId, delta.columnName, delta.hlc.toString()],
        );
        applied++;
      }
    } else {
      // Update existing row column-by-column
      for (final delta in winningDeltas) {
        await txn.execute(
          'UPDATE "$tableName" SET "${delta.columnName}" = ? WHERE "$pkCol" = ?;',
          [delta.value, rowId],
        );

        await txn.execute(
          '''
INSERT INTO ${ShadowTables.tableColumnClock} (table_name, row_id, column_name, hlc)
VALUES (?, ?, ?, ?)
ON CONFLICT (table_name, row_id, column_name) DO UPDATE SET hlc = EXCLUDED.hlc;
''',
          [tableName, rowId, delta.columnName, delta.hlc.toString()],
        );
        applied++;
      }
    }

    return (applied: applied, conflicts: conflicts);
  }
}
