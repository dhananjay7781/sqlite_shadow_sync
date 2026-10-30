import 'package:meta/meta.dart';
import 'hlc.dart';

/// The operational status of the synchronization engine.
@immutable
sealed class SyncStatus {
  const SyncStatus();
}

/// The engine is idle and waiting for the next sync cycle.
final class SyncIdle extends SyncStatus {
  /// The timestamp of the last idle transition.
  final DateTime timestamp;

  /// Creates a [SyncIdle] status.
  SyncIdle({DateTime? timestamp})
    : timestamp = timestamp ?? DateTime.now().toUtc();

  @override
  String toString() => 'SyncIdle(timestamp: $timestamp)';
}

/// The engine is actively pulling remote updates from the sync transport.
final class SyncPulling extends SyncStatus {
  /// The watermark clock requested from the transport.
  final Hlc? sinceHlc;

  /// Creates a [SyncPulling] status.
  const SyncPulling({this.sinceHlc});

  @override
  String toString() => 'SyncPulling(sinceHlc: $sinceHlc)';
}

/// The engine is actively pushing local deltas to the sync transport.
final class SyncPushing extends SyncStatus {
  /// The number of deltas being pushed in the current batch.
  final int batchSize;

  /// Creates a [SyncPushing] status.
  const SyncPushing({required this.batchSize});

  @override
  String toString() => 'SyncPushing(batchSize: $batchSize)';
}

/// Statistics and outcome metrics of a completed synchronization cycle.
@immutable
class SyncStats {
  /// The total number of deltas pushed to the remote server.
  final int deltasPushed;

  /// The total number of deltas pulled from the remote server.
  final int deltasPulled;

  /// The total number of conflicts resolved during merge.
  final int conflictsResolved;

  /// The elapsed duration of the synchronization cycle.
  final Duration duration;

  /// Creates a [SyncStats] metrics instance.
  const SyncStats({
    required this.deltasPushed,
    required this.deltasPulled,
    required this.conflictsResolved,
    required this.duration,
  });

  @override
  String toString() =>
      'SyncStats(pushed: $deltasPushed, pulled: $deltasPulled, conflicts: $conflictsResolved, duration: ${duration.inMilliseconds}ms)';
}

/// The engine successfully completed a synchronization cycle.
final class SyncSuccess extends SyncStatus {
  /// The statistics of the completed synchronization cycle.
  final SyncStats stats;

  /// The timestamp when the cycle completed.
  final DateTime completedAt;

  /// Creates a [SyncSuccess] status with completed [stats].
  SyncSuccess({required this.stats, DateTime? completedAt})
    : completedAt = completedAt ?? DateTime.now().toUtc();

  @override
  String toString() => 'SyncSuccess(stats: $stats, completedAt: $completedAt)';
}

/// The engine encountered an unrecoverable or transient error during synchronization.
final class SyncError extends SyncStatus {
  /// The error or exception that occurred.
  final Object error;

  /// The optional stack trace accompanying [error].
  final StackTrace? stackTrace;

  /// The duration until the next retry attempt if backoff is active.
  final Duration? nextRetryDelay;

  /// The zero-based retry attempt number.
  final int attempt;

  /// Creates a [SyncError] status.
  const SyncError({
    required this.error,
    this.stackTrace,
    this.nextRetryDelay,
    this.attempt = 0,
  });

  @override
  String toString() =>
      'SyncError(error: $error, attempt: $attempt, nextRetry: ${nextRetryDelay?.inMilliseconds}ms)';
}

/// Represents a column-level CRDT mutation or deletion delta.
@immutable
class ColumnDelta {
  /// The user table name.
  final String tableName;

  /// The serialized primary key value of the row.
  final String rowId;

  /// The column name modified, or `'*'` for whole-row tombstone deletions.
  final String columnName;

  /// The new column value (primitive, string, or JSON-serializable).
  final Object? value;

  /// The Hybrid Logical Clock associated with this column mutation.
  final Hlc hlc;

  /// Whether this delta signifies a row or column tombstone deletion.
  final bool isDeleted;

  /// Creates a [ColumnDelta].
  const ColumnDelta({
    required this.tableName,
    required this.rowId,
    required this.columnName,
    required this.value,
    required this.hlc,
    this.isDeleted = false,
  });

  /// Serializes this delta to a JSON map.
  Map<String, Object?> toJson() => {
    'table_name': tableName,
    'row_id': rowId,
    'column_name': columnName,
    'value': value,
    'hlc': hlc.toString(),
    'is_deleted': isDeleted,
  };

  /// Deserializes a [ColumnDelta] from a JSON map.
  factory ColumnDelta.fromJson(Map<String, Object?> json) {
    return ColumnDelta(
      tableName: json['table_name'] as String,
      rowId: json['row_id'].toString(),
      columnName: json['column_name'] as String,
      value: json['value'],
      hlc: Hlc.parse(json['hlc'] as String),
      isDeleted: json['is_deleted'] == true || json['is_deleted'] == 1,
    );
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ColumnDelta &&
          tableName == other.tableName &&
          rowId == other.rowId &&
          columnName == other.columnName &&
          value == other.value &&
          hlc == other.hlc &&
          isDeleted == other.isDeleted;

  @override
  int get hashCode =>
      Object.hash(tableName, rowId, columnName, value, hlc, isDeleted);

  @override
  String toString() =>
      'ColumnDelta($tableName.$columnName @ $rowId = $value [HLC: $hlc, del: $isDeleted])';
}

/// A row-level aggregation of column deltas or row tombstone.
@immutable
class RowDelta {
  /// The user table name.
  final String tableName;

  /// The primary key identifier of the row.
  final String rowId;

  /// Column values indexed by column name.
  final Map<String, Object?> columns;

  /// The latest HLC across all modified columns in this row, or tombstone HLC.
  final Hlc hlc;

  /// Whether this row is marked as deleted (tombstone).
  final bool isDeleted;

  /// Creates a [RowDelta].
  const RowDelta({
    required this.tableName,
    required this.rowId,
    required this.columns,
    required this.hlc,
    this.isDeleted = false,
  });

  /// Converts this [RowDelta] into a list of discrete [ColumnDelta] items.
  List<ColumnDelta> toColumnDeltas() {
    if (isDeleted) {
      return [
        ColumnDelta(
          tableName: tableName,
          rowId: rowId,
          columnName: '*',
          value: null,
          hlc: hlc,
          isDeleted: true,
        ),
      ];
    }

    return columns.entries.map((entry) {
      return ColumnDelta(
        tableName: tableName,
        rowId: rowId,
        columnName: entry.key,
        value: entry.value,
        hlc: hlc,
        isDeleted: false,
      );
    }).toList();
  }

  /// Groups a list of discrete [ColumnDelta]s into row-level [RowDelta]s.
  static List<RowDelta> fromColumnDeltas(List<ColumnDelta> deltas) {
    final Map<String, List<ColumnDelta>> groups = {};
    for (final delta in deltas) {
      final key = '${delta.tableName}:::${delta.rowId}';
      groups.putIfAbsent(key, () => []).add(delta);
    }

    final results = <RowDelta>[];
    for (final entry in groups.entries) {
      final list = entry.value;
      final first = list.first;

      final isDeleted = list.any((d) => d.isDeleted && d.columnName == '*');
      Hlc maxHlc = first.hlc;
      final columns = <String, Object?>{};

      for (final d in list) {
        if (d.hlc > maxHlc) {
          maxHlc = d.hlc;
        }
        if (!d.isDeleted && d.columnName != '*') {
          columns[d.columnName] = d.value;
        }
      }

      results.add(
        RowDelta(
          tableName: first.tableName,
          rowId: first.rowId,
          columns: columns,
          hlc: maxHlc,
          isDeleted: isDeleted,
        ),
      );
    }

    return results;
  }

  /// Serializes this [RowDelta] to a JSON-compatible map.
  Map<String, Object?> toJson() => {
    'table_name': tableName,
    'row_id': rowId,
    'columns': columns,
    'hlc': hlc.toString(),
    'is_deleted': isDeleted,
  };

  /// Deserializes a [RowDelta] from a JSON map.
  factory RowDelta.fromJson(Map<String, Object?> json) {
    return RowDelta(
      tableName: json['table_name'] as String,
      rowId: json['row_id'].toString(),
      columns: Map<String, Object?>.from(json['columns'] as Map? ?? {}),
      hlc: Hlc.parse(json['hlc'] as String),
      isDeleted: json['is_deleted'] == true || json['is_deleted'] == 1,
    );
  }

  @override
  String toString() =>
      'RowDelta($tableName @ $rowId: ${columns.keys.join(",")} [HLC: $hlc, del: $isDeleted])';
}

/// A transport-level batch payload containing multiple deltas and pagination metadata.
@immutable
class SyncBatch {
  /// The originating node identifier for this batch.
  final String nodeId;

  /// The list of discrete column mutations or tombstones in this batch.
  final List<ColumnDelta> deltas;

  /// The cursor or watermark marking the end of this batch.
  final Hlc? cursor;

  /// Whether additional pages remain available on the remote server.
  final bool hasMore;

  /// Creates a [SyncBatch].
  const SyncBatch({
    required this.nodeId,
    required this.deltas,
    this.cursor,
    this.hasMore = false,
  });

  /// Convenience getter returning deltas grouped by row.
  List<RowDelta> get rows => RowDelta.fromColumnDeltas(deltas);

  /// Serializes this batch to a JSON map.
  Map<String, Object?> toJson() => {
    'node_id': nodeId,
    'deltas': deltas.map((d) => d.toJson()).toList(),
    'cursor': cursor?.toString(),
    'has_more': hasMore,
  };

  /// Deserializes a [SyncBatch] from a JSON map.
  factory SyncBatch.fromJson(Map<String, Object?> json) {
    final rawDeltas = json['deltas'] as List<dynamic>? ?? [];
    final deltas = rawDeltas
        .map((d) => ColumnDelta.fromJson(Map<String, Object?>.from(d as Map)))
        .toList();

    return SyncBatch(
      nodeId: json['node_id'] as String,
      deltas: deltas,
      cursor: Hlc.tryParse(json['cursor'] as String?),
      hasMore: json['has_more'] == true,
    );
  }

  @override
  String toString() =>
      'SyncBatch(nodeId: $nodeId, deltas: ${deltas.length}, cursor: $cursor, hasMore: $hasMore)';
}

/// The winning party in a conflict resolution.
enum ConflictResolution {
  /// The incoming remote value superseded the local value.
  remoteWins,

  /// The current local value superseded the incoming remote value.
  localWins,
}

/// Details of a detected concurrent column-level conflict.
@immutable
class SyncConflict {
  /// The affected table name.
  final String tableName;

  /// The primary key identifier of the row.
  final String rowId;

  /// The column name that conflicted.
  final String columnName;

  /// The local column value before merge.
  final Object? localValue;

  /// The local column Hybrid Logical Clock before merge.
  final Hlc? localHlc;

  /// The incoming remote column value.
  final Object? remoteValue;

  /// The incoming remote Hybrid Logical Clock.
  final Hlc remoteHlc;

  /// The deterministic resolution chosen according to Last-Write-Wins.
  final ConflictResolution resolution;

  /// Creates a [SyncConflict] record.
  const SyncConflict({
    required this.tableName,
    required this.rowId,
    required this.columnName,
    required this.localValue,
    required this.localHlc,
    required this.remoteValue,
    required this.remoteHlc,
    required this.resolution,
  });

  @override
  String toString() =>
      'SyncConflict($tableName.$columnName @ $rowId: local=$localValue ($localHlc) vs remote=$remoteValue ($remoteHlc) => $resolution)';
}

/// The outcome of pushing a batch of local deltas to the remote sync transport.
@immutable
class PushResult {
  /// Whether the push operation was accepted by the server.
  final bool success;

  /// The total number of column deltas successfully merged and persisted.
  final int acceptedCount;

  /// The updated server-side HLC watermark after applying the batch.
  final Hlc? serverHlc;

  /// An optional error message if [success] is `false`.
  final String? errorMessage;

  /// Creates a [PushResult].
  const PushResult({
    required this.success,
    this.acceptedCount = 0,
    this.serverHlc,
    this.errorMessage,
  });

  /// Factory creating a successful [PushResult].
  factory PushResult.ok({required int acceptedCount, Hlc? serverHlc}) =>
      PushResult(
        success: true,
        acceptedCount: acceptedCount,
        serverHlc: serverHlc,
      );

  /// Factory creating a failed [PushResult].
  factory PushResult.error(String message) =>
      PushResult(success: false, errorMessage: message);

  /// Serializes to a JSON map.
  Map<String, Object?> toJson() => {
    'success': success,
    'accepted_count': acceptedCount,
    'server_hlc': serverHlc?.toString(),
    'error_message': errorMessage,
  };

  /// Deserializes from a JSON map.
  factory PushResult.fromJson(Map<String, Object?> json) => PushResult(
    success: json['success'] == true,
    acceptedCount: (json['accepted_count'] as num?)?.toInt() ?? 0,
    serverHlc: Hlc.tryParse(json['server_hlc'] as String?),
    errorMessage: json['error_message'] as String?,
  );

  @override
  String toString() =>
      'PushResult(success: $success, accepted: $acceptedCount, serverHlc: $serverHlc, error: $errorMessage)';
}
