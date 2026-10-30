import 'dart:async';
import 'dart:math' as math;
import '../core/hlc.dart';
import '../core/sql_executor.dart';
import '../core/sync_models.dart';
import '../core/sync_transport.dart';
import '../schema/shadow_tables.dart';
import 'merge_engine.dart';

/// Orchestrates full push/pull sync cycles, batch chunking, and exponential backoff retry.
class SyncOrchestrator {
  /// The local database executor.
  final SyncSqlExecutor db;

  /// The transport layer communicating with the remote sync service.
  final SyncTransport transport;

  /// The Last-Write-Wins merge processor.
  final MergeEngine mergeEngine;

  /// Maximum number of outbox deltas pushed in a single batch chunk.
  final int pushBatchSize;

  /// Maximum number of remote deltas pulled in a single batch chunk.
  final int pullBatchSize;

  /// Initial retry delay applied before the first retry attempt.
  final Duration initialRetryDelay;

  /// Maximum upper ceiling for exponential backoff delay.
  final Duration maxRetryDelay;

  /// Maximum consecutive retry attempts allowed before aborting.
  final int maxRetries;

  /// Exponential backoff multiplier.
  final double backoffMultiplier;

  /// Random jitter factor between 0.0 and 1.0 (e.g. 0.25 = ±25% jitter).
  final double jitterFactor;

  final StreamController<SyncStatus> _statusController =
      StreamController<SyncStatus>.broadcast();
  final math.Random _random;

  SyncStatus _currentStatus = SyncIdle();
  Completer<void>? _activeSyncCompleter;
  Timer? _periodicTimer;
  Hlc? _inMemoryClock;

  /// Creates a [SyncOrchestrator].
  SyncOrchestrator({
    required this.db,
    required this.transport,
    MergeEngine? mergeEngine,
    this.pushBatchSize = 100,
    this.pullBatchSize = 500,
    this.initialRetryDelay = const Duration(milliseconds: 500),
    this.maxRetryDelay = const Duration(seconds: 30),
    this.maxRetries = 5,
    this.backoffMultiplier = 2.0,
    this.jitterFactor = 0.25,
    math.Random? random,
  })  : mergeEngine = mergeEngine ?? MergeEngine(db),
        _random = random ?? math.Random();

  /// Stream of state changes emitted as the sync state machine progresses.
  Stream<SyncStatus> get statusStream => _statusController.stream;

  /// The current state machine status.
  SyncStatus get currentStatus => _currentStatus;

  /// Whether a synchronization cycle is currently executing.
  bool get isSyncing => _activeSyncCompleter != null;

  void _setStatus(SyncStatus status) {
    _currentStatus = status;
    if (!_statusController.isClosed) {
      _statusController.add(status);
    }
  }

  /// Calculates the backoff delay for retry [attempt] incorporating exponential scaling and jitter.
  Duration computeBackoffDelay(int attempt) {
    final expMillis =
        (initialRetryDelay.inMilliseconds * math.pow(backoffMultiplier, attempt)).round();
    final cappedMillis = math.min(expMillis, maxRetryDelay.inMilliseconds);
    final jitterDelta =
        (cappedMillis * jitterFactor * (_random.nextDouble() * 2 - 1)).round();
    final finalMillis = math.max(0, cappedMillis + jitterDelta);
    return Duration(milliseconds: finalMillis);
  }

  /// Executes a single complete synchronization cycle with automatic retry and backoff.
  ///
  /// If transient network failures occur, retries up to [maxRetries] times with
  /// jittered exponential backoff before throwing the underlying error.
  Future<SyncStats> synchronizeWithRetry({int? maxRetriesOverride}) async {
    final retries = maxRetriesOverride ?? maxRetries;
    int attempt = 0;

    while (true) {
      try {
        return await synchronize();
      } catch (error, stackTrace) {
        if (attempt >= retries) {
          _setStatus(SyncError(
            error: error,
            stackTrace: stackTrace,
            attempt: attempt,
          ));
          rethrow;
        }

        final delay = computeBackoffDelay(attempt);
        _setStatus(SyncError(
          error: error,
          stackTrace: stackTrace,
          nextRetryDelay: delay,
          attempt: attempt,
        ));

        await Future<void>.delayed(delay);
        attempt++;
      }
    }
  }

  /// Performs a single push/pull synchronization cycle.
  ///
  /// 1. Pushes pending outbox mutations chunked by [pushBatchSize].
  /// 2. Pulls remote updates strictly after the local watermark up to [pullBatchSize].
  /// 3. Merges remote deltas under trigger suppression.
  Future<SyncStats> synchronize() async {
    // If a sync is already running, wait for it rather than overlapping cycles.
    if (_activeSyncCompleter != null) {
      await _activeSyncCompleter!.future;
      return const SyncStats(
        deltasPushed: 0,
        deltasPulled: 0,
        conflictsResolved: 0,
        duration: Duration.zero,
      );
    }

    final completer = Completer<void>();
    _activeSyncCompleter = completer;

    final stopwatch = Stopwatch()..start();
    int totalPushed = 0;
    int totalPulled = 0;
    int totalConflicts = 0;

    try {
      final nodeId = await ShadowTables.getNodeId(db);
      _inMemoryClock ??= Hlc.now(nodeId);

      // ==========================================
      // PHASE 1: PUSH LOCAL OUTBOX DELTAS (CHUNKED)
      // ==========================================
      while (true) {
        final pendingDeltas = await ShadowTables.getOutboxDeltas(
          db,
          limit: pushBatchSize,
        );
        if (pendingDeltas.isEmpty) break;

        _setStatus(SyncPushing(batchSize: pendingDeltas.length));

        // Ensure outgoing deltas are stamped with strictly monotonic HLC
        final stampedDeltas = <ColumnDelta>[];
        for (final delta in pendingDeltas) {
          if (delta.hlc > _inMemoryClock!) {
            _inMemoryClock = delta.hlc;
          } else {
            _inMemoryClock = _inMemoryClock!.increment();
          }

          final stamped = ColumnDelta(
            tableName: delta.tableName,
            rowId: delta.rowId,
            columnName: delta.columnName,
            value: delta.value,
            hlc: _inMemoryClock!,
            isDeleted: delta.isDeleted,
          );
          stampedDeltas.add(stamped);

          if (!delta.isDeleted && delta.columnName != '*') {
            await db.execute(
              'UPDATE ${ShadowTables.tableColumnClock} SET hlc = ? WHERE table_name = ? AND row_id = ? AND column_name = ?;',
              [_inMemoryClock!.toString(), delta.tableName, delta.rowId, delta.columnName],
            );
          }
        }

        final batch = SyncBatch(
          nodeId: nodeId,
          deltas: stampedDeltas,
          cursor: _inMemoryClock,
        );

        final pushResult = await transport.push(batch);
        if (!pushResult.success) {
          throw StateError(
            pushResult.errorMessage ?? 'Remote transport rejected push batch.',
          );
        }

        if (pushResult.serverHlc != null) {
          _inMemoryClock = _inMemoryClock!.receive(pushResult.serverHlc!);
        }

        // Remove only the exact acknowledged items from the outbox
        await ShadowTables.clearOutboxDeltas(db, pendingDeltas);
        totalPushed += pendingDeltas.length;

        if (pendingDeltas.length < pushBatchSize) break;
      }

      // ==========================================
      // PHASE 2: PULL REMOTE DELTAS (PAGINATED)
      // ==========================================
      while (true) {
        final watermark = await ShadowTables.getLastPulledHlc(db);
        _setStatus(SyncPulling(sinceHlc: watermark));

        final remoteBatch = await transport.pull(
          sinceHlc: watermark,
          limit: pullBatchSize,
        );

        if (remoteBatch.deltas.isNotEmpty) {
          // Advance local clock with all remote timestamps
          for (final d in remoteBatch.deltas) {
            _inMemoryClock = _inMemoryClock!.receive(d.hlc);
          }

          // Filter out mutations originating from this local node
          final foreignDeltas =
              remoteBatch.deltas.where((d) => d.hlc.nodeId != nodeId).toList();

          if (foreignDeltas.isNotEmpty) {
            final mergeOutcome = await mergeEngine.mergeDeltas(foreignDeltas);
            totalPulled += mergeOutcome.appliedCount;
            totalConflicts += mergeOutcome.conflicts.length;
          }

          // Advance watermark to remote batch cursor or newest delta
          final newWatermark = remoteBatch.cursor ?? remoteBatch.deltas.last.hlc;
          final currentWatermark = await ShadowTables.getLastPulledHlc(db);
          if (currentWatermark == null || newWatermark > currentWatermark) {
            await ShadowTables.setLastPulledHlc(db, newWatermark);
          }
        }

        if (!remoteBatch.hasMore || remoteBatch.deltas.isEmpty) {
          break;
        }
      }

      final stats = SyncStats(
        deltasPushed: totalPushed,
        deltasPulled: totalPulled,
        conflictsResolved: totalConflicts,
        duration: stopwatch.elapsed,
      );

      _setStatus(SyncSuccess(stats: stats));
      _setStatus(SyncIdle());
      return stats;
    } catch (e, st) {
      _setStatus(SyncError(error: e, stackTrace: st));
      rethrow;
    } finally {
      _activeSyncCompleter = null;
      completer.complete();
    }
  }

  /// Starts a recurring periodic sync schedule every [interval].
  void startPeriodicSync(Duration interval) {
    stopPeriodicSync();
    _periodicTimer = Timer.periodic(interval, (_) {
      unawaited(synchronizeWithRetry());
    });
  }

  /// Cancels any active periodic sync schedule.
  void stopPeriodicSync() {
    _periodicTimer?.cancel();
    _periodicTimer = null;
  }

  /// Disposes background timers and closes the status stream.
  Future<void> dispose() async {
    stopPeriodicSync();
    await _statusController.close();
  }
}
