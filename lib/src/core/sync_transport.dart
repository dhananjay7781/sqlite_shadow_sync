import 'dart:async';
import 'dart:math' as math;
import 'hlc.dart';
import 'sync_models.dart';

/// Contract defining the network transport layer for pushing and pulling sync deltas.
///
/// Implementations may bridge over HTTP REST, WebSockets, gRPC, or an in-memory bus.
abstract interface class SyncTransport {
  /// Pushes a local [batch] of deltas to the remote synchronization backend.
  ///
  /// Returns a [PushResult] detailing acceptance, number of deltas persisted,
  /// and the updated server watermark HLC.
  Future<PushResult> push(SyncBatch batch);

  /// Pulls remote deltas that occurred strictly after [sinceHlc].
  ///
  /// If [sinceHlc] is `null`, pulls historical deltas from genesis up to [limit].
  Future<SyncBatch> pull({Hlc? sinceHlc, int limit = 500});
}

class _LoggedChange {
  final ColumnDelta delta;
  final Hlc serverHlc;
  const _LoggedChange(this.delta, this.serverHlc);
}

/// An in-memory implementation of [SyncTransport] representing a centralized sync server.
///
/// Ideal for unit tests, local multi-client simulation, and offline/fault injection.
class InMemorySyncTransport implements SyncTransport {
  /// The node identifier representing the central server.
  final String serverNodeId;

  /// Artificial network latency injected into calls.
  Duration latency;

  /// When `true`, simulating transport failures by throwing [StateError].
  bool isOffline;

  final Map<String, ColumnDelta> _serverState = {};
  final List<_LoggedChange> _changeLog = [];
  Hlc _serverClock;

  /// Creates an [InMemorySyncTransport].
  InMemorySyncTransport({
    this.serverNodeId = 'server',
    this.latency = Duration.zero,
    this.isOffline = false,
  }) : _serverClock = Hlc.now(serverNodeId);

  /// Current clock watermark of the central server.
  Hlc get serverClock => _serverClock;

  /// Total number of distinct change-log entries on the server.
  int get totalDeltas => _changeLog.length;

  /// Clears all server storage and resets the clock.
  void reset() {
    _serverState.clear();
    _changeLog.clear();
    _serverClock = Hlc.now(serverNodeId);
  }

  @override
  Future<PushResult> push(SyncBatch batch) async {
    if (latency > Duration.zero) {
      await Future<void>.delayed(latency);
    }
    if (isOffline) {
      throw StateError(
        'InMemorySyncTransport: Network connection unreachable.',
      );
    }

    int accepted = 0;
    for (final incoming in batch.deltas) {
      // Advance server clock for total server ordering
      _serverClock = _serverClock.receive(incoming.hlc);

      final key =
          '${incoming.tableName}:::${incoming.rowId}:::${incoming.columnName}';
      final existing = _serverState[key];

      if (existing == null || incoming.hlc > existing.hlc) {
        _serverState[key] = incoming;
        _changeLog.add(_LoggedChange(incoming, _serverClock));
        accepted++;
      }
    }

    return PushResult.ok(acceptedCount: accepted, serverHlc: _serverClock);
  }

  @override
  Future<SyncBatch> pull({Hlc? sinceHlc, int limit = 500}) async {
    if (latency > Duration.zero) {
      await Future<void>.delayed(latency);
    }
    if (isOffline) {
      throw StateError(
        'InMemorySyncTransport: Network connection unreachable.',
      );
    }

    final filtered = _changeLog.where((logged) {
      if (sinceHlc == null) return true;
      return logged.serverHlc > sinceHlc;
    }).toList();

    // Sort strictly in ascending server HLC order for causal delivery
    filtered.sort((a, b) => a.serverHlc.compareTo(b.serverHlc));

    final count = math.min(limit, filtered.length);
    final page = filtered.sublist(0, count);
    final hasMore = filtered.length > count;
    final cursor = page.isNotEmpty ? page.last.serverHlc : sinceHlc;

    return SyncBatch(
      nodeId: serverNodeId,
      deltas: page.map((l) => l.delta).toList(),
      cursor: cursor,
      hasMore: hasMore,
    );
  }
}
