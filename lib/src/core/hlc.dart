import 'dart:math' as math;
import 'package:meta/meta.dart';

/// Exception thrown when a received remote clock timestamp exceeds the permitted
/// physical clock drift threshold.
class ClockDriftException implements Exception {
  /// The detected drift duration in milliseconds.
  final int driftMillis;

  /// The maximum allowed drift threshold in milliseconds.
  final int maxDriftMillis;

  /// The offending remote [Hlc] instance.
  final Hlc remoteHlc;

  /// Creates a [ClockDriftException] with drift details.
  const ClockDriftException({
    required this.driftMillis,
    required this.maxDriftMillis,
    required this.remoteHlc,
  });

  @override
  String toString() =>
      'ClockDriftException: Remote timestamp ($remoteHlc) drifted ahead by '
      '$driftMillis ms, which exceeds the allowed threshold of $maxDriftMillis ms.';
}

/// A lexicographically sortable Hybrid Logical Clock (HLC) for distributed systems.
///
/// HLC provides causality tracking and total ordering across distributed nodes
/// without requiring synchronized physical clocks. Each HLC consists of:
/// * [millis]: UTC physical timestamp in milliseconds since epoch.
/// * [counter]: Monotonically increasing logical counter for ties or sub-millisecond events.
/// * [nodeId]: Unique identifier of the node generating the clock.
@immutable
class Hlc implements Comparable<Hlc> {
  /// The UTC physical timestamp in milliseconds since epoch.
  final int millis;

  /// The logical counter within the millisecond window.
  final int counter;

  /// The unique identifier of the node that generated this clock.
  final String nodeId;

  /// Creates an immutable [Hlc] instance.
  const Hlc({
    required this.millis,
    required this.counter,
    required this.nodeId,
  })  : assert(millis >= 0, 'millis must be non-negative'),
        assert(counter >= 0, 'counter must be non-negative'),
        assert(nodeId.length > 0, 'nodeId must not be empty');

  /// Creates an [Hlc] initialized to the current wall-clock time.
  ///
  /// If [wallClockMillis] is provided, it is used instead of the current system time.
  factory Hlc.now(String nodeId, {int? wallClockMillis}) {
    final now = wallClockMillis ?? DateTime.now().toUtc().millisecondsSinceEpoch;
    return Hlc(millis: now, counter: 0, nodeId: nodeId);
  }

  /// Parses a serialized canonical HLC string into an [Hlc] instance.
  ///
  /// Throws a [FormatException] if [serialized] is not a valid HLC string.
  factory Hlc.parse(String serialized) {
    final zIndex = serialized.indexOf('Z');
    if (zIndex == -1 || zIndex < 19) {
      throw FormatException(
        'Invalid HLC format "$serialized". Expected "ISO8601:COUNTER:NODEID".',
        serialized,
      );
    }

    final isoPart = serialized.substring(0, zIndex + 1);
    final rest = serialized.substring(zIndex + 1);
    if (!rest.startsWith(':')) {
      throw FormatException('Expected ":" delimiter following ISO timestamp', serialized);
    }

    final subParts = rest.substring(1).split(':');
    if (subParts.length < 2) {
      throw FormatException('Expected "COUNTER:NODEID" after timestamp', serialized);
    }

    final counterPart = subParts[0];
    final nodePart = subParts.sublist(1).join(':');

    final dt = DateTime.tryParse(isoPart);
    if (dt == null) {
      throw FormatException('Invalid ISO8601 timestamp in HLC: "$isoPart"', serialized);
    }

    final counter = int.tryParse(counterPart, radix: 16);
    if (counter == null || counter < 0) {
      throw FormatException('Invalid counter in HLC: "$counterPart"', serialized);
    }

    if (nodePart.isEmpty) {
      throw FormatException('Node ID must not be empty in HLC', serialized);
    }

    return Hlc(
      millis: dt.millisecondsSinceEpoch,
      counter: counter,
      nodeId: nodePart,
    );
  }

  /// Parses a serialized HLC string, or returns `null` if the string is invalid.
  static Hlc? tryParse(String? serialized) {
    if (serialized == null || serialized.isEmpty) return null;
    try {
      return Hlc.parse(serialized);
    } on FormatException {
      return null;
    }
  }

  /// Creates a JSON-compatible string representation.
  String toJson() => toString();

  /// Deserializes an [Hlc] from a JSON string.
  factory Hlc.fromJson(String json) => Hlc.parse(json);

  /// Generates the next monotonically increasing clock for a local mutation.
  ///
  /// If the physical wall clock [wallClockMillis] has advanced beyond [millis],
  /// the new clock resets the [counter] to `0` and adopts the new physical timestamp.
  /// Otherwise, it increments [counter] while retaining [millis].
  Hlc increment({int? wallClockMillis}) {
    final physical = wallClockMillis ?? DateTime.now().toUtc().millisecondsSinceEpoch;
    if (physical > millis) {
      return Hlc(millis: physical, counter: 0, nodeId: nodeId);
    } else {
      return Hlc(millis: millis, counter: counter + 1, nodeId: nodeId);
    }
  }

  /// Updates the local clock state upon receiving a [remote] HLC.
  ///
  /// Verifies that the remote clock has not drifted past [maxDriftMillis]
  /// (default 60 seconds). Throws a [ClockDriftException] if forward clock drift
  /// exceeds this threshold.
  Hlc receive(
    Hlc remote, {
    int? wallClockMillis,
    int maxDriftMillis = 60000,
  }) {
    final physical = wallClockMillis ?? DateTime.now().toUtc().millisecondsSinceEpoch;
    final drift = remote.millis - physical;
    if (drift > maxDriftMillis) {
      throw ClockDriftException(
        driftMillis: drift,
        maxDriftMillis: maxDriftMillis,
        remoteHlc: remote,
      );
    }

    final newMillis = math.max(math.max(millis, remote.millis), physical);
    int newCounter;
    if (newMillis == millis && newMillis == remote.millis) {
      newCounter = math.max(counter, remote.counter) + 1;
    } else if (newMillis == millis) {
      newCounter = counter + 1;
    } else if (newMillis == remote.millis) {
      newCounter = remote.counter + 1;
    } else {
      newCounter = 0;
    }

    return Hlc(millis: newMillis, counter: newCounter, nodeId: nodeId);
  }

  @override
  int compareTo(Hlc other) {
    if (millis != other.millis) {
      return millis.compareTo(other.millis);
    }
    if (counter != other.counter) {
      return counter.compareTo(other.counter);
    }
    return nodeId.compareTo(other.nodeId);
  }

  /// Whether this clock is strictly after [other].
  bool operator >(Hlc other) => compareTo(other) > 0;

  /// Whether this clock is after or equal to [other].
  bool operator >=(Hlc other) => compareTo(other) >= 0;

  /// Whether this clock is strictly before [other].
  bool operator <(Hlc other) => compareTo(other) < 0;

  /// Whether this clock is before or equal to [other].
  bool operator <=(Hlc other) => compareTo(other) <= 0;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Hlc &&
          millis == other.millis &&
          counter == other.counter &&
          nodeId == other.nodeId;

  @override
  int get hashCode => Object.hash(millis, counter, nodeId);

  /// Formats a millisecond timestamp as a strict 24-character UTC ISO 8601 string.
  static String formatIso(int millis) {
    final dt = DateTime.fromMillisecondsSinceEpoch(millis, isUtc: true);
    final y = dt.year.toString().padLeft(4, '0');
    final m = dt.month.toString().padLeft(2, '0');
    final d = dt.day.toString().padLeft(2, '0');
    final h = dt.hour.toString().padLeft(2, '0');
    final min = dt.minute.toString().padLeft(2, '0');
    final s = dt.second.toString().padLeft(2, '0');
    final ms = dt.millisecond.toString().padLeft(3, '0');
    return '$y-$m-${d}T$h:$min:$s.${ms}Z';
  }

  @override
  String toString() {
    final iso = formatIso(millis);
    final counterHex = counter.toRadixString(16).padLeft(4, '0').toLowerCase();
    return '$iso:$counterHex:$nodeId';
  }
}
