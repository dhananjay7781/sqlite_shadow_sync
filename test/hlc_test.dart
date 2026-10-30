import 'package:sqlite_shadow_sync/sqlite_shadow_sync.dart';
import 'package:test/test.dart';

void main() {
  group('Hybrid Logical Clock (Hlc)', () {
    test('creates clock with current physical time', () {
      final hlc = Hlc.now('node-1');
      expect(hlc.nodeId, equals('node-1'));
      expect(hlc.counter, equals(0));
      expect(hlc.millis, greaterThan(0));
    });

    group('Monotonicity', () {
      test('strictly increases counter when physical time is frozen', () {
        const frozenTime = 1700000000000;
        final h0 = Hlc.now('node-1', wallClockMillis: frozenTime);
        final h1 = h0.increment(wallClockMillis: frozenTime);
        final h2 = h1.increment(wallClockMillis: frozenTime);

        expect(h0.millis, equals(frozenTime));
        expect(h0.counter, equals(0));

        expect(h1.millis, equals(frozenTime));
        expect(h1.counter, equals(1));

        expect(h2.millis, equals(frozenTime));
        expect(h2.counter, equals(2));

        expect(h1 > h0, isTrue);
        expect(h2 > h1, isTrue);
      });

      test('preserves high-water millis and increments counter if wall-clock regresses', () {
        const futureTime = 1700000005000;
        const pastTime = 1700000001000; // clock drifted backwards

        final h0 = Hlc.now('node-1', wallClockMillis: futureTime);
        final h1 = h0.increment(wallClockMillis: pastTime);

        expect(h1.millis, equals(futureTime));
        expect(h1.counter, equals(1));
        expect(h1 > h0, isTrue);
      });

      test('resets counter to 0 when physical clock moves forward', () {
        const time1 = 1700000000000;
        const time2 = 1700000001000;

        final h0 = Hlc.now('node-1', wallClockMillis: time1);
        final h1 = h0.increment(wallClockMillis: time1);
        expect(h1.counter, equals(1));

        final h2 = h1.increment(wallClockMillis: time2);
        expect(h2.millis, equals(time2));
        expect(h2.counter, equals(0));
        expect(h2 > h1, isTrue);
      });
    });

    group('Clock Drift Guard', () {
      test('accepts remote clock within allowed forward drift', () {
        const localTime = 1700000000000;
        const remoteTime = localTime + 30000; // +30s, under 60s limit

        final local = Hlc.now('local-node', wallClockMillis: localTime);
        final remote = Hlc.now('remote-node', wallClockMillis: remoteTime);

        final updated = local.receive(
          remote,
          wallClockMillis: localTime,
          maxDriftMillis: 60000,
        );

        expect(updated.millis, equals(remoteTime));
        expect(updated.nodeId, equals('local-node'));
        expect(updated > local, isTrue);
      });

      test('throws ClockDriftException when remote clock exceeds drift threshold', () {
        const localTime = 1700000000000;
        const remoteTime = localTime + 90000; // +90s, exceeds 60s limit

        final local = Hlc.now('local-node', wallClockMillis: localTime);
        final remote = Hlc.now('remote-node', wallClockMillis: remoteTime);

        expect(
          () => local.receive(
            remote,
            wallClockMillis: localTime,
            maxDriftMillis: 60000,
          ),
          throwsA(isA<ClockDriftException>().having(
            (e) => e.driftMillis,
            'driftMillis',
            equals(90000),
          )),
        );
      });
    });

    group('Lexicographical String Ordering', () {
      test('string comparison matches Hlc.compareTo ordering identically', () {
        final clocks = [
          const Hlc(millis: 1000, counter: 0, nodeId: 'node-a'),
          const Hlc(millis: 1000, counter: 0, nodeId: 'node-b'),
          const Hlc(millis: 1000, counter: 1, nodeId: 'node-a'),
          const Hlc(millis: 1000, counter: 2, nodeId: 'node-a'),
          const Hlc(millis: 2000, counter: 0, nodeId: 'node-a'),
          const Hlc(millis: 2000, counter: 0, nodeId: 'node-z'),
          const Hlc(millis: 2000, counter: 1, nodeId: 'node-a'),
        ];

        for (int i = 0; i < clocks.length - 1; i++) {
          final c1 = clocks[i];
          final c2 = clocks[i + 1];

          expect(c1.compareTo(c2), lessThan(0), reason: '$c1 should be < $c2');
          expect(
            c1.toString().compareTo(c2.toString()),
            lessThan(0),
            reason: '${c1.toString()} string should be < ${c2.toString()}',
          );
        }
      });

      test('round-trip parsing preserves exact state and equality', () {
        const original = Hlc(
          millis: 1730290000123,
          counter: 42,
          nodeId: 'client-phone-xyz',
        );

        final serialized = original.toString();
        final parsed = Hlc.parse(serialized);

        expect(parsed, equals(original));
        expect(parsed.millis, equals(original.millis));
        expect(parsed.counter, equals(original.counter));
        expect(parsed.nodeId, equals(original.nodeId));
        expect(parsed.hashCode, equals(original.hashCode));
      });

      test('tryParse handles null and invalid formats gracefully', () {
        expect(Hlc.tryParse(null), isNull);
        expect(Hlc.tryParse(''), isNull);
        expect(Hlc.tryParse('not-a-valid-hlc'), isNull);
        expect(Hlc.tryParse('2026-01-01T00:00:00.000Z:invalid:node'), isNull);
      });

      test('parse throws FormatException on malformed input', () {
        expect(() => Hlc.parse('malformed'), throwsFormatException);
        expect(() => Hlc.parse('2026-01-01:bad:node'), throwsFormatException);
      });
    });
  });
}
