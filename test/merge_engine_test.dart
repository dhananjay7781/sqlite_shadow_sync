import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite_shadow_sync/sqlite_shadow_sync.dart';
import 'package:test/test.dart';

void main() {
  group('MergeEngine (Column-Level LWW CRDT)', () {
    late Sqlite3Executor dbA;
    late MergeEngine mergeEngineA;

    setUp(() async {
      final sqliteDb = sqlite3.openInMemory();
      dbA = Sqlite3Executor(sqliteDb);

      await ShadowTables.createSchema(dbA);
      await ShadowTables.initNodeState(dbA, 'node-a');

      await dbA.execute('''
CREATE TABLE users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  role TEXT DEFAULT 'member'
);
''');
      await TriggerGenerator.attachTriggers(dbA, ['users']);

      mergeEngineA = MergeEngine(dbA);
    });

    tearDown(() async {
      await dbA.close();
    });

    test('two offline nodes editing different columns of the same row merge cleanly without data loss', () async {
      // 1. Initial shared state at T0
      final t0 = Hlc.now('genesis', wallClockMillis: 1700000000000);
      await dbA.execute('''
INSERT INTO users (id, name, email, role)
VALUES ('u1', 'Original Name', 'original@example.com', 'member');
''');
      // Set initial column clocks
      for (final col in ['name', 'email', 'role']) {
        await dbA.execute(
          'UPDATE _sync_column_clock SET hlc = ? WHERE table_name = ? AND row_id = ? AND column_name = ?;',
          [t0.toString(), 'users', 'u1', col],
        );
      }
      // Clear outbox from initial setup
      final setupDeltas = await ShadowTables.getOutboxDeltas(dbA);
      await ShadowTables.clearOutboxDeltas(dbA, setupDeltas);

      // 2. Node A makes an offline edit to 'name' at T1_A
      final t1A = t0.increment(wallClockMillis: 1700000010000);
      await dbA.execute("UPDATE users SET name = 'Alice In Wonderland' WHERE id = 'u1';");
      await dbA.execute(
        'UPDATE _sync_column_clock SET hlc = ? WHERE table_name = ? AND row_id = ? AND column_name = ?;',
        [t1A.toString(), 'users', 'u1', 'name'],
      );

      // Verify Node A's local state before merge
      final rowsBefore = await dbA.query("SELECT * FROM users WHERE id = 'u1';");
      expect(rowsBefore.first['name'], equals('Alice In Wonderland'));
      expect(rowsBefore.first['email'], equals('original@example.com'));

      // 3. Node B concurrently edited 'email' offline at T2_B
      const t2B = Hlc(millis: 1700000020000, counter: 0, nodeId: 'node-b');
      const deltaFromB = ColumnDelta(
        tableName: 'users',
        rowId: 'u1',
        columnName: 'email',
        value: 'alice.wonderland@newdomain.com',
        hlc: t2B,
      );

      // 4. Merge Node B's delta into Node A
      final result = await mergeEngineA.mergeDeltas([deltaFromB]);

      expect(result.appliedCount, equals(1));
      expect(result.conflicts, isEmpty); // No column conflict because columns were disjoint!

      // 5. Verify the converged row on Node A:
      // Both Node A's 'name' and Node B's 'email' are present!
      final rowsAfter = await dbA.query("SELECT * FROM users WHERE id = 'u1';");
      expect(rowsAfter.length, equals(1));
      final user = rowsAfter.first;

      expect(user['name'], equals('Alice In Wonderland')); // Node A edit preserved!
      expect(user['email'], equals('alice.wonderland@newdomain.com')); // Node B edit merged!
      expect(user['role'], equals('member')); // Untouched column preserved!

      // Verify trigger suppression: incoming merge did NOT create new outbox entries
      final outboxDeltas = await ShadowTables.getOutboxDeltas(dbA);
      // Outbox should only contain Node A's local edit for 'name', NOT Node B's 'email'
      expect(outboxDeltas.any((d) => d.columnName == 'email'), isFalse);
    });

    test('same column conflict resolves deterministically via LWW (newer HLC wins)', () async {
      const t0 = Hlc(millis: 1700000000000, counter: 0, nodeId: 'node-a');
      await dbA.execute('''
INSERT INTO users (id, name, email) VALUES ('u1', 'Alice Local', 'alice@example.com');
''');
      await dbA.execute(
        'UPDATE _sync_column_clock SET hlc = ? WHERE table_name = ? AND row_id = ? AND column_name = ?;',
        [t0.toString(), 'users', 'u1', 'name'],
      );

      // Case 1: Remote HLC is newer -> Remote wins
      const tNewer = Hlc(millis: 1700000050000, counter: 0, nodeId: 'node-b');
      const remoteDeltaNewer = ColumnDelta(
        tableName: 'users',
        rowId: 'u1',
        columnName: 'name',
        value: 'Alice Remote Wins',
        hlc: tNewer,
      );

      final result1 = await mergeEngineA.mergeDeltas([remoteDeltaNewer]);
      expect(result1.appliedCount, equals(1));
      expect(result1.conflicts.length, equals(1));
      expect(result1.conflicts.first.resolution, equals(ConflictResolution.remoteWins));

      var row = (await dbA.query("SELECT name FROM users WHERE id = 'u1';")).first;
      expect(row['name'], equals('Alice Remote Wins'));

      // Case 2: Older remote delta arrives -> Local wins
      const tOlder = Hlc(millis: 1700000010000, counter: 0, nodeId: 'node-c');
      const remoteDeltaOlder = ColumnDelta(
        tableName: 'users',
        rowId: 'u1',
        columnName: 'name',
        value: 'Alice Stale Lost',
        hlc: tOlder,
      );

      final result2 = await mergeEngineA.mergeDeltas([remoteDeltaOlder]);
      expect(result2.appliedCount, equals(0));
      expect(result2.conflicts.length, equals(1));
      expect(result2.conflicts.first.resolution, equals(ConflictResolution.localWins));

      row = (await dbA.query("SELECT name FROM users WHERE id = 'u1';")).first;
      expect(row['name'], equals('Alice Remote Wins'));
    });

    test('inserts remote row when row does not exist locally', () async {
      const hlc = Hlc(millis: 1700000000000, counter: 0, nodeId: 'node-b');
      final deltas = [
        const ColumnDelta(tableName: 'users', rowId: 'u99', columnName: 'name', value: 'Charlie', hlc: hlc),
        const ColumnDelta(tableName: 'users', rowId: 'u99', columnName: 'email', value: 'charlie@test.com', hlc: hlc),
      ];

      final result = await mergeEngineA.mergeDeltas(deltas);
      expect(result.appliedCount, equals(2));

      final rows = await dbA.query("SELECT * FROM users WHERE id = 'u99';");
      expect(rows.length, equals(1));
      expect(rows.first['name'], equals('Charlie'));
      expect(rows.first['email'], equals('charlie@test.com'));
    });

    test('deletes row when remote deletion tombstone is newer', () async {
      const t0 = Hlc(millis: 1700000000000, counter: 0, nodeId: 'node-a');
      await dbA.execute('''
INSERT INTO users (id, name, email) VALUES ('u1', 'Dave', 'dave@example.com');
''');
      await dbA.execute(
        'UPDATE _sync_column_clock SET hlc = ? WHERE table_name = ? AND row_id = ?;',
        [t0.toString(), 'users', 'u1'],
      );

      const tDel = Hlc(millis: 1700000050000, counter: 0, nodeId: 'node-b');
      const deleteDelta = ColumnDelta(
        tableName: 'users',
        rowId: 'u1',
        columnName: '*',
        value: null,
        hlc: tDel,
        isDeleted: true,
      );

      final result = await mergeEngineA.mergeDeltas([deleteDelta]);
      expect(result.appliedCount, equals(1));

      final rows = await dbA.query("SELECT * FROM users WHERE id = 'u1';");
      expect(rows, isEmpty);

      final tombstones = await dbA.query(
        'SELECT * FROM _sync_tombstones WHERE table_name = ? AND row_id = ?;',
        ['users', 'u1'],
      );
      expect(tombstones.length, equals(1));
    });
  });
}
