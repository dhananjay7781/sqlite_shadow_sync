import 'dart:async';
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite_shadow_sync/sqlite_shadow_sync.dart';
import 'package:test/test.dart';

void main() {
  group('SyncOrchestrator End-to-End', () {
    late InMemorySyncTransport transport;

    setUp(() {
      transport = InMemorySyncTransport(serverNodeId: 'central-cloud-server');
    });

    test('3-node convergence: concurrent offline edits to different columns converge to identical state', () async {
      // Setup 3 nodes with isolated in-memory SQLite databases
      final db1 = Sqlite3Executor(sqlite3.openInMemory());
      final db2 = Sqlite3Executor(sqlite3.openInMemory());
      final db3 = Sqlite3Executor(sqlite3.openInMemory());

      addTearDown(() async {
        await db1.close();
        await db2.close();
        await db3.close();
      });

      const ddl = '''
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  priority INTEGER DEFAULT 1,
  status TEXT NOT NULL,
  assignee TEXT
);
''';

      for (final (db, nodeId) in [
        (db1, 'node-mobile'),
        (db2, 'node-desktop'),
        (db3, 'node-web'),
      ]) {
        await ShadowTables.createSchema(db);
        await ShadowTables.initNodeState(db, nodeId);
        await db.execute(ddl);
        await TriggerGenerator.attachTriggers(db, ['tasks']);
      }

      final sync1 = SyncOrchestrator(db: db1, transport: transport);
      final sync2 = SyncOrchestrator(db: db2, transport: transport);
      final sync3 = SyncOrchestrator(db: db3, transport: transport);

      addTearDown(() async {
        await sync1.dispose();
        await sync2.dispose();
        await sync3.dispose();
      });

      // Step 1: Node 1 creates task 't1' and synchronizes to server
      await db1.execute('''
INSERT INTO tasks (id, title, priority, status, assignee)
VALUES ('t1', 'Release v1.0', 1, 'todo', 'Alice');
''');

      await sync1.synchronize();

      // Step 2: Node 2 and Node 3 pull from server
      await sync2.synchronize();
      await sync3.synchronize();

      // Verify all 3 nodes have identical baseline row
      for (final db in [db1, db2, db3]) {
        final rows = await db.query("SELECT * FROM tasks WHERE id = 't1';");
        expect(rows.length, equals(1));
        expect(rows.first['title'], equals('Release v1.0'));
        expect(rows.first['status'], equals('todo'));
        expect(rows.first['assignee'], equals('Alice'));
      }

      // Step 3: All 3 nodes go OFFLINE and make concurrent edits to DIFFERENT columns of 't1'
      // Node 1 edits 'title'
      await db1.execute("UPDATE tasks SET title = 'Release v1.0-RC1' WHERE id = 't1';");

      // Node 2 edits 'status'
      await db2.execute("UPDATE tasks SET status = 'in_progress' WHERE id = 't1';");

      // Node 3 edits 'assignee'
      await db3.execute("UPDATE tasks SET assignee = 'Bob' WHERE id = 't1';");

      // Step 4: All 3 nodes synchronize their changes to the server
      await sync1.synchronize();
      await sync2.synchronize();
      await sync3.synchronize();

      // Step 5: Full gossip pass so all nodes receive all converged changes
      await sync1.synchronize();
      await sync2.synchronize();
      await sync3.synchronize();

      // Step 6: Verify full convergence across all 3 nodes!
      for (final (db, name) in [
        (db1, 'Node 1 (Mobile)'),
        (db2, 'Node 2 (Desktop)'),
        (db3, 'Node 3 (Web)'),
      ]) {
        final rows = await db.query("SELECT * FROM tasks WHERE id = 't1';");
        expect(rows.length, equals(1), reason: '$name must have task');
        final task = rows.first;

        expect(task['title'], equals('Release v1.0-RC1'),
            reason: '$name should have Node 1 title');
        expect(task['status'], equals('in_progress'),
            reason: '$name should have Node 2 status');
        expect(task['assignee'], equals('Bob'),
            reason: '$name should have Node 3 assignee');
        expect(task['priority'], equals(1), reason: '$name priority intact');
      }
    });

    test('batch chunking pushes outbox deltas in configured batch sizes', () async {
      final db = Sqlite3Executor(sqlite3.openInMemory());
      addTearDown(db.close);

      await ShadowTables.createSchema(db);
      await ShadowTables.initNodeState(db, 'batch-node');
      await db.execute('''
CREATE TABLE items (id TEXT PRIMARY KEY, val TEXT);
''');
      await TriggerGenerator.attachTriggers(db, ['items']);

      // Insert 25 rows -> 50 column deltas (id + val)
      for (int i = 0; i < 25; i++) {
        await db.execute(
          'INSERT INTO items (id, val) VALUES (?, ?);',
          ['item_$i', 'value_$i'],
        );
      }

      final initialCount = await ShadowTables.getOutboxCount(db);
      expect(initialCount, equals(50));

      // Configure orchestrator with a small batch size of 10
      final orchestrator = SyncOrchestrator(
        db: db,
        transport: transport,
        pushBatchSize: 10,
      );
      addTearDown(orchestrator.dispose);

      final stats = await orchestrator.synchronize();
      expect(stats.deltasPushed, equals(50));

      // Outbox must be completely cleared
      final remainingCount = await ShadowTables.getOutboxCount(db);
      expect(remainingCount, equals(0));

      // Server received all deltas
      expect(transport.totalDeltas, equals(50));
    });

    test('retry backoff recovers when network comes back online', () async {
      final db = Sqlite3Executor(sqlite3.openInMemory());
      addTearDown(db.close);

      await ShadowTables.createSchema(db);
      await ShadowTables.initNodeState(db, 'retry-node');
      await db.execute('CREATE TABLE notes (id TEXT PRIMARY KEY, text TEXT);');
      await TriggerGenerator.attachTriggers(db, ['notes']);

      await db.execute("INSERT INTO notes (id, text) VALUES ('n1', 'Test Note');");

      // Set transport to offline
      transport.isOffline = true;

      final orchestrator = SyncOrchestrator(
        db: db,
        transport: transport,
        initialRetryDelay: const Duration(milliseconds: 50),
        maxRetryDelay: const Duration(milliseconds: 200),
        maxRetries: 3,
      );
      addTearDown(orchestrator.dispose);

      final statuses = <SyncStatus>[];
      final sub = orchestrator.statusStream.listen(statuses.add);
      addTearDown(sub.cancel);

      // Restore network after 120ms
      Timer(const Duration(milliseconds: 120), () {
        transport.isOffline = false;
      });

      final stats = await orchestrator.synchronizeWithRetry();
      expect(stats.deltasPushed, greaterThan(0));

      // Allow microtask stream delivery
      await Future<void>.delayed(Duration.zero);

      // Verify status stream observed error and eventual success
      expect(statuses.any((s) => s is SyncError), isTrue);
      expect(statuses.any((s) => s is SyncSuccess), isTrue);
      expect(statuses.last, isA<SyncIdle>());
    });
  });
}
