import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite_shadow_sync/sqlite_shadow_sync.dart';
import 'package:test/test.dart';

void main() {
  group('TriggerGenerator & Shadow Capture', () {
    late Sqlite3Executor db;

    setUp(() async {
      final sqliteDb = sqlite3.openInMemory();
      db = Sqlite3Executor(sqliteDb);

      // Create shadow tables and register node
      await ShadowTables.createSchema(db);
      await ShadowTables.initNodeState(db, 'test-node-1');

      // Create user table
      await db.execute('''
CREATE TABLE products (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  price REAL NOT NULL,
  quantity INTEGER DEFAULT 0,
  description TEXT
);
''');
    });

    tearDown(() async {
      await db.close();
    });

    test(
      'introspectTable correctly extracts column types and primary keys',
      () async {
        final columns = await TriggerGenerator.introspectTable(db, 'products');

        expect(columns.length, equals(5));

        final idCol = columns.firstWhere((c) => c.name == 'id');
        expect(idCol.isPrimaryKey, isTrue);
        expect(idCol.type, equals('TEXT'));

        final titleCol = columns.firstWhere((c) => c.name == 'title');
        expect(titleCol.isPrimaryKey, isFalse);
        expect(titleCol.notNull, isTrue);

        final priceCol = columns.firstWhere((c) => c.name == 'price');
        expect(priceCol.type, equals('REAL'));
      },
    );

    test('attaches loop-safe triggers to registered table', () async {
      await TriggerGenerator.attachTriggers(db, ['products']);

      final rows = await db.query(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'products';",
      );
      final triggerNames = rows.map((r) => r['name'] as String).toSet();

      expect(triggerNames, contains('_sync_trg_products_ins'));
      expect(triggerNames, contains('_sync_trg_products_upd'));
      expect(triggerNames, contains('_sync_trg_products_del'));
    });

    test('captures INSERT into _sync_outbox and _sync_column_clock', () async {
      await TriggerGenerator.attachTriggers(db, ['products']);

      await db.execute('''
INSERT INTO products (id, title, price, quantity, description)
VALUES ('p1', 'Mechanical Keyboard', 129.99, 15, 'Tactile switches');
''');

      final outboxDeltas = await ShadowTables.getOutboxDeltas(db);
      expect(outboxDeltas.length, equals(5));

      final titleDelta = outboxDeltas.firstWhere(
        (d) => d.columnName == 'title',
      );
      expect(titleDelta.value, equals('Mechanical Keyboard'));
      expect(titleDelta.rowId, equals('p1'));
      expect(titleDelta.tableName, equals('products'));
      expect(titleDelta.isDeleted, isFalse);
      expect(titleDelta.hlc.nodeId, equals('test-node-1'));

      final priceDelta = outboxDeltas.firstWhere(
        (d) => d.columnName == 'price',
      );
      expect(priceDelta.value, equals('129.99'));

      // Verify column clocks were recorded
      final clockRows = await db.query(
        'SELECT * FROM _sync_column_clock WHERE table_name = ? AND row_id = ?;',
        ['products', 'p1'],
      );
      expect(clockRows.length, equals(5));
    });

    test(
      'captures only modified columns on UPDATE (column-level precision)',
      () async {
        await TriggerGenerator.attachTriggers(db, ['products']);

        await db.execute('''
INSERT INTO products (id, title, price, quantity)
VALUES ('p1', 'Mouse', 49.99, 50);
''');

        // Clear initial insert outbox deltas to isolate update verification
        final initialDeltas = await ShadowTables.getOutboxDeltas(db);
        await ShadowTables.clearOutboxDeltas(db, initialDeltas);
        expect(await ShadowTables.getOutboxCount(db), equals(0));

        // Update ONLY the price column
        await db.execute("UPDATE products SET price = 39.99 WHERE id = 'p1';");

        final updateDeltas = await ShadowTables.getOutboxDeltas(db);
        expect(updateDeltas.length, equals(1));
        expect(updateDeltas.first.columnName, equals('price'));
        expect(updateDeltas.first.value, equals('39.99'));
        expect(updateDeltas.first.rowId, equals('p1'));
      },
    );

    test('compacts repeated offline edits to same column in-place', () async {
      await TriggerGenerator.attachTriggers(db, ['products']);

      await db.execute('''
INSERT INTO products (id, title, price, quantity)
VALUES ('p1', 'Headset', 99.99, 10);
''');

      // Simulate offline edits to title
      await db.execute(
        "UPDATE products SET title = 'Headset V2' WHERE id = 'p1';",
      );
      await db.execute(
        "UPDATE products SET title = 'Headset Pro' WHERE id = 'p1';",
      );
      await db.execute(
        "UPDATE products SET title = 'Headset Ultimate' WHERE id = 'p1';",
      );

      final outboxDeltas = await ShadowTables.getOutboxDeltas(db);
      final titleDeltas = outboxDeltas
          .where((d) => d.columnName == 'title')
          .toList();

      // Compaction must maintain exactly 1 row for (products, p1, title)
      expect(titleDeltas.length, equals(1));
      expect(titleDeltas.first.value, equals('Headset Ultimate'));
    });

    test(
      'captures DELETE into _sync_tombstones and compacts outbox into tombstone',
      () async {
        await TriggerGenerator.attachTriggers(db, ['products']);

        await db.execute('''
INSERT INTO products (id, title, price, quantity)
VALUES ('p1', 'Webcam', 79.99, 5);
''');

        expect(await ShadowTables.getOutboxCount(db), equals(5));

        // Delete the row
        await db.execute("DELETE FROM products WHERE id = 'p1';");

        // Verify tombstone table
        final tombstones = await db.query(
          'SELECT * FROM _sync_tombstones WHERE table_name = ? AND row_id = ?;',
          ['products', 'p1'],
        );
        expect(tombstones.length, equals(1));
        expect(tombstones.first['row_id'], equals('p1'));

        // Verify outbox was compacted: previous columns cleared, only tombstone remains
        final outboxDeltas = await ShadowTables.getOutboxDeltas(db);
        expect(outboxDeltas.length, equals(1));
        expect(outboxDeltas.first.columnName, equals('*'));
        expect(outboxDeltas.first.isDeleted, isTrue);
        expect(outboxDeltas.first.value, isNull);
      },
    );

    test(
      'suppresses triggers during remote pulls when is_pulling = 1',
      () async {
        await TriggerGenerator.attachTriggers(db, ['products']);

        // Activate trigger suppression
        await ShadowTables.setPulling(db, true);
        expect(await ShadowTables.isPulling(db), isTrue);

        // Perform INSERT, UPDATE, DELETE while suppression is active
        await db.execute('''
INSERT INTO products (id, title, price, quantity)
VALUES ('remote-p1', 'Monitor', 299.99, 8);
''');
        await db.execute(
          "UPDATE products SET price = 279.99 WHERE id = 'remote-p1';",
        );
        await db.execute("DELETE FROM products WHERE id = 'remote-p1';");

        // Outbox must be completely empty: zero local triggers fired!
        expect(await ShadowTables.getOutboxCount(db), equals(0));

        final tombstones = await db.query('SELECT * FROM _sync_tombstones;');
        expect(tombstones, isEmpty);

        // Restore suppression
        await ShadowTables.setPulling(db, false);
        expect(await ShadowTables.isPulling(db), isFalse);
      },
    );
  });
}
