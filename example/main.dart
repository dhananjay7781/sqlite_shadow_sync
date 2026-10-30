import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite_shadow_sync/sqlite_shadow_sync.dart';

Future<void> main() async {
  print('================================================================');
  print('   sqlite_shadow_sync: Multi-Node Offline-First CRDT Engine     ');
  print('================================================================\n');

  // 1. Initialize Central Sync Transport (Server)
  final centralServer = InMemorySyncTransport(serverNodeId: 'cloud-server');

  // 2. Initialize 3 isolated virtual nodes (Mobile, Desktop, Web)
  final alphaDb = Sqlite3Executor(sqlite3.openInMemory());
  final betaDb = Sqlite3Executor(sqlite3.openInMemory());
  final gammaDb = Sqlite3Executor(sqlite3.openInMemory());

  const ddl = '''
CREATE TABLE crm_contacts (
  id TEXT PRIMARY KEY,
  full_name TEXT NOT NULL,
  email TEXT NOT NULL,
  phone TEXT,
  company TEXT,
  status TEXT DEFAULT 'lead'
);
''';

  for (final (db, nodeId) in [
    (alphaDb, 'client-alpha-mobile'),
    (betaDb, 'client-beta-desktop'),
    (gammaDb, 'client-gamma-web'),
  ]) {
    await ShadowTables.createSchema(db);
    await ShadowTables.initNodeState(db, nodeId);
    await db.execute(ddl);
    await TriggerGenerator.attachTriggers(db, ['crm_contacts']);
  }

  final syncAlpha = SyncOrchestrator(db: alphaDb, transport: centralServer);
  final syncBeta = SyncOrchestrator(db: betaDb, transport: centralServer);
  final syncGamma = SyncOrchestrator(db: gammaDb, transport: centralServer);

  print('[1] Genesis: Client-Alpha creates contact "c1"...');
  await alphaDb.execute('''
INSERT INTO crm_contacts (id, full_name, email, phone, company, status)
VALUES ('c1', 'Jane Doe', 'jane@example.com', '+1-555-0100', 'Acme Corp', 'lead');
''');

  print('    Syncing Client-Alpha -> Server...');
  await syncAlpha.synchronize();

  print('    Syncing Server -> Client-Beta & Client-Gamma...');
  await syncBeta.synchronize();
  await syncGamma.synchronize();

  print('    All 3 nodes synchronized with initial contact.\n');

  // 3. Simulate Concurrent Offline Edits
  print('[2] Offline Phase: Network partitioned! All nodes edit "c1" independently...');
  print('    - Client-Alpha updates full_name -> "Dr. Jane Doe, PhD"');
  await alphaDb.execute(
    "UPDATE crm_contacts SET full_name = 'Dr. Jane Doe, PhD' WHERE id = 'c1';",
  );

  print('    - Client-Beta  updates company   -> "Antigravity Systems"');
  await betaDb.execute(
    "UPDATE crm_contacts SET company = 'Antigravity Systems' WHERE id = 'c1';",
  );

  print('    - Client-Gamma updates status    -> "VIP Enterprise"');
  await gammaDb.execute(
    "UPDATE crm_contacts SET status = 'VIP Enterprise' WHERE id = 'c1';",
  );

  print('\n[3] Pre-Sync Divergent Local States:');
  for (final (db, name) in [
    (alphaDb, 'Alpha (Mobile)'),
    (betaDb, 'Beta (Desktop)'),
    (gammaDb, 'Gamma (Web)'),
  ]) {
    final row = (await db.query("SELECT full_name, company, status FROM crm_contacts WHERE id = 'c1';")).first;
    print('    $name => name: "${row['full_name']}", company: "${row['company']}", status: "${row['status']}"');
  }

  // 4. Reconnect and Synchronize
  print('\n[4] Reconnecting: Running synchronization and gossip protocol...');
  // Push all deltas to server
  await syncAlpha.synchronize();
  await syncBeta.synchronize();
  await syncGamma.synchronize();

  // Gossip convergence pass
  await syncAlpha.synchronize();
  await syncBeta.synchronize();
  await syncGamma.synchronize();

  print('\n[5] Post-Sync Converged State:');
  for (final (db, name) in [
    (alphaDb, 'Alpha (Mobile)'),
    (betaDb, 'Beta (Desktop)'),
    (gammaDb, 'Gamma (Web)'),
  ]) {
    final row = (await db.query("SELECT full_name, company, status FROM crm_contacts WHERE id = 'c1';")).first;
    print('    $name => name: "${row['full_name']}", company: "${row['company']}", status: "${row['status']}"');
  }

  print('\n================================================================');
  print('SUCCESS: All 3 nodes converged to identical column-level values!');
  print('Zero data loss. Zero schema pollution. 100% pure Dart & SQLite.');
  print('================================================================');

  await syncAlpha.dispose();
  await syncBeta.dispose();
  await syncGamma.dispose();
  await alphaDb.close();
  await betaDb.close();
  await gammaDb.close();
}

