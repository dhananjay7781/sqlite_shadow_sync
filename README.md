# sqlite_shadow_sync

[![pub package](https://img.shields.io/pub/v/sqlite_shadow_sync.svg)](https://pub.dev/packages/sqlite_shadow_sync)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Platforms](https://img.shields.io/badge/platform-android%20%7C%20ios%20%7C%20web%20%28wasm%29%20%7C%20macos%20%7C%20windows%20%7C%20linux-blue.svg)](https://pub.dev/packages/sqlite_shadow_sync)

A zero-touch, trigger-driven, column-level CRDT (Last-Write-Wins Element-Set via Hybrid Logical Clocks) offline-first synchronization engine for SQLite (`drift`, `sqflite`, and `sqlite3`).

---

## Why This Architecture is Unique

1. **Zero User Schema Pollution:** User tables are **never** altered with `hlc`, `node_id`, or `is_deleted` columns. All synchronization state lives in isolated `_sync_*` shadow tables, keeping native `drift` and `sqflite` migrations 100% intact.
2. **Dynamic SQLite Trigger Capture:** On initialization, the engine inspects registered user tables via `PRAGMA table_info(table_name)` and dynamically generates `AFTER INSERT`, `AFTER UPDATE`, and `AFTER DELETE` SQLite triggers.
3. **Loop-Safe Remote Pulls (Trigger Suppression):** Every generated trigger checks:
   ```sql
   WHEN (SELECT is_pulling FROM _sync_node_state WHERE id = 1) = 0
   ```
   When applying remote deltas from the server, `is_pulling` is set to `1` inside an exclusive transaction so incoming server updates never re-fire local outbox triggers.
4. **Column-Level Conflict Resolution & Outbox Compaction:** Updates are tracked per-column (`OLD.col IS NOT NEW.col`). Repeated offline edits to the same `(table_name, row_id, column_name)` compact in-place via `ON CONFLICT DO UPDATE` in the outbox.
5. **100% WebAssembly (Wasm) Compatible:** Pure Dart with zero `dart:io` or `dart:html` imports in `lib/`. Full multi-platform compatibility across Android, iOS, Web/Wasm, macOS, Windows, and Linux.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Client Application
        App["User App Code / Drift / Sqflite"]
        UserTable[("User SQLite Table\n(e.g., crm_contacts)")]
    end

    subgraph SQLite Trigger Layer
        TrgIns["AFTER INSERT Trigger"]
        TrgUpd["AFTER UPDATE Trigger\n(OLD.col IS NOT NEW.col)"]
        TrgDel["AFTER DELETE Trigger"]
        SuppressionCheck{"is_pulling == 0?"}
    end

    subgraph Shadow Tables
        Outbox[("_sync_outbox\n(In-Place Compaction)")]
        ColumnClock[("_sync_column_clock\n(HLC per column)")]
        Tombstones[("_sync_tombstones\n(Causal deletions)")]
        NodeState[("_sync_node_state\n(is_pulling, node_id)")]
    end

    subgraph Sync Engine
        Orchestrator["SyncOrchestrator\n(Chunking, Backoff, State Machine)"]
        MergeEng["MergeEngine\n(Column-level LWW, Trigger Suppression)"]
    end

    subgraph Remote
        Transport["SyncTransport\n(HTTP / WebSocket / In-Memory)"]
        Server[("Central Sync Server\n(e.g., PostgreSQL / ASP.NET)")]
    end

    App -->|"Regular SQL INSERT/UPDATE/DELETE"| UserTable
    UserTable --> TrgIns & TrgUpd & TrgDel
    TrgIns & TrgUpd & TrgDel --> SuppressionCheck
    SuppressionCheck -->|Yes| Outbox & ColumnClock & Tombstones
    SuppressionCheck -->|No (Remote Pull)| NodeState

    Orchestrator -->|"Push chunked outbox deltas"| Transport
    Transport -->|"Pull paginated server deltas"| Orchestrator
    Orchestrator -->|"Set is_pulling=1 & Merge"| MergeEng
    MergeEng -->|"Update winning columns"| UserTable
    Transport <--> Server
```

---

## Synchronization Protocol Flow

```mermaid
sequenceDiagram
    autonumber
    participant App as Flutter / Dart Client
    participant LocalDB as SQLite (User + Shadow Tables)
    participant Engine as SyncOrchestrator & MergeEngine
    participant Server as Central Sync Server (PostgreSQL)

    Note over App,LocalDB: Offline Local Edits
    App->>LocalDB: UPDATE crm_contacts SET company = 'Antigravity' WHERE id = 'c1';
    LocalDB->>LocalDB: Trigger: OLD.company IS NOT NEW.company
    LocalDB->>LocalDB: Upsert into _sync_outbox (Compacts offline edits)
    LocalDB->>LocalDB: Upsert into _sync_column_clock

    Note over Engine,Server: Synchronization Cycle
    Engine->>LocalDB: Query _sync_outbox (LIMIT pushBatchSize)
    Engine->>Server: POST /api/sync/push (SyncBatch)
    Server->>Server: PL/pgSQL fn_merge_column_delta (LWW)
    Server-->>Engine: PushResult (Success: true, ServerHlc)
    Engine->>LocalDB: DELETE FROM _sync_outbox WHERE hlc = delta.hlc

    Engine->>LocalDB: Query last_pulled_hlc watermark
    Engine->>Server: GET /api/sync/pull?sinceHlc=watermark
    Server-->>Engine: SyncBatch (remote deltas)
    
    rect rgb(240, 248, 255)
    Note over Engine,LocalDB: Trigger-Suppressed Merge Pass
    Engine->>LocalDB: BEGIN IMMEDIATE; UPDATE _sync_node_state SET is_pulling = 1;
    Engine->>LocalDB: Compare remote HLC vs _sync_column_clock (LWW)
    Engine->>LocalDB: UPDATE user table for winning columns
    Engine->>LocalDB: UPDATE _sync_node_state SET is_pulling = 0; COMMIT;
    end
```

---

## Installation

Add `sqlite_shadow_sync` to your `pubspec.yaml`:

```yaml
dependencies:
  sqlite_shadow_sync: ^1.0.0
  sqlite3: ^3.8.0 # or sqflite / drift
```

Or run:

```sh
dart pub add sqlite_shadow_sync
```

---

## Quick Start

```dart
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite_shadow_sync/sqlite_shadow_sync.dart';

Future<void> main() async {
  // 1. Wrap your SQLite database (native or wasm)
  final rawDb = sqlite3.openInMemory();
  final db = Sqlite3Executor(rawDb);

  // 2. Initialize sync shadow tables and register this device's unique node ID
  await ShadowTables.createSchema(db);
  await ShadowTables.initNodeState(db, 'client-device-uuid-123');

  // 3. Create your user tables normally (no extra columns needed!)
  await db.execute('''
    CREATE TABLE contacts (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      company TEXT
    );
  ''');

  // 4. Attach dynamic SQLite capture triggers
  await TriggerGenerator.attachTriggers(db, ['contacts']);

  // 5. Setup your transport (e.g. HTTP, WebSocket, or InMemory for testing)
  final transport = InMemorySyncTransport(serverNodeId: 'cloud-server');

  // 6. Instantiate the Sync Orchestrator
  final orchestrator = SyncOrchestrator(
    db: db,
    transport: transport,
    pushBatchSize: 100,
    pullBatchSize: 500,
    initialRetryDelay: const Duration(seconds: 1),
    maxRetryDelay: const Duration(seconds: 30),
  );

  // Listen to state changes
  orchestrator.statusStream.listen((status) {
    print('Sync Status: $status');
  });

  // 7. Make local mutations using standard SQL or Drift/Sqflite
  await db.execute('''
    INSERT INTO contacts (id, name, email, company)
    VALUES ('c1', 'Jane Doe', 'jane@example.com', 'Acme Corp');
  ''');

  // 8. Execute synchronization
  final stats = await orchestrator.synchronizeWithRetry();
  print('Sync completed: pushed ${stats.deltasPushed}, pulled ${stats.deltasPulled} in ${stats.duration.inMilliseconds}ms');

  // 9. Cleanup when done
  await orchestrator.dispose();
  await db.close();
}
```

---

## Column-Level Conflict Resolution (LWW)

When two offline devices edit the same row simultaneously:

* **Client A** edits `name` to `"Jane Smith"` at `T1`.
* **Client B** edits `company` to `"Antigravity"` at `T2`.

Because `sqlite_shadow_sync` tracks Hybrid Logical Clocks **per column**:
* Both edits survive!
* When synchronized, the merged row becomes:
  ```json
  {
    "id": "c1",
    "name": "Jane Smith",
    "email": "jane@example.com",
    "company": "Antigravity"
  }
  ```
* If both clients edit the **same column**, the change with the higher `Hlc` timestamp deterministically wins, and a `SyncConflict` record is logged.

---

## Backend Reference Architecture (.NET & PostgreSQL)

The repository includes a complete, production-ready backend reference implementation under `backend_examples/dotnet_postgres/`:

* **`schema_and_merge.sql`**: PostgreSQL shadow schema + PL/pgSQL `fn_merge_column_delta` function implementing column-level LWW merge directly in the database.
* **`SyncEndpoints.cs`**: ASP.NET Core (.NET 8/10) Minimal API controller using `Npgsql` and `Dapper` with endpoints:
  - `POST /api/sync/push`: Receives client `SyncBatch`, executes transactional merge, and returns `PushResult`.
  - `GET /api/sync/pull`: Paginated change feed queryable via `sinceHlc` watermark.

---

## Verification & Testing

Run static analysis and the comprehensive test suite:

```sh
# Verify 0 lints, 0 warnings
dart analyze

# Run unit and end-to-end multi-node convergence tests
dart test

# Run the multi-node sync demonstration example
dart run example/main.dart
```

---

## License

MIT License. See [LICENSE](file:///LICENSE) for details.
