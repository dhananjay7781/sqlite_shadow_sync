/// A zero-touch, trigger-driven, column-level CRDT offline-first synchronization engine for SQLite.
///
/// `sqlite_shadow_sync` brings multi-master, peer-to-peer, and client-server synchronization
/// to SQLite without altering user schemas or requiring specialized SQLite forks.
///
/// ## Core Innovations
///
/// 1. **Zero Schema Pollution:** User tables remain 100% standard SQLite tables.
///    No metadata columns (`hlc`, `node_id`, `is_deleted`) are added. All sync state is
///    isolated in lightweight `_sync_*` shadow tables.
///
/// 2. **Dynamic Trigger Capture:** Sync capture is automated via dynamically generated
///    `AFTER INSERT`, `AFTER UPDATE`, and `AFTER DELETE` SQLite triggers synthesized via
///    `PRAGMA table_info` introspection.
///
/// 3. **Loop-Safe Remote Pulls:** Triggers include suppression guards:
///    `WHEN (SELECT is_pulling FROM _sync_node_state WHERE id = 1) = 0`. During remote merges,
///    incoming deltas never trigger local outbox entries.
///
/// 4. **Column-Level Last-Write-Wins (LWW):** Conflicts resolve at the granular column level
///    using lexicographically sortable Hybrid Logical Clocks ([Hlc]). Concurrent edits to
///    different columns of the same row merge automatically without clobbering each other.
///
/// 5. **Outbox Compaction:** Repeated offline mutations to the same column compact in-place
///    via SQLite `ON CONFLICT DO UPDATE`, minimizing bandwidth and storage overhead.
///
/// 6. **100% WebAssembly (Wasm) Compatible:** Pure Dart with zero `dart:io` or `dart:html` imports
///    in the core library, ensuring seamless cross-platform deployment across Mobile, Desktop,
///    and Web.
library;

import 'src/core/hlc.dart';

export 'src/core/hlc.dart';
export 'src/core/sql_executor.dart';
export 'src/core/sync_models.dart';
export 'src/core/sync_transport.dart';
export 'src/engine/merge_engine.dart';
export 'src/engine/sync_orchestrator.dart';
export 'src/schema/shadow_tables.dart';
export 'src/schema/trigger_generator.dart';
