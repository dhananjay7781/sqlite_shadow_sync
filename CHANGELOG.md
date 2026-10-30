# Changelog

All notable changes to `sqlite_shadow_sync` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-10-30

### Added
- **Core Engine**:
  - `Hlc`: Lexicographically sortable Hybrid Logical Clock implementation with physical clock drift guards, sub-millisecond logical counters, and string parsing.
  - `SyncModels`: Sealed `SyncStatus` hierarchy (`SyncIdle`, `SyncPulling`, `SyncPushing`, `SyncSuccess`, `SyncError`), `ColumnDelta`, `RowDelta`, `SyncBatch`, and `SyncConflict`.
  - `SyncSqlExecutor`: Database-agnostic SQL execution abstraction with high-performance `Sqlite3Executor` adapter.
  - `SyncTransport`: Abstract network transport contract with `InMemorySyncTransport` for testing and simulation.
- **Shadow Schema Architecture**:
  - Isolated metadata tables (`_sync_node_state`, `_sync_column_clock`, `_sync_outbox`, `_sync_tombstones`) guaranteeing zero schema pollution on user tables.
  - `TriggerGenerator`: Dynamic `PRAGMA table_info` introspector and SQLite trigger generator (`AFTER INSERT`, `AFTER UPDATE`, `AFTER DELETE`).
  - Outbox in-place compaction via SQLite `ON CONFLICT DO UPDATE`.
  - Loop-safe trigger suppression during remote merges via `WHEN (SELECT is_pulling FROM _sync_node_state WHERE id = 1) = 0`.
- **Merge & Orchestration**:
  - `MergeEngine`: Granular column-level Last-Write-Wins (LWW) conflict resolution and causal tombstone deletion handling.
  - `SyncOrchestrator`: Full push/pull state machine with batch chunking, pagination watermarks, and exponential backoff retry with random jitter.
- **Backend Reference Implementation**:
  - PostgreSQL schema and PL/pgSQL column-level LWW merge functions (`schema_and_merge.sql`).
  - Production-ready ASP.NET Core (.NET 8/10) Minimal API controller (`SyncEndpoints.cs`) using `Npgsql` and `Dapper`.
- **Multi-Platform Support**:
  - 100% WebAssembly (Wasm) and native platform support with zero `dart:io` or `dart:html` imports in `lib/`.
