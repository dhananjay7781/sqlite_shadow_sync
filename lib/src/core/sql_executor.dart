import 'dart:async';
import 'package:sqlite3/common.dart';

/// Database-agnostic SQL execution abstraction for SQLite-backed storage.
///
/// Implementations of this interface allow `sqlite_shadow_sync` to operate seamlessly
/// across `sqlite3` (native & Wasm), `sqflite` (mobile), and `drift`.
abstract interface class SyncSqlExecutor {
  /// Executes a single SQL statement [sql] with optional bound [parameters].
  Future<void> execute(String sql, [List<Object?>? parameters]);

  /// Executes a SQL query [sql] with optional [parameters] and returns matching rows.
  Future<List<Map<String, Object?>>> query(
    String sql, [
    List<Object?>? parameters,
  ]);

  /// Executes the given asynchronous [action] inside an exclusive database transaction.
  ///
  /// Automatically issues `BEGIN IMMEDIATE` prior to [action], followed by `COMMIT`
  /// upon completion. If [action] throws, `ROLLBACK` is executed and the error rethrown.
  Future<T> transaction<T>(Future<T> Function(SyncSqlExecutor txn) action);

  /// Closes the database connection and frees any underlying system resources.
  Future<void> close();
}

/// An implementation of [SyncSqlExecutor] backed by `package:sqlite3`'s [CommonDatabase].
///
/// This executor works across both native environments (desktop, mobile) and
/// WebAssembly (Wasm) without importing `dart:io` or `dart:html`.
class Sqlite3Executor implements SyncSqlExecutor {
  /// The underlying [CommonDatabase] instance.
  final CommonDatabase database;

  /// Creates a [Sqlite3Executor] wrapping the given [database].
  const Sqlite3Executor(this.database);

  @override
  Future<void> execute(String sql, [List<Object?>? parameters]) async {
    database.execute(sql, parameters ?? const []);
  }

  @override
  Future<List<Map<String, Object?>>> query(
    String sql, [
    List<Object?>? parameters,
  ]) async {
    final result = database.select(sql, parameters ?? const []);
    final cols = result.columnNames;
    final rows = <Map<String, Object?>>[];
    for (final row in result) {
      final map = <String, Object?>{};
      for (final col in cols) {
        map[col] = row[col];
      }
      rows.add(map);
    }
    return rows;
  }

  @override
  Future<T> transaction<T>(
    Future<T> Function(SyncSqlExecutor txn) action,
  ) async {
    database.execute('BEGIN IMMEDIATE;');
    try {
      final result = await action(this);
      database.execute('COMMIT;');
      return result;
    } catch (e) {
      try {
        database.execute('ROLLBACK;');
      } catch (_) {
        // Ignore secondary rollback failures if transaction already failed.
      }
      rethrow;
    }
  }

  @override
  Future<void> close() async {
    database.close();
  }
}
