using System.Data;
using System.Text.Json.Serialization;
using Dapper;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Npgsql;

namespace SqliteShadowSync.Backend.DotNet;

/// <summary>
/// Data Transfer Object representing a single column-level mutation or deletion.
/// </summary>
public record ColumnDeltaDto(
    [property: JsonPropertyName("table_name")] string TableName,
    [property: JsonPropertyName("row_id")] string RowId,
    [property: JsonPropertyName("column_name")] string ColumnName,
    [property: JsonPropertyName("value")] object? Value,
    [property: JsonPropertyName("hlc")] string Hlc,
    [property: JsonPropertyName("is_deleted")] bool IsDeleted
);

/// <summary>
/// Data Transfer Object representing a sync payload batch.
/// </summary>
public record SyncBatchDto(
    [property: JsonPropertyName("node_id")] string NodeId,
    [property: JsonPropertyName("deltas")] List<ColumnDeltaDto> Deltas,
    [property: JsonPropertyName("cursor")] string? Cursor,
    [property: JsonPropertyName("has_more")] bool HasMore
);

/// <summary>
/// Response payload returned after pushing deltas to the server.
/// </summary>
public record PushResultDto(
    [property: JsonPropertyName("success")] bool Success,
    [property: JsonPropertyName("accepted_count")] int AcceptedCount,
    [property: JsonPropertyName("server_hlc")] string? ServerHlc,
    [property: JsonPropertyName("error_message")] string? ErrorMessage
);

/// <summary>
/// Production-ready ASP.NET Core Minimal API endpoints for sqlite_shadow_sync backend.
/// </summary>
public static class SyncEndpoints
{
    private const string ServerNodeId = "dotnet-postgres-server";

    /// <summary>
    /// Generates a canonical Hybrid Logical Clock string on the server.
    /// </summary>
    private static string GenerateServerHlc()
    {
        var nowUtc = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        return $"{nowUtc}:0000:{ServerNodeId}";
    }

    /// <summary>
    /// Registers sync push and pull endpoints onto the application route builder.
    /// </summary>
    public static IEndpointRouteBuilder MapSyncEndpoints(
        this IEndpointRouteBuilder app,
        string connectionString)
    {
        var group = app.MapGroup("/api/sync")
                       .WithTags("Offline-First Synchronization");

        // -------------------------------------------------------------
        // POST /api/sync/push
        // -------------------------------------------------------------
        group.MapPost("/push", async (SyncBatchDto batch, HttpContext ctx) =>
        {
            if (batch == null || batch.Deltas == null || batch.Deltas.Count == 0)
            {
                return Results.Ok(new PushResultDto(
                    Success: true,
                    AcceptedCount: 0,
                    ServerHlc: GenerateServerHlc(),
                    ErrorMessage: null
                ));
            }

            var serverHlc = GenerateServerHlc();
            int acceptedCount = 0;

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ctx.RequestAborted);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ctx.RequestAborted);

            try
            {
                const string mergeSql = @"
                    SELECT fn_merge_column_delta(
                        @TableName,
                        @RowId,
                        @ColumnName,
                        @Value,
                        @Hlc,
                        @IsDeleted,
                        @ServerHlc
                    );
                ";

                foreach (var delta in batch.Deltas)
                {
                    var stringValue = delta.Value?.ToString();

                    var merged = await connection.ExecuteScalarAsync<bool>(
                        mergeSql,
                        new
                        {
                            TableName = delta.TableName,
                            RowId = delta.RowId,
                            ColumnName = delta.ColumnName,
                            Value = stringValue,
                            Hlc = delta.Hlc,
                            IsDeleted = delta.IsDeleted,
                            ServerHlc = serverHlc
                        },
                        transaction
                    );

                    if (merged)
                    {
                        acceptedCount++;
                    }
                }

                await transaction.CommitAsync(ctx.RequestAborted);

                return Results.Ok(new PushResultDto(
                    Success: true,
                    AcceptedCount: acceptedCount,
                    ServerHlc: serverHlc,
                    ErrorMessage: null
                ));
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync(ctx.RequestAborted);
                return Results.BadRequest(new PushResultDto(
                    Success: false,
                    AcceptedCount: 0,
                    ServerHlc: null,
                    ErrorMessage: ex.Message
                ));
            }
        })
        .WithName("PushDeltas")
        .WithSummary("Merges incoming client column deltas into PostgreSQL via LWW");

        // -------------------------------------------------------------
        // GET /api/sync/pull?sinceHlc=...&limit=500
        // -------------------------------------------------------------
        group.MapGet("/pull", async (string? sinceHlc, int? limit, HttpContext ctx) =>
        {
            var fetchLimit = Math.Clamp(limit ?? 500, 1, 2000);

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ctx.RequestAborted);

            const string countSql = @"
                SELECT COUNT(*) FROM _sync_changes_log
                WHERE (@SinceHlc IS NULL OR server_hlc > @SinceHlc);
            ";

            const string selectSql = @"
                SELECT table_name, row_id, column_name, value, hlc, is_deleted, server_hlc
                FROM _sync_changes_log
                WHERE (@SinceHlc IS NULL OR server_hlc > @SinceHlc)
                ORDER BY server_hlc ASC, sequence_id ASC
                LIMIT @Limit;
            ";

            var totalAvailable = await connection.ExecuteScalarAsync<long>(
                countSql,
                new { SinceHlc = sinceHlc }
            );

            var rows = (await connection.QueryAsync(
                selectSql,
                new { SinceHlc = sinceHlc, Limit = fetchLimit }
            )).ToList();

            var deltas = rows.Select(r => new ColumnDeltaDto(
                TableName: (string)r.table_name,
                RowId: (string)r.row_id,
                ColumnName: (string)r.column_name,
                Value: (object?)r.value,
                Hlc: (string)r.hlc,
                IsDeleted: (bool)r.is_deleted
            )).ToList();

            string? nextCursor = sinceHlc;
            if (rows.Count > 0)
            {
                var lastRow = rows.Last();
                nextCursor = (string)lastRow.server_hlc;
            }

            var hasMore = totalAvailable > rows.Count;

            return Results.Ok(new SyncBatchDto(
                NodeId: ServerNodeId,
                Deltas: deltas,
                Cursor: nextCursor,
                HasMore: hasMore
            ));
        })
        .WithName("PullDeltas")
        .WithSummary("Fetches remote column deltas committed since the specified HLC watermark");

        return app;
    }
}

