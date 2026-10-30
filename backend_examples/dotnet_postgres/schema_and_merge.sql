-- ============================================================================
-- sqlite_shadow_sync: PostgreSQL Backend Shadow Schema & PL/pgSQL LWW Engine
-- ============================================================================

-- 1. Table storing column-level Hybrid Logical Clocks for Last-Write-Wins
CREATE TABLE IF NOT EXISTS _sync_column_clock (
    table_name VARCHAR(128) NOT NULL,
    row_id VARCHAR(128) NOT NULL,
    column_name VARCHAR(128) NOT NULL,
    hlc VARCHAR(128) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (table_name, row_id, column_name)
);

CREATE INDEX IF NOT EXISTS idx_sync_col_clock_hlc ON _sync_column_clock (hlc);

-- 2. Table storing row tombstones for deletions
CREATE TABLE IF NOT EXISTS _sync_tombstones (
    table_name VARCHAR(128) NOT NULL,
    row_id VARCHAR(128) NOT NULL,
    hlc VARCHAR(128) NOT NULL,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (table_name, row_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_tombstones_hlc ON _sync_tombstones (hlc);

-- 3. Central append-only change log for fan-out and client pagination
CREATE TABLE IF NOT EXISTS _sync_changes_log (
    sequence_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(128) NOT NULL,
    row_id VARCHAR(128) NOT NULL,
    column_name VARCHAR(128) NOT NULL,
    value TEXT,
    hlc VARCHAR(128) NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    server_hlc VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_sync_changes_server_hlc ON _sync_changes_log (server_hlc);
CREATE INDEX IF NOT EXISTS idx_sync_changes_tbl_row ON _sync_changes_log (table_name, row_id);

-- ============================================================================
-- PL/pgSQL Function: Column-Level Last-Write-Wins (LWW) Merge
-- ============================================================================
CREATE OR REPLACE FUNCTION fn_merge_column_delta(
    p_table_name TEXT,
    p_row_id TEXT,
    p_column_name TEXT,
    p_value TEXT,
    p_hlc TEXT,
    p_is_deleted BOOLEAN,
    p_server_hlc TEXT
) RETURNS BOOLEAN AS $$
DECLARE
    v_local_hlc TEXT;
    v_tombstone_hlc TEXT;
    v_max_local_hlc TEXT;
    v_sql TEXT;
BEGIN
    -- 1. Check existing tombstone
    SELECT hlc INTO v_tombstone_hlc
    FROM _sync_tombstones
    WHERE table_name = p_table_name AND row_id = p_row_id;

    -- Case A: Incoming Deletion
    IF p_is_deleted OR p_column_name = '*' THEN
        -- Check if any local column has a higher HLC than incoming deletion
        SELECT MAX(hlc) INTO v_max_local_hlc
        FROM _sync_column_clock
        WHERE table_name = p_table_name AND row_id = p_row_id;

        IF v_max_local_hlc IS NOT NULL AND v_max_local_hlc > p_hlc THEN
            -- Local column modification occurred after deletion -> local edit resurrects row
            RETURN FALSE;
        END IF;

        -- Remote deletion wins: Delete physical row
        v_sql := format('DELETE FROM %I WHERE id::text = %L;', p_table_name, p_row_id);
        EXECUTE v_sql;

        -- Insert/update tombstone
        INSERT INTO _sync_tombstones (table_name, row_id, hlc, deleted_at)
        VALUES (p_table_name, p_row_id, p_hlc, clock_timestamp())
        ON CONFLICT (table_name, row_id) DO UPDATE
        SET hlc = EXCLUDED.hlc, deleted_at = EXCLUDED.deleted_at;

        -- Append to server change log
        INSERT INTO _sync_changes_log (table_name, row_id, column_name, value, hlc, is_deleted, server_hlc)
        VALUES (p_table_name, p_row_id, '*', NULL, p_hlc, TRUE, p_server_hlc);

        RETURN TRUE;
    END IF;

    -- Case B: Incoming Mutation on Deleted Row
    IF v_tombstone_hlc IS NOT NULL THEN
        IF v_tombstone_hlc >= p_hlc THEN
            -- Local deletion is newer than remote edit -> deletion wins
            RETURN FALSE;
        ELSE
            -- Remote edit is newer than local tombstone -> resurrect row
            DELETE FROM _sync_tombstones
            WHERE table_name = p_table_name AND row_id = p_row_id;
        END IF;
    END IF;

    -- Case C: Standard Column-Level LWW
    SELECT hlc INTO v_local_hlc
    FROM _sync_column_clock
    WHERE table_name = p_table_name AND row_id = p_row_id AND column_name = p_column_name;

    IF v_local_hlc IS NOT NULL AND v_local_hlc >= p_hlc THEN
        -- Local clock is equal or newer -> local wins
        RETURN FALSE;
    END IF;

    -- Remote wins: Upsert column value in target user table
    -- 1. Ensure row shell exists
    v_sql := format(
        'INSERT INTO %I (id) VALUES (%L) ON CONFLICT (id) DO NOTHING;',
        p_table_name, p_row_id
    );
    EXECUTE v_sql;

    -- 2. Update specific column
    v_sql := format(
        'UPDATE %I SET %I = %L WHERE id::text = %L;',
        p_table_name, p_column_name, p_value, p_row_id
    );
    EXECUTE v_sql;

    -- 3. Update column clock
    INSERT INTO _sync_column_clock (table_name, row_id, column_name, hlc, updated_at)
    VALUES (p_table_name, p_row_id, p_column_name, p_hlc, clock_timestamp())
    ON CONFLICT (table_name, row_id, column_name) DO UPDATE
    SET hlc = EXCLUDED.hlc, updated_at = EXCLUDED.updated_at;

    -- 4. Append to server change log
    INSERT INTO _sync_changes_log (table_name, row_id, column_name, value, hlc, is_deleted, server_hlc)
    VALUES (p_table_name, p_row_id, p_column_name, p_value, p_hlc, FALSE, p_server_hlc);

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Batch Merge Helper Function
-- ============================================================================
CREATE OR REPLACE FUNCTION fn_merge_sync_batch(
    p_deltas JSONB,
    p_server_hlc TEXT
) RETURNS INTEGER AS $$
DECLARE
    v_delta JSONB;
    v_accepted_count INTEGER := 0;
    v_merged BOOLEAN;
BEGIN
    FOR v_delta IN SELECT * FROM jsonb_array_elements(p_deltas)
    LOOP
        v_merged := fn_merge_column_delta(
            v_delta->>'table_name',
            v_delta->>'row_id',
            v_delta->>'column_name',
            v_delta->>'value',
            v_delta->>'hlc',
            COALESCE((v_delta->>'is_deleted')::boolean, FALSE),
            p_server_hlc
        );

        IF v_merged THEN
            v_accepted_count := v_accepted_count + 1;
        END IF;
    END LOOP;

    RETURN v_accepted_count;
END;
$$ LANGUAGE plpgsql;

