# PostgreSQL stats queries

Run against the migrated database (`psql`, pgAdmin, etc.). Counts cell values only, not schema names or metadata.

---

## Database size on disk

```sql
SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size;
```

---

## Content tables: total cells and populated cells

All rows × all columns in `database_content` only. Populated = non-NULL.

```sql
DO $$
DECLARE
    tbl       TEXT;
    col       TEXT;
    row_n     BIGINT;
    notnull_n BIGINT;
    total     BIGINT := 0;
    filled    BIGINT := 0;
BEGIN
    FOR tbl IN
        SELECT DISTINCT table_name
        FROM information_schema.columns
        WHERE table_schema = 'database_content'
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM database_content.%I', tbl) INTO row_n;
        FOR col IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'database_content' AND table_name = tbl
        LOOP
            total  := total + row_n;
            EXECUTE format('SELECT COUNT(%I) FROM database_content.%I', col, tbl) INTO notnull_n;
            filled := filled + notnull_n;
        END LOOP;
    END LOOP;
    RAISE NOTICE 'Total cells:     %', total;
    RAISE NOTICE 'Populated cells: %', filled;
    RAISE NOTICE 'Fill rate:       %%%', ROUND(100.0 * filled / NULLIF(total, 0), 2);
END $$;
```

---

## Content tables: total characters in field values

Sum of characters in every cell in `database_content` (text columns and array elements; no column names or DDL).

```sql
DO $$
DECLARE
    rec         RECORD;
    chunk       BIGINT;
    total_chars BIGINT := 0;
BEGIN
    FOR rec IN
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'database_content'
    LOOP
        IF rec.data_type = 'ARRAY' THEN
            EXECUTE format(
                'SELECT COALESCE(SUM(char_length(array_to_string(%I, ''''))), 0) FROM database_content.%I',
                rec.column_name, rec.table_name
            ) INTO chunk;
        ELSE
            EXECUTE format(
                'SELECT COALESCE(SUM(char_length(%I::text)), 0) FROM database_content.%I',
                rec.column_name, rec.table_name
            ) INTO chunk;
        END IF;
        total_chars := total_chars + chunk;
    END LOOP;
    RAISE NOTICE 'Total characters in database_content schema: %', total_chars;
END $$;
```

---

## Whole database: total characters in stored values

Sum across all schemas (including JSONB cast to text).

```sql
DO $$
DECLARE
    rec         RECORD;
    chunk       BIGINT;
    total_chars BIGINT := 0;
BEGIN
    FOR rec IN
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema IN (
            'database_content', 'database_select_options', 'database_computed_fields',
            'database_property_meta', 'database_views', 'database_meta', 'standalone_pages'
        )
    LOOP
        IF rec.data_type = 'ARRAY' THEN
            EXECUTE format(
                'SELECT COALESCE(SUM(char_length(array_to_string(%I, ''''))), 0) FROM %I.%I',
                rec.column_name, rec.table_schema, rec.table_name
            ) INTO chunk;
        ELSE
            EXECUTE format(
                'SELECT COALESCE(SUM(char_length(%I::text)), 0) FROM %I.%I',
                rec.column_name, rec.table_schema, rec.table_name
            ) INTO chunk;
        END IF;
        total_chars := total_chars + chunk;
    END LOOP;
    RAISE NOTICE 'Total characters across entire database: %', total_chars;
END $$;
```

---

## Per-table character count (sorted descending)

One row per table across all schemas, sorted by total character count descending.

```sql
CREATE TEMP TABLE _table_char_stats (
    schema_name TEXT,
    table_name  TEXT,
    char_count  BIGINT
);

DO $$
DECLARE
    rec    RECORD;
    col    RECORD;
    chunk  BIGINT;
    tbl_total BIGINT;
BEGIN
    FOR rec IN
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN (
            'database_content', 'database_select_options', 'database_computed_fields',
            'database_property_meta', 'database_views', 'database_meta', 'standalone_pages'
        )
        AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
    LOOP
        tbl_total := 0;
        FOR col IN
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = rec.table_schema AND table_name = rec.table_name
        LOOP
            IF col.data_type = 'ARRAY' THEN
                EXECUTE format(
                    'SELECT COALESCE(SUM(char_length(array_to_string(%I, ''''))), 0) FROM %I.%I',
                    col.column_name, rec.table_schema, rec.table_name
                ) INTO chunk;
            ELSE
                EXECUTE format(
                    'SELECT COALESCE(SUM(char_length(%I::text)), 0) FROM %I.%I',
                    col.column_name, rec.table_schema, rec.table_name
                ) INTO chunk;
            END IF;
            tbl_total := tbl_total + chunk;
        END LOOP;
        INSERT INTO _table_char_stats VALUES (rec.table_schema, rec.table_name, tbl_total);
    END LOOP;
END $$;

SELECT schema_name, table_name, char_count
FROM _table_char_stats
ORDER BY char_count DESC;

DROP TABLE _table_char_stats;
```
