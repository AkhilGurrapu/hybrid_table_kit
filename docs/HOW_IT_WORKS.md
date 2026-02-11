# How the Hybrid Table Materialization Works

This document explains the internal architecture of the pure-macro hybrid table materialization.

## Overview

The materialization consists of two files:

- **`hybrid_table.sql`** — The `{% materialization hybrid_table, adapter='snowflake' %}` block. Handles configuration loading, validation, table creation, and incremental updates.
- **`hybrid_table_helpers.sql`** — Helper macros for type canonicalization, schema change detection, and schema change application.

## Execution Flow

### Full Refresh / First Run

```
1. Load config (column_definitions, primary_key, indexes, etc.)
2. Validate config (columns exist, types supported, FK references valid)
3. Check existing relation type (drop if view/dynamic table)
4. Choose creation path:
   a. Non-CTAS path (if FK or AUTOINCREMENT present):
      - CREATE OR REPLACE HYBRID TABLE ... (empty, with all constraints)
      - INSERT INTO ... SELECT (excluding AUTOINCREMENT columns)
   b. CTAS path (no FK, no AUTOINCREMENT):
      - CREATE OR REPLACE HYBRID TABLE ... AS (SELECT ...)
5. Apply grants, persist docs
```

### Incremental Run

```
1. Load config, validate
2. Detect schema changes (if on_schema_change != 'ignore'):
   a. DESCRIBE TABLE to get existing columns
   b. Compare with column_definitions
   c. Apply changes (ADD/DROP columns, or error)
3. Execute incremental strategy:
   a. MERGE: match on PK → UPDATE matched rows, INSERT new rows
   b. delete+insert: DELETE matching rows → INSERT all source rows
4. Apply grants, persist docs
```

## CTAS vs Non-CTAS Paths

Snowflake has two ways to create a hybrid table:

### CTAS (CREATE ... AS SELECT)

```sql
CREATE OR REPLACE HYBRID TABLE my_table (
    id INT NOT NULL,
    name VARCHAR(200),
    PRIMARY KEY (id),
    INDEX idx_name (name)
) AS (
    SELECT id, name FROM source
);
```

**Limitations:** CTAS does NOT support FOREIGN KEY or UNIQUE constraints in the column definition. It also cannot handle AUTOINCREMENT columns (which have no source data).

### Non-CTAS (CREATE empty + INSERT)

```sql
CREATE OR REPLACE HYBRID TABLE my_table (
    id INT NOT NULL,
    name VARCHAR(200),
    PRIMARY KEY (id),
    FOREIGN KEY (customer_id) REFERENCES customers (id)
);

INSERT INTO my_table (id, name) SELECT id, name FROM source;
```

**When used:** The materialization automatically uses non-CTAS when:
- `foreign_keys` is non-empty (FK must be in CREATE TABLE DDL)
- `autoincrement_columns` is non-empty (AUTOINCREMENT columns can't be in INSERT)
- `force_non_ctas=true` is set

## Type Canonicalization

Snowflake's `DESCRIBE TABLE` returns canonical type names that differ from what users commonly write:

| User Writes | DESCRIBE TABLE Returns |
|-------------|----------------------|
| `INT` | `NUMBER(38,0)` |
| `INTEGER` | `NUMBER(38,0)` |
| `VARCHAR` | `VARCHAR(16777216)` |
| `STRING` | `VARCHAR(16777216)` |
| `TIMESTAMP_NTZ` | `TIMESTAMP_NTZ(9)` |
| `DECIMAL(10,2)` | `NUMBER(10,2)` |
| `BOOLEAN` | `BOOLEAN` |
| `FLOAT` | `FLOAT` |

The `_hybrid_canonicalize_type()` macro normalizes both the user's type and the DESCRIBE TABLE type to the same canonical form before comparison. This prevents false "type changed" detections on incremental runs.

## Schema Change Detection

The `_hybrid_detect_schema_changes()` macro:

1. Runs `DESCRIBE TABLE <relation>` to get current columns
2. Accesses results using positional indexing (`row[0]` = name, `row[1]` = type) to avoid column name casing issues
3. Builds a map of existing columns (name → canonical type)
4. Builds a map of new columns from `column_definitions` (name → canonical type)
5. Compares the two maps to find:
   - **Additions:** columns in new but not existing
   - **Drops:** columns in existing but not new
   - **Type changes:** columns in both but with different canonical types

Returns a dict with `add_columns`, `drop_columns`, `type_changes`, `has_changes`, and `requires_full_refresh`.

## Schema Change Application

Based on the `on_schema_change` config:

| Mode | New Columns | Dropped Columns | Type Changes |
|------|------------|-----------------|--------------|
| `ignore` | Ignored | Ignored | Ignored |
| `fail` | Error | Error | Error |
| `append_new_columns` | ALTER TABLE ADD | Error | Error (requires --full-refresh) |
| `sync_all_columns` | ALTER TABLE ADD | ALTER TABLE DROP | Error (requires --full-refresh) |

**Important:** Type changes always require `--full-refresh` because Snowflake doesn't support ALTER COLUMN TYPE.

Column modifications use `ALTER TABLE` (not `ALTER HYBRID TABLE`):
```sql
ALTER TABLE my_table ADD COLUMN new_col VARCHAR(100);
ALTER TABLE my_table DROP COLUMN old_col;
```

## FK Validation

The materialization validates foreign keys at compile time:

1. **Column existence:** FK columns must exist in `column_definitions`
2. **Reference resolution:** Handles plain table names, 2-part (`schema.table`), and 3-part (`db.schema.table`) dot notation
3. **Referenced table existence:** Uses `adapter.get_relation()` to verify the referenced table exists in Snowflake
4. **Referenced column existence:** Uses `adapter.get_columns_in_relation()` to verify referenced columns exist on the target table
5. **Column count match:** Referencing and referenced column lists must have the same length

## AUTOINCREMENT Handling

AUTOINCREMENT columns are detected by scanning `column_definitions` for the `AUTOINCREMENT` or `IDENTITY` keywords. They are automatically:

1. **Excluded from INSERT** statements (Snowflake auto-generates the values)
2. **Excluded from MERGE INSERT** (NOT MATCHED clause)
3. **Excluded from MERGE UPDATE** (MATCHED clause)
4. **Used to trigger non-CTAS path** (since CTAS can't handle AUTOINCREMENT)

Special case: If ALL primary key columns are AUTOINCREMENT, the materialization uses INSERT-only on incremental runs (no MERGE possible since PKs aren't in source data).

## Incremental Strategies

### MERGE

```sql
MERGE INTO target AS target
USING (SELECT ...) AS source
ON target.pk1 = source.pk1 AND target.pk2 = source.pk2
WHEN MATCHED THEN
    UPDATE SET target.col1 = source.col1, target.col2 = source.col2
WHEN NOT MATCHED THEN
    INSERT (pk1, pk2, col1, col2) VALUES (source.pk1, source.pk2, source.col1, source.col2)
```

Update columns exclude: PKs, AUTOINCREMENT columns, and columns in `merge_exclude_columns`.

### delete+insert

```sql
DELETE FROM target USING (SELECT ...) AS source WHERE target.pk = source.pk;
INSERT INTO target (pk, col1, col2) SELECT pk, col1, col2 FROM source;
```

Requires at least one non-AUTOINCREMENT PK column.

## Pure Macro vs Adapter Fork

This implementation is a **pure-macro** approach — everything runs as Jinja macros in your project's `macros/` folder. No Python adapter changes are needed.

**What the pure-macro approach replaces:**

| Adapter Fork (Python) | Pure Macro (Jinja) |
|----------------------|-------------------|
| `this.incorporate(type=this.HybridTable)` | `this` (used directly) |
| `existing_relation.is_hybrid_table` | Check `existing_relation.type not in ('table', 'hybrid_table')` |
| `adapter.describe_hybrid_table()` | `run_query('DESCRIBE TABLE ...')` |
| Python `_canonicalize_snowflake_type()` | Jinja `_hybrid_canonicalize_type()` |
| Python `build_hybrid_table_changeset()` | Jinja `_hybrid_detect_schema_changes()` |
| `DROP HYBRID TABLE` | `DROP TABLE IF EXISTS` |

Standard dbt adapter methods that work unchanged in project macros:
- `adapter.get_relation()` — look up relations in Snowflake
- `adapter.get_columns_in_relation()` — get column metadata
- `adapter.drop_relation()` — drop tables/views
- `load_cached_relation()` — check dbt's relation cache
- `run_query()` — execute arbitrary SQL and get results
- `should_full_refresh()` — check for --full-refresh flag
- `apply_grants()`, `persist_docs()`, `run_hooks()` — standard dbt lifecycle
