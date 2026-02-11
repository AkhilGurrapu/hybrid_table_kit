# Snowflake Type Canonicalization

## What Is Type Canonicalization?

Snowflake accepts many type aliases when creating tables (e.g., `INT`, `STRING`, `TIMESTAMP_NTZ`), but internally resolves them to canonical forms. When you run `DESCRIBE TABLE`, Snowflake returns only these canonical forms — never the original aliases you wrote.

This matters for the hybrid table materialization because it must compare user-defined types from `column_definitions` against `DESCRIBE TABLE` output during incremental runs to detect schema changes. Without normalizing both sides to the same canonical form, every incremental run would produce false "type changed" detections and trigger unnecessary full refreshes.

## Why This Is Unique to Hybrid Tables

Other dbt materializations (table, view, incremental, dynamic table) do **not** need type canonicalization:

| Materialization | Schema Comparison? | Why No Canonicalization Needed |
|---|---|---|
| **Table** | None — rebuilt every run | No comparison ever happens |
| **View** | None — rebuilt every run | No comparison ever happens |
| **Incremental** | Compares two Snowflake relations (temp table vs target) | Both already have canonical types from Snowflake's type system — it's `NUMBER(38,0)` vs `NUMBER(38,0)` |
| **Dynamic Table** | Compares config properties (`target_lag`, `warehouse`) | Never compares column types at all |
| **Hybrid Table** | Compares user-written types vs `DESCRIBE TABLE` output | **User writes `INT`, Snowflake returns `NUMBER(38,0)` — mismatch without canonicalization** |

The standard incremental materialization uses `adapter.expand_target_column_types()` which does fuzzy type matching (can this fit in that?) between two Snowflake-native representations. It never compares a user-written string against a system-reported string.

Hybrid tables are different because they require explicit `column_definitions` with constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY). These user-written definitions are the source of truth, and they must be compared against the existing table's schema to detect adds, drops, and type changes.

## Snowflake Type Alias Mappings

The following mappings are confirmed by official Snowflake documentation.

### Numeric Types

All integer aliases resolve to `NUMBER(38,0)`:

| User Writes | DESCRIBE TABLE Returns | Snowflake Docs |
|---|---|---|
| `INT` | `NUMBER(38,0)` | "Synonymous with NUMBER, except that precision and scale can't be specified (that is, it always defaults to NUMBER(38, 0))." |
| `INTEGER` | `NUMBER(38,0)` | Same as above |
| `BIGINT` | `NUMBER(38,0)` | Same as above |
| `SMALLINT` | `NUMBER(38,0)` | Same as above |
| `TINYINT` | `NUMBER(38,0)` | Same as above |
| `BYTEINT` | `NUMBER(38,0)` | Same as above |
| `DECIMAL(p,s)` | `NUMBER(p,s)` | "Synonymous with NUMBER." |
| `NUMERIC(p,s)` | `NUMBER(p,s)` | "Synonymous with NUMBER." |
| `DECIMAL` (no args) | `NUMBER(38,0)` | Default precision 38, scale 0 |

All float aliases resolve to `FLOAT`:

| User Writes | DESCRIBE TABLE Returns |
|---|---|
| `FLOAT` | `FLOAT` |
| `FLOAT4` | `FLOAT` |
| `FLOAT8` | `FLOAT` |
| `DOUBLE` | `FLOAT` |
| `DOUBLE PRECISION` | `FLOAT` |
| `REAL` | `FLOAT` |

**Source:** [Numeric data types - Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/data-types-numeric)

### String Types

| User Writes | DESCRIBE TABLE Returns | Snowflake Docs |
|---|---|---|
| `VARCHAR` (no length) | `VARCHAR(16777216)` | "If no length is specified, the default is 16777216." |
| `VARCHAR(n)` | `VARCHAR(n)` | Length preserved as specified |
| `STRING` | `VARCHAR(16777216)` | "Synonymous with VARCHAR." |
| `TEXT` | `VARCHAR(16777216)` | "Synonymous with VARCHAR." |

**Source:** [String & binary data types - Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/data-types-text)

### Date & Time Types

| User Writes | DESCRIBE TABLE Returns | Snowflake Docs |
|---|---|---|
| `TIMESTAMP_NTZ` | `TIMESTAMP_NTZ(9)` | "The default precision is 9." (nanoseconds) |
| `TIMESTAMP_LTZ` | `TIMESTAMP_LTZ(9)` | Same — default precision 9 |
| `TIMESTAMP_TZ` | `TIMESTAMP_TZ(9)` | Same — default precision 9 |
| `TIMESTAMP` | `TIMESTAMP_NTZ(9)` | Alias for TIMESTAMP_NTZ by default |
| `TIME` | `TIME(9)` | Default precision 9 |
| `DATE` | `DATE` | No precision parameter |

**Source:** [Date & time data types - Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/data-types-datetime)

### Binary Types

| User Writes | DESCRIBE TABLE Returns |
|---|---|
| `BINARY` | `BINARY(8388608)` |
| `VARBINARY` | `BINARY(8388608)` |

### Semi-Structured Types

These have no aliases — they are returned as-is:

| User Writes | DESCRIBE TABLE Returns |
|---|---|
| `VARIANT` | `VARIANT` |
| `OBJECT` | `OBJECT` |
| `ARRAY` | `ARRAY` |
| `BOOLEAN` | `BOOLEAN` |

## How Canonicalization Works in the Kit

The `_hybrid_canonicalize_type()` macro in `macros/hybrid_table_helpers.sql` normalizes types by:

1. **Exact alias lookup** — Checks a dictionary of known aliases (e.g., `INT` -> `NUMBER(38,0)`)
2. **Parameterized alias rewrite** — Converts `DECIMAL(p,s)` / `NUMERIC(p,s)` to `NUMBER(p,s)`
3. **Pass-through** — Already-canonical types (e.g., `NUMBER(38,0)`) are returned as-is

### Example Flow

During an incremental run on a hybrid table with this config:

```yaml
column_definitions:
  id: "INT NOT NULL"
  name: "VARCHAR(200)"
  created_at: "TIMESTAMP_NTZ"
```

1. `DESCRIBE TABLE` returns: `NUMBER(38,0)`, `VARCHAR(200)`, `TIMESTAMP_NTZ(9)`
2. User-defined types extracted: `INT`, `VARCHAR(200)`, `TIMESTAMP_NTZ`
3. Canonicalization applied to both sides:

| Column | User Type | Canonicalized | DESCRIBE Type | Canonicalized | Match? |
|---|---|---|---|---|---|
| `id` | `INT` | `NUMBER(38,0)` | `NUMBER(38,0)` | `NUMBER(38,0)` | Yes |
| `name` | `VARCHAR(200)` | `VARCHAR(200)` | `VARCHAR(200)` | `VARCHAR(200)` | Yes |
| `created_at` | `TIMESTAMP_NTZ` | `TIMESTAMP_NTZ(9)` | `TIMESTAMP_NTZ(9)` | `TIMESTAMP_NTZ(9)` | Yes |

Without canonicalization, `INT` != `NUMBER(38,0)` and `TIMESTAMP_NTZ` != `TIMESTAMP_NTZ(9)` would both be flagged as type changes, requiring a full refresh on every single run.

## What Happens When a Type Actually Changes

If a user changes `column_definitions` from `INT` to `FLOAT`, canonicalization correctly detects this:

- User type: `FLOAT` -> canonical: `FLOAT`
- Existing type: `NUMBER(38,0)` -> canonical: `NUMBER(38,0)`
- `FLOAT` != `NUMBER(38,0)` -> genuine type change detected

Since Snowflake does not support `ALTER COLUMN TYPE` on hybrid tables, type changes require `--full-refresh`.
