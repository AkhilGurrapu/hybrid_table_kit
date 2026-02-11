# Hybrid Table Materialization — Usage Guide

## Table of Contents

1. [What is a Hybrid Table?](#1-what-is-a-hybrid-table)
2. [Installation](#2-installation)
3. [Your First Hybrid Table](#3-your-first-hybrid-table)
4. [Column Definitions](#4-column-definitions)
5. [Primary Keys](#5-primary-keys)
6. [Secondary Indexes](#6-secondary-indexes)
7. [UNIQUE Constraints](#7-unique-constraints)
8. [FOREIGN KEY Constraints](#8-foreign-key-constraints)
9. [AUTOINCREMENT Columns](#9-autoincrement-columns)
10. [DEFAULT Values](#10-default-values)
11. [Incremental Strategies](#11-incremental-strategies)
12. [Schema Evolution](#12-schema-evolution)
13. [Merge Control](#13-merge-control)
14. [Column Ordering](#14-column-ordering)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. What is a Hybrid Table?

Snowflake Hybrid Tables provide **unistore** capability: a single table type that handles both transactional (OLTP) and analytical (OLAP) workloads. Key differences from regular Snowflake tables:

| Feature | Regular Table | Hybrid Table |
|---------|--------------|--------------|
| Storage engine | Columnar | Row-based |
| PRIMARY KEY | Advisory (not enforced) | **Enforced** (required) |
| UNIQUE constraints | Advisory | **Enforced** |
| FOREIGN KEY | Advisory | **Enforced** |
| Secondary indexes | Not available | Available |
| Point lookups | Milliseconds-seconds | **Single-digit milliseconds** |
| Row-level locking | No | Yes |

**When to use hybrid tables:**
- Low-latency lookups by primary key (e.g., serving APIs)
- Tables that need enforced referential integrity
- Mixed OLTP+OLAP workloads
- Dimension tables with enforced FK relationships

---

## 2. Installation

Copy two files into your dbt project's `macros/` directory:

```bash
cp hybrid_table_kit/macros/hybrid_table.sql       your_project/macros/
cp hybrid_table_kit/macros/hybrid_table_helpers.sql your_project/macros/
```

That's it. No pip install, no adapter changes, no dbt packages.

**Requirements:**
- dbt-core 1.7+
- dbt-snowflake adapter (standard, unmodified)
- Snowflake account with hybrid table support

---

## 3. Your First Hybrid Table

Create a new `.sql` file in your `models/` directory:

```sql
-- models/customers_hybrid.sql
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'customer_id': 'INT NOT NULL',
        'name': 'VARCHAR(200)',
        'email': 'VARCHAR(255)'
    },
    primary_key=['customer_id']
) }}

SELECT
    customer_id::INT as customer_id,
    email::VARCHAR(255) as email,
    name::VARCHAR(200) as name
FROM {{ ref('raw_customers') }}
```

Run it:

```bash
# First time — creates the hybrid table
dbt run --full-refresh -s customers_hybrid

# Incremental — MERGEs new/updated data
dbt run -s customers_hybrid
```

**Important:** The SELECT column order must match the column_definitions order (alphabetical by default). See [Column Ordering](#14-column-ordering).

---

## 4. Column Definitions

The `column_definitions` config maps column names to their full Snowflake type definitions:

```sql
column_definitions={
    'id': 'INT NOT NULL',
    'name': 'VARCHAR(200)',
    'amount': 'DECIMAL(12,2)',
    'is_active': 'BOOLEAN DEFAULT TRUE',
    'created_at': 'TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()',
    'row_num': 'INT NOT NULL AUTOINCREMENT'
}
```

Each value is a complete Snowflake column definition that can include:
- Data type: `INT`, `VARCHAR(200)`, `DECIMAL(12,2)`, `TIMESTAMP_NTZ`, etc.
- `NOT NULL` constraint
- `DEFAULT <value>` clause
- `AUTOINCREMENT` or `IDENTITY` keyword

**Why explicit column definitions?** Unlike regular dbt tables (which infer types from the SELECT), hybrid tables need explicit DDL for enforced constraints, indexes, and hybrid table creation syntax.

---

## 5. Primary Keys

Every hybrid table requires at least one primary key column:

```sql
-- Single column
primary_key=['id']

-- Composite key
primary_key=['tenant_id', 'user_id']
```

**Rules:**
- All PK columns must exist in `column_definitions`
- PK columns cannot use: VARIANT, ARRAY, OBJECT, GEOGRAPHY, GEOMETRY, VECTOR, or TIMESTAMP_TZ
- PK values must be unique and NOT NULL

---

## 6. Secondary Indexes

Indexes improve lookup performance. They can optionally INCLUDE extra columns for covering queries:

```sql
indexes=[
    -- Simple index
    {'name': 'idx_email', 'columns': ['email']},

    -- Multi-column index
    {'name': 'idx_name_date', 'columns': ['last_name', 'first_name']},

    -- Index with INCLUDE (covering index)
    {'name': 'idx_customer', 'columns': ['customer_id'], 'include': ['name', 'email']}
]
```

**INCLUDE columns** are stored with the index, allowing queries that only need those columns to skip the base table entirely.

---

## 7. UNIQUE Constraints

UNIQUE constraints are **enforced** on hybrid tables:

```sql
unique_constraints=[
    {'name': 'uq_email', 'columns': ['email']},
    {'columns': ['tenant_id', 'username']}  -- unnamed constraint
]
```

An INSERT or MERGE that violates uniqueness will fail.

---

## 8. FOREIGN KEY Constraints

FKs are **enforced** on hybrid tables. The referenced table must also be a hybrid table with a matching PK.

### Same-schema FK

```sql
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'order_id': 'INT NOT NULL',
        'customer_id': 'INT NOT NULL',
        'amount': 'DECIMAL(12,2)'
    },
    primary_key=['order_id'],
    foreign_keys=[
        {
            'name': 'fk_order_customer',
            'columns': ['customer_id'],
            'references': {
                'table': 'customers_hybrid',
                'columns': ['id']
            }
        }
    ]
) }}

-- depends_on: {{ ref('customers_hybrid') }}

SELECT ...
```

### Cross-schema FK (dot notation)

Reference tables in other schemas using 2-part or 3-part notation:

```sql
foreign_keys=[
    {
        'columns': ['product_id'],
        'references': {
            'table': 'OTHER_SCHEMA.products',          -- 2-part
            'columns': ['id']
        }
    }
]

-- or 3-part:
'table': 'MY_DB.OTHER_SCHEMA.products'
```

### Multi-column FK

```sql
foreign_keys=[
    {
        'name': 'fk_line_order',
        'columns': ['order_date', 'order_id'],
        'references': {
            'table': 'orders_hybrid',
            'columns': ['order_date', 'order_id']
        }
    }
]
```

### Critical FK Rules

1. **Use plain strings, not ref()** in FK `references.table`. Use `-- depends_on:` for build order.
2. **FK constraints must be defined at CREATE TABLE time.** The materialization handles this automatically (uses non-CTAS path when FKs are present).
3. **The referenced table must exist** before the FK table is created.
4. **Referenced columns must match** the PK of the referenced hybrid table.

---

## 9. AUTOINCREMENT Columns

AUTOINCREMENT columns have their values auto-generated by Snowflake:

```sql
column_definitions={
    'row_id': 'INT NOT NULL AUTOINCREMENT',
    'name': 'VARCHAR(200)',
    'email': 'VARCHAR(200)'
}

-- Your SELECT should NOT include AUTOINCREMENT columns
SELECT
    email::VARCHAR(200) as email,
    name::VARCHAR(200) as name
FROM ...
```

**How the materialization handles AUTOINCREMENT:**
- Uses non-CTAS path (CREATE empty + INSERT)
- Excludes AUTOINCREMENT columns from INSERT and MERGE statements
- If ALL PKs are AUTOINCREMENT, incremental runs use INSERT-only (no MERGE possible since PKs aren't in source data)

---

## 10. DEFAULT Values

DEFAULT values are specified in the column definition:

```sql
column_definitions={
    'status': 'VARCHAR(50) DEFAULT ''active''',
    'priority': 'INT DEFAULT 0',
    'is_active': 'BOOLEAN DEFAULT TRUE',
    'created_at': 'TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()'
}
```

Defaults are applied by Snowflake when a column value is not provided in an INSERT.

---

## 11. Incremental Strategies

### MERGE (default)

Matches source rows to target by primary key. Updates existing rows and inserts new ones.

```sql
{{ config(
    materialized='hybrid_table',
    incremental_strategy='merge',
    ...
) }}
```

On incremental runs, generates:
```sql
MERGE INTO target AS target
USING (SELECT ...) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET target.name = source.name, ...
WHEN NOT MATCHED THEN INSERT (id, name, ...) VALUES (source.id, source.name, ...)
```

### delete+insert

Deletes matching rows first, then inserts all source rows. Useful when you want to fully replace matched rows.

```sql
{{ config(
    materialized='hybrid_table',
    incremental_strategy='delete+insert',
    ...
) }}
```

On incremental runs, generates:
```sql
DELETE FROM target USING (SELECT ...) AS source WHERE target.id = source.id;
INSERT INTO target (id, name, ...) SELECT ...;
```

**Requires** at least one non-AUTOINCREMENT primary key column.

---

## 12. Schema Evolution

Control what happens when your model's columns change between runs:

### ignore (default)

No schema changes applied. New columns in your model are silently ignored; old columns remain.

### fail

Raises an error if any schema change is detected:

```sql
on_schema_change='fail'
```

### append_new_columns

Adds new columns via ALTER TABLE. Raises an error if you try to remove columns:

```sql
on_schema_change='append_new_columns'
```

### sync_all_columns

Adds new columns AND drops removed columns:

```sql
on_schema_change='sync_all_columns'
```

**Type changes** (e.g., changing a column from INT to VARCHAR) always require `--full-refresh` regardless of on_schema_change mode.

---

## 13. Merge Control

### merge_exclude_columns

Prevents specific columns from being updated on MERGE:

```sql
merge_exclude_columns=['created_at', 'created_by']
```

On WHEN MATCHED, the listed columns will not be updated. Useful for preserving original creation timestamps.

### merge_update_columns

Explicitly list which columns to update on MERGE (overrides the default of "all non-PK columns"):

```sql
merge_update_columns=['name', 'email', 'updated_at']
```

---

## 14. Column Ordering

**CTAS (CREATE ... AS SELECT) maps columns by position, not by name.** This means your SELECT column order must match the column definition order.

**Default order: alphabetical by column name.**

If your `column_definitions` has keys `id`, `amount`, `name`, the alphabetical order is `amount`, `id`, `name`. Your SELECT must output them in that order:

```sql
SELECT
    amount::DECIMAL(12,2) as amount,
    id::INT as id,
    name::VARCHAR(200) as name
FROM ...
```

**Custom order:** Use `column_order` to override:

```sql
{{ config(
    column_definitions={
        'id': 'INT NOT NULL',
        'name': 'VARCHAR(200)',
        'amount': 'DECIMAL(12,2)'
    },
    column_order=['id', 'name', 'amount'],
    primary_key=['id']
) }}

SELECT
    id::INT as id,
    name::VARCHAR(200) as name,
    amount::DECIMAL(12,2) as amount
FROM ...
```

---

## 15. Troubleshooting

### "Hybrid tables require a PRIMARY KEY"
Every hybrid table must have at least one PK column. Add `primary_key=['your_pk_column']` to the config.

### "column 'X' not found in column_definitions"
A column referenced in `primary_key`, `indexes`, `unique_constraints`, or `foreign_keys` doesn't exist in `column_definitions`. Check spelling and case.

### "does not exist" error for FK reference
The referenced table hasn't been built yet, or the table name is misspelled. Ensure:
1. The referenced table exists in Snowflake
2. You added `-- depends_on: {{ ref('referenced_table') }}` to your model
3. The table name in `references.table` matches the actual Snowflake table name

### "Unique and foreign-key constraints can only be defined at table creation time"
This is a Snowflake limitation. FK constraints cannot be added via ALTER TABLE on hybrid tables. The materialization handles this by using the non-CTAS path (CREATE + INSERT) when FKs are present.

### SELECT column count mismatch
Your SELECT must output the same number of columns as `column_definitions` (minus any AUTOINCREMENT columns). Check that column_definitions and your SELECT match.

### Column order mismatch (wrong data in columns)
CTAS maps by position. If your columns appear in the wrong order, ensure your SELECT matches the alphabetical sort of column_definitions keys (or uses a custom `column_order`).

### "Schema changes detected... on_schema_change=fail"
You changed column_definitions but have `on_schema_change='fail'`. Either:
1. Run with `--full-refresh`
2. Change to `on_schema_change='sync_all_columns'`
3. Revert the column change

### Type change requires full refresh
Changing a column's data type (e.g., INT to VARCHAR) always requires `--full-refresh` because Snowflake doesn't support ALTER COLUMN TYPE on hybrid tables.
