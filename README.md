# Snowflake Hybrid Table — dbt Custom Materialization

A **pure-macro** custom materialization that lets you create and manage [Snowflake Hybrid Tables](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table) in any dbt project.

**No adapter fork. No pip install. Just copy two files.**

## Quick Start

### 1. Copy the macros

Copy the `macros/` folder into your dbt project:

```
your_dbt_project/
  macros/
    hybrid_table.sql            ← main materialization
    hybrid_table_helpers.sql    ← helper macros
  models/
    ...
```

### 2. Create a model

```sql
-- models/my_hybrid_table.sql
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'name': 'VARCHAR(200)',
        'email': 'VARCHAR(255)',
        'created_at': 'TIMESTAMP_NTZ'
    },
    primary_key=['id'],
    indexes=[
        {'name': 'idx_email', 'columns': ['email']}
    ]
) }}

SELECT
    created_at::TIMESTAMP_NTZ as created_at,
    email::VARCHAR(255) as email,
    id::INT as id,
    name::VARCHAR(200) as name
FROM {{ ref('raw_users') }}
```

### 3. Run it

```bash
# First time (creates the hybrid table)
dbt run --full-refresh -s my_hybrid_table

# Subsequent runs (MERGE into existing table)
dbt run -s my_hybrid_table
```

## What Are Hybrid Tables?

Snowflake Hybrid Tables provide **unistore** capability — combining transactional (OLTP) and analytical (OLAP) workloads in a single table. They support:

- **Enforced PRIMARY KEY** (required)
- **Enforced UNIQUE constraints**
- **Enforced FOREIGN KEY constraints** (referencing other hybrid tables)
- **Secondary indexes** with optional INCLUDE columns
- **Row-level locking** for concurrent DML
- **Low-latency point lookups** (single-digit millisecond)

## Features

| Feature | Supported |
|---------|-----------|
| Primary Key (single & composite) | Yes |
| UNIQUE constraints | Yes |
| FOREIGN KEY constraints | Yes |
| Cross-schema FK references | Yes |
| Secondary indexes with INCLUDE | Yes |
| AUTOINCREMENT / IDENTITY | Yes |
| DEFAULT values | Yes |
| Table COMMENT | Yes |
| Incremental: merge | Yes |
| Incremental: delete+insert | Yes |
| merge_exclude_columns | Yes |
| merge_update_columns | Yes |
| on_schema_change: ignore | Yes |
| on_schema_change: fail | Yes |
| on_schema_change: append_new_columns | Yes |
| on_schema_change: sync_all_columns | Yes |
| Grants (apply_grants) | Yes |
| persist_docs | Yes |
| Pre/post hooks | Yes |

## Config Reference

```sql
{{ config(
    materialized='hybrid_table',

    -- REQUIRED: Column names → Snowflake type definitions
    column_definitions={
        'id': 'INT NOT NULL',
        'name': 'VARCHAR(200)',
        'amount': 'DECIMAL(12,2)',
        'auto_id': 'INT NOT NULL AUTOINCREMENT',
        'status': 'VARCHAR(50) DEFAULT ''active'''
    },

    -- REQUIRED: One or more PK columns (must exist in column_definitions)
    primary_key=['id'],

    -- OPTIONAL: Secondary indexes
    indexes=[
        {'name': 'idx_name', 'columns': ['name']},
        {'name': 'idx_amount', 'columns': ['amount'], 'include': ['status']}
    ],

    -- OPTIONAL: UNIQUE constraints
    unique_constraints=[
        {'name': 'uq_email', 'columns': ['email']}
    ],

    -- OPTIONAL: FOREIGN KEY constraints
    foreign_keys=[
        {
            'name': 'fk_customer',
            'columns': ['customer_id'],
            'references': {
                'table': 'customers_hybrid',
                'columns': ['id']
            }
        }
    ],

    -- OPTIONAL: Incremental strategy (default: 'merge')
    incremental_strategy='merge',  -- or 'delete+insert'

    -- OPTIONAL: Schema evolution (default: 'ignore')
    on_schema_change='ignore',  -- 'fail', 'append_new_columns', 'sync_all_columns'

    -- OPTIONAL: Merge behavior control
    merge_exclude_columns=['created_at'],
    -- merge_update_columns=['name', 'amount'],  -- alternative: explicit list

    -- OPTIONAL: Table comment
    comment='My hybrid table description',

    -- OPTIONAL: Explicit column order (default: alphabetical)
    -- column_order=['id', 'name', 'amount', 'status'],
) }}
```

## Critical Rules

### 1. SELECT column order must match column_definitions order

By default, columns are sorted **alphabetically** by name. Your SELECT must output columns in the same order:

```sql
column_definitions={
    'amount': 'DECIMAL(12,2)',  -- alphabetically first
    'id': 'INT NOT NULL',
    'name': 'VARCHAR(200)'
}

-- SELECT must match alphabetical order
SELECT
    amount::DECIMAL(12,2) as amount,
    id::INT as id,
    name::VARCHAR(200) as name
FROM ...
```

Or use `column_order` to specify a custom order:

```sql
column_definitions={...},
column_order=['id', 'name', 'amount'],

SELECT id, name, amount FROM ...
```

### 2. FK tables must use plain strings, not ref()

Foreign key references use plain table name strings. Use `-- depends_on:` to ensure build order:

```sql
foreign_keys=[{
    'columns': ['customer_id'],
    'references': {
        'table': 'customers_hybrid',  -- plain string, NOT ref()
        'columns': ['id']
    }
}]

-- depends_on: {{ ref('customers_hybrid') }}
```

### 3. AUTOINCREMENT columns are excluded from INSERT/MERGE

Snowflake auto-generates values for AUTOINCREMENT columns. Your SELECT should NOT include them:

```sql
column_definitions={
    'row_id': 'INT NOT NULL AUTOINCREMENT',  -- auto-generated
    'name': 'VARCHAR(200)',
    'email': 'VARCHAR(200)'
}

-- Only select non-AUTOINCREMENT columns (alphabetical order)
SELECT
    email::VARCHAR(200) as email,
    name::VARCHAR(200) as name
FROM ...
```

## Folder Structure

```
hybrid_table_kit/
  README.md                          ← You are here
  macros/                            ← COPY THIS to your project
    hybrid_table.sql                 ← Main materialization
    hybrid_table_helpers.sql         ← Type canonicalization, schema detection
  examples/
    dbt_project.yml                  ← Example project config
    profiles_example.yml             ← Example Snowflake profile
    models/                          ← 11 example models
      basic_hybrid.sql               ← Simple PK + indexes
      composite_pk_hybrid.sql        ← Composite PK + INDEX INCLUDE
      unique_constraint_hybrid.sql   ← UNIQUE constraint
      default_values_hybrid.sql      ← DEFAULT values
      orders_with_fk_hybrid.sql      ← Single-column FK
      multi_col_fk_hybrid.sql        ← Multi-column FK
      cross_schema_fk_hybrid.sql     ← Cross-schema FK (3-part notation)
      autoincrement_hybrid.sql       ← AUTOINCREMENT / IDENTITY
      delete_insert_hybrid.sql       ← delete+insert strategy
      schema_change_hybrid.sql       ← Schema evolution (sync_all_columns)
      merge_control_hybrid.sql       ← merge_exclude_columns
    seeds/
      raw_customers.csv
      raw_orders.csv
      raw_products.csv
  docs/
    USAGE_GUIDE.md                   ← Comprehensive usage guide
    HOW_IT_WORKS.md                  ← Internal architecture
    TESTING_REPORT.md                ← Live Snowflake test results
```

## Requirements

- **dbt-core** 1.7+
- **dbt-snowflake** adapter (standard, unmodified — any recent version)
- Snowflake account with hybrid table support (available in most regions)

## Further Reading

- [docs/USAGE_GUIDE.md](docs/USAGE_GUIDE.md) — Detailed usage guide with all config options
- [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md) — How the materialization works internally
- [docs/TESTING_REPORT.md](docs/TESTING_REPORT.md) — Live Snowflake testing results (11 models, all passing)
- [Snowflake Hybrid Tables Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table)
