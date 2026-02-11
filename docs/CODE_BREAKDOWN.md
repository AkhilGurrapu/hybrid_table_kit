# Code Breakdown — Hybrid Table Materialization Macros

A detailed walkthrough of every section of code in the `macros/` directory, written for someone new to dbt materializations.

**Files covered:**
- `macros/hybrid_table.sql` — The main materialization (519 lines)
- `macros/hybrid_table_helpers.sql` — Supporting helper macros (235 lines)

---

## Background: What Is a dbt Materialization?

In dbt, a **materialization** is a Jinja template that controls how a model's SQL query gets turned into an actual database object. dbt ships with built-in materializations: `table`, `view`, `incremental`, `ephemeral`. Each one generates different DDL/DML.

When you write a dbt model like:

```sql
{{ config(materialized='table') }}
SELECT * FROM raw.customers
```

The `table` materialization wraps your SELECT in `CREATE TABLE AS SELECT ...`. The `incremental` materialization checks if the table already exists and does a `MERGE` or `INSERT` instead of recreating it.

A **custom materialization** is one you write yourself. That is what `hybrid_table.sql` is — it teaches dbt how to create and manage Snowflake Hybrid Tables, which dbt does not natively support yet.

---

## File 1: `macros/hybrid_table.sql`

This is the main materialization file. It registers a new materialization type called `hybrid_table` that is available when using the Snowflake adapter.

### Line 36: Materialization Declaration

```jinja
{% materialization hybrid_table, adapter='snowflake' %}
```

This is how dbt registers a custom materialization. The `adapter='snowflake'` parameter means this materialization is only available when using the dbt-snowflake adapter. If someone tries to use `materialized='hybrid_table'` with a Postgres adapter, dbt will throw an error.

The matching closing tag is at line 519:

```jinja
{% endmaterialization %}
```

Everything between these two lines is the materialization logic.

---

### Lines 38–41: Setup Variables

```jinja
{% set query_tag = set_query_tag() %}
{% set existing_relation = load_cached_relation(this) %}
{% set target_relation = this %}
{{ run_hooks(pre_hooks) }}
```

| Variable | What It Does |
|---|---|
| `query_tag` | Sets a Snowflake query tag for tracking/auditing. Comes from dbt's built-in query tagging. |
| `existing_relation` | Checks dbt's metadata cache to see if this table already exists in Snowflake. Returns `none` if the table has never been created, or a relation object if it exists. |
| `target_relation` | `this` is a dbt variable that refers to the fully-qualified name of the current model (e.g., `MY_DB.MY_SCHEMA.MY_MODEL`). |
| `run_hooks(pre_hooks)` | Executes any SQL the user defined in `pre_hook` config before the materialization runs. |

The `existing_relation` check is critical — it determines whether we do a **full create** (table doesn't exist) or an **incremental update** (table already exists).

---

### Lines 45–88: Configuration Loading

This block reads all user-provided configuration from the model's `config()` block.

```jinja
{# Required #}
{% set column_definitions = config.get('column_definitions', {}) %}
{% set primary_key = config.get('primary_key', []) %}
```

`column_definitions` is the core config. Unlike regular dbt models where dbt infers columns from your SELECT, hybrid tables require you to explicitly declare every column and its type. This is because hybrid tables need constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY) which must be defined in the `CREATE TABLE` DDL.

Example of what a user writes in their model:

```sql
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'name': 'VARCHAR(200)',
        'email': 'VARCHAR(500) NOT NULL',
        'created_at': 'TIMESTAMP_NTZ'
    },
    primary_key=['id']
) }}

SELECT id, name, email, created_at FROM {{ source('raw', 'users') }}
```

The rest of the config loading:

```jinja
{# Optional — constraints and indexes #}
{% set indexes = config.get('indexes', []) %}
{% set unique_constraints = config.get('unique_constraints', []) %}
{% set foreign_keys = config.get('foreign_keys', []) %}

{# Optional — behavior #}
{% set force_ctas = config.get('force_ctas', false) %}
{% set merge_exclude_columns = config.get('merge_exclude_columns', []) %}
{% set merge_update_columns = config.get('merge_update_columns', []) %}
{% set full_refresh_mode = should_full_refresh() %}
{% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
{% set incremental_strategy = (config.get('incremental_strategy') or 'merge') | lower %}
```

| Config | Purpose | Default |
|---|---|---|
| `indexes` | Secondary indexes for fast lookups on non-PK columns | `[]` |
| `unique_constraints` | Snowflake-enforced UNIQUE constraints | `[]` |
| `foreign_keys` | FK relationships to other hybrid tables | `[]` |
| `force_ctas` | Force CREATE ... AS SELECT even on existing table | `false` |
| `merge_exclude_columns` | Columns to skip during MERGE UPDATE | `[]` |
| `merge_update_columns` | Explicit list of columns to UPDATE during MERGE | `[]` |
| `full_refresh_mode` | `true` when user passes `--full-refresh` flag | From CLI |
| `on_schema_change` | How to handle column additions/removals between runs | `'ignore'` |
| `incremental_strategy` | How incremental data is applied: `merge` or `delete+insert` | `'merge'` |

`should_full_refresh()` is a dbt built-in that returns `true` when the user ran `dbt run --full-refresh`.

`incremental_validate_on_schema_change()` is a dbt built-in that validates the `on_schema_change` value is one of: `ignore`, `fail`, `append_new_columns`, `sync_all_columns`.

---

### Lines 66–72: AUTOINCREMENT Detection

```jinja
{% set autoincrement_columns = [] %}
{% for column_name, column_definition in column_definitions.items() %}
    {% set column_definition_upper = column_definition | upper %}
    {% if 'AUTOINCREMENT' in column_definition_upper or 'IDENTITY' in column_definition_upper %}
        {% do autoincrement_columns.append(column_name) %}
    {% endif %}
{% endfor %}
```

This scans every column definition for the keywords `AUTOINCREMENT` or `IDENTITY`. Snowflake treats these the same — they auto-generate sequential integer values.

Why this matters:
- AUTOINCREMENT columns cannot appear in `INSERT ... SELECT` statements (Snowflake generates the values automatically)
- AUTOINCREMENT columns cannot appear in `CREATE TABLE ... AS SELECT` (CTAS)
- They need special handling in MERGE statements (can't update them, can't include them in INSERT list)

The result is a list like `['id']` that is checked throughout the rest of the materialization.

---

### Lines 74–81: CTAS vs Non-CTAS Path Decision

```jinja
{% if force_non_ctas is not none %}
    {% set use_non_ctas_create = force_non_ctas %}
{% else %}
    {% set use_non_ctas_create = autoincrement_columns | length > 0 or foreign_keys | length > 0 %}
{% endif %}
```

There are two ways to create a hybrid table and populate it with data:

**CTAS (CREATE TABLE ... AS SELECT)** — one statement:
```sql
CREATE HYBRID TABLE my_table (
    id INT NOT NULL,
    name VARCHAR(200),
    PRIMARY KEY (id)
) AS (
    SELECT id, name FROM source_table
);
```

**Non-CTAS (CREATE empty + INSERT)** — two statements:
```sql
CREATE HYBRID TABLE my_table (
    id INT NOT NULL AUTOINCREMENT,
    name VARCHAR(200),
    PRIMARY KEY (id),
    FOREIGN KEY (dept_id) REFERENCES departments(id)
);

INSERT INTO my_table (name) SELECT name FROM source_table;
```

Non-CTAS is required when:
1. **FOREIGN KEY constraints exist** — Snowflake requires FK constraints to be defined at `CREATE TABLE` time. CTAS syntax does not support FK clauses.
2. **AUTOINCREMENT columns exist** — CTAS would try to insert data into the auto-generated column, which fails. The non-CTAS path creates the table first, then inserts only the non-AUTOINCREMENT columns.

---

### Lines 88: Column Ordering

```jinja
{% set column_order = config.get('column_order', column_definitions.keys() | sort | list) %}
```

This determines the order columns appear in the `CREATE TABLE` DDL. If the user does not provide an explicit `column_order`, the columns are sorted alphabetically.

This is important for CTAS because `CREATE TABLE ... AS SELECT` maps columns **by position**, not by name. If your DDL says `(id, name, email)` but your SELECT returns `(email, id, name)`, the data goes into the wrong columns. The user's SELECT must match this order.

---

### Lines 90–97: Incremental Strategy Validation

```jinja
{% set supported_incremental_strategies = ['merge', 'delete+insert'] %}
{% if incremental_strategy not in supported_incremental_strategies %}
    {{ exceptions.raise_compiler_error(
        "Invalid incremental_strategy '" ~ incremental_strategy ~ "' for hybrid_table. ..."
    ) }}
{% endif %}
```

Only two strategies are supported. If someone writes `incremental_strategy='append'`, dbt will error at compile time before touching Snowflake.

---

### Lines 99–177: Validation Block

This is a series of compile-time checks that run before any SQL is sent to Snowflake. They prevent invalid configurations from producing cryptic Snowflake errors.

**Required config check (lines 101–126):**

```jinja
{% if column_definitions | length == 0 %}
    {{ exceptions.raise_compiler_error("Hybrid table materialization requires 'column_definitions' ...") }}
{% endif %}

{% if primary_key | length == 0 %}
    {{ exceptions.raise_compiler_error("Hybrid tables require a PRIMARY KEY constraint ...") }}
{% endif %}
```

Snowflake hybrid tables physically require a primary key. Without one, the `CREATE HYBRID TABLE` statement would fail. These checks catch it early with a clear error message.

**Primary key type validation (lines 128–146):**

```jinja
{% set unsupported_constraint_types = ['VARIANT', 'ARRAY', 'OBJECT', 'GEOGRAPHY', 'GEOMETRY', 'VECTOR', 'TIMESTAMP_TZ'] %}

{% for pk_col in primary_key %}
    {% if pk_col not in column_definitions %}
        {{ exceptions.raise_compiler_error("Primary key column '" ~ pk_col ~ "' not found in column_definitions.") }}
    {% endif %}
    {% set pk_type = column_definitions[pk_col] | upper %}
    {% for unsupported in unsupported_constraint_types %}
        {% if unsupported in pk_type %}
            {{ exceptions.raise_compiler_error("Primary key column '" ~ pk_col ~ "' uses unsupported data type ...") }}
        {% endif %}
    {% endfor %}
{% endfor %}
```

Snowflake does not allow semi-structured types (`VARIANT`, `ARRAY`, `OBJECT`), geospatial types, or `TIMESTAMP_TZ` as primary key columns. This validates that constraint before sending SQL to Snowflake.

**Unique constraint and index validation (lines 148–177):**

Checks that every column referenced in `unique_constraints` and `indexes` actually exists in `column_definitions`. Also validates INCLUDE columns on indexes (a Snowflake feature where additional non-indexed columns are stored alongside the index for covering queries).

---

### Lines 179–294: Foreign Key Validation

This is the longest validation block because foreign keys have the most things that can go wrong.

**Structure validation (lines 182–221):**

```jinja
{% for fk in foreign_keys %}
    {% if fk.columns is not defined or fk.columns | length == 0 %}
        {{ exceptions.raise_compiler_error("Foreign key configuration requires 'columns' ...") }}
    {% endif %}

    {% for fk_col in fk.columns %}
        {% if fk_col not in column_definitions %}
            {{ exceptions.raise_compiler_error("Foreign key column '" ~ fk_col ~ "' not found ...") }}
        {% endif %}
    {% endfor %}

    {% if fk.references is not defined %}
        {{ exceptions.raise_compiler_error("Foreign key configuration requires a 'references' dictionary ...") }}
    {% endif %}

    {% if fk_reference.columns | length != fk.columns | length %}
        {{ exceptions.raise_compiler_error("Foreign key column count mismatch ...") }}
    {% endif %}
```

Validates that:
- FK columns exist in this model's `column_definitions`
- A `references` block is provided
- The number of FK columns matches the number of referenced columns (e.g., a composite FK `(a, b)` must reference exactly two columns `(x, y)`)

**Dot notation parsing (lines 234–248):**

```jinja
{% set reference_identifier = reference_identifier_value %}
{% if reference_identifier is string %}
    {% set identifier_parts = reference_identifier.split('.') %}
    {% if identifier_parts | length == 3 %}
        {% set reference_database = identifier_parts[0] %}
        {% set reference_schema = identifier_parts[1] %}
        {% set reference_identifier = identifier_parts[2] %}
    {% elif identifier_parts | length == 2 %}
        {% if fk_reference.schema is not defined %}
            {% set reference_schema = identifier_parts[0] %}
        {% endif %}
        {% set reference_identifier = identifier_parts[1] %}
    {% endif %}
{% endif %}
```

This allows users to reference tables across schemas or databases using dot notation:

| User Writes | Parsed As |
|---|---|
| `table: 'DEPARTMENTS'` | Same database, same schema, table `DEPARTMENTS` |
| `table: 'OTHER_SCHEMA.DEPARTMENTS'` | Same database, schema `OTHER_SCHEMA`, table `DEPARTMENTS` |
| `table: 'OTHER_DB.OTHER_SCHEMA.DEPARTMENTS'` | Database `OTHER_DB`, schema `OTHER_SCHEMA`, table `DEPARTMENTS` |

**Runtime existence check (lines 258–283):**

```jinja
{% set referenced_relation = adapter.get_relation(
    database=reference_database,
    schema=reference_schema,
    identifier=reference_identifier
) %}

{% if referenced_relation is none %}
    {{ exceptions.raise_compiler_error("Foreign key references relation ... which does not exist.") }}
{% endif %}
```

This actually queries Snowflake's information schema to verify the referenced table exists and that the referenced columns exist on that table. Without this check, you would get a Snowflake DDL error at runtime with a less clear message.

**Normalized FK output (lines 285–294):**

```jinja
{% set normalized_fk = {
    'name': fk.name if fk.name is defined else none,
    'columns': fk.columns,
    'references_relation': referenced_relation,
    'references_columns': fk_reference.columns
} %}
{% do validated_foreign_keys.append(normalized_fk) %}
```

After validation, each FK is normalized into a clean dictionary with a resolved relation object (instead of raw string references). This normalized form is used later in the CREATE TABLE DDL.

---

### Lines 296–304: Relation Type Check

```jinja
{% if existing_relation is not none and existing_relation.type not in ('table', 'hybrid_table') %}
    {{ log("Dropping " ~ existing_relation ~ " (type: " ~ existing_relation.type ~ ") ...", info=True) }}
    {% do run_query('DROP ' ~ existing_relation.type ~ ' IF EXISTS ' ~ existing_relation) %}
    {% set existing_relation = none %}
{% endif %}
```

If the model name already exists but as a different object type (a `VIEW` or `DYNAMIC TABLE`), it gets dropped so the hybrid table can be created in its place.

The check `not in ('table', 'hybrid_table')` handles two adapter behaviors:
- The standard dbt-snowflake adapter reports hybrid tables as `type='table'`
- A forked adapter may report them as `type='hybrid_table'`

Both are acceptable — any other type gets dropped.

The code uses `run_query('DROP ...')` instead of `adapter.drop_relation()` because the forked adapter's `drop_relation()` generates `DROP HYBRID TABLE` which Snowflake does not accept (the correct SQL is just `DROP TABLE`).

---

### Lines 306–390: Full Refresh / Initial Create Path

```jinja
{% if existing_relation is none or full_refresh_mode or force_ctas %}
```

This path runs when:
- The table does not exist yet (first run)
- The user passed `--full-refresh`
- The user set `force_ctas=true`

#### Non-CTAS Path (lines 311–359)

```jinja
{% if use_non_ctas_create %}
    {% call statement('create_hybrid_table') %}
        CREATE OR REPLACE HYBRID TABLE {{ target_relation }} (
            {% for column in column_order %}
                {{ column }} {{ column_definitions[column] }}{% if not loop.last %},{% endif %}
            {% endfor %}

            {% if primary_key %}
                , PRIMARY KEY ({{ primary_key | join(', ') }})
            {% endif %}

            {% for unique in unique_constraints %}
                , {% if unique.name is defined %}CONSTRAINT {{ unique.name }} {% endif %}UNIQUE ({{ unique.columns | join(', ') }})
            {% endfor %}

            {% for fk in foreign_keys %}
                , {% if fk.name is not none %}CONSTRAINT {{ fk.name }} {% endif %}FOREIGN KEY ({{ fk.columns | join(', ') }}) REFERENCES {{ fk.references_relation }} ({{ fk.references_columns | join(', ') }})
            {% endfor %}

            {% for index in indexes %}
                , INDEX {{ index.name }} ({{ index.columns | join(', ') }}){% if index.include is defined %} INCLUDE ({{ index.include | join(', ') }}){% endif %}
            {% endfor %}
        )
        {% if table_comment %}
        COMMENT = '{{ table_comment | replace("'", "''") }}'
        {% endif %}
    {% endcall %}
```

This generates SQL like:

```sql
CREATE OR REPLACE HYBRID TABLE MY_DB.MY_SCHEMA.MY_MODEL (
    id INT NOT NULL AUTOINCREMENT,
    dept_id INT NOT NULL,
    name VARCHAR(200),
    PRIMARY KEY (id),
    FOREIGN KEY (dept_id) REFERENCES MY_DB.MY_SCHEMA.DEPARTMENTS (id),
    INDEX idx_dept (dept_id)
)
```

Then it inserts data, **excluding AUTOINCREMENT columns**:

```jinja
{% set insert_columns = [] %}
{% for col in column_order %}
    {% if col not in autoincrement_columns %}
        {% do insert_columns.append(col) %}
    {% endif %}
{% endfor %}

{% call statement('main') %}
    INSERT INTO {{ target_relation }} ({{ insert_columns | join(', ') }})
    {{ sql }}
{% endcall %}
```

`{{ sql }}` is the user's SELECT query from their model file. The generated SQL becomes:

```sql
INSERT INTO MY_DB.MY_SCHEMA.MY_MODEL (dept_id, name)
SELECT dept_id, name FROM source_table
```

Note: `{% call statement('main') %}` is required. dbt expects every materialization to have a statement named `'main'`. Without it, dbt will error.

#### CTAS Path (lines 361–390)

```jinja
{% call statement('main') %}
    CREATE OR REPLACE HYBRID TABLE {{ target_relation }} (
        {% for column in column_order %}
            {{ column }} {{ column_definitions[column] }}{% if not loop.last %},{% endif %}
        {% endfor %}
        ...constraints...
    )
    AS (
        {{ sql }}
    )
{% endcall %}
```

This generates a single statement:

```sql
CREATE OR REPLACE HYBRID TABLE MY_DB.MY_SCHEMA.MY_MODEL (
    id INT NOT NULL,
    name VARCHAR(200),
    PRIMARY KEY (id),
    UNIQUE (email),
    INDEX idx_name (name)
)
AS (
    SELECT id, name FROM source_table
)
```

This is simpler but cannot include FOREIGN KEY clauses or AUTOINCREMENT columns.

---

### Lines 394–504: Incremental Update Path

```jinja
{% else %}
    {# ========== INCREMENTAL UPDATE ========== #}
```

This runs when the table already exists and the user did not pass `--full-refresh`.

#### Schema Change Detection (lines 398–401)

```jinja
{% if on_schema_change != 'ignore' %}
    {% set schema_changes = _hybrid_detect_schema_changes(existing_relation, column_definitions) %}
    {{ _hybrid_apply_schema_changes(on_schema_change, target_relation, schema_changes) }}
{% endif %}
```

Calls the helper macros (covered below in File 2) to detect if the user's `column_definitions` differ from what currently exists in Snowflake. If columns were added or removed, it applies ALTER TABLE statements before the incremental data load.

#### Merge Strategy (lines 403–460)

**Determining which columns to update (lines 405–418):**

```jinja
{% if merge_update_columns | length > 0 %}
    {% set update_columns = merge_update_columns %}
{% else %}
    {% set update_columns = [] %}
    {% for col in column_order %}
        {% set col_def = column_definitions[col] | upper %}
        {% if col not in primary_key
           and col not in merge_exclude_columns
           and 'AUTOINCREMENT' not in col_def
           and 'IDENTITY' not in col_def %}
            {% do update_columns.append(col) %}
        {% endif %}
    {% endfor %}
{% endif %}
```

If the user provided `merge_update_columns`, use that list. Otherwise, auto-compute it by taking all columns except:
- Primary key columns (you don't update the key you're matching on)
- Columns in `merge_exclude_columns` (user explicitly excluded them)
- AUTOINCREMENT/IDENTITY columns (Snowflake generates these, you can't update them)

**Building the MERGE statement (lines 443–459):**

```jinja
{% call statement('main') %}
    MERGE INTO {{ target_relation }} AS target
    USING ({{ sql }}) AS source
    ON {% for pk in merge_keys %}
        target.{{ pk }} = source.{{ pk }}{% if not loop.last %} AND {% endif %}
    {% endfor %}
    {% if update_columns | length > 0 %}
    WHEN MATCHED THEN
        UPDATE SET
        {% for column in update_columns %}
            target.{{ column }} = source.{{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    {% endif %}
    WHEN NOT MATCHED THEN
        INSERT ({{ merge_insert_columns | join(', ') }})
        VALUES ({% for col in merge_insert_columns %}source.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %})
{% endcall %}
```

This generates:

```sql
MERGE INTO MY_DB.MY_SCHEMA.MY_MODEL AS target
USING (SELECT id, name, email FROM new_data) AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.email = source.email
WHEN NOT MATCHED THEN
    INSERT (id, name, email)
    VALUES (source.id, source.name, source.email)
```

**Special case — all PKs are AUTOINCREMENT (lines 436–441):**

```jinja
{% if merge_keys | length == 0 %}
    {% call statement('main') %}
        INSERT INTO {{ target_relation }} ({{ merge_insert_columns | join(', ') }})
        {{ sql }}
    {% endcall %}
{% else %}
```

If every primary key column is AUTOINCREMENT, there is nothing to match on. You can't write `ON target.id = source.id` if `id` is auto-generated and the source doesn't have it. In this case, it falls back to a plain INSERT.

#### Delete+Insert Strategy (lines 462–501)

```jinja
{% call statement('delete_incremental') %}
    DELETE FROM {{ target_relation }} AS target
    USING ({{ sql }}) AS source
    WHERE {% for pk in di_merge_keys %}
        target.{{ pk }} = source.{{ pk }}{% if not loop.last %} AND {% endif %}
    {% endfor %}
{% endcall %}

{% call statement('main') %}
    INSERT INTO {{ target_relation }} ({{ di_insert_columns | join(', ') }})
    {{ sql }}
{% endcall %}
```

Two-step process:
1. Delete all rows from the target where the PK matches the incoming data
2. Insert all incoming rows

This is useful when you want to fully replace matched rows rather than selectively update columns. The generated SQL:

```sql
-- Step 1: Delete matching rows
DELETE FROM MY_DB.MY_SCHEMA.MY_MODEL AS target
USING (SELECT id, name FROM new_data) AS source
WHERE target.id = source.id

-- Step 2: Insert fresh rows
INSERT INTO MY_DB.MY_SCHEMA.MY_MODEL (id, name)
SELECT id, name FROM new_data
```

---

### Lines 506–517: Cleanup and Finalization

```jinja
{{ run_hooks(post_hooks) }}

{% do unset_query_tag(query_tag) %}

{% set grant_config = config.get('grants') %}
{% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

{% do persist_docs(target_relation, model) %}

{{ return({'relations': [target_relation]}) }}
```

| Step | What It Does |
|---|---|
| `run_hooks(post_hooks)` | Execute any SQL the user defined in `post_hook` config |
| `unset_query_tag` | Clears the Snowflake query tag set at the start |
| `apply_grants` | Applies GRANT statements if the user configured `grants` (e.g., `grants={'select': ['ANALYST_ROLE']}`) |
| `persist_docs` | Applies column-level and table-level documentation comments to Snowflake if `persist_docs` config is enabled |
| `return({'relations': [target_relation]})` | Required return value — tells dbt which relations were created/modified |

---

## File 2: `macros/hybrid_table_helpers.sql`

This file contains four helper macros that the main materialization calls. They handle type normalization and schema evolution.

---

### `_hybrid_canonicalize_type(data_type)` — Lines 19–68

**Purpose:** Convert any Snowflake type alias to the canonical form that `DESCRIBE TABLE` returns.

**Why it exists:** When you create a column as `INT`, Snowflake internally stores it as `NUMBER(38,0)`. When you run `DESCRIBE TABLE`, it returns `NUMBER(38,0)`, not `INT`. The materialization needs to compare user-defined types against DESCRIBE output during incremental runs. Without canonicalization, `INT` != `NUMBER(38,0)` would be a false positive "type changed" on every run.

```jinja
{% macro _hybrid_canonicalize_type(data_type) %}
    {% set upper_type = data_type | trim | upper %}

    {% set aliases = {
        'INT': 'NUMBER(38,0)',
        'INTEGER': 'NUMBER(38,0)',
        'BIGINT': 'NUMBER(38,0)',
        'SMALLINT': 'NUMBER(38,0)',
        'TINYINT': 'NUMBER(38,0)',
        'BYTEINT': 'NUMBER(38,0)',
        'FLOAT': 'FLOAT',
        'FLOAT4': 'FLOAT',
        'FLOAT8': 'FLOAT',
        'DOUBLE': 'FLOAT',
        'DOUBLE PRECISION': 'FLOAT',
        'REAL': 'FLOAT',
        'BOOLEAN': 'BOOLEAN',
        'BOOL': 'BOOLEAN',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP_NTZ(9)',
        'TIMESTAMP_NTZ': 'TIMESTAMP_NTZ(9)',
        'TIMESTAMP_LTZ': 'TIMESTAMP_LTZ(9)',
        'TIMESTAMP_TZ': 'TIMESTAMP_TZ(9)',
        'TIME': 'TIME(9)',
        'STRING': 'VARCHAR(16777216)',
        'TEXT': 'VARCHAR(16777216)',
        'VARCHAR': 'VARCHAR(16777216)',
        'BINARY': 'BINARY(8388608)',
        'VARBINARY': 'BINARY(8388608)',
        'VARIANT': 'VARIANT',
        'OBJECT': 'OBJECT',
        'ARRAY': 'ARRAY',
        'DECIMAL': 'NUMBER(38,0)',
        'NUMERIC': 'NUMBER(38,0)',
        'NUMBER': 'NUMBER(38,0)'
    } %}
```

**Step 1: Exact alias lookup.** If the input matches a key exactly, return the canonical form.

```jinja
    {% if upper_type in aliases %}
        {{ return(aliases[upper_type]) }}
    {% endif %}
```

Examples: `INT` → `NUMBER(38,0)`, `STRING` → `VARCHAR(16777216)`, `TIMESTAMP_NTZ` → `TIMESTAMP_NTZ(9)`

**Step 2: Parameterized alias rewrite.** Handles types with precision/scale arguments.

```jinja
    {% if upper_type.startswith('DECIMAL(') or upper_type.startswith('NUMERIC(') %}
        {% set inner = upper_type.split('(')[1].rstrip(')') %}
        {{ return('NUMBER(' ~ inner ~ ')') }}
    {% endif %}
```

Example: `DECIMAL(10,2)` → splits on `(` → gets `10,2)` → strips `)` → gets `10,2` → returns `NUMBER(10,2)`

**Step 3: Pass-through.** If neither alias nor parameterized match, the type is assumed to already be canonical.

```jinja
    {{ return(upper_type) }}
{% endmacro %}
```

Example: `NUMBER(38,0)` comes from DESCRIBE, already canonical, returned as-is.

---

### `_hybrid_extract_data_type(definition)` — Lines 80–91

**Purpose:** Strip column modifiers from a definition string, returning only the data type.

**Why it exists:** In `column_definitions`, users write things like `'INT NOT NULL AUTOINCREMENT'`. To compare types, we need just `INT`, not the entire definition.

```jinja
{% macro _hybrid_extract_data_type(definition) %}
    {% set upper_def = definition | upper %}
    {% set stop_keywords = [' NOT NULL', ' NULL', ' DEFAULT ', ' AUTOINCREMENT', ' IDENTITY', ' COMMENT '] %}
    {% set ns = namespace(end_index = definition | length) %}
    {% for keyword in stop_keywords %}
        {% set pos = upper_def.find(keyword) %}
        {% if pos != -1 and pos < ns.end_index %}
            {% set ns.end_index = pos %}
        {% endif %}
    {% endfor %}
    {{ return(definition[:ns.end_index] | trim) }}
{% endmacro %}
```

It finds the earliest occurrence of any modifier keyword and truncates the string there.

**Example walkthrough** for input `'INT NOT NULL AUTOINCREMENT'`:
1. `upper_def` = `'INT NOT NULL AUTOINCREMENT'`
2. Search for `' NOT NULL'` → found at position 3
3. Search for `' NULL'` → found at position 7 (but 7 > 3, so keep 3)
4. Search for `' DEFAULT '` → not found
5. Search for `' AUTOINCREMENT'` → found at position 12 (but 12 > 3, so keep 3)
6. `end_index` = 3
7. `definition[:3]` = `'INT'`

Note: `namespace()` is used because Jinja2 does not allow reassigning variables inside a `for` loop. The `namespace` object is a workaround — its attributes can be modified inside loops.

---

### `_hybrid_detect_schema_changes(existing_relation, column_definitions)` — Lines 107–172

**Purpose:** Compare the current Snowflake table schema against the user's `column_definitions` and return a structured diff.

**Step 1: Query current schema from Snowflake (lines 109–118):**

```jinja
{% set describe_result = run_query('DESCRIBE TABLE ' ~ existing_relation) %}

{% set existing_cols = {} %}
{% for row in describe_result %}
    {% set col_name_lower = row[0] | lower %}
    {% set col_name_original = row[0] %}
    {% set col_type = row[1] %}
    {% do existing_cols.update({col_name_lower: {'name': col_name_original, 'type': col_type}}) %}
{% endfor %}
```

`DESCRIBE TABLE MY_DB.MY_SCHEMA.MY_MODEL` returns rows like:

| name | type | kind | null? | ... |
|---|---|---|---|---|
| ID | NUMBER(38,0) | COLUMN | N | ... |
| NAME | VARCHAR(200) | COLUMN | Y | ... |

The code accesses columns by position (`row[0]`, `row[1]`) rather than by name (`row['name']`) because Snowflake sometimes returns column names in different cases depending on the session. Positional access avoids this ambiguity.

All column names are lowercased for case-insensitive comparison.

**Step 2: Build map from user config (lines 121–131):**

```jinja
{% set new_cols = {} %}
{% for col_name, col_def in column_definitions.items() %}
    {% set data_type = _hybrid_extract_data_type(col_def) %}
    {% set canonical = _hybrid_canonicalize_type(data_type) %}
    {% do new_cols.update({col_name | lower: {
        'name': col_name,
        'full_definition': col_def,
        'data_type': data_type,
        'canonical_type': canonical
    }}) %}
{% endfor %}
```

For each column in the user's config:
1. Extract the pure data type (`_hybrid_extract_data_type`)
2. Canonicalize it (`_hybrid_canonicalize_type`)
3. Store all forms in a dictionary for later comparison

Example for `'id': 'INT NOT NULL'`:
- `data_type` = `'INT'`
- `canonical` = `'NUMBER(38,0)'`

**Step 3: Compute differences (lines 133–161):**

```jinja
{# Columns in new config but not in existing table → additions #}
{% for name, info in new_cols.items() %}
    {% if name not in existing_cols %}
        {% do add_columns.append({'name': info['name'], 'definition': info['data_type']}) %}
    {% else %}
        {# Both exist → compare canonical types #}
        {% set existing_canonical = _hybrid_canonicalize_type(existing_cols[name]['type']) %}
        {% if existing_canonical != info['canonical_type'] %}
            {% do type_changes.append({...}) %}
        {% endif %}
    {% endif %}
{% endfor %}

{# Columns in existing table but not in new config → drops #}
{% for name, info in existing_cols.items() %}
    {% if name not in new_cols %}
        {% do drop_columns.append({'name': info['name']}) %}
    {% endif %}
{% endfor %}
```

Three types of changes detected:
1. **add_columns** — column in config but not in Snowflake → needs `ALTER TABLE ADD COLUMN`
2. **drop_columns** — column in Snowflake but not in config → needs `ALTER TABLE DROP COLUMN`
3. **type_changes** — column exists in both but canonical types differ → needs `--full-refresh` (Snowflake does not support `ALTER COLUMN TYPE` on hybrid tables)

**Step 4: Return structured result (lines 162–171):**

```jinja
{% set has_changes = (add_columns | length > 0) or (drop_columns | length > 0) or (type_changes | length > 0) %}
{% set requires_full_refresh = type_changes | length > 0 %}

{{ return({
    'add_columns': add_columns,
    'drop_columns': drop_columns,
    'type_changes': type_changes,
    'has_changes': has_changes,
    'requires_full_refresh': requires_full_refresh
}) }}
```

`requires_full_refresh` is `true` only when there are type changes. Column additions and drops can be handled with ALTER TABLE, but type changes cannot.

---

### `_hybrid_apply_schema_changes(on_schema_change, relation, schema_changes)` — Lines 182–208

**Purpose:** Take the detected changes and either apply them, raise an error, or do nothing, depending on the `on_schema_change` setting.

```jinja
{% macro _hybrid_apply_schema_changes(on_schema_change, relation, schema_changes) %}
    {% if schema_changes is none or not schema_changes['has_changes'] %}
        {{ return(none) }}
    {% endif %}

    {% if schema_changes['requires_full_refresh'] %}
        {{ exceptions.raise_compiler_error(
            "Hybrid table column type changes require a full refresh. Run with --full-refresh ..."
        ) }}
    {% endif %}
```

Type changes always error regardless of `on_schema_change` setting. There is no ALTER COLUMN TYPE for hybrid tables.

```jinja
    {% if on_schema_change == 'fail' %}
        {{ exceptions.raise_compiler_error("Schema changes detected ... and on_schema_change=fail.") }}
    {% elif on_schema_change == 'append_new_columns' %}
        {% if schema_changes['drop_columns'] | length > 0 %}
            {{ exceptions.raise_compiler_error("on_schema_change='append_new_columns' does not allow dropping columns ...") }}
        {% endif %}
        {{ _hybrid_alter_add_columns(relation, schema_changes['add_columns']) }}
    {% elif on_schema_change == 'sync_all_columns' %}
        {{ _hybrid_alter_add_columns(relation, schema_changes['add_columns']) }}
        {{ _hybrid_alter_drop_columns(relation, schema_changes['drop_columns']) }}
    {% endif %}
{% endmacro %}
```

| `on_schema_change` | Behavior |
|---|---|
| `'ignore'` | Caller does not invoke this macro at all (see line 398 in `hybrid_table.sql`) |
| `'fail'` | Error if any column was added or dropped |
| `'append_new_columns'` | Add new columns. Error if columns were removed. |
| `'sync_all_columns'` | Add new columns AND drop removed columns |

---

### `_hybrid_alter_add_columns(relation, columns)` — Lines 212–221

```jinja
{% macro _hybrid_alter_add_columns(relation, columns) %}
    {% if not columns %}
        {{ return(none) }}
    {% endif %}
    {% for column in columns %}
        {% call statement('hybrid_table_add_column_' ~ loop.index0) %}
            ALTER TABLE {{ relation }} ADD COLUMN {{ column['name'] }} {{ column['definition'] }}
        {% endcall %}
    {% endfor %}
{% endmacro %}
```

Generates one `ALTER TABLE ADD COLUMN` statement per new column. Each gets a unique statement name (`hybrid_table_add_column_0`, `hybrid_table_add_column_1`, ...) so dbt can track them independently.

Note: The SQL uses `ALTER TABLE`, not `ALTER HYBRID TABLE`. Snowflake's DDL for modifying hybrid table columns uses the standard `ALTER TABLE` syntax.

---

### `_hybrid_alter_drop_columns(relation, columns)` — Lines 225–234

```jinja
{% macro _hybrid_alter_drop_columns(relation, columns) %}
    {% if not columns %}
        {{ return(none) }}
    {% endif %}
    {% for column in columns %}
        {% call statement('hybrid_table_drop_column_' ~ loop.index0) %}
            ALTER TABLE {{ relation }} DROP COLUMN {{ column['name'] }}
        {% endcall %}
    {% endfor %}
{% endmacro %}
```

Same pattern as add — one `ALTER TABLE DROP COLUMN` per removed column.

---

## How the Two Files Work Together

The execution flow on an incremental run:

```
hybrid_table.sql (main materialization)
│
├── Load config (column_definitions, primary_key, etc.)
├── Validate everything
├── Check: does the table already exist?
│
├── YES (incremental path):
│   ├── Call _hybrid_detect_schema_changes()  ──→  hybrid_table_helpers.sql
│   │   ├── DESCRIBE TABLE on Snowflake
│   │   ├── Call _hybrid_extract_data_type()  ──→  strips modifiers
│   │   ├── Call _hybrid_canonicalize_type()   ──→  normalizes types
│   │   └── Return {add_columns, drop_columns, type_changes}
│   │
│   ├── Call _hybrid_apply_schema_changes()   ──→  hybrid_table_helpers.sql
│   │   ├── Call _hybrid_alter_add_columns()  ──→  ALTER TABLE ADD COLUMN
│   │   └── Call _hybrid_alter_drop_columns() ──→  ALTER TABLE DROP COLUMN
│   │
│   └── Execute MERGE or DELETE+INSERT
│
├── NO (full refresh path):
│   └── Execute CREATE HYBRID TABLE (CTAS or non-CTAS)
│
└── Finalize (grants, docs, hooks)
```

On a first run, only the right branch (CREATE) executes. On subsequent runs, the left branch (incremental) executes, calling into the helper macros for schema evolution before loading data.
