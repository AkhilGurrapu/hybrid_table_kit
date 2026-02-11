{#
    Pure-Macro Custom Materialization: hybrid_table

    Creates and manages Snowflake Hybrid Tables entirely from Jinja macros.
    No adapter fork or pip install required — just copy this macros/ folder
    into your dbt project's macros/ directory.

    Requires: dbt-snowflake adapter (standard, unmodified)

    Features:
    - PRIMARY KEY (required for hybrid tables)
    - UNIQUE constraints
    - FOREIGN KEY constraints (references other hybrid tables)
    - Secondary indexes with optional INCLUDE columns
    - AUTOINCREMENT / IDENTITY columns
    - DEFAULT values
    - Table COMMENT
    - Incremental strategies: merge, delete+insert
    - Schema evolution: ignore, fail, append_new_columns, sync_all_columns
    - merge_exclude_columns / merge_update_columns

    Usage:
        {{ config(
            materialized='hybrid_table',
            column_definitions={
                'id': 'INT NOT NULL',
                'name': 'VARCHAR(200)',
                'created_at': 'TIMESTAMP_NTZ'
            },
            primary_key=['id']
        ) }}

        SELECT id, name, created_at FROM source_table
#}

{% materialization hybrid_table, adapter='snowflake' %}

    {% set query_tag = set_query_tag() %}

    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this %}

    {{ run_hooks(pre_hooks) }}

    {# ============== CONFIGURATION ============== #}

    {# Required #}
    {% set column_definitions = config.get('column_definitions', {}) %}
    {% set primary_key = config.get('primary_key', []) %}

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
    {% set force_non_ctas = config.get('force_non_ctas', none) %}

    {# Detect AUTOINCREMENT / IDENTITY columns #}
    {% set autoincrement_columns = [] %}
    {% for column_name, column_definition in column_definitions.items() %}
        {% set column_definition_upper = column_definition | upper %}
        {% if 'AUTOINCREMENT' in column_definition_upper or 'IDENTITY' in column_definition_upper %}
            {% do autoincrement_columns.append(column_name) %}
        {% endif %}
    {% endfor %}

    {# Decide CTAS vs non-CTAS path #}
    {% if force_non_ctas is not none %}
        {% set use_non_ctas_create = force_non_ctas %}
    {% else %}
        {# FK constraints MUST be defined at CREATE TABLE time (not ALTER), and
           AUTOINCREMENT columns don't work with CTAS — both need non-CTAS path #}
        {% set use_non_ctas_create = autoincrement_columns | length > 0 or foreign_keys | length > 0 %}
    {% endif %}

    {# Optional — table properties #}
    {% set table_comment = config.get('comment', none) %}

    {# Column ordering: explicit column_order or alphabetical sort.
       IMPORTANT: Your SELECT columns must match this order (CTAS maps by position). #}
    {% set column_order = config.get('column_order', column_definitions.keys() | sort | list) %}

    {# Validate incremental strategy #}
    {% set supported_incremental_strategies = ['merge', 'delete+insert'] %}
    {% if incremental_strategy not in supported_incremental_strategies %}
        {{ exceptions.raise_compiler_error(
            "Invalid incremental_strategy '" ~ incremental_strategy ~ "' for hybrid_table. Supported strategies: " ~
            supported_incremental_strategies | join(', ')
        ) }}
    {% endif %}

    {# ============== VALIDATION ============== #}

    {% if column_definitions | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "Hybrid table materialization requires 'column_definitions' in model config.\n\n" ~
            "Example:\n" ~
            "{{ config(\n" ~
            "    materialized='hybrid_table',\n" ~
            "    column_definitions={\n" ~
            "        'id': 'INT NOT NULL',\n" ~
            "        'name': 'VARCHAR(200)',\n" ~
            "        'created_at': 'TIMESTAMP_NTZ'\n" ~
            "    },\n" ~
            "    primary_key=['id']\n" ~
            ") }}"
        ) }}
    {% endif %}

    {% if primary_key | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "Hybrid tables require a PRIMARY KEY constraint.\n\n" ~
            "Add 'primary_key' to model config:\n" ~
            "  primary_key=['id']  -- single column\n" ~
            "  primary_key=['tenant_id', 'user_id']  -- composite key\n\n" ~
            "Note: PRIMARY KEY columns cannot use VARIANT, ARRAY, OBJECT,\n" ~
            "GEOGRAPHY, GEOMETRY, VECTOR, or TIMESTAMP_TZ types."
        ) }}
    {% endif %}

    {% set unsupported_constraint_types = ['VARIANT', 'ARRAY', 'OBJECT', 'GEOGRAPHY', 'GEOMETRY', 'VECTOR', 'TIMESTAMP_TZ'] %}

    {# Validate primary key columns exist in column_definitions #}
    {% for pk_col in primary_key %}
        {% if pk_col not in column_definitions %}
            {{ exceptions.raise_compiler_error(
                "Primary key column '" ~ pk_col ~ "' not found in column_definitions.\n" ~
                "Defined columns: " ~ (column_definitions.keys() | list | join(', '))
            ) }}
        {% endif %}
        {% set pk_type = column_definitions[pk_col] | upper %}
        {% for unsupported in unsupported_constraint_types %}
            {% if unsupported in pk_type %}
                {{ exceptions.raise_compiler_error(
                    "Primary key column '" ~ pk_col ~ "' uses unsupported data type '" ~ column_definitions[pk_col] ~ "'."
                ) }}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {# Validate unique constraint columns exist #}
    {% for unique in unique_constraints %}
        {% for col in unique.columns %}
            {% if col not in column_definitions %}
                {{ exceptions.raise_compiler_error(
                    "UNIQUE constraint column '" ~ col ~ "' not found in column_definitions."
                ) }}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {# Validate index columns exist #}
    {% for index in indexes %}
        {% for col in index.columns %}
            {% if col not in column_definitions %}
                {{ exceptions.raise_compiler_error(
                    "Index column '" ~ col ~ "' in index '" ~ index.name ~ "' not found in column_definitions."
                ) }}
            {% endif %}
        {% endfor %}
        {% if index.include is defined %}
            {% for col in index.include %}
                {% if col not in column_definitions %}
                    {{ exceptions.raise_compiler_error(
                        "INCLUDE column '" ~ col ~ "' in index '" ~ index.name ~ "' not found in column_definitions."
                    ) }}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}

    {# ============== FOREIGN KEY VALIDATION ============== #}

    {% set validated_foreign_keys = [] %}
    {% for fk in foreign_keys %}
        {% if fk.columns is not defined or fk.columns | length == 0 %}
            {{ exceptions.raise_compiler_error(
                "Foreign key configuration requires 'columns' with at least one column name."
            ) }}
        {% endif %}

        {% for fk_col in fk.columns %}
            {% if fk_col not in column_definitions %}
                {{ exceptions.raise_compiler_error(
                    "Foreign key column '" ~ fk_col ~ "' not found in column_definitions."
                ) }}
            {% endif %}
        {% endfor %}

        {% if fk.references is not defined %}
            {{ exceptions.raise_compiler_error(
                "Foreign key configuration requires a 'references' dictionary with table and columns."
            ) }}
        {% endif %}

        {% set fk_reference = fk.references %}
        {% if fk_reference.columns is not defined or fk_reference.columns | length == 0 %}
            {{ exceptions.raise_compiler_error(
                "Foreign key references must include 'columns' with at least one column name."
            ) }}
        {% endif %}

        {% if fk_reference.table is not defined and fk_reference.identifier is not defined %}
            {{ exceptions.raise_compiler_error(
                "Foreign key references must include 'table' (optionally with 'database'/'schema')."
            ) }}
        {% endif %}

        {% if fk_reference.columns | length != fk.columns | length %}
            {{ exceptions.raise_compiler_error(
                "Foreign key column count mismatch. Referencing columns (" ~ (fk.columns | join(', ')) ~ ") " ~
                "must match referenced columns (" ~ (fk_reference.columns | join(', ')) ~ ")."
            ) }}
        {% endif %}

        {# Resolve the referenced relation's database, schema, identifier #}
        {% set reference_database = fk_reference.database if fk_reference.database is defined else target_relation.database %}
        {% set reference_schema = fk_reference.schema if fk_reference.schema is defined else target_relation.schema %}
        {% set reference_identifier_value = fk_reference.identifier if fk_reference.identifier is defined else fk_reference.table %}

        {% if reference_identifier_value is none %}
            {{ exceptions.raise_compiler_error(
                "Foreign key references must define a target relation via 'table' or 'identifier'."
            ) }}
        {% endif %}

        {# Handle dot notation: "SCHEMA.TABLE" or "DB.SCHEMA.TABLE" #}
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

        {% if reference_database is none %}
            {% set reference_database = target_relation.database %}
        {% endif %}
        {% if reference_schema is none %}
            {% set reference_schema = target_relation.schema %}
        {% endif %}

        {# Verify the referenced table exists #}
        {% set referenced_relation = adapter.get_relation(
            database=reference_database,
            schema=reference_schema,
            identifier=reference_identifier
        ) %}

        {% if referenced_relation is none %}
            {{ exceptions.raise_compiler_error(
                "Foreign key references relation '" ~ reference_database ~ "." ~ reference_schema ~ "." ~ reference_identifier ~ "' which does not exist."
            ) }}
        {% endif %}

        {# Verify referenced columns exist on the target table #}
        {% set referenced_column_names = [] %}
        {% set reference_columns = adapter.get_columns_in_relation(referenced_relation) %}
        {% for reference_column in reference_columns %}
            {% do referenced_column_names.append(reference_column.name | upper) %}
        {% endfor %}

        {% for reference_column_name in fk_reference.columns %}
            {% if reference_column_name | upper not in referenced_column_names %}
                {{ exceptions.raise_compiler_error(
                    "Foreign key references column '" ~ reference_column_name ~ "' that does not exist on " ~ referenced_relation ~ "."
                ) }}
            {% endif %}
        {% endfor %}

        {% set normalized_fk = {
            'name': fk.name if fk.name is defined else none,
            'columns': fk.columns,
            'references_relation': referenced_relation,
            'references_columns': fk_reference.columns
        } %}
        {% do validated_foreign_keys.append(normalized_fk) %}
    {% endfor %}

    {% set foreign_keys = validated_foreign_keys %}

    {# ============== RELATION TYPE CHECK ============== #}

    {# If existing object is a view or dynamic table, drop it first.
       Accept both 'table' (standard adapter) and 'hybrid_table' (forked adapter). #}
    {% if existing_relation is not none and existing_relation.type not in ('table', 'hybrid_table') %}
        {{ log("Dropping " ~ existing_relation ~ " (type: " ~ existing_relation.type ~ ") to replace with hybrid table", info=True) }}
        {% do run_query('DROP ' ~ existing_relation.type ~ ' IF EXISTS ' ~ existing_relation) %}
        {% set existing_relation = none %}
    {% endif %}

    {# ============== CREATE OR MERGE ============== #}

    {% if existing_relation is none or full_refresh_mode or force_ctas %}
        {# ========== FULL REFRESH / INITIAL CREATE ========== #}

        {% if use_non_ctas_create %}
            {# Non-CTAS path: CREATE empty table then INSERT data.
               Required when FK or AUTOINCREMENT columns are present. #}

            {% if autoincrement_columns | length > 0 %}
                {{ log("Using CREATE + INSERT flow because AUTOINCREMENT/IDENTITY columns are present", info=True) }}
            {% elif foreign_keys | length > 0 %}
                {{ log("Using CREATE + INSERT flow because FOREIGN KEY constraints require DDL-time definition", info=True) }}
            {% endif %}

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

            {# Exclude AUTOINCREMENT columns from INSERT — Snowflake generates them #}
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

        {% else %}
            {# CTAS path: CREATE ... AS (SELECT ...).
               Used when no FK or AUTOINCREMENT columns are present. #}

            {% call statement('main') %}
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

                    {% for index in indexes %}
                        , INDEX {{ index.name }} ({{ index.columns | join(', ') }}){% if index.include is defined %} INCLUDE ({{ index.include | join(', ') }}){% endif %}
                    {% endfor %}
                )
                {% if table_comment %}
                COMMENT = '{{ table_comment | replace("'", "''") }}'
                {% endif %}
                AS (
                    {{ sql }}
                )
            {% endcall %}
        {% endif %}

        {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}

    {% else %}
        {# ========== INCREMENTAL UPDATE ========== #}

        {# Detect and apply schema changes if on_schema_change != 'ignore' #}
        {% if on_schema_change != 'ignore' %}
            {% set schema_changes = _hybrid_detect_schema_changes(existing_relation, column_definitions) %}
            {{ _hybrid_apply_schema_changes(on_schema_change, target_relation, schema_changes) }}
        {% endif %}

        {% if incremental_strategy == 'merge' %}
            {# Determine columns to update on MATCHED #}
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

            {# Build merge key list (PKs excluding AUTOINCREMENT) #}
            {% set merge_keys = [] %}
            {% for pk in primary_key %}
                {% if pk not in autoincrement_columns %}
                    {% do merge_keys.append(pk) %}
                {% endif %}
            {% endfor %}

            {# Build insert column list (excluding AUTOINCREMENT) #}
            {% set merge_insert_columns = [] %}
            {% for col in column_order %}
                {% if col not in autoincrement_columns %}
                    {% do merge_insert_columns.append(col) %}
                {% endif %}
            {% endfor %}

            {% if merge_keys | length == 0 %}
                {# All PKs are AUTOINCREMENT — can't MERGE, just INSERT new rows #}
                {% call statement('main') %}
                    INSERT INTO {{ target_relation }} ({{ merge_insert_columns | join(', ') }})
                    {{ sql }}
                {% endcall %}
            {% else %}
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
            {% endif %}

        {% elif incremental_strategy == 'delete+insert' %}
            {% if primary_key | length == 0 %}
                {{ exceptions.raise_compiler_error(
                    "incremental_strategy='delete+insert' requires a primary_key configuration."
                ) }}
            {% endif %}

            {# Build key/column lists excluding AUTOINCREMENT #}
            {% set di_merge_keys = [] %}
            {% for pk in primary_key %}
                {% if pk not in autoincrement_columns %}
                    {% do di_merge_keys.append(pk) %}
                {% endif %}
            {% endfor %}
            {% set di_insert_columns = [] %}
            {% for col in column_order %}
                {% if col not in autoincrement_columns %}
                    {% do di_insert_columns.append(col) %}
                {% endif %}
            {% endfor %}

            {% if di_merge_keys | length == 0 %}
                {{ exceptions.raise_compiler_error(
                    "incremental_strategy='delete+insert' requires at least one non-AUTOINCREMENT primary key column."
                ) }}
            {% endif %}

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
        {% endif %}

        {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=False) %}
    {% endif %}

    {{ run_hooks(post_hooks) }}

    {% do unset_query_tag(query_tag) %}

    {# Apply grants #}
    {% set grant_config = config.get('grants') %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {# Persist documentation #}
    {% do persist_docs(target_relation, model) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
