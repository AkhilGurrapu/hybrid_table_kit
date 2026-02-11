{# ============================================================
   Hybrid Table Helper Macros

   Supporting macros for the hybrid_table custom materialization.
   Handles type canonicalization, schema detection, and schema
   change application — all in pure Jinja (no adapter Python).
   ============================================================ #}


{# ---- Canonicalize Snowflake Type ----
   Normalizes a user-facing Snowflake type to its canonical
   DESCRIBE TABLE form.
   Examples:
     INT          → NUMBER(38,0)
     VARCHAR      → VARCHAR(16777216)
     TIMESTAMP_NTZ → TIMESTAMP_NTZ(9)
     DECIMAL(10,2) → NUMBER(10,2)
#}
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

    {% if upper_type in aliases %}
        {{ return(aliases[upper_type]) }}
    {% endif %}

    {# Handle DECIMAL(p,s) / NUMERIC(p,s) → NUMBER(p,s) #}
    {% if upper_type.startswith('DECIMAL(') or upper_type.startswith('NUMERIC(') %}
        {% set inner = upper_type.split('(')[1].rstrip(')') %}
        {{ return('NUMBER(' ~ inner ~ ')') }}
    {% endif %}

    {# Already canonical — return as-is #}
    {{ return(upper_type) }}
{% endmacro %}


{# ---- Extract Data Type ----
   Strips NOT NULL, NULL, DEFAULT, AUTOINCREMENT, IDENTITY, and COMMENT
   from a column definition string, returning just the data type.
   Examples:
     'INT NOT NULL'                      → 'INT'
     'VARCHAR(200)'                      → 'VARCHAR(200)'
     'INT NOT NULL AUTOINCREMENT'        → 'INT'
     'TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()' → 'TIMESTAMP_NTZ'
#}
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


{# ---- Detect Schema Changes ----
   Compares existing hybrid table columns (via DESCRIBE TABLE) against
   the model's column_definitions config.

   Returns a dict:
     {
       'add_columns':    [{name, definition}, ...],
       'drop_columns':   [{name}, ...],
       'type_changes':   [{name, existing_type, new_type}, ...],
       'has_changes':    true/false,
       'requires_full_refresh': true/false
     }
#}
{% macro _hybrid_detect_schema_changes(existing_relation, column_definitions) %}
    {# Query Snowflake for the current table schema #}
    {% set describe_result = run_query('DESCRIBE TABLE ' ~ existing_relation) %}

    {# Build map of existing columns: lowercase_name → {name, type} #}
    {% set existing_cols = {} %}
    {% for row in describe_result %}
        {% set col_name_lower = row[0] | lower %}
        {% set col_name_original = row[0] %}
        {% set col_type = row[1] %}
        {% do existing_cols.update({col_name_lower: {'name': col_name_original, 'type': col_type}}) %}
    {% endfor %}

    {# Build map of new columns from config: lowercase_name → {name, full_definition, data_type, canonical_type} #}
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

    {# Compute differences #}
    {% set add_columns = [] %}
    {% set drop_columns = [] %}
    {% set type_changes = [] %}

    {# Columns in new config but not in existing table → additions #}
    {% for name, info in new_cols.items() %}
        {% if name not in existing_cols %}
            {% do add_columns.append({'name': info['name'], 'definition': info['data_type']}) %}
        {% else %}
            {# Both exist → compare canonical types #}
            {% set existing_canonical = _hybrid_canonicalize_type(existing_cols[name]['type']) %}
            {% if existing_canonical != info['canonical_type'] %}
                {% do type_changes.append({
                    'name': info['name'],
                    'existing_type': existing_cols[name]['type'],
                    'new_type': info['data_type']
                }) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Columns in existing table but not in new config → drops #}
    {% for name, info in existing_cols.items() %}
        {% if name not in new_cols %}
            {% do drop_columns.append({'name': info['name']}) %}
        {% endif %}
    {% endfor %}

    {% set has_changes = (add_columns | length > 0) or (drop_columns | length > 0) or (type_changes | length > 0) %}
    {% set requires_full_refresh = type_changes | length > 0 %}

    {{ return({
        'add_columns': add_columns,
        'drop_columns': drop_columns,
        'type_changes': type_changes,
        'has_changes': has_changes,
        'requires_full_refresh': requires_full_refresh
    }) }}
{% endmacro %}


{# ---- Apply Schema Changes ----
   Routes schema changes based on on_schema_change mode:
     'ignore'             → do nothing (caller should not call this)
     'fail'               → raise error if any changes detected
     'append_new_columns' → add new columns; error if columns removed
     'sync_all_columns'   → add new columns AND drop removed columns
#}
{% macro _hybrid_apply_schema_changes(on_schema_change, relation, schema_changes) %}
    {% if schema_changes is none or not schema_changes['has_changes'] %}
        {{ return(none) }}
    {% endif %}

    {% if schema_changes['requires_full_refresh'] %}
        {{ exceptions.raise_compiler_error(
            "Hybrid table column type changes require a full refresh. Run with --full-refresh to apply: " ~ relation
        ) }}
    {% endif %}

    {% if on_schema_change == 'fail' %}
        {{ exceptions.raise_compiler_error(
            "Schema changes detected for " ~ relation ~ " and on_schema_change=fail."
        ) }}
    {% elif on_schema_change == 'append_new_columns' %}
        {% if schema_changes['drop_columns'] | length > 0 %}
            {{ exceptions.raise_compiler_error(
                "on_schema_change='append_new_columns' does not allow dropping columns on " ~ relation
            ) }}
        {% endif %}
        {{ _hybrid_alter_add_columns(relation, schema_changes['add_columns']) }}
    {% elif on_schema_change == 'sync_all_columns' %}
        {{ _hybrid_alter_add_columns(relation, schema_changes['add_columns']) }}
        {{ _hybrid_alter_drop_columns(relation, schema_changes['drop_columns']) }}
    {% endif %}
{% endmacro %}


{# ---- Add Columns ---- #}
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


{# ---- Drop Columns ---- #}
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
