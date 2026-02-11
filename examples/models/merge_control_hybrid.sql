{#
    Example: Merge Control with merge_exclude_columns
    Features: merge_exclude_columns prevents overwriting created_at on MERGE

    On incremental runs, the MERGE statement will update all non-PK columns
    EXCEPT those listed in merge_exclude_columns. This preserves the original
    created_at timestamp while updating other fields.
#}
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'customer_name': 'VARCHAR(200)',
        'order_total': 'DECIMAL(12,2)',
        'status': 'VARCHAR(50)',
        'created_at': 'TIMESTAMP_NTZ',
        'updated_at': 'TIMESTAMP_NTZ'
    },
    primary_key=['id'],
    merge_exclude_columns=['created_at'],
    indexes=[
        {'name': 'idx_status', 'columns': ['status']}
    ],
    comment='Hybrid table testing merge_exclude_columns'
) }}

SELECT
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as created_at,
    customer_name::VARCHAR(200) as customer_name,
    id::INT as id,
    order_total::DECIMAL(12,2) as order_total,
    status::VARCHAR(50) as status,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as updated_at
FROM {{ ref('raw_orders') }}
