{#
    Example: FOREIGN KEY Constraint
    Features: Single-column FK referencing basic_hybrid, non-CTAS path

    IMPORTANT: FK constraints in Snowflake hybrid tables MUST be defined at
    CREATE TABLE time (not via ALTER TABLE). The materialization handles this
    automatically by using the non-CTAS (CREATE + INSERT) path when FKs are present.

    IMPORTANT: Use plain string table names (not ref()) in the FK references config,
    then add a "-- depends_on:" comment to ensure correct build order.
#}
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'order_id': 'INT NOT NULL',
        'customer_id': 'INT NOT NULL',
        'order_date': 'DATE',
        'order_total': 'DECIMAL(12,2)',
        'status': 'VARCHAR(50)'
    },
    primary_key=['order_id'],
    foreign_keys=[
        {
            'name': 'fk_orders_customers',
            'columns': ['customer_id'],
            'references': {
                'table': 'basic_hybrid',
                'columns': ['id']
            }
        }
    ],
    comment='Orders table with FK referencing basic_hybrid'
) }}

-- depends_on: {{ ref('basic_hybrid') }}

SELECT
    id::INT as customer_id,
    order_date::DATE as order_date,
    (id + 100)::INT as order_id,
    order_total::DECIMAL(12,2) as order_total,
    status::VARCHAR(50) as status
FROM {{ ref('raw_orders') }}
