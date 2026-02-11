{#
    Example: Multi-Column Foreign Key
    Features: FK with two columns referencing composite_pk_hybrid's composite PK
#}
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'line_id': 'INT NOT NULL',
        'order_date': 'DATE NOT NULL',
        'order_id': 'INT NOT NULL',
        'product_name': 'VARCHAR(200)',
        'quantity': 'INT'
    },
    primary_key=['line_id'],
    foreign_keys=[
        {
            'name': 'fk_line_items_orders',
            'columns': ['order_date', 'order_id'],
            'references': {
                'table': 'composite_pk_hybrid',
                'columns': ['order_date', 'order_id']
            }
        }
    ],
    comment='Hybrid table with multi-column FK referencing composite PK'
) }}

-- depends_on: {{ ref('composite_pk_hybrid') }}

SELECT
    (ROW_NUMBER() OVER (ORDER BY o.id))::INT as line_id,
    o.order_date::DATE as order_date,
    o.id::INT as order_id,
    p.product_name::VARCHAR(200) as product_name,
    (o.id % 5 + 1)::INT as quantity
FROM {{ ref('raw_orders') }} o
CROSS JOIN (SELECT * FROM {{ ref('raw_products') }} LIMIT 1) p
