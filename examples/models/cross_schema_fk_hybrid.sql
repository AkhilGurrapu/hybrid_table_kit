{#
    Example: Cross-Schema Foreign Key
    Features: FK referencing a table in a DIFFERENT schema using 3-part dot notation

    IMPORTANT: Before running this model, the referenced table must exist in the
    target schema. Update the 'table' value to match your environment:
      - 2-part: 'OTHER_SCHEMA.ref_products'
      - 3-part: 'MY_DB.OTHER_SCHEMA.ref_products'

    The referenced table must be a hybrid table with a PK on product_id.
#}
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'line_id': 'INT NOT NULL',
        'product_id': 'INT NOT NULL',
        'quantity': 'INT',
        'unit_price': 'DECIMAL(10,2)'
    },
    primary_key=['line_id'],
    foreign_keys=[
        {
            'name': 'fk_cross_schema_product',
            'columns': ['product_id'],
            'references': {
                'table': 'ADMIN_DB.DBT_HYBRID_REF.ref_products',
                'columns': ['product_id']
            }
        }
    ],
    comment='Cross-schema FK test: references table in another schema'
) }}

SELECT
    (id * 10)::INT as line_id,
    (MOD(id, 3) + 1)::INT as product_id,
    (id + 5)::INT as quantity,
    order_total::DECIMAL(10,2) as unit_price
FROM {{ ref('raw_orders') }}
