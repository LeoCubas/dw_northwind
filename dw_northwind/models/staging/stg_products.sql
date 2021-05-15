with
    source as (
        select *
        from {{ source('northwind','products') }}
    )
    , transformed as (
        select
            product_id
            , trim(product_name) as product_name
            , category_id
            , supplier_id
            , unit_price
            , units_in_stock
            , quantity_per_unit
            , cast(discontinued as boolean) as dicontinued
            , reorder_level
        from source
    )
    , transformed_with_sk as (
        select
            {{ dbt_utils.surrogate_key(['product_id']) }} as sk_product
            , {{ dbt_utils.surrogate_key(['category_id']) }} as sk_category
            , {{ dbt_utils.surrogate_key(['supplier_id']) }} as sk_supplier
            , *
        from transformed
    )
select *
from transformed_with_sk