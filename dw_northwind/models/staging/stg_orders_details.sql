with
    source as (
        select *
        from {{ source('northwind','order_details') }}
    )
    , transformed as (
        select
        order_id
        , product_id
        , discount
        , unit_price
        , quantity
        , ((quantity*unit_price) - discount) as final_value
        from source
    )
    , transformed_with_sk as (
        select
            {{ dbt_utils.surrogate_key(['order_id']) }} as sk_order
            , {{ dbt_utils.surrogate_key(['product_id']) }} as sk_product
            , *
        from transformed
    )
select *
from transformed_with_sk