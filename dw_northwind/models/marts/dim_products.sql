with
    products as (
        select *
        from {{ ref('stg_products') }}
    )
    , categories as (
        select *
        from {{ ref('stg_categories') }}
    )
    , joined as (
        select
            products.sk_product
            , products.product_id
            , products.product_name
            , products.category_id
            , categories.category_name
            , categories.category_description
            , products.supplier_id
            , products.unit_price
            , products.units_in_stock
            , products.quantity_per_unit
            , products.dicontinued
            , products.reorder_level
        from products
        left join categories
            on products.sk_category = categories.sk_category
    )
select *
from joined
order by
    product_id asc
    , product_name asc