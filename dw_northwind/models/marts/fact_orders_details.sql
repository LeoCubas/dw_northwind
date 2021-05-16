with
    orders as (
        select *
        from {{ ref('stg_orders') }}
    )
    , orders_details as (
        select
            od.*
            , p.sk_supplier
        from {{ ref('stg_orders_details') }} od
        left join {{ ref('stg_products') }} p
            on od.sk_product = p.sk_product
    )
    , joined as (
        select
            orders_details.sk_order_detail
            , orders.sk_order
            , orders_details.sk_product
            , orders.sk_customer
            , orders.sk_employee
            , orders_details.sk_supplier
            , orders.order_date
            , orders.required_date
            , orders.freight
            , orders_details.discount
            , orders_details.unit_price
            , orders_details.quantity
            , orders_details.final_value
        from orders
        left join orders_details
            on orders.sk_order = orders_details.sk_order
    )
select *
from joined