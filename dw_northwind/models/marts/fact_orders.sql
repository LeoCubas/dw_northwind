with
    orders as (
        select *
        from {{ ref('fact_orders_details') }}
    )
    , grouped as (
        select
            sk_order
            , sk_customer
            , sk_employee
            , order_date
            , required_date
            , freight
            , sum(final_value) as sum_value
            , (sum(final_value) - freight) as discounted_value
        from orders
        group by
            sk_order
            , sk_customer
            , sk_employee
            , order_date
            , required_date
            , freight
    )
select *
from grouped