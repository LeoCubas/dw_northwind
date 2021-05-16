with
    source as (
        select *
        from {{ source('northwind','orders') }}
    )
    , transformed as (
        select
            order_id
            , customer_id
            , employee_id
            , cast(order_date as date) as order_date
            , cast(required_date as date) as required_date
            , freight
            , ship_via
            , trim(ship_region) as ship_region
            , cast(shipped_date as date) as shipped_date
            , trim(ship_country) as ship_country
            , trim(ship_address) as ship_address
            , trim(ship_name) as ship_name
            , trim(ship_postal_code) as ship_postal_code
            , trim(ship_city) as ship_city
        from source
    )
    , transformed_with_sk as (
        select
            {{ dbt_utils.surrogate_key(['order_id']) }} as sk_order
            , {{ dbt_utils.surrogate_key(['customer_id']) }} as sk_customer
            , {{ dbt_utils.surrogate_key(['employee_id']) }} as sk_employee
            , *
        from transformed
    )
select *
from transformed_with_sk