with
    customers as (
        select *
        from {{ ref('stg_customers') }}
    )
    , joined as (
        select
            sk_customer
            , customer_id
            , contact_name
            , company_name
            , contact_title
            , country
            , city
            , region
            , postal_code
            , address
        from customers
    )
select *
from joined
order by
    customer_id asc