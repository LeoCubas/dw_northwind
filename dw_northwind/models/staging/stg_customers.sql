with
    source as (
        select *
        from {{ source('northwind','customers') }}
    )
    , transformed as (
        select
            trim(customer_id) as customer_id
            , trim(contact_name) as contact_name
            , trim(company_name) as company_name
            , trim(contact_title) as contact_title
            , trim(country) as country
            , trim(city) as city
            , trim(region) as region
            , trim(postal_code) as postal_code
            , trim(address) as address
        from source
    )
    , transformed_with_sk as (
        select
            {{ dbt_utils.surrogate_key(['customer_id']) }} as sk_customer
            , *
        from transformed
    )
select *
from transformed_with_sk