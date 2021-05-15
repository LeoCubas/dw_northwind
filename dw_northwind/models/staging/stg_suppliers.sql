with
    source as (
        select *
        from {{ source('northwind','suppliers') }}
    )
    , transformed as (
        select
            supplier_id
            , trim(contact_name) as contact_name
            , trim(company_name) as company_name
            , trim(contact_title) as contact_title
            , trim(country) as supplier_country
            , trim(city) as supplier_city
            , trim(postal_code) as supplier_postal_code
            , trim(region) as supplier_region
        from source
    )
    , transformed_with_sk as (
        select
            {{ dbt_utils.surrogate_key(['supplier_id']) }} as sk_supplier
            , *
        from transformed
    )
select *
from transformed_with_sk