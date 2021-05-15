with
    source as (
        select *
        from {{ source('northwind','shippers') }}
    )
    , transformed as (
        select
            shipper_id
            , trim(company_name) as company_name
        from source
    )
    , transformed_with_sk as (
        select
            {{ dbt_utils.surrogate_key(['shipper_id']) }} as sk_shipper
            , *
        from transformed
    )
select *
from transformed_with_sk