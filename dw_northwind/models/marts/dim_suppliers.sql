with
    suppliers as (
        select *
        from {{ ref('stg_suppliers') }}
    )
    , joined as (
        select
            sk_supplier
            , supplier_id
            , contact_name
            , company_name
            , contact_title
            , supplier_country
            , supplier_city
            , supplier_postal_code
            , supplier_region
        from suppliers
    )
select *
from joined
order by
    supplier_id asc