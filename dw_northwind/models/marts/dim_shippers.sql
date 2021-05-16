with
    shippers as (
        select *
        from {{ ref('stg_shippers') }}
    )
    , joined as (
        select
            sk_shipper
            , shipper_id
            , company_name
        from shippers
    )
select *
from joined
order by
    shipper_id asc