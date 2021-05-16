with
    source as (
        select *
        from {{ source('northwind','categories') }}
    )
    , transformed as (
        select
            category_id
            , trim(category_name) as category_name
            , trim(description) as category_description
        from source
    )
    , transformed_with_sk as (
        select
            {{ dbt_utils.surrogate_key(['category_id']) }} as sk_category
            , *
        from transformed
    )
select *
from transformed_with_sk