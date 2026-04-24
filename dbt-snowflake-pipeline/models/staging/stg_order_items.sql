with source as (
    select * from {{ source('raw', 'order_items') }}
),
renamed as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_at,
        cast(price as float)                   as price,
        cast(freight_value as float)           as freight_value
    from source
    where order_id is not null
)
select * from renamed