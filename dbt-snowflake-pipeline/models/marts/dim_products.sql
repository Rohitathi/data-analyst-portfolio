with order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

orders as (
    select order_id, order_status from {{ ref('stg_orders') }}
),

product_metrics as (
    select
        oi.product_id,
        oi.category_name,
        oi.seller_state,
        count(distinct oi.order_id)         as total_orders,
        sum(oi.price)                       as total_revenue,
        avg(oi.price)                       as avg_price,
        sum(oi.freight_value)               as total_freight,
        avg(oi.weight_g)                    as avg_weight_g
    from order_items oi
    left join orders o on oi.order_id = o.order_id
    where o.order_status = 'delivered'
    group by oi.product_id, oi.category_name, oi.seller_state
)

select * from product_metrics