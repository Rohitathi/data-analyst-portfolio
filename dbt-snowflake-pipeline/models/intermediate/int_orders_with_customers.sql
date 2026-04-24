with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchased_at,
        o.order_approved_at,
        o.order_shipped_at,
        o.order_delivered_at,
        o.order_estimated_delivery_at,
        c.customer_unique_id,
        c.city            as customer_city,
        c.state           as customer_state,
        datediff('day', o.order_purchased_at, o.order_delivered_at) as days_to_deliver,
        datediff('day', o.order_delivered_at, o.order_estimated_delivery_at) as days_early_or_late
    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from joined