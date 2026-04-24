with orders as (
    select * from {{ ref('fct_orders') }}
),

customer_metrics as (
    select
        customer_unique_id,
        customer_city,
        customer_state,
        count(distinct order_id)            as total_orders,
        sum(total_order_value)              as lifetime_value,
        avg(total_order_value)              as avg_order_value,
        min(order_purchased_at)             as first_order_at,
        max(order_purchased_at)             as last_order_at,
        avg(days_to_deliver)                as avg_days_to_deliver,
        datediff('day',
            min(order_purchased_at),
            max(order_purchased_at))        as customer_lifespan_days
    from orders
    where order_status = 'delivered'
    group by customer_unique_id, customer_city, customer_state
)

select * from customer_metrics