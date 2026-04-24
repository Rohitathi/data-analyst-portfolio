with orders as (
    select * from {{ ref('int_orders_with_customers') }}
),

order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

payments as (
    select * from {{ ref('stg_order_payments') }}
),

order_totals as (
    select
        order_id,
        sum(price)          as total_items_value,
        sum(freight_value)  as total_freight_value,
        sum(total_item_value) as total_order_value,
        count(order_item_id)  as total_items
    from order_items
    group by order_id
),

payment_totals as (
    select
        order_id,
        sum(payment_value)  as total_payment_value,
        count(distinct payment_type) as payment_methods_used
    from payments
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.customer_unique_id,
        o.customer_city,
        o.customer_state,
        o.order_status,
        o.order_purchased_at,
        o.order_delivered_at,
        o.order_estimated_delivery_at,
        o.days_to_deliver,
        o.days_early_or_late,
        ot.total_items,
        ot.total_items_value,
        ot.total_freight_value,
        ot.total_order_value,
        pt.total_payment_value,
        pt.payment_methods_used
    from orders o
    left join order_totals ot on o.order_id = ot.order_id
    left join payment_totals pt on o.order_id = pt.order_id
)

select * from final