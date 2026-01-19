with orders as (
    select * from {{ref('stg_jaffle_shop_orders')}}

),

payments as (
    select * from {{ref('stg_stripe__payments')}}

),

payment_final as (
    select order_id 
          ,sum(amount) as amount
    from payments
    where status = 'success'
    group by order_id
),

final as (
    select  
         o.order_id
        ,o.customer_id
        ,o.order_date
        ,coalesce(pf.amount,0) as total_amount
    from orders o
    left join payment_final pf on pf.order_id = o.order_id 
)

select * from final