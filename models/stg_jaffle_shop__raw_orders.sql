
with source as (

    select * from {{ source('jaffle_shop', 'raw_orders') }}

),

renamed as (

    select
        id as order_id,
        customer as customer_id,
        store_id,
        timestamp(ordered_at) as ordered_at,
        date(timestamp(ordered_at)) as order_date,
        cast(subtotal as numeric) as order_subtotal,
        cast(tax_paid as numeric) as order_tax_paid,
        cast(order_total as numeric) as order_total

    from source

)

select * from renamed

