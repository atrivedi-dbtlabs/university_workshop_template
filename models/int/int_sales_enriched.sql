with items as (

    select
        item_id,
        order_id,
        sku
    from {{ ref('stg_jaffle_shop__raw_items') }}

),

orders as (

    select
        order_id,
        customer_id,
        store_id,
        ordered_at,
        order_date,
        order_subtotal,
        order_tax_paid,
        order_total
    from {{ ref('stg_jaffle_shop__raw_orders') }}

),

products as (

    select
        sku,
        product_name,
        product_type,
        product_price,
        product_description
    from {{ ref('stg_jaffle_shop__raw_products') }}

),

supplies as (

    select
        sku,
        supply_id,
        supply_name,
        supply_cost,
        is_perishable
    from {{ ref('stg_jaffle_shop__raw_supplies') }}

),

stores as (

    select
        store_id,
        store_name,
        opened_at,
        tax_rate
    from {{ ref('stg_jaffle_shop__raw_stores') }}

),

joined as (

    select
        --keys
        i.item_id,
        i.order_id,
        i.sku,
        --order context
        o.customer_id,
        o.store_id,
        o.ordered_at,
        o.order_date,
        --product attributes
        p.product_name,
        p.product_type,
        p.product_description,
        --store attributes
        s.store_name,
        s.opened_at as store_opened_at,
        s.tax_rate,
        --cost basis / supply attributes
        sup.supply_id,
        sup.supply_name,
        sup.is_perishable,
        --unit economics
        cast(1 as int64) as units_sold,
        cast(p.product_price as numeric) as unit_revenue,
        cast(sup.supply_cost as numeric) as unit_cost,
        cast(p.product_price as numeric) - cast(sup.supply_cost as numeric) as unit_margin,
        safe_divide(
            cast(p.product_price as numeric) - cast(sup.supply_cost as numeric),
            nullif(cast(p.product_price as numeric), 0)
        ) as unit_margin_pct
    from items i
    join orders o
        on i.order_id = o.order_id
    left join products p
        on i.sku = p.sku
    left join supplies sup
        on i.sku = sup.sku
    left join stores s
        on o.store_id = s.store_id

)

select *
from joined