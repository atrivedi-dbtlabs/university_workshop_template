with sales as (

    select *
    from {{ ref('int_sales_enriched') }}

),

aggregated as (

    select
        order_date,
        store_id,
        store_name,
        sku,
        product_name,
        product_type,
        is_perishable,
        
        sum(units_sold) as units_sold,
        sum(unit_revenue) as revenue,
        sum(unit_cost) as cost,
        sum(unit_margin) as margin,
        safe_divide(sum(unit_margin), nullif(sum(unit_revenue), 0)) as margin_pct
    from sales
    group by 1,2,3,4,5,6,7

)

select *
from aggregated