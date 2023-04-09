

with line_items as (
select * from {{ ref('stg_line_items') }}
),
pivoted as (
select 

    product_crop,
    sum(case when order_type = 'ONLINE' then total_price_without_tax else 0 end) as online_sales,
    sum(case when order_type = 'OFFLINE' then total_price_without_tax else 0 end) as offline_sales,

from line_items
group by 1
)
select * from pivoted


-------

with line_items as (
select * from {{ ref('stg_line_items') }}
),
pivoted as (
select 

    product_crop,
        {%- set order_type = ['ONLINE', 'OFFLINE'] -%}
        {% for order_type in order_type %}
        sum(case when order_type = '{{ order_type }}' then total_price_without_tax else 0 end) as {{ order_type }}_amount
        {%- if not loop. last -%}
        ,
        {%- endif -%}
        {% endfor -%}


from line_items
group by 1
)
select * from pivoted