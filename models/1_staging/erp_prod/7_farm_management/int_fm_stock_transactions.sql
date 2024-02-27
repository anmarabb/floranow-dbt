
with a as (
        select
        fm_product_id,
        production_date,

        sum(case when transaction_type = 'INBOUND'then quantity else 0 end) as inbound_quantity,
        sum(case when transaction_type = 'OUTBOUND'then quantity else 0 end) as outbound_quantity,

        from   {{ ref('stg_fm_stock_transactions') }} as st
        where production_date >='2024-01-01'
        --where  production_date is not null 
        group by 1,2
        order by 1,2
    )
select
st.production_date,

p.product_name,
p.sub_group,
p.color,
p.contract_status,
p.bud_count,
p.stem_length,

st.inbound_quantity - st.outbound_quantity as quantity,
from a as st
left join {{ ref('fct_fm_products') }} as p on st.fm_product_id = p.fm_product_id

where inbound_quantity - outbound_quantity !=0
