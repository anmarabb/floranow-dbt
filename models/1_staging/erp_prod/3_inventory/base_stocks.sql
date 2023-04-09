
select 

st.id as stock_id,
st.name as stock_name,

st.warehouse_id,
w.warehouse_name,


st.reseller_id,


re.name as reseller_name,
re.debtor_number as reseller_debtor_number,
re.account_type,
re.account_manager,
re.user_category,
re.country,
re.payment_term,
re.financial_administration,

case when st.stock_type = 0 then 'inventory' else 'flying' end as stock_type,
case when st.status = 0 then 'visible' else 'hidden' end as stock_status,

st.out_feed_source_id,
fs.feed_source_name as out_feed_source_name,

st.availability_type,
st.has_custom_sales_unit,
st.custom_sales_unit,



current_timestamp() as ingestion_timestamp,
 




from {{source('erp_prod', 'stocks')}} as st
left join {{ ref('base_users') }} as re on re.id = st.reseller_id
left join {{ref('stg_warehouses')}} as w on w.warehouse_id = st.warehouse_id
left join {{ref('stg_feed_sources')}} as fs on fs.feed_source_id = st.out_feed_source_id
