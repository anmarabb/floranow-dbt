
select 
            --PK
                st.id as stock_id,
            --FK
                st.warehouse_id,
                st.reseller_id,
                st.out_feed_source_id,


--st.name as stock_name,

case 
    when st.name = 'default inventory stock' then 'Inventory Stock'
    when st.name = 'default flying stock' then 'Flying Stock'
    else null
    end as stock_name,


w.warehouse_name,
re.name as reseller_name,
fs.feed_source_name as out_feed_source_name,

re.debtor_number as reseller_debtor_number,
re.account_type,
re.account_manager,
re.user_category,
re.country,
re.payment_term,
re.financial_administration,

case when st.stock_type = 0 then 'inventory' else 'flying' end as stock_type,
case when st.status = 0 then 'visible' else 'hidden' end as stock_status,



st.availability_type,
st.has_custom_sales_unit,
st.custom_sales_unit,


case 
when st.id in (12,13) then 'Internal - Jumeriah'
when st.id in (10,11,618,619) then 'Internal - Spinnyes'
when st.id in (16,17) then 'Internal - TBF'
when st.id in (15) then 'Commission Based - Wish Flowers'
when st.id in (304,305) then 'Commission Based - Ward'
when st.id in (128,129,18,19,22,23,266,267,486,526,529,565,90,91) then 'Commission Based - Astra Express'
when st.id in (165,64,569,451,450,415,414,571,570,408,411,410,572,407,406,413,412) then 'Reselling Event'
when st.id in (614,615) then 'Internal - BX Shop'
when st.id in (616,617) then 'Internal - Wedding & Events'
when st.id in (522,484,567,566,531,530) then 'Reselling'
else 'Reselling'
end as stock_model,


current_timestamp() as ingestion_timestamp,
 




from {{source('erp_prod', 'stocks')}} as st
left join {{ ref('base_users') }} as re on re.id = st.reseller_id
left join {{ref('stg_warehouses')}} as w on w.warehouse_id = st.warehouse_id
left join {{ref('stg_feed_sources')}} as fs on fs.feed_source_id = st.out_feed_source_id
