
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
when st.id in (10,11) then 'Internal - Spinnyes'
when st.id in (16,17) then 'Internal - TBF'
when st.id in (18,19,22,23,90,91,128,129) then 'Marketplace - Astra Express'
when st.id in (304,305) then 'Marketplace - Ward'
when st.id in (14,15) then 'Marketplace - Wish Flowers'
when st.id in (1,2,6,7,20,21,56,57,92,93,126,127,130,131,164,165,198,199,232,233,266,267,300,301,302,303,338,339,372,373,407,413,417,485,523,525) then 'Reselling'
else 'not_set'
end as stock_model,

case 
when st.id in (12,13) then 'Internal - Jumeriah'
when st.id in (10,11) then 'Internal - Spinnyes'
when st.id in (16,17) then 'Internal - TBF'
when st.id in (18,19,22,23,90,91,128,129,266,267,486,487,526,527,564,565) then 'Marketplace - Astra Express'
when st.id in (304,305) then 'Marketplace - Ward'
when st.id in (14,15) then 'Marketplace - Wish Flowers'
when st.id in (164,165,406,407,408,409,410,411,412,413,414,415,416,417,450,451) then 'Event'
when st.id in (20,21,56,57,130,131,198,199,300,301,484,485,522,523,530,531,566,567) then 'Imported items'
when st.id in (92,93,232,233,302,303) then 'Flash sales'
when st.id in (6,7) then 'Columbia items'
when st.id in (126,127) then 'Equador items'
when st.id in (372,373) then 'returns & additional Kuwait'
when st.id in (528,529,524,525,520,521) then 'mster account'
when st.id in (1,2,338,339,525) then 'Reselling'
else 'not_set'
end as stock_model_jibu, --jibu



current_timestamp() as ingestion_timestamp,
 




from {{source('erp_prod', 'stocks')}} as st
left join {{ ref('base_users') }} as re on re.id = st.reseller_id
left join {{ref('stg_warehouses')}} as w on w.warehouse_id = st.warehouse_id
left join {{ref('stg_feed_sources')}} as fs on fs.feed_source_id = st.out_feed_source_id
