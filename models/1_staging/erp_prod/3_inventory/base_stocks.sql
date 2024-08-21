
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
--st.custom_sales_unit,

st.label as modified_stock_model_details,
TRIM(IF(STRPOS(label, '-') > 0,SUBSTR(label, 1, STRPOS(label, '-') - 1),label)) AS modified_stock_model,
case 
when st.id in (12,13) then 'Internal - Jumeriah'
when st.id in (10,11,618,619) then 'Internal - Spinnyes'
when st.id in (16,17) then 'Internal - TBF'
when st.id in (15) then 'Commission Based - Wish Flowers'
when st.id in (304,305) then 'Commission Based - Ward'
when st.id in (128,129,18,19,22,23,266,267,486,526,529,565,90,91,527,564) then 'Commission Based - Astra Express'
when st.id in (165,64,569,451,450,415,414,571,570,408,411,410,572,407,406,413,412,416,417,164,165,568,573) then 'Reselling Event'
when st.id in (613,614,615,606,607,608) then 'Internal - BX Shop'
when st.id in (616,617) then 'Internal - Wedding & Events'
when st.id in (621,620) then 'Internal - BX DMM'
when st.id in (622,623) then 'Internal - Grandiose'
when st.id in (808,809) then 'Internal - Riyadh Project X'
when st.id in (812,813) then 'Internal - Dammam Project X'
when st.id in (730,522,484,567,566,531,530,523,485,373,372,301,300,199,198,131,130,127,126,57,56,21,20,7,6,2,1) then  'Reselling'
     
else 'Others'
end as stock_model_details,

case 
when st.id in (12,13) then 'Internal'
when st.id in (10,11,618,619) then 'Internal'
when st.id in (16,17) then 'Internal'
when st.id in (15) then 'Commission Based'
when st.id in (304,305) then 'Commission Based'
when st.id in (128,129,18,19,22,23,266,267,486,526,529,565,90,91,527,564) then 'Commission Based'
when st.id in (165,64,569,451,450,415,414,571,570,408,411,410,572,407,406,413,412,416,417,164,165,568,573) then 'Reselling'
when st.id in (613,614,615,606,607,608) then 'Internal'
when st.id in (616,617) then 'Internal'
when st.id in (621,620) then 'Internal'
when st.id in (622,623) then 'Internal'
when st.id in (808,809) then 'Internal - Project X'
when st.id in (812,813) then 'Internal - Project X'
when st.id in (730,522,484,567,566,531,530,523,485,373,372,301,300,199,198,131,130,127,126,57,56,21,20,7,6,2,1) then  'Reselling'
else 'Others'
end as stock_model,

current_timestamp() as ingestion_timestamp,
 



from {{source(var('erp_source'), 'stocks')}} as st
left join {{ ref('base_users') }} as re on re.id = st.reseller_id
left join {{ref('stg_warehouses')}} as w on w.warehouse_id = st.warehouse_id
left join {{ref('stg_feed_sources')}} as fs on fs.feed_source_id = st.out_feed_source_id
