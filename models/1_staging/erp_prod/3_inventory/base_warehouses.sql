with
  prep_countryas as (select distinct country_iso_code as code, country_name from `floranow.erp_prod.country`),
  prep_stock_count as (select warehouse_id, count(*) as stock_count, from {{source('erp_prod', 'stocks')}} as st group by 1),
  prep_reseller_count as (select warehouse_id, count(*) as reseller_count, from {{ source('erp_prod', 'users') }} as u where u.customer_type = 0 group by 1)
select 

w.warehouse_id,
w.warehouse_name,

c.country_name as warehouse_country,

w.warehouse_region,
w.reseller_id,

re.name as reseller_name,
re.debtor_number as reseller_debtor_number,

w.company_id,
w.landing_region_id,
w.status,
w.created_at,
w.updated_at,
w.deleted_at, 

co.name as company_name,

sc.stock_count,
rc.reseller_count,

case 
    when w.country ='SA' then 'KSA'
    when w.country ='AE' then 'UAE'
    when w.country ='QA' then 'Oatar'
    when w.country ='JO' then 'Jordan'
    when w.country ='KW' then 'kuwait'
    else 'check'
    end as financial_administration,




case
when w.warehouse_name = 'Dammam Warehouse' then 'FN-Dammam'
when w.warehouse_name = 'Dubai Warehouse' then 'DXB'
when w.warehouse_name = 'Jeddah Warehouse' then 'FN-Jeddah'
when w.warehouse_name = 'Riyadh Warehouse' then 'FN-Riyadh'
when w.warehouse_name = 'Tabuk Warehouse' then 'FN-Tabuk'
when warehouse_name = 'Hail Warehouse' then 'FN-Hail'
when warehouse_name = 'Qassim Warehouse' then 'FN-Qassim'
when warehouse_name = 'Medina Warehouse' then 'FN-Medinah'
when warehouse_name = 'Jouf WareHouse' then 'FN-Jouf'
when warehouse_name = 'Hafar WareHouse' then 'FN-Hafar'
when warehouse_name = 'Kuwait Warehouse' then 'KWT-Design Cell'
else null end as box_label,



current_timestamp() as ingestion_timestamp, 
  

from {{ ref('stg_warehouses') }} as w 
left join prep_countryas as c on w.country = c.code
left join {{ ref('base_users') }} as re on re.id = w.reseller_id
left join {{ref('stg_companies')}} as co on co.id = w.company_id
left join prep_stock_count as sc on sc.warehouse_id = w.warehouse_id
left join prep_reseller_count as rc on rc.warehouse_id = w.warehouse_id