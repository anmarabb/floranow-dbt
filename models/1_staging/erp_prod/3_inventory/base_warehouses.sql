with
  prep_countryas as (select distinct country_iso_code as code, country_name from `floranow.erp_prod.country`),
  prep_stock_count as (select warehouse_id, count(*) as stock_count, from {{source('erp_prod', 'stocks')}} as st group by 1)
  --prep_reseller_count as (select warehouse_id, count(*) as reseller_count, from {{ ref('dim_reseller') }} group by 1)
select 

w.id as warehouse_id,
w.name as warehouse_name,

c.country_name as warehouse_country,

w.region_name as warehouse_region,
w.reseller_id,
--re.reseller_name,
--re.reseller_debtor_number,

w.company_id,
w.landing_region_id,
w.status,
w.created_at,
w.updated_at,
w.deleted_at, 

co.name as company_name,

sc.stock_count,
rc.reseller_count,
current_timestamp() as ingestion_timestamp, 
  

from {{ source('erp_prod', 'warehouses') }} as w
left join prep_countryas as c on w.country = c.code
--left join {{ref('dim_reseller')}} as re on re.reseller_id = w.reseller_id
left join {{ref('stg_companies')}} as co on co.id = w.company_id
left join prep_stock_count as sc on sc.warehouse_id = w.id
--left join prep_reseller_count as rc on rc.warehouse_id = w.id