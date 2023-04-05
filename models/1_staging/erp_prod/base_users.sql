
with
  prep_countryas as (select distinct country_iso_code as code, country_name from `floranow.erp_prod.country`),
  base_manageable_accounts_user as (select account_manager_id, manageable_id, from `floranow.erp_prod.manageable_accounts` where manageable_type = 'User')


select

    u.id,
    u.name,
    u.debtor_number,
    u.warehouse_id,
    --w.warehouse_name,

 case
    when u.customer_type = 0 then 'reseller'
    when u.customer_type = 1 then 'retail'
    when u.customer_type = 2 then 'fob'
    when u.customer_type = 3 then 'cif'
    else 'check_my_logic'
  end as customer_type,
  
  case when u.internal is true then 'Internal' else 'External' end as account_type,

  
  
  
  
  u2.name as account_manager,
  uc.name as user_category,
  c.country_name as country,
  pt.name as payment_term,
  f.name as financial_administration,

  
current_timestamp() as ingestion_timestamp,

  from {{ source('erp_prod', 'users') }} as u
  left join prep_countryas as c on u.country = c.code
  left join base_manageable_accounts_user as mau on mau.manageable_id = u.id
  left join {{ source('erp_prod', 'account_managers') }} as account_m on mau.account_manager_id = account_m.id
  left join {{ source('erp_prod', 'users') }} as u2 on u2.id = account_m.user_id
  left join {{ source('erp_prod', 'user_categories') }} as uc on u.user_category_id = uc.id
  left join {{ source('erp_prod', 'payment_terms') }} as pt on pt.id = u.payment_term_id
  left join {{ source('erp_prod', 'financial_administrations') }} as f on f.id = u.financial_administration_id

 -- left join {{ref('base_warehouses')}} as w on w.warehouse_id = u.warehouse_id
