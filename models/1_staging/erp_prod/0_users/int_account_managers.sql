with a as (

select 
account_manager,
financial_administration,
date,
daily_budget,
 --sum(daily_budget) as daily_budget, 
  from   {{ref('fct_budget')}} as b  
  where account_manager is not null
  and account_manager !=''
  and monthly_budget is not null
  --group by 1,2,3
)

select
a.account_manager,
a.financial_administration,

from   {{ ref('base_account_managers') }} as a 
where a.account_manager_type = 'USER'
