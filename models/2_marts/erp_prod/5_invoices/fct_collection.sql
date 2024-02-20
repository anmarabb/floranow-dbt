
select 
    b.date,
    b.financial_administration,
    b.account_manager,
    --b.city,
    --b.client_category,
    b.monthly_budget,
    b.daily_budget,
    b.warehouse,

FROM  {{ref('int_collection')}} as b   

