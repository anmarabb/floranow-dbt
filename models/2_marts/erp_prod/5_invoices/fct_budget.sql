
select 
    b.date,
    b.financial_administration,
    b.account_manager,
    b.city,
    b.user_category,
    b.monthly_budget,
    b.daily_budget,
    b.warehouse,
    
    b.mtd_budget,
    b.current_month_budget,

FROM  {{ref('int_budget')}} as b   

