
select 
    b.date,
    b.financial_administration,
    b.account_manager,
    b.city_related as City,
    b.user_category,
    b.monthly_budget,
    b.daily_budget,
    b.warehouse,
    
    b.mtd_budget,
    b.current_month_budget,
    CONCAT(coalesce(financial_administration,''), coalesce(account_manager,''), coalesce(city_related,''), coalesce(user_category,''), coalesce(warehouse,'')) as budget_link

FROM  {{ref('int_budget')}} as b   

