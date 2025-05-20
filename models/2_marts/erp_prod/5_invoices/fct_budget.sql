
select 
    b.date,
    b.financial_administration,
    b.account_manager,
    b.Country as City,
    b.user_category,
    b.monthly_budget,
    b.daily_budget,
    b.warehouse,
    
    -- b.mtd_budget,
    b.current_month_budget,
    CONCAT(coalesce(financial_administration,''), coalesce(account_manager,''), coalesce(Country,''), coalesce(user_category,''), coalesce(warehouse,'')) as budget_link,

    case when  date_diff(date(date) , current_date() , MONTH) = 0 and extract(day FROM date) <= extract(day FROM current_date()) then daily_budget else 0 end as mtd_budget,
    case when date_diff(current_date(),date(date), MONTH) = 1 and extract(day FROM date) <= extract(day FROM current_date()) then daily_budget else 0 end as lmtd_budget,
    CASE WHEN EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM current_date()) and extract(month FROM date) <= extract(month FROM current_date())
    and date(date) <= current_date() THEN daily_budget ELSE 0 END AS ytd_budget,

FROM  {{ref('int_budget')}} as b   

