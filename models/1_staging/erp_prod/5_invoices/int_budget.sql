with

source as ( 

SELECT 
    db.year_month,
    d AS date,  
    db.daily_budget,
    db.financial_administration,
    db.account_manager,
    db.city,
    db.client_category,
    db.warehouse,
    case when  date_diff(date(d) , current_date() , MONTH) = 0 then db.daily_budget else 0 end as mtd_budget,
      CASE 
            WHEN EXTRACT(YEAR FROM date(d)) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM date(d)) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN db.daily_budget
            ELSE 0
        END AS current_month_budget,


    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC(date, MONTH),financial_administration,account_manager,city) = 1 THEN 
            SUM(db.daily_budget) OVER (PARTITION BY DATE_TRUNC(date, MONTH),financial_administration,account_manager,city)
        ELSE 
            NULL 
    END AS monthly_budget,


    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC(date, MONTH),financial_administration) = 1 THEN 
            max(DATETIME_DIFF(date(DATETIME_SUB(DATETIME_ADD(DATETIME_TRUNC(date,MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY)),DATE_TRUNC( date,month),DAY)+1) OVER (PARTITION BY DATE_TRUNC(date, MONTH),financial_administration)
        ELSE 
            NULL 
    END AS days_total,





--EXTRACT(DAY FROM d) as day_of_month,
CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY d,financial_administration) = 1 THEN EXTRACT(DAY FROM d)
    ELSE NULL
END as day_of_month,


CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY d,financial_administration) = 1 THEN sum(daily_budget) OVER (PARTITION BY d,financial_administration) 
    ELSE NULL
END as partition_budget,



FROM  {{ref('stg_budget')}} as db
JOIN UNNEST(db.date_range) AS d


)

select * from source

