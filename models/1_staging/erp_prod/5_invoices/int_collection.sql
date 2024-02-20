with

source as ( 

SELECT 
    db.year_month,
    d AS date,  
    db.daily_budget,
    db.financial_administration,
    db.account_manager,
    --db.city,
    --db.client_category,
    db.warehouse,

    CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC(date, MONTH),financial_administration,account_manager/*,city*/) = 1 THEN 
            SUM(db.daily_budget) OVER (PARTITION BY DATE_TRUNC(date, MONTH),financial_administration,account_manager/*,city*/)
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



FROM  {{ref('stg_collection')}} as db
JOIN UNNEST(db.date_range) AS d


)

select * from source

