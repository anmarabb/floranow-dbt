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





CASE
    WHEN ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC(d, MONTH)) = 1 THEN
        CASE WHEN DATETIME_DIFF(DATETIME_SUB(DATETIME_ADD(DATETIME_TRUNC(d,MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY),d,DAY) >=0 
            THEN DATETIME_DIFF(DATETIME_SUB(DATETIME_ADD(DATETIME_TRUNC(d,MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY),d,DAY) 
            ELSE 0 
        END
    ELSE 0 
END AS days_remaining,

    DATETIME_DIFF(d,DATE_TRUNC(d,MONTH),DAY)+1 as days_passed,
    
FROM  {{ref('stg_budget')}} as db
JOIN UNNEST(db.date_range) AS d


)

select * from source

