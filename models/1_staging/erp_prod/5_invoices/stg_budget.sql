With source as (
 select * from {{ source('erp_prod', 'budget') }}
)
select 
bud.financial_administration,
bud.account_manager,
bud.city,
bud.date,
FORMAT_TIMESTAMP('%Y-%m', bud.date) as month_year,  -- creates a string in the format 'YYYY-MM'


bud.client_category,
bud.budget,
bud.warehouse,
CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC(date, MONTH)) = 1 THEN 
            SUM(budget) OVER (PARTITION BY DATE_TRUNC(date, MONTH),financial_administration)
        ELSE 
            NULL 
    END AS monthly_budget,


DATETIME_DIFF(date(DATETIME_SUB(DATETIME_ADD(DATETIME_TRUNC(CURRENT_DATE(),MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY)),DATE_TRUNC( current_date(),month),DAY)+1 as days_total_current_month,
DATETIME_DIFF(DATETIME_SUB(DATETIME_ADD(DATETIME_TRUNC(CURRENT_DATE(),MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY),CURRENT_DATE(),DAY) as days_remaining_current_month,
DATETIME_DIFF(CURRENT_DATE(),DATE_TRUNC( current_date(),month),day) as days_left_current_month,  --days_passed_current_month



current_timestamp() as ingestion_timestamp,

from source as bud
