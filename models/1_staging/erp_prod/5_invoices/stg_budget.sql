

    SELECT 
        b.financial_administration,
        b.account_manager,
        b.city,
        b.date,
        b.client_category,
        PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT_TIMESTAMP('%Y-%m', b.date), '-01')) as year_month,
        b.budget,
        b.budget / CAST(DATETIME_DIFF(DATETIME_ADD(DATETIME_TRUNC(b.date, MONTH), INTERVAL 1 MONTH), DATETIME_TRUNC(b.date, MONTH), DAY) AS FLOAT64) AS daily_budget,
        GENERATE_DATE_ARRAY(DATE(DATETIME_TRUNC(b.date, MONTH)), DATE(DATETIME_SUB(DATETIME_ADD(DATETIME_TRUNC(b.date, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY))) AS date_range,

    FROM {{ source('erp_prod', 'budget') }} as b


