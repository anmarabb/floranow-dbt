

    SELECT 
        b.financial_administration,
        b.account_manager,
        b.City,
        b.data_date as date,
        b.client_category as user_category ,
        b.warehouse,
        b.budget,

        PARSE_DATE('%Y-%m-%d', CONCAT(FORMAT_TIMESTAMP('%Y-%m', b.data_date), '-01')) as year_month,
        b.budget / CAST(DATETIME_DIFF(DATETIME_ADD(DATETIME_TRUNC(b.data_date, MONTH), INTERVAL 1 MONTH), DATETIME_TRUNC(b.data_date, MONTH), DAY) AS FLOAT64) AS daily_budget,
        GENERATE_DATE_ARRAY(DATE(DATETIME_TRUNC(b.data_date, MONTH)), DATE(DATETIME_SUB(DATETIME_ADD(DATETIME_TRUNC(b.data_date, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY))) AS date_range,

    FROM {{ source(var('erp_source'), 'budget') }} as b


