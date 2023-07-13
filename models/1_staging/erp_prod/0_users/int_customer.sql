WITH orders AS 
(
    SELECT 
        customer_id,
        MAX(li.created_at) AS customers_last_order_date,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) as days_since_last_order,
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) <= 7 THEN 'active'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) > 7 AND DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) <= 30 THEN 'inactive'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.created_at)), DAY) > 30 THEN 'churned'
            ELSE 'churned'
        END as Account_Status
    FROM  {{ ref('int_line_items') }} as li
    GROUP BY
        customer_id

),

invoice AS 
(
    SELECT
        customer_id,
        MAX(i.invoice_header_printed_at) as customers_last_purchase_date,
        MIN(i.invoice_header_printed_at) as customer_acquisition_date,
        DATE_DIFF (DATE(MAX(i.invoice_header_printed_at)), DATE(MIN(i.invoice_header_printed_at)), MONTH)+1 as customer_lifespan,
        COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at)) as months_of_customer_engagement,

        
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(i.invoice_header_printed_at)), DAY) AS days_since_last_drop,
        SUM(CASE WHEN i.invoice_header_printed_at IS NOT NULL THEN i.remaining_amount ELSE 0 END) as total_outstanding_balance
    FROM {{ ref('int_invoices') }} as i
    GROUP BY
        customer_id
),

invoice_items AS
(
    SELECT
        customer_id,
        COUNT(DISTINCT ii.invoice_header_id) as total_order_count_per_customer,
        SUM(ii.price_without_tax) as total_order_value_per_customer
    FROM  {{ ref('int_invoice_items') }} as ii
    GROUP BY
        customer_id
)

SELECT


    u.* EXCEPT (City),


    i.customer_acquisition_date,
    o.customers_last_order_date,
    i.customers_last_purchase_date,
    i.customer_lifespan,
    i.months_of_customer_engagement,

   case
        when u.financial_administration = 'Saudi' and u.City is null and  u.Warehouse like '%Hafar%' then 'Hafar'
        when u.City = 'Hafar Al-Batin' then 'Hafar'
        when u.financial_administration = 'Saudi' and u.City is null and  u.Warehouse like '%Qassim%' then 'Qassim'
        when u.City = 'Al-Qassim Region' then 'Qassim'
        when u.financial_administration = 'Saudi' and u.City is null and  u.Warehouse like '%Jeddah%' then 'Jeddah'
        when u.financial_administration = 'Saudi' and u.City is null and  u.Warehouse like '%Dammam%' then 'Dammam'
        when u.financial_administration = 'Saudi' and u.City is null and  u.Warehouse like '%Tabuk%' then 'Tabuk'
        when u.financial_administration = 'Saudi' and u.City is null and  u.Warehouse like '%Hail%' then 'Hail'
        when u.City like "Ha'il" then 'Hail'
        when u.financial_administration = 'Saudi' and u.City is null and  u.Warehouse like '%Riyadh%' then 'Riyadh'
        when u.financial_administration = 'Saudi' and u.City is null and  u.Warehouse like '%Medina%' then 'Medina'
        when u.City = 'Dawmat Al Jandal' then 'Jouf'

    else  u.city end as City,

from   {{ ref('base_users') }} as u 
LEFT JOIN orders as o ON u.id = o.customer_id
LEFT JOIN invoice as i ON u.id = i.customer_id
LEFT JOIN invoice_items as ii ON u.id = ii.customer_id
