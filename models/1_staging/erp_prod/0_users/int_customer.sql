WITH line_items AS 
(
    SELECT 
        customer_id,
        MAX(li.order_date) AS customers_last_order_date,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(li.order_date)), DAY) as days_since_last_order,
        --count (DISTINCT li.order_number) as customer_orders,
        count( DISTINCT case when  date_diff(date(delivery_date) , current_date() , MONTH) = 0 then order_number else null end) as mtd_orders,
        count ( DISTINCT case when  date_diff(date(delivery_date) , current_date() , MONTH) = 0 then order_with_incidents else null end) as mtd_orders_affected,

        --
     
    FROM  {{ ref('fct_order_items') }} as li
    GROUP BY
        customer_id

),

invoice AS 
(
    SELECT
        customer_id,
        MAX(i.invoice_header_printed_at) as customers_last_purchase_date,
        MIN(i.invoice_header_printed_at) as customer_acquisition_date,
        

 

        DATE_DIFF (DATE(MAX(i.invoice_header_printed_at)), DATE(MIN(i.invoice_header_printed_at)), MONTH) as customer_lifespan,
        COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at)) as months_of_customer_engagement,
       
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(i.invoice_header_printed_at)), DAY) AS days_since_last_drop,
        SUM(CASE WHEN i.invoice_header_printed_at IS NOT NULL THEN i.remaining_amount ELSE 0 END) as total_outstanding_balance,

        sum (mtd_gross_revenue) as mtd_gross_revenue,
        sum (mtd_credit_note) as mtd_credit_note,

        sum (lmtd_credit_note) as lmtd_credit_note,
        sum (lmtd_gross_revenue) as lmtd_gross_revenue,

        sum (lymtd_gross_revenue) as lymtd_gross_revenue,
        sum (lymtd_credit_note) as lymtd_credit_note,

        sum (m_1_gross_revenue) as m_1_gross_revenue,
        sum (m_1_credit_note) as m_1_credit_note,

        sum (m_2_gross_revenue) as m_2_gross_revenue,
        sum (m_2_credit_note) as m_2_credit_note,


        sum (m_3_gross_revenue) as m_3_gross_revenue,
        sum (m_3_credit_note) as m_3_credit_note,



        

        sum(gross_revenue) as total_gross_revenue_per_customer,
        sum(credit_note) as total_credit_note_per_customer,
        sum(gross_revenue+credit_note) as total_net_revenue_per_customer,
        sum(total_tax) as total_tax_per_customer,
        SAFE_DIVIDE(sum(gross_revenue+credit_note),COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at))) as monthly_demand,

        case 
            when SAFE_DIVIDE(sum(gross_revenue+credit_note),COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at))) >= 49999 then "1- Clients who pay +50K per month"
            when SAFE_DIVIDE(sum(gross_revenue+credit_note),COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at))) >=25000 then "2- Clients who pay +24K per month"
            when SAFE_DIVIDE(sum(gross_revenue+credit_note),COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at))) >=12000 then "3- Clients who pay +12K per month"
            when SAFE_DIVIDE(sum(gross_revenue+credit_note),COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at))) >=6000 then "4- Clients who pay +6K per month"
            when SAFE_DIVIDE(sum(gross_revenue+credit_note),COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at))) >=3000 then "5- Clients who pay +3K per month"
            when SAFE_DIVIDE(sum(gross_revenue+credit_note),COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at))) >=1000 then "6- Clients who pay +1K per month"
            when SAFE_DIVIDE(sum(gross_revenue+credit_note),COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', i.invoice_header_printed_at))) <1000 then "7- Clients who pay less than 999 per month"
            when date_diff(cast(Max(i.invoice_header_printed_at) as date), cast(Min(i.invoice_header_printed_at) as date ), MONTH) = 0 then 'One order clinets'
            when date_diff(cast(Max(i.invoice_header_printed_at) as date), cast(Min(i.invoice_header_printed_at) as date ), MONTH) is null then 'Zero order clinets'
            else 'Ceack'
        end as client_value_segments,

    FROM {{ ref('fct_invoices') }} as i

    GROUP BY
        customer_id
),

invoice_items AS
(
    SELECT
        customer_id,
        COUNT(DISTINCT ii.invoice_header_id) as total_order_count_per_customer,
        --SUM(ii.price_without_tax) as total_order_value_per_customer



    FROM  {{ ref('int_invoice_items') }} as ii
    GROUP BY
        customer_id
),

move_items AS
(
    SELECT
        user_id,
        sum( case when mi.entry_type = 'DEBIT' then mi.residual else 0 end) as mi_debit_balance,
        sum( case when mi.entry_type = 'CREDIT' then mi.residual else 0 end) as mi_credit_balance,
        sum(mi.residual) as mi_residual,
        sum(mi.total_debits) as mi_total_order_value_per_customer, --with VAT
        --total_credit_not_value_per_customer




    FROM  {{ ref('fct_move_items') }} as mi
    GROUP BY
        user_id
)



--move items
--incendnts


SELECT


    u.*,


    case
        when u.order_blocked_status != 'Unblocked' then 'Blocked'
        when li.days_since_last_order <= 7 then 'Active'
        when i.days_since_last_drop <= 7 then 'Active'   
        when li.days_since_last_order > 7 and li.days_since_last_order  <= 30 then 'Inactive'
        when i.days_since_last_drop > 7 and i.days_since_last_drop  <= 30 then 'Inactive'

        when li.days_since_last_order  > 30 then 'Churned'
        else 'Churned'
    end as client_engagement_status,



case when u.master_id is not null then 'have master' else null end as have_master_id,

    
case when i.customer_acquisition_date is not null then i.customer_acquisition_date else u.created_at end as customer_acquisition_date,



    li.customers_last_order_date,
    li.mtd_orders,
    li.mtd_orders_affected,
    i.customers_last_purchase_date,
    i.customer_lifespan,
    i.months_of_customer_engagement,
    i.monthly_demand,
    case when i.client_value_segments is null then 'Zero order clinets' else i.client_value_segments end as client_value_segments,



    i.days_since_last_drop,

    i.mtd_gross_revenue,
    i.mtd_credit_note,
    i.lmtd_credit_note,
    i.lmtd_gross_revenue,
    i.lymtd_credit_note,
    i.lymtd_gross_revenue,
    i.m_1_gross_revenue,
    i.m_1_credit_note,
    i.m_2_gross_revenue,
    i.m_2_credit_note,
    i.m_3_gross_revenue,
    i.m_3_credit_note,

    i.total_gross_revenue_per_customer,
    i.total_credit_note_per_customer,
    i.total_net_revenue_per_customer,
    i.total_tax_per_customer,

    mi.mi_total_order_value_per_customer,
    mi.mi_residual,
    mi.mi_credit_balance,
    mi.mi_debit_balance,







from   {{ ref('base_users') }} as u 
LEFT JOIN line_items as li ON u.id = li.customer_id
LEFT JOIN invoice as i ON u.id = i.customer_id
LEFT JOIN invoice_items as ii ON u.id = ii.customer_id
left join move_items as mi on u.id = mi.user_id 
