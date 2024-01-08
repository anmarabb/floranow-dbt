with

source as ( 
        
select    
    user_validity_filter,
    u.id as customer_id,
    u.debtor_number,
    u.name as Customer,
    u.financial_administration, --UAE, Saudi, Qatar, Jordan, Kuwait, Bulk, Internal
    u.email,

case when user_validity_filter = 'normal' and client_engagement_status = 'Active' then 1 else 0 end as active_clients,
case when user_validity_filter = 'normal' and client_engagement_status = 'Churned' then 1 else 0 end as churned_clients,
case when user_validity_filter = 'normal' and client_engagement_status = 'Inactive' then 1 else 0 end as inactive_clients,
case when user_validity_filter = 'normal' and client_engagement_status = 'Blocked' then 1 else 0 end as blocked_clients,

case when user_validity_filter = 'normal' and client_engagement_status != 'Active' then 1 else 0 end as not_active_clients,

case when user_validity_filter = 'normal' then 1 else 0 end as registered_clients,

    Country,
    City,
    account_manager,

    have_master_id,
 

    user_category as customer_category,
    Warehouse,



    row_city,
    u.state,



    --dim Selector
        u.account_type, --External, Internal
        u.customer_type, --reseller, retail, fob, cif
        u.company_name, --Flora Express Flower Trading LLC, Bloomax Flowers LTD, Global Floral Arabia tr
        case when odoo_code is not null then 'Odoo' else null end as odoo_code_check,
        u.payment_term,



    u.odoo_code,
    u.statement_type,


    accessible_warehouses,
    --commercial_register,
    --lpo_number,
    --accessible_internal_stocks,
    order_block,
    u.has_all_warehouses_access,
    u.has_trade_access,
    u.allow_due_invoices,
    u.customized_invoice,
    u.with_stamp,


u.created_at,
u.updated_at,
u.deleted_at,


customers_last_order_date,
customer_acquisition_date,
customers_last_purchase_date,
customer_lifespan,
months_of_customer_engagement,
total_credit_note_per_customer,
total_gross_revenue_per_customer,
total_net_revenue_per_customer,
monthly_demand,
client_value_segments,




 user_link,

 client_engagement_status,

 total_blocked,

mtd_gross_revenue,
mtd_credit_note,
lmtd_gross_revenue,
lmtd_credit_note,
lymtd_credit_note,
lymtd_gross_revenue,
m_1_gross_revenue,
m_1_credit_note,
m_2_gross_revenue,
m_2_credit_note,
m_3_gross_revenue,
m_3_credit_note,


credit_balance,   


credit_limit, 
debit_balance, 
pending_balance, 
pending_order_requests_balance,
total_pending_balance,
days_since_last_drop,

credit_balance + debit_balance as residual,


mtd_orders,
mtd_orders_affected,

    current_timestamp() as insertion_timestamp 

from {{ ref('int_customer')}} as u

)

select * from source


