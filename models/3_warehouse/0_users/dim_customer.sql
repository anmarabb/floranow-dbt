with

source as ( 
        
select     
    u.id as customer_id,
    u.name as Customer,
    u.financial_administration as User_Market, --UAE, Saudi, Qatar, Jordan, Kuwait, Bulk, Internal




    Country,
    City,
    fake_filter,
 

    u.user_category as Segment,
    u.Warehouse,



    row_city,
    u.state,

    u.debtor_number,


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

    current_timestamp() as insertion_timestamp 

from {{ ref('int_customer')}} as u

)

select * from source
