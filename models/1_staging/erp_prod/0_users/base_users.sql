
with
  prep_countryas as (select distinct country_iso_code as code, country_name from `floranow.erp_prod.country`),
  base_manageable_accounts_user as (select account_manager_id, manageable_id, from `floranow.erp_prod.manageable_accounts` where manageable_type = 'User')


select
            --PK
                u.id,

            --FK
                u.warehouse_id,
                u.parent_id,
                u.user_reference_id,
                u.payment_term_id,
                u.master_id,
                u.route_id,
                u.company_id,
                u.user_category_id,
                u.financial_administration_id,
                u.netsuite_ref_id,
                u.credit_note_template_id,
                u.invoice_template_id,
                u.statement_template_id,
                u.payment_receipt_template_ar_id,
                u.statement_template_ar_id,
                u.creditable_invoice_template_id,
                u.invoice_template_ar_id,
                u.payment_receipt_template_id,
                u.credit_note_template_ar_id,
                u.creditable_invoice_template_ar_id,
                u.bank_account_id,
                u.ledger_template_id,
                u.ledger_template_ar_id,


            --dim
                
                
                case when u.internal is true then 'Internal' else 'External' end as account_type,
                case when u.customer_type = 0 then 'reseller' when u.customer_type = 1 then 'retail' when u.customer_type = 2 then 'fob' when u.customer_type = 3 then 'cif' else 'check_my_logic' end as customer_type,
                

                --BOOLEAN
                    u.order_block,
                    u.internal,
                    u.has_all_warehouses_access,
                    u.has_trade_access,
                    u.allow_due_invoices,
                    u.customized_invoice,
                    u.with_stamp,
                    
                    





                --STRING
                    u.name,
                    u.debtor_number,
                    u.email,
                    case when u.email like '%fake.com%' then 'fake' else 'good' end as fake_filter,
                    u.street_address,
                    u.phone_number,
                    u.username,
                    u.accessible_warehouses,
                    u.odoo_code,
                    u.statement_type,

                    --INITCAP(u.city) as City,

                    case 
                        when u.state = 'AJ' then 'Ajman'
                        when u.state = 'AZ' and INITCAP(u.city) = 'Al Ain City'  then 'Al Ain'
                        when u.state = 'AZ' then 'Abu Dhabi'
                        when u.state = 'FU' then 'Sharjah' --Fujairah
                        when u.state = 'DU' then 'Dubai'
                        when u.state = 'RK' then 'Ras Al Khaimah'
                        when u.state = 'SH' and u.city = 'Ras al-Khaimah'  then 'Ras Al Khaimah'
                        when u.state = 'SH' then 'Sharjah' 
                        when u.state = 'UQ' then 'Umm Al Quwain'
                        else 'To Be Fixed Ask IT'
                        end as  City,



                    u.state,
                    u.country as row_country,
                    u.city as row_city,





            --date
                u.created_at,
                u.updated_at,
                u.deleted_at,

            --fct
                u.remaining_credit,
                u.credit_limit,
                u.debit_balance,
                u.credit_balance,
                u.pending_balance,
                --u.pending_order_requests_balance,
                --u.credit_note_balance,
                --u.advance_balance,
    



  w.warehouse_name as Warehouse,
  u2.name as account_manager,
  uc.name as user_category,
  c.country_name as Country,
  pt.name as payment_term,

  --f.name as financial_administration,
  case when f.name = 'Saudi' then 'KSA' else f.name end as financial_administration,


  
  com.name as company_name,

current_timestamp() as ingestion_timestamp,

  from {{ source('erp_prod', 'users') }} as u
  left join prep_countryas as c on u.country = c.code
  left join base_manageable_accounts_user as mau on mau.manageable_id = u.id
  left join {{ source('erp_prod', 'account_managers') }} as account_m on mau.account_manager_id = account_m.id
  left join {{ source('erp_prod', 'users') }} as u2 on u2.id = account_m.user_id
  left join {{ source('erp_prod', 'user_categories') }} as uc on u.user_category_id = uc.id
  left join {{ source('erp_prod', 'payment_terms') }} as pt on pt.id = u.payment_term_id
  left join {{ source('erp_prod', 'financial_administrations') }} as f on f.id = u.financial_administration_id
  left join {{ ref('stg_warehouses') }} as w on w.warehouse_id = u.warehouse_id 
  left join {{ ref('stg_companies') }} as com on com.id = u.company_id 
