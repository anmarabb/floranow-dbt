
with
  prep_countryas as (select distinct country_iso_code as code, country_name from {{ source(var('erp_source'), 'country') }}),
  base_manageable_accounts_user as (select account_manager_id, manageable_id, from {{ source(var('erp_source'), 'manageable_accounts') }}  where manageable_type = 'User')


select
            --PK
                u.id,

            --FK
                u.warehouse_id,
                --u.parent_id,
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

                case 
                    when u.order_blocked_status = 0 then 'Unblocked'
                    when u.order_blocked_status = 1 then 'Manually Blocked'
                    when u.order_blocked_status = 2 then 'Exceeded Credit Limit'
                    when u.order_blocked_status = 3 then 'Overdue Invoices'
                    else null
                 end as order_blocked_status,


                 case when  u.order_blocked_status in (1,2,3) then '1' end as total_blocked,





            --dim              
                case when u.internal is true then 'Internal' else 'External' end as account_type,
                case when u.customer_type = 0 then 'reseller' when u.customer_type = 1 then 'retail' when u.customer_type = 2 then 'fob' when u.customer_type = 3 then 'cif' when u.customer_type is null then 'null' else 'check_my_logic' end as customer_type,
                

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

                    u.street_address,
                    u.phone_number,
                   -- u.username,
                    u.accessible_warehouses,
                    u.odoo_code,
                    u.statement_type,


                    --INITCAP(u.city) as City,
/*
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

*/
                        case 
                            when w.warehouse_name = 'Riyadh Warehouse' then 'Riyadh'
                            when w.warehouse_name = 'Jeddah Warehouse' then 'Jeddah'
                            when w.warehouse_name = 'Medina Warehouse' then 'Medina'
                            when w.warehouse_name = 'Tabuk Warehouse' then 'Tabuk'
                            when w.warehouse_name = 'Dammam Warehouse' then 'Dammam'
                            when w.warehouse_name = 'Qassim Warehouse' then 'Qassim'
                            when w.warehouse_name = 'Hail Warehouse' then 'Hail'
                            when w.warehouse_name = 'Hafar WareHouse' then 'Hafar'
                            when w.warehouse_name = 'Jouf WareHouse' then 'Jouf'
                            when w.warehouse_name = 'Hail Warehouse' then 'Riyadh'

                            when u.state = 'AZ' and INITCAP(u.city) = 'Al Ain City'  then 'Al Ain'
                            when u.state = 'AZ' then 'Abu Dhabi'
                            when u.state = 'FU' then 'Fujairah'
                            when u.state = 'UQ' then 'Umm Al Quwain'
                            else INITCAP(u.city)
                            end as City,



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

                u.debit_balance, --printed invoices not paid or partially paid
                u.credit_balance, --printed credit notes not used  and  payment transactions not used
                
                u.pending_balance, --line items for invoices not printed (draft))
                u.pending_order_requests_balance, --order requests  status: (requested or partially placed
                --u.credit_note_balance,
                --u.advance_balance,
                (u.debit_balance + u.credit_balance + u.pending_balance + u.pending_order_requests_balance) as total_pending_balance, 
                u.credit_balance + u.debit_balance as residual,
 


    



  w.warehouse_name as warehouse,
  u2.name as account_manager,
  uc.name as user_category,

  c.country_name as Country,
  pt.name as payment_term,

  --f.name as financial_administration,
  case when f.name = 'Saudi' then 'KSA' else f.name end as financial_administration,


  
  com.name as company_name,

        case 
           when u.email  like '%fake_%' and w.warehouse_name = 'Riyadh Warehouse' then 'fake_temp_Riyadh' 
           when u.email  like '%temp_%' and f.name != 'UAE' and w.warehouse_name = 'Riyadh Warehouse'  then 'fake_temp_Riyadh'
           when u.email  like '%fake_%' and w.warehouse_name != 'Riyadh Warehouse' then 'fake_temp_others' 
           when u.email  like '%temp_%' and f.name != 'UAE' and w.warehouse_name != 'Riyadh Warehouse'  then 'fake_temp_others'

           when u.internal is true then 'Internal'
           when uc.name in ('Closed','Deleted Customers') then 'Category Closed'
           WHEN u.debtor_number IN (
               '4383', '132005', '132013', '132014', '132031', '132077', '132081', '132083', '132091', '132092',
               '132101', '132102', '132104', '132105', '132106', '132107', '132113', '132120', '132134', '132135',
               '132139', '132152', '132153', '132155', '132166', '132170', '132185', '132192', '132194', '132200',
               '132202', '132203', '132206', '132208', '132214', '132216', '132220', '132228', '132234', '132245',
               '132250', '132369', '132437', 'BF60022', 'BF60094', 'BF601', 'BF60234', 'EVEMED', 'BF60257',
               'BF60262', 'BF60274', 'BF60275', 'BF60279', 'BF60298', 'BF603', 'BF60300', 'BF60308',
               '134249', 'BF60353', 'BF60355', 'BF60356', 'BF604', '134152', '134153', '134156', '134154',
               '134157', 'khaled', 'aaldridi', '134155', '134159', 'm.salah', 'ASHIK', '134002', 'ashik',
               '134160', '131168', '131318', '131241', '131342', '131220', '131160', '131246', 'ASTMED', 'lndmed',
               '131140', '131137', '131199', '131170', '131069', '131247', '131123', '131017', '131119',
               '131298', '131317', '131003', '131008', '131389', '131206', '131259', '131334', '131315', '131019',
               '131231', '131321', '131294', '131312', '131120', '131324', '131037', '131018', '131207',
               '131105', '131330', '131252', '131237', '131038', '131122', '131020', '131230', '131092', '131413',
               '131326', '131186', '131178', '131185', '131323', '131327', '131145', '131035', '131125', '131264',
               '131166', '131030', '131193', '131063', '131060', '131192', '131216', '131176', '131223', '131194',
               '131083', '131094', '131050', '131182', '131213', '131283', '131196', '131202', '131335', '131278',
               '131129', '131205', '131179', '131204', '131124', '131046', '131224', '131225', '131261', '131372',
               '131184', '131180', '131101', '131203', '131087', '131228', '131197', '131347', 'BJ70005', 'BJ70013',
               'BJ70083', 'BJ70126', 'BJ70114', 'BJ21479', 'BJ70061', '134191', '134193', '134202', '134219',
               '134015', '134053', '134052', '134017', '134016', '134038', '134044', '134081', '134055', '134083',
               '134078', '134012', '134032', '134046', '134072', '134273', 'moutaz', 'alamohammed', '130312',
               '130303', '130305', '130311', 'ASTRUH', 's.hussain', '130309', '130310', '130313', 'm.bilal',
               'noushad', '130528', 'LNDRUH', '134314', 'scriyadh', '130314', '130521', '130304', '130307',
               '130306', 'm.tayseer', '4769', '134005', '132008', '132009', '134064', '132145', '132260',
               '132261', '132262', '132263', '132264', '132265', '132266', '132267', '132268', '132269', '132270',
               '132271', '132272', '132273', '132274', '132350', '132351', '132352', '132353', '132354', '132355',
               '132356', '132358', '132359', '132360', '132361', '132362', '132363', '132364', '132365', '132366',
               '132367', '132368', '132370', '132371', '132372', '132373', '132374', '132375', '132376', '132377',
               '132378', '132379', '132380', '132382', '132383', '132384', '132385', '132386', '132387', '132388',
               '132389', '132390', '132391', '132393', '132394', '132395', '132396', '132397', '132398', '132399',
               '132400', '132401', '132402', '132403', '132404', '132405', '132406', '132407', '132408', '132409',
               '132410', '132411', '132412', '132413', '132414', '132416', '132417', '132418', '132419', '132420',
               '132421', '132422', '132423', '132424', '132425', '132426', '130205', '132427', '132428', '132429',
               '130136', '130035', '130227', '132430', '132431', '132432', '130074', '132433', '130182', '130174',
               'ABDURAFEEQ', 'ALIAKBAR', 'ANSONJAMAL', 'BABUL', 'EVEDMM', 'FAISAL', '130192', 'faisal',
               'hani', 'hebeeb', 'Irshad', '130158', 'j.cabarles', '130079', 'jazer', 'jojo', 'JUNAIDALI',
               '130180', '130124', '130131', '130135', 'k.khalel', 'LNDDMM', 'MDMONIR', 'MPUTHIYAPURAYIL',
               '130010', '130213', 'NAWAF', 'NISARMON', '130149', 'salah', 'sameh', 'SAMEHKHAIRY', 'UNAIS',
               'ASTHAF', '130151', 'bf60118', '130110', '130111', 'EVEHAF', 'hafartest', 'KHALID', '130146',
               'LNDHAF', 'MCHAFAR', 'MDAZAD', 'HAILTST', 'bhmarketing', 'demo_floranow', 'LNDHAI',
               '130073', '130164', '130125', '130155', '134145', '130165', 'ASTHAI', '130137', '130093', '130152',
               '130092', '130075', '130077', '130123', '130095', 'EVEHAI', 'deenislam', 'WIJDAN',
               'mariamraja', 'anas', 'cashcustomerhail2', 'FLORANOWVIP', 'FNCLASSA', 'WALKINCUSTOMER',
               '130101', 'EVEJOU', 'ASTJOU', 'BJ00003', 'LNDJOU', '130178', '130108', '130159', '130191',
               '130156', 'bq40111111', '130173', '130154', '130243', '130254', 'bq40dsadas', 'EVEQAS', 'FNQSIM',
               'fntestqassim', 'LNDQAS', 'walkintestqassim', '133209', '133206', '133082', '133071', '133132',
               '133042', '133049', '133180', '133179', '133064', '133229', '133025', '133104', '133224',
               '133073', '133266', '133230', '13079', '133084', '133197', '133149', '133150', '133164', '133145',
               '133156', '133163', '133146', '133182', '133127', '133155', '133153', '133184', '133154', '133239',
               '133166', '133272', '133126', '133160', '133157', '133158', '133159', '133125', '133131', '133188',
               '133101', '133173', '133074', '133069', '133186', '133176', '133148', '133205', '133151', '133175',
               '133161', '133190', '133147', '133048', '133072', '133121', '133248', '133227', '133057',
               '133168', '133162', '133171', '133172', '133191', '133167', '133193', '133170', '133194', '133189',
               'YASER', 'm.masri', 'mmustafa', 'FSTUU'
           )
           THEN 'Faisal Comment'

else 'normal' end as user_validity_filter,

case 
when u.debtor_number in ('ASTMED','FNQSIM','132009','132008','130220','134002','130257','130188','LNDHAF','LNDHAI','LNDJOU','LNDQAS','Indmed') then 'Internal Aging'
when u.debtor_number in ('131379','131106','131380','131107','131381','131108','131382','131109','131383','131110','131384','131111','131112','131386','131113','133208') then 'Astra Aging'
else 'Floranow Aging'
end as user_aging_type,

concat( "https://erp.floranow.com/users/", u.id) as user_link,




current_timestamp() as ingestion_timestamp,

  from {{ source(var('erp_source'), 'users') }} as u
  left join prep_countryas as c on u.country = c.code
  left join base_manageable_accounts_user as mau on mau.manageable_id = u.id
  left join {{ source(var('erp_source'), 'account_managers') }} as account_m on mau.account_manager_id = account_m.id
  left join {{ source(var('erp_source'), 'users') }} as u2 on u2.id = account_m.user_id
  left join {{ source(var('erp_source'), 'user_categories') }} as uc on u.user_category_id = uc.id
  left join {{ source(var('erp_source'), 'payment_terms') }} as pt on pt.id = u.payment_term_id
  left join {{ source(var('erp_source'), 'financial_administrations') }} as f on f.id = u.financial_administration_id
  left join {{ ref('stg_warehouses') }} as w on w.warehouse_id = u.warehouse_id 
  left join {{ ref('stg_companies') }} as com on com.id = u.company_id 
