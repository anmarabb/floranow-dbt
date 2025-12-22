with margin_drivers as
(
    select invoice_item_id,
           case 
                when trading_model = 'Pre-Selling' and order_type in ('ONLINE', 'IN_SHOP', 'PICKED_ORDER') and supplier != 'ASTRA Farms' 
                and (
                    (invoice_header_type = 'invoice' and generation_type = 'AUTO')
                    OR 
                    invoice_header_type = 'credit note'
                )
                and warehouse not like '%Project%' and user_category != 'SuperMarkets' then 'Platform(Pre-Sale)'

                when trading_model = 'Re-Selling (Express)' and order_type in ('ONLINE', 'IN_SHOP', 'PICKED_ORDER') and supplier != 'ASTRA Farms' 
                and (
                    (invoice_header_type = 'invoice' and generation_type = 'AUTO')
                    OR 
                    invoice_header_type = 'credit note'
                )
                and warehouse not like '%Project%' and user_category != 'SuperMarkets' then 'Platform(Re-Sale)'

                when trading_model = 'Pre-Selling' and order_type in ('OFFLINE', 'STANDING', 'ADDITIONAL') and supplier != 'ASTRA Farms' 
                and (
                    (invoice_header_type = 'invoice' and generation_type = 'AUTO')
                    OR 
                    invoice_header_type = 'credit note'
                )
                and warehouse not like '%Project%' and user_category != 'SuperMarkets' then 'Offline (Pre-Sale)'

                when trading_model = 'Re-Selling (Express)' and order_type in ('OFFLINE', 'STANDING', 'ADDITIONAL') and supplier != 'ASTRA Farms' 
                and (
                    (invoice_header_type = 'invoice' and generation_type = 'AUTO')
                    OR 
                    invoice_header_type = 'credit note'
                )
                and warehouse not like '%Project%' and user_category != 'SuperMarkets' then 'Offline (Re-Sale)'

                when user_category = 'SuperMarkets' then 'SuperMarkets'

                when warehouse like '%Project%' then 'SCaaS'

                when order_type in ('ONLINE', 'IN_SHOP', 'PICKED_ORDER') and supplier = 'ASTRA Farms' 
                and (
                    (invoice_header_type = 'invoice' and generation_type = 'AUTO')
                    OR 
                    invoice_header_type = 'credit note'
                )
                and warehouse not like '%Project%' and user_category != 'SuperMarkets' then 'Platform (Astra)'

                when order_type in ('OFFLINE', 'STANDING', 'ADDITIONAL') and supplier = 'ASTRA Farms' 
                and (
                    (invoice_header_type = 'invoice' and generation_type = 'AUTO')
                    OR 
                    invoice_header_type = 'credit note'
                )
                and warehouse not like '%Project%' and user_category != 'SuperMarkets' then 'Offline (Astra)'

            end as sales_channels,


    from {{ ref("int_invoice_items") }}
),
sales_channel_targets as (
    select 'Offline (Astra)' as sales_channel, 'UAE' as financial_administration, 0.36 as target_margin_percentage
    union all select 'Offline (Astra)', 'KSA', 0.27
    union all select 'Offline (Pre-Sale)', 'UAE', 0.26
    union all select 'Offline (Pre-Sale)', 'KSA', 0.25
    union all select 'Offline (Re-Sale)', 'UAE', 0.26
    union all select 'Offline (Re-Sale)', 'KSA', 0.25
    union all select 'Platform (Astra)', 'UAE', 0.36
    union all select 'Platform (Astra)', 'KSA', 0.27
    union all select 'Platform(Pre-Sale)', 'UAE', 0.36
    union all select 'Platform(Pre-Sale)', 'KSA', 0.31
    union all select 'Platform(Re-Sale)', 'UAE', 0.36
    union all select 'Platform(Re-Sale)', 'KSA', 0.31
    union all select 'SCaaS', 'KSA', 0.22
    union all select 'SuperMarkets', 'UAE', 0.29
    union all select 'SuperMarkets', 'KSA', 0.32
)
select

        case 
    when invoice_item_status not in ('APPROVED') then 'Filter Out' 
    when debtor_number = 'FNSAMPLE' then 'Floranow Sample'
    when debtor_number = '132008' then 'Intercompany Sales'
    when customer_type = 'reseller' then 'Intercompany Sales'
    when debtor_number = '130188' then 'Intercompany Sales'   
    else 'Floranow Sales'
    end as inv_items_reprot_filter,

            -- invoice Items
            creditable_id,
            invoice_item_generation_type,

            -- fct
            price_without_tax,
            price,
            total_tax,
            quantity,
            total_cost,

            unit_price,
            unit_landed_cost,
            unit_fob_price,

            gross_revenue,
            credit_note,
            auto_gross_revenue,
            auto_credit_note,

            tamimi_rema_customer,

            user_validity_filter,
            user_aging_type,

            case
                when sales_source = 'Astra' and tamimi_rema_customer = 'Tamimi Customer'
                then 'Astra - Tamimi Sales'
                when
                    sales_source = 'Non Astra'
                    and tamimi_rema_customer = 'Tamimi Customer'
                then 'Non Astra - Tamimi Sales'
                when sales_source = 'Astra' and tamimi_rema_customer = 'REMA Customer'
                then 'Astra - REMA Sales'
                when
                    sales_source = 'Non Astra'
                    and tamimi_rema_customer = 'REMA Customer'
                then 'Non Astra - REMA Sales'
                when sales_source = 'Astra'
                then 'Astra'
                when sales_source = 'Non Astra'
                then 'Non Astra'
                else 'To Be Scoped'
            end as sales_source_details,

            case
                when sales_source = 'Non Astra' then gross_revenue else 0
            end as non_astra_gross_revenue,
            case
                when sales_source = 'Non Astra' then credit_note else 0
            end as non_astra_credit_note,

            case
                when sales_source = 'Astra' then gross_revenue else 0
            end as astra_gross_revenue,
            case
                when sales_source = 'Astra' then credit_note else 0
            end as astra_credit_note,

            case
                when sales_source = 'To Be Scoped' then gross_revenue else 0
            end as tbs_gross_revenue,
            case
                when sales_source = 'To Be Scoped' then credit_note else 0
            end as tbs_credit_note,

            -- ---
            case
                when sales_source = 'Astra' and tamimi_rema_customer = 'Tamimi Customer'
                then gross_revenue
                else 0
            end as astra_tamimi_gross_revenue,
            case
                when sales_source = 'Astra' and tamimi_rema_customer = 'Tamimi Customer'
                then credit_note
                else 0
            end as astra_tamimi_credit_note,

            case
                when sales_source = 'Astra' and tamimi_rema_customer = 'REMA Customer'
                then gross_revenue
                else 0
            end as astra_rema_gross_revenue,
            case
                when sales_source = 'Astra' and tamimi_rema_customer = 'REMA Customer'
                then credit_note
                else 0
            end as astra_rema_credit_note,

            -- ---
            case
                when
                    sales_source = 'Non Astra'
                    and tamimi_rema_customer = 'Tamimi Customer'
                then gross_revenue
                else 0
            end as non_astra_tamimi_gross_revenue,
            case
                when
                    sales_source = 'Non Astra'
                    and tamimi_rema_customer = 'Tamimi Customer'
                then credit_note
                else 0
            end as non_astra_tamimi_credit_note,

            case
                when
                    sales_source = 'Non Astra'
                    and tamimi_rema_customer = 'REMA Customer'
                then gross_revenue
                else 0
            end as non_astra_rema_gross_revenue,
            case
                when
                    sales_source = 'Non Astra'
                    and tamimi_rema_customer = 'REMA Customer'
                then credit_note
                else 0
            end as non_astra_rema_credit_note,

            -- --
            case
                when
                    sales_source = 'Non Astra'
                    and tamimi_rema_customer = 'Normal Customer'
                then gross_revenue
                else 0
            end as non_astra_normal_gross_revenue,
            case
                when
                    sales_source = 'Non Astra'
                    and tamimi_rema_customer = 'Normal Customer'
                then credit_note
                else 0
            end as non_astra_normal_credit_note,

            case
                when sales_source = 'Astra' and tamimi_rema_customer = 'Normal Customer'
                then gross_revenue
                else 0
            end as astra_normal_gross_revenue,
            case
                when sales_source = 'Astra' and tamimi_rema_customer = 'Normal Customer'
                then credit_note
                else 0
            end as astra_normal_credit_note,

            -- ---
            case
                when invoice_header_printed_at is not null then 'Printed' else null
            end as printed_status,

            -- -Gross Revenue: This is the total amount of revenue generated from all
            -- printed invoices in a given period, without considering any adjustments
            -- like credit notes.
            case
                when
                    invoice_header_type = 'credit note'
                    and invoice_item_status = 'APPROVED'
                then 1
                else 0
            end as credit_note_items_count,
            case
                when
                    invoice_header_type = 'invoice' and invoice_item_status = 'APPROVED'
                then 1
                else 0
            end as invoice_items_count,

            case
                when date(invoice_header_printed_at) is not null
                then date(invoice_header_printed_at)
                else date(invoice_header_printed_at)
            end as master_date,

            -- This represents the total monetary value deducted from the Gross
            -- Revenue for a specific period, such as a month, due to the issuance of
            -- credit notes. Credit notes are typically issued when a customer returns
            -- a product, doesn't accept a delivery, or when a correction to an
            -- invoice is required.
            case
                when creditable_id is not null then 'creditable_id' else null
            end as creditable_id_check,

            case
                when invoice_header_id is not null then 'invoice_header_id' else null
            end as invoice_header_id_check,

            invoice_item_type_row,
            creditable_type,

            -- dim
            financial_administration,  -- Market
            customer,
            --company_name,
            user_category,  -- Segment
            debtor_number,
            account_manager,
            warehouse,

            ii.invoice_item_id,
            drop_id,  -- concat(customer.debtor_number,ii.delivery_date)

            source_type,  -- ERP, Florisft
            invoice_item_type,
            invoice_item_status,
            customer_type,
            product_name as product,

            -- date
            order_date,
            delivery_date,
            deleted_at,

            -- Line Items
            -- dim
            line_item_id,
            supplier,
            supplier_id,
            origin,
            fulfillment_mode,
            order_status,
            order_number,
            order_type,
            stock_model,

            meta_supplier,
            meta_supplier_code,
            meta_supplier_name,

            ordering_stock_type,

            product_category,
            product_subcategory,

            -- invoice Header
            -- dim
            invoice_header_id,
            invoice_header_status,  -- draft, open, printed, signed, closed, canceled, rejected, voided
            invoice_header_type,  -- credit note, invoice
            generation_type,
            record_type,
            li_record_type_details,
            li_record_type,
            invoice_number,

            -- date
            invoice_header_created_at,
            invoice_header_printed_at,

            sales_source,

            order_source,

            registered_clients,

            feed_source_name,
            line_item_id_check,
            parent_id_check,

            invoice_items_link,
            invoice_link,
            line_items_link,

            pod_source_type,
            trading_model,

            stem_length,

            tags,
            offer_type,
            reason,

            case 
            when warehouse in ('Riyadh Warehouse','Qassim Warehouse','Jouf WareHouse','Hail Warehouse') then 'Al Amir'
            when warehouse in ('Dammam Warehouse','Hafar WareHouse') then 'Hani'
            when warehouse in ('Jeddah Warehouse') then 'Mahmoud'
            when warehouse in ('Tabuk Warehouse') then 'Majed'
            when warehouse in ('Medina Warehouse') then 'Abd Alaziz'
            else null end as astra_accountant,

            delivery_charge_amount,

            case 
            when (auto_gross_revenue + auto_credit_note - total_cost)/if(gross_revenue = 0,1,gross_revenue) >= 0.15 then 'Normal Margin'
            when (auto_gross_revenue + auto_credit_note - total_cost)/if(gross_revenue = 0,1,gross_revenue) < 0.15 then 'Low Margin' end as Flag,

            case 
            when (auto_gross_revenue + auto_credit_note - total_cost)/if(gross_revenue = 0,1,gross_revenue) <= 0 then 'Negative'
            when (auto_gross_revenue + auto_credit_note - total_cost)/if(gross_revenue = 0,1,gross_revenue) > 0 and (auto_gross_revenue + auto_credit_note - total_cost)/gross_revenue < 0.26 then 'Critical'
            when (auto_gross_revenue + auto_credit_note - total_cost)/if(gross_revenue = 0,1,gross_revenue) >= 0.26 and (auto_gross_revenue + auto_credit_note - total_cost)/gross_revenue < 0.35 then 'Risky'
            when (auto_gross_revenue + auto_credit_note - total_cost)/if(gross_revenue = 0,1,gross_revenue) >= 0.35 then 'Healthy' end as Flag2,

            -- Target margin percentage by sales channel and financial administration (for aggregation)
            tgt.target_margin_percentage,

            current_timestamp() as insertion_timestamp,
            city,
            currency,
            manual_invoicing_filtration,
            unit_price_modified,

            auto_gross_revenue_mod,
            manual_gross_revenue,
            auto_credit_note_mod,
            manual_credit_note,
            unit_landed_cost_mod,
            total_cost_with_manual,

            EXTRACT(MONTH FROM invoice_header_printed_at) AS month_invoice_header_printed_at,
            -- FORMAT_DATE('%b', invoice_header_printed_at) as month_invoice_header_printed_at,

            base_metric_usd,

            base_metric_usd * gross_revenue as gross_revenue_usd,
            base_metric_usd * credit_note as credit_note_usd,

            CASE
                WHEN warehouse IN ('Dammam Project X', 'Dammam Warehouse', 'Hafar WareHouse') THEN 'Dammam'
                WHEN warehouse = 'Dubai Warehouse' THEN 'Dubai'
                WHEN warehouse IN ('Riyadh Warehouse', 'Riyadh Project X', 'Qassim Warehouse', 'Hail Warehouse') THEN 'Riyadh'
                WHEN warehouse IN ('Jeddah Warehouse', 'Tabuk Warehouse', 'Medina Warehouse', 'Jouf WareHouse', 'Jeddah Project X') THEN 'Jeddah'
                WHEN warehouse = 'Qatar Warehouse' THEN 'Qatar'
                WHEN warehouse = 'Kuwait Warehouse' THEN 'Kuwait'
                WHEN warehouse = 'Jordan Warehouse' THEN 'Jordan'
                ELSE warehouse
            END AS main_hub,

            case when  date_diff(date(invoice_header_printed_at) , current_date() , MONTH) = 0 then gross_revenue else 0 end as mtd_gross_revenue,
            case when  date_diff(date(invoice_header_printed_at) , current_date() , MONTH) = 0 then credit_note else 0 end as mtd_credit_note,

            case when date_diff(current_date(),date(invoice_header_printed_at), MONTH) = 1 and extract(day FROM invoice_header_printed_at) <= extract(day FROM current_date()) then gross_revenue else 0 end as lmtd_gross_revenue,
            case when date_diff(current_date(),date(invoice_header_printed_at), MONTH) = 1 and extract(day FROM invoice_header_printed_at) <= extract(day FROM current_date()) then credit_note else 0 end as lmtd_credit_note,

            case when date_diff(current_date(), date(invoice_header_printed_at), YEAR) = 1 and extract(month FROM invoice_header_printed_at) = extract(month FROM current_date()) 
            and extract(day FROM invoice_header_printed_at) <= extract(day FROM current_date()) then gross_revenue else 0 end as lymtd_gross_revenue,
            case when date_diff(current_date(), date(invoice_header_printed_at), YEAR) = 1 and extract(month FROM invoice_header_printed_at) = extract(month FROM current_date()) 
            and extract(day FROM invoice_header_printed_at) <= extract(day FROM current_date()) then credit_note else 0 end as lymtd_credit_note,

            CASE WHEN EXTRACT(YEAR FROM invoice_header_printed_at) = EXTRACT(YEAR FROM current_date()) AND date(invoice_header_printed_at) <= current_date() THEN gross_revenue ELSE 0 END AS ytd_gross_revenue,
            CASE WHEN EXTRACT(YEAR FROM invoice_header_printed_at) = EXTRACT(YEAR FROM current_date()) AND date(invoice_header_printed_at) <= current_date() THEN credit_note ELSE 0 END AS ytd_credit_note,

            CASE WHEN EXTRACT(YEAR FROM invoice_header_printed_at) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AND DATE(invoice_header_printed_at) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) THEN gross_revenue ELSE 0 END AS lytd_gross_revenue,
            CASE WHEN EXTRACT(YEAR FROM invoice_header_printed_at) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AND DATE(invoice_header_printed_at) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) THEN credit_note ELSE 0 END AS lytd_credit_note,

            -- CASE WHEN  invoice_header_printed_at >= '2022-01-01' AND invoice_header_printed_at < '2023-01-01' THEN gross_revenue ELSE 0 END AS gross_revenue_2022,
            -- CASE WHEN  invoice_header_printed_at >= '2023-01-01' AND invoice_header_printed_at < '2024-01-01' THEN gross_revenue ELSE 0 END AS gross_revenue_2023,
            -- CASE WHEN  invoice_header_printed_at >= '2024-01-01' AND invoice_header_printed_at < '2025-01-01' THEN gross_revenue ELSE 0 END AS gross_revenue_2024,
            -- CASE WHEN  invoice_header_printed_at >= '2025-01-01' AND invoice_header_printed_at < '2026-01-01' THEN gross_revenue ELSE 0 END AS gross_revenue_2025,

            selling_stage,
            product_color,

            md.sales_channels,
            case 
                when sales_channels = 'Platform(Pre-Sale)' then 'Samer'
                when sales_channels = 'Platform(Re-Sale)' then 'Samer'
                when sales_channels = 'Platform (Astra)' then 'Samer'

                when sales_channels = 'Offline (Pre-Sale)' and financial_administration = 'UAE' then 'Mercy'
                when sales_channels = 'Offline (Pre-Sale)' and financial_administration = 'KSA' then 'Faisal'
                when sales_channels = 'Offline (Re-Sale)' and financial_administration = 'UAE' then 'Mercy'
                when sales_channels = 'Offline (Re-Sale)' and financial_administration = 'KSA' then 'Faisal'
                when sales_channels = 'Offline (Astra)' and financial_administration = 'UAE' then 'Mercy'
                when sales_channels = 'Offline (Astra)' and financial_administration = 'KSA' then 'Faisal'

                when sales_channels = 'SuperMarkets' and financial_administration = 'UAE' then 'Mercy'
                when sales_channels = 'SuperMarkets' and financial_administration = 'KSA' then 'Faisal'

                when sales_channels = 'SCaaS' and financial_administration = 'KSA' then 'Mutaz'
                
            end as sales_channel_owner,

            case 
                when sales_channels in ('Platform(Re-Sale)', 'Offline (Re-Sale)') then 'Re-Sale'
                when sales_channels = 'SuperMarkets' then 'Supermarket'    
                when sales_channels = 'SCaaS' then 'SCaaS'
                -- when 'Faas - TBF'
                when sales_channels in ('Platform (Astra)', 'Offline (Astra)') then 'Astra'
                else 'Other'   
            end as matching_driver,

            inventory_damaged_quantity,
            inventory_damaged_cost,

            case 
                when master_name in ('Grandiose', 'Kibsons') and generation_type = 'AUTO' then 'Grandiose + Kibsons (Auto)'
                when master_name in ('Grandiose', 'Kibsons') and generation_type = 'MANUAL' then 'Grandiose + Kibsons (Manual)'
                when master_name in ('Spinneys') then 'Spinneys'
                when master_name in ('Maf') then 'MAF'
                else 'Other'
                end as supermarkets_subcategory,
        
        from {{ ref("int_invoice_items") }} as ii
        left join margin_drivers md on ii.invoice_item_id = md.invoice_item_id
        left join sales_channel_targets tgt on md.sales_channels = tgt.sales_channel 
            and ii.financial_administration = tgt.financial_administration


    -- where invoice_type != 'credit note' and generation_type !='MANUAL'
    