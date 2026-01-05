
select
    -- Primary Key
    master_shipment_id,
    
    -- Identifiers
    master_shipment_name,
    
    -- Status & State
    state as master_shipment_status,
    
    -- Location Information
    origin,
    origin_country_name,
    destination,
    destination_country_name,
    point_of_entry,
    consignee,
    consignee_name,
    consignee_country,
    consignee_region,
    consignee_address_line1,
    consignee_address_line2,
    consignee_cut_off_stage,
    
    -- Freight Information
    freight_forwarder,
    freight_forwarder_name,
    freight_forwarder_id,
    delivery_term,
    customer_debtor_number,
    
    -- Dates
    departure_date,
    created_at,
    updated_at,
    
    -- Fact Measures (Aggregated Counts)
    total_weight,
    total_shipments_count,
    total_attachments_count,
    actual_shipments_count,
    
    -- Calculated Fields
    case 
        when state in ('DRAFT', 'CANCELED') then 'Not Active'
        when state in ('PREPARING', 'SENT', 'PENDING_CONSOLIDATION', 'DEPARTED') then 'Active'
        else 'Unknown'
    end as master_shipment_status_category,
    
    case 
        when total_shipments_count > 0 and actual_shipments_count = total_shipments_count then 'Complete'
        when actual_shipments_count > 0 and actual_shipments_count < total_shipments_count then 'Partial'
        when actual_shipments_count = 0 then 'Empty'
        else 'Unknown'
    end as shipment_completeness,

from {{ ref('int_vp_master_shipments') }}
