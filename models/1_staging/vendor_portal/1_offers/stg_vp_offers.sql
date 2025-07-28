select 


    -- PK
    _id as offer_id,


    -- Data 
    number as offer_number,
    name as Product,
    mask,
    active,
    moq as minimum_ordered_quantity,
    price as unit_price,
    validityfrom as valid_from,
    validityto as valid_to,
    maxdailyfulfillmentquantity as max_daily_fulfillment_quantity,
    availablequantity as available_quantity,
    farmcatalogvariationsnapshot.active as variation_active_status,
    farmcatalogvariationsnapshot.color as product_color,
    farmcatalogvariationsnapshot.minimumlengthofflowerstem as stem_length,
    farmcatalogvariationsnapshot.countryoforigin as farm_origin,
    farmcatalogvariationsnapshot.producttypename as product_type_name,
    farmcatalogvariationsnapshot. categoryname as product_category,
    farmsnapshot.name as Farm,
    farmsnapshot.active as farm_active_status,
    farmsnapshot. currency as currency,
    vendorsnapshot. name as Vendor,
    vendorsnapshot.active as vendor_active_status,



from {{ source(var('erp_source'), 'vp_offers') }}