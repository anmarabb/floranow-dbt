select 


    -- PK
    _id as offer_id,


    -- Data 
    number as offer_number,
    name as Product,
    mask,
    -- active,
    CAST(moq AS INT64) as minimum_ordered_quantity,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(TRIM(price), r'[^0-9.\-]', ''),'') AS FLOAT64) as unit_price,
    date(validityfrom) as valid_from,
    date(validityto) as valid_to,
    CAST(maxdailyfulfillmentquantity AS INT64) as max_daily_fulfillment_quantity,
    CAST(availablequantity AS INT64) as available_quantity,
    -- farmcatalogvariationsnapshot.active as variation_active_status,
    farmcatalogvariationsnapshot.color as product_color,
    farmcatalogvariationsnapshot.minimumlengthofflowerstem as stem_length,
    farmcatalogvariationsnapshot.countryoforigin as vendor_region,
    farmcatalogvariationsnapshot.producttypename as product_type_name,
    farmcatalogvariationsnapshot. categoryname as product_category,
    farmsnapshot.name as Farm,
    -- farmsnapshot.active as farm_active_status,
    farmsnapshot. currency as currency,
    vendorsnapshot. name as Vendor,
    case when vendorsnapshot.active = True then "active" else "inactive" end as vendor_status,
    case when farmcatalogvariationsnapshot.active = True then "active" else "inactive" end as variation_status,
    case when farmsnapshot.active = True then "active" else "inactive" end as farm_status,
    case when active = True then "active" else "inactive" end as offer_status,
    DATE_DIFF(DATE(validityto), DATE(validityfrom), DAY) AS number_of_days,
    maxdailyfulfillmentquantity * DATE_DIFF(DATE(validityto), DATE(validityfrom), DAY) AS total_offered_quantity,
    farmcatalogvariationsnapshot.mainimage.url as mainimage_url,


from {{ source(var('erp_source'), 'vp_offers') }}