select 

    -- PK
    _id as catalog_id,
    variationid as variation_id,
    farmcatalogid as farm_catalog_id,
    farmid as farm_id,
    vendorid as vendor_id,

    -- Data
    name as Product,
    case when active = True then "active" else "inactive" end as farm_variation_status,
    mainimage. url as mainimage_url,
    color as product_color,
    CAST(length AS INT64) AS stem_lengthh,
    countryoforigin as vendor_region,
    producttypename as product_type_name,
    categoryname as product_category,
    CAST(moq AS INT64) AS minimum_ordered_quantity,
    CAST(price AS FLOAT64) AS unit_price,
    date(createdat) as created_at,
    date(updatedat) as updated_at,
    CAST(headsize AS FLOAT64) AS head_size,
    CAST(minimumbudheight AS FLOAT64) AS minimum_bud_height,
    quality,
    CAST(age AS INT64) AS age,
    vendorname as Vendor,
    farmname as Farm,

from {{ source(var('erp_source'), 'vp_farm_catalog_variations') }}