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
    length as stem_length,
    countryoforigin as vendor_region,
    producttypename as product_type_name,
    categoryname as product_category,
    moq as minimum_ordered_quantity,
    price as unit_price,
    date(createdat) as created_at,
    date(updatedat) as updated_at,
    headsize as head_size,
    minimumbudheight as minimum_bud_height,
    quality,
    age,


from {{ source(var('erp_source'), 'vp_farm_catalog_variations') }}