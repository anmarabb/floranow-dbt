with prep_countries as (
    select
        upper(trim(country_iso_code)) as code,
        country_name
    from {{ source(var('erp_source'), 'country') }}
    where country_iso_code is not null
    qualify row_number() over (
        partition by upper(trim(country_iso_code))
        order by country_name
    ) = 1
)

select

    -- PK
    v._id as catalog_id,
    v.variationid as variation_id,
    v.farmcatalogid as farm_catalog_id,
    v.farmid as farm_id,
    v.vendorid as vendor_id,

    -- Data
    v.name as Product,
    case when v.active = True then "active" else "inactive" end as farm_variation_status,
    v.mainimage.url as mainimage_url,
    v.color as product_color,
    v.length AS stem_length,
    coalesce(coo_by_code.country_name, v.countryoforigin) as vendor_region,
    farm_origin.country_name as farm_origin_country,
    v.producttypename as product_type_name,
    v.categoryname as product_category,
    CAST(v.moq AS INT64) AS minimum_ordered_quantity,
    SAFE_CAST(NULLIF(REGEXP_REPLACE(TRIM(v.price), r'[^0-9.\-]', ''),'') AS FLOAT64) AS unit_price,
    date(v.createdat) as created_at,
    date(v.updatedat) as updated_at,
    v.headsize AS head_size,
    v.minimumbudheight AS minimum_bud_height,
    v.quality,
    v.age AS age,
    v.vendorsnapshot.name as Vendor,
    v.farmsnapshot.name as Farm,
    CAST(JSON_EXTRACT_SCALAR(v.boxconfigurations, '$[0].packingRate') AS INT64) AS packing_rate,

from {{ source(var('erp_source'), 'vp_farm_catalog_variations') }} as v
left join prep_countries as coo_by_code
    on upper(trim(v.countryoforigin)) = coo_by_code.code
left join prep_countries as farm_origin
    on upper(trim(v.farmsnapshot.origin)) = farm_origin.code
