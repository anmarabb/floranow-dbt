select 

    -- PK
    _id as vendor_id,

    -- Data
    floranownumber as floranow_number,
    name as Vendor,
    contactname as contact_name,
    email as vendor_email,
    case when active = True then "active" else "inactive" end as vendor_status,
    cast(numberoffarmsunderthevendor as int) as number_of_farms_under_the_vendor,
    cast(numberofemployees as int) as number_of_employees,
    cast(dailyproductioncapacity as int) as daily_production_capacity,
    date(createdat) as created_at,
    origin,
    phonenumber as vendor_phone_number,
    corecategories as core_categories,
    currency,
    info, 
    website,
    establishmentdate as establishment_date,
    activemarkets as active_markets,
    size,
    location,
    remarks,
 
from {{ source(var('erp_source'), 'vp_vendors') }}