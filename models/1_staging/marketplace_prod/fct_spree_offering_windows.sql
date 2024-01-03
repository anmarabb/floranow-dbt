with temp_calculation as 
(SELECT ow.id,
       vendor.name as Supplier,
       vendor.country as origin,
       w.warehouse_name as warehouse,
       window_start_day,
       window_end_day,
       departure_day,
       departure_date,
       window_type,
       departure_skip_days,
       vendor.id as supplier_id,
       CASE
           WHEN window_type = 0  THEN 'Periodic'
           WHEN window_type = 1 THEN 'Event'
           ELSE NULL
           END AS window_type_name,
      CASE
           WHEN (window_type = 0 and window_start_day IS NOT NULL and window_start_day = window_end_day and window_start_hour = window_end_hour) THEN PARSE_DATETIME('%Y-%m-%d %H:%M:%S',CONCAT(date_SUB(CURRENT_DATE(),INTERVAL (MOD(EXTRACT(DAYOFWEEK FROM CURRENT_DATE()-1) - window_start_day + 7, 7)) DAY), window_start_hour))
           WHEN (window_type = 0 and window_start_day IS NOT NULL) THEN PARSE_DATETIME('%Y-%m-%d %H:%M:%S',CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL (MOD(window_start_day - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()-1) + 7,7)) DAY),window_start_hour))
           WHEN (window_type = 1 and window_start_date IS NOT NULL) THEN PARSE_DATETIME('%Y-%m-%d %H:%M:%S',CONCAT(window_start_date, window_start_hour))
           ELSE NULL
           END AS next_start_date,
      CASE
           WHEN (window_type = 0 and window_end_day IS NOT NULL and window_start_day = window_end_day and window_start_hour = window_end_hour) THEN 
              CASE 
                  WHEN(MOD(window_end_day - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()-1)  + 7,7) < 0) 
                          THEN PARSE_DATETIME('%Y-%m-%d %H:%M:%S',concat(date_add(current_date(),interval window_end_day day),window_start_hour))
                          else PARSE_DATETIME('%Y-%m-%d %H:%M:%S',concat(date_add(current_date(),interval mod(window_end_day - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()-1) + 7,7) day),window_start_hour))
                          end
            when (window_type = 0 and window_end_day IS NOT NULL) then PARSE_DATETIME('%Y-%m-%d %H:%M:%S',concat(date_add(current_date(),interval mod(window_end_day - EXTRACT(DAYOFWEEK FROM CURRENT_DATE()-1) + 7,7) day),window_end_hour))
            WHEN (window_type = 1 and window_end_date IS NOT NULL) THEN PARSE_DATETIME('%Y-%m-%d %H:%M:%S',concat(window_end_date, window_end_hour))
           ELSE NULL
           END AS next_end_date


FROM {{ref('stg_spree_offering_windows')}} as ow
join {{ref('stg_spree_feeds')}} as feed on feed_id = feed.id
join {{ref('stg_spree_vendors')}} as vendor on feed.vendor_id = vendor.id
left join {{ref('base_warehouses')}} as w on w.landing_region_id = ow.landing_region_id





where ow.deleted_at is null and ow.active = true ), -- and ow.id = 9834
aggregated as(
    select 
       Supplier,
       supplier_id,
       origin,
       warehouse,
       next_start_date as start_at,
       next_end_date as end_at,
       CASE
           WHEN (window_type = 0 and next_end_date IS NOT NULL) THEN  
             case when (mod(departure_day - EXTRACT(DAYOFWEEK FROM date(next_end_date) -1) + 7,7) < 0)
                  then date_add(date(next_end_date), interval (departure_day + departure_skip_days) day)
                  else date_add((date_add(date(next_end_date), interval mod((departure_day - EXTRACT(DAYOFWEEK FROM date(next_end_date) - 1) + 7) ,7) day)), interval departure_skip_days day)
                                    end
        WHEN (window_type = 1 and departure_date IS NOT NULL) THEN date(departure_date)
        ELSE NULL
    ENd AS departure_date
FROM
    temp_calculation
where next_end_date > current_datetime() and (( current_datetime() between next_start_date AND next_end_date ) or (next_start_date >= next_end_date)))
, 
last_one as(
select 

origin, 
supplier_id,Supplier, 
warehouse, 
departure_date, 
DENSE_RANK() OVER (PARTITION BY warehouse, origin,Supplier, supplier_id ORDER BY departure_date) AS departure_rank 
from aggregated

where departure_date >= current_date()

group by origin, Supplier, warehouse,departure_date,supplier_id


)
select 

origin,
Supplier,
warehouse,
departure_date as current_departure_date,

from last_one where departure_rank = 1 