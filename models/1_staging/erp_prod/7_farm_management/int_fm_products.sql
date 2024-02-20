
select
    p.* EXCEPT(sub_group),



case 
when raw_color in ('Bicolor pink','bicolour','bicolour orange-yellow') then 'bicolor'
when raw_color in ('blue white','light blue','white blue','dark blue') then 'blue'
when raw_color in ('dark red','burgundy') then 'red'
when raw_color in ('Pink White','Light Pink','dark pink') then 'pink'
when raw_color in ('bronze','violet','Mix','apricot','brown','peach','Peach','cream','lilac') then 'other color'
when raw_color in ('fuchsia') then 'cerise'
when raw_color in ('dark green') then 'green'
else raw_color
end as color,



case 
when p.product_name like '%Double%' THEN 'Lily Or Double'
when p.product_name like '%Lily Or%' THEN 'Lily Or' 
when p.product_name like '%Lily La%' THEN 'Lily LA' 

when p.product_name like '%Chrysanthemum Ying Yang%' THEN 'Chrysanthemum Santini' 
when p.product_name like '%Chrysanthemum Daria%' THEN 'Chrysanthemum Spray' 
when p.product_name like '%Chrysanthemum Doppia%' THEN 'Chrysanthemum Spray' 
when p.product_name like '%Chrysanthemum Harley%' THEN 'Chrysanthemum Spray' 
when p.product_name like '%Chrysanthemum  Calimero%' THEN 'Chrysanthemum Spray' 

when p.product_name like '%Dracaena Massangeana Leaves%' THEN 'Greeneries' 
when p.product_name like '%Strelitzia Leaves%' THEN 'Greeneries' 

when p.product_name like '%Cycas%' THEN 'Cycas' 

when p.sub_group is null then p.main_group
else p.sub_group
end as sub_group,


from   {{ ref('stg_fm_products') }} as p

