select

    p.fm_product_id,

   -- p.number,

    p.product_name,

    p.color,
    p.raw_color,

    p.quantity,

    p.available_quantity,

    p.fob_price,
    p.main_group,
    p.sub_group,
    p.stem_length,
    p.bud_height,
    p.bud_count,

    p.created_at,
    p.astra_barcode,
    p.fm_product_link,


CASE
    WHEN p.sub_group IN ('Alstroemeria', 'Aster', 'Cycas', 'Eucalyptus', 'Eustoma', 'Liatris', 'Gerbera', 'Trachelium', 'Sunflower', 'Statice', 'Solidago', 'Ruscus', 'Lily Or Double', 'Lily Or', 'Lily LA', 'Chrysanthemum Santini', 'Chrysanthemum Single', 'Chrysanthemum Spray') THEN 'Contract'
    WHEN p.sub_group IN ('Antirrhinum', 'Carnation', 'Dianthus barbatus', 'Gypsophila', 'Rose', 'Spray Rose', 'Celosia', 'Greeneries') THEN 'Out Of Contract'
    ELSE 'Unknown'
  END AS contract_status,
  'KSA National Warehouse' as warehouse

from   {{ ref('int_fm_products') }} as p