WITH base_incidents AS (
    SELECT
        fm_product_incident_id AS incident_id,
        DATE(incident_at) AS incident_date,
        incident_quantity AS damage_quantity,
        fm_product_id AS product_id,
        product,
        stem_length,
        main_group as product_category,
        sub_group as product_subcategory,
        fob_price AS fob_unit_price, -- سعر الوحدة
        -- حساب التكلفة الكلية للحادث (Total Cost)
        fob_price * incident_quantity AS incident_total_cost,
        -- استخراج الشهر والسنة لتحديد فترة احتساب المبيعات الكلية
        DATE_TRUNC(incident_at, MONTH) AS incident_month_start
    FROM `dbt_prod_dwh.fct_fm_product_incidents`
    WHERE 1=1
        AND incident_type = 'DAMAGED'
),

-- 2. حساب إجمالي قيمة المبيعات الكلية (الوزن) لكل مستودع في شهر وقوع أي حادث
monthly_warehouse_sales AS (
    SELECT
        DATE_TRUNC(Created_date, MONTH) AS order_month_start,
        warehouse_name,
        SUM(confirmed_total_price) AS warehouse_monthly_sales_value
    FROM `dbt_prod_dwh.fct_fm_orders`
    GROUP BY 1, 2
),

-- 3. توليد جميع توليفات الحوادث-المستودعات الشهرية وتعبئة بيانات المبيعات (الوزن)
incident_sales_volume AS (
    SELECT
        inc.incident_id,
        inc.incident_date,
        inc.damage_quantity,
        inc.product_id,
        inc.product,
        inc.stem_length,
        inc.product_category,
        inc.product_subcategory,
        inc.fob_unit_price,
        inc.incident_total_cost,
        inc.incident_month_start,
        all_warehouses.warehouse_name, 
        COALESCE(pur.warehouse_monthly_sales_value, 0) AS warehouse_monthly_sales_value
    FROM base_incidents inc
    CROSS JOIN (
        SELECT DISTINCT warehouse_name
        FROM `dbt_prod_dwh.fct_fm_orders`
    ) AS all_warehouses
    LEFT JOIN monthly_warehouse_sales pur
        ON inc.incident_month_start = pur.order_month_start
        AND all_warehouses.warehouse_name = pur.warehouse_name
),

-- 4. حساب الحصة المئوية لكل مستودع (وزن التوزيع)
warehouse_sales_share AS (
    SELECT
        incident_id,
        incident_date,
        damage_quantity,
        incident_total_cost,
        fob_unit_price,
        product_id,
        product,
        stem_length,
        product_category,
        product_subcategory,
        warehouse_name,
        warehouse_monthly_sales_value,

        SUM(warehouse_monthly_sales_value) OVER (PARTITION BY incident_id) AS total_company_monthly_sales,
        warehouse_monthly_sales_value / NULLIF(SUM(warehouse_monthly_sales_value) OVER (PARTITION BY incident_id), 0) AS sales_percentage_share
    FROM incident_sales_volume
),

-- 5. الحساب الكسري والتحضير للباقي الأكبر
largest_remainder_prep AS (
    SELECT
        share.incident_id,
        share.incident_date,
        share.warehouse_name,
        share.product_id,
        share.product,
        share.stem_length,
        share.product_category,
        share.product_subcategory,
        share.fob_unit_price,
        share.incident_total_cost,
        share.damage_quantity AS incident_total_damage_quantity,
        share.sales_percentage_share,
        share.total_company_monthly_sales AS total_company_monthly_sales_value,
        share.warehouse_monthly_sales_value,

        -- 1. الحساب الكسري للكمية (المرشح للتوزيع)
        share.damage_quantity * share.sales_percentage_share AS fractional_quantity,
        
        -- 2. الجزء الصحيح (التقريب الأساسي)
        CAST(FLOOR(share.damage_quantity * share.sales_percentage_share) AS INT) AS base_allocated_quantity,
        
        -- 3. حساب الباقي (الكسر)
        (share.damage_quantity * share.sales_percentage_share) - FLOOR(share.damage_quantity * share.sales_percentage_share) AS remainder,
        
        -- 4. إجمالي الكمية الموزعة بشكل أساسي (المجموع الذي تم فقده)
        SUM(CAST(FLOOR(share.damage_quantity * share.sales_percentage_share) AS INT)) OVER (PARTITION BY share.incident_id) AS sum_base_allocated_quantity
        
    FROM warehouse_sales_share share
    WHERE share.total_company_monthly_sales IS NOT NULL AND share.total_company_monthly_sales > 0
    AND share.fob_unit_price IS NOT NULL
),

-- 6. تحديد الفرق المتبقي وتعيين رتبة التعديل
final_rounding_adjustment AS (
    SELECT
        prep.*,
        
        -- 5. تحديد الوحدات التي يجب توزيعها (الفرق المتبقي)
        prep.incident_total_damage_quantity - prep.sum_base_allocated_quantity AS remaining_units_to_adjust,
        
        -- 6. تعيين الرتبة بناءً على الباقي الأكبر (Largest Remainder)
        -- نستخدم الرتبة لتحديد من يحصل على وحدة إضافية (1+)
        ROW_NUMBER() OVER (
            PARTITION BY prep.incident_id
            ORDER BY prep.remainder DESC, prep.sales_percentage_share DESC
        ) AS remainder_rank
    FROM largest_remainder_prep prep
),

-- 7. التوزيع النهائي (Final Allocation)
final_allocation AS (
    SELECT
        adj.incident_id,
        adj.incident_date,
        adj.warehouse_name,
        adj.product_id,
        adj.product,
        adj.stem_length,
        adj.product_category,
        adj.product_subcategory,
        adj.fob_unit_price,
        adj.incident_total_cost,
        adj.incident_total_damage_quantity,
        adj.sales_percentage_share,
        adj.total_company_monthly_sales_value AS total_company_monthly_sales, 
        adj.warehouse_monthly_sales_value,
        
        -- الكمية الموزعة النهائية
        -- (الكمية الأساسية + 1 إذا كانت رتبته أقل أو تساوي عدد الوحدات المتبقية للتعديل)
        adj.base_allocated_quantity + 
        CASE 
            WHEN adj.remainder_rank <= adj.remaining_units_to_adjust 
            THEN 1 
            ELSE 0 
        END AS allocated_damage_quantity,
        
        -- التكلفة الموزعة النهائية (الكمية النهائية * سعر الوحدة)
        ROUND((adj.base_allocated_quantity + 
        CASE 
            WHEN adj.remainder_rank <= adj.remaining_units_to_adjust 
            THEN 1 
            ELSE 0 
        END) * adj.fob_unit_price, 2) AS allocated_damage_cost
        
    FROM final_rounding_adjustment adj
    -- نحذف السجلات التي تكون فيها الكمية الموزعة النهائية والتكلفة الموزعة صفرًا
    WHERE (adj.base_allocated_quantity + CASE WHEN adj.remainder_rank <= adj.remaining_units_to_adjust THEN 1 ELSE 0 END) > 0
)

SELECT * FROM final_allocation
-- where incident_id in (17208, 17209)