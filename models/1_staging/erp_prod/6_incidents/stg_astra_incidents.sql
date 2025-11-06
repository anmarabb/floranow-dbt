WITH base_incidents AS (
    SELECT
        fm_product_incident_id AS incident_id,
        DATE(incident_at) AS incident_date,
        incident_quantity AS damage_quantity,
        fm_product_id AS product_id,
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
        -- استخدام confirmed_total_price لحساب الوزن (القيمة المالية لإجمالي المبيعات)
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
        inc.fob_unit_price,
        inc.incident_total_cost,
        inc.incident_month_start,
        all_warehouses.warehouse_name, 
        COALESCE(pur.warehouse_monthly_sales_value, 0) AS warehouse_monthly_sales_value
    FROM base_incidents inc

    -- الخطوة أ: جلب قائمة فريدة بكل المستودعات التي لدينا
    CROSS JOIN (
        SELECT DISTINCT warehouse_name
        FROM `dbt_prod_dwh.fct_fm_orders`
    ) AS all_warehouses

    -- الخطوة ب: ربط كل حادث بكل مستودع بناءً على الشهر
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
        warehouse_name,
        warehouse_monthly_sales_value,

        -- إجمالي قيمة المبيعات الكلية على مستوى جميع المستودعات في شهر الحادث
        SUM(warehouse_monthly_sales_value) OVER (PARTITION BY incident_id) AS total_company_monthly_sales,

        -- حساب النسبة المئوية للمبيعات (وزن التوزيع)
        warehouse_monthly_sales_value / NULLIF(SUM(warehouse_monthly_sales_value) OVER (PARTITION BY incident_id), 0) AS sales_percentage_share
    FROM incident_sales_volume
),

-- 5. التوزيع النهائي للتلف على المستودعات (الحساب النهائي)
final_allocation AS (
    SELECT
        share.incident_id,
        share.incident_date,
        share.warehouse_name,
        share.product_id,
        share.fob_unit_price,
        share.incident_total_cost AS incident_total_cost,
        share.total_company_monthly_sales,

        -- الخطوة أ: حساب الكمية الموزعة (يتم ضربها في النسبة ثم تقريبها إلى INT)
        -- يتم تخزين هذا الحساب في متغير مؤقت لاستخدامه في الخطوة ب
        CAST(ROUND(share.damage_quantity * share.sales_percentage_share) AS INT) AS allocated_damage_quantity,

        -- **الخطوة ب: حساب التكلفة الموزعة بناءً على الكمية المقربة**
        -- إذا كانت الكمية الموزعة = 0، فستكون التكلفة = 0
        ROUND(
            CAST(ROUND(share.damage_quantity * share.sales_percentage_share) AS INT) * share.fob_unit_price, 
        2) AS allocated_damage_cost,
        
        -- إظهار المكونات للتدقيق
        share.damage_quantity AS incident_total_damage_quantity,
        share.sales_percentage_share,
        share.warehouse_monthly_sales_value
    FROM warehouse_sales_share share
    -- تصفية الحالات التي ليس فيها مبيعات كلية (تجنباً للقسمة على صفر)
    WHERE share.total_company_monthly_sales IS NOT NULL AND share.total_company_monthly_sales > 0
    AND share.fob_unit_price IS NOT NULL
)

SELECT *
FROM final_allocation

-- where incident_id in (17208, 17209)