-- ===========================================
-- Query to compare replacement products with alternative products
-- Calculates price differences, margin differences, shipping differences, etc.
-- Written for Databricks (Spark SQL)
-- ===========================================

SELECT
    pr.manufacturer_id,
    br.manufacturer,
    
    -- Vendor and product info
    sh.product_id,
    sh.vendor_id,
    sh.name AS vendor_name,
    
    -- Replacement product info
    pr.sku,
    pr.price AS replacement_price,
    product.today_cost AS replacement_cost,
    pr.price - product.today_cost AS replacement_item_margin,
    pr.main_product_id,
    orig.oe_parts AS oe_parts,
    orig.original_sku AS replacement_original_sku,
    pr.url_sku,
    
    -- Alternative product info
    Other.sku AS alternative_product_sku,
    Other.manufacturer_id AS alternative_product_manufacturer_id,
    Other.manufacturer AS alternative_product_manufacturer,
    Other.vendor_id AS alternative_product_vendor_id,
    Other.Vendor_name AS alternative_product_vendor_name,
    Other.Vendor_price AS alternative_product_price,
    Other.Vendor_cost_p AS alternative_product_cost,
    Other.Vendor_price - Other.Vendor_cost_p AS alternative_product_item_margin,
    Other.shipping_price AS alternative_product_shipping_price,
    Other.shipping_cost AS alternative_product_shipping_cost,
    Other.shipping_price - Other.shipping_cost AS alternative_product_shipping_margin,
    Other.in_stock AS alternative_product_in_stock,
    Other.oe_parts AS alternative_product_oe_parts,
    Other.original_sku AS alternative_product_original_sku,
    Other.url_sku AS alternative_product_url_sku,
    
    -- Calculations
    (Other.Vendor_price - pr.price) AS price_difference,
    CONCAT(round(((Other.Vendor_price - pr.price) / pr.price) * 100, 2), '%') AS price_difference_percent,
    
    -- Margin differences
    (Other.Vendor_price - Other.Vendor_cost_p) - (pr.price - product.today_cost) AS item_margin_difference,
    CONCAT(
        ROUND(
            ((Other.Vendor_price - Other.Vendor_cost_p) - (pr.price - product.today_cost))
            / NULLIF(pr.price - product.today_cost, 0) * 100
        , 2), '%'
    ) AS item_margin_difference_percent,
    
    -- Shipping differences
    sh.shipping_price AS replacement_shipping_price,
    sh.shipping_cost AS replacement_shipping_cost,
    sh.shipping_price - sh.shipping_cost AS replacement_shipping_margin,
    (Other.shipping_price - Other.shipping_cost) - (sh.shipping_price - sh.shipping_cost) AS shipping_margin_difference,
    CONCAT(
        ROUND(
            ((Other.shipping_price - Other.shipping_cost) - (sh.shipping_price - sh.shipping_cost))
            / NULLIF((sh.shipping_price - sh.shipping_cost), 0) * 100
        , 2), '%'
    ) AS shipping_margin_difference_percent,
    
    -- Overall margin
    (pr.price - product.today_cost) + (sh.shipping_price - sh.shipping_cost) AS replacement_total_margin,
    (Other.Vendor_price - Other.Vendor_cost_p) + (Other.shipping_price - Other.shipping_cost) 
        - ((pr.price - product.today_cost) + (sh.shipping_price - sh.shipping_cost)) AS total_margin_difference,
    CONCAT(
        ROUND(
            ((Other.Vendor_price - Other.Vendor_cost_p) + (Other.shipping_price - Other.shipping_cost) 
             - ((pr.price - product.today_cost) + (sh.shipping_price - sh.shipping_cost)))
            / NULLIF((pr.price - product.today_cost) + (sh.shipping_price - sh.shipping_cost), 0) * 100
        , 2), '%'
    ) AS total_margin_difference_percent,
    
    -- Product type info
    product.ptype_group_id,
    product.ptype_id,
    product.in_stock

FROM main.inventory.data.mpn_product pr

-- Join to get manufacturer name
INNER JOIN main.inventory.data.xcart_manufacturers br
    ON pr.manufacturer_id = br.manufacturerid

-- Join to map MPN to internal product ID
INNER JOIN main.inventory.data.mpn_id_to_product_id id
    ON pr.id = id.mpn_id

-- Extra product info
INNER JOIN main.inventory.data.mpn_product_extra_data orig
    ON pr.id = orig.mpn_product_id

-- OE parts for the product
INNER JOIN main.inventory.data.oe_part_to_product oep
    ON oep.product_id = id.product_id
INNER JOIN main.inventory.data.oe_part oes
    ON oes.id = oep.oe_part_id

-- Join to get actual product in stock
INNER JOIN main.products.product product
    ON product.manufacturer_id = pr.manufacturer_id
    AND product.mpn = pr.sku

-- Join shipping info
INNER JOIN (
    SELECT 
        s.product_id,
        s.vendor_id,
        s.shipping_price,
        s.shipping_cost,
        v.name
    FROM main.products.product_calculated_shipping s
    INNER JOIN main.products.vendor v ON s.vendor_id = v.id
    WHERE s.shipping_zone_id = 1
) AS sh
    ON product.id = sh.product_id

-- Join alternative products
INNER JOIN (
    SELECT
        pr.sku,
        pr.manufacturer_id,
        br.manufacturer,
        pr.url_sku,
        pr.price AS Vendor_price,
        product.today_cost AS Vendor_cost_p,
        pr.main_product_id,
        orig.original_sku,
        orig.oe_parts,
        oep.oe_part_id,
        product.id,
        product.ptype_group_id,
        product.ptype_id,
        product.in_stock,
        sh.product_id,
        sh.vendor_id,
        sh.name AS Vendor_name,
        sh.shipping_price,
        sh.shipping_cost
    FROM main.inventory.data.mpn_product pr
    INNER JOIN main.inventory.data.xcart_manufacturers br ON pr.manufacturer_id = br.manufacturerid
    INNER JOIN main.inventory.data.mpn_id_to_product_id id ON pr.id = id.mpn_id
    INNER JOIN main.inventory.data.mpn_product_extra_data orig ON pr.id = orig.mpn_product_id
    INNER JOIN main.inventory.data.oe_part_to_product oep ON oep.product_id = id.product_id
    INNER JOIN main.inventory.data.oe_part oes ON oes.id = oep.oe_part_id
    INNER JOIN main.products.product product
        ON product.manufacturer_id = pr.manufacturer_id
        AND product.mpn = pr.sku
    INNER JOIN (
        SELECT 
            s.product_id,
            s.vendor_id,
            s.shipping_price,
            s.shipping_cost,
            v.name
        FROM main.products.product_calculated_shipping s
        INNER JOIN main.products.vendor v ON s.vendor_id = v.id
        WHERE s.shipping_zone_id = 1
    ) AS sh
        ON product.id = sh.product_id
    WHERE pr.manufacturer_id != 11517
) AS Other
    ON orig.oe_parts = Other.oe_parts
    AND Other.oe_part_id = oep.oe_part_id

-- Filters
WHERE pr.manufacturer_id = 11517
  AND pr.price != 0
