-- ===========================================
-- Query to compare replacement products with alternative products
-- Calculates price differences, margin differences, shipping differences, etc.
-- Written for Databricks (Spark SQL)
-- ===========================================

-- Step 1: Get all orders for analysis

WITH order_info AS (
    SELECT
        o.order_id,
        o.product_id,
        o.supplier AS order_supplier,   -- supplier who actually fulfilled the order
        o.order_date,
        o.price AS order_price,
        o.quantity AS order_quantity
    FROM orders o
    WHERE o.order_date BETWEEN '2025-01-01' AND CURRENT_DATE
),

-- Step 2: Get product snapshots at the order date
product_snapshot AS (
    SELECT
        p.product_id,
        p.supplier,
        p.update_date,
        p.price AS product_price,
        p.inventory AS product_inventory,
        p.shipping_cost AS product_shipping_cost,
        p.brand,
        p.category
    FROM pas.product_xcart p
),

-- Step 3: Combine orders with all suppliers for the same product and date
combined AS (
    SELECT
        oi.order_id,
        oi.product_id,
        oi.order_supplier,
        ps.supplier,
        oi.order_date,
        oi.order_price,
        oi.order_quantity,
        ps.product_price,
        ps.product_inventory,
        ps.product_shipping_cost,
        ps.brand,
        ps.category,

        -- Determine why the supplier did not sell
        CASE WHEN ps.product_price > oi.order_price THEN 1 ELSE 0 END AS not_sold_price,
        CASE WHEN ps.product_inventory < oi.order_quantity THEN 1 ELSE 0 END AS not_sold_inventory,

        -- Price and inventory differences
        ps.product_price - oi.order_price AS price_diff,
        ps.product_inventory - oi.order_quantity AS inventory_diff,
        ROUND((ps.product_price - oi.order_price)/oi.order_price*100, 2) AS percent_price_diff,
        ROUND((ps.product_inventory - oi.order_quantity)/oi.order_quantity*100, 2) AS percent_inventory_diff,

        -- Determine loss reason in text format
        CASE 
            WHEN ps.product_price > oi.order_price AND ps.product_inventory < oi.order_quantity THEN 'Price + Inventory'
            WHEN ps.product_price > oi.order_price THEN 'Price'
            WHEN ps.product_inventory < oi.order_quantity THEN 'Inventory'
            ELSE 'Sold'
        END AS loss_reason

    FROM order_info oi
    JOIN product_snapshot ps
        ON oi.product_id = ps.product_id
        AND oi.order_date = ps.update_date
)

-- Final selection of all columns for analysis
SELECT *
FROM combined
ORDER BY order_id, supplier;
