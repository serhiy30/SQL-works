# 📊 Orders Loss Analysis

This SQL query analyzes **orders that could not be fulfilled by other suppliers** due to **price increases**, **inventory shortage**, or a combination of both.  

It compares **actual orders** with **product snapshots** (`pas.product_xcart`) at the **order date**, calculates differences in price and inventory, and marks suppliers who could not sell.  

---

## ⚡ Features

- ✅ Shows **all suppliers** for each order.  
- ✅ Flags if a supplier did **not sell due to price** (`not_sold_price`).  
- ✅ Flags if a supplier did **not sell due to inventory** (`not_sold_inventory`).  
- ✅ Calculates **price and inventory differences** (`price_diff`, `inventory_diff`).  
- ✅ Calculates **percentage differences** (`percent_price_diff`, `percent_inventory_diff`).  
- ✅ Provides a **loss reason** (`Price`, `Inventory`, `Price + Inventory`, `Sold`).  
- ✅ Includes product info: **brand**, **category**, **shipping cost**.  

---

## 📖 Sample Output

| order_id | product_id | order_supplier | supplier   | order_date  | order_price | order_quantity | product_price | product_inventory | product_shipping_cost | brand   | category   | not_sold_price | not_sold_inventory | price_diff | inventory_diff | percent_price_diff | percent_inventory_diff | loss_reason       |
|----------|------------|----------------|-----------|------------|-------------|----------------|---------------|-----------------|---------------------|--------|------------|----------------|------------------|------------|----------------|------------------|----------------------|-----------------|
| 12345    | 1001       | Supplier A     | Supplier A | 2025-10-20 | 1000        | 10             | 1000          | 10              | 50                  | BrandX | Category1  | 0              | 0                | 0          | 0              | 0.00             | 0.00                 | Sold            |
| 12345    | 1001       | Supplier A     | Supplier B | 2025-10-20 | 1000        | 10             | 1050          | 8               | 55                  | BrandX | Category1  | 1              | 1                | 50         | -2             | 5.00             | -20.00               | Price + Inventory|
| 12345    | 1001     | Supplier A     | Supplier C | 2025-10-20 | 1000        | 10             | 950           | 0               | 45                  | BrandX | Category1  | 0              | 1                | -50        | -10            | -5.00            | -100.00              | Inventory       |

---

## 🚀 How it works

1. Selects orders (`orders`) in the chosen period.  
2. Fetches **product snapshots** from `pas.product_xcart` at the **same date** as the order.  
3. Joins **all suppliers** for the product to see who could have sold.  
4. Compares prices and inventory, marking reasons for not selling.  
5. Calculates differences and percentage differences for detailed analysis.  

---

This query is perfect for building **dashboards** showing potential lost sales per supplier, product, or category. 📈
