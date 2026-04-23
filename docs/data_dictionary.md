# Data Dictionary

Full column reference for the Superstore Sales Data Pipeline.

---

## Base Columns (from source CSV)

| Column | Type | Nullable | Description | Example |
|---|---|---|---|---|
| `Row ID` | int64 | No | Unique sequential row identifier | 1 |
| `Order ID` | string | No | Unique order reference (multiple rows per order) | CA-2016-152156 |
| `Order Date` | datetime | No | Date the customer placed the order | 2016-11-08 |
| `Ship Date` | datetime | No | Date the order was dispatched | 2016-11-11 |
| `Ship Mode` | string | No | Shipping service tier | Second Class |
| `Customer ID` | string | No | Unique customer identifier | CG-12520 |
| `Customer Name` | string | No | Full customer name | Claire Gute |
| `Segment` | string | No | Market segment | Consumer |
| `Postal Code` | string | Yes | Delivery ZIP code (zero-padded to 5 chars) | 42420 |
| `City` | string | No | Delivery city | Henderson |
| `State` | string | No | US delivery state | Kentucky |
| `Country` | string | No | Country (always "United States") | United States |
| `Region` | string | No | US sales region | South |
| `Product ID` | string | No | Unique product catalogue identifier | FUR-BO-10001798 |
| `Category` | string | No | Top-level product category | Furniture |
| `Sub-Category` | string | No | Second-level product classification | Bookcases |
| `Product Name` | string | No | Full descriptive product name | Bush Somerset Collection Bookcase |
| `Sales` | float64 | No | Post-discount revenue in USD | 261.96 |
| `Quantity` | int64 | No | Units sold in this line item | 2 |
| `Discount` | float64 | No | Fractional discount applied (0.0–1.0) | 0.20 |
| `Profit` | float64 | No | Net profit or loss in USD (can be negative) | 41.91 |

### Categorical Value Sets

**Ship Mode:** Second Class, Standard Class, First Class, Same Day

**Segment:** Consumer, Corporate, Home Office

**Region:** East, West, Central, South

**Category:** Furniture, Office Supplies, Technology

---

## Derived Columns (added by feature_engineer.py)

### Time Features

| Column | Type | Description | Example |
|---|---|---|---|
| `order_year` | int64 | Calendar year of Order Date | 2016 |
| `order_month` | int64 | Month number of Order Date (1–12) | 11 |
| `order_month_name` | string | Abbreviated month name | Nov |
| `order_quarter` | int64 | Calendar quarter (1–4) | 4 |
| `order_day_of_week` | int64 | Weekday number (Mon=0, Sun=6) | 1 |
| `shipping_days` | int64 | Calendar days between order and shipment | 3 |

### Financial Features

| Column | Type | Description | Formula |
|---|---|---|---|
| `profit_margin_pct` | float64 | Net profit as % of revenue | `(Profit / Sales) × 100` |
| `discount_amount` | float64 | Absolute discount value in USD | `Sales × Discount` |
| `revenue_per_unit` | float64 | Average revenue per unit sold | `Sales / Quantity` |
| `profit_per_unit` | float64 | Average profit (or loss) per unit | `Profit / Quantity` |
| `is_profitable` | bool | True when this line item made money | `Profit > 0` |

### Categorical Features

| Column | Values | Thresholds |
|---|---|---|
| `profit_tier` | Loss, Low, Medium, High | Loss: margin < 0%, Low: 0–10%, Medium: 10–20%, High: > 20% |
| `shipping_speed` | Same Day, Express, Standard, Slow | 0–1 day, 2–3 days, 4–6 days, 7+ days |

---

## Dataset Statistics (full Kaggle dataset)

| Metric | Value |
|---|---|
| Total rows | 9,994 |
| Unique orders | 5,009 |
| Unique customers | 793 |
| Unique products | 1,850 |
| Date range | Jan 2014 – Dec 2017 |
| Total revenue | $2,297,201 |
| Total profit | $286,397 |
| Overall profit margin | 12.5% |
