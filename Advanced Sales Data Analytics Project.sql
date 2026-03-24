-- CHANGE OVER TIME ANALYSIS

-- each year's sales, number of customers, total quantity
select 
year(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date);

/* since the measures were quite less in 2010 and 2014, 
checking if the data was available for throughtout the year.
Below we can can see that for these years, 
order information is available not even for 1 complete month.
*/
select year(order_date) as order_year,
min(order_date) as first_order_date_of_year, 
max(order_date) as last_order_date_of_year
from fact_sales 
where order_date is not null
group by year(order_date)
order by year(order_date);

-- checking for seasonality of sales throughout the year by each month's analysis
-- Removing data of 2010 and 2014 because the data was not there for the entire year.
select 
month(order_date) as order_month,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity,
dense_rank() over (order by sum(sales_amount) desc) ranking_by_sales
from fact_sales
where order_date is not null and year(order_date) not in (2010,2014)
group by month(order_date)
order by month(order_date);


-- CUMULATIVE ANALYSIS 
/* Aggregate the data progressively over time
that helps us to understand whether our business is growing or declining*/

-- calculating the total sales per month and the running total of sales over time
select
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_average_price
from
(select
DATETRUNC(year, order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from fact_sales
where order_date is not null
group by DATETRUNC(year, order_date)
) t


-- PERFORMANCE ANALYSIS
/*comparing the current value to a target value which helps measure success and compare performance*/

/* Analyzing the yearly performance of products by comparing their sales
to both the average sales performance of the product and the previous year's sales*/
with yearly_product_sales as (
select 
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from fact_sales f
left join dim_products p
on f.product_key = p.product_key
where order_date is not null
group by
year(f.order_date),
p.product_name
)

select
order_year,
product_name,
current_sales,
avg(current_sales) over (partition by product_name) as avg_sales,
current_sales - avg(current_sales) over (partition by product_name) as diff_avg,
case when current_sales - avg(current_sales) over (partition by product_name) > 0 then 'Above Avg'
	 when current_sales - avg(current_sales) over (partition by product_name) < 0 then 'Below Avg'
	 else 'Avg'
end avg_change,
lag(current_sales) over (partition by product_name order by order_year) as py_sales,
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_py,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
	 when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
	 else 'No change'
end py_change
from yearly_product_sales
order by product_name, order_year

-- PART TO WHOLE ANALYSIS
/*
analyze how an individual part is performing compared to the overall,
allowing us to understand which category has the greatest impact on the business
*/

-- which category contributes the most to the overall sales?
with category_sales as (
select 
category, 
sum(sales_amount) total_sales
from fact_sales f
left join dim_products p
on p.product_key = f.product_key
group by category
)

select 
category,
total_sales,
sum(total_sales) over() overall_sales,
concat(round((cast(total_sales as float)/sum(total_sales) over())*100, 2),'%') as percentage_of_total
from category_sales
order by total_sales desc

-- DATA SEGMENTATION - generating insights from segmenting measures also 

-- Segment products into cost ranges and count how many products fall into each segment 
with product_segments as (
select
product_key,
product_name,
cost,
case when cost < 100 then 'Below 100'
	 when cost between 100 and 500 then '100-500'
	 when cost between 500 and 1000 then '500-1000'
	 else 'Above 1000'
end as cost_range
from dim_products)

select 
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range
order by 2 desc

/*
Grouping customers into three segments based on their spending behavior:

- VIP: at least 12 months of history and spending more than €5,000.
- Regular: at least 12 months of history but spending €5,000 or less.
- New: lifespan less than 12 months.
*/

with customer_spending as (
select 
c.customer_key,
sum(f.sales_amount) as total_spending,
max(f.order_date) as last_order,
min(f.order_date) as first_order,
datediff(month, min(order_date), max(order_date)) as lifespan
from fact_sales f
left join dim_customers c
on c.customer_key = f.customer_key
group by c.customer_key),

customer_segmention as (
select
customer_key,
total_spending,
lifespan,
case when lifespan >=12 and total_spending > 5000 then 'VIP'
	 when lifespan >=12 and total_spending <= 5000 then 'Regular'
	 else 'new'
end as customer_segment
from customer_spending)

select 
customer_segment,
count(customer_key) as total_customers
from customer_segmention
group by customer_segment
order by total_customers desc

/*
======================================================================
Customer Report
======================================================================

Purpose:
- This report consolidates key customer metrics and behaviors

Highlights:
1. Gathers essential fields such as names, ages, and transaction details.
2. Segments customers into categories (VIP, Regular, New) and age groups.
3. Aggregates customer-level metrics:
   - total orders
   - total sales
   - total quantity purchased
   - total products
   - lifespan (in months)
4. Calculates valuable KPIs:
   - recency (months since last order)
   - average order value
   - average monthly spend

======================================================================
*/

create view report_customers as 

with base_query as(
/*--------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
--------------------------------------------------------------------*/
select
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name, ' ', c.last_name) as customer_name,
datediff(year, c.birthdate, getdate()) as age
from fact_sales f
left join dim_customers c
on c.customer_key = f.customer_key
where f.order_date is not null)

, customer_aggregation as (
/*--------------------------------------------------------------------
2) Customer Aggregations: summarizes key metrics at the customer level
--------------------------------------------------------------------*/
select 
	customer_key,
	customer_number,
	customer_name,
	age,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	count(distinct product_key) as total_products,
	max(order_date) as last_order_date,
	datediff(month, min(order_date), max(order_date)) as lifespan
from base_query
group by 
	customer_key,
	customer_number,
	customer_name,
	age)

select 
customer_key,
customer_number,
customer_name,
age,
case when age < 20 then 'Under 20'
	 when age between 20 and 29 then '20-29'
	 when age between 30 and 39 then '30-39'
	 when age between 40 and 49 then '40-49'
	 else '50 and above'
end as age_group,
case when lifespan >=12 and total_sales > 5000 then 'VIP'
	 when lifespan >=12 and total_sales <= 5000 then 'Regular'
	 else 'new'
end as customer_segment,
last_order_date,
datediff(month, last_order_date, getdate()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
-- compute average order value (AVO)
case when total_orders = 0 then 0
	 else total_sales / total_orders
end as avg_order_value,
-- compute average monthly spend
case when lifespan = 0 then total_sales
     else total_sales / lifespan
end as avg_monthly_spend
from customer_aggregation


select * from report_customers


/*
================================================================
Product Report
================================================================

Purpose:
  - This report consolidates key product metrics and behaviors.

Highlights:
  1. Gathers essential fields such as product name, category, subcategory, and cost.
  2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
  3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
  4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue

================================================================
*/

CREATE VIEW report_products AS

WITH base_query AS (
/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_products
---------------------------------------------------------------------------*/
SELECT
    f.order_number,
    f.order_date,
    f.customer_key,
    f.sales_amount,
    f.quantity,
    p.product_key,
    p.product_name,
    p.category,
    p.subcategory,
    p.cost
FROM fact_sales f
LEFT JOIN dim_products p
    ON f.product_key = p.product_key
WHERE order_date IS NOT NULL  -- only consider valid sales dates
)

,product_aggregations AS (
/*---------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
---------------------------------------------------------------------------*/
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
    MAX(order_date) AS last_sale_date,
    COUNT(DISTINCT order_number) AS total_orders,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)),1) AS avg_selling_price
FROM base_query
group by 
    product_key,
    product_name,
    category,
    subcategory,
    cost
)

/*---------------------------------------------------------------------------
  3) Final Query: Combines all product results into one output
---------------------------------------------------------------------------*/
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
    CASE
        WHEN total_sales > 50000 THEN 'High-Performer'
        WHEN total_sales >= 10000 THEN 'Mid-Range'
        ELSE 'Low-Performer'
    END AS product_segment,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
    -- Average Order Revenue (AOR)
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_revenue,

    -- Average Monthly Revenue
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan
    END AS avg_monthly_revenue

FROM product_aggregations

select * from report_products