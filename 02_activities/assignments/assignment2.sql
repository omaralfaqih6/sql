/* ASSIGNMENT 2 */
/* SECTION 2 */

-- COALESCE
/* 1. Our favourite manager wants a detailed long list of products, but is afraid of tables! 
We tell them, no problem! We can produce a list with all of the appropriate details. 

Using the following syntax you create our super cool and not at all needy manager a list:

SELECT 
product_name || ', ' || product_size|| ' (' || product_qty_type || ')'
FROM product

But wait! The product table has some bad data (a few NULL values). 
Find the NULLs and then using COALESCE, replace the NULL with a 
blank for the first problem, and 'unit' for the second problem. 

HINT: keep the syntax the same, but edited the correct components with the string. 
The `||` values concatenate the columns into strings. 
Edit the appropriate columns -- you're making two edits -- and the NULL rows will be fixed. 
All the other rows will remain the same.) */

select
product_name
,product_size
,product_qty_type
,product_name || ',' || product_size || '(' || product_qty_type || ')' as manager_list --before the null fix
,product_name || ',' || coalesce(product_size,'') || '(' || coalesce(product_qty_type,'unit') || ')' as manager_list_fixed --after the null fix

from product

--Windowed Functions
/* 1. Write a query that selects from the customer_purchases table and numbers each customer’s  
visits to the farmer’s market (labeling each market date with a different number). 
Each customer’s first visit is labeled 1, second visit is labeled 2, etc. 

You can either display all rows in the customer_purchases table, with the counter changing on
each new market date for each customer, or select only the unique market dates per customer 
(without purchase details) and number those visits. 
HINT: One of these approaches uses ROW_NUMBER() and one uses DENSE_RANK(). */

SELECT
customer_id,
market_date
,row_number () OVER(partition by customer_id order by market_date ASC) as customer_visits_rows --displays all rows in the customer_purchases table, with the counter changing on each new market date for each customer.
from customer_purchases

/* 2. Reverse the numbering of the query from a part so each customer’s most recent visit is labeled 1, 
then write another query that uses this one as a subquery (or temp table) and filters the results to 
only the customer’s most recent visit. */

--customer's most recent visit is labeled 1

SELECT
customer_id,
market_date
,row_number () OVER(partition by customer_id order by market_date DESC) as customer_visits_rows --displays all rows in the customer_purchases table, with the counter changing on each new market date for each customer.
from customer_purchases

--customer's most recent visit only
SELECT
customer_id,
market_date
from(
	SELECT
		customer_id,
		market_date
		,row_number () OVER(partition by customer_id order by market_date DESC) as ranked_visits --displays all rows in the customer_purchases table, with the counter changing on each new market date for each customer.
	from customer_purchases
) as recent_visits
WHERE ranked_visits=1

/* 3. Using a COUNT() window function, include a value along with each row of the 
customer_purchases table that indicates how many different times that customer has purchased that product_id. */

SELECT DISTINCT
customer_id,
product_id
,count () OVER(partition by customer_id order by product_id ASC) as times_customer_purchased_prod

from customer_purchases


-- String manipulations
/* 1. Some product names in the product table have descriptions like "Jar" or "Organic". 
These are separated from the product name with a hyphen. 
Create a column using SUBSTR (and a couple of other commands) that captures these, but is otherwise NULL. 
Remove any trailing or leading whitespaces. Don't just use a case statement for each product! 

| product_name               | description |
|----------------------------|-------------|
| Habanero Peppers - Organic | Organic     |

Hint: you might need to use INSTR(product_name,'-') to find the hyphens. INSTR will help split the column. */

select 
product_name,
CASE
	WHEN product_name like '%- jar%' THEN SUBSTR(product_name,INSTR(product_name,'- ')+2,3) 
	WHEN product_name like '%- org%' THEN SUBSTR(product_name,INSTR(product_name,'- ')+2,7) 
end as description

from product

/* 2. Filter the query to show any product_size value that contain a number with REGEXP. */

select 
product_name,
product_size,
CASE
	WHEN product_name like '%- jar%' THEN SUBSTR(product_name,INSTR(product_name,'- ')+2,3) 
	WHEN product_name like '%- org%' THEN SUBSTR(product_name,INSTR(product_name,'- ')+2,7) 
end as description

from product
where product_size REGEXP '\d'

-- UNION
/* 1. Using a UNION, write a query that displays the market dates with the highest and lowest total sales.

HINT: There are a possibly a few ways to do this query, but if you're struggling, try the following: 
1) Create a CTE/Temp Table to find sales values grouped dates; 
2) Create another CTE/Temp table with a rank windowed function on the previous query to create 
"best day" and "worst day"; 
3) Query the second temp table twice, once for the best day, once for the worst day, 
with a UNION binding them. */

--create first CTE for daily sales query 
WITH daily_sales as (
		SELECT
		market_date,
		sum(quantity * cost_to_customer_per_qty) as sales
		
		from customer_purchases
	Group by market_date
),
--create second CTE for ranking daily sales 
ranked_daily_sales as (
SELECT
market_date,
sales,
rank()OVER(order by sales DESC) as best_daily_sales,
rank()OVER(order by sales ASC) as worst_daily_sales
FROM daily_sales
)
--show the best daily sales 
SELECT
market_date,
sales
from ranked_daily_sales
where best_daily_sales=1
UNION -- unify best and worst daily sales
--show the worst daily sales 
SELECT
market_date,
sales
from ranked_daily_sales
where worst_daily_sales=1

/* SECTION 3 */

-- Cross Join
/*1. Suppose every vendor in the `vendor_inventory` table had 5 of each of their products to sell to **every** 
customer on record. How much money would each vendor make per product? 
Show this by vendor_name and product name, rather than using the IDs.

HINT: Be sure you select only relevant columns and rows. 
Remember, CROSS JOIN will explode your table rows, so CROSS JOIN should likely be a subquery. 
Think a bit about the row counts: how many distinct vendors, product names are there (x)?
How many customers are there (y). 
Before your final group by you should have the product of those two queries (x*y).  */

DROP TABLE if EXISTS temp.vendor_prod_price;

CREATE TABLE temp.vendor_prod_price as --create a temporary table listing each vendor with itsproduct while displaying vendor names and product names rather than IDs. 
select DISTINCT
--vendor_id,
vendor_name,
--product_id,
product_name,
original_price as price
,(5) as quantity_sold
from vendor_inventory vi
INNER JOIN product p ON p.product_id=vi.product_id
INNER JOIN vendor v ON v.vendor_id=vi.vendor_id;

DROP TABLE if EXISTS temp.prod_sales_per_customer; --create a temporary table listing each vendor's products sold to every customer. customers' names are for better view.
--There are 26 customers. 1 vendor is selling 4 products, 1 vendor is selling 3 products and 1 vendor is selling 1 product. This means that 26 rows * 8 product rows will result in the prod_price_per_customer table.
-- so a total 208 rows is generated in this temporary table.
CREATE TABLE temp.prod_sales_per_customer as
SELECT
vendor_name
,product_name
,(customer_first_name || ' ' || customer_last_name) as customer_name
,price
,quantity_sold
,(price*quantity_sold) as total_price

from vendor_prod_price
cross join customer
--group by product_name
order by vendor_name,product_name;
--this query is showing the final required table; vendor's sales per product if 5 of each were sold to every customer.
SELECT
vendor_name
,product_name
,sum(price*quantity_sold) as total_product_sales

from prod_sales_per_customer
group by product_name
order by vendor_name,product_name


-- INSERT
/*1.  Create a new table "product_units". 
This table will contain only products where the `product_qty_type = 'unit'`. 
It should use all of the columns from the product table, as well as a new column for the `CURRENT_TIMESTAMP`.  
Name the timestamp column `snapshot_timestamp`. */

DROP TABLE if EXISTS temp.product_units;

CREATE TABLE temp.product_units as

SELECT *
,CURRENT_TIMESTAMP as snapshot_timestamp

from product
where product_qty_type='unit';

select *
from product_units


/*2. Using `INSERT`, add a new row to the product_units table (with an updated timestamp). 
This can be any product you desire (e.g. add another record for Apple Pie). */

INSERT INTO product_units (product_id,product_name,product_size,product_category_id,product_qty_type,snapshot_timestamp)
values ('999','Black Forest Cake','large','3','unit',CURRENT_TIMESTAMP);

-- DELETE
/* 1. Delete the older record for the whatever product you added. 

HINT: If you don't specify a WHERE clause, you are going to have a bad time.*/



-- UPDATE
/* 1.We want to add the current_quantity to the product_units table. 
First, add a new column, current_quantity to the table using the following syntax.

ALTER TABLE product_units
ADD current_quantity INT;

Then, using UPDATE, change the current_quantity equal to the last quantity value from the vendor_inventory details.

HINT: This one is pretty hard. 
First, determine how to get the "last" quantity per product. 
Second, coalesce null values to 0 (if you don't have null values, figure out how to rearrange your query so you do.) 
Third, SET current_quantity = (...your select statement...), remembering that WHERE can only accommodate one column. 
Finally, make sure you have a WHERE statement to update the right row, 
	you'll need to use product_units.product_id to refer to the correct row within the product_units table. 
When you have all of these components, you can run the update statement. */




