-- load the data sets
orders = LOAD '/dualcore/orders' AS (order_id:int,
             cust_id:int,
             order_dtm:chararray);

details = LOAD '/dualcore/order_details' AS (order_id:int,
             prod_id:int);

products = LOAD '/dualcore/products' AS (prod_id:int,
             brand:chararray,
             name:chararray,
             price:int,
             cost:int,
             shipping_wt:int);

a2012orders = FILTER orders BY order_dtm matches '^2012.*$';

group_orders = GROUP orders BY cust_id;
count_orders = FOREACH group_orders GENERATE group, COUNT(orders) AS num_orders;
order5 = FILTER count_orders BY num_orders >= 5;

customer_order = JOIN order5 BY group,a2012orders BY cust_id;

customer_order_details = JOIN customer_order BY a2012orders::order_id, details BY order_id;

customer_order_products = JOIN customer_order_details BY details::prod_id, products BY prod_id;

customer_prices = FOREACH customer_order_products GENERATE order5::group AS cust_id, products::price AS price;

group_customer = GROUP customer_prices BY cust_id;
customert = FOREACH group_customer GENERATE group, SUM(customer_prices.price) AS totals;

SPLIT customert INTO
platinum IF totals >=1000000,
gold IF totals >=500000 AND totals <1000000,
silver IF totals >=250000 AND totals < 500000;

STORE platinum INTO '/dualcore/hw81_platinum';
STORE gold INTO '/dualcore/hw81_gold';
STORE silver INTO '/dualcore/hw81_silver';


/*
 * TODO: Find all customers who had at least five orders
 *       during 2012. For each such customer, calculate 
 *       the total price of all products he or she ordered
 *       during that year. Split those customers into three
 *       new groups based on the total amount:
 *
 *        platinum: At least $10,000
 *        gold:     At least $5,000 but less than $10,000
 *        silver:   At least $2,500 but less than $5,000
 *
 *       Write the customer ID and total price into a new
 *       directory in HDFS. After you run the script, use
 *       'hadoop fs -getmerge' to create a local text file
 *       containing the data for each of these three groups.
 *       Finally, use the 'head' or 'tail' commands to check 
 *       a few records from your results, and then use the  
 *       'wc -l' command to count the number of customers in 
 *       each group.
 */
