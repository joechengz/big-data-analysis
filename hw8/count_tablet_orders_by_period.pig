orders = LOAD '/dualcore/orders' AS (order_id:int,
             cust_id:int,
             order_dtm:chararray);

details = LOAD '/dualcore/order_details' AS (order_id:int,
             prod_id:int);

-- include only the months we want to analyze
recent = FILTER orders BY order_dtm matches '^2013-0[2345]-.*$';

-- include only the product we're advertising
tablets = FILTER details BY prod_id == 1274348;


-- TODO (A): Join the two relations on the order_id field

joined = JOIN recent BY order_id, tablets BY order_id;
-- TODO (B): Create a new relation containing just the month

monthorder = FOREACH joined GENERATE 
recent::order_id,
recent::cust_id,
SUBSTRING(recent::order_dtm,5,7) AS month,
tablets::prod_id;


-- TODO (C): Group by month and then count the records in each group
groupby = GROUP monthorder BY month;


-- TODO (D): Display the results to the screen
ans = FOREACH groupby GENERATE COUNT(monthorder);

DUMP ans;
