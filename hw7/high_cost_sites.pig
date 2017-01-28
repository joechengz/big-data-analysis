data = LOAD '/dualcore/{ad_data1/part-m-00000,ad_data2/part-r-00000}' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
             placement:chararray, was_clicked:int, cpc:int);



clicked = FILTER data BY was_clicked ==1;
-- TODO (C): Group the data by the appropriate field

bykeyword = GROUP clicked BY keyword;

/* TODO (D): Create a new relation which includes only the 
 *           display site and the total cost of all clicks 
 *           on that site
 */
totals = FOREACH bykeyword GENERATE group, SUM(clicked.cpc) AS totalcost;


-- TODO (E): Sort that new relation by cost (ascending)
sorttotals = ORDER totals BY totalcost DESC;


-- TODO (F): Display just the first three records to the screen
top3 = LIMIT sorttotals 3;
STORE top3 INTO '/dualcore/hw7_2';
DUMP top3;
