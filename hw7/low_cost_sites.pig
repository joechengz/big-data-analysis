-- TODO (A): Replace 'FIXME' to load the test_ad_data.txt file.

data = LOAD '/dualcore/{ad_data1/part-m-00000,ad_data2/part-r-00000}' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
             placement:chararray, was_clicked:int, cpc:int);


-- TODO (B): Include only records where was_clicked has a value of 1
clicked = FILTER data BY (was_clicked==1);


-- TODO (C): Group the data by the appropriate field

bysite = GROUP clicked BY display_site;

/* TODO (D): Create a new relation which includes only the 
 *           display site and the total cost of all clicks 
 *           on that site
 */
totals = FOREACH bysite GENERATE group, SUM(clicked.cpc) AS totalcost;


-- TODO (E): Sort that new relation by cost (ascending)
sorttotals = ORDER totals BY totalcost ASC;


-- TODO (F): Display just the first three records to the screen
top4 = LIMIT sorttotals 4;
DUMP top4;

