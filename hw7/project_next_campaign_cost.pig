data = LOAD '/dualcore/{ad_data1/part-m-00000,ad_data2/part-r-00000}' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
             placement:chararray, was_clicked:int, cpc:int);


-- TODO (B): Include only records where was_clicked has a value of 1
clicked = FILTER data BY (was_clicked==1);



grouped = GROUP clicked ALL;


maxc = FOREACH grouped GENERATE MAX(clicked.cpc);
DUMP maxc
result = FOREACH grouped GENERATE MAX(clicked.cpc)*50000;

DUMP result;
