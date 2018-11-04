.header on
.mode csv


SELECT item_name, SUM(quantity), temp_int FROM sales_with_temps GROUP BY temp_int;
