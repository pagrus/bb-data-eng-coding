.header on
.mode csv

SELECT item_name_a, AVG(item_total_diff), temp_diff FROM item_summary_diffs WHERE temp_diff = 2 GROUP BY item_name_a;
