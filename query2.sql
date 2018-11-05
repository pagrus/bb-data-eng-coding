.header on
.mode csv

SELECT p.item_name_a AS item_name, n.avg_diff AS colder, p.avg_diff AS warmer FROM item_change_pos_2f p JOIN item_change_neg_2f n ON p.item_name_a = n.item_name_a;
