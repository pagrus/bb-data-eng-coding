import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime

sales_table = """ 
CREATE TABLE IF NOT EXISTS sales (
    id integer PRIMARY KEY,
    stamp text NOT NULL,
    item_id integer NOT NULL,
    quantity integer NOT NULL
    ); """

items_table = """
CREATE TABLE IF NOT EXISTS items (
    id integer PRIMARY KEY,
    item_name text NOT NULL
    ); """

temps_table_hourly = """
CREATE TABLE IF NOT EXISTS temps_hourly (
    id integer PRIMARY KEY,
    stamp text NOT NULL,
    temp_val real NOT NULL
    ); """
    
temps_table_daily = """
CREATE TABLE IF NOT EXISTS temps_daily (
    id integer PRIMARY KEY,
    stamp text NOT NULL,
    temp_val real NOT NULL
    ); """

sales_view = """
CREATE VIEW sales_detail 
AS SELECT s.id AS sales_id, stamp, item_name, item_id, quantity, 
    strftime('%Y-%m-%d', stamp) AS transaction_date,
    strftime('%H', stamp) AS transaction_hour,
    strftime('%Y-%m-%d-%H', stamp) AS transaction_date_hour
FROM sales s 
JOIN items i
ON s.item_id = i.id;
"""

item_daily_summary_view = """
CREATE VIEW item_daily_summary
AS SELECT item_name, SUM(quantity) AS daily_total, transaction_date 
FROM sales_detail 
GROUP BY item_name, transaction_date;
"""

temps_view_hourly = """
CREATE VIEW temps_breakout_hourly
AS SELECT id, stamp, temp_val, 
    CAST(round(temp_val) AS INTEGER) AS temp_int,
    strftime('%Y-%m-%d', stamp) AS temp_date,
    strftime('%H', stamp) AS temp_hour,
    strftime('%Y-%m-%d-%H', stamp) AS temp_date_hour
FROM temps_hourly;
"""

temps_view_daily = """
CREATE VIEW temps_breakout_daily
AS SELECT id, stamp, temp_val, 
    CAST(round(temp_val) AS INTEGER) AS temp_int
FROM temps_daily;
"""

sales_with_temps_view = """
CREATE VIEW sales_with_temps
AS SELECT item_name, quantity, sd.stamp, temp_int
FROM sales_detail sd
JOIN temps_breakout_hourly tb
ON sd.transaction_date_hour = tb.temp_date_hour;
"""

temp_deltas_view = """
CREATE VIEW temp_deltas
AS SELECT ta.id, ta.stamp AS stamp_a, ta.temp_int AS temp_a, tb.stamp AS stamp_b, tb.temp_int AS temp_b, (tb.temp_int - ta.temp_int) as diff 
FROM temps_breakout_daily ta 
JOIN temps_breakout_daily tb on ta.id = (tb.id - 1);
"""

item_summary_diffs_view = """
CREATE VIEW item_summary_diffs 
AS SELECT id, stamp_a, stamp_b, diff AS temp_diff, 
dsa.item_name AS item_name_a, dsa.daily_total AS daily_total_a, dsa.transaction_date AS transaction_date_a,  
dsb.item_name AS item_name_b, dsb.daily_total AS daily_total_b, dsb.transaction_date AS transaction_date_b,
(dsb.daily_total - dsa.daily_total) AS item_total_diff  
FROM temp_deltas td 
JOIN item_daily_summary dsa ON td.stamp_a = dsa.transaction_date
JOIN item_daily_summary dsb ON td.stamp_b = dsb.transaction_date AND dsa.item_name = dsb.item_name;
"""

item_change_pos_2f = """
CREATE VIEW item_change_pos_2f
AS SELECT item_name_a, AVG(item_total_diff) AS avg_diff
FROM item_summary_diffs 
WHERE temp_diff = 2  
GROUP BY item_name_a;
"""

item_change_neg_2f = """
CREATE VIEW item_change_neg_2f
AS SELECT item_name_a, AVG(item_total_diff) AS avg_diff 
FROM item_summary_diffs 
WHERE temp_diff = -2  
GROUP BY item_name_a;
"""

##############################################################

### Make the database, define tables

##############################################################

print("connecting to database...")
conn = sqlite3.connect("sales.db")
cur = conn.cursor()

print("creating tables and views...")
cur.execute(sales_table)
cur.execute(items_table)
cur.execute(temps_table_hourly)
cur.execute(temps_table_daily)
cur.execute(sales_view)
cur.execute(item_daily_summary_view)
cur.execute(temps_view_hourly)
cur.execute(temps_view_daily)
cur.execute(sales_with_temps_view)
cur.execute(temp_deltas_view)
cur.execute(item_summary_diffs_view)
cur.execute(item_change_pos_2f)
cur.execute(item_change_neg_2f)

##############################################################

### Read sales entries into a pandas dataframe, let pandas figure out date format

##############################################################

print("reading in sales data...")
df = pd.read_csv('data/morse.csv', parse_dates=['local_created_at'])
df = df.rename(columns={'local_created_at': 'stamp', 'net_quantity': 'quantity'})

##############################################################

### Get items, give them IDs, insert into items table
### Swap keys and vals for replacing in the dataframe
### Make a new column with item IDs
### Probably don't have to do all this but it is nicer to have a normalized database

##############################################################

item_arr = df.item_name.unique()
item_dict = dict(enumerate(item_arr, 1))
inv_item_dict = {v: k for k, v in item_dict.items()}

for item_key, item_value in item_dict.items():
    cur.execute('insert into items values (?,?)', [item_key, item_value])
conn.commit()

df['item_id'] = df['item_name'].map(inv_item_dict)

##############################################################

### Insert sales transactions into db

##############################################################

print("inserting sales data...")
df[['stamp', 'quantity', 'item_id']].to_sql('sales', con=conn, index=False, if_exists='append')

##############################################################

### Read in hourly temp data, write to table

##############################################################

print("reading hourly temp data...")
tdf = pd.read_csv('data/1524261.csv', usecols=[5, 10], parse_dates=['DATE'])
tdf = tdf.rename(columns={'DATE': 'stamp', 'HOURLYDRYBULBTEMPF': 'temp_val'})
tdf.fillna(method='ffill', inplace=True)
print("inserting hourly temp data...")
tdf[['stamp', 'temp_val']].to_sql('temps_hourly', con=conn, index=False, if_exists='append')

##############################################################

### Read in daily temp data, write to table

##############################################################

print("reading daily average temps...")
with open('data/KCAOAKLA11_2016-01-01_2016-12-31.json') as json_file:    
    weather_json = json.load(json_file)
print("inserting daily average temps...")
for day in weather_json['history']['days']:
    temp_date = day['summary']['date']['iso8601'][:10]
    temp_val = day['summary']['temperature']
    cur.execute('insert into temps_daily (stamp, temp_val) values (?,?)', [temp_date, temp_val])
conn.commit()
