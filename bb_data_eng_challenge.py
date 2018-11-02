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

temps_table = """
CREATE TABLE IF NOT EXISTS temps (
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

temps_view = """
CREATE VIEW temps_breakout
AS SELECT id, stamp, temp_val, 
    CAST(round(temp_val) AS INTEGER) AS temp_int,
    strftime('%Y-%m-%d', stamp) AS temp_date,
    strftime('%H', stamp) AS temp_hour,
    strftime('%Y-%m-%d-%H', stamp) AS temp_date_hour
FROM temps;
"""

sales_with_temps_view = """
CREATE VIEW sales_with_temps
AS SELECT item_name, quantity, sd.stamp, temp_int
FROM sales_detail sd
JOIN temps_breakout tb
ON sd.transaction_date_hour = tb.temp_date_hour;
"""

"""

Make the database, define tables

"""

conn = sqlite3.connect("sales.db")
cur = conn.cursor()

cur.execute(sales_table)
cur.execute(items_table)
cur.execute(temps_table)
cur.execute(sales_view)
cur.execute(temps_view)
cur.execute(sales_with_temps_view)

"""

Read sales entries into a pandas dataframe, let pandas figure out date format

"""

df = pd.read_csv('data/morse.csv', parse_dates=['local_created_at'])
df = df.rename(columns={'local_created_at': 'stamp', 'net_quantity': 'quantity'})

"""

Get items, give them IDs, insert into items table
Swap keys and vals for replacing in the dataframe
Make a new column with item IDs
Probably don't have to do all this but it is nicer to have a normalized database

"""

item_arr = df.item_name.unique()
item_dict = dict(enumerate(item_arr, 1))
inv_item_dict = {v: k for k, v in item_dict.items()}

for item_key, item_value in item_dict.items():
    cur.execute('insert into items values (?,?)', [item_key, item_value])
conn.commit()

df['item_id'] = df['item_name'].map(inv_item_dict)

"""

Insert sales transactions into db


"""

df[['stamp', 'quantity', 'item_id']].to_sql('sales', con=conn, index=False, if_exists='append')

"""

Read in temp data, write to temps table

"""

tdf = pd.read_csv('data/1524261.csv', usecols=[5, 10], parse_dates=['DATE'])
tdf = tdf.rename(columns={'DATE': 'stamp', 'HOURLYDRYBULBTEMPF': 'temp_val'})
tdf.fillna(method='ffill', inplace=True)
tdf[['stamp', 'temp_val']].to_sql('temps', con=conn, index=False, if_exists='append')

