import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime

data_dir = 'data'

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
    temp_date text NOT NULL,
    temp_avg real NOT NULL
    ); """

sales_view = """
CREATE VIEW sales_detail 
AS SELECT s.id AS sales_id, stamp, item_name, item_id, quantity, strftime('%Y-%m-%d', stamp) AS transaction_date
FROM sales s 
JOIN items i
ON s.item_id = i.id;
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

"""

Read sales entries into a pandas dataframe, let pandas figure out date format

"""

df = pd.read_csv('data/morse.csv', parse_dates=['local_created_at'])
df = df.rename(columns={'local_created_at': 'stamp', 'net_quantity': 'quantity'})

"""

Get items, give them IDs, insert in to items table
Swap keys and vals for replacing in the dataframe
Make a new column with item IDs
Probably don't have to do all this but it nicer to have a normalized database

"""

item_arr = df.item_name.unique()
item_dict = dict(enumerate(item_arr, 1))
inv_item_dict = {v: k for k, v in item_dict.items()}

for item_key, item_value in item_dict.items():
    cur.execute('insert into items values (?,?)', [item_key, item_value])
conn.commit()

df['item_id'] = df['item_name'].map(inv_item_dict)

"""

Insert sales items into db


"""

df[['stamp', 'quantity', 'item_id']].to_sql('sales', con=conn, index= False, if_exists='append')

