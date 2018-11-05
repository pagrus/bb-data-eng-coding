# Data Engineer Coding Challenge

## Challenge overview

### What should you deliver?

- Written summary of the approach you took to solve the challenge
- Code used to ETL weather data
- Queries used to generate reports
- Results of your solutions in CSV format
- Instructions for runnning your code
- Answers to extra credit

## Files

- README.md - This document
- bb_data_eng_challenge.py - code to create database, define tables, import data
- data/morse.csv - data provided with instructions - ignored by .gitignore per request
- data/1524261.csv - weather data provided by ncdc.noaa.gov
- data/KCAOAKLA11_2016-01-01_2016-12-31.json - daily summary of temp data provided by Weather Underground
- query1.sql - SQL file containing my solution to part 1
- query2.sql - SQL file containing my solution to part 2
- query1.csv - results of query 1
- query2.csv - results of query 2

## Approach

I started with some EDA, during which I noticed some negative values in the quantity sold field. I took these to be representative of the data set they were taken from and while these didn't seem to match up with positive values (like they would in a canceled or void transaction) there weren't enough of them to cause concern. Since there were only 25 unique items it seemed to be better to split them off into their own table and de-duplicate/normalize the sales data prior to insertion in the database.

In looking for weather data I considered purchasing a data set or attempting to use a trial version but suitable options didn't present themselves. I found historical data from https://www.ncdc.noaa.gov/ which had hourly logs of temps but was taken from Oakland International Airport, some distance away. Weather Underground had JSON data (obtained by copying from the response payload from a JS request in a browser inspector) that was much closer but only had daily averages. I emailed the CS department at Oakland Technical High School, thinking that they might have temp records for use in student projects but I never heard back from them. I wound up using both the NOAA and WU sets, one for each query.

An SQLite database was created and tables were defined, along with a number of views. I like to use views to troubleshoot bottlenecks, expose subsets or specially-formatted data, and to clarify logic. 

The normalized sales transaction data was inserted into two tables via pandas, in order to handle the nonstandard date format. The NOAA data was pared down to two columns and inserted into the table from a pandas dataframe after filling in nulls with adjacent values. The WU JSON data was read into a dictionary, then inserted into a table.

I wrote a couple queries to fetch the solutions from the database. The queries themselves are pretty straightforward, as most of the work is done by the views ahead of time. SQLite will provide CSV data so piping the queries into files is very straightforward.

## Instructions

This solution requires python 3 and the pandas, numpy, sqlite3, json, and datetime modules. It was written and tested on Ubuntu Mate Linux 18.04 and bash but I imagine pretty much any shell would work just as well.

1. Copy the files or clone the GitHub repo 
2. Copy or move morse.csv into the data directory
3. Run `python3 bb_data_eng_challenge.py` to create and populate the database
4. Run `sqlite3 sales.db < query1.sql > query1.csv` This one takes a minute to execute
5. Run `sqlite3 sales.db < query2.sql > query2.csv`

## Extra Credit

### How would productize both reports? Please consider the following in your answer: Data modeling, Data partioning, Data backfill

I would first try to establish what incoming weather data would look like in a production setting, and maybe even recommend a more DIY type solution with an IOT device or SBC onsite rather than relying on a service to provide temp logs. I would probably also think about notifications for when it completes sucessfully, chokes on errors, or encounters weird data. I am coming back to RSS for this kind of thing and while that's not really a tool it could inform whatever actual tools are used.

### What are some tradeoffs and assumptions for your design of this ETL?

The big assumption I'm making is that these queries would be run once per day and could be cached somewhere for use in dashboards, reports, daily summaries and so forth. If they were more likely to be executed many times per hour (or minute, or second) then that would change my approach pretty dramatically. I also am assuming that the weather at the airport tracks the weather in Temescal pretty closely, or that whatever difference there is would be consistent. One tradeoff is the use of SQLite-- while it can be used for demonstration and proof of concept type stuff I am a little uncomfortable with its fast and loose data types. On a stylistic note I sacrifice some efficiency and speed for readability and simplicity, that I attempt to do this probably surprises no one. I am also assuming that the data provided is a good random sample and eg the negative values would be something I could expect in a production dataset. 

### What some of the tools you would consider to build this into an ETL pipeline?

Mainly a faster beefier database like PostgreSQL, and depending on the use case(s) maybe cron or other scheduler. Something to refresh data, probably curl or the python requests module unless something more specific is required to interact with either the sales or weather APIs. Maybe something to summarize and/or archive old data, although that's probably technically ouside the scope of this tool. 

## Some schemas and example data

The joined transactions and items tables
```sqlite> SELECT stamp, item_name, item_id, quantity, transaction_date_hour FROM sales_detail ORDER BY RANDOM() LIMIT 10;
stamp                item_name         item_id     quantity    transaction_date_hour
-------------------  ----------------  ----------  ----------  ---------------------
2016-08-08 10:04:37  Au Lait, Unknown  16          2           2016-08-08-10        
2016-02-06 09:34:08  Espresso          12          1           2016-02-06-09        
2016-06-29 14:18:37  NOLA Carton       8           1           2016-06-29-14        
2016-08-21 16:53:35  Cascara Fizz      14          1           2016-08-21-16        
2016-08-10 09:01:12  Drip Coffee       6           1           2016-08-10-09        
2016-02-09 12:14:45  Latte             1           1           2016-02-09-12        
2016-12-09 11:02:36  Macchiato         20          1           2016-12-09-11        
2016-05-16 15:12:47  Latte             1           1           2016-05-16-15        
2016-11-28 09:19:25  Drip Coffee       6           2           2016-11-28-09        
2016-03-01 13:55:46  Drip Coffee       6           1           2016-03-01-13 
```

Every sale with the temp at the time
```sqlite> SELECT * FROM sales_with_temps ORDER BY RANDOM() LIMIT 10;
item_name        quantity    stamp                temp_int  
---------------  ----------  -------------------  ----------
Sparkling Water  1           2016-02-14 15:11:28  70        
Latte            1           2016-09-19 10:01:14  80        
S.O. Iced - 4 o  1           2016-07-23 10:30:02  74        
Latte            1           2016-04-09 07:26:05  58        
Americano        1           2016-11-22 11:11:22  58        
Drip Coffee      1           2016-07-16 07:14:14  62        
Drip Coffee      1           2016-08-06 12:37:28  64        
Cappuccino, Unk  2           2016-09-09 09:26:46  63        
Drip Coffee      2           2016-03-28 17:14:45  59        
Drip Coffee      1           2016-04-24 14:01:38  65 
``` 

Pairs of dates with differences in temp
```sqlite> SELECT * FROM temp_deltas ORDER BY RANDOM() LIMIT 10;
id          stamp_a     temp_a      stamp_b     temp_b      diff      
----------  ----------  ----------  ----------  ----------  ----------
98          2016-04-07  63          2016-04-08  62          -1        
34          2016-02-03  49          2016-02-04  53          4         
56          2016-02-25  61          2016-02-26  59          -2        
192         2016-07-10  66          2016-07-11  68          2         
253         2016-09-09  62          2016-09-10  62          0         
129         2016-05-08  60          2016-05-09  60          0         
166         2016-06-14  61          2016-06-15  58          -3        
193         2016-07-11  68          2016-07-12  67          -1        
19          2016-01-19  56          2016-01-20  53          -3        
354         2016-12-19  45          2016-12-20  48          3 
```