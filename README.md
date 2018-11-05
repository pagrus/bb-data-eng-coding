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
- bb_data_eng_challenge_db_init.py - code to create database, define tables, import data
- bb_data_eng_challenge.py - code to run queries and output CSV files
- data/morse.csv - data provided with instructions - ignored by .gitignore as per request
- data/1524261.csv - weather data provided by ncdc.noaa.gov
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

I would first try to establish what incoming weather data would look like in a production setting, and maybe even recommend a more DIY type solution with an IOT device or SBC onsite rather than relying on a service to provide temp logs. 

### What are some tradeoffs and assumptions for your design of this ETL?

The big assumption I'm making is that these queries would be run once per day and could be cached somewhere for use in dashboards, reports, daily summaries and so forth. If they were more likely to be executed many times per hour (or minute, or second) then that would change my approach pretty dramatically. I also am assuming that the weather at the airport tracks the weather in Temescal pretty closely, or that whatever difference there is would be consistent.

### What some of the tools you would consider to build this into an ETL pipeline?

Mainly a faster beefier database like PostgreSQL, depending on the use case(s) maybe cron or other scheduler. 
