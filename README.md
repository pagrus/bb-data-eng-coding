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

Describe approach here

## Instructions

List requirements, modules etc. Describe procedure

## Extra Credit

### How would productize both reports? Please consider the following in your answer: Data modeling, Data partioning, Data backfill


### What are some tradeoffs and assumptions for your design of this ETL?


### What some of the tools you would consider to build this into an ETL pipeline?

## notes
- nice to have normalized tables with keys &c
- convert datestamps to unix time? or nah
- let's check for weird data, mistakes etc
- well lookit that, there's negative vals in the quantity column. what to do with those?
- let's plan to use postgres but maybe start with sqlite for kicks
- thinkin bout schemas
