# Project 1 - Currency Exchange Rates

## Context 

The aim of this project is to build ETL pipeline using both Python as well as SQL that extracts historical exchange rates from an API endpoint. 
The data will show the value of 1 Australian dollar ($AUD) against 6 currencies:
<li>Chinese Yuan ¥</li>
<li>European Union Euro € </li>
<li>Indian Rupee ₹ </li>
<li>Japanese Yen 円 </li>
<li>Singaporean Dollar $ </li>
<li>United States Dollar $ </li>
<li>Goal of the project is to build a clean set of exchange rates data, so that data analysts are able to track daily currency movements throughout year 2022, and also review monthly averages. </li>

<li></li>

## Architecture 

- **Extraction**: Full extract of 365 days worth of currency rates from 1st of Jan to 31st of Dec 2022
- **Transformation**: Series of different transformation techniques done using Python & SQL in PostgresTransformations done Work on your project in class (and outside of class) 
- **Load**: Upsert load to Postgres 
- **Docker Container**: Docker container successfully created, runs locally and also on AWS as well 

## Limitations 

We initially wanted to have two finished products, first table is to show currency rates by individual day, the second table is to show monthly aggregated data.

Screenshots show the final product, unfortunately we couldn't get them all uploaded to AWS in time.


