# Data Warehouse and BigQuery

## What is a data warehouse?

A data warehouse is a centralized storage solution that stores structured, semi-structured or unstructured data and enables reporting, analysis and data visualization.

## What is BigQuery?

Google's data warehouse solution with the biggest benefit of being serverless and able to analyze large datasets.

## What is partitioning and how does partitioning work in BigQuery?

Partitioning is a technique to divide a Database Table into smaller partitions depending on an identifier (imagine date). It does not create multiple new tables.

## What is clustering and how does clustering work in BigQuery?

Clustering is a technique to organize data into groups within a table depending on a column name.

## What is important with partitioning and clustering?

Partitioning and Clustering are only beneficial for table sizes above 1GB and we should favor clustering over partitioning.

## What are some best practices with BigQuery cost reduction?

- Avoid `SELECT *`
- Use streaming inserts with caution due to automatic reclustering in the background
