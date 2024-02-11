# Question 1. What is count of records for the 2022 Green Taxi Data?

840 402

# Question 2. What is the estimated amount of data in the tables?

0 MB for the External Table and 6.41MB for the Materialized Table

```sql
SELECT DISTINCT PULocationID
FROM `week3-hw.week3_dataset.week3_table_partitioned_tripdata_2`
```

# Question 3. How many records have a fare_amount of 0?

1 622

# Question 4. What is the best strategy to make an optimized table in Big Query?

Partition by `lpep_pickup_datetime` Cluster on `PUlocationID`

# Question 5. What's the size of the tables?

12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

```sql
SELECT DISTINCT PULocationID
FROM `week3-hw.week3_dataset.week3_table_partitioned_tripdata_2`
WHERE CAST(lpep_pickup_datetime as DATE) > '2022-06-01' AND CAST(lpep_pickup_datetime as DATE) <= '2022-06-30';
```

# Question 6. Where is the data stored in the External Table you created?

GCP Bucket

# Question 7. It is best practice in Big Query to always cluster your Data?

False
