# Deeptanshu Jariwala - CWID: A20448079

# ITMD-521 Assignment - Cluster Read & Write

## Cluster Command

Command for CSV File
```bash
spark-submit --verbose --name dmj-read-write-csv-2008.py --master yarn --deploy-mode cluster read-write-csv-2008.py
```
Command for Parquet File
```bash
spark-submit --verbose --name dmj-read-write-parquet-2008.py --master yarn --deploy-mode cluster read-write-parquet-2008.py
```

### Your Explanation

DELIVERABLE 1
I used the following function from PySpark 2.4.5 documentation:  
pyspark.sql.functions.year(col) 
The actual code I created from refering to above function is:
df1.withColumn("Year",year(col("ObservationDate")))
This created a separate column named "Year" and extracted year from Observation Date and dispalyed it in the column "Year" as an Integer. 
After the year is extracted from the Observation Date, I applied the filter function as mentioned below:
df3.filter(df3.Year=='2008')
The filter fucntion then matches the Year 2008 from the Observation Date ranging from years 2000 to 2018 from the file 2000-2018.txt. 

DELIVERABLE 2
I refered the TextBook "Spark: The Definitive Guide" to find the different read modes used by spark to process bad/corrupted data. The deafult mode is called as "Permissive" that sets the records to null when it encounters bad/corrupted data. Since it is the default mode used by spark, it has been applied to our assigment schema and would display null for bad/corrupted data. And looking at "99999" as the bad data, the schema has deined it as a value. We can handle this by using function fillna() as menttioned in PySpark 2.4.5 documentation that allows us to replace null values with desired user input. 

The other two read modes are as follows:
1) dropMalformed : Drops the row that contains malformed records
2) failFast : Fails immediately upon encountering malformed records

DELIVERABLE 3
During the reading of file 2000-2018.txt, we found that data can vary in gigabytes and take huge processing time. To ideally optimize the use of resources we can choose file formats that are splittable e.g. gzip, bzip2, or lz4 that will partition the huge input data into separate files, ideally each no larger than a few hundred megabytes. 
To further enhance the performance we can imply different methodologies such as:
1) Parallelism - Increasing the degree of parallelism helps in optimizing the processing time. This can be done by setting the parallelism property and tuning the partitions according to the number of cores in your cluster.
2) Improved Filtering -  Moving filters to the earliest part of your Spark job can avoid reading and working with data that is irrelevant to our end result.  
3) Custom partitioning - Performing custom partitioning allows us to define a custom partition function that will organize the data across the cluster to a finer level of precision than is available at the DataFrame level.

Reference: "Spark: The Definitive Guide"



