# Big Data Project - DBS Assignment

Project scope is to source a large dataset and programatically process it in a big data environment to produce a smaller summarized dataset suitable for analysis.

## Requirements:
1. Hadoop
2. Hbase
3. Java JDK
4. Maven

## Usage: 
1. Import the maven project
2. Update the Constants.java file appropriately
3. Run the job with mvn exec:java

### Presuming hadoop and hbase are setup correctly the job does the following tasks

1. Stores songdata.csv file in hdfs
2. Creates table to store songdata in hbase
3. Imports data into hbase table
4. Creates table to store summarised data in hbase
5. Runs MapReduce on source data producing a summary couny of words used in song lyrics and stores data in hbase summary table
6. Copys the summary table data to a more suitable SQL Lite database for further processing and analysis
