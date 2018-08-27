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

Note: Main.java executes each of the jobs and is a good starting point when reviewing the code.

### Presuming hadoop and hbase are setup correctly the job does the following tasks

1. Stores lyrics.csv file in hdfs
2. Creates table to store songdata in hbase
3. Imports data into hbase table
4. Creates tables to store summarised data in hbase
5. Runs MapReducers on source data producing a summary data for the 360,000+ song lyrics and stores the summary data in a hbase tables
6. Copies the hbase summary tables to a more suitable SQL Lite database for further processing and analysis
