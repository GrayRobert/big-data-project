package com.dbs;

import org.apache.hadoop.conf.Configuration;

public class Constants {

    //Don't think i needed these -- For connecting to Kaggle API
    public static final String KAGGLE_USERNAME = "username";
    public static final String KAGGLE_PASSWORD = "password";

    //CSV file
    public static final String CSV_FILE = "/Users/robertgray/sandbox/big-data-project/src/main/resources/temp/data/lyrics.csv";
    public static final String PROFANITY_CSV_FILE = "/Users/robertgray/sandbox/big-data-project/src/main/resources/temp/data/profanity.csv";

    //Temp directory for store data in HDFS
    public static final String TEMP_DATA_DIRECTORY = "/bigdataproject/data";
    public static final String TEMP_FILE_NAME = "lyrics.csv";
    public static final String TEMP_CSV_FILE = TEMP_DATA_DIRECTORY + "/" + TEMP_FILE_NAME;

    //HDFS Configuration Settings
    public static final String HDFS_PATH = "hdfs://localhost:9000";
    public static final String HDFS_NAME = "fs.defaultFS";


    public static final String HBASE_TABLE_NAME = "songdata";
    public static final String HBASE_SUMMARY_TABLE_NAME = "songdata_summary";
    public static final String HBASE_PROFANITY_SUMMARY_TABLE_NAME = "profanity_summary";

    //SQL Lite Setings
    public static final String SQL_LITE_DATABASE = "/Users/robertgray/sandbox/big-data-project/src/main/resources/temp/data/summary.db";
}
