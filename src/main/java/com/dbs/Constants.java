package com.dbs;

import org.apache.hadoop.conf.Configuration;

public class Constants {

    //Don't think i needed these -- For connecting to Kaggle API
    public static final String KAGGLE_USERNAME = "grayrobert";
    public static final String KAGGLE_PASSWORD = "Remember2018!";

    //Direct download url for data csv file
    public static final String CSV_FILE = "/Users/robertgray/sandbox/bigdataproject/src/main/resources/temp/data/songdata.csv";

    //Temp directory for store data in HDFS
    public static final String TEMP_DATA_DIRECTORY = "/bigdataproject/data";
    public static final String TEMP_FILE_NAME = "songdata.csv";
    public static final String TEMP_CSV_FILE = TEMP_DATA_DIRECTORY + "/" + TEMP_FILE_NAME;

    //HDFS Configuration Settings
    public static final String HDFS_PATH = "hdfs://localhost:9000";
    public static final String HDFS_NAME = "fs.defaultFS";


    public static final String HBASE_TABLE_NAME = "songdata";
    public static final String HBASE_SUMMARY_TABLE_NAME = "songdata_summary";

    //SQL Lite Setings
    public static final String SQL_LITE_DATABASE = "/Users/robertgray/sandbox/bigdataproject/src/main/resources/temp/data/summary.db";
}
