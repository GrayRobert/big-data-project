package com.dbs.utils.hdfs;

import com.dbs.Constants;
import org.apache.hadoop.conf.Configuration;

public class ReadCSVFileFromHDFS {


    public static String run() throws Exception {

        String data = "";

        HDFSFileIO hdfs = new HDFSFileIO();

        Configuration conf = new Configuration();

        conf.set(Constants.HDFS_NAME, Constants.HDFS_PATH);

        try {
            System.out.println("Reading csv file from hdfs: " + Constants.TEMP_CSV_FILE);
            data = hdfs.readFile(Constants.TEMP_CSV_FILE,conf);
        } catch (Exception e) {
            System.out.println("Could not create temp data directory: Error");
            e.printStackTrace();
        }

        return data;

    }

}
