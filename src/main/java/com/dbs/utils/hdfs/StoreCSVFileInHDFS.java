package com.dbs.utils.hdfs;

import com.dbs.Constants;
import com.dbs.utils.hdfs.HDFSFileIO;
import org.apache.hadoop.conf.Configuration;

public class StoreCSVFileInHDFS {

    public static void main(String[] args) throws Exception {

        run();

    }

    public static void run() throws Exception {
        HDFSFileIO hdfs = new HDFSFileIO();

        Configuration conf = new Configuration();

        conf.set(Constants.HDFS_NAME, Constants.HDFS_PATH);

        try {
            System.out.println("Creating data directory in hdfs: " + Constants.TEMP_DATA_DIRECTORY);
            hdfs.mkdir(Constants.TEMP_DATA_DIRECTORY, conf);
        } catch (Exception e) {
            System.out.println("Could not create temp data directory: Error");
            e.printStackTrace();
        }

        /* NOTE: was going to connect directly to Kaggle to pull down csv file and stream directly to hdfs.
                 The presumption here is that the data might be too big to pull down to the local file system
                 and so might need to be streamed directly to hadoop. However i ran into authentication problems
                 with Kaggle and ultimately time constraints so opted instead to just add from local directory for
                 the purpose of the assignment.
        */



        try {
            System.out.println("Adding file to hdfs: " + Constants.CSV_FILE);
            hdfs.deleteFile("/bigdataproject/data/songdata.csv",conf);
            hdfs.addFile(Constants.CSV_FILE, Constants.TEMP_DATA_DIRECTORY,conf);
        } catch (Exception e) {
            System.out.println("Could not add file to hdfs: Error");
            e.printStackTrace();
        }

        //hdfs.deleteFile("/bigdataproject/data/songdata.csv",conf);




    }

}
