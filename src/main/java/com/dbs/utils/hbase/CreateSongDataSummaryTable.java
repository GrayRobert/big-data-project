package com.dbs.utils.hbase;

import com.dbs.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class CreateSongDataSummaryTable {

    public static void run() {

        Connection connection = null;

        try {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            try {
                Admin admin = connection.getAdmin();

                HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf(Constants.HBASE_SUMMARY_TABLE_NAME));

                tableName.addFamily(new HColumnDescriptor("words"));

                if (!admin.tableExists(tableName.getTableName())) {
                    System.out.print("Creating the " + Constants.HBASE_TABLE_NAME + " table. ");

                    admin.createTable(tableName);

                    System.out.println("Done.");
                } else {
                    System.out.println("Table already exists");
                }
            } finally {
                connection.close();
            }
        } catch (IOException e) {
            System.out.println("Could not create table : ");
            e.printStackTrace();
        }

    }
}





