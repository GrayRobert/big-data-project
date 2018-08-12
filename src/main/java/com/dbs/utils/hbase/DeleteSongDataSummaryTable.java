package com.dbs.utils.hbase;

import com.dbs.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class DeleteSongDataSummaryTable {

    public static void run() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(Constants.HBASE_SUMMARY_TABLE_NAME);

            if (admin.tableExists(tableName)) {
                System.out.print("Table exists, Deleting.. ");

                admin.disableTable(tableName);
                admin.deleteTable(tableName);

                System.out.println(" Done.");
            } else {
                System.out.println("Table does not exist.");
            }
        } finally {
            connection.close();
        }
    }

}