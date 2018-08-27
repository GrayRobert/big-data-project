package com.dbs.utils.hbase;

import com.dbs.Constants;
import com.dbs.utils.SqlLiteHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.SQLException;

public class CopyProfanityDataToSqlLiteDb {
    public static void run() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        ResultScanner scanResult = null;
        try {
            table = connection.getTable(TableName.valueOf(Constants.HBASE_PROFANITY_SUMMARY_TABLE_NAME));

            Scan scan = new Scan();

            scanResult = table.getScanner(scan);
            int id = 0;
            for (Result res : scanResult) {

                for (Cell cell : res.listCells()) {
                    String row = new String(CellUtil.cloneRow(cell));

                    String key[] = row.split(":");
                    String genre = key[0];
                    String year = key[1];

                    Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                    ++id;
                    //System.out.println("Inserting data for: " + id + " " + genre + " " + year + " " + value);
                    try {
                        SqlLiteHelper.insertProfanityData(id,genre,year,value);
                    } catch (SQLException e) {
                        System.out.println("Could not insert data for: " + id + " " + genre + " " + year + " " + value);
                        e.printStackTrace();
                    }
                }

            }
            System.out.println("Finished inserting data into SQLLite Database: " + id + " rows");
        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }
            if (scanResult != null) {
                scanResult.close();
            }
        }
    }
}



