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

public class CopyDataToSqlLiteDb {
    public static void run() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        ResultScanner scanResult = null;
        try {
            table = connection.getTable(TableName.valueOf(Constants.HBASE_SUMMARY_TABLE_NAME));

            Scan scan = new Scan();

            scanResult = table.getScanner(scan);
            int id = 0;
            for (Result res : scanResult) {

                for (Cell cell : res.listCells()) {
                    String row = new String(CellUtil.cloneRow(cell));
                    String family = new String(CellUtil.cloneFamily(cell));
                    String column = new String(CellUtil.cloneQualifier(cell));
                    Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                    ++id;
                    System.out.println("Inserting data for: " + id + " " + row + " " + value);
                    try {
                        SqlLiteHelper.insertData(id,row,value);
                    } catch (SQLException e) {
                        System.out.println("Could not insert data for: " + id + " " + row + " " + value);
                        e.printStackTrace();
                    }
                }
            }
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



