package com.dbs.utils.hbase;

import com.dbs.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanDataInSongDataSummaryTable {
    public static void run() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        ResultScanner scanResult = null;
        try {
            table = connection.getTable(TableName.valueOf(Constants.HBASE_SUMMARY_TABLE_NAME));

            Scan scan = new Scan();

            scanResult = table.getScanner(scan);
            for (Result res : scanResult) {
                for (Cell cell : res.listCells()) {
                    String row = new String(CellUtil.cloneRow(cell));
                    String family = new String(CellUtil.cloneFamily(cell));
                    String column = new String(CellUtil.cloneQualifier(cell));
                    Integer value = Bytes.toInt(CellUtil.cloneValue(cell));

                    System.out.println(row + " " + family + " " + column + " " + value);
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



