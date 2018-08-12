package com.dbs.utils.hbase;

import com.dbs.Constants;
import com.dbs.utils.hdfs.HDFSFileIO;
import com.dbs.utils.hdfs.ReadCSVFileFromHDFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AddDataToSongDataTable {

    private static byte[] META_CF = Bytes.toBytes("meta");
    private static byte[] SONG_CF = Bytes.toBytes("song");

    private static byte[] ARTIST_COLUMN = Bytes.toBytes("artist");
    private static byte[] SONG_TITLE_COLUMN = Bytes.toBytes("song");
    private static byte[] SONG_LYRICS_COLUMN = Bytes.toBytes("lyrics");


    public static void run() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(Constants.HBASE_TABLE_NAME));

            List<Put> songList = new ArrayList<Put>();

            String data = ReadCSVFileFromHDFS.run();

            List<String> dataRows = Arrays.asList(data.split("(?<=\")\\n(?=\")"));


            for (int i = 1; i < dataRows.size(); i++) {
                String row = dataRows.get(i);
                Put record = new Put(Bytes.toBytes(i));

                List<String> dataColumns = Arrays.asList(row.split("(?<=\"),(?=\")"));

                String artist = dataColumns.get(0).replace("\"", "");
                String song = dataColumns.get(1).replace("\"", "");
                String lyrics = dataColumns.get(3).replace("\"", "");

                record.addColumn(META_CF, ARTIST_COLUMN, Bytes.toBytes(artist));
                record.addColumn(META_CF, SONG_TITLE_COLUMN, Bytes.toBytes(song));
                record.addColumn(SONG_CF, SONG_LYRICS_COLUMN, Bytes.toBytes(lyrics));

                table.put(record);

                System.out.println(i + ": Inserted song " + song + " by artist " + artist);
            }
            System.out.println("Finished adding data");
        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }
        }
    }
}






















