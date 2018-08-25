package com.dbs.utils.hbase;

import com.dbs.Constants;
import com.dbs.utils.hdfs.HDFSFileIO;
import com.dbs.utils.hdfs.ReadCSVFileFromHDFS;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
import java.io.Reader;
import java.io.StringReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AddDataToSongDataTable {

    private static byte[] META_CF = Bytes.toBytes("meta");
    private static byte[] SONG_CF = Bytes.toBytes("song");

    private static byte[] META_TITLE_COLUMN = Bytes.toBytes("title");
    private static byte[] META_YEAR_COLUMN = Bytes.toBytes("year");
    private static byte[] META_ARTIST_COLUMN = Bytes.toBytes("artist");
    private static byte[] META_GENRE_COLUMN = Bytes.toBytes("genre");
    private static byte[] SONG_LYRICS_COLUMN = Bytes.toBytes("lyrics");


    public static void run() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(Constants.HBASE_TABLE_NAME));

            List<Put> songList = new ArrayList<Put>();

            String data = ReadCSVFileFromHDFS.run();

            try (
                    Reader reader = new StringReader(data);
                    CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            ) {
                for (CSVRecord csvRecord : csvParser) {
                    Put record = new Put(Bytes.toBytes(csvRecord.get(0)));

                    String title = csvRecord.get(1);
                    String year = csvRecord.get(2);
                    String artist = csvRecord.get(3);
                    String genre = csvRecord.get(4);
                    String lyrics = csvRecord.get(5);

                    record.addColumn(META_CF, META_TITLE_COLUMN, Bytes.toBytes(title));
                    record.addColumn(META_CF, META_YEAR_COLUMN, Bytes.toBytes(year));
                    record.addColumn(META_CF, META_ARTIST_COLUMN, Bytes.toBytes(artist));
                    record.addColumn(META_CF, META_GENRE_COLUMN, Bytes.toBytes(genre));
                    record.addColumn(SONG_CF, SONG_LYRICS_COLUMN, Bytes.toBytes(lyrics));

                    table.put(record);

                    System.out.println(csvRecord.get(0) + ": Inserted song " + title + " by artist " + artist);
                }
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






















