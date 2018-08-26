package com.dbs.utils.mapreduce;

import com.dbs.utils.ProfanityChecker;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ProfanityMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

    public static final byte[] CF_SONG = "song".getBytes();
    public static final byte[] CF_META = "meta".getBytes();
    public static final byte[] COL_META_YEAR = "year".getBytes();
    public static final byte[] COL_META_GENRE = "genre".getBytes();
    public static final byte[] COL_SONG_LYRICS = "lyrics".getBytes();

    public void map(ImmutableBytesWritable row, Result value, Context context)
            throws IOException, InterruptedException {
        String lyrics = Bytes.toString(value.getValue(CF_SONG,COL_SONG_LYRICS));
        String year = Bytes.toString(value.getValue(CF_META,COL_META_YEAR)).toUpperCase().trim();
        String genre = Bytes.toString(value.getValue(CF_META,COL_META_GENRE)).toUpperCase().trim();

        Pattern pattern = Pattern.compile("\\w+");
        Matcher matcher = pattern.matcher(lyrics);

        ProfanityChecker profanityChecker = new ProfanityChecker();

        while (matcher.find()) {

            String word = matcher.group().toUpperCase().trim();

            // Skip word if it's not profanity
            if(!profanityChecker.checkWord(word)) {
                System.out.println("Skipping word that is not profanity: " + word);
                continue;
            } else {
                System.out.println("Mapping word identified as profanity: " + word);
                String key = genre + ":" + year;
                ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
                outputKey.set(key.getBytes());
                IntWritable outputValue = new IntWritable(1);
                context.write(outputKey, outputValue);
                System.out.println(Bytes.toString(outputKey.get()) + " " + outputValue.get());
            }
        }
    }
}
