package com.dbs.utils.mapreduce;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LyricsMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

    public static final byte[] CF = "song".getBytes();
    public static final byte[] COL = "lyrics".getBytes();

    public void map(ImmutableBytesWritable row, Result value, Context context)
            throws IOException, InterruptedException {
        String line = Bytes.toString(value.getValue(CF,COL));

        Pattern pattern = Pattern.compile("\\w+");
        Matcher matcher = pattern.matcher(line);
        while (matcher.find()) {

            ImmutableBytesWritable outputKey = new ImmutableBytesWritable();

            outputKey.set(matcher.group().toUpperCase().trim().getBytes());
            IntWritable outputValue = new IntWritable(1);
            context.write(outputKey, outputValue);
            System.out.println(Bytes.toString(outputKey.get()) + " " + outputValue.get());
        }
    }
}
