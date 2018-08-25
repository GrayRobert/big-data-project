package com.dbs.utils.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;



public class LyricsReducer extends
        TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>
{
    public static final byte[] CF = "words".getBytes();
    public static final byte[] COUNT = "count".getBytes();

    public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        String  keyString = Bytes.toString(key.get());

        if(!keyString.equals("")) {
            for(IntWritable value : values)
            {
                sum += value.get();
            }
        }

        System.out.println("The key is " + keyString + " and the value is " + sum);

        Put put = new Put(key.get());
        put.addColumn(CF, COUNT, Bytes.toBytes(sum));

        context.write(key, put);
    }
}
