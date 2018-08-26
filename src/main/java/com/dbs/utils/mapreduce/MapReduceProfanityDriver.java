package com.dbs.utils.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;


public class MapReduceProfanityDriver {


    public static void run() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapReduceProfanityDriver.class);

        String sourceTable = "songdata";
        String targetTable = "profanity_summary";

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.addDependencyJars(job);
        
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,
                scan,
                ProfanityMapper.class,
                ImmutableBytesWritable.class,
                IntWritable.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                targetTable,
                ProfanityReducer.class,
                job);
        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }

}
