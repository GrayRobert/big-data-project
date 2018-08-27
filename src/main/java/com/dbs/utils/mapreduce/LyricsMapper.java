package com.dbs.utils.mapreduce;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LyricsMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {

    public static final byte[] CF_SONG = "song".getBytes();
    public static final byte[] CF_META = "meta".getBytes();
    public static final byte[] COL_META_YEAR = "year".getBytes();
    public static final byte[] COL_META_GENRE = "genre".getBytes();
    public static final byte[] COL_SONG_LYRICS = "lyrics".getBytes();

    // A couple of words to ignore because they skew the results and the 3 letter rule won't catch them.
    List<String> ignoreList = Arrays.asList("THIS","THAT","THEY","YOUR","CHORUS","VERSE","MUSIC","INSTRUMENTAL","LYRICS","FROM",
                                            "WHEN","WITH","WHAT","WHERE","HAVE","HERE","WILL");

    public void map(ImmutableBytesWritable row, Result value, Context context)
            throws IOException, InterruptedException {
        String lyrics = Bytes.toString(value.getValue(CF_SONG,COL_SONG_LYRICS));
        String year = Bytes.toString(value.getValue(CF_META,COL_META_YEAR)).toUpperCase().trim();
        String genre = Bytes.toString(value.getValue(CF_META,COL_META_GENRE)).toUpperCase().trim();

        Pattern pattern = Pattern.compile("\\w+");
        Matcher matcher = pattern.matcher(lyrics);
        while (matcher.find()) {

            String word = matcher.group().toUpperCase().trim();

            //Skip list of uninteresting words
            if (ignoreList.contains(word) || word.length() <= 3){
                //System.out.println("Skipped word " + word + " length:" + word.length());
                continue;
            }

            String key = year + ":" + genre + ":" + word;

            ImmutableBytesWritable outputKey = new ImmutableBytesWritable();

            outputKey.set(key.getBytes());
            IntWritable outputValue = new IntWritable(1);
            context.write(outputKey, outputValue);
            //System.out.println(Bytes.toString(outputKey.get()) + " " + outputValue.get());
        }
    }
}
